//! Shard-side partial aggregates: one pass over the matching documents produces an
//! `AggregatePartial` the coordinator can merge with every other shard's. No per-document
//! RPC amplification — the shard summarizes locally and ships the summary.

use std::collections::HashMap;

use common::pb::{AggKind, AggregatePartial, AggregateRequest, FlightDocument, TDigest};
use common::tdigest;

/// A text field's value for value-counts, or `None` if the name isn't a text field.
fn text_field<'a>(doc: &'a FlightDocument, field: &str) -> Option<&'a str> {
    match field {
        "callsign" => Some(&doc.callsign),
        "origin" => Some(&doc.origin),
        "destination" => Some(&doc.destination),
        "aircraft_type" => Some(&doc.aircraft_type),
        "tenant_id" => Some(&doc.tenant_id),
        // Generic connector-supplied text field.
        _ => doc.text.get(field).map(String::as_str),
    }
}

/// A numeric field's value for histograms/percentiles, or `None` if not numeric.
fn numeric_field(doc: &FlightDocument, field: &str) -> Option<f64> {
    match field {
        "altitude" => Some(doc.altitude),
        "velocity" => Some(doc.velocity),
        "heading" => Some(doc.heading),
        "vertical_rate" => Some(doc.vertical_rate),
        "latitude" => Some(doc.latitude),
        "longitude" => Some(doc.longitude),
        "observed_at" => Some(doc.observed_at as f64),
        // Generic connector-supplied numeric field.
        _ => doc.number.get(field).copied(),
    }
}

/// Bucket a value by a fixed interval, keyed by the bucket's lower bound. `interval <= 0`
/// falls back to width 1 so a missing/zero interval can't divide by zero.
fn bucket_key(value: f64, interval: f64) -> String {
    let w = if interval > 0.0 { interval } else { 1.0 };
    let lower = (value / w).floor() * w;
    // Integer-ish lower bounds print cleanly; keys are only ever compared for equality.
    format!("{lower}")
}

/// Compute this shard's partial aggregate over `docs` (already filtered by the query).
pub fn partial(docs: &[&FlightDocument], req: &AggregateRequest) -> AggregatePartial {
    let kind = req.kind();
    let mut buckets: HashMap<String, u64> = HashMap::new();
    let mut digest = tdigest::TDigest::new();
    let mut count = 0u64;

    for doc in docs {
        count += 1;
        match kind {
            AggKind::AggUnspecified | AggKind::AggCount => {}
            AggKind::AggValueCounts => {
                if let Some(v) = text_field(doc, &req.field) {
                    *buckets.entry(v.to_string()).or_insert(0) += 1;
                }
            }
            AggKind::AggTimeHistogram => {
                *buckets.entry(bucket_key(doc.observed_at as f64, req.interval)).or_insert(0) += 1;
            }
            AggKind::AggNumericHistogram => {
                if let Some(v) = numeric_field(doc, &req.field) {
                    *buckets.entry(bucket_key(v, req.interval)).or_insert(0) += 1;
                }
            }
            AggKind::AggGeoGrid => {
                let cell = if req.interval > 0.0 { req.interval } else { 1.0 };
                let lat = (doc.latitude / cell).floor() * cell;
                let lon = (doc.longitude / cell).floor() * cell;
                *buckets.entry(format!("{lat},{lon}")).or_insert(0) += 1;
            }
            AggKind::AggPercentiles => {
                if let Some(v) = numeric_field(doc, &req.field) {
                    digest.add(v);
                }
            }
        }
    }

    let digest_msg = if kind == AggKind::AggPercentiles {
        let (centroid_mean, centroid_weight) = digest.to_parts();
        Some(TDigest { centroid_mean, centroid_weight })
    } else {
        None
    };

    AggregatePartial {
        kind: kind as i32,
        count,
        buckets,
        digest: digest_msg,
    }
}
