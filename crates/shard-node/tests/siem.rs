//! SIEM generality proof: point the SAME store + aggregation code at SECURITY events and
//! show the SOC workflows fall out with zero new machinery — detection breakdowns, entity
//! pivots (co-occurrence), severity distributions, and a timeline are all just the existing
//! search / filter / aggregate over security-shaped documents.

use common::pb::filter_condition::Test;
use common::pb::{AggKind, AggregateRequest, Filter, FilterCondition, NumericRange};
use shard_node::agg::partial;
use shard_node::ingest::{FlightSource, SecuritySource};
use shard_node::store::ShardStore;

async fn security_store(n: usize) -> ShardStore {
    let src = SecuritySource::new(1, n);
    let batch = src.fetch().await.unwrap();
    let mut store = ShardStore::new();
    for d in batch {
        store.insert(d);
    }
    store
}

fn agg(kind: AggKind, field: &str, interval: f64, filter: Option<Filter>) -> AggregateRequest {
    AggregateRequest {
        query: String::new(),
        kind: kind as i32,
        field: field.to_string(),
        interval,
        percentiles: vec![50.0, 95.0, 99.0],
        filter,
    }
}

#[tokio::test]
async fn security_events_ingest_and_the_soc_workflows_are_just_aggregations() {
    let store = security_store(400).await;
    let docs: Vec<&common::pb::FlightDocument> = store.matching("");
    assert_eq!(docs.len(), 400, "security events ingested into the same store, unchanged");

    // Detection breakdown: count by event category (aircraft_type) — a value-counts agg.
    let p = partial(&docs, &agg(AggKind::AggValueCounts, "aircraft_type", 0.0, None));
    assert!(p.buckets.contains_key("auth_fail"), "categories aggregate: {:?}", p.buckets.keys().collect::<Vec<_>>());
    assert!(p.buckets.values().sum::<u64>() == 400);

    // Entity pivot / correlation: WHICH source IPs are failing auth — filter
    // category=auth_fail, then value-counts by source IP (origin). Co-occurrence via a
    // filtered aggregation, no new code.
    let auth_fail = Filter {
        conditions: vec![FilterCondition {
            field: "aircraft_type".into(),
            test: Some(Test::Equals("auth_fail".into())),
        }],
    };
    let filtered: Vec<&common::pb::FlightDocument> =
        docs.iter().copied().filter(|d| common::filter::passes(d, Some(&auth_fail))).collect();
    let pivot = partial(&filtered, &agg(AggKind::AggValueCounts, "origin", 0.0, None));
    assert!(!pivot.buckets.is_empty(), "auth-fail-by-source-IP pivot returns offenders");
    assert!(pivot.count < 400 && pivot.count > 0, "the filter narrowed to auth_fail events only");

    // Severity triage: percentiles of severity (altitude field) via the t-digest.
    let sev = partial(&docs, &agg(AggKind::AggPercentiles, "altitude", 0.0, None));
    let d = sev.digest.unwrap();
    assert!(!d.centroid_mean.is_empty(), "severity percentiles computed over security events");

    // High-severity detection: filter severity >= 100, count what's left (a saved query).
    let high = Filter {
        conditions: vec![FilterCondition {
            field: "altitude".into(),
            test: Some(Test::Range(NumericRange { min: Some(100.0), max: None })),
        }],
    };
    let high_docs: Vec<&common::pb::FlightDocument> =
        docs.iter().copied().filter(|d| common::filter::passes(d, Some(&high))).collect();
    assert!(high_docs.len() < docs.len(), "the severity filter excludes low-severity events");

    // Timeline: events per time bucket — a time-histogram, exactly as for flights.
    let timeline = partial(&docs, &agg(AggKind::AggTimeHistogram, "", 50.0, None));
    assert!(!timeline.buckets.is_empty(), "event timeline buckets populated");
}

#[tokio::test]
async fn per_event_keying_retains_the_full_stream_and_the_entity_is_a_field() {
    // The learning made concrete: keying by unique EVENT id (icao24) retains every event
    // (200 in, 200 held — no upsert collapse), and the entity (source IP) is a
    // high-cardinality FIELD carried on `origin` that aggregations pivot on.
    let store = security_store(200).await;
    let docs = store.matching("");
    assert_eq!(docs.len(), 200, "per-event keying retains the whole stream (no collapse)");
    assert!(docs.iter().all(|d| d.icao24.starts_with("evt-")), "shard key is the event id");
    assert!(docs.iter().all(|d| d.origin.contains('.')), "the entity (source IP) is a field on origin");
}
