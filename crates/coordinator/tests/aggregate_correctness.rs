//! The aggregation correctness property: for EVERY agg type, sharding the corpus and
//! merging the per-shard partials yields the same answer as computing over the whole
//! corpus on one node — across several random shardings. This is what makes distributed
//! aggregation trustworthy: distribution is invisible in the result.
//!
//! Runs against the real shard-side partial (`shard_node::agg::partial`) and the real
//! coordinator merge (`coordinator::agg::merge_partials`) — no network, so it's fast and
//! deterministic, but it exercises the exact code the RPC path uses.

use std::collections::BTreeMap;

use common::pb::{AggKind, AggregateRequest, FlightDocument};
use coordinator::agg::merge_partials;
use shard_node::agg::partial;

/// Deterministic pseudo-random corpus: varied origins, aircraft, altitudes, positions.
fn corpus(n: usize, seed: u64) -> Vec<FlightDocument> {
    let mut rng = seed;
    let mut next = move || {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (rng >> 33) as u64
    };
    let origins = ["SFO", "JFK", "ATL", "ORD", "DEN", "LAX"];
    let crafts = ["Boeing 737", "Airbus A320", "Boeing 777", "Embraer E190"];
    (0..n)
        .map(|i| FlightDocument {
            icao24: format!("ac{i:05}"),
            callsign: format!("FL{i}"),
            origin: origins[(next() % origins.len() as u64) as usize].to_string(),
            aircraft_type: crafts[(next() % crafts.len() as u64) as usize].to_string(),
            altitude: (next() % 12000) as f64,
            velocity: (next() % 600) as f64,
            latitude: (next() % 180) as f64 - 90.0,
            longitude: (next() % 360) as f64 - 180.0,
            observed_at: 1_700_000_000_000 + (next() % 100_000) as i64,
            ..Default::default()
        })
        .collect()
}

/// Split a corpus into `shards` disjoint slices by a stable hash of the key — a realistic
/// sharding, and a different one for each `shard_seed` via rotation.
fn shard_it(docs: &[FlightDocument], shards: usize, rot: usize) -> Vec<Vec<FlightDocument>> {
    let mut out = vec![Vec::new(); shards];
    for (i, d) in docs.iter().enumerate() {
        out[(i + rot) % shards].push(d.clone());
    }
    out
}

/// Compute the merged partial the distributed way, and the single-node partial the direct
/// way, for one request; return both bucket maps (sorted) and counts.
fn distributed_vs_single(
    docs: &[FlightDocument],
    shards: usize,
    rot: usize,
    req: &AggregateRequest,
) -> (BTreeMap<String, u64>, u64, BTreeMap<String, u64>, u64) {
    // Distributed: each shard computes a partial, coordinator merges.
    let parts: Vec<_> = shard_it(docs, shards, rot)
        .iter()
        .map(|slice| {
            let refs: Vec<&FlightDocument> = slice.iter().collect();
            partial(&refs, req)
        })
        .collect();
    let (merged, _pcts) = merge_partials(req.kind(), parts, &req.percentiles);

    // Single node: one partial over the whole corpus.
    let all: Vec<&FlightDocument> = docs.iter().collect();
    let single = partial(&all, req);

    (
        merged.buckets.into_iter().collect(),
        merged.count,
        single.buckets.into_iter().collect(),
        single.count,
    )
}

fn req(kind: AggKind, field: &str, interval: f64) -> AggregateRequest {
    AggregateRequest {
        query: String::new(),
        kind: kind as i32,
        field: field.to_string(),
        interval,
        percentiles: vec![50.0, 90.0, 99.0],
    }
}

#[test]
fn every_simple_agg_is_sharding_invariant() {
    let docs = corpus(2000, 42);
    let cases = [
        req(AggKind::AggCount, "", 0.0),
        req(AggKind::AggValueCounts, "origin", 0.0),
        req(AggKind::AggValueCounts, "aircraft_type", 0.0),
        req(AggKind::AggTimeHistogram, "", 10_000.0),
        req(AggKind::AggNumericHistogram, "altitude", 1000.0),
        req(AggKind::AggGeoGrid, "", 30.0),
    ];

    // Several shard counts and rotations — different shardings of the same corpus must all
    // merge to the identical single-node answer.
    for case in &cases {
        for &shards in &[1usize, 2, 3, 5, 8] {
            for rot in 0..shards {
                let (m_buckets, m_count, s_buckets, s_count) =
                    distributed_vs_single(&docs, shards, rot, case);
                assert_eq!(
                    m_count, s_count,
                    "{:?} field={}: count diverged (shards={shards}, rot={rot})",
                    case.kind(), case.field
                );
                assert_eq!(
                    m_buckets, s_buckets,
                    "{:?} field={}: buckets diverged (shards={shards}, rot={rot})",
                    case.kind(), case.field
                );
            }
        }
    }
}

#[test]
fn percentiles_from_merged_shards_track_the_single_node_digest() {
    // Percentiles can't be summed, so this is the sketch's proof: the merged-across-shards
    // percentile is close to the single-node percentile (and to exact) regardless of how
    // the corpus was split.
    let docs = corpus(6000, 7);
    let request = req(AggKind::AggPercentiles, "altitude", 0.0);

    // Exact reference.
    let mut alts: Vec<f64> = docs.iter().map(|d| d.altitude).collect();
    alts.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let exact = |p: f64| alts[(((p / 100.0) * (alts.len() as f64 - 1.0)).round() as usize).min(alts.len() - 1)];

    for &shards in &[1usize, 3, 7] {
        let parts: Vec<_> = shard_it(&docs, shards, 0)
            .iter()
            .map(|slice| {
                let refs: Vec<&FlightDocument> = slice.iter().collect();
                partial(&refs, &request)
            })
            .collect();
        let (_merged, pcts) = merge_partials(AggKind::AggPercentiles, parts, &request.percentiles);
        assert_eq!(pcts.len(), 3);
        for pc in &pcts {
            let e = exact(pc.p);
            let err = (pc.value - e).abs() / 12000.0; // spread of the altitude range
            assert!(err < 0.03, "shards={shards} p{}: {} vs exact {e} (rel err {err:.4})", pc.p, pc.value);
        }
    }
}
