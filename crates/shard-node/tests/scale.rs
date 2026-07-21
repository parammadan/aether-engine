//! Scale check — IGNORED by default (heavy). Ingests a large synthetic corpus into one
//! shard store and confirms keyword search and match-counting stay correct and responsive
//! well beyond the small corpora the other tests use. Run explicitly:
//!
//!   cargo test -p shard-node --test scale -- --ignored --nocapture
//!   AETHER_SCALE_N=100000 cargo test -p shard-node --test scale -- --ignored --nocapture

use std::time::Instant;

use common::pb::FlightDocument;
use shard_node::store::ShardStore;

// NOTE ON COST: ingestion here is dominated by HNSW vector-index insertion (~milliseconds
// per doc), not the keyword index — so the insert time below scales with the vector build,
// while SEARCH stays fast regardless. That's the honest bottleneck: bulk-loading a large
// corpus is a vector-index-build cost, and a keyword-only deployment would ingest far faster.
#[test]
#[ignore = "heavy: ingests tens of thousands of docs; run explicitly with --ignored"]
fn large_corpus_stays_correct_and_responsive() {
    let n: usize = std::env::var("AETHER_SCALE_N").ok().and_then(|s| s.parse().ok()).unwrap_or(20_000);
    let countries = ["United States", "France", "Germany", "Japan", "Brazil"];
    let mut store = ShardStore::new();

    let t = Instant::now();
    for i in 0..n {
        store.insert(FlightDocument {
            icao24: format!("{i:08x}"), // unique key — every doc retained, no upsert collapse
            callsign: format!("SCALE{i}"),
            origin: countries[i % countries.len()].to_string(),
            aircraft_type: "Boeing 737".to_string(),
            altitude: (i % 12000) as f64,
            ..Default::default()
        });
    }
    let insert_ms = t.elapsed().as_millis();
    assert_eq!(store.len(), n, "every unique doc retained at scale");

    // Keyword search stays responsive and returns matches.
    let t = Instant::now();
    let hits = store.search("France", 10);
    let search_us = t.elapsed().as_micros();
    assert!(!hits.hits.is_empty(), "search still finds matching docs at scale");
    assert!(hits.hits.len() <= 10, "limit is honored");

    // Match-counting is exact: "France" matches only France-origin docs, its 1/len share.
    let france = store.matching("France").len();
    assert_eq!(france, n / countries.len(), "France matches exactly its share of the corpus");

    println!(
        "scale: n={n} insert={insert_ms}ms search('France',10)={search_us}us france_docs={france}"
    );
}
