//! Live test: semantic vector search through the full ShardStore with the ONNX embedder.
//! Ignored by default (needs model files — scripts/fetch-model.sh) and the `onnx` feature:
//!
//!   cargo test -p shard-node --features onnx --test onnx_store -- --ignored --nocapture
#![cfg(feature = "onnx")]

use std::path::PathBuf;
use std::sync::Arc;

use common::embed::Embedder;
use common::embed_onnx::OnnxEmbedder;
use common::pb::FlightDocument;
use shard_node::store::ShardStore;

fn model_dir() -> PathBuf {
    std::env::var("AETHER_ONNX_MODEL_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../models/all-MiniLM-L6-v2"))
}

fn doc(icao24: &str, callsign: &str, origin: &str, aircraft: &str) -> FlightDocument {
    FlightDocument {
        icao24: icao24.to_string(),
        callsign: callsign.to_string(),
        origin: origin.to_string(),
        aircraft_type: aircraft.to_string(),
        ..Default::default()
    }
}

#[test]
#[ignore = "needs model files on disk (scripts/fetch-model.sh)"]
fn store_answers_semantic_queries_with_no_token_overlap() {
    let embedder = Arc::new(OnnxEmbedder::from_dir(&model_dir()).expect("model should load"));
    let mut store = ShardStore::with_embedder(embedder.clone());

    store.insert(doc("us1", "UAL231", "United States", "Boeing 737"));
    store.insert(doc("fr1", "AFR006", "France", "Airbus A320"));

    // "american" and "jet" share no tokens with either document's text — only meaning links
    // the query to the United States / Boeing document.
    let query = embedder.embed("an american jet airliner");
    let hits = store.vector_search(&query, 2);

    assert_eq!(hits.len(), 2);
    assert_eq!(
        hits[0].doc.icao24, "us1",
        "expected the United States/Boeing doc to rank first for 'american jet airliner'"
    );
    assert!(hits[0].score > hits[1].score);
}
