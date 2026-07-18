//! Aether shard node (data plane) — Q1 demo binary.
//!
//! Not the real server yet: it builds a small in-memory index from synthetic documents and
//! runs a query, to show the inverted index working end-to-end. Still to come this quarter:
//! OpenSky ingestion (with backpressure) to fill the index, and the `ShardSearch` gRPC
//! server so it can be queried over the wire.

use std::num::NonZeroU32;

use common::pb::FlightDocument;
use shard_node::index::InvertedIndex;

fn demo_doc(icao24: &str, callsign: &str, origin: &str, destination: &str, aircraft: &str) -> FlightDocument {
    FlightDocument {
        icao24: icao24.to_string(),
        callsign: callsign.to_string(),
        origin: origin.to_string(),
        destination: destination.to_string(),
        aircraft_type: aircraft.to_string(),
        ..Default::default()
    }
}

fn main() {
    let mut index = InvertedIndex::new();
    index.insert(demo_doc("a1b2c3", "UAL231", "SFO", "JFK", "Boeing 737"));
    index.insert(demo_doc("d4e5f6", "DAL45", "ATL", "LAX", "Airbus A320"));
    index.insert(demo_doc("aa11bb", "UAL900", "ORD", "SFO", "Boeing 777"));

    let query = "sfo";
    let results = index.search(query, 10);
    println!(
        "aether-shard-node demo — {} docs indexed; query '{}' matched {}:",
        index.len(),
        query,
        results.total_matched
    );
    for hit in &results.hits {
        println!(
            "  {} {} ({} -> {})  score={}",
            hit.doc.icao24, hit.doc.callsign, hit.doc.origin, hit.doc.destination, hit.score
        );
    }

    // Which shard would this aircraft's documents live on in a 4-node cluster?
    let n = NonZeroU32::new(4).unwrap();
    println!(
        "shard for icao24=a1b2c3 with N=4 -> {}",
        common::shard::shard_for("a1b2c3", n)
    );
}
