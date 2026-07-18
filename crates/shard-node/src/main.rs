//! Aether shard node (data plane) — Q1 gRPC server.
//!
//! Builds an in-memory inverted index, seeds it with a few synthetic documents (live
//! OpenSky ingestion is the next Q1 step and will replace the seed), and serves the
//! `ShardSearch.Search` RPC over gRPC. This is the single-node "search over gRPC" milestone.
//!
//! Config via env: `AETHER_SHARD_ADDR` (default 127.0.0.1:50051), `AETHER_SHARD_ID`
//! (default "shard-0").

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use common::pb::shard_search_server::ShardSearchServer;
use common::pb::FlightDocument;
use shard_node::index::InvertedIndex;
use shard_node::server::ShardSearchService;
use tonic::transport::Server;

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

fn seed_docs() -> Vec<FlightDocument> {
    vec![
        demo_doc("a1b2c3", "UAL231", "SFO", "JFK", "Boeing 737"),
        demo_doc("d4e5f6", "DAL45", "ATL", "LAX", "Airbus A320"),
        demo_doc("aa11bb", "UAL900", "ORD", "SFO", "Boeing 777"),
    ]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut index = InvertedIndex::new();
    for doc in seed_docs() {
        index.insert(doc);
    }
    let index = Arc::new(RwLock::new(index));

    let addr: SocketAddr = std::env::var("AETHER_SHARD_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:50051".to_string())
        .parse()?;
    let shard_id = std::env::var("AETHER_SHARD_ID").unwrap_or_else(|_| "shard-0".to_string());

    let service = ShardSearchService::new(index, shard_id.clone());
    println!("aether-shard-node '{shard_id}' serving ShardSearch on {addr}");

    Server::builder()
        .add_service(ShardSearchServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
