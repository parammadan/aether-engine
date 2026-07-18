//! Aether shard node (data plane) — Q1 single-node engine.
//!
//! Wires the three Q1 pieces together: a background task ingests live flight observations
//! from OpenSky into an in-memory inverted index (with backpressure), while the
//! `ShardSearch` gRPC server serves keyword queries against that same index.
//!
//! Config via env:
//!   AETHER_SHARD_ADDR   gRPC listen address   (default 127.0.0.1:50051)
//!   AETHER_SHARD_ID     this shard's id       (default "shard-0")
//!   AETHER_POLL_SECS    OpenSky poll interval (default 10; OpenSky rate-limits anonymous)
//!   OPENSKY_USERNAME / OPENSKY_PASSWORD   optional, raise the OpenSky rate limit

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use common::pb::shard_search_server::ShardSearchServer;
use shard_node::index::InvertedIndex;
use shard_node::ingest::{run_ingestion, OpenSkySource};
use shard_node::server::ShardSearchService;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = std::env::var("AETHER_SHARD_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:50051".to_string())
        .parse()?;
    let shard_id = std::env::var("AETHER_SHARD_ID").unwrap_or_else(|_| "shard-0".to_string());
    let poll_secs: u64 = std::env::var("AETHER_POLL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);

    // One index, shared by the ingestion writer and the gRPC readers.
    let index = Arc::new(RwLock::new(InvertedIndex::new()));

    // Ingestion runs in the background; the index starts empty and fills as snapshots arrive.
    let ingest_index = index.clone();
    tokio::spawn(async move {
        run_ingestion(
            OpenSkySource::from_env(),
            ingest_index,
            Duration::from_secs(poll_secs),
            None, // run forever
        )
        .await;
    });

    let service = ShardSearchService::new(index, shard_id.clone());
    println!(
        "aether-shard-node '{shard_id}' serving ShardSearch on {addr}; ingesting OpenSky every {poll_secs}s"
    );

    Server::builder()
        .add_service(ShardSearchServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
