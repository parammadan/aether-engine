//! Aether shard node (data plane).
//!
//! Registers its shard with the coordinator (if configured), ingests live OpenSky data —
//! keeping only the documents this shard owns (`hash(icao24) % N == shard index`) — and
//! serves `ShardSearch` over gRPC against that shard's slice of the data.
//!
//! Config via env:
//!   AETHER_SHARD_ADDR         gRPC listen address       (default 127.0.0.1:50051)
//!   AETHER_SHARD_INDEX        this shard's index 0..N   (default 0)
//!   AETHER_SHARD_COUNT        N (total shards)          (default 1 = single-node)
//!   AETHER_COORDINATOR_ADDR   coordinator to register with (optional; skipped if unset)
//!   AETHER_NODE_ID            stable node id            (default "node-<index>")
//!   AETHER_POLL_SECS          OpenSky poll interval     (default 10)
//!   OPENSKY_USERNAME / OPENSKY_PASSWORD   optional, raise the OpenSky rate limit

use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use common::pb::shard_search_server::ShardSearchServer;
use common::pb::NodeRole;
use shard_node::cluster::register_with_coordinator;
use shard_node::index::InvertedIndex;
use shard_node::ingest::{run_ingestion, OpenSkySource, ShardAssignment};
use shard_node::server::ShardSearchService;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr_str = std::env::var("AETHER_SHARD_ADDR").unwrap_or_else(|_| "127.0.0.1:50051".to_string());
    let addr: SocketAddr = addr_str.parse()?;
    let shard_index: u32 = std::env::var("AETHER_SHARD_INDEX")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let shard_count: u32 = std::env::var("AETHER_SHARD_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let node_id = std::env::var("AETHER_NODE_ID").unwrap_or_else(|_| format!("node-{shard_index}"));
    let poll_secs: u64 = std::env::var("AETHER_POLL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);

    let shard_id_label = format!("shard-{shard_index}");

    // Register with the coordinator if one is configured. A failure here is logged but does
    // NOT stop the node from serving: the data plane should keep running even if the control
    // plane is briefly unreachable.
    if let Ok(coord_addr) = std::env::var("AETHER_COORDINATOR_ADDR") {
        match register_with_coordinator(&coord_addr, &node_id, &addr_str, shard_index, NodeRole::Leader).await {
            Ok(n) => println!("registered '{node_id}' as leader of shard {shard_index} with coordinator (cluster N={n})"),
            Err(e) => eprintln!("warning: could not register with coordinator at {coord_addr}: {e}"),
        }
    }

    // Only filter by shard when the cluster has more than one shard.
    let assignment = match NonZeroU32::new(shard_count) {
        Some(count) if shard_count > 1 => Some(ShardAssignment { index: shard_index, count }),
        _ => None,
    };

    let index = Arc::new(RwLock::new(InvertedIndex::new()));

    let ingest_index = index.clone();
    tokio::spawn(async move {
        run_ingestion(
            OpenSkySource::from_env(),
            ingest_index,
            Duration::from_secs(poll_secs),
            None,
            assignment,
        )
        .await;
    });

    let service = ShardSearchService::new(index, shard_id_label.clone());
    println!(
        "aether-shard-node '{shard_id_label}' serving ShardSearch on {addr}; shard {shard_index}/{shard_count}; ingesting OpenSky every {poll_secs}s"
    );

    Server::builder()
        .add_service(ShardSearchServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
