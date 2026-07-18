//! Aether shard node (data plane).
//!
//! Registers its shard with the coordinator (if configured) and serves `ShardSearch` +
//! `Replication` over gRPC. A leader ingests live OpenSky data — keeping only the documents
//! this shard owns (`hash(icao24) % N == shard index`) — and replicates each indexed batch
//! to its follower(s). A follower does not ingest; its index fills from the leader's
//! replication stream, so it can be promoted to serve the shard if the leader dies.
//!
//! Config via env:
//!   AETHER_SHARD_ADDR         gRPC listen address       (default 127.0.0.1:50051)
//!   AETHER_SHARD_INDEX        this shard's index 0..N   (default 0)
//!   AETHER_SHARD_COUNT        N (total shards)          (default 1 = single-node)
//!   AETHER_ROLE               leader | follower         (default leader)
//!   AETHER_COORDINATOR_ADDR   coordinator to register with (optional; skipped if unset)
//!   AETHER_NODE_ID            stable node id            (default "node-<index>")
//!   AETHER_POLL_SECS          OpenSky poll interval     (default 10)
//!   OPENSKY_USERNAME / OPENSKY_PASSWORD   optional, raise the OpenSky rate limit

use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use common::pb::replication_server::ReplicationServer;
use common::pb::shard_search_server::ShardSearchServer;
use common::pb::NodeRole;
use shard_node::cluster::{register_with_coordinator, run_heartbeat};
use shard_node::store::ShardStore;
use shard_node::ingest::{run_ingestion, OpenSkySource, ShardAssignment};
use shard_node::replication::{run_replication, ReplicationService};
use shard_node::server::ShardSearchService;
use tonic::transport::Server;

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

/// Build the shard's document store with the configured embedder.
///
/// `AETHER_EMBEDDER=hash` (default) uses the deterministic feature-hashing embedder;
/// `AETHER_EMBEDDER=onnx` loads a learned sentence-transformer from
/// `AETHER_ONNX_MODEL_DIR` (requires building with `--features onnx`). Every node in a
/// cluster MUST use the same embedder and model — embeddings are a cross-node contract, and
/// the shard rejects query vectors whose dimension doesn't match its own.
fn build_store() -> Result<ShardStore, Box<dyn std::error::Error + Send + Sync>> {
    if std::env::var("AETHER_EMBEDDER").as_deref() == Ok("onnx") {
        #[cfg(feature = "onnx")]
        {
            let dir = std::env::var("AETHER_ONNX_MODEL_DIR")
                .map_err(|_| "AETHER_EMBEDDER=onnx requires AETHER_ONNX_MODEL_DIR")?;
            let embedder = common::embed_onnx::OnnxEmbedder::from_dir(std::path::Path::new(&dir))?;
            println!("embedder: onnx model at {dir} (dim {})", common::embed::Embedder::dim(&embedder));
            return Ok(ShardStore::with_embedder(Arc::new(embedder)));
        }
        #[cfg(not(feature = "onnx"))]
        return Err("AETHER_EMBEDDER=onnx, but this binary was built without --features onnx".into());
    }
    Ok(ShardStore::new())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr_str = std::env::var("AETHER_SHARD_ADDR").unwrap_or_else(|_| "127.0.0.1:50051".to_string());
    let addr: SocketAddr = addr_str.parse()?;
    let shard_index: u32 = env_or("AETHER_SHARD_INDEX", 0);
    let shard_count: u32 = env_or("AETHER_SHARD_COUNT", 1);
    let node_id = std::env::var("AETHER_NODE_ID").unwrap_or_else(|_| format!("node-{shard_index}"));
    let poll_secs: u64 = env_or("AETHER_POLL_SECS", 10);
    let is_follower = std::env::var("AETHER_ROLE").map(|r| r.eq_ignore_ascii_case("follower")).unwrap_or(false);
    let role = if is_follower { NodeRole::Follower } else { NodeRole::Leader };
    let coordinator = std::env::var("AETHER_COORDINATOR_ADDR").ok();

    let shard_id_label = format!("shard-{shard_index}");
    let index = Arc::new(RwLock::new(build_store()?));

    // Register with the coordinator if configured. A failure is logged but does NOT stop the
    // node from serving: the data plane keeps running even if the control plane is down.
    if let Some(coord_addr) = &coordinator {
        match register_with_coordinator(coord_addr, &node_id, &addr_str, shard_index, role).await {
            Ok(n) => println!("registered '{node_id}' as {role:?} of shard {shard_index} (cluster N={n})"),
            Err(e) => eprintln!("warning: could not register with coordinator at {coord_addr}: {e}"),
        }
        // Keep proving we're alive so the coordinator doesn't reap us.
        let hb_secs: u64 = env_or("AETHER_HEARTBEAT_SECS", 5);
        tokio::spawn(run_heartbeat(
            coord_addr.clone(),
            node_id.clone(),
            addr_str.clone(),
            shard_index,
            role,
            Duration::from_secs(hb_secs),
        ));
    }

    // A leader ingests and replicates; a follower only receives replication.
    if role == NodeRole::Leader {
        let assignment = match NonZeroU32::new(shard_count) {
            Some(count) if shard_count > 1 => Some(ShardAssignment { index: shard_index, count }),
            _ => None,
        };

        // Replicate only when there is a coordinator to discover followers from.
        let replicate_tx = if let Some(coord_addr) = coordinator.clone() {
            let (tx, rx) = tokio::sync::mpsc::channel(8);
            tokio::spawn(run_replication(coord_addr, shard_index, rx));
            Some(tx)
        } else {
            None
        };

        let ingest_index = index.clone();
        tokio::spawn(async move {
            run_ingestion(
                OpenSkySource::from_env(),
                ingest_index,
                Duration::from_secs(poll_secs),
                None,
                assignment,
                replicate_tx,
            )
            .await;
        });
    }

    let search = ShardSearchService::new(index.clone(), shard_id_label.clone());
    let replication = ReplicationService::new(index);
    println!(
        "aether-shard-node '{shard_id_label}' ({role:?}) serving ShardSearch + Replication on {addr}; shard {shard_index}/{shard_count}"
    );

    Server::builder()
        .add_service(ShardSearchServer::new(search))
        .add_service(ReplicationServer::new(replication))
        .serve(addr)
        .await?;

    Ok(())
}
