//! Aether shard node (data plane).
//!
//! Two operating modes:
//!
//! **Legacy (default):** registers with the coordinator, and if it's the leader it ingests
//! OpenSky data (keeping its `hash(icao24) % N` slice) and pushes each indexed batch to its
//! follower(s) best-effort. Failover is coordinator-driven (reaper + promotion).
//!
//! **Consensus-managed (`AETHER_CONSENSUS=raft`):** the members of this shard form a raft
//! group. The node registers, waits for the full group, and the member with the smallest
//! raft id initializes it. The ELECTED leader ingests, writing every batch through the raft
//! log so it quorum-commits into all members' stores; heartbeats report raft leadership so
//! the coordinator routes queries to the elected leader. Elections replace promotion;
//! best-effort replication is off (the log is the replication).
//!
//! Config via env:
//!   AETHER_SHARD_ADDR         gRPC listen address       (default 127.0.0.1:50051)
//!   AETHER_SHARD_INDEX        this shard's index 0..N   (default 0)
//!   AETHER_SHARD_COUNT        N (total shards)          (default 1 = single-node)
//!   AETHER_ROLE               leader | follower         (default leader; raft mode ignores)
//!   AETHER_CONSENSUS          off | raft                (default off)
//!   AETHER_GROUP_SIZE         raft group size           (default 3; groups of 2 can't
//!                                                        survive a failure — quorum of 2 is 2)
//!   AETHER_COORDINATOR_ADDR   coordinator to register with (optional; skipped if unset)
//!   AETHER_COORDINATOR_ADDRS  comma-separated coordinator list (overrides the singular;
//!                                                        register+heartbeat to ALL,
//!                                                        reads use first healthy)
//!   AETHER_NODE_ID            stable node id            (default "node-<index>")
//!   AETHER_POLL_SECS          OpenSky poll interval     (default 10)
//!   AETHER_INGEST             on | off                  (default on)
//!   AETHER_EMBEDDER           hash | onnx               (default hash; onnx needs the
//!                                                        onnx build feature + model dir)
//!   OPENSKY_USERNAME / OPENSKY_PASSWORD   optional, raise the OpenSky rate limit

use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use common::pb::raft_transport_server::RaftTransportServer;
use common::pb::replication_server::ReplicationServer;
use common::pb::shard_search_server::ShardSearchServer;
use common::pb::NodeRole;
use shard_node::cluster::run_heartbeat;
use shard_node::ingest::{
    run_ingestion, run_vshard_view, FlightSource, OpenSkySource, Ownership, ShardAssignment,
    SyntheticSource,
};
use shard_node::raft::bootstrap::{bootstrap_group, raft_node_id, run_leader_ingestion, wait_for_group};
use shard_node::raft::network::GrpcRaftNetworkFactory;
use shard_node::raft::service::RaftTransportService;
use shard_node::raft::storage::{LogStore, StateMachineStore};
use shard_node::raft::wal::WalLogStore;
use shard_node::raft::{raft_config, Raft};
use shard_node::replication::{run_replication, ReplicationService};
use shard_node::server::ShardSearchService;
use shard_node::store::ShardStore;
use tonic::transport::Server;

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

/// The flight source: live OpenSky by default, or a deterministic synthetic feed
/// (`AETHER_SOURCE=synthetic`) for offline demos and tests. The synthetic seed is derived
/// from the node id so two producers can never fabricate colliding aircraft.
fn build_source(node_id: &str) -> Box<dyn FlightSource> {
    if std::env::var("AETHER_SOURCE").as_deref() == Ok("synthetic") {
        let seed = common::shard::fnv1a_64(node_id.as_bytes()) as u32;
        Box::new(SyntheticSource::new(seed, 5))
    } else {
        Box::new(OpenSkySource::from_env())
    }
}

/// Build the shard's document store with the configured embedder.
///
/// `AETHER_EMBEDDER=hash` (default) uses the deterministic feature-hashing embedder;
/// `AETHER_EMBEDDER=onnx` loads a learned sentence-transformer from
/// `AETHER_ONNX_MODEL_DIR` (requires building with `--features onnx`). Every node in a
/// cluster MUST use the same embedder and model — embeddings are a cross-node contract, and
/// the shard rejects query vectors whose dimension doesn't match its own.
fn build_store() -> Result<ShardStore, Box<dyn std::error::Error + Send + Sync>> {
    // AETHER_VECTOR=quantized runs vector search on the two-tier quantized pipeline
    // (binary candidate scan, exact rescore).
    let quantized = std::env::var("AETHER_VECTOR").as_deref() == Ok("quantized");
    if quantized {
        println!("vector search: quantized (binary scan + exact rescore)");
    }
    if std::env::var("AETHER_EMBEDDER").as_deref() == Ok("onnx") {
        #[cfg(feature = "onnx")]
        {
            let dir = std::env::var("AETHER_ONNX_MODEL_DIR")
                .map_err(|_| "AETHER_EMBEDDER=onnx requires AETHER_ONNX_MODEL_DIR")?;
            let embedder = common::embed_onnx::OnnxEmbedder::from_dir(std::path::Path::new(&dir))?;
            println!("embedder: onnx model at {dir} (dim {})", common::embed::Embedder::dim(&embedder));
            return Ok(ShardStore::with_embedder(Arc::new(embedder)).with_quantized(quantized));
        }
        #[cfg(not(feature = "onnx"))]
        return Err("AETHER_EMBEDDER=onnx, but this binary was built without --features onnx".into());
    }
    Ok(ShardStore::new().with_quantized(quantized))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    common::net::install_crypto();
    let addr_str = std::env::var("AETHER_SHARD_ADDR").unwrap_or_else(|_| "127.0.0.1:50051".to_string());
    let addr: SocketAddr = addr_str.parse()?;
    // Bind vs advertise: on a real network a node binds 0.0.0.0 but must register an
    // address PEERS can dial (its private IP). Everything downstream — routing, raft
    // membership, replication — carries the advertised address, so getting this wrong
    // breaks the cluster the moment it leaves one machine. Defaults to the bind address
    // for single-host runs.
    let advertise = std::env::var("AETHER_ADVERTISE_ADDR").unwrap_or_else(|_| addr_str.clone());
    let shard_index: u32 = env_or("AETHER_SHARD_INDEX", 0);
    let shard_count: u32 = env_or("AETHER_SHARD_COUNT", 1);
    let node_id = std::env::var("AETHER_NODE_ID").unwrap_or_else(|_| format!("node-{shard_index}"));
    let poll_secs: u64 = env_or("AETHER_POLL_SECS", 10);
    let is_follower = std::env::var("AETHER_ROLE").map(|r| r.eq_ignore_ascii_case("follower")).unwrap_or(false);
    let raft_mode = std::env::var("AETHER_CONSENSUS").map(|c| c.eq_ignore_ascii_case("raft")).unwrap_or(false);
    let group_size: usize = env_or("AETHER_GROUP_SIZE", 3);
    let ingest_on = std::env::var("AETHER_INGEST").map(|v| !v.eq_ignore_ascii_case("off")).unwrap_or(true);
    let coordinators = shard_node::cluster::Coordinators::from_env();

    // Under raft, roles are outcomes of elections; every member registers as a follower.
    let role = if raft_mode || is_follower { NodeRole::Follower } else { NodeRole::Leader };

    let shard_id_label = format!("shard-{shard_index}");
    let index = Arc::new(RwLock::new(build_store()?));

    // Consensus: this member's raft instance (state machine = the same store search reads).
    // With AETHER_DATA_DIR set, the log/vote live in a local WAL and snapshots persist to
    // disk, so a restarted member recovers its own state instead of forgetting it — an
    // in-memory vote isn't just data loss, it's a double-vote safety hole.
    let raft = if raft_mode {
        let my_raft_id = raft_node_id(&node_id);
        let config = Arc::new(raft_config().validate().map_err(|e| e.to_string())?);
        let raft = match std::env::var("AETHER_DATA_DIR").ok() {
            Some(dir) => {
                let dir = std::path::PathBuf::from(dir);
                let log_store = WalLogStore::open(&dir).map_err(|e| e.to_string())?;
                // Optional S3 snapshot tier (AETHER_S3_BUCKET): uploads after each build,
                // cold-boots from the newest object when the local tier is empty.
                let s3 = shard_node::raft::s3::SnapshotS3::from_env().await.map(Arc::new);
                let sm = StateMachineStore::open_durable(index.clone(), dir.join("snapshots"), s3)
                    .await
                    .map_err(|e| e.to_string())?;
                println!("raft storage: durable at {}", dir.display());
                Raft::new(my_raft_id, config, GrpcRaftNetworkFactory, log_store, sm)
                    .await
                    .map_err(|e| e.to_string())?
            }
            None => Raft::new(
                my_raft_id,
                config,
                GrpcRaftNetworkFactory,
                LogStore::default(),
                StateMachineStore::new(index.clone()),
            )
            .await
            .map_err(|e| e.to_string())?,
        };
        Some((raft, my_raft_id))
    } else {
        None
    };

    // Register with the coordinators if configured. A failure is logged but does NOT stop
    // the node from serving: the data plane keeps running even if the control plane is
    // down. Registration goes to EVERY coordinator — each replica's view is its own.
    if let Some(coords) = &coordinators {
        match coords.register_all(&node_id, &advertise, shard_index, role).await {
            Ok(n) => println!("registered '{node_id}' as {role:?} of shard {shard_index} (cluster N={n})"),
            Err(e) => eprintln!("warning: {e}"),
        }
        // Keep proving we're alive; raft members also report whether they lead their group.
        let hb_secs: u64 = env_or("AETHER_HEARTBEAT_SECS", 5);
        tokio::spawn(run_heartbeat(
            coords.clone(),
            node_id.clone(),
            advertise.clone(),
            shard_index,
            role,
            Duration::from_secs(hb_secs),
            raft.clone(),
        ));
    }

    // What this node keeps from the stream. If the coordinator runs virtual-shard
    // placement (its table is non-empty), ownership follows the live table — reassigning a
    // virtual shard moves ingestion between groups with no restarts. Otherwise fall back to
    // the fixed hash % N slice (or everything, single-node). Auto-detected from the
    // coordinator so nodes need no placement config of their own.
    let ownership = {
        let mut mapped = None;
        if let Some(coords) = &coordinators {
            for _ in 0..10 {
                if let Some(mut c) = coords.first_healthy().await {
                    if let Ok(resp) = c.get_v_shard_assignments(common::pb::VShardAssignmentsRequest {}).await {
                        let table = resp.into_inner().group_of;
                        if !table.is_empty() {
                            println!("placement: virtual shards (V={}, group {shard_index})", table.len());
                            let assignments = Arc::new(RwLock::new(table));
                            tokio::spawn(run_vshard_view(coords.clone(), assignments.clone()));
                            mapped = Some(Ownership::Mapped { group: shard_index, assignments });
                        }
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
        }
        match mapped {
            Some(ownership) => ownership,
            None => match NonZeroU32::new(shard_count) {
                Some(count) if shard_count > 1 => {
                    Ownership::Modulo(ShardAssignment { index: shard_index, count })
                }
                _ => Ownership::All,
            },
        }
    };

    // Grab the live vshard table (if any) for the search path's provenance BEFORE the
    // ingestion branches move `ownership` into their spawned tasks.
    let search_assignments = ownership.assignments();

    match &raft {
        // Consensus-managed: form the group, then the ELECTED leader ingests through the log.
        Some((raft, my_raft_id)) => {
            let coords = coordinators.clone().ok_or("AETHER_CONSENSUS=raft requires AETHER_COORDINATOR_ADDR(S)")?;
            let my_id = *my_raft_id;

            // A JOINING member (AETHER_RAFT_JOIN=1) never bootstraps: it stays uninitialized
            // (an uninitialized raft doesn't campaign, so it can't disturb the live group)
            // and waits for the group's leader to admit it as a learner and promote it.
            let joining = std::env::var("AETHER_RAFT_JOIN").map(|v| v == "1").unwrap_or(false);
            if !joining {
                let boot_raft = raft.clone();
                let boot_coords = coords.clone();
                tokio::spawn(async move {
                    let members = wait_for_group(&boot_coords, shard_index, group_size).await;
                    bootstrap_group(&boot_raft, my_id, members).await;
                });
            }

            // Every member runs the reconciler; it acts only while leader, admitting newly
            // registered members into the group live (learner -> voter).
            tokio::spawn(shard_node::raft::reconcile::run_membership_reconciler(
                raft.clone(),
                my_id,
                coords,
                shard_index,
            ));

            if ingest_on {
                tokio::spawn(run_leader_ingestion(
                    build_source(&node_id),
                    raft.clone(),
                    my_id,
                    Duration::from_secs(poll_secs),
                    ownership.clone(),
                ));
            }
        }
        // Legacy: a configured leader ingests directly and pushes best-effort replication.
        None if role == NodeRole::Leader && ingest_on => {
            // Legacy replication predates multi-coordinator; it discovers followers via
            // the FIRST coordinator only (this mode exists as the consensus contrast now).
            let replicate_tx = if let Some(coords) = &coordinators {
                let (tx, rx) = tokio::sync::mpsc::channel(8);
                tokio::spawn(run_replication(coords.addrs()[0].clone(), shard_index, rx));
                Some(tx)
            } else {
                None
            };
            let ingest_index = index.clone();
            let source = build_source(&node_id);
            tokio::spawn(async move {
                run_ingestion(
                    source,
                    ingest_index,
                    Duration::from_secs(poll_secs),
                    None,
                    ownership.clone(),
                    replicate_tx,
                )
                .await;
            });
        }
        None => {}
    }

    let mut search = ShardSearchService::new(index.clone(), shard_id_label.clone());
    if let Some(assignments) = search_assignments {
        search = search.with_assignments(assignments);
    }
    let replication = ReplicationService::new(index);
    let mode = if raft_mode { "raft" } else { "legacy" };
    println!(
        "aether-shard-node '{shard_id_label}' ({role:?}, {mode}) serving on {addr}; shard {shard_index}/{shard_count}"
    );

    let mut builder = Server::builder();
    if let Some(tls) = common::net::server_tls() {
        builder = builder.tls_config(tls)?;
        println!("tls: mTLS required on {addr}");
    }
    let mut router = builder
        .add_service(ShardSearchServer::new(search))
        .add_service(ReplicationServer::new(replication));
    if let Some((raft, _)) = raft {
        router = router.add_service(RaftTransportServer::new(RaftTransportService::new(raft)));
    }
    router.serve(addr).await?;

    Ok(())
}
