//! Aether coordinator (control plane) server.
//!
//! Serves the `Coordinator` RPCs (RegisterNode, Search, ListReplicas, Heartbeat): shard nodes
//! register at runtime into an N-parameterized shard map, queries are fanned out across
//! leaders, and a background reaper drops nodes that stop heartbeating.
//!
//! Config via env:
//!   AETHER_COORDINATOR_ADDR       listen address        (default 127.0.0.1:50050)
//!   AETHER_SHARD_COUNT            N (shard count)        (default 3)
//!   AETHER_LIVENESS_TIMEOUT_SECS  drop a node unseen for this long (default 15)
//!   AETHER_CONTROL_ID             this replica's id in the coordinator state group
//!   AETHER_CONTROL_PEERS          "1=host:port,2=host:port,..." (all replicas' gRPC
//!                                 addresses; with CONTROL_ID, operator intent —
//!                                 placement + drains — replicates across the group)
//!   AETHER_DATA_DIR               durable raft log + snapshots for the state group

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use common::pb::coordinator_server::CoordinatorServer;
use coordinator::registry::Registry;
use coordinator::service::CoordinatorService;
use tonic::transport::Server;

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    common::net::install_crypto();
    let addr: SocketAddr = std::env::var("AETHER_COORDINATOR_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:50050".to_string())
        .parse()?;
    let shard_count: u32 = env_or("AETHER_SHARD_COUNT", 3);
    let liveness_timeout = Duration::from_secs(env_or("AETHER_LIVENESS_TIMEOUT_SECS", 15));

    // The registration guard and the reaper share one definition of "alive".
    // AETHER_VSHARDS > 0 enables virtual-shard placement (V fixed forever; load moves by
    // reassigning whole virtual shards between groups, never by changing the modulus).
    let vshards: u32 = env_or("AETHER_VSHARDS", 0);
    let control_mode = std::env::var("AETHER_CONTROL_ID").is_ok();
    let mut registry_inner = Registry::new(shard_count).with_liveness_timeout(liveness_timeout);
    if vshards > 0 {
        registry_inner = registry_inner.with_vshards(vshards);
        println!("virtual shards: {vshards} across {shard_count} groups");
    }
    if control_mode {
        // The authoritative state now belongs to the group; local view-driven cleanup off.
        registry_inner = registry_inner.with_replicated_authority();
    }
    let registry = Arc::new(RwLock::new(registry_inner));

    // Reaper: periodically drop nodes we haven't heard from within the liveness timeout, so a
    // dead node stops being routed to. Checks at roughly a third of the timeout.
    let reaper_registry = registry.clone();
    let reap_every = (liveness_timeout / 3).max(Duration::from_secs(1));
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(reap_every);
        loop {
            ticker.tick().await;
            // Reap dead nodes and fail over orphaned shards atomically under one write lock.
            let (removed, promotions) = {
                let mut reg = reaper_registry.write().expect("registry lock poisoned");
                let now = Instant::now();
                let removed = reg.reap_dead(now, liveness_timeout);
                let promotions = reg.promote_orphaned_shards();
                (removed, promotions)
            };
            for node in removed {
                println!("coordinator: dropped dead node '{}' ({:?})", node.node_id, node.role);
            }
            for (shard, node_id) in promotions {
                println!("coordinator: promoted '{node_id}' to leader of shard {shard} (failover)");
            }
        }
    });

    // Coordinator state group: with AETHER_CONTROL_ID/PEERS set, this replica joins a
    // raft group replicating operator intent (vshard table + drain set), and its gRPC
    // server also carries the group's transport.
    let control = coordinator::control::ControlPlane::from_env(registry.clone()).await?;
    let auth = Arc::new(coordinator::auth::Auth::from_env()?);

    // Operational metrics: Prometheus exposition on a SEPARATE port, so scraping never
    // touches the gRPC data plane. Disable by setting AETHER_METRICS_ADDR=off.
    let metrics = Arc::new(coordinator::metrics::Metrics::default());
    let metrics_addr = std::env::var("AETHER_METRICS_ADDR").unwrap_or_else(|_| "127.0.0.1:9090".to_string());
    if metrics_addr != "off" {
        match metrics_addr.parse::<std::net::SocketAddr>() {
            Ok(maddr) => {
                tokio::spawn(coordinator::metrics::serve(maddr, metrics.clone(), registry.clone()));
            }
            Err(_) => eprintln!("metrics: invalid AETHER_METRICS_ADDR '{metrics_addr}', metrics disabled"),
        }
    }

    println!(
        "aether-coordinator serving on {addr}; cluster N={shard_count}; liveness timeout {}s",
        liveness_timeout.as_secs()
    );

    let mut builder = Server::builder();
    if let Some(tls) = common::net::server_tls() {
        builder = builder.tls_config(tls)?;
        println!("tls: mTLS required on {addr}");
    }
    match control {
        Some(control) => {
            let control = Arc::new(control);
            let transport = consensus::service::RaftTransportService::new(control.raft.clone());
            let service =
                CoordinatorService::with_control(registry, control).with_auth(auth).with_metrics(metrics);
            builder
                .add_service(CoordinatorServer::new(service))
                .add_service(common::pb::raft_transport_server::RaftTransportServer::new(transport))
                .serve(addr)
                .await?;
        }
        None => {
            let service = CoordinatorService::new(registry).with_auth(auth).with_metrics(metrics);
            builder
                .add_service(CoordinatorServer::new(service))
                .serve(addr)
                .await?;
        }
    }

    Ok(())
}
