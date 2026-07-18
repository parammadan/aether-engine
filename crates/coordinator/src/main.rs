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
    let addr: SocketAddr = std::env::var("AETHER_COORDINATOR_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:50050".to_string())
        .parse()?;
    let shard_count: u32 = env_or("AETHER_SHARD_COUNT", 3);
    let liveness_timeout = Duration::from_secs(env_or("AETHER_LIVENESS_TIMEOUT_SECS", 15));

    let registry = Arc::new(RwLock::new(Registry::new(shard_count)));

    // Reaper: periodically drop nodes we haven't heard from within the liveness timeout, so a
    // dead node stops being routed to. Checks at roughly a third of the timeout.
    let reaper_registry = registry.clone();
    let reap_every = (liveness_timeout / 3).max(Duration::from_secs(1));
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(reap_every);
        loop {
            ticker.tick().await;
            let removed = reaper_registry
                .write()
                .expect("registry lock poisoned")
                .reap_dead(Instant::now(), liveness_timeout);
            for node in removed {
                println!("coordinator: dropped dead node '{}' ({:?})", node.node_id, node.role);
            }
        }
    });

    let service = CoordinatorService::new(registry);
    println!(
        "aether-coordinator serving on {addr}; cluster N={shard_count}; liveness timeout {}s",
        liveness_timeout.as_secs()
    );

    Server::builder()
        .add_service(CoordinatorServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
