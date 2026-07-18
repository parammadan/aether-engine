//! Aether coordinator (control plane) — Q2 registration server.
//!
//! Serves the `Coordinator.RegisterNode` RPC so shard nodes can register at runtime, and
//! holds the N-parameterized shard map. Scatter-gather query fan-out is added next.
//!
//! Config via env:
//!   AETHER_COORDINATOR_ADDR   listen address   (default 127.0.0.1:50050)
//!   AETHER_SHARD_COUNT        N (shard count)  (default 3)

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use common::pb::coordinator_server::CoordinatorServer;
use coordinator::registry::Registry;
use coordinator::service::CoordinatorService;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = std::env::var("AETHER_COORDINATOR_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:50050".to_string())
        .parse()?;
    let shard_count: u32 = std::env::var("AETHER_SHARD_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);

    let registry = Arc::new(RwLock::new(Registry::new(shard_count)));
    let service = CoordinatorService::new(registry);

    println!("aether-coordinator serving RegisterNode on {addr}; cluster N={shard_count}");

    Server::builder()
        .add_service(CoordinatorServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
