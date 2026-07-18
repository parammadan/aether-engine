//! Tests the shard node's registration client against a real coordinator server.

use std::sync::{Arc, RwLock};

use common::pb::coordinator_server::CoordinatorServer;
use common::pb::NodeRole;
use coordinator::registry::Registry;
use coordinator::service::CoordinatorService;
use shard_node::cluster::register_with_coordinator;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// Boot a coordinator with N shards on an ephemeral port; return (addr, registry).
async fn start_coordinator(n: u32) -> (String, Arc<RwLock<Registry>>) {
    let registry = Arc::new(RwLock::new(Registry::new(n)));
    let service = CoordinatorService::new(registry.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(CoordinatorServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (addr.to_string(), registry)
}

#[tokio::test]
async fn node_registers_itself_with_coordinator() {
    let (coord_addr, registry) = start_coordinator(3).await;

    let n = register_with_coordinator(&coord_addr, "node-1", "127.0.0.1:50051", 1, NodeRole::Leader)
        .await
        .expect("registration should succeed");

    assert_eq!(n, 3); // coordinator reported N back
    assert_eq!(registry.read().unwrap().leaders_registered(), 1);
    assert_eq!(registry.read().unwrap().leader_addresses(), vec!["127.0.0.1:50051".to_string()]);
}

#[tokio::test]
async fn registration_is_rejected_for_bad_shard() {
    let (coord_addr, _registry) = start_coordinator(2).await;

    let result =
        register_with_coordinator(&coord_addr, "node-x", "127.0.0.1:50060", 7, NodeRole::Leader).await;

    assert!(result.is_err(), "shard 7 with N=2 must be rejected");
}
