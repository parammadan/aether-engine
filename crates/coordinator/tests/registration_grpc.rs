//! End-to-end test of the coordinator's RegisterNode RPC over gRPC.

use std::sync::{Arc, RwLock};

use common::pb::coordinator_client::CoordinatorClient;
use common::pb::coordinator_server::CoordinatorServer;
use common::pb::{NodeRole, RegisterNodeRequest};
use coordinator::registry::Registry;
use coordinator::service::CoordinatorService;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// Boot a coordinator with the given N on an ephemeral port; return (client, registry).
async fn start(n: u32) -> (CoordinatorClient<tonic::transport::Channel>, Arc<RwLock<Registry>>) {
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

    let endpoint = format!("http://{addr}");
    let client = loop {
        if let Ok(c) = CoordinatorClient::connect(endpoint.clone()).await {
            break c;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    };
    (client, registry)
}

fn req(node_id: &str, shard_id: u32, role: NodeRole) -> RegisterNodeRequest {
    RegisterNodeRequest {
        node_id: node_id.to_string(),
        address: format!("127.0.0.1:600{shard_id}"),
        shard_id,
        role: role as i32,
    }
}

#[tokio::test]
async fn register_leaders_builds_the_shard_map() {
    let (mut client, registry) = start(2).await;

    let r0 = client.register_node(req("n0", 0, NodeRole::Leader)).await.unwrap().into_inner();
    assert!(r0.accepted);
    assert_eq!(r0.cluster_size, 2);

    let r1 = client.register_node(req("n1", 1, NodeRole::Leader)).await.unwrap().into_inner();
    assert!(r1.accepted);

    assert!(registry.read().unwrap().all_shards_have_leader());
}

#[tokio::test]
async fn register_rejects_out_of_range_shard() {
    let (mut client, _registry) = start(2).await;

    let resp = client.register_node(req("bad", 9, NodeRole::Leader)).await.unwrap().into_inner();
    assert!(!resp.accepted);
    assert_eq!(resp.cluster_size, 2);
}

#[tokio::test]
async fn list_replicas_returns_a_shards_followers() {
    use common::pb::ListReplicasRequest;

    let (mut client, _registry) = start(2).await;

    // Shard 0 gets a leader and a follower; the leader should be able to discover the follower.
    client.register_node(req("leader0", 0, NodeRole::Leader)).await.unwrap();
    let mut follower = req("follower0", 0, NodeRole::Follower);
    follower.address = "127.0.0.1:7000".to_string();
    client.register_node(follower).await.unwrap();

    let resp = client
        .list_replicas(ListReplicasRequest { shard_id: 0 })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.addresses, vec!["127.0.0.1:7000".to_string()]);

    // Shard 1 has no followers registered.
    let empty = client
        .list_replicas(ListReplicasRequest { shard_id: 1 })
        .await
        .unwrap()
        .into_inner();
    assert!(empty.addresses.is_empty());
}
