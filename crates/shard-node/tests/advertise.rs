//! Bind/advertise split: a node that binds a wildcard address must register an address
//! peers can actually dial. On one machine that's the difference between registering
//! `0.0.0.0:port` (undialable garbage) and `127.0.0.1:port`; on a real network it's the
//! difference between a cluster and a pile of unreachable processes.

use std::process::{Command, Stdio};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use common::pb::coordinator_client::CoordinatorClient;
use common::pb::coordinator_server::CoordinatorServer;
use common::pb::{ClusterStateRequest, SearchRequest};
use coordinator::registry::Registry;
use coordinator::service::CoordinatorService;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

async fn start_coordinator() -> String {
    let registry = Arc::new(RwLock::new(Registry::new(1)));
    let service = CoordinatorService::new(registry);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(CoordinatorServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    addr
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_registers_its_advertised_address_not_its_bind_address() {
    let coordinator = start_coordinator().await;
    let port = free_port();

    // Bind the wildcard (as on a real host), advertise the dialable loopback address.
    let mut child = Command::new(env!("CARGO_BIN_EXE_shard-node"))
        .env("AETHER_NODE_ID", "adv-node")
        .env("AETHER_SHARD_ADDR", format!("0.0.0.0:{port}"))
        .env("AETHER_ADVERTISE_ADDR", format!("127.0.0.1:{port}"))
        .env("AETHER_SHARD_INDEX", "0")
        .env("AETHER_SHARD_COUNT", "1")
        .env("AETHER_COORDINATOR_ADDR", &coordinator)
        .env("AETHER_INGEST", "off")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn shard-node");

    let mut client = loop {
        if let Ok(c) = CoordinatorClient::connect(format!("http://{coordinator}")).await {
            break c;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    // The coordinator must record the ADVERTISED address...
    let mut registered = None;
    for _ in 0..60 {
        let state = client.get_cluster_state(ClusterStateRequest {}).await.unwrap().into_inner();
        if let Some(node) = state.nodes.first() {
            registered = Some(node.address.clone());
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert_eq!(
        registered.as_deref(),
        Some(format!("127.0.0.1:{port}").as_str()),
        "the registered address must be the advertised one, never the bind address"
    );

    // ...and that address must actually be routable: a fan-out query reaches the node.
    let resp = client
        .search(SearchRequest { query: "anything".into(), limit: 1, filter: None })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.shards_answered, 1, "the advertised address must be dialable");

    let _ = child.kill();
    let _ = child.wait();
}
