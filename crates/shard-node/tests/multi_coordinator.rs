//! Multi-coordinator control plane, node side: registration and heartbeats fan out to
//! EVERY coordinator (each replica's liveness view is independent), and losing one
//! coordinator must not degrade the node's standing with the survivors.

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

async fn start_coordinator() -> (String, tokio::task::JoinHandle<()>) {
    let registry = Arc::new(RwLock::new(Registry::new(1)));
    let service = CoordinatorService::new(registry);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let handle = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(CoordinatorServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    (addr, handle)
}

/// Age of the node's last heartbeat as this coordinator saw it, or None if unregistered.
async fn seen_age_ms(coordinator: &str) -> Option<u64> {
    let mut client = CoordinatorClient::connect(format!("http://{coordinator}")).await.ok()?;
    let state = client.get_cluster_state(ClusterStateRequest {}).await.ok()?.into_inner();
    state.nodes.first().map(|n| n.millis_since_seen)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_registers_and_heartbeats_every_coordinator_and_survives_losing_one() {
    let (coord_a, handle_a) = start_coordinator().await;
    let (coord_b, _handle_b) = start_coordinator().await;

    let mut child = Command::new(env!("CARGO_BIN_EXE_shard-node"))
        .env("AETHER_NODE_ID", "mc-node")
        .env("AETHER_SHARD_ADDR", "127.0.0.1:0".replace(":0", &format!(":{}", free_port())))
        .env("AETHER_SHARD_INDEX", "0")
        .env("AETHER_SHARD_COUNT", "1")
        .env("AETHER_COORDINATOR_ADDRS", format!("{coord_a},{coord_b}"))
        .env("AETHER_HEARTBEAT_SECS", "1")
        .env("AETHER_INGEST", "off")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn shard-node");

    // BOTH coordinators must learn of the node — registration is a fan-out, not a pick.
    for coord in [&coord_a, &coord_b] {
        let mut registered = false;
        for _ in 0..60 {
            if seen_age_ms(coord).await.is_some() {
                registered = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        assert!(registered, "node never registered with coordinator {coord}");
    }

    // And BOTH must keep hearing from it: after several beat intervals, each copy of the
    // liveness view is independently fresh.
    tokio::time::sleep(Duration::from_secs(3)).await;
    for coord in [&coord_a, &coord_b] {
        let age = seen_age_ms(coord).await.expect("node vanished from a coordinator");
        assert!(age < 2_500, "coordinator {coord} has a stale view ({age}ms since seen)");
    }

    // Kill coordinator A — the FIRST in the node's list, so any first-entry favoritism
    // would show up here. The node must keep the survivor fresh and routable.
    handle_a.abort();
    tokio::time::sleep(Duration::from_secs(3)).await;

    let age = seen_age_ms(&coord_b).await.expect("node lost from the surviving coordinator");
    assert!(age < 2_500, "surviving coordinator went stale ({age}ms) after its peer died");

    let mut client = CoordinatorClient::connect(format!("http://{coord_b}")).await.unwrap();
    let resp = client
        .search(SearchRequest { query: "anything".into(), limit: 1 })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.shards_answered, 1, "survivor must still route to the node");

    let _ = child.kill();
    let _ = child.wait();
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}
