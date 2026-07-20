//! Derived-view convergence under coordinator death: the shard map is a VIEW that
//! heartbeats rebuild, so killing a coordinator must not interrupt routing through the
//! survivor — and a restarted coordinator must reconverge its map from heartbeats alone,
//! with no state carried over.

use std::process::{Command, Stdio};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use common::pb::coordinator_client::CoordinatorClient;
use common::pb::coordinator_server::CoordinatorServer;
use common::pb::SearchRequest;
use coordinator::registry::Registry;
use coordinator::service::CoordinatorService;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// Serve a coordinator on a SPECIFIC port (so a "restart" can reclaim the same address).
async fn serve_coordinator(port: u16) -> tokio::task::JoinHandle<()> {
    let registry = Arc::new(RwLock::new(Registry::new(1)));
    let service = CoordinatorService::new(registry);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await.unwrap();
    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(CoordinatorServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    })
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

async fn matched_via(addr: &str) -> Option<(u32, u64)> {
    let mut c = CoordinatorClient::connect(format!("http://{addr}")).await.ok()?;
    let resp = c
        .search(SearchRequest { query: "synthetica".into(), limit: 1, filter: None })
        .await
        .ok()?
        .into_inner();
    Some((resp.shards_answered, resp.total_matched))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killing_one_coordinator_never_breaks_routing_and_a_restart_reconverges() {
    let (port_a, port_b) = (free_port(), free_port());
    let (addr_a, addr_b) = (format!("127.0.0.1:{port_a}"), format!("127.0.0.1:{port_b}"));
    let handle_a = serve_coordinator(port_a).await;
    let _handle_b = serve_coordinator(port_b).await;

    let mut child = Command::new(env!("CARGO_BIN_EXE_shard-node"))
        .env("AETHER_NODE_ID", "cf-node")
        .env("AETHER_SHARD_ADDR", format!("127.0.0.1:{}", free_port()))
        .env("AETHER_SHARD_INDEX", "0")
        .env("AETHER_SHARD_COUNT", "1")
        .env("AETHER_COORDINATOR_ADDRS", format!("{addr_a},{addr_b}"))
        .env("AETHER_HEARTBEAT_SECS", "1")
        .env("AETHER_SOURCE", "synthetic")
        .env("AETHER_POLL_SECS", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn shard-node");

    // Both coordinators must route queries to the ingesting node.
    for addr in [&addr_a, &addr_b] {
        let mut routed = false;
        for _ in 0..60 {
            if let Some((answered, matched)) = matched_via(addr).await {
                if answered == 1 && matched > 0 {
                    routed = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        assert!(routed, "coordinator {addr} never routed to the node");
    }

    // KILL coordinator A, then hammer B with queries for several heartbeat intervals:
    // every single one must succeed — the view is per-coordinator, so A's death is
    // invisible through B.
    handle_a.abort();
    let (_, matched_at_kill) = matched_via(&addr_b).await.expect("survivor must answer");
    let mut last_matched = matched_at_kill;
    for _ in 0..15 {
        let (answered, matched) = matched_via(&addr_b)
            .await
            .expect("zero-error-under-failure: a query through the survivor failed");
        assert_eq!(answered, 1, "survivor lost the node from its view");
        last_matched = matched;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(last_matched >= matched_at_kill, "ingest went backwards");

    // RESTART A on the same address with a FRESH, empty registry. Heartbeats alone must
    // rebuild its map: the node notices `known=false` and re-registers within a beat.
    let _handle_a2 = serve_coordinator(port_a).await;
    let mut reconverged = false;
    for _ in 0..40 {
        if let Some((answered, matched)) = matched_via(&addr_a).await {
            if answered == 1 && matched > 0 {
                reconverged = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(reconverged, "restarted coordinator never reconverged its view from heartbeats");

    let _ = child.kill();
    let _ = child.wait();
}
