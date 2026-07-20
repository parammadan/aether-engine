//! Live membership change across real processes: a fourth node joins a raft group that is
//! actively ingesting and serving queries. The group's leader admits it (learner → voter)
//! while writes continue; the joiner catches up from replication and then tracks the live
//! log — all with zero interruption to the query stream.

use std::collections::HashMap;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use common::pb::coordinator_client::CoordinatorClient;
use common::pb::coordinator_server::CoordinatorServer;
use common::pb::shard_search_client::ShardSearchClient;
use common::pb::{ClusterStateRequest, NodeRole, SearchRequest};
use coordinator::registry::Registry;
use coordinator::service::CoordinatorService;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

struct Cluster {
    children: HashMap<String, Child>,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for (_, child) in self.children.iter_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

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

fn spawn_member(node_id: &str, coordinator: &str, port: u16, joining: bool) -> Child {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_shard-node"));
    cmd.env("AETHER_NODE_ID", node_id)
        .env("AETHER_SHARD_ADDR", format!("127.0.0.1:{port}"))
        .env("AETHER_SHARD_INDEX", "0")
        .env("AETHER_SHARD_COUNT", "1")
        .env("AETHER_CONSENSUS", "raft")
        .env("AETHER_GROUP_SIZE", "3")
        .env("AETHER_COORDINATOR_ADDR", coordinator)
        .env("AETHER_HEARTBEAT_SECS", "1")
        .env("AETHER_SOURCE", "synthetic")
        .env("AETHER_POLL_SECS", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if joining {
        cmd.env("AETHER_RAFT_JOIN", "1");
    }
    cmd.spawn().expect("spawn shard-node")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_member_joins_a_live_group_and_catches_up_without_dropping_queries() {
    let coordinator = start_coordinator().await;
    let mut cluster = Cluster { children: HashMap::new() };
    for id in ["j-a", "j-b", "j-c"] {
        cluster.children.insert(id.to_string(), spawn_member(id, &coordinator, free_port(), false));
    }

    let endpoint = format!("http://{coordinator}");
    let mut client = loop {
        if let Ok(c) = CoordinatorClient::connect(endpoint.clone()).await {
            break c;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    // Continuous query load through the coordinator; must never error.
    let errors = Arc::new(Mutex::new(Vec::<String>::new()));
    let load = tokio::spawn({
        let errors = errors.clone();
        let mut client = client.clone();
        async move {
            loop {
                if let Err(status) =
                    client.search(SearchRequest { query: "synthetica".into(), limit: 3, filter: None }).await
                {
                    errors.lock().unwrap().push(status.to_string());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    });

    // Steady state: an elected, routed leader that is producing searchable data.
    let mut steady = false;
    for _ in 0..120 {
        let state = client.get_cluster_state(ClusterStateRequest {}).await.unwrap().into_inner();
        let has_leader = state.nodes.iter().any(|n| n.role == NodeRole::Leader as i32);
        let has_data = client
            .search(SearchRequest { query: "synthetica".into(), limit: 1, filter: None })
            .await
            .map(|r| r.into_inner().total_matched > 0)
            .unwrap_or(false);
        if has_leader && has_data {
            steady = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(steady, "group never reached steady ingesting state");

    // --- A fourth member JOINS the live group ---
    let joiner_port = free_port();
    cluster.children.insert("j-new".to_string(), spawn_member("j-new", &coordinator, joiner_port, true));

    // The joiner's own store must fill up (catch-up via replication) and then keep growing
    // (it is tracking the live log as a group member) — all observed by querying IT directly.
    let mut joiner = loop {
        if let Ok(c) = ShardSearchClient::connect(format!("http://127.0.0.1:{joiner_port}")).await {
            break c;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    let mut first_seen = 0u64;
    for _ in 0..120 {
        let total = joiner
            .search(SearchRequest { query: "synthetica".into(), limit: 1, filter: None })
            .await
            .map(|r| r.into_inner().total_matched)
            .unwrap_or(0);
        if total > 0 {
            first_seen = total;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(first_seen > 0, "joiner never caught up (no replicated docs in its store)");

    let mut grew = false;
    for _ in 0..120 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let total = joiner
            .search(SearchRequest { query: "synthetica".into(), limit: 1, filter: None })
            .await
            .map(|r| r.into_inner().total_matched)
            .unwrap_or(0);
        if total > first_seen {
            grew = true;
            break;
        }
    }
    assert!(grew, "joiner caught up but is not tracking the live log (count stuck at {first_seen})");

    load.abort();
    let errs = errors.lock().unwrap();
    assert!(errs.is_empty(), "query stream errored during the join: {errs:?}");
}
