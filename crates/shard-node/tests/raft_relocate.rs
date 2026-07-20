//! Live replica relocation across real processes: a replica moves from node A to node D
//! while the group ingests and serves continuously. Join D (learner → voter), drain A
//! (deliberate removal, executed by the group's leader), then kill A — asserting that A
//! stopped receiving the log after removal (its store plateaus while the cluster grows),
//! and that not one query failed across the whole move.

use std::collections::HashMap;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use common::pb::coordinator_client::CoordinatorClient;
use common::pb::coordinator_server::CoordinatorServer;
use common::pb::shard_search_client::ShardSearchClient;
use common::pb::{ClusterStateRequest, DrainRequest, NodeRole, SearchRequest};
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

async fn total_on(client: &mut ShardSearchClient<tonic::transport::Channel>) -> u64 {
    client
        .search(SearchRequest { query: "synthetica".into(), limit: 1, filter: None })
        .await
        .map(|r| r.into_inner().total_matched)
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replica_relocates_from_one_node_to_another_without_dropping_queries() {
    let coordinator = start_coordinator().await;
    let mut cluster = Cluster { children: HashMap::new() };
    let mut ports = HashMap::new();
    for id in ["r-a", "r-b", "r-c"] {
        let port = free_port();
        ports.insert(id.to_string(), port);
        cluster.children.insert(id.to_string(), spawn_member(id, &coordinator, port, false));
    }

    let endpoint = format!("http://{coordinator}");
    let mut client = loop {
        if let Ok(c) = CoordinatorClient::connect(endpoint.clone()).await {
            break c;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    // Continuous query load; must never error across the entire relocation.
    let errors = Arc::new(Mutex::new(Vec::<String>::new()));
    let latest_total = Arc::new(Mutex::new(0u64));
    let load = tokio::spawn({
        let errors = errors.clone();
        let latest_total = latest_total.clone();
        let mut client = client.clone();
        async move {
            loop {
                match client.search(SearchRequest { query: "synthetica".into(), limit: 3, filter: None }).await {
                    Ok(resp) => *latest_total.lock().unwrap() = resp.into_inner().total_matched,
                    Err(status) => errors.lock().unwrap().push(status.to_string()),
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    });

    // Steady ingesting state.
    for _ in 0..120 {
        if *latest_total.lock().unwrap() > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(*latest_total.lock().unwrap() > 0, "group never started ingesting");

    // --- Step 1: JOIN the incoming node D and wait until it is caught up ---
    let d_port = free_port();
    cluster.children.insert("r-new".to_string(), spawn_member("r-new", &coordinator, d_port, true));
    let mut d_client = loop {
        if let Ok(c) = ShardSearchClient::connect(format!("http://127.0.0.1:{d_port}")).await {
            break c;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let mut d_caught_up = false;
    for _ in 0..120 {
        if total_on(&mut d_client).await > 0 {
            d_caught_up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(d_caught_up, "incoming node never caught up");

    // --- Step 2: DRAIN the outgoing node A (a follower, so the leader survives to act) ---
    let state = client.get_cluster_state(ClusterStateRequest {}).await.unwrap().into_inner();
    let outgoing = state
        .nodes
        .iter()
        .find(|n| n.role == NodeRole::Follower as i32 && n.node_id.starts_with("r-") && n.node_id != "r-new")
        .expect("an original follower to drain")
        .node_id
        .clone();
    let drain = client.drain_node(DrainRequest { node_id: outgoing.clone() }).await.unwrap().into_inner();
    assert!(drain.ok, "drain refused: {}", drain.message);

    // --- Step 3: A is OUT — its store plateaus while the cluster keeps growing ---
    let mut a_client =
        ShardSearchClient::connect(format!("http://127.0.0.1:{}", ports[&outgoing])).await.unwrap();
    // Give the reconciler time to commit the removal, then measure A twice.
    let mut relocated = false;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let a_before = total_on(&mut a_client).await;
        let cluster_before = *latest_total.lock().unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;
        let a_after = total_on(&mut a_client).await;
        let cluster_after = *latest_total.lock().unwrap();
        if a_after == a_before && cluster_after > cluster_before {
            relocated = true;
            break;
        }
    }
    assert!(relocated, "drained node kept receiving the log (removal never took effect)");

    // --- Step 4: stop A's process entirely; the group must not care ---
    let mut a_child = cluster.children.remove(&outgoing).unwrap();
    a_child.kill().unwrap();
    a_child.wait().unwrap();

    let grew_after_kill = {
        let before = *latest_total.lock().unwrap();
        tokio::time::sleep(Duration::from_secs(4)).await;
        *latest_total.lock().unwrap() > before
    };
    assert!(grew_after_kill, "cluster stopped ingesting after the drained node was stopped");

    load.abort();
    let errs = errors.lock().unwrap();
    assert!(errs.is_empty(), "query stream errored during relocation: {errs:?}");
}
