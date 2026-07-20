//! Consensus chaos under load, across real processes: a 3-member raft shard ingests a
//! synthetic feed while queries stream through the coordinator; the elected leader is
//! SIGKILLed mid-flight. Asserts the three properties that matter:
//!   1. the query stream NEVER errors (degrades to partial coverage at worst);
//!   2. quorum-committed data survives the leader's death (counts recover);
//!   3. the NEW leader resumes ingestion on its own (counts GROW past the kill point) —
//!      leadership is observed, not assigned, so ingestion follows the election.

use std::collections::HashMap;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use common::pb::coordinator_client::CoordinatorClient;
use common::pb::coordinator_server::CoordinatorServer;
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

fn spawn_member(node_id: &str, coordinator: &str) -> Child {
    Command::new(env!("CARGO_BIN_EXE_shard-node"))
        .env("AETHER_NODE_ID", node_id)
        .env("AETHER_SHARD_ADDR", format!("127.0.0.1:{}", free_port()))
        .env("AETHER_SHARD_INDEX", "0")
        .env("AETHER_SHARD_COUNT", "1")
        .env("AETHER_CONSENSUS", "raft")
        .env("AETHER_GROUP_SIZE", "3")
        .env("AETHER_COORDINATOR_ADDR", coordinator)
        .env("AETHER_HEARTBEAT_SECS", "1")
        .env("AETHER_SOURCE", "synthetic")
        .env("AETHER_POLL_SECS", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn shard-node")
}

async fn routed_leader(client: &mut CoordinatorClient<tonic::transport::Channel>) -> Option<String> {
    let state = client.get_cluster_state(ClusterStateRequest {}).await.ok()?.into_inner();
    state
        .nodes
        .iter()
        .find(|n| n.shard_id == 0 && n.role == NodeRole::Leader as i32)
        .map(|n| n.node_id.clone())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queries_and_ingestion_survive_raft_leader_sigkill() {
    let coordinator = start_coordinator().await;
    let mut cluster = Cluster { children: HashMap::new() };
    for id in ["c-a", "c-b", "c-c"] {
        cluster.children.insert(id.to_string(), spawn_member(id, &coordinator));
    }

    let endpoint = format!("http://{coordinator}");
    let mut client = loop {
        if let Ok(c) = CoordinatorClient::connect(endpoint.clone()).await {
            break c;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    // Continuous query load: every 100ms, one search for the term every synthetic doc has.
    // Record RPC errors and the running match count.
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

    // --- Group forms, elects, and the elected leader's ingestion produces searchable data ---
    let mut first_leader = None;
    for _ in 0..120 {
        if first_leader.is_none() {
            first_leader = routed_leader(&mut client).await;
        }
        if first_leader.is_some() && *latest_total.lock().unwrap() > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let first_leader = first_leader.expect("an elected leader should be routed");
    let total_at_kill = *latest_total.lock().unwrap();
    assert!(total_at_kill > 0, "elected leader never ingested (no synthetic docs searchable)");

    // --- SIGKILL the elected leader mid-ingest, mid-queries ---
    let mut child = cluster.children.remove(&first_leader).unwrap();
    child.kill().unwrap();
    child.wait().unwrap();

    // --- Survivors elect; the NEW leader resumes ingestion: counts grow past the kill ---
    let mut recovered = false;
    for _ in 0..120 {
        let leader_now = routed_leader(&mut client).await;
        let total_now = *latest_total.lock().unwrap();
        if let Some(l) = &leader_now {
            if l != &first_leader && total_now > total_at_kill {
                recovered = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    load.abort();

    assert!(
        recovered,
        "expected a new leader and growing data after the kill (was {total_at_kill} docs)"
    );
    // The whole time — group formation, kill, election, recovery — not one query errored.
    let errs = errors.lock().unwrap();
    assert!(errs.is_empty(), "query stream errored during chaos: {errs:?}");
}
