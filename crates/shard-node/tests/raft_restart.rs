//! Durability chaos: a raft member is SIGKILLed mid-ingest and restarted on its own data
//! directory. It must recover from its local WAL + snapshots (proven by its recovery log
//! line), rejoin the group, and converge — with zero query errors across the whole cycle.
//! This is the difference between "correct while nothing restarts" and correct.

use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
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

/// Spawn a member with a persistent data dir and a FIXED port (its address lives in raft
/// membership, so a restart must come back at the same address). Stdout goes to a file so
/// the test can verify recovery actually used the local WAL.
fn spawn_member(
    node_id: &str,
    coordinator: &str,
    port: u16,
    data_dir: &PathBuf,
    stdout_to: &PathBuf,
) -> Child {
    Command::new(env!("CARGO_BIN_EXE_shard-node"))
        .env("AETHER_NODE_ID", node_id)
        .env("AETHER_SHARD_ADDR", format!("127.0.0.1:{port}"))
        .env("AETHER_SHARD_INDEX", "0")
        .env("AETHER_SHARD_COUNT", "1")
        .env("AETHER_CONSENSUS", "raft")
        .env("AETHER_GROUP_SIZE", "3")
        .env("AETHER_COORDINATOR_ADDR", coordinator)
        .env("AETHER_HEARTBEAT_SECS", "1")
        .env("AETHER_SOURCE", "synthetic")
        .env("AETHER_POLL_SECS", "1")
        .env("AETHER_DATA_DIR", data_dir)
        .stdout(Stdio::from(File::create(stdout_to).unwrap()))
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn shard-node")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_killed_member_recovers_from_its_wal_and_rejoins() {
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("raft-restart-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    let coordinator = start_coordinator().await;
    let mut cluster = Cluster { children: HashMap::new() };
    let mut ports = HashMap::new();
    for id in ["p-a", "p-b", "p-c"] {
        let port = free_port();
        ports.insert(id.to_string(), port);
        let child = spawn_member(id, &coordinator, port, &root.join(id), &root.join(format!("{id}.log")));
        cluster.children.insert(id.to_string(), child);
    }

    let endpoint = format!("http://{coordinator}");
    let mut client = loop {
        if let Ok(c) = CoordinatorClient::connect(endpoint.clone()).await {
            break c;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    // Continuous query load; must never error across kill + restart.
    let errors = Arc::new(Mutex::new(Vec::<String>::new()));
    let latest_total = Arc::new(Mutex::new(0u64));
    let load = tokio::spawn({
        let errors = errors.clone();
        let latest_total = latest_total.clone();
        let mut client = client.clone();
        async move {
            loop {
                match client.search(SearchRequest { query: "synthetica".into(), limit: 3 }).await {
                    Ok(resp) => *latest_total.lock().unwrap() = resp.into_inner().total_matched,
                    Err(status) => errors.lock().unwrap().push(status.to_string()),
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    });

    // Steady ingesting state with a routed leader.
    let mut leader = None;
    for _ in 0..120 {
        let state = client.get_cluster_state(ClusterStateRequest {}).await.unwrap().into_inner();
        leader = state
            .nodes
            .iter()
            .find(|n| n.role == NodeRole::Leader as i32)
            .map(|n| n.node_id.clone());
        if leader.is_some() && *latest_total.lock().unwrap() >= 20 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let victim = leader.expect("a routed leader with data");
    let total_at_kill = *latest_total.lock().unwrap();
    assert!(total_at_kill >= 20, "not enough committed data before the kill");

    // --- SIGKILL the leader mid-ingest ---
    let mut child = cluster.children.remove(&victim).unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await; // survivors elect meanwhile

    // --- Restart the SAME member: same identity, same address, same data dir ---
    let restart_log = root.join(format!("{victim}-restart.log"));
    let child = spawn_member(
        &victim,
        &coordinator,
        ports[&victim],
        &root.join(&victim),
        &restart_log,
    );
    cluster.children.insert(victim.clone(), child);

    // Its own store must recover the pre-kill data (local WAL replay + catch-up) — we
    // query the restarted node DIRECTLY, not through the coordinator.
    let mut direct = loop {
        if let Ok(c) =
            ShardSearchClient::connect(format!("http://127.0.0.1:{}", ports[&victim])).await
        {
            break c;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let mut recovered = 0u64;
    for _ in 0..120 {
        recovered = direct
            .search(SearchRequest { query: "synthetica".into(), limit: 1 })
            .await
            .map(|r| r.into_inner().total_matched)
            .unwrap_or(0);
        if recovered >= total_at_kill {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        recovered >= total_at_kill,
        "restarted member holds {recovered} docs, expected at least the {total_at_kill} committed before the kill"
    );

    // Proof the recovery came through the durable path: its boot log reports WAL replay.
    let boot_log = std::fs::read_to_string(&restart_log).unwrap_or_default();
    let recovered_records = boot_log
        .lines()
        .find(|l| l.starts_with("wal: recovered"))
        .and_then(|l| l.split_whitespace().nth(2))
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    assert!(
        recovered_records > 0,
        "restart did not replay a local WAL (boot log: {boot_log:?})"
    );

    load.abort();
    let errs = errors.lock().unwrap();
    assert!(errs.is_empty(), "query stream errored across kill+restart: {errs:?}");

    let _ = std::fs::remove_dir_all(&root);
}
