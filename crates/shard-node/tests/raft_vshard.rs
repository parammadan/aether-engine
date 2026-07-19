//! Live shard migration across real processes: two raft groups serve four virtual shards
//! (two each) under active ingestion and query load; ALL of group 0's virtual shards are
//! reassigned to group 1. Asserts the load actually moves — group 0's leader stops
//! ingesting (its store plateaus) while the cluster keeps growing — with deduplicated hits
//! and zero query errors throughout. Placement moves; the modulus never changes.

use std::collections::{HashMap, HashSet};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use common::pb::coordinator_client::CoordinatorClient;
use common::pb::coordinator_server::CoordinatorServer;
use common::pb::shard_search_client::ShardSearchClient;
use common::pb::{ClusterStateRequest, NodeRole, ReassignVShardRequest, SearchRequest};
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
    // Two groups, four virtual shards (round-robin: v0,v2 -> group 0; v1,v3 -> group 1).
    let registry = Arc::new(RwLock::new(Registry::new(2).with_vshards(4)));
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

fn spawn_member(node_id: &str, group: u32, coordinator: &str) -> Child {
    Command::new(env!("CARGO_BIN_EXE_shard-node"))
        .env("AETHER_NODE_ID", node_id)
        .env("AETHER_SHARD_ADDR", format!("127.0.0.1:{}", free_port()))
        .env("AETHER_SHARD_INDEX", group.to_string())
        .env("AETHER_SHARD_COUNT", "2")
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reassigning_virtual_shards_moves_ingestion_between_groups_live() {
    let coordinator = start_coordinator().await;
    let mut cluster = Cluster { children: HashMap::new() };
    for group in 0..2u32 {
        for member in 0..3 {
            let id = format!("v{group}-m{member}");
            cluster.children.insert(id.clone(), spawn_member(&id, group, &coordinator));
        }
    }

    let endpoint = format!("http://{coordinator}");
    let mut client = loop {
        if let Ok(c) = CoordinatorClient::connect(endpoint.clone()).await {
            break c;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    // Continuous query load; also checks every page for duplicate aircraft.
    let errors = Arc::new(Mutex::new(Vec::<String>::new()));
    let latest_total = Arc::new(Mutex::new(0u64));
    let dup_seen = Arc::new(Mutex::new(false));
    let load = tokio::spawn({
        let errors = errors.clone();
        let latest_total = latest_total.clone();
        let dup_seen = dup_seen.clone();
        let mut client = client.clone();
        async move {
            loop {
                match client.search(SearchRequest { query: "synthetica".into(), limit: 50 }).await {
                    Ok(resp) => {
                        let resp = resp.into_inner();
                        *latest_total.lock().unwrap() = resp.total_matched;
                        let mut ids = HashSet::new();
                        for h in &resp.hits {
                            if !ids.insert(h.document.as_ref().unwrap().icao24.clone()) {
                                *dup_seen.lock().unwrap() = true;
                            }
                        }
                    }
                    Err(status) => errors.lock().unwrap().push(status.to_string()),
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    });

    // Steady state: both groups routed, data flowing.
    let mut steady = false;
    for _ in 0..120 {
        let state = client.get_cluster_state(ClusterStateRequest {}).await.unwrap().into_inner();
        let leaders = state.nodes.iter().filter(|n| n.role == NodeRole::Leader as i32).count();
        if leaders == 2 && *latest_total.lock().unwrap() > 0 {
            steady = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(steady, "cluster never reached two routed leaders with data");

    // Find group 0's leader so we can watch its store directly.
    let state = client.get_cluster_state(ClusterStateRequest {}).await.unwrap().into_inner();
    let g0_leader_addr = state
        .nodes
        .iter()
        .find(|n| n.shard_id == 0 && n.role == NodeRole::Leader as i32)
        .expect("group 0 leader")
        .address
        .clone();
    let mut g0 = ShardSearchClient::connect(format!("http://{g0_leader_addr}")).await.unwrap();

    // --- MIGRATE: move every virtual shard off group 0 ---
    for vshard in [0u32, 2] {
        let resp = client
            .reassign_v_shard(ReassignVShardRequest { vshard, group: 1 })
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok, "reassign refused: {}", resp.message);
    }

    // Group 0's leader must stop ingesting (owns nothing now): its own store plateaus,
    // while the cluster total keeps growing (group 1 carries the whole load).
    let mut moved = false;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let g0_before = g0
            .search(SearchRequest { query: "synthetica".into(), limit: 1 })
            .await
            .map(|r| r.into_inner().total_matched)
            .unwrap_or(0);
        let cluster_before = *latest_total.lock().unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;
        let g0_after = g0
            .search(SearchRequest { query: "synthetica".into(), limit: 1 })
            .await
            .map(|r| r.into_inner().total_matched)
            .unwrap_or(0);
        let cluster_after = *latest_total.lock().unwrap();
        if g0_after == g0_before && cluster_after > cluster_before {
            moved = true;
            break;
        }
    }
    assert!(moved, "group 0 kept ingesting after all its virtual shards were reassigned");

    load.abort();
    assert!(!*dup_seen.lock().unwrap(), "duplicate aircraft appeared in merged results");
    let errs = errors.lock().unwrap();
    assert!(errs.is_empty(), "query stream errored during migration: {errs:?}");
}
