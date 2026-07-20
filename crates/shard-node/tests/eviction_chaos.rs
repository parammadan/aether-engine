//! Eviction chaos: when every virtual shard moves off a group, that group must PHYSICALLY
//! drop the migrated documents — on every member, identically, because eviction rides the
//! raft log. And it must do so under continuous query load with zero errors and no
//! duplicate aircraft in merged results (the dedup window closes as the old copies vanish).
//!
//! This is the stronger claim than raft_vshard's "ingestion moves": there we showed group
//! 0 stops growing; here we show it actively SHEDS what it no longer owns.

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
        for (_, c) in self.children.iter_mut() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

async fn start_coordinator() -> String {
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

fn spawn_member(node_id: &str, group: u32, coordinator: &str) -> (Child, String) {
    let addr = format!("127.0.0.1:{}", free_port());
    let child = Command::new(env!("CARGO_BIN_EXE_shard-node"))
        .env("AETHER_NODE_ID", node_id)
        .env("AETHER_SHARD_ADDR", &addr)
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
        .expect("spawn shard-node");
    (child, addr)
}

async fn member_count(addr: &str) -> Option<u64> {
    let mut c = ShardSearchClient::connect(format!("http://{addr}")).await.ok()?;
    let r = c.search(SearchRequest { query: "synthetica".into(), limit: 1 }).await.ok()?;
    Some(r.into_inner().total_matched)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_group_that_loses_all_its_vshards_sheds_the_documents_on_every_member() {
    let coordinator = start_coordinator().await;
    let mut cluster = Cluster { children: HashMap::new() };
    let mut g0_addrs = Vec::new();
    for group in 0..2u32 {
        for member in 0..3 {
            let id = format!("v{group}-m{member}");
            let (child, addr) = spawn_member(&id, group, &coordinator);
            if group == 0 {
                g0_addrs.push(addr);
            }
            cluster.children.insert(id, child);
        }
    }

    let mut client = loop {
        if let Ok(c) = CoordinatorClient::connect(format!("http://{coordinator}")).await {
            break c;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    // Continuous load: track cluster total, watch for query errors and duplicate aircraft.
    let errors = Arc::new(Mutex::new(0u64));
    let latest_total = Arc::new(Mutex::new(0u64));
    let dup_seen = Arc::new(Mutex::new(false));
    let load = tokio::spawn({
        let (errors, latest_total, dup_seen) = (errors.clone(), latest_total.clone(), dup_seen.clone());
        let mut client = client.clone();
        async move {
            loop {
                match client.search(SearchRequest { query: "synthetica".into(), limit: 100 }).await {
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
                    Err(_) => *errors.lock().unwrap() += 1,
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    });

    // Steady state: both groups routed, and group 0 holds a meaningful number of docs.
    let mut g0_baseline = 0;
    for _ in 0..160 {
        let state = client.get_cluster_state(ClusterStateRequest {}).await.unwrap().into_inner();
        let leaders = state.nodes.iter().filter(|n| n.role == NodeRole::Leader as i32).count();
        let held = member_count(&g0_addrs[0]).await.unwrap_or(0);
        if leaders == 2 && held > 15 {
            g0_baseline = held;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(g0_baseline > 15, "group 0 never reached a meaningful document count");

    // MIGRATE every virtual shard off group 0.
    for vshard in [0u32, 2] {
        let resp = client
            .reassign_v_shard(ReassignVShardRequest { vshard, group: 1 })
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok, "reassign refused: {}", resp.message);
    }

    // EVERY group-0 member must physically shed its documents (converge to ~0). Probing
    // each member directly, not just the leader — the eviction rode the log to all of them.
    let mut all_shed = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let mut counts = Vec::new();
        for addr in &g0_addrs {
            counts.push(member_count(addr).await.unwrap_or(u64::MAX));
        }
        if counts.iter().all(|&c| c == 0) {
            all_shed = true;
            break;
        }
    }
    assert!(all_shed, "some group-0 member kept documents after losing all its vshards");

    // The cluster kept serving throughout: group 1 still has the data, total didn't collapse.
    assert!(*latest_total.lock().unwrap() > 0, "cluster lost all data — eviction over-removed");

    load.abort();
    assert_eq!(*errors.lock().unwrap(), 0, "zero-error-under-eviction violated");
    assert!(!*dup_seen.lock().unwrap(), "duplicate aircraft appeared during the eviction window");
}
