//! The ASYMMETRIC partition — the case that kills naive implementations. One direction
//! of one link dies: the leader can no longer reach one follower, but that follower can
//! still reach the leader. The follower stops hearing heartbeats, times out, and
//! campaigns with ever-higher terms — and its vote requests DO arrive, repeatedly
//! deposing a leader that cannot talk back. A correct system rides the term churn:
//! whoever can reach a quorum ends up committing, the query stream never errors, and
//! healing converges every store.
//!
//! Separately gated from the symmetric partition test on purpose: this scenario must
//! hold its own flake-free bar, not hide behind the easier one.

use std::collections::HashMap;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use common::pb::coordinator_client::CoordinatorClient;
use common::pb::coordinator_server::CoordinatorServer;
use common::pb::shard_search_client::ShardSearchClient;
use common::pb::{ClusterStateRequest, NodeRole, SearchRequest};
use coordinator::registry::Registry;
use coordinator::service::CoordinatorService;
use testkit::Proxy;
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

async fn routed_leader(coordinator: &str) -> Option<String> {
    let mut c = CoordinatorClient::connect(format!("http://{coordinator}")).await.ok()?;
    let state = c.get_cluster_state(ClusterStateRequest {}).await.ok()?.into_inner();
    state
        .nodes
        .iter()
        .find(|n| n.role == NodeRole::Leader as i32)
        .map(|n| n.node_id.clone())
}

async fn direct_count(member_addr: &str) -> Option<u64> {
    let mut c = ShardSearchClient::connect(format!("http://{member_addr}")).await.ok()?;
    let resp = c
        .search(SearchRequest { query: "synthetica".into(), limit: 1 })
        .await
        .ok()?
        .into_inner();
    Some(resp.total_matched)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn asymmetric_cut_keeps_committing_with_zero_query_errors_and_heals() {
    let coordinator = start_coordinator().await;

    let ports: Vec<u16> = (0..3).map(|_| free_port()).collect();
    let addrs: Vec<String> = ports.iter().map(|p| format!("127.0.0.1:{p}")).collect();
    let mut proxies: Vec<Vec<Option<Proxy>>> = Vec::new();
    for i in 0..3 {
        let mut row = Vec::new();
        for j in 0..3 {
            row.push(if i == j { None } else { Some(Proxy::spawn(addrs[j].clone()).await) });
        }
        proxies.push(row);
    }

    let mut children = HashMap::new();
    for i in 0..3 {
        let dial_map: Vec<String> = (0..3)
            .filter(|&j| j != i)
            .map(|j| format!("{}={}", addrs[j], proxies[i][j].as_ref().unwrap().listen_addr()))
            .collect();
        let child = Command::new(env!("CARGO_BIN_EXE_shard-node"))
            .env("AETHER_NODE_ID", format!("m{i}"))
            .env("AETHER_SHARD_ADDR", &addrs[i])
            .env("AETHER_SHARD_INDEX", "0")
            .env("AETHER_SHARD_COUNT", "1")
            .env("AETHER_CONSENSUS", "raft")
            .env("AETHER_GROUP_SIZE", "3")
            .env("AETHER_COORDINATOR_ADDR", &coordinator)
            .env("AETHER_HEARTBEAT_SECS", "1")
            .env("AETHER_SOURCE", "synthetic")
            .env("AETHER_POLL_SECS", "1")
            .env("AETHER_RAFT_DIAL_MAP", dial_map.join(","))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn member");
        children.insert(format!("m{i}"), child);
    }
    let _cluster = Cluster { children };

    // The zero-error witness runs through every phase.
    let query_errors = Arc::new(AtomicU64::new(0));
    let query_count = Arc::new(AtomicU64::new(0));
    let load = {
        let (coordinator, errors, count) = (coordinator.clone(), query_errors.clone(), query_count.clone());
        tokio::spawn(async move {
            loop {
                match CoordinatorClient::connect(format!("http://{coordinator}")).await {
                    Ok(mut c) => {
                        if c.search(SearchRequest { query: "synthetica".into(), limit: 3 }).await.is_err() {
                            errors.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::SeqCst);
                    }
                }
                count.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    };

    // Steady state.
    let mut leader = None;
    for _ in 0..120 {
        if let Some(l) = routed_leader(&coordinator).await {
            let idx: usize = l.trim_start_matches('m').parse().unwrap();
            if direct_count(&addrs[idx]).await.unwrap_or(0) > 10 {
                leader = Some(idx);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let leader = leader.expect("no committing leader emerged");
    let follower = (0..3).find(|&i| i != leader).unwrap();

    // THE CUT: leader→follower only. The follower can still call the leader.
    proxies[leader][follower].as_ref().unwrap().block();

    // Through the term churn this provokes, the group must KEEP COMMITTING: some member's
    // applied count must keep growing across the window. (Which member leads at any
    // instant is the churn's business, not ours.)
    let max_count = |counts: &[u64]| counts.iter().copied().max().unwrap_or(0);
    let mut base = Vec::new();
    for addr in &addrs {
        base.push(direct_count(addr).await.unwrap_or(0));
    }
    let base_max = max_count(&base);
    let mut progressed = false;
    for _ in 0..240 {
        let mut now = Vec::new();
        for addr in &addrs {
            now.push(direct_count(addr).await.unwrap_or(0));
        }
        if max_count(&now) > base_max + 10 {
            progressed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(progressed, "the group stopped committing under an asymmetric cut (term-war livelock)");

    // HEAL: everyone converges to one count, and a single stable leader is routed.
    proxies[leader][follower].as_ref().unwrap().unblock();
    let mut converged = false;
    for _ in 0..240 {
        let mut counts = Vec::new();
        for addr in &addrs {
            counts.push(direct_count(addr).await.unwrap_or(0));
        }
        if counts[0] > 0 && counts.iter().all(|&c| c == counts[0]) && routed_leader(&coordinator).await.is_some() {
            converged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(converged, "stores never converged after healing the asymmetric cut");

    load.abort();
    let total = query_count.load(Ordering::SeqCst);
    let errors = query_errors.load(Ordering::SeqCst);
    assert!(total > 50, "the load generator barely ran ({total} queries)");
    assert_eq!(errors, 0, "zero-error under asymmetric partition violated: {errors}/{total} failed");
}
