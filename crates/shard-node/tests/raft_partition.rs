//! Partition chaos, CI tier: a 3-member raft group whose peer links run through
//! controllable TCP proxies (testkit), so the network can be cut deterministically.
//!
//! Scenario (a): ISOLATE THE LEADER. Its writes can no longer commit (no quorum), so its
//! applied store freezes; the majority elects a new leader and keeps committing.
//! Scenario (b): HEAL. The old leader rejoins, discards its uncommitted suffix, and every
//! member's store converges to the same document count.
//! Throughout: a continuous query stream through the coordinator must see ZERO errors —
//! partitions may cost coverage or freshness, never correctness.
//!
//! Topology note: every member dials its peers through its OWN pair of proxies
//! (AETHER_RAFT_DIAL_MAP), while raft membership carries the real addresses — so the
//! test controls each direction of each link independently. Coordinator traffic is NOT
//! proxied: the isolated member stays reachable by clients, exactly like a real
//! machine whose inter-node fabric failed while its front door still answers.

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

/// The routed leader's node id, per the coordinator's view.
async fn routed_leader(coordinator: &str) -> Option<String> {
    let mut c = CoordinatorClient::connect(format!("http://{coordinator}")).await.ok()?;
    let state = c.get_cluster_state(ClusterStateRequest {}).await.ok()?.into_inner();
    state
        .nodes
        .iter()
        .find(|n| n.role == NodeRole::Leader as i32)
        .map(|n| n.node_id.clone())
}

/// This member's own applied document count (via its data-plane gRPC, un-proxied).
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
async fn isolated_leader_cannot_commit_and_heal_converges_with_zero_query_errors() {
    let coordinator = start_coordinator().await;

    // Fixed member ports (raft membership carries these); per-direction proxies for
    // every ordered pair. proxies[i][j] carries member i's dials to member j.
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

    // 5.3: the zero-error witness — hammer the coordinator for the whole test.
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
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
    };

    // Steady state: a routed leader whose group is committing (its own count grows).
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
    let old_leader = leader.expect("no committing leader emerged");

    // (a) ISOLATE the leader: cut all four of its directions (its dials out, peers' dials in).
    for j in 0..3 {
        if j != old_leader {
            proxies[old_leader][j].as_ref().unwrap().block();
            proxies[j][old_leader].as_ref().unwrap().block();
        }
    }

    // The majority must elect a new leader and KEEP COMMITTING...
    let survivors: Vec<usize> = (0..3).filter(|&i| i != old_leader).collect();
    let majority_base = direct_count(&addrs[survivors[0]]).await.expect("survivor answers");
    let mut new_leader = None;
    for _ in 0..120 {
        if let Some(l) = routed_leader(&coordinator).await {
            let idx: usize = l.trim_start_matches('m').parse().unwrap();
            if idx != old_leader
                && direct_count(&addrs[survivors[0]]).await.unwrap_or(0) > majority_base
            {
                new_leader = Some(idx);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let new_leader = new_leader.expect("majority never elected a committing leader");
    assert_ne!(new_leader, old_leader);

    // ...while the ISOLATED leader's applied store freezes: nothing it accepts can reach
    // quorum, so nothing new commits or applies. Two reads across a growing-majority
    // window must be identical.
    let frozen_a = direct_count(&addrs[old_leader]).await.expect("isolated member still answers clients");
    let grow_base = direct_count(&addrs[survivors[0]]).await.unwrap_or(0);
    for _ in 0..120 {
        if direct_count(&addrs[survivors[0]]).await.unwrap_or(0) > grow_base + 5 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let frozen_b = direct_count(&addrs[old_leader]).await.expect("isolated member still answers clients");
    assert_eq!(
        frozen_a, frozen_b,
        "a minority leader committed writes during the partition — split-brain"
    );

    // (b) HEAL: every member's store must converge to the same count.
    for j in 0..3 {
        if j != old_leader {
            proxies[old_leader][j].as_ref().unwrap().unblock();
            proxies[j][old_leader].as_ref().unwrap().unblock();
        }
    }
    let mut converged = false;
    for _ in 0..240 {
        let mut counts = Vec::new();
        for addr in &addrs {
            counts.push(direct_count(addr).await.unwrap_or(0));
        }
        if counts[0] > 0 && counts.iter().all(|&c| c == counts[0]) {
            converged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(converged, "stores never converged after heal");

    // The whole run's verdict on the query stream: plenty of traffic, zero errors.
    load.abort();
    let total = query_count.load(Ordering::SeqCst);
    let errors = query_errors.load(Ordering::SeqCst);
    assert!(total > 20, "the load generator barely ran ({total} queries)");
    assert_eq!(errors, 0, "zero-error-under-partition violated: {errors}/{total} queries failed");
}
