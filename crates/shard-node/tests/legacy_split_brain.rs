//! The CONTRAST EXHIBIT: the same partition that consensus rides out cleanly, run under
//! legacy coordinator-driven failover — demonstrating the split-brain window and the
//! permanent data hole it leaves. This test PASSES by proving the flaw exists: it is the
//! documented "why consensus exists" artifact, not a regression test.
//!
//! Anatomy of the failure. Cut the leader off from the COORDINATOR only (the leader
//! itself is healthy and keeps ingesting). The reaper declares it dead and promotes a
//! follower: now two nodes both believe they lead. The deposed leader keeps accepting
//! writes it can no longer replicate (discovery goes through the coordinator it can't
//! reach — each batch replicates to nobody and is gone). Heal the link, and legacy has
//! no log to reconcile from: the follower is left permanently missing every document
//! from the window. Under raft the identical cut converges byte-for-byte — see the
//! partition tests.

use std::process::{Child, Command, Stdio};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use common::pb::coordinator_client::CoordinatorClient;
use common::pb::coordinator_server::CoordinatorServer;
use common::pb::shard_search_client::ShardSearchClient;
use common::pb::{ClusterStateRequest, NodeRole, SearchRequest};
use coordinator::registry::Registry;
use coordinator::service::CoordinatorService;
use testkit::Proxy;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

const LIVENESS: Duration = Duration::from_secs(2);

/// A coordinator WITH the reaper+promotion loop (the legacy failover machinery).
async fn start_reaping_coordinator() -> String {
    let registry = Arc::new(RwLock::new(
        Registry::new(1).with_liveness_timeout(LIVENESS),
    ));
    let reaper = registry.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(500));
        loop {
            ticker.tick().await;
            let mut reg = reaper.write().unwrap();
            let now = Instant::now();
            let _ = reg.reap_dead(now, LIVENESS);
            let _ = reg.promote_orphaned_shards();
        }
    });
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
        .search(SearchRequest { query: "synthetica".into(), limit: 1, filter: None })
        .await
        .ok()?
        .into_inner();
    Some(resp.total_matched)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_failover_has_a_split_brain_window_and_never_reconciles() {
    let coordinator = start_reaping_coordinator().await;

    // The leader reaches the coordinator through a cuttable proxy; the follower directly.
    let leader_link = Proxy::spawn(coordinator.clone()).await;
    let leader_addr = format!("127.0.0.1:{}", free_port());
    let follower_addr = format!("127.0.0.1:{}", free_port());

    let mut leader: Child = Command::new(env!("CARGO_BIN_EXE_shard-node"))
        .env("AETHER_NODE_ID", "legacy-leader")
        .env("AETHER_SHARD_ADDR", &leader_addr)
        .env("AETHER_SHARD_INDEX", "0")
        .env("AETHER_SHARD_COUNT", "1")
        .env("AETHER_ROLE", "leader")
        .env("AETHER_COORDINATOR_ADDR", leader_link.listen_addr())
        .env("AETHER_HEARTBEAT_SECS", "1")
        .env("AETHER_SOURCE", "synthetic")
        .env("AETHER_POLL_SECS", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn legacy leader");
    let mut follower: Child = Command::new(env!("CARGO_BIN_EXE_shard-node"))
        .env("AETHER_NODE_ID", "legacy-follower")
        .env("AETHER_SHARD_ADDR", &follower_addr)
        .env("AETHER_SHARD_INDEX", "0")
        .env("AETHER_SHARD_COUNT", "1")
        .env("AETHER_ROLE", "follower")
        .env("AETHER_COORDINATOR_ADDR", &coordinator)
        .env("AETHER_HEARTBEAT_SECS", "1")
        .env("AETHER_INGEST", "off")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn legacy follower");

    // Steady state: leader routed, follower receiving best-effort replication.
    let mut steady = false;
    for _ in 0..120 {
        if routed_leader(&coordinator).await.as_deref() == Some("legacy-leader")
            && direct_count(&follower_addr).await.unwrap_or(0) > 5
        {
            steady = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(steady, "legacy pair never reached steady replication");

    // THE CUT: leader loses the coordinator (only). The reaper declares it dead and
    // promotes the follower — while the real leader is alive and ingesting.
    leader_link.block();
    let mut promoted = false;
    for _ in 0..40 {
        if routed_leader(&coordinator).await.as_deref() == Some("legacy-follower") {
            promoted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(promoted, "the reaper never promoted the follower");

    // THE SPLIT-BRAIN WINDOW, measured: the deposed leader keeps accepting writes...
    let deposed_at_promotion = direct_count(&leader_addr).await.expect("deposed leader still serves");
    let follower_at_promotion = direct_count(&follower_addr).await.expect("promoted follower serves");
    let mut deposed_grew = 0;
    for _ in 0..40 {
        let now = direct_count(&leader_addr).await.unwrap_or(0);
        if now > deposed_at_promotion + 5 {
            deposed_grew = now - deposed_at_promotion;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        deposed_grew > 0,
        "the deposed leader should keep accepting writes — that IS the split brain"
    );
    // ...that the routed 'leader' never sees: its store is frozen at the cut.
    let follower_during = direct_count(&follower_addr).await.unwrap();
    assert_eq!(
        follower_during, follower_at_promotion,
        "the promoted follower has no source of new data — every write in the window is lost to it"
    );

    // HEAL. Legacy has no log to reconcile from: the follower stays permanently short.
    leader_link.unblock();
    tokio::time::sleep(Duration::from_secs(4)).await; // replication resumes for NEW batches
    let leader_final = direct_count(&leader_addr).await.unwrap();
    let follower_final = direct_count(&follower_addr).await.unwrap();
    assert!(
        follower_final < leader_final,
        "legacy replication should NOT be able to close the window's hole \
         (leader={leader_final}, follower={follower_final}) — if this ever fails, \
         the exhibit is stale and consensus has quietly grown a reconciliation path"
    );

    println!(
        "split-brain window: deposed leader accepted {deposed_grew}+ documents invisible to the \
         promoted follower; permanent hole after heal: {} documents",
        leader_final - follower_final
    );

    let _ = leader.kill();
    let _ = leader.wait();
    let _ = follower.kill();
    let _ = follower.wait();
}
