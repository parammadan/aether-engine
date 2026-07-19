//! The coordinator state group: operator intent (vshard placement, drain markers)
//! committed through ANY of three coordinator replicas must appear on ALL of them —
//! followers forward to the group's leader, so the caller never needs to know who leads.

use std::process::{Command as Proc, Stdio};
use std::time::Duration;

use common::pb::coordinator_client::CoordinatorClient;
use common::pb::{
    ClusterStateRequest, DrainRequest, NodeRole, ReassignVShardRequest, RegisterNodeRequest,
    VShardAssignmentsRequest,
};

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

async fn connect(addr: &str) -> Option<CoordinatorClient<tonic::transport::Channel>> {
    CoordinatorClient::connect(format!("http://{addr}")).await.ok()
}

async fn table_of(addr: &str) -> Option<Vec<u32>> {
    let mut c = connect(addr).await?;
    Some(c.get_v_shard_assignments(VShardAssignmentsRequest {}).await.ok()?.into_inner().group_of)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn intent_committed_at_any_replica_is_visible_on_all() {
    let addrs: Vec<String> = (0..3).map(|_| format!("127.0.0.1:{}", free_port())).collect();
    let peers = format!("1={},2={},3={}", addrs[0], addrs[1], addrs[2]);

    let mut children: Vec<std::process::Child> = (0..3)
        .map(|i| {
            Proc::new(env!("CARGO_BIN_EXE_coordinator"))
                .env("AETHER_COORDINATOR_ADDR", &addrs[i])
                .env("AETHER_SHARD_COUNT", "2")
                .env("AETHER_VSHARDS", "4")
                .env("AETHER_CONTROL_ID", (i + 1).to_string())
                .env("AETHER_CONTROL_PEERS", &peers)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn coordinator replica")
        })
        .collect();

    // All replicas up, each with the initial round-robin table.
    for addr in &addrs {
        let mut ready = false;
        for _ in 0..80 {
            if table_of(addr).await.as_deref() == Some(&[0, 1, 0, 1]) {
                ready = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        assert!(ready, "replica {addr} never came up with the initial table");
    }

    // Reassign through replica 2 — whichever role it holds, the write must land (the
    // group needs a leader first, so retry through election).
    let mut accepted = false;
    for _ in 0..40 {
        if let Some(mut c) = connect(&addrs[1]).await {
            if let Ok(resp) = c.reassign_v_shard(ReassignVShardRequest { vshard: 2, group: 1 }).await {
                let resp = resp.into_inner();
                assert!(resp.ok, "validated reassign must succeed: {}", resp.message);
                accepted = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(accepted, "the group never accepted the reassignment");

    // ...and EVERY replica must converge on the new table.
    for addr in &addrs {
        let mut converged = false;
        for _ in 0..40 {
            if table_of(addr).await.as_deref() == Some(&[0, 1, 1, 1]) {
                converged = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        assert!(converged, "replica {addr} never saw the replicated reassignment");
    }

    // Validation still rejects garbage, replicated or not.
    let mut c = connect(&addrs[1]).await.unwrap();
    let resp = c.reassign_v_shard(ReassignVShardRequest { vshard: 99, group: 0 }).await.unwrap().into_inner();
    assert!(!resp.ok, "out-of-range vshard must be rejected before it reaches the log");

    // Drain: register a node with every replica (as real nodes do), mark it through
    // replica 3, and expect the marker on all three.
    for addr in &addrs {
        let mut c = connect(addr).await.unwrap();
        c.register_node(RegisterNodeRequest {
            node_id: "drain-me".into(),
            address: "127.0.0.1:1".into(),
            shard_id: 0,
            role: NodeRole::Follower as i32,
        })
        .await
        .unwrap();
    }
    let mut c = connect(&addrs[2]).await.unwrap();
    let resp = c.drain_node(DrainRequest { node_id: "drain-me".into() }).await.unwrap().into_inner();
    assert!(resp.ok, "drain of a known node must be accepted: {}", resp.message);

    for addr in &addrs {
        let mut marked = false;
        for _ in 0..40 {
            let mut c = connect(addr).await.unwrap();
            let state = c.get_cluster_state(ClusterStateRequest {}).await.unwrap().into_inner();
            if state.nodes.iter().any(|n| n.node_id == "drain-me" && n.draining) {
                marked = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        assert!(marked, "replica {addr} never saw the replicated drain marker");
    }

    for child in &mut children {
        let _ = child.kill();
        let _ = child.wait();
    }
}
