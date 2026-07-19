//! Authoritative-state chaos: operator intent accepted by one coordinator replica must
//! SURVIVE that replica's death — the reason the vshard table and drain set are raft
//! state and not view state. And the surviving majority must keep accepting new intent.

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
async fn intent_survives_the_death_of_the_replica_that_accepted_it() {
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

    for addr in &addrs {
        for _ in 0..80 {
            if table_of(addr).await.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    // Register a node with every replica, then commit BOTH kinds of intent through
    // replica A (index 0): a placement change and a drain marker.
    for addr in &addrs {
        let mut c = connect(addr).await.expect("replica up");
        c.register_node(RegisterNodeRequest {
            node_id: "survivor-check".into(),
            address: "127.0.0.1:1".into(),
            shard_id: 0,
            role: NodeRole::Follower as i32,
        })
        .await
        .unwrap();
    }

    let mut accepted = false;
    for _ in 0..40 {
        if let Some(mut c) = connect(&addrs[0]).await {
            if let Ok(resp) = c.reassign_v_shard(ReassignVShardRequest { vshard: 3, group: 0 }).await {
                assert!(resp.get_ref().ok, "reassign rejected: {}", resp.get_ref().message);
                accepted = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(accepted, "the group never accepted the reassignment through A");

    let mut c = connect(&addrs[0]).await.unwrap();
    let resp = c.drain_node(DrainRequest { node_id: "survivor-check".into() }).await.unwrap();
    assert!(resp.get_ref().ok, "drain rejected: {}", resp.get_ref().message);

    // Both intents must be replicated before the kill is a fair test: wait for them on
    // the replicas that will survive.
    for addr in &addrs[1..] {
        let mut seen = false;
        for _ in 0..40 {
            if table_of(addr).await.as_deref() == Some(&[0, 1, 0, 0]) {
                let mut c = connect(addr).await.unwrap();
                let state = c.get_cluster_state(ClusterStateRequest {}).await.unwrap().into_inner();
                if state.nodes.iter().any(|n| n.node_id == "survivor-check" && n.draining) {
                    seen = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        assert!(seen, "replica {addr} never replicated the intent");
    }

    // SIGKILL replica A — the one that ACCEPTED both intents.
    children[0].kill().expect("kill replica A");
    let _ = children[0].wait();

    // The intents live on the survivors, and the surviving majority (2 of 3) must still
    // COMMIT NEW intent — quorum holds.
    for addr in &addrs[1..] {
        assert_eq!(
            table_of(addr).await.as_deref(),
            Some(&[0, 1, 0, 0][..]),
            "replica {addr} lost the placement decision with A's death"
        );
        let mut c = connect(addr).await.unwrap();
        let state = c.get_cluster_state(ClusterStateRequest {}).await.unwrap().into_inner();
        assert!(
            state.nodes.iter().any(|n| n.node_id == "survivor-check" && n.draining),
            "replica {addr} lost the drain marker with A's death"
        );
    }

    let mut accepted_after = false;
    for _ in 0..60 {
        if let Some(mut c) = connect(&addrs[1]).await {
            if let Ok(resp) = c.reassign_v_shard(ReassignVShardRequest { vshard: 0, group: 1 }).await {
                if resp.get_ref().ok {
                    accepted_after = true;
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(accepted_after, "the surviving majority stopped accepting intent");

    let mut converged = false;
    for _ in 0..40 {
        if table_of(&addrs[2]).await.as_deref() == Some(&[1, 1, 0, 0]) {
            converged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(converged, "post-kill intent never replicated across the survivors");

    for child in &mut children[1..] {
        let _ = child.kill();
        let _ = child.wait();
    }
}
