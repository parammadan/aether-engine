//! Consensus in the full cluster, across real processes: three shard-node binaries form a
//! raft group via coordinator discovery, elect a leader (reported through heartbeats into
//! the coordinator's routing view), get their leader SIGKILLed, and re-elect — with the
//! coordinator's view following the election, not driving it.

use std::collections::HashMap;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use common::pb::coordinator_client::CoordinatorClient;
use common::pb::coordinator_server::CoordinatorServer;
use common::pb::{ClusterStateRequest, NodeRole};
use coordinator::registry::Registry;
use coordinator::service::CoordinatorService;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// Kills all children on drop so a failing test never leaks processes.
struct Cluster {
    children: HashMap<String, Child>,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for child in self.children.values_mut() {
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
    // Bind-then-drop; the port stays free long enough for the child to claim it.
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
        .env("AETHER_INGEST", "off") // no live network in this test; replication is covered elsewhere
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn shard-node")
}

/// The coordinator-routed leader of shard 0, if any.
async fn routed_leader(client: &mut CoordinatorClient<tonic::transport::Channel>) -> Option<String> {
    let state = client.get_cluster_state(ClusterStateRequest {}).await.ok()?.into_inner();
    state
        .nodes
        .iter()
        .find(|n| n.shard_id == 0 && n.role == NodeRole::Leader as i32)
        .map(|n| n.node_id.clone())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn processes_form_a_raft_group_and_survive_leader_sigkill() {
    let coordinator = start_coordinator().await;
    let mut cluster = Cluster { children: HashMap::new() };
    for id in ["m-a", "m-b", "m-c"] {
        cluster.children.insert(id.to_string(), spawn_member(id, &coordinator));
    }

    let mut client = loop {
        if let Ok(c) = CoordinatorClient::connect(format!("http://{coordinator}")).await {
            break c;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    // --- The group forms, elects, and the election shows up in coordinator routing ---
    let mut first_leader = None;
    for _ in 0..60 {
        if let Some(leader) = routed_leader(&mut client).await {
            first_leader = Some(leader);
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let first_leader = first_leader.expect("a raft-elected leader should be routed within 30s");
    assert!(cluster.children.contains_key(&first_leader));

    // --- SIGKILL the elected leader's PROCESS ---
    let mut child = cluster.children.remove(&first_leader).unwrap();
    child.kill().unwrap();
    child.wait().unwrap();

    // --- The survivors re-elect, and routing follows the new leader ---
    let mut new_leader = None;
    for _ in 0..60 {
        if let Some(leader) = routed_leader(&mut client).await {
            if leader != first_leader {
                new_leader = Some(leader);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let new_leader = new_leader.expect("survivors should elect and route a new leader within 30s");
    assert_ne!(new_leader, first_leader);
    assert!(cluster.children.contains_key(&new_leader), "new leader must be a survivor");
}
