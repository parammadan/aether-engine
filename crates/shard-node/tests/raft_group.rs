//! A shard group under real consensus: three members, each with its own document store and
//! gRPC raft transport. Elects a leader, quorum-replicates document batches into every
//! member's searchable store, survives the leader dying by electing a new one, and keeps
//! accepting writes — the properties that make failover principled rather than hand-waved.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use openraft::BasicNode;
use prost::Message;
use shard_node::raft::network::GrpcRaftNetworkFactory;
use shard_node::raft::service::RaftTransportService;
use shard_node::raft::storage::{LogStore, StateMachineStore};
use shard_node::raft::{raft_config, DocBatch, Raft};
use shard_node::store::ShardStore;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use common::pb::raft_transport_server::RaftTransportServer;
use common::pb::{FlightDocument, ReplicateRequest};

struct Member {
    raft: Raft,
    store: Arc<RwLock<ShardStore>>,
    server: tokio::task::JoinHandle<()>,
}

/// Start one group member: its store, raft instance, and raft transport server on an
/// ephemeral port. Returns the member and its address.
async fn start_member(id: u64) -> (Member, String) {
    let store = Arc::new(RwLock::new(ShardStore::new()));
    let raft = Raft::new(
        id,
        Arc::new(raft_config().validate().expect("valid raft config")),
        GrpcRaftNetworkFactory,
        LogStore::default(),
        StateMachineStore::new(store.clone()),
    )
    .await
    .expect("raft node should start");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let service = RaftTransportService::new(raft.clone());
    let server = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(RaftTransportServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });

    (Member { raft, store, server }, addr)
}

fn batch(icao24: &str, callsign: &str) -> DocBatch {
    let req = ReplicateRequest {
        documents: vec![FlightDocument {
            icao24: icao24.to_string(),
            callsign: callsign.to_string(),
            ..Default::default()
        }],
        shard_id: 0,
    };
    let cmd = common::pb::ShardCommand {
        kind: Some(common::pb::shard_command::Kind::Batch(req)),
    };
    DocBatch(cmd.encode_to_vec())
}

/// Poll until a member's store can find the callsign (committed entries apply async).
async fn wait_for_doc(store: &Arc<RwLock<ShardStore>>, callsign: &str) -> bool {
    for _ in 0..100 {
        if store.read().unwrap().search(callsign, 5).total_matched == 1 {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shard_group_elects_replicates_and_survives_leader_death() {
    // --- Boot three members and form the group ---
    let (m1, a1) = start_member(1).await;
    let (m2, a2) = start_member(2).await;
    let (m3, a3) = start_member(3).await;

    let mut membership = BTreeMap::new();
    membership.insert(1u64, BasicNode::new(a1));
    membership.insert(2u64, BasicNode::new(a2));
    membership.insert(3u64, BasicNode::new(a3));
    m1.raft.initialize(membership).await.expect("group should initialize");

    // --- A leader is ELECTED (not appointed by any control plane) ---
    m1.raft
        .wait(Some(Duration::from_secs(10)))
        .metrics(|m| m.current_leader.is_some(), "leader elected")
        .await
        .expect("group should elect a leader");
    let leader_id = m1.raft.metrics().borrow().current_leader.unwrap();
    let members = [(1u64, &m1), (2, &m2), (3, &m3)];
    let leader = members.iter().find(|(id, _)| *id == leader_id).unwrap().1;

    // --- A write commits via quorum and lands in EVERY member's searchable store ---
    leader
        .raft
        .client_write(batch("a1b2c3", "UAL231"))
        .await
        .expect("quorum write should commit");
    for (id, member) in &members {
        assert!(
            wait_for_doc(&member.store, "ual231").await,
            "member {id} never applied the committed write"
        );
    }

    // --- KILL the leader (raft shut down, transport gone) ---
    leader.raft.shutdown().await.expect("shutdown");
    leader.server.abort();

    // --- The survivors elect a NEW leader among themselves ---
    let survivors: Vec<&(u64, &Member)> =
        members.iter().filter(|(id, _)| *id != leader_id).collect();
    let watch = &survivors[0].1.raft;
    watch
        .wait(Some(Duration::from_secs(10)))
        .metrics(
            |m| m.current_leader.is_some() && m.current_leader != Some(leader_id),
            "new leader elected after leader death",
        )
        .await
        .expect("survivors should elect a new leader");
    let new_leader_id = watch.metrics().borrow().current_leader.unwrap();
    assert_ne!(new_leader_id, leader_id);

    // --- Writes CONTINUE on the new leader (quorum = the two survivors) ---
    let new_leader = members.iter().find(|(id, _)| *id == new_leader_id).unwrap().1;
    new_leader
        .raft
        .client_write(batch("d4e5f6", "DAL45"))
        .await
        .expect("write after failover should commit");
    for (id, member) in &survivors {
        assert!(
            wait_for_doc(&member.store, "dal45").await,
            "survivor {id} never applied the post-failover write"
        );
    }
}
