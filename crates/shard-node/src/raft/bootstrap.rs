//! Forming a shard's raft group in a live cluster, and running ingestion under raft.
//!
//! # Group formation
//! Members register with the coordinator like any node, then poll `ListShardMembers` until
//! the group is complete (`group_size` members). Every member computes the same member map
//! from the same set, and the one with the **smallest raft id** calls `initialize` —
//! deterministic over an identical set, so exactly one member bootstraps and the others
//! join through the resulting election. No hardcoded peer lists.
//!
//! # Ingestion under raft (leadership-gated)
//! Only the elected leader ingests, and it writes through `client_write`, so every document
//! batch is quorum-committed into all members' stores. The gate is raft metrics: win the
//! election → start ingesting; lose leadership (metrics change, or a write refuses with
//! forward-to-leader) → stop. A node acts on leadership it can *observe locally*, rather
//! than being told by a control plane — which is also what lets a newly elected leader pick
//! up ingestion automatically after a failover.

use std::collections::BTreeMap;
use std::time::Duration;

use openraft::BasicNode;
use prost::Message;

use common::pb::coordinator_client::CoordinatorClient;
use common::pb::ShardMembersRequest;
use common::shard::fnv1a_64;

use crate::ingest::{FlightSource, ShardAssignment};

use super::{DocBatch, Raft};

/// A node's raft id: the FNV-1a hash of its cluster node id. Deterministic everywhere (the
/// same argument as the shard key), so every member derives the same id for every peer.
pub fn raft_node_id(node_id: &str) -> u64 {
    fnv1a_64(node_id.as_bytes())
}

/// Poll the coordinator until `group_size` members of this shard have registered; return
/// the raft member map (raft id -> transport address).
pub async fn wait_for_group(
    coordinator_addr: &str,
    shard_id: u32,
    group_size: usize,
) -> BTreeMap<u64, BasicNode> {
    let endpoint = format!("http://{coordinator_addr}");
    loop {
        if let Ok(mut client) = CoordinatorClient::connect(endpoint.clone()).await {
            if let Ok(resp) = client.list_shard_members(ShardMembersRequest { shard_id }).await {
                let members = resp.into_inner().members;
                if members.len() >= group_size {
                    return members
                        .into_iter()
                        .map(|m| (raft_node_id(&m.node_id), BasicNode::new(m.address)))
                        .collect();
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Initialize the group if this member is the designated bootstrapper (smallest raft id).
/// An "already initialized" refusal is fine — someone else got there first (e.g. after a
/// restart into an existing group).
pub async fn bootstrap_group(raft: &Raft, my_raft_id: u64, members: BTreeMap<u64, BasicNode>) {
    let smallest = members.keys().min().copied();
    if smallest == Some(my_raft_id) {
        match raft.initialize(members).await {
            Ok(()) => println!("raft: initialized shard group as bootstrapper"),
            Err(e) => println!("raft: initialize skipped ({e})"),
        }
    }
}

/// Leadership-gated ingestion: whenever this member is the group's elected leader, poll the
/// source and quorum-commit each owned batch via `client_write`; otherwise stand by. Runs
/// forever (per-iteration waits bound the loop; shutdown ends the waits with errors, which
/// also stand by rather than spin).
pub async fn run_leader_ingestion<S: FlightSource>(
    source: S,
    raft: Raft,
    my_raft_id: u64,
    poll_interval: Duration,
    shard: Option<ShardAssignment>,
) {
    loop {
        // Wait (in bounded slices) until this node is the leader.
        let is_leader =
            { raft.metrics().borrow().current_leader == Some(my_raft_id) };
        if !is_leader {
            let _ = raft
                .wait(Some(Duration::from_secs(5)))
                .metrics(|m| m.current_leader == Some(my_raft_id), "await leadership")
                .await;
            continue;
        }

        println!("raft: leading — ingestion active");
        while raft.metrics().borrow().current_leader == Some(my_raft_id) {
            match source.fetch().await {
                Ok(batch) if !batch.is_empty() => {
                    let docs: Vec<_> = batch
                        .into_iter()
                        .filter(|d| shard.map_or(true, |a| a.owns(&d.icao24)))
                        .collect();
                    if !docs.is_empty() {
                        let payload = common::pb::ReplicateRequest { documents: docs, shard_id: 0 }
                            .encode_to_vec();
                        // A refused write (e.g. leadership just moved) drops this batch and
                        // re-enters the standby loop; the source re-observes next poll, and
                        // upserts make replays harmless.
                        if let Err(e) = raft.client_write(DocBatch(payload)).await {
                            println!("raft: write refused, standing down ({e})");
                            break;
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => eprintln!("ingest: fetch failed: {e}"),
            }
            tokio::time::sleep(poll_interval).await;
        }
        println!("raft: no longer leading — ingestion paused");
    }
}
