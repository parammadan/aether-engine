//! Membership reconciliation: the elected leader admits newly registered members into the
//! raft group, live.
//!
//! The coordinator's member list is *intent* (who registered for this shard); raft
//! membership is *authority* (who consensus actually includes). The leader periodically
//! diffs the two and closes the gap in the standard two-step:
//!   1. `add_learner` — the newcomer starts receiving the log (or a snapshot) immediately,
//!      catching up in the background with zero impact on quorum;
//!   2. `change_membership` to voter — once caught up, it starts counting toward quorum.
//! Both steps are leader-only, idempotent across ticks (a failed promotion is retried on
//! the next tick because the node shows up as learner-but-not-voter), and never remove
//! anyone: removal is a deliberate operation, not something to infer from a liveness view —
//! a flapping heartbeat must not be able to shrink quorum.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use openraft::{BasicNode, ChangeMembers};

use common::pb::coordinator_client::CoordinatorClient;
use common::pb::ShardMembersRequest;

use super::bootstrap::raft_node_id;
use super::Raft;

/// Run forever on every member; acts only while this member is the leader.
pub async fn run_membership_reconciler(
    raft: Raft,
    my_raft_id: u64,
    coordinator_addr: String,
    shard_id: u32,
) {
    let endpoint = format!("http://{coordinator_addr}");
    let mut ticker = tokio::time::interval(Duration::from_secs(3));
    loop {
        ticker.tick().await;
        if raft.metrics().borrow().current_leader != Some(my_raft_id) {
            continue;
        }

        // Intent: who has registered for this shard.
        let Ok(mut client) = CoordinatorClient::connect(endpoint.clone()).await else {
            continue;
        };
        let Ok(resp) = client.list_shard_members(ShardMembersRequest { shard_id }).await else {
            continue;
        };
        let members = resp.into_inner().members;
        let draining: BTreeSet<u64> = members
            .iter()
            .filter(|m| m.draining)
            .map(|m| raft_node_id(&m.node_id))
            .collect();
        let desired: BTreeMap<u64, String> = members
            .into_iter()
            .filter(|m| !m.draining) // a draining node is never (re-)added
            .map(|m| (raft_node_id(&m.node_id), m.address))
            .collect();

        // Authority: who consensus currently includes, and at what standing.
        let (in_group, voters): (BTreeSet<u64>, BTreeSet<u64>) = {
            let metrics = raft.metrics().borrow().clone();
            let membership = metrics.membership_config.membership().clone();
            (
                membership.nodes().map(|(id, _)| *id).collect(),
                membership.voter_ids().collect(),
            )
        };

        // Execute deliberate removals (drains) — with a hard floor: never shrink the voter
        // set below 3, because a 2-voter group cannot survive any failure. Removing a
        // learner is always safe (it never counted toward quorum).
        for id in &draining {
            if !in_group.contains(id) {
                continue; // already out; the operator can stop the process
            }
            if voters.contains(id) {
                if voters.len() <= 3 {
                    println!("reconciler: refusing to drain a voter below 3 voters");
                    continue;
                }
                println!("reconciler: removing drained voter from the group");
                if let Err(e) = raft
                    .change_membership(ChangeMembers::RemoveVoters([*id].into()), false)
                    .await
                {
                    println!("reconciler: removal pending (will retry): {e}");
                }
            } else {
                println!("reconciler: removing drained learner from the group");
                if let Err(e) = raft
                    .change_membership(ChangeMembers::RemoveNodes([*id].into()), false)
                    .await
                {
                    println!("reconciler: learner removal pending (will retry): {e}");
                }
            }
        }

        for (id, addr) in &desired {
            if !in_group.contains(id) {
                // Step 1: admit as learner — starts catch-up, costs quorum nothing.
                println!("reconciler: admitting {addr} as learner");
                if let Err(e) = raft.add_learner(*id, BasicNode::new(addr), true).await {
                    println!("reconciler: add_learner failed (will retry): {e}");
                    continue;
                }
            }
            if in_group.contains(id) && !voters.contains(id) {
                // Step 2: promote to voter once membership knows it (retries while it is
                // still catching up — openraft refuses to promote a lagging learner).
                println!("reconciler: promoting learner to voter");
                if let Err(e) = raft
                    .change_membership(ChangeMembers::AddVoterIds([*id].into()), false)
                    .await
                {
                    println!("reconciler: promotion pending (will retry): {e}");
                }
            }
        }
    }
}
