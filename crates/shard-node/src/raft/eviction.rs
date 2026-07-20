//! Vshard eviction watcher: the group's leader proposes eviction of virtual shards that
//! have migrated AWAY from this group, so stale copies don't linger forever after a move.
//!
//! Eviction rides the raft log (an `EvictVShard` command), never a local decision — every
//! member applies the same eviction at the same log position, so replicas converge
//! identically. The watcher runs on every member but acts only while leader; it is
//! self-healing (it proposes for any not-owned vshard it still holds documents for, so a
//! missed transition is caught on the next tick) and idempotent (evicting an
//! already-empty vshard removes zero documents).

use std::sync::{Arc, RwLock};
use std::time::Duration;

use prost::Message;

use crate::store::ShardStore;
use super::{DocBatch, Raft};

/// Run forever on every member; proposes vshard evictions only while this member leads.
pub async fn run_eviction_watcher(
    raft: Raft,
    my_raft_id: u64,
    group: u32,
    assignments: Arc<RwLock<Vec<u32>>>,
    store: Arc<RwLock<ShardStore>>,
) {
    let mut ticker = tokio::time::interval(Duration::from_secs(3));
    loop {
        ticker.tick().await;
        if raft.metrics().borrow().current_leader != Some(my_raft_id) {
            continue;
        }

        let table = assignments.read().expect("assignments lock poisoned").clone();
        let v = table.len() as u32;
        if v == 0 {
            continue;
        }

        // Which virtual shards does this group still hold documents for?
        let held = {
            let store = store.read().expect("store lock poisoned");
            let mut held = std::collections::BTreeSet::new();
            if let Some(v) = std::num::NonZeroU32::new(v) {
                for doc in store.documents() {
                    held.insert(common::shard::vshard_for(&doc.icao24, v));
                }
            }
            held
        };

        // Evict any held vshard the table no longer assigns to us.
        for vshard in held {
            let owned_by_us = table.get(vshard as usize).copied() == Some(group);
            if owned_by_us {
                continue;
            }
            let payload = common::pb::ShardCommand {
                kind: Some(common::pb::shard_command::Kind::Evict(common::pb::EvictVShard {
                    vshard,
                    v,
                })),
            }
            .encode_to_vec();
            if let Err(e) = raft.client_write(DocBatch(payload)).await {
                // Lost leadership or a transient refusal: stop; the next leader's watcher
                // (or our next tick) retries. Eviction being idempotent makes this safe.
                println!("raft: eviction proposal refused, standing down ({e})");
                break;
            }
        }
    }
}
