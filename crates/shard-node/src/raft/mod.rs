//! Consensus for a shard group.
//!
//! The members serving one shard (its leader and followers) form a **raft group**: the
//! stream of indexed document batches is the group's replicated log, and the shard's
//! `ShardStore` is the state machine the log is applied to. This replaces coordinator-driven
//! promotion for raft-managed shards with the real thing:
//!   - **leader election**: the group elects its own leader (terms + quorum votes); a node
//!     *knows* it leads rather than being told by a control plane;
//!   - **log replication**: a write is acknowledged only when a quorum has it — replication
//!     stops being best-effort;
//!   - **split-brain**: an old leader's writes can't commit without a quorum, and a stale
//!     term loses to a newer one by construction.
//!
//! The raft machinery itself (type config, WAL, transport, snapshot tiers) lives in the
//! `consensus` crate and is payload-agnostic; this module supplies the one shard-specific
//! piece — [`ShardApp`], which applies committed document batches to the `ShardStore` and
//! dumps/restores it for snapshots — plus group formation and membership reconciliation.
//!
//! Note on group size: a group of 2 cannot survive any failure (a quorum of 2 is 2), so a
//! raft-managed shard runs at least 3 members — quorum 2 of 3 rides out one death.

pub mod bootstrap;
pub mod eviction;
pub mod reconcile;

// The shared machinery, re-exported under the paths this crate has always used.
pub use consensus::Payload as DocBatch;
pub use consensus::{network, s3, service, wal};
pub use consensus::{raft_config, Applied, NodeId, Raft, TypeConfig};

/// The log/state-machine storage types, with the state machine pinned to this crate's
/// application (the shard's document store).
pub mod storage {
    pub use consensus::storage::LogStore;

    /// State machine over the shard's document store.
    pub type StateMachineStore = consensus::storage::StateMachineStore<super::ShardApp>;
}

use std::sync::{Arc, RwLock};

use prost::Message;

use crate::store::ShardStore;

/// The shard's state-machine application: committed payloads are prost-encoded
/// `ReplicateRequest`s (batches of flight documents), applied by upserting into the
/// searchable store. Kept as opaque bytes in the log because prost types don't speak
/// serde; the encoding is already the replication wire format, so snapshots and log
/// entries share one codec.
#[derive(Clone)]
pub struct ShardApp(pub Arc<RwLock<ShardStore>>);

impl From<Arc<RwLock<ShardStore>>> for ShardApp {
    fn from(store: Arc<RwLock<ShardStore>>) -> Self {
        Self(store)
    }
}

impl consensus::StateMachineApp for ShardApp {
    fn apply(&self, payload: &[u8]) -> u32 {
        use common::pb::shard_command::Kind;
        // Every log entry is a ShardCommand: index a batch, or evict a migrated vshard.
        // Both are applied by every member in log order, so members converge identically.
        let cmd = common::pb::ShardCommand::decode(payload).unwrap_or_default();
        let mut store = self.0.write().unwrap();
        match cmd.kind {
            Some(Kind::Batch(batch)) => {
                let count = batch.documents.len() as u32;
                for doc in batch.documents {
                    store.insert(doc);
                }
                count
            }
            Some(Kind::Evict(e)) => {
                let removed = store.evict_vshard(e.vshard, e.v) as u32;
                if removed > 0 {
                    println!("raft: evicted {removed} documents of vshard {} (migrated away)", e.vshard);
                }
                removed
            }
            None => 0,
        }
    }

    fn snapshot_bytes(&self) -> Vec<u8> {
        let docs = self.0.read().unwrap().documents();
        common::pb::ReplicateRequest { documents: docs, shard_id: 0 }.encode_to_vec()
    }

    fn restore(&self, bytes: &[u8]) -> u32 {
        let decoded = common::pb::ReplicateRequest::decode(bytes).unwrap_or_default();
        let count = decoded.documents.len() as u32;
        self.0.write().unwrap().replace_all(decoded.documents);
        count
    }

    fn unit(&self) -> &'static str {
        "documents"
    }
}
