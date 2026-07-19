//! Consensus for a shard group, via `openraft`.
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
//! Consensus itself is `openraft`'s (deliberately not hand-rolled); this module supplies the
//! three integration points the library asks for: log storage, the state machine, and a
//! network transport (our gRPC).
//!
//! Note on group size: a group of 2 cannot survive any failure (a quorum of 2 is 2), so a
//! raft-managed shard runs at least 3 members — quorum 2 of 3 rides out one death.

pub mod bootstrap;
pub mod network;
pub mod service;
pub mod storage;

use std::io::Cursor;

use openraft::BasicNode;
use serde::{Deserialize, Serialize};

/// One log entry's payload: a prost-encoded `ReplicateRequest` (a batch of flight
/// documents). Kept as opaque bytes here because prost types don't speak serde; the state
/// machine decodes on apply. The encoding is already the replication wire format, so
/// snapshots and log entries share one codec.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocBatch(pub Vec<u8>);

/// The state machine's reply to an applied entry: how many documents were upserted.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Applied(pub u32);

openraft::declare_raft_types!(
    /// Raft type configuration for a shard group. Node ids are small integers assigned per
    /// group member; `BasicNode` carries the member's transport address.
    pub TypeConfig:
        D = DocBatch,
        R = Applied,
        Node = BasicNode,
        SnapshotData = Cursor<Vec<u8>>,
);

pub type NodeId = <TypeConfig as openraft::RaftTypeConfig>::NodeId;
pub type Raft = openraft::Raft<TypeConfig>;

/// Raft timing tuned for small, chatty LAN groups (and fast tests/demos): 100ms heartbeats,
/// elections after 300–600ms of silence.
pub fn raft_config() -> openraft::Config {
    openraft::Config {
        heartbeat_interval: 100,
        election_timeout_min: 300,
        election_timeout_max: 600,
        ..Default::default()
    }
}
