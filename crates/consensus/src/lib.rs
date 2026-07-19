//! Reusable raft machinery, via `openraft`.
//!
//! Consensus doesn't care what it replicates. Everything here works over **opaque
//! payload bytes**: the type config, the in-memory and durable (WAL) log stores, the
//! gRPC transport, the snapshotting state machine, and the S3 snapshot tier. An
//! application — a shard's document store, the control plane's placement table —
//! supplies one thing: a [`StateMachineApp`], which says how to apply a committed
//! payload to its state and how to dump/restore that state as bytes for snapshots.
//!
//! Consensus itself is `openraft`'s (deliberately not hand-rolled); this crate supplies
//! the integration points the library asks for, once, so every raft group in the system
//! shares one tested implementation of the parts that are easy to get subtly wrong
//! (fsync ordering, torn-tail recovery, snapshot atomicity).

pub mod network;
pub mod s3;
pub mod service;
pub mod storage;
pub mod wal;

use std::io::Cursor;

use openraft::BasicNode;
use serde::{Deserialize, Serialize};

/// One log entry's payload: opaque bytes. The application encodes its own commands and
/// decodes them on apply — consensus never looks inside, which is exactly what makes
/// this machinery reusable across state machines.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Payload(pub Vec<u8>);

/// The state machine's reply to an applied entry: an application-defined count (e.g.
/// how many records the payload upserted).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Applied(pub u32);

openraft::declare_raft_types!(
    /// Raft type configuration shared by every group in the system. Node ids are small
    /// integers assigned per group member; `BasicNode` carries the member's transport
    /// address, so addressing lives in the raft membership itself.
    pub TypeConfig:
        D = Payload,
        R = Applied,
        Node = BasicNode,
        SnapshotData = Cursor<Vec<u8>>,
);

pub type NodeId = <TypeConfig as openraft::RaftTypeConfig>::NodeId;
pub type Raft = openraft::Raft<TypeConfig>;

/// What an application supplies to run behind the generic state machine. All three
/// methods speak bytes; the application owns its own codec (and its own locking — a
/// handle here is typically an `Arc` over the real state).
pub trait StateMachineApp: Clone + Send + Sync + 'static {
    /// Apply one committed payload to the state; return the applied-record count.
    fn apply(&self, payload: &[u8]) -> u32;
    /// Dump the full state as snapshot bytes.
    fn snapshot_bytes(&self) -> Vec<u8>;
    /// Replace the full state from snapshot bytes; return the restored-record count.
    fn restore(&self, bytes: &[u8]) -> u32;
    /// The noun for log lines ("restored N <unit> from disk").
    fn unit(&self) -> &'static str {
        "entries"
    }
}

/// Raft timing tuned for small, chatty LAN groups (and fast tests/demos): 100ms
/// heartbeats, elections after 300–600ms of silence.
pub fn raft_config() -> openraft::Config {
    let mut config = openraft::Config {
        heartbeat_interval: 100,
        election_timeout_min: 300,
        election_timeout_max: 600,
        ..Default::default()
    };
    // Snapshot cadence (log entries between snapshots) — smaller values bound WAL growth
    // and speed cold recovery at the cost of more frequent snapshot writes.
    if let Some(n) = std::env::var("AETHER_SNAPSHOT_LOGS").ok().and_then(|s| s.parse::<u64>().ok()) {
        if n > 0 {
            config.snapshot_policy = openraft::SnapshotPolicy::LogsSinceLast(n);
        }
    }
    config
}
