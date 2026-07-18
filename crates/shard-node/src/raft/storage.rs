//! In-memory raft storage: the log store and the state machine.
//!
//! The state machine IS the shard's `ShardStore` — applying a committed entry means
//! upserting its documents, so a quorum-committed write is immediately searchable on every
//! member that applied it. Storage is in-memory to match the rest of the store (the shard
//! rebuilds from the live stream / snapshots on restart); durable log storage is a separate
//! step when persistence exists at all.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex, RwLock};

use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, OptionalSend, RaftLogReader, RaftSnapshotBuilder,
    SnapshotMeta, StorageError, StoredMembership, Vote,
};
use prost::Message;

use crate::store::ShardStore;

use super::{Applied, NodeId, TypeConfig};

// =============================================================================
// Log store
// =============================================================================

#[derive(Debug, Default)]
struct LogInner {
    log: BTreeMap<u64, Entry<TypeConfig>>,
    last_purged: Option<LogId<NodeId>>,
    committed: Option<LogId<NodeId>>,
    vote: Option<Vote<NodeId>>,
}

/// In-memory raft log. Cloneable handle over shared state (the reader is a clone).
#[derive(Debug, Clone, Default)]
pub struct LogStore {
    inner: Arc<Mutex<LogInner>>,
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.log.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.lock().unwrap();
        let last = inner
            .log
            .iter()
            .next_back()
            .map(|(_, e)| e.log_id)
            .or(inner.last_purged);
        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.inner.lock().unwrap().vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().unwrap().vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        self.inner.lock().unwrap().committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().unwrap().committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        {
            let mut inner = self.inner.lock().unwrap();
            for entry in entries {
                inner.log.insert(entry.log_id.index, entry);
            }
        }
        // In-memory "flush" is complete the moment it's inserted.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Remove everything at and after log_id (conflicting suffix on a follower).
        let mut inner = self.inner.lock().unwrap();
        let keys: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Drop everything up to and including log_id (compacted into a snapshot).
        let mut inner = self.inner.lock().unwrap();
        inner.last_purged = Some(log_id);
        let keys: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        Ok(())
    }
}

// =============================================================================
// State machine (the ShardStore)
// =============================================================================

#[derive(Debug, Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    /// prost-encoded `ReplicateRequest` holding the full document set.
    data: Vec<u8>,
}

#[derive(Default)]
struct SmInner {
    last_applied: Option<LogId<NodeId>>,
    membership: StoredMembership<NodeId, BasicNode>,
    snapshot_seq: u64,
    snapshot: Option<StoredSnapshot>,
}

/// State machine over the shard's document store. Cheap-clone handle (`Arc` inside), so the
/// same instance serves raft and the search path.
#[derive(Clone)]
pub struct StateMachineStore {
    store: Arc<RwLock<ShardStore>>,
    inner: Arc<Mutex<SmInner>>,
}

impl StateMachineStore {
    pub fn new(store: Arc<RwLock<ShardStore>>) -> Self {
        Self {
            store,
            inner: Arc::new(Mutex::new(SmInner::default())),
        }
    }

    fn encode_all_docs(&self) -> Vec<u8> {
        let docs = self.store.read().unwrap().documents();
        common::pb::ReplicateRequest { documents: docs, shard_id: 0 }.encode_to_vec()
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let data = self.encode_all_docs();
        let mut inner = self.inner.lock().unwrap();
        inner.snapshot_seq += 1;
        let meta = SnapshotMeta {
            last_log_id: inner.last_applied,
            last_membership: inner.membership.clone(),
            snapshot_id: format!("snap-{}", inner.snapshot_seq),
        };
        inner.snapshot = Some(StoredSnapshot { meta: meta.clone(), data: data.clone() });
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let inner = self.inner.lock().unwrap();
        Ok((inner.last_applied, inner.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Applied>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        let mut replies = Vec::new();
        for entry in entries {
            let reply = match entry.payload {
                EntryPayload::Blank => Applied(0),
                EntryPayload::Normal(batch) => {
                    // A committed batch: decode and upsert into the searchable store.
                    let decoded = common::pb::ReplicateRequest::decode(batch.0.as_slice())
                        .unwrap_or_default();
                    let count = decoded.documents.len() as u32;
                    let mut store = self.store.write().unwrap();
                    for doc in decoded.documents {
                        store.insert(doc);
                    }
                    Applied(count)
                }
                EntryPayload::Membership(ref membership) => {
                    let mut inner = self.inner.lock().unwrap();
                    inner.membership =
                        StoredMembership::new(Some(entry.log_id), membership.clone());
                    Applied(0)
                }
            };
            self.inner.lock().unwrap().last_applied = Some(entry.log_id);
            replies.push(reply);
        }
        Ok(replies)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let decoded = common::pb::ReplicateRequest::decode(data.as_slice()).unwrap_or_default();
        self.store.write().unwrap().replace_all(decoded.documents);
        let mut inner = self.inner.lock().unwrap();
        inner.last_applied = meta.last_log_id;
        inner.membership = meta.last_membership.clone();
        inner.snapshot = Some(StoredSnapshot { meta: meta.clone(), data });
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.snapshot.as_ref().map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: Box::new(Cursor::new(s.data.clone())),
        }))
    }
}
