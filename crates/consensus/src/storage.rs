//! Raft storage: the in-memory log store and the generic snapshotting state machine.
//!
//! The state machine is generic over a [`StateMachineApp`]: applying a committed entry
//! hands the payload bytes to the application, so a quorum-committed write is visible in
//! the application's state the moment it applies. The snapshot machinery (in-memory copy,
//! local dir with atomic rename, optional S3 tier) is entirely application-agnostic —
//! snapshots are whatever bytes the application dumps.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, OptionalSend, RaftLogReader, RaftSnapshotBuilder,
    SnapshotMeta, StorageError, StoredMembership, Vote,
};

use crate::{Applied, NodeId, StateMachineApp, TypeConfig};

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
// State machine (generic over the application)
// =============================================================================

#[derive(Debug, Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    /// Application-encoded full-state dump.
    data: Vec<u8>,
}

#[derive(Default)]
struct SmInner {
    last_applied: Option<LogId<NodeId>>,
    membership: StoredMembership<NodeId, BasicNode>,
    snapshot_seq: u64,
    snapshot: Option<StoredSnapshot>,
}

/// State machine over an application's state. Cheap-clone handle (`Arc` inside), so the
/// same instance serves raft and the application's read path.
#[derive(Clone)]
pub struct StateMachineStore<A: StateMachineApp> {
    app: A,
    inner: Arc<Mutex<SmInner>>,
    /// When set, snapshots are persisted here (tmp -> atomic rename) and the latest one is
    /// loaded at construction — restart = latest snapshot + WAL tail replay.
    snap_dir: Option<std::path::PathBuf>,
    /// Optional S3 tier: snapshots are uploaded after they are built, and a cold boot with
    /// no local state fetches the newest object. Local disk owns the log; S3 owns recovery
    /// points (it has no fsync semantics, so it is exactly wrong for a WAL).
    s3: Option<Arc<crate::s3::SnapshotS3>>,
}

/// On-disk snapshot format: the raft meta (last applied id + membership) plus the
/// application's state dump. Bincode-framed as one file, replaced atomically.
#[derive(serde::Serialize, serde::Deserialize)]
struct SnapshotFile {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

impl<A: StateMachineApp> StateMachineStore<A> {
    pub fn new(app: impl Into<A>) -> Self {
        Self {
            app: app.into(),
            inner: Arc::new(Mutex::new(SmInner::default())),
            snap_dir: None,
            s3: None,
        }
    }

    /// Durable variant: persists every snapshot under `dir` and, at construction, restores
    /// the newest one (state into the application, last-applied + membership into the state
    /// machine) so openraft resumes log replay from the snapshot point.
    pub fn with_snapshot_dir(
        app: impl Into<A>,
        dir: std::path::PathBuf,
    ) -> std::io::Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let sm = Self {
            app: app.into(),
            inner: Arc::new(Mutex::new(SmInner::default())),
            snap_dir: Some(dir.clone()),
            s3: None,
        };
        if let Some((meta, data)) = Self::load_latest(&dir)? {
            let restored = sm.app.restore(&data);
            let mut inner = sm.inner.lock().unwrap();
            inner.last_applied = meta.last_log_id;
            inner.membership = meta.last_membership.clone();
            inner.snapshot = Some(StoredSnapshot { meta, data });
            println!("snapshot: restored {restored} {} from disk", sm.app.unit());
        }
        Ok(sm)
    }

    /// Fully durable open: local snapshot restore first; if the local tier is empty and an
    /// S3 tier is configured, cold-boot from the newest object (and re-persist it locally).
    /// Chain: local -> S3 -> empty.
    pub async fn open_durable(
        app: impl Into<A>,
        dir: std::path::PathBuf,
        s3: Option<Arc<crate::s3::SnapshotS3>>,
    ) -> std::io::Result<Self> {
        let mut sm = Self::with_snapshot_dir(app, dir)?;
        sm.s3 = s3;

        let has_local = sm.inner.lock().unwrap().snapshot.is_some();
        if !has_local {
            if let Some(s3) = sm.s3.clone() {
                if let Some(bytes) = s3.latest().await {
                    match bincode::deserialize::<SnapshotFile>(&bytes) {
                        Ok(file) => {
                            let restored = sm.app.restore(&file.data);
                            {
                                let mut inner = sm.inner.lock().unwrap();
                                inner.last_applied = file.meta.last_log_id;
                                inner.membership = file.meta.last_membership.clone();
                                inner.snapshot = Some(StoredSnapshot {
                                    meta: file.meta.clone(),
                                    data: file.data.clone(),
                                });
                            }
                            sm.persist(&file.meta, &file.data); // re-seed the local tier
                            println!("snapshot: cold-booted {restored} {} from s3", sm.app.unit());
                        }
                        Err(e) => eprintln!("s3: undecodable snapshot ignored: {e}"),
                    }
                }
            }
        }
        Ok(sm)
    }

    fn snapshot_path(dir: &std::path::Path, index: u64) -> std::path::PathBuf {
        dir.join(format!("snap-{index:020}.bin"))
    }

    /// Persist a snapshot atomically (tmp -> fsync -> rename) and drop older ones.
    fn persist(&self, meta: &SnapshotMeta<NodeId, BasicNode>, data: &[u8]) {
        let Some(dir) = &self.snap_dir else { return };
        let index = meta.last_log_id.map(|l| l.index).unwrap_or(0);
        let path = Self::snapshot_path(dir, index);
        let tmp = path.with_extension("tmp");
        let file = SnapshotFile { meta: meta.clone(), data: data.to_vec() };
        let bytes = bincode::serialize(&file).expect("snapshot serializes");
        let write = (|| -> std::io::Result<()> {
            std::fs::write(&tmp, &bytes)?;
            let f = std::fs::File::open(&tmp)?;
            f.sync_data()?;
            std::fs::rename(&tmp, &path)?;
            Ok(())
        })();
        match write {
            Ok(()) => {
                // Older snapshots are superseded; keep only the newest on disk.
                if let Ok(entries) = std::fs::read_dir(dir) {
                    for e in entries.flatten() {
                        let name = e.file_name().to_string_lossy().into_owned();
                        if name.starts_with("snap-") && name.ends_with(".bin") && e.path() != path {
                            let _ = std::fs::remove_file(e.path());
                        }
                    }
                }
            }
            Err(e) => eprintln!("snapshot: persist failed (kept in memory): {e}"),
        }
    }

    fn load_latest(
        dir: &std::path::Path,
    ) -> std::io::Result<Option<(SnapshotMeta<NodeId, BasicNode>, Vec<u8>)>> {
        let mut newest: Option<(u64, std::path::PathBuf)> = None;
        for e in std::fs::read_dir(dir)?.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if let Some(idx) = name.strip_prefix("snap-").and_then(|s| s.strip_suffix(".bin")) {
                if let Ok(idx) = idx.parse::<u64>() {
                    if newest.as_ref().map_or(true, |(n, _)| idx > *n) {
                        newest = Some((idx, e.path()));
                    }
                }
            }
        }
        let Some((_, path)) = newest else { return Ok(None) };
        let bytes = std::fs::read(path)?;
        match bincode::deserialize::<SnapshotFile>(&bytes) {
            Ok(f) => Ok(Some((f.meta, f.data))),
            Err(e) => {
                eprintln!("snapshot: unreadable snapshot ignored: {e}");
                Ok(None)
            }
        }
    }
}

impl<A: StateMachineApp> RaftSnapshotBuilder<TypeConfig> for StateMachineStore<A> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let data = self.app.snapshot_bytes();
        // Lexically scoped lock: the guard must be provably gone before the S3 await below.
        let meta = {
            let mut inner = self.inner.lock().unwrap();
            inner.snapshot_seq += 1;
            let meta = SnapshotMeta {
                last_log_id: inner.last_applied,
                last_membership: inner.membership.clone(),
                snapshot_id: format!("snap-{}", inner.snapshot_seq),
            };
            inner.snapshot = Some(StoredSnapshot { meta: meta.clone(), data: data.clone() });
            meta
        };
        self.persist(&meta, &data);
        if let Some(s3) = &self.s3 {
            let index = meta.last_log_id.map(|l| l.index).unwrap_or(0);
            let file = SnapshotFile { meta: meta.clone(), data: data.clone() };
            let bytes = bincode::serialize(&file).expect("snapshot serializes");
            s3.upload(&format!("snap-{index:020}.bin"), bytes).await;
        }
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl<A: StateMachineApp> RaftStateMachine<TypeConfig> for StateMachineStore<A> {
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
                EntryPayload::Normal(ref payload) => {
                    // A committed payload: the application decodes and applies it.
                    Applied(self.app.apply(&payload.0))
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
        self.app.restore(&data);
        let mut inner = self.inner.lock().unwrap();
        inner.last_applied = meta.last_log_id;
        inner.membership = meta.last_membership.clone();
        inner.snapshot = Some(StoredSnapshot { meta: meta.clone(), data: data.clone() });
        drop(inner);
        self.persist(meta, &data);
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
