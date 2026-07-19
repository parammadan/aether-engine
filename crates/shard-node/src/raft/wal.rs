//! Durable raft storage: a write-ahead log on local disk, plus a separately-fsynced vote
//! file.
//!
//! # Why this exists
//! An in-memory raft log is not just a data-loss risk — it is a **correctness hole**:
//! Raft's safety proof assumes votes and log entries survive restarts. A member that
//! forgets its vote can vote twice in one term and help elect two leaders. So the log is
//! a precondition, not a feature; RAM stays the read path, disk is the truth.
//!
//! # Design: log-structured records, marker-based mutation
//! The WAL is a sequence of segments (`seg-{seq:016}.wal`). Every record is framed as
//! `[u32 len][u32 crc32(payload)][payload]`, payload = bincode of a [`WalRecord`]:
//!
//!   - `Entry`      — a raft log entry
//!   - `TruncateFrom(i)` — conflict-suffix removal: entries with index ≥ i are void
//!   - `Purge(log_id)`   — snapshot compaction point: entries with index ≤ i are void
//!   - `Committed(..)`   — the committed pointer (small, appended per commit advance)
//!   - `Checkpoint {..}` — written at the head of every new segment so any whole-prefix
//!                          of segments can be deleted safely
//!
//! Mutations are **appends with markers**, never in-place file surgery: replay folds the
//! record stream into state, so a truncate is one fsync'd append instead of a rewrite.
//! Physical space is reclaimed at segment granularity — a closed segment whose entries
//! all sit at or below the purge point is unlinked, which is safe because every later
//! segment begins with a checkpoint carrying the surviving pointers.
//!
//! # Durability contract
//! `append` fsyncs **before** the raft flush callback fires (`LogFlushed` is the promise
//! openraft builds on); truncate/purge/vote fsync before returning. The vote lives in its
//! own tiny file written tmp → fsync → atomic rename, read at boot.
//!
//! # Recovery
//! Boot scans segments in order, validating each frame's CRC. The first bad, torn, or
//! undecodable record ends the scan: the file is truncated back to the last good offset
//! and any later segments are removed — recovering the longest clean prefix, which is
//! exactly what a crash mid-write can leave behind.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use openraft::{Entry, LogId, Vote};
use serde::{Deserialize, Serialize};

use super::{NodeId, TypeConfig};

const SEGMENT_MAX_BYTES: u64 = 8 * 1024 * 1024;
const FRAME_HEADER: usize = 8; // u32 len + u32 crc

#[derive(Serialize, Deserialize)]
enum WalRecord {
    Entry(Entry<TypeConfig>),
    TruncateFrom(u64),
    Purge(LogId<NodeId>),
    Committed(Option<LogId<NodeId>>),
    Checkpoint {
        last_purged: Option<LogId<NodeId>>,
        committed: Option<LogId<NodeId>>,
    },
}

/// The folded, in-memory view of the record stream (RAM is the read path).
#[derive(Default)]
pub(crate) struct WalState {
    pub entries: BTreeMap<u64, Entry<TypeConfig>>,
    pub last_purged: Option<LogId<NodeId>>,
    pub committed: Option<LogId<NodeId>>,
}

impl WalState {
    fn apply(&mut self, record: WalRecord) {
        match record {
            WalRecord::Entry(e) => {
                self.entries.insert(e.log_id.index, e);
            }
            WalRecord::TruncateFrom(index) => {
                let keys: Vec<u64> = self.entries.range(index..).map(|(k, _)| *k).collect();
                for k in keys {
                    self.entries.remove(&k);
                }
            }
            WalRecord::Purge(log_id) => {
                self.last_purged = Some(log_id);
                let keys: Vec<u64> = self.entries.range(..=log_id.index).map(|(k, _)| *k).collect();
                for k in keys {
                    self.entries.remove(&k);
                }
            }
            WalRecord::Committed(c) => self.committed = c,
            WalRecord::Checkpoint { last_purged, committed } => {
                // A checkpoint restates pointers for prefix-GC safety; it never rewinds
                // state that later records in this same segment will refine.
                if self.last_purged.is_none() {
                    self.last_purged = last_purged;
                }
                if self.committed.is_none() {
                    self.committed = committed;
                }
            }
        }
    }
}

/// One open segment being appended to.
struct Segment {
    file: File,
    path: PathBuf,
    seq: u64,
    bytes: u64,
    /// Highest entry index ever written into this segment (for purge-time GC).
    max_index: Option<u64>,
}

/// The write-ahead log: an append-only record stream with fsync-before-acknowledge.
pub struct Wal {
    dir: PathBuf,
    segment: Segment,
    /// Closed segments (seq, path, max entry index) eligible for purge-time GC.
    closed: Vec<(u64, PathBuf, Option<u64>)>,
    pub(crate) state: WalState,
}

fn segment_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("seg-{seq:016}.wal"))
}

fn frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(FRAME_HEADER + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
    out.extend_from_slice(payload);
    out
}

impl Wal {
    /// Open (or create) the WAL in `dir`, recovering the longest clean record prefix.
    pub fn open(dir: &Path) -> std::io::Result<Self> {
        fs::create_dir_all(dir)?;

        // Discover segments in sequence order.
        let mut seqs: Vec<u64> = fs::read_dir(dir)?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name().to_string_lossy().into_owned();
                name.strip_prefix("seg-")?.strip_suffix(".wal")?.parse::<u64>().ok()
            })
            .collect();
        seqs.sort_unstable();

        let mut state = WalState::default();
        let mut closed = Vec::new();
        let mut recovered_records = 0usize;
        let mut corrupt_at: Option<(u64, u64)> = None; // (seq, good_offset)

        for &seq in &seqs {
            let path = segment_path(dir, seq);
            let bytes = fs::read(&path)?;
            let mut offset = 0usize;
            let mut max_index = None;
            loop {
                if offset + FRAME_HEADER > bytes.len() {
                    if offset != bytes.len() {
                        corrupt_at = Some((seq, offset as u64)); // torn header
                    }
                    break;
                }
                let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
                let crc = u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap());
                let start = offset + FRAME_HEADER;
                if start + len > bytes.len() {
                    corrupt_at = Some((seq, offset as u64)); // torn payload
                    break;
                }
                let payload = &bytes[start..start + len];
                if crc32fast::hash(payload) != crc {
                    corrupt_at = Some((seq, offset as u64)); // bit rot / partial overwrite
                    break;
                }
                let Ok(record) = bincode::deserialize::<WalRecord>(payload) else {
                    corrupt_at = Some((seq, offset as u64));
                    break;
                };
                if let WalRecord::Entry(e) = &record {
                    max_index = Some(max_index.map_or(e.log_id.index, |m: u64| m.max(e.log_id.index)));
                }
                state.apply(record);
                recovered_records += 1;
                offset = start + len;
            }
            closed.push((seq, path, max_index));
            if corrupt_at.is_some() {
                break;
            }
        }

        // Clean-prefix recovery: truncate the corrupt segment at its last good offset and
        // drop anything after it — a crash mid-write can only ever dirty the tail.
        if let Some((bad_seq, good_offset)) = corrupt_at {
            let path = segment_path(dir, bad_seq);
            let f = OpenOptions::new().write(true).open(&path)?;
            f.set_len(good_offset)?;
            f.sync_data()?;
            for &seq in seqs.iter().filter(|&&s| s > bad_seq) {
                let _ = fs::remove_file(segment_path(dir, seq));
            }
            closed.retain(|(s, _, _)| *s <= bad_seq);
            println!("wal: recovered clean prefix (truncated seg {bad_seq} at {good_offset})");
        }

        // The highest existing segment becomes the active one; none -> start at 0.
        let active_seq = seqs.last().copied().filter(|s| {
            corrupt_at.map_or(true, |(bad, _)| *s <= bad)
        });
        let (seq, path) = match active_seq {
            Some(s) => (s, segment_path(dir, s)),
            None => (0, segment_path(dir, 0)),
        };
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let bytes = file.metadata()?.len();
        let max_index = closed
            .iter()
            .find(|(s, _, _)| *s == seq)
            .and_then(|(_, _, m)| *m);
        closed.retain(|(s, _, _)| *s != seq);

        println!("wal: recovered {recovered_records} records across {} segment(s)", seqs.len().max(1));

        Ok(Self {
            dir: dir.to_path_buf(),
            segment: Segment { file, path, seq, bytes, max_index },
            closed,
            state,
        })
    }

    /// Append one record. NOT yet durable — call [`Wal::sync`] before acknowledging.
    fn append(&mut self, record: &WalRecord) -> std::io::Result<()> {
        let payload = bincode::serialize(record).expect("wal records always serialize");
        let framed = frame(&payload);
        self.segment.file.write_all(&framed)?;
        self.segment.bytes += framed.len() as u64;
        if let WalRecord::Entry(e) = record {
            self.segment.max_index =
                Some(self.segment.max_index.map_or(e.log_id.index, |m| m.max(e.log_id.index)));
        }
        if self.segment.bytes >= SEGMENT_MAX_BYTES {
            self.rotate()?;
        }
        Ok(())
    }

    /// Make everything appended so far durable. This is the promise the raft flush
    /// callback (and every mutating return) stands on.
    fn sync(&mut self) -> std::io::Result<()> {
        self.segment.file.sync_data()
    }

    /// Close the current segment and start the next, headed by a checkpoint record so any
    /// whole prefix of closed segments can later be deleted safely.
    fn rotate(&mut self) -> std::io::Result<()> {
        self.segment.file.sync_data()?;
        let finished = (
            self.segment.seq,
            self.segment.path.clone(),
            self.segment.max_index,
        );
        self.closed.push(finished);

        let seq = self.segment.seq + 1;
        let path = segment_path(&self.dir, seq);
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        self.segment = Segment { file, path, seq, bytes: 0, max_index: None };

        let checkpoint = WalRecord::Checkpoint {
            last_purged: self.state.last_purged,
            committed: self.state.committed,
        };
        self.append(&checkpoint)?;
        Ok(())
    }

    // ---- public mutations (each folds into state, appends, and syncs) ----

    pub fn append_entries(&mut self, entries: Vec<Entry<TypeConfig>>) -> std::io::Result<()> {
        for entry in entries {
            self.append(&WalRecord::Entry(entry.clone()))?;
            self.state.apply(WalRecord::Entry(entry));
        }
        self.sync()
    }

    pub fn truncate_from(&mut self, index: u64) -> std::io::Result<()> {
        self.append(&WalRecord::TruncateFrom(index))?;
        self.state.apply(WalRecord::TruncateFrom(index));
        self.sync()
    }

    pub fn purge(&mut self, log_id: LogId<NodeId>) -> std::io::Result<()> {
        self.append(&WalRecord::Purge(log_id))?;
        self.state.apply(WalRecord::Purge(log_id));
        self.sync()?;
        // Reclaim closed segments wholly at-or-below the purge point. Safe: every later
        // segment opens with a checkpoint restating the surviving pointers.
        self.closed.retain(|(_, path, max_index)| {
            let removable = max_index.map_or(true, |m| m <= log_id.index);
            if removable {
                let _ = fs::remove_file(path);
            }
            !removable
        });
        Ok(())
    }

    pub fn save_committed(&mut self, committed: Option<LogId<NodeId>>) -> std::io::Result<()> {
        self.append(&WalRecord::Committed(committed))?;
        self.state.apply(WalRecord::Committed(committed));
        self.sync()
    }
}

// =============================================================================
// Vote file — tiny, separate, atomically replaced
// =============================================================================

/// Persist the vote: write tmp → fsync → atomic rename. A vote that isn't durable before
/// we answer a candidate is a double-vote waiting to happen.
pub fn save_vote(path: &Path, vote: &Vote<NodeId>) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    let bytes = bincode::serialize(vote).expect("vote always serializes");
    let mut f = File::create(&tmp)?;
    f.write_all(&bytes)?;
    f.sync_data()?;
    fs::rename(&tmp, path)?;
    // Best-effort directory sync so the rename itself is durable where supported.
    if let Some(dir) = path.parent() {
        if let Ok(d) = File::open(dir) {
            let _ = d.sync_all();
        }
    }
    Ok(())
}

pub fn read_vote(path: &Path) -> std::io::Result<Option<Vote<NodeId>>> {
    match fs::read(path) {
        Ok(bytes) => Ok(bincode::deserialize(&bytes).ok()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::{CommittedLeaderId, EntryPayload};

    fn dir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("aether-wal-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        d
    }

    fn entry(index: u64) -> Entry<TypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Blank,
        }
    }

    #[test]
    fn append_survives_reopen() {
        let d = dir("reopen");
        {
            let mut wal = Wal::open(&d).unwrap();
            wal.append_entries(vec![entry(1), entry(2), entry(3)]).unwrap();
            wal.save_committed(Some(entry(2).log_id)).unwrap();
        }
        let wal = Wal::open(&d).unwrap();
        assert_eq!(wal.state.entries.len(), 3);
        assert_eq!(wal.state.committed.unwrap().index, 2);
    }

    #[test]
    fn truncate_and_purge_markers_fold_across_reopen() {
        let d = dir("fold");
        {
            let mut wal = Wal::open(&d).unwrap();
            wal.append_entries((1..=10).map(entry).collect()).unwrap();
            wal.truncate_from(8).unwrap(); // conflict suffix 8..10 void
            wal.purge(entry(3).log_id).unwrap(); // ..=3 compacted
        }
        let wal = Wal::open(&d).unwrap();
        let keys: Vec<u64> = wal.state.entries.keys().copied().collect();
        assert_eq!(keys, vec![4, 5, 6, 7]);
        assert_eq!(wal.state.last_purged.unwrap().index, 3);
    }

    #[test]
    fn torn_tail_recovers_longest_clean_prefix() {
        let d = dir("torn");
        {
            let mut wal = Wal::open(&d).unwrap();
            wal.append_entries((1..=5).map(entry).collect()).unwrap();
        }
        // Simulate a crash mid-write: chop bytes off the end of the segment.
        let seg = segment_path(&d, 0);
        let len = fs::metadata(&seg).unwrap().len();
        let f = OpenOptions::new().write(true).open(&seg).unwrap();
        f.set_len(len - 7).unwrap(); // tears the last record

        let wal = Wal::open(&d).unwrap();
        assert_eq!(wal.state.entries.len(), 4, "clean prefix = first four entries");
        // And the WAL must be appendable again after recovery.
        let mut wal = wal;
        wal.append_entries(vec![entry(5)]).unwrap();
        drop(wal);
        assert_eq!(Wal::open(&d).unwrap().state.entries.len(), 5);
    }

    #[test]
    fn corrupted_byte_ends_the_prefix_at_the_bad_record() {
        let d = dir("crc");
        {
            let mut wal = Wal::open(&d).unwrap();
            wal.append_entries((1..=3).map(entry).collect()).unwrap();
        }
        // Flip one byte in the middle record's payload region.
        let seg = segment_path(&d, 0);
        let mut bytes = fs::read(&seg).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;
        fs::write(&seg, &bytes).unwrap();

        let wal = Wal::open(&d).unwrap();
        assert!(wal.state.entries.len() < 3, "entries at/after the corruption are gone");
    }

    #[test]
    fn vote_file_roundtrip_and_absence() {
        let d = dir("vote");
        let path = d.join("vote.bin");
        assert!(read_vote(&path).unwrap().is_none());
        let vote = Vote::new(7, 42);
        save_vote(&path, &vote).unwrap();
        assert_eq!(read_vote(&path).unwrap(), Some(vote));
    }
}

// =============================================================================
// WalLogStore — RaftLogStorage over the WAL (disk is truth, RAM is the read path)
// =============================================================================

use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

use openraft::storage::{LogFlushed, LogState, RaftLogStorage};
use openraft::{OptionalSend, RaftLogReader, StorageError};

/// Durable raft log storage. Cloneable handle (the reader is a clone); every mutating
/// call is fsynced before it is acknowledged — `append` fsyncs before firing the raft
/// flush callback, which is the exact promise openraft's replication builds on.
#[derive(Clone)]
pub struct WalLogStore {
    wal: Arc<Mutex<Wal>>,
    vote_path: PathBuf,
    vote: Arc<Mutex<Option<Vote<NodeId>>>>,
}

impl WalLogStore {
    /// Open the durable store under `data_dir` (WAL segments in `wal/`, vote alongside).
    pub fn open(data_dir: &Path) -> std::io::Result<Self> {
        let wal = Wal::open(&data_dir.join("wal"))?;
        let vote_path = data_dir.join("vote.bin");
        let vote = read_vote(&vote_path)?;
        Ok(Self {
            wal: Arc::new(Mutex::new(wal)),
            vote_path,
            vote: Arc::new(Mutex::new(vote)),
        })
    }
}

fn io_err(e: std::io::Error) -> StorageError<NodeId> {
    StorageError::IO { source: openraft::StorageIOError::write(&e) }
}

impl RaftLogReader<TypeConfig> for WalLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let wal = self.wal.lock().unwrap();
        Ok(wal.state.entries.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogStorage<TypeConfig> for WalLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let wal = self.wal.lock().unwrap();
        let last = wal
            .state
            .entries
            .iter()
            .next_back()
            .map(|(_, e)| e.log_id)
            .or(wal.state.last_purged);
        Ok(LogState {
            last_purged_log_id: wal.state.last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Durable BEFORE returning: answering a candidate with a vote we could forget is
        // the double-vote hole this file exists to close.
        save_vote(&self.vote_path, vote).map_err(io_err)?;
        *self.vote.lock().unwrap() = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(*self.vote.lock().unwrap())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        self.wal.lock().unwrap().save_committed(committed).map_err(io_err)
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self.wal.lock().unwrap().state.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        let batch: Vec<Entry<TypeConfig>> = entries.into_iter().collect();
        let result = self.wal.lock().unwrap().append_entries(batch);
        match result {
            Ok(()) => {
                // fsync has completed — only now may the flush promise be honored.
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(e) => {
                let err = std::io::Error::new(e.kind(), e.to_string());
                callback.log_io_completed(Err(e));
                Err(io_err(err))
            }
        }
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.wal.lock().unwrap().truncate_from(log_id.index).map_err(io_err)
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.wal.lock().unwrap().purge(log_id).map_err(io_err)
    }
}
