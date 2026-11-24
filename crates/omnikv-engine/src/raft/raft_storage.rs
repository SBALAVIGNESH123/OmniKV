use crate::OmniKV;
use crate::WriteBatch;
use crate::raft_impl::{OmniNode, TypeConfig};
use crate::transaction::SsiCommitRecord;
use openraft::{
    AnyError, Entry, EntryPayload, LogId, OptionalSend, RaftTypeConfig, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership, Vote,
    storage::{LogState, RaftLogReader, RaftSnapshotBuilder, RaftStorage, Snapshot},
};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const RAFT_LOG_PREFIX: &str = "__sys__/raft/log/";
const RAFT_META_KEY: &str = "__sys__/raft/meta";
const LEGACY_OLD_SNAPSHOT_DIR: &str = "old_snapshot";
const OLD_SNAPSHOT_DIR_PREFIX: &str = "old_snapshot_";
const INSTALL_COMPLETE_MARKER: &str = ".install_complete";
const ROLLBACK_FAILED_MARKER: &str = ".rollback_failed";
static NEXT_SNAPSHOT_ROLLBACK_SEQ: AtomicU64 = AtomicU64::new(1);
/// Bumped to 2 when the SSI committed history (`ssi_history`) joined the
/// envelope. The version check below REJECTS any mismatch, so a snapshot
/// written by pre-history code (same version number, no field) must not
/// slip through: `#[serde(default)]` would turn its absent field into an
/// EMPTY history, and `install_from` would then wipe the installer's own
/// history — losing every conflict record for commits the snapshot's data
/// actually carries. Keeping the version in lockstep with the format means
/// a legacy snapshot is rejected instead of silently corrupting history.
const SNAPSHOT_VERSION: u32 = 3;

/// Versioned snapshot envelope — adding version field now prevents future migration pain.
#[derive(Serialize, Deserialize, Debug)]
struct SnapshotEnvelope {
    version: u32,
    last_log_id: Option<LogId<u64>>,
    membership: StoredMembership<u64, OmniNode>,
    /// Max storage sequence represented by this snapshot.
    /// Critical: global_seq must be set >= this after install to preserve MVCC ordering.
    max_seq: u64,
    entries: Vec<(String, String)>,
    /// The committed-transaction history at snapshot time, so a node that
    /// installs this snapshot keeps detecting conflicts against commits
    /// that arrived IN the snapshot rather than through log apply. Without
    /// it, a node promoted after installing a snapshot validates against a
    /// history missing every commit the snapshot carried. No serde default:
    /// the version gate in install_snapshot rejects anything older before
    /// this struct is parsed, so an accepted envelope always carries the
    /// field and a missing one is a malformed snapshot that must fail
    /// loudly rather than silently install an empty history.
    ssi_history: Vec<SsiHistoryEntry>,
    /// SSI eviction floor carried with the snapshot history.
    ssi_eviction_floor: u64,
}

/// One committed transaction in a snapshot envelope: the SSI record plus
/// the marker it was recorded at in the sender's sequence space. The
/// marker is per-node, but the sender's markers are ≤ `max_seq` and the
/// installer bumps its own counter past `max_seq`, so the carried values
/// stay ordered correctly below everything the node writes afterwards.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct SsiHistoryEntry {
    txn_id: u64,
    commit_seq: u64,
    record: crate::transaction::SsiCommitRecord,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
struct RaftStateMeta {
    vote: Option<Vote<u64>>,
    last_log_id: Option<LogId<u64>>,
    last_purged_log_id: Option<LogId<u64>>,
    last_applied: Option<LogId<u64>>,
    membership: StoredMembership<u64, OmniNode>,
}

/// OmniKV-backed Raft storage.
pub struct OmniRaftStorage {
    db: Arc<OmniKV>,
    meta: Arc<Mutex<RaftStateMeta>>,
}

impl Clone for OmniRaftStorage {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            meta: Arc::clone(&self.meta),
        }
    }
}

impl OmniRaftStorage {
    pub fn new(db: Arc<OmniKV>) -> Self {
        let meta = Self::read_meta(&db);
        Self {
            db,
            meta: Arc::new(Mutex::new(meta)),
        }
    }

    fn read_meta(db: &OmniKV) -> RaftStateMeta {
        if let Ok(Some(val)) = db.find_latest_internal(RAFT_META_KEY)
            && let Ok(meta) = serde_json::from_str(&val)
        {
            return meta;
        }
        RaftStateMeta::default()
    }

    fn save_meta(&self, meta: &RaftStateMeta, batch: &mut WriteBatch) {
        let json = serde_json::to_string(meta)
            .expect("RaftStateMeta serialization should not fail for supported fields");
        batch
            .set(RAFT_META_KEY, json)
            .expect("internal Raft metadata key must be valid");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Synchronous test-facing helpers
    //
    // These methods provide a simple synchronous API for integration tests
    // to exercise Raft log replication, state machine application, vote
    // persistence, and log compaction without needing the full async
    // OpenRaft runtime.
    // ═══════════════════════════════════════════════════════════════════════

    /// Append (or overwrite) a log entry at the given index.
    pub fn append_log(&self, index: u64, entry: &str) -> Result<(), crate::OmniError> {
        let key = format!("{}{:020}", RAFT_LOG_PREFIX, index);
        let mut batch = WriteBatch::new();
        batch.set(&key, entry.to_string())?;
        self.db.commit_batch_local(&batch)?;
        Ok(())
    }

    /// Read the log entry at the given index. Returns `None` if absent.
    pub fn read_log(&self, index: u64) -> Option<String> {
        let key = format!("{}{:020}", RAFT_LOG_PREFIX, index);
        self.db.find_latest_internal(&key).ok().flatten()
    }

    /// Apply a `SET key value` command from the Raft log to the underlying
    /// storage engine (state machine).
    pub fn apply_write(&self, entry: &str) -> Result<(), crate::OmniError> {
        if entry.starts_with("SET ") {
            let parts: Vec<&str> = entry.splitn(3, ' ').collect();
            if parts.len() == 3 {
                let mut batch = WriteBatch::new();
                batch.set(parts[1], parts[2].to_string())?;
                self.db.commit_batch_local(&batch)?;
            }
        }
        Ok(())
    }

    /// Record the given index as the last applied log index.
    pub fn mark_applied(&self, index: u64) -> Result<(), crate::OmniError> {
        let mut batch = WriteBatch::new();
        let next_meta = {
            let meta = self
                .meta
                .lock()
                .expect("RaftStorage meta lock poisoned: fatal invariant");
            let mut next = meta.clone();
            // For the test helper, the exact leader_id is not critical — only the index matters.
            let leader_id = next
                .last_applied
                .map(|existing| existing.leader_id)
                .unwrap_or_else(|| openraft::CommittedLeaderId::new(0, 0));
            next.last_applied = Some(LogId::new(leader_id, index));
            self.save_meta(&next, &mut batch);
            next
        };
        // The meta guard is dropped BEFORE the commit: commit_batch_local
        // takes the transition lock, and holding meta across it would
        // invert the lock order against build_snapshot/install_snapshot
        // (transition -> meta) and deadlock.
        self.db.commit_batch_local(&batch)?;
        *self
            .meta
            .lock()
            .expect("RaftStorage meta lock poisoned: fatal invariant") = next_meta;
        Ok(())
    }

    /// Returns the index of the last applied log entry, or 0 if none.
    pub fn last_applied_index(&self) -> u64 {
        let meta = self
            .meta
            .lock()
            .expect("RaftStorage meta lock poisoned: fatal invariant");
        meta.last_applied.map(|id| id.index).unwrap_or(0)
    }

    /// Delete log entries in the half-open range `[start, end)`.
    pub fn delete_log_range(&self, start: u64, end: u64) -> Result<(), crate::OmniError> {
        let mut batch = WriteBatch::new();
        for idx in start..end {
            let key = format!("{}{:020}", RAFT_LOG_PREFIX, idx);
            batch.delete(&key)?;
        }
        self.db.commit_batch_local(&batch)?;
        Ok(())
    }

    /// Persist a vote (as a JSON string) for crash recovery.
    pub fn save_vote(&self, vote_json: &str) -> Result<(), crate::OmniError> {
        let key = format!("{}vote", RAFT_LOG_PREFIX);
        let mut batch = WriteBatch::new();
        batch.set(&key, vote_json.to_string())?;
        self.db.commit_batch_local(&batch)?;
        Ok(())
    }

    /// Read the persisted vote JSON string.
    pub fn read_vote(&self) -> Option<String> {
        let key = format!("{}vote", RAFT_LOG_PREFIX);
        self.db.find_latest_internal(&key).ok().flatten()
    }
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "openraft's storage/network traits declare async fns; the storage and network adapters are synchronous internally and async is required by the trait signatures."
)]
impl RaftLogReader<TypeConfig> for OmniRaftStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        let mut entries = Vec::new();
        let start = match range.start_bound() {
            std::ops::Bound::Included(&s) => s,
            std::ops::Bound::Excluded(&s) => s + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(&e) => e + 1,
            std::ops::Bound::Excluded(&e) => e,
            std::ops::Bound::Unbounded => u64::MAX,
        };

        for idx in start..end {
            let key = format!("{}{:020}", RAFT_LOG_PREFIX, idx);
            match self.db.find_latest_internal(&key) {
                Ok(Some(val)) => {
                    let entry = serde_json::from_str::<Entry<TypeConfig>>(&val).map_err(|e| {
                        storage_read_err(format!("corrupt raft log at index {idx}: {e}"))
                    })?;
                    entries.push(entry);
                }
                Ok(None) => break,
                Err(e) => {
                    return Err(storage_read_err(format!(
                        "failed to read raft log at index {idx}: {e}"
                    )));
                }
            }
        }
        Ok(entries)
    }
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "openraft's storage/network traits declare async fns; the storage and network adapters are synchronous internally and async is required by the trait signatures."
)]
impl RaftSnapshotBuilder<TypeConfig> for OmniRaftStorage {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        // The history, the sequence counter, and the data entries are read as
        // ONE atomic group under the EXCLUSIVE transition lock. An apply
        // publishes a commit's DATA and its history record as a pair (the
        // apply path holds the SHARED lock across both — see flush_staged),
        // so the exclusive lock here waits for any in-flight apply to finish
        // publishing both, and blocks new applies from publishing either,
        // while these reads happen. Without that, an apply landing between
        // the reads could put data in the scanned entries but its record in
        // the history sample — or the reverse — and the snapshot would carry
        // a data/history pair that never existed together. This is the same
        // lock install_snapshot already holds for its whole rebuild, so
        // snapshot capture pays the same writer-freeze cost install already
        // does, and only when one is actually built.
        let _exclusive = self
            .db
            .transition_guard
            .write()
            .map_err(|_| StorageError::IO {
                source: StorageIOError::new(
                    openraft::ErrorSubject::Store,
                    openraft::ErrorVerb::Write,
                    AnyError::error("transition_guard poisoned"),
                ),
            })?;

        // Read the committed history BEFORE the sequence counter. A record's
        // commit_seq is always ≤ the counter at the moment it was pushed, so
        // sampling history first and max_seq afterwards guarantees every
        // carried marker is ≤ max_seq — the invariant the installer relies
        // on when it bumps its own counter past max_seq to keep the carried
        // records ordered below its own future commits.
        let ssi_history: Vec<SsiHistoryEntry> = self
            .db
            .ssi_history()
            .snapshot_entries()
            .into_iter()
            .map(|(txn_id, commit_seq, record)| SsiHistoryEntry {
                txn_id,
                commit_seq,
                record,
            })
            .collect();
        let ssi_eviction_floor = self.db.ssi_history().eviction_floor();

        let mut entries = Vec::new();
        let (snap_meta, max_seq) = {
            let m = self
                .meta
                .lock()
                .expect("RaftStorage meta lock poisoned: fatal invariant");
            if m.last_applied.is_none() {
                return Err(StorageError::IO {
                    source: StorageIOError::new(
                        openraft::ErrorSubject::Store,
                        openraft::ErrorVerb::Write,
                        AnyError::error("Cannot snapshot empty state machine"),
                    ),
                });
            }
            let la = m.last_applied.ok_or_else(|| {
                StorageError::from_io_error(
                    openraft::ErrorSubject::StateMachine,
                    openraft::ErrorVerb::Read,
                    std::io::Error::other("last_applied is None during snapshot"),
                )
            })?;
            let meta = SnapshotMeta {
                last_log_id: m.last_applied,
                last_membership: m.membership.clone(),
                snapshot_id: format!("{}-{}", la.leader_id, la.index),
            };
            (meta, self.db.get_seq())
        };

        let mut iter = self
            .db
            .scan_all_latest_internal()
            .map_err(|e| storage_read_err(format!("snapshot storage scan failed: {e}")))?;
        while let Some((k, v)) = iter
            .try_next()
            .map_err(|e| storage_read_err(format!("snapshot heap read failed: {e}")))?
        {
            if k.starts_with("__sys__/raft/") {
                continue;
            }
            entries.push((k, v));
        }

        let envelope = SnapshotEnvelope {
            version: SNAPSHOT_VERSION,
            last_log_id: snap_meta.last_log_id,
            membership: snap_meta.last_membership.clone(),
            max_seq,
            entries,
            ssi_history,
            ssi_eviction_floor,
        };
        let serialized = serde_json::to_vec(&envelope).map_err(|e| StorageError::IO {
            source: StorageIOError::new(
                openraft::ErrorSubject::Store,
                openraft::ErrorVerb::Write,
                AnyError::error(e.to_string()),
            ),
        })?;

        Ok(Snapshot {
            meta: snap_meta,
            snapshot: Box::new(Cursor::new(serialized)),
        })
    }
}

fn storage_write_err(e: impl std::fmt::Display) -> openraft::StorageError<u64> {
    openraft::StorageError::from_io_error(
        openraft::ErrorSubject::Store,
        openraft::ErrorVerb::Write,
        std::io::Error::other(e.to_string()),
    )
}

fn parse_canonical_raft_log_key(key: &str) -> Option<u64> {
    let rest = key.strip_prefix(RAFT_LOG_PREFIX)?;
    if rest.len() != 20 || !rest.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    rest.parse::<u64>().ok()
}

fn storage_read_err(e: impl std::fmt::Display) -> openraft::StorageError<u64> {
    openraft::StorageError::from_io_error(
        openraft::ErrorSubject::Store,
        openraft::ErrorVerb::Read,
        std::io::Error::other(e.to_string()),
    )
}

/// Flush directory metadata so renames and unlinks inside `dir` survive power
/// loss.
///
/// Platform durability boundary: directory `fsync` is enforced on Unix/POSIX
/// targets. On Windows, calling `FlushFileBuffers` on a directory handle
/// requires write access that NTFS denies to standard unprivileged processes,
/// so this function is an explicit no-op on non-Unix targets; snapshot
/// directory-rename crash durability guarantees are Unix/POSIX-only.
fn sync_dir(dir: &std::path::Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::fs::File::open(dir)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
    }
    Ok(())
}

fn write_durable_marker(path: &std::path::Path, contents: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = std::fs::File::create(path)?;
    file.write_all(contents)?;
    file.sync_all()?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn is_empty_dir(path: &std::path::Path) -> std::io::Result<bool> {
    Ok(std::fs::read_dir(path)?.next().is_none())
}

fn retire_completed_rollback_dir(dir: &std::path::Path) -> std::io::Result<()> {
    let marker_path = dir.join(INSTALL_COMPLETE_MARKER);
    if !marker_path.exists() || dir.join(ROLLBACK_FAILED_MARKER).exists() {
        return Err(std::io::Error::other(
            "cannot retire rollback directory without a clean install-complete marker",
        ));
    }
    // Remove every non-marker entry first; if any deletion fails (e.g., an open
    // Windows handle), `.install_complete` stays intact so a later attempt can
    // finish retiring the directory instead of mistaking it for an unresolved rollback.
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_name() == INSTALL_COMPLETE_MARKER {
            continue;
        }
        let path = entry.path();
        if path.is_dir() {
            std::fs::remove_dir_all(&path)?;
        } else {
            std::fs::remove_file(&path)?;
        }
    }
    // Barrier: persist backup entry removals before removing `.install_complete`
    // so a power loss cannot leave non-marker backup files without the marker.
    sync_dir(dir)?;
    std::fs::remove_file(&marker_path)?;
    std::fs::remove_dir(dir)?;
    if let Some(parent) = dir.parent() {
        let _ = sync_dir(parent);
    }
    Ok(())
}

/// Commits `batch` locally, in [`WriteBatch::MAX_OPS`]-sized chunks if
/// it exceeds the op cap. The raft storage paths are the only callers
/// that can legitimately exceed one client batch: a follower catch-up
/// appends many log entries in one call, a purge deletes one key per
/// purged index, and a snapshot install replays the whole captured
/// dataset. Committing the whole thing as ONE batch would trip
/// `BatchTooLarge` there and stall replication; the chunk boundaries are
/// invisible to the engine because every op is idempotent (same-key ops
/// re-apply in log order) and each chunk gets its own monotonically
/// increasing storage seq.
fn commit_chunked(db: &OmniKV, batch: &mut WriteBatch) -> Result<(), crate::OmniError> {
    if batch.op_count() <= WriteBatch::MAX_OPS {
        db.commit_batch_local(batch).map(|_| ())?;
        return Ok(());
    }
    // Drain in chunks: buffered_writes and buffered_deletes are each
    // <= MAX_OPS-safe in the original only if the TOTAL was; draining
    // one op at a time into a fresh capped batch preserves order.
    let mut chunk = WriteBatch::new();
    for (key, value, expiry) in std::mem::take(&mut batch.buffered_writes) {
        if chunk.op_count() >= WriteBatch::MAX_OPS {
            db.commit_batch_local(&chunk).map(|_| ())?;
            chunk.clear();
        }
        // Absolute expiry: the entry already carries it.
        chunk.set_with_expiry(&String::from_utf8_lossy(&key), value, expiry)?;
    }
    for key in std::mem::take(&mut batch.buffered_deletes) {
        if chunk.op_count() >= WriteBatch::MAX_OPS {
            db.commit_batch_local(&chunk).map(|_| ())?;
            chunk.clear();
        }
        chunk.delete(&String::from_utf8_lossy(&key))?;
    }
    if !chunk.is_empty() {
        db.commit_batch_local(&chunk).map(|_| ())?;
    }
    Ok(())
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "openraft's storage/network traits declare async fns; the storage and network adapters are synchronous internally and async is required by the trait signatures."
)]
impl RaftStorage<TypeConfig> for OmniRaftStorage {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let mut batch = WriteBatch::new();
        let next_meta = {
            let meta = self
                .meta
                .lock()
                .expect("RaftStorage meta lock poisoned: fatal invariant");
            let mut next = meta.clone();
            next.vote = Some(*vote);
            self.save_meta(&next, &mut batch);
            next
        };
        self.db
            .commit_batch_local(&batch)
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    openraft::ErrorSubject::Store,
                    openraft::ErrorVerb::Write,
                    AnyError::error(e.to_string()),
                ),
            })?;
        *self
            .meta
            .lock()
            .expect("RaftStorage meta lock poisoned: fatal invariant") = next_meta;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let meta = self
            .meta
            .lock()
            .expect("RaftStorage meta lock poisoned: fatal invariant");
        Ok(meta.vote)
    }

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let meta = self
            .meta
            .lock()
            .expect("RaftStorage meta lock poisoned: fatal invariant");
        Ok(LogState {
            last_purged_log_id: meta.last_purged_log_id,
            last_log_id: meta.last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut batch = WriteBatch::new();
        let mut last_log_id = None;
        for entry in entries {
            let key = format!("{}{:020}", RAFT_LOG_PREFIX, entry.log_id.index);
            let val = serde_json::to_string(&entry).map_err(|e| storage_write_err(&e))?;
            batch.set(&key, val).map_err(|e| storage_write_err(&e))?;
            // A follower catch-up can append more entries in one call
            // than one batch holds; flush chunks as we go.
            if batch.op_count() >= WriteBatch::MAX_OPS {
                self.db
                    .commit_batch_local(&batch)
                    .map_err(|e| StorageError::IO {
                        source: StorageIOError::new(
                            openraft::ErrorSubject::Store,
                            openraft::ErrorVerb::Write,
                            AnyError::error(e.to_string()),
                        ),
                    })?;
                batch.clear();
            }
            last_log_id = Some(entry.log_id);
        }

        let next_meta = if last_log_id.is_some() {
            // Scoped: the guard must be released before commit_chunked
            // takes the transition lock (lock order is transition -> meta).
            let meta = self
                .meta
                .lock()
                .expect("RaftStorage meta lock poisoned: fatal invariant");
            let mut next = meta.clone();
            next.last_log_id = last_log_id;
            self.save_meta(&next, &mut batch);
            Some(next)
        } else {
            None
        };

        // Tail chunk (never larger than MAX_OPS thanks to the flushes
        // above; commit_chunked is belt-and-braces for the meta op).
        commit_chunked(&self.db, &mut batch).map_err(|e| StorageError::IO {
            source: StorageIOError::new(
                openraft::ErrorSubject::Store,
                openraft::ErrorVerb::Write,
                AnyError::error(e.to_string()),
            ),
        })?;
        if let Some(next_meta) = next_meta {
            *self
                .meta
                .lock()
                .expect("RaftStorage meta lock poisoned: fatal invariant") = next_meta;
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<u64>,
    ) -> Result<(), StorageError<u64>> {
        let first_key = format!("{}{:020}", RAFT_LOG_PREFIX, log_id.index);
        let first_exists = self
            .db
            .find_latest_internal(&first_key)
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    openraft::ErrorSubject::Logs,
                    openraft::ErrorVerb::Read,
                    AnyError::error(e.to_string()),
                ),
            })?
            .is_some();
        if !first_exists {
            return Ok(());
        }

        // Resolve the new last_log_id before deleting conflict tail chunks:
        // only fall back to the purge floor when truncation starts immediately
        // after it. If an expected predecessor above the purge floor is
        // missing, unreadable, or invalid, fail closed with a storage error.
        let last_purged = {
            let meta = self
                .meta
                .lock()
                .expect("RaftStorage meta lock poisoned: fatal invariant");
            meta.last_purged_log_id
        };
        let resolve_predecessor = |prev_idx: u64| -> Result<Option<LogId<u64>>, String> {
            if last_purged.is_some_and(|purged| purged.index == prev_idx) {
                return Ok(last_purged);
            }
            let prev_key = format!("{}{:020}", RAFT_LOG_PREFIX, prev_idx);
            match self.db.find_latest_internal(&prev_key) {
                Ok(Some(val)) => {
                    let entry = serde_json::from_str::<Entry<TypeConfig>>(&val).map_err(|e| {
                        format!("corrupt predecessor raft log at index {prev_idx}: {e}")
                    })?;
                    Ok(Some(entry.log_id))
                }
                Ok(None) if prev_idx == 0 && last_purged.is_none() => Ok(None),
                Ok(None) => Err(format!(
                    "missing predecessor raft log at index {prev_idx} above purge floor {:?}",
                    last_purged
                )),
                Err(e) => Err(format!(
                    "failed to read predecessor raft log at index {prev_idx}: {e}"
                )),
            }
        };
        let log_read_err = |msg: String| StorageError::IO {
            source: StorageIOError::new(
                openraft::ErrorSubject::Logs,
                openraft::ErrorVerb::Read,
                AnyError::error(msg),
            ),
        };
        let new_last_log_id = if log_id.index == 0 {
            None
        } else {
            resolve_predecessor(log_id.index - 1).map_err(log_read_err)?
        };

        let mut end_idx = log_id.index;
        loop {
            let key = format!("{}{:020}", RAFT_LOG_PREFIX, end_idx);
            let exists = self
                .db
                .find_latest_internal(&key)
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::new(
                        openraft::ErrorSubject::Logs,
                        openraft::ErrorVerb::Read,
                        AnyError::error(e.to_string()),
                    ),
                })?
                .is_some();
            if exists {
                end_idx += 1;
            } else {
                break;
            }
        }

        // Delete the conflict tail HIGH-TO-LOW in bounded chunks, updating
        // `meta.last_log_id` atomically with each chunk so a crash mid-truncate
        // never leaves a hole at `log_id.index` or orphaned upper entries.
        let chunk_cap = WriteBatch::MAX_OPS.saturating_sub(1).max(1) as u64;
        let mut cursor = end_idx;
        while cursor > log_id.index {
            let chunk_start = cursor.saturating_sub(chunk_cap).max(log_id.index);
            let chunk_last_log_id = if chunk_start == log_id.index {
                new_last_log_id
            } else {
                resolve_predecessor(chunk_start - 1).map_err(log_read_err)?
            };
            let mut batch = WriteBatch::new();
            for idx in chunk_start..cursor {
                let key = format!("{}{:020}", RAFT_LOG_PREFIX, idx);
                batch.delete(&key).map_err(|e| storage_write_err(&e))?;
            }
            let next_meta = {
                let meta = self
                    .meta
                    .lock()
                    .expect("RaftStorage meta lock poisoned: fatal invariant");
                let mut next = meta.clone();
                next.last_log_id = chunk_last_log_id;
                self.save_meta(&next, &mut batch);
                next
            };
            self.db
                .commit_batch_local(&batch)
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::new(
                        openraft::ErrorSubject::Store,
                        openraft::ErrorVerb::Write,
                        AnyError::error(e.to_string()),
                    ),
                })?;
            *self
                .meta
                .lock()
                .expect("RaftStorage meta lock poisoned: fatal invariant") = next_meta;
            cursor = chunk_start;
        }
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut batch = WriteBatch::new();

        let (mut current_floor, mut cursor_idx) = {
            let meta = self
                .meta
                .lock()
                .expect("RaftStorage meta lock poisoned: fatal invariant");
            if meta.last_purged_log_id.is_some_and(|cur| log_id <= cur) {
                return Ok(());
            }
            (
                meta.last_purged_log_id,
                meta.last_purged_log_id
                    .map_or(Some(0), |id| id.index.checked_add(1)),
            )
        };

        // Validate the final target log entry upfront whenever it exists in
        // storage so a corrupt or mismatched target LogId fails closed before
        // any intermediate chunk is deleted.
        let target_key = format!("{}{:020}", RAFT_LOG_PREFIX, log_id.index);
        if let Some(val) = self
            .db
            .find(&target_key, self.db.get_seq())
            .map_err(|e| storage_read_err(format!("read purge target log {}: {e}", log_id.index)))?
        {
            let entry = serde_json::from_str::<Entry<TypeConfig>>(&val).map_err(|e| {
                storage_read_err(format!(
                    "corrupt Raft log entry at purge target {}: {e}",
                    log_id.index
                ))
            })?;
            if entry.log_id != log_id {
                return Err(storage_read_err(format!(
                    "mismatched Raft log id at purge target {}: expected {}, found {}",
                    log_id.index, log_id, entry.log_id
                )));
            }
        }

        let chunk_cap = WriteBatch::MAX_OPS.saturating_sub(1);
        // Use u128 arithmetic so even `log_id.index == u64::MAX` forms a 20-digit
        // exclusive upper bound (`18446744073709551616`), staying strictly below
        // non-numeric sibling keys such as `__sys__/raft/log/vote`.
        let end_exclusive = Some(format!(
            "{RAFT_LOG_PREFIX}{:020}",
            u128::from(log_id.index) + 1
        ));

        loop {
            let entries = if let Some(c) = cursor_idx
                && c <= log_id.index
            {
                let start_key = format!("{RAFT_LOG_PREFIX}{c:020}");
                self.db
                    .scan_exclusive_limit(
                        &start_key,
                        end_exclusive.as_deref(),
                        self.db.get_seq(),
                        chunk_cap.saturating_add(1),
                    )
                    .map_err(storage_read_err)?
            } else {
                Vec::new()
            };

            let has_more = entries.len() > chunk_cap;
            let chunk_len = entries.len().min(chunk_cap);

            // Validate EVERY entry in this chunk (including the final partial
            // chunk) before staging any deletes or advancing the purge floor:
            // canonical 20-digit key, valid JSON Entry, exact key/index match,
            // strictly monotonic full LogId starting from `current_floor`, and
            // `entry.log_id <= log_id`.
            let mut prev_log_id = current_floor;
            let mut last_validated: Option<(u64, LogId<u64>)> = None;
            for (key, val) in entries.iter().take(chunk_len) {
                let idx = parse_canonical_raft_log_key(key).ok_or_else(|| {
                    storage_read_err(format!(
                        "non-canonical Raft log key '{key}' encountered during purge"
                    ))
                })?;
                let entry = serde_json::from_str::<Entry<TypeConfig>>(val).map_err(|e| {
                    storage_read_err(format!("corrupt Raft log entry at purge index {idx}: {e}"))
                })?;
                if entry.log_id.index != idx {
                    return Err(storage_read_err(format!(
                        "mismatched Raft log index at purge index {idx}: found {}",
                        entry.log_id.index
                    )));
                }
                if entry.log_id > log_id || prev_log_id.is_some_and(|prev| entry.log_id <= prev) {
                    return Err(storage_read_err(format!(
                        "non-monotonic Raft log id at purge index {idx}: found {}, expected strictly after {:?} and at most {}",
                        entry.log_id, prev_log_id, log_id
                    )));
                }
                prev_log_id = Some(entry.log_id);
                last_validated = Some((idx, entry.log_id));
            }

            // Chunk actual existing local log entries instead of numeric
            // indexes so purging over a sparse or empty local prefix succeeds,
            // while every populated intermediate chunk advances the durable
            // purge floor to the exact validated LogId of its final entry.
            let chunk_floor = if has_more {
                Some(last_validated.ok_or_else(|| {
                    storage_read_err("empty Raft purge chunk with remaining entries")
                })?)
            } else {
                None
            };

            for (key, _) in entries.iter().take(chunk_len) {
                batch.delete(key).map_err(|e| storage_write_err(&e))?;
            }

            let floor_log_id = chunk_floor.map_or(log_id, |(_, id)| id);
            let next_meta = {
                let meta = self
                    .meta
                    .lock()
                    .expect("RaftStorage meta lock poisoned: fatal invariant");
                let mut next = meta.clone();
                next.last_purged_log_id = Some(floor_log_id);
                self.save_meta(&next, &mut batch);
                next
            };
            self.db
                .commit_batch_local(&batch)
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::new(
                        openraft::ErrorSubject::Store,
                        openraft::ErrorVerb::Write,
                        AnyError::error(e.to_string()),
                    ),
                })?;
            *self
                .meta
                .lock()
                .expect("RaftStorage meta lock poisoned: fatal invariant") = next_meta;
            current_floor = Some(floor_log_id);
            batch.clear();

            if let Some((idx, _)) = chunk_floor {
                cursor_idx = idx.checked_add(1);
                if cursor_idx.is_none() {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, OmniNode>), StorageError<u64>> {
        let meta = self
            .meta
            .lock()
            .expect("RaftStorage meta lock poisoned: fatal invariant");
        Ok((meta.last_applied, meta.membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<String>, StorageError<u64>> {
        let mut res = Vec::with_capacity(entries.len());
        let mut batch = WriteBatch::new();
        let mut last_applied = None;
        let mut new_membership = None;
        // openraft applies entries in batches, and a follower catch-up can
        // carry more ops in ONE apply call than a WriteBatch holds, so
        // commits happen in capped chunks. The chunk boundary must never
        // fall INSIDE a Raft entry: one entry is one atomic client write,
        // and splitting it would let a reader (or a crash) see only part
        // of it. Flush only at ENTRY boundaries — before staging an entry
        // whose ops would not fit alongside what is already staged.
        //
        // This is safe because a single RaftCommand can never exceed the
        // cap on its own: the engine's WriteBatch rejects the op that
        // would exceed MAX_BATCH_SIZE before the command is ever proposed
        // (RaftCommand::from_batch carries that cap), so any one entry
        // always fits in a fresh batch and the flush keeps every commit
        // ≤ MAX_OPS without ever splitting an entry.
        let io_err = |e: crate::OmniError| StorageError::IO {
            source: StorageIOError::new(
                openraft::ErrorSubject::Store,
                openraft::ErrorVerb::Write,
                AnyError::error(e.to_string()),
            ),
        };
        // Slots in `res` for entries whose ops are staged in `batch` but
        // not yet committed, each paired with the SSI commit record the
        // command carried (None for plain writes). The placeholder
        // responses are stamped with the batch's commit marker when the
        // batch flushes, and the record — if any — is entered into this
        // node's committed history at that same marker.
        let mut staged_slots: Vec<(usize, Option<SsiCommitRecord>)> = Vec::new();
        let mut staged_keys: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        // Flushes the staged batch (if non-empty) and stamps every entry
        // staged in it with the batch's COMMIT MARKER, which the proposer
        // reads back through client_write's response as the sequence its
        // write became visible at.
        //
        // The marker is captured HERE, never reconstructed from
        // db.get_seq() after the apply returns: log purges and snapshot
        // installs reserve from the same global counter and can fire
        // between this commit and any later read, which would hand back a
        // bookkeeping marker instead of the transaction's and abort a
        // later transaction spuriously. (openraft's StorageError is ~224
        // bytes; mapping it at the call sites keeps this closure cheap and
        // clippy's result_large_err quiet.)
        //
        // This is also where a replicated SSI record lands in the node's
        // own history: recording at apply (not at the COMMIT on the node
        // that ran it) means a follower promoted after a failover already
        // has every pre-failover commit in its history when it first leads.
        // Hold the shared transition lock across the entire apply call — both
        // every staged data/history flush AND the final `last_applied` /
        // `membership` metadata publication — so `build_snapshot` (which takes
        // the exclusive transition lock) can never observe committed user data
        // paired with a stale `last_applied` or missing SSI commit history.
        // Every commit inside this span uses `commit_batch_local_locked*` so
        // `transition_guard` is never re-entered while held.
        let _topology_guard = self
            .db
            .transition_guard
            .read()
            .map_err(|_| io_err(crate::OmniError::LockPoisoned("transition_guard".into())))?;

        let flush_staged = |batch: &mut WriteBatch,
                            slots: &mut Vec<(usize, Option<SsiCommitRecord>)>,
                            keys: &mut std::collections::HashSet<Vec<u8>>,
                            res: &mut Vec<String>,
                            pending_applied: &mut Option<LogId<u64>>,
                            pending_membership: &mut Option<StoredMembership<u64, OmniNode>>|
         -> Result<(), crate::OmniError> {
            if batch.is_empty() && pending_applied.is_none() && pending_membership.is_none() {
                return Ok(());
            }
            let touches_2pc = batch
                .buffered_writes
                .iter()
                .any(|(key, _, _)| key.starts_with(b"__2PC_PREPARE__/"))
                || batch
                    .buffered_deletes
                    .iter()
                    .any(|key| key.starts_with(b"__2PC_PREPARE__/"));
            // Atomically persist `last_applied` and `membership` in the SAME
            // WAL batch as every durable state-machine flush (covering any
            // preceding rejected/blank entries up to this point) so a crash or
            // later flush failure can never replay a rejected entry against
            // state committed by a later entry.
            let next_meta = if pending_applied.is_some() || pending_membership.is_some() {
                let meta = self
                    .meta
                    .lock()
                    .expect("RaftStorage meta lock poisoned: fatal invariant");
                let mut next = meta.clone();
                if let Some(la) = *pending_applied {
                    next.last_applied = Some(la);
                }
                if let Some(m) = pending_membership.as_ref() {
                    next.membership = m.clone();
                }
                let json = serde_json::to_string(&next)
                    .map_err(|e| crate::OmniError::IoError(format!("serialize raft meta: {e}")))?;
                batch.set_internal_meta(RAFT_META_KEY, json)?;
                Some(next)
            } else {
                None
            };
            let history = self.db.ssi_history();
            // Hold `committed_txns` across data+meta commit, 2PC reservation
            // sync, and SSI history insertion (preserving global lock order
            // `transition_guard -> committed_txns -> write_mutex -> prepared_keys`)
            // so concurrent validators cannot observe published data or
            // released 2PC reservations before the SSI commit record is visible.
            let mut committed_guard = history.lock_committed();
            let marker = self.db.commit_batch_local_locked_skip_prepared(batch)?;
            let sync_res = if touches_2pc {
                self.db.sync_prepared_2pc_from_batch(batch)
            } else {
                Ok(())
            };
            // Entries sharing a batch committed atomically at one marker —
            // a transaction whose read_seq equals it sees all of them.
            for &(slot, ref ssi) in slots.iter() {
                res[slot] = marker.to_string();
                if let Some(record) = ssi {
                    history.record_committed_locked(
                        &mut committed_guard,
                        record.txn_id,
                        marker,
                        record,
                    );
                }
            }
            drop(committed_guard);
            if let Some(next_meta) = next_meta {
                *self
                    .meta
                    .lock()
                    .expect("RaftStorage meta lock poisoned: fatal invariant") = next_meta;
            }
            // Records below the oldest in-play snapshot can never again
            // be a conflict; drop them so a node that only ever APPLIES
            // (never runs a local COMMIT) does not grow without bound.
            history.prune(self.db.min_active_snapshot());
            slots.clear();
            keys.clear();
            batch.clear();
            *pending_applied = None;
            *pending_membership = None;
            sync_res?;
            Ok(())
        };

        for entry in entries {
            match &entry.payload {
                EntryPayload::Blank => res.push("".to_string()),
                EntryPayload::Normal(req) => {
                    // Structured commands (the cluster write path): one
                    // entry = one atomic batch of sets and deletes. The
                    // legacy "SET <key> <value>" text form still applies,
                    // so pre-cluster log entries and the storage tests
                    // keep their meaning.
                    if let Some(mut cmd) = crate::raft_command::RaftCommand::decode(req) {
                        let system_key = cmd.sets.iter().any(|op| {
                            op.key == "__COMMIT_MARKER__" || op.key.starts_with("__sys__/raft/")
                        }) || cmd
                            .dels
                            .iter()
                            .any(|k| k == "__COMMIT_MARKER__" || k.starts_with("__sys__/raft/"));
                        let conflicting_2pc =
                            crate::dist_txn::conflicts_with_2pc_decision(&self.db, &batch, &cmd)
                                .map_err(io_err)?;
                        if system_key || conflicting_2pc || cmd.is_empty() {
                            res.push("ERR".to_string());
                            // A rejected entry is still DECIDED and applied —
                            // advance `last_applied` so the next durable flush
                            // covers it atomically and replay never re-evaluates
                            // it against later entries' state.
                            last_applied = Some(entry.log_id);
                            continue;
                        }
                        // Flush at the ENTRY boundary (never mid-command):
                        // a reader or crash between commits must never see
                        // part of this entry's writes. Also flush whenever
                        // this entry touches any key already staged in the
                        // current batch so conflicting sets/deletes across
                        // entries never reorder or share a commit marker.
                        let touches_staged_key = !staged_keys.is_empty()
                            && (cmd
                                .sets
                                .iter()
                                .any(|op| staged_keys.contains(op.key.as_bytes()))
                                || cmd.dels.iter().any(|k| staged_keys.contains(k.as_bytes())));
                        if !batch.is_empty()
                            && (touches_staged_key
                                || batch.op_count() + cmd.sets.len() + cmd.dels.len()
                                    > WriteBatch::MAX_OPS)
                        {
                            flush_staged(
                                &mut batch,
                                &mut staged_slots,
                                &mut staged_keys,
                                &mut res,
                                &mut last_applied,
                                &mut new_membership,
                            )
                            .map_err(io_err)?;
                        }
                        for op in cmd.sets {
                            // The entry carries the ABSOLUTE expiry the
                            // originating node computed — apply it verbatim.
                            // Re-deriving `now + ttl` here would drift the
                            // expiry forward by the replication delay on
                            // every follower hop.
                            staged_keys.insert(op.key.as_bytes().to_vec());
                            batch
                                .set_with_expiry(&op.key, op.value, op.ttl)
                                .map_err(|e| storage_write_err(&e))?;
                        }
                        for key in &cmd.dels {
                            batch.delete(key).map_err(|e| storage_write_err(&e))?;
                            staged_keys.insert(key.as_bytes().to_vec());
                        }
                        // Placeholder: stamped with the batch's commit
                        // marker when it flushes. Empty until then.
                        res.push(String::new());
                        staged_slots.push((res.len() - 1, cmd.ssi.take()));
                    } else if let Some(rest) = req.strip_prefix("SET ") {
                        let parts: Vec<&str> = rest.splitn(2, ' ').collect();
                        if parts.len() == 2
                            && parts[0] != "__COMMIT_MARKER__"
                            && !parts[0].starts_with("__sys__/raft/")
                            && !parts[0].starts_with("__2PC_")
                            && !crate::dist_txn::key_is_reserved_by_prepared_2pc(
                                &self.db, &batch, parts[0],
                            )
                            .map_err(io_err)?
                        {
                            let touches_staged_key = staged_keys.contains(parts[0].as_bytes());
                            if !batch.is_empty()
                                && (touches_staged_key
                                    || batch.op_count() + 1 > WriteBatch::MAX_OPS)
                            {
                                flush_staged(
                                    &mut batch,
                                    &mut staged_slots,
                                    &mut staged_keys,
                                    &mut res,
                                    &mut last_applied,
                                    &mut new_membership,
                                )
                                .map_err(io_err)?;
                            }
                            staged_keys.insert(parts[0].as_bytes().to_vec());
                            batch
                                .set(parts[0], parts[1].to_string())
                                .map_err(|e| storage_write_err(&e))?;
                            res.push(String::new());
                            staged_slots.push((res.len() - 1, None));
                        } else {
                            res.push("ERR".to_string());
                        }
                    } else {
                        res.push("ERR".to_string());
                    }
                }
                EntryPayload::Membership(m) => {
                    new_membership = Some(StoredMembership::new(Some(entry.log_id), m.clone()));
                    res.push("".to_string());
                }
            }
            last_applied = Some(entry.log_id);
        }

        // Atomically flush any remaining staged user writes together with the
        // final `last_applied` / `membership` bookmark (or commit the bookmark
        // alone when the suffix contained only blank/rejected/membership entries).
        flush_staged(
            &mut batch,
            &mut staged_slots,
            &mut staged_keys,
            &mut res,
            &mut last_applied,
            &mut new_membership,
        )
        .map_err(io_err)?;
        drop(_topology_guard);

        Ok(res)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        // Return an empty buffer that the Raft runtime will fill.
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        snap_meta: &SnapshotMeta<u64, OmniNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        use std::fs;

        let io_err = |msg: &str| StorageError::IO {
            source: StorageIOError::new(
                openraft::ErrorSubject::Store,
                openraft::ErrorVerb::Write,
                AnyError::error(msg.to_string()),
            ),
        };

        // ── Deserialize snapshot envelope ──
        let data = snapshot.into_inner();
        // Check the version BEFORE the full deserialize. A snapshot from
        // pre-SSI-history code has the same envelope shape MINUS
        // ssi_history, so the useful error for an operator is the version
        // mismatch — not a raw "missing field" from the struct parse below,
        // which is why the field carries no serde default: after this gate
        // every accepted envelope is version 2 and MUST carry the field, so
        // anything else is a malformed snapshot that fails loudly here
        // rather than silently installing an empty history.
        let version = serde_json::from_slice::<serde_json::Value>(&data)
            .ok()
            .and_then(|v| v.get("version").and_then(|f| f.as_u64()))
            .unwrap_or(u64::MAX);
        if version != SNAPSHOT_VERSION as u64 {
            return Err(io_err(&format!(
                "Snapshot version mismatch: expected {}, got {}",
                SNAPSHOT_VERSION, version
            )));
        }
        let envelope: SnapshotEnvelope = serde_json::from_slice(&data)
            .map_err(|e| io_err(&format!("Snapshot deserialize: {}", e)))?;

        // ── Determine paths from current manifest ──
        let manifest_path = self.db.manifest_path.clone();
        let wal_path = self.db.wal_path.clone();

        // ── Phase A: Acquire EXCLUSIVE transition lock (freezes all writers) ──
        let _exclusive = self
            .db
            .transition_guard
            .write()
            .map_err(|_| io_err("transition_guard poisoned"))?;

        let data_dir = std::path::Path::new(&manifest_path)
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(std::path::Path::new("."))
            .to_path_buf();
        let wal_dir = std::path::Path::new(&wal_path)
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(std::path::Path::new("."))
            .to_path_buf();
        if wal_dir != data_dir {
            return Err(io_err(
                "snapshot install requires manifest and WAL paths in the same directory",
            ));
        }
        let tmp_dir = data_dir.join(format!(
            "omni_snapshot_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        if tmp_dir.exists() {
            fs::remove_dir_all(&tmp_dir).map_err(|e| io_err(&format!("rm tmp dir: {}", e)))?;
        }
        fs::create_dir_all(&tmp_dir).map_err(|e| io_err(&format!("mkdir tmp: {}", e)))?;
        struct TempDirGuard {
            path: std::path::PathBuf,
            active: bool,
        }
        impl TempDirGuard {
            fn disarm(&mut self) {
                self.active = false;
            }
        }
        impl Drop for TempDirGuard {
            fn drop(&mut self) {
                if self.active {
                    let _ = std::fs::remove_dir_all(&self.path);
                }
            }
        }
        let mut tmp_cleanup = TempDirGuard {
            path: tmp_dir.clone(),
            active: true,
        };

        // ── Phase C: Write snapshot entries into a fresh WriteBatch in tmp engine ──
        let manifest_file_name = std::path::Path::new(&manifest_path)
            .file_name()
            .ok_or_else(|| io_err("manifest path has no file name"))?;
        let wal_file_name = std::path::Path::new(&wal_path)
            .file_name()
            .ok_or_else(|| io_err("WAL path has no file name"))?;
        let tmp_manifest_path = tmp_dir
            .join(manifest_file_name)
            .to_string_lossy()
            .to_string();
        let tmp_wal_path = tmp_dir.join(wal_file_name).to_string_lossy().to_string();
        let tmp_heap_path = tmp_dir.join("data_heap.bin").to_string_lossy().to_string();
        let tmp_base_path = tmp_dir.join("data_base.bin").to_string_lossy().to_string();

        OmniKV::init_empty_storage_files(&tmp_base_path, &tmp_heap_path)
            .map_err(|e| io_err(&format!("init tmp storage files: {}", e)))?;
        let tmp_manifest = crate::Manifest {
            format_version: crate::MANIFEST_FORMAT_VERSION,
            heap_path: tmp_heap_path.clone(),
            base_path: tmp_base_path.clone(),
            sstables: vec![],
            l1_sstables: vec![],
            max_seq: 0,
            wal_checkpoint_seq: 0,
        };
        tmp_manifest
            .save(&tmp_manifest_path)
            .map_err(|e| io_err(&format!("save tmp manifest: {}", e)))?;

        let tmp_db = OmniKV::open(&tmp_manifest_path, &tmp_wal_path)
            .map_err(|e| io_err(&format!("open tmp db: {}", e)))?;

        let mut batch = crate::WriteBatch::new();
        for (k, v) in &envelope.entries {
            // The captured dataset can exceed one batch's op cap; stage
            // into a fresh tmp batch per chunk so a large snapshot
            // installs without BatchTooLarge.
            if batch.op_count() >= crate::WriteBatch::MAX_OPS {
                tmp_db
                    .commit_batch_local(&batch)
                    .map_err(|e| io_err(&format!("commit snapshot batch: {}", e)))?;
                batch = crate::WriteBatch::new();
            }
            let value = crate::dist_txn::sanitize_2pc_entry_for_snapshot_install(k, v)
                .unwrap_or_else(|| v.clone());
            batch
                .set(k, value)
                .map_err(|e| io_err(&format!("batch set: {}", e)))?;
        }

        // ── Atomically include Raft metadata in the snapshot build ──
        // If the entries filled the batch to EXACTLY the cap, the meta
        // set below would hit BatchTooLarge and the snapshot install
        // would fail. Commit the full data chunk first (same guard as the
        // apply path): data before pointer.
        if batch.op_count() >= crate::WriteBatch::MAX_OPS {
            tmp_db
                .commit_batch_local(&batch)
                .map_err(|e| io_err(&format!("commit snapshot batch: {}", e)))?;
            batch = crate::WriteBatch::new();
        }
        let next_meta = {
            let meta = self
                .meta
                .lock()
                .expect("RaftStorage meta lock poisoned: fatal invariant");
            let mut next = meta.clone();
            next.last_applied = snap_meta.last_log_id;
            next.last_log_id = snap_meta.last_log_id;
            next.membership = snap_meta.last_membership.clone();
            next.last_purged_log_id = snap_meta.last_log_id;
            let json = serde_json::to_string(&next).map_err(|e| storage_write_err(&e))?;
            batch
                .set(RAFT_META_KEY, json)
                .map_err(|e| io_err(&format!("meta set: {}", e)))?;
            next
        };

        if !batch.is_empty() {
            tmp_db
                .commit_batch_local(&batch)
                .map_err(|e| io_err(&format!("commit snapshot batch: {}", e)))?;
            tmp_db
                .compact_sstables()
                .map_err(|e| io_err(&format!("compact snapshot: {}", e)))?;
        }
        // Rebuild and validate the 2PC prepared-reservation map from the staged
        // database BEFORE promoting files or publishing live roots/metadata so
        // any scan/read error leaves the original live state untouched.
        let prepared_reservations = crate::dist_txn::collect_prepared_reservations(&tmp_db)
            .map_err(|e| io_err(&format!("sync 2PC prepared reservations: {e}")))?;
        drop(tmp_db);

        let rebase_snapshot_path = |path: &str| -> Option<String> {
            let file_name = std::path::Path::new(path).file_name()?;
            Some(data_dir.join(file_name).to_string_lossy().to_string())
        };
        let mut installed_manifest = crate::Manifest::load(&tmp_manifest_path)
            .map_err(|e| io_err(&format!("load installed snapshot manifest: {e}")))?;
        installed_manifest.heap_path = rebase_snapshot_path(&installed_manifest.heap_path)
            .ok_or_else(|| {
                io_err(&format!(
                    "snapshot path has no file name: {}",
                    installed_manifest.heap_path
                ))
            })?;
        installed_manifest.base_path = rebase_snapshot_path(&installed_manifest.base_path)
            .ok_or_else(|| {
                io_err(&format!(
                    "snapshot path has no file name: {}",
                    installed_manifest.base_path
                ))
            })?;
        let mut rebased_sstables = Vec::with_capacity(installed_manifest.sstables.len());
        for path in &installed_manifest.sstables {
            rebased_sstables.push(
                rebase_snapshot_path(path)
                    .ok_or_else(|| io_err(&format!("snapshot path has no file name: {path}")))?,
            );
        }
        installed_manifest.sstables = rebased_sstables;
        let mut rebased_l1_sstables = Vec::with_capacity(installed_manifest.l1_sstables.len());
        for path in &installed_manifest.l1_sstables {
            rebased_l1_sstables.push(
                rebase_snapshot_path(path)
                    .ok_or_else(|| io_err(&format!("snapshot path has no file name: {path}")))?,
            );
        }
        installed_manifest.l1_sstables = rebased_l1_sstables;
        let cur_seq = self.db.global_seq.load(Ordering::SeqCst);
        let prev_floor = self.db.ssi_history().eviction_floor();
        let prev_read_floor = self.db.load_roots().read_seq_floor;
        let snapshot_floor = envelope
            .max_seq
            .max(installed_manifest.max_seq)
            .max(cur_seq.saturating_add(1))
            .max(prev_floor)
            .max(prev_read_floor)
            .max(envelope.ssi_eviction_floor);
        if snapshot_floor == u64::MAX {
            return Err(io_err("snapshot sequence floor overflow"));
        }
        installed_manifest.max_seq = snapshot_floor;
        installed_manifest
            .save(&tmp_manifest_path)
            .map_err(|e| io_err(&format!("save installed snapshot manifest: {e}")))?;

        // ── Phase D: Same-filesystem directory promotion ──
        // Never delete a rollback directory left by an unresolved failed
        // promotion/rollback — it may hold the only copy of the original
        // manifest and data files. Retire only directories explicitly marked
        // `.install_complete`, and refuse promotion if any unresolved rollback
        // directory remains.
        let is_rollback_dir_name = |name: &std::ffi::OsStr| -> bool {
            name == LEGACY_OLD_SNAPSHOT_DIR
                || name
                    .to_str()
                    .is_some_and(|s| s.starts_with(OLD_SNAPSHOT_DIR_PREFIX))
        };
        for entry in
            fs::read_dir(&data_dir).map_err(|e| io_err(&format!("read data dir: {}", e)))?
        {
            let entry = entry.map_err(|e| io_err(&format!("read data dir entry: {}", e)))?;
            let name = entry.file_name();
            if is_rollback_dir_name(&name) {
                let path = entry.path();
                if path.join(INSTALL_COMPLETE_MARKER).exists()
                    && !path.join(ROLLBACK_FAILED_MARKER).exists()
                {
                    let _ = retire_completed_rollback_dir(&path);
                } else if path.is_dir() && is_empty_dir(&path).unwrap_or(false) {
                    // An empty rollback dir can only remain after a clean
                    // rollback or a crash between deleting `.install_complete`
                    // and removing the now-empty directory; remove it instead
                    // of wedging future snapshot installations.
                    let _ = fs::remove_dir(&path);
                } else {
                    return Err(io_err(&format!(
                        "unresolved snapshot rollback directory '{}' exists; restore or set it aside before installing another snapshot",
                        name.to_string_lossy()
                    )));
                }
            }
        }
        let rollback_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let rollback_seq = NEXT_SNAPSHOT_ROLLBACK_SEQ.fetch_add(1, Ordering::Relaxed);
        let old_dir_name = std::ffi::OsString::from(format!(
            "{OLD_SNAPSHOT_DIR_PREFIX}{rollback_nanos:x}_{rollback_seq:x}"
        ));
        let old_dir = data_dir.join(&old_dir_name);
        fs::create_dir_all(&old_dir)
            .map_err(|e| io_err(&format!("create old snapshot dir: {e}")))?;

        let tmp_dir_name = tmp_dir
            .file_name()
            .ok_or_else(|| io_err("temporary snapshot directory has no file name"))?
            .to_os_string();
        let manifest_file_name = std::path::Path::new(&manifest_path)
            .file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("manifest.json"));
        let mut moved_to_old: Vec<std::ffi::OsString> = Vec::new();
        let mut promoted_from_tmp: Vec<std::ffi::OsString> = Vec::new();
        let rollback_promotion = |moved_to_old: &[std::ffi::OsString],
                                  promoted_from_tmp: &[std::ffi::OsString],
                                  tmp_cleanup: &mut TempDirGuard| {
            let mut rollback_ok = true;
            for name in promoted_from_tmp.iter().rev() {
                if fs::rename(data_dir.join(name), tmp_dir.join(name)).is_err() {
                    rollback_ok = false;
                }
            }
            for name in moved_to_old.iter().rev() {
                if fs::rename(old_dir.join(name), data_dir.join(name)).is_err() {
                    rollback_ok = false;
                }
            }
            if rollback_ok
                && crate::failpoints::maybe_fail("snapshot::rollback_sync_data_dir")
                    .and_then(|()| sync_dir(&data_dir))
                    .is_err()
            {
                rollback_ok = false;
            }
            if !rollback_ok {
                // Preserve both `old_dir` and `tmp_dir` for recovery whenever
                // any rollback rename fails; mark the rollback unresolved so
                // future installs refuse to delete these recovery files.
                let _ = write_durable_marker(
                    &old_dir.join(ROLLBACK_FAILED_MARKER),
                    b"rollback_incomplete",
                );
                tmp_cleanup.disarm();
            } else if fs::remove_dir(&old_dir).is_err() {
                let _ =
                    write_durable_marker(&old_dir.join(INSTALL_COMPLETE_MARKER), b"rolled_back");
            }
        };

        // Move current files to old_dir with the manifest moved FIRST so a
        // reverse rollback restores all data files before restoring the manifest.
        let mut current_names: Vec<std::ffi::OsString> = Vec::new();
        for entry in
            fs::read_dir(&data_dir).map_err(|e| io_err(&format!("read data dir: {}", e)))?
        {
            let entry = match entry {
                Ok(entry) => entry,
                Err(e) => {
                    rollback_promotion(&moved_to_old, &promoted_from_tmp, &mut tmp_cleanup);
                    return Err(io_err(&format!("read data dir entry: {}", e)));
                }
            };
            let name = entry.file_name();
            if name != "LOCK"
                && name != tmp_dir_name
                && name != old_dir_name
                && !is_rollback_dir_name(&name)
            {
                current_names.push(name);
            }
        }
        current_names.sort_by(|a, b| {
            (a.as_os_str() != manifest_file_name)
                .cmp(&(b.as_os_str() != manifest_file_name))
                .then_with(|| a.cmp(b))
        });
        for name in current_names {
            if let Err(e) = fs::rename(data_dir.join(&name), old_dir.join(&name)) {
                rollback_promotion(&moved_to_old, &promoted_from_tmp, &mut tmp_cleanup);
                return Err(io_err(&format!(
                    "move current snapshot file {}: {}",
                    name.to_string_lossy(),
                    e
                )));
            }
            moved_to_old.push(name);
            if let Err(e) = crate::failpoints::maybe_fail("snapshot::after_move_to_old") {
                rollback_promotion(&moved_to_old, &promoted_from_tmp, &mut tmp_cleanup);
                return Err(io_err(&format!(
                    "injected failure after moving old snapshot file: {e}"
                )));
            }
        }
        // Durability barrier BEFORE promoting any new snapshot file:
        // durably record both `old_dir` (backup filenames) and `data_dir`
        // (removal of old live filenames) so a power loss during promotion
        // can never lose the original database backup.
        if let Err(e) = crate::failpoints::maybe_fail("snapshot::sync_old_dir")
            .and_then(|()| sync_dir(&old_dir))
        {
            rollback_promotion(&moved_to_old, &promoted_from_tmp, &mut tmp_cleanup);
            return Err(io_err(&format!(
                "sync rollback snapshot directory before promotion: {e}"
            )));
        }
        if let Err(e) = crate::failpoints::maybe_fail("snapshot::sync_data_dir_pre_promote")
            .and_then(|()| sync_dir(&data_dir))
        {
            rollback_promotion(&moved_to_old, &promoted_from_tmp, &mut tmp_cleanup);
            return Err(io_err(&format!(
                "sync data directory before promotion: {e}"
            )));
        }

        // Move tmp files to data_dir with the manifest promoted LAST so data
        // files (heap, SSTables, bloom, WAL) are in place before the manifest
        // points at them, and rollback removes the manifest first.
        let tmp_entries = match fs::read_dir(&tmp_dir) {
            Ok(entries) => entries,
            Err(e) => {
                rollback_promotion(&moved_to_old, &promoted_from_tmp, &mut tmp_cleanup);
                return Err(io_err(&format!("read temp snapshot dir: {}", e)));
            }
        };
        let mut tmp_names: Vec<std::ffi::OsString> = Vec::new();
        for entry in tmp_entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(e) => {
                    rollback_promotion(&moved_to_old, &promoted_from_tmp, &mut tmp_cleanup);
                    return Err(io_err(&format!("read temp snapshot entry: {}", e)));
                }
            };
            let name = entry.file_name();
            if name == "LOCK" {
                continue;
            }
            tmp_names.push(name);
        }
        tmp_names.sort_by(|a, b| {
            (a.as_os_str() == manifest_file_name)
                .cmp(&(b.as_os_str() == manifest_file_name))
                .then_with(|| a.cmp(b))
        });
        for name in tmp_names {
            if let Err(e) = fs::rename(tmp_dir.join(&name), data_dir.join(&name)) {
                rollback_promotion(&moved_to_old, &promoted_from_tmp, &mut tmp_cleanup);
                return Err(io_err(&format!(
                    "install snapshot file {}: {}",
                    name.to_string_lossy(),
                    e
                )));
            }
            promoted_from_tmp.push(name);
            if let Err(e) = crate::failpoints::maybe_fail("snapshot::after_promote_to_live") {
                rollback_promotion(&moved_to_old, &promoted_from_tmp, &mut tmp_cleanup);
                return Err(io_err(&format!(
                    "injected failure after promoting snapshot file: {e}"
                )));
            }
        }
        // Durability barrier: require `data_dir` directory sync to succeed
        // before publishing live roots/metadata or retiring the rollback backup.
        if let Err(e) = crate::failpoints::maybe_fail("snapshot::sync_data_dir_post_promote")
            .and_then(|()| sync_dir(&data_dir))
        {
            rollback_promotion(&moved_to_old, &promoted_from_tmp, &mut tmp_cleanup);
            return Err(io_err(&format!("sync promoted snapshot directory: {e}")));
        }

        // ── Phase E: Recover fresh storage from installed snapshot ──
        let recovered = match OmniKV::recover_storage_roots(&manifest_path, &wal_path) {
            Ok(recovered) => recovered,
            Err(e) => {
                rollback_promotion(&moved_to_old, &promoted_from_tmp, &mut tmp_cleanup);
                return Err(io_err(&format!("recover snapshot storage: {}", e)));
            }
        };

        // CRITICAL: Advance global_seq strictly past the persisted snapshot
        // floor (and any recovered sequence) BEFORE publishing `new_roots` so
        // every pre-install `read_seq` sits strictly below `read_seq_floor`
        // while post-install snapshots (`global_seq - 1 == final_floor`) pass,
        // and no lock-free reader can observe `new_roots` alongside a stale
        // sequence counter, SSI history, 2PC reservation table, or Raft meta.
        let final_floor = snapshot_floor.max(recovered.max_seq);
        let next_global_seq = match final_floor.checked_add(1) {
            Some(next) => next,
            None => {
                rollback_promotion(&moved_to_old, &promoted_from_tmp, &mut tmp_cleanup);
                return Err(io_err("snapshot sequence floor overflow"));
            }
        };

        // ── Phase F: Swap mutable write handles & advance sequence/SSI/meta state ──
        *self
            .db
            .heap_file
            .lock()
            .expect("heap_file lock poisoned: fatal invariant") = recovered.heap_file;
        *self
            .db
            .wal
            .lock()
            .expect("wal lock poisoned: fatal invariant") = recovered.wal;
        self.db
            .heap_offset
            .store(recovered.heap_offset, Ordering::Release);
        self.db.global_seq.store(next_global_seq, Ordering::SeqCst);

        // The snapshot replaced the data wholesale, so the committed
        // history must be replaced too — otherwise a node promoted after
        // installing it validates against a history missing every commit
        // the snapshot carried (they never passed through its log apply).
        // The carried markers are ≤ max_seq, which the bump above keeps
        // below everything the node writes next, so they stay ordered
        // correctly against its own future commits.
        self.db.ssi_history().install_from(
            envelope
                .ssi_history
                .into_iter()
                .map(|e| (e.txn_id, e.commit_seq, e.record))
                .collect(),
            envelope.ssi_eviction_floor,
        );
        self.db.ssi_history().advance_eviction_floor(final_floor);
        // Publish the pre-validated 2PC prepared-reservation map infallibly
        // alongside the installed roots and Raft metadata.
        self.db
            .ssi_history()
            .replace_all_prepared_keys(prepared_reservations);

        // Publish in-memory Raft metadata only after disk promotion and
        // storage recovery succeeded so a failed install cannot diverge
        // in-memory metadata from on-disk state.
        *self
            .meta
            .lock()
            .expect("RaftStorage meta lock poisoned: fatal invariant") = next_meta;

        // ── Phase G: Single atomic StorageRoots publish ──
        let new_roots = crate::StorageRoots {
            base_mmap: recovered.base_mmap,
            base_bloom: recovered.base_bloom,
            sstables: recovered.sstables,
            l1_sstables: recovered.l1_sstables,
            memtable: recovered.memtable,
            frozen_memtables: Arc::new(Vec::new()),
            manifest: recovered.manifest,
            heap_reader: recovered.heap_reader,
            heap_generation: self.db.next_heap_generation.fetch_add(1, Ordering::Relaxed),
            read_seq_floor: final_floor,
        };
        self.db.roots.store(Arc::new(new_roots));
        self.db.block_cache.invalidate_all();

        // ── Phase H: Durably mark completion and retire rollback backup ──
        // Promoted filenames were already synced in Phase D before live
        // publication; durably record `.install_complete` (syncing marker and
        // rollback dir) before retiring backups. If persisting that marker
        // fails, preserve `old_dir` and surface an explicit recovery-required
        // error instead of reporting clean success.
        if let Err(e) = crate::failpoints::maybe_fail("snapshot::write_install_complete")
            .and_then(|()| write_durable_marker(&old_dir.join(INSTALL_COMPLETE_MARKER), b"ok"))
        {
            drop(_exclusive);
            return Err(io_err(&format!(
                "snapshot promoted live, but persisting completion marker in '{}' failed ({e}); rollback directory preserved — operator recovery required before restart or next snapshot install",
                old_dir.display()
            )));
        }
        let _ = retire_completed_rollback_dir(&old_dir);

        // ── Phase I: Release exclusive lock (writers resume on new topology) ──
        drop(_exclusive);

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::{CommittedLeaderId, EntryPayload, RaftStorage};

    #[tokio::test]
    async fn delete_conflict_logs_since_rejects_corrupt_predecessor_above_purge_floor() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manifest = dir.path().join("manifest.json");
        let wal = dir.path().join("wal.bin");
        let db = OmniKV::open(
            manifest.to_str().expect("manifest path"),
            wal.to_str().expect("wal path"),
        )
        .expect("open db");
        let mut storage = OmniRaftStorage::new(db.clone());
        let leader1 = CommittedLeaderId::new(1, 1);

        storage
            .append_to_log(vec![
                Entry {
                    log_id: LogId::new(leader1, 1),
                    payload: EntryPayload::Normal("SET k1 v1".into()),
                },
                Entry {
                    log_id: LogId::new(leader1, 2),
                    payload: EntryPayload::Normal("SET k2 v2".into()),
                },
                Entry {
                    log_id: LogId::new(leader1, 3),
                    payload: EntryPayload::Normal("SET k3 v3".into()),
                },
                Entry {
                    log_id: LogId::new(leader1, 4),
                    payload: EntryPayload::Normal("SET k4 v4".into()),
                },
            ])
            .await
            .expect("append logs");
        storage
            .purge_logs_upto(LogId::new(leader1, 1))
            .await
            .expect("purge index 1");

        // Corrupt predecessor index 3 (above purge floor 1) directly in local storage.
        let mut corrupt_wb = WriteBatch::new();
        corrupt_wb
            .set(&format!("{RAFT_LOG_PREFIX}{:020}", 3u64), "{corrupt".into())
            .expect("stage corrupt log");
        db.commit_batch_local(&corrupt_wb)
            .expect("commit corrupt log");

        assert!(
            storage
                .delete_conflict_logs_since(LogId::new(leader1, 4))
                .await
                .is_err(),
            "corrupt predecessor above purge floor must return StorageError"
        );
        assert_eq!(
            storage
                .get_log_state()
                .await
                .expect("log state")
                .last_log_id,
            Some(LogId::new(leader1, 4)),
            "failed truncation must not report purge floor as last_log_id"
        );
    }

    #[tokio::test]
    async fn try_get_log_entries_fails_closed_on_corrupt_entry() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manifest = dir.path().join("manifest.json");
        let wal = dir.path().join("wal.bin");
        let db = OmniKV::open(
            manifest.to_str().expect("manifest path"),
            wal.to_str().expect("wal path"),
        )
        .expect("open db");
        let mut storage = OmniRaftStorage::new(db.clone());
        let leader1 = CommittedLeaderId::new(1, 1);

        storage
            .append_to_log(vec![
                Entry {
                    log_id: LogId::new(leader1, 1),
                    payload: EntryPayload::Normal("SET k1 v1".into()),
                },
                Entry {
                    log_id: LogId::new(leader1, 2),
                    payload: EntryPayload::Normal("SET k2 v2".into()),
                },
            ])
            .await
            .expect("append logs");

        let mut corrupt_wb = WriteBatch::new();
        corrupt_wb
            .set(
                &format!("{RAFT_LOG_PREFIX}{:020}", 2u64),
                "{corrupt_log".into(),
            )
            .expect("stage corrupt log");
        db.commit_batch_local(&corrupt_wb)
            .expect("commit corrupt log");

        let err = storage
            .try_get_log_entries(1..=2)
            .await
            .expect_err("corrupt raft log entry must fail closed instead of silently truncating");
        assert!(
            err.to_string().contains("corrupt raft log at index 2"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn delete_conflict_logs_since_truncates_multi_chunk_tail_cleanly() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manifest = dir.path().join("manifest.json");
        let wal = dir.path().join("wal.bin");
        let db = OmniKV::open(
            manifest.to_str().expect("manifest path"),
            wal.to_str().expect("wal path"),
        )
        .expect("open db");
        let mut storage = OmniRaftStorage::new(db.clone());
        let leader1 = CommittedLeaderId::new(1, 1);

        let total = WriteBatch::MAX_OPS as u64 + 25;
        let entries: Vec<Entry<TypeConfig>> = (1..=total)
            .map(|idx| Entry {
                log_id: LogId::new(leader1, idx),
                payload: EntryPayload::Blank,
            })
            .collect();
        storage.append_to_log(entries).await.expect("append tail");

        storage
            .delete_conflict_logs_since(LogId::new(leader1, 3))
            .await
            .expect("high-to-low chunked conflict truncation must succeed");

        assert_eq!(
            storage
                .get_log_state()
                .await
                .expect("log state")
                .last_log_id,
            Some(LogId::new(leader1, 2))
        );
        let reloaded = OmniRaftStorage::new(db);
        assert!(
            reloaded.read_log(2).is_some(),
            "surviving entry 2 must remain on disk"
        );
        assert_eq!(
            reloaded.read_log(3),
            None,
            "conflict start entry 3 must be deleted"
        );
        assert_eq!(
            reloaded.read_log(total),
            None,
            "conflict tail end entry must be deleted"
        );
    }

    #[tokio::test]
    async fn purge_logs_upto_persists_floor_per_chunk_and_handles_exact_cap_minus_one() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manifest = dir.path().join("manifest.json");
        let wal = dir.path().join("wal.bin");
        let db = OmniKV::open(
            manifest.to_str().expect("manifest path"),
            wal.to_str().expect("wal path"),
        )
        .expect("open db");
        let mut storage = OmniRaftStorage::new(db.clone());
        let leader1 = CommittedLeaderId::new(1, 1);

        let exact_cap_minus_one = WriteBatch::MAX_OPS.saturating_sub(1) as u64;
        let total = exact_cap_minus_one + 15;
        let entries: Vec<Entry<TypeConfig>> = (1..=total)
            .map(|idx| Entry {
                log_id: LogId::new(leader1, idx),
                payload: EntryPayload::Blank,
            })
            .collect();
        storage.append_to_log(entries).await.expect("append logs");

        // Purge exactly MAX_OPS - 1 entries (single atomic chunk with meta).
        storage
            .purge_logs_upto(LogId::new(leader1, exact_cap_minus_one))
            .await
            .expect("exact cap-1 purge must succeed");
        assert_eq!(
            storage
                .get_log_state()
                .await
                .expect("log state")
                .last_purged_log_id,
            Some(LogId::new(leader1, exact_cap_minus_one))
        );
        assert_eq!(storage.read_log(exact_cap_minus_one), None);
        assert!(storage.read_log(exact_cap_minus_one + 1).is_some());

        // Purge a multi-chunk range cleanly and verify reloaded floor.
        let more_start = total + 1;
        let more_end = more_start + WriteBatch::MAX_OPS as u64 + 10;
        let more_entries: Vec<Entry<TypeConfig>> = (more_start..=more_end)
            .map(|idx| Entry {
                log_id: LogId::new(leader1, idx),
                payload: EntryPayload::Blank,
            })
            .collect();
        storage
            .append_to_log(more_entries)
            .await
            .expect("append second wave");
        storage
            .purge_logs_upto(LogId::new(leader1, more_end - 2))
            .await
            .expect("multi-chunk purge must succeed");

        let mut reloaded = OmniRaftStorage::new(db);
        let state = reloaded.meta.lock().expect("meta lock").last_purged_log_id;
        assert_eq!(state, Some(LogId::new(leader1, more_end - 2)));
        assert_eq!(reloaded.read_log(more_end - 2), None);
        assert!(reloaded.read_log(more_end - 1).is_some());

        // Purging an older LogId must be a no-op and never regress `last_purged_log_id`.
        reloaded
            .purge_logs_upto(LogId::new(leader1, 500))
            .await
            .expect("older purge must succeed as no-op");
        let after_older = reloaded.meta.lock().expect("meta lock").last_purged_log_id;
        assert_eq!(after_older, Some(LogId::new(leader1, more_end - 2)));
    }

    #[tokio::test]
    async fn build_snapshot_propagates_heap_read_error_instead_of_truncating() {
        use openraft::RaftSnapshotBuilder;
        use std::io::{Seek, SeekFrom, Write};

        let dir = tempfile::tempdir().expect("tempdir");
        let manifest = dir.path().join("manifest.json");
        let wal = dir.path().join("wal.bin");
        let db = OmniKV::open(
            manifest.to_str().expect("manifest path"),
            wal.to_str().expect("wal path"),
        )
        .expect("open db");
        let mut storage = OmniRaftStorage::new(db.clone());
        let leader1 = CommittedLeaderId::new(1, 1);

        storage
            .apply_to_state_machine(&[
                Entry {
                    log_id: LogId::new(leader1, 1),
                    payload: EntryPayload::Normal("SET snap_a val_a".into()),
                },
                Entry {
                    log_id: LogId::new(leader1, 2),
                    payload: EntryPayload::Normal("SET snap_b val_b".into()),
                },
                Entry {
                    log_id: LogId::new(leader1, 3),
                    payload: EntryPayload::Normal("SET snap_c val_c".into()),
                },
            ])
            .await
            .expect("apply snapshot entries");

        // Corrupt bytes inside the active heap file so reading stored values
        // fails CRC verification instead of returning a truncated snapshot.
        let heap_path = db.load_roots().manifest.heap_path.clone();
        let mut heap = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&heap_path)
            .expect("open heap file");
        let heap_len = heap.metadata().expect("heap metadata").len();
        assert!(heap_len > 0, "heap must hold committed entries");
        heap.seek(SeekFrom::Start(0)).expect("seek heap");
        heap.write_all(&vec![0xFFu8; heap_len as usize])
            .expect("corrupt heap bytes");
        heap.sync_all().expect("sync corrupted heap");
        drop(heap);

        let err = storage
            .build_snapshot()
            .await
            .expect_err("heap CRC error during snapshot capture must fail closed");
        assert!(
            err.to_string().contains("snapshot heap read failed"),
            "unexpected snapshot error: {err}"
        );
    }

    #[tokio::test]
    async fn purge_logs_upto_rejects_corrupt_and_mismatched_intermediate_boundaries() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manifest = dir.path().join("manifest.json");
        let wal = dir.path().join("wal.bin");
        let db = OmniKV::open(
            manifest.to_str().expect("manifest path"),
            wal.to_str().expect("wal path"),
        )
        .expect("open db");
        let mut storage = OmniRaftStorage::new(db.clone());
        let leader1 = CommittedLeaderId::new(1, 1);
        let boundary = WriteBatch::MAX_OPS as u64 - 1;
        let total = boundary + 5;

        let mut chunk = Vec::with_capacity(2_500);
        for idx in 1..=total {
            chunk.push(Entry {
                log_id: LogId::new(leader1, idx),
                payload: EntryPayload::Blank,
            });
            if chunk.len() == 2_500 {
                storage
                    .append_to_log(std::mem::take(&mut chunk))
                    .await
                    .expect("append chunk");
            }
        }
        if !chunk.is_empty() {
            storage.append_to_log(chunk).await.expect("append tail");
        }

        // Case 1: invalid JSON at intermediate boundary must fail BEFORE deleting the chunk.
        storage
            .append_log(boundary, "{corrupt json")
            .expect("seed corrupt boundary");
        let err = storage
            .purge_logs_upto(LogId::new(leader1, total))
            .await
            .expect_err("corrupt intermediate boundary must fail");
        assert!(err.to_string().contains("corrupt Raft log entry"));
        assert!(storage.read_log(1).is_some(), "chunk 1 must stay intact");
        assert_eq!(
            storage.meta.lock().expect("meta lock").last_purged_log_id,
            None
        );

        // Case 2: valid JSON with mismatched index at boundary must also fail closed.
        let wrong_entry: Entry<TypeConfig> = Entry {
            log_id: LogId::new(leader1, boundary + 42),
            payload: EntryPayload::Blank,
        };
        let wrong_json = serde_json::to_string(&wrong_entry).expect("serialize mismatched entry");
        storage
            .append_log(boundary, &wrong_json)
            .expect("seed mismatched boundary");
        let err2 = storage
            .purge_logs_upto(LogId::new(leader1, total))
            .await
            .expect_err("mismatched boundary index must fail");
        assert!(err2.to_string().contains("mismatched Raft log index"));
        assert!(
            storage.read_log(1).is_some(),
            "chunk 1 must still stay intact"
        );
        assert_eq!(
            storage.meta.lock().expect("meta lock").last_purged_log_id,
            None
        );
    }

    #[tokio::test]
    async fn apply_to_state_machine_persists_last_applied_with_each_intermediate_flush() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manifest = dir.path().join("manifest.json");
        let wal = dir.path().join("wal.bin");
        let db = OmniKV::open(
            manifest.to_str().expect("manifest path"),
            wal.to_str().expect("wal path"),
        )
        .expect("open db");
        let mut storage = OmniRaftStorage::new(db.clone());
        let leader = CommittedLeaderId::new(1, 1);

        // Entry 1 reserves key `k_atom` via a durable 2PC PREPARED record.
        let prep_entry = serde_json::json!({
            "txn_id": [1, 1],
            "node_id": 10,
            "state": "PREPARED",
            "writes": [{"node_id": 10, "key": "k_atom", "value": "prep", "ttl": 0}],
            "prepare_seq": 1,
            "commit_seq": null,
            "snapshot_imported": false
        });
        let mut b1 = WriteBatch::new();
        b1.set("__2PC_PREPARE__/10/1_1", prep_entry.to_string())
            .expect("stage prepare");
        let cmd1 = crate::raft_command::RaftCommand::from_batch(&b1).encode();
        storage
            .apply_to_state_machine(&[Entry {
                log_id: LogId::new(leader, 1),
                payload: EntryPayload::Normal(cmd1),
            }])
            .await
            .expect("apply prepare");

        // Entry 2 tries to write reserved `k_atom` (rejected ERR);
        // Entry 3 aborts owner (flushed atomically with last_applied=3 before entry 4);
        // Entry 4 writes `k_atom` touching the same key so entry 3 must flush first.
        let mut b2 = WriteBatch::new();
        b2.set("k_atom", "must_reject".into()).expect("stage b2");
        let abort_entry = serde_json::json!({
            "txn_id": [1, 1],
            "node_id": 10,
            "state": "ABORTED",
            "writes": [],
            "prepare_seq": 0,
            "commit_seq": null,
            "snapshot_imported": false
        });
        let mut b3 = WriteBatch::new();
        b3.set("__2PC_PREPARE__/10/1_1", abort_entry.to_string())
            .expect("stage abort");
        let mut b4 = WriteBatch::new();
        b4.set("__2PC_PREPARE__/10/1_1", abort_entry.to_string())
            .expect("stage touch");
        b4.set("k_atom", "after_abort".into()).expect("stage write");

        let res = storage
            .apply_to_state_machine(&[
                Entry {
                    log_id: LogId::new(leader, 2),
                    payload: EntryPayload::Normal(
                        crate::raft_command::RaftCommand::from_batch(&b2).encode(),
                    ),
                },
                Entry {
                    log_id: LogId::new(leader, 3),
                    payload: EntryPayload::Normal(
                        crate::raft_command::RaftCommand::from_batch(&b3).encode(),
                    ),
                },
                Entry {
                    log_id: LogId::new(leader, 4),
                    payload: EntryPayload::Normal(
                        crate::raft_command::RaftCommand::from_batch(&b4).encode(),
                    ),
                },
            ])
            .await
            .expect("apply entries 2..=4");
        assert_eq!(res[0], "ERR");
        assert_ne!(res[1], "ERR");
        assert_ne!(res[2], "ERR");
        // Verify durable meta persisted AT the intermediate abort commit marker
        // already covers entries 2 (rejected) and 3 (abort) before entry 4 flushed.
        let abort_marker: u64 = res[1].parse().expect("parse abort commit marker");
        let mid_meta_raw = db
            .find(RAFT_META_KEY, abort_marker)
            .expect("read intermediate meta")
            .expect("intermediate meta present");
        let mid_meta: RaftStateMeta =
            serde_json::from_str(&mid_meta_raw).expect("parse intermediate meta");
        assert_eq!(
            mid_meta.last_applied,
            Some(LogId::new(leader, 3)),
            "intermediate flush must persist last_applied=3 atomically at abort marker"
        );
        // Verify durable meta persisted alongside the final flush covers all 4 entries.
        let meta_raw = db
            .find_latest_internal(RAFT_META_KEY)
            .expect("read meta")
            .expect("meta present");
        let meta: RaftStateMeta = serde_json::from_str(&meta_raw).expect("parse meta");
        assert_eq!(meta.last_applied, Some(LogId::new(leader, 4)));
        assert_eq!(
            db.find_latest_internal("k_atom").expect("read k_atom"),
            Some("after_abort".into())
        );
    }

    #[tokio::test]
    async fn install_snapshot_refuses_promotion_when_unresolved_rollback_dir_exists() {
        use openraft::RaftSnapshotBuilder;

        let src_dir = tempfile::tempdir().expect("src tempdir");
        let src_db = OmniKV::open(
            src_dir
                .path()
                .join("manifest.json")
                .to_str()
                .expect("src manifest"),
            src_dir.path().join("wal.bin").to_str().expect("src wal"),
        )
        .expect("open src db");
        let mut src_storage = OmniRaftStorage::new(src_db);
        let leader = CommittedLeaderId::new(1, 1);
        src_storage
            .apply_to_state_machine(&[Entry {
                log_id: LogId::new(leader, 1),
                payload: EntryPayload::Normal("SET snap_k v1".into()),
            }])
            .await
            .expect("apply src");
        let snapshot = src_storage.build_snapshot().await.expect("build snapshot");

        let dst_dir = tempfile::tempdir().expect("dst tempdir");
        let dst_db = OmniKV::open(
            dst_dir
                .path()
                .join("manifest.json")
                .to_str()
                .expect("dst manifest"),
            dst_dir.path().join("wal.bin").to_str().expect("dst wal"),
        )
        .expect("open dst db");
        let mut dst_storage = OmniRaftStorage::new(dst_db);

        // Plant an unresolved rollback directory containing the original manifest copy.
        let unresolved = dst_dir.path().join("old_snapshot_unresolved");
        std::fs::create_dir_all(&unresolved).expect("create unresolved dir");
        let sentinel = unresolved.join("manifest.json");
        std::fs::write(&sentinel, b"original-manifest-backup").expect("write sentinel");

        let err = dst_storage
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect_err(
                "install_snapshot must refuse promotion when unresolved rollback dir exists",
            );
        assert!(
            err.to_string()
                .contains("unresolved snapshot rollback directory"),
            "unexpected error: {err}"
        );
        assert_eq!(
            std::fs::read(&sentinel).expect("sentinel must survive untouched"),
            b"original-manifest-backup"
        );
    }

    #[tokio::test]
    async fn purge_logs_upto_rejects_corrupt_or_mismatched_final_target_log_id() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manifest = dir.path().join("manifest.json");
        let wal = dir.path().join("wal.bin");
        let db = OmniKV::open(
            manifest.to_str().expect("manifest path"),
            wal.to_str().expect("wal path"),
        )
        .expect("open db");
        let mut storage = OmniRaftStorage::new(db.clone());
        let leader1 = CommittedLeaderId::new(1, 1);
        let leader2 = CommittedLeaderId::new(2, 1);

        let entries: Vec<Entry<TypeConfig>> = (1..=5)
            .map(|idx| Entry {
                log_id: LogId::new(leader1, idx),
                payload: EntryPayload::Blank,
            })
            .collect();
        storage.append_to_log(entries).await.expect("append logs");

        // Mismatched term/leader at the final purge target must fail closed
        // before deleting any log entries.
        let err = storage
            .purge_logs_upto(LogId::new(leader2, 5))
            .await
            .expect_err("mismatched final target LogId must fail");
        assert!(
            err.to_string().contains("mismatched Raft log id"),
            "unexpected error: {err}"
        );
        assert!(storage.read_log(1).is_some(), "entry 1 must remain intact");
        assert_eq!(
            storage.meta.lock().expect("meta lock").last_purged_log_id,
            None
        );

        // Corrupt JSON at the final purge target must also fail closed.
        storage
            .append_log(5, "{corrupt target json")
            .expect("seed corrupt target");
        let err2 = storage
            .purge_logs_upto(LogId::new(leader1, 5))
            .await
            .expect_err("corrupt final target entry must fail");
        assert!(
            err2.to_string()
                .contains("corrupt Raft log entry at purge target"),
            "unexpected error: {err2}"
        );
        assert!(storage.read_log(1).is_some(), "entry 1 must remain intact");
        assert_eq!(
            storage.meta.lock().expect("meta lock").last_purged_log_id,
            None
        );

        // When the final target entry is absent (e.g., snapshot already moved
        // past local log), advancing the purge floor remains permitted.
        let mut del = WriteBatch::new();
        del.delete(&format!("{RAFT_LOG_PREFIX}{:020}", 5))
            .expect("stage delete target");
        db.commit_batch_local(&del).expect("delete target entry");
        storage
            .purge_logs_upto(LogId::new(leader1, 5))
            .await
            .expect("purge past absent target entry should succeed");
        assert_eq!(
            storage.meta.lock().expect("meta lock").last_purged_log_id,
            Some(LogId::new(leader1, 5))
        );
        assert_eq!(storage.read_log(1), None);
    }

    #[tokio::test]
    async fn install_snapshot_retires_empty_and_completed_rollback_dirs_without_wedging() {
        use openraft::RaftSnapshotBuilder;

        let src_dir = tempfile::tempdir().expect("src tempdir");
        let src_db = OmniKV::open(
            src_dir
                .path()
                .join("manifest.json")
                .to_str()
                .expect("src manifest"),
            src_dir.path().join("wal.bin").to_str().expect("src wal"),
        )
        .expect("open src db");
        let mut src_storage = OmniRaftStorage::new(src_db);
        let leader = CommittedLeaderId::new(1, 1);
        src_storage
            .apply_to_state_machine(&[Entry {
                log_id: LogId::new(leader, 1),
                payload: EntryPayload::Normal("SET snap_k v_ok".into()),
            }])
            .await
            .expect("apply src");
        let snapshot = src_storage.build_snapshot().await.expect("build snapshot");

        let dst_dir = tempfile::tempdir().expect("dst tempdir");
        let dst_db = OmniKV::open(
            dst_dir
                .path()
                .join("manifest.json")
                .to_str()
                .expect("dst manifest"),
            dst_dir.path().join("wal.bin").to_str().expect("dst wal"),
        )
        .expect("open dst db");
        let mut dst_storage = OmniRaftStorage::new(dst_db.clone());

        // 1. Empty rollback dir left by a crash between removing `.install_complete`
        //    and removing the empty directory must NOT wedge future installs.
        let empty_old = dst_dir.path().join("old_snapshot_empty_crash");
        std::fs::create_dir_all(&empty_old).expect("create empty rollback dir");

        // 2. Completed rollback dir with `.install_complete` and leftover backup file
        //    must be retired cleanly (removing backup files before the marker).
        let completed_old = dst_dir.path().join("old_snapshot_completed_retry");
        std::fs::create_dir_all(&completed_old).expect("create completed rollback dir");
        std::fs::write(completed_old.join("stale_backup.bin"), b"stale")
            .expect("write stale backup");
        write_durable_marker(&completed_old.join(INSTALL_COMPLETE_MARKER), b"ok")
            .expect("write durable install-complete marker");

        // 3. Direct guard check: `retire_completed_rollback_dir` refuses to touch
        //    a directory carrying `.rollback_failed` even if `.install_complete` exists.
        let conflicted_dir = tempfile::tempdir().expect("conflicted tempdir");
        std::fs::write(conflicted_dir.path().join("keep.bin"), b"keep").expect("write keep");
        std::fs::write(conflicted_dir.path().join(INSTALL_COMPLETE_MARKER), b"ok")
            .expect("write complete marker");
        std::fs::write(
            conflicted_dir.path().join(ROLLBACK_FAILED_MARKER),
            b"rollback_incomplete",
        )
        .expect("write failed marker");
        assert!(
            retire_completed_rollback_dir(conflicted_dir.path()).is_err(),
            "directory marked rollback_failed must never be retired automatically"
        );
        assert!(conflicted_dir.path().join("keep.bin").exists());

        dst_storage
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("install_snapshot must succeed past empty and completed rollback dirs");

        assert!(
            !empty_old.exists(),
            "empty rollback directory must be removed"
        );
        assert!(
            !completed_old.exists(),
            "completed rollback directory must be retired"
        );
        assert_eq!(
            dst_db
                .find_latest_internal("snap_k")
                .expect("read installed key"),
            Some("v_ok".into())
        );
    }

    #[tokio::test]
    async fn purge_logs_upto_advances_over_empty_local_prefix_past_batch_cap() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manifest = dir.path().join("manifest.json");
        let wal = dir.path().join("wal.bin");
        let db = OmniKV::open(
            manifest.to_str().expect("manifest path"),
            wal.to_str().expect("wal path"),
        )
        .expect("open db");
        let mut storage = OmniRaftStorage::new(db.clone());
        let leader = CommittedLeaderId::new(1, 1);

        // Seed index 0 (initial membership log entry) and a durable vote under
        // both the RaftStorage trait and legacy `__sys__/raft/log/vote` key.
        let vote = Vote::new(1, 1);
        RaftStorage::save_vote(&mut storage, &vote)
            .await
            .expect("save trait vote");
        storage
            .save_vote("{\"term\":1,\"node_id\":1}")
            .expect("save raw log-prefix vote");
        storage
            .append_to_log(vec![Entry {
                log_id: LogId::new(CommittedLeaderId::new(0, 0), 0),
                payload: EntryPayload::Blank,
            }])
            .await
            .expect("append index 0");

        // No local log entries exist past index 0, and target index exceeds MAX_OPS.
        let target = LogId::new(leader, WriteBatch::MAX_OPS as u64 + 2_000);
        storage
            .purge_logs_upto(target)
            .await
            .expect("purge over empty local prefix past batch cap must succeed");
        assert_eq!(
            storage.read_log(0),
            None,
            "log index 0 must be purged when last_purged_log_id started at None"
        );
        let reloaded = OmniRaftStorage::new(db.clone());
        assert_eq!(
            reloaded.meta.lock().expect("meta lock").last_purged_log_id,
            Some(target),
            "persisted RAFT_META_KEY must reflect advanced purge floor"
        );

        // Purging at u64::MAX must never overflow or delete `__sys__/raft/log/vote`.
        let max_target = LogId::new(leader, u64::MAX);
        storage
            .purge_logs_upto(max_target)
            .await
            .expect("purge at u64::MAX must succeed");
        assert_eq!(
            RaftStorage::read_vote(&mut storage)
                .await
                .expect("read trait vote"),
            Some(vote),
            "purge at u64::MAX must preserve trait Raft vote"
        );
        assert_eq!(
            storage.read_vote(),
            Some("{\"term\":1,\"node_id\":1}".to_string()),
            "purge at u64::MAX must preserve `__sys__/raft/log/vote`"
        );
        // Higher-term purge at u64::MAX must not overflow `id.index + 1`.
        let higher_term_max = LogId::new(CommittedLeaderId::new(2, 1), u64::MAX);
        storage
            .purge_logs_upto(higher_term_max)
            .await
            .expect("higher-term purge at u64::MAX must not overflow");
        assert_eq!(
            OmniRaftStorage::new(db)
                .meta
                .lock()
                .expect("meta lock")
                .last_purged_log_id,
            Some(higher_term_max)
        );
    }

    #[tokio::test]
    async fn purge_logs_upto_rejects_non_monotonic_intermediate_boundary_term() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manifest = dir.path().join("manifest.json");
        let wal = dir.path().join("wal.bin");
        let db = OmniKV::open(
            manifest.to_str().expect("manifest path"),
            wal.to_str().expect("wal path"),
        )
        .expect("open db");
        let mut storage = OmniRaftStorage::new(db);
        let leader2 = CommittedLeaderId::new(2, 1);
        let leader5 = CommittedLeaderId::new(5, 1);
        let boundary = WriteBatch::MAX_OPS as u64 - 1;
        let total = boundary + 5;

        let mut chunk = Vec::with_capacity(2_500);
        for idx in 1..=total {
            let entry_leader = if idx == boundary { leader5 } else { leader2 };
            chunk.push(Entry {
                log_id: LogId::new(entry_leader, idx),
                payload: EntryPayload::Blank,
            });
            if chunk.len() == 2_500 {
                storage
                    .append_to_log(std::mem::take(&mut chunk))
                    .await
                    .expect("append chunk");
            }
        }
        if !chunk.is_empty() {
            storage.append_to_log(chunk).await.expect("append tail");
        }

        let err = storage
            .purge_logs_upto(LogId::new(leader2, total))
            .await
            .expect_err("higher-term intermediate boundary above target LogId must fail");
        assert!(
            err.to_string().contains("non-monotonic Raft log id"),
            "unexpected error: {err}"
        );
        assert!(storage.read_log(1).is_some(), "chunk 1 must stay intact");
        assert_eq!(
            storage.meta.lock().expect("meta lock").last_purged_log_id,
            None
        );
    }

    #[tokio::test]
    async fn install_snapshot_invalidates_pre_install_read_only_transactions() {
        let src_dir = tempfile::tempdir().expect("src tempdir");
        let src_db = OmniKV::open(
            src_dir
                .path()
                .join("manifest.json")
                .to_str()
                .expect("src manifest"),
            src_dir.path().join("wal.bin").to_str().expect("src wal"),
        )
        .expect("open src db");
        let mut src_storage = OmniRaftStorage::new(src_db.clone());
        let leader = CommittedLeaderId::new(1, 1);
        src_storage
            .apply_to_state_machine(&[Entry {
                log_id: LogId::new(leader, 1),
                payload: EntryPayload::Normal("SET k new".into()),
            }])
            .await
            .expect("apply src write");
        let snapshot = src_storage.build_snapshot().await.expect("build snapshot");

        let dst_dir = tempfile::tempdir().expect("dst tempdir");
        let dst_db = OmniKV::open(
            dst_dir
                .path()
                .join("manifest.json")
                .to_str()
                .expect("dst manifest"),
            dst_dir.path().join("wal.bin").to_str().expect("dst wal"),
        )
        .expect("open dst db");
        let mut dst_storage = OmniRaftStorage::new(dst_db.clone());
        // Advance dst_db sequence above src_db's temporary record sequences so
        // pre-install read_seq > snapshot record seq reproduces the MVCC leak.
        let mut seed = WriteBatch::new();
        seed.set("k", "old".into()).expect("seed k");
        for i in 0..8 {
            seed.set(&format!("pad_{i}"), "x".into()).expect("seed pad");
        }
        dst_db.commit_batch_local(&seed).expect("commit dst seed");

        let tm = crate::transaction::TransactionManager::new(dst_db.clone());
        let mut txn_read = tm.begin();
        let mut txn_commit_only = tm.begin();
        assert_eq!(
            tm.get(&mut txn_commit_only, "k").expect("pre-install read"),
            Some("old".into())
        );
        let mut pre_iter = dst_db
            .scan_iter("k", "k\u{10ffff}", txn_read.read_seq)
            .expect("pre-install scan_iter");

        dst_storage
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("install snapshot");

        let get_err = tm
            .get(&mut txn_read, "k")
            .expect_err("stale read-only txn get must fail closed after install_snapshot");
        assert!(
            matches!(get_err, crate::OmniError::StaleSnapshot { .. }),
            "expected StaleSnapshot on get, got: {get_err}"
        );
        assert_eq!(txn_read.state, crate::transaction::TxnState::Aborted);
        assert!(tm.commit(&mut txn_read).is_err());

        let scan_err = dst_db
            .scan("k", "k\u{10ffff}", txn_read.read_seq)
            .expect_err("stale scan must fail closed after install_snapshot");
        assert!(
            matches!(scan_err, crate::OmniError::StaleSnapshot { .. }),
            "expected StaleSnapshot on scan, got: {scan_err}"
        );

        let commit_err = tm
            .commit(&mut txn_commit_only)
            .expect_err("pre-install read-only txn commit must fail closed after install_snapshot");
        assert!(
            matches!(commit_err, crate::OmniError::StaleSnapshot { .. }),
            "expected StaleSnapshot on read-only commit, got: {commit_err}"
        );
        assert_eq!(txn_commit_only.state, crate::transaction::TxnState::Aborted);
        assert_eq!(tm.active_count(), 0);
        assert_eq!(
            pre_iter.next(),
            Some(("k".into(), "old".into())),
            "pre-install ScanIterator must stay pinned to old heap/roots"
        );

        let mut fresh_txn = tm.begin();
        assert_eq!(
            tm.get(&mut fresh_txn, "k").expect("post-install read"),
            Some("new".into())
        );
        tm.commit(&mut fresh_txn)
            .expect("post-install read-only txn must commit cleanly");
    }

    #[tokio::test]
    async fn purge_logs_upto_rejects_interior_chunk_defects() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = OmniKV::open(
            dir.path().join("manifest.json").to_str().expect("manifest"),
            dir.path().join("wal.bin").to_str().expect("wal"),
        )
        .expect("open db");
        let mut storage = OmniRaftStorage::new(db.clone());
        let leader2 = CommittedLeaderId::new(2, 1);
        let leader5 = CommittedLeaderId::new(5, 1);
        let boundary = WriteBatch::MAX_OPS as u64 - 1;
        let total = boundary + 5;
        let interior = 3u64;
        let interior_key = format!("{RAFT_LOG_PREFIX}{interior:020}");

        let mut chunk = Vec::with_capacity(2_500);
        for idx in 1..=total {
            chunk.push(Entry {
                log_id: LogId::new(leader2, idx),
                payload: EntryPayload::Blank,
            });
            if chunk.len() == 2_500 {
                storage
                    .append_to_log(std::mem::take(&mut chunk))
                    .await
                    .expect("append chunk");
            }
        }
        if !chunk.is_empty() {
            storage.append_to_log(chunk).await.expect("append tail");
        }

        let cases = [
            (
                "not_valid_json".to_string(),
                "corrupt Raft log entry",
                "malformed interior entry",
            ),
            (
                serde_json::to_string(&Entry::<TypeConfig> {
                    log_id: LogId::new(leader5, interior),
                    payload: EntryPayload::Blank,
                })
                .expect("serialize higher term"),
                "non-monotonic Raft log id",
                "higher-term interior entry",
            ),
            (
                serde_json::to_string(&Entry::<TypeConfig> {
                    log_id: LogId::new(leader2, 999),
                    payload: EntryPayload::Blank,
                })
                .expect("serialize mismatched index"),
                "mismatched Raft log index",
                "mismatched key/index interior entry",
            ),
        ];

        for (bad_val, expected_msg, label) in cases {
            let mut wb = WriteBatch::new();
            wb.set(&interior_key, bad_val).expect("set bad interior");
            db.commit_batch_local(&wb).expect("commit bad interior");
            let err = storage
                .purge_logs_upto(LogId::new(leader2, total))
                .await
                .expect_err(label);
            assert!(
                err.to_string().contains(expected_msg),
                "{label}: unexpected error: {err}"
            );
            assert!(
                storage.read_log(1).is_some(),
                "{label}: chunk 1 must stay intact"
            );
            assert_eq!(
                storage.meta.lock().expect("meta lock").last_purged_log_id,
                None,
                "{label}: purge floor must not advance"
            );
        }
    }

    #[tokio::test]
    async fn purge_logs_upto_rejects_defects_in_final_partial_chunk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = OmniKV::open(
            dir.path().join("manifest.json").to_str().expect("manifest"),
            dir.path().join("wal.bin").to_str().expect("wal"),
        )
        .expect("open db");
        let mut storage = OmniRaftStorage::new(db.clone());
        let leader1 = CommittedLeaderId::new(1, 1);
        let leader2 = CommittedLeaderId::new(2, 1);
        let leader5 = CommittedLeaderId::new(5, 1);

        let entries: Vec<Entry<TypeConfig>> = (1..=6u64)
            .map(|idx| Entry {
                log_id: LogId::new(leader2, idx),
                payload: EntryPayload::Blank,
            })
            .collect();
        storage.append_to_log(entries).await.expect("append logs");

        let key3 = format!("{RAFT_LOG_PREFIX}{:020}", 3);
        let key4 = format!("{RAFT_LOG_PREFIX}{:020}", 4);

        // 1. Corrupt entry in final partial chunk (has_more == false).
        let mut wb = WriteBatch::new();
        wb.set(&key3, "{broken".into()).expect("set corrupt");
        db.commit_batch_local(&wb).expect("commit corrupt");
        let err = storage
            .purge_logs_upto(LogId::new(leader2, 6))
            .await
            .expect_err("corrupt entry in final partial chunk must fail");
        assert!(err.to_string().contains("corrupt Raft log entry"));
        assert!(storage.read_log(1).is_some());
        assert_eq!(
            storage.meta.lock().expect("meta lock").last_purged_log_id,
            None
        );

        // 2. Non-monotonic term regression (term 2 at idx 3 -> term 1 at idx 4).
        let mut wb2 = WriteBatch::new();
        wb2.set(
            &key3,
            serde_json::to_string(&Entry::<TypeConfig> {
                log_id: LogId::new(leader2, 3),
                payload: EntryPayload::Blank,
            })
            .expect("serialize valid idx 3"),
        )
        .expect("restore idx 3");
        wb2.set(
            &key4,
            serde_json::to_string(&Entry::<TypeConfig> {
                log_id: LogId::new(leader1, 4),
                payload: EntryPayload::Blank,
            })
            .expect("serialize regressed term"),
        )
        .expect("set regressed idx 4");
        db.commit_batch_local(&wb2).expect("commit term regression");
        let err = storage
            .purge_logs_upto(LogId::new(leader2, 6))
            .await
            .expect_err("term regression inside final chunk must fail");
        assert!(err.to_string().contains("non-monotonic Raft log id"));
        assert!(storage.read_log(1).is_some());

        // 3. Higher term than purge target inside final partial chunk.
        let mut wb3 = WriteBatch::new();
        wb3.set(
            &key4,
            serde_json::to_string(&Entry::<TypeConfig> {
                log_id: LogId::new(leader5, 4),
                payload: EntryPayload::Blank,
            })
            .expect("serialize higher term"),
        )
        .expect("set higher term idx 4");
        db.commit_batch_local(&wb3).expect("commit higher term");
        let err = storage
            .purge_logs_upto(LogId::new(leader2, 6))
            .await
            .expect_err("higher-term entry inside final chunk must fail");
        assert!(err.to_string().contains("non-monotonic Raft log id"));
        assert!(storage.read_log(1).is_some());
        assert_eq!(
            storage.meta.lock().expect("meta lock").last_purged_log_id,
            None
        );
    }

    #[tokio::test]
    async fn malformed_authoritative_2pc_record_fails_open_and_snapshot_install_closed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manifest = dir.path().join("manifest.json");
        let wal = dir.path().join("wal.bin");
        {
            let db = OmniKV::open(
                manifest.to_str().expect("manifest"),
                wal.to_str().expect("wal"),
            )
            .expect("open db");
            let mut wb = WriteBatch::new();
            wb.set("live_k", "keep".into()).expect("set live_k");
            wb.set("__2PC_PREPARE__/1/1_1", "{not_valid_json".into())
                .expect("set corrupt 2pc");
            db.commit_batch_local(&wb).expect("commit corrupt 2pc");
        }
        let open_res = OmniKV::open(
            manifest.to_str().expect("manifest"),
            wal.to_str().expect("wal"),
        );
        assert!(
            open_res.is_err(),
            "OmniKV::open must fail closed on a malformed authoritative 2PC record"
        );

        let dst_dir = tempfile::tempdir().expect("dst tempdir");
        let dst_db = OmniKV::open(
            dst_dir
                .path()
                .join("manifest.json")
                .to_str()
                .expect("dst manifest"),
            dst_dir.path().join("wal.bin").to_str().expect("dst wal"),
        )
        .expect("open dst db");
        let mut init = WriteBatch::new();
        init.set("live_k", "original".into()).expect("set original");
        dst_db.commit_batch_local(&init).expect("commit original");

        let mut dst_storage = OmniRaftStorage::new(dst_db.clone());
        let snap_log = LogId::new(CommittedLeaderId::new(1, 1), 10);
        let envelope = SnapshotEnvelope {
            version: SNAPSHOT_VERSION,
            last_log_id: Some(snap_log),
            membership: StoredMembership::default(),
            max_seq: 100,
            entries: vec![
                ("live_k".into(), "corrupted_snap".into()),
                ("__2PC_PREPARE__/1/1_1".into(), "{not_valid_json".into()),
            ],
            ssi_history: Vec::new(),
            ssi_eviction_floor: 0,
        };
        let snap_meta = SnapshotMeta {
            last_log_id: Some(snap_log),
            last_membership: StoredMembership::default(),
            snapshot_id: "bad-2pc-snap".into(),
        };
        let bytes = serde_json::to_vec(&envelope).expect("encode envelope");
        let err = dst_storage
            .install_snapshot(&snap_meta, Box::new(Cursor::new(bytes)))
            .await
            .expect_err("install_snapshot must fail closed on malformed 2PC record");
        assert!(
            err.to_string().contains("sync 2PC prepared reservations"),
            "unexpected error: {err}"
        );
        assert_eq!(
            dst_db
                .find_latest_internal("live_k")
                .expect("read untouched key"),
            Some("original".into()),
            "failed snapshot install must leave live state untouched"
        );
    }

    #[tokio::test]
    async fn snapshot_install_rejects_2pc_write_targeting_wrong_participant() {
        let dst_dir = tempfile::tempdir().expect("dst tempdir");
        let dst_db = OmniKV::open(
            dst_dir
                .path()
                .join("manifest.json")
                .to_str()
                .expect("dst manifest"),
            dst_dir.path().join("wal.bin").to_str().expect("dst wal"),
        )
        .expect("open dst db");
        let mut init = WriteBatch::new();
        init.set("customer:42", "local_ok".into())
            .expect("set local_ok");
        dst_db.commit_batch_local(&init).expect("commit init");

        let mut dst_storage = OmniRaftStorage::new(dst_db.clone());
        let snap_log = LogId::new(CommittedLeaderId::new(1, 1), 12);
        let bad_2pc_json = r#"{"txn_id":[1,9],"node_id":10,"state":"PREPARED","writes":[{"node_id":11,"key":"customer:42","value":"wrong_shard","ttl":0}],"prepare_seq":1,"commit_seq":null}"#;
        let envelope = SnapshotEnvelope {
            version: SNAPSHOT_VERSION,
            last_log_id: Some(snap_log),
            membership: StoredMembership::default(),
            max_seq: 100,
            entries: vec![
                ("customer:42".into(), "replaced".into()),
                ("__2PC_PREPARE__/10/1_9".into(), bad_2pc_json.into()),
            ],
            ssi_history: Vec::new(),
            ssi_eviction_floor: 0,
        };
        let snap_meta = SnapshotMeta {
            last_log_id: Some(snap_log),
            last_membership: StoredMembership::default(),
            snapshot_id: "wrong-target-2pc-snap".into(),
        };
        let bytes = serde_json::to_vec(&envelope).expect("encode envelope");
        let err = dst_storage
            .install_snapshot(&snap_meta, Box::new(Cursor::new(bytes)))
            .await
            .expect_err("snapshot with wrong-node DistWrite must fail before live promotion");
        assert!(
            err.to_string()
                .contains("targets participant node 11 instead of node 10"),
            "unexpected error: {err}"
        );
        assert_eq!(
            dst_db
                .find_latest_internal("customer:42")
                .expect("read preserved key"),
            Some("local_ok".into()),
            "live DB must remain untouched after rejected snapshot"
        );
    }
}
