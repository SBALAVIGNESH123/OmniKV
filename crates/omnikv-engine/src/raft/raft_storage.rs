use crate::OmniKV;
use crate::WriteBatch;
use crate::raft_impl::{OmniNode, TypeConfig};
use openraft::{
    AnyError, Entry, EntryPayload, LogId, OptionalSend, RaftTypeConfig, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership, Vote,
    storage::{LogState, RaftLogReader, RaftSnapshotBuilder, RaftStorage, Snapshot},
};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

const RAFT_LOG_PREFIX: &str = "__sys__/raft/log/";
const RAFT_META_KEY: &str = "__sys__/raft/meta";
const SNAPSHOT_VERSION: u32 = 1;

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
        let mut meta = self
            .meta
            .lock()
            .expect("RaftStorage meta lock poisoned: fatal invariant");
        // For the test helper, the exact leader_id is not critical — only the index matters.
        let leader_id = meta
            .last_applied
            .map(|existing| existing.leader_id)
            .unwrap_or_else(|| openraft::CommittedLeaderId::new(0, 0));
        meta.last_applied = Some(LogId::new(leader_id, index));
        let mut batch = WriteBatch::new();
        self.save_meta(&meta, &mut batch);
        self.db.commit_batch_local(&batch)?;
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
            if let Ok(Some(val)) = self.db.find_latest_internal(&key) {
                if let Ok(entry) = serde_json::from_str::<Entry<TypeConfig>>(&val) {
                    entries.push(entry);
                } else {
                    break;
                }
            } else {
                break;
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

        if let Ok(iter) = self.db.scan_all_latest_internal() {
            for (k, v) in iter {
                if k.starts_with("__sys__/raft/") {
                    continue;
                }
                entries.push((k, v));
            }
        }

        let envelope = SnapshotEnvelope {
            version: SNAPSHOT_VERSION,
            last_log_id: snap_meta.last_log_id,
            membership: snap_meta.last_membership.clone(),
            max_seq,
            entries,
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

fn storage_read_err(e: impl std::fmt::Display) -> openraft::StorageError<u64> {
    openraft::StorageError::from_io_error(
        openraft::ErrorSubject::Store,
        openraft::ErrorVerb::Read,
        std::io::Error::other(e.to_string()),
    )
}

/// Commits `batch` locally, in [`WriteBatch::MAX_OPS`]-sized chunks if
/// it exceeds the op cap. The raft storage paths are the only callers
/// that can legitimately exceed one client batch: a follower catch-up
/// appends many log entries in one call, a purge deletes one key per
/// purged index, and a snapshot install replays the whole captured
/// dataset. Committing the whole thing as ONE batch used to trip
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
        {
            let mut meta = self
                .meta
                .lock()
                .expect("RaftStorage meta lock poisoned: fatal invariant");
            meta.vote = Some(*vote);
            self.save_meta(&meta, &mut batch);
        }
        self.db
            .commit_batch_local(&batch)
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    openraft::ErrorSubject::Store,
                    openraft::ErrorVerb::Write,
                    AnyError::error(e.to_string()),
                ),
            })?;
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

        if last_log_id.is_some() {
            let mut meta = self
                .meta
                .lock()
                .expect("RaftStorage meta lock poisoned: fatal invariant");
            meta.last_log_id = last_log_id;
            self.save_meta(&meta, &mut batch);
        }

        // Tail chunk (never larger than MAX_OPS thanks to the flushes
        // above; commit_chunked is belt-and-braces for the meta op).
        commit_chunked(&self.db, &mut batch).map_err(|e| StorageError::IO {
            source: StorageIOError::new(
                openraft::ErrorSubject::Store,
                openraft::ErrorVerb::Write,
                AnyError::error(e.to_string()),
            ),
        })?;
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<u64>,
    ) -> Result<(), StorageError<u64>> {
        let mut batch = WriteBatch::new();
        let mut idx = log_id.index;
        loop {
            let key = format!("{}{:020}", RAFT_LOG_PREFIX, idx);
            if let Ok(Some(_)) = self.db.find_latest_internal(&key) {
                batch.delete(&key).map_err(|e| storage_write_err(&e))?;
                // Conflict truncation is normally small, but a long
                // divergent tail can exceed one batch; flush chunks.
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
                idx += 1;
            } else {
                break;
            }
        }

        {
            let mut meta = self
                .meta
                .lock()
                .expect("RaftStorage meta lock poisoned: fatal invariant");
            if idx > log_id.index {
                // Determine new last_log_id (log_id.index - 1)
                if log_id.index == 1 {
                    meta.last_log_id = None;
                } else {
                    let prev_key = format!("{}{:020}", RAFT_LOG_PREFIX, log_id.index - 1);
                    if let Ok(Some(val)) = self.db.find_latest_internal(&prev_key)
                        && let Ok(entry) = serde_json::from_str::<Entry<TypeConfig>>(&val)
                    {
                        meta.last_log_id = Some(entry.log_id);
                    }
                }
                self.save_meta(&meta, &mut batch);
            }
        }

        self.db
            .commit_batch_local(&batch)
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    openraft::ErrorSubject::Store,
                    openraft::ErrorVerb::Write,
                    AnyError::error(e.to_string()),
                ),
            })?;
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut batch = WriteBatch::new();

        let start_idx = {
            let meta = self
                .meta
                .lock()
                .expect("RaftStorage meta lock poisoned: fatal invariant");
            meta.last_purged_log_id.map(|id| id.index + 1).unwrap_or(1)
        };

        for idx in start_idx..=log_id.index {
            let key = format!("{}{:020}", RAFT_LOG_PREFIX, idx);
            batch.delete(&key).map_err(|e| storage_write_err(&e))?;
            // A purge after snapshot/compaction can span far more log
            // indexes than one batch holds; flush chunks as we go. The
            // meta update (last_purged_log_id) rides the FINAL chunk
            // only — a crash mid-purge just re-purges idempotently.
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
        }

        {
            let mut meta = self
                .meta
                .lock()
                .expect("RaftStorage meta lock poisoned: fatal invariant");
            meta.last_purged_log_id = Some(log_id);
            self.save_meta(&meta, &mut batch);
        }

        self.db
            .commit_batch_local(&batch)
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    openraft::ErrorSubject::Store,
                    openraft::ErrorVerb::Write,
                    AnyError::error(e.to_string()),
                ),
            })?;
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
        // Flushes the accumulated batch when it nears the op cap —
        // openraft applies in batches, and a follower catch-up can
        // easily carry more ops than one WriteBatch holds. Chunking here
        // is what keeps a big apply from failing with BatchTooLarge and
        // stalling replication. Ordering is preserved (chunks commit in
        // log order); atomicity across entries was never guaranteed
        // anyway (openraft may split/merge entry batches).
        let io_err = |e: crate::OmniError| StorageError::IO {
            source: StorageIOError::new(
                openraft::ErrorSubject::Store,
                openraft::ErrorVerb::Write,
                AnyError::error(e.to_string()),
            ),
        };
        // Returns the small engine error (openraft's StorageError is
        // ~224 bytes; mapping it here keeps clippy's result_large_err
        // quiet and the closure cheap to invoke per op).
        let flush_if_full = |batch: &mut WriteBatch| -> Result<(), crate::OmniError> {
            if batch.op_count() >= WriteBatch::MAX_OPS {
                self.db.commit_batch_local(batch)?;
                batch.clear();
            }
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
                    if let Some(cmd) = crate::raft_command::RaftCommand::decode(req) {
                        let system_key = cmd
                            .sets
                            .iter()
                            .any(|op| op.key.starts_with("__sys__/raft/"))
                            || cmd.dels.iter().any(|k| k.starts_with("__sys__/raft/"));
                        if system_key || cmd.is_empty() {
                            res.push("ERR".to_string());
                            continue;
                        }
                        for op in cmd.sets {
                            // The entry carries the ABSOLUTE expiry the
                            // originating node computed — apply it verbatim.
                            // Re-deriving `now + ttl` here would drift the
                            // expiry forward by the replication delay on
                            // every follower hop.
                            batch
                                .set_with_expiry(&op.key, op.value, op.ttl)
                                .map_err(|e| storage_write_err(&e))?;
                            flush_if_full(&mut batch).map_err(io_err)?;
                        }
                        for key in &cmd.dels {
                            batch.delete(key).map_err(|e| storage_write_err(&e))?;
                            flush_if_full(&mut batch).map_err(io_err)?;
                        }
                        res.push("OK".to_string());
                    } else if let Some(rest) = req.strip_prefix("SET ") {
                        let parts: Vec<&str> = rest.splitn(2, ' ').collect();
                        if parts.len() == 2 && !parts[0].starts_with("__sys__/raft/") {
                            batch
                                .set(parts[0], parts[1].to_string())
                                .map_err(|e| storage_write_err(&e))?;
                            flush_if_full(&mut batch).map_err(io_err)?;
                            res.push("OK".to_string());
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

        if last_applied.is_some() || new_membership.is_some() {
            let mut meta = self
                .meta
                .lock()
                .expect("RaftStorage meta lock poisoned: fatal invariant");
            if let Some(la) = last_applied {
                meta.last_applied = Some(la);
            }
            if let Some(m) = new_membership {
                meta.membership = m;
            }
            self.save_meta(&meta, &mut batch);
        }

        // Final chunk (≤ MAX_OPS thanks to the flushes above;
        // commit_chunked is belt-and-braces for the meta op).
        commit_chunked(&self.db, &mut batch).map_err(io_err)?;

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
        let envelope: SnapshotEnvelope = serde_json::from_slice(&data)
            .map_err(|e| io_err(&format!("Snapshot deserialize: {}", e)))?;
        if envelope.version != SNAPSHOT_VERSION {
            return Err(io_err(&format!(
                "Snapshot version mismatch: expected {}, got {}",
                SNAPSHOT_VERSION, envelope.version
            )));
        }

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
            .unwrap_or(std::path::Path::new("."))
            .to_path_buf();
        let wal_dir = std::path::Path::new(&wal_path)
            .parent()
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

        let tmp_manifest = crate::Manifest {
            format_version: crate::MANIFEST_FORMAT_VERSION,
            heap_path: tmp_heap_path.clone(),
            base_path: tmp_base_path.clone(),
            sstables: vec![],
            l1_sstables: vec![],
            max_seq: 0,
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
                    .commit_batch(&batch)
                    .map_err(|e| io_err(&format!("commit snapshot batch: {}", e)))?;
                batch = crate::WriteBatch::new();
            }
            batch
                .set(k, v.clone())
                .map_err(|e| io_err(&format!("batch set: {}", e)))?;
        }

        // ── Atomically include Raft metadata in the snapshot build ──
        let mut meta = self
            .meta
            .lock()
            .expect("RaftStorage meta lock poisoned: fatal invariant");
        meta.last_applied = snap_meta.last_log_id;
        meta.last_log_id = snap_meta.last_log_id;
        meta.membership = snap_meta.last_membership.clone();
        meta.last_purged_log_id = snap_meta.last_log_id;
        let json = serde_json::to_string(&*meta).map_err(|e| storage_write_err(&e))?;
        batch
            .set(RAFT_META_KEY, json)
            .map_err(|e| io_err(&format!("meta set: {}", e)))?;
        drop(meta);

        if !batch.is_empty() {
            tmp_db
                .commit_batch(&batch)
                .map_err(|e| io_err(&format!("commit snapshot batch: {}", e)))?;
            tmp_db
                .compact_sstables()
                .map_err(|e| io_err(&format!("compact snapshot: {}", e)))?;
        }
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
        installed_manifest
            .save(&tmp_manifest_path)
            .map_err(|e| io_err(&format!("save installed snapshot manifest: {e}")))?;

        // ── Phase D: Same-filesystem directory promotion ──
        let old_dir = data_dir.join("old_snapshot");
        if old_dir.exists() {
            fs::remove_dir_all(&old_dir)
                .map_err(|e| io_err(&format!("remove old snapshot dir: {e}")))?;
        }
        fs::create_dir_all(&old_dir)
            .map_err(|e| io_err(&format!("create old snapshot dir: {e}")))?;

        let tmp_dir_name = tmp_dir
            .file_name()
            .ok_or_else(|| io_err("temporary snapshot directory has no file name"))?
            .to_os_string();
        let restore_active_files = || {
            if let Ok(entries) = fs::read_dir(&data_dir) {
                for entry in entries.flatten() {
                    let name = entry.file_name();
                    if name != "old_snapshot" && name != "LOCK" && name != tmp_dir_name {
                        let _ = fs::rename(entry.path(), tmp_dir.join(&name));
                    }
                }
            }
            if let Ok(entries) = fs::read_dir(&old_dir) {
                for entry in entries.flatten() {
                    let name = entry.file_name();
                    if name != "LOCK" {
                        let _ = fs::rename(entry.path(), data_dir.join(&name));
                    }
                }
            }
        };

        // Move current files to old_dir
        for entry in
            fs::read_dir(&data_dir).map_err(|e| io_err(&format!("read data dir: {}", e)))?
        {
            let entry = entry.map_err(|e| io_err(&format!("read data dir entry: {}", e)))?;
            let name = entry.file_name();
            if name != "old_snapshot"
                && name != "LOCK"
                && name != tmp_dir_name
                && let Err(e) = fs::rename(entry.path(), old_dir.join(&name))
            {
                restore_active_files();
                return Err(io_err(&format!(
                    "move current snapshot file {}: {}",
                    name.to_string_lossy(),
                    e
                )));
            }
        }

        // Move tmp files to data_dir
        for entry in
            fs::read_dir(&tmp_dir).map_err(|e| io_err(&format!("read temp snapshot dir: {}", e)))?
        {
            let entry = entry.map_err(|e| io_err(&format!("read temp snapshot entry: {}", e)))?;
            let name = entry.file_name();
            if name == "LOCK" {
                continue;
            }
            if let Err(e) = fs::rename(entry.path(), data_dir.join(&name)) {
                restore_active_files();
                return Err(io_err(&format!(
                    "install snapshot file {}: {}",
                    name.to_string_lossy(),
                    e
                )));
            }
        }

        let _ = fs::remove_dir_all(&tmp_dir);

        // ── Phase E: Recover fresh storage from installed snapshot ──
        let recovered = OmniKV::recover_storage_roots(&manifest_path, &wal_path)
            .map_err(|e| io_err(&format!("recover snapshot storage: {}", e)))?;

        // ── Phase F: Single atomic StorageRoots publish ──
        let new_roots = crate::StorageRoots {
            base_mmap: recovered.base_mmap,
            base_bloom: recovered.base_bloom,
            sstables: recovered.sstables,
            l1_sstables: recovered.l1_sstables,
            memtable: recovered.memtable,
            frozen_memtables: Arc::new(Vec::new()),
            manifest: recovered.manifest,
            heap_reader: recovered.heap_reader,
        };
        self.db.roots.store(Arc::new(new_roots));
        self.db.block_cache.invalidate_all();

        // ── Phase G: Swap mutable write handles ──
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

        // CRITICAL: Advance global_seq to at least snapshot max_seq.
        let cur_seq = self.db.global_seq.load(Ordering::SeqCst);
        if envelope.max_seq >= cur_seq {
            self.db
                .global_seq
                .store(envelope.max_seq + 1, Ordering::SeqCst);
        }

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
