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
        let json = serde_json::to_string(meta).unwrap();
        batch.set(RAFT_META_KEY, json).unwrap();
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
        self.db.commit_batch(&batch)?;
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
                self.db.commit_batch(&batch)?;
            }
        }
        Ok(())
    }

    /// Record the given index as the last applied log index.
    pub fn mark_applied(&self, index: u64) -> Result<(), crate::OmniError> {
        let mut meta = self.meta.lock().expect("RaftStorage meta lock poisoned: fatal invariant");
        // For the test helper, the exact leader_id is not critical — only the index matters.
        let leader_id = meta
            .last_applied
            .map(|existing| existing.leader_id)
            .unwrap_or_else(|| openraft::CommittedLeaderId::new(0, 0));
        meta.last_applied = Some(LogId::new(leader_id, index));
        let mut batch = WriteBatch::new();
        self.save_meta(&meta, &mut batch);
        self.db.commit_batch(&batch)?;
        Ok(())
    }

    /// Returns the index of the last applied log entry, or 0 if none.
    pub fn last_applied_index(&self) -> u64 {
        let meta = self.meta.lock().expect("RaftStorage meta lock poisoned: fatal invariant");
        meta.last_applied.map(|id| id.index).unwrap_or(0)
    }

    /// Delete log entries in the half-open range `[start, end)`.
    pub fn delete_log_range(&self, start: u64, end: u64) -> Result<(), crate::OmniError> {
        let mut batch = WriteBatch::new();
        for idx in start..end {
            let key = format!("{}{:020}", RAFT_LOG_PREFIX, idx);
            batch.delete(&key)?;
        }
        self.db.commit_batch(&batch)?;
        Ok(())
    }

    /// Persist a vote (as a JSON string) for crash recovery.
    pub fn save_vote(&self, vote_json: &str) -> Result<(), crate::OmniError> {
        let key = format!("{}vote", RAFT_LOG_PREFIX);
        let mut batch = WriteBatch::new();
        batch.set(&key, vote_json.to_string())?;
        self.db.commit_batch(&batch)?;
        Ok(())
    }

    /// Read the persisted vote JSON string.
    pub fn read_vote(&self) -> Option<String> {
        let key = format!("{}vote", RAFT_LOG_PREFIX);
        self.db.find_latest_internal(&key).ok().flatten()
    }
}

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

impl RaftSnapshotBuilder<TypeConfig> for OmniRaftStorage {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let mut entries = Vec::new();
        let (snap_meta, max_seq) = {
            let m = self.meta.lock().expect("RaftStorage meta lock poisoned: fatal invariant");
            if m.last_applied.is_none() {
                return Err(StorageError::IO {
                    source: StorageIOError::new(
                        openraft::ErrorSubject::Store,
                        openraft::ErrorVerb::Write,
                        AnyError::error("Cannot snapshot empty state machine"),
                    ),
                });
            }
            let la = m.last_applied.ok_or_else(|| StorageError::read(
                openraft::StorageIOError::read_state_machine(
                    &std::io::Error::new(std::io::ErrorKind::Other, "last_applied is None during snapshot"),
                ),
            ))?;
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
        openraft::error::ErrorSubject::Store,
        openraft::error::ErrorVerb::Write,
        std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
    )
}

fn storage_read_err(e: impl std::fmt::Display) -> openraft::StorageError<u64> {
    openraft::StorageError::from_io_error(
        openraft::error::ErrorSubject::Store,
        openraft::error::ErrorVerb::Read,
        std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
    )
}

impl RaftStorage<TypeConfig> for OmniRaftStorage {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let mut batch = WriteBatch::new();
        {
            let mut meta = self.meta.lock().expect("RaftStorage meta lock poisoned: fatal invariant");
            meta.vote = Some(*vote);
            self.save_meta(&meta, &mut batch);
        }
        self.db.commit_batch(&batch).map_err(|e| StorageError::IO {
            source: StorageIOError::new(
                openraft::ErrorSubject::Store,
                openraft::ErrorVerb::Write,
                AnyError::error(e.to_string()),
            ),
        })?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let meta = self.meta.lock().expect("RaftStorage meta lock poisoned: fatal invariant");
        Ok(meta.vote)
    }

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let meta = self.meta.lock().expect("RaftStorage meta lock poisoned: fatal invariant");
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
            last_log_id = Some(entry.log_id);
        }

        if last_log_id.is_some() {
            let mut meta = self.meta.lock().expect("RaftStorage meta lock poisoned: fatal invariant");
            meta.last_log_id = last_log_id;
            self.save_meta(&meta, &mut batch);
        }

        self.db.commit_batch(&batch).map_err(|e| StorageError::IO {
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
                idx += 1;
            } else {
                break;
            }
        }

        {
            let mut meta = self.meta.lock().expect("RaftStorage meta lock poisoned: fatal invariant");
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

        self.db.commit_batch(&batch).map_err(|e| StorageError::IO {
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
            let meta = self.meta.lock().expect("RaftStorage meta lock poisoned: fatal invariant");
            meta.last_purged_log_id.map(|id| id.index + 1).unwrap_or(1)
        };

        for idx in start_idx..=log_id.index {
            let key = format!("{}{:020}", RAFT_LOG_PREFIX, idx);
            batch.delete(&key).map_err(|e| storage_write_err(&e))?;
        }

        {
            let mut meta = self.meta.lock().expect("RaftStorage meta lock poisoned: fatal invariant");
            meta.last_purged_log_id = Some(log_id);
            self.save_meta(&meta, &mut batch);
        }

        self.db.commit_batch(&batch).map_err(|e| StorageError::IO {
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
        let meta = self.meta.lock().expect("RaftStorage meta lock poisoned: fatal invariant");
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

        for entry in entries {
            match &entry.payload {
                EntryPayload::Blank => res.push("".to_string()),
                EntryPayload::Normal(req) => {
                    if req.starts_with("SET ") {
                        let parts: Vec<&str> = req.splitn(3, ' ').collect();
                        if parts.len() == 3 {
                            if parts[1].starts_with("__sys__/raft/") {
                                res.push("ERR".to_string());
                                continue;
                            }
                            let key = parts[1].to_string();
                            batch.set(&key, parts[2].to_string()).map_err(|e| storage_write_err(&e))?;
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
            let mut meta = self.meta.lock().expect("RaftStorage meta lock poisoned: fatal invariant");
            if let Some(la) = last_applied {
                meta.last_applied = Some(la);
            }
            if let Some(m) = new_membership {
                meta.membership = m;
            }
            self.save_meta(&meta, &mut batch);
        }

        self.db.commit_batch(&batch).map_err(|e| StorageError::IO {
            source: StorageIOError::new(
                openraft::ErrorSubject::Store,
                openraft::ErrorVerb::Write,
                AnyError::error(e.to_string()),
            ),
        })?;

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
        let (manifest_path, wal_path) = {
            let manifest = self.db.roots.load().manifest.clone();
            let data_dir = std::path::Path::new(&self.db.manifest_path)
                .parent()
                .unwrap_or(std::path::Path::new("."))
                .to_path_buf();
            let tmp_dir = data_dir.join("tmp_snapshot_install");
            (
                self.db.manifest_path.clone(),
                format!("{}/raft_snapshot.wal", tmp_dir.display()),
            )
        };

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
        let tmp_dir = std::env::temp_dir().join(format!(
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
        let tmp_manifest_path = tmp_dir.join("manifest.json").to_string_lossy().to_string();
        let tmp_wal_path = tmp_dir.join("raft.wal").to_string_lossy().to_string();
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
            batch
                .set(k, v.clone())
                .map_err(|e| io_err(&format!("batch set: {}", e)))?;
        }

        // ── Atomically include Raft metadata in the snapshot build ──
        let mut meta = self.meta.lock().expect("RaftStorage meta lock poisoned: fatal invariant");
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

        // ── Phase D: Atomic directory swap ──
        let old_dir = data_dir.join("old_snapshot");
        if old_dir.exists() {
            fs::remove_dir_all(&old_dir).ok();
        }
        fs::create_dir_all(&old_dir).ok();

        // Move current files to old_dir
        for entry in
            fs::read_dir(&data_dir).map_err(|e| io_err(&format!("read data dir: {}", e)))?
        {
            let entry = entry.map_err(|e| io_err(&format!("read data dir entry: {}", e)))?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name != "old_snapshot" {
                fs::rename(entry.path(), old_dir.join(&name))
                    .map_err(|e| io_err(&format!("move current snapshot file {}: {}", name, e)))?;
            }
        }

        // Move tmp files to data_dir
        for entry in
            fs::read_dir(&tmp_dir).map_err(|e| io_err(&format!("read temp snapshot dir: {}", e)))?
        {
            let entry = entry.map_err(|e| io_err(&format!("read temp snapshot entry: {}", e)))?;
            let name = entry.file_name();
            fs::rename(entry.path(), data_dir.join(&name)).map_err(|e| {
                io_err(&format!(
                    "install snapshot file {}: {}",
                    name.to_string_lossy(),
                    e
                ))
            })?;
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
        *self.db.heap_file.lock().expect("heap_file lock poisoned: fatal invariant") = recovered.heap_file;
        *self.db.wal.lock().expect("wal lock poisoned: fatal invariant") = recovered.wal;
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
