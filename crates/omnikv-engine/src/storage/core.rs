#![expect(
    dead_code,
    reason = "Storage core intentionally carries staged LSM, compaction, transaction, and compatibility paths while the module is being split into smaller production components."
)]

use crate::{metrics_prometheus, wal};
use arc_swap::ArcSwap;
use crossbeam_skiplist::SkipMap;
use memmap2::{Mmap, MmapOptions};
use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{BufWriter, ErrorKind, Read, Write};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, RwLock, Weak};

pub struct FnvHasher(u64);
impl Default for FnvHasher {
    fn default() -> Self {
        Self(0xcbf29ce484222325)
    }
}
impl Hasher for FnvHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 ^= b as u64;
            self.0 = self.0.wrapping_mul(0x100000001b3);
        }
    }
}
#[derive(Debug, Clone)]
pub enum OmniError {
    IoError(String),
    DatabaseAlreadyOpen {
        lock_path: String,
    },
    BatchTooLarge(usize),
    ValueTooLarge(usize),
    KeyNotFound,
    HashCollision,
    LockPoisoned(String),
    WriteStall,
    InvalidCompactionPolicy(String),
    /// A clustered write was rejected because this node is not the
    /// leader. Unlike most errors this one is **client-caused and safe
    /// to expose**: the caller needs `leader_id` to retry there, and it
    /// carries no internal state. `None` means the cluster has no leader
    /// yet (an election in progress).
    NotLeader {
        leader_id: Option<u64>,
    },
    /// The on-disk format version is newer than this binary understands.
    /// The `found` version was read; `supported` is the maximum this build accepts.
    UnsupportedVersion {
        found: u32,
        supported: u32,
    },
    /// A read or read-only commit attempted to use an MVCC `read_seq` older
    /// than the currently installed snapshot's minimum readable sequence floor.
    StaleSnapshot {
        read_seq: u64,
        floor: u64,
    },
}

impl std::fmt::Display for OmniError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OmniError::DatabaseAlreadyOpen { lock_path } => {
                write!(f, "database is already open: lock file {lock_path}")
            }
            OmniError::UnsupportedVersion { found, supported } => write!(
                f,
                "unsupported manifest version {found}: this build supports up to {supported}"
            ),
            OmniError::StaleSnapshot { read_seq, floor } => write!(
                f,
                "stale MVCC snapshot: read_seq {read_seq} precedes installed snapshot floor {floor}"
            ),
            OmniError::InvalidCompactionPolicy(reason) => {
                write!(f, "invalid compaction policy: {reason}")
            }
            OmniError::NotLeader { leader_id } => match leader_id {
                Some(id) => write!(f, "not the leader; the leader is node {id}"),
                None => write!(f, "not the leader; no leader elected yet"),
            },
            other => write!(f, "{other:?}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionPolicy {
    pub l0_compaction_trigger: usize,
    pub l1_compaction_trigger: usize,
    pub l0_write_stall_threshold: usize,
    pub write_stall_wait_attempts: u32,
    pub write_stall_wait_ms: u64,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            l0_compaction_trigger: 4,
            l1_compaction_trigger: 4,
            l0_write_stall_threshold: 12,
            write_stall_wait_attempts: 50,
            write_stall_wait_ms: 100,
        }
    }
}

impl CompactionPolicy {
    pub fn validate(self) -> Result<Self, OmniError> {
        if self.l0_compaction_trigger == 0 {
            return Err(OmniError::InvalidCompactionPolicy(
                "l0_compaction_trigger must be greater than 0".into(),
            ));
        }
        if self.l1_compaction_trigger == 0 {
            return Err(OmniError::InvalidCompactionPolicy(
                "l1_compaction_trigger must be greater than 0".into(),
            ));
        }
        if self.l0_write_stall_threshold <= self.l0_compaction_trigger {
            return Err(OmniError::InvalidCompactionPolicy(
                "l0_write_stall_threshold must be greater than l0_compaction_trigger".into(),
            ));
        }
        Ok(self)
    }
}

#[derive(Default)]
struct CompactionStats {
    bytes_rewritten: u64,
    tombstones: u64,
    expired_records_dropped: u64,
}

impl CompactionStats {
    fn record(&self, stage: &str, started_at: std::time::Instant, backlog_sstables: usize) {
        metrics_prometheus::record_compaction(
            stage,
            started_at.elapsed(),
            self.bytes_rewritten,
            self.tombstones,
            self.expired_records_dropped,
            backlog_sstables,
        );
    }
}

static PROCESS_DB_LOCKS: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();

fn process_db_locks() -> &'static Mutex<HashSet<PathBuf>> {
    PROCESS_DB_LOCKS.get_or_init(|| Mutex::new(HashSet::new()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CleanupDeleteOutcome {
    Deleted,
    NotFound,
    Failed,
}

fn cleanup_obsolete_file(path: impl AsRef<Path>, context: &'static str) -> CleanupDeleteOutcome {
    cleanup_obsolete_file_with(path.as_ref(), context, |path| std::fs::remove_file(path))
}

fn cleanup_obsolete_sstable_pair(path: &str, context: &'static str) {
    cleanup_obsolete_file(path, context);
    cleanup_obsolete_file(path.replace(".sst", ".bloom"), context);
}

/// Return the Bloom sidecar path for a base file. Base files use `.bin` in a
/// fresh database and `.sst` after L1-to-base compaction, so replacing only
/// `.bin` would make a reopen treat the SSTable itself as the Bloom file.
pub(crate) fn bloom_path_for_base(path: &str) -> String {
    let extension_is = |expected: &str| {
        Path::new(path)
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case(expected))
    };
    if extension_is("sst") || extension_is("bin") {
        Path::new(path)
            .with_extension("bloom")
            .to_string_lossy()
            .into_owned()
    } else {
        format!("{path}.bloom")
    }
}

fn cleanup_obsolete_base_pair(path: &str, context: &'static str) {
    cleanup_obsolete_file(path, context);
    cleanup_obsolete_file(bloom_path_for_base(path), context);
}

/// Removes newly-created compaction outputs if publication fails. A failed
/// compaction must not leave orphaned data or Bloom sidecars that accumulate
/// across retries and eventually exhaust the database volume.
struct CompactionOutputGuard {
    paths: Vec<String>,
}

impl CompactionOutputGuard {
    fn new(paths: Vec<String>) -> Self {
        Self { paths }
    }

    fn disarm(&mut self) {
        self.paths.clear();
    }
}

impl Drop for CompactionOutputGuard {
    fn drop(&mut self) {
        for path in &self.paths {
            let _ = std::fs::remove_file(path);
        }
    }
}

fn cleanup_obsolete_file_with<F>(
    path: &Path,
    context: &'static str,
    remove_file: F,
) -> CleanupDeleteOutcome
where
    F: FnOnce(&Path) -> std::io::Result<()>,
{
    match remove_file(path) {
        Ok(()) => CleanupDeleteOutcome::Deleted,
        Err(err) if err.kind() == ErrorKind::NotFound => CleanupDeleteOutcome::NotFound,
        Err(err) => {
            let error_kind = err.kind();
            metrics_prometheus::record_cleanup_delete_failure(context, error_kind);
            tracing::warn!(
                cleanup_context = context,
                path = %path.display(),
                error = %err,
                error_kind = ?error_kind,
                "best-effort cleanup failed to delete obsolete file"
            );
            CleanupDeleteOutcome::Failed
        }
    }
}

fn database_lock_path(manifest_path: &str) -> Result<PathBuf, OmniError> {
    let manifest_path = Path::new(manifest_path);
    let dir = manifest_path.parent().filter(|p| !p.as_os_str().is_empty());
    let dir = dir.unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(dir)?;
    Ok(std::fs::canonicalize(dir)?.join("LOCK"))
}

struct DatabaseLock {
    file: File,
    path: PathBuf,
}

impl DatabaseLock {
    fn acquire(manifest_path: &str) -> Result<Self, OmniError> {
        let path = database_lock_path(manifest_path)?;
        {
            let mut locks = process_db_locks()
                .lock()
                .map_err(|_| OmniError::LockPoisoned("process database lock registry".into()))?;
            if locks.contains(&path) {
                return Err(OmniError::DatabaseAlreadyOpen {
                    lock_path: path.display().to_string(),
                });
            }
            locks.insert(path.clone());
        }

        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|err| {
                let _ = release_process_db_lock(&path);
                OmniError::IoError(format!("open database lock {}: {err}", path.display()))
            })?;

        if let Err(_err) = fs2::FileExt::try_lock_exclusive(&file) {
            let _ = release_process_db_lock(&path);
            return Err(OmniError::DatabaseAlreadyOpen {
                lock_path: path.display().to_string(),
            });
        }

        Ok(Self { file, path })
    }
}

impl Drop for DatabaseLock {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.file);
        let _ = release_process_db_lock(&self.path);
    }
}

fn release_process_db_lock(path: &Path) -> Result<(), OmniError> {
    let mut locks = process_db_locks()
        .lock()
        .map_err(|_| OmniError::LockPoisoned("process database lock registry".into()))?;
    locks.remove(path);
    Ok(())
}

impl std::error::Error for OmniError {}

impl From<std::io::Error> for OmniError {
    fn from(e: std::io::Error) -> Self {
        OmniError::IoError(e.to_string())
    }
}

pub fn string_to_key(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

pub fn key_to_string(key: &[u8]) -> String {
    String::from_utf8_lossy(key).to_string()
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct OmniRecord {
    pub seq: u64,
    pub key: Vec<u8>,
    pub offset: u64,
    pub length: u64,
    pub crc32: u32,
    pub payload_crc32: u32,
    pub expiry: u64,
}

impl OmniRecord {
    pub fn new(
        seq: u64,
        key: Vec<u8>,
        offset: u64,
        length: u64,
        payload_crc32: u32,
        expiry: u64,
    ) -> Self {
        let mut rec = Self {
            seq,
            key,
            offset,
            length,
            crc32: 0,
            payload_crc32,
            expiry,
        };
        rec.crc32 = rec.compute_crc();
        rec
    }

    pub fn compute_crc(&self) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&self.seq.to_le_bytes());
        hasher.update(&(self.key.len() as u16).to_le_bytes());
        hasher.update(&self.key);
        hasher.update(&self.offset.to_le_bytes());
        hasher.update(&self.length.to_le_bytes());
        hasher.update(&self.payload_crc32.to_le_bytes());
        hasher.update(&self.expiry.to_le_bytes());
        hasher.finalize()
    }

    pub fn is_valid(&self) -> bool {
        self.crc32 == self.compute_crc()
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(42 + self.key.len());
        buf.extend_from_slice(&self.seq.to_le_bytes());
        buf.extend_from_slice(&(self.key.len() as u16).to_le_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.offset.to_le_bytes());
        buf.extend_from_slice(&self.length.to_le_bytes());
        buf.extend_from_slice(&self.payload_crc32.to_le_bytes());
        buf.extend_from_slice(&self.expiry.to_le_bytes());
        buf.extend_from_slice(&self.crc32.to_le_bytes());
        buf
    }

    pub fn decode(buf: &[u8]) -> Option<(Self, usize)> {
        if buf.len() < 42 {
            return None;
        }
        let key_len = u16::from_le_bytes(
            buf[8..10]
                .try_into()
                .expect("record header length was prevalidated"),
        ) as usize;
        if buf.len() < 42 + key_len {
            return None;
        }

        let seq = u64::from_le_bytes(
            buf[0..8]
                .try_into()
                .expect("record sequence length was prevalidated"),
        );
        let key = buf[10..10 + key_len].to_vec();
        let off = 10 + key_len;
        let offset = u64::from_le_bytes(
            buf[off..off + 8]
                .try_into()
                .expect("record offset length was prevalidated"),
        );
        let length = u64::from_le_bytes(
            buf[off + 8..off + 16]
                .try_into()
                .expect("record value length field was prevalidated"),
        );
        let payload_crc32 = u32::from_le_bytes(
            buf[off + 16..off + 20]
                .try_into()
                .expect("record payload checksum length was prevalidated"),
        );
        let expiry = u64::from_le_bytes(
            buf[off + 20..off + 28]
                .try_into()
                .expect("record expiry length was prevalidated"),
        );
        let crc32 = u32::from_le_bytes(
            buf[off + 28..off + 32]
                .try_into()
                .expect("record checksum length was prevalidated"),
        );
        let rec = Self {
            seq,
            key,
            offset,
            length,
            crc32,
            payload_crc32,
            expiry,
        };

        if !rec.is_valid() {
            return None; // Invalid CRC
        }

        Some((rec, 42 + key_len))
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct BloomFilter {
    bitset: Vec<u64>,
    num_bits: usize,
    num_hashes: usize,
}

#[derive(Debug)]
enum AtomicReplaceError {
    BeforeRename(OmniError),
    AfterRenameSyncFailed(OmniError),
}

impl AtomicReplaceError {
    fn into_omni_error(self) -> OmniError {
        match self {
            Self::BeforeRename(err) | Self::AfterRenameSyncFailed(err) => err,
        }
    }
}

/// Atomically publish a small metadata file after its contents have reached
/// stable storage, distinguishing failures before `rename()` from failures in
/// post-rename directory durability.
fn atomic_replace_checked(
    path: &str,
    content: &[u8],
    before_rename_failpoint: Option<&str>,
    after_rename_failpoint: Option<&str>,
) -> Result<(), AtomicReplaceError> {
    let tmp_path = format!("{path}.tmp");
    let before_rename = (|| {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)?;
        file.write_all(content)?;
        file.sync_all()?;
        drop(file);
        if let Some(fp) = before_rename_failpoint {
            crate::failpoints::maybe_fail(fp)?;
        }
        std::fs::rename(&tmp_path, path)?;
        Ok::<(), OmniError>(())
    })();

    if let Err(err) = before_rename {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(AtomicReplaceError::BeforeRename(err));
    }

    // `rename()` has already made the new file visible at `path`. Any failure
    // from this point on leaves publication durability uncertain, so callers
    // must preserve newly referenced files and stop accepting writes under a
    // superseded in-memory topology.
    if let Some(fp) = after_rename_failpoint
        && let Err(err) = crate::failpoints::maybe_fail(fp)
    {
        return Err(AtomicReplaceError::AfterRenameSyncFailed(
            OmniError::IoError(format!("post-rename directory sync failed: {err}")),
        ));
    }

    // On Unix, syncing the directory makes the rename durable across a
    // sudden power loss as well as making the file contents durable.
    #[cfg(unix)]
    {
        let parent = Path::new(path)
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let sync_res: Result<(), std::io::Error> = (|| {
            let dir = File::open(parent)?;
            dir.sync_all()
        })();
        if let Err(err) = sync_res {
            return Err(AtomicReplaceError::AfterRenameSyncFailed(
                OmniError::IoError(format!(
                    "post-rename directory sync failed for '{}': {err}",
                    parent.display()
                )),
            ));
        }
    }

    Ok(())
}

/// Atomically publish a small metadata file after its contents have reached
/// stable storage. Readers either see the previous complete file or the new
/// complete file; they never observe a partially-written JSON sidecar.
fn atomic_replace(path: &str, content: &[u8]) -> Result<(), OmniError> {
    atomic_replace_checked(path, content, None, None).map_err(AtomicReplaceError::into_omni_error)
}

impl BloomFilter {
    pub fn save(&self, path: &str) -> Result<(), OmniError> {
        let content = serde_json::to_string(self)
            .map_err(|e| OmniError::IoError(format!("Bloom filter serialize: {}", e)))?;
        atomic_replace(path, content.as_bytes())
    }

    pub fn load(path: &str) -> Result<Self, OmniError> {
        let content = std::fs::read_to_string(path)?;
        serde_json::from_str(&content).map_err(|e| OmniError::IoError(e.to_string()))
    }

    pub fn new(expected_elements: usize) -> Self {
        let p = 0.01_f64;
        let num_bits =
            (-(expected_elements as f64) * p.ln() / (2.0_f64.ln().powi(2))).ceil() as usize;
        let num_hashes =
            (2.0_f64.ln() * (num_bits as f64) / (expected_elements as f64)).ceil() as usize;
        let bitset = vec![0; num_bits.div_ceil(64)];
        let num_bits_total = bitset.len() * 64;
        Self {
            bitset,
            num_bits: num_bits_total,
            num_hashes,
        }
    }

    fn hashes(&self, key: u64) -> Vec<usize> {
        let mut h = FnvHasher::default();
        key.hash(&mut h);
        let hash1 = h.finish();

        let mut h2 = FnvHasher::default();
        hash1.hash(&mut h2);
        let hash2 = h2.finish();

        let mut res = Vec::with_capacity(self.num_hashes);
        for i in 0..self.num_hashes {
            let combined = hash1.wrapping_add((i as u64).wrapping_mul(hash2));
            res.push((combined % self.num_bits as u64) as usize);
        }
        res
    }

    pub fn add(&mut self, key: u64) {
        if self.num_bits == 0 {
            return;
        }
        for idx in self.hashes(key) {
            self.bitset[idx / 64] |= 1u64 << (idx % 64);
        }
    }

    pub fn might_contain(&self, key: u64) -> bool {
        if self.num_bits == 0 {
            return false;
        }
        for idx in self.hashes(key) {
            if self.bitset[idx / 64] & (1u64 << (idx % 64)) == 0 {
                return false;
            }
        }
        true
    }
}

/// Per-batch op cap. Exceeding it is a caller bug — `commit_batch` and
/// friends reject the batch — but the raft storage adapter must never
/// construct one bigger: follower catch-up, log purge, and snapshot
/// install can carry far more ops than a client batch, so they commit
/// in chunks of this size (`WriteBatch::MAX_OPS`).
pub const MAX_BATCH_SIZE: usize = 10_000;
pub(crate) const MAX_VALUE_SIZE: usize = 10 * 1024 * 1024; // 10 MB
const UNCOMPRESSED_FLAG: u64 = 1 << 63;
const NUM_SHARDS: usize = 16;

type ValuePointer = (u64, u64, u32, u64);
type VersionedValuePointers = std::collections::BTreeMap<u64, ValuePointer>;
type MemtableShard = SkipMap<(Vec<u8>, Reverse<u64>), ValuePointer>;
type Memtable = [MemtableShard; NUM_SHARDS];
type SharedMemtable = Arc<Memtable>;
type SstableHandle = (Arc<Mmap>, Arc<BloomFilter>, String);
type SharedSstables = Arc<Vec<SstableHandle>>;

fn retain_versions_needed_by_floor(
    versions: &mut VersionedValuePointers,
    retention_floor: Option<u64>,
) {
    let Some(latest_seq) = versions.keys().next_back().copied() else {
        return;
    };
    let predecessor_for_retention_floor =
        retention_floor.and_then(|floor| versions.range(..=floor).next_back().map(|(&seq, _)| seq));

    versions.retain(|seq, _| {
        *seq == latest_seq
            || retention_floor.is_some_and(|floor| *seq >= floor)
            || predecessor_for_retention_floor == Some(*seq)
    });
}

fn insert_record_version(merged: &mut BTreeMap<Vec<u8>, VersionedValuePointers>, rec: &OmniRecord) {
    merged.entry(rec.key.clone()).or_default().insert(
        rec.seq,
        (rec.offset, rec.length, rec.payload_crc32, rec.expiry),
    );
}

#[inline]
pub fn shard_idx(key: &[u8]) -> usize {
    let mut h = FnvHasher::default();
    key.hash(&mut h);
    (h.finish() as usize) % NUM_SHARDS
}

#[derive(Default)]
pub struct WriteBatch {
    pub buffered_writes: Vec<(Vec<u8>, String, u64)>,
    pub buffered_deletes: Vec<Vec<u8>>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn set(&mut self, key: &str, value: String) -> Result<(), OmniError> {
        self.set_with_ttl(key, value, 0)
    }
    pub fn set_with_ttl(
        &mut self,
        key: &str,
        value: String,
        ttl_secs: u64,
    ) -> Result<(), OmniError> {
        if self.buffered_writes.len() + self.buffered_deletes.len() >= MAX_BATCH_SIZE {
            return Err(OmniError::BatchTooLarge(MAX_BATCH_SIZE));
        }
        if value.len() > MAX_VALUE_SIZE {
            return Err(OmniError::ValueTooLarge(MAX_VALUE_SIZE));
        }
        let expiry = if ttl_secs > 0 {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                + ttl_secs
        } else {
            0
        };
        self.buffered_writes
            .push((string_to_key(key), value, expiry));
        Ok(())
    }
    /// Writes a key with an ABSOLUTE unix-expiry timestamp (0 = never),
    /// the storage-level primitive `set_with_ttl` computes from now.
    /// The raft apply path needs this: a replicated entry carries the
    /// expiry the originating node computed, and re-deriving it from
    /// `now + ttl` on every follower would drift the expiry forward by
    /// the replication delay on every hop.
    pub fn set_with_expiry(
        &mut self,
        key: &str,
        value: String,
        expiry: u64,
    ) -> Result<(), OmniError> {
        if self.buffered_writes.len() + self.buffered_deletes.len() >= MAX_BATCH_SIZE {
            return Err(OmniError::BatchTooLarge(MAX_BATCH_SIZE));
        }
        if value.len() > MAX_VALUE_SIZE {
            return Err(OmniError::ValueTooLarge(MAX_VALUE_SIZE));
        }
        self.buffered_writes
            .push((string_to_key(key), value, expiry));
        Ok(())
    }

    /// Appends an internal metadata write (such as Raft's `last_applied`
    /// bookmark) that must commit atomically with a full user batch, even when
    /// the user entry already reached [`MAX_OPS`](Self::MAX_OPS). Still
    /// enforces [`MAX_VALUE_SIZE`].
    pub(crate) fn set_internal_meta(&mut self, key: &str, value: String) -> Result<(), OmniError> {
        if value.len() > MAX_VALUE_SIZE {
            return Err(OmniError::ValueTooLarge(MAX_VALUE_SIZE));
        }
        self.buffered_writes.push((string_to_key(key), value, 0));
        Ok(())
    }

    pub fn delete(&mut self, key: &str) -> Result<(), OmniError> {
        if self.buffered_writes.len() + self.buffered_deletes.len() >= MAX_BATCH_SIZE {
            return Err(OmniError::BatchTooLarge(MAX_BATCH_SIZE));
        }
        self.buffered_deletes.push(string_to_key(key));
        Ok(())
    }
    pub fn is_empty(&self) -> bool {
        self.buffered_writes.is_empty() && self.buffered_deletes.is_empty()
    }
    /// Number of operations staged so far. The raft storage adapter
    /// flushes and starts a fresh batch before this reaches
    /// [`MAX_OPS`](Self::MAX_OPS).
    pub fn op_count(&self) -> usize {
        self.buffered_writes.len() + self.buffered_deletes.len()
    }
    /// Empties the batch (e.g. after a chunk was committed), keeping
    /// capacity for the next chunk.
    pub fn clear(&mut self) {
        self.buffered_writes.clear();
        self.buffered_deletes.clear();
    }

    /// The op cap every commit path enforces. Raft-side callers that can
    /// legitimately exceed one client batch (catch-up appends, purges,
    /// snapshot installs) chunk their work at this size.
    pub const MAX_OPS: usize = MAX_BATCH_SIZE;
}

/// Current on-disk manifest format version. Increment for incompatible changes.
pub const MANIFEST_FORMAT_VERSION: u32 = 1;

fn default_manifest_format_version() -> u32 {
    MANIFEST_FORMAT_VERSION
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct Manifest {
    pub heap_path: String,
    pub base_path: String,
    pub sstables: Vec<String>,
    #[serde(default)]
    pub l1_sstables: Vec<String>,
    #[serde(default)]
    pub max_seq: u64,
    /// Highest MVCC sequence already rewritten into the current base/heap files
    /// by heap GC. WAL replay skips records with `seq <= wal_checkpoint_seq` so
    /// a crash before `wal.clear()` can never replay old-heap offsets into a
    /// replacement heap.
    #[serde(default)]
    pub wal_checkpoint_seq: u64,
    /// Minimum MVCC `read_seq` valid against the persisted storage topology,
    /// advanced by Raft snapshot installation so stale pre-install snapshots
    /// stay rejected even after restart.
    #[serde(default)]
    pub read_seq_floor: u64,
    /// Format version. Missing in old manifests → treated as v1.
    #[serde(default = "default_manifest_format_version")]
    pub format_version: u32,
}

impl Manifest {
    pub fn load(path: &str) -> Result<Self, OmniError> {
        let content = std::fs::read_to_string(path)?;
        let m: Self =
            serde_json::from_str(&content).map_err(|e| OmniError::IoError(e.to_string()))?;
        if m.format_version > MANIFEST_FORMAT_VERSION {
            return Err(OmniError::UnsupportedVersion {
                found: m.format_version,
                supported: MANIFEST_FORMAT_VERSION,
            });
        }
        Ok(m)
    }
    fn save_checked(&self, path: &str) -> Result<(), AtomicReplaceError> {
        // Always write current format version.
        let mut to_save = self.clone();
        to_save.format_version = MANIFEST_FORMAT_VERSION;
        let content = serde_json::to_string(&to_save).map_err(|e| {
            AtomicReplaceError::BeforeRename(OmniError::IoError(format!(
                "Manifest serialize: {}",
                e
            )))
        })?;
        atomic_replace_checked(
            path,
            content.as_bytes(),
            Some("manifest::before_rename"),
            Some("manifest::sync_dir_after_rename"),
        )
    }

    pub fn save(&self, path: &str) -> Result<(), OmniError> {
        self.save_checked(path)
            .map_err(AtomicReplaceError::into_omni_error)
    }
}

pub struct Metrics {
    pub read_latencies: Mutex<hdrhistogram::Histogram<u64>>,
    pub commit_latencies: Mutex<hdrhistogram::Histogram<u64>>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            read_latencies: Mutex::new(
                hdrhistogram::Histogram::<u64>::new_with_bounds(1, 10_000_000, 3).unwrap_or_else(
                    |_| {
                        hdrhistogram::Histogram::new(3)
                            .expect("default histogram precision must be valid")
                    },
                ),
            ),
            commit_latencies: Mutex::new(
                hdrhistogram::Histogram::<u64>::new_with_bounds(1, 10_000_000, 3).unwrap_or_else(
                    |_| {
                        hdrhistogram::Histogram::new(3)
                            .expect("default histogram precision must be valid")
                    },
                ),
            ),
        }
    }
}

const SCAN_BUFFER_INITIAL_CAPACITY: usize = 8 * 1024;
const SCAN_BUFFER_POOL_MAX_BUFFERS: usize = 64;

#[derive(Default)]
struct ScanBufferPool {
    buffers: Mutex<Vec<Vec<u8>>>,
}

impl ScanBufferPool {
    fn acquire(self: &Arc<Self>) -> PooledScanBuffer {
        let buffer = self
            .buffers
            .lock()
            .ok()
            .and_then(|mut buffers| buffers.pop())
            .unwrap_or_else(|| Vec::with_capacity(SCAN_BUFFER_INITIAL_CAPACITY));

        PooledScanBuffer {
            pool: self.clone(),
            buffer: Some(buffer),
        }
    }

    fn available(&self) -> usize {
        self.buffers
            .lock()
            .map(|buffers| buffers.len())
            .unwrap_or_default()
    }

    fn clear(&self) {
        if let Ok(mut buffers) = self.buffers.lock() {
            buffers.clear();
        }
    }

    fn release(&self, mut buffer: Vec<u8>) {
        buffer.clear();
        if let Ok(mut buffers) = self.buffers.lock()
            && buffers.len() < SCAN_BUFFER_POOL_MAX_BUFFERS
        {
            buffers.push(buffer);
        }
    }
}

struct PooledScanBuffer {
    pool: Arc<ScanBufferPool>,
    buffer: Option<Vec<u8>>,
}

impl PooledScanBuffer {
    fn as_mut_vec(&mut self) -> &mut Vec<u8> {
        self.buffer
            .as_mut()
            .expect("pooled scan buffer is present until drop")
    }
}

impl Drop for PooledScanBuffer {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.pool.release(buffer);
        }
    }
}

struct ScanCandidate {
    key: Vec<u8>,
    seq: u64,
    offset: u64,
    length: u64,
    crc: u32,
    expiry: u64,
}

#[derive(Clone, Copy)]
enum ScanEnd<'a> {
    Inclusive(&'a [u8]),
    Exclusive(&'a [u8]),
    Unbounded,
}

impl ScanEnd<'_> {
    #[inline]
    fn is_past(&self, key: &[u8]) -> bool {
        match *self {
            Self::Inclusive(end) => key > end,
            Self::Exclusive(end) => key >= end,
            Self::Unbounded => false,
        }
    }
}

fn next_unicode_scalar(ch: char) -> Option<char> {
    let code = ch as u32;
    if code == char::MAX as u32 {
        return None;
    }
    let next = if code == 0xD7FF { 0xE000 } else { code + 1 };
    char::from_u32(next)
}

pub struct ScanIterator<'a> {
    db: &'a OmniKV,
    heap_reader: Arc<File>,
    heap_generation: u64,
    candidates: std::vec::IntoIter<ScanCandidate>,
    now: u64,
    read_buffer: PooledScanBuffer,
}

impl ScanIterator<'_> {
    pub(crate) fn try_next(&mut self) -> Result<Option<(String, String)>, OmniError> {
        for candidate in self.candidates.by_ref() {
            if candidate.length == 0 || (candidate.expiry > 0 && candidate.expiry <= self.now) {
                continue;
            }
            let key = key_to_string(&candidate.key);
            let value = self.db.read_from_heap_with_reader_into(
                &self.heap_reader,
                self.heap_generation,
                candidate.offset,
                candidate.length,
                candidate.crc,
                self.read_buffer.as_mut_vec(),
            )?;
            return Ok(Some((key, value)));
        }
        Ok(None)
    }
}

impl Iterator for ScanIterator<'_> {
    type Item = (String, String);

    fn next(&mut self) -> Option<Self::Item> {
        // Preserve the legacy infallible iterator contract. Collected scans
        // use try_next directly so a heap error cannot become partial success.
        self.try_next().ok().flatten()
    }
}

#[derive(Clone)]
enum SSTableData {
    Mmap(Arc<Mmap>),
    Bytes(Arc<[u8]>),
}

impl SSTableData {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Mmap(mmap) => mmap,
            Self::Bytes(bytes) => bytes,
        }
    }
}

pub struct SSTableReader {
    data: SSTableData,
}

impl SSTableReader {
    /// Creates a reader from arbitrary bytes.
    ///
    /// This copies into an owned `Arc<[u8]>` so iterators derived from this
    /// reader can outlive the source byte slice. Production mmap paths should
    /// use `from_mmap` to avoid copying table files.
    pub fn new(data: &[u8]) -> Self {
        Self {
            data: SSTableData::Bytes(Arc::<[u8]>::from(data)),
        }
    }

    pub fn from_mmap(mmap: Arc<Mmap>) -> Self {
        Self {
            data: SSTableData::Mmap(mmap),
        }
    }

    fn get_block_for_key(&self, target: &[u8]) -> Option<usize> {
        let data = self.data.as_slice();
        if data.len() < 16 {
            return Some(0);
        }
        let magic = &data[data.len() - 8..];
        if magic != b"OMNIV2**" {
            return Some(0);
        }

        let index_offset = u64::from_le_bytes(
            data[data.len() - 16..data.len() - 8]
                .try_into()
                .expect("SSTable trailer offset length was prevalidated"),
        ) as usize;
        if index_offset >= data.len() {
            return Some(0);
        }

        let index_data = &data[index_offset..data.len() - 16];
        if index_data.len() < 8 {
            return Some(0);
        }
        let num_entries = u64::from_le_bytes(
            index_data[0..8]
                .try_into()
                .expect("SSTable index header length was prevalidated"),
        ) as usize;

        let mut entries = Vec::with_capacity(num_entries);
        let mut curr = 8;
        for _ in 0..num_entries {
            if curr + 2 > index_data.len() {
                break;
            }
            let key_len = u16::from_le_bytes(
                index_data[curr..curr + 2]
                    .try_into()
                    .expect("SSTable index key length was prevalidated"),
            ) as usize;
            curr += 2;
            if curr + key_len + 8 > index_data.len() {
                break;
            }
            let key = &index_data[curr..curr + key_len];
            curr += key_len;
            let offset = u64::from_le_bytes(
                index_data[curr..curr + 8]
                    .try_into()
                    .expect("SSTable index block offset was prevalidated"),
            ) as usize;
            curr += 8;
            entries.push((key, offset));
        }

        let idx = entries.partition_point(|&(k, _)| k <= target);
        if idx == 0 {
            Some(0)
        } else {
            Some(entries[idx - 1].1)
        }
    }

    pub fn find(&self, target_key: &[u8], read_seq: u64) -> Option<(u64, u64, u32, u64)> {
        let data = self.data.as_slice();
        let mut offset = self.get_block_for_key(target_key).unwrap_or(0);
        let mut best = None;
        let mut best_seq = 0;

        while offset < data.len() {
            if data.len() >= 16 && &data[data.len() - 8..] == b"OMNIV2**" {
                let index_offset = u64::from_le_bytes(
                    data[data.len() - 16..data.len() - 8]
                        .try_into()
                        .expect("SSTable trailer offset length was prevalidated"),
                ) as usize;
                if offset >= index_offset {
                    break;
                }
            }

            if let Some((rec, len)) = OmniRecord::decode(&data[offset..]) {
                offset += len;
                if rec.key.as_slice() > target_key {
                    break;
                }
                if rec.key.as_slice() == target_key
                    && rec.is_valid()
                    && rec.seq <= read_seq
                    && (best.is_none() || rec.seq > best_seq)
                {
                    best_seq = rec.seq;
                    best = Some((rec.offset, rec.length, rec.payload_crc32, rec.expiry));
                }
            } else {
                break;
            }
        }
        best
    }

    pub fn iter_from(&self, start_key: &[u8]) -> SSTableIterator {
        let offset = self.get_block_for_key(start_key).unwrap_or(0);
        SSTableIterator {
            data: self.data.clone(),
            offset,
        }
    }
}

pub struct SSTableIterator {
    data: SSTableData,
    offset: usize,
}

impl Iterator for SSTableIterator {
    type Item = OmniRecord;

    fn next(&mut self) -> Option<Self::Item> {
        let data = self.data.as_slice();
        if data.len() >= 16 && &data[data.len() - 8..] == b"OMNIV2**" {
            let index_offset = u64::from_le_bytes(
                data[data.len() - 16..data.len() - 8]
                    .try_into()
                    .expect("SSTable trailer offset length was prevalidated"),
            ) as usize;
            if self.offset >= index_offset {
                return None;
            }
        }
        if let Some((rec, len)) = OmniRecord::decode(&data[self.offset..]) {
            self.offset += len;
            Some(rec)
        } else {
            None
        }
    }
}

pub struct SSTableWriter<'a> {
    writer: BufWriter<&'a File>,
    offset: usize,
    index: Vec<(Vec<u8>, u64)>,
    current_block_start_key: Option<Vec<u8>>,
    block_size: usize,
}

impl<'a> SSTableWriter<'a> {
    pub fn new(file: &'a File) -> Self {
        Self {
            writer: BufWriter::new(file),
            offset: 0,
            index: Vec::new(),
            current_block_start_key: None,
            block_size: 4096,
        }
    }

    pub fn append(&mut self, record: &OmniRecord) -> Result<(), OmniError> {
        let bytes = record.encode();

        if self.current_block_start_key.is_none() {
            self.current_block_start_key = Some(record.key.clone());
            self.index.push((record.key.clone(), self.offset as u64));
        }

        self.writer.write_all(&bytes)?;
        self.offset += bytes.len();

        if self.current_block_start_key.is_some()
            && self.offset as u64
                - self
                    .index
                    .last()
                    .expect("current block marker implies at least one index entry")
                    .1
                >= self.block_size as u64
        {
            self.current_block_start_key = None;
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<(), OmniError> {
        let index_offset = self.offset as u64;
        self.writer
            .write_all(&(self.index.len() as u64).to_le_bytes())?;
        for (k, off) in &self.index {
            self.writer.write_all(&(k.len() as u16).to_le_bytes())?;
            self.writer.write_all(k)?;
            self.writer.write_all(&off.to_le_bytes())?;
        }
        self.writer.write_all(&index_offset.to_le_bytes())?;
        self.writer.write_all(b"OMNIV2**")?;
        self.writer.flush()?;
        Ok(())
    }
}

/// Single atomic snapshot of all read-visible storage topology.
/// Readers load this once and operate on stable Arc-owned handles — zero locking.
/// Snapshot install replaces this with a single `roots.store(Arc::new(new_roots))`.
/// No mixed-topology window is possible.
#[derive(Clone)]
pub struct StorageRoots {
    pub base_mmap: Arc<Mmap>,
    pub base_bloom: Arc<BloomFilter>,
    pub sstables: SharedSstables,    // L0
    pub l1_sstables: SharedSstables, // L1
    pub memtable: SharedMemtable,
    pub frozen_memtables: Arc<Vec<SharedMemtable>>,
    pub manifest: Arc<Manifest>,
    pub heap_reader: Arc<File>,
    /// Monotonic identifier for the underlying heap file (`heap_reader`),
    /// incremented whenever GC or Raft snapshot installation replaces the heap.
    /// Pairs with byte offsets in `block_cache` so reads captured from an older
    /// root snapshot never read or hit cache entries from a different heap.
    pub heap_generation: u64,
    /// Minimum MVCC `read_seq` valid against this topology. Advanced to the
    /// installed snapshot's floor by Raft `install_snapshot` so pre-install
    /// transactions cannot silently read records from the replacement snapshot.
    pub read_seq_floor: u64,
}

/// Pure storage recovery result — returned by recover_storage_from_path().
/// All topology data lives here; no background threads are started.
pub(crate) struct RecoveredStorage {
    pub base_mmap: Arc<Mmap>,
    pub base_bloom: Arc<BloomFilter>,
    pub sstables: SharedSstables,
    pub l1_sstables: SharedSstables,
    pub manifest: Arc<Manifest>,
    pub heap_reader: Arc<File>,
    pub heap_file: File,
    pub heap_offset: u64,
    pub wal: wal::WriteAheadLog,
    pub memtable: SharedMemtable,
    pub max_seq: u64,
}

#[derive(Default)]
struct SqlTableLockRegistry {
    locks: Mutex<HashMap<String, Weak<SqlTableLock>>>,
}

impl SqlTableLockRegistry {
    fn get(self: &Arc<Self>, table_name: &str) -> Result<Arc<SqlTableLock>, OmniError> {
        let name = table_name.to_lowercase();
        let mut locks = self
            .locks
            .lock()
            .map_err(|_| OmniError::LockPoisoned("sql_table_locks".into()))?;

        // Upgrade under the registry mutex so every holder and waiter shares
        // one allocation. Only a dead weak entry may be replaced. Do not drop
        // an upgraded Arc here: its last drop re-enters this registry.
        let lock = match locks.get(&name).and_then(Weak::upgrade) {
            Some(lock) => lock,
            None => {
                let lock = Arc::new(SqlTableLock {
                    mutex: Mutex::new(()),
                    name: name.clone(),
                    registry: Arc::downgrade(self),
                });
                locks.insert(name, Arc::downgrade(&lock));
                lock
            }
        };
        drop(locks);
        Ok(lock)
    }
}

pub(crate) struct SqlTableLock {
    mutex: Mutex<()>,
    name: String,
    registry: Weak<SqlTableLockRegistry>,
}

impl SqlTableLock {
    pub(crate) fn lock(&self) -> std::sync::LockResult<MutexGuard<'_, ()>> {
        self.mutex.lock()
    }
}

impl Drop for SqlTableLock {
    fn drop(&mut self) {
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        let Ok(mut locks) = registry.locks.lock() else {
            return;
        };

        // A new lock for the same table may have replaced this dead weak
        // entry concurrently. Only remove the entry if it still points at
        // this exact allocation. Compare the weak pointer without upgrading:
        // dropping a temporary Arc here could recursively lock the registry.
        let is_this_lock = locks
            .get(&self.name)
            .is_some_and(|lock| std::ptr::eq(lock.as_ptr(), std::ptr::from_ref(self)));
        if is_this_lock {
            locks.remove(&self.name);
        }
    }
}

pub struct OmniKV {
    // ── Single atomic topology root (replaced atomically during snapshot install) ──
    // Reads: load roots once, work on stable Arc handles — zero locking needed.
    // Install: store a new Arc<StorageRoots> — single atomic publish, no mixed-state window.
    pub(crate) roots: ArcSwap<StorageRoots>,

    // ── Durable mutable write handles (outside topology, never root-swapped) ──
    pub(crate) global_seq: AtomicU64,
    pub(crate) heap_file: Mutex<File>,
    pub(crate) heap_offset: AtomicU64,
    pub(crate) wal: Mutex<wal::WriteAheadLog>,
    /// Per-table SQL write locks. The guard spans the read/check/transform/
    /// commit sequence so autocommit DML cannot race uniqueness checks,
    /// legacy-row migration, or DROP TABLE for the same table. Different
    /// tables remain independent. The registry stores weak references and
    /// each lock removes its own entry when the final statement releases it.
    sql_table_locks: Arc<SqlTableLockRegistry>,
    write_mutex: Mutex<()>,
    pub(crate) manifest_path: String,
    pub(crate) wal_path: String,
    pub metrics: Arc<Metrics>,
    pub(crate) active_snapshots: Mutex<std::collections::BTreeMap<u64, usize>>,
    pub(crate) replica_retention_floors: Mutex<BTreeMap<String, u64>>,
    pub(crate) block_cache: moka::sync::Cache<(u64, u64), String>,
    pub(crate) next_heap_generation: AtomicU64,
    scan_buffer_pool: Arc<ScanBufferPool>,
    compaction_policy: RwLock<CompactionPolicy>,

    // Wakes writers stalled on L0 compaction backlog.
    compaction_cv: (std::sync::Mutex<()>, std::sync::Condvar),

    // ── Group commit engine ──
    // Batches concurrent fsyncs: N writers → 1 fsync instead of N fsyncs.
    group_commit: crate::hardening::GroupCommitEngine,

    // ── Storage transition barrier ──
    // Shared (read) lock: commit_batch, compaction, flush.
    // Exclusive (write) lock: snapshot install only — freezes all writers/compactors.
    pub(crate) transition_guard: RwLock<()>,
    /// When clustered: routes client writes through consensus before
    /// they touch local storage (the leader proposes, every node's raft
    /// state machine applies — including this one). `None` in
    /// single-node mode: commit_batch applies directly, exactly as
    /// before. Set once at server boot via
    /// [`OmniKV::set_cluster_gateway`].
    pub(crate) cluster_gateway:
        std::sync::OnceLock<std::sync::Arc<crate::raft_gateway::ClusterGateway>>,

    /// The SSI committed-transaction history. Created eagerly (not a
    /// OnceLock) because BOTH transaction paths need it from the first
    /// moment they can run: the leader's [`TransactionManager::commit`]
    /// records locally-committed transactions here, and the raft state
    /// machine's apply path records the [`SsiCommitRecord`] carried inside
    /// each replicated command — including on a follower that has not yet
    /// (and may never) run a local COMMIT. Sharing one store at the db
    /// level is what makes the two converge; see
    /// [`crate::transaction::SsiHistory`].
    pub(crate) ssi_history: std::sync::Arc<crate::transaction::SsiHistory>,

    /// Live in-flight 2PC coordinator transactions (`txn_id -> owner coordinator instance id`)
    /// shared across `TwoPhaseCoordinator` handles on this `OmniKV` instance so
    /// concurrent `recover_with` calls cannot presumed-abort a transaction while
    /// another coordinator handle on the same database is actively collecting votes.
    pub(crate) active_2pc_coord_txns: Mutex<HashMap<(u64, u64), u64>>,

    /// All live `TwoPhaseCoordinator` handles currently retaining a `txn_id` in
    /// their local `active_txns` map (`txn_id -> set of coordinator instance ids`).
    /// Prevents `gc_completed_logs` from deleting terminal `COMMIT`/`COMPLETE`
    /// records while another live coordinator handle can still act on `txn_id`.
    pub(crate) live_2pc_coord_handles: Mutex<HashMap<(u64, u64), HashSet<u64>>>,

    // Monotonic per-instance counter for compaction output filenames. The
    // wall clock alone is not unique: two flushes in the same millisecond
    // collide, and create_new(true) then fails with EEXIST.
    file_seq: std::sync::atomic::AtomicU64,

    // Fail-stop write fence tripped whenever manifest `rename()` succeeded but
    // post-rename directory durability failed, leaving the on-disk manifest and
    // in-memory topology potentially diverged until reopen/recovery.
    write_fenced: AtomicBool,
    write_fence_reason: Mutex<Option<String>>,

    // Must be declared last: Rust drops struct fields in declaration order, so
    // all mmap-bearing roots and files are released before the database LOCK
    // file is unlocked and closed.
    db_lock: DatabaseLock,
}

/// RAII registration for a storage read snapshot. Keeping the registration
/// coupled to the sequence prevents compaction from discarding versions while
/// a statement still needs them, including when statement execution unwinds.
pub(crate) struct SnapshotGuard {
    db: Arc<OmniKV>,
    sequence: u64,
}

impl SnapshotGuard {
    pub(crate) fn sequence(&self) -> u64 {
        self.sequence
    }
}

impl Drop for SnapshotGuard {
    fn drop(&mut self) {
        self.db.unregister_snapshot(self.sequence);
    }
}

impl OmniKV {
    /// Attaches the cluster gateway (server boot, before serving
    /// clients). After this, every `commit_batch` on this handle
    /// proposes through consensus instead of writing locally first.
    /// Idempotent-guarded: a second attach is a no-op.
    pub fn set_cluster_gateway(
        &self,
        gateway: std::sync::Arc<crate::raft_gateway::ClusterGateway>,
    ) {
        let _ = self.cluster_gateway.set(gateway);
    }

    /// Whether writes on this handle route through consensus.
    pub fn is_clustered(&self) -> bool {
        self.cluster_gateway.get().is_some()
    }

    /// The attached cluster gateway, when clustered. Engine internals
    /// (the SSI transaction manager's clustered commit path) use this
    /// to reach the gateway without re-cloning the OnceLock contents.
    pub fn cluster_gateway(&self) -> Option<std::sync::Arc<crate::raft_gateway::ClusterGateway>> {
        self.cluster_gateway.get().cloned()
    }

    /// The SSI committed-transaction history shared by the transaction
    /// manager (recording locally-committed transactions) and the raft
    /// state machine apply path (recording replicated
    /// [`crate::raft_command::SsiCommitRecord`]s). Both must land in the
    /// same store or a node promoted after a failover validates new
    /// transactions against a history missing the old leader's commits.
    pub fn ssi_history(&self) -> std::sync::Arc<crate::transaction::SsiHistory> {
        self.ssi_history.clone()
    }

    /// Fails closed if `manifest_path`'s directory contains an unfinished
    /// snapshot promotion or failed rollback directory, preventing `open()`
    /// from silently initializing an empty database over preserved recovery data.
    fn check_no_unfinished_snapshot_promotion(manifest_path: &str) -> Result<(), OmniError> {
        const LEGACY_OLD_SNAPSHOT_DIR: &str = "old_snapshot";
        const OLD_SNAPSHOT_DIR_PREFIX: &str = "old_snapshot_";
        const INSTALL_COMPLETE_MARKER: &str = ".install_complete";
        const ROLLBACK_FAILED_MARKER: &str = ".rollback_failed";

        let manifest_exists = Path::new(manifest_path).exists();
        let data_dir = Path::new(manifest_path)
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let entries = match std::fs::read_dir(data_dir) {
            Ok(entries) => entries,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err.into()),
        };

        for entry in entries {
            let entry = entry?;
            let name = entry.file_name();
            let is_rollback_dir = name == LEGACY_OLD_SNAPSHOT_DIR
                || name
                    .to_str()
                    .is_some_and(|s| s.starts_with(OLD_SNAPSHOT_DIR_PREFIX));
            if !is_rollback_dir {
                continue;
            }
            let path = entry.path();
            if manifest_exists && path.is_dir() && std::fs::read_dir(&path)?.next().is_none() {
                // Empty rollback directory left by a clean rollback or crash
                // right before removing the empty dir while the live manifest
                // is present; safe to clean up.
                let _ = std::fs::remove_dir(&path);
                continue;
            }
            let has_complete = std::fs::read(path.join(INSTALL_COMPLETE_MARKER))
                .ok()
                .as_deref()
                .is_some_and(|b| b == b"ok" || b == b"rolled_back");
            let has_failed = path.join(ROLLBACK_FAILED_MARKER).exists();
            if has_complete && !has_failed && manifest_exists {
                continue;
            }
            return Err(OmniError::IoError(format!(
                "cannot open database '{}': unfinished snapshot promotion/rollback directory '{}' exists; restore from the rollback directory or explicitly set it aside before reopening",
                manifest_path,
                path.display()
            )));
        }
        Ok(())
    }

    /// Opens an OmniKV database from the given manifest and WAL paths.
    /// Recovers state from WAL and initializes the storage engine.
    pub fn open(manifest_path: &str, wal_path: &str) -> Result<Arc<Self>, OmniError> {
        let db_lock = DatabaseLock::acquire(manifest_path)?;
        Self::check_no_unfinished_snapshot_promotion(manifest_path)?;
        let recovered = Self::recover_storage_roots(manifest_path, wal_path)?;

        let persisted_read_seq_floor = recovered.manifest.read_seq_floor;
        let recovered_max_seq = recovered.max_seq.max(persisted_read_seq_floor);
        let initial_roots = StorageRoots {
            base_mmap: recovered.base_mmap,
            base_bloom: recovered.base_bloom,
            sstables: recovered.sstables,
            l1_sstables: recovered.l1_sstables,
            memtable: recovered.memtable,
            frozen_memtables: Arc::new(Vec::new()),
            manifest: recovered.manifest,
            heap_reader: recovered.heap_reader,
            heap_generation: 1,
            read_seq_floor: persisted_read_seq_floor,
        };

        let ssi_history = std::sync::Arc::new(crate::transaction::SsiHistory::default());
        ssi_history.advance_eviction_floor(recovered_max_seq);

        let db = Arc::new(Self {
            roots: ArcSwap::from_pointee(initial_roots),
            global_seq: AtomicU64::new(recovered_max_seq.saturating_add(1)),
            heap_file: Mutex::new(recovered.heap_file),
            heap_offset: AtomicU64::new(recovered.heap_offset),
            wal: Mutex::new(recovered.wal),
            sql_table_locks: Arc::new(SqlTableLockRegistry::default()),
            write_mutex: Mutex::new(()),
            manifest_path: manifest_path.to_string(),
            wal_path: wal_path.to_string(),
            metrics: Arc::new(Metrics::new()),
            active_snapshots: Mutex::new(std::collections::BTreeMap::new()),
            replica_retention_floors: Mutex::new(BTreeMap::new()),
            block_cache: moka::sync::Cache::builder().max_capacity(100_000).build(),
            next_heap_generation: AtomicU64::new(2),
            scan_buffer_pool: Arc::new(ScanBufferPool::default()),
            compaction_policy: RwLock::new(CompactionPolicy::default()),
            compaction_cv: (std::sync::Mutex::new(()), std::sync::Condvar::new()),
            // Group commit: wait up to 200µs for more writers to join each batch.
            // On SSDs, this batches 5-50 concurrent writes into a single fsync.
            group_commit: crate::hardening::GroupCommitEngine::new(200),
            transition_guard: RwLock::new(()),
            cluster_gateway: std::sync::OnceLock::new(),
            ssi_history,
            active_2pc_coord_txns: Mutex::new(HashMap::new()),
            live_2pc_coord_handles: Mutex::new(HashMap::new()),
            // Start from a process-unique seed so reopening a database does
            // not immediately reuse the previous instance's token sequence.
            file_seq: std::sync::atomic::AtomicU64::new(rand::random()),
            write_fenced: AtomicBool::new(false),
            write_fence_reason: Mutex::new(None),
            db_lock,
        });
        db.sync_prepared_2pc_reservations()?;
        db.scan_buffer_pool.clear();
        Ok(db)
    }

    // ── Topology accessor helpers ─────────────────────────────────────────────
    // These load a stable root snapshot once. Callers work on Arc handles with no locking.
    // Compaction and reads use these; snapshot install bypasses them and swaps roots directly.

    /// A per-instance token unique across concurrent and back-to-back
    /// compactions. The random seed also avoids immediately reusing the
    /// previous instance's token sequence after a reopen.
    fn next_file_token(&self) -> u64 {
        self.file_seq
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// Creates a compaction output with an exclusive create. The token makes
    /// normal output names unique; retrying on `AlreadyExists` makes that
    /// guarantee hold even if a previous process used the same random seed or
    /// left an output file behind after a crash.
    fn create_unique_output<F>(&self, mut path_for_token: F) -> Result<(String, File), OmniError>
    where
        F: FnMut(u64) -> String,
    {
        const MAX_ATTEMPTS: usize = 32;

        for _ in 0..MAX_ATTEMPTS {
            let path = path_for_token(self.next_file_token());
            match OpenOptions::new()
                .create_new(true)
                .write(true)
                .read(true)
                .open(&path)
            {
                Ok(file) => return Ok((path, file)),
                Err(err) if err.kind() == ErrorKind::AlreadyExists => {}
                Err(err) => return Err(err.into()),
            }
        }

        Err(OmniError::IoError(
            "unable to allocate a unique compaction output path after 32 attempts".into(),
        ))
    }

    #[inline]
    pub fn load_roots(&self) -> arc_swap::Guard<Arc<StorageRoots>> {
        self.roots.load()
    }
    #[inline]
    pub(crate) fn memtable_arc(&self) -> SharedMemtable {
        self.roots.load().memtable.clone()
    }
    #[inline]
    pub fn sstable_count(&self) -> usize {
        self.roots.load().sstables.len()
    }
    #[inline]
    pub fn l1_sstable_count(&self) -> usize {
        self.roots.load().l1_sstables.len()
    }

    pub fn compaction_policy(&self) -> Result<CompactionPolicy, OmniError> {
        self.compaction_policy
            .read()
            .map(|policy| *policy)
            .map_err(|_| OmniError::LockPoisoned("compaction_policy".into()))
    }

    pub fn set_compaction_policy(&self, policy: CompactionPolicy) -> Result<(), OmniError> {
        let policy = policy.validate()?;
        let mut current = self
            .compaction_policy
            .write()
            .map_err(|_| OmniError::LockPoisoned("compaction_policy".into()))?;
        *current = policy;
        Ok(())
    }

    #[inline]
    fn compaction_backlog_sstables(&self) -> usize {
        self.sstable_count() + self.l1_sstable_count()
    }

    /// Returns `true` when this database instance has entered fail-stop
    /// read-only mode after an uncertain post-rename manifest publication.
    pub fn is_write_fenced(&self) -> bool {
        self.write_fenced.load(Ordering::Acquire)
    }

    fn trip_write_fence(&self, reason: String) {
        if let Ok(mut guard) = self.write_fence_reason.lock()
            && guard.is_none()
        {
            *guard = Some(reason);
        }
        self.write_fenced.store(true, Ordering::Release);
        self.notify_compaction_waiters();
    }

    fn ensure_writes_not_fenced(&self) -> Result<(), OmniError> {
        if !self.write_fenced.load(Ordering::Acquire) {
            return Ok(());
        }
        let reason = self
            .write_fence_reason
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
            .unwrap_or_else(|| {
                "storage writes fenced after uncertain manifest publication; reopen required".into()
            });
        Err(OmniError::IoError(reason))
    }

    fn publish_manifest_or_fence(
        &self,
        manifest: &Manifest,
        output_guard: Option<&mut CompactionOutputGuard>,
    ) -> Result<(), OmniError> {
        match manifest.save_checked(&self.manifest_path) {
            Ok(()) => Ok(()),
            Err(AtomicReplaceError::BeforeRename(err)) => Err(err),
            Err(AtomicReplaceError::AfterRenameSyncFailed(err)) => {
                // `rename()` already made the replacement manifest visible on
                // disk. Never delete newly referenced compaction/GC outputs,
                // keep prior files intact for live readers, and fail-stop all
                // further writes until the database is reopened and recovered.
                if let Some(guard) = output_guard {
                    guard.disarm();
                }
                let reason = format!(
                    "manifest publication durability uncertain after rename ({err}); writes halted until database reopen"
                );
                self.trip_write_fence(reason.clone());
                Err(OmniError::IoError(reason))
            }
        }
    }

    fn remove_frozen_memtable(&self, target: &SharedMemtable) -> Result<(), OmniError> {
        let _guard = self
            .write_mutex
            .lock()
            .map_err(|_| OmniError::LockPoisoned("compact".into()))?;
        let mut frozen = self.roots.load().frozen_memtables.clone().as_ref().clone();
        if let Some(pos) = frozen.iter().position(|entry| Arc::ptr_eq(entry, target)) {
            frozen.remove(pos);
            self.update_roots(|r| StorageRoots {
                frozen_memtables: Arc::new(frozen),
                ..r.clone()
            });
        }
        Ok(())
    }

    /// Compaction helper: apply a topology mutation function and publish the result atomically.
    /// Compaction methods call this instead of doing individual `.store()` calls,
    /// ensuring no mixed-state observability.
    pub(crate) fn update_roots(&self, f: impl FnOnce(&StorageRoots) -> StorageRoots) {
        let current = self.roots.load();
        let new_roots = f(&current);
        self.roots.store(Arc::new(new_roots));
    }

    /// Creates and durably syncs an empty base SSTable (4096 bytes for mmap)
    /// and empty heap file before an initial manifest referencing them is saved.
    pub(crate) fn init_empty_storage_files(
        base_path: &str,
        heap_path: &str,
    ) -> Result<(), OmniError> {
        let base_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(base_path)?;
        if base_file.metadata()?.len() == 0 {
            base_file.set_len(4096)?; // dummy length to allow mmap
        }
        base_file.sync_all()?;

        let heap_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(heap_path)?;
        heap_file.sync_all()?;
        Ok(())
    }

    /// Pure storage recovery — no threads, no side effects.
    /// Returns raw storage handles that can be atomically installed.
    /// Used by install_snapshot to swap topology without restarting the engine.
    pub(crate) fn recover_storage_roots(
        manifest_path: &str,
        wal_path: &str,
    ) -> Result<RecoveredStorage, OmniError> {
        let manifest = if Path::new(manifest_path).exists() {
            Manifest::load(manifest_path)?
        } else {
            let m = Manifest {
                format_version: MANIFEST_FORMAT_VERSION,
                heap_path: format!("{}_heap.bin", manifest_path),
                base_path: format!("{}_base.bin", manifest_path),
                sstables: vec![],
                l1_sstables: vec![],
                max_seq: 0,
                wal_checkpoint_seq: 0,
                read_seq_floor: 0,
            };
            Self::init_empty_storage_files(&m.base_path, &m.heap_path)?;
            m.save(manifest_path)?;
            m
        };

        // Every file referenced by an existing manifest must be present and
        // readable; never fabricate an empty base/heap or silently skip missing
        // L0/L1 SSTables on recovery.
        let base_file = OpenOptions::new()
            .read(true)
            .open(&manifest.base_path)
            .map_err(|e| {
                OmniError::IoError(format!(
                    "missing or unreadable base SSTable '{}' referenced by manifest: {e}",
                    manifest.base_path
                ))
            })?;
        if base_file.metadata()?.len() == 0 {
            return Err(OmniError::IoError(format!(
                "corrupt empty base SSTable '{}' referenced by manifest",
                manifest.base_path
            )));
        }
        // SAFETY: The base SSTable file is mapped read-only after it is created
        // or opened. OmniKV holds the database LOCK before this point, so no
        // second engine instance can truncate or mutate the file while mapped.
        // Internal compaction never edits a mapped file in place; it writes a
        // new file, syncs it, maps it, and atomically publishes a new root.
        let base_mmap = Arc::new(unsafe { MmapOptions::new().map(&base_file) }.map_err(|e| {
            OmniError::IoError(format!(
                "failed to memory-map base SSTable '{}' referenced by manifest: {e}",
                manifest.base_path
            ))
        })?);

        let reader = SSTableReader::from_mmap(base_mmap.clone());
        let base_bloom_path = bloom_path_for_base(&manifest.base_path);
        let base_bloom = match BloomFilter::load(&base_bloom_path) {
            Ok(b) => b,
            Err(_) => {
                let mut count = 0;
                for _ in reader.iter_from(b"") {
                    count += 1;
                }
                let mut bloom = BloomFilter::new(count.max(100));
                for rec in reader.iter_from(b"") {
                    let mut h = FnvHasher::default();
                    rec.key.hash(&mut h);
                    bloom.add(h.finish());
                }
                let _ = bloom.save(&base_bloom_path);
                bloom
            }
        };

        let mut loaded_sstables = Vec::with_capacity(manifest.sstables.len());
        for sst_path in &manifest.sstables {
            let file = OpenOptions::new().read(true).open(sst_path).map_err(|e| {
                OmniError::IoError(format!(
                    "missing or unreadable L0 SSTable '{sst_path}' referenced by manifest: {e}"
                ))
            })?;
            // SAFETY: Recovered L0 SSTables are opened read-only and are
            // immutable once published in the manifest. The database LOCK
            // prevents concurrent OmniKV processes from mutating them.
            let mmap = Arc::new(unsafe { MmapOptions::new().map(&file) }.map_err(|e| {
                OmniError::IoError(format!(
                    "failed to memory-map L0 SSTable '{sst_path}' referenced by manifest: {e}"
                ))
            })?);
            let reader = SSTableReader::from_mmap(mmap.clone());
            let bloom_path = sst_path.replace(".sst", ".bloom");
            let bloom = match BloomFilter::load(&bloom_path) {
                Ok(b) => b,
                Err(_) => {
                    let mut count = 0;
                    for _ in reader.iter_from(b"") {
                        count += 1;
                    }
                    let mut b = BloomFilter::new(count.max(100));
                    for rec in reader.iter_from(b"") {
                        if rec.is_valid() {
                            let mut h = FnvHasher::default();
                            rec.key.hash(&mut h);
                            b.add(h.finish());
                        }
                    }
                    let _ = b.save(&bloom_path);
                    b
                }
            };
            loaded_sstables.push((mmap, Arc::new(bloom), sst_path.clone()));
        }

        let mut loaded_l1_sstables = Vec::with_capacity(manifest.l1_sstables.len());
        for sst_path in &manifest.l1_sstables {
            let file = OpenOptions::new().read(true).open(sst_path).map_err(|e| {
                OmniError::IoError(format!(
                    "missing or unreadable L1 SSTable '{sst_path}' referenced by manifest: {e}"
                ))
            })?;
            // SAFETY: Recovered L1 SSTables are immutable read-only table
            // files. They are replaced by publishing new root snapshots,
            // never by in-place mutation while mapped.
            let mmap = Arc::new(unsafe { MmapOptions::new().map(&file) }.map_err(|e| {
                OmniError::IoError(format!(
                    "failed to memory-map L1 SSTable '{sst_path}' referenced by manifest: {e}"
                ))
            })?);
            let reader = SSTableReader::from_mmap(mmap.clone());
            let bloom_path = sst_path.replace(".sst", ".bloom");
            let bloom = match BloomFilter::load(&bloom_path) {
                Ok(b) => b,
                Err(_) => {
                    let mut count = 0;
                    for _ in reader.iter_from(b"") {
                        count += 1;
                    }
                    let mut b = BloomFilter::new(count.max(100));
                    for rec in reader.iter_from(b"") {
                        if rec.is_valid() {
                            let mut h = FnvHasher::default();
                            rec.key.hash(&mut h);
                            b.add(h.finish());
                        }
                    }
                    let _ = b.save(&bloom_path);
                    b
                }
            };
            loaded_l1_sstables.push((mmap, Arc::new(bloom), sst_path.clone()));
        }

        let heap_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&manifest.heap_path)
            .map_err(|e| {
                OmniError::IoError(format!(
                    "missing or unreadable heap file '{}' referenced by manifest: {e}",
                    manifest.heap_path
                ))
            })?;
        let heap_offset = heap_file.metadata()?.len();

        let memtable = Arc::new(std::array::from_fn(|_| SkipMap::new()));
        let mut global_seq = manifest.max_seq;

        let recovered_records = wal::WriteAheadLog::replay(wal_path, &manifest.heap_path)?;
        if !recovered_records.is_empty() {
            for rec in recovered_records {
                if rec.seq <= manifest.wal_checkpoint_seq {
                    continue;
                }
                let shard = shard_idx(&rec.key);
                memtable[shard].insert(
                    (rec.key, Reverse(rec.seq)),
                    (rec.offset, rec.length, rec.payload_crc32, rec.expiry),
                );
                if rec.seq > global_seq {
                    global_seq = rec.seq;
                }
            }
        }

        let wal = wal::WriteAheadLog::new(wal_path)?;

        Ok(RecoveredStorage {
            base_mmap,
            base_bloom: Arc::new(base_bloom),
            sstables: Arc::new(loaded_sstables),
            l1_sstables: Arc::new(loaded_l1_sstables),
            heap_reader: Arc::new(OpenOptions::new().read(true).open(&manifest.heap_path)?),
            manifest: Arc::new(manifest),
            heap_file,
            heap_offset,
            wal,
            memtable,
            max_seq: global_seq,
        })
    }

    /// Returns the current global sequence number.
    pub fn get_seq(&self) -> u64 {
        self.global_seq.load(Ordering::SeqCst)
    }

    /// Returns the process-local SQL lock for one table. The caller keeps the
    /// returned `Arc` alive while locking it, so removing a table from the
    /// catalog cannot invalidate a lock that another statement is waiting on.
    pub(crate) fn sql_table_lock(&self, table_name: &str) -> Result<Arc<SqlTableLock>, OmniError> {
        self.sql_table_locks.get(table_name)
    }

    /// Captures a committed read point and pins its historical versions until
    /// the returned guard is dropped. SQL autocommit statements use this for
    /// statement-level snapshot consistency and compaction safety.
    pub(crate) fn snapshot_guard(self: &Arc<Self>) -> SnapshotGuard {
        SnapshotGuard {
            db: self.clone(),
            sequence: self.snapshot(),
        }
    }

    pub fn snapshot(&self) -> u64 {
        // Writers hold the transition read lock from sequence reservation
        // through memtable publication. Take the exclusive side while
        // choosing a snapshot so the returned sequence is a fully published
        // commit point rather than a range reserved by an in-flight batch.
        // The lock is held only for the sequence/pin operation; readers use
        // the retained MVCC history after it is released.
        let _transition_guard = self
            .transition_guard
            .write()
            .expect("FATAL: transition lock poisoned");
        let seq = self.get_seq().saturating_sub(1);
        let mut snaps = self
            .active_snapshots
            .lock()
            .expect("FATAL: snapshot lock poisoned");
        *snaps.entry(seq).or_insert(0) += 1;
        seq
    }

    pub fn unregister_snapshot(&self, seq: u64) {
        let mut snaps = self
            .active_snapshots
            .lock()
            .expect("FATAL: snapshot lock poisoned");
        if let Some(count) = snaps.get_mut(&seq) {
            *count -= 1;
            if *count == 0 {
                snaps.remove(&seq);
            }
        }
    }

    pub fn min_active_snapshot(&self) -> u64 {
        self.active_snapshots
            .lock()
            .expect("FATAL: snapshot lock poisoned")
            .keys()
            .next()
            .copied()
            .unwrap_or_else(|| self.get_seq().saturating_sub(1))
    }

    fn oldest_active_snapshot(&self) -> Result<Option<u64>, OmniError> {
        self.active_snapshots
            .lock()
            .map_err(|_| OmniError::LockPoisoned("active_snapshots".into()))
            .map(|snapshots| snapshots.keys().next().copied())
    }

    /// Pins the oldest storage sequence that a replica or external catch-up
    /// consumer may still need. Compaction and heap GC treat this like an
    /// external snapshot until the caller releases it.
    ///
    /// Re-pinning the same replica id replaces its previous floor, which lets
    /// replication code advance the retention point as the follower catches up.
    pub fn pin_replica_retention(
        &self,
        replica_id: impl Into<String>,
        seq: u64,
    ) -> Result<(), OmniError> {
        let replica_id = replica_id.into();
        let replica_id = replica_id.trim();
        if replica_id.is_empty() {
            return Err(OmniError::IoError(
                "replica retention id must not be empty".into(),
            ));
        }

        let floor = {
            let mut floors = self
                .replica_retention_floors
                .lock()
                .map_err(|_| OmniError::LockPoisoned("replica_retention_floors".into()))?;
            floors.insert(replica_id.to_owned(), seq);
            floors.values().min().copied()
        };
        metrics_prometheus::record_replica_retention_floor(floor);
        Ok(())
    }

    /// Releases a replica retention pin after that replica has caught up or
    /// switched to snapshot install. Releasing an unknown id is intentionally
    /// idempotent so recovery code can clean up defensively.
    pub fn release_replica_retention(&self, replica_id: &str) -> Result<(), OmniError> {
        let floor = {
            let mut floors = self
                .replica_retention_floors
                .lock()
                .map_err(|_| OmniError::LockPoisoned("replica_retention_floors".into()))?;
            floors.remove(replica_id.trim());
            floors.values().min().copied()
        };
        metrics_prometheus::record_replica_retention_floor(floor);
        Ok(())
    }

    /// Returns the oldest sequence retained for replica catch-up, if any.
    pub fn min_replica_retention(&self) -> Result<Option<u64>, OmniError> {
        self.replica_retention_floors
            .lock()
            .map_err(|_| OmniError::LockPoisoned("replica_retention_floors".into()))
            .map(|floors| floors.values().min().copied())
    }

    fn compaction_retention_floor(&self) -> Result<Option<u64>, OmniError> {
        match (
            self.oldest_active_snapshot()?,
            self.min_replica_retention()?,
        ) {
            (Some(snapshot), Some(replica)) => Ok(Some(snapshot.min(replica))),
            (Some(snapshot), None) => Ok(Some(snapshot)),
            (None, Some(replica)) => Ok(Some(replica)),
            (None, None) => Ok(None),
        }
    }

    pub fn memtable_size(&self) -> usize {
        let roots = self.roots.load();
        let mut sum = roots
            .memtable
            .iter()
            .map(|shard| shard.len())
            .sum::<usize>();
        for frozen in roots.frozen_memtables.iter() {
            sum += frozen.iter().map(|shard| shard.len()).sum::<usize>();
        }
        sum
    }

    pub fn min_memtable_seq(&self) -> u64 {
        let mut min_seq = self.get_seq();
        let memtable = self.roots.load().memtable.clone();
        for shard in memtable.iter() {
            if let Some(entry) = shard.front() {
                let seq = entry.key().1.0;
                if seq < min_seq {
                    min_seq = seq;
                }
            }
        }
        min_seq
    }

    /// Collects the complete visible range, or returns the first heap read error.
    /// Computes the smallest valid UTF-8 string strictly greater than every
    /// UTF-8 string beginning with `prefix`, or `None` if `prefix` is empty or
    /// consists solely of `char::MAX` (`'\u{10ffff}'`).
    ///
    /// Because UTF-8 preserves Unicode scalar ordering and is prefix-free on
    /// scalar values, a valid UTF-8 key `k` starts with `prefix` if and only if
    /// `k >= prefix` and (`successor.is_none()` or `k < successor`).
    pub(crate) fn prefix_successor_str(prefix: &str) -> Option<String> {
        let mut chars: Vec<char> = prefix.chars().collect();
        while let Some(ch) = chars.pop() {
            if let Some(next_ch) = next_unicode_scalar(ch) {
                let mut out: String = chars.into_iter().collect();
                out.push(next_ch);
                return Some(out);
            }
        }
        None
    }

    /// Returns the half-open `[start, end)` range covering every valid UTF-8
    /// key with `prefix`.
    pub(crate) fn prefix_range_exclusive(prefix: &str) -> (String, Option<String>) {
        (prefix.to_string(), Self::prefix_successor_str(prefix))
    }

    /// Collects all visible entries whose keys begin with `prefix` at `read_seq`,
    /// including keys containing `'\u{10ffff}'` while excluding the bytewise
    /// prefix successor itself.
    pub(crate) fn scan_prefix(
        &self,
        prefix: &str,
        read_seq: u64,
    ) -> Result<Vec<(String, String)>, OmniError> {
        let end = Self::prefix_successor_str(prefix);
        self.scan_exclusive(prefix, end.as_deref(), read_seq)
    }

    /// Collects all visible entries in the half-open lexical range
    /// `[start_key, end_exclusive)` (or `[start_key, +inf)` when `end_exclusive`
    /// is `None`).
    pub(crate) fn scan_exclusive(
        &self,
        start_key: &str,
        end_exclusive: Option<&str>,
        read_seq: u64,
    ) -> Result<Vec<(String, String)>, OmniError> {
        let mut results = Vec::new();
        let end_bytes = end_exclusive.map(string_to_key);
        let end_bound = match end_bytes.as_deref() {
            Some(bytes) => ScanEnd::Exclusive(bytes),
            None => ScanEnd::Unbounded,
        };
        let mut iter = self.scan_iter_bounds(string_to_key(start_key), end_bound, read_seq)?;
        while let Some(entry) = iter.try_next()? {
            results.push(entry);
        }
        Ok(results)
    }

    /// Unlike the legacy `scan_iter`, errors cannot yield a successful prefix.
    pub fn scan(
        &self,
        start_key: &str,
        end_key: &str,
        read_seq: u64,
    ) -> Result<Vec<(String, String)>, OmniError> {
        let mut results = Vec::new();
        let mut iter = self.scan_iter_inner(start_key, end_key, read_seq)?;
        while let Some(entry) = iter.try_next()? {
            results.push(entry);
        }
        Ok(results)
    }

    /// Collects up to `limit` visible entries in `[start_key, end_exclusive)` at `read_seq`.
    pub(crate) fn scan_exclusive_limit(
        &self,
        start_key: &str,
        end_exclusive: Option<&str>,
        read_seq: u64,
        limit: usize,
    ) -> Result<Vec<(String, String)>, OmniError> {
        let end_bytes = end_exclusive.map(string_to_key);
        let end_bound = match end_bytes.as_deref() {
            Some(bytes) => ScanEnd::Exclusive(bytes),
            None => ScanEnd::Unbounded,
        };
        let mut iter = self.scan_iter_bounds(string_to_key(start_key), end_bound, read_seq)?;
        let mut results = Vec::with_capacity(limit.min(iter.candidates.len()));
        while results.len() < limit {
            let Some(entry) = iter.try_next()? else {
                break;
            };
            results.push(entry);
        }
        Ok(results)
    }

    /// Collects up to `limit` visible entries whose keys begin with `prefix` at `read_seq`.
    pub(crate) fn scan_prefix_limit(
        &self,
        prefix: &str,
        read_seq: u64,
        limit: usize,
    ) -> Result<Vec<(String, String)>, OmniError> {
        let end = Self::prefix_successor_str(prefix);
        self.scan_exclusive_limit(prefix, end.as_deref(), read_seq, limit)
    }

    /// Collects up to `limit` visible entries in the lexical range `[start_key, end_key]`.
    pub fn scan_limit(
        &self,
        start_key: &str,
        end_key: &str,
        read_seq: u64,
        limit: usize,
    ) -> Result<Vec<(String, String)>, OmniError> {
        let mut iter = self.scan_iter_inner(start_key, end_key, read_seq)?;
        let mut results = Vec::with_capacity(limit.min(iter.candidates.len()));
        while results.len() < limit {
            let Some(entry) = iter.try_next()? else {
                break;
            };
            results.push(entry);
        }
        Ok(results)
    }

    pub fn set_global_seq(&self, seq: u64) {
        self.global_seq.store(seq, Ordering::SeqCst);
    }

    pub fn total_records(&self) -> usize {
        self.memtable_size() // + other counts if needed
    }

    pub fn scan_buffer_pool_available(&self) -> usize {
        self.scan_buffer_pool.available()
    }

    pub fn flush_memtable_to_disk(&self, _id: u64) -> Result<(), OmniError> {
        self.compact_sstables()
    }

    pub fn append_to_heap(&self, data: &[u8]) -> Result<u64, OmniError> {
        let _guard = self
            .write_mutex
            .lock()
            .map_err(|_| OmniError::LockPoisoned("heap write mutex".into()))?;
        self.ensure_writes_not_fenced()?;
        let heap = self
            .heap_file
            .lock()
            .map_err(|_| OmniError::LockPoisoned("heap_file".into()))?;
        let offset = self.heap_offset.load(Ordering::SeqCst);
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt as _;
            heap.write_all_at(data, offset)?;
        }
        #[cfg(windows)]
        {
            let mut remaining = data;
            let mut pos = offset;
            while !remaining.is_empty() {
                let n = heap.seek_write(remaining, pos)?;
                if n == 0 {
                    return Err(OmniError::IoError("heap pwrite: zero-length write".into()));
                }
                remaining = &remaining[n..];
                pos += n as u64;
            }
        }
        crate::failpoints::maybe_fail("heap::sync_data")
            .map_err(|e| OmniError::IoError(format!("heap failure point: {e}")))?;
        heap.sync_data()?;
        self.heap_offset
            .store(offset + data.len() as u64, Ordering::SeqCst);
        Ok(offset)
    }

    pub fn insert_replica_record(
        &self,
        key: Vec<u8>,
        seq: u64,
        offset: u64,
        length: u64,
        crc: u32,
        expiry: u64,
    ) {
        let shard = shard_idx(&key);
        let memtable = self.roots.load().memtable.clone();
        memtable[shard].insert((key, Reverse(seq)), (offset, length, crc, expiry));
        self.global_seq.fetch_max(seq + 1, Ordering::SeqCst);
    }

    pub fn sync_all(&self) -> Result<(), OmniError> {
        let _guard = self
            .write_mutex
            .lock()
            .map_err(|_| OmniError::LockPoisoned("sync write mutex".into()))?;
        let heap = self
            .heap_file
            .lock()
            .map_err(|_| OmniError::LockPoisoned("heap_file".into()))?;
        crate::failpoints::maybe_fail("heap::sync_all")
            .map_err(|e| OmniError::IoError(format!("heap failure point: {e}")))?;
        heap.sync_all()?;
        Ok(())
    }

    pub(crate) fn validate_client_key_bytes(key: &[u8]) -> Result<(), OmniError> {
        if key == b"__COMMIT_MARKER__"
            || key.starts_with(b"__sys__/raft/")
            || key.starts_with(b"__2PC_")
        {
            return Err(OmniError::IoError(
                "reserved consensus/transaction system key".into(),
            ));
        }
        Ok(())
    }

    /// Commits a batch of write operations atomically.
    ///
    /// Uses a 3-phase pipelined write (inspired by RocksDB):
    ///   Phase 1: Compression + CRC computation (no lock, fully parallel)
    ///   Phase 2: Sequence assignment + offset reservation (write_mutex, microseconds)
    ///   Phase 3: Heap I/O + WAL append + memtable insert (heap_file lock, no write_mutex)
    ///
    /// This allows multiple concurrent batches to overlap their CPU-intensive
    /// compression work and only serialize briefly for sequence numbering.
    pub fn commit_batch(&self, tx: &WriteBatch) -> Result<u64, OmniError> {
        self.ensure_writes_not_fenced()?;
        for key in tx
            .buffered_writes
            .iter()
            .map(|(key, _, _)| key.as_slice())
            .chain(tx.buffered_deletes.iter().map(Vec::as_slice))
        {
            Self::validate_client_key_bytes(key)?;
            if let Ok(key_str) = std::str::from_utf8(key)
                && let Some(conflict) = self.ssi_history.check_prepared_write_conflict(key_str)
            {
                return Err(OmniError::IoError(conflict));
            }
        }
        // Clustered: client writes must be consensus-replicated before
        // they are acknowledged. The gateway proposes this batch as one
        // raft entry; raft's state machine (not this call) applies it
        // locally on every node, including this one. The returned number
        // is the raft log index the write was applied at — on followers
        // too, since only the leader accepts client writes.
        if let Some(gateway) = self.cluster_gateway.get() {
            return gateway.commit_batch_checked(tx, &self.ssi_history);
        }
        self.commit_batch_local(tx)
    }

    /// Commits engine-internal 2PC metadata, bypassing client key validation.
    pub(crate) fn commit_2pc_batch(&self, tx: &WriteBatch) -> Result<u64, OmniError> {
        self.ensure_writes_not_fenced()?;
        if let Some(gateway) = self.cluster_gateway.get() {
            crate::dist_txn::validate_2pc_decision_writes(self, None, tx)?;
            return gateway.commit_2pc_batch(tx);
        }
        let lock = self.sql_table_lock("\x002PC_LOCAL_COMMIT")?;
        let _guard = lock
            .lock()
            .map_err(|_| OmniError::LockPoisoned("2PC local commit".into()))?;
        let _topology_guard = self
            .transition_guard
            .read()
            .map_err(|_| OmniError::LockPoisoned("transition_guard".into()))?;
        crate::dist_txn::validate_2pc_decision_writes(self, None, tx)?;
        let seq = self.commit_batch_local_locked(tx)?;
        self.sync_prepared_2pc_from_batch(tx)?;
        Ok(seq)
    }

    pub(crate) fn ensure_cluster_linearizable(&self) -> Result<(), OmniError> {
        if let Some(gateway) = self.cluster_gateway.get() {
            gateway.probe_blocking().map_err(OmniError::IoError)?;
        }
        Ok(())
    }

    pub(crate) fn check_2pc_prepare_conflicts(
        &self,
        read_seq: u64,
        write_keys: &[String],
    ) -> Option<String> {
        let committed = self.ssi_history.lock_committed();
        self.check_2pc_prepare_conflicts_locked(&committed, read_seq, write_keys)
    }

    fn check_2pc_prepare_conflicts_locked(
        &self,
        committed: &[crate::transaction::CommittedTxn],
        read_seq: u64,
        write_keys: &[String],
    ) -> Option<String> {
        self.ssi_history
            .check_write_conflicts_locked(committed, read_seq, write_keys)
            .or_else(|| {
                write_keys.iter().find_map(|key| {
                    let latest_seq = self.get_seq_for_key(key, u64::MAX);
                    (latest_seq > read_seq).then(|| {
                        format!(
                            "SSI CONFLICT (WW): key '{}' written at seq {}",
                            key, latest_seq
                        )
                    })
                })
            })
    }

    /// Commits a 2PC `PREPARED` batch while atomically re-validating `read_seq`
    /// against SSI history and current key sequences under the same ordering
    /// guards as commits and snapshot installs.
    pub(crate) fn commit_2pc_prepare_checked(
        &self,
        tx: &WriteBatch,
        read_seq: u64,
        write_keys: &[String],
    ) -> Result<u64, crate::dist_txn::PrepareCommitError> {
        use crate::dist_txn::PrepareCommitError;
        self.ensure_writes_not_fenced()
            .map_err(PrepareCommitError::PreProposal)?;
        if let Some(gateway) = self.cluster_gateway.get() {
            return gateway.commit_2pc_prepare_checked(self, tx, read_seq, write_keys);
        }
        let lock = self
            .sql_table_lock("\x002PC_LOCAL_COMMIT")
            .map_err(PrepareCommitError::PreProposal)?;
        let _guard = lock.lock().map_err(|_| {
            PrepareCommitError::PreProposal(OmniError::LockPoisoned("2PC local commit".into()))
        })?;
        let _topology_guard = self.transition_guard.read().map_err(|_| {
            PrepareCommitError::PreProposal(OmniError::LockPoisoned("transition_guard".into()))
        })?;
        crate::dist_txn::validate_2pc_decision_writes(self, None, tx)
            .map_err(PrepareCommitError::PreProposal)?;
        let committed_guard = self.ssi_history.lock_committed();
        if let Some(conflict) =
            self.check_2pc_prepare_conflicts_locked(&committed_guard, read_seq, write_keys)
        {
            return Err(PrepareCommitError::PreProposal(OmniError::IoError(
                conflict,
            )));
        }
        let seq = self
            .commit_batch_local_locked_skip_prepared(tx)
            .map_err(PrepareCommitError::PostProposal)?;
        self.sync_prepared_2pc_from_batch(tx)
            .map_err(PrepareCommitError::PostProposal)?;
        drop(committed_guard);
        Ok(seq)
    }

    pub(crate) fn commit_2pc_batch_with_ssi(
        &self,
        tx: &WriteBatch,
        ssi: crate::transaction::SsiCommitRecord,
    ) -> Result<u64, OmniError> {
        self.ensure_writes_not_fenced()?;
        if let Some(gateway) = self.cluster_gateway.get() {
            crate::dist_txn::validate_2pc_decision_writes(self, None, tx)?;
            return gateway.commit_2pc_batch_with_ssi(tx, ssi);
        }
        let lock = self.sql_table_lock("\x002PC_LOCAL_COMMIT")?;
        let _guard = lock
            .lock()
            .map_err(|_| OmniError::LockPoisoned("2PC local commit".into()))?;
        // Acquire transition_guard BEFORE committed_txns to preserve the
        // engine's global lock order (matching TransactionManager::commit),
        // and hold committed_txns across both batch publication and history
        // insertion so no concurrent SSI transaction can validate against the
        // published data before this commit's SSI record is visible.
        let _topology_guard = self
            .transition_guard
            .read()
            .map_err(|_| OmniError::LockPoisoned("transition_guard".into()))?;
        crate::dist_txn::validate_2pc_decision_writes(self, None, tx)?;
        let mut committed_guard = self.ssi_history.lock_committed();
        let commit_seq = self.commit_batch_local_locked_skip_prepared(tx)?;
        self.sync_prepared_2pc_from_batch(tx)?;
        self.ssi_history.record_committed_locked(
            &mut committed_guard,
            ssi.txn_id,
            commit_seq,
            &ssi,
        );
        drop(committed_guard);
        self.ssi_history.prune(self.min_active_snapshot());
        Ok(commit_seq)
    }

    /// Fences any in-flight consensus proposals and local sequence reservations
    /// so a newly registered 2PC prepared-key reservation cannot race past a
    /// concurrent writer.
    pub(crate) fn fence_write_reservation(&self) -> Result<(), OmniError> {
        if let Some(gateway) = self.cluster_gateway.get() {
            gateway.fence_blocking();
        }
        let _guard = self
            .write_mutex
            .lock()
            .map_err(|_| OmniError::LockPoisoned("commit_seq".into()))?;
        Ok(())
    }

    pub(crate) fn sync_prepared_2pc_reservations(&self) -> Result<(), OmniError> {
        crate::dist_txn::sync_all_prepared_reservations(self)
    }

    pub(crate) fn sync_prepared_2pc_from_batch(&self, batch: &WriteBatch) -> Result<(), OmniError> {
        crate::dist_txn::sync_prepared_reservations_from_batch(self, batch)
    }

    /// The single-node commit path, also used by the raft state machine
    /// itself when it applies entries: raft's own bookkeeping must
    /// never route back through the cluster gateway.
    pub(crate) fn commit_batch_local(&self, tx: &WriteBatch) -> Result<u64, OmniError> {
        // Acquire shared topology lock — blocks only during an exclusive
        // snapshot install or a build_snapshot's capture window. Thousands
        // of concurrent writers can hold this simultaneously.
        let _topology_guard = self
            .transition_guard
            .read()
            .map_err(|_| OmniError::LockPoisoned("transition_guard".into()))?;
        self.commit_batch_local_locked(tx)
    }

    /// The commit body for a caller that ALREADY holds the transition guard's
    /// read lock. The raft apply path uses this so it can keep that ONE lock
    /// across both the data commit AND the SSI history record that describes
    /// it: a snapshot built concurrently must never capture the data with its
    /// record missing (or the reverse), and pairing them under the shared lock
    /// is what matches build_snapshot's exclusive capture window.
    pub(crate) fn commit_batch_local_locked(&self, tx: &WriteBatch) -> Result<u64, OmniError> {
        self.commit_batch_local_locked_inner(tx, false)
    }

    pub(crate) fn commit_batch_local_locked_skip_prepared(
        &self,
        tx: &WriteBatch,
    ) -> Result<u64, OmniError> {
        self.commit_batch_local_locked_inner(tx, true)
    }

    fn commit_batch_local_locked_inner(
        &self,
        tx: &WriteBatch,
        skip_prepared_check: bool,
    ) -> Result<u64, OmniError> {
        self.ensure_writes_not_fenced()?;
        let start_time = std::time::Instant::now();

        // Write Backpressure: If L0 SSTables exceed threshold, wait for compaction
        // to catch up before rejecting the write. This avoids spurious failures
        // under bursty write loads where compaction just needs a moment.
        let compaction_policy = self.compaction_policy()?;
        let l0_sstables = self.sstable_count();
        metrics_prometheus::record_compaction_backlog(self.compaction_backlog_sstables());
        if l0_sstables >= compaction_policy.l0_write_stall_threshold {
            // Cap the stall wait budget so an extreme policy cannot overflow
            // `Instant` or block indefinitely while holding `transition_guard`.
            const MAX_WRITE_STALL_WAIT: std::time::Duration = std::time::Duration::from_secs(3600);
            let total_wait = std::time::Duration::from_millis(
                compaction_policy
                    .write_stall_wait_ms
                    .saturating_mul(compaction_policy.write_stall_wait_attempts as u64),
            )
            .min(MAX_WRITE_STALL_WAIT);
            let deadline = std::time::Instant::now().checked_add(total_wait);
            let mut stalled = true;
            let (ref stall_lock, ref cv) = self.compaction_cv;
            let mut guard = stall_lock.lock().unwrap_or_else(|e| e.into_inner());
            loop {
                if let Err(err) = self.ensure_writes_not_fenced() {
                    drop(guard);
                    return Err(err);
                }
                if self.sstable_count() < compaction_policy.l0_write_stall_threshold {
                    stalled = false;
                    break;
                }
                let Some(limit) = deadline else {
                    break;
                };
                let remaining = limit.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let (returned, _timeout) = cv
                    .wait_timeout(guard, remaining)
                    .unwrap_or_else(|e| e.into_inner());
                guard = returned;
            }
            drop(guard);
            if stalled {
                metrics_prometheus::record_write_stall();
                return Err(OmniError::WriteStall);
            }
        }

        // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        // PHASE 1: CPU-intensive work â€” NO LOCK (fully parallelizable)
        // Compression + CRC computation for all values in the batch.
        // Multiple threads can execute this phase simultaneously.
        // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        let mut prepped_writes = Vec::with_capacity(tx.buffered_writes.len());
        for (key, value, expiry) in &tx.buffered_writes {
            let payload_bytes = value.as_bytes();
            let (final_bytes, length) = if payload_bytes.len() < 64 {
                (
                    payload_bytes.to_vec(),
                    (payload_bytes.len() as u64) | UNCOMPRESSED_FLAG,
                )
            } else {
                let comp = lz4_flex::compress_prepend_size(payload_bytes);
                let len = comp.len() as u64;
                (comp, len)
            };

            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&final_bytes);
            let payload_crc32 = hasher.finalize();

            prepped_writes.push((key.clone(), final_bytes, length, payload_crc32, *expiry));
        }

        // Calculate total byte size needed in the heap for offset reservation
        let total_heap_bytes: u64 = prepped_writes
            .iter()
            .map(|(_, _, length, _, _)| length & !UNCOMPRESSED_FLAG)
            .sum();
        let num_records = prepped_writes.len() as u64 + tx.buffered_deletes.len() as u64;

        // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        // PHASE 2: Sequence + Offset reservation â€” WRITE_MUTEX (microseconds)
        // This is the ONLY serialization point. We assign monotonic sequence
        // numbers and reserve a contiguous region in the heap file.
        // No I/O happens here â€” just atomic integer operations.
        // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        let (base_seq, reserved_offset) = {
            let _guard = self
                .write_mutex
                .lock()
                .map_err(|_| OmniError::LockPoisoned("commit_seq".into()))?;
            self.ensure_writes_not_fenced()?;
            if !skip_prepared_check
                && let Some(conflict) = self.ssi_history.check_batch_prepared_conflicts(tx)
            {
                return Err(OmniError::IoError(conflict));
            }
            let seq = self.global_seq.fetch_add(num_records + 1, Ordering::SeqCst);
            let commit_marker_seq = seq + num_records;
            let mut stamped_any = false;
            if skip_prepared_check {
                for ((key, value, _), prepped) in
                    tx.buffered_writes.iter().zip(prepped_writes.iter_mut())
                {
                    if let Some(stamped) =
                        crate::dist_txn::stamp_2pc_commit_seq(key, value, commit_marker_seq)
                    {
                        let payload_bytes = stamped.as_bytes();
                        let (final_bytes, length) = if payload_bytes.len() < 64 {
                            (
                                payload_bytes.to_vec(),
                                (payload_bytes.len() as u64) | UNCOMPRESSED_FLAG,
                            )
                        } else {
                            let comp = lz4_flex::compress_prepend_size(payload_bytes);
                            let len = comp.len() as u64;
                            (comp, len)
                        };
                        let mut hasher = crc32fast::Hasher::new();
                        hasher.update(&final_bytes);
                        prepped.1 = final_bytes;
                        prepped.2 = length;
                        prepped.3 = hasher.finalize();
                        stamped_any = true;
                    }
                }
            }
            let effective_heap_bytes = if stamped_any {
                prepped_writes
                    .iter()
                    .map(|(_, _, length, _, _)| length & !UNCOMPRESSED_FLAG)
                    .sum()
            } else {
                total_heap_bytes
            };
            let offset = self
                .heap_offset
                .fetch_add(effective_heap_bytes, Ordering::SeqCst);
            self.ssi_history.record_plain_batch(tx, commit_marker_seq);
            (seq, offset)
        };
        // write_mutex is RELEASED here â€” other batches can now reserve their sequences

        // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        // PHASE 3: I/O + Memtable â€” HEAP_FILE lock (separate from write_mutex)
        // Write to heap at our reserved offset, then WAL, then memtable.
        // Multiple batches can interleave here (they write to non-overlapping
        // heap regions) though the heap_file Mutex serializes actual writes.
        // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        let mut wal_records =
            Vec::with_capacity(prepped_writes.len() + tx.buffered_deletes.len() + 1);
        let mut current_seq = base_seq;
        let mut current_offset = reserved_offset;

        for (key, final_bytes, length, payload_crc32, expiry) in prepped_writes {
            wal_records.push((
                OmniRecord::new(
                    current_seq,
                    key,
                    current_offset,
                    length,
                    payload_crc32,
                    expiry,
                ),
                Some(final_bytes),
            ));
            current_offset += length & !UNCOMPRESSED_FLAG;
            current_seq += 1;
        }

        for key in &tx.buffered_deletes {
            wal_records.push((OmniRecord::new(current_seq, key.clone(), 0, 0, 0, 0), None));
            current_seq += 1;
        }

        let commit_marker = OmniRecord::new(
            current_seq,
            string_to_key("__COMMIT_MARKER__"),
            wal_records.len() as u64,
            0,
            0,
            0,
        );
        wal_records.push((commit_marker, None));

        // Heap write using POSITIONAL I/O at reserved offsets.
        // Each batch writes at its reserved offset via pwrite/seek_write,
        // so concurrent batches write to non-overlapping regions without
        // needing the heap_file Mutex for correctness.
        // The Mutex is still held to serialize fsync calls.
        {
            let heap_writer = self
                .heap_file
                .lock()
                .map_err(|_| OmniError::LockPoisoned("heap lock".into()))?;

            let mut write_offset = reserved_offset;
            for (_, payload) in &wal_records {
                if let Some(bytes) = payload {
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::FileExt as _;
                        heap_writer
                            .write_all_at(bytes, write_offset)
                            .map_err(|e| OmniError::IoError(format!("heap pwrite: {}", e)))?;
                    }
                    #[cfg(windows)]
                    {
                        let mut remaining = &bytes[..];
                        let mut pos = write_offset;
                        while !remaining.is_empty() {
                            let n = heap_writer
                                .seek_write(remaining, pos)
                                .map_err(|e| OmniError::IoError(format!("heap pwrite: {}", e)))?;
                            if n == 0 {
                                return Err(OmniError::IoError(
                                    "heap pwrite: zero-length write".into(),
                                ));
                            }
                            remaining = &remaining[n..];
                            pos += n as u64;
                        }
                    }
                    write_offset += bytes.len() as u64;
                }
            }
            // No sync here — group commit leader syncs below
        }

        // WAL write — append without fsync (group commit syncs below)
        {
            let mut wal = self
                .wal
                .lock()
                .map_err(|_| OmniError::LockPoisoned("wal lock".into()))?;
            if self.group_commit.is_poisoned() {
                return Err(OmniError::IoError(
                    "group commit engine poisoned by an earlier fsync failure".into(),
                ));
            }
            wal.append_batch_nosync(&wal_records)?;
        }

        // ── GROUP COMMIT: batch heap + WAL fsyncs ──
        let sync_outcome = {
            let guard = self.group_commit.join_group()?;
            if guard.is_leader {
                let result = self.sync_group_commit_files();
                // Followers are released with this outcome.
                guard.mark_synced(result.clone());
                result
            } else {
                Ok(())
            }
        };
        // The batch is not on disk; it must not become visible.
        sync_outcome?;

        // Memtable insertion (SkipMap is lock-free for concurrent inserts)
        let memtable = self.roots.load().memtable.clone();
        for (rec, _) in &wal_records {
            if key_to_string(&rec.key) != "__COMMIT_MARKER__" {
                let shard = shard_idx(&rec.key);
                memtable[shard].insert(
                    (rec.key.clone(), Reverse(rec.seq)),
                    (rec.offset, rec.length, rec.payload_crc32, rec.expiry),
                );
            }
        }

        // Metrics
        if let Ok(mut lock) = self.metrics.commit_latencies.lock() {
            let _ = lock.record(start_time.elapsed().as_micros() as u64);
        }

        metrics_prometheus::WRITE_LATENCY.observe(start_time.elapsed().as_secs_f64());
        metrics_prometheus::MEMTABLE_SIZE.set(self.memtable_size() as i64);
        metrics_prometheus::SSTABLE_COUNT
            .set((self.sstable_count() + self.l1_sstable_count()) as i64);
        metrics_prometheus::COMMIT_RATE.inc();

        Ok(current_seq)
    }

    /// fsyncs the heap then the WAL, holding the heap lock across both.
    fn sync_group_commit_files(&self) -> Result<(), OmniError> {
        let heap = self
            .heap_file
            .lock()
            .map_err(|_| OmniError::LockPoisoned("heap_file lock".into()))?;
        let mut wal = self
            .wal
            .lock()
            .map_err(|_| OmniError::LockPoisoned("wal lock".into()))?;

        let result = crate::failpoints::maybe_fail("heap::sync_data")
            .map_err(|e| OmniError::IoError(format!("heap failure point: {e}")))
            .and_then(|()| {
                heap.sync_data()
                    .map_err(|e| OmniError::IoError(format!("group commit heap fsync failed: {e}")))
            })
            .and_then(|()| {
                wal.sync()
                    .map_err(|e| OmniError::IoError(format!("group commit wal fsync failed: {e}")))
            });

        if let Err(ref e) = result {
            self.group_commit.poison(e.clone());
            if let Err(te) = wal.discard_undurable() {
                eprintln!("[WAL] failed to discard undurable bytes after fsync failure: {te:?}");
            }
        }
        result
    }

    fn read_from_heap_with_reader(
        &self,
        heap_reader: &File,
        heap_generation: u64,
        offset: u64,
        length_with_flag: u64,
        expected_crc32: u32,
    ) -> Result<String, OmniError> {
        let mut buf = Vec::new();
        self.read_from_heap_with_reader_into(
            heap_reader,
            heap_generation,
            offset,
            length_with_flag,
            expected_crc32,
            &mut buf,
        )
    }

    fn read_from_heap_with_reader_into(
        &self,
        heap_reader: &File,
        heap_generation: u64,
        offset: u64,
        length_with_flag: u64,
        expected_crc32: u32,
        buf: &mut Vec<u8>,
    ) -> Result<String, OmniError> {
        let cache_key = (heap_generation, offset);
        if let Some(cached) = self.block_cache.get(&cache_key) {
            return Ok(cached);
        }

        let is_uncompressed = (length_with_flag & UNCOMPRESSED_FLAG) != 0;
        let length = length_with_flag & !UNCOMPRESSED_FLAG;

        buf.clear();
        buf.resize(length as usize, 0);

        #[cfg(unix)]
        {
            heap_reader.read_exact_at(buf.as_mut_slice(), offset)?;
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            // seek_read can return fewer bytes than requested (short read).
            // Loop until the entire buffer is filled.
            let mut total_read = 0usize;
            while total_read < buf.len() {
                let n = heap_reader
                    .seek_read(&mut buf[total_read..], offset + total_read as u64)
                    .map_err(|e| OmniError::IoError(format!("heap seek_read: {}", e)))?;
                if n == 0 {
                    return Err(OmniError::IoError("heap seek_read: unexpected EOF".into()));
                }
                total_read += n;
            }
        }

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(buf.as_slice());
        if hasher.finalize() != expected_crc32 {
            return Err(OmniError::IoError("Payload CRC32 mismatch!".into()));
        }

        let result = if is_uncompressed {
            Ok(String::from_utf8_lossy(buf.as_slice()).to_string())
        } else {
            let decompressed = lz4_flex::decompress_size_prepended(buf.as_slice())
                .map_err(|e| OmniError::IoError(format!("LZ4 Error: {:?}", e)))?;
            Ok(String::from_utf8_lossy(&decompressed).to_string())
        };

        if let Ok(ref val) = result {
            self.block_cache.insert(cache_key, val.clone());
        }

        result
    }

    /// Finds a value by its key, up to the specified read sequence number (MVCC).
    /// Returns `Ok(None)` if the key does not exist or has been deleted.
    /// Internal bypass for Raft: Gets the absolute latest version of a key, ignoring MVCC snapshots.
    pub fn find_latest_internal(&self, target_key: &str) -> Result<Option<String>, OmniError> {
        self.find(target_key, u64::MAX)
    }

    /// Internal bypass for Raft Snapshots: Iterates over the entire keyspace at the absolute latest sequence.
    pub(crate) fn scan_all_latest_internal(&self) -> Result<ScanIterator<'_>, OmniError> {
        self.scan_iter_bounds(Vec::new(), ScanEnd::Unbounded, u64::MAX)
    }

    pub fn find(&self, target_key: &str, read_seq: u64) -> Result<Option<String>, OmniError> {
        let start_time = std::time::Instant::now();
        let target_key = string_to_key(target_key);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let shard = shard_idx(&target_key);
        let roots = self.roots.load();
        if read_seq < roots.read_seq_floor {
            return Err(OmniError::StaleSnapshot {
                read_seq,
                floor: roots.read_seq_floor,
            });
        }

        let memtable = roots.memtable.clone();
        // Reverse(seq) ordering: exclusive lower bound at Reverse(read_seq+1) includes read_seq.
        let memtable_start = Reverse(read_seq.saturating_add(1));
        for entry in memtable[shard].range((target_key.clone(), memtable_start)..) {
            if entry.key().0 != target_key {
                break;
            }
            if entry.key().1.0 <= read_seq {
                let (_, length, _, expiry) = entry.value();
                if *length == 0 {
                    return Ok(None);
                }
                if *expiry > 0 && *expiry <= now {
                    return Ok(None);
                }
                let val = self.read_from_heap_with_reader(
                    &roots.heap_reader,
                    roots.heap_generation,
                    entry.value().0,
                    *length,
                    entry.value().2,
                )?;
                if let Ok(mut lock) = self.metrics.read_latencies.lock() {
                    let _ = lock.record(start_time.elapsed().as_micros() as u64);
                }
                return Ok(Some(val));
            }
        }

        let frozen_memtables = roots.frozen_memtables.clone();
        for frozen in frozen_memtables.iter().rev() {
            let frozen_start = Reverse(read_seq.saturating_add(1));
            for entry in frozen[shard].range((target_key.clone(), frozen_start)..) {
                if entry.key().0 != target_key {
                    break;
                }
                if entry.key().1.0 <= read_seq {
                    let (_, length, _, expiry) = entry.value();
                    if *length == 0 {
                        return Ok(None);
                    }
                    if *expiry > 0 && *expiry <= now {
                        return Ok(None);
                    }
                    let val = self.read_from_heap_with_reader(
                        &roots.heap_reader,
                        roots.heap_generation,
                        entry.value().0,
                        *length,
                        entry.value().2,
                    )?;
                    if let Ok(mut lock) = self.metrics.read_latencies.lock() {
                        let _ = lock.record(start_time.elapsed().as_micros() as u64);
                    }
                    return Ok(Some(val));
                }
            }
        }

        let mut pointer = None;
        let sstables = roots.sstables.clone();
        for (mmap, bloom, _) in sstables.iter().rev() {
            let mut h = FnvHasher::default();
            target_key.hash(&mut h);
            if bloom.might_contain(h.finish()) {
                let reader = SSTableReader::from_mmap(mmap.clone());
                if let Some(ptr) = reader.find(&target_key, read_seq) {
                    pointer = Some(ptr);
                    break;
                }
            }
        }

        if pointer.is_none() {
            let l1_sstables = roots.l1_sstables.clone();
            for (mmap, bloom, _) in l1_sstables.iter().rev() {
                let mut h = FnvHasher::default();
                target_key.hash(&mut h);
                if bloom.might_contain(h.finish()) {
                    let reader = SSTableReader::from_mmap(mmap.clone());
                    if let Some(ptr) = reader.find(&target_key, read_seq) {
                        pointer = Some(ptr);
                        break;
                    }
                }
            }
        }

        if pointer.is_none() {
            let base_bloom = roots.base_bloom.clone();
            let mut h = FnvHasher::default();
            target_key.hash(&mut h);
            if base_bloom.might_contain(h.finish()) {
                let base_mmap = roots.base_mmap.clone();
                let reader = SSTableReader::from_mmap(base_mmap);
                pointer = reader.find(&target_key, read_seq);
            }
        }

        if let Some((offset, length, crc, expiry)) = pointer {
            if length == 0 {
                return Ok(None);
            }
            if expiry > 0 && expiry <= now {
                return Ok(None);
            }
            let val = self.read_from_heap_with_reader(
                &roots.heap_reader,
                roots.heap_generation,
                offset,
                length,
                crc,
            )?;
            if let Ok(mut lock) = self.metrics.read_latencies.lock() {
                let _ = lock.record(start_time.elapsed().as_micros() as u64);
            }
            metrics_prometheus::READ_LATENCY.observe(start_time.elapsed().as_secs_f64());
            return Ok(Some(val));
        }

        metrics_prometheus::READ_LATENCY.observe(start_time.elapsed().as_secs_f64());
        Ok(None)
    }

    /// Returns an iterator over all key-value pairs within the given lexical range,
    /// up to the specified read sequence number (MVCC).
    ///
    /// Uses a merge-sort approach: collects candidate records from all levels
    /// (memtable, frozen memtables, L0, L1, base), deduplicates by key keeping
    /// only the newest version visible at `read_seq`, then lazily reads values
    /// from the heap only when consumed.
    ///
    /// This compatibility iterator cannot report lazy heap errors: `next`
    /// returns `None` on a read failure. Use `scan` for an all-or-error result.
    pub fn scan_iter(
        &self,
        start_key: &str,
        end_key: &str,
        read_seq: u64,
    ) -> Result<impl Iterator<Item = (String, String)> + '_, OmniError> {
        self.scan_iter_inner(start_key, end_key, read_seq)
    }

    fn scan_iter_inner(
        &self,
        start_key: &str,
        end_key: &str,
        read_seq: u64,
    ) -> Result<ScanIterator<'_>, OmniError> {
        let start_key = string_to_key(start_key);
        let end_key = string_to_key(end_key);
        self.scan_iter_bounds(start_key, ScanEnd::Inclusive(&end_key), read_seq)
    }

    fn scan_iter_bounds(
        &self,
        start_key: Vec<u8>,
        end_bound: ScanEnd<'_>,
        read_seq: u64,
    ) -> Result<ScanIterator<'_>, OmniError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Phase 1: Collect candidate records from one consistent root snapshot.
        let roots = self.roots.load();
        if read_seq < roots.read_seq_floor {
            return Err(OmniError::StaleSnapshot {
                read_seq,
                floor: roots.read_seq_floor,
            });
        }
        let mut candidates: Vec<ScanCandidate> = Vec::new();

        // Memtable (highest priority — newest data)
        let active_mem = roots.memtable.clone();
        for shard in active_mem.iter() {
            let memtable_start = Reverse(read_seq.saturating_add(1));
            for entry in shard.range((start_key.clone(), memtable_start)..) {
                let key = entry.key().0.clone();
                if end_bound.is_past(&key) {
                    break;
                }
                let seq = entry.key().1.0;
                if seq <= read_seq {
                    candidates.push(ScanCandidate {
                        key,
                        seq,
                        offset: entry.value().0,
                        length: entry.value().1,
                        crc: entry.value().2,
                        expiry: entry.value().3,
                    });
                }
            }
        }

        // Frozen memtables (second priority)
        let frozen = roots.frozen_memtables.clone();
        for f_mem in frozen.iter().rev() {
            for shard in f_mem.iter() {
                let frozen_start = Reverse(read_seq.saturating_add(1));
                for entry in shard.range((start_key.clone(), frozen_start)..) {
                    let key = entry.key().0.clone();
                    if end_bound.is_past(&key) {
                        break;
                    }
                    let seq = entry.key().1.0;
                    if seq <= read_seq {
                        candidates.push(ScanCandidate {
                            key,
                            seq,
                            offset: entry.value().0,
                            length: entry.value().1,
                            crc: entry.value().2,
                            expiry: entry.value().3,
                        });
                    }
                }
            }
        }

        // L0 SSTables (newest first)
        let sstables = roots.sstables.clone();
        for (mmap, _, _) in sstables.iter().rev() {
            let reader = SSTableReader::from_mmap(mmap.clone());
            // Sparse-index seeks may rewind before start_key; enforce both
            // bounds before collecting any persisted candidate.
            for rec in reader.iter_from(&start_key) {
                if end_bound.is_past(&rec.key) {
                    break;
                }
                if rec.key >= start_key && rec.is_valid() && rec.seq <= read_seq {
                    candidates.push(ScanCandidate {
                        key: rec.key,
                        seq: rec.seq,
                        offset: rec.offset,
                        length: rec.length,
                        crc: rec.payload_crc32,
                        expiry: rec.expiry,
                    });
                }
            }
        }

        // L1 SSTables
        let l1_sstables = roots.l1_sstables.clone();
        for (mmap, _, _) in &**l1_sstables {
            let reader = SSTableReader::from_mmap(mmap.clone());
            for rec in reader.iter_from(&start_key) {
                if end_bound.is_past(&rec.key) {
                    break;
                }
                if rec.key >= start_key && rec.is_valid() && rec.seq <= read_seq {
                    candidates.push(ScanCandidate {
                        key: rec.key,
                        seq: rec.seq,
                        offset: rec.offset,
                        length: rec.length,
                        crc: rec.payload_crc32,
                        expiry: rec.expiry,
                    });
                }
            }
        }

        // Base file (oldest data, lowest priority)
        let base_mmap = roots.base_mmap.clone();
        let reader = SSTableReader::from_mmap(base_mmap);
        for rec in reader.iter_from(&start_key) {
            if end_bound.is_past(&rec.key) {
                break;
            }
            if rec.key >= start_key && rec.is_valid() && rec.seq <= read_seq {
                candidates.push(ScanCandidate {
                    key: rec.key,
                    seq: rec.seq,
                    offset: rec.offset,
                    length: rec.length,
                    crc: rec.payload_crc32,
                    expiry: rec.expiry,
                });
            }
        }

        // Phase 2: Sort by (key ASC, seq DESC) â€” this groups duplicates together
        // with the newest version first, enabling O(N) deduplication in one pass.
        candidates.sort_unstable_by(|a, b| a.key.cmp(&b.key).then(b.seq.cmp(&a.seq)));

        // Phase 3: Deduplicate â€” keep only the first (newest) entry per key.
        candidates.dedup_by(|a, b| a.key == b.key);

        // Phase 4: Lazy heap I/O — retain the heap reader and generation from
        // the exact StorageRoots snapshot that produced these offsets.
        Ok(ScanIterator {
            db: self,
            heap_reader: roots.heap_reader.clone(),
            heap_generation: roots.heap_generation,
            candidates: candidates.into_iter(),
            now,
            read_buffer: self.scan_buffer_pool.acquire(),
        })
    }

    /// Returns the sequence number of the latest version of a key visible at `read_seq`.
    /// Used for Optimistic Concurrency Control (OCC) conflict detection.
    pub fn get_seq_for_key(&self, target_key: &str, read_seq: u64) -> u64 {
        let target_key = target_key.as_bytes().to_vec();
        let shard = shard_idx(&target_key);

        // 1. Active memtable
        let memtable = self.roots.load().memtable.clone();
        let memtable_start = Reverse(read_seq.saturating_add(1));
        for entry in memtable[shard].range((target_key.clone(), memtable_start)..) {
            if entry.key().0 != target_key {
                break;
            }
            if entry.key().1.0 <= read_seq {
                return entry.key().1.0;
            }
        }

        // 2. Frozen memtables
        let frozen_memtables = self.roots.load().frozen_memtables.clone();
        for frozen in frozen_memtables.iter().rev() {
            let frozen_start = Reverse(read_seq.saturating_add(1));
            for entry in frozen[shard].range((target_key.clone(), frozen_start)..) {
                if entry.key().0 != target_key {
                    break;
                }
                if entry.key().1.0 <= read_seq {
                    return entry.key().1.0;
                }
            }
        }

        // Helper: search SSTables via iter_from (returns OmniRecord with .seq)
        let search_sstable = |mmap: &Arc<Mmap>, bloom: &BloomFilter| -> Option<u64> {
            let mut h = FnvHasher::default();
            target_key.hash(&mut h);
            if !bloom.might_contain(h.finish()) {
                return None;
            }
            let reader = SSTableReader::from_mmap(mmap.clone());
            let mut best_seq = 0u64;
            for rec in reader.iter_from(&target_key) {
                if rec.key > target_key {
                    break;
                }
                if rec.key == target_key
                    && rec.is_valid()
                    && rec.seq <= read_seq
                    && rec.seq > best_seq
                {
                    best_seq = rec.seq;
                }
            }
            if best_seq > 0 { Some(best_seq) } else { None }
        };

        // 3. L0 SSTables (newest first)
        let sstables = self.roots.load().sstables.clone();
        for (mmap, bloom, _) in sstables.iter().rev() {
            if let Some(seq) = search_sstable(mmap, bloom) {
                return seq;
            }
        }

        // 4. L1 SSTables
        let l1_sstables = self.roots.load().l1_sstables.clone();
        for (mmap, bloom, _) in l1_sstables.iter().rev() {
            if let Some(seq) = search_sstable(mmap, bloom) {
                return seq;
            }
        }

        // 5. Base file
        {
            let base_bloom = self.roots.load().base_bloom.clone();
            let mut h = FnvHasher::default();
            target_key.hash(&mut h);
            if !base_bloom.might_contain(h.finish()) {
                return 0;
            }
        }
        {
            let base_mmap = self.roots.load().base_mmap.clone();
            let reader = SSTableReader::from_mmap(base_mmap);
            let mut best_seq = 0u64;
            for rec in reader.iter_from(&target_key) {
                if rec.key > target_key {
                    break;
                }
                if rec.key == target_key
                    && rec.is_valid()
                    && rec.seq <= read_seq
                    && rec.seq > best_seq
                {
                    best_seq = rec.seq;
                }
            }
            if best_seq > 0 {
                return best_seq;
            }
        }
        0
    }

    fn notify_compaction_waiters(&self) {
        let (ref stall_lock, ref cv) = self.compaction_cv;
        let _guard = stall_lock.lock().unwrap_or_else(|e| e.into_inner());
        cv.notify_all();
    }

    /// Flushes the active memtable to disk as an L0 SSTable.
    pub fn compact_sstables(&self) -> Result<(), OmniError> {
        self.ensure_writes_not_fenced()?;
        {
            let _topology_guard = self
                .transition_guard
                .read()
                .map_err(|_| OmniError::LockPoisoned("transition_guard".into()))?;
            self.compact_sstables_locked()?;
        }
        // Only rotate (truncate) the WAL AFTER the flushed SSTable and manifest
        // are durably published, and only when no concurrent writers or
        // unflushed memtable entries exist. Otherwise keep the WAL intact so
        // concurrent writes are never lost on crash.
        if let Ok(_exclusive) = self.transition_guard.try_write()
            && let Ok(_write_guard) = self.write_mutex.lock()
            && !self.is_write_fenced()
        {
            let roots = self.roots.load();
            let active_empty = roots.memtable.iter().all(|shard| shard.is_empty());
            let frozen_empty = roots.frozen_memtables.is_empty();
            if active_empty
                && frozen_empty
                && let Ok(mut wal) = self.wal.lock()
            {
                let _ = wal.rotate_segment();
            }
        }
        Ok(())
    }

    /// Compacts the active memtable while the caller already holds the
    /// transition barrier. Backup creation uses this form while holding the
    /// barrier exclusively; reacquiring the read side there would deadlock.
    pub(crate) fn compact_sstables_locked(&self) -> Result<(), OmniError> {
        self.ensure_writes_not_fenced()?;
        let started_at = std::time::Instant::now();
        let mut stats = CompactionStats::default();
        let frozen_arc = {
            let _guard = self
                .write_mutex
                .lock()
                .map_err(|_| OmniError::LockPoisoned("compact".into()))?;
            self.ensure_writes_not_fenced()?;
            let new_empty: SharedMemtable = Arc::new(std::array::from_fn(|_| SkipMap::new()));
            // Atomically: swap memtable to empty and add old one to frozen list.
            // Do NOT truncate the WAL here: the frozen memtable is not durable
            // until the L0 SSTable and updated manifest are fsynced below.
            let current = self.roots.load();
            let old = current.memtable.clone();
            let mut new_frozen = current.frozen_memtables.as_ref().clone();
            new_frozen.push(old.clone());
            self.roots.store(Arc::new(StorageRoots {
                memtable: new_empty,
                frozen_memtables: Arc::new(new_frozen),
                base_mmap: current.base_mmap.clone(),
                base_bloom: current.base_bloom.clone(),
                sstables: current.sstables.clone(),
                l1_sstables: current.l1_sstables.clone(),
                manifest: current.manifest.clone(),
                heap_reader: current.heap_reader.clone(),
                heap_generation: current.heap_generation,
                read_seq_floor: current.read_seq_floor,
            }));
            old
        };

        let mut records = Vec::new();

        for shard in frozen_arc.iter() {
            for entry in shard.iter() {
                let key = entry.key().0.clone();
                let seq = entry.key().1.0;
                let val = entry.value();
                if val.1 == 0 {
                    stats.tombstones += 1;
                }
                stats.bytes_rewritten += val.1 & !UNCOMPRESSED_FLAG;
                records.push(OmniRecord::new(seq, key, val.0, val.1, val.2, val.3));
            }
        }

        if records.is_empty() {
            self.remove_frozen_memtable(&frozen_arc)?;
            stats.record(
                "memtable_to_l0",
                started_at,
                self.compaction_backlog_sstables(),
            );
            return Ok(());
        }
        records.sort_by_key(|a| a.key.clone());

        let mut bloom = BloomFilter::new(records.len().max(100));
        for rec in &records {
            let mut h = FnvHasher::default();
            rec.key.hash(&mut h);
            bloom.add(h.finish());
        }

        let (sst_path, file) = self.create_unique_output(|token| {
            format!(
                "{}_l0_0_{}_{}.sst",
                self.manifest_path,
                std::time::UNIX_EPOCH
                    .elapsed()
                    .unwrap_or_default()
                    .as_millis(),
                token
            )
        })?;
        let mut output_guard =
            CompactionOutputGuard::new(vec![sst_path.clone(), sst_path.replace(".sst", ".bloom")]);

        let mut writer = SSTableWriter::new(&file);
        for rec in &records {
            writer.append(rec)?;
        }
        writer.finish()?;

        file.sync_all()?;
        bloom.save(&sst_path.replace(".sst", ".bloom"))?;

        // SAFETY: The new L0 SSTable is fully written, flushed, and fsynced
        // before this read-only mapping is created. After publication, OmniKV
        // treats the table as immutable and replaces it only by root swap.
        let mmap = unsafe { MmapOptions::new().map(&file)? };

        let _publish_guard = self
            .write_mutex
            .lock()
            .map_err(|_| OmniError::LockPoisoned("compact_publish".into()))?;
        self.ensure_writes_not_fenced()?;

        let mut manifest = self.roots.load().manifest.clone().as_ref().clone();
        manifest.sstables.push(sst_path.clone());
        manifest.max_seq = manifest.max_seq.max(self.global_seq.load(Ordering::SeqCst));
        self.publish_manifest_or_fence(&manifest, Some(&mut output_guard))?;

        // CRITICAL: Publish manifest + sstables + frozen removal in a SINGLE
        // atomic swap under `write_mutex`.
        let cur_roots = self.roots.load();
        let mut sstables = cur_roots.sstables.clone().as_ref().clone();
        sstables.push((Arc::new(mmap), Arc::new(bloom), sst_path));
        let mut frozen = cur_roots.frozen_memtables.clone().as_ref().clone();
        if let Some(pos) = frozen
            .iter()
            .position(|entry| Arc::ptr_eq(entry, &frozen_arc))
        {
            frozen.remove(pos);
        }
        self.update_roots(|r| StorageRoots {
            manifest: Arc::new(manifest),
            sstables: Arc::new(sstables),
            frozen_memtables: Arc::new(frozen),
            ..r.clone()
        });
        output_guard.disarm();
        drop(_publish_guard);

        stats.record(
            "memtable_to_l0",
            started_at,
            self.compaction_backlog_sstables(),
        );

        Ok(())
    }

    /// Compacts multiple L0 SSTables into a single L1 SSTable while preserving
    /// deletion markers so lower-level values cannot reappear.
    pub fn compact_l0_to_l1(&self) -> Result<(), OmniError> {
        let started_at = std::time::Instant::now();
        let mut stats = CompactionStats::default();
        let _topology_guard = self
            .transition_guard
            .read()
            .map_err(|_| OmniError::LockPoisoned("transition_guard".into()))?;
        let write_guard = self
            .write_mutex
            .lock()
            .map_err(|_| OmniError::LockPoisoned("compact_l0".into()))?;
        self.ensure_writes_not_fenced()?;
        let sstables = self.roots.load().sstables.clone();
        if sstables.is_empty() {
            return Ok(());
        }

        let retention_floor = self.compaction_retention_floor()?;
        let mut merged: BTreeMap<Vec<u8>, VersionedValuePointers> = BTreeMap::new();

        for (mmap, _, _) in &**sstables {
            let reader = SSTableReader::from_mmap(mmap.clone());
            for rec in reader.iter_from(b"") {
                if rec.is_valid() {
                    if rec.length == 0 {
                        stats.tombstones += 1;
                    }
                    insert_record_version(&mut merged, &rec);
                }
            }
        }

        let mut records = Vec::new();
        for (key, mut versions) in merged {
            retain_versions_needed_by_floor(&mut versions, retention_floor);
            for (seq, (offset, length, crc, expiry)) in versions {
                stats.bytes_rewritten += length & !UNCOMPRESSED_FLAG;
                records.push(OmniRecord::new(
                    seq,
                    key.clone(),
                    offset,
                    length,
                    crc,
                    expiry,
                ));
            }
        }

        if records.is_empty() {
            let mut manifest = self.roots.load().manifest.clone().as_ref().clone();
            manifest.sstables.clear();
            self.publish_manifest_or_fence(&manifest, None)?;
            self.update_roots(|r| StorageRoots {
                manifest: Arc::new(manifest),
                sstables: Arc::new(Vec::new()),
                ..r.clone()
            });

            let old_paths: Vec<String> = sstables.iter().map(|(_, _, p)| p.clone()).collect();
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_secs(5));
                for path in old_paths {
                    cleanup_obsolete_sstable_pair(&path, "l0_empty_cleanup");
                }
            });

            stats.record("l0_to_l1", started_at, self.compaction_backlog_sstables());
            drop(write_guard);
            self.notify_compaction_waiters();
            return Ok(());
        }

        records.sort_by_key(|a| a.key.clone());

        let mut bloom = BloomFilter::new(records.len().max(100));
        for rec in &records {
            let mut h = FnvHasher::default();
            rec.key.hash(&mut h);
            bloom.add(h.finish());
        }

        let (sst_path, file) = self.create_unique_output(|token| {
            format!(
                "{}_l1_{}_{}.sst",
                self.manifest_path,
                std::time::UNIX_EPOCH
                    .elapsed()
                    .unwrap_or_default()
                    .as_millis(),
                token
            )
        })?;
        let mut output_guard =
            CompactionOutputGuard::new(vec![sst_path.clone(), sst_path.replace(".sst", ".bloom")]);
        let mut writer = SSTableWriter::new(&file);
        for rec in &records {
            writer.append(rec)?;
        }
        writer.finish()?;
        file.sync_all()?;
        bloom.save(&sst_path.replace(".sst", ".bloom"))?;

        // SAFETY: The new L1 SSTable is fully materialized and synced before
        // mapping. Compaction installs the mapping via an atomic root swap and
        // does not mutate the file afterward.
        let mmap = unsafe { MmapOptions::new().map(&file)? };

        let mut manifest = self.roots.load().manifest.clone().as_ref().clone();
        manifest.sstables.clear();
        manifest.l1_sstables.push(sst_path.clone());
        manifest.max_seq = manifest.max_seq.max(self.global_seq.load(Ordering::SeqCst));
        self.publish_manifest_or_fence(&manifest, Some(&mut output_guard))?;

        // CRITICAL: Publish manifest + l1_sstables + cleared L0 in a SINGLE atomic swap.
        let mut l1_sstables = self.roots.load().l1_sstables.clone().as_ref().clone();
        l1_sstables.push((Arc::new(mmap), Arc::new(bloom), sst_path));
        self.update_roots(|r| StorageRoots {
            manifest: Arc::new(manifest),
            l1_sstables: Arc::new(l1_sstables),
            sstables: Arc::new(Vec::new()),
            ..r.clone()
        });
        output_guard.disarm();

        let old_paths: Vec<String> = sstables.iter().map(|(_, _, p)| p.clone()).collect();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(5));
            for path in old_paths {
                cleanup_obsolete_sstable_pair(&path, "l0_to_l1_cleanup");
            }
        });

        stats.record("l0_to_l1", started_at, self.compaction_backlog_sstables());
        drop(write_guard);
        self.notify_compaction_waiters();
        Ok(())
    }

    pub fn compact_l1_to_l2(&self) -> Result<(), OmniError> {
        let started_at = std::time::Instant::now();
        let mut stats = CompactionStats::default();
        let _topology_guard = self
            .transition_guard
            .read()
            .map_err(|_| OmniError::LockPoisoned("transition_guard".into()))?;
        let _guard = self
            .write_mutex
            .lock()
            .map_err(|_| OmniError::LockPoisoned("compact_l1_l2".into()))?;
        self.ensure_writes_not_fenced()?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let retention_floor = self.compaction_retention_floor()?;
        let preserve_deletion_markers = retention_floor.is_some();
        let mut merged: BTreeMap<Vec<u8>, VersionedValuePointers> = BTreeMap::new();

        let base_mmap = self.roots.load().base_mmap.clone();
        let reader = SSTableReader::from_mmap(base_mmap);
        for rec in reader.iter_from(b"") {
            if rec.is_valid() {
                if rec.length == 0 {
                    stats.tombstones += 1;
                }
                insert_record_version(&mut merged, &rec);
            }
        }

        let l1_sstables_snap = self.roots.load().l1_sstables.clone();
        if l1_sstables_snap.is_empty() {
            return Ok(());
        }
        let num_l1_merged = l1_sstables_snap.len();

        for (mmap, _, _) in &**l1_sstables_snap {
            let reader = SSTableReader::from_mmap(mmap.clone());
            for rec in reader.iter_from(b"") {
                if rec.is_valid() {
                    if rec.length == 0 {
                        stats.tombstones += 1;
                    }
                    insert_record_version(&mut merged, &rec);
                }
            }
        }

        let mut records = Vec::new();
        for (key, mut versions) in merged {
            retain_versions_needed_by_floor(&mut versions, retention_floor);
            for (seq, (offset, length, crc, expiry)) in versions {
                let expired = expiry > 0 && expiry <= now;
                if (length == 0 || expired) && !preserve_deletion_markers {
                    if expired {
                        stats.expired_records_dropped += 1;
                    }
                    continue;
                }
                stats.bytes_rewritten += length & !UNCOMPRESSED_FLAG;
                records.push(OmniRecord::new(
                    seq,
                    key.clone(),
                    offset,
                    length,
                    crc,
                    expiry,
                ));
            }
        }

        records.sort_by_key(|a| a.key.clone());

        let mut bloom = BloomFilter::new(records.len().max(100));
        for rec in &records {
            let mut h = FnvHasher::default();
            rec.key.hash(&mut h);
            bloom.add(h.finish());
        }

        let (new_base_path, file) = self.create_unique_output(|token| {
            format!(
                "{}_base_{}_{}.sst",
                self.manifest_path,
                std::time::UNIX_EPOCH
                    .elapsed()
                    .unwrap_or_default()
                    .as_millis(),
                token
            )
        })?;
        let mut output_guard = CompactionOutputGuard::new(vec![
            new_base_path.clone(),
            new_base_path.replace(".sst", ".bloom"),
        ]);
        let mut writer = SSTableWriter::new(&file);
        for rec in &records {
            writer.append(rec)?;
        }
        writer.finish()?;
        file.sync_all()?;
        bloom.save(&new_base_path.replace(".sst", ".bloom"))?;

        // SAFETY: The replacement base SSTable is complete and fsynced before
        // mapping. Existing readers retain Arc-owned mappings for old files,
        // while new readers see this immutable mapping after the root swap.
        let new_base_mmap = unsafe { MmapOptions::new().map(&file)? };

        // Critical Section: Swap ALL topology in a SINGLE atomic update.
        let old_base_path = self.roots.load().manifest.clone().base_path.clone();

        let current_l1 = self.roots.load().l1_sstables.clone();
        let remaining_l1 = current_l1[num_l1_merged..].to_vec();

        let mut manifest = self.roots.load().manifest.clone().as_ref().clone();
        manifest.base_path = new_base_path;
        manifest.l1_sstables = remaining_l1
            .clone()
            .into_iter()
            .map(|(_, _, path)| path)
            .collect();
        self.publish_manifest_or_fence(&manifest, Some(&mut output_guard))?;

        self.update_roots(|r| StorageRoots {
            base_mmap: Arc::new(new_base_mmap),
            base_bloom: Arc::new(bloom),
            l1_sstables: Arc::new(remaining_l1),
            manifest: Arc::new(manifest),
            ..r.clone()
        });
        output_guard.disarm();

        let old_paths: Vec<String> = l1_sstables_snap[..num_l1_merged]
            .iter()
            .map(|(_, _, p)| p.clone())
            .collect();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(5));
            cleanup_obsolete_base_pair(&old_base_path, "l1_to_base_cleanup");
            for path in old_paths {
                cleanup_obsolete_sstable_pair(&path, "l1_to_base_cleanup");
            }
        });

        stats.record("l1_to_base", started_at, self.compaction_backlog_sstables());
        Ok(())
    }

    /// Runs the garbage collection process on the heap file, compacting it
    /// and freeing space taken up by obsolete values.
    pub fn run_garbage_collection(&self) -> Result<(), OmniError> {
        let started_at = std::time::Instant::now();
        let mut stats = CompactionStats::default();
        // Acquire the exclusive transition barrier plus write_mutex so no
        // writer or L0/L1 compaction can mutate memtables, publish stale-heap
        // SSTables, or race heap swaps until GC publishes the replacement root.
        // Lock-free readers keep using their previously captured Arc<StorageRoots>.
        let _topology_guard = self
            .transition_guard
            .write()
            .map_err(|_| OmniError::LockPoisoned("transition_guard".into()))?;
        let write_guard = self
            .write_mutex
            .lock()
            .map_err(|_| OmniError::LockPoisoned("GC write mutex".into()))?;
        self.ensure_writes_not_fenced()?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let retention_floor = self.compaction_retention_floor()?;
        let preserve_deletion_markers = retention_floor.is_some();
        let mut merged: BTreeMap<Vec<u8>, VersionedValuePointers> = BTreeMap::new();

        let mut add_to_merged =
            |key: Vec<u8>, seq: u64, offset: u64, length: u64, crc: u32, expiry: u64| {
                let versions = merged.entry(key).or_default();
                versions.insert(seq, (offset, length, crc, expiry));
            };

        let cur_roots = self.roots.load();
        let reader = SSTableReader::from_mmap(cur_roots.base_mmap.clone());
        for rec in reader.iter_from(b"") {
            if rec.is_valid() {
                add_to_merged(
                    rec.key,
                    rec.seq,
                    rec.offset,
                    rec.length,
                    rec.payload_crc32,
                    rec.expiry,
                );
            }
        }

        for (mmap, _, _) in &**cur_roots.sstables {
            let reader = SSTableReader::from_mmap(mmap.clone());
            for rec in reader.iter_from(b"") {
                if rec.is_valid() {
                    add_to_merged(
                        rec.key,
                        rec.seq,
                        rec.offset,
                        rec.length,
                        rec.payload_crc32,
                        rec.expiry,
                    );
                }
            }
        }

        for (mmap, _, _) in &**cur_roots.l1_sstables {
            let reader = SSTableReader::from_mmap(mmap.clone());
            for rec in reader.iter_from(b"") {
                if rec.is_valid() {
                    add_to_merged(
                        rec.key,
                        rec.seq,
                        rec.offset,
                        rec.length,
                        rec.payload_crc32,
                        rec.expiry,
                    );
                }
            }
        }

        for frozen in cur_roots.frozen_memtables.iter() {
            for shard in frozen.iter() {
                for entry in shard.iter() {
                    let key = entry.key().0.clone();
                    let seq = entry.key().1.0;
                    add_to_merged(
                        key,
                        seq,
                        entry.value().0,
                        entry.value().1,
                        entry.value().2,
                        entry.value().3,
                    );
                }
            }
        }

        for shard in cur_roots.memtable.iter() {
            for entry in shard.iter() {
                let key = entry.key().0.clone();
                let seq = entry.key().1.0;
                add_to_merged(
                    key,
                    seq,
                    entry.value().0,
                    entry.value().1,
                    entry.value().2,
                    entry.value().3,
                );
            }
        }

        let (new_heap_path, new_heap_file) = self.create_unique_output(|token| {
            format!(
                "{}_heap_compacted_{}_{}.bin",
                self.manifest_path,
                std::time::UNIX_EPOCH
                    .elapsed()
                    .unwrap_or_default()
                    .as_millis(),
                token
            )
        })?;
        let (new_base_path, new_base_file) = match self.create_unique_output(|token| {
            format!(
                "{}_database_compacted_{}_{}.bin",
                self.manifest_path,
                std::time::UNIX_EPOCH
                    .elapsed()
                    .unwrap_or_default()
                    .as_millis(),
                token
            )
        }) {
            Ok(output) => output,
            Err(err) => {
                let _ = std::fs::remove_file(&new_heap_path);
                return Err(err);
            }
        };
        let mut output_guard = CompactionOutputGuard::new(vec![
            new_heap_path.clone(),
            new_base_path.clone(),
            bloom_path_for_base(&new_base_path),
        ]);

        let mut new_heap_writer = BufWriter::new(&new_heap_file);
        let mut base_sst_writer = SSTableWriter::new(&new_base_file);

        let mut final_records = Vec::new();
        let mut bloom = BloomFilter::new(merged.len().max(100));
        let mut current_offset = 0;

        for (k, versions) in merged {
            let mut versions = versions;
            retain_versions_needed_by_floor(&mut versions, retention_floor);
            for (seq, (old_offset, length, crc, expiry)) in versions {
                if length == 0 {
                    stats.tombstones += 1;
                    if preserve_deletion_markers {
                        let mut h = FnvHasher::default();
                        k.hash(&mut h);
                        bloom.add(h.finish());

                        let rec = OmniRecord::new(seq, k.clone(), 0, 0, crc, expiry);
                        base_sst_writer.append(&rec)?;
                        final_records.push(rec);
                    }
                    continue;
                }
                if expiry > 0 && expiry <= now && !preserve_deletion_markers {
                    stats.expired_records_dropped += 1;
                    continue;
                }

                // Raw read, NO decompression!
                let manifest = self.roots.load().manifest.clone();
                let mut file = File::open(&manifest.heap_path)?;
                use std::io::Seek;
                file.seek(std::io::SeekFrom::Start(old_offset))?;
                let mut final_bytes = vec![0u8; (length & !UNCOMPRESSED_FLAG) as usize];
                file.read_exact(&mut final_bytes)?;

                // Validate CRC just to be safe
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(&final_bytes);
                if hasher.finalize() != crc {
                    return Err(OmniError::IoError("GC CRC32 mismatch!".into()));
                }
                let new_crc = crc;
                let new_length = length;
                stats.bytes_rewritten += new_length & !UNCOMPRESSED_FLAG;

                new_heap_writer.write_all(&final_bytes)?;

                let mut h = FnvHasher::default();
                k.hash(&mut h);
                bloom.add(h.finish());

                let rec =
                    OmniRecord::new(seq, k.clone(), current_offset, new_length, new_crc, expiry);
                base_sst_writer.append(&rec)?;
                final_records.push(rec);
                current_offset += new_length & !UNCOMPRESSED_FLAG;
            }
        }

        new_heap_writer.flush()?;
        base_sst_writer.finish()?;
        new_heap_file.sync_all()?;
        new_base_file.sync_all()?;

        drop(new_heap_writer);

        // SAFETY: Garbage collection writes a brand-new compacted base file,
        // fsyncs it, maps it read-only, and then publishes it atomically. Old
        // mmap handles remain Arc-owned until all readers release them.
        let new_base_mmap = unsafe { MmapOptions::new().map(&new_base_file)? };

        let new_heap_reader = OpenOptions::new().read(true).open(&new_heap_path)?;
        let old_manifest = cur_roots.manifest.as_ref().clone();
        let mut manifest_for_roots = old_manifest.clone();
        manifest_for_roots.heap_path = new_heap_path.clone();
        manifest_for_roots.base_path = new_base_path.clone();
        manifest_for_roots.sstables.clear();
        manifest_for_roots.l1_sstables.clear();
        let cur_next_seq = self.global_seq.load(Ordering::SeqCst);
        let checkpoint_seq = cur_next_seq.saturating_sub(1);
        manifest_for_roots.wal_checkpoint_seq =
            manifest_for_roots.wal_checkpoint_seq.max(checkpoint_seq);
        manifest_for_roots.max_seq = manifest_for_roots.max_seq.max(cur_next_seq);

        // Acquire `heap_file` BEFORE persisting the replacement manifest so no
        // fallible lock acquisition remains between `publish_manifest_or_fence`
        // and `output_guard.disarm()`.
        let mut heap_lock = self
            .heap_file
            .lock()
            .map_err(|_| OmniError::LockPoisoned("heap_file".into()))?;
        bloom.save(&bloom_path_for_base(&new_base_path))?;
        self.publish_manifest_or_fence(&manifest_for_roots, Some(&mut output_guard))?;

        // Writers and compactions are fenced by exclusive `transition_guard` +
        // `write_mutex`, and every memtable/frozen_memtable entry has been
        // rewritten into `new_base_mmap` / `new_heap_path`. Publish brand-new
        // empty memtables so no pointer addressing the old heap generation can
        // ever be read against the replacement heap. Existing readers keep
        // reading the previous `Arc<StorageRoots>` untouched.
        let new_empty_memtable: SharedMemtable = Arc::new(std::array::from_fn(|_| SkipMap::new()));
        *heap_lock = new_heap_file;
        self.heap_offset.store(current_offset, Ordering::SeqCst);
        self.roots.store(Arc::new(StorageRoots {
            base_mmap: Arc::new(new_base_mmap),
            base_bloom: Arc::new(bloom),
            sstables: Arc::new(Vec::new()),
            l1_sstables: Arc::new(Vec::new()),
            heap_reader: Arc::new(new_heap_reader),
            heap_generation: self.next_heap_generation.fetch_add(1, Ordering::Relaxed),
            read_seq_floor: cur_roots.read_seq_floor,
            manifest: Arc::new(manifest_for_roots),
            memtable: new_empty_memtable,
            frozen_memtables: Arc::new(Vec::new()),
        }));
        output_guard.disarm();
        self.block_cache.invalidate_all();

        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(5));
            cleanup_obsolete_file(&old_manifest.heap_path, "heap_gc_cleanup");
            cleanup_obsolete_base_pair(&old_manifest.base_path, "heap_gc_cleanup");
            for p in old_manifest
                .sstables
                .iter()
                .chain(old_manifest.l1_sstables.iter())
            {
                cleanup_obsolete_sstable_pair(p, "heap_gc_cleanup");
            }
        });

        match self.wal.lock() {
            Ok(mut wal) => {
                if let Err(err) = wal.clear() {
                    tracing::warn!(error = %err, "heap_gc WAL clear failed (checkpointed in manifest)");
                }
            }
            Err(_) => {
                tracing::warn!("heap_gc WAL lock poisoned while clearing WAL");
            }
        }

        stats.record("heap_gc", started_at, self.compaction_backlog_sstables());
        drop(heap_lock);
        drop(write_guard);
        self.notify_compaction_waiters();
        Ok(())
    }

    /// Starts a background compaction thread that monitors memtable size
    /// and SSTable counts, triggering compaction without blocking writes.
    ///
    /// Returns a handle to the background thread for graceful shutdown.
    pub fn start_background_compaction(
        self: &Arc<Self>,
        check_interval_ms: u64,
        memtable_flush_threshold: usize,
    ) -> std::thread::JoinHandle<()> {
        let db = Arc::clone(self);
        std::thread::Builder::new()
            .name("omni-compaction".into())
            .spawn(move || {
                let mut gc_counter: u32 = 0;
                loop {
                    std::thread::sleep(std::time::Duration::from_millis(check_interval_ms));
                    if db.is_write_fenced() {
                        continue;
                    }

                    let mut did_compact = false;
                    let compaction_policy = match db.compaction_policy() {
                        Ok(policy) => policy,
                        Err(e) => {
                            eprintln!("[COMPACTION] Policy read error: {:?}", e);
                            continue;
                        }
                    };
                    metrics_prometheus::record_compaction_backlog(db.compaction_backlog_sstables());

                    // Phase 1: Flush memtable to L0 if it exceeds threshold
                    if db.memtable_size() > memtable_flush_threshold {
                        if let Err(e) = db.compact_sstables() {
                            eprintln!("[COMPACTION] Memtable flush error: {:?}", e);
                        } else {
                            did_compact = true;
                        }
                    }

                    // Phase 2: Compact L0 → L1 if too many L0 SSTables
                    if db.sstable_count() >= compaction_policy.l0_compaction_trigger {
                        if let Err(e) = db.compact_l0_to_l1() {
                            eprintln!("[COMPACTION] L0→L1 error: {:?}", e);
                        } else {
                            did_compact = true;
                        }
                    }

                    // Phase 3: Compact L1 → L2 (base) if too many L1 SSTables
                    if db.l1_sstable_count() >= compaction_policy.l1_compaction_trigger {
                        if let Err(e) = db.compact_l1_to_l2() {
                            eprintln!("[COMPACTION] L1→L2 error: {:?}", e);
                        } else {
                            did_compact = true;
                        }
                    }

                    // Phase 4: MVCC Garbage Collection — every 5th compaction cycle
                    if did_compact {
                        gc_counter += 1;
                        if gc_counter.is_multiple_of(5)
                            && let Err(e) = db.run_garbage_collection()
                        {
                            eprintln!("[GC] MVCC garbage collection error: {:?}", e);
                        }
                    }
                }
            })
            .expect("Failed to spawn compaction thread")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    use tempfile::TempDir;

    #[test]
    fn scan_reports_heap_errors_and_returns_its_pooled_buffer() {
        let dir = TempDir::new().expect("tempdir");
        let manifest = dir.path().join("manifest");
        let wal = dir.path().join("wal");
        let db = OmniKV::open(
            manifest.to_str().expect("UTF-8 manifest path"),
            wal.to_str().expect("UTF-8 WAL path"),
        )
        .expect("open");
        let mut batch = WriteBatch::new();
        batch
            .set("a_valid", "value".into())
            .expect("stage valid value");
        db.commit_batch(&batch).expect("commit valid value");
        db.insert_replica_record(
            b"z_unreadable".to_vec(),
            db.get_seq() + 1,
            u64::MAX / 2,
            1,
            0,
            0,
        );
        let seq = db.get_seq();

        // Existing lazy callers retain their (String, String) item type and
        // legacy stop-on-error behavior. Only strict collection reports it.
        let legacy: Vec<(String, String)> = db
            .scan_iter("a", "z\x7F", seq)
            .expect("create legacy scan")
            .collect();
        assert_eq!(legacy, [("a_valid".into(), "value".into())]);
        assert_eq!(db.scan_buffer_pool_available(), 1);

        assert!(matches!(
            db.scan("a", "z\x7F", seq),
            Err(OmniError::IoError(_))
        ));
        assert_eq!(
            db.scan_buffer_pool_available(),
            1,
            "a failed scan must return its one reusable heap buffer"
        );
    }

    #[test]
    fn scan_preserves_snapshot_range_ttl_and_tombstone_selection() {
        let dir = TempDir::new().expect("tempdir");
        let manifest = dir.path().join("manifest");
        let wal = dir.path().join("wal");
        let db = OmniKV::open(
            manifest.to_str().expect("UTF-8 manifest path"),
            wal.to_str().expect("UTF-8 WAL path"),
        )
        .expect("open");
        let insert_value = |key: &str, seq, value: &str| {
            let offset = db
                .append_to_heap(value.as_bytes())
                .expect("append test value");
            db.insert_replica_record(
                key.as_bytes().to_vec(),
                seq,
                offset,
                UNCOMPRESSED_FLAG | value.len() as u64,
                crc32fast::hash(value.as_bytes()),
                0,
            );
        };
        for key in ["b", "c", "d", "e", "f"] {
            insert_value(key, 10, "old");
        }
        let unreadable = u64::MAX / 2;
        db.insert_replica_record(b"b".to_vec(), 20, unreadable, 1, 0, 0);
        insert_value("b", 30, "new");
        db.insert_replica_record(b"c".to_vec(), 30, unreadable, 0, 0, 0);
        db.insert_replica_record(b"d".to_vec(), 30, unreadable, 1, 0, 1);
        db.insert_replica_record(b"e".to_vec(), 40, unreadable, 1, 0, 0);
        for key in [b"a", b"z"] {
            db.insert_replica_record(key.to_vec(), 30, unreadable, 1, 0, 0);
        }

        let current: Vec<(String, String)> = vec![
            ("b".into(), "new".into()),
            ("e".into(), "old".into()),
            ("f".into(), "old".into()),
        ];
        assert_eq!(
            db.scan("b", "f", 30).expect("scan current visible values"),
            current
        );
        assert_eq!(
            db.scan_iter("b", "f", 30)
                .expect("create legacy scan")
                .collect::<Vec<_>>(),
            current
        );
        let historical: Vec<(String, String)> = ["b", "c", "d", "e", "f"]
            .into_iter()
            .map(|key| (key.to_string(), "old".into()))
            .collect();
        assert_eq!(
            db.scan("b", "f", 10).expect("scan historical values"),
            historical
        );
        assert_eq!(
            db.scan("g", "y", 30).expect("scan empty range"),
            Vec::<(String, String)>::new()
        );
        assert_eq!(db.scan_buffer_pool_available(), 1);
    }

    #[derive(Clone, Copy)]
    enum ScanTestTier {
        L0,
        L1,
        Base,
    }

    fn check_persisted_scan_bounds(tier: ScanTestTier, unreadable_prefix: bool) {
        let dir = TempDir::new().expect("tempdir");
        let manifest = dir.path().join("manifest");
        let wal = dir.path().join("wal");
        let db = OmniKV::open(
            manifest.to_str().expect("UTF-8 manifest path"),
            wal.to_str().expect("UTF-8 WAL path"),
        )
        .expect("open");
        let insert_value = |key: &str, seq, value: &str| {
            let offset = db
                .append_to_heap(value.as_bytes())
                .expect("append test value");
            db.insert_replica_record(
                key.as_bytes().to_vec(),
                seq,
                offset,
                UNCOMPRESSED_FLAG | value.len() as u64,
                crc32fast::hash(value.as_bytes()),
                0,
            );
        };
        let unreadable = u64::MAX / 2;
        if unreadable_prefix {
            db.insert_replica_record(b"a".to_vec(), 10, unreadable, 1, 0, 0);
        } else {
            insert_value("a", 10, "outside");
        }
        for key in ["b", "c", "d", "e", "f"] {
            insert_value(key, 10, "old");
        }
        let snapshot = db.snapshot();
        assert_eq!(snapshot, 10);
        db.insert_replica_record(b"b".to_vec(), 20, unreadable, 1, 0, 0);
        insert_value("b", 30, "new");
        db.insert_replica_record(b"c".to_vec(), 30, unreadable, 0, 0, 0);
        db.insert_replica_record(b"d".to_vec(), 30, unreadable, 1, 0, 1);
        db.insert_replica_record(b"e".to_vec(), 40, unreadable, 1, 0, 0);
        db.insert_replica_record(b"z".to_vec(), 30, unreadable, 1, 0, 0);

        // Use real compaction outputs, with the snapshot pin retaining history
        // and deletion markers through L1 and base compaction.
        db.compact_sstables().expect("flush test values into L0");
        if matches!(tier, ScanTestTier::L1 | ScanTestTier::Base) {
            db.compact_l0_to_l1().expect("compact test values into L1");
        }
        if matches!(tier, ScanTestTier::Base) {
            db.compact_l1_to_l2()
                .expect("compact test values into base");
        }
        assert_eq!(db.memtable_size(), 0);
        let roots = db.roots.load();
        assert!(roots.frozen_memtables.is_empty());
        assert_eq!(
            roots.sstables.len(),
            usize::from(matches!(tier, ScanTestTier::L0))
        );
        assert_eq!(
            roots.l1_sstables.len(),
            usize::from(matches!(tier, ScanTestTier::L1))
        );
        let mmap = match tier {
            ScanTestTier::L0 => roots.sstables[0].0.clone(),
            ScanTestTier::L1 => roots.l1_sstables[0].0.clone(),
            ScanTestTier::Base => roots.base_mmap.clone(),
        };
        let reader = SSTableReader::from_mmap(mmap);
        assert_eq!(
            reader
                .iter_from(b"b")
                .next()
                .expect("persisted prefix record")
                .key,
            b"a",
            "the sparse-index seek must start before the requested bound"
        );

        let current: Vec<(String, String)> = vec![
            ("b".into(), "new".into()),
            ("e".into(), "old".into()),
            ("f".into(), "old".into()),
        ];
        assert_eq!(
            db.scan("b", "f", 30)
                .expect("scan persisted visible values"),
            current
        );
        let legacy: Vec<(String, String)> = db
            .scan_iter("b", "f", 30)
            .expect("create persisted legacy scan")
            .collect();
        assert_eq!(legacy, current);
        let historical: Vec<(String, String)> = ["b", "c", "d", "e", "f"]
            .into_iter()
            .map(|key| (key.to_string(), "old".into()))
            .collect();
        assert_eq!(
            db.scan("b", "f", snapshot)
                .expect("scan persisted historical values"),
            historical
        );
        assert_eq!(
            db.scan("b", "b", 30)
                .expect("scan inclusive start endpoint"),
            [("b".into(), "new".into())]
        );
        assert_eq!(
            db.scan("f", "f", 30).expect("scan inclusive end endpoint"),
            [("f".into(), "old".into())]
        );
        for (start, end) in [("g", "y"), ("g", "g"), ("f", "b"), ("c", "c")] {
            assert!(
                db.scan(start, end, 30)
                    .expect("scan empty persisted range")
                    .is_empty(),
                "{start}..={end}"
            );
            assert!(
                db.scan_iter(start, end, 30)
                    .expect("create empty persisted scan")
                    .next()
                    .is_none()
            );
        }

        // Filtering an unrelated unreadable prefix must not mask errors when
        // that payload (or another unreadable version) is inside the range.
        assert!(matches!(db.scan("b", "f", 20), Err(OmniError::IoError(_))));
        if unreadable_prefix {
            assert!(matches!(db.scan("a", "f", 30), Err(OmniError::IoError(_))));
        }
        assert_eq!(db.scan_buffer_pool_available(), 1);
        db.unregister_snapshot(snapshot);
    }

    #[test]
    fn scan_persisted_l0_respects_start_bound() {
        check_persisted_scan_bounds(ScanTestTier::L0, false);
    }

    #[test]
    fn scan_persisted_l1_respects_start_bound() {
        check_persisted_scan_bounds(ScanTestTier::L1, false);
    }

    #[test]
    fn scan_persisted_base_respects_start_bound() {
        check_persisted_scan_bounds(ScanTestTier::Base, false);
    }

    #[test]
    fn scan_persisted_l0_skips_unreadable_prefix() {
        check_persisted_scan_bounds(ScanTestTier::L0, true);
    }

    #[test]
    fn scan_persisted_l1_skips_unreadable_prefix() {
        check_persisted_scan_bounds(ScanTestTier::L1, true);
    }

    #[test]
    fn scan_persisted_base_skips_unreadable_prefix() {
        check_persisted_scan_bounds(ScanTestTier::Base, true);
    }

    #[test]
    fn cleanup_obsolete_file_treats_not_found_as_benign() {
        let outcome = cleanup_obsolete_file_with(
            Path::new("already-gone.sst"),
            "unit_test_cleanup_not_found",
            |_| Err(io::Error::from(ErrorKind::NotFound)),
        );

        assert_eq!(outcome, CleanupDeleteOutcome::NotFound);
        assert!(
            !metrics_prometheus::render_metrics().contains("unit_test_cleanup_not_found"),
            "not-found cleanup should not be counted as a failure"
        );
    }

    #[test]
    fn cleanup_obsolete_file_records_delete_failures() {
        let outcome = cleanup_obsolete_file_with(
            Path::new("locked.sst"),
            "unit_test_cleanup_failure",
            |_| Err(io::Error::from(ErrorKind::PermissionDenied)),
        );

        assert_eq!(outcome, CleanupDeleteOutcome::Failed);
        let metrics = metrics_prometheus::render_metrics();
        assert!(metrics.contains("omnikv_cleanup_delete_failures_total"));
        assert!(metrics.contains("context=\"unit_test_cleanup_failure\""));
        assert!(metrics.contains("error_kind=\"PermissionDenied\""));
    }

    #[test]
    fn compaction_policy_rejects_stall_threshold_at_trigger() {
        let policy = CompactionPolicy {
            l0_compaction_trigger: 4,
            l1_compaction_trigger: 4,
            l0_write_stall_threshold: 4,
            write_stall_wait_attempts: 50,
            write_stall_wait_ms: 100,
        };

        let err = policy.validate().unwrap_err();
        assert!(
            matches!(err, OmniError::InvalidCompactionPolicy(_)),
            "expected InvalidCompactionPolicy, got {err:?}"
        );
    }

    #[test]
    fn version_retention_keeps_predecessor_for_oldest_snapshot() {
        let mut versions = VersionedValuePointers::new();
        versions.insert(1, (10, UNCOMPRESSED_FLAG | 3, 0, 0));
        versions.insert(5, (20, UNCOMPRESSED_FLAG | 3, 0, 0));
        versions.insert(8, (30, UNCOMPRESSED_FLAG | 3, 0, 0));
        versions.insert(10, (40, UNCOMPRESSED_FLAG | 3, 0, 0));

        retain_versions_needed_by_floor(&mut versions, Some(7));

        let retained: Vec<u64> = versions.keys().copied().collect();
        assert_eq!(
            retained,
            vec![5, 8, 10],
            "oldest snapshot 7 needs predecessor 5, plus versions >= 7 and latest"
        );
    }

    #[test]
    fn empty_memtable_compaction_removes_frozen_memtable() {
        let dir = TempDir::new().expect("tempdir");
        let manifest = dir.path().join("manifest").to_string_lossy().to_string();
        let wal = dir.path().join("wal").to_string_lossy().to_string();
        let db = OmniKV::open(&manifest, &wal).expect("open");

        db.compact_sstables()
            .expect("empty compaction should succeed");

        assert_eq!(
            db.roots.load().frozen_memtables.len(),
            0,
            "empty compaction must not leave an unreachable frozen memtable"
        );
        assert!(
            metrics_prometheus::render_metrics().contains("omnikv_compaction_latency_seconds"),
            "empty compaction should still emit compaction metrics"
        );
    }

    const SQL_TABLE_LOCK_TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

    fn run_sql_table_lock_test(test: impl FnOnce() + Send + 'static) {
        use std::sync::mpsc::{self, RecvTimeoutError};

        // A regression in Drop can deadlock even a single-threaded caller.
        // Keep the entire scenario off the test thread, including teardown.
        let (done_tx, done_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            test();
            let _ = done_tx.send(());
        });
        assert_ne!(
            done_rx.recv_timeout(SQL_TABLE_LOCK_TEST_TIMEOUT),
            Err(RecvTimeoutError::Timeout),
            "SQL table-lock scenario timed out (possible deadlock)"
        );
        if let Err(panic) = worker.join() {
            std::panic::resume_unwind(panic);
        }
    }

    #[test]
    fn sql_table_lock_registry_reclaims_released_entries() {
        run_sql_table_lock_test(|| {
            let dir = TempDir::new().expect("tempdir");
            let manifest = dir.path().join("manifest").to_string_lossy().to_string();
            let wal = dir.path().join("wal").to_string_lossy().to_string();
            let db = OmniKV::open(&manifest, &wal).expect("open");

            let lock = db
                .sql_table_lock("ephemeral_table")
                .expect("create SQL table lock");
            assert!(
                db.sql_table_locks
                    .locks
                    .lock()
                    .expect("table-lock registry")
                    .contains_key("ephemeral_table")
            );

            drop(lock);

            assert!(
                !db.sql_table_locks
                    .locks
                    .lock()
                    .expect("table-lock registry")
                    .contains_key("ephemeral_table"),
                "released table locks must not leave registry entries behind"
            );
        });
    }

    #[test]
    fn sql_table_lock_registry_shares_case_insensitive_holders_and_waiters() {
        run_sql_table_lock_test(|| {
            use std::sync::{TryLockError, mpsc};

            let registry = Arc::new(SqlTableLockRegistry::default());
            let owner = registry.get("Orders").expect("owner lock");
            let owner_guard = owner.lock().expect("owner guard");
            let (waiting_tx, waiting_rx) = mpsc::channel();
            let (acquired_tx, acquired_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let waiter_registry = registry.clone();
            let waiter = std::thread::spawn(move || {
                let lock = waiter_registry.get("ORDERS").expect("waiter lock");
                assert!(matches!(
                    lock.mutex.try_lock(),
                    Err(TryLockError::WouldBlock)
                ));
                waiting_tx
                    .send(Arc::downgrade(&lock))
                    .expect("waiter ready");
                let _guard = lock.lock().expect("waiter guard");
                acquired_tx.send(()).expect("waiter acquired");
                release_rx
                    .recv_timeout(SQL_TABLE_LOCK_TEST_TIMEOUT)
                    .expect("release waiter");
            });

            let waiting_lock = waiting_rx
                .recv_timeout(SQL_TABLE_LOCK_TEST_TIMEOUT)
                .expect("waiter must reach the held lock");
            let peer = registry.get("oRdErS").expect("peer lock");
            assert!(Arc::ptr_eq(&owner, &peer));
            assert!(Weak::ptr_eq(&waiting_lock, &Arc::downgrade(&peer)));
            drop(peer);
            drop(owner_guard);
            drop(owner);

            // The original holder is gone, but the waiter's Arc must keep
            // the entry alive before and after it acquires the mutex.
            let next = registry.get("orders").expect("next lock");
            assert!(Weak::ptr_eq(&waiting_lock, &Arc::downgrade(&next)));
            acquired_rx
                .recv_timeout(SQL_TABLE_LOCK_TEST_TIMEOUT)
                .expect("waiter must acquire after owner releases");
            assert!(matches!(
                next.mutex.try_lock(),
                Err(TryLockError::WouldBlock)
            ));
            drop(next);
            release_tx.send(()).expect("release waiter");
            waiter.join().expect("waiter completed");
            assert!(registry.locks.lock().expect("registry").is_empty());
        });
    }

    #[test]
    fn sql_table_lock_registry_reclaims_high_churn_names() {
        run_sql_table_lock_test(|| {
            let registry = Arc::new(SqlTableLockRegistry::default());
            let survivor = registry.get("survivor").expect("surviving lock");
            for round in 0..64 {
                let locks: Vec<_> = (0..64)
                    .map(|index| {
                        registry
                            .get(&format!("ephemeral_{round}_{index}"))
                            .expect("churn lock")
                    })
                    .collect();
                assert_eq!(registry.locks.lock().expect("registry").len(), 65);
                drop(locks);
                assert_eq!(
                    registry.locks.lock().expect("registry").len(),
                    1,
                    "only the live survivor should remain after round {round}"
                );
            }
            drop(survivor);
            assert!(registry.locks.lock().expect("registry").is_empty());
        });
    }

    #[test]
    fn sql_table_lock_registry_last_release_preserves_replacement_serialization() {
        run_sql_table_lock_test(|| {
            use std::sync::TryLockError;

            let registry = Arc::new(SqlTableLockRegistry::default());
            let old = registry.get("orders").expect("old lock");
            let old_weak = Arc::downgrade(&old);
            let mut entries = registry.locks.lock().expect("registry");
            let teardown = std::thread::spawn(move || drop(old));

            // Force the last Arc to disappear while its destructor cannot
            // take the registry mutex. Poll the count, never upgrade here:
            // dropping an upgraded last Arc under `entries` would deadlock.
            let deadline = std::time::Instant::now() + SQL_TABLE_LOCK_TEST_TIMEOUT;
            while old_weak.strong_count() != 0 {
                assert!(
                    std::time::Instant::now() < deadline,
                    "last release timed out"
                );
                std::thread::yield_now();
            }
            assert!(Weak::ptr_eq(
                entries.get("orders").expect("old entry still present"),
                &old_weak
            ));

            // Model a lookup replacing that now-dead weak entry before the
            // old destructor obtains the mutex. No live lock is replaced.
            let replacement = Arc::new(SqlTableLock {
                mutex: Mutex::new(()),
                name: "orders".into(),
                registry: Arc::downgrade(&registry),
            });
            entries.insert("orders".into(), Arc::downgrade(&replacement));
            drop(entries);
            teardown.join().expect("old destructor completed");

            assert!(Weak::ptr_eq(
                registry
                    .locks
                    .lock()
                    .expect("registry")
                    .get("orders")
                    .expect("old destructor must preserve the replacement"),
                &Arc::downgrade(&replacement)
            ));
            let guard = replacement.lock().expect("replacement guard");
            let next = registry.get("ORDERS").expect("next lock");
            assert!(Arc::ptr_eq(&replacement, &next));
            assert!(matches!(
                next.mutex.try_lock(),
                Err(TryLockError::WouldBlock)
            ));
            drop(guard);
            drop(next);
            drop(replacement);
            assert!(registry.locks.lock().expect("registry").is_empty());
        });
    }

    #[test]
    fn not_leader_error_names_the_leader_for_clients() {
        // This is the one error whose details ARE client-facing: a clustered
        // write landed on a follower and the caller needs the leader's id to
        // retry. The message shape is load-bearing — protocol layers map it
        // to a redirect, and the failover test matches on it.
        let with_leader = OmniError::NotLeader { leader_id: Some(7) };
        assert_eq!(
            with_leader.to_string(),
            "not the leader; the leader is node 7"
        );
        let no_leader = OmniError::NotLeader { leader_id: None };
        assert_eq!(
            no_leader.to_string(),
            "not the leader; no leader elected yet"
        );
    }

    #[test]
    fn write_stall_wait_ignores_no_progress_wakeup_and_proceeds_when_l0_clears() {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = OmniKV::open(
            dir.path()
                .join("manifest.json")
                .to_str()
                .expect("manifest path"),
            dir.path().join("wal.bin").to_str().expect("wal path"),
        )
        .expect("open db");

        // Raise threshold while seeding 2 L0 SSTables.
        db.set_compaction_policy(CompactionPolicy {
            l0_compaction_trigger: 1,
            l1_compaction_trigger: 1,
            l0_write_stall_threshold: 4,
            write_stall_wait_attempts: 1,
            write_stall_wait_ms: 500,
        })
        .expect("set seed policy");
        for i in 0..2 {
            let mut b = WriteBatch::new();
            b.set(&format!("seed_{i}"), "v".into())
                .expect("stage seed key");
            db.commit_batch(&b).expect("commit seed key");
            db.compact_sstables().expect("flush L0");
        }
        assert_eq!(db.sstable_count(), 2);

        // Stall at 2 L0 SSTables with 1 attempt of 500ms.
        db.set_compaction_policy(CompactionPolicy {
            l0_compaction_trigger: 1,
            l1_compaction_trigger: 1,
            l0_write_stall_threshold: 2,
            write_stall_wait_attempts: 1,
            write_stall_wait_ms: 500,
        })
        .expect("set stall policy");

        let started = std::time::Instant::now();
        let db_bg = db.clone();
        let writer_ready = Arc::new(std::sync::Barrier::new(2));
        let bg_ready = writer_ready.clone();
        let bg = std::thread::spawn(move || {
            bg_ready.wait();
            // Hold `stall_lock` momentarily only after the writer has raced into
            // `commit_batch`, then fire multiple no-progress wakeups before clearing L0.
            for _ in 0..3 {
                std::thread::sleep(std::time::Duration::from_millis(20));
                db_bg.notify_compaction_waiters();
            }
            // Clear L0 shortly afterwards, well within the 500ms budget.
            std::thread::sleep(std::time::Duration::from_millis(40));
            db_bg.compact_l0_to_l1().expect("compact L0 to L1");
        });

        let mut writer_batch = WriteBatch::new();
        writer_batch
            .set("after_stall", "ok".into())
            .expect("stage writer key");
        writer_ready.wait();
        db.commit_batch(&writer_batch)
            .expect("writer must wait through no-progress wakeup until compaction clears L0");
        let elapsed = started.elapsed();
        assert!(
            elapsed >= std::time::Duration::from_millis(80),
            "writer should wait until compaction clears L0 (elapsed={elapsed:?})"
        );
        bg.join().expect("background compactor joined");
        assert_eq!(
            db.find("after_stall", db.get_seq()).expect("read key"),
            Some("ok".into())
        );
    }

    #[test]
    fn open_restores_durable_2pc_prepared_key_reservations_before_participant_init() {
        let dir = tempfile::tempdir().expect("temp dir");
        let manifest = dir
            .path()
            .join("manifest.json")
            .to_str()
            .expect("manifest path")
            .to_string();
        let wal = dir
            .path()
            .join("wal.bin")
            .to_str()
            .expect("wal path")
            .to_string();

        {
            let db = OmniKV::open(&manifest, &wal).expect("open db");
            let participant = crate::dist_txn::TwoPhaseParticipant::new(2, db.clone());
            let prep = participant.prepare(
                (1, 501),
                &[crate::dist_txn::DistWrite {
                    node_id: 2,
                    key: "reserved_on_reopen".into(),
                    value: Some("prepared_v".into()),
                    ttl: 0,
                }],
            );
            assert_eq!(prep.vote, crate::dist_txn::Vote::Commit);
        }

        // Reopen without creating TwoPhaseParticipant yet: plain writes must
        // still be blocked from clobbering the durable PREPARED key.
        let reopened = OmniKV::open(&manifest, &wal).expect("reopen db");
        let mut plain = WriteBatch::new();
        plain
            .set("reserved_on_reopen", "clobber".into())
            .expect("stage plain write");
        assert!(
            reopened.commit_batch(&plain).is_err(),
            "plain write must not overwrite a durable 2PC PREPARED key after restart"
        );
    }

    #[test]
    fn scan_iterator_retains_captured_heap_generation_across_heap_gc_swap() {
        let dir = tempfile::tempdir().expect("temp dir");
        let manifest = dir
            .path()
            .join("manifest.json")
            .to_str()
            .expect("manifest path")
            .to_string();
        let wal = dir
            .path()
            .join("wal.bin")
            .to_str()
            .expect("wal path")
            .to_string();
        let db = OmniKV::open(&manifest, &wal).expect("open db");

        let mut b1 = WriteBatch::new();
        b1.set("gen/a", "old_value_a".into()).expect("stage a");
        b1.set("gen/b", "old_value_b".into()).expect("stage b");
        db.commit_batch(&b1).expect("commit gen 1");
        let snap_seq = db.get_seq();

        // Capture a lazy ScanIterator against generation 1 before GC replaces the heap.
        let mut iter = db
            .scan_iter_bounds(
                string_to_key("gen/"),
                ScanEnd::Exclusive(&string_to_key("gen0")),
                snap_seq,
            )
            .expect("start scan on gen 1");

        // Overwrite with new values and run heap GC to swap to generation 2,
        // then read gen 2 so block_cache holds offset 0 of generation 2.
        let mut b2 = WriteBatch::new();
        b2.set("gen/a", "new_value_a".into()).expect("overwrite a");
        b2.set("gen/b", "new_value_b".into()).expect("overwrite b");
        db.commit_batch(&b2).expect("commit gen 2 writes");
        db.flush_memtable_to_disk(1).expect("flush before gc");
        db.run_garbage_collection().expect("run heap gc");
        assert_eq!(
            db.find("gen/a", db.get_seq()).expect("read gen 2 a"),
            Some("new_value_a".into())
        );

        // Consuming the gen-1 ScanIterator must still return gen-1 values,
        // never cross-reading or hitting block_cache entries from gen 2.
        let mut collected = Vec::new();
        while let Some(pair) = iter.try_next().expect("read gen 1 entry") {
            collected.push(pair);
        }
        assert_eq!(
            collected,
            vec![
                ("gen/a".to_string(), "old_value_a".to_string()),
                ("gen/b".to_string(), "old_value_b".to_string()),
            ]
        );
    }

    #[test]
    fn heap_gc_without_prior_flush_does_not_publish_stale_memtable_offsets() {
        let dir = tempfile::tempdir().expect("temp dir");
        let manifest = dir
            .path()
            .join("manifest.json")
            .to_str()
            .expect("manifest path")
            .to_string();
        let wal = dir
            .path()
            .join("wal.bin")
            .to_str()
            .expect("wal path")
            .to_string();
        let db = OmniKV::open(&manifest, &wal).expect("open db");

        // Commit `z` first (heap offset 0) and then `a` without flushing.
        // GC rewrites the heap in sorted key order (`a` at offset 0, `z` later).
        let mut b1 = WriteBatch::new();
        b1.set("z", "Z".into()).expect("stage z");
        db.commit_batch(&b1).expect("commit z");

        let mut b2 = WriteBatch::new();
        b2.set("a", "A".into()).expect("stage a");
        db.commit_batch(&b2).expect("commit a");

        // Capture pre-GC WAL bytes to simulate a crash right after manifest.save()
        // and before wal.clear() completes.
        let pre_gc_wal_bytes = std::fs::read(&wal).expect("read pre-gc wal bytes");
        assert!(
            !pre_gc_wal_bytes.is_empty(),
            "pre-gc WAL must contain unflushed records"
        );

        // Capture a pre-GC lazy iterator before GC runs (still without flushing).
        let snap_seq = db.get_seq();
        let mut pre_gc_iter = db
            .scan_iter_bounds(string_to_key("a"), ScanEnd::Inclusive(b"z"), snap_seq)
            .expect("start pre-gc scan");

        db.run_garbage_collection()
            .expect("heap gc without prior flush must succeed");

        // Uncached post-GC reads must read the replacement heap via the rebuilt
        // base SSTable instead of stale memtable pointers into the old heap.
        assert_eq!(
            db.find("z", db.get_seq()).expect("read z after gc"),
            Some("Z".into())
        );
        assert_eq!(
            db.find("a", db.get_seq()).expect("read a after gc"),
            Some("A".into())
        );

        let mut pre_gc_rows = Vec::new();
        while let Some(pair) = pre_gc_iter.try_next().expect("read pre-gc iter") {
            pre_gc_rows.push(pair);
        }
        assert_eq!(
            pre_gc_rows,
            vec![
                ("a".to_string(), "A".to_string()),
                ("z".to_string(), "Z".to_string()),
            ]
        );

        // Drop DB, restore pre-GC WAL bytes (old heap offsets!), reopen, and
        // verify wal_checkpoint_seq prevents stale WAL replay from corrupting reads.
        drop(pre_gc_iter);
        drop(db);
        std::fs::write(&wal, &pre_gc_wal_bytes).expect("restore pre-gc wal bytes");
        let reopened =
            OmniKV::open(&manifest, &wal).expect("reopen after gc crash before wal clear");
        assert_eq!(
            reopened.find("z", reopened.get_seq()).expect("reopen z"),
            Some("Z".into())
        );
        assert_eq!(
            reopened.find("a", reopened.get_seq()).expect("reopen a"),
            Some("A".into())
        );
    }

    #[test]
    fn open_fails_closed_when_unfinished_snapshot_rollback_dir_exists() {
        let dir = tempfile::tempdir().expect("temp dir");
        let manifest_path = dir.path().join("manifest.json");
        let wal_path = dir.path().join("wal.bin");
        let manifest_str = manifest_path.to_str().expect("manifest path").to_string();
        let wal_str = wal_path.to_str().expect("wal path").to_string();

        // Case 1: non-empty unresolved rollback dir without manifest fails closed.
        let unresolved = dir.path().join("old_snapshot_interrupted");
        std::fs::create_dir_all(&unresolved).expect("create unresolved dir");
        let backup_manifest = unresolved.join("manifest.json");
        std::fs::write(&backup_manifest, b"original-manifest").expect("write backup manifest");

        let err = OmniKV::open(&manifest_str, &wal_str)
            .err()
            .expect("open must fail closed on unfinished snapshot promotion");
        assert!(
            err.to_string().contains("unfinished snapshot promotion"),
            "unexpected error: {err}"
        );
        assert!(
            !manifest_path.exists(),
            "open must not create an empty manifest when unfinished rollback dir exists"
        );
        assert_eq!(
            std::fs::read(&backup_manifest).expect("backup must remain intact"),
            b"original-manifest"
        );

        // Case 2: even an empty rollback dir without a live manifest fails closed.
        std::fs::remove_file(&backup_manifest).expect("remove sentinel");
        assert!(
            OmniKV::open(&manifest_str, &wal_str).is_err(),
            "empty rollback dir without live manifest must fail closed"
        );
        assert!(!manifest_path.exists());

        // Removing the unresolved dir lets a genuinely fresh database open cleanly.
        std::fs::remove_dir_all(&unresolved).expect("clear unresolved dir");
        let fresh = OmniKV::open(&manifest_str, &wal_str).expect("fresh open should succeed");
        drop(fresh);

        // Case 3: `.install_complete` paired with `.rollback_failed` must fail closed even with manifest present.
        let failed_dir = dir.path().join("old_snapshot_failed");
        std::fs::create_dir_all(&failed_dir).expect("create failed dir");
        std::fs::write(failed_dir.join(".install_complete"), b"ok").expect("write complete");
        std::fs::write(failed_dir.join(".rollback_failed"), b"failed").expect("write failed");
        assert!(
            OmniKV::open(&manifest_str, &wal_str).is_err(),
            "rollback_failed marker must fail closed even when manifest exists"
        );
    }
}
