#![expect(
    dead_code,
    reason = "Storage core intentionally carries staged LSM, compaction, transaction, and compatibility paths while the module is being split into smaller production components."
)]

use crate::{metrics_prometheus, wal};
use arc_swap::ArcSwap;
use crossbeam_skiplist::SkipMap;
use memmap2::{Mmap, MmapOptions};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
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
    cleanup_obsolete_file(bloom_path_for_base(path), context);
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

const MAX_BLOOM_WORDS: usize = 8 * 1024 * 1024; // 64 MiB bitset cap (~536M bits)
const MAX_BLOOM_HASHES: usize = 64;

impl BloomFilter {
    pub fn save(&self, path: &str) -> Result<(), OmniError> {
        let content = serde_json::to_string(self)
            .map_err(|e| OmniError::IoError(format!("Bloom filter serialize: {}", e)))?;
        atomic_replace(path, content.as_bytes())
    }

    pub fn load(path: &str) -> Result<Self, OmniError> {
        let content = std::fs::read_to_string(path)?;
        let bloom: Self =
            serde_json::from_str(&content).map_err(|e| OmniError::IoError(e.to_string()))?;
        if bloom.bitset.is_empty()
            || bloom.bitset.len() > MAX_BLOOM_WORDS
            || bloom.num_bits == 0
            || bloom.bitset.len().checked_mul(64) != Some(bloom.num_bits)
            || !(1..=MAX_BLOOM_HASHES).contains(&bloom.num_hashes)
        {
            return Err(OmniError::IoError(format!(
                "corrupt bloom filter metadata in '{path}' (words={}, num_bits={}, num_hashes={})",
                bloom.bitset.len(),
                bloom.num_bits,
                bloom.num_hashes
            )));
        }
        Ok(bloom)
    }

    pub(crate) fn has_any_bit_set(&self) -> bool {
        self.bitset.iter().any(|&word| word != 0)
    }

    pub fn new(expected_elements: usize) -> Self {
        let elements = expected_elements.max(1);
        let p = 0.01_f64;
        let num_bits = (-(elements as f64) * p.ln() / (2.0_f64.ln().powi(2))).ceil() as usize;
        let num_hashes = ((2.0_f64.ln() * (num_bits as f64) / (elements as f64)).ceil() as usize)
            .clamp(1, MAX_BLOOM_HASHES);
        let bitset = vec![0; num_bits.max(1).div_ceil(64)];
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

        let count = self.num_hashes.min(MAX_BLOOM_HASHES);
        let mut res = Vec::with_capacity(count);
        for i in 0..count {
            let combined = hash1.wrapping_add((i as u64).wrapping_mul(hash2));
            res.push((combined % self.num_bits as u64) as usize);
        }
        res
    }

    pub fn add(&mut self, key: u64) {
        if self.num_bits == 0
            || self.num_hashes == 0
            || self.bitset.len().saturating_mul(64) < self.num_bits
        {
            return;
        }
        for idx in self.hashes(key) {
            self.bitset[idx / 64] |= 1u64 << (idx % 64);
        }
    }

    pub fn might_contain(&self, key: u64) -> bool {
        // Fail safe on malformed in-memory filters: return `true` so callers
        // check the underlying SSTable rather than risking a false negative.
        if self.num_bits == 0
            || self.num_hashes == 0
            || self.bitset.len().saturating_mul(64) < self.num_bits
        {
            return true;
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
pub const MAX_KEY_SIZE: usize = u16::MAX as usize;
pub(crate) const MAX_VALUE_SIZE: usize = 10 * 1024 * 1024; // 10 MB
pub(crate) const MAX_ENCODED_VALUE_SIZE: u64 = (MAX_VALUE_SIZE as u64).saturating_mul(2);
pub(crate) const UNCOMPRESSED_FLAG: u64 = 1 << 63;
const NUM_SHARDS: usize = 16;

pub(crate) fn validate_key_len(key: &[u8]) -> Result<(), OmniError> {
    if key.len() > MAX_KEY_SIZE {
        return Err(OmniError::IoError(format!(
            "key length {} exceeds maximum representable length {MAX_KEY_SIZE}",
            key.len()
        )));
    }
    Ok(())
}

type ValuePointer = (u64, u64, u32, u64);
type BlockCacheKey = (u64, u64, u64, u32);
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
        validate_key_len(key.as_bytes())?;
        if value.len() > MAX_VALUE_SIZE {
            return Err(OmniError::ValueTooLarge(MAX_VALUE_SIZE));
        }
        let expiry = if ttl_secs > 0 {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                .saturating_add(ttl_secs)
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
        validate_key_len(key.as_bytes())?;
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
    /// enforces [`MAX_KEY_SIZE`] and [`MAX_VALUE_SIZE`].
    pub(crate) fn set_internal_meta(&mut self, key: &str, value: String) -> Result<(), OmniError> {
        validate_key_len(key.as_bytes())?;
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
        validate_key_len(key.as_bytes())?;
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

type MemShardRange<'a> = crossbeam_skiplist::map::Range<
    'a,
    (Vec<u8>, Reverse<u64>),
    std::ops::RangeFrom<(Vec<u8>, Reverse<u64>)>,
    (Vec<u8>, Reverse<u64>),
    ValuePointer,
>;

enum MergeSource<'a> {
    MemShard {
        iter: MemShardRange<'a>,
        last_key: Option<Vec<u8>>,
        exhausted: bool,
    },
    Sst {
        iter: SSTableIterator,
        peeked: Option<OmniRecord>,
        exhausted: bool,
    },
}

impl MergeSource<'_> {
    fn next_visible(
        &mut self,
        start_key: &[u8],
        end_bound: ScanEnd<'_>,
        read_seq: u64,
    ) -> Result<Option<ScanCandidate>, OmniError> {
        match self {
            Self::MemShard {
                iter,
                last_key,
                exhausted,
            } => {
                if *exhausted {
                    return Ok(None);
                }
                for entry in iter.by_ref() {
                    let (key, rev_seq) = entry.key();
                    if end_bound.is_past(key) {
                        *exhausted = true;
                        return Ok(None);
                    }
                    let seq = rev_seq.0;
                    if seq > read_seq {
                        continue;
                    }
                    if last_key.as_deref() == Some(key.as_slice()) {
                        continue;
                    }
                    let owned_key = key.clone();
                    *last_key = Some(owned_key.clone());
                    let val = entry.value();
                    return Ok(Some(ScanCandidate {
                        key: owned_key,
                        seq,
                        offset: val.0,
                        length: val.1,
                        crc: val.2,
                        expiry: val.3,
                    }));
                }
                *exhausted = true;
                Ok(None)
            }
            Self::Sst {
                iter,
                peeked,
                exhausted,
            } => {
                if *exhausted {
                    return Ok(None);
                }
                let mut best: Option<OmniRecord> = None;
                loop {
                    let rec = match peeked.take() {
                        Some(rec) => rec,
                        None => match iter.try_next()? {
                            Some(rec) => rec,
                            None => {
                                *exhausted = true;
                                break;
                            }
                        },
                    };
                    if end_bound.is_past(&rec.key) {
                        *exhausted = true;
                        break;
                    }
                    if rec.key.as_slice() < start_key {
                        continue;
                    }
                    if let Some(ref current) = best {
                        if rec.key == current.key {
                            if rec.is_valid() && rec.seq <= read_seq && rec.seq > current.seq {
                                best = Some(rec);
                            }
                            continue;
                        }
                        *peeked = Some(rec);
                        break;
                    }
                    if rec.is_valid() && rec.seq <= read_seq {
                        best = Some(rec);
                    }
                }
                Ok(best.map(|rec| ScanCandidate {
                    key: rec.key,
                    seq: rec.seq,
                    offset: rec.offset,
                    length: rec.length,
                    crc: rec.payload_crc32,
                    expiry: rec.expiry,
                }))
            }
        }
    }
}

struct MergeHeapEntry {
    candidate: ScanCandidate,
    source_id: usize,
}

impl PartialEq for MergeHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.candidate.key == other.candidate.key
            && self.candidate.seq == other.candidate.seq
            && self.source_id == other.source_id
    }
}

impl Eq for MergeHeapEntry {}

impl Ord for MergeHeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .candidate
            .key
            .cmp(&self.candidate.key)
            .then_with(|| self.candidate.seq.cmp(&other.candidate.seq))
            .then_with(|| other.source_id.cmp(&self.source_id))
    }
}

impl PartialOrd for MergeHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
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

struct ValidatedSstableLayout {
    data_end: usize,
    /// `(key_range_in_data, block_start_offset)` for each sparse-index entry.
    index_entries: Vec<(std::ops::Range<usize>, usize)>,
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

    /// Validates the complete `OMNIV2**` footer and sparse-index layout.
    ///
    /// Every valid `OMNIV2**` table writes:
    /// `records[0..index_offset]`, `num_entries: u64`, `num_entries` packed
    /// `(key_len: u16, key, offset: u64)` entries, `index_offset: u64`, and
    /// `b"OMNIV2**"`.
    /// - When `index_offset == 0`, requires `num_entries == 0 && data.len() == 24`
    ///   (matching `SSTableWriter::finish` on an empty table) so a zeroed footer
    ///   offset on a non-empty table fails closed.
    /// - When `index_offset > 0`, requires `num_entries >= 1`, exact byte
    ///   consumption of `index_offset..data.len() - 16`, `entries[0].1 == 0`,
    ///   strictly increasing offsets `< index_offset`, and non-decreasing keys.
    /// - Only accepts a 4096-byte all-zero file when `allow_legacy_zero_base`
    ///   is `true` (backed by manifest evidence that the table is an untouched
    ///   pre-v2 placeholder).
    fn parse_strict_sstable_layout(
        data: &[u8],
        allow_legacy_zero_base: bool,
    ) -> Result<ValidatedSstableLayout, OmniError> {
        if data.len() >= 16 && &data[data.len() - 8..] == b"OMNIV2**" {
            if data.len() < 24 {
                return Err(OmniError::IoError(
                    "corrupt SSTable trailer: OMNIV2 file shorter than 24-byte minimum".into(),
                ));
            }
            let index_offset = usize::try_from(u64::from_le_bytes(
                data[data.len() - 16..data.len() - 8]
                    .try_into()
                    .expect("SSTable trailer offset length was prevalidated"),
            ))
            .map_err(|_| {
                OmniError::IoError("corrupt SSTable trailer: OMNIV2 index offset overflow".into())
            })?;
            if index_offset > data.len() - 24 {
                return Err(OmniError::IoError(
                    "corrupt SSTable trailer: invalid OMNIV2 index offset".into(),
                ));
            }

            let index_data = &data[index_offset..data.len() - 16];
            let num_entries = usize::try_from(u64::from_le_bytes(
                index_data[0..8]
                    .try_into()
                    .expect("SSTable index header length was prevalidated"),
            ))
            .map_err(|_| {
                OmniError::IoError("corrupt SSTable sparse index: entry count overflow".into())
            })?;

            if index_offset == 0 {
                if num_entries != 0 || data.len() != 24 {
                    return Err(OmniError::IoError(
                        "corrupt SSTable trailer: zero index offset on non-empty table".into(),
                    ));
                }
                return Ok(ValidatedSstableLayout {
                    data_end: 0,
                    index_entries: Vec::new(),
                });
            }
            if num_entries == 0 {
                return Err(OmniError::IoError(
                    "corrupt SSTable sparse index: non-empty table has zero index entries".into(),
                ));
            }

            // Each sparse index entry is at least 10 bytes (2-byte key_len + 8-byte offset).
            let max_possible_entries = (index_data.len() - 8) / 10;
            if num_entries > max_possible_entries {
                return Err(OmniError::IoError(
                    "corrupt SSTable sparse index: declared entry count exceeds index bounds"
                        .into(),
                ));
            }

            let mut index_entries = Vec::with_capacity(num_entries);
            let mut curr = 8usize;
            let mut prev_offset: Option<usize> = None;
            let mut prev_key: Option<&[u8]> = None;

            for i in 0..num_entries {
                if curr + 2 > index_data.len() {
                    return Err(OmniError::IoError(
                        "corrupt SSTable sparse index: truncated key length".into(),
                    ));
                }
                let key_len = u16::from_le_bytes(
                    index_data[curr..curr + 2]
                        .try_into()
                        .expect("SSTable index key length was prevalidated"),
                ) as usize;
                curr += 2;
                if curr + key_len + 8 > index_data.len() {
                    return Err(OmniError::IoError(
                        "corrupt SSTable sparse index: truncated entry".into(),
                    ));
                }
                let key_start = index_offset + curr;
                let key_end = key_start + key_len;
                let key = &data[key_start..key_end];
                curr += key_len;
                let entry_offset = usize::try_from(u64::from_le_bytes(
                    index_data[curr..curr + 8]
                        .try_into()
                        .expect("SSTable index block offset was prevalidated"),
                ))
                .map_err(|_| {
                    OmniError::IoError("corrupt SSTable sparse index: block offset overflow".into())
                })?;
                curr += 8;

                if entry_offset >= index_offset {
                    return Err(OmniError::IoError(
                        "corrupt SSTable sparse index: block offset outside data section".into(),
                    ));
                }
                if i == 0 {
                    if entry_offset != 0 {
                        return Err(OmniError::IoError(
                            "corrupt SSTable sparse index: first block offset must be 0".into(),
                        ));
                    }
                } else {
                    if prev_offset.is_some_and(|prev| entry_offset <= prev) {
                        return Err(OmniError::IoError(
                            "corrupt SSTable sparse index: block offsets not strictly increasing"
                                .into(),
                        ));
                    }
                    if prev_key.is_some_and(|prev| key < prev) {
                        return Err(OmniError::IoError(
                            "corrupt SSTable sparse index: index keys out of order".into(),
                        ));
                    }
                }
                prev_offset = Some(entry_offset);
                prev_key = Some(key);
                index_entries.push((key_start..key_end, entry_offset));
            }

            if curr != index_data.len() {
                return Err(OmniError::IoError(
                    "corrupt SSTable sparse index: trailing unparsed bytes".into(),
                ));
            }

            return Ok(ValidatedSstableLayout {
                data_end: index_offset,
                index_entries,
            });
        }

        if allow_legacy_zero_base && data.len() == 4096 && data.iter().all(|&b| b == 0) {
            return Ok(ValidatedSstableLayout {
                data_end: 0,
                index_entries: Vec::new(),
            });
        }

        Err(OmniError::IoError(
            "corrupt SSTable: missing OMNIV2** footer magic".into(),
        ))
    }

    /// Validates the `OMNIV2**` footer, sparse-index structure, and every
    /// sparse-index block-start record alignment once at table load or
    /// publication time so malformed tables fail closed before entering
    /// `StorageRoots`.
    pub(crate) fn validate_table_layout(
        data: &[u8],
        allow_legacy_zero_base: bool,
    ) -> Result<(), OmniError> {
        let layout = Self::parse_strict_sstable_layout(data, allow_legacy_zero_base)?;
        for (idx, (key_range, entry_offset)) in layout.index_entries.iter().enumerate() {
            let block_end = layout
                .index_entries
                .get(idx + 1)
                .map_or(layout.data_end, |(_, next_off)| *next_off);
            let Some((rec, _)) = OmniRecord::decode(&data[*entry_offset..block_end]) else {
                return Err(OmniError::IoError(format!(
                    "corrupt SSTable sparse index: block offset {entry_offset} does not point to a valid record"
                )));
            };
            if rec.key.as_slice() != &data[key_range.clone()] {
                return Err(OmniError::IoError(format!(
                    "corrupt SSTable sparse index: key mismatch at block offset {entry_offset}"
                )));
            }
        }
        Ok(())
    }

    /// Resolves `(data_end, start_offset)` for strict internal read paths
    /// (`find_with_seq`, `try_iter_from`).
    ///
    /// Fails closed on any malformed `OMNIV2**` footer/index or missing magic
    /// (unless `allow_legacy_zero_base` is `true` for a manifest-backed
    /// 4096-byte zero placeholder). Uses strict `<` so multiple MVCC versions
    /// of `target` spanning a sparse-index block boundary remain reachable from
    /// the preceding block.
    fn resolve_strict_read_offsets(
        data: &[u8],
        target: &[u8],
        allow_legacy_zero_base: bool,
    ) -> Result<(usize, usize), OmniError> {
        let layout = Self::parse_strict_sstable_layout(data, allow_legacy_zero_base)?;
        if layout.data_end == 0 || layout.index_entries.is_empty() {
            return Ok((layout.data_end, 0));
        }

        let idx = layout
            .index_entries
            .partition_point(|(key_range, _)| &data[key_range.clone()] < target);
        let start = if idx == 0 {
            0
        } else {
            layout.index_entries[idx - 1].1
        };
        Ok((layout.data_end, start))
    }

    /// Resolves `(data_end, start_offset, fallback_offsets)` for the public
    /// infallible best-effort inspection APIs (`SSTableReader::find`,
    /// `SSTableReader::iter_from`).
    ///
    /// For `OMNIV2**` tables, parses and validates the complete sparse-index
    /// layout (returning `data_end = 0` on malformed footers or indices) and
    /// queues later matching block offsets so callers inspecting a single
    /// partially damaged table can recover intact blocks. For raw-record slices
    /// without `OMNIV2**` magic, scans linearly from `0..data.len()`.
    fn resolve_best_effort_read_offsets(
        data: &[u8],
        target: &[u8],
        only_equal_key: bool,
    ) -> (usize, usize, std::collections::VecDeque<usize>) {
        if data.len() >= 16 && &data[data.len() - 8..] == b"OMNIV2**" {
            let Ok(layout) = Self::parse_strict_sstable_layout(data, false) else {
                return (0, 0, std::collections::VecDeque::new());
            };
            if layout.data_end == 0 || layout.index_entries.is_empty() {
                return (layout.data_end, 0, std::collections::VecDeque::new());
            }

            let idx = layout
                .index_entries
                .partition_point(|(key_range, _)| &data[key_range.clone()] < target);
            let start = if idx == 0 {
                0
            } else {
                layout.index_entries[idx - 1].1
            };

            let mut fallbacks = std::collections::VecDeque::new();
            let mut last_queued = start;
            for (key_range, off) in &layout.index_entries[idx..] {
                if only_equal_key && &data[key_range.clone()] != target {
                    break;
                }
                if *off > last_queued && *off < layout.data_end {
                    fallbacks.push_back(*off);
                    last_queued = *off;
                }
            }
            return (layout.data_end, start, fallbacks);
        }

        (data.len(), 0, std::collections::VecDeque::new())
    }

    pub fn find(&self, target_key: &[u8], read_seq: u64) -> Option<(u64, u64, u32, u64)> {
        let data = self.data.as_slice();
        let (data_end, mut offset, mut fallback_offsets) =
            Self::resolve_best_effort_read_offsets(data, target_key, true);
        let mut best: Option<(u64, ValuePointer)> = None;

        'walk: while offset < data_end {
            if let Some((rec, len)) = OmniRecord::decode(&data[offset..data_end]) {
                offset += len;
                if rec.key.as_slice() > target_key {
                    break;
                }
                if rec.key.as_slice() == target_key
                    && rec.seq <= read_seq
                    && best.is_none_or(|(best_seq, _)| rec.seq > best_seq)
                {
                    best = Some((
                        rec.seq,
                        (rec.offset, rec.length, rec.payload_crc32, rec.expiry),
                    ));
                }
            } else {
                while let Some(fb) = fallback_offsets.pop_front() {
                    if fb > offset && fb < data_end {
                        offset = fb;
                        continue 'walk;
                    }
                }
                break;
            }
        }
        best.map(|(_, ptr)| ptr)
    }

    pub(crate) fn find_with_seq(
        &self,
        target_key: &[u8],
        read_seq: u64,
        allow_legacy_zero_base: bool,
    ) -> Result<Option<(u64, ValuePointer)>, OmniError> {
        let data = self.data.as_slice();
        let (data_end, mut offset) =
            Self::resolve_strict_read_offsets(data, target_key, allow_legacy_zero_base)?;
        let mut best: Option<(u64, ValuePointer)> = None;

        while offset < data_end {
            let Some((rec, len)) = OmniRecord::decode(&data[offset..data_end]) else {
                return Err(OmniError::IoError(format!(
                    "corrupt SSTable record at byte offset {offset} (data_end={data_end})"
                )));
            };
            offset += len;
            if rec.key.as_slice() > target_key {
                break;
            }
            if rec.key.as_slice() == target_key
                && rec.seq <= read_seq
                && best.is_none_or(|(best_seq, _)| rec.seq > best_seq)
            {
                best = Some((
                    rec.seq,
                    (rec.offset, rec.length, rec.payload_crc32, rec.expiry),
                ));
            }
        }
        Ok(best)
    }

    pub(crate) fn try_iter_from(
        &self,
        start_key: &[u8],
        allow_legacy_zero_base: bool,
    ) -> Result<SSTableIterator, OmniError> {
        let (data_end, offset) = Self::resolve_strict_read_offsets(
            self.data.as_slice(),
            start_key,
            allow_legacy_zero_base,
        )?;
        Ok(SSTableIterator {
            data: self.data.clone(),
            offset,
            data_end,
            fallback_offsets: std::collections::VecDeque::new(),
            strict: true,
        })
    }

    pub fn iter_from(&self, start_key: &[u8]) -> SSTableIterator {
        let (data_end, offset, fallback_offsets) =
            Self::resolve_best_effort_read_offsets(self.data.as_slice(), start_key, false);
        SSTableIterator {
            data: self.data.clone(),
            offset,
            data_end,
            fallback_offsets,
            strict: false,
        }
    }

    /// Strict fail-closed full-table scan for compaction, heap GC, and L0
    /// tombstone safety checks.
    ///
    /// Validates the complete `OMNIV2**` footer and sparse-index structure
    /// upfront, then verifies every record CRC and confirms every sparse-index
    /// entry aligns with an exact decoded record boundary and key. Pass
    /// `allow_legacy_zero_base = true` only when the manifest proves the base
    /// table is an untouched pre-v2 placeholder.
    pub(crate) fn try_iter_all(
        &self,
        allow_legacy_zero_base: bool,
    ) -> Result<SSTableCheckedIterator, OmniError> {
        let layout =
            Self::parse_strict_sstable_layout(self.data.as_slice(), allow_legacy_zero_base)?;
        Ok(SSTableCheckedIterator {
            data: self.data.clone(),
            offset: 0,
            data_end: layout.data_end,
            index_entries: layout.index_entries,
            next_index_pos: 0,
            failed: false,
        })
    }
}

pub(crate) struct SSTableCheckedIterator {
    data: SSTableData,
    offset: usize,
    data_end: usize,
    index_entries: Vec<(std::ops::Range<usize>, usize)>,
    next_index_pos: usize,
    failed: bool,
}

impl Iterator for SSTableCheckedIterator {
    type Item = Result<OmniRecord, OmniError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        if self.offset >= self.data_end {
            if self.next_index_pos < self.index_entries.len() {
                self.failed = true;
                return Some(Err(OmniError::IoError(
                    "corrupt SSTable sparse index: unmatched trailing index entries".into(),
                )));
            }
            return None;
        }

        let data = self.data.as_slice();
        if let Some((_, expected_off)) = self.index_entries.get(self.next_index_pos)
            && self.offset > *expected_off
        {
            self.failed = true;
            return Some(Err(OmniError::IoError(format!(
                "corrupt SSTable sparse index: block offset {expected_off} does not align with a record boundary"
            ))));
        }

        let Some((rec, len)) = OmniRecord::decode(&data[self.offset..self.data_end]) else {
            self.failed = true;
            return Some(Err(OmniError::IoError(format!(
                "corrupt SSTable record at byte offset {} (data_end={})",
                self.offset, self.data_end
            ))));
        };

        let rec_start = self.offset;
        let rec_end = self.offset + len;
        if let Some((expected_key_range, expected_off)) =
            self.index_entries.get(self.next_index_pos)
            && rec_start == *expected_off
        {
            if rec.key.as_slice() != &data[expected_key_range.clone()] {
                self.failed = true;
                return Some(Err(OmniError::IoError(format!(
                    "corrupt SSTable sparse index: key mismatch at block offset {rec_start}"
                ))));
            }
            self.next_index_pos += 1;
        }
        if let Some((_, next_off)) = self.index_entries.get(self.next_index_pos)
            && *next_off < rec_end
        {
            self.failed = true;
            return Some(Err(OmniError::IoError(format!(
                "corrupt SSTable sparse index: block offset {next_off} falls inside record [{rec_start}..{rec_end})"
            ))));
        }

        self.offset = rec_end;
        if self.offset == self.data_end && self.next_index_pos != self.index_entries.len() {
            self.failed = true;
            return Some(Err(OmniError::IoError(
                "corrupt SSTable sparse index: unmatched index entries at end of data section"
                    .into(),
            )));
        }

        Some(Ok(rec))
    }
}

pub struct SSTableIterator {
    data: SSTableData,
    offset: usize,
    data_end: usize,
    fallback_offsets: std::collections::VecDeque<usize>,
    strict: bool,
}

impl SSTableIterator {
    pub(crate) fn try_next(&mut self) -> Result<Option<OmniRecord>, OmniError> {
        let data = self.data.as_slice();
        'walk: loop {
            if self.offset >= self.data_end {
                return Ok(None);
            }
            if let Some((rec, len)) = OmniRecord::decode(&data[self.offset..self.data_end]) {
                self.offset += len;
                return Ok(Some(rec));
            }
            if self.strict {
                return Err(OmniError::IoError(format!(
                    "corrupt SSTable record at byte offset {} (data_end={})",
                    self.offset, self.data_end
                )));
            }
            while let Some(fb) = self.fallback_offsets.pop_front() {
                if fb > self.offset && fb < self.data_end {
                    self.offset = fb;
                    continue 'walk;
                }
            }
            self.offset = self.data_end;
            return Ok(None);
        }
    }
}

impl Iterator for SSTableIterator {
    type Item = OmniRecord;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().ok().flatten()
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
    pub(crate) fn empty_table_bytes() -> [u8; 24] {
        let mut bytes = [0u8; 24];
        bytes[16..24].copy_from_slice(b"OMNIV2**");
        bytes
    }

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
        validate_key_len(&record.key)?;
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

type InFlightWriteKeys = HashMap<Vec<u8>, BTreeMap<u64, usize>>;

struct InFlightWriteGuard<'a> {
    db: &'a OmniKV,
    entries: Vec<(Vec<u8>, u64)>,
}

impl Drop for InFlightWriteGuard<'_> {
    fn drop(&mut self) {
        self.db.unregister_in_flight_write(&self.entries);
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
    /// Per-key sequence numbers reserved by batches that have released
    /// `write_mutex` for heap/WAL I/O and have not yet finished memtable
    /// publication (or definitively failed). Checked per key alongside active/
    /// frozen memtables and L0 SSTables so `compact_l1_to_l2` never drops a
    /// newer tombstone or expired record while an older version of that key
    /// exists in or can still publish into an unmerged upper tier.
    in_flight_write_keys: Mutex<InFlightWriteKeys>,
    pub(crate) block_cache: moka::sync::Cache<BlockCacheKey, String>,
    pub(crate) next_heap_generation: AtomicU64,
    /// Monotonically incremented under `transition_guard.write()` on every
    /// Raft snapshot installation so in-flight 2PC prepares detect any
    /// intervening snapshot replacement before voting `Commit`.
    pub(crate) snapshot_install_epoch: AtomicU64,
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

    // Fail-stop write fence tripped whenever on-disk durability or WAL state
    // becomes uncertain (post-rename manifest sync failure, group-commit fsync
    // failure, failed WAL partial-append rollback, or post-truncation WAL
    // rotation sync failure), halting all storage mutations until reopen.
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
            in_flight_write_keys: Mutex::new(HashMap::new()),
            block_cache: moka::sync::Cache::builder().max_capacity(100_000).build(),
            next_heap_generation: AtomicU64::new(2),
            snapshot_install_epoch: AtomicU64::new(0),
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
    /// read-only mode after an uncertain manifest, WAL, or group-commit fsync
    /// failure. Reopen the database to recover.
    pub fn is_write_fenced(&self) -> bool {
        self.write_fenced.load(Ordering::Acquire)
    }

    pub(crate) fn trip_write_fence(&self, reason: String) {
        if let Ok(mut guard) = self.write_fence_reason.lock()
            && guard.is_none()
        {
            *guard = Some(reason);
        }
        self.write_fenced.store(true, Ordering::Release);
        self.notify_compaction_waiters();
    }

    /// Trips the engine-wide write fence (`Release`) BEFORE poisoning
    /// `group_commit`, establishing a strict happens-before edge so any writer
    /// woken by poison (and dropping its `InFlightWriteGuard`) guarantees
    /// concurrent `compact_l1_to_l2` / `publish_manifest_or_fence` observes
    /// `write_fenced == true` (`Acquire`) and aborts before dropping tombstones.
    fn fence_and_poison_group_commit(&self, reason: String, group_err: Option<OmniError>) {
        self.trip_write_fence(reason.clone());
        self.group_commit
            .poison(group_err.unwrap_or(OmniError::IoError(reason)));
    }

    pub(crate) fn ensure_writes_not_fenced(&self) -> Result<(), OmniError> {
        if !self.write_fenced.load(Ordering::Acquire) {
            return Ok(());
        }
        let reason = self
            .write_fence_reason
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
            .unwrap_or_else(|| {
                "storage writes fenced after an unrecovered durability failure; reopen required"
                    .into()
            });
        Err(OmniError::IoError(reason))
    }

    fn publish_manifest_or_fence(
        &self,
        manifest: &Manifest,
        output_guard: Option<&mut CompactionOutputGuard>,
    ) -> Result<(), OmniError> {
        self.ensure_writes_not_fenced()?;
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

    /// Creates and durably syncs an empty valid `OMNIV2**` base SSTable (24
    /// bytes, non-empty for mmap) and empty heap file before an initial
    /// manifest referencing them is saved. Truncates any leftover partial file
    /// from an interrupted first-time initialization before rewriting.
    pub(crate) fn init_empty_storage_files(
        base_path: &str,
        heap_path: &str,
    ) -> Result<(), OmniError> {
        let base_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(base_path)?;
        SSTableWriter::new(&base_file).finish()?;
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

        let rebuild_bloom_checked =
            |reader: &SSTableReader, allow_legacy_base: bool| -> Result<BloomFilter, OmniError> {
                let mut hashes = Vec::new();
                for rec in reader.try_iter_all(allow_legacy_base)? {
                    let rec = rec?;
                    let mut h = FnvHasher::default();
                    rec.key.hash(&mut h);
                    hashes.push(h.finish());
                }
                let mut bloom = BloomFilter::new(hashes.len().max(100));
                for hash in hashes {
                    bloom.add(hash);
                }
                Ok(bloom)
            };

        let reader = SSTableReader::from_mmap(base_mmap.clone());
        let base_bloom_path = bloom_path_for_base(&manifest.base_path);
        let allow_legacy_base = Self::manifest_allows_legacy_zero_base(&manifest, manifest_path);
        SSTableReader::validate_table_layout(&base_mmap, allow_legacy_base).map_err(|e| {
            OmniError::IoError(format!(
                "corrupt base SSTable '{}' referenced by manifest: {e}",
                manifest.base_path
            ))
        })?;
        let base_bloom = match BloomFilter::load(&base_bloom_path) {
            Ok(b) => b,
            Err(_) => {
                let bloom = rebuild_bloom_checked(&reader, allow_legacy_base)?;
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
            if file.metadata()?.len() == 0 {
                return Err(OmniError::IoError(format!(
                    "corrupt empty L0 SSTable '{sst_path}' referenced by manifest"
                )));
            }
            // SAFETY: Recovered L0 SSTables are opened read-only and are
            // immutable once published in the manifest. The database LOCK
            // prevents concurrent OmniKV processes from mutating them.
            let mmap = Arc::new(unsafe { MmapOptions::new().map(&file) }.map_err(|e| {
                OmniError::IoError(format!(
                    "failed to memory-map L0 SSTable '{sst_path}' referenced by manifest: {e}"
                ))
            })?);
            SSTableReader::validate_table_layout(&mmap, false).map_err(|e| {
                OmniError::IoError(format!(
                    "corrupt L0 SSTable '{sst_path}' referenced by manifest: {e}"
                ))
            })?;
            let reader = SSTableReader::from_mmap(mmap.clone());
            let bloom_path = bloom_path_for_base(sst_path);
            let bloom = match BloomFilter::load(&bloom_path) {
                Ok(b) => b,
                Err(_) => {
                    let b = rebuild_bloom_checked(&reader, false)?;
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
            if file.metadata()?.len() == 0 {
                return Err(OmniError::IoError(format!(
                    "corrupt empty L1 SSTable '{sst_path}' referenced by manifest"
                )));
            }
            // SAFETY: Recovered L1 SSTables are immutable read-only table
            // files. They are replaced by publishing new root snapshots,
            // never by in-place mutation while mapped.
            let mmap = Arc::new(unsafe { MmapOptions::new().map(&file) }.map_err(|e| {
                OmniError::IoError(format!(
                    "failed to memory-map L1 SSTable '{sst_path}' referenced by manifest: {e}"
                ))
            })?);
            SSTableReader::validate_table_layout(&mmap, false).map_err(|e| {
                OmniError::IoError(format!(
                    "corrupt L1 SSTable '{sst_path}' referenced by manifest: {e}"
                ))
            })?;
            let reader = SSTableReader::from_mmap(mmap.clone());
            let bloom_path = bloom_path_for_base(sst_path);
            let bloom = match BloomFilter::load(&bloom_path) {
                Ok(b) => b,
                Err(_) => {
                    let b = rebuild_bloom_checked(&reader, false)?;
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

        let (wal, recovered_records) = wal::WriteAheadLog::open_and_heal_since(
            wal_path,
            &manifest.heap_path,
            Some(manifest.wal_checkpoint_seq),
        )?;
        let mut replayed_any = false;
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
                replayed_any = true;
                if rec.seq > global_seq {
                    global_seq = rec.seq;
                }
            }
        }
        if replayed_any {
            // Ensure any page-cache heap writes recovered after a process crash
            // (e.g. SIGKILL before fsync) are durable before exposing them.
            heap_file.sync_data()?;
        }

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

        // Synchronize against in-flight L0->L1 / L1->L2 compactions (matching
        // `snapshot()`) so no running compaction can drop versions `<= seq`
        // after `pin_replica_retention` has returned `Ok(())`.
        let _transition_guard = self
            .transition_guard
            .write()
            .map_err(|_| OmniError::LockPoisoned("transition_guard".into()))?;
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

    fn register_in_flight_write(
        &self,
        entries: Vec<(Vec<u8>, u64)>,
    ) -> Result<InFlightWriteGuard<'_>, OmniError> {
        let mut map = self
            .in_flight_write_keys
            .lock()
            .map_err(|_| OmniError::LockPoisoned("in_flight_write_keys".into()))?;
        for (key, seq) in &entries {
            *map.entry(key.clone()).or_default().entry(*seq).or_insert(0) += 1;
        }
        Ok(InFlightWriteGuard { db: self, entries })
    }

    fn unregister_in_flight_write(&self, entries: &[(Vec<u8>, u64)]) {
        if let Ok(mut map) = self.in_flight_write_keys.lock() {
            for (key, seq) in entries {
                if let Some(seqs) = map.get_mut(key) {
                    if let Some(count) = seqs.get_mut(seq) {
                        *count -= 1;
                        if *count == 0 {
                            seqs.remove(seq);
                        }
                    }
                    if seqs.is_empty() {
                        map.remove(key);
                    }
                }
            }
        }
    }

    /// Returns `true` only when `manifest` provides positive evidence that its
    /// `base_path` still references an untouched pre-v2 4096-byte empty base
    /// placeholder:
    /// 1. Untouched initial DB placeholder (`{manifest_path}_base.bin`,
    ///    `wal_checkpoint_seq == 0`, `read_seq_floor == 0`).
    /// 2. Untouched pre-v2 Raft snapshot install placeholder (`data_base.bin`,
    ///    `wal_checkpoint_seq == 0`; compaction and GC never write `data_base.bin`).
    /// 3. Restored pre-v2 empty backup (`base.bin`, `max_seq == 0`, no L0/L1,
    ///    `wal_checkpoint_seq == 0`, `read_seq_floor == 0`).
    ///
    /// Generated base tables (`_base_<ts>_<tok>.sst`, `_database_compacted_*`,
    /// or non-empty restored `base.bin`) always return `false`.
    pub(crate) fn manifest_allows_legacy_zero_base(
        manifest: &Manifest,
        manifest_path: &str,
    ) -> bool {
        fn file_names_match(a: &std::ffi::OsStr, b: &std::ffi::OsStr) -> bool {
            #[cfg(windows)]
            {
                if let (Some(a_str), Some(b_str)) = (a.to_str(), b.to_str()) {
                    return a_str.eq_ignore_ascii_case(b_str);
                }
            }
            a == b
        }

        let Some(actual_name) = Path::new(&manifest.base_path).file_name() else {
            return false;
        };
        if manifest.wal_checkpoint_seq != 0 {
            return false;
        }
        let expected_initial = format!("{manifest_path}_base.bin");
        if let Some(expected_name) = Path::new(&expected_initial).file_name()
            && file_names_match(actual_name, expected_name)
            && manifest.read_seq_floor == 0
        {
            return true;
        }
        if file_names_match(actual_name, std::ffi::OsStr::new("data_base.bin")) {
            return true;
        }
        file_names_match(actual_name, std::ffi::OsStr::new("base.bin"))
            && manifest.max_seq == 0
            && manifest.read_seq_floor == 0
            && manifest.sstables.is_empty()
            && manifest.l1_sstables.is_empty()
    }

    /// Runs a single strict checked pass over all L0 SSTables, validating every
    /// footer, sparse index, record boundary, and record CRC once per table and
    /// returning the minimum sequence number present in L0 for each key.
    fn collect_l0_min_seqs(sstables: &SharedSstables) -> Result<HashMap<Vec<u8>, u64>, OmniError> {
        let mut min_seqs: HashMap<Vec<u8>, u64> = HashMap::new();
        for (mmap, _, _) in sstables.iter() {
            let reader = SSTableReader::from_mmap(mmap.clone());
            for rec in reader.try_iter_all(false)? {
                let rec = rec?;
                min_seqs
                    .entry(rec.key)
                    .and_modify(|min_seq| *min_seq = (*min_seq).min(rec.seq))
                    .or_insert(rec.seq);
            }
        }
        Ok(min_seqs)
    }

    /// Returns `true` if any storage tier not merged by `compact_l1_to_l2`
    /// (`in_flight_write_keys`, active memtable, frozen memtables, or checked
    /// L0 SSTables via `l0_min_seqs`) contains — or can still publish — a
    /// version of `key` strictly older than `seq`.
    ///
    /// Evaluated per key against a single consistent `StorageRoots` snapshot
    /// while holding `write_mutex`, so:
    /// 1. Tombstones and expired records at `seq` cannot be dropped while an
    ///    older version of `key` remains in an unmerged upper tier (in-flight,
    ///    post-publish, frozen, L0, or recovered after restart / snapshot
    ///    install).
    /// 2. Unrelated keys in upper tiers do not block tombstone reclamation for
    ///    `key`.
    fn has_older_version_above_l1(
        roots: &StorageRoots,
        in_flight: &InFlightWriteKeys,
        l0_min_seqs: &HashMap<Vec<u8>, u64>,
        key: &[u8],
        seq: u64,
    ) -> bool {
        if seq == 0 {
            return false;
        }
        let max_older = seq - 1;

        if in_flight
            .get(key)
            .and_then(|seqs| seqs.keys().next().copied())
            .is_some_and(|min_seq| min_seq < seq)
        {
            return true;
        }

        if l0_min_seqs.get(key).is_some_and(|&min_seq| min_seq < seq) {
            return true;
        }

        let shard = shard_idx(key);
        let active_non_empty = !roots.memtable[shard].is_empty();
        let frozen_non_empty = roots
            .frozen_memtables
            .iter()
            .any(|frozen| !frozen[shard].is_empty());
        if active_non_empty
            && let Some(entry) = roots.memtable[shard]
                .range((key.to_vec(), Reverse(max_older))..)
                .next()
            && entry.key().0.as_slice() == key
            && entry.key().1.0 < seq
        {
            return true;
        }
        if frozen_non_empty {
            let lower_bound = (key.to_vec(), Reverse(max_older));
            for frozen in roots.frozen_memtables.iter() {
                if !frozen[shard].is_empty()
                    && let Some(entry) = frozen[shard].range(lower_bound.clone()..).next()
                    && entry.key().0.as_slice() == key
                    && entry.key().1.0 < seq
                {
                    return true;
                }
            }
        }
        false
    }

    /// Returns the most conservative (minimum) retention floor across active
    /// MVCC snapshots and replica catch-up pins.
    fn compaction_retention_floor(&self) -> Result<Option<u64>, OmniError> {
        Ok([
            self.oldest_active_snapshot()?,
            self.min_replica_retention()?,
        ]
        .into_iter()
        .flatten()
        .min())
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

    /// Collects up to `limit` visible entries in `[start_key, end_exclusive)` at `read_seq`
    /// using a bounded k-way streaming merge across memtables and SSTables.
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
        self.scan_bounds_limit(string_to_key(start_key), end_bound, read_seq, limit)
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

    /// Collects up to `limit` visible entries in the lexical range `[start_key, end_key]`
    /// using a bounded k-way streaming merge across memtables and SSTables.
    pub fn scan_limit(
        &self,
        start_key: &str,
        end_key: &str,
        read_seq: u64,
        limit: usize,
    ) -> Result<Vec<(String, String)>, OmniError> {
        let end_bytes = string_to_key(end_key);
        self.scan_bounds_limit(
            string_to_key(start_key),
            ScanEnd::Inclusive(&end_bytes),
            read_seq,
            limit,
        )
    }

    fn scan_bounds_limit(
        &self,
        start_key: Vec<u8>,
        end_bound: ScanEnd<'_>,
        read_seq: u64,
        limit: usize,
    ) -> Result<Vec<(String, String)>, OmniError> {
        let roots = self.roots.load();
        if read_seq < roots.read_seq_floor {
            return Err(OmniError::StaleSnapshot {
                read_seq,
                floor: roots.read_seq_floor,
            });
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let active_mem = roots.memtable.clone();
        let frozen = roots.frozen_memtables.clone();
        let mem_start = Reverse(read_seq.saturating_add(1));

        let mut sources: Vec<MergeSource<'_>> = Vec::with_capacity(
            active_mem.len()
                + frozen.len().saturating_mul(NUM_SHARDS)
                + roots.sstables.len()
                + roots.l1_sstables.len()
                + 1,
        );
        for shard in active_mem.iter() {
            sources.push(MergeSource::MemShard {
                iter: shard.range((start_key.clone(), mem_start)..),
                last_key: None,
                exhausted: false,
            });
        }
        for f_mem in frozen.iter().rev() {
            for shard in f_mem.iter() {
                sources.push(MergeSource::MemShard {
                    iter: shard.range((start_key.clone(), mem_start)..),
                    last_key: None,
                    exhausted: false,
                });
            }
        }
        let allow_legacy_base =
            Self::manifest_allows_legacy_zero_base(&roots.manifest, &self.manifest_path);
        for (mmap, _, _) in roots.sstables.iter().rev() {
            let reader = SSTableReader::from_mmap(mmap.clone());
            sources.push(MergeSource::Sst {
                iter: reader.try_iter_from(&start_key, false)?,
                peeked: None,
                exhausted: false,
            });
        }
        for (mmap, _, _) in &**roots.l1_sstables {
            let reader = SSTableReader::from_mmap(mmap.clone());
            sources.push(MergeSource::Sst {
                iter: reader.try_iter_from(&start_key, false)?,
                peeked: None,
                exhausted: false,
            });
        }
        let base_reader = SSTableReader::from_mmap(roots.base_mmap.clone());
        sources.push(MergeSource::Sst {
            iter: base_reader.try_iter_from(&start_key, allow_legacy_base)?,
            peeked: None,
            exhausted: false,
        });

        let mut heap: BinaryHeap<MergeHeapEntry> = BinaryHeap::with_capacity(sources.len());
        for (source_id, source) in sources.iter_mut().enumerate() {
            if let Some(candidate) = source.next_visible(&start_key, end_bound, read_seq)? {
                heap.push(MergeHeapEntry {
                    candidate,
                    source_id,
                });
            }
        }

        let mut results = Vec::with_capacity(limit.min(1024));
        let mut last_decided_key: Option<Vec<u8>> = None;
        let mut read_buffer = self.scan_buffer_pool.acquire();

        while results.len() < limit {
            let Some(MergeHeapEntry {
                candidate,
                source_id,
            }) = heap.pop()
            else {
                break;
            };
            if let Some(next_candidate) =
                sources[source_id].next_visible(&start_key, end_bound, read_seq)?
            {
                heap.push(MergeHeapEntry {
                    candidate: next_candidate,
                    source_id,
                });
            }
            if last_decided_key.as_deref() == Some(candidate.key.as_slice()) {
                continue;
            }
            last_decided_key = Some(candidate.key.clone());
            if candidate.length == 0 || (candidate.expiry > 0 && candidate.expiry <= now) {
                continue;
            }
            let key = key_to_string(&candidate.key);
            let value = self.read_from_heap_with_reader_into(
                &roots.heap_reader,
                roots.heap_generation,
                candidate.offset,
                candidate.length,
                candidate.crc,
                read_buffer.as_mut_vec(),
            )?;
            results.push((key, value));
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
        let _topology_guard = self
            .transition_guard
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _guard = self
            .write_mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.is_write_fenced() {
            tracing::warn!(
                seq,
                "dropping replica memtable insert while storage is write-fenced"
            );
            return;
        }
        if key.len() > MAX_KEY_SIZE {
            tracing::warn!(
                seq,
                key_len = key.len(),
                max_key_size = MAX_KEY_SIZE,
                "dropping replica memtable insert with unrepresentable key length"
            );
            return;
        }
        let shard = shard_idx(&key);
        let memtable = self.roots.load().memtable.clone();
        memtable[shard].insert((key, Reverse(seq)), (offset, length, crc, expiry));
        self.global_seq
            .fetch_max(seq.saturating_add(1), Ordering::SeqCst);
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
        validate_key_len(key)?;
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
    ///   Phase 2: Sequence, heap-offset, and in-flight key reservation (brief `write_mutex`)
    ///   Phase 3: Heap I/O + WAL append + group-commit fsync, followed by serialized
    ///            memtable publication under `write_mutex` so flushes cannot drop the target memtable
    ///
    /// This allows multiple concurrent batches to overlap CPU compression and
    /// disk I/O while keeping sequence reservation and memtable swaps atomic.
    pub fn commit_batch(&self, tx: &WriteBatch) -> Result<u64, OmniError> {
        self.ensure_writes_not_fenced()?;
        if tx.op_count() > MAX_BATCH_SIZE {
            return Err(OmniError::BatchTooLarge(MAX_BATCH_SIZE));
        }
        for (key, value, _) in &tx.buffered_writes {
            Self::validate_client_key_bytes(key)?;
            if value.len() > MAX_VALUE_SIZE {
                return Err(OmniError::ValueTooLarge(MAX_VALUE_SIZE));
            }
            if let Ok(key_str) = std::str::from_utf8(key)
                && let Some(conflict) = self.ssi_history.check_prepared_write_conflict(key_str)
            {
                return Err(OmniError::IoError(conflict));
            }
        }
        for key in &tx.buffered_deletes {
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

    /// Captures a linearizable, pinned read snapshot after briefly quiescing
    /// in-flight local writers so every write with `seq <= snapshot` is already
    /// visible in the memtable, every subsequent write will receive
    /// `seq > snapshot`, and compaction/GC will preserve versions and
    /// tombstones above `snapshot` until the guard is dropped.
    pub(crate) fn consistent_snapshot_guard(self: &Arc<Self>) -> Result<SnapshotGuard, OmniError> {
        self.ensure_cluster_linearizable()?;
        let _quiesce = self
            .transition_guard
            .write()
            .map_err(|_| OmniError::LockPoisoned("transition_guard".into()))?;
        let seq = self.global_seq.load(Ordering::SeqCst).saturating_sub(1);
        let mut snaps = self
            .active_snapshots
            .lock()
            .map_err(|_| OmniError::LockPoisoned("active_snapshots".into()))?;
        *snaps.entry(seq).or_insert(0) += 1;
        drop(snaps);
        Ok(SnapshotGuard {
            db: self.clone(),
            sequence: seq,
        })
    }

    /// Returns `true` if any record (put or tombstone) in `[range_start, range_end)`
    /// has a sequence number `> after_seq` across memtables and persisted tables.
    fn has_key_in_range_after_seq(
        &self,
        range_start: &str,
        range_end: &str,
        after_seq: u64,
    ) -> bool {
        if self.global_seq.load(Ordering::SeqCst).saturating_sub(1) <= after_seq {
            return false;
        }
        let start_bytes = range_start.as_bytes().to_vec();
        let end_bytes = range_end.as_bytes().to_vec();
        if start_bytes >= end_bytes {
            return false;
        }
        let roots = self.roots.load();
        let mem_range_start = (start_bytes.clone(), Reverse(u64::MAX));
        let mem_range_end = (end_bytes.clone(), Reverse(u64::MAX));

        for shard in roots.memtable.iter() {
            for entry in shard.range(mem_range_start.clone()..mem_range_end.clone()) {
                if entry.key().1.0 > after_seq {
                    return true;
                }
            }
        }
        for frozen in roots.frozen_memtables.iter() {
            for shard in frozen.iter() {
                for entry in shard.range(mem_range_start.clone()..mem_range_end.clone()) {
                    if entry.key().1.0 > after_seq {
                        return true;
                    }
                }
            }
        }
        let allow_legacy_base =
            Self::manifest_allows_legacy_zero_base(&roots.manifest, &self.manifest_path);
        let scan_table = |mmap: &Arc<Mmap>, allow_legacy: bool| -> bool {
            let reader = SSTableReader::from_mmap(mmap.clone());
            let mut iter = match reader.try_iter_from(&start_bytes, allow_legacy) {
                Ok(iter) => iter,
                Err(err) => {
                    tracing::error!(
                        "corrupt SSTable encountered during range conflict check ({err}); failing closed"
                    );
                    return true;
                }
            };
            loop {
                match iter.try_next() {
                    Ok(Some(rec)) => {
                        if rec.key >= end_bytes {
                            break;
                        }
                        if rec.key >= start_bytes && rec.is_valid() && rec.seq > after_seq {
                            return true;
                        }
                    }
                    Ok(None) => break,
                    Err(err) => {
                        tracing::error!(
                            "corrupt SSTable record encountered during range conflict check ({err}); failing closed"
                        );
                        return true;
                    }
                }
            }
            false
        };
        for (mmap, _, _) in &**roots.sstables {
            if scan_table(mmap, false) {
                return true;
            }
        }
        for (mmap, _, _) in &**roots.l1_sstables {
            if scan_table(mmap, false) {
                return true;
            }
        }
        scan_table(&roots.base_mmap, allow_legacy_base)
    }

    /// Returns `true` if any key in `keys` has a record (put or tombstone)
    /// with a sequence number `> after_seq`.
    fn has_any_key_after_seq(&self, keys: &[String], after_seq: u64) -> bool {
        if keys.is_empty() || self.global_seq.load(Ordering::SeqCst).saturating_sub(1) <= after_seq
        {
            return false;
        }
        keys.iter()
            .any(|key| self.get_seq_for_key(key, u64::MAX) > after_seq)
    }

    fn validate_conditional_batch(&self, tx: &WriteBatch) -> Result<(), OmniError> {
        self.ensure_writes_not_fenced()?;
        if tx.op_count() > MAX_BATCH_SIZE {
            return Err(OmniError::BatchTooLarge(MAX_BATCH_SIZE));
        }
        for (key, value, _) in &tx.buffered_writes {
            Self::validate_client_key_bytes(key)?;
            if value.len() > MAX_VALUE_SIZE {
                return Err(OmniError::ValueTooLarge(MAX_VALUE_SIZE));
            }
            if let Ok(key_str) = std::str::from_utf8(key)
                && let Some(conflict) = self.ssi_history.check_prepared_write_conflict(key_str)
            {
                return Err(OmniError::IoError(conflict));
            }
        }
        for key in &tx.buffered_deletes {
            Self::validate_client_key_bytes(key)?;
            if let Ok(key_str) = std::str::from_utf8(key)
                && let Some(conflict) = self.ssi_history.check_prepared_write_conflict(key_str)
            {
                return Err(OmniError::IoError(conflict));
            }
        }
        Ok(())
    }

    /// Atomically verifies that no write (put or tombstone) landed on any key
    /// in `watched_keys` after `read_seq`, and commits `tx` under that same
    /// exclusion barrier. Used by multi-batch online document migrations so
    /// intermediate chunks never clobber concurrent document updates or
    /// resurrect concurrent deletes.
    pub(crate) fn commit_batch_if_keys_unchanged(
        &self,
        tx: WriteBatch,
        watched_keys: &[String],
        read_seq: u64,
    ) -> Result<Result<u64, ()>, OmniError> {
        const KEYS_CHANGED_SENTINEL: &str = "__OMNIKV_KEYS_CHANGED__";
        self.validate_conditional_batch(&tx)?;
        if let Some(gateway) = self.cluster_gateway.get() {
            let write_keys: Vec<String> = tx
                .buffered_writes
                .iter()
                .map(|(k, _, _)| key_to_string(k))
                .chain(tx.buffered_deletes.iter().map(|k| key_to_string(k)))
                .collect();
            let ssi = crate::transaction::SsiCommitRecord {
                txn_id: 0,
                write_keys,
                read_keys: watched_keys.to_vec(),
                read_ranges: Vec::new(),
            };
            let outcome = gateway.commit_ssi_blocking(ssi, move || {
                {
                    let _quiesce = self
                        .transition_guard
                        .write()
                        .map_err(|_| "transition_guard poisoned".to_string())?;
                    if self.has_any_key_after_seq(watched_keys, read_seq) {
                        return Err(KEYS_CHANGED_SENTINEL.to_string());
                    }
                }
                if let Some(conflict) = self.ssi_history.check_batch_prepared_conflicts(&tx) {
                    return Err(conflict);
                }
                Ok(tx)
            });
            return match outcome {
                Ok(seq) => Ok(Ok(seq)),
                Err(msg) if msg == KEYS_CHANGED_SENTINEL => Ok(Err(())),
                Err(msg) => Err(OmniError::IoError(msg)),
            };
        }

        let _exclusive = self
            .transition_guard
            .write()
            .map_err(|_| OmniError::LockPoisoned("transition_guard".into()))?;
        self.ensure_writes_not_fenced()?;
        if self.has_any_key_after_seq(watched_keys, read_seq) {
            return Ok(Err(()));
        }
        if tx.is_empty() {
            return Ok(Ok(read_seq));
        }
        let seq = self.commit_batch_local_locked(&tx)?;
        Ok(Ok(seq))
    }

    /// Atomically verifies that no write (put or tombstone) landed in
    /// `[range_start, range_end)` after `read_seq`, and commits `tx` under
    /// that same exclusion barrier.
    ///
    /// Returns `Ok(Ok(commit_seq))` when committed, `Ok(Err(()))` when a
    /// concurrent write modified `[range_start, range_end)` after `read_seq`
    /// (signaling the caller to run another catch-up pass), or `Err(e)` on I/O
    /// or validation error.
    pub(crate) fn commit_batch_if_range_unchanged(
        &self,
        tx: WriteBatch,
        range_start: &str,
        range_end: &str,
        read_seq: u64,
    ) -> Result<Result<u64, ()>, OmniError> {
        const RANGE_CHANGED_SENTINEL: &str = "__OMNIKV_RANGE_CHANGED__";
        self.validate_conditional_batch(&tx)?;
        if let Some(gateway) = self.cluster_gateway.get() {
            let write_keys: Vec<String> = tx
                .buffered_writes
                .iter()
                .map(|(k, _, _)| key_to_string(k))
                .chain(tx.buffered_deletes.iter().map(|k| key_to_string(k)))
                .collect();
            let ssi = crate::transaction::SsiCommitRecord {
                txn_id: 0,
                write_keys,
                read_keys: Vec::new(),
                read_ranges: vec![(range_start.to_string(), range_end.to_string())],
            };
            let outcome = gateway.commit_ssi_blocking(ssi, move || {
                {
                    let _quiesce = self
                        .transition_guard
                        .write()
                        .map_err(|_| "transition_guard poisoned".to_string())?;
                    if self.has_key_in_range_after_seq(range_start, range_end, read_seq) {
                        return Err(RANGE_CHANGED_SENTINEL.to_string());
                    }
                }
                if let Some(conflict) = self.ssi_history.check_batch_prepared_conflicts(&tx) {
                    return Err(conflict);
                }
                Ok(tx)
            });
            return match outcome {
                Ok(seq) => Ok(Ok(seq)),
                Err(msg) if msg == RANGE_CHANGED_SENTINEL => Ok(Err(())),
                Err(msg) => Err(OmniError::IoError(msg)),
            };
        }

        let _exclusive = self
            .transition_guard
            .write()
            .map_err(|_| OmniError::LockPoisoned("transition_guard".into()))?;
        self.ensure_writes_not_fenced()?;
        if self.has_key_in_range_after_seq(range_start, range_end, read_seq) {
            return Ok(Err(()));
        }
        if tx.is_empty() {
            return Ok(Ok(read_seq));
        }
        let seq = self.commit_batch_local_locked(&tx)?;
        Ok(Ok(seq))
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

    pub(crate) fn snapshot_install_epoch(&self) -> u64 {
        self.snapshot_install_epoch.load(Ordering::SeqCst)
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
        let read_seq_floor = self.roots.load().read_seq_floor;
        if read_seq < read_seq_floor {
            return Some(format!(
                "STALE SNAPSHOT: 2PC prepare read_seq {read_seq} is below storage read_seq_floor {read_seq_floor}"
            ));
        }
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

    /// Commits a 2PC `PREPARED` batch (and, in clustered mode, its validated
    /// `unvalidated: false` transition under the same consensus flight lock)
    /// while atomically re-validating `read_seq` against `read_seq_floor`,
    /// `snapshot_install_epoch`, SSI history, and current key sequences under
    /// the same ordering guards as commits and snapshot installs.
    pub(crate) fn commit_2pc_prepare_checked<F>(
        &self,
        tx: &WriteBatch,
        read_seq: u64,
        write_keys: &[String],
        build_validated: F,
    ) -> Result<u64, crate::dist_txn::PrepareCommitError>
    where
        F: FnOnce(u64) -> Result<WriteBatch, OmniError>,
    {
        use crate::dist_txn::PrepareCommitError;
        self.ensure_writes_not_fenced()
            .map_err(PrepareCommitError::PreProposal)?;
        if let Some(gateway) = self.cluster_gateway.get() {
            return gateway.commit_2pc_prepare_checked(
                self,
                tx,
                read_seq,
                write_keys,
                build_validated,
            );
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
        if let Ok(validated_batch) = build_validated(seq) {
            let _ = crate::dist_txn::validate_2pc_decision_writes(self, None, &validated_batch)
                .and_then(|()| self.commit_batch_local_locked_skip_prepared(&validated_batch))
                .and_then(|_| self.sync_prepared_2pc_from_batch(&validated_batch));
        }
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
        // Validate batch size, every key, and every value at the shared commit
        // boundary before any write stall, sequence reservation, heap write, or
        // WAL append so manually constructed `WriteBatch` instances and internal
        // callers cannot persist keys exceeding `u16::MAX`, values exceeding
        // `MAX_VALUE_SIZE`, or WAL record counts exceeding `MAX_WAL_BATCH_RECORDS`.
        if tx.op_count() >= wal::MAX_WAL_BATCH_RECORDS {
            return Err(OmniError::BatchTooLarge(MAX_BATCH_SIZE));
        }
        for (key, value, _) in &tx.buffered_writes {
            validate_key_len(key)?;
            if value.len() > MAX_VALUE_SIZE {
                return Err(OmniError::ValueTooLarge(MAX_VALUE_SIZE));
            }
        }
        for key in &tx.buffered_deletes {
            validate_key_len(key)?;
        }
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
        // PHASE 2: Sequence, heap-offset & in-flight key reservation — WRITE_MUTEX
        // Assigns monotonic sequences, registers keys in `in_flight_write_keys`,
        // reserves contiguous heap bytes, and records SSI plain-batch history.
        // No disk I/O happens here.
        // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        let (base_seq, reserved_offset, in_flight_guard) = {
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
            let mut in_flight_entries =
                Vec::with_capacity(prepped_writes.len() + tx.buffered_deletes.len());
            let mut reserved_seq = seq;
            for (key, _, _, _, _) in &prepped_writes {
                in_flight_entries.push((key.clone(), reserved_seq));
                reserved_seq += 1;
            }
            for key in &tx.buffered_deletes {
                in_flight_entries.push((key.clone(), reserved_seq));
                reserved_seq += 1;
            }
            let in_flight_guard = self.register_in_flight_write(in_flight_entries)?;
            let mut stamped_any = false;
            if skip_prepared_check {
                for ((key, value, _), prepped) in
                    tx.buffered_writes.iter().zip(prepped_writes.iter_mut())
                {
                    if let Some(stamped) =
                        crate::dist_txn::stamp_2pc_commit_seq(key, value, commit_marker_seq)
                    {
                        let payload_bytes = stamped.as_bytes();
                        if payload_bytes.len() > MAX_VALUE_SIZE {
                            return Err(OmniError::ValueTooLarge(MAX_VALUE_SIZE));
                        }
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
            (seq, offset, in_flight_guard)
        };
        // write_mutex is RELEASED here â€” other batches can now reserve their sequences

        // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        // PHASE 3: Positional heap I/O + WAL + group-commit fsync + memtable publish
        // Concurrent batches overlap heap/WAL I/O, coalesce fsyncs via
        // `group_commit`, then briefly reacquire `write_mutex` for memtable
        // publication so concurrent flushes cannot discard the target memtable.
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
            if let Err(err) = wal.append_batch_nosync(&wal_records) {
                if wal.is_fenced() {
                    let reason = format!(
                        "WAL partial-append rollback failed ({err}); writes halted until database reopen"
                    );
                    self.fence_and_poison_group_commit(reason, None);
                }
                return Err(err);
            }
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

        // Memtable insertion must serialize with `compact_sstables_locked`'s
        // memtable swap under `write_mutex` so a concurrent flush cannot rotate
        // and discard a memtable while this committed batch is still inserting.
        {
            let _publish_guard = self
                .write_mutex
                .lock()
                .map_err(|_| OmniError::LockPoisoned("commit_publish".into()))?;
            let memtable = self.roots.load().memtable.clone();
            for (rec, _) in &wal_records {
                if rec.key.as_slice() != b"__COMMIT_MARKER__" {
                    let shard = shard_idx(&rec.key);
                    memtable[shard].insert(
                        (rec.key.clone(), Reverse(rec.seq)),
                        (rec.offset, rec.length, rec.payload_crc32, rec.expiry),
                    );
                }
            }
            // Remove per-key in-flight registrations only after every record is
            // visible in the active memtable while still holding `write_mutex`.
            drop(in_flight_guard);
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
        let heap = match self.heap_file.lock() {
            Ok(guard) => guard,
            Err(_) => {
                self.trip_write_fence(
                    "group commit heap_file lock poisoned; writes halted until database reopen"
                        .into(),
                );
                return Err(OmniError::LockPoisoned("heap_file lock".into()));
            }
        };
        let mut wal = match self.wal.lock() {
            Ok(guard) => guard,
            Err(_) => {
                self.trip_write_fence(
                    "group commit wal lock poisoned; writes halted until database reopen".into(),
                );
                return Err(OmniError::LockPoisoned("wal lock".into()));
            }
        };

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
            // Order is critical:
            // 1. Roll back undurable WAL bytes while group-commit followers are
            //    still blocked holding their `InFlightWriteGuard`s.
            // 2. Trip the engine-wide write fence (`Release`) BEFORE poisoning
            //    `group_commit`, so any woken writer that drops its
            //    `InFlightWriteGuard` guarantees concurrent `compact_l1_to_l2`
            //    observes `write_fenced == true` (`Acquire`) and aborts rather
            //    than dropping a tombstone above an unrolled-back WAL batch.
            let reason = match wal.discard_undurable() {
                Ok(()) => {
                    format!("group commit fsync failed ({e}); writes halted until database reopen")
                }
                Err(te) => {
                    let msg = format!(
                        "WAL rollback after fsync failure ({e}) failed ({te}); writes halted until database reopen"
                    );
                    tracing::warn!("{msg}");
                    msg
                }
            };
            self.fence_and_poison_group_commit(reason, Some(e.clone()));
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
        let is_uncompressed = (length_with_flag & UNCOMPRESSED_FLAG) != 0;
        let length = length_with_flag & !UNCOMPRESSED_FLAG;

        // Empty-string values occupy 0 heap bytes, so their `offset` can equal
        // the next non-empty value's offset. Validate CRC directly without
        // touching disk or colliding with neighboring entries in `block_cache`.
        if is_uncompressed && length == 0 {
            if crc32fast::hash(b"") != expected_crc32 {
                return Err(OmniError::IoError("Payload CRC32 mismatch!".into()));
            }
            return Ok(String::new());
        }

        if length > MAX_ENCODED_VALUE_SIZE {
            return Err(OmniError::IoError(format!(
                "corrupt heap record length {length} exceeds maximum {MAX_ENCODED_VALUE_SIZE}"
            )));
        }
        if !is_uncompressed && length < 4 {
            return Err(OmniError::IoError(
                "corrupt heap record: missing LZ4 size prefix".into(),
            ));
        }

        let cache_key: BlockCacheKey = (heap_generation, offset, length_with_flag, expected_crc32);
        if let Some(cached) = self.block_cache.get(&cache_key) {
            return Ok(cached);
        }

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
                let read_pos = offset
                    .checked_add(total_read as u64)
                    .ok_or_else(|| OmniError::IoError("heap seek_read: offset overflow".into()))?;
                let n = heap_reader
                    .seek_read(&mut buf[total_read..], read_pos)
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
            let declared = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            if declared > MAX_VALUE_SIZE {
                return Err(OmniError::IoError(format!(
                    "corrupt heap record: decoded length {declared} exceeds maximum {MAX_VALUE_SIZE}"
                )));
            }
            let decompressed = lz4_flex::decompress(&buf[4..], declared)
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

        let mut key_hasher = FnvHasher::default();
        target_key.hash(&mut key_hasher);
        let key_hash = key_hasher.finish();

        // Pick the highest sequence <= read_seq across every level of this
        // single atomic `StorageRoots` snapshot. Concurrent writers release
        // `write_mutex` before memtable insertion, so an older seq can land in
        // a newer memtable or L0 SSTable after a higher seq was already rotated.
        let mut best: Option<(u64, ValuePointer)> = None;
        let consider = |best: &mut Option<(u64, ValuePointer)>, seq: u64, ptr: ValuePointer| {
            if best.is_none_or(|(best_seq, _)| seq > best_seq) {
                *best = Some((seq, ptr));
            }
        };

        // SkipMap orders entries for a key by `Reverse(seq)` ascending (newest
        // first), so an inclusive lower bound at `Reverse(read_seq)` lands
        // directly on the highest version `<= read_seq`.
        let memtable_start = Reverse(read_seq);
        for entry in roots.memtable[shard].range((target_key.clone(), memtable_start)..) {
            if entry.key().0 != target_key {
                break;
            }
            let seq = entry.key().1.0;
            if seq <= read_seq {
                consider(&mut best, seq, *entry.value());
                break;
            }
        }

        if best.is_none_or(|(seq, _)| seq < read_seq) {
            for frozen in roots.frozen_memtables.iter().rev() {
                for entry in frozen[shard].range((target_key.clone(), memtable_start)..) {
                    if entry.key().0 != target_key {
                        break;
                    }
                    let seq = entry.key().1.0;
                    if seq <= read_seq {
                        consider(&mut best, seq, *entry.value());
                        break;
                    }
                }
            }
        }

        let allow_legacy_base =
            Self::manifest_allows_legacy_zero_base(&roots.manifest, &self.manifest_path);

        if best.is_none_or(|(seq, _)| seq < read_seq) {
            for (mmap, bloom, _) in roots.sstables.iter().rev() {
                if bloom.might_contain(key_hash) {
                    let reader = SSTableReader::from_mmap(mmap.clone());
                    if let Some((seq, ptr)) = reader.find_with_seq(&target_key, read_seq, false)? {
                        consider(&mut best, seq, ptr);
                    }
                }
            }
        }

        if best.is_none_or(|(seq, _)| seq < read_seq) {
            for (mmap, bloom, _) in roots.l1_sstables.iter().rev() {
                if bloom.might_contain(key_hash) {
                    let reader = SSTableReader::from_mmap(mmap.clone());
                    if let Some((seq, ptr)) = reader.find_with_seq(&target_key, read_seq, false)? {
                        consider(&mut best, seq, ptr);
                    }
                }
            }
        }

        if best.is_none_or(|(seq, _)| seq < read_seq) && roots.base_bloom.might_contain(key_hash) {
            let reader = SSTableReader::from_mmap(roots.base_mmap.clone());
            if let Some((seq, ptr)) =
                reader.find_with_seq(&target_key, read_seq, allow_legacy_base)?
            {
                consider(&mut best, seq, ptr);
            }
        }

        if let Some((_, (offset, length, crc, expiry))) = best {
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
        let allow_legacy_base =
            Self::manifest_allows_legacy_zero_base(&roots.manifest, &self.manifest_path);
        let sstables = roots.sstables.clone();
        for (mmap, _, _) in sstables.iter().rev() {
            let reader = SSTableReader::from_mmap(mmap.clone());
            // Sparse-index seeks may rewind before start_key; enforce both
            // bounds before collecting any persisted candidate.
            let mut iter = reader.try_iter_from(&start_key, false)?;
            while let Some(rec) = iter.try_next()? {
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
            let mut iter = reader.try_iter_from(&start_key, false)?;
            while let Some(rec) = iter.try_next()? {
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
        let mut base_iter = reader.try_iter_from(&start_key, allow_legacy_base)?;
        while let Some(rec) = base_iter.try_next()? {
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
        let roots = self.roots.load();
        let mut key_hasher = FnvHasher::default();
        target_key.hash(&mut key_hasher);
        let key_hash = key_hasher.finish();

        let mut best_seq = 0u64;
        let memtable_start = Reverse(read_seq);

        // 1. Active memtable
        for entry in roots.memtable[shard].range((target_key.clone(), memtable_start)..) {
            if entry.key().0 != target_key {
                break;
            }
            let seq = entry.key().1.0;
            if seq <= read_seq {
                best_seq = best_seq.max(seq);
                break;
            }
        }

        // 2. Frozen memtables
        if best_seq < read_seq {
            for frozen in roots.frozen_memtables.iter().rev() {
                for entry in frozen[shard].range((target_key.clone(), memtable_start)..) {
                    if entry.key().0 != target_key {
                        break;
                    }
                    let seq = entry.key().1.0;
                    if seq <= read_seq {
                        best_seq = best_seq.max(seq);
                        break;
                    }
                }
            }
        }

        let allow_legacy_base =
            Self::manifest_allows_legacy_zero_base(&roots.manifest, &self.manifest_path);

        // 3. L0 SSTables
        if best_seq < read_seq {
            for (mmap, bloom, _) in roots.sstables.iter().rev() {
                if bloom.might_contain(key_hash) {
                    let reader = SSTableReader::from_mmap(mmap.clone());
                    match reader.find_with_seq(&target_key, read_seq, false) {
                        Ok(Some((seq, _))) => best_seq = best_seq.max(seq),
                        Ok(None) => {}
                        Err(err) => {
                            tracing::error!(
                                "corrupt L0 SSTable encountered during get_seq_for_key ({err}); failing closed"
                            );
                            return u64::MAX;
                        }
                    }
                }
            }
        }

        // 4. L1 SSTables
        if best_seq < read_seq {
            for (mmap, bloom, _) in roots.l1_sstables.iter().rev() {
                if bloom.might_contain(key_hash) {
                    let reader = SSTableReader::from_mmap(mmap.clone());
                    match reader.find_with_seq(&target_key, read_seq, false) {
                        Ok(Some((seq, _))) => best_seq = best_seq.max(seq),
                        Ok(None) => {}
                        Err(err) => {
                            tracing::error!(
                                "corrupt L1 SSTable encountered during get_seq_for_key ({err}); failing closed"
                            );
                            return u64::MAX;
                        }
                    }
                }
            }
        }

        // 5. Base file
        if best_seq < read_seq && roots.base_bloom.might_contain(key_hash) {
            let reader = SSTableReader::from_mmap(roots.base_mmap.clone());
            match reader.find_with_seq(&target_key, read_seq, allow_legacy_base) {
                Ok(Some((seq, _))) => best_seq = best_seq.max(seq),
                Ok(None) => {}
                Err(err) => {
                    tracing::error!(
                        "corrupt base SSTable encountered during get_seq_for_key ({err}); failing closed"
                    );
                    return u64::MAX;
                }
            }
        }

        best_seq
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
            if active_empty && frozen_empty {
                let mut wal = self
                    .wal
                    .lock()
                    .map_err(|_| OmniError::LockPoisoned("wal lock".into()))?;
                if !wal.is_empty()
                    && let Err(err) = wal.rotate_segment()
                {
                    if wal.is_fenced() {
                        let reason = format!(
                            "WAL rotation failed after truncation ({err}); writes halted until database reopen"
                        );
                        self.fence_and_poison_group_commit(reason, None);
                    }
                    return Err(err);
                }
            }
        }
        Ok(())
    }

    /// Restores a frozen memtable back into the active memtable when an L0
    /// flush fails before manifest rename (while the engine is not fenced) so
    /// subsequent flushes and backups never leave those records orphaned.
    fn restore_frozen_memtable(&self, frozen_arc: &SharedMemtable) {
        let Ok(_guard) = self.write_mutex.lock() else {
            return;
        };
        if self.is_write_fenced() {
            return;
        }
        let cur = self.roots.load();
        for (shard_i, shard) in frozen_arc.iter().enumerate() {
            for entry in shard.iter() {
                cur.memtable[shard_i].insert(entry.key().clone(), *entry.value());
            }
        }
        let mut frozen = cur.frozen_memtables.as_ref().clone();
        if let Some(pos) = frozen
            .iter()
            .position(|entry| Arc::ptr_eq(entry, frozen_arc))
        {
            frozen.remove(pos);
            self.update_roots(|r| StorageRoots {
                frozen_memtables: Arc::new(frozen),
                ..r.clone()
            });
        }
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

        let flush_res: Result<(), OmniError> = (|| {
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
            // Sort by `(key ASC, seq ASC)` across shards to match L1/L2/GC SSTable
            // ordering so the latest version or tombstone for a multi-block key
            // always resides in the final block consistently across every level.
            records.sort_by(|a, b| a.key.cmp(&b.key).then_with(|| a.seq.cmp(&b.seq)));

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
            let bloom_path = bloom_path_for_base(&sst_path);
            let mut output_guard =
                CompactionOutputGuard::new(vec![sst_path.clone(), bloom_path.clone()]);

            let mut writer = SSTableWriter::new(&file);
            for rec in &records {
                writer.append(rec)?;
            }
            writer.finish()?;

            file.sync_all()?;
            bloom.save(&bloom_path)?;

            // SAFETY: The new L0 SSTable is fully written, flushed, and fsynced
            // before this read-only mapping is created. After publication, OmniKV
            // treats the table as immutable and replaces it only by root swap.
            let mmap = unsafe { MmapOptions::new().map(&file)? };
            SSTableReader::validate_table_layout(&mmap, false)?;

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
        })();

        if let Err(err) = flush_res {
            if !self.is_write_fenced() {
                self.restore_frozen_memtable(&frozen_arc);
            }
            return Err(err);
        }
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
            for rec in reader.try_iter_all(false)? {
                let rec = rec?;
                if rec.length == 0 {
                    stats.tombstones += 1;
                }
                insert_record_version(&mut merged, &rec);
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

        // `merged` iterates in strictly ascending `(key, seq)` order.
        debug_assert!(
            records
                .windows(2)
                .all(|w| (&w[0].key, w[0].seq) < (&w[1].key, w[1].seq))
        );

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
        let bloom_path = bloom_path_for_base(&sst_path);
        let mut output_guard =
            CompactionOutputGuard::new(vec![sst_path.clone(), bloom_path.clone()]);
        let mut writer = SSTableWriter::new(&file);
        for rec in &records {
            writer.append(rec)?;
        }
        writer.finish()?;
        file.sync_all()?;
        bloom.save(&bloom_path)?;

        // SAFETY: The new L1 SSTable is fully materialized and synced before
        // mapping. Compaction installs the mapping via an atomic root swap and
        // does not mutate the file afterward.
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        SSTableReader::validate_table_layout(&mmap, false)?;

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
        let cur_roots = self.roots.load();
        let l1_sstables_snap = cur_roots.l1_sstables.clone();
        if l1_sstables_snap.is_empty() {
            return Ok(());
        }
        let mut merged: BTreeMap<Vec<u8>, VersionedValuePointers> = BTreeMap::new();

        let allow_legacy_base =
            Self::manifest_allows_legacy_zero_base(&cur_roots.manifest, &self.manifest_path);
        let reader = SSTableReader::from_mmap(cur_roots.base_mmap.clone());
        for rec in reader.try_iter_all(allow_legacy_base)? {
            let rec = rec?;
            if rec.length == 0 {
                stats.tombstones += 1;
            }
            insert_record_version(&mut merged, &rec);
        }

        for (mmap, _, _) in &**l1_sstables_snap {
            let reader = SSTableReader::from_mmap(mmap.clone());
            for rec in reader.try_iter_all(false)? {
                let rec = rec?;
                if rec.length == 0 {
                    stats.tombstones += 1;
                }
                insert_record_version(&mut merged, &rec);
            }
        }

        let has_droppable_markers = !preserve_deletion_markers
            && merged.values().any(|versions| {
                versions
                    .values()
                    .any(|&(_, length, _, expiry)| length == 0 || (expiry > 0 && expiry <= now))
            });
        let l0_min_seqs = if has_droppable_markers && !cur_roots.sstables.is_empty() {
            Self::collect_l0_min_seqs(&cur_roots.sstables)?
        } else {
            HashMap::new()
        };

        let mut records = Vec::new();
        {
            let in_flight = self
                .in_flight_write_keys
                .lock()
                .map_err(|_| OmniError::LockPoisoned("in_flight_write_keys".into()))?;
            // Re-check write fence (`Acquire`) AFTER locking `in_flight_write_keys`:
            // because `fence_and_poison_group_commit` sets `write_fenced` (`Release`)
            // before poisoning `group_commit`, any writer that dropped its
            // `InFlightWriteGuard` due to poison is guaranteed to make this fail
            // closed before any tombstone can be dropped.
            self.ensure_writes_not_fenced()?;
            for (key, mut versions) in merged {
                retain_versions_needed_by_floor(&mut versions, retention_floor);
                for (seq, (offset, length, crc, expiry)) in versions {
                    let expired = expiry > 0 && expiry <= now;
                    if (length == 0 || expired)
                        && !preserve_deletion_markers
                        && !Self::has_older_version_above_l1(
                            &cur_roots,
                            &in_flight,
                            &l0_min_seqs,
                            &key,
                            seq,
                        )
                    {
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
        }

        // `merged` iterates in strictly ascending `(key, seq)` order.
        debug_assert!(
            records
                .windows(2)
                .all(|w| (&w[0].key, w[0].seq) < (&w[1].key, w[1].seq))
        );

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
        let bloom_path = bloom_path_for_base(&new_base_path);
        let mut output_guard =
            CompactionOutputGuard::new(vec![new_base_path.clone(), bloom_path.clone()]);
        let mut writer = SSTableWriter::new(&file);
        for rec in &records {
            writer.append(rec)?;
        }
        writer.finish()?;
        file.sync_all()?;
        bloom.save(&bloom_path)?;

        // SAFETY: The replacement base SSTable is complete and fsynced before
        // mapping. Existing readers retain Arc-owned mappings for old files,
        // while new readers see this immutable mapping after the root swap.
        let new_base_mmap = unsafe { MmapOptions::new().map(&file)? };
        SSTableReader::validate_table_layout(&new_base_mmap, false)?;

        // Critical Section: Swap ALL topology in a SINGLE atomic update from
        // the same `cur_roots` snapshot held across `write_mutex`.
        let old_base_path = cur_roots.manifest.base_path.clone();

        let mut manifest = cur_roots.manifest.as_ref().clone();
        manifest.base_path = new_base_path;
        manifest.l1_sstables.clear();
        self.publish_manifest_or_fence(&manifest, Some(&mut output_guard))?;

        self.update_roots(|r| StorageRoots {
            base_mmap: Arc::new(new_base_mmap),
            base_bloom: Arc::new(bloom),
            l1_sstables: Arc::new(Vec::new()),
            manifest: Arc::new(manifest),
            ..r.clone()
        });
        output_guard.disarm();

        let old_paths: Vec<String> = l1_sstables_snap.iter().map(|(_, _, p)| p.clone()).collect();
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
        let allow_legacy_base =
            Self::manifest_allows_legacy_zero_base(&cur_roots.manifest, &self.manifest_path);
        let reader = SSTableReader::from_mmap(cur_roots.base_mmap.clone());
        for rec in reader.try_iter_all(allow_legacy_base)? {
            let rec = rec?;
            add_to_merged(
                rec.key,
                rec.seq,
                rec.offset,
                rec.length,
                rec.payload_crc32,
                rec.expiry,
            );
        }

        for (mmap, _, _) in &**cur_roots.sstables {
            let reader = SSTableReader::from_mmap(mmap.clone());
            for rec in reader.try_iter_all(false)? {
                let rec = rec?;
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

        for (mmap, _, _) in &**cur_roots.l1_sstables {
            let reader = SSTableReader::from_mmap(mmap.clone());
            for rec in reader.try_iter_all(false)? {
                let rec = rec?;
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

        let mut old_heap_file = File::open(&cur_roots.manifest.heap_path)?;
        let old_heap_len = old_heap_file.metadata()?.len();
        let mut final_bytes = Vec::new();
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
                    }
                    continue;
                }
                if expiry > 0 && expiry <= now && !preserve_deletion_markers {
                    stats.expired_records_dropped += 1;
                    continue;
                }

                let is_uncompressed = (length & UNCOMPRESSED_FLAG) != 0;
                let raw_len = length & !UNCOMPRESSED_FLAG;
                if is_uncompressed && raw_len == 0 {
                    if crc32fast::hash(b"") != crc {
                        return Err(OmniError::IoError("GC CRC32 mismatch!".into()));
                    }
                    let mut h = FnvHasher::default();
                    k.hash(&mut h);
                    bloom.add(h.finish());

                    let rec = OmniRecord::new(seq, k.clone(), current_offset, length, crc, expiry);
                    base_sst_writer.append(&rec)?;
                    continue;
                }

                if raw_len > MAX_ENCODED_VALUE_SIZE {
                    return Err(OmniError::IoError(format!(
                        "GC corrupt heap record length {raw_len} exceeds maximum {MAX_ENCODED_VALUE_SIZE}"
                    )));
                }
                if !is_uncompressed && raw_len < 4 {
                    return Err(OmniError::IoError(
                        "GC corrupt heap record: missing LZ4 size prefix".into(),
                    ));
                }
                if old_offset
                    .checked_add(raw_len)
                    .is_none_or(|end| end > old_heap_len)
                {
                    return Err(OmniError::IoError(format!(
                        "GC corrupt heap pointer: offset {old_offset} + len {raw_len} exceeds heap size {old_heap_len}"
                    )));
                }

                // Raw read, NO decompression!
                use std::io::Seek;
                old_heap_file.seek(std::io::SeekFrom::Start(old_offset))?;
                final_bytes.clear();
                final_bytes.resize(raw_len as usize, 0);
                old_heap_file.read_exact(&mut final_bytes)?;

                // Validate CRC and compressed LZ4 length header before copying.
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(&final_bytes);
                if hasher.finalize() != crc {
                    return Err(OmniError::IoError("GC CRC32 mismatch!".into()));
                }
                if !is_uncompressed {
                    let declared = u32::from_le_bytes([
                        final_bytes[0],
                        final_bytes[1],
                        final_bytes[2],
                        final_bytes[3],
                    ]) as usize;
                    if declared > MAX_VALUE_SIZE {
                        return Err(OmniError::IoError(format!(
                            "GC corrupt heap record: decoded LZ4 length {declared} exceeds maximum {MAX_VALUE_SIZE}"
                        )));
                    }
                }
                let new_crc = crc;
                let new_length = length;
                stats.bytes_rewritten += raw_len;

                new_heap_writer.write_all(&final_bytes)?;

                let mut h = FnvHasher::default();
                k.hash(&mut h);
                bloom.add(h.finish());

                let rec =
                    OmniRecord::new(seq, k.clone(), current_offset, new_length, new_crc, expiry);
                base_sst_writer.append(&rec)?;
                current_offset += raw_len;
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
        SSTableReader::validate_table_layout(&new_base_mmap, false)?;

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

        let wal_clear_result = match self.wal.lock() {
            Ok(wal) if wal.is_empty() => Ok(()),
            Ok(mut wal) => match wal.clear() {
                Ok(()) => Ok(()),
                Err(err) => {
                    if wal.is_fenced() {
                        let reason = format!(
                            "heap_gc WAL clear failed after truncation ({err}); writes halted until database reopen"
                        );
                        self.fence_and_poison_group_commit(reason, None);
                    }
                    Err(err)
                }
            },
            Err(_) => Err(OmniError::LockPoisoned("wal lock".into())),
        };

        stats.record("heap_gc", started_at, self.compaction_backlog_sstables());
        drop(heap_lock);
        drop(write_guard);
        self.notify_compaction_waiters();
        wal_clear_result
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

    #[test]
    fn bounded_streaming_scans_match_full_scans_across_levels_mvcc_tombstones_and_ttl() {
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

        // Layer 1 (compacted all the way to base): initial keys k00..k19.
        let mut b1 = WriteBatch::new();
        for i in 0..20 {
            b1.set(&format!("k{i:02}"), format!("base-{i}"))
                .expect("set base");
        }
        let seq_base = db.commit_batch(&b1).expect("commit base");
        db.flush_memtable_to_disk(0).expect("flush base to l0");
        db.compact_l0_to_l1().expect("compact to l1");
        db.compact_l1_to_l2().expect("compact to base");

        // Layer 2 (L1): overwrite multiples of 3, delete k02/k05, add expired k04.
        let mut b2 = WriteBatch::new();
        for i in (0..20).step_by(3) {
            b2.set(&format!("k{i:02}"), format!("l1-{i}"))
                .expect("set l1");
        }
        b2.delete("k02").expect("del k02");
        b2.delete("k05").expect("del k05");
        b2.set_with_expiry("k04", "expired".into(), 1)
            .expect("set expired k04");
        let seq_l1 = db.commit_batch(&b2).expect("commit l1");
        db.flush_memtable_to_disk(0).expect("flush l1");
        db.compact_l0_to_l1().expect("compact l0 to l1");

        // Layer 3 (L0): multiple versions of k03 & k06, resurrect k05.
        let mut b3 = WriteBatch::new();
        b3.set("k03", "l0-a".into()).expect("set k03 a");
        b3.set("k06", "l0-a".into()).expect("set k06 a");
        b3.set("k05", "resurrected".into()).expect("resurrect k05");
        db.commit_batch(&b3).expect("commit l0 a");
        let mut b4 = WriteBatch::new();
        b4.set("k03", "l0-b".into()).expect("set k03 b");
        b4.delete("k06").expect("del k06");
        let seq_l0 = db.commit_batch(&b4).expect("commit l0 b");
        db.flush_memtable_to_disk(0).expect("flush l0");

        // Layer 4 (active memtable): overwrite k00, delete k01, add k07-mem.
        let mut b5 = WriteBatch::new();
        b5.set("k00", "mem-0".into()).expect("set k00");
        b5.delete("k01").expect("del k01");
        b5.set("k07", "mem-7".into()).expect("set k07");
        let seq_latest = db.commit_batch(&b5).expect("commit mem");

        for snap_seq in [seq_base, seq_l1, seq_l0, seq_latest] {
            let full_inc = db.scan("k00", "k19", snap_seq).expect("full scan");
            let full_exc = db.scan_prefix("k", snap_seq).expect("full prefix scan");
            for lim in [0, 1, 2, 5, 11, 50] {
                let got_inc = db
                    .scan_limit("k00", "k19", snap_seq, lim)
                    .expect("scan_limit");
                assert_eq!(got_inc, full_inc[..lim.min(full_inc.len())]);

                let got_exc = db
                    .scan_exclusive_limit("k00", Some("k15"), snap_seq, lim)
                    .expect("scan_exclusive_limit");
                let expected_exc: Vec<_> = full_exc
                    .iter()
                    .filter(|(k, _)| k.as_str() >= "k00" && k.as_str() < "k15")
                    .take(lim)
                    .cloned()
                    .collect();
                assert_eq!(got_exc, expected_exc);
            }
        }
        // Empty inverted range and stale snapshot floor checks.
        assert_eq!(
            db.scan_limit("k10", "k05", seq_latest, 5)
                .expect("inverted range")
                .len(),
            0
        );
        assert_eq!(
            db.scan_exclusive_limit("k05", Some("k05"), seq_latest, 5)
                .expect("empty exclusive range")
                .len(),
            0
        );
        db.update_roots(|r| StorageRoots {
            read_seq_floor: seq_latest.saturating_add(1),
            ..r.clone()
        });
        assert!(matches!(
            db.scan_exclusive_limit("k00", Some("k15"), seq_latest, 0),
            Err(OmniError::StaleSnapshot { .. })
        ));
        assert!(matches!(
            db.scan_limit("k00", "k19", seq_latest, 5),
            Err(OmniError::StaleSnapshot { .. })
        ));
    }

    fn base_contains_key(db: &OmniKV, key: &[u8]) -> bool {
        let roots = db.roots.load();
        let reader = SSTableReader::from_mmap(roots.base_mmap.clone());
        reader.iter_from(b"").any(|rec| rec.key.as_slice() == key)
    }

    #[test]
    fn in_flight_older_put_retains_only_matching_key_tombstone() {
        let dir = TempDir::new().expect("tempdir");
        let manifest = dir.path().join("manifest");
        let wal = dir.path().join("wal");
        let db = OmniKV::open(
            manifest.to_str().expect("UTF-8 manifest path"),
            wal.to_str().expect("UTF-8 WAL path"),
        )
        .expect("open");

        let low_seq = db.global_seq.fetch_add(2, Ordering::SeqCst);
        let guard = db
            .register_in_flight_write(vec![(b"raced_key".to_vec(), low_seq)])
            .expect("register in-flight write");

        let mut del = WriteBatch::new();
        del.delete("raced_key").expect("stage raced delete");
        del.delete("unrelated_key").expect("stage unrelated delete");
        let del_marker = db.commit_batch(&del).expect("commit deletes");
        assert!(del_marker > low_seq);
        db.compact_sstables().expect("flush L0");
        db.compact_l0_to_l1().expect("compact L0->L1");
        db.compact_l1_to_l2().expect("compact L1->L2");

        assert!(
            base_contains_key(&db, b"raced_key"),
            "tombstone for key with in-flight older write must be retained in L2"
        );
        assert!(
            !base_contains_key(&db, b"unrelated_key"),
            "unrelated key tombstone must not be retained by another key's in-flight write"
        );

        let offset = db
            .append_to_heap(b"resurrected_old")
            .expect("append delayed put");
        db.insert_replica_record(
            b"raced_key".to_vec(),
            low_seq,
            offset,
            UNCOMPRESSED_FLAG | b"resurrected_old".len() as u64,
            crc32fast::hash(b"resurrected_old"),
            0,
        );
        drop(guard);

        assert_eq!(
            db.find("raced_key", db.get_seq())
                .expect("find raced_key after delayed publish"),
            None,
            "newer tombstone in L2 must shadow delayed lower-seq memtable publish"
        );
        assert_eq!(
            db.get_seq_for_key("raced_key", db.get_seq()),
            del_marker - 2,
            "get_seq_for_key must pick the higher-seq L2 tombstone over the lower-seq memtable PUT"
        );
    }

    #[test]
    fn active_memtable_older_put_retains_l1_to_l2_tombstone_and_reclaims_after_merge() {
        let dir = TempDir::new().expect("tempdir");
        let manifest = dir.path().join("manifest");
        let wal = dir.path().join("wal");
        let manifest_str = manifest.to_str().expect("UTF-8 manifest path").to_string();
        let wal_str = wal.to_str().expect("UTF-8 WAL path").to_string();
        let db = OmniKV::open(&manifest_str, &wal_str).expect("open");

        // 1. Reserve low_seq for delayed PUT; commit newer DELETE into L1 (NOT L2 yet)
        //    and also place a newer live PUT for `seq_check_k` in L1 to test
        //    cross-tier `get_seq_for_key` max-seq selection.
        let low_seq = db.global_seq.fetch_add(2, Ordering::SeqCst);
        let live_low_seq = db.global_seq.fetch_add(2, Ordering::SeqCst);
        let mut newer = WriteBatch::new();
        newer.delete("raced_key").expect("stage raced delete");
        newer
            .set("seq_check_k", "newer_in_l1".into())
            .expect("stage newer live put");
        let newer_marker = db.commit_batch(&newer).expect("commit newer batch");
        let newer_live_seq = newer_marker - 2;
        db.compact_sstables().expect("flush newer batch to L0");
        db.compact_l0_to_l1().expect("compact newer batch to L1");

        // 2. Publish delayed older PUTs SOLELY into the active memtable (no
        //    in-flight guard held!) and run `compact_l1_to_l2()`.
        let offset = db
            .append_to_heap(b"resurrected_old")
            .expect("append delayed put");
        db.insert_replica_record(
            b"raced_key".to_vec(),
            low_seq,
            offset,
            UNCOMPRESSED_FLAG | b"resurrected_old".len() as u64,
            crc32fast::hash(b"resurrected_old"),
            0,
        );
        db.insert_replica_record(
            b"seq_check_k".to_vec(),
            live_low_seq,
            offset,
            UNCOMPRESSED_FLAG | b"resurrected_old".len() as u64,
            crc32fast::hash(b"resurrected_old"),
            0,
        );
        assert_eq!(
            db.get_seq_for_key("seq_check_k", db.get_seq()),
            newer_live_seq,
            "get_seq_for_key must scan past lower-seq memtable entry to higher-seq L1 entry"
        );
        assert_eq!(
            db.find("seq_check_k", db.get_seq())
                .expect("find seq_check_k"),
            Some("newer_in_l1".into())
        );

        db.compact_l1_to_l2()
            .expect("L1->L2 while older PUT resides only in active memtable");
        assert!(
            base_contains_key(&db, b"raced_key"),
            "active memtable older PUT must retain newer tombstone in L2"
        );
        assert_eq!(
            db.find("raced_key", db.get_seq()).expect("find raced_key"),
            None
        );

        // 3. Flush active memtable (`low_seq`) to L0, reopen, verify cross-tier
        //    `get_seq_for_key`, then drain L0->L1->L2 so the tombstone reclaims.
        db.compact_sstables().expect("flush older PUT to L0");
        drop(db);

        let reopened = OmniKV::open(&manifest_str, &wal_str).expect("reopen");
        assert_eq!(
            reopened.get_seq_for_key("seq_check_k", reopened.get_seq()),
            newer_live_seq,
            "get_seq_for_key after restart must pick higher-seq L2 entry over lower-seq L0 entry"
        );
        reopened.compact_l0_to_l1().expect("drain L0->L1");
        reopened
            .compact_l1_to_l2()
            .expect("reclaim merged tombstone");
        assert_eq!(
            reopened
                .find("raced_key", reopened.get_seq())
                .expect("find raced_key after full merge"),
            None
        );
        assert!(
            !base_contains_key(&reopened, b"raced_key"),
            "tombstone must be reclaimed once older version merges into L1/L2"
        );
    }

    #[test]
    fn post_restart_recovered_l0_older_put_retains_l1_to_l2_tombstone() {
        let dir = TempDir::new().expect("tempdir");
        let manifest = dir.path().join("manifest");
        let wal = dir.path().join("wal");
        let manifest_str = manifest.to_str().expect("UTF-8 manifest path").to_string();
        let wal_str = wal.to_str().expect("UTF-8 WAL path").to_string();
        let db = OmniKV::open(&manifest_str, &wal_str).expect("open");

        let low_seq = db.global_seq.fetch_add(2, Ordering::SeqCst);
        let mut del = WriteBatch::new();
        del.delete("raced_key").expect("stage delete");
        db.commit_batch(&del).expect("commit delete");
        db.compact_sstables().expect("flush delete to L0");
        db.compact_l0_to_l1().expect("compact delete to L1");

        let offset = db
            .append_to_heap(b"resurrected_old")
            .expect("append delayed put");
        db.insert_replica_record(
            b"raced_key".to_vec(),
            low_seq,
            offset,
            UNCOMPRESSED_FLAG | b"resurrected_old".len() as u64,
            crc32fast::hash(b"resurrected_old"),
            0,
        );
        // Flush delayed PUT to L0 (memtable empty, WAL rotates!), then restart
        // with L0=[low_seq PUT] and L1=[newer DELETE] simultaneously on disk.
        db.compact_sstables().expect("flush delayed put to L0");
        drop(db);

        let reopened = OmniKV::open(&manifest_str, &wal_str).expect("reopen");
        reopened
            .compact_l1_to_l2()
            .expect("compact L1->L2 immediately after restart while L0 holds low_seq");
        assert!(
            base_contains_key(&reopened, b"raced_key"),
            "L1->L2 after restart must retain tombstone while recovered L0 still holds older PUT"
        );
        assert_eq!(
            reopened
                .find("raced_key", reopened.get_seq())
                .expect("find raced_key after post-restart L1->L2"),
            None,
            "key must not resurrect after post-restart L1->L2 compaction"
        );
    }

    #[test]
    fn frozen_memtable_and_refcounted_in_flight_keys_retain_l1_to_l2_tombstone() {
        let dir = TempDir::new().expect("tempdir");
        let manifest = dir.path().join("manifest");
        let wal = dir.path().join("wal");
        let db = OmniKV::open(
            manifest.to_str().expect("UTF-8 manifest path"),
            wal.to_str().expect("UTF-8 WAL path"),
        )
        .expect("open");

        // 1. Verify refcount > 1 in `in_flight_write_keys`: dropping only one
        //    registration keeps protection active until the second guard drops.
        let low_seq = db.global_seq.fetch_add(2, Ordering::SeqCst);
        let g1 = db
            .register_in_flight_write(vec![(b"ref_k".to_vec(), low_seq)])
            .expect("register first guard");
        let g2 = db
            .register_in_flight_write(vec![(b"ref_k".to_vec(), low_seq)])
            .expect("register duplicate guard");
        drop(g1);
        {
            let roots = db.roots.load();
            let in_flight = db.in_flight_write_keys.lock().expect("in_flight lock");
            let empty_l0 = HashMap::new();
            assert!(
                OmniKV::has_older_version_above_l1(
                    &roots,
                    &in_flight,
                    &empty_l0,
                    b"ref_k",
                    low_seq + 5
                ),
                "second duplicate in-flight guard must keep key protected"
            );
        }
        drop(g2);
        {
            let roots = db.roots.load();
            let in_flight = db.in_flight_write_keys.lock().expect("in_flight lock");
            let empty_l0 = HashMap::new();
            assert!(
                !OmniKV::has_older_version_above_l1(
                    &roots,
                    &in_flight,
                    &empty_l0,
                    b"ref_k",
                    low_seq + 5
                ),
                "key must be unprotected after final guard drops"
            );
        }

        // 2. Place an older PUT solely inside `roots.frozen_memtables` while a
        //    newer DELETE lives in L1, then run `compact_l1_to_l2()`.
        let frozen_seq = db.global_seq.fetch_add(2, Ordering::SeqCst);
        let mut del = WriteBatch::new();
        del.delete("frozen_k").expect("stage delete");
        let del_marker = db.commit_batch(&del).expect("commit delete");
        assert!(del_marker > frozen_seq);
        db.compact_sstables().expect("flush delete to L0");
        db.compact_l0_to_l1().expect("compact delete to L1");

        let offset = db
            .append_to_heap(b"older_in_frozen")
            .expect("append frozen payload");
        let frozen_mem: SharedMemtable = Arc::new(std::array::from_fn(|_| SkipMap::new()));
        frozen_mem[shard_idx(b"frozen_k")].insert(
            (b"frozen_k".to_vec(), Reverse(frozen_seq)),
            (
                offset,
                UNCOMPRESSED_FLAG | b"older_in_frozen".len() as u64,
                crc32fast::hash(b"older_in_frozen"),
                0,
            ),
        );
        db.update_roots(|r| {
            let mut frozen = r.frozen_memtables.as_ref().clone();
            frozen.push(frozen_mem.clone());
            StorageRoots {
                frozen_memtables: Arc::new(frozen),
                ..r.clone()
            }
        });

        db.compact_l1_to_l2()
            .expect("compact L1->L2 with older version in frozen_memtables");
        assert!(
            base_contains_key(&db, b"frozen_k"),
            "L1->L2 must retain tombstone while older version sits in frozen_memtables"
        );
        assert_eq!(
            db.find("frozen_k", db.get_seq())
                .expect("find frozen_k after L1->L2"),
            None,
            "frozen memtable older PUT must not resurrect past newer tombstone"
        );
    }

    #[test]
    fn expired_ttl_record_in_l1_is_retained_while_older_live_put_exists_above_l1() {
        let dir = TempDir::new().expect("tempdir");
        let manifest = dir.path().join("manifest");
        let wal = dir.path().join("wal");
        let db = OmniKV::open(
            manifest.to_str().expect("UTF-8 manifest path"),
            wal.to_str().expect("UTF-8 WAL path"),
        )
        .expect("open");

        let low_seq = db.global_seq.fetch_add(2, Ordering::SeqCst);
        let guard = db
            .register_in_flight_write(vec![(b"ttl_raced".to_vec(), low_seq)])
            .expect("register in-flight");
        let mut exp = WriteBatch::new();
        exp.set_with_expiry("ttl_raced", "already_expired".into(), 1)
            .expect("stage expired overwrite");
        db.commit_batch(&exp).expect("commit expired overwrite");
        db.compact_sstables().expect("flush expired to L0");
        db.compact_l0_to_l1().expect("compact expired to L1");

        // Publish non-expiring older PUT into active memtable and drop guard.
        let offset = db.append_to_heap(b"old_live").expect("append old live");
        db.insert_replica_record(
            b"ttl_raced".to_vec(),
            low_seq,
            offset,
            UNCOMPRESSED_FLAG | b"old_live".len() as u64,
            crc32fast::hash(b"old_live"),
            0,
        );
        drop(guard);

        // L1->L2 must keep the expired marker because active memtable has older live PUT.
        db.compact_l1_to_l2().expect("compact expired L1->L2");
        assert!(
            base_contains_key(&db, b"ttl_raced"),
            "expired record in L1 must be retained while older non-expiring PUT sits in memtable"
        );
        assert_eq!(
            db.find("ttl_raced", db.get_seq())
                .expect("find ttl_raced after L1->L2"),
            None,
            "older non-expiring PUT must not resurrect past newer expired record"
        );
    }

    #[test]
    fn snapshot_and_replica_retention_boundaries_survive_l1_to_l2_compaction() {
        let dir = TempDir::new().expect("tempdir");
        let manifest = dir.path().join("manifest");
        let wal = dir.path().join("wal");
        let db = OmniKV::open(
            manifest.to_str().expect("UTF-8 manifest path"),
            wal.to_str().expect("UTF-8 WAL path"),
        )
        .expect("open");

        let mut b1 = WriteBatch::new();
        b1.set("pin_key", "v1".into()).expect("stage v1");
        db.commit_batch(&b1).expect("commit v1");
        let snap_early = db.snapshot();

        let mut b2 = WriteBatch::new();
        b2.set("pin_key", "v2".into()).expect("stage v2");
        db.commit_batch(&b2).expect("commit v2");
        let replica_later = db.get_seq().saturating_sub(1);
        assert!(snap_early < replica_later);
        db.pin_replica_retention("replica_a", replica_later)
            .expect("pin replica");

        // Floor must equal `min(snap_early, replica_later) == snap_early`, not `.max()`.
        assert_eq!(
            db.compaction_retention_floor().expect("combined min floor"),
            Some(snap_early)
        );

        let mut b3 = WriteBatch::new();
        b3.delete("pin_key").expect("stage delete");
        db.commit_batch(&b3).expect("commit delete");
        db.compact_sstables().expect("flush L0");
        db.compact_l0_to_l1().expect("compact L0->L1");
        db.compact_l1_to_l2()
            .expect("compact L1->L2 under both pins");

        assert_eq!(
            db.find("pin_key", snap_early).expect("read v1 under snap"),
            Some("v1".into())
        );
        assert_eq!(
            db.find("pin_key", replica_later)
                .expect("read v2 under replica pin"),
            Some("v2".into())
        );
        assert_eq!(db.find("pin_key", db.get_seq()).expect("read latest"), None);

        // Release replica pin FIRST: active snapshot alone must still retain `v1`.
        db.release_replica_retention("replica_a")
            .expect("release replica");
        assert_eq!(
            db.compaction_retention_floor()
                .expect("snapshot-only floor"),
            Some(snap_early)
        );
        let mut b4 = WriteBatch::new();
        b4.set("other", "1".into()).expect("stage other");
        db.commit_batch(&b4).expect("commit other");
        db.compact_sstables().expect("flush other");
        db.compact_l0_to_l1().expect("compact other L1");
        db.compact_l1_to_l2()
            .expect("compact L1->L2 under snapshot-only pin");
        assert_eq!(
            db.find("pin_key", snap_early)
                .expect("read v1 under snapshot alone"),
            Some("v1".into())
        );

        // Re-pin replica at `replica_later`, drop snapshot: replica pin alone
        // must still retain `v2` (and now `compaction_retention_floor == Some(replica_later)`).
        db.pin_replica_retention("replica_b", replica_later)
            .expect("pin replica_b");
        db.unregister_snapshot(snap_early);
        assert_eq!(
            db.compaction_retention_floor().expect("replica-only floor"),
            Some(replica_later)
        );
        let mut b5 = WriteBatch::new();
        b5.set("other", "2".into()).expect("stage other2");
        db.commit_batch(&b5).expect("commit other2");
        db.compact_sstables().expect("flush other2");
        db.compact_l0_to_l1().expect("compact other2 L1");
        db.compact_l1_to_l2()
            .expect("compact L1->L2 under replica-only pin");
        assert_eq!(
            db.find("pin_key", replica_later)
                .expect("read v2 under replica pin alone"),
            Some("v2".into())
        );

        // Release final pin; next L1->L2 must reclaim the tombstone.
        db.release_replica_retention("replica_b")
            .expect("release replica_b");
        assert_eq!(db.compaction_retention_floor().expect("empty floor"), None);
        let mut b6 = WriteBatch::new();
        b6.set("other", "3".into()).expect("stage other3");
        db.commit_batch(&b6).expect("commit other3");
        db.compact_sstables().expect("flush other3");
        db.compact_l0_to_l1().expect("compact other3 L1");
        db.compact_l1_to_l2().expect("reclaim tombstone");
        assert!(
            !base_contains_key(&db, b"pin_key"),
            "tombstone must be reclaimed once both snapshot and replica pins are released"
        );
    }

    #[test]
    fn internal_commit_paths_reject_oversized_keys_and_empty_v2_sstable_passes_strict_scan() {
        let dir = TempDir::new().expect("tempdir");
        let manifest = dir.path().join("manifest");
        let wal = dir.path().join("wal");
        let db = OmniKV::open(
            manifest.to_str().expect("UTF-8 manifest path"),
            wal.to_str().expect("UTF-8 WAL path"),
        )
        .expect("open");

        let oversized_write = WriteBatch {
            buffered_writes: vec![(vec![b'k'; MAX_KEY_SIZE + 1], "v".into(), 0)],
            buffered_deletes: Vec::new(),
        };
        let oversized_delete = WriteBatch {
            buffered_writes: Vec::new(),
            buffered_deletes: vec![vec![b'd'; MAX_KEY_SIZE + 1]],
        };
        assert!(
            db.commit_batch_local(&oversized_write).is_err(),
            "internal commit_batch_local must reject direct WriteBatch key > u16::MAX"
        );
        assert!(
            db.commit_batch_local(&oversized_delete).is_err(),
            "internal commit_batch_local must reject direct WriteBatch delete key > u16::MAX"
        );
        assert!(
            db.commit_2pc_batch(&oversized_write).is_err(),
            "internal commit_2pc_batch must reject direct WriteBatch key > u16::MAX"
        );

        let mut meta_batch = WriteBatch::new();
        assert!(
            meta_batch
                .set_internal_meta(&"m".repeat(MAX_KEY_SIZE + 1), "v".into())
                .is_err(),
            "set_internal_meta must reject key > u16::MAX"
        );
        assert!(
            meta_batch
                .set_with_ttl("ttl_max", "v".into(), u64::MAX)
                .is_ok(),
            "set_with_ttl with u64::MAX must saturate instead of overflowing"
        );
        assert_eq!(meta_batch.buffered_writes[0].2, u64::MAX);

        let oversized_val_batch = WriteBatch {
            buffered_writes: vec![(b"big_val".to_vec(), "a".repeat(MAX_VALUE_SIZE + 1), 0)],
            buffered_deletes: Vec::new(),
        };
        assert!(
            matches!(
                db.commit_batch(&oversized_val_batch),
                Err(OmniError::ValueTooLarge(_))
            ),
            "commit_batch must reject manually constructed WriteBatch with value > MAX_VALUE_SIZE"
        );
        assert!(
            matches!(
                db.commit_batch_local(&oversized_val_batch),
                Err(OmniError::ValueTooLarge(_))
            ),
            "commit_batch_local must reject manually constructed WriteBatch with value > MAX_VALUE_SIZE"
        );

        let oversized_count_batch = WriteBatch {
            buffered_writes: Vec::new(),
            buffered_deletes: (0..=MAX_BATCH_SIZE)
                .map(|i| format!("k{i}").into_bytes())
                .collect(),
        };
        assert!(
            matches!(
                db.commit_batch(&oversized_count_batch),
                Err(OmniError::BatchTooLarge(_))
            ),
            "commit_batch must reject manually constructed WriteBatch exceeding MAX_BATCH_SIZE"
        );

        // `insert_replica_record` must drop keys > u16::MAX without poisoning
        // the memtable or breaking subsequent flushes.
        let seq_before = db.get_seq();
        db.insert_replica_record(
            vec![b'r'; MAX_KEY_SIZE + 1],
            seq_before + 5,
            0,
            UNCOMPRESSED_FLAG,
            0,
            0,
        );
        assert_eq!(db.get_seq(), seq_before);
        db.compact_sstables()
            .expect("flush after dropped oversized replica key must succeed");

        let empty_path = dir.path().join("empty_v2.sst");
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&empty_path)
            .expect("create empty v2 sst");
        SSTableWriter::new(&file)
            .finish()
            .expect("finish empty v2 sst");
        drop(file);

        let bytes = std::fs::read(&empty_path).expect("read empty v2 sst");
        assert_eq!(bytes.len(), 24, "empty OMNIV2** table must be 24 bytes");
        let reader = SSTableReader::new(&bytes);
        let recs: Vec<OmniRecord> = reader
            .try_iter_all(false)
            .expect("valid 24-byte empty OMNIV2** table must pass strict scan")
            .collect::<Result<Vec<_>, _>>()
            .expect("iterate empty v2 sst");
        assert_eq!(recs, Vec::<OmniRecord>::new());
    }

    #[test]
    fn bloom_filter_rejects_corrupt_metadata_and_missing_bloom_rebuild_fails_closed_on_corrupt_sstable()
     {
        let dir = TempDir::new().expect("bloom dir");

        let zero_elem_bloom = BloomFilter::new(0);
        assert!(zero_elem_bloom.num_bits >= 64);
        assert!(zero_elem_bloom.num_hashes >= 1);

        for (idx, bad_json) in [
            r#"{"bitset":[1],"num_bits":64,"num_hashes":1000000000}"#,
            r#"{"bitset":[],"num_bits":64,"num_hashes":1}"#,
            r#"{"bitset":[1],"num_bits":1000000,"num_hashes":7}"#,
            r#"{"bitset":[],"num_bits":0,"num_hashes":0}"#,
        ]
        .into_iter()
        .enumerate()
        {
            let p = dir.path().join(format!("bad_{idx}.bloom"));
            std::fs::write(&p, bad_json).expect("write bad bloom json");
            assert!(
                BloomFilter::load(p.to_str().expect("UTF-8 bloom path")).is_err(),
                "corrupt bloom JSON case {idx} must be rejected by BloomFilter::load"
            );
        }

        // Deleting `.bloom` next to a corrupt L0 SSTable must cause reopen to
        // fail closed instead of persisting a poisoned empty `.bloom`.
        let manifest = dir.path().join("manifest.json");
        let wal = dir.path().join("wal.bin");
        let m_str = manifest.to_str().expect("UTF-8 manifest").to_string();
        let w_str = wal.to_str().expect("UTF-8 wal").to_string();
        let db = OmniKV::open(&m_str, &w_str).expect("open db");
        let mut b = WriteBatch::new();
        b.set("k1", "v1".into()).expect("stage k1");
        db.commit_batch(&b).expect("commit k1");
        db.compact_sstables().expect("flush L0");
        let l0_path = db.roots.load().manifest.sstables[0].clone();
        let l0_bloom = bloom_path_for_base(&l0_path);
        drop(db);

        std::fs::write(&l0_path, vec![0xFF; 64]).expect("corrupt L0 sstable");
        std::fs::remove_file(&l0_bloom).expect("remove L0 bloom");
        assert!(
            OmniKV::open(&m_str, &w_str).is_err(),
            "reopen must fail closed when rebuilding missing bloom from corrupt L0 SSTable"
        );
        assert!(
            !Path::new(&l0_bloom).exists(),
            "failed bloom rebuild must not write a poisoned bloom sidecar to disk"
        );
    }

    #[test]
    fn malformed_v2_sstable_fails_closed_on_reads_and_compaction_without_stale_fallthrough() {
        let dir = TempDir::new().expect("malformed v2 dir");
        let manifest = dir.path().join("manifest.json");
        let wal = dir.path().join("wal.bin");
        let m_str = manifest.to_str().expect("UTF-8 manifest").to_string();
        let w_str = wal.to_str().expect("UTF-8 wal").to_string();
        let db = OmniKV::open(&m_str, &w_str).expect("open db");

        // Commit older live values and compact them into the L2 base table.
        let mut base_batch = WriteBatch::new();
        base_batch
            .set("k_del", "stale_deleted_base".into())
            .expect("stage k_del base");
        base_batch
            .set("k_hot", "stale_old_base".into())
            .expect("stage k_hot base");
        db.commit_batch(&base_batch).expect("commit base batch");
        db.compact_sstables().expect("flush base L0");
        db.compact_l0_to_l1().expect("compact base L1");
        db.compact_l1_to_l2().expect("compact base L2");
        let base_seq = db.get_seq_for_key("k_hot", db.get_seq());
        assert!(base_seq > 0);

        // Commit a newer overwrite for `k_hot` and a newer tombstone for `k_del`
        // into an L0 table above the base table.
        let mut l0_batch = WriteBatch::new();
        l0_batch.delete("k_del").expect("stage k_del delete");
        l0_batch
            .set("k_hot", "fresh_l0_val".into())
            .expect("stage k_hot update");
        db.commit_batch(&l0_batch).expect("commit l0 batch");
        db.compact_sstables().expect("flush L0");

        let cur_roots = db.roots.load();
        let orig_l0_path = cur_roots.sstables[0].2.clone();
        let orig_l0_bloom = cur_roots.sstables[0].1.clone();
        let orig_l0_bytes = std::fs::read(&orig_l0_path).expect("read original L0");
        let (_, rec0_len) = OmniRecord::decode(&orig_l0_bytes).expect("decode first L0 rec");

        // Build two distinct post-load corrupted copies of the L0 table:
        // 1. Malformed v2 footer (`index_offset = 0` on non-empty table)
        // 2. Intact v2 footer/index with a corrupted record in the data block
        let mut bad_footer_bytes = orig_l0_bytes.clone();
        let blen = bad_footer_bytes.len();
        bad_footer_bytes[blen - 16..blen - 8].copy_from_slice(&0u64.to_le_bytes());

        let mut bad_record_bytes = orig_l0_bytes;
        bad_record_bytes[rec0_len..rec0_len + 12].fill(0xFF);

        for (label, corrupt_bytes) in [
            ("malformed_v2_footer", bad_footer_bytes),
            ("corrupt_v2_record", bad_record_bytes),
        ] {
            let corrupt_path = dir.path().join(format!("{label}.sst"));
            std::fs::write(&corrupt_path, &corrupt_bytes).expect("write corrupt L0");
            let file = OpenOptions::new()
                .read(true)
                .open(&corrupt_path)
                .expect("open corrupt L0");
            // SAFETY: Test-only immutable file mapped for the duration of this loop.
            let corrupt_mmap = Arc::new(unsafe { MmapOptions::new().map(&file) }.expect("mmap"));
            db.update_roots(|r| StorageRoots {
                sstables: Arc::new(vec![(
                    corrupt_mmap.clone(),
                    orig_l0_bloom.clone(),
                    orig_l0_path.clone(),
                )]),
                ..r.clone()
            });

            let seq = db.get_seq();
            for key in ["k_hot", "k_del"] {
                assert!(
                    db.find(key, seq).is_err(),
                    "{label}: db.find({key}) must fail closed instead of falling through to stale base"
                );
                assert_eq!(
                    db.get_seq_for_key(key, seq),
                    u64::MAX,
                    "{label}: get_seq_for_key({key}) must fail closed instead of returning stale base seq {base_seq}"
                );
            }
            assert!(
                db.scan("k_del", "k_hot", seq).is_err(),
                "{label}: db.scan must fail closed on corrupt L0 table"
            );
            assert!(
                db.scan_limit("k_del", "k_hot", seq, 10).is_err(),
                "{label}: db.scan_limit must fail closed on corrupt L0 table"
            );
            assert!(
                db.has_key_in_range_after_seq("k_del", "k_zzz", base_seq),
                "{label}: range conflict check must fail closed on corrupt L0 table"
            );
            assert!(
                db.compact_l0_to_l1().is_err(),
                "{label}: compact_l0_to_l1 must fail closed on corrupt L0 table"
            );
            assert!(
                db.run_garbage_collection().is_err(),
                "{label}: GC must fail closed on corrupt L0 table"
            );
        }

        // Also prove direct defense-in-depth when a generated base is replaced
        // in-memory with 4096 zero bytes: reads, GC, and L1->L2 all fail closed.
        let zero_base_path = dir.path().join("zero_4096_base.sst");
        std::fs::write(&zero_base_path, vec![0u8; 4096]).expect("write 4096 zero base");
        let zero_file = OpenOptions::new()
            .read(true)
            .open(&zero_base_path)
            .expect("open zero base");
        // SAFETY: Test-only immutable file mapped for the remainder of this test.
        let zero_mmap = Arc::new(unsafe { MmapOptions::new().map(&zero_file) }.expect("mmap"));
        db.update_roots(|r| StorageRoots {
            sstables: Arc::new(Vec::new()),
            base_mmap: zero_mmap.clone(),
            ..r.clone()
        });
        assert!(
            db.find("k_hot", db.get_seq()).is_err(),
            "4096B zeroed generated base must fail closed on point read"
        );
        assert!(
            db.run_garbage_collection().is_err(),
            "4096B zeroed generated base must fail closed on GC"
        );
    }
}
