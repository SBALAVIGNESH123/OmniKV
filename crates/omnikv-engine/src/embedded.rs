//! Stable embedded API for product integrations such as SketchLog.
//!
//! This module is intentionally a thin facade over the storage engine. It gives
//! applications a directory-based open path, namespace isolation, durability
//! helpers, and a compact key-value contract without depending on internal WAL,
//! manifest, SSTable, or compaction details.

use crate::backup::{
    create_backup_with_wal, create_encrypted_backup_with_wal, restore_backup,
    restore_encrypted_backup,
};
use crate::catalog::Catalog;
use crate::sql::parse_sql;
use crate::sql_exec::{ExecResult, SqlExecutor};
use crate::{CompactionPolicy, OmniError, OmniKV, WriteBatch};
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

const MANIFEST_FILE: &str = "manifest.json";
const WAL_FILE: &str = "wal.bin";
const NAMESPACE_PREFIX: &str = "__omnikv_ns";
const RANGE_END_SENTINEL: &str = "\u{10ffff}";

/// Error type exposed by the embedded API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmbeddedError {
    /// Storage-engine error.
    Storage(String),
    /// Filesystem or path error.
    Io(String),
    /// Backup or restore error.
    Backup(String),
    /// SQL parser or executor error.
    Sql(String),
    /// Namespace names must be stable and path-safe.
    InvalidNamespace(String),
    /// Keys must be non-empty.
    InvalidKey(String),
}

impl Display for EmbeddedError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(err) => write!(f, "storage error: {err}"),
            Self::Io(err) => write!(f, "I/O error: {err}"),
            Self::Backup(err) => write!(f, "backup error: {err}"),
            Self::Sql(err) => write!(f, "SQL error: {err}"),
            Self::InvalidNamespace(namespace) => {
                write!(f, "invalid embedded namespace: {namespace}")
            }
            Self::InvalidKey(key) => write!(f, "invalid embedded key: {key}"),
        }
    }
}

impl std::error::Error for EmbeddedError {}

impl From<OmniError> for EmbeddedError {
    fn from(value: OmniError) -> Self {
        Self::Storage(value.to_string())
    }
}

/// Configuration for opening an embedded OmniKV database.
#[derive(Debug, Clone)]
pub struct EmbeddedConfig {
    /// Directory containing the manifest, WAL, heap, and SSTable files.
    pub data_dir: PathBuf,
    /// Optional key namespace. Use this for product or tenant isolation.
    pub namespace: Option<String>,
    /// Optional compaction policy override.
    pub compaction_policy: Option<CompactionPolicy>,
    /// Optional timeout for SQL execution.
    pub query_timeout: Option<Duration>,
}

impl EmbeddedConfig {
    /// Create a config rooted at `data_dir`.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            namespace: None,
            compaction_policy: None,
            query_timeout: Some(Duration::from_secs(30)),
        }
    }

    /// Set the namespace for key-value operations.
    #[must_use]
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Set the compaction policy to apply after opening.
    #[must_use]
    pub fn compaction_policy(mut self, policy: CompactionPolicy) -> Self {
        self.compaction_policy = Some(policy);
        self
    }

    /// Set the SQL query timeout.
    #[must_use]
    pub fn query_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.query_timeout = timeout;
        self
    }
}

/// One namespaced key-value row returned by embedded scans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyValue {
    /// Application-visible key with the embedded namespace stripped.
    pub key: String,
    /// Stored string payload.
    pub value: String,
}

/// One mutation in an embedded write batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmbeddedWrite {
    /// Store a string payload.
    Put { key: String, value: String },
    /// Store a string payload with a TTL in seconds.
    PutWithTtl {
        key: String,
        value: String,
        ttl_secs: u64,
    },
    /// Delete a key.
    Delete { key: String },
}

/// Application-facing write batch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EmbeddedBatch {
    operations: Vec<EmbeddedWrite>,
}

impl EmbeddedBatch {
    /// Create an empty batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a put operation.
    #[must_use]
    pub fn put(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.operations.push(EmbeddedWrite::Put {
            key: key.into(),
            value: value.into(),
        });
        self
    }

    /// Add a put-with-TTL operation.
    #[must_use]
    pub fn put_with_ttl(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
        ttl_secs: u64,
    ) -> Self {
        self.operations.push(EmbeddedWrite::PutWithTtl {
            key: key.into(),
            value: value.into(),
            ttl_secs,
        });
        self
    }

    /// Add a delete operation.
    #[must_use]
    pub fn delete(mut self, key: impl Into<String>) -> Self {
        self.operations
            .push(EmbeddedWrite::Delete { key: key.into() });
        self
    }

    /// Number of operations in the batch.
    pub fn len(&self) -> usize {
        self.operations.len()
    }

    /// Whether the batch has no operations.
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }
}

/// Pinned MVCC snapshot. Dropping it releases the snapshot registration.
pub struct EmbeddedSnapshot {
    db: Arc<OmniKV>,
    sequence: u64,
}

impl EmbeddedSnapshot {
    /// Snapshot sequence number.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}

impl Drop for EmbeddedSnapshot {
    fn drop(&mut self) {
        self.db.unregister_snapshot(self.sequence);
    }
}

/// Result of executing SQL through the embedded facade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmbeddedSqlResult {
    /// Tabular query result.
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
    },
    /// Mutation result.
    Modified { count: usize, command: String },
    /// Statement completed without tabular output.
    Ok(String),
}

impl From<ExecResult> for EmbeddedSqlResult {
    fn from(value: ExecResult) -> Self {
        match value {
            ExecResult::Rows { columns, rows } => Self::Rows { columns, rows },
            ExecResult::Modified { count, command } => Self::Modified { count, command },
            ExecResult::Ok(message) => Self::Ok(message),
        }
    }
}

/// Lightweight operational stats for embedded callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbeddedStats {
    /// Latest committed sequence.
    pub sequence: u64,
    /// In-memory memtable entry count.
    pub memtable_size: usize,
    /// Approximate total record count visible to the engine.
    pub total_records: usize,
    /// Number of level-0 SSTables.
    pub l0_sstables: usize,
    /// Number of level-1 SSTables.
    pub l1_sstables: usize,
    /// Available pooled scan buffers.
    pub scan_buffer_pool_available: usize,
}

/// Stable embedded database handle.
#[derive(Clone)]
pub struct EmbeddedOmniKv {
    db: Arc<OmniKV>,
    catalog: Arc<Catalog>,
    data_dir: PathBuf,
    manifest_path: PathBuf,
    wal_path: PathBuf,
    namespace: Option<String>,
    query_timeout: Option<Duration>,
}

impl EmbeddedOmniKv {
    /// Open an embedded database from config.
    pub fn open(config: EmbeddedConfig) -> Result<Self, EmbeddedError> {
        let namespace = validate_namespace(config.namespace)?;
        std::fs::create_dir_all(&config.data_dir)
            .map_err(|err| EmbeddedError::Io(format!("create data dir: {err}")))?;

        let manifest_path = config.data_dir.join(MANIFEST_FILE);
        let wal_path = config.data_dir.join(WAL_FILE);
        let db = OmniKV::open(&path_string(&manifest_path), &path_string(&wal_path))?;

        if let Some(policy) = config.compaction_policy {
            db.set_compaction_policy(policy)?;
        }

        Ok(Self {
            catalog: Arc::new(Catalog::new(db.clone())),
            db,
            data_dir: config.data_dir,
            manifest_path,
            wal_path,
            namespace,
            query_timeout: config.query_timeout,
        })
    }

    /// Open a database in `data_dir` without a namespace.
    pub fn open_dir(data_dir: impl Into<PathBuf>) -> Result<Self, EmbeddedError> {
        Self::open(EmbeddedConfig::new(data_dir))
    }

    /// Create another handle over the same database with a different namespace.
    pub fn scoped(&self, namespace: impl Into<String>) -> Result<Self, EmbeddedError> {
        Ok(Self {
            db: self.db.clone(),
            catalog: self.catalog.clone(),
            data_dir: self.data_dir.clone(),
            manifest_path: self.manifest_path.clone(),
            wal_path: self.wal_path.clone(),
            namespace: validate_namespace(Some(namespace.into()))?,
            query_timeout: self.query_timeout,
        })
    }

    /// Data directory used by this embedded handle.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Manifest path used by this embedded handle.
    pub fn manifest_path(&self) -> &Path {
        &self.manifest_path
    }

    /// WAL path used by this embedded handle.
    pub fn wal_path(&self) -> &Path {
        &self.wal_path
    }

    /// Namespace used by key-value operations.
    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    /// Put a string payload.
    pub fn put(&self, key: &str, value: impl Into<String>) -> Result<u64, EmbeddedError> {
        let mut batch = WriteBatch::new();
        batch.set(&self.storage_key(key)?, value.into())?;
        Ok(self.db.commit_batch(&batch)?)
    }

    /// Put a string payload with a TTL in seconds.
    pub fn put_with_ttl(
        &self,
        key: &str,
        value: impl Into<String>,
        ttl_secs: u64,
    ) -> Result<u64, EmbeddedError> {
        let mut batch = WriteBatch::new();
        batch.set_with_ttl(&self.storage_key(key)?, value.into(), ttl_secs)?;
        Ok(self.db.commit_batch(&batch)?)
    }

    /// Get a key at the latest committed sequence.
    pub fn get(&self, key: &str) -> Result<Option<String>, EmbeddedError> {
        self.get_at_sequence(key, self.db.get_seq())
    }

    /// Get a key at a pinned snapshot.
    pub fn get_at(
        &self,
        key: &str,
        snapshot: &EmbeddedSnapshot,
    ) -> Result<Option<String>, EmbeddedError> {
        self.get_at_sequence(key, snapshot.sequence)
    }

    /// Delete a key.
    pub fn delete(&self, key: &str) -> Result<u64, EmbeddedError> {
        let mut batch = WriteBatch::new();
        batch.delete(&self.storage_key(key)?)?;
        Ok(self.db.commit_batch(&batch)?)
    }

    /// Commit an application-facing batch atomically.
    pub fn write_batch(&self, batch: EmbeddedBatch) -> Result<u64, EmbeddedError> {
        let mut storage_batch = WriteBatch::new();
        for operation in batch.operations {
            match operation {
                EmbeddedWrite::Put { key, value } => {
                    storage_batch.set(&self.storage_key(&key)?, value)?;
                }
                EmbeddedWrite::PutWithTtl {
                    key,
                    value,
                    ttl_secs,
                } => {
                    storage_batch.set_with_ttl(&self.storage_key(&key)?, value, ttl_secs)?;
                }
                EmbeddedWrite::Delete { key } => {
                    storage_batch.delete(&self.storage_key(&key)?)?;
                }
            }
        }
        Ok(self.db.commit_batch(&storage_batch)?)
    }

    /// Scan a half-open key range `[start, end)`.
    pub fn scan(
        &self,
        start: &str,
        end: &str,
        limit: Option<usize>,
    ) -> Result<Vec<KeyValue>, EmbeddedError> {
        let raw_start = self.storage_key(start)?;
        let raw_end = self.storage_key(end)?;
        self.scan_raw_range(&raw_start, &raw_end, limit)
    }

    /// Scan keys with an application-visible prefix.
    pub fn scan_prefix(
        &self,
        prefix: &str,
        limit: Option<usize>,
    ) -> Result<Vec<KeyValue>, EmbeddedError> {
        let raw_start = self.storage_key(prefix)?;
        let raw_end = format!("{raw_start}{RANGE_END_SENTINEL}");
        self.scan_raw_range(&raw_start, &raw_end, limit)
    }

    /// Scan all keys visible inside this handle's namespace.
    pub fn scan_all(&self, limit: Option<usize>) -> Result<Vec<KeyValue>, EmbeddedError> {
        if let Some(prefix) = self.namespace_prefix() {
            let raw_end = format!("{prefix}{RANGE_END_SENTINEL}");
            self.scan_raw_range(&prefix, &raw_end, limit)
        } else {
            self.scan_raw_range("", RANGE_END_SENTINEL, limit)
        }
    }

    /// Pin a snapshot for repeatable reads.
    pub fn snapshot(&self) -> EmbeddedSnapshot {
        EmbeddedSnapshot {
            db: self.db.clone(),
            sequence: self.db.snapshot(),
        }
    }

    /// Flush and compact active storage levels.
    pub fn compact(&self) -> Result<(), EmbeddedError> {
        self.db.compact_sstables()?;
        self.db.compact_l0_to_l1()?;
        Ok(())
    }

    /// Sync all active storage files.
    pub fn sync(&self) -> Result<(), EmbeddedError> {
        Ok(self.db.sync_all()?)
    }

    /// Create a plain backup archive that includes the WAL.
    pub fn create_backup(&self, output_path: impl AsRef<Path>) -> Result<PathBuf, EmbeddedError> {
        let output_path = output_path.as_ref().to_path_buf();
        create_backup_with_wal(
            &self.db,
            &path_string(&self.manifest_path),
            &path_string(&self.wal_path),
            &path_string(&output_path),
        )
        .map_err(EmbeddedError::Backup)?;
        Ok(output_path)
    }

    /// Create an encrypted backup archive that includes the WAL.
    pub fn create_encrypted_backup(
        &self,
        output_path: impl AsRef<Path>,
        passphrase: &str,
    ) -> Result<PathBuf, EmbeddedError> {
        let output_path = output_path.as_ref().to_path_buf();
        create_encrypted_backup_with_wal(
            &self.db,
            &path_string(&self.manifest_path),
            &path_string(&self.wal_path),
            &path_string(&output_path),
            passphrase,
        )
        .map_err(EmbeddedError::Backup)?;
        Ok(output_path)
    }

    /// Restore a plain backup into `data_dir` and open it.
    pub fn restore_from_backup(
        backup_path: impl AsRef<Path>,
        data_dir: impl Into<PathBuf>,
    ) -> Result<Self, EmbeddedError> {
        let data_dir = data_dir.into();
        std::fs::create_dir_all(&data_dir)
            .map_err(|err| EmbeddedError::Io(format!("create restore dir: {err}")))?;
        restore_backup(&path_string(backup_path.as_ref()), &path_string(&data_dir))
            .map_err(EmbeddedError::Backup)?;
        Self::open_dir(data_dir)
    }

    /// Restore an encrypted backup into `data_dir` and open it.
    pub fn restore_from_encrypted_backup(
        backup_path: impl AsRef<Path>,
        data_dir: impl Into<PathBuf>,
        passphrase: &str,
    ) -> Result<Self, EmbeddedError> {
        let data_dir = data_dir.into();
        std::fs::create_dir_all(&data_dir)
            .map_err(|err| EmbeddedError::Io(format!("create restore dir: {err}")))?;
        restore_encrypted_backup(
            &path_string(backup_path.as_ref()),
            &path_string(&data_dir),
            passphrase,
        )
        .map_err(EmbeddedError::Backup)?;
        Self::open_dir(data_dir)
    }

    /// Execute SQL against the embedded engine.
    ///
    /// Key-value namespaces do not rewrite SQL table/catalog storage. Use SQL
    /// for engine-global analytical tables, and use `put`/`scan_prefix` for
    /// namespaced SketchLog event and sketch-state payloads.
    pub fn execute_sql(&self, sql: &str) -> Result<EmbeddedSqlResult, EmbeddedError> {
        let statement = parse_sql(sql).map_err(EmbeddedError::Sql)?;
        let mut executor = SqlExecutor::new(self.db.clone(), self.catalog.clone());
        executor.set_query_timeout(self.query_timeout);
        executor
            .execute(&statement)
            .map(EmbeddedSqlResult::from)
            .map_err(EmbeddedError::Sql)
    }

    /// Return lightweight operational stats for dashboards and health checks.
    pub fn stats(&self) -> EmbeddedStats {
        EmbeddedStats {
            sequence: self.db.get_seq(),
            memtable_size: self.db.memtable_size(),
            total_records: self.db.total_records(),
            l0_sstables: self.db.sstable_count(),
            l1_sstables: self.db.l1_sstable_count(),
            scan_buffer_pool_available: self.db.scan_buffer_pool_available(),
        }
    }

    fn get_at_sequence(&self, key: &str, sequence: u64) -> Result<Option<String>, EmbeddedError> {
        Ok(self.db.find(&self.storage_key(key)?, sequence)?)
    }

    fn scan_raw_range(
        &self,
        start: &str,
        end: &str,
        limit: Option<usize>,
    ) -> Result<Vec<KeyValue>, EmbeddedError> {
        let namespace_prefix = self.namespace_prefix();
        let mut rows = Vec::new();
        for (key, value) in self.db.scan(start, end, self.db.get_seq())? {
            if let Some(prefix) = &namespace_prefix {
                if let Some(stripped) = key.strip_prefix(prefix) {
                    rows.push(KeyValue {
                        key: stripped.to_string(),
                        value,
                    });
                }
            } else {
                rows.push(KeyValue { key, value });
            }

            if limit.is_some_and(|limit| rows.len() >= limit) {
                break;
            }
        }
        Ok(rows)
    }

    fn storage_key(&self, key: &str) -> Result<String, EmbeddedError> {
        if key.is_empty() {
            return Err(EmbeddedError::InvalidKey("key must not be empty".into()));
        }
        Ok(match self.namespace_prefix() {
            Some(prefix) => format!("{prefix}{key}"),
            None => key.to_string(),
        })
    }

    fn namespace_prefix(&self) -> Option<String> {
        self.namespace
            .as_ref()
            .map(|namespace| format!("{NAMESPACE_PREFIX}:{namespace}:"))
    }
}

fn validate_namespace(namespace: Option<String>) -> Result<Option<String>, EmbeddedError> {
    let Some(namespace) = namespace else {
        return Ok(None);
    };
    if namespace.is_empty()
        || !namespace
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | ':'))
    {
        return Err(EmbeddedError::InvalidNamespace(namespace));
    }
    Ok(Some(namespace))
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
