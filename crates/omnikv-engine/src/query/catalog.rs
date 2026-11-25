//! Table Catalog — Schema storage for multi-table SQL support
//!
//! Stores table definitions (columns, types) inside OmniKV itself
//! using the key prefix `\x00CATALOG\x00`.

use crate::secondary_index::IndexManager;
use crate::{OmniError, OmniKV, WriteBatch};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const CATALOG_PREFIX: &str = "\x00CATALOG\x00";
const DROPPING_TABLE_PREFIX: &str = "\x00DROPPING_TABLE\x00";
static NEXT_TABLE_UID_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ColumnType {
    Text,
    Integer,
    Float,
    Boolean,
    Timestamp,
    Json,
}

impl FromStr for ColumnType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "TEXT" | "VARCHAR" | "STRING" | "CHAR" => Ok(Self::Text),
            "INT" | "INTEGER" | "BIGINT" | "SMALLINT" => Ok(Self::Integer),
            "FLOAT" | "DOUBLE" | "REAL" | "NUMERIC" | "DECIMAL" => Ok(Self::Float),
            "BOOL" | "BOOLEAN" => Ok(Self::Boolean),
            "TIMESTAMP" | "DATETIME" | "DATE" => Ok(Self::Timestamp),
            "JSON" | "JSONB" => Ok(Self::Json),
            _ => Err(format!("Unknown type: {}", s)),
        }
    }
}

impl ColumnType {
    pub fn pg_oid(&self) -> i32 {
        match self {
            Self::Text => 25,        // TEXT
            Self::Integer => 20,     // INT8
            Self::Float => 701,      // FLOAT8
            Self::Boolean => 16,     // BOOL
            Self::Timestamp => 1114, // TIMESTAMP
            Self::Json => 114,       // JSON
        }
    }

    /// A numeric column orders by value, so "7" and "007" are one key.
    pub fn is_numeric(&self) -> bool {
        matches!(self, Self::Integer | Self::Float)
    }

    /// Canonical form for a value of this type: a numeric payload prints as
    /// its number, anything else is kept verbatim. `1.50` and `1.5` parse to
    /// the same f64, so the trailing zero is not information the type holds;
    /// keeping typed digits is a `NUMERIC` type's job, which this engine does
    /// not have.
    pub fn canonicalize(&self, value: &str) -> String {
        match self {
            Self::Integer => value
                .parse::<i128>()
                .map(|n| n.to_string())
                .unwrap_or_else(|_| value.to_string()),
            Self::Float => value
                .parse::<f64>()
                .map(|f| {
                    let normalized = if f == 0.0 { 0.0 } else { f };
                    normalized.to_string()
                })
                .unwrap_or_else(|_| value.to_string()),
            _ => value.to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub col_type: ColumnType,
    pub nullable: bool,
    pub primary_key: bool,
    pub default: Option<String>,
}

/// How a table's rows encode a SQL NULL on disk.
///
/// A stored "NULL" is a SQL NULL under [`Legacy`](Self::Legacy) and the
/// user's literal text under [`Typed`](Self::Typed); the bytes are identical,
/// so the table records which one its rows use and the decoders consult it.
/// Manifests without the field read back as [`Legacy`](Self::Legacy).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum RowFormat {
    /// NULL is the literal string "NULL"; older databases only.
    #[default]
    Legacy,
    /// NULL is a JSON `null`; "NULL" is the user's literal text.
    Typed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDef {
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: String,
    pub created_at: u64,
    /// How this table's rows encode NULL. Defaults to legacy for tables
    /// deserialized from a manifest written before the field existed.
    #[serde(default)]
    pub row_format: RowFormat,
    /// Durable per-creation table identity so drop-and-recreate with the same
    /// table name produces a distinct identity for schema migration receipts.
    #[serde(default)]
    pub table_uid: String,
}

impl TableDef {
    pub(crate) fn generate_uid() -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let seq = NEXT_TABLE_UID_COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("{nanos:x}_{seq:x}")
    }

    pub(crate) fn durable_identity(&self) -> String {
        format!("{}:{}:{}", self.table_uid, self.created_at, self.name)
    }

    pub fn row_prefix(&self) -> String {
        format!("\x00T\x00{}\x00", self.name)
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }
}

/// The table catalog — stored inside OmniKV itself.
pub struct Catalog {
    db: Arc<OmniKV>,
    cache: std::sync::RwLock<HashMap<String, TableDef>>,
    pending_drop_lock: std::sync::Mutex<()>,
    index_manager: Arc<IndexManager>,
}

impl Catalog {
    pub fn new(db: Arc<OmniKV>) -> Self {
        let index_manager = Arc::new(IndexManager::new(db.clone()));
        let cat = Self {
            db,
            cache: std::sync::RwLock::new(HashMap::new()),
            pending_drop_lock: std::sync::Mutex::new(()),
            index_manager,
        };
        cat.load_all(None);
        cat
    }

    /// Loads table definitions as they were at `seq`. A transaction reading
    /// at an older snapshot must also see the older schema: a table another
    /// connection migrated in the meantime would otherwise decode this
    /// transaction's rows under the wrong NULL encoding.
    pub fn at_snapshot(db: Arc<OmniKV>, seq: u64) -> Self {
        let index_manager = Arc::new(IndexManager::new(db.clone()));
        let cat = Self {
            db,
            cache: std::sync::RwLock::new(HashMap::new()),
            pending_drop_lock: std::sync::Mutex::new(()),
            index_manager,
        };
        cat.load_all(Some(seq));
        cat
    }

    fn load_all(&self, seq: Option<u64>) {
        let seq = seq.unwrap_or_else(|| self.db.get_seq());
        if let Ok(results) = self.db.scan_prefix(CATALOG_PREFIX, seq) {
            let mut cache = self
                .cache
                .write()
                .expect("catalog cache RwLock poisoned: fatal invariant");
            for (_key, value) in results {
                if let Ok(table) = serde_json::from_str::<TableDef>(&value) {
                    cache.insert(table.name.to_lowercase(), table);
                }
            }
        }
    }

    /// Storage key for a durable large-table `DROP-IN-PROGRESS` marker.
    pub fn dropping_table_key(name: &str) -> String {
        format!("{}{}", DROPPING_TABLE_PREFIX, name.to_lowercase())
    }

    /// Drains any interrupted large-table drop for `name` before the name can
    /// be recreated or reused. Safe and idempotent to call when no drop is pending.
    pub fn finish_pending_drop(&self, name: &str) -> Result<(), String> {
        let _drop_guard = self
            .pending_drop_lock
            .lock()
            .map_err(|_| "catalog pending-drop lock poisoned".to_string())?;
        self.finish_pending_drop_locked(name)
    }

    fn finish_pending_drop_locked(&self, name: &str) -> Result<(), String> {
        let dropping_key = Self::dropping_table_key(name);
        let Some(raw) = self
            .db
            .find(&dropping_key, self.db.get_seq())
            .map_err(|e| format!("{e:?}"))?
        else {
            return Ok(());
        };
        let table: TableDef = serde_json::from_str(&raw)
            .map_err(|e| format!("Invalid pending DROP TABLE record '{dropping_key}': {e}"))?;
        self.drain_dropping_table_entry(&dropping_key, &table)
    }

    /// Completes all interrupted large-table drops recorded under
    /// `\x00DROPPING_TABLE\x00`, returning the number of pending drops finalized.
    pub fn recover_pending_drops(&self) -> Result<usize, String> {
        let _drop_guard = self
            .pending_drop_lock
            .lock()
            .map_err(|_| "catalog pending-drop lock poisoned".to_string())?;
        let seq = self.db.get_seq();
        let pending = self
            .db
            .scan_prefix(DROPPING_TABLE_PREFIX, seq)
            .map_err(|e| format!("{e:?}"))?;
        let mut recovered = 0usize;
        for (dropping_key, raw) in pending {
            let table: TableDef = serde_json::from_str(&raw)
                .map_err(|e| format!("Invalid pending DROP TABLE record '{dropping_key}': {e}"))?;
            self.drain_dropping_table_entry(&dropping_key, &table)?;
            recovered += 1;
        }
        Ok(recovered)
    }

    fn drain_dropping_table_entry(
        &self,
        dropping_key: &str,
        table: &TableDef,
    ) -> Result<(), String> {
        let chunk_cap = WriteBatch::MAX_OPS.saturating_sub(4).max(1);
        let row_prefix = table.row_prefix();
        loop {
            let seq = self.db.get_seq();
            let chunk = self
                .db
                .scan_prefix_limit(&row_prefix, seq, chunk_cap.saturating_add(1))
                .map_err(|e| format!("{e:?}"))?;
            if chunk.is_empty() {
                break;
            }
            let has_more = chunk.len() > chunk_cap;
            let mut batch = WriteBatch::new();
            for (key, _) in chunk.into_iter().take(chunk_cap) {
                batch.delete(&key).map_err(|e| format!("{e:?}"))?;
            }
            self.db.commit_batch(&batch).map_err(|e| format!("{e:?}"))?;
            if has_more {
                crate::failpoints::maybe_fail("catalog::drop_table_after_partial_chunk")
                    .map_err(|e| format!("catalog drop_table failure point: {e}"))?;
            }
        }

        self.index_manager
            .cleanup_retired_indexes_for_collection(
                &table.name,
                crate::secondary_index::IndexNamespace::Sql,
            )
            .map_err(|e| format!("{e:?}"))?;

        let stage_entries = crate::schema::SchemaManager::active_rename_stages_for_namespace(
            &self.db,
            &table.name,
            crate::secondary_index::IndexNamespace::Sql,
            self.db.get_seq(),
        )
        .map_err(|e| format!("{e:?}"))?;
        for (stage_key, _) in stage_entries {
            let row_marker_prefix =
                crate::schema::SchemaManager::rename_row_stage_prefix(&stage_key);
            loop {
                let marker_seq = self.db.get_seq();
                let markers = self
                    .db
                    .scan_prefix_limit(&row_marker_prefix, marker_seq, chunk_cap.saturating_add(1))
                    .map_err(|e| format!("{e:?}"))?;
                if markers.is_empty() {
                    let mut clear_stage = WriteBatch::new();
                    clear_stage
                        .delete(&stage_key)
                        .map_err(|e| format!("{e:?}"))?;
                    self.db
                        .commit_batch(&clear_stage)
                        .map_err(|e| format!("{e:?}"))?;
                    break;
                }
                let has_more = markers.len() > chunk_cap;
                let mut batch = WriteBatch::new();
                for (marker_key, _) in markers.into_iter().take(chunk_cap) {
                    batch.delete(&marker_key).map_err(|e| format!("{e:?}"))?;
                }
                if !has_more {
                    batch.delete(&stage_key).map_err(|e| format!("{e:?}"))?;
                    self.db.commit_batch(&batch).map_err(|e| format!("{e:?}"))?;
                    break;
                }
                self.db.commit_batch(&batch).map_err(|e| format!("{e:?}"))?;
            }
        }

        let mut finalize = WriteBatch::new();
        finalize
            .delete(dropping_key)
            .map_err(|e| format!("{e:?}"))?;
        self.db
            .commit_batch(&finalize)
            .map_err(|e| format!("{e:?}"))?;
        Ok(())
    }

    pub fn create_table(&self, mut table: TableDef) -> Result<(), String> {
        let _drop_guard = self
            .pending_drop_lock
            .lock()
            .map_err(|_| "catalog pending-drop lock poisoned".to_string())?;
        let name_lower = table.name.to_lowercase();
        if table.table_uid.is_empty() {
            table.table_uid = TableDef::generate_uid();
        }
        {
            let cache = self
                .cache
                .read()
                .expect("catalog cache RwLock poisoned: fatal invariant");
            if cache.contains_key(&name_lower) {
                return Err(format!("Table '{}' already exists", table.name));
            }
        }
        // Finish any interrupted large drop for this table name before writing
        // a new catalog entry so orphaned rows/indexes can never resurrect.
        self.finish_pending_drop_locked(&table.name)?;

        let key = format!("{}{}", CATALOG_PREFIX, name_lower);
        let value = serde_json::to_string(&table).map_err(|e| format!("Serialize: {}", e))?;

        let mut batch = WriteBatch::new();
        batch.set(&key, value).map_err(|e| format!("{:?}", e))?;
        self.db
            .commit_batch(&batch)
            .map_err(|e| format!("{:?}", e))?;

        let mut cache = self
            .cache
            .write()
            .expect("catalog cache RwLock poisoned: fatal invariant");
        cache.insert(name_lower, table);
        Ok(())
    }

    pub fn drop_table(&self, name: &str) -> Result<(), String> {
        let _drop_guard = self
            .pending_drop_lock
            .lock()
            .map_err(|_| "catalog pending-drop lock poisoned".to_string())?;
        let name_lower = name.to_lowercase();
        let table = {
            let cache = self
                .cache
                .read()
                .expect("catalog cache RwLock poisoned: fatal invariant");
            cache
                .get(&name_lower)
                .cloned()
                .ok_or_else(|| format!("Table '{}' does not exist", name))?
        };

        let seq = self.db.get_seq();
        let crate::secondary_index::CollectionIndexDropPlan {
            active: active_indexes,
            retired: retired_indexes,
        } = self
            .index_manager
            .plan_drop_indexes_for_collection_at(&table.name, seq)
            .map_err(|e| format!("{e:?}"))?;
        let stage_entries = crate::schema::SchemaManager::active_rename_stages_for_namespace(
            &self.db,
            &table.name,
            crate::secondary_index::IndexNamespace::Sql,
            seq,
        )
        .map_err(|e| format!("{e:?}"))?;

        let cat_key = format!("{}{}", CATALOG_PREFIX, name_lower);
        let fixed_meta_ops = 1usize
            .saturating_add(active_indexes.len().saturating_mul(2))
            .saturating_add(retired_indexes.len())
            .saturating_add(stage_entries.len());

        // Fast path: if all rows, index entries, rename markers, and metadata
        // fit in a single WriteBatch, delete everything atomically in one commit.
        let mut fits_single_batch = fixed_meta_ops <= WriteBatch::MAX_OPS;
        let mut single_batch_data_keys: Vec<String> = Vec::new();
        if fits_single_batch {
            let mut remaining = WriteBatch::MAX_OPS.saturating_sub(fixed_meta_ops);
            let row_chunk = self
                .db
                .scan_prefix_limit(&table.row_prefix(), seq, remaining.saturating_add(1))
                .map_err(|e| format!("{e:?}"))?;
            if row_chunk.len() > remaining {
                fits_single_batch = false;
            } else {
                remaining = remaining.saturating_sub(row_chunk.len());
                single_batch_data_keys.extend(row_chunk.into_iter().map(|(k, _)| k));
            }

            if fits_single_batch {
                for id in active_indexes
                    .iter()
                    .map(|d| d.id)
                    .chain(retired_indexes.iter().map(|(_, d)| d.id))
                {
                    let idx_prefix = IndexManager::index_entry_prefix_for_id(id);
                    let chunk = self
                        .db
                        .scan_prefix_limit(&idx_prefix, seq, remaining.saturating_add(1))
                        .map_err(|e| format!("{e:?}"))?;
                    if chunk.len() > remaining {
                        fits_single_batch = false;
                        break;
                    }
                    remaining = remaining.saturating_sub(chunk.len());
                    single_batch_data_keys.extend(chunk.into_iter().map(|(k, _)| k));
                }
            }

            if fits_single_batch {
                for (stage_key, _) in &stage_entries {
                    let row_marker_prefix =
                        crate::schema::SchemaManager::rename_row_stage_prefix(stage_key);
                    let chunk = self
                        .db
                        .scan_prefix_limit(&row_marker_prefix, seq, remaining.saturating_add(1))
                        .map_err(|e| format!("{e:?}"))?;
                    if chunk.len() > remaining {
                        fits_single_batch = false;
                        break;
                    }
                    remaining = remaining.saturating_sub(chunk.len());
                    single_batch_data_keys.extend(chunk.into_iter().map(|(k, _)| k));
                }
            }
        }

        if fits_single_batch {
            let mut batch = WriteBatch::new();
            for key in &single_batch_data_keys {
                batch.delete(key).map_err(|e| format!("{e:?}"))?;
            }
            for def in &active_indexes {
                let (idx_cat_key, idx_ready_key) = IndexManager::index_meta_keys_for_def(def);
                batch.delete(&idx_cat_key).map_err(|e| format!("{e:?}"))?;
                batch.delete(&idx_ready_key).map_err(|e| format!("{e:?}"))?;
            }
            for (ret_key, _) in &retired_indexes {
                batch.delete(ret_key).map_err(|e| format!("{e:?}"))?;
            }
            for (stage_key, _) in &stage_entries {
                batch.delete(stage_key).map_err(|e| format!("{e:?}"))?;
            }
            batch.delete(&cat_key).map_err(|e| format!("{e:?}"))?;
            self.db.commit_batch(&batch).map_err(|e| format!("{e:?}"))?;
            let mut cache = self
                .cache
                .write()
                .expect("catalog cache RwLock poisoned: fatal invariant");
            cache.remove(&name_lower);
            return Ok(());
        }

        // Large-table path: atomically remove the table from the catalog,
        // retire its active SQL indexes, and persist a durable DROP-IN-PROGRESS
        // marker BEFORE deleting any row chunks. A crash at any subsequent point
        // leaves the table hidden and recoverable via `recover_pending_drops`,
        // `DROP TABLE IF EXISTS`, or `CREATE TABLE` with the same name.
        let cutover_ops = 2usize.saturating_add(active_indexes.len().saturating_mul(3));
        if cutover_ops > WriteBatch::MAX_OPS {
            return Err(format!(
                "DROP TABLE '{}' has too many active SQL indexes ({}) for an atomic cutover batch; drop indexes first",
                table.name,
                active_indexes.len()
            ));
        }
        let dropping_key = Self::dropping_table_key(&table.name);
        let dropping_value = serde_json::to_string(&table)
            .map_err(|e| format!("Serialize pending drop table: {e}"))?;
        let mut cutover = WriteBatch::new();
        cutover.delete(&cat_key).map_err(|e| format!("{e:?}"))?;
        let mut write_keys = vec![cat_key];
        let retired_cat_keys =
            IndexManager::append_retire_sql_indexes_to_batch(&active_indexes, &mut cutover)
                .map_err(|e| format!("{e:?}"))?;
        write_keys.extend(retired_cat_keys);
        cutover
            .set(&dropping_key, dropping_value)
            .map_err(|e| format!("{e:?}"))?;
        let cutover_seq = self
            .db
            .commit_batch(&cutover)
            .map_err(|e| format!("{e:?}"))?;
        self.db.ssi_history().record_committed(
            0,
            cutover_seq,
            &crate::transaction::SsiCommitRecord {
                txn_id: 0,
                write_keys,
                read_keys: Vec::new(),
                read_ranges: Vec::new(),
            },
        );
        {
            let mut cache = self
                .cache
                .write()
                .expect("catalog cache RwLock poisoned: fatal invariant");
            cache.remove(&name_lower);
        }

        self.drain_dropping_table_entry(&dropping_key, &table)
    }

    /// The storage key and serialized TableDef a CREATE TABLE writes —
    /// the transactional executor stages exactly this pair so the SSI
    /// engine applies it at COMMIT and a ROLLBACK discards it.
    pub fn staged_create_entry(table: &TableDef) -> Result<(String, String), String> {
        let key = Self::catalog_key(&table.name);
        let value = serde_json::to_string(table).map_err(|e| format!("Serialize: {}", e))?;
        Ok((key, value))
    }

    /// The storage key a DROP TABLE deletes from the catalog.
    pub fn staged_drop_key(name: &str) -> String {
        Self::catalog_key(name)
    }

    /// The single key under which a table's definition is stored.
    pub fn catalog_key(name: &str) -> String {
        format!("{}{}", CATALOG_PREFIX, name.to_lowercase())
    }

    /// Applies a transaction's staged catalog writes over the loaded
    /// cache, so this statement sees the block's own uncommitted CREATEs
    /// and DROPs (read-your-own-writes for DDL). The cache instance is
    /// per-statement, so no other connection is affected; the staged
    /// writes themselves only become durable at COMMIT.
    pub fn apply_txn_overlay(
        &self,
        staged: &std::collections::HashMap<String, (Option<String>, u64)>,
    ) {
        let mut cache = self
            .cache
            .write()
            .expect("catalog cache RwLock poisoned: fatal invariant");
        for (key, (value, _ttl)) in staged {
            let Some(name) = key.strip_prefix(CATALOG_PREFIX) else {
                continue;
            };
            match value {
                Some(serialized) => {
                    if let Ok(table) = serde_json::from_str::<TableDef>(serialized) {
                        cache.insert(table.name.to_lowercase(), table);
                    }
                }
                None => {
                    cache.remove(name);
                }
            }
        }
    }

    /// Inserts a staged CREATE's table into this statement's cache so the
    /// very statement that created it (and later statements in the same
    /// block, via the overlay) can use it.
    pub fn insert_staged(&self, table: TableDef) {
        let mut cache = self
            .cache
            .write()
            .expect("catalog cache RwLock poisoned: fatal invariant");
        cache.insert(table.name.to_lowercase(), table);
    }

    /// Removes a staged DROP's table from this statement's cache, mirroring
    /// the staged delete for the block's own later statements.
    pub fn remove_staged(&self, name: &str) {
        let mut cache = self
            .cache
            .write()
            .expect("catalog cache RwLock poisoned: fatal invariant");
        cache.remove(&name.to_lowercase());
    }

    pub fn get_table(&self, name: &str) -> Option<TableDef> {
        let cache = self
            .cache
            .read()
            .expect("catalog cache RwLock poisoned: fatal invariant");
        cache.get(&name.to_lowercase()).cloned()
    }

    /// Shared index maintenance/catalog view for SQL planning and DML.
    pub fn index_manager(&self) -> Arc<IndexManager> {
        self.index_manager.clone()
    }

    pub fn list_tables(&self) -> Vec<String> {
        let cache = self
            .cache
            .read()
            .expect("catalog cache RwLock poisoned: fatal invariant");
        cache.keys().cloned().collect()
    }

    /// Lists the catalog as of a caller-selected MVCC sequence. Long-lived
    /// executors may have a cache from before a concurrent CREATE or DROP;
    /// SHOW TABLES must report the statement's persisted view instead of that
    /// stale cache. An unreadable payload or malformed definition fails the
    /// entire listing rather than silently omitting tables.
    pub fn list_tables_at(&self, seq: u64) -> Result<Vec<String>, String> {
        self.list_tables_with_overlay_at(seq, None)
    }

    /// Lists a transaction's snapshot with its own staged catalog writes.
    /// Decode after merging so staged CREATE/DROP entries take precedence over
    /// their persisted definitions, while errors cannot be hidden by the cache.
    pub(crate) fn list_tables_with_overlay_at(
        &self,
        seq: u64,
        overlay: Option<&HashMap<String, Option<String>>>,
    ) -> Result<Vec<String>, String> {
        let mut entries: BTreeMap<_, _> = self
            .db
            .scan_prefix(CATALOG_PREFIX, seq)
            .map_err(|e| format!("Catalog scan failed: {e:?}"))?
            .into_iter()
            .collect();
        if let Some(overlay) = overlay {
            for (key, value) in overlay {
                if !key.starts_with(CATALOG_PREFIX) {
                    continue;
                }
                match value {
                    Some(value) => {
                        entries.insert(key.clone(), value.clone());
                    }
                    None => {
                        entries.remove(key);
                    }
                }
            }
        }
        entries
            .into_iter()
            .map(|(key, value)| {
                serde_json::from_str::<TableDef>(&value)
                    .map(|table| table.name.to_lowercase())
                    .map_err(|e| format!("Catalog entry {key:?} is invalid: {e}"))
            })
            .collect()
    }
}
