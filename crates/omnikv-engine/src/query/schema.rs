//! Online Schema Evolution Engine
//!
//! Applies schema changes incrementally while the database continues to
//! serve reads and writes.
//!
//! ## Features
//!
//! - **Versioned migrations**: Numbered, ordered, replayable schema changes
//! - **Online index creation**: Builds indexes in background batches
//! - **Online index deletion**: Drops indexes without blocking operations
//! - **Migration log**: Full audit trail persisted in OmniKV
//! - **Idempotent replay**: Safe to re-run migrations after crash recovery
//! - **Rollback support**: Each migration defines an `up` and `down` action
//!
//! ## Key Design
//!
//! ```text
//! SchemaManager
//!   ├── MigrationLog    (persisted: tracks which migrations have been applied)
//!   ├── MigrationRunner (executes migrations online, in batches)
//!   └── IndexManager    (creates/drops indexes as part of migrations)
//! ```

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::secondary_index::{
    IndexFieldType, IndexManager, IndexNamespace, collection_row_ranges_for_namespace,
};
use crate::{OmniError, OmniKV, WriteBatch};

static NEXT_MIGRATION_ATTEMPT_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Prefix for schema metadata stored in OmniKV.
const SCHEMA_PREFIX: &str = "\x00SCHEMA\x00";
/// Key for the current schema version.
const VERSION_KEY: &str = "\x00SCHEMA\x00__version__";
/// Prefix for individual migration records.
const MIGRATION_PREFIX: &str = "\x00SCHEMA\x00migration:";
const MIGRATION_LOCK_NAME: &str = "\x00SCHEMA\x00__migration_lock__";
/// Maximum convergence passes for lock-free online Document schema migrations
/// when concurrent `db.commit_batch` writers race with field backfill/rename.
const MAX_DOC_CONVERGENCE_ROUNDS: usize = 32;

#[cfg(test)]
type DocMigrationHookFn = Box<dyn FnMut(&str) + Send>;

#[cfg(test)]
static DOC_MIGRATION_HOOK: std::sync::Mutex<Option<DocMigrationHookFn>> =
    std::sync::Mutex::new(None);

#[cfg(test)]
fn run_doc_migration_hook(point: &str) {
    let mut guard = DOC_MIGRATION_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(hook) = guard.as_mut() {
        hook(point);
    }
}

/// Schema version number.
pub type Version = u64;

/// The current state of a migration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MigrationState {
    /// Not yet applied.
    Pending,
    /// Currently being applied (background indexing in progress).
    InProgress { progress_pct: u8 },
    /// Successfully applied.
    Applied,
    /// Rolled back.
    RolledBack,
    /// Failed (with error message).
    Failed(String),
}

/// A single schema change operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchemaOp {
    /// Create a secondary index.
    CreateIndex {
        name: String,
        collection: String,
        fields: Vec<(String, IndexFieldType)>,
        unique: bool,
    },
    /// Drop a secondary index.
    DropIndex { name: String },
    /// Add a default value for a new field in existing documents.
    AddField {
        collection: String,
        field_name: String,
        default_value: serde_json::Value,
    },
    /// Remove a field from all documents in a collection.
    RemoveField {
        collection: String,
        field_name: String,
    },
    /// Rename a field in all documents in a collection.
    RenameField {
        collection: String,
        old_name: String,
        new_name: String,
    },
    /// Explicitly scope a schema operation to [`IndexNamespace::Sql`] or
    /// [`IndexNamespace::Document`] (never [`IndexNamespace::Both`]), allowing
    /// migrations to disambiguate when a SQL table and a document collection
    /// share the same name.
    InNamespace {
        namespace: IndexNamespace,
        op: Box<SchemaOp>,
    },
}

impl SchemaOp {
    /// Wraps this schema operation with an explicit [`IndexNamespace::Sql`] or
    /// [`IndexNamespace::Document`] target.
    #[must_use]
    pub fn in_namespace(self, namespace: IndexNamespace) -> Self {
        Self::InNamespace {
            namespace,
            op: Box::new(self),
        }
    }

    fn unwrap_namespace(&self) -> Result<(&SchemaOp, Option<IndexNamespace>), OmniError> {
        match self {
            SchemaOp::InNamespace { namespace, op } => {
                if *namespace == IndexNamespace::Both {
                    return Err(OmniError::IoError(
                        "schema migration explicit namespace must be Sql or Document, not Both"
                            .into(),
                    ));
                }
                if matches!(op.as_ref(), SchemaOp::InNamespace { .. }) {
                    return Err(OmniError::IoError(
                        "nested SchemaOp::InNamespace wrappers are not supported".into(),
                    ));
                }
                Ok((op.as_ref(), Some(*namespace)))
            }
            other => Ok((other, None)),
        }
    }
}

/// A versioned migration — defines what to do (up) and how to undo (down).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Migration {
    /// Version number (monotonically increasing).
    pub version: Version,
    /// Human-readable description.
    pub description: String,
    /// Operations to apply (in order).
    pub up: Vec<SchemaOp>,
    /// Operations to undo (in reverse order).
    pub down: Vec<SchemaOp>,
}

/// Persisted record of a migration's execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationRecord {
    pub version: Version,
    pub description: String,
    pub state: MigrationState,
    pub applied_at: u64, // Unix timestamp millis
}

/// The Schema Manager — orchestrates online schema evolution.
pub struct SchemaManager {
    db: Arc<OmniKV>,
    index_manager: Arc<IndexManager>,
}

impl SchemaManager {
    /// Creates a new SchemaManager.
    pub fn new(db: Arc<OmniKV>, index_manager: Arc<IndexManager>) -> Self {
        Self { db, index_manager }
    }

    /// Returns the current schema version.
    pub fn current_version(&self) -> Result<Version, OmniError> {
        let seq = self.db.get_seq();
        match self.db.find(VERSION_KEY, seq)? {
            Some(v) => v
                .parse::<Version>()
                .map_err(|e| OmniError::IoError(format!("Invalid schema version: {}", e))),
            None => Ok(0),
        }
    }

    /// Returns the full migration log.
    pub fn migration_log(&self) -> Result<Vec<MigrationRecord>, OmniError> {
        let seq = self.db.get_seq();
        let start = MIGRATION_PREFIX;
        let end = &format!("{}~", MIGRATION_PREFIX);
        let mut records = Vec::new();

        for (_, value) in self.db.scan(start, end, seq)? {
            if let Ok(record) = serde_json::from_str::<MigrationRecord>(&value) {
                records.push(record);
            }
        }

        records.sort_by_key(|r| r.version);
        Ok(records)
    }

    /// MIGRATE UP — applies a migration online (non-blocking for reads/writes).
    ///
    /// If the migration has already been applied, it's skipped (idempotent).
    /// Progress is tracked and persisted for crash recovery.
    pub fn migrate_up(&self, migration: &Migration) -> Result<(), OmniError> {
        let lock = self.db.sql_table_lock(MIGRATION_LOCK_NAME)?;
        let _guard = lock
            .lock()
            .map_err(|_| OmniError::LockPoisoned("schema migration".into()))?;
        self.migrate_up_locked(migration)
    }

    fn migrate_up_locked(&self, migration: &Migration) -> Result<(), OmniError> {
        let current = self.current_version()?;

        // Check idempotency
        if migration.version <= current {
            return match self.migration_status(migration.version)? {
                Some(MigrationRecord {
                    state: MigrationState::Applied,
                    ..
                }) => Ok(()),
                Some(record) => Err(OmniError::IoError(format!(
                    "schema version {} is already at or below current version {} but has state {:?}",
                    migration.version, current, record.state
                ))),
                None => Err(OmniError::IoError(format!(
                    "schema migration v{} is missing its durable record",
                    migration.version
                ))),
            };
        }
        if migration.version != current.saturating_add(1) {
            return Err(OmniError::IoError(format!(
                "schema migration versions must be applied consecutively: expected {}, got {}",
                current.saturating_add(1),
                migration.version
            )));
        }

        let attempt_id = self.get_or_create_attempt_id(migration.version, "up")?;

        // Mark as in-progress
        self.save_migration_record(&MigrationRecord {
            version: migration.version,
            description: migration.description.clone(),
            state: MigrationState::InProgress { progress_pct: 0 },
            applied_at: timestamp_ms(),
        })?;

        // Execute each operation
        let total_ops = migration.up.len();
        let exec_ops: Vec<&SchemaOp> = migration.up.iter().collect();
        for (i, op) in exec_ops.iter().enumerate() {
            match self.execute_op(op, &exec_ops, migration.version, "up", &attempt_id, i) {
                Ok(()) => {
                    let pct = ((i + 1) * 100).checked_div(total_ops).unwrap_or(100) as u8;
                    self.save_migration_record(&MigrationRecord {
                        version: migration.version,
                        description: migration.description.clone(),
                        state: MigrationState::InProgress { progress_pct: pct },
                        applied_at: timestamp_ms(),
                    })?;
                }
                Err(e) => {
                    // Mark as failed
                    self.save_migration_record(&MigrationRecord {
                        version: migration.version,
                        description: migration.description.clone(),
                        state: MigrationState::Failed(e.to_string()),
                        applied_at: timestamp_ms(),
                    })?;
                    return Err(e);
                }
            }
        }

        // Publish the applied record and version in one durable batch. A crash
        // between two separate commits would leave a migration looking
        // applied while the version still points at its predecessor, causing
        // a replay to run non-idempotent DDL a second time.
        self.save_applied_version(
            &MigrationRecord {
                version: migration.version,
                description: migration.description.clone(),
                state: MigrationState::Applied,
                applied_at: timestamp_ms(),
            },
            migration.version,
            &attempt_id,
        )?;

        Ok(())
    }

    /// MIGRATE DOWN — rolls back a migration.
    pub fn migrate_down(&self, migration: &Migration) -> Result<(), OmniError> {
        let lock = self.db.sql_table_lock(MIGRATION_LOCK_NAME)?;
        let _guard = lock
            .lock()
            .map_err(|_| OmniError::LockPoisoned("schema migration".into()))?;
        self.migrate_down_locked(migration)
    }

    fn migrate_down_locked(&self, migration: &Migration) -> Result<(), OmniError> {
        let current = self.current_version()?;
        if migration.version != current {
            return Err(OmniError::IoError(format!(
                "schema rollback must remove the current version: current is v{}, requested v{}",
                current, migration.version
            )));
        }

        let attempt_id = self.get_or_create_attempt_id(migration.version, "down")?;

        // Execute down operations in reverse
        let exec_ops: Vec<&SchemaOp> = migration.down.iter().rev().collect();
        for (i, op) in exec_ops.iter().enumerate() {
            self.execute_op(op, &exec_ops, migration.version, "down", &attempt_id, i)?;
        }

        // Publish the rollback record and predecessor version atomically.
        self.save_applied_version(
            &MigrationRecord {
                version: migration.version,
                description: migration.description.clone(),
                state: MigrationState::RolledBack,
                applied_at: timestamp_ms(),
            },
            migration.version.saturating_sub(1),
            &attempt_id,
        )?;

        Ok(())
    }

    /// MIGRATE TO — applies or rolls back migrations to reach a target version.
    pub fn migrate_to(&self, migrations: &[Migration], target: Version) -> Result<(), OmniError> {
        let lock = self.db.sql_table_lock(MIGRATION_LOCK_NAME)?;
        let _guard = lock
            .lock()
            .map_err(|_| OmniError::LockPoisoned("schema migration".into()))?;
        let current = self.current_version()?;

        if target > current {
            let mut sorted: Vec<&Migration> = migrations
                .iter()
                .filter(|m| m.version > current && m.version <= target)
                .collect();
            sorted.sort_by_key(|m| m.version);
            for m in sorted {
                self.migrate_up_locked(m)?;
            }
        } else if target < current {
            let mut sorted: Vec<&Migration> = migrations
                .iter()
                .filter(|m| m.version <= current && m.version > target)
                .collect();
            sorted.sort_by_key(|m| std::cmp::Reverse(m.version));
            for m in sorted {
                self.migrate_down_locked(m)?;
            }
        }

        let reached = self.current_version()?;
        if reached != target {
            return Err(OmniError::IoError(format!(
                "could not reach schema version {}; ended at {}",
                target, reached
            )));
        }
        Ok(())
    }

    /// Returns the status of a specific migration version.
    pub fn migration_status(&self, version: Version) -> Result<Option<MigrationRecord>, OmniError> {
        let key = format!("{}{:06}", MIGRATION_PREFIX, version);
        let seq = self.db.get_seq();
        match self.db.find(&key, seq)? {
            Some(val) => {
                let record: MigrationRecord =
                    serde_json::from_str(&val).map_err(|e| OmniError::IoError(e.to_string()))?;
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Internal
    // ═══════════════════════════════════════════════════════════════

    /// Serializes schema work with SQL DML for the affected collection.
    fn with_collection_lock<T>(
        &self,
        collection: &str,
        operation: impl FnOnce() -> Result<T, OmniError>,
    ) -> Result<T, OmniError> {
        let lock = self.db.sql_table_lock(collection)?;
        let _guard = lock
            .lock()
            .map_err(|_| OmniError::LockPoisoned("schema collection".into()))?;
        operation()
    }

    fn ensure_no_dependent_indexes(
        &self,
        seq: u64,
        table_name: &str,
        collection: &str,
        field_name: &str,
        canonical_field: &str,
        action: &str,
    ) -> Result<(), OmniError> {
        let catalog = self.index_manager.catalog_snapshot_at(seq)?;
        for idx_def in catalog.indexes_for_collection(table_name) {
            if catalog.effective_namespace(idx_def) == Some(IndexNamespace::Document) {
                continue;
            }
            if idx_def
                .fields
                .iter()
                .any(|(f, _)| f.eq_ignore_ascii_case(canonical_field))
            {
                return Err(OmniError::IoError(format!(
                    "cannot {action} field '{field_name}' in SQL table '{collection}' \
                     because index '{}' references it; drop the index first",
                    idx_def.name
                )));
            }
        }
        Ok(())
    }

    fn load_target_sql_table(
        &self,
        collection: &str,
        explicit_namespace: Option<IndexNamespace>,
        seq: u64,
    ) -> Result<Option<(String, crate::catalog::TableDef)>, OmniError> {
        match explicit_namespace {
            Some(IndexNamespace::Document) => Ok(None),
            Some(IndexNamespace::Both) => Err(OmniError::IoError(
                "schema migration explicit namespace must be Sql or Document, not Both".into(),
            )),
            Some(IndexNamespace::Sql) => {
                let catalog_key = format!("\x00CATALOG\x00{}", collection.to_lowercase());
                let raw = self.db.find(&catalog_key, seq)?.ok_or_else(|| {
                    OmniError::IoError(format!("SQL table '{collection}' not found"))
                })?;
                let table: crate::catalog::TableDef = serde_json::from_str(&raw).map_err(|e| {
                    OmniError::IoError(format!("invalid SQL table definition: {e}"))
                })?;
                Ok(Some((catalog_key, table)))
            }
            None => {
                let catalog_key = format!("\x00CATALOG\x00{}", collection.to_lowercase());
                let Some(raw) = self.db.find(&catalog_key, seq)? else {
                    return Ok(None);
                };
                let table: crate::catalog::TableDef = serde_json::from_str(&raw).map_err(|e| {
                    OmniError::IoError(format!("invalid SQL table definition: {e}"))
                })?;
                Ok(Some((catalog_key, table)))
            }
        }
    }

    fn execute_op(
        &self,
        op: &SchemaOp,
        exec_ops: &[&SchemaOp],
        version: Version,
        direction: &str,
        attempt_id: &str,
        step_index: usize,
    ) -> Result<(), OmniError> {
        let (inner_op, explicit_namespace) = op.unwrap_namespace()?;
        match inner_op {
            SchemaOp::CreateIndex {
                name,
                collection,
                fields,
                unique,
            } => self.with_collection_lock(collection, || {
                let seq = self.db.get_seq();
                let (resolved_collection, expected_namespace) = match explicit_namespace {
                    Some(IndexNamespace::Document) => {
                        (collection.clone(), IndexNamespace::Document)
                    }
                    Some(IndexNamespace::Sql) => {
                        let (_, table) = self
                            .load_target_sql_table(collection, Some(IndexNamespace::Sql), seq)?
                            .ok_or_else(|| {
                                OmniError::IoError(format!("SQL table '{collection}' not found"))
                            })?;
                        (table.name, IndexNamespace::Sql)
                    }
                    None => self
                        .index_manager
                        .resolve_collection_and_namespace_at(collection, seq)?,
                    Some(IndexNamespace::Both) => {
                        unreachable!("rejected by unwrap_namespace")
                    }
                };
                let receipt_key = Self::create_index_done_key(
                    version,
                    direction,
                    attempt_id,
                    step_index,
                    &resolved_collection,
                    name,
                );
                let matches_requested =
                    |def: &crate::secondary_index::IndexDefinition, expected_id: u32| -> bool {
                        def.id == expected_id
                            && def.namespace == expected_namespace
                            && def.name.eq_ignore_ascii_case(name)
                            && (def.collection == *collection
                                || def.collection == resolved_collection)
                            && def.unique == *unique
                            && def.fields.len() == fields.len()
                            && def.fields.iter().zip(fields.iter()).all(
                                |((af, at), (bf, bt))| {
                                    let name_ok = if def.namespace == IndexNamespace::Document {
                                        af == bf
                                    } else {
                                        af.eq_ignore_ascii_case(bf)
                                    };
                                    name_ok && at == bt
                                },
                            )
                    };
                let (done_id, owned_building_id) = match self.db.find(&receipt_key, seq)? {
                    Some(receipt) => {
                        if let Some(id_str) = receipt.strip_prefix("done:") {
                            let id = id_str.parse::<u32>().map_err(|e| {
                                OmniError::IoError(format!(
                                    "invalid CreateIndex receipt '{receipt}': {e}"
                                ))
                            })?;
                            (Some(id), None)
                        } else if let Some(id_str) = receipt.strip_prefix("building:") {
                            let id = id_str.parse::<u32>().map_err(|e| {
                                OmniError::IoError(format!(
                                    "invalid CreateIndex receipt '{receipt}': {e}"
                                ))
                            })?;
                            (None, Some(id))
                        } else {
                            return Err(OmniError::IoError(format!(
                                "invalid CreateIndex receipt '{receipt}': expected 'building:<id>' or 'done:<id>'"
                            )));
                        }
                    }
                    None => (None, None),
                };
                if let Some(done_id) = done_id {
                    let catalog = self.index_manager.catalog_snapshot_at(seq)?;
                    if let Some(def) = catalog.get(done_id)
                        && matches_requested(def, done_id)
                    {
                        if catalog.is_ready(done_id) {
                            return Ok(());
                        }
                        // Still owned by this attempt with matching definition,
                        // just not ready yet: rebuild to recover cleanly.
                        self.index_manager
                            .rebuild_index_locked(name, &resolved_collection)?;
                        return Ok(());
                    }
                    if self.was_index_dropped_by_later_step(
                        exec_ops,
                        version,
                        direction,
                        attempt_id,
                        step_index,
                        name,
                        &resolved_collection,
                        expected_namespace,
                        done_id,
                        seq,
                    )? {
                        return Ok(());
                    }
                    return Err(OmniError::IoError(format!(
                        "migration CreateIndex '{name}' (id {done_id}) receipt mismatch: index is missing, unready, or was replaced before retry"
                    )));
                }
                let index_id = if let Some(owned_id) = owned_building_id {
                    let catalog = self.index_manager.catalog_snapshot_at(seq)?;
                    if let Some(def) = catalog.get(owned_id) {
                        if !matches_requested(def, owned_id) {
                            return Err(OmniError::IoError(format!(
                                "migration CreateIndex '{name}' (id {owned_id}) building receipt mismatch: existing index definition does not match requested schema"
                            )));
                        }
                        owned_id
                    } else {
                        self.index_manager.create_index_locked_with_options(
                            name,
                            collection,
                            fields.clone(),
                            *unique,
                            Some(&receipt_key),
                            explicit_namespace,
                        )?
                    }
                } else {
                    self.index_manager.create_index_locked_with_options(
                        name,
                        collection,
                        fields.clone(),
                        *unique,
                        Some(&receipt_key),
                        explicit_namespace,
                    )?
                };
                if let Err(error) = self
                    .index_manager
                    .rebuild_index_locked(name, &resolved_collection)
                {
                    if let Err(cleanup_error) = self
                        .index_manager
                        .drop_index_locked(name, &resolved_collection)
                    {
                        return Err(OmniError::IoError(format!(
                            "{error}; failed index cleanup also failed: {cleanup_error}"
                        )));
                    }
                    let mut clear_receipt = WriteBatch::new();
                    clear_receipt.delete(&receipt_key)?;
                    let _ = self.db.commit_batch(&clear_receipt);
                    return Err(error);
                }
                let mut done_batch = WriteBatch::new();
                done_batch.set(&receipt_key, format!("done:{index_id}"))?;
                self.db.commit_batch(&done_batch)?;
                Ok(())
            }),
            SchemaOp::DropIndex { name } => {
                let receipt_key = Self::drop_index_done_key(
                    version,
                    direction,
                    attempt_id,
                    step_index,
                    explicit_namespace,
                    name,
                );
                let legacy_receipt_key = Self::legacy_drop_index_done_key(
                    version, direction, attempt_id, step_index, name,
                );
                self.index_manager.drop_index_with_receipt(
                    name,
                    &receipt_key,
                    Some(&legacy_receipt_key),
                    explicit_namespace,
                )
            }
            SchemaOp::AddField {
                collection,
                field_name,
                default_value,
            } => self.with_collection_lock(collection, || {
                let seq = self.db.get_seq();
                if let Some((catalog_key, mut table)) =
                    self.load_target_sql_table(collection, explicit_namespace, seq)?
                {
                    let table_id = table.durable_identity();
                    let done_key = Self::add_done_key(
                        version,
                        direction,
                        attempt_id,
                        step_index,
                        IndexNamespace::Sql,
                        &table.name,
                        field_name,
                    );
                    let expected_done = Self::sql_done_receipt(&table_id);
                    // Honor a completed receipt for this exact table identity
                    // BEFORE inspecting columns or rows so retrying a failed
                    // later step never re-adds a field that a subsequent step
                    // already renamed or removed.
                    if self.db.find(&done_key, seq)?.as_deref() == Some(expected_done.as_str()) {
                        return Ok(());
                    }
                    let effective_field = table
                        .columns
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(field_name))
                        .map(|c| c.name.clone())
                        .unwrap_or_else(|| field_name.clone());
                    let table_name = table.name.clone();
                    let row_format = table.row_format;
                    let catalog_update = if !table
                        .columns
                        .iter()
                        .any(|c| c.name.eq_ignore_ascii_case(field_name))
                    {
                        let col_type = match default_value {
                            serde_json::Value::Number(n) if n.is_i64() || n.is_u64() => {
                                crate::catalog::ColumnType::Integer
                            }
                            serde_json::Value::Number(_) => crate::catalog::ColumnType::Float,
                            serde_json::Value::Bool(_) => crate::catalog::ColumnType::Boolean,
                            _ => crate::catalog::ColumnType::Text,
                        };
                        table.columns.push(crate::catalog::Column {
                            name: field_name.clone(),
                            col_type,
                            nullable: true,
                            primary_key: false,
                            default: match default_value {
                                serde_json::Value::Null => None,
                                serde_json::Value::String(s) => Some(s.clone()),
                                other => Some(other.to_string()),
                            },
                        });
                        let cat_value = serde_json::to_string(&table)
                            .map_err(|e| crate::OmniError::IoError(e.to_string()))?;
                        Some((catalog_key, cat_value))
                    } else {
                        None
                    };
                    return self.add_field_online(
                        &table_name,
                        IndexNamespace::Sql,
                        row_format,
                        &effective_field,
                        default_value,
                        catalog_update
                            .as_ref()
                            .map(|(k, v)| (k.as_str(), v.as_str())),
                        &done_key,
                        &expected_done,
                    );
                }

                let done_key = Self::add_done_key(
                    version,
                    direction,
                    attempt_id,
                    step_index,
                    IndexNamespace::Document,
                    collection,
                    field_name,
                );
                if self.db.find(&done_key, seq)?.as_deref() == Some("done") {
                    return Ok(());
                }
                self.add_field_online(
                    collection,
                    IndexNamespace::Document,
                    crate::catalog::RowFormat::default(),
                    field_name,
                    default_value,
                    None,
                    &done_key,
                    "done",
                )
            }),
            SchemaOp::RemoveField {
                collection,
                field_name,
            } => self.with_collection_lock(collection, || {
                let seq = self.db.get_seq();
                if let Some((catalog_key, mut table)) =
                    self.load_target_sql_table(collection, explicit_namespace, seq)?
                {
                    let table_id = table.durable_identity();
                    let done_key = Self::remove_done_key(
                        version,
                        direction,
                        attempt_id,
                        step_index,
                        &table.name,
                        field_name,
                    );
                    let expected_done = Self::sql_done_receipt(&table_id);
                    if let Some(receipt) = self.db.find(&done_key, seq)? {
                        if receipt == expected_done {
                            return Ok(());
                        }
                        if let Some(canonical_name) =
                            Self::parse_sql_cleanup_receipt(&receipt, &table_id)
                        {
                            self.backfill_field(
                                &table.name,
                                IndexNamespace::Sql,
                                canonical_name,
                                None,
                            )?;
                            let mut finish_batch = WriteBatch::new();
                            finish_batch.set(&done_key, expected_done)?;
                            self.db.commit_batch(&finish_batch)?;
                            return Ok(());
                        }
                        // Receipt belonged to an earlier table instance that was
                        // dropped and recreated; do not apply it to this replacement.
                    }
                    if table.primary_key.eq_ignore_ascii_case(field_name) {
                        return Err(crate::OmniError::IoError(format!(
                            "cannot remove primary key field '{}' from SQL table '{}'",
                            field_name, collection
                        )));
                    }
                    let Some(canonical_name) = table
                        .columns
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(field_name))
                        .map(|c| c.name.clone())
                    else {
                        return Err(crate::OmniError::IoError(format!(
                            "column '{}' does not exist in SQL table '{}'",
                            field_name, collection
                        )));
                    };
                    self.ensure_no_dependent_indexes(
                        seq,
                        &table.name,
                        collection,
                        field_name,
                        &canonical_name,
                        "remove",
                    )?;
                    let table_name = table.name.clone();
                    let sql_row_format = table.row_format;
                    table
                        .columns
                        .retain(|c| !c.name.eq_ignore_ascii_case(&canonical_name));
                    let cat_value = serde_json::to_string(&table)
                        .map_err(|e| crate::OmniError::IoError(e.to_string()))?;
                    return self.remove_sql_field_online(
                        &table_name,
                        &table_id,
                        sql_row_format,
                        &canonical_name,
                        &catalog_key,
                        &cat_value,
                        &done_key,
                    );
                }
                let done_key = Self::doc_remove_done_key(
                    version, direction, attempt_id, step_index, collection, field_name,
                );
                if self.db.find(&done_key, seq)?.as_deref() == Some("done") {
                    return Ok(());
                }
                self.backfill_field_with_receipt(
                    collection,
                    IndexNamespace::Document,
                    crate::catalog::RowFormat::default(),
                    field_name,
                    None,
                    None,
                    Some((&done_key, "done")),
                )
            }),
            SchemaOp::RenameField {
                collection,
                old_name,
                new_name,
            } => self.with_collection_lock(collection, || {
                let seq = self.db.get_seq();
                if let Some((catalog_key, mut table)) =
                    self.load_target_sql_table(collection, explicit_namespace, seq)?
                {
                    let table_id = table.durable_identity();
                    let done_key = Self::rename_done_key(
                        version,
                        direction,
                        attempt_id,
                        step_index,
                        &table.name,
                        old_name,
                        new_name,
                    );
                    let expected_done = Self::sql_done_receipt(&table_id);
                    // Honor a completed receipt for this table identity BEFORE
                    // inspecting the current column layout so a retry after a
                    // later step re-adds `old_name` does not trip `has_new`.
                    if self.db.find(&done_key, seq)?.as_deref() == Some(expected_done.as_str()) {
                        return Ok(());
                    }
                    if table.primary_key.eq_ignore_ascii_case(old_name) {
                        return Err(crate::OmniError::IoError(format!(
                            "cannot rename primary key field '{}' in SQL table '{}' \
                             without also updating the row-key contract",
                            old_name, collection
                        )));
                    }
                    let canonical_new = table
                        .columns
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(new_name))
                        .map(|c| c.name.clone());
                    let has_new = canonical_new.is_some();
                    let canonical_old_opt = table
                        .columns
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(old_name))
                        .map(|c| c.name.clone());
                    let has_old = canonical_old_opt.is_some();
                    if let Some(ref resolved_new) = canonical_new
                        && !has_old
                    {
                        for (candidate_stage_key, staged_old) in
                            Self::active_rename_stages_for_namespace(
                                &self.db,
                                &table.name,
                                IndexNamespace::Sql,
                                seq,
                            )?
                        {
                            let matches_stage = staged_old.eq_ignore_ascii_case(old_name)
                                && (candidate_stage_key
                                    == Self::rename_stage_key_for_namespace(
                                        &table.name,
                                        IndexNamespace::Sql,
                                        &staged_old,
                                        resolved_new,
                                    )
                                    || candidate_stage_key
                                        == Self::legacy_rename_stage_key(
                                            &table.name,
                                            &staged_old,
                                            resolved_new,
                                        ));
                            if matches_stage {
                                return self.cleanup_staged_rename(
                                    &table.name,
                                    IndexNamespace::Sql,
                                    &staged_old,
                                    resolved_new,
                                    &candidate_stage_key,
                                    &done_key,
                                    &expected_done,
                                );
                            }
                        }
                    }
                    if has_new {
                        return Err(crate::OmniError::IoError(format!(
                            "column '{}' already exists in SQL table '{}'",
                            new_name, collection
                        )));
                    }
                    let Some(canonical_old) = canonical_old_opt else {
                        return Err(crate::OmniError::IoError(format!(
                            "column '{}' does not exist in SQL table '{}'",
                            old_name, collection
                        )));
                    };
                    self.ensure_no_dependent_indexes(
                        seq,
                        &table.name,
                        collection,
                        old_name,
                        &canonical_old,
                        "rename",
                    )?;
                    let table_name = table.name.clone();
                    for col in &mut table.columns {
                        if col.name.eq_ignore_ascii_case(&canonical_old) {
                            col.name = new_name.clone();
                        }
                    }
                    let cat_value = serde_json::to_string(&table)
                        .map_err(|e| crate::OmniError::IoError(e.to_string()))?;
                    return self.rename_field_online(
                        &table_name,
                        &canonical_old,
                        new_name,
                        Some((&catalog_key, &cat_value)),
                        &done_key,
                        &expected_done,
                    );
                }
                let done_key = Self::rename_done_key(
                    version, direction, attempt_id, step_index, collection, old_name, new_name,
                );
                self.rename_field_online(collection, old_name, new_name, None, &done_key, "done")
            }),
            SchemaOp::InNamespace { .. } => unreachable!("unwrapped above"),
        }
    }

    fn migration_attempt_key(version: Version, direction: &str) -> String {
        format!("\x00SCHEMA\x00attempt:{version:06}:{direction}")
    }

    fn get_or_create_attempt_id(
        &self,
        version: Version,
        direction: &str,
    ) -> Result<String, OmniError> {
        let attempt_key = Self::migration_attempt_key(version, direction);
        if let Some(existing) = self.db.find(&attempt_key, self.db.get_seq())? {
            return Ok(existing);
        }
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let seq = NEXT_MIGRATION_ATTEMPT_COUNTER.fetch_add(1, Ordering::Relaxed);
        let attempt_id = format!("{nanos:x}_{seq:x}");
        let mut batch = WriteBatch::new();
        batch.set(&attempt_key, attempt_id.clone())?;
        self.db.commit_batch(&batch)?;
        Ok(attempt_id)
    }

    fn sql_done_receipt(table_id: &str) -> String {
        format!("done:{}:{table_id}", table_id.len())
    }

    fn sql_cleanup_receipt(table_id: &str, canonical_name: &str) -> String {
        format!("cleanup:{}:{table_id}:{canonical_name}", table_id.len())
    }

    fn parse_sql_cleanup_receipt<'a>(receipt: &'a str, expected_table_id: &str) -> Option<&'a str> {
        let rest = receipt.strip_prefix("cleanup:")?;
        let (len_str, remainder) = rest.split_once(':')?;
        let id_len = len_str.parse::<usize>().ok()?;
        if remainder.len() < id_len + 1 || !remainder.is_char_boundary(id_len) {
            return None;
        }
        let (stored_id, tail) = remainder.split_at(id_len);
        let canonical_name = tail.strip_prefix(':')?;
        (stored_id == expected_table_id).then_some(canonical_name)
    }

    pub(crate) fn legacy_rename_stage_prefix_for_table(collection: &str) -> String {
        format!(
            "\x00SCHEMA\x00rename_stage:{}:{}:",
            collection.len(),
            collection
        )
    }

    pub(crate) fn legacy_rename_stage_key(
        collection: &str,
        old_name: &str,
        new_name: &str,
    ) -> String {
        format!(
            "{}{}:{}:{}:{}",
            Self::legacy_rename_stage_prefix_for_table(collection),
            old_name.len(),
            old_name,
            new_name.len(),
            new_name
        )
    }

    pub(crate) fn rename_stage_prefix_for_namespace(
        collection: &str,
        namespace: IndexNamespace,
    ) -> String {
        match namespace {
            IndexNamespace::Sql => format!(
                "\x00SCHEMA\x00rename_stage:sql:{}:{}:",
                collection.len(),
                collection
            ),
            IndexNamespace::Document => format!(
                "\x00SCHEMA\x00rename_stage:doc:{}:{}:",
                collection.len(),
                collection
            ),
            IndexNamespace::Both => Self::legacy_rename_stage_prefix_for_table(collection),
        }
    }

    pub(crate) fn rename_stage_key_for_namespace(
        collection: &str,
        namespace: IndexNamespace,
        old_name: &str,
        new_name: &str,
    ) -> String {
        format!(
            "{}{}:{}:{}:{}",
            Self::rename_stage_prefix_for_namespace(collection, namespace),
            old_name.len(),
            old_name,
            new_name.len(),
            new_name
        )
    }

    /// Determines whether a legacy un-namespaced rename stage belongs to
    /// `namespace` by inspecting its row markers (`\x00T\x00<table>\x00` vs
    /// `<collection>:`). Prevents cross-namespace reuse or marker sweeping
    /// while preserving recovery of pending legacy stages.
    pub(crate) fn legacy_rename_stage_matches_namespace(
        db: &OmniKV,
        collection: &str,
        legacy_stage_key: &str,
        namespace: IndexNamespace,
        seq: u64,
    ) -> Result<bool, OmniError> {
        let marker_prefix = Self::rename_row_stage_prefix(legacy_stage_key);
        let sql_row_prefix = format!("\x00T\x00{collection}\x00");
        let doc_row_prefix = format!("{collection}:");
        let mut has_sql_marker = false;
        let mut has_doc_marker = false;
        for (marker_key, _) in db.scan_prefix(&marker_prefix, seq)? {
            let row_key = marker_key.strip_prefix(&marker_prefix).unwrap_or("");
            if row_key.starts_with(&sql_row_prefix) {
                has_sql_marker = true;
            } else if row_key.starts_with(&doc_row_prefix) {
                has_doc_marker = true;
            }
        }
        match namespace {
            IndexNamespace::Sql => {
                if has_sql_marker {
                    Ok(true)
                } else if has_doc_marker {
                    Ok(false)
                } else {
                    Ok(db
                        .find(
                            &format!("\x00CATALOG\x00{}", collection.to_lowercase()),
                            seq,
                        )?
                        .is_some())
                }
            }
            IndexNamespace::Document => {
                if has_doc_marker {
                    Ok(true)
                } else if has_sql_marker {
                    Ok(false)
                } else {
                    Ok(db
                        .find(
                            &format!("\x00CATALOG\x00{}", collection.to_lowercase()),
                            seq,
                        )?
                        .is_none())
                }
            }
            IndexNamespace::Both => Ok(true),
        }
    }

    /// Lists all active rename stages owned by `(collection, namespace)`,
    /// including both namespace-scoped keys and matching legacy keys.
    pub(crate) fn active_rename_stages_for_namespace(
        db: &OmniKV,
        collection: &str,
        namespace: IndexNamespace,
        seq: u64,
    ) -> Result<Vec<(String, String)>, OmniError> {
        let mut stages = db.scan_prefix(
            &Self::rename_stage_prefix_for_namespace(collection, namespace),
            seq,
        )?;
        if namespace != IndexNamespace::Both {
            for (legacy_key, val) in
                db.scan_prefix(&Self::legacy_rename_stage_prefix_for_table(collection), seq)?
            {
                if Self::legacy_rename_stage_matches_namespace(
                    db,
                    collection,
                    &legacy_key,
                    namespace,
                    seq,
                )? {
                    stages.push((legacy_key, val));
                }
            }
        }
        Ok(stages)
    }

    pub(crate) fn rename_row_stage_prefix(stage_key: &str) -> String {
        let suffix = stage_key
            .strip_prefix("\x00SCHEMA\x00rename_stage:")
            .unwrap_or(stage_key);
        format!("\x00SCHEMA\x00rename_row:{}:{suffix}\x00", suffix.len())
    }

    pub(crate) fn rename_row_stage_key(stage_key: &str, row_key: &str) -> String {
        format!("{}{row_key}", Self::rename_row_stage_prefix(stage_key))
    }

    fn rename_done_prefix(version: Version, direction: &str, attempt_id: &str) -> String {
        format!(
            "\x00SCHEMA\x00rename_done:{version:06}:{direction}:{}:{attempt_id}:",
            attempt_id.len()
        )
    }

    fn rename_done_key(
        version: Version,
        direction: &str,
        attempt_id: &str,
        step_index: usize,
        collection: &str,
        old_name: &str,
        new_name: &str,
    ) -> String {
        format!(
            "{}{step_index:06}:{}:{}:{}:{}:{}:{}",
            Self::rename_done_prefix(version, direction, attempt_id),
            collection.len(),
            collection,
            old_name.len(),
            old_name,
            new_name.len(),
            new_name
        )
    }

    fn create_index_done_prefix(version: Version, direction: &str, attempt_id: &str) -> String {
        format!(
            "\x00SCHEMA\x00create_idx:{version:06}:{direction}:{}:{attempt_id}:",
            attempt_id.len()
        )
    }

    fn create_index_done_key(
        version: Version,
        direction: &str,
        attempt_id: &str,
        step_index: usize,
        collection: &str,
        index_name: &str,
    ) -> String {
        let idx_lower = index_name.to_ascii_lowercase();
        format!(
            "{}{step_index:06}:{}:{}:{}:{}",
            Self::create_index_done_prefix(version, direction, attempt_id),
            collection.len(),
            collection,
            idx_lower.len(),
            idx_lower
        )
    }

    fn drop_index_done_prefix(version: Version, direction: &str, attempt_id: &str) -> String {
        format!(
            "\x00SCHEMA\x00drop_idx:{version:06}:{direction}:{}:{attempt_id}:",
            attempt_id.len()
        )
    }

    fn legacy_drop_index_done_key(
        version: Version,
        direction: &str,
        attempt_id: &str,
        step_index: usize,
        index_name: &str,
    ) -> String {
        let idx_lower = index_name.to_ascii_lowercase();
        format!(
            "{}{step_index:06}:{}:{idx_lower}",
            Self::drop_index_done_prefix(version, direction, attempt_id),
            idx_lower.len()
        )
    }

    fn drop_index_done_key(
        version: Version,
        direction: &str,
        attempt_id: &str,
        step_index: usize,
        explicit_namespace: Option<IndexNamespace>,
        index_name: &str,
    ) -> String {
        let ns_tag = match explicit_namespace {
            Some(IndexNamespace::Sql) => "sql",
            Some(IndexNamespace::Document) => "doc",
            Some(IndexNamespace::Both) => "both",
            None => "any",
        };
        let idx_lower = index_name.to_ascii_lowercase();
        format!(
            "{}{step_index:06}:{ns_tag}:{}:{idx_lower}",
            Self::drop_index_done_prefix(version, direction, attempt_id),
            idx_lower.len()
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn was_index_dropped_by_later_step(
        &self,
        exec_ops: &[&SchemaOp],
        version: Version,
        direction: &str,
        attempt_id: &str,
        create_step_index: usize,
        index_name: &str,
        resolved_collection: &str,
        create_namespace: IndexNamespace,
        index_id: crate::secondary_index::IndexId,
        seq: u64,
    ) -> Result<bool, OmniError> {
        for (drop_step, op) in exec_ops.iter().enumerate().skip(create_step_index + 1) {
            let Ok((SchemaOp::DropIndex { name: drop_name }, drop_step_ns)) = op.unwrap_namespace()
            else {
                continue;
            };
            if !drop_name.eq_ignore_ascii_case(index_name) {
                continue;
            }
            if drop_step_ns.is_some_and(|ns| ns != create_namespace) {
                continue;
            }
            let receipt_key = Self::drop_index_done_key(
                version,
                direction,
                attempt_id,
                drop_step,
                drop_step_ns,
                index_name,
            );
            let legacy_key = Self::legacy_drop_index_done_key(
                version, direction, attempt_id, drop_step, index_name,
            );
            let loaded = if let Some(receipt) = self.db.find(&receipt_key, seq)? {
                Some((receipt, false))
            } else {
                self.db
                    .find(&legacy_key, seq)?
                    .map(|receipt| (receipt, true))
            };
            let Some((receipt, from_legacy_key)) = loaded else {
                continue;
            };
            // Always propagate malformed receipts fail-closed.
            crate::secondary_index::parse_drop_index_receipt(&receipt)?;
            // Enforce the exact same namespace and legacy-attribution rules as
            // `drop_index_with_receipt`, plus collection-level attribution so a
            // namespace-less legacy receipt cannot satisfy replay unless safely
            // attributable to `create_namespace`.
            let Ok((_, _, dropped_ids)) = self.index_manager.validate_drop_index_receipt_at(
                index_name,
                &receipt,
                from_legacy_key,
                Some(create_namespace),
                Some(resolved_collection),
                seq,
            ) else {
                continue;
            };
            if dropped_ids.contains(&index_id) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn add_done_prefix(version: Version, direction: &str, attempt_id: &str) -> String {
        format!(
            "\x00SCHEMA\x00add_done:{version:06}:{direction}:{}:{attempt_id}:",
            attempt_id.len()
        )
    }

    fn add_done_key(
        version: Version,
        direction: &str,
        attempt_id: &str,
        step_index: usize,
        namespace: IndexNamespace,
        collection: &str,
        field_name: &str,
    ) -> String {
        let ns_tag = match namespace {
            IndexNamespace::Sql => "sql",
            IndexNamespace::Document => "doc",
            IndexNamespace::Both => "both",
        };
        let field = if namespace == IndexNamespace::Sql {
            field_name.to_lowercase()
        } else {
            field_name.to_string()
        };
        format!(
            "{}{step_index:06}:{ns_tag}:{}:{}:{}:{}",
            Self::add_done_prefix(version, direction, attempt_id),
            collection.len(),
            collection,
            field.len(),
            field
        )
    }

    fn doc_remove_done_prefix(version: Version, direction: &str, attempt_id: &str) -> String {
        format!(
            "\x00SCHEMA\x00doc_remove_done:{version:06}:{direction}:{}:{attempt_id}:",
            attempt_id.len()
        )
    }

    fn doc_remove_done_key(
        version: Version,
        direction: &str,
        attempt_id: &str,
        step_index: usize,
        collection: &str,
        field_name: &str,
    ) -> String {
        format!(
            "{}{step_index:06}:{}:{}:{}:{}",
            Self::doc_remove_done_prefix(version, direction, attempt_id),
            collection.len(),
            collection,
            field_name.len(),
            field_name
        )
    }

    fn remove_done_prefix(version: Version, direction: &str, attempt_id: &str) -> String {
        format!(
            "\x00SCHEMA\x00remove_done:{version:06}:{direction}:{}:{attempt_id}:",
            attempt_id.len()
        )
    }

    fn remove_done_key(
        version: Version,
        direction: &str,
        attempt_id: &str,
        step_index: usize,
        collection: &str,
        field_name: &str,
    ) -> String {
        let field = field_name.to_lowercase();
        format!(
            "{}{step_index:06}:{}:{}:{}:{}",
            Self::remove_done_prefix(version, direction, attempt_id),
            collection.len(),
            collection,
            field.len(),
            field
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn remove_sql_field_online(
        &self,
        collection: &str,
        table_id: &str,
        sql_row_format: crate::catalog::RowFormat,
        field_name: &str,
        catalog_key: &str,
        cat_value: &str,
        done_key: &str,
    ) -> Result<(), OmniError> {
        self.index_manager.refresh_catalog()?;
        let seq = self.db.get_seq();
        let catalog = self.index_manager.catalog_snapshot_at(seq)?;
        let max_doc_ops = 1 + 2 * catalog.indexes_for_collection(collection).len();
        // Reserve 2 ops for the catalog update + step receipt so small
        // removals commit all row rewrites, catalog cutover, and receipt
        // atomically in one WriteBatch.
        let single_batch_budget = WriteBatch::MAX_OPS.saturating_sub(2);
        let mut single_batch = WriteBatch::new();
        let mut fits_in_one_batch = true;

        'ranges: for (prefix, end) in
            collection_row_ranges_for_namespace(collection, IndexNamespace::Sql)
        {
            let is_sql_rows = prefix.starts_with("\x00T\x00");
            for (key, value) in self.db.scan_exclusive(&prefix, Some(&end), seq)? {
                let Ok(mut doc) = serde_json::from_str::<serde_json::Value>(&value) else {
                    continue;
                };
                let old_doc = doc.clone();
                let Some(obj) = doc.as_object_mut() else {
                    continue;
                };
                if obj.remove(field_name).is_some() {
                    if single_batch.op_count().saturating_add(max_doc_ops) > single_batch_budget {
                        fits_in_one_batch = false;
                        break 'ranges;
                    }
                    self.stage_doc_update(
                        &catalog,
                        collection,
                        &prefix,
                        is_sql_rows,
                        sql_row_format,
                        &key,
                        &value,
                        old_doc,
                        doc,
                        &mut single_batch,
                    )?;
                }
            }
        }

        let done_receipt = Self::sql_done_receipt(table_id);
        if fits_in_one_batch {
            single_batch.set(catalog_key, cat_value.to_string())?;
            single_batch.set(done_key, done_receipt)?;
            self.db.commit_batch(&single_batch)?;
            return Ok(());
        }

        // Large removal: commit the catalog cutover and a table-bound cleanup
        // receipt atomically first so the old column stays readable until
        // cutover is durable, then clean stored row values in retryable chunks.
        let mut cutover_batch = WriteBatch::new();
        cutover_batch.set(catalog_key, cat_value.to_string())?;
        cutover_batch.set(done_key, Self::sql_cleanup_receipt(table_id, field_name))?;
        self.db.commit_batch(&cutover_batch)?;

        self.backfill_field(collection, IndexNamespace::Sql, field_name, None)?;

        let mut finish_batch = WriteBatch::new();
        finish_batch.set(done_key, done_receipt)?;
        self.db.commit_batch(&finish_batch)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn stage_doc_update(
        &self,
        catalog: &crate::secondary_index::IndexCatalog,
        collection: &str,
        prefix: &str,
        is_sql_rows: bool,
        sql_row_format: crate::catalog::RowFormat,
        key: &str,
        previous_json: &str,
        old_doc: serde_json::Value,
        new_doc: serde_json::Value,
        batch: &mut WriteBatch,
    ) -> Result<(), OmniError> {
        let updated =
            serde_json::to_string(&new_doc).map_err(|e| OmniError::IoError(e.to_string()))?;
        let primary_key = key.strip_prefix(prefix).unwrap_or(key);
        let (old_index_doc, new_index_doc) = if is_sql_rows {
            let old_row = crate::plan_exec::PlanExecutor::deserialize_row_checked(
                previous_json,
                sql_row_format,
            )
            .map_err(OmniError::IoError)?;
            let new_row =
                crate::plan_exec::PlanExecutor::deserialize_row_checked(&updated, sql_row_format)
                    .map_err(OmniError::IoError)?;
            (
                serde_json::to_value(&old_row).map_err(|e| OmniError::IoError(e.to_string()))?,
                serde_json::to_value(&new_row).map_err(|e| OmniError::IoError(e.to_string()))?,
            )
        } else {
            (old_doc, new_doc)
        };
        let namespace = if is_sql_rows {
            IndexNamespace::Sql
        } else {
            IndexNamespace::Document
        };
        self.index_manager
            .replace_document_indexes_with_catalog_for_namespace(
                catalog,
                collection,
                namespace,
                primary_key,
                &old_index_doc,
                primary_key,
                &new_index_doc,
                batch,
                None,
            )?;
        batch.set(key, updated)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn add_field_online(
        &self,
        collection: &str,
        namespace: IndexNamespace,
        sql_row_format: crate::catalog::RowFormat,
        field_name: &str,
        default_value: &serde_json::Value,
        sql_catalog_update: Option<(&str, &str)>,
        done_key: &str,
        done_value: &str,
    ) -> Result<(), OmniError> {
        self.backfill_field_with_receipt(
            collection,
            namespace,
            sql_row_format,
            field_name,
            Some(default_value),
            sql_catalog_update,
            Some((done_key, done_value)),
        )
    }

    fn backfill_field(
        &self,
        collection: &str,
        namespace: IndexNamespace,
        field_name: &str,
        default_value: Option<&serde_json::Value>,
    ) -> Result<(), OmniError> {
        let seq = self.db.get_seq();
        let sql_row_format = if namespace == IndexNamespace::Sql {
            self.db
                .find(
                    &format!("\x00CATALOG\x00{}", collection.to_lowercase()),
                    seq,
                )?
                .and_then(|raw| serde_json::from_str::<crate::catalog::TableDef>(&raw).ok())
                .map(|table| table.row_format)
                .unwrap_or_default()
        } else {
            crate::catalog::RowFormat::default()
        };
        self.backfill_field_with_receipt(
            collection,
            namespace,
            sql_row_format,
            field_name,
            default_value,
            None,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn backfill_field_with_receipt(
        &self,
        collection: &str,
        namespace: IndexNamespace,
        sql_row_format: crate::catalog::RowFormat,
        field_name: &str,
        default_value: Option<&serde_json::Value>,
        sql_catalog_update: Option<(&str, &str)>,
        done_receipt: Option<(&str, &str)>,
    ) -> Result<(), OmniError> {
        self.index_manager.refresh_catalog()?;
        if namespace == IndexNamespace::Document {
            return self.backfill_document_field_with_receipt(
                collection,
                field_name,
                default_value,
                done_receipt,
            );
        }
        let seq = self.db.get_seq();
        let catalog = self.index_manager.catalog_snapshot_at(seq)?;
        let sql_table_present = namespace == IndexNamespace::Sql
            && self
                .db
                .find(
                    &format!("\x00CATALOG\x00{}", collection.to_lowercase()),
                    seq,
                )?
                .is_some();
        let sql_default = match default_value {
            Some(serde_json::Value::Null) => Some(serde_json::Value::Null),
            Some(serde_json::Value::String(s)) => {
                if namespace == IndexNamespace::Sql
                    && s == "NULL"
                    && sql_row_format == crate::catalog::RowFormat::Legacy
                {
                    let sql_prefix = format!("\x00T\x00{}\x00", collection);
                    if sql_table_present || !self.db.scan_prefix(&sql_prefix, seq)?.is_empty() {
                        return Err(OmniError::IoError(format!(
                            "literal \"NULL\" default for field '{field_name}' is not supported on legacy SQL table '{collection}'"
                        )));
                    }
                }
                Some(serde_json::Value::String(s.clone()))
            }
            Some(serde_json::Value::Number(n)) => Some(serde_json::Value::String(n.to_string())),
            Some(serde_json::Value::Bool(b)) => Some(serde_json::Value::String(b.to_string())),
            Some(other) => {
                if namespace == IndexNamespace::Sql {
                    let sql_prefix = format!("\x00T\x00{}\x00", collection);
                    if sql_table_present || !self.db.scan_prefix(&sql_prefix, seq)?.is_empty() {
                        return Err(OmniError::IoError(format!(
                            "unsupported SQL row value for field '{field_name}': {other}"
                        )));
                    }
                }
                None
            }
            None => None,
        };

        let mut batch = WriteBatch::new();
        let max_doc_ops = 1 + 2 * catalog.indexes_for_collection(collection).len();

        for (prefix, end) in collection_row_ranges_for_namespace(collection, namespace) {
            let is_sql_rows = prefix.starts_with("\x00T\x00");
            for (key, value) in self.db.scan_exclusive(&prefix, Some(&end), seq)? {
                let Ok(mut doc) = serde_json::from_str::<serde_json::Value>(&value) else {
                    continue;
                };
                let old_doc = doc.clone();
                let Some(obj) = doc.as_object_mut() else {
                    continue;
                };
                let changed = match default_value {
                    Some(val) if !obj.contains_key(field_name) => {
                        let insert_val = if is_sql_rows {
                            sql_default.clone().unwrap_or(serde_json::Value::Null)
                        } else {
                            val.clone()
                        };
                        obj.insert(field_name.to_string(), insert_val);
                        true
                    }
                    Some(_) => false,
                    None => obj.remove(field_name).is_some(),
                };
                if changed {
                    if !batch.is_empty()
                        && (batch.op_count() >= WriteBatch::MAX_OPS / 2
                            || batch.op_count().saturating_add(max_doc_ops) > WriteBatch::MAX_OPS)
                    {
                        self.db.commit_batch(&batch)?;
                        batch = WriteBatch::new();
                        if default_value.is_some() {
                            crate::failpoints::maybe_fail("schema::add_field_after_partial_batch")
                                .map_err(|e| {
                                    OmniError::IoError(format!(
                                        "schema add_field failure point: {e}"
                                    ))
                                })?;
                        }
                    }
                    self.stage_doc_update(
                        &catalog,
                        collection,
                        &prefix,
                        is_sql_rows,
                        sql_row_format,
                        &key,
                        &value,
                        old_doc,
                        doc,
                        &mut batch,
                    )?;
                    if batch.op_count() >= WriteBatch::MAX_OPS / 2 && !batch.is_empty() {
                        self.db.commit_batch(&batch)?;
                        batch = WriteBatch::new();
                        if default_value.is_some() {
                            crate::failpoints::maybe_fail("schema::add_field_after_partial_batch")
                                .map_err(|e| {
                                    OmniError::IoError(format!(
                                        "schema add_field failure point: {e}"
                                    ))
                                })?;
                        }
                    }
                }
            }
        }

        // Publish any trailing backfilled rows, SQL catalog column update, and
        // durable step-completion receipt atomically in the final WriteBatch.
        // For small tables/collections this commits the entire AddField step
        // atomically in one batch; for large multi-batch migrations the
        // intermediate row chunks are idempotent on retry and the catalog +
        // receipt flip atomically only once every row has been backfilled.
        let tail_ops =
            usize::from(sql_catalog_update.is_some()) + usize::from(done_receipt.is_some());
        if !batch.is_empty() && batch.op_count().saturating_add(tail_ops) > WriteBatch::MAX_OPS {
            self.db.commit_batch(&batch)?;
            batch = WriteBatch::new();
        }
        if let Some((cat_key, cat_val)) = sql_catalog_update {
            batch.set(cat_key, cat_val.to_string())?;
        }
        if let Some((done_key, done_val)) = done_receipt {
            batch.set(done_key, done_val.to_string())?;
        }
        if !batch.is_empty() {
            self.db.commit_batch(&batch)?;
        }

        Ok(())
    }

    fn backfill_document_field_with_receipt(
        &self,
        collection: &str,
        field_name: &str,
        default_value: Option<&serde_json::Value>,
        done_receipt: Option<(&str, &str)>,
    ) -> Result<(), OmniError> {
        let Some((doc_prefix, doc_end)) =
            collection_row_ranges_for_namespace(collection, IndexNamespace::Document)
                .into_iter()
                .next()
        else {
            return Ok(());
        };
        let tail_ops = usize::from(done_receipt.is_some());

        'rounds: for _ in 0..MAX_DOC_CONVERGENCE_ROUNDS {
            let snap = self.db.consistent_snapshot_guard()?;
            let round_seq = snap.sequence();
            if let Some((done_key, done_val)) = done_receipt
                && self.db.find(done_key, round_seq)?.as_deref() == Some(done_val)
            {
                return Ok(());
            }
            let catalog = self.index_manager.catalog_snapshot_at(round_seq)?;
            let max_doc_ops = 1 + 2 * catalog.indexes_for_collection(collection).len();
            let mut batch = WriteBatch::new();
            let mut chunk_doc_keys = Vec::new();
            let mut flushed_any = false;

            for (key, value) in self
                .db
                .scan_exclusive(&doc_prefix, Some(&doc_end), round_seq)?
            {
                let Ok(mut doc) = serde_json::from_str::<serde_json::Value>(&value) else {
                    continue;
                };
                let old_doc = doc.clone();
                let Some(obj) = doc.as_object_mut() else {
                    continue;
                };
                let changed = match default_value {
                    Some(val) if !obj.contains_key(field_name) => {
                        obj.insert(field_name.to_string(), val.clone());
                        true
                    }
                    Some(_) => false,
                    None => obj.remove(field_name).is_some(),
                };
                if !changed {
                    continue;
                }
                if !batch.is_empty()
                    && (batch.op_count() >= WriteBatch::MAX_OPS / 2
                        || batch.op_count().saturating_add(max_doc_ops) > WriteBatch::MAX_OPS)
                {
                    match self.db.commit_batch_if_keys_unchanged(
                        batch,
                        &chunk_doc_keys,
                        round_seq,
                    )? {
                        Ok(_) => {
                            flushed_any = true;
                            batch = WriteBatch::new();
                            chunk_doc_keys.clear();
                            if default_value.is_some() {
                                crate::failpoints::maybe_fail(
                                    "schema::add_field_after_partial_batch",
                                )
                                .map_err(|e| {
                                    OmniError::IoError(format!(
                                        "schema add_field failure point: {e}"
                                    ))
                                })?;
                            }
                        }
                        Err(()) => continue 'rounds,
                    }
                }
                self.stage_doc_update(
                    &catalog,
                    collection,
                    &doc_prefix,
                    false,
                    crate::catalog::RowFormat::default(),
                    &key,
                    &value,
                    old_doc,
                    doc,
                    &mut batch,
                )?;
                chunk_doc_keys.push(key);
                if batch.op_count() >= WriteBatch::MAX_OPS / 2 && !batch.is_empty() {
                    match self.db.commit_batch_if_keys_unchanged(
                        batch,
                        &chunk_doc_keys,
                        round_seq,
                    )? {
                        Ok(_) => {
                            flushed_any = true;
                            batch = WriteBatch::new();
                            chunk_doc_keys.clear();
                            if default_value.is_some() {
                                crate::failpoints::maybe_fail(
                                    "schema::add_field_after_partial_batch",
                                )
                                .map_err(|e| {
                                    OmniError::IoError(format!(
                                        "schema add_field failure point: {e}"
                                    ))
                                })?;
                            }
                        }
                        Err(()) => continue 'rounds,
                    }
                }
            }

            if flushed_any
                || (!batch.is_empty()
                    && batch.op_count().saturating_add(tail_ops) > WriteBatch::MAX_OPS)
            {
                if !batch.is_empty()
                    && self
                        .db
                        .commit_batch_if_keys_unchanged(batch, &chunk_doc_keys, round_seq)?
                        .is_ok()
                    && default_value.is_some()
                {
                    crate::failpoints::maybe_fail("schema::add_field_after_partial_batch")
                        .map_err(|e| {
                            OmniError::IoError(format!("schema add_field failure point: {e}"))
                        })?;
                }
                continue 'rounds;
            }

            if let Some((done_key, done_val)) = done_receipt {
                batch.set(done_key, done_val.to_string())?;
            }
            #[cfg(test)]
            run_doc_migration_hook("backfill_before_final_commit");
            if self
                .db
                .commit_batch_if_range_unchanged(batch, &doc_prefix, &doc_end, round_seq)?
                .is_ok()
            {
                return Ok(());
            }
        }

        Err(OmniError::IoError(format!(
            "document field migration for '{collection}.{field_name}' could not converge after {MAX_DOC_CONVERGENCE_ROUNDS} rounds due to concurrent writes; retry migration"
        )))
    }

    fn rename_field_online(
        &self,
        collection: &str,
        old_name: &str,
        new_name: &str,
        sql_catalog_update: Option<(&str, &str)>,
        done_key: &str,
        done_value: &str,
    ) -> Result<(), OmniError> {
        self.index_manager.refresh_catalog()?;
        if sql_catalog_update.is_none() {
            return self.rename_document_field_online(
                collection, old_name, new_name, done_key, done_value,
            );
        }
        let seq = self.db.get_seq();
        let catalog = self.index_manager.catalog_snapshot_at(seq)?;
        let sql_row_format = self
            .db
            .find(
                &format!("\x00CATALOG\x00{}", collection.to_lowercase()),
                seq,
            )?
            .and_then(|raw| serde_json::from_str::<crate::catalog::TableDef>(&raw).ok())
            .filter(|table| table.name == collection)
            .map(|table| table.row_format)
            .unwrap_or_default();

        let namespace = IndexNamespace::Sql;
        let ns_stage_key =
            Self::rename_stage_key_for_namespace(collection, namespace, old_name, new_name);
        let legacy_stage_key = Self::legacy_rename_stage_key(collection, old_name, new_name);
        let (stage_key, is_staged_retry) = if self.db.find(&ns_stage_key, seq)?.is_some() {
            (ns_stage_key, true)
        } else if self.db.find(&legacy_stage_key, seq)?.is_some()
            && Self::legacy_rename_stage_matches_namespace(
                &self.db,
                collection,
                &legacy_stage_key,
                namespace,
                seq,
            )?
        {
            (legacy_stage_key, true)
        } else {
            (ns_stage_key, false)
        };

        let row_ranges = collection_row_ranges_for_namespace(collection, namespace);

        for (prefix, end) in &row_ranges {
            for (key, value) in self.db.scan_exclusive(prefix, Some(end), seq)? {
                if let Ok(serde_json::Value::Object(obj)) = serde_json::from_str(&value)
                    && let (Some(old_val), Some(new_val)) = (obj.get(old_name), obj.get(new_name))
                {
                    let row_staged = is_staged_retry
                        && (old_val == new_val
                            || self
                                .db
                                .find(&Self::rename_row_stage_key(&stage_key, &key), seq)?
                                .is_some());
                    if !row_staged {
                        return Err(OmniError::IoError(format!(
                            "cannot rename field '{}' to '{}' because the destination exists",
                            old_name, new_name
                        )));
                    }
                }
            }
        }

        let max_doc_ops = 1 + 2 * catalog.indexes_for_collection(collection).len();
        let catalog_ops = usize::from(sql_catalog_update.is_some()) + 1;
        let single_batch_budget = WriteBatch::MAX_OPS.saturating_sub(catalog_ops);

        if !is_staged_retry {
            let mut single_batch = WriteBatch::new();
            let mut fits_in_one_batch = true;

            'ranges: for (prefix, end) in &row_ranges {
                let is_sql_rows = prefix.starts_with("\x00T\x00");
                for (key, value) in self.db.scan_exclusive(prefix, Some(end), seq)? {
                    let Ok(mut doc) = serde_json::from_str::<serde_json::Value>(&value) else {
                        continue;
                    };
                    let old_doc = doc.clone();
                    let Some(obj) = doc.as_object_mut() else {
                        continue;
                    };
                    if let Some(val) = obj.remove(old_name) {
                        if single_batch.op_count().saturating_add(max_doc_ops) > single_batch_budget
                        {
                            fits_in_one_batch = false;
                            break 'ranges;
                        }
                        obj.insert(new_name.to_string(), val);
                        self.stage_doc_update(
                            &catalog,
                            collection,
                            prefix,
                            is_sql_rows,
                            sql_row_format,
                            &key,
                            &value,
                            old_doc,
                            doc,
                            &mut single_batch,
                        )?;
                    }
                }
            }

            if fits_in_one_batch {
                if let Some((cat_key, cat_val)) = sql_catalog_update {
                    single_batch.set(cat_key, cat_val.to_string())?;
                }
                single_batch.set(done_key, done_value.to_string())?;
                self.db.commit_batch(&single_batch)?;
                return Ok(());
            }
        }

        // Large rename: stage the new field while keeping the old field readable,
        // switch the table catalog once backfilled, then remove the old field.
        let stage_doc_ops = max_doc_ops.saturating_add(1);
        let mut batch = WriteBatch::new();
        if !is_staged_retry {
            batch.set(&stage_key, old_name.to_string())?;
        }

        for (prefix, end) in &row_ranges {
            let is_sql_rows = prefix.starts_with("\x00T\x00");
            for (key, value) in self.db.scan_exclusive(prefix, Some(end), seq)? {
                let Ok(mut doc) = serde_json::from_str::<serde_json::Value>(&value) else {
                    continue;
                };
                let old_doc = doc.clone();
                let Some(obj) = doc.as_object_mut() else {
                    continue;
                };
                let Some(val) = obj.get(old_name).cloned() else {
                    continue;
                };
                let row_stage_key = Self::rename_row_stage_key(&stage_key, &key);
                if obj.get(new_name) == Some(&val) && self.db.find(&row_stage_key, seq)?.is_some() {
                    continue;
                }
                if !batch.is_empty()
                    && (batch.op_count() >= WriteBatch::MAX_OPS / 2
                        || batch.op_count().saturating_add(stage_doc_ops) > WriteBatch::MAX_OPS)
                {
                    self.db.commit_batch(&batch)?;
                    batch = WriteBatch::new();
                }
                obj.insert(new_name.to_string(), val);
                self.stage_doc_update(
                    &catalog,
                    collection,
                    prefix,
                    is_sql_rows,
                    sql_row_format,
                    &key,
                    &value,
                    old_doc,
                    doc,
                    &mut batch,
                )?;
                batch.set(&row_stage_key, "1".into())?;
                if batch.op_count() >= WriteBatch::MAX_OPS / 2 && !batch.is_empty() {
                    self.db.commit_batch(&batch)?;
                    batch = WriteBatch::new();
                }
            }
        }

        if !batch.is_empty() {
            self.db.commit_batch(&batch)?;
        }

        if let Some((cat_key, cat_val)) = sql_catalog_update {
            let mut cat_batch = WriteBatch::new();
            cat_batch.set(cat_key, cat_val.to_string())?;
            self.db.commit_batch(&cat_batch)?;
        }

        self.cleanup_staged_rename(
            collection, namespace, old_name, new_name, &stage_key, done_key, done_value,
        )
    }

    fn encode_doc_row_stage_marker(val: &serde_json::Value) -> Result<String, OmniError> {
        let json = serde_json::to_string(val).map_err(|e| OmniError::IoError(e.to_string()))?;
        Ok(format!("v2:{json}"))
    }

    fn decode_doc_row_stage_marker(raw: &str) -> Option<serde_json::Value> {
        raw.strip_prefix("v2:")
            .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
    }

    fn resolve_doc_rename_stage(
        &self,
        collection: &str,
        old_name: &str,
        new_name: &str,
        seq: u64,
    ) -> Result<(String, bool), OmniError> {
        let ns_stage_key = Self::rename_stage_key_for_namespace(
            collection,
            IndexNamespace::Document,
            old_name,
            new_name,
        );
        let legacy_stage_key = Self::legacy_rename_stage_key(collection, old_name, new_name);
        if self.db.find(&ns_stage_key, seq)?.is_some() {
            Ok((ns_stage_key, true))
        } else if self.db.find(&legacy_stage_key, seq)?.is_some()
            && Self::legacy_rename_stage_matches_namespace(
                &self.db,
                collection,
                &legacy_stage_key,
                IndexNamespace::Document,
                seq,
            )?
        {
            Ok((legacy_stage_key, true))
        } else {
            Ok((ns_stage_key, false))
        }
    }

    fn rename_document_field_online(
        &self,
        collection: &str,
        old_name: &str,
        new_name: &str,
        done_key: &str,
        done_value: &str,
    ) -> Result<(), OmniError> {
        let Some((doc_prefix, doc_end)) =
            collection_row_ranges_for_namespace(collection, IndexNamespace::Document)
                .into_iter()
                .next()
        else {
            return Ok(());
        };
        let single_batch_budget = WriteBatch::MAX_OPS.saturating_sub(1);
        let mut active_stage_key = None;

        // Phase 1: Attempt single-batch atomic rename when not already staged.
        for _ in 0..MAX_DOC_CONVERGENCE_ROUNDS {
            let snap = self.db.consistent_snapshot_guard()?;
            let round_seq = snap.sequence();
            let (stage_key, is_staged_retry) =
                self.resolve_doc_rename_stage(collection, old_name, new_name, round_seq)?;
            if !is_staged_retry && self.db.find(done_key, round_seq)?.as_deref() == Some(done_value)
            {
                return Ok(());
            }
            if is_staged_retry {
                active_stage_key = Some(stage_key);
                break;
            }
            let catalog = self.index_manager.catalog_snapshot_at(round_seq)?;
            let max_doc_ops = 1 + 2 * catalog.indexes_for_collection(collection).len();
            let scanned = self
                .db
                .scan_exclusive(&doc_prefix, Some(&doc_end), round_seq)?;
            for (_, value) in &scanned {
                if let Ok(serde_json::Value::Object(obj)) = serde_json::from_str(value)
                    && obj.contains_key(old_name)
                    && obj.contains_key(new_name)
                {
                    return Err(OmniError::IoError(format!(
                        "cannot rename field '{}' to '{}' because the destination exists",
                        old_name, new_name
                    )));
                }
            }
            let mut single_batch = WriteBatch::new();
            let mut fits_in_one_batch = true;
            for (key, value) in scanned {
                let Ok(mut doc) = serde_json::from_str::<serde_json::Value>(&value) else {
                    continue;
                };
                let old_doc = doc.clone();
                let Some(obj) = doc.as_object_mut() else {
                    continue;
                };
                if let Some(val) = obj.remove(old_name) {
                    if single_batch.op_count().saturating_add(max_doc_ops) > single_batch_budget {
                        fits_in_one_batch = false;
                        break;
                    }
                    obj.insert(new_name.to_string(), val);
                    self.stage_doc_update(
                        &catalog,
                        collection,
                        &doc_prefix,
                        false,
                        crate::catalog::RowFormat::default(),
                        &key,
                        &value,
                        old_doc,
                        doc,
                        &mut single_batch,
                    )?;
                }
            }
            if !fits_in_one_batch {
                active_stage_key = Some(stage_key);
                break;
            }
            single_batch.set(done_key, done_value.to_string())?;
            #[cfg(test)]
            run_doc_migration_hook("rename_single_before_commit");
            if self
                .db
                .commit_batch_if_range_unchanged(single_batch, &doc_prefix, &doc_end, round_seq)?
                .is_ok()
            {
                return Ok(());
            }
        }

        let Some(stage_key) = active_stage_key else {
            return Err(OmniError::IoError(format!(
                "document rename '{collection}.{old_name}->{new_name}' could not converge after {MAX_DOC_CONVERGENCE_ROUNDS} rounds due to concurrent writes; retry migration"
            )));
        };

        // Phase 2: Staged multi-batch rename using per-key OCC guards so
        // intermediate staging chunks never clobber concurrent doc updates.
        let mut staged_cleanly = false;
        'staging: for _ in 0..MAX_DOC_CONVERGENCE_ROUNDS {
            let snap = self.db.consistent_snapshot_guard()?;
            let round_seq = snap.sequence();
            let stage_present = self.db.find(&stage_key, round_seq)?.is_some();
            let catalog = self.index_manager.catalog_snapshot_at(round_seq)?;
            let stage_doc_ops = 2 + 2 * catalog.indexes_for_collection(collection).len();
            let mut batch = WriteBatch::new();
            let mut chunk_doc_keys = Vec::new();
            if !stage_present {
                batch.set(&stage_key, old_name.to_string())?;
            }

            for (key, value) in self
                .db
                .scan_exclusive(&doc_prefix, Some(&doc_end), round_seq)?
            {
                let Ok(mut doc) = serde_json::from_str::<serde_json::Value>(&value) else {
                    continue;
                };
                let old_doc = doc.clone();
                let Some(obj) = doc.as_object_mut() else {
                    continue;
                };
                let Some(val) = obj.get(old_name).cloned() else {
                    continue;
                };
                let row_stage_key = Self::rename_row_stage_key(&stage_key, &key);
                let marker_raw = self.db.find(&row_stage_key, round_seq)?;
                let is_legacy_marker = marker_raw.as_deref() == Some("1");
                let staged_val = marker_raw
                    .as_deref()
                    .and_then(Self::decode_doc_row_stage_marker);
                if let Some(existing_new) = obj.get(new_name) {
                    if existing_new == &val {
                        if staged_val.as_ref() == Some(&val) {
                            continue;
                        }
                        if marker_raw.is_none() && !stage_present {
                            return Err(OmniError::IoError(format!(
                                "cannot rename field '{}' to '{}' because the destination exists",
                                old_name, new_name
                            )));
                        }
                    } else if staged_val.as_ref() == Some(&val) {
                        // `old_name` still holds the staged baseline while
                        // `new_name` received a concurrent update: keep it.
                        continue;
                    } else if staged_val.as_ref() != Some(existing_new) && !is_legacy_marker {
                        return Err(OmniError::IoError(format!(
                            "cannot rename field '{}' to '{}' because the destination exists or diverged concurrently",
                            old_name, new_name
                        )));
                    }
                    // Otherwise either `existing_new == staged_val != val` (a
                    // concurrent old-schema writer updated `old_name`) or this
                    // row carries a pre-upgrade legacy `"1"` marker whose
                    // source field changed after staging: fall through to
                    // promote `old_name` into `new_name` and record `v2:<val>`.
                }
                if !batch.is_empty()
                    && (batch.op_count() >= WriteBatch::MAX_OPS / 2
                        || batch.op_count().saturating_add(stage_doc_ops) > WriteBatch::MAX_OPS)
                {
                    match self.db.commit_batch_if_keys_unchanged(
                        batch,
                        &chunk_doc_keys,
                        round_seq,
                    )? {
                        Ok(_) => {
                            batch = WriteBatch::new();
                            chunk_doc_keys.clear();
                        }
                        Err(()) => continue 'staging,
                    }
                }
                let marker_value = Self::encode_doc_row_stage_marker(&val)?;
                obj.insert(new_name.to_string(), val);
                self.stage_doc_update(
                    &catalog,
                    collection,
                    &doc_prefix,
                    false,
                    crate::catalog::RowFormat::default(),
                    &key,
                    &value,
                    old_doc,
                    doc,
                    &mut batch,
                )?;
                batch.set(&row_stage_key, marker_value)?;
                chunk_doc_keys.push(key);
                if batch.op_count() >= WriteBatch::MAX_OPS / 2 && !batch.is_empty() {
                    match self.db.commit_batch_if_keys_unchanged(
                        batch,
                        &chunk_doc_keys,
                        round_seq,
                    )? {
                        Ok(_) => {
                            batch = WriteBatch::new();
                            chunk_doc_keys.clear();
                        }
                        Err(()) => continue 'staging,
                    }
                }
            }

            if !batch.is_empty()
                && self
                    .db
                    .commit_batch_if_keys_unchanged(batch, &chunk_doc_keys, round_seq)?
                    .is_err()
            {
                continue 'staging;
            }
            staged_cleanly = true;
            break;
        }
        if !staged_cleanly {
            return Err(OmniError::IoError(format!(
                "document rename staging for '{collection}.{old_name}->{new_name}' could not converge after {MAX_DOC_CONVERGENCE_ROUNDS} rounds due to concurrent writes; retry migration"
            )));
        }

        #[cfg(test)]
        run_doc_migration_hook("rename_after_staging_before_cleanup");
        self.cleanup_document_staged_rename(
            collection, old_name, new_name, &stage_key, done_key, done_value,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn cleanup_staged_rename(
        &self,
        collection: &str,
        namespace: IndexNamespace,
        old_name: &str,
        new_name: &str,
        stage_key: &str,
        done_key: &str,
        done_value: &str,
    ) -> Result<(), OmniError> {
        self.index_manager.refresh_catalog()?;
        if namespace == IndexNamespace::Document {
            return self.cleanup_document_staged_rename(
                collection, old_name, new_name, stage_key, done_key, done_value,
            );
        }
        let seq = self.db.get_seq();
        let catalog = self.index_manager.catalog_snapshot_at(seq)?;
        let sql_row_format = self
            .db
            .find(
                &format!("\x00CATALOG\x00{}", collection.to_lowercase()),
                seq,
            )?
            .and_then(|raw| serde_json::from_str::<crate::catalog::TableDef>(&raw).ok())
            .filter(|table| table.name == collection)
            .map(|table| table.row_format)
            .unwrap_or_default();

        let mut batch = WriteBatch::new();
        let cleanup_doc_ops = 2 + 2 * catalog.indexes_for_collection(collection).len();

        for (prefix, end) in collection_row_ranges_for_namespace(collection, namespace) {
            let is_sql_rows = prefix.starts_with("\x00T\x00");
            for (key, value) in self.db.scan_exclusive(&prefix, Some(&end), seq)? {
                let Ok(mut doc) = serde_json::from_str::<serde_json::Value>(&value) else {
                    continue;
                };
                let old_doc = doc.clone();
                let Some(obj) = doc.as_object_mut() else {
                    continue;
                };
                if let Some(old_val) = obj.remove(old_name) {
                    if !obj.contains_key(new_name) {
                        obj.insert(new_name.to_string(), old_val);
                    } else if obj.get(new_name) != Some(&old_val)
                        && self
                            .db
                            .find(&Self::rename_row_stage_key(stage_key, &key), seq)?
                            .is_none()
                    {
                        return Err(OmniError::IoError(format!(
                            "cannot rename field '{}' to '{}' because the destination exists",
                            old_name, new_name
                        )));
                    }
                    if !batch.is_empty()
                        && (batch.op_count() >= WriteBatch::MAX_OPS / 2
                            || batch.op_count().saturating_add(cleanup_doc_ops)
                                > WriteBatch::MAX_OPS)
                    {
                        self.db.commit_batch(&batch)?;
                        batch = WriteBatch::new();
                    }
                    self.stage_doc_update(
                        &catalog,
                        collection,
                        &prefix,
                        is_sql_rows,
                        sql_row_format,
                        &key,
                        &value,
                        old_doc,
                        doc,
                        &mut batch,
                    )?;
                    batch.delete(&Self::rename_row_stage_key(stage_key, &key))?;
                    if batch.op_count() >= WriteBatch::MAX_OPS / 2 && !batch.is_empty() {
                        self.db.commit_batch(&batch)?;
                        batch = WriteBatch::new();
                    }
                }
            }
        }

        let row_marker_prefix = Self::rename_row_stage_prefix(stage_key);
        let latest_seq = self.db.get_seq();
        for (marker_key, _) in self.db.scan_prefix(&row_marker_prefix, latest_seq)? {
            if batch
                .buffered_deletes
                .iter()
                .any(|k| k.as_slice() == marker_key.as_bytes())
            {
                continue;
            }
            if batch.op_count() >= WriteBatch::MAX_OPS / 2 && !batch.is_empty() {
                self.db.commit_batch(&batch)?;
                batch = WriteBatch::new();
            }
            batch.delete(&marker_key)?;
        }

        if batch.op_count().saturating_add(2) > WriteBatch::MAX_OPS {
            self.db.commit_batch(&batch)?;
            batch = WriteBatch::new();
        }
        batch.set(done_key, done_value.to_string())?;
        batch.delete(stage_key)?;
        self.db.commit_batch(&batch)?;
        Ok(())
    }

    fn cleanup_document_staged_rename(
        &self,
        collection: &str,
        old_name: &str,
        new_name: &str,
        stage_key: &str,
        done_key: &str,
        done_value: &str,
    ) -> Result<(), OmniError> {
        let Some((doc_prefix, doc_end)) =
            collection_row_ranges_for_namespace(collection, IndexNamespace::Document)
                .into_iter()
                .next()
        else {
            return Ok(());
        };
        let row_marker_prefix = Self::rename_row_stage_prefix(stage_key);

        'rounds: for _ in 0..MAX_DOC_CONVERGENCE_ROUNDS {
            let snap = self.db.consistent_snapshot_guard()?;
            let round_seq = snap.sequence();
            let catalog = self.index_manager.catalog_snapshot_at(round_seq)?;
            let cleanup_doc_ops = 1 + 2 * catalog.indexes_for_collection(collection).len();
            let mut batch = WriteBatch::new();
            let mut chunk_doc_keys = Vec::new();
            let mut flushed_any = false;
            let mut cleaned_any_doc = false;

            for (key, value) in self
                .db
                .scan_exclusive(&doc_prefix, Some(&doc_end), round_seq)?
            {
                let Ok(mut doc) = serde_json::from_str::<serde_json::Value>(&value) else {
                    continue;
                };
                let old_doc = doc.clone();
                let Some(obj) = doc.as_object_mut() else {
                    continue;
                };
                let Some(old_val) = obj.remove(old_name) else {
                    continue;
                };
                cleaned_any_doc = true;
                let row_stage_key = Self::rename_row_stage_key(stage_key, &key);
                let marker_raw = self.db.find(&row_stage_key, round_seq)?;
                let is_legacy_marker = marker_raw.as_deref() == Some("1");
                let staged_val = marker_raw
                    .as_deref()
                    .and_then(Self::decode_doc_row_stage_marker);
                if !obj.contains_key(new_name) {
                    obj.insert(new_name.to_string(), old_val);
                } else if obj.get(new_name) != Some(&old_val) {
                    if staged_val.as_ref() == obj.get(new_name) || is_legacy_marker {
                        // Either `new_name` still holds the staged baseline
                        // while a concurrent writer updated `old_name`, or a
                        // pre-upgrade legacy `"1"` marker saw `old_name`
                        // change after staging: promote the newer source value.
                        obj.insert(new_name.to_string(), old_val);
                    } else if staged_val.as_ref() == Some(&old_val) {
                        // `old_name` still holds the staged copy while a
                        // concurrent writer updated `new_name`: keep the
                        // newer destination value and drop `old_name`.
                    } else {
                        return Err(OmniError::IoError(format!(
                            "cannot rename field '{}' to '{}' because the destination exists or diverged concurrently",
                            old_name, new_name
                        )));
                    }
                }
                if !batch.is_empty()
                    && (batch.op_count() >= WriteBatch::MAX_OPS / 2
                        || batch.op_count().saturating_add(cleanup_doc_ops) > WriteBatch::MAX_OPS)
                {
                    match self.db.commit_batch_if_keys_unchanged(
                        batch,
                        &chunk_doc_keys,
                        round_seq,
                    )? {
                        Ok(_) => {
                            flushed_any = true;
                            batch = WriteBatch::new();
                            chunk_doc_keys.clear();
                        }
                        Err(()) => continue 'rounds,
                    }
                }
                self.stage_doc_update(
                    &catalog,
                    collection,
                    &doc_prefix,
                    false,
                    crate::catalog::RowFormat::default(),
                    &key,
                    &value,
                    old_doc,
                    doc,
                    &mut batch,
                )?;
                chunk_doc_keys.push(key);
                if batch.op_count() >= WriteBatch::MAX_OPS / 2 && !batch.is_empty() {
                    match self.db.commit_batch_if_keys_unchanged(
                        batch,
                        &chunk_doc_keys,
                        round_seq,
                    )? {
                        Ok(_) => {
                            flushed_any = true;
                            batch = WriteBatch::new();
                            chunk_doc_keys.clear();
                        }
                        Err(()) => continue 'rounds,
                    }
                }
            }

            // Retain row stage markers while any document still retains
            // `old_name` so a concurrent writer whose read observed a staged
            // row right before its cleanup chunk can still be reconciled via
            // its `v2:<json>` baseline. Sweep markers once a round sees zero
            // remaining `old_name` documents; if markers are already gone and
            // everything fits in one batch, commit atomically below.
            let defer_final_for_markers = if cleaned_any_doc {
                !self
                    .db
                    .scan_prefix_limit(&row_marker_prefix, round_seq, 1)?
                    .is_empty()
            } else {
                for (marker_key, _) in self.db.scan_prefix(&row_marker_prefix, round_seq)? {
                    if batch
                        .buffered_deletes
                        .iter()
                        .any(|k| k.as_slice() == marker_key.as_bytes())
                    {
                        continue;
                    }
                    if batch.op_count() >= WriteBatch::MAX_OPS / 2 && !batch.is_empty() {
                        match self.db.commit_batch_if_keys_unchanged(
                            batch,
                            &chunk_doc_keys,
                            round_seq,
                        )? {
                            Ok(_) => {
                                flushed_any = true;
                                batch = WriteBatch::new();
                                chunk_doc_keys.clear();
                            }
                            Err(()) => continue 'rounds,
                        }
                    }
                    batch.delete(&marker_key)?;
                }
                false
            };

            if defer_final_for_markers
                || flushed_any
                || batch.op_count().saturating_add(2) > WriteBatch::MAX_OPS
            {
                if !batch.is_empty() {
                    let _ = self.db.commit_batch_if_keys_unchanged(
                        batch,
                        &chunk_doc_keys,
                        round_seq,
                    )?;
                }
                continue 'rounds;
            }

            batch.set(done_key, done_value.to_string())?;
            batch.delete(stage_key)?;
            #[cfg(test)]
            run_doc_migration_hook("rename_cleanup_before_final_commit");
            if self
                .db
                .commit_batch_if_range_unchanged(batch, &doc_prefix, &doc_end, round_seq)?
                .is_ok()
            {
                return Ok(());
            }
        }

        Err(OmniError::IoError(format!(
            "document rename cleanup for '{collection}.{old_name}->{new_name}' could not converge after {MAX_DOC_CONVERGENCE_ROUNDS} rounds due to concurrent writes; retry migration"
        )))
    }

    /// Persists a migration record.
    fn save_migration_record(&self, record: &MigrationRecord) -> Result<(), OmniError> {
        let key = format!("{}{:06}", MIGRATION_PREFIX, record.version);
        let value = serde_json::to_string(record).map_err(|e| OmniError::IoError(e.to_string()))?;
        let mut batch = WriteBatch::new();
        batch.set(&key, value)?;
        self.db.commit_batch(&batch)?;
        Ok(())
    }

    /// Publishes a migration state and schema version atomically.
    ///
    /// Receipts are scoped to `attempt_id`. When all receipt deletions fit in
    /// the publication batch they commit atomically alongside the terminal
    /// state; otherwise the terminal migration state, schema version, and
    /// attempt-token removal are published atomically FIRST, and the retired
    /// attempt's receipts are trimmed afterward so a mid-cleanup crash can
    /// never delete receipts ahead of terminal version publication.
    fn save_applied_version(
        &self,
        record: &MigrationRecord,
        version: Version,
        attempt_id: &str,
    ) -> Result<(), OmniError> {
        let key = format!("{}{:06}", MIGRATION_PREFIX, record.version);
        let value = serde_json::to_string(record).map_err(|e| OmniError::IoError(e.to_string()))?;
        let direction = match record.state {
            MigrationState::RolledBack => "down",
            _ => "up",
        };
        let attempt_key = Self::migration_attempt_key(record.version, direction);
        let seq = self.db.get_seq();
        let mut receipt_keys = Vec::new();
        for done_prefix in [
            Self::create_index_done_prefix(record.version, direction, attempt_id),
            Self::drop_index_done_prefix(record.version, direction, attempt_id),
            Self::add_done_prefix(record.version, direction, attempt_id),
            Self::rename_done_prefix(record.version, direction, attempt_id),
            Self::remove_done_prefix(record.version, direction, attempt_id),
            Self::doc_remove_done_prefix(record.version, direction, attempt_id),
        ] {
            for (done_key, _) in self.db.scan_prefix(&done_prefix, seq)? {
                receipt_keys.push(done_key);
            }
        }

        if receipt_keys.len().saturating_add(3) <= WriteBatch::MAX_OPS {
            let mut batch = WriteBatch::new();
            for done_key in receipt_keys {
                batch.delete(&done_key)?;
            }
            batch.delete(&attempt_key)?;
            batch.set(&key, value)?;
            batch.set(VERSION_KEY, version.to_string())?;
            self.db.commit_batch(&batch)?;
            return Ok(());
        }

        // Publish terminal migration state + new schema version + clear
        // active attempt token atomically BEFORE trimming any receipts.
        let mut publish_batch = WriteBatch::new();
        publish_batch.delete(&attempt_key)?;
        publish_batch.set(&key, value)?;
        publish_batch.set(VERSION_KEY, version.to_string())?;
        self.db.commit_batch(&publish_batch)?;

        // Best-effort post-publication cleanup of retired attempt receipts.
        let mut cleanup_batch = WriteBatch::new();
        for done_key in receipt_keys {
            if cleanup_batch.op_count() >= WriteBatch::MAX_OPS {
                let _ = self.db.commit_batch(&cleanup_batch);
                cleanup_batch = WriteBatch::new();
            }
            if cleanup_batch.delete(&done_key).is_err() {
                break;
            }
        }
        if !cleanup_batch.is_empty() {
            let _ = self.db.commit_batch(&cleanup_batch);
        }
        Ok(())
    }
}

/// Returns current timestamp in milliseconds.
fn timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Helper: builds a migration for creating an index.
pub fn create_index_migration(
    version: Version,
    description: &str,
    index_name: &str,
    collection: &str,
    fields: Vec<(String, IndexFieldType)>,
    unique: bool,
) -> Migration {
    Migration {
        version,
        description: description.to_string(),
        up: vec![SchemaOp::CreateIndex {
            name: index_name.to_string(),
            collection: collection.to_string(),
            fields: fields.clone(),
            unique,
        }],
        down: vec![SchemaOp::DropIndex {
            name: index_name.to_string(),
        }],
    }
}

/// Helper: builds a migration for adding a field with a default value.
pub fn add_field_migration(
    version: Version,
    description: &str,
    collection: &str,
    field_name: &str,
    default_value: serde_json::Value,
) -> Migration {
    Migration {
        version,
        description: description.to_string(),
        up: vec![SchemaOp::AddField {
            collection: collection.to_string(),
            field_name: field_name.to_string(),
            default_value: default_value.clone(),
        }],
        down: vec![SchemaOp::RemoveField {
            collection: collection.to_string(),
            field_name: field_name.to_string(),
        }],
    }
}

/// Helper: builds a migration for renaming a field.
pub fn rename_field_migration(
    version: Version,
    description: &str,
    collection: &str,
    old_name: &str,
    new_name: &str,
) -> Migration {
    Migration {
        version,
        description: description.to_string(),
        up: vec![SchemaOp::RenameField {
            collection: collection.to_string(),
            old_name: old_name.to_string(),
            new_name: new_name.to_string(),
        }],
        down: vec![SchemaOp::RenameField {
            collection: collection.to_string(),
            old_name: new_name.to_string(),
            new_name: old_name.to_string(),
        }],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
    use tempfile::TempDir;

    static DOC_MIGRATION_TEST_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct HookGuard {
        _serial: std::sync::MutexGuard<'static, ()>,
    }

    impl HookGuard {
        fn install(hook: impl FnMut(&str) + Send + 'static) -> Self {
            let serial = DOC_MIGRATION_TEST_SERIAL
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let mut slot = DOC_MIGRATION_HOOK
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *slot = Some(Box::new(hook));
            drop(slot);
            Self { _serial: serial }
        }
    }

    impl Drop for HookGuard {
        fn drop(&mut self) {
            let mut slot = DOC_MIGRATION_HOOK
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *slot = None;
        }
    }

    fn open_test_db() -> (Arc<OmniKV>, TempDir, SchemaManager) {
        let dir = TempDir::new().expect("tempdir");
        let db = OmniKV::open(
            dir.path().join("manifest.json").to_str().expect("manifest"),
            dir.path().join("wal.bin").to_str().expect("wal"),
        )
        .expect("open db");
        let schema = SchemaManager::new(db.clone(), Arc::new(IndexManager::new(db.clone())));
        (db, dir, schema)
    }

    fn read_doc(db: &OmniKV, key: &str) -> serde_json::Value {
        let raw = db
            .find(key, db.get_seq())
            .expect("find")
            .unwrap_or_else(|| panic!("missing doc {key}"));
        serde_json::from_str(&raw).expect("valid json")
    }

    #[test]
    fn doc_rename_preserves_concurrent_insert_between_staging_and_cleanup_and_at_final_barrier() {
        let (db, _dir, schema) = open_test_db();
        let initial_count = WriteBatch::MAX_OPS + 10;
        let mut seed = WriteBatch::new();
        for i in 0..initial_count {
            seed.set(
                &format!("ren_doc:d{i:04}"),
                format!(r#"{{"old_f":"v{i}"}}"#),
            )
            .expect("set");
            if seed.op_count() >= 500 {
                db.commit_batch(&seed).expect("commit seed");
                seed = WriteBatch::new();
            }
        }
        if !seed.is_empty() {
            db.commit_batch(&seed).expect("commit tail seed");
        }

        let db_hook = db.clone();
        let injected_mid = Arc::new(AtomicBool::new(false));
        let injected_barrier = Arc::new(AtomicBool::new(false));
        let mid_flag = injected_mid.clone();
        let barrier_flag = injected_barrier.clone();
        let _hook = HookGuard::install(move |point| {
            if point == "rename_after_staging_before_cleanup"
                && !mid_flag.swap(true, AtomicOrdering::SeqCst)
            {
                let mut wb = WriteBatch::new();
                wb.set(
                    "ren_doc:concurrent_mid",
                    r#"{"old_f":"mid_val","keep":1}"#.into(),
                )
                .expect("set mid");
                wb.set(
                    "ren_doc:d0005",
                    r#"{"old_f":"overwritten_mid","keep":2}"#.into(),
                )
                .expect("set overwrite");
                db_hook.commit_batch(&wb).expect("commit mid");
            } else if point == "rename_cleanup_before_final_commit"
                && !barrier_flag.swap(true, AtomicOrdering::SeqCst)
            {
                let mut wb = WriteBatch::new();
                wb.set(
                    "ren_doc:concurrent_barrier",
                    r#"{"old_f":"barrier_val","keep":3}"#.into(),
                )
                .expect("set barrier");
                wb.set(
                    "ren_doc:d0000",
                    r#"{"new_f":"edited_at_barrier","extra":true}"#.into(),
                )
                .expect("set edit");
                db_hook.commit_batch(&wb).expect("commit barrier");
            }
        });

        let mig = rename_field_migration(1, "doc rename", "ren_doc", "old_f", "new_f");
        schema.migrate_up(&mig).expect("migrate_up");

        assert!(injected_mid.load(AtomicOrdering::SeqCst));
        assert!(injected_barrier.load(AtomicOrdering::SeqCst));

        let mid = read_doc(&db, "ren_doc:concurrent_mid");
        assert_eq!(mid.get("old_f"), None);
        assert_eq!(mid.get("new_f"), Some(&serde_json::json!("mid_val")));
        assert_eq!(mid.get("keep"), Some(&serde_json::json!(1)));

        let d5 = read_doc(&db, "ren_doc:d0005");
        assert_eq!(d5.get("old_f"), None);
        assert_eq!(d5.get("new_f"), Some(&serde_json::json!("overwritten_mid")));
        assert_eq!(d5.get("keep"), Some(&serde_json::json!(2)));

        let barrier = read_doc(&db, "ren_doc:concurrent_barrier");
        assert_eq!(barrier.get("old_f"), None);
        assert_eq!(
            barrier.get("new_f"),
            Some(&serde_json::json!("barrier_val"))
        );
        assert_eq!(barrier.get("keep"), Some(&serde_json::json!(3)));

        let d0 = read_doc(&db, "ren_doc:d0000");
        assert_eq!(d0.get("old_f"), None);
        assert_eq!(
            d0.get("new_f"),
            Some(&serde_json::json!("edited_at_barrier"))
        );
        assert_eq!(d0.get("extra"), Some(&serde_json::json!(true)));

        let seq = db.get_seq();
        let remaining_docs = db
            .scan_exclusive("ren_doc:", Some("ren_doc;"), seq)
            .expect("scan");
        assert_eq!(remaining_docs.len(), initial_count + 2);
        for (k, raw) in remaining_docs {
            let doc: serde_json::Value = serde_json::from_str(&raw).expect("json");
            assert_eq!(doc.get("old_f"), None, "doc {k} still has old_f");
            assert!(doc.get("new_f").is_some(), "doc {k} lost new_f");
        }
        assert_eq!(
            db.scan_prefix("\x00SCHEMA\x00rename_stage:", seq)
                .expect("stages")
                .len(),
            0
        );
        assert_eq!(
            db.scan_prefix("\x00SCHEMA\x00rename_row:", seq)
                .expect("markers")
                .len(),
            0
        );
    }

    #[test]
    fn doc_add_remove_and_small_rename_converge_across_final_commit_barrier() {
        let (db, _dir, schema) = open_test_db();
        let mut seed = WriteBatch::new();
        seed.set("small_doc:d1", r#"{"note":"orig"}"#.into())
            .expect("set d1");
        db.commit_batch(&seed).expect("commit seed");

        // 1. AddField interrupted right before final barrier by an insert + update.
        let db_add = db.clone();
        let add_fired = Arc::new(AtomicBool::new(false));
        let add_flag = add_fired.clone();
        {
            let _hook = HookGuard::install(move |point| {
                if point == "backfill_before_final_commit"
                    && !add_flag.swap(true, AtomicOrdering::SeqCst)
                {
                    let mut wb = WriteBatch::new();
                    wb.set("small_doc:late_add", r#"{"note":"new_doc"}"#.into())
                        .expect("set late_add");
                    wb.set("small_doc:d1", r#"{"note":"updated_during_add"}"#.into())
                        .expect("set d1");
                    db_add.commit_batch(&wb).expect("commit concurrent add");
                }
            });
            let m1 = add_field_migration(1, "add old_f", "small_doc", "old_f", "def".into());
            schema.migrate_up(&m1).expect("add migrate_up");
        }
        assert!(add_fired.load(AtomicOrdering::SeqCst));
        assert_eq!(
            read_doc(&db, "small_doc:d1"),
            serde_json::json!({"note": "updated_during_add", "old_f": "def"})
        );
        assert_eq!(
            read_doc(&db, "small_doc:late_add"),
            serde_json::json!({"note": "new_doc", "old_f": "def"})
        );

        // 2. Small single-batch RenameField interrupted right before final barrier.
        let db_ren = db.clone();
        let ren_fired = Arc::new(AtomicBool::new(false));
        let ren_flag = ren_fired.clone();
        {
            let _hook = HookGuard::install(move |point| {
                if point == "rename_single_before_commit"
                    && !ren_flag.swap(true, AtomicOrdering::SeqCst)
                {
                    let mut wb = WriteBatch::new();
                    wb.set("small_doc:late_ren", r#"{"old_f":"late_v"}"#.into())
                        .expect("set late_ren");
                    db_ren.commit_batch(&wb).expect("commit concurrent ren");
                }
            });
            let m2 = rename_field_migration(2, "rename small", "small_doc", "old_f", "new_f");
            schema.migrate_up(&m2).expect("rename migrate_up");
        }
        assert!(ren_fired.load(AtomicOrdering::SeqCst));
        assert_eq!(
            read_doc(&db, "small_doc:late_ren"),
            serde_json::json!({"new_f": "late_v"})
        );
        assert_eq!(read_doc(&db, "small_doc:d1").get("old_f"), None);

        // 3. RemoveField interrupted right before final barrier.
        let db_remove = db.clone();
        let remove_fired = Arc::new(AtomicBool::new(false));
        let remove_flag = remove_fired.clone();
        {
            let _hook = HookGuard::install(move |point| {
                if point == "backfill_before_final_commit"
                    && !remove_flag.swap(true, AtomicOrdering::SeqCst)
                {
                    let mut wb = WriteBatch::new();
                    wb.set(
                        "small_doc:late_rem",
                        r#"{"new_f":"must_drop","keep":9}"#.into(),
                    )
                    .expect("set late_rem");
                    db_remove.commit_batch(&wb).expect("commit concurrent rem");
                }
            });
            let m3 = Migration {
                version: 3,
                description: "remove new_f".into(),
                up: vec![SchemaOp::RemoveField {
                    collection: "small_doc".into(),
                    field_name: "new_f".into(),
                }],
                down: vec![],
            };
            schema.migrate_up(&m3).expect("remove migrate_up");
        }
        assert!(remove_fired.load(AtomicOrdering::SeqCst));
        assert_eq!(
            read_doc(&db, "small_doc:late_rem"),
            serde_json::json!({"keep": 9})
        );
    }

    #[test]
    fn doc_rename_cleanup_rejects_unstaged_concurrent_destination_conflict() {
        let (db, _dir, schema) = open_test_db();
        let initial_count = WriteBatch::MAX_OPS + 5;
        let mut seed = WriteBatch::new();
        for i in 0..initial_count {
            seed.set(
                &format!("conf_doc:d{i:04}"),
                format!(r#"{{"old_f":"v{i}"}}"#),
            )
            .expect("set");
            if seed.op_count() >= 500 {
                db.commit_batch(&seed).expect("commit seed");
                seed = WriteBatch::new();
            }
        }
        if !seed.is_empty() {
            db.commit_batch(&seed).expect("commit tail seed");
        }

        let db_hook = db.clone();
        let fired = Arc::new(AtomicBool::new(false));
        let flag = fired.clone();
        let _hook = HookGuard::install(move |point| {
            if point == "rename_after_staging_before_cleanup"
                && !flag.swap(true, AtomicOrdering::SeqCst)
            {
                let mut wb = WriteBatch::new();
                wb.set(
                    "conf_doc:conflict",
                    r#"{"old_f":"src","new_f":"existing_dest"}"#.into(),
                )
                .expect("set conflict");
                db_hook.commit_batch(&wb).expect("commit conflict");
            }
        });

        let mig = rename_field_migration(1, "rename conflict", "conf_doc", "old_f", "new_f");
        let err = schema
            .migrate_up(&mig)
            .expect_err("unstaged destination conflict must fail closed");
        assert!(
            err.to_string().contains("destination exists"),
            "unexpected error: {err}"
        );
        assert_eq!(
            read_doc(&db, "conf_doc:conflict"),
            serde_json::json!({"old_f": "src", "new_f": "existing_dest"})
        );
    }

    #[test]
    fn doc_rename_preserves_same_row_old_and_new_field_updates_between_staging_and_cleanup() {
        let (db, _dir, schema) = open_test_db();
        let initial_count = WriteBatch::MAX_OPS + 10;
        let mut seed = WriteBatch::new();
        for i in 0..initial_count {
            seed.set(&format!("upd_doc:d{i:04}"), format!(r#"{{"old_f":{i}}}"#))
                .expect("set");
            if seed.op_count() >= 500 {
                db.commit_batch(&seed).expect("commit seed");
                seed = WriteBatch::new();
            }
        }
        if !seed.is_empty() {
            db.commit_batch(&seed).expect("commit tail seed");
        }

        let db_hook = db.clone();
        let fired = Arc::new(AtomicBool::new(false));
        let flag = fired.clone();
        let _hook = HookGuard::install(move |point| {
            if point == "rename_after_staging_before_cleanup"
                && !flag.swap(true, AtomicOrdering::SeqCst)
            {
                let mut wb = WriteBatch::new();
                // Old-schema concurrent writer updates `old_f` on already-staged row (`new_f` still 1).
                wb.set("upd_doc:d0001", r#"{"old_f":2,"new_f":1}"#.into())
                    .expect("set d0001");
                // New-schema concurrent writer updates `new_f` on already-staged row (`old_f` still 2).
                wb.set("upd_doc:d0002", r#"{"old_f":2,"new_f":200}"#.into())
                    .expect("set d0002");
                db_hook.commit_batch(&wb).expect("commit updates");
            }
        });

        let mig = rename_field_migration(1, "preserve updates", "upd_doc", "old_f", "new_f");
        schema.migrate_up(&mig).expect("migrate_up");
        assert!(fired.load(AtomicOrdering::SeqCst));

        assert_eq!(
            read_doc(&db, "upd_doc:d0001"),
            serde_json::json!({"new_f": 2}),
            "concurrent update to old_f on an already-staged row must be preserved"
        );
        assert_eq!(
            read_doc(&db, "upd_doc:d0002"),
            serde_json::json!({"new_f": 200}),
            "concurrent update to new_f on an already-staged row must be preserved"
        );
        assert_eq!(
            read_doc(&db, "upd_doc:d0003"),
            serde_json::json!({"new_f": 3})
        );
        let seq = db.get_seq();
        assert_eq!(
            db.scan_prefix("\x00SCHEMA\x00rename_row:", seq)
                .expect("markers")
                .len(),
            0
        );
    }

    #[test]
    fn doc_rename_fails_closed_on_dual_divergence_and_promotes_legacy_marker_update() {
        for use_legacy_marker in [false, true] {
            let (db, _dir, schema) = open_test_db();
            let initial_count = WriteBatch::MAX_OPS + 5;
            let mut seed = WriteBatch::new();
            for i in 0..initial_count {
                seed.set(&format!("div_doc:d{i:04}"), format!(r#"{{"old_f":{i}}}"#))
                    .expect("set");
                if seed.op_count() >= 500 {
                    db.commit_batch(&seed).expect("commit seed");
                    seed = WriteBatch::new();
                }
            }
            if !seed.is_empty() {
                db.commit_batch(&seed).expect("commit tail seed");
            }

            let stage_key = SchemaManager::rename_stage_key_for_namespace(
                "div_doc",
                IndexNamespace::Document,
                "old_f",
                "new_f",
            );
            let row_marker = SchemaManager::rename_row_stage_key(&stage_key, "div_doc:d0001");
            let db_hook = db.clone();
            let fired = Arc::new(AtomicBool::new(false));
            let flag = fired.clone();
            let _hook = HookGuard::install(move |point| {
                if point == "rename_after_staging_before_cleanup"
                    && !flag.swap(true, AtomicOrdering::SeqCst)
                {
                    let mut wb = WriteBatch::new();
                    if use_legacy_marker {
                        wb.set(&row_marker, "1".into()).expect("legacy marker");
                        wb.set("div_doc:d0001", r#"{"old_f":2,"new_f":1}"#.into())
                            .expect("set legacy updated");
                    } else {
                        wb.set("div_doc:d0001", r#"{"old_f":999,"new_f":888}"#.into())
                            .expect("set dual divergent");
                    }
                    db_hook.commit_batch(&wb).expect("commit divergence");
                }
            });

            let mig = rename_field_migration(1, "diverge check", "div_doc", "old_f", "new_f");
            if use_legacy_marker {
                schema
                    .migrate_up(&mig)
                    .expect("legacy marker with updated source must promote latest source value");
                assert_eq!(
                    read_doc(&db, "div_doc:d0001"),
                    serde_json::json!({"new_f": 2})
                );
            } else {
                let err = schema
                    .migrate_up(&mig)
                    .expect_err("dual-diverged staged row must fail closed without losing data");
                assert!(
                    err.to_string().contains("diverged concurrently"),
                    "unexpected error: {err}"
                );
                assert_eq!(
                    read_doc(&db, "div_doc:d0001"),
                    serde_json::json!({"old_f": 999, "new_f": 888})
                );
            }
        }
    }
}
