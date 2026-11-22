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

use crate::secondary_index::{IndexFieldType, IndexManager, collection_row_ranges};
use crate::{OmniError, OmniKV, WriteBatch};

/// Prefix for schema metadata stored in OmniKV.
const SCHEMA_PREFIX: &str = "\x00SCHEMA\x00";
/// Key for the current schema version.
const VERSION_KEY: &str = "\x00SCHEMA\x00__version__";
/// Prefix for individual migration records.
const MIGRATION_PREFIX: &str = "\x00SCHEMA\x00migration:";
const MIGRATION_LOCK_NAME: &str = "\x00SCHEMA\x00__migration_lock__";

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

        // Mark as in-progress
        self.save_migration_record(&MigrationRecord {
            version: migration.version,
            description: migration.description.clone(),
            state: MigrationState::InProgress { progress_pct: 0 },
            applied_at: timestamp_ms(),
        })?;

        // Execute each operation
        let total_ops = migration.up.len();
        for (i, op) in migration.up.iter().enumerate() {
            match self.execute_op(op, migration.version, "up", i) {
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

        // Execute down operations in reverse
        for (i, op) in migration.down.iter().rev().enumerate() {
            self.execute_op(op, migration.version, "down", i)?;
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
        for idx_def in self
            .index_manager
            .catalog_snapshot_at(seq)?
            .indexes_for_collection(table_name)
        {
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

    fn execute_op(
        &self,
        op: &SchemaOp,
        version: Version,
        direction: &str,
        step_index: usize,
    ) -> Result<(), OmniError> {
        match op {
            SchemaOp::CreateIndex {
                name,
                collection,
                fields,
                unique,
            } => self.with_collection_lock(collection, || {
                self.index_manager.create_index_locked(
                    name,
                    collection,
                    fields.clone(),
                    *unique,
                )?;
                if let Err(error) = self.index_manager.rebuild_index_locked(name, collection) {
                    if let Err(cleanup_error) =
                        self.index_manager.drop_index_locked(name, collection)
                    {
                        return Err(OmniError::IoError(format!(
                            "{error}; failed index cleanup also failed: {cleanup_error}"
                        )));
                    }
                    return Err(error);
                }
                Ok(())
            }),
            SchemaOp::DropIndex { name } => self.index_manager.drop_index(name),
            SchemaOp::AddField {
                collection,
                field_name,
                default_value,
            } => self.with_collection_lock(collection, || {
                let seq = self.db.get_seq();
                let catalog_key = format!("\x00CATALOG\x00{}", collection.to_lowercase());
                let sql_table = self
                    .db
                    .find(&catalog_key, seq)?
                    .as_deref()
                    .and_then(|v| serde_json::from_str::<crate::catalog::TableDef>(v).ok());
                let effective_field = sql_table
                    .as_ref()
                    .and_then(|t| {
                        t.columns
                            .iter()
                            .find(|c| c.name.eq_ignore_ascii_case(field_name))
                            .map(|c| c.name.clone())
                    })
                    .unwrap_or_else(|| field_name.clone());
                let target_collection = sql_table
                    .as_ref()
                    .map_or(collection.as_str(), |t| t.name.as_str());
                self.backfill_field(target_collection, &effective_field, Some(default_value))?;
                if let Some(mut table) = sql_table
                    && !table
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
                    let mut cat_batch = WriteBatch::new();
                    cat_batch.set(&catalog_key, cat_value)?;
                    self.db.commit_batch(&cat_batch)?;
                }
                Ok(())
            }),
            SchemaOp::RemoveField {
                collection,
                field_name,
            } => self.with_collection_lock(collection, || {
                let seq = self.db.get_seq();
                let catalog_key = format!("\x00CATALOG\x00{}", collection.to_lowercase());
                if let Some(mut table) = self
                    .db
                    .find(&catalog_key, seq)?
                    .as_deref()
                    .and_then(|v| serde_json::from_str::<crate::catalog::TableDef>(v).ok())
                {
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
                    self.backfill_field(&table.name, &canonical_name, None)?;
                    table
                        .columns
                        .retain(|c| !c.name.eq_ignore_ascii_case(&canonical_name));
                    let cat_value = serde_json::to_string(&table)
                        .map_err(|e| crate::OmniError::IoError(e.to_string()))?;
                    let mut cat_batch = WriteBatch::new();
                    cat_batch.set(&catalog_key, cat_value)?;
                    self.db.commit_batch(&cat_batch)?;
                    return Ok(());
                }
                self.backfill_field(collection, field_name, None)
            }),
            SchemaOp::RenameField {
                collection,
                old_name,
                new_name,
            } => self.with_collection_lock(collection, || {
                let seq = self.db.get_seq();
                let catalog_key = format!("\x00CATALOG\x00{}", collection.to_lowercase());
                if let Some(mut table) = self
                    .db
                    .find(&catalog_key, seq)?
                    .as_deref()
                    .and_then(|v| serde_json::from_str::<crate::catalog::TableDef>(v).ok())
                {
                    if table.primary_key.eq_ignore_ascii_case(old_name) {
                        return Err(crate::OmniError::IoError(format!(
                            "cannot rename primary key field '{}' in SQL table '{}' \
                             without also updating the row-key contract",
                            old_name, collection
                        )));
                    }
                    let stage_key = Self::rename_stage_key(&table.name, old_name, new_name);
                    let done_key = Self::rename_done_key(
                        version,
                        direction,
                        step_index,
                        &table.name,
                        old_name,
                        new_name,
                    );
                    let has_new = table
                        .columns
                        .iter()
                        .any(|c| c.name.eq_ignore_ascii_case(new_name));
                    let has_old = table
                        .columns
                        .iter()
                        .any(|c| c.name.eq_ignore_ascii_case(old_name));
                    if let Some(staged_old) = self.db.find(&stage_key, seq)?
                        && has_new
                        && !has_old
                    {
                        return self.cleanup_staged_rename(
                            &table.name,
                            &staged_old,
                            &stage_key,
                            &done_key,
                        );
                    }
                    if has_new && !has_old && self.db.find(&done_key, seq)?.is_some() {
                        return Ok(());
                    }
                    if has_new {
                        return Err(crate::OmniError::IoError(format!(
                            "column '{}' already exists in SQL table '{}'",
                            new_name, collection
                        )));
                    }
                    let canonical_old = table
                        .columns
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(old_name))
                        .map(|c| c.name.clone())
                        .unwrap_or_else(|| old_name.clone());
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
                    );
                }
                let done_key = Self::rename_done_key(
                    version, direction, step_index, collection, old_name, new_name,
                );
                self.rename_field_online(collection, old_name, new_name, None, &done_key)
            }),
        }
    }

    pub(crate) fn rename_stage_prefix_for_table(collection: &str) -> String {
        let col = collection.to_lowercase();
        format!("\x00SCHEMA\x00rename_stage:{}:{}:", col.len(), col)
    }

    pub(crate) fn rename_stage_key(collection: &str, old_name: &str, new_name: &str) -> String {
        let old = old_name.to_lowercase();
        let new = new_name.to_lowercase();
        format!(
            "{}{}:{}:{}:{}",
            Self::rename_stage_prefix_for_table(collection),
            old.len(),
            old,
            new.len(),
            new
        )
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

    fn rename_done_prefix(version: Version, direction: &str) -> String {
        format!("\x00SCHEMA\x00rename_done:{version:06}:{direction}:")
    }

    fn rename_done_key(
        version: Version,
        direction: &str,
        step_index: usize,
        collection: &str,
        old_name: &str,
        new_name: &str,
    ) -> String {
        format!(
            "{}{step_index:06}:{}:{old_name}:{new_name}",
            Self::rename_done_prefix(version, direction),
            collection.to_lowercase(),
        )
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
        self.index_manager.replace_document_indexes_with_catalog(
            catalog,
            collection,
            primary_key,
            &old_index_doc,
            primary_key,
            &new_index_doc,
            batch,
        )?;
        batch.set(key, updated)?;
        Ok(())
    }

    fn backfill_field(
        &self,
        collection: &str,
        field_name: &str,
        default_value: Option<&serde_json::Value>,
    ) -> Result<(), OmniError> {
        self.index_manager.refresh_catalog()?;
        let seq = self.db.get_seq();
        let catalog = self.index_manager.catalog_snapshot_at(seq)?;
        let sql_table = self
            .db
            .find(
                &format!("\x00CATALOG\x00{}", collection.to_lowercase()),
                seq,
            )?
            .and_then(|raw| serde_json::from_str::<crate::catalog::TableDef>(&raw).ok());
        let sql_row_format = sql_table
            .as_ref()
            .map(|table| table.row_format)
            .unwrap_or_default();
        let sql_default = match default_value {
            Some(serde_json::Value::Null) => Some(serde_json::Value::Null),
            Some(serde_json::Value::String(s)) => {
                if s == "NULL" && sql_row_format == crate::catalog::RowFormat::Legacy {
                    let sql_prefix = format!("\x00T\x00{}\x00", collection);
                    let sql_end = format!("\x00T\x00{}\x00\u{10ffff}", collection);
                    if sql_table.is_some() || !self.db.scan(&sql_prefix, &sql_end, seq)?.is_empty()
                    {
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
                let sql_prefix = format!("\x00T\x00{}\x00", collection);
                let sql_end = format!("\x00T\x00{}\x00\u{10ffff}", collection);
                if sql_table.is_some() || !self.db.scan(&sql_prefix, &sql_end, seq)?.is_empty() {
                    return Err(OmniError::IoError(format!(
                        "unsupported SQL row value for field '{field_name}': {other}"
                    )));
                }
                None
            }
            None => None,
        };

        let mut batch = WriteBatch::new();
        let max_doc_ops = 1 + 2 * catalog.indexes_for_collection(collection).len();

        for (prefix, end) in collection_row_ranges(collection) {
            let is_sql_rows = prefix.starts_with("\x00T\x00");
            for (key, value) in self.db.scan(&prefix, &end, seq)? {
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
                    }
                }
            }
        }

        if !batch.is_empty() {
            self.db.commit_batch(&batch)?;
        }

        Ok(())
    }

    fn rename_field_online(
        &self,
        collection: &str,
        old_name: &str,
        new_name: &str,
        sql_catalog_update: Option<(&str, &str)>,
        done_key: &str,
    ) -> Result<(), OmniError> {
        self.index_manager.refresh_catalog()?;
        let seq = self.db.get_seq();
        let catalog = self.index_manager.catalog_snapshot_at(seq)?;
        let sql_row_format = self
            .db
            .find(
                &format!("\x00CATALOG\x00{}", collection.to_lowercase()),
                seq,
            )?
            .and_then(|raw| serde_json::from_str::<crate::catalog::TableDef>(&raw).ok())
            .map(|table| table.row_format)
            .unwrap_or_default();

        let stage_key = Self::rename_stage_key(collection, old_name, new_name);
        let is_staged_retry = self.db.find(&stage_key, seq)?.is_some();
        if !is_staged_retry
            && sql_catalog_update.is_none()
            && self.db.find(done_key, seq)?.is_some()
        {
            return Ok(());
        }

        for (prefix, end) in collection_row_ranges(collection) {
            for (key, value) in self.db.scan(&prefix, &end, seq)? {
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

            'ranges: for (prefix, end) in collection_row_ranges(collection) {
                let is_sql_rows = prefix.starts_with("\x00T\x00");
                for (key, value) in self.db.scan(&prefix, &end, seq)? {
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

            if fits_in_one_batch {
                if let Some((cat_key, cat_val)) = sql_catalog_update {
                    single_batch.set(cat_key, cat_val.to_string())?;
                }
                if !single_batch.is_empty() {
                    single_batch.set(done_key, "done".into())?;
                    self.db.commit_batch(&single_batch)?;
                }
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

        for (prefix, end) in collection_row_ranges(collection) {
            let is_sql_rows = prefix.starts_with("\x00T\x00");
            for (key, value) in self.db.scan(&prefix, &end, seq)? {
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
                    &prefix,
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

        self.cleanup_staged_rename(collection, old_name, &stage_key, done_key)
    }

    fn cleanup_staged_rename(
        &self,
        collection: &str,
        old_name: &str,
        stage_key: &str,
        done_key: &str,
    ) -> Result<(), OmniError> {
        self.index_manager.refresh_catalog()?;
        let seq = self.db.get_seq();
        let catalog = self.index_manager.catalog_snapshot_at(seq)?;
        let sql_row_format = self
            .db
            .find(
                &format!("\x00CATALOG\x00{}", collection.to_lowercase()),
                seq,
            )?
            .and_then(|raw| serde_json::from_str::<crate::catalog::TableDef>(&raw).ok())
            .map(|table| table.row_format)
            .unwrap_or_default();

        let mut batch = WriteBatch::new();
        let cleanup_doc_ops = 2 + 2 * catalog.indexes_for_collection(collection).len();

        for (prefix, end) in collection_row_ranges(collection) {
            let is_sql_rows = prefix.starts_with("\x00T\x00");
            for (key, value) in self.db.scan(&prefix, &end, seq)? {
                let Ok(mut doc) = serde_json::from_str::<serde_json::Value>(&value) else {
                    continue;
                };
                let old_doc = doc.clone();
                let Some(obj) = doc.as_object_mut() else {
                    continue;
                };
                if obj.remove(old_name).is_some() {
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
        let row_marker_end = format!("{row_marker_prefix}\u{10ffff}");
        let latest_seq = self.db.get_seq();
        for (marker_key, _) in self
            .db
            .scan(&row_marker_prefix, &row_marker_end, latest_seq)?
        {
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
        batch.set(done_key, "done".into())?;
        batch.delete(stage_key)?;
        self.db.commit_batch(&batch)?;
        Ok(())
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
    fn save_applied_version(
        &self,
        record: &MigrationRecord,
        version: Version,
    ) -> Result<(), OmniError> {
        let key = format!("{}{:06}", MIGRATION_PREFIX, record.version);
        let value = serde_json::to_string(record).map_err(|e| OmniError::IoError(e.to_string()))?;
        let direction = match record.state {
            MigrationState::RolledBack => "down",
            _ => "up",
        };
        let done_prefix = Self::rename_done_prefix(record.version, direction);
        let done_end = format!("{done_prefix}\u{10ffff}");
        let mut batch = WriteBatch::new();
        for (done_key, _) in self.db.scan(&done_prefix, &done_end, self.db.get_seq())? {
            if batch.op_count().saturating_add(3) > WriteBatch::MAX_OPS {
                self.db.commit_batch(&batch)?;
                batch = WriteBatch::new();
            }
            batch.delete(&done_key)?;
        }
        batch.set(&key, value)?;
        batch.set(VERSION_KEY, version.to_string())?;
        self.db.commit_batch(&batch)?;
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
