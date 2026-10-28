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

use crate::secondary_index::{IndexFieldType, IndexManager};
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
            match self.execute_op(op) {
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
        for op in migration.down.iter().rev() {
            self.execute_op(op)?;
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

    /// Executes a single schema operation.
    fn execute_op(&self, op: &SchemaOp) -> Result<(), OmniError> {
        match op {
            SchemaOp::CreateIndex {
                name,
                collection,
                fields,
                unique,
            } => {
                self.index_manager
                    .create_index(name, collection, fields.clone(), *unique)?;
                // Online rebuild: index existing documents in batches
                self.index_manager.rebuild_index(name)?;
                Ok(())
            }
            SchemaOp::DropIndex { name } => self.index_manager.drop_index(name),
            SchemaOp::AddField {
                collection,
                field_name,
                default_value,
            } => self.backfill_field(collection, field_name, Some(default_value)),
            SchemaOp::RemoveField {
                collection,
                field_name,
            } => self.backfill_field(collection, field_name, None),
            SchemaOp::RenameField {
                collection,
                old_name,
                new_name,
            } => self.rename_field_online(collection, old_name, new_name),
        }
    }

    /// Online field backfill — adds or removes a field from all documents
    /// in a collection, processing in batches to avoid blocking.
    fn backfill_field(
        &self,
        collection: &str,
        field_name: &str,
        default_value: Option<&serde_json::Value>,
    ) -> Result<(), OmniError> {
        let prefix = format!("{}:", collection);
        let end = format!("{}:~", collection);
        let seq = self.db.get_seq();

        let mut batch = WriteBatch::new();
        let mut count = 0;

        for (key, value) in self.db.scan(&prefix, &end, seq)? {
            let Ok(mut doc) = serde_json::from_str::<serde_json::Value>(&value) else {
                continue;
            };
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
            if changed {
                let updated =
                    serde_json::to_string(&doc).map_err(|e| OmniError::IoError(e.to_string()))?;
                batch.set(&key, updated)?;
                count += 1;

                // Commit in batches of 500
                if count % 500 == 0 {
                    self.db.commit_batch(&batch)?;
                    batch = WriteBatch::new();
                }
            }
        }

        if !batch.buffered_writes.is_empty() {
            self.db.commit_batch(&batch)?;
        }

        Ok(())
    }

    /// Online field rename — renames a field in all documents.
    fn rename_field_online(
        &self,
        collection: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), OmniError> {
        let prefix = format!("{}:", collection);
        let end = format!("{}:~", collection);
        let seq = self.db.get_seq();

        let mut batch = WriteBatch::new();
        let mut count = 0;

        for (key, value) in self.db.scan(&prefix, &end, seq)? {
            let Ok(mut doc) = serde_json::from_str::<serde_json::Value>(&value) else {
                continue;
            };
            let Some(obj) = doc.as_object_mut() else {
                continue;
            };
            if let Some(val) = obj.remove(old_name) {
                if obj.contains_key(new_name) {
                    return Err(OmniError::IoError(format!(
                        "cannot rename field '{}' to '{}' because the destination exists",
                        old_name, new_name
                    )));
                }
                obj.insert(new_name.to_string(), val);
                let updated =
                    serde_json::to_string(&doc).map_err(|e| OmniError::IoError(e.to_string()))?;
                batch.set(&key, updated)?;
                count += 1;

                if count % 500 == 0 {
                    self.db.commit_batch(&batch)?;
                    batch = WriteBatch::new();
                }
            }
        }

        if !batch.buffered_writes.is_empty() {
            self.db.commit_batch(&batch)?;
        }

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
        let mut batch = WriteBatch::new();
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
