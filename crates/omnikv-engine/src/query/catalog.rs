//! Table Catalog — Schema storage for multi-table SQL support
//!
//! Stores table definitions (columns, types) inside OmniKV itself
//! using the key prefix `\x00CATALOG\x00`.

use crate::{OmniError, OmniKV, WriteBatch};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

const CATALOG_PREFIX: &str = "\x00CATALOG\x00";

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
                .map(|f| f.to_string())
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
}

impl TableDef {
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
}

impl Catalog {
    pub fn new(db: Arc<OmniKV>) -> Self {
        let cat = Self {
            db,
            cache: std::sync::RwLock::new(HashMap::new()),
        };
        cat.load_all(None);
        cat
    }

    /// Loads table definitions as they were at `seq`. A transaction reading
    /// at an older snapshot must also see the older schema: a table another
    /// connection migrated in the meantime would otherwise decode this
    /// transaction's rows under the wrong NULL encoding.
    pub fn at_snapshot(db: Arc<OmniKV>, seq: u64) -> Self {
        let cat = Self {
            db,
            cache: std::sync::RwLock::new(HashMap::new()),
        };
        cat.load_all(Some(seq));
        cat
    }

    fn load_all(&self, seq: Option<u64>) {
        let seq = seq.unwrap_or_else(|| self.db.get_seq());
        if let Ok(results) = self
            .db
            .scan(CATALOG_PREFIX, &format!("{}\x7F", CATALOG_PREFIX), seq)
        {
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

    pub fn create_table(&self, table: TableDef) -> Result<(), String> {
        let name_lower = table.name.to_lowercase();
        {
            let cache = self
                .cache
                .read()
                .expect("catalog cache RwLock poisoned: fatal invariant");
            if cache.contains_key(&name_lower) {
                return Err(format!("Table '{}' already exists", table.name));
            }
        }

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

        // Delete all rows
        let prefix = table.row_prefix();
        let seq = self.db.get_seq();
        if let Ok(rows) = self.db.scan(&prefix, &format!("{}\x7F", prefix), seq) {
            let mut batch = WriteBatch::new();
            for (key, _) in &rows {
                let _ = batch.delete(key);
            }
            if !rows.is_empty() {
                self.db
                    .commit_batch(&batch)
                    .map_err(|e| format!("{:?}", e))?;
            }
        }

        // Delete catalog entry
        let cat_key = format!("{}{}", CATALOG_PREFIX, name_lower);
        let mut batch = WriteBatch::new();
        batch.delete(&cat_key).map_err(|e| format!("{:?}", e))?;
        self.db
            .commit_batch(&batch)
            .map_err(|e| format!("{:?}", e))?;

        let mut cache = self
            .cache
            .write()
            .expect("catalog cache RwLock poisoned: fatal invariant");
        cache.remove(&name_lower);
        Ok(())
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
    /// stale cache.
    pub fn list_tables_at(&self, seq: u64) -> Vec<String> {
        self.db
            .scan(CATALOG_PREFIX, &format!("{}\x7F", CATALOG_PREFIX), seq)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|(_, value)| {
                serde_json::from_str::<TableDef>(&value)
                    .ok()
                    .map(|table| table.name.to_lowercase())
            })
            .collect()
    }
}
