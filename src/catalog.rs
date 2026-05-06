//! Table Catalog — Schema storage for multi-table SQL support
//!
//! Stores table definitions (columns, types) inside OmniKV itself
//! using the key prefix `\x00CATALOG\x00`.

use std::collections::HashMap;
use std::sync::Arc;
use serde::{Serialize, Deserialize};
use crate::{OmniKV, WriteBatch, OmniError};

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

impl ColumnType {
    pub fn from_str(s: &str) -> Result<Self, String> {
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

    pub fn pg_oid(&self) -> i32 {
        match self {
            Self::Text => 25,      // TEXT
            Self::Integer => 20,   // INT8
            Self::Float => 701,    // FLOAT8
            Self::Boolean => 16,   // BOOL
            Self::Timestamp => 1114, // TIMESTAMP
            Self::Json => 114,     // JSON
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDef {
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: String,
    pub created_at: u64,
}

impl TableDef {
    pub fn row_prefix(&self) -> String {
        format!("\x00T\x00{}\x00", self.name)
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name.eq_ignore_ascii_case(name))
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
        cat.load_all();
        cat
    }

    fn load_all(&self) {
        let seq = self.db.get_seq();
        if let Ok(results) = self.db.scan(CATALOG_PREFIX, &format!("{}\x7F", CATALOG_PREFIX), seq) {
            let mut cache = self.cache.write().unwrap();
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
            let cache = self.cache.read().unwrap();
            if cache.contains_key(&name_lower) {
                return Err(format!("Table '{}' already exists", table.name));
            }
        }

        let key = format!("{}{}", CATALOG_PREFIX, name_lower);
        let value = serde_json::to_string(&table)
            .map_err(|e| format!("Serialize: {}", e))?;

        let mut batch = WriteBatch::new();
        batch.set(&key, value).map_err(|e| format!("{:?}", e))?;
        self.db.commit_batch(&batch).map_err(|e| format!("{:?}", e))?;

        let mut cache = self.cache.write().unwrap();
        cache.insert(name_lower, table);
        Ok(())
    }

    pub fn drop_table(&self, name: &str) -> Result<(), String> {
        let name_lower = name.to_lowercase();
        let table = {
            let cache = self.cache.read().unwrap();
            cache.get(&name_lower).cloned()
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
                self.db.commit_batch(&batch).map_err(|e| format!("{:?}", e))?;
            }
        }

        // Delete catalog entry
        let cat_key = format!("{}{}", CATALOG_PREFIX, name_lower);
        let mut batch = WriteBatch::new();
        batch.delete(&cat_key).map_err(|e| format!("{:?}", e))?;
        self.db.commit_batch(&batch).map_err(|e| format!("{:?}", e))?;

        let mut cache = self.cache.write().unwrap();
        cache.remove(&name_lower);
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Option<TableDef> {
        let cache = self.cache.read().unwrap();
        cache.get(&name.to_lowercase()).cloned()
    }

    pub fn list_tables(&self) -> Vec<String> {
        let cache = self.cache.read().unwrap();
        cache.keys().cloned().collect()
    }
}
