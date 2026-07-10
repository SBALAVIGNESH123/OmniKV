//! Production-grade Secondary Index Engine
//!
//! Replaces the naive `__idx:{collection}:{field}:{value}` string hack with
//! a proper sorted index system that supports:
//!
//! - **Range queries** on indexed fields (e.g., `age >= 18 AND age < 65`)
//! - **Composite indexes** (multi-field, ordered)
//! - **Unique indexes** with constraint enforcement
//! - **Automatic maintenance** on write/update/delete
//! - **Index catalog** persisted in the engine itself
//!
//! ## Key Encoding
//!
//! Index entries use a lexicographically sortable key format:
//! ```text
//! \x00IDX\x00{index_id:08}\x00{encoded_value_1}\x00{encoded_value_2}\x00...\x00{primary_key}
//! ```
//!
//! The `\x00IDX\x00` prefix guarantees separation from user data.
//! Values are encoded to preserve sort order (strings: UTF-8, numbers: big-endian with sign flip).
//! The primary key suffix ensures uniqueness for non-unique indexes.
//!
//! ## Architecture
//!
//! ```text
//! IndexManager
//!   ├── IndexCatalog (persistent: stores index definitions in OmniKV)
//!   ├── IndexWriter  (automatic maintenance on write/update/delete)
//!   └── IndexScanner (range queries on indexed fields)
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::{OmniError, OmniKV, WriteBatch};

/// Prefix for all index entries in the key space.
const INDEX_PREFIX: &[u8] = b"\x00IDX\x00";
/// Prefix for index catalog metadata.
const CATALOG_PREFIX: &str = "\x00IDX_CATALOG\x00";

/// Unique identifier for an index.
pub type IndexId = u32;

/// Defines the type of values stored in an index.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum IndexFieldType {
    String,
    Integer,
    Float,
}

/// Definition of a single index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDefinition {
    /// Unique index ID (auto-assigned).
    pub id: IndexId,
    /// Human-readable index name (e.g., "users_email_idx").
    pub name: String,
    /// The collection/table this index belongs to.
    pub collection: String,
    /// Ordered list of (field_name, field_type) for composite indexes.
    pub fields: Vec<(String, IndexFieldType)>,
    /// If true, duplicate values are rejected.
    pub unique: bool,
}

/// The Index Catalog — persisted in OmniKV as JSON under `\x00IDX_CATALOG\x00` keys.
pub struct IndexCatalog {
    /// All known indexes, keyed by index ID.
    indexes: HashMap<IndexId, IndexDefinition>,
    /// Lookup: (collection, field_name) → list of index IDs that include this field.
    field_index: HashMap<(String, String), Vec<IndexId>>,
    /// Next auto-increment index ID.
    next_id: IndexId,
}

impl IndexCatalog {
    /// Loads the catalog from OmniKV, or creates an empty one.
    pub fn load(db: &Arc<OmniKV>) -> Self {
        let mut catalog = Self {
            indexes: HashMap::new(),
            field_index: HashMap::new(),
            next_id: 1,
        };

        let seq = db.get_seq();
        // Scan all catalog entries
        if let Ok(iter) = db.scan_iter(CATALOG_PREFIX, &format!("{}\x7f", CATALOG_PREFIX), seq) {
            for (_key, value) in iter {
                if let Ok(def) = serde_json::from_str::<IndexDefinition>(&value) {
                    if def.id >= catalog.next_id {
                        catalog.next_id = def.id + 1;
                    }
                    catalog.register_in_memory(def);
                }
            }
        }

        catalog
    }

    /// Registers an index definition in the in-memory lookup tables.
    fn register_in_memory(&mut self, def: IndexDefinition) {
        for (field_name, _) in &def.fields {
            self.field_index
                .entry((def.collection.clone(), field_name.clone()))
                .or_default()
                .push(def.id);
        }
        self.indexes.insert(def.id, def);
    }

    /// Returns all indexes that cover the given collection.
    pub fn indexes_for_collection(&self, collection: &str) -> Vec<&IndexDefinition> {
        self.indexes
            .values()
            .filter(|idx| idx.collection == collection)
            .collect()
    }

    /// Returns a specific index by name.
    pub fn get_by_name(&self, name: &str) -> Option<&IndexDefinition> {
        self.indexes.values().find(|idx| idx.name == name)
    }

    /// Returns a specific index by ID.
    pub fn get(&self, id: IndexId) -> Option<&IndexDefinition> {
        self.indexes.get(&id)
    }
}

/// Encodes a field value into a byte sequence that preserves sort order.
pub fn encode_index_value(value: &serde_json::Value, field_type: &IndexFieldType) -> Vec<u8> {
    match field_type {
        IndexFieldType::String => {
            // UTF-8 strings are already lexicographically sortable.
            // We escape \x00 bytes to avoid collision with our separator.
            let s = value.as_str().unwrap_or("");
            let mut encoded = Vec::with_capacity(s.len());
            for b in s.as_bytes() {
                if *b == 0x00 {
                    encoded.push(0x00);
                    encoded.push(0x01); // escape
                } else {
                    encoded.push(*b);
                }
            }
            encoded
        }
        IndexFieldType::Integer => {
            // Big-endian encoding with sign bit flip for correct sort order.
            // This ensures -100 < -1 < 0 < 1 < 100 in byte ordering.
            let n = value.as_i64().unwrap_or(0);
            let flipped = (n ^ i64::MIN) as u64; // flip sign bit
            flipped.to_be_bytes().to_vec()
        }
        IndexFieldType::Float => {
            // IEEE 754 float encoding with sign handling for sort order.
            let f = value.as_f64().unwrap_or(0.0);
            let bits = f.to_bits();
            let sortable = if f.is_sign_negative() {
                !bits // negative: flip all bits
            } else {
                bits ^ (1u64 << 63) // positive: flip sign bit only
            };
            sortable.to_be_bytes().to_vec()
        }
    }
}

/// Builds the full index key for an entry.
///
/// Format: `\x00IDX\x00{index_id:08}\x00{value_1}\x00{value_2}\x00...\x00{primary_key}`
fn build_index_key(index_id: IndexId, encoded_values: &[Vec<u8>], primary_key: &str) -> String {
    let mut key = String::new();
    key.push_str(&format!("\x00IDX\x00{:08}\x00", index_id));
    for (i, val) in encoded_values.iter().enumerate() {
        // Use base64 to safely embed binary values in string keys
        let hex = hex_encode(val);
        key.push_str(&hex);
        if i < encoded_values.len() - 1 {
            key.push('\x00');
        }
    }
    key.push('\x00');
    key.push_str(primary_key);
    key
}

/// Builds the scan prefix for an index (all entries for a given index).
fn build_index_scan_prefix(index_id: IndexId) -> String {
    format!("\x00IDX\x00{:08}\x00", index_id)
}

/// Builds a scan range for a specific value prefix on an index.
fn build_index_value_prefix(index_id: IndexId, encoded_values: &[Vec<u8>]) -> String {
    let mut key = format!("\x00IDX\x00{:08}\x00", index_id);
    for (i, val) in encoded_values.iter().enumerate() {
        let hex = hex_encode(val);
        key.push_str(&hex);
        if i < encoded_values.len() - 1 {
            key.push('\x00');
        }
    }
    key
}

/// Hex encoding for binary index values — preserves lexicographic sort order.
fn hex_encode(data: &[u8]) -> String {
    data.iter().map(|b| format!("{:02x}", b)).collect()
}

/// The Index Manager — the main public interface for index operations.
///
/// Manages the catalog, writes index entries on mutations, and provides
/// index-based lookups and range scans.
pub struct IndexManager {
    db: Arc<OmniKV>,
    catalog: Mutex<IndexCatalog>,
}

impl IndexManager {
    /// Creates a new IndexManager, loading existing index definitions from OmniKV.
    pub fn new(db: Arc<OmniKV>) -> Self {
        let catalog = IndexCatalog::load(&db);
        Self {
            db,
            catalog: Mutex::new(catalog),
        }
    }

    /// CREATE INDEX — defines a new secondary index on a collection.
    ///
    /// The index is persisted in the catalog and immediately available for
    /// future writes. Existing data is NOT retroactively indexed (use
    /// `rebuild_index` for that).
    pub fn create_index(
        &self,
        name: &str,
        collection: &str,
        fields: Vec<(String, IndexFieldType)>,
        unique: bool,
    ) -> Result<IndexId, OmniError> {
        let mut catalog = self
            .catalog
            .lock()
            .map_err(|_| OmniError::LockPoisoned("index catalog".into()))?;

        // Check for duplicate name
        if catalog.get_by_name(name).is_some() {
            return Err(OmniError::IoError(format!(
                "Index '{}' already exists",
                name
            )));
        }

        let id = catalog.next_id;
        catalog.next_id += 1;

        let def = IndexDefinition {
            id,
            name: name.to_string(),
            collection: collection.to_string(),
            fields,
            unique,
        };

        // Persist to OmniKV
        let catalog_key = format!("{}{}", CATALOG_PREFIX, id);
        let catalog_value =
            serde_json::to_string(&def).map_err(|e| OmniError::IoError(e.to_string()))?;

        let mut batch = WriteBatch::new();
        batch.set(&catalog_key, catalog_value)?;
        self.db.commit_batch(&batch)?;

        catalog.register_in_memory(def);

        Ok(id)
    }

    /// DROP INDEX — removes an index definition and all its entries.
    pub fn drop_index(&self, name: &str) -> Result<(), OmniError> {
        let mut catalog = self
            .catalog
            .lock()
            .map_err(|_| OmniError::LockPoisoned("index catalog".into()))?;

        let id = catalog
            .get_by_name(name)
            .map(|d| d.id)
            .ok_or_else(|| OmniError::IoError(format!("Index '{}' not found", name)))?;

        // Delete catalog entry
        let catalog_key = format!("{}{}", CATALOG_PREFIX, id);
        let mut batch = WriteBatch::new();
        batch.delete(&catalog_key)?;

        // Delete all index entries (scan and delete)
        let prefix = build_index_scan_prefix(id);
        let end = format!("{}\x7f", prefix);
        let seq = self.db.get_seq();
        if let Ok(entries) = self.db.scan_iter(&prefix, &end, seq) {
            for (key, _) in entries {
                batch.delete(&key)?;
            }
        }

        self.db.commit_batch(&batch)?;

        // Remove from catalog
        catalog.indexes.remove(&id);
        // Rebuild field_index
        catalog.field_index.retain(|_, ids| {
            ids.retain(|i| *i != id);
            !ids.is_empty()
        });

        Ok(())
    }

    /// Writes index entries for an INSERT or UPDATE operation.
    ///
    /// `collection` — the collection being written to
    /// `primary_key` — the document's primary key
    /// `document` — the JSON document being stored
    /// `batch` — the WriteBatch to append index writes to (atomic with data write)
    pub fn index_document(
        &self,
        collection: &str,
        primary_key: &str,
        document: &serde_json::Value,
        batch: &mut WriteBatch,
    ) -> Result<(), OmniError> {
        let catalog = self
            .catalog
            .lock()
            .map_err(|_| OmniError::LockPoisoned("index catalog".into()))?;

        let indexes = catalog.indexes_for_collection(collection);

        for idx_def in indexes {
            // Extract field values from the document
            let mut encoded_values = Vec::new();
            let mut all_fields_present = true;

            for (field_name, field_type) in &idx_def.fields {
                if let Some(val) = document.get(field_name) {
                    encoded_values.push(encode_index_value(val, field_type));
                } else {
                    all_fields_present = false;
                    break;
                }
            }

            if !all_fields_present {
                continue; // Skip indexing if any field is missing
            }

            // Unique constraint check
            if idx_def.unique {
                let prefix = build_index_value_prefix(idx_def.id, &encoded_values);
                let end = format!("{}\x7f", prefix);
                let seq = self.db.get_seq();
                if let Ok(existing) = self.db.scan_iter(&prefix, &end, seq) {
                    for (existing_key, _) in existing {
                        // Extract the primary key from the existing index entry
                        if let Some(existing_pk) = existing_key.rsplit('\x00').next() {
                            if existing_pk != primary_key {
                                return Err(OmniError::IoError(format!(
                                    "UNIQUE CONSTRAINT VIOLATION: index '{}' on {:?}",
                                    idx_def.name,
                                    idx_def
                                        .fields
                                        .iter()
                                        .map(|(f, _)| f.as_str())
                                        .collect::<Vec<_>>()
                                )));
                            }
                        }
                    }
                }
            }

            // Write the index entry (value = primary key for O(1) lookup)
            let idx_key = build_index_key(idx_def.id, &encoded_values, primary_key);
            batch.set(&idx_key, primary_key.to_string())?;
        }

        Ok(())
    }

    /// Removes index entries for a DELETE or pre-UPDATE cleanup.
    pub fn remove_document_indexes(
        &self,
        collection: &str,
        primary_key: &str,
        old_document: &serde_json::Value,
        batch: &mut WriteBatch,
    ) -> Result<(), OmniError> {
        let catalog = self
            .catalog
            .lock()
            .map_err(|_| OmniError::LockPoisoned("index catalog".into()))?;

        let indexes = catalog.indexes_for_collection(collection);

        for idx_def in indexes {
            let mut encoded_values = Vec::new();
            let mut all_fields_present = true;

            for (field_name, field_type) in &idx_def.fields {
                if let Some(val) = old_document.get(field_name) {
                    encoded_values.push(encode_index_value(val, field_type));
                } else {
                    all_fields_present = false;
                    break;
                }
            }

            if !all_fields_present {
                continue;
            }

            let idx_key = build_index_key(idx_def.id, &encoded_values, primary_key);
            batch.delete(&idx_key)?;
        }

        Ok(())
    }

    /// LOOKUP — finds all primary keys matching an exact value on an index.
    pub fn lookup(
        &self,
        index_name: &str,
        values: &[serde_json::Value],
    ) -> Result<Vec<String>, OmniError> {
        let catalog = self
            .catalog
            .lock()
            .map_err(|_| OmniError::LockPoisoned("index catalog".into()))?;

        let idx_def = catalog
            .get_by_name(index_name)
            .ok_or_else(|| OmniError::IoError(format!("Index '{}' not found", index_name)))?;

        if values.len() != idx_def.fields.len() {
            return Err(OmniError::IoError(format!(
                "Expected {} values for index '{}', got {}",
                idx_def.fields.len(),
                index_name,
                values.len()
            )));
        }

        let encoded: Vec<Vec<u8>> = values
            .iter()
            .zip(idx_def.fields.iter())
            .map(|(v, (_, ft))| encode_index_value(v, ft))
            .collect();

        let prefix = build_index_value_prefix(idx_def.id, &encoded);
        let end = format!("{}\x7f", prefix);
        let seq = self.db.get_seq();

        let mut results = Vec::new();
        if let Ok(iter) = self.db.scan_iter(&prefix, &end, seq) {
            for (_, pk) in iter {
                results.push(pk);
            }
        }

        Ok(results)
    }

    /// RANGE SCAN — finds all primary keys where the indexed field falls
    /// within [start_value, end_value].
    pub fn range_scan(
        &self,
        index_name: &str,
        start_value: &serde_json::Value,
        end_value: &serde_json::Value,
    ) -> Result<Vec<String>, OmniError> {
        let catalog = self
            .catalog
            .lock()
            .map_err(|_| OmniError::LockPoisoned("index catalog".into()))?;

        let idx_def = catalog
            .get_by_name(index_name)
            .ok_or_else(|| OmniError::IoError(format!("Index '{}' not found", index_name)))?;

        if idx_def.fields.len() != 1 {
            return Err(OmniError::IoError(
                "Range scan only supported on single-field indexes".into(),
            ));
        }

        let (_, ref ft) = idx_def.fields[0];
        let start_encoded = encode_index_value(start_value, ft);
        let end_encoded = encode_index_value(end_value, ft);

        let start_key = build_index_value_prefix(idx_def.id, &[start_encoded]);
        // Use the end value prefix + \xff\xff to capture all entries at the end value
        // (including all possible primary key suffixes after the \x00 separator)
        let end_key = format!("{}~~", build_index_value_prefix(idx_def.id, &[end_encoded]));
        let seq = self.db.get_seq();

        let mut results = Vec::new();
        if let Ok(iter) = self.db.scan_iter(&start_key, &end_key, seq) {
            for (_, pk) in iter {
                results.push(pk);
            }
        }

        Ok(results)
    }

    /// REBUILD INDEX — retroactively indexes all existing documents in a collection.
    ///
    /// Scans all keys matching the collection prefix and writes index entries.
    /// Should be called after CREATE INDEX on a collection with existing data.
    pub fn rebuild_index(&self, index_name: &str) -> Result<usize, OmniError> {
        let catalog = self
            .catalog
            .lock()
            .map_err(|_| OmniError::LockPoisoned("index catalog".into()))?;

        let idx_def = catalog
            .get_by_name(index_name)
            .ok_or_else(|| OmniError::IoError(format!("Index '{}' not found", index_name)))?
            .clone();
        drop(catalog); // Release lock before scanning

        let collection_prefix = format!("{}:", idx_def.collection);
        let collection_end = format!("{}:\x7f", idx_def.collection);
        let seq = self.db.get_seq();

        let mut count = 0;
        let mut batch = WriteBatch::new();

        if let Ok(iter) = self.db.scan_iter(&collection_prefix, &collection_end, seq) {
            for (key, value) in iter {
                if let Ok(doc) = serde_json::from_str::<serde_json::Value>(&value) {
                    let pk = key.strip_prefix(&collection_prefix).unwrap_or(&key);
                    self.index_document(&idx_def.collection, pk, &doc, &mut batch)?;
                    count += 1;

                    // Commit in batches of 1000 to avoid huge memory usage
                    if count % 1000 == 0 {
                        self.db.commit_batch(&batch)?;
                        batch = WriteBatch::new();
                    }
                }
            }
        }

        if !batch.buffered_writes.is_empty() {
            self.db.commit_batch(&batch)?;
        }

        Ok(count)
    }

    /// Returns all index definitions for a collection.
    pub fn list_indexes(&self, collection: &str) -> Result<Vec<IndexDefinition>, OmniError> {
        let catalog = self
            .catalog
            .lock()
            .map_err(|_| OmniError::LockPoisoned("index catalog".into()))?;
        Ok(catalog
            .indexes_for_collection(collection)
            .into_iter()
            .cloned()
            .collect())
    }
}
