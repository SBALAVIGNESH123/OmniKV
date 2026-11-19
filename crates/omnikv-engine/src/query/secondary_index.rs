//! Secondary Index Engine
//!
//! A sorted index system supporting:
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
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::{OmniError, OmniKV, WriteBatch};

const INDEX_PREFIX: &[u8] = b"\x00IDX\x00";
const CATALOG_PREFIX: &str = "\x00IDX_CATALOG\x00";
const READY_PREFIX: &str = "\x00IDX_READY\x00";
const RETIRED_PREFIX: &str = "\x00IDX_RETIRED\x00";
const NEXT_ID_KEY: &str = "\x00IDX_NEXT_ID\x00";
const DDL_LOCK_NAME: &str = "\x00IDX_CATALOG\x00ddl";

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
#[derive(Clone)]
pub struct IndexCatalog {
    /// All known indexes, keyed by index ID.
    indexes: HashMap<IndexId, IndexDefinition>,
    /// Lookup: (collection, field_name) → list of index IDs that include this field.
    field_index: HashMap<(String, String), Vec<IndexId>>,
    /// Next auto-increment index ID.
    next_id: IndexId,
    /// An index becomes a query access path only after a successful backfill.
    ready: HashSet<IndexId>,
}

impl IndexCatalog {
    /// Loads the catalog from OmniKV, or creates an empty one.
    pub fn load(db: &Arc<OmniKV>) -> Self {
        Self::load_at(db, db.get_seq())
    }

    /// Loads the catalog as of an MVCC sequence.
    pub fn load_at(db: &Arc<OmniKV>, seq: u64) -> Self {
        Self::load_checked_at(db, seq).unwrap_or_else(|_| Self::empty())
    }

    fn empty() -> Self {
        Self {
            indexes: HashMap::new(),
            field_index: HashMap::new(),
            next_id: 1,
            ready: HashSet::new(),
        }
    }

    /// The production path fails closed on unreadable or malformed metadata.
    pub fn load_checked_at(db: &Arc<OmniKV>, seq: u64) -> Result<Self, OmniError> {
        let mut catalog = Self::empty();
        let mut names = HashSet::new();
        for (key, value) in db.scan(
            CATALOG_PREFIX,
            &format!("{}\u{10ffff}", CATALOG_PREFIX),
            seq,
        )? {
            let def: IndexDefinition = serde_json::from_str(&value)
                .map_err(|error| OmniError::IoError(format!("invalid index catalog: {error}")))?;
            if def.fields.is_empty()
                || key != format!("{CATALOG_PREFIX}{}", def.id)
                || !names.insert(def.name.to_lowercase())
            {
                return Err(OmniError::IoError(
                    "invalid or duplicate index definition".into(),
                ));
            }
            catalog.next_id = catalog.next_id.max(
                def.id
                    .checked_add(1)
                    .ok_or_else(|| OmniError::IoError("index ID space exhausted".into()))?,
            );
            if db
                .find(&format!("{READY_PREFIX}{}", def.id), seq)?
                .as_deref()
                == Some("ready")
            {
                catalog.ready.insert(def.id);
            }
            catalog.register_in_memory(def);
        }
        for (key, _) in db.scan(
            RETIRED_PREFIX,
            &format!("{}\u{10ffff}", RETIRED_PREFIX),
            seq,
        )? {
            if let Some(id_str) = key.strip_prefix(RETIRED_PREFIX)
                && let Ok(id) = id_str.parse::<IndexId>()
            {
                catalog.next_id =
                    catalog
                        .next_id
                        .max(id.checked_add(1).ok_or_else(|| {
                            OmniError::IoError("index ID space exhausted".into())
                        })?);
            }
        }
        if let Some(next_id) = db.find(NEXT_ID_KEY, seq)? {
            let next_id = next_id.parse::<IndexId>().map_err(|error| {
                OmniError::IoError(format!("invalid index ID counter: {error}"))
            })?;
            catalog.next_id = catalog.next_id.max(next_id);
        }
        Ok(catalog)
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
            .filter(|idx| idx.collection.eq_ignore_ascii_case(collection))
            .collect()
    }

    /// Returns a specific index by name.
    pub fn get_by_name(&self, name: &str) -> Option<&IndexDefinition> {
        self.indexes
            .values()
            .find(|idx| idx.name.eq_ignore_ascii_case(name))
    }

    /// Returns a specific index by ID.
    pub fn get(&self, id: IndexId) -> Option<&IndexDefinition> {
        self.indexes.get(&id)
    }

    pub fn is_ready(&self, id: IndexId) -> bool {
        self.ready.contains(&id)
    }

    pub(crate) fn catalog_scan_range() -> (&'static str, &'static str) {
        (CATALOG_PREFIX, "\x00IDX_CATALOG\x00\u{10ffff}")
    }

    pub(crate) fn apply_overlay(&mut self, overlay: &HashMap<String, Option<String>>) {
        for (key, value) in overlay {
            if let Some(id_str) = key.strip_prefix(CATALOG_PREFIX)
                && let Ok(id) = id_str.parse::<IndexId>()
            {
                if let Some(old) = self.indexes.remove(&id) {
                    for (field_name, _) in &old.fields {
                        if let Some(ids) = self
                            .field_index
                            .get_mut(&(old.collection.clone(), field_name.clone()))
                        {
                            ids.retain(|&existing| existing != id);
                        }
                    }
                    self.ready.remove(&id);
                }
                if let Some(raw) = value
                    && let Ok(def) = serde_json::from_str::<IndexDefinition>(raw)
                {
                    self.register_in_memory(def);
                }
            } else if let Some(id_str) = key.strip_prefix(READY_PREFIX)
                && let Ok(id) = id_str.parse::<IndexId>()
            {
                if value.as_deref() == Some("ready") {
                    self.ready.insert(id);
                } else {
                    self.ready.remove(&id);
                }
            }
        }
    }
}

/// Encodes a field value into a byte sequence that preserves sort order.
pub fn encode_index_value(value: &serde_json::Value, field_type: &IndexFieldType) -> Vec<u8> {
    try_encode_index_value(value, field_type).unwrap_or_default()
}

fn try_encode_index_value(
    value: &serde_json::Value,
    field_type: &IndexFieldType,
) -> Option<Vec<u8>> {
    match field_type {
        IndexFieldType::String => {
            // UTF-8 strings are already lexicographically sortable.
            // We escape \x00 bytes to avoid collision with our separator.
            let s = value.as_str()?;
            let mut encoded = Vec::with_capacity(s.len());
            for b in s.as_bytes() {
                if *b == 0x00 {
                    encoded.push(0x00);
                    encoded.push(0x01); // escape
                } else {
                    encoded.push(*b);
                }
            }
            Some(encoded)
        }
        IndexFieldType::Integer => {
            // Big-endian encoding with sign bit flip for correct sort order.
            // This ensures -100 < -1 < 0 < 1 < 100 in byte ordering.
            let n = value
                .as_i64()
                .or_else(|| value.as_str()?.parse::<i64>().ok())?;
            let flipped = (n ^ i64::MIN) as u64; // flip sign bit
            Some(flipped.to_be_bytes().to_vec())
        }
        IndexFieldType::Float => {
            // IEEE 754 float encoding with sign handling for sort order.
            let f = value
                .as_f64()
                .or_else(|| value.as_str()?.parse::<f64>().ok())?;
            if !f.is_finite() {
                return None;
            }
            let f = if f == 0.0 { 0.0 } else { f };
            let bits = f.to_bits();
            let sortable = if f.is_sign_negative() {
                !bits // negative: flip all bits
            } else {
                bits ^ (1u64 << 63) // positive: flip sign bit only
            };
            Some(sortable.to_be_bytes().to_vec())
        }
    }
}

fn build_index_key(index_id: IndexId, encoded_values: &[Vec<u8>], primary_key: &str) -> String {
    let mut key = String::new();
    key.push_str(&format!("\x00IDX\x00{:08}\x00", index_id));
    for (i, val) in encoded_values.iter().enumerate() {
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

fn build_exact_index_value_prefix(index_id: IndexId, encoded_values: &[Vec<u8>]) -> String {
    let mut key = build_index_value_prefix(index_id, encoded_values);
    key.push('\x00');
    key
}

/// Hex encoding for binary index values — preserves lexicographic sort order.
fn hex_encode(data: &[u8]) -> String {
    data.iter().map(|b| format!("{:02x}", b)).collect()
}

fn document_field<'a>(
    document: &'a serde_json::Value,
    field: &str,
) -> Option<&'a serde_json::Value> {
    document.get(field).or_else(|| {
        document
            .as_object()?
            .iter()
            .find_map(|(name, value)| name.eq_ignore_ascii_case(field).then_some(value))
    })
}

fn index_entries(
    catalog: &IndexCatalog,
    collection: &str,
    primary_key: &str,
    document: &serde_json::Value,
) -> Result<Vec<(IndexDefinition, String)>, OmniError> {
    let mut entries = Vec::new();
    'indexes: for definition in catalog.indexes_for_collection(collection) {
        let mut values = Vec::new();
        for (field, field_type) in &definition.fields {
            let Some(value) = document_field(document, field).filter(|value| !value.is_null())
            else {
                continue 'indexes;
            };
            let encoded = try_encode_index_value(value, field_type).ok_or_else(|| {
                OmniError::IoError(format!(
                    "invalid value for indexed field '{field}' in index '{}'",
                    definition.name
                ))
            })?;
            values.push(encoded);
        }
        entries.push((
            definition.clone(),
            build_index_key(definition.id, &values, primary_key),
        ));
    }
    Ok(entries)
}

/// Converts a SQL comparison to the stored index representation.
pub(crate) fn sql_index_bound(
    value: &crate::sql::SqlValue,
    field_type: &IndexFieldType,
) -> Option<serde_json::Value> {
    if matches!(
        value,
        crate::sql::SqlValue::Null | crate::sql::SqlValue::Placeholder(_)
    ) {
        return None;
    }
    let text = value.as_string();
    let result = serde_json::Value::String(text);
    try_encode_index_value(&result, field_type)?;
    Some(result)
}

fn unique_violation(idx_def: &IndexDefinition) -> OmniError {
    OmniError::IoError(format!(
        "UNIQUE CONSTRAINT VIOLATION: index '{}' on {:?}",
        idx_def.name,
        idx_def
            .fields
            .iter()
            .map(|(field, _)| field.as_str())
            .collect::<Vec<_>>()
    ))
}

/// Batch helpers: last-write-wins even when an indexed value is replaced more than once.
fn append_index_set(batch: &mut WriteBatch, key: &str, value: &str) -> Result<(), OmniError> {
    let key_bytes = key.as_bytes();
    batch
        .buffered_writes
        .retain(|(existing, _, _)| existing.as_slice() != key_bytes);
    batch
        .buffered_deletes
        .retain(|existing| existing.as_slice() != key_bytes);
    batch.set(key, value.to_string())
}

fn append_index_delete(batch: &mut WriteBatch, key: &str) -> Result<(), OmniError> {
    let key_bytes = key.as_bytes();
    batch
        .buffered_writes
        .retain(|(existing, _, _)| existing.as_slice() != key_bytes);
    batch
        .buffered_deletes
        .retain(|existing| existing.as_slice() != key_bytes);
    batch.delete(key)
}

/// Row ranges for both collection/doc keys and SQL table keys.
pub(crate) fn collection_row_ranges(collection: &str) -> [(String, String); 2] {
    [
        (
            format!("{}:", collection),
            format!("{}:\u{10ffff}", collection),
        ),
        (
            format!("\x00T\x00{}\x00", collection),
            format!("\x00T\x00{}\x00\u{10ffff}", collection),
        ),
    ]
}

/// The Index Manager — the main public interface for index operations.
///
/// Manages the catalog, writes index entries on mutations, and provides
/// index-based lookups and range scans.
pub struct IndexManager {
    db: Arc<OmniKV>,
}

impl IndexManager {
    /// Creates a new IndexManager, loading existing index definitions from OmniKV.
    pub fn new(db: Arc<OmniKV>) -> Self {
        Self { db }
    }

    /// Refreshes definitions from durable storage for the latest sequence.
    pub fn refresh_catalog(&self) -> Result<(), OmniError> {
        self.refresh_catalog_at(self.db.get_seq())
    }

    /// Refreshes definitions from durable storage at a caller-owned snapshot.
    pub fn refresh_catalog_at(&self, seq: u64) -> Result<(), OmniError> {
        IndexCatalog::load_checked_at(&self.db, seq).map(|_| ())
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
        let lock = self.db.sql_table_lock(collection)?;
        let _guard = lock
            .lock()
            .map_err(|_| OmniError::LockPoisoned("index collection".into()))?;
        self.create_index_locked(name, collection, fields, unique)
    }

    pub(crate) fn create_index_locked(
        &self,
        name: &str,
        collection: &str,
        fields: Vec<(String, IndexFieldType)>,
        unique: bool,
    ) -> Result<IndexId, OmniError> {
        let ddl_lock = self.db.sql_table_lock(DDL_LOCK_NAME)?;
        let _ddl_guard = ddl_lock
            .lock()
            .map_err(|_| OmniError::LockPoisoned("index DDL".into()))?;
        if name.trim().is_empty() || collection.trim().is_empty() || fields.is_empty() {
            return Err(OmniError::IoError(
                "index name, collection, and fields must not be empty".into(),
            ));
        }
        let mut field_names = std::collections::HashSet::new();
        if fields
            .iter()
            .any(|(field, _)| field.trim().is_empty() || !field_names.insert(field.to_lowercase()))
        {
            return Err(OmniError::IoError(
                "index fields must be non-empty and unique".into(),
            ));
        }
        let table = self
            .db
            .find(
                &crate::catalog::Catalog::catalog_key(collection),
                self.db.get_seq(),
            )?
            .map(|value| {
                serde_json::from_str::<crate::catalog::TableDef>(&value).map_err(|error| {
                    OmniError::IoError(format!("invalid SQL table definition: {error}"))
                })
            })
            .transpose()?;
        if let Some(table) = &table {
            for (field, field_type) in &fields {
                let column = table
                    .columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(field))
                    .ok_or_else(|| {
                        OmniError::IoError(format!("unknown indexed column '{field}'"))
                    })?;
                let compatible = match field_type {
                    IndexFieldType::Integer => {
                        column.col_type == crate::catalog::ColumnType::Integer
                    }
                    IndexFieldType::Float => column.col_type == crate::catalog::ColumnType::Float,
                    IndexFieldType::String => !column.col_type.is_numeric(),
                };
                if !compatible {
                    return Err(OmniError::IoError(format!(
                        "index field type does not match SQL column '{field}'"
                    )));
                }
            }
        }
        let collection = table
            .as_ref()
            .map_or(collection, |table| table.name.as_str());
        self.cleanup_retired_indexes_for_collection(collection)?;
        let catalog = IndexCatalog::load_checked_at(&self.db, self.db.get_seq())?;

        if catalog.get_by_name(name).is_some() {
            return Err(OmniError::IoError(format!(
                "Index '{}' already exists",
                name
            )));
        }

        let id = catalog.next_id;
        let next_id = id
            .checked_add(1)
            .ok_or_else(|| OmniError::IoError("index ID space exhausted".into()))?;

        let def = IndexDefinition {
            id,
            name: name.to_string(),
            collection: collection.to_string(),
            fields,
            unique,
        };

        let catalog_key = format!("{}{}", CATALOG_PREFIX, id);
        let catalog_value =
            serde_json::to_string(&def).map_err(|e| OmniError::IoError(e.to_string()))?;

        let mut batch = WriteBatch::new();
        batch.set(&catalog_key, catalog_value)?;
        batch.set(NEXT_ID_KEY, next_id.to_string())?;
        let commit_seq = self.db.commit_batch(&batch)?;
        self.db.ssi_history().record_committed(
            0,
            commit_seq,
            &crate::transaction::SsiCommitRecord {
                txn_id: 0,
                write_keys: vec![catalog_key],
                read_keys: Vec::new(),
                read_ranges: Vec::new(),
            },
        );

        Ok(id)
    }

    /// DROP INDEX — removes an index definition and all its entries.
    pub fn drop_index(&self, name: &str) -> Result<(), OmniError> {
        let definition = match self.definition(name) {
            Ok(definition) => definition,
            Err(err) => self.find_retired_index(name, None)?.ok_or(err)?,
        };
        let lock = self.db.sql_table_lock(&definition.collection)?;
        let _guard = lock
            .lock()
            .map_err(|_| OmniError::LockPoisoned("index collection".into()))?;
        self.drop_index_locked(name, &definition.collection)
    }

    pub(crate) fn drop_index_locked(
        &self,
        name: &str,
        expected_collection: &str,
    ) -> Result<(), OmniError> {
        let ddl_lock = self.db.sql_table_lock(DDL_LOCK_NAME)?;
        let _ddl_guard = ddl_lock
            .lock()
            .map_err(|_| OmniError::LockPoisoned("index DDL".into()))?;
        let catalog = IndexCatalog::load_checked_at(&self.db, self.db.get_seq())?;

        let id = if let Some(def) = catalog
            .get_by_name(name)
            .filter(|d| d.collection.eq_ignore_ascii_case(expected_collection))
        {
            let id = def.id;
            let catalog_key = format!("{CATALOG_PREFIX}{id}");
            let ready_key = format!("{READY_PREFIX}{id}");
            let retired_key = format!("{RETIRED_PREFIX}{id}");
            let retired_value =
                serde_json::to_string(def).map_err(|e| OmniError::IoError(e.to_string()))?;
            let mut meta_batch = WriteBatch::new();
            meta_batch.delete(&catalog_key)?;
            meta_batch.delete(&ready_key)?;
            meta_batch.set(&retired_key, retired_value)?;
            let commit_seq = self.db.commit_batch(&meta_batch)?;
            self.db.ssi_history().record_committed(
                0,
                commit_seq,
                &crate::transaction::SsiCommitRecord {
                    txn_id: 0,
                    write_keys: vec![catalog_key],
                    read_keys: Vec::new(),
                    read_ranges: Vec::new(),
                },
            );
            id
        } else if let Some(retired) = self.find_retired_index(name, Some(expected_collection))? {
            retired.id
        } else {
            return Err(OmniError::IoError(format!("Index '{}' not found", name)));
        };

        self.purge_retired_index_entries(id)
    }

    fn find_retired_index(
        &self,
        name: &str,
        expected_collection: Option<&str>,
    ) -> Result<Option<IndexDefinition>, OmniError> {
        let seq = self.db.get_seq();
        for (_, value) in self.db.scan(
            RETIRED_PREFIX,
            &format!("{}\u{10ffff}", RETIRED_PREFIX),
            seq,
        )? {
            let Ok(def) = serde_json::from_str::<IndexDefinition>(&value) else {
                continue;
            };
            if def.name.eq_ignore_ascii_case(name)
                && expected_collection
                    .is_none_or(|expected| def.collection.eq_ignore_ascii_case(expected))
            {
                return Ok(Some(def));
            }
        }
        Ok(None)
    }

    fn cleanup_retired_indexes_for_collection(&self, collection: &str) -> Result<(), OmniError> {
        let seq = self.db.get_seq();
        for (_, value) in self.db.scan(
            RETIRED_PREFIX,
            &format!("{}\u{10ffff}", RETIRED_PREFIX),
            seq,
        )? {
            if let Ok(def) = serde_json::from_str::<IndexDefinition>(&value)
                && def.collection.eq_ignore_ascii_case(collection)
            {
                self.purge_retired_index_entries(def.id)?;
            }
        }
        Ok(())
    }

    fn purge_retired_index_entries(&self, id: IndexId) -> Result<(), OmniError> {
        const DELETE_BATCH_LIMIT: usize = WriteBatch::MAX_OPS / 2;
        let prefix = build_index_scan_prefix(id);
        let end = format!("{}\u{10ffff}", prefix);
        let retired_key = format!("{RETIRED_PREFIX}{id}");
        loop {
            let seq = self.db.get_seq();
            let entries = self.db.scan(&prefix, &end, seq)?;
            if entries.is_empty() {
                let mut clear_batch = WriteBatch::new();
                clear_batch.delete(&retired_key)?;
                self.db.commit_batch(&clear_batch)?;
                return Ok(());
            }
            let total = entries.len();
            let mut batch = WriteBatch::new();
            for (key, _) in entries.into_iter().take(DELETE_BATCH_LIMIT) {
                batch.delete(&key)?;
            }
            if total <= DELETE_BATCH_LIMIT {
                batch.delete(&retired_key)?;
                self.db.commit_batch(&batch)?;
                return Ok(());
            }
            self.db.commit_batch(&batch)?;
        }
    }

    /// Returns the collection owning an index, used by schema migration for DDL serialization.
    pub fn collection_for_index(&self, name: &str) -> Option<String> {
        IndexCatalog::load_checked_at(&self.db, self.db.get_seq())
            .ok()
            .and_then(|catalog| catalog.get_by_name(name).map(|idx| idx.collection.clone()))
    }

    pub fn collect_drop_index_keys_for_collection_at(
        &self,
        collection: &str,
        seq: u64,
    ) -> Result<(Vec<String>, Vec<String>), OmniError> {
        let catalog = IndexCatalog::load_checked_at(&self.db, seq)?;
        let mut entry_keys = Vec::new();
        let mut meta_keys = Vec::new();
        for def in catalog.indexes_for_collection(collection) {
            meta_keys.push(format!("{}{}", CATALOG_PREFIX, def.id));
            meta_keys.push(format!("{READY_PREFIX}{}", def.id));
            let prefix = build_index_scan_prefix(def.id);
            let end = format!("{}\u{10ffff}", prefix);
            for (key, _) in self.db.scan(&prefix, &end, seq)? {
                entry_keys.push(key);
            }
        }
        for (ret_key, value) in self.db.scan(
            RETIRED_PREFIX,
            &format!("{}\u{10ffff}", RETIRED_PREFIX),
            seq,
        )? {
            if let Ok(def) = serde_json::from_str::<IndexDefinition>(&value)
                && def.collection.eq_ignore_ascii_case(collection)
            {
                meta_keys.push(ret_key);
                let prefix = build_index_scan_prefix(def.id);
                let end = format!("{}\u{10ffff}", prefix);
                for (key, _) in self.db.scan(&prefix, &end, seq)? {
                    entry_keys.push(key);
                }
            }
        }
        Ok((entry_keys, meta_keys))
    }

    pub fn append_drop_indexes_for_collection(
        &self,
        collection: &str,
        batch: &mut WriteBatch,
    ) -> Result<(), OmniError> {
        self.append_drop_indexes_for_collection_at(collection, self.db.get_seq(), batch)
    }

    pub fn append_drop_indexes_for_collection_at(
        &self,
        collection: &str,
        seq: u64,
        batch: &mut WriteBatch,
    ) -> Result<(), OmniError> {
        let catalog = IndexCatalog::load_checked_at(&self.db, seq)?;
        for def in catalog.indexes_for_collection(collection) {
            let cat_key = format!("{}{}", CATALOG_PREFIX, def.id);
            let ready_key = format!("{READY_PREFIX}{}", def.id);
            let prefix = build_index_scan_prefix(def.id);
            let end = format!("{}\u{10ffff}", prefix);
            let staged_keys: Vec<String> = batch
                .buffered_writes
                .iter()
                .filter_map(|(key, _, _)| {
                    if key.starts_with(prefix.as_bytes()) {
                        std::str::from_utf8(key).ok().map(str::to_string)
                    } else {
                        None
                    }
                })
                .collect();
            batch.buffered_writes.retain(|(key, _, _)| {
                key.as_slice() != cat_key.as_bytes()
                    && key.as_slice() != ready_key.as_bytes()
                    && !key.starts_with(prefix.as_bytes())
            });
            batch.delete(&cat_key)?;
            batch.delete(&ready_key)?;
            for key in staged_keys {
                batch.delete(&key)?;
            }
            for (key, _) in self.db.scan(&prefix, &end, seq)? {
                if !batch
                    .buffered_deletes
                    .iter()
                    .any(|deleted| deleted.as_slice() == key.as_bytes())
                {
                    batch.delete(&key)?;
                }
            }
        }
        Ok(())
    }

    /// Returns a stable copy of an index definition for a query plan.
    pub fn definition(&self, name: &str) -> Result<IndexDefinition, OmniError> {
        self.definition_at(name, self.db.get_seq())
    }

    /// Returns an index definition visible at a caller-owned MVCC sequence.
    pub fn definition_at(&self, name: &str, seq: u64) -> Result<IndexDefinition, OmniError> {
        let catalog = IndexCatalog::load_checked_at(&self.db, seq)?;
        catalog
            .get_by_name(name)
            .cloned()
            .ok_or_else(|| OmniError::IoError(format!("Index '{}' not found", name)))
    }

    /// Read-only catalog snapshot for the optimizer.
    pub fn catalog_snapshot(&self) -> Result<IndexCatalog, OmniError> {
        self.catalog_snapshot_at(self.db.get_seq())
    }

    /// Returns definitions visible at a caller-owned MVCC sequence.
    pub fn catalog_snapshot_at(&self, seq: u64) -> Result<IndexCatalog, OmniError> {
        IndexCatalog::load_checked_at(&self.db, seq)
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
        let catalog = self.catalog_snapshot()?;
        self.index_document_with_catalog(&catalog, collection, primary_key, document, batch)
    }

    pub fn index_document_with_catalog(
        &self,
        catalog: &IndexCatalog,
        collection: &str,
        primary_key: &str,
        document: &serde_json::Value,
        batch: &mut WriteBatch,
    ) -> Result<(), OmniError> {
        self.index_document_with_catalog_and_reads(
            catalog,
            collection,
            primary_key,
            document,
            batch,
            None,
        )
    }

    pub fn index_document_with_catalog_and_reads(
        &self,
        catalog: &IndexCatalog,
        collection: &str,
        primary_key: &str,
        document: &serde_json::Value,
        batch: &mut WriteBatch,
        read_collector: Option<&mut crate::volcano::ReadCollector>,
    ) -> Result<(), OmniError> {
        let entries = index_entries(catalog, collection, primary_key, document)?;
        self.write_new_index_entries(&entries, primary_key, batch, read_collector)
    }

    /// Replaces index entries for one row. Skips unchanged values to avoid delete-after-write.
    pub fn replace_document_indexes(
        &self,
        collection: &str,
        old_primary_key: &str,
        old_document: &serde_json::Value,
        new_primary_key: &str,
        new_document: &serde_json::Value,
        batch: &mut WriteBatch,
    ) -> Result<(), OmniError> {
        let catalog = self.catalog_snapshot()?;
        self.replace_document_indexes_with_catalog(
            &catalog,
            collection,
            old_primary_key,
            old_document,
            new_primary_key,
            new_document,
            batch,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn replace_document_indexes_with_catalog(
        &self,
        catalog: &IndexCatalog,
        collection: &str,
        old_primary_key: &str,
        old_document: &serde_json::Value,
        new_primary_key: &str,
        new_document: &serde_json::Value,
        batch: &mut WriteBatch,
    ) -> Result<(), OmniError> {
        self.replace_document_indexes_with_catalog_and_reads(
            catalog,
            collection,
            old_primary_key,
            old_document,
            new_primary_key,
            new_document,
            batch,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn replace_document_indexes_with_catalog_and_reads(
        &self,
        catalog: &IndexCatalog,
        collection: &str,
        old_primary_key: &str,
        old_document: &serde_json::Value,
        new_primary_key: &str,
        new_document: &serde_json::Value,
        batch: &mut WriteBatch,
        read_collector: Option<&mut crate::volcano::ReadCollector>,
    ) -> Result<(), OmniError> {
        let old_entries = index_entries(catalog, collection, old_primary_key, old_document)?;
        let new_entries = index_entries(catalog, collection, new_primary_key, new_document)?;
        let new_keys: std::collections::HashSet<&str> =
            new_entries.iter().map(|(_, key)| key.as_str()).collect();

        for (_, old_key) in &old_entries {
            if !new_keys.contains(old_key.as_str()) {
                append_index_delete(batch, old_key)?;
            }
        }
        self.write_new_index_entries(&new_entries, new_primary_key, batch, read_collector)
    }

    /// Removes index entries for a DELETE or pre-UPDATE cleanup.
    pub fn remove_document_indexes(
        &self,
        collection: &str,
        primary_key: &str,
        old_document: &serde_json::Value,
        batch: &mut WriteBatch,
    ) -> Result<(), OmniError> {
        let catalog = self.catalog_snapshot()?;
        self.remove_document_indexes_with_catalog(
            &catalog,
            collection,
            primary_key,
            old_document,
            batch,
        )
    }

    pub fn remove_document_indexes_with_catalog(
        &self,
        catalog: &IndexCatalog,
        collection: &str,
        primary_key: &str,
        old_document: &serde_json::Value,
        batch: &mut WriteBatch,
    ) -> Result<(), OmniError> {
        let entries = index_entries(catalog, collection, primary_key, old_document)?;
        for (_, key) in entries {
            append_index_delete(batch, &key)?;
        }
        Ok(())
    }

    fn write_new_index_entries(
        &self,
        entries: &[(IndexDefinition, String)],
        primary_key: &str,
        batch: &mut WriteBatch,
        mut read_collector: Option<&mut crate::volcano::ReadCollector>,
    ) -> Result<(), OmniError> {
        for (idx_def, idx_key) in entries {
            if idx_def.unique {
                self.check_unique(
                    idx_def,
                    idx_key,
                    primary_key,
                    batch,
                    read_collector.as_deref_mut(),
                )?;
            }
            append_index_set(batch, idx_key, primary_key)?;
        }
        Ok(())
    }

    fn check_unique(
        &self,
        idx_def: &IndexDefinition,
        idx_key: &str,
        primary_key: &str,
        batch: &WriteBatch,
        read_collector: Option<&mut crate::volcano::ReadCollector>,
    ) -> Result<(), OmniError> {
        let prefix = &idx_key[..idx_key.len() - primary_key.len()];
        let end = format!("{}\x01", &prefix[..prefix.len() - 1]);
        if let Some(collector) = read_collector {
            collector.ranges.push((prefix.to_string(), end.clone()));
        }
        let seq = self.db.get_seq();
        for (existing_key, existing_pk) in self.db.scan(prefix, &end, seq)? {
            if batch
                .buffered_deletes
                .iter()
                .any(|deleted| deleted.as_slice() == existing_key.as_bytes())
            {
                continue;
            }
            if existing_pk != primary_key {
                return Err(unique_violation(idx_def));
            }
        }
        // Dedup against conflicting staged deletes in the same batch.
        for (pending_key, pending_pk, _) in &batch.buffered_writes {
            let Ok(pending_key) = std::str::from_utf8(pending_key) else {
                continue;
            };
            if pending_key >= prefix
                && pending_key < end.as_str()
                && !batch
                    .buffered_deletes
                    .iter()
                    .any(|deleted| deleted.as_slice() == pending_key.as_bytes())
                && pending_pk != primary_key
            {
                return Err(unique_violation(idx_def));
            }
        }
        Ok(())
    }

    /// LOOKUP — finds all primary keys matching an exact value on an index.
    pub fn lookup(
        &self,
        index_name: &str,
        values: &[serde_json::Value],
    ) -> Result<Vec<String>, OmniError> {
        self.refresh_catalog()?;
        self.lookup_at(index_name, values, self.db.get_seq())
    }

    /// LOOKUP at a caller-owned MVCC sequence.
    pub fn lookup_at(
        &self,
        index_name: &str,
        values: &[serde_json::Value],
        seq: u64,
    ) -> Result<Vec<String>, OmniError> {
        let catalog = self.catalog_snapshot_at(seq)?;

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

        let encoded = values
            .iter()
            .zip(idx_def.fields.iter())
            .map(|(value, (_, field_type))| {
                try_encode_index_value(value, field_type)
                    .ok_or_else(|| OmniError::IoError("invalid index lookup value".into()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let prefix = build_exact_index_value_prefix(idx_def.id, &encoded);
        let end = format!("{}\x01", &prefix[..prefix.len() - 1]);

        let mut results = Vec::new();
        for (_, pk) in self.db.scan(&prefix, &end, seq)? {
            results.push(pk);
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
        self.refresh_catalog()?;
        self.range_scan_bounds_at(
            index_name,
            Some(start_value),
            Some(end_value),
            self.db.get_seq(),
        )
    }

    /// Range scan on the index's leading field at a given MVCC sequence.
    pub fn range_scan_bounds_at(
        &self,
        index_name: &str,
        start_value: Option<&serde_json::Value>,
        end_value: Option<&serde_json::Value>,
        seq: u64,
    ) -> Result<Vec<String>, OmniError> {
        let catalog = self.catalog_snapshot_at(seq)?;

        let idx_def = catalog
            .get_by_name(index_name)
            .ok_or_else(|| OmniError::IoError(format!("Index '{}' not found", index_name)))?;

        let Some((_, ft)) = idx_def.fields.first() else {
            return Err(OmniError::IoError(format!(
                "Index '{}' has no fields",
                index_name
            )));
        };
        let index_prefix = build_index_scan_prefix(idx_def.id);
        let encode_bound = |value| {
            try_encode_index_value(value, ft)
                .map(|encoded| hex_encode(&encoded))
                .ok_or_else(|| OmniError::IoError("invalid index range bound".into()))
        };
        let start_hex = start_value.map(encode_bound).transpose()?;
        let end_hex = end_value.map(encode_bound).transpose()?;
        if start_hex
            .as_ref()
            .zip(end_hex.as_ref())
            .is_some_and(|(start, end)| start > end)
        {
            return Ok(Vec::new());
        }
        let start_key = start_hex.as_ref().map_or_else(
            || index_prefix.clone(),
            |value| format!("{index_prefix}{value}\x00"),
        );
        let end_key = end_hex.as_ref().map_or_else(
            || format!("{}\x01", &index_prefix[..index_prefix.len() - 1]),
            |value| format!("{index_prefix}{value}\x01"),
        );

        let mut results = Vec::new();
        for (key, pk) in self.db.scan(&start_key, &end_key, seq)? {
            let Some(value_hex) = key
                .strip_prefix(&index_prefix)
                .and_then(|suffix| suffix.split('\x00').next())
            else {
                continue;
            };
            if start_hex.as_deref().is_some_and(|start| value_hex < start)
                || end_hex.as_deref().is_some_and(|end| value_hex > end)
            {
                continue;
            }
            results.push(pk);
        }

        Ok(results)
    }

    /// REBUILD INDEX — retroactively indexes all existing documents in a collection.
    ///
    /// Scans all keys matching the collection prefix and writes index entries.
    /// Should be called after CREATE INDEX on a collection with existing data.
    pub fn rebuild_index(&self, index_name: &str) -> Result<usize, OmniError> {
        let definition = self.definition(index_name)?;
        let lock = self.db.sql_table_lock(&definition.collection)?;
        let _guard = lock
            .lock()
            .map_err(|_| OmniError::LockPoisoned("index collection".into()))?;
        self.rebuild_index_locked(index_name, &definition.collection)
    }

    pub(crate) fn rebuild_index_locked(
        &self,
        index_name: &str,
        expected_collection: &str,
    ) -> Result<usize, OmniError> {
        let idx_def = self.definition(index_name)?;
        if !idx_def.collection.eq_ignore_ascii_case(expected_collection) {
            return Err(OmniError::IoError(format!(
                "Index '{}' not found on collection '{}'",
                index_name, expected_collection
            )));
        }
        let mut invalidate = WriteBatch::new();
        invalidate.delete(&format!("{READY_PREFIX}{}", idx_def.id))?;
        self.db.commit_batch(&invalidate)?;

        let seq = self.db.get_seq();
        let catalog = self.catalog_snapshot_at(seq)?;
        let table_row_format = self
            .db
            .find(
                &format!("\x00CATALOG\x00{}", idx_def.collection.to_lowercase()),
                seq,
            )?
            .and_then(|raw| serde_json::from_str::<crate::catalog::TableDef>(&raw).ok())
            .map(|table| table.row_format)
            .unwrap_or_default();

        let mut count = 0;
        let mut batch = WriteBatch::new();

        for (collection_prefix, collection_end) in collection_row_ranges(&idx_def.collection) {
            let is_sql_rows = collection_prefix.starts_with("\x00T\x00");
            for (key, value) in self.db.scan(&collection_prefix, &collection_end, seq)? {
                let doc = if is_sql_rows {
                    let row = crate::plan_exec::PlanExecutor::deserialize_row_checked(
                        &value,
                        table_row_format,
                    )
                    .map_err(|error| {
                        OmniError::IoError(format!("invalid row during index rebuild: {error}"))
                    })?;
                    serde_json::to_value(&row).map_err(|error| {
                        OmniError::IoError(format!("invalid row during index rebuild: {error}"))
                    })?
                } else {
                    serde_json::from_str::<serde_json::Value>(&value).map_err(|error| {
                        OmniError::IoError(format!("invalid row during index rebuild: {error}"))
                    })?
                };
                let pk = key.strip_prefix(&collection_prefix).unwrap_or(&key);
                self.index_document_with_catalog(
                    &catalog,
                    &idx_def.collection,
                    pk,
                    &doc,
                    &mut batch,
                )?;
                count += 1;

                if count % 1000 == 0 && !batch.is_empty() {
                    self.db.commit_batch(&batch)?;
                    batch = WriteBatch::new();
                }
            }
        }

        if !batch.buffered_writes.is_empty() {
            self.db.commit_batch(&batch)?;
        }
        let mut ready = WriteBatch::new();
        ready.set(&format!("{READY_PREFIX}{}", idx_def.id), "ready".into())?;
        self.db.commit_batch(&ready)?;

        Ok(count)
    }

    /// Returns all index definitions for a collection.
    pub fn list_indexes(&self, collection: &str) -> Result<Vec<IndexDefinition>, OmniError> {
        let catalog = self.catalog_snapshot()?;
        Ok(catalog
            .indexes_for_collection(collection)
            .into_iter()
            .cloned()
            .collect())
    }
}
