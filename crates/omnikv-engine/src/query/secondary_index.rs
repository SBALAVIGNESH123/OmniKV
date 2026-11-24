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
const V0_NEG_ZERO_HEX: &str = "7fffffffffffffff";
const V0_POS_ZERO_HEX: &str = "8000000000000000";

const READY_PREFIX: &str = "\x00IDX_READY\x00";
const RETIRED_PREFIX: &str = "\x00IDX_RETIRED\x00";
const NEXT_ID_KEY: &str = "\x00IDX_NEXT_ID\x00";
const DDL_LOCK_NAME: &str = "\x00IDX_CATALOG\x00ddl";

/// Legacy integer index encoding version (8-byte signed `i64` big-endian).
/// Persisted definitions written before `encoding_version` existed deserialize as `0`.
pub const LEGACY_INDEX_ENCODING_VERSION: u8 = 0;

/// Current integer index encoding version (16-byte signed `i128` big-endian,
/// covering the full SQL `INTEGER` domain).
pub const CURRENT_INDEX_ENCODING_VERSION: u8 = 1;

/// Unique identifier for an index.
pub type IndexId = u32;

/// Defines the type of values stored in an index.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum IndexFieldType {
    String,
    Integer,
    Float,
}

/// Row namespace an index is bound to.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum IndexNamespace {
    /// Legacy index definitions persisted before explicit namespace binding.
    #[default]
    Both,
    /// SQL table rows stored under `\x00T\x00<table_name>\x00`.
    Sql,
    /// Document collection rows stored under `<collection>:`.
    Document,
}

impl IndexNamespace {
    pub(crate) fn matches(self, target: Self) -> bool {
        self == Self::Both || target == Self::Both || self == target
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DropIndexReceiptState {
    Dropping,
    Done,
}

/// Strictly validates a schema-migration `DropIndex` receipt.
/// Accepts only `dropping:<id[,id...]>` or `done:<id[,id...]>`, requiring a
/// non-empty comma-separated list where every token parses cleanly as `IndexId`.
pub(crate) fn parse_drop_index_receipt(
    receipt: &str,
) -> Result<(DropIndexReceiptState, Vec<IndexId>), OmniError> {
    let (state, ids_csv) = if let Some(rest) = receipt.strip_prefix("done:") {
        (DropIndexReceiptState::Done, rest)
    } else if let Some(rest) = receipt.strip_prefix("dropping:") {
        (DropIndexReceiptState::Dropping, rest)
    } else {
        return Err(OmniError::IoError(format!(
            "invalid drop receipt '{receipt}': expected 'dropping:<ids>' or 'done:<ids>'"
        )));
    };
    if ids_csv.is_empty() {
        return Err(OmniError::IoError(format!(
            "invalid drop receipt '{receipt}': missing index IDs"
        )));
    }
    let mut ids = Vec::new();
    for token in ids_csv.split(',') {
        if token.is_empty() || token != token.trim() {
            return Err(OmniError::IoError(format!(
                "invalid drop receipt '{receipt}': empty or padded index ID token"
            )));
        }
        let id = token.parse::<IndexId>().map_err(|e| {
            OmniError::IoError(format!(
                "invalid drop receipt '{receipt}': invalid index ID '{token}': {e}"
            ))
        })?;
        ids.push(id);
    }
    Ok((state, ids))
}

/// Resolves an unbound legacy `IndexNamespace::Both` definition to a single
/// owning namespace when only one domain exists for `collection`. Returns
/// `Ok(None)` when ownership is ambiguous (both SQL and document domains exist,
/// or neither exists yet) so callers can require explicit binding and rebuild.
fn resolve_legacy_namespace_at(
    db: &OmniKV,
    collection: &str,
    seq: u64,
) -> Result<Option<IndexNamespace>, OmniError> {
    let has_sql_table = db
        .find(&crate::catalog::Catalog::catalog_key(collection), seq)?
        .map(|raw| {
            serde_json::from_str::<crate::catalog::TableDef>(&raw).map_err(|error| {
                OmniError::IoError(format!("invalid SQL table definition: {error}"))
            })
        })
        .transpose()?
        .is_some_and(|table| table.name == collection);
    let has_sql_rows = !db
        .scan_prefix_limit(&format!("\x00T\x00{collection}\x00"), seq, 1)?
        .is_empty();
    let has_sql = has_sql_table || has_sql_rows;
    let has_doc = !db
        .scan_prefix_limit(&format!("{collection}:"), seq, 1)?
        .is_empty();
    match (has_sql, has_doc) {
        (true, false) => Ok(Some(IndexNamespace::Sql)),
        (false, true) => Ok(Some(IndexNamespace::Document)),
        _ => Ok(None),
    }
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
    /// Key encoding format version (`0` = legacy `i64`, `1` = full SQL `i128`).
    #[serde(default)]
    pub encoding_version: u8,
    /// Row namespace this index scans and maintains (`Sql`, `Document`, or legacy `Both`).
    #[serde(default)]
    pub namespace: IndexNamespace,
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
    /// Disambiguated ownership for legacy `IndexNamespace::Both` definitions at
    /// this catalog snapshot (`None` means ownership is ambiguous).
    resolved_legacy_namespaces: HashMap<IndexId, Option<IndexNamespace>>,
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
            resolved_legacy_namespaces: HashMap::new(),
        }
    }

    /// The production path fails closed on unreadable or malformed metadata.
    pub fn load_checked_at(db: &Arc<OmniKV>, seq: u64) -> Result<Self, OmniError> {
        let mut catalog = Self::empty();
        let mut names = HashSet::new();
        for (key, value) in db.scan_prefix(CATALOG_PREFIX, seq)? {
            let def: IndexDefinition = serde_json::from_str(&value)
                .map_err(|error| OmniError::IoError(format!("invalid index catalog: {error}")))?;
            if def.fields.is_empty()
                || key != format!("{CATALOG_PREFIX}{}", def.id)
                || !names.insert(def.name.to_ascii_lowercase())
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
            if def.namespace == IndexNamespace::Both {
                let resolved = resolve_legacy_namespace_at(db, &def.collection, seq)?;
                catalog.resolved_legacy_namespaces.insert(def.id, resolved);
            }
            catalog.register_in_memory(def);
        }
        for (key, _) in db.scan_prefix(RETIRED_PREFIX, seq)? {
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
    ///
    /// Matches collection names exactly so differently-cased document
    /// collections (e.g., `docs` vs `DOCS`) remain isolated; SQL callers
    /// resolve the canonical `TableDef.name` first.
    pub fn indexes_for_collection(&self, collection: &str) -> Vec<&IndexDefinition> {
        self.indexes
            .values()
            .filter(|idx| idx.collection == collection)
            .collect()
    }

    pub(crate) fn effective_namespace(&self, def: &IndexDefinition) -> Option<IndexNamespace> {
        match def.namespace {
            IndexNamespace::Sql => Some(IndexNamespace::Sql),
            IndexNamespace::Document => Some(IndexNamespace::Document),
            IndexNamespace::Both => self
                .resolved_legacy_namespaces
                .get(&def.id)
                .copied()
                .flatten(),
        }
    }

    pub(crate) fn indexes_for_namespace(
        &self,
        collection: &str,
        namespace: IndexNamespace,
    ) -> Vec<&IndexDefinition> {
        self.indexes
            .values()
            .filter(|idx| {
                idx.collection == collection
                    && match namespace {
                        IndexNamespace::Both => true,
                        target => self.effective_namespace(idx) == Some(target),
                    }
            })
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

    /// Verifies that the planned index ID is still ready at this catalog
    /// snapshot and still refers to the planned index name and SQL table.
    pub fn is_ready_for_plan(&self, index_id: IndexId, index_name: &str, table_name: &str) -> bool {
        self.is_ready(index_id)
            && self.get(index_id).is_some_and(|def| {
                self.effective_namespace(def) == Some(IndexNamespace::Sql)
                    && def.name.eq_ignore_ascii_case(index_name)
                    && def.collection.eq_ignore_ascii_case(table_name)
            })
    }

    pub(crate) fn catalog_scan_range() -> (&'static str, &'static str) {
        (CATALOG_PREFIX, "\x00IDX_CATALOG\x01")
    }

    pub(crate) fn apply_overlay(&mut self, overlay: &HashMap<String, Option<String>>) {
        // Two-pass application so `CATALOG_PREFIX` replacements are always
        // applied before `READY_PREFIX` updates regardless of HashMap iteration order.
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
                    self.resolved_legacy_namespaces.remove(&id);
                }
                if let Some(raw) = value
                    && let Ok(def) = serde_json::from_str::<IndexDefinition>(raw)
                {
                    self.register_in_memory(def);
                }
            }
        }
        for (key, value) in overlay {
            if let Some(id_str) = key.strip_prefix(READY_PREFIX)
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

pub(crate) fn try_encode_index_value(
    value: &serde_json::Value,
    field_type: &IndexFieldType,
) -> Option<Vec<u8>> {
    try_encode_index_value_for_version(value, field_type, CURRENT_INDEX_ENCODING_VERSION)
}

pub(crate) fn try_encode_index_value_for_version(
    value: &serde_json::Value,
    field_type: &IndexFieldType,
    encoding_version: u8,
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
            if encoding_version >= CURRENT_INDEX_ENCODING_VERSION {
                let n = value
                    .as_i64()
                    .map(i128::from)
                    .or_else(|| value.as_u64().map(i128::from))
                    .or_else(|| value.as_str()?.parse::<i128>().ok())?;
                let flipped = (n ^ i128::MIN) as u128; // flip sign bit
                Some(flipped.to_be_bytes().to_vec())
            } else {
                let n = value
                    .as_i64()
                    .or_else(|| value.as_str()?.parse::<i64>().ok())?;
                let flipped = (n ^ i64::MIN) as u64; // flip sign bit
                Some(flipped.to_be_bytes().to_vec())
            }
        }
        IndexFieldType::Float => {
            // IEEE 754 float encoding with sign handling for sort order.
            // Pre-rebuild v0 indexes preserved IEEE-754 signed zero (-0.0 vs
            // +0.0); keep v0 encoding until rebuild publishes v1 so lookup,
            // deletion, and replacement match existing v0 entries cleanly.
            let f = value
                .as_f64()
                .or_else(|| value.as_str()?.parse::<f64>().ok())?;
            if !f.is_finite() {
                return None;
            }
            let f = if encoding_version >= CURRENT_INDEX_ENCODING_VERSION && f == 0.0 {
                0.0
            } else {
                f
            };
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

fn index_entries_for_namespace(
    catalog: &IndexCatalog,
    collection: &str,
    namespace: IndexNamespace,
    primary_key: &str,
    document: &serde_json::Value,
) -> Result<Vec<(IndexDefinition, String)>, OmniError> {
    if namespace != IndexNamespace::Both {
        for def in catalog.indexes_for_collection(collection) {
            if def.namespace == IndexNamespace::Both
                && catalog.effective_namespace(def) != Some(namespace)
            {
                return Err(OmniError::IoError(format!(
                    "legacy index '{}' on collection '{}' has unbound namespace 'Both' and cannot be shared across SQL and document writes; call bind_index_namespace('{}', IndexNamespace::Sql | IndexNamespace::Document) to bind and rebuild it",
                    def.name, collection, def.name
                )));
            }
        }
    }
    let mut entries = Vec::new();
    'indexes: for definition in catalog.indexes_for_namespace(collection, namespace) {
        let mut values = Vec::new();
        for (field, field_type) in &definition.fields {
            let Some(value) = document_field(document, field).filter(|value| !value.is_null())
            else {
                continue 'indexes;
            };
            let encoded =
                try_encode_index_value_for_version(value, field_type, definition.encoding_version)
                    .ok_or_else(|| {
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
pub(crate) fn sql_index_bound_for_version(
    value: &crate::sql::SqlValue,
    field_type: &IndexFieldType,
    encoding_version: u8,
) -> Option<serde_json::Value> {
    if matches!(
        value,
        crate::sql::SqlValue::Null | crate::sql::SqlValue::Placeholder(_)
    ) {
        return None;
    }
    let text = value.as_string();
    let result = serde_json::Value::String(text);
    try_encode_index_value_for_version(&result, field_type, encoding_version)?;
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

fn validate_sql_index_fields(
    table: &crate::catalog::TableDef,
    fields: &[(String, IndexFieldType)],
) -> Result<(), OmniError> {
    for (field, field_type) in fields {
        let column = table
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(field))
            .ok_or_else(|| OmniError::IoError(format!("unknown indexed column '{field}'")))?;
        let compatible = match field_type {
            IndexFieldType::Integer => column.col_type == crate::catalog::ColumnType::Integer,
            IndexFieldType::Float => column.col_type == crate::catalog::ColumnType::Float,
            IndexFieldType::String => !column.col_type.is_numeric(),
        };
        if !compatible {
            return Err(OmniError::IoError(format!(
                "index field type does not match SQL column '{field}'"
            )));
        }
    }
    Ok(())
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

/// Half-open `[prefix, successor)` row ranges for both document collection
/// keys (`<collection>:`) and SQL table keys (`\x00T\x00<collection>\x00`).
///
/// Callers must treat the second element as an EXCLUSIVE upper bound (or scan
/// with `OmniKV::scan_prefix` / `OmniKV::scan_exclusive`).
pub(crate) fn collection_row_ranges(collection: &str) -> [(String, String); 2] {
    let doc_prefix = format!("{collection}:");
    let doc_end = format!("{collection};");
    let sql_prefix = format!("\x00T\x00{collection}\x00");
    let sql_end = format!("\x00T\x00{collection}\x01");
    [(doc_prefix, doc_end), (sql_prefix, sql_end)]
}

pub(crate) fn collection_row_ranges_for_namespace(
    collection: &str,
    namespace: IndexNamespace,
) -> Vec<(String, String)> {
    match namespace {
        IndexNamespace::Document => vec![(format!("{collection}:"), format!("{collection};"))],
        IndexNamespace::Sql => vec![(
            format!("\x00T\x00{collection}\x00"),
            format!("\x00T\x00{collection}\x01"),
        )],
        IndexNamespace::Both => collection_row_ranges(collection).into_iter().collect(),
    }
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

    /// Resolves `collection` to its canonical SQL `TableDef.name` and
    /// `IndexNamespace::Sql` when a SQL table exists for it (preserving
    /// case-insensitive SQL semantics), or returns `(collection, Document)`
    /// verbatim for document collections so differently cased prefixes stay
    /// distinct. Fails closed on corrupt SQL table metadata.
    pub(crate) fn resolve_collection_and_namespace_at(
        &self,
        collection: &str,
        seq: u64,
    ) -> Result<(String, IndexNamespace), OmniError> {
        let table = self
            .db
            .find(&crate::catalog::Catalog::catalog_key(collection), seq)?
            .map(|value| {
                serde_json::from_str::<crate::catalog::TableDef>(&value).map_err(|error| {
                    OmniError::IoError(format!("invalid SQL table definition: {error}"))
                })
            })
            .transpose()?;
        Ok(table.map_or_else(
            || (collection.to_string(), IndexNamespace::Document),
            |t| (t.name, IndexNamespace::Sql),
        ))
    }

    /// Resolves `collection` to its canonical SQL `TableDef.name` when a SQL
    /// table exists for it (preserving case-insensitive SQL semantics), or
    /// returns `collection` verbatim for document collections so differently
    /// cased prefixes (e.g., `docs` vs `DOCS`) stay distinct.
    pub(crate) fn resolve_collection_name_at(
        &self,
        collection: &str,
        seq: u64,
    ) -> Result<String, OmniError> {
        Ok(self.resolve_collection_and_namespace_at(collection, seq)?.0)
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
    /// Infers `IndexNamespace::Sql` when a SQL table exists for `collection`,
    /// otherwise `IndexNamespace::Document`. Use [`Self::create_document_index`]
    /// or [`Self::create_sql_index`] to bind a namespace explicitly when a SQL
    /// table and a document collection share the same name.
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

    /// Explicitly creates an index bound to `IndexNamespace::Document`, even
    /// when a SQL table with the same name exists. Skips SQL table resolution
    /// and SQL column validation.
    pub fn create_document_index(
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
        self.create_index_locked_with_options(
            name,
            collection,
            fields,
            unique,
            None,
            Some(IndexNamespace::Document),
        )
    }

    /// Explicitly creates an index bound to `IndexNamespace::Sql`, requiring a
    /// matching SQL table and compatible column types.
    pub fn create_sql_index(
        &self,
        name: &str,
        table_name: &str,
        fields: Vec<(String, IndexFieldType)>,
        unique: bool,
    ) -> Result<IndexId, OmniError> {
        let lock = self.db.sql_table_lock(table_name)?;
        let _guard = lock
            .lock()
            .map_err(|_| OmniError::LockPoisoned("index collection".into()))?;
        self.create_index_locked_with_options(
            name,
            table_name,
            fields,
            unique,
            None,
            Some(IndexNamespace::Sql),
        )
    }

    /// Binds a legacy `IndexNamespace::Both` (or existing) index explicitly to
    /// `IndexNamespace::Sql` or `IndexNamespace::Document` and rebuilds it from
    /// that single namespace only.
    pub fn bind_index_namespace(
        &self,
        name: &str,
        namespace: IndexNamespace,
    ) -> Result<usize, OmniError> {
        if namespace == IndexNamespace::Both {
            return Err(OmniError::IoError(
                "cannot bind index to IndexNamespace::Both; choose IndexNamespace::Sql or IndexNamespace::Document"
                    .into(),
            ));
        }
        let initial = self.definition(name)?;
        let lock = self.db.sql_table_lock(&initial.collection)?;
        let _guard = lock
            .lock()
            .map_err(|_| OmniError::LockPoisoned("index collection".into()))?;
        let target_collection = {
            let ddl_lock = self.db.sql_table_lock(DDL_LOCK_NAME)?;
            let _ddl_guard = ddl_lock
                .lock()
                .map_err(|_| OmniError::LockPoisoned("index DDL".into()))?;
            let seq = self.db.get_seq();
            let catalog = IndexCatalog::load_checked_at(&self.db, seq)?;
            let mut def = catalog
                .get_by_name(name)
                .cloned()
                .ok_or_else(|| OmniError::IoError(format!("Index '{}' not found", name)))?;
            if def.collection != initial.collection {
                return Err(OmniError::IoError(format!(
                    "Index '{}' collection changed concurrently; retry bind_index_namespace",
                    name
                )));
            }
            if namespace == IndexNamespace::Sql {
                let table = self
                    .db
                    .find(&crate::catalog::Catalog::catalog_key(&def.collection), seq)?
                    .map(|value| {
                        serde_json::from_str::<crate::catalog::TableDef>(&value).map_err(|error| {
                            OmniError::IoError(format!("invalid SQL table definition: {error}"))
                        })
                    })
                    .transpose()?
                    .ok_or_else(|| {
                        OmniError::IoError(format!(
                            "cannot bind index '{}' to SQL namespace: SQL table '{}' not found",
                            name, def.collection
                        ))
                    })?;
                validate_sql_index_fields(&table, &def.fields)?;
                def.collection = table.name;
            }
            def.namespace = namespace;
            def.encoding_version = CURRENT_INDEX_ENCODING_VERSION;
            let catalog_key = format!("{CATALOG_PREFIX}{}", def.id);
            let ready_key = format!("{READY_PREFIX}{}", def.id);
            // Invalidate readiness and clear ALL stale pre-binding entries
            // BEFORE publishing the new single-namespace definition so a crash
            // mid-binding can never leave cross-namespace entries under the new
            // namespace (which would cause phantom UNIQUE violations or stale lookups).
            let mut invalidate = WriteBatch::new();
            invalidate.delete(&ready_key)?;
            self.db.commit_batch(&invalidate)?;

            let index_prefix = build_index_scan_prefix(def.id);
            loop {
                let clear_seq = self.db.get_seq();
                let stale =
                    self.db
                        .scan_prefix_limit(&index_prefix, clear_seq, WriteBatch::MAX_OPS / 2)?;
                if stale.is_empty() {
                    break;
                }
                let mut clear_batch = WriteBatch::new();
                for (stale_key, _) in stale {
                    clear_batch.delete(&stale_key)?;
                }
                self.db.commit_batch(&clear_batch)?;
            }

            let catalog_value =
                serde_json::to_string(&def).map_err(|e| OmniError::IoError(e.to_string()))?;
            let mut batch = WriteBatch::new();
            batch.set(&catalog_key, catalog_value)?;
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
            def.collection
        };
        self.rebuild_index_locked(name, &target_collection)
    }

    pub(crate) fn create_index_locked(
        &self,
        name: &str,
        collection: &str,
        fields: Vec<(String, IndexFieldType)>,
        unique: bool,
    ) -> Result<IndexId, OmniError> {
        self.create_index_locked_with_receipt(name, collection, fields, unique, None)
    }

    pub(crate) fn create_index_locked_with_receipt(
        &self,
        name: &str,
        collection: &str,
        fields: Vec<(String, IndexFieldType)>,
        unique: bool,
        building_receipt_key: Option<&str>,
    ) -> Result<IndexId, OmniError> {
        self.create_index_locked_with_options(
            name,
            collection,
            fields,
            unique,
            building_receipt_key,
            None,
        )
    }

    fn create_index_locked_with_options(
        &self,
        name: &str,
        collection: &str,
        fields: Vec<(String, IndexFieldType)>,
        unique: bool,
        building_receipt_key: Option<&str>,
        explicit_namespace: Option<IndexNamespace>,
    ) -> Result<IndexId, OmniError> {
        if explicit_namespace == Some(IndexNamespace::Both) {
            return Err(OmniError::IoError(
                "cannot create a new index with IndexNamespace::Both".into(),
            ));
        }
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
        let (collection, namespace) = match explicit_namespace {
            Some(IndexNamespace::Document) => (collection.to_string(), IndexNamespace::Document),
            Some(IndexNamespace::Sql) => {
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
                    .transpose()?
                    .ok_or_else(|| {
                        OmniError::IoError(format!("SQL table '{collection}' not found"))
                    })?;
                validate_sql_index_fields(&table, &fields)?;
                (table.name, IndexNamespace::Sql)
            }
            None => {
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
                if let Some(table) = table {
                    validate_sql_index_fields(&table, &fields)?;
                    (table.name, IndexNamespace::Sql)
                } else {
                    (collection.to_string(), IndexNamespace::Document)
                }
            }
            Some(IndexNamespace::Both) => unreachable!("rejected above"),
        };
        let collection = collection.as_str();
        self.cleanup_retired_indexes_for_collection(collection, namespace)?;
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
            encoding_version: CURRENT_INDEX_ENCODING_VERSION,
            namespace,
        };

        let catalog_key = format!("{}{}", CATALOG_PREFIX, id);
        let catalog_value =
            serde_json::to_string(&def).map_err(|e| OmniError::IoError(e.to_string()))?;

        let mut batch = WriteBatch::new();
        batch.set(&catalog_key, catalog_value)?;
        batch.set(NEXT_ID_KEY, next_id.to_string())?;
        if let Some(receipt_key) = building_receipt_key {
            batch.set(receipt_key, format!("building:{id}"))?;
        }
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
        let target_collection = self.resolve_drop_target_at(name, self.db.get_seq())?;
        let lock = self.db.sql_table_lock(&target_collection)?;
        let _guard = lock
            .lock()
            .map_err(|_| OmniError::LockPoisoned("index collection".into()))?;
        self.drop_index_locked_inner(name, &target_collection, true, None)
    }

    /// Resumable `DROP INDEX` for schema migrations: tracks `dropping:<ids>`
    /// atomically alongside retirement and `done:<ids>` once all entries are
    /// purged, while still failing a fresh drop of a non-existent index or any
    /// malformed receipt.
    pub(crate) fn drop_index_with_receipt(
        &self,
        name: &str,
        receipt_key: &str,
    ) -> Result<(), OmniError> {
        let seq = self.db.get_seq();
        if let Some(receipt) = self.db.find(receipt_key, seq)? {
            let (state, ids) = parse_drop_index_receipt(&receipt)?;
            match state {
                DropIndexReceiptState::Done => return Ok(()),
                DropIndexReceiptState::Dropping => {
                    for &id in &ids {
                        self.purge_retired_index_if_present(id)?;
                        if let Some(def) =
                            IndexCatalog::load_checked_at(&self.db, self.db.get_seq())?
                                .get(id)
                                .cloned()
                        {
                            let lock = self.db.sql_table_lock(&def.collection)?;
                            let _guard = lock
                                .lock()
                                .map_err(|_| OmniError::LockPoisoned("index collection".into()))?;
                            self.drop_index_locked_inner(&def.name, &def.collection, false, None)?;
                        }
                    }
                    let ids_csv = ids
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(",");
                    let mut done = WriteBatch::new();
                    done.set(receipt_key, format!("done:{ids_csv}"))?;
                    self.db.commit_batch(&done)?;
                    return Ok(());
                }
            }
        }

        let target_collection = self.resolve_drop_target_at(name, seq)?;
        let lock = self.db.sql_table_lock(&target_collection)?;
        let _guard = lock
            .lock()
            .map_err(|_| OmniError::LockPoisoned("index collection".into()))?;
        self.drop_index_locked_inner(name, &target_collection, true, Some(receipt_key))
    }

    fn resolve_drop_target_at(&self, name: &str, seq: u64) -> Result<String, OmniError> {
        let catalog = IndexCatalog::load_checked_at(&self.db, seq)?;
        let active = catalog.get_by_name(name).cloned();
        let retired = self.find_retired_indexes_at(name, None, seq)?;
        let mut collections: Vec<String> = Vec::new();
        for def in active.iter().chain(retired.iter()) {
            if !collections.iter().any(|c| c == &def.collection) {
                collections.push(def.collection.clone());
            }
        }
        match collections.as_slice() {
            [] => Err(OmniError::IoError(format!("Index '{}' not found", name))),
            [single] => Ok(single.clone()),
            _ => Err(OmniError::IoError(format!(
                "Index '{}' is ambiguous across active and retired definitions; specify an explicit cleanup target",
                name
            ))),
        }
    }

    /// Cleans up a specific retired index by its retired ID.
    pub fn purge_retired_index(&self, id: IndexId) -> Result<(), OmniError> {
        let retired_key = format!("{RETIRED_PREFIX}{id}");
        let raw = self
            .db
            .find(&retired_key, self.db.get_seq())?
            .ok_or_else(|| OmniError::IoError(format!("Retired index {id} not found")))?;
        let def: IndexDefinition =
            serde_json::from_str(&raw).map_err(|e| OmniError::IoError(e.to_string()))?;
        let lock = self.db.sql_table_lock(&def.collection)?;
        let _guard = lock
            .lock()
            .map_err(|_| OmniError::LockPoisoned("index collection".into()))?;
        let ddl_lock = self.db.sql_table_lock(DDL_LOCK_NAME)?;
        let _ddl_guard = ddl_lock
            .lock()
            .map_err(|_| OmniError::LockPoisoned("index DDL".into()))?;
        self.purge_retired_index_entries(id)
    }

    fn purge_retired_index_if_present(&self, id: IndexId) -> Result<(), OmniError> {
        let retired_key = format!("{RETIRED_PREFIX}{id}");
        let Some(raw) = self.db.find(&retired_key, self.db.get_seq())? else {
            return Ok(());
        };
        let def: IndexDefinition =
            serde_json::from_str(&raw).map_err(|e| OmniError::IoError(e.to_string()))?;
        let lock = self.db.sql_table_lock(&def.collection)?;
        let _guard = lock
            .lock()
            .map_err(|_| OmniError::LockPoisoned("index collection".into()))?;
        let ddl_lock = self.db.sql_table_lock(DDL_LOCK_NAME)?;
        let _ddl_guard = ddl_lock
            .lock()
            .map_err(|_| OmniError::LockPoisoned("index DDL".into()))?;
        if self.db.find(&retired_key, self.db.get_seq())?.is_none() {
            return Ok(());
        }
        self.purge_retired_index_entries(id)
    }

    pub(crate) fn drop_index_locked(
        &self,
        name: &str,
        expected_collection: &str,
    ) -> Result<(), OmniError> {
        self.drop_index_locked_inner(name, expected_collection, false, None)
    }

    fn drop_index_locked_inner(
        &self,
        name: &str,
        expected_collection: &str,
        revalidate_unambiguous: bool,
        receipt_key: Option<&str>,
    ) -> Result<(), OmniError> {
        let ddl_lock = self.db.sql_table_lock(DDL_LOCK_NAME)?;
        let _ddl_guard = ddl_lock
            .lock()
            .map_err(|_| OmniError::LockPoisoned("index DDL".into()))?;
        let seq = self.db.get_seq();
        let catalog = IndexCatalog::load_checked_at(&self.db, seq)?;
        let resolved_collection = self.resolve_collection_name_at(expected_collection, seq)?;
        // Prioritize matching an active index on `expected_collection` or its
        // canonical SQL collection BEFORE falling back to retired-only records,
        // so an unrelated retired index on a case-differing document collection
        // can never shadow cleanup of a failed active SQL index.
        let target_collection = if revalidate_unambiguous
            || catalog
                .get_by_name(name)
                .is_some_and(|d| d.collection == expected_collection)
        {
            expected_collection.to_string()
        } else if catalog
            .get_by_name(name)
            .is_some_and(|d| d.collection == resolved_collection)
        {
            resolved_collection
        } else if !self
            .find_retired_indexes_at(name, Some(expected_collection), seq)?
            .is_empty()
        {
            expected_collection.to_string()
        } else {
            resolved_collection
        };
        if revalidate_unambiguous {
            let current_target = self.resolve_drop_target_at(name, seq)?;
            if current_target != target_collection {
                return Err(OmniError::IoError(format!(
                    "Index '{}' target collection changed concurrently; retry drop_index",
                    name
                )));
            }
        }
        let mut ids_to_purge: Vec<IndexId> = self
            .find_retired_indexes_at(name, Some(&target_collection), seq)?
            .into_iter()
            .map(|d| d.id)
            .collect();

        if let Some(def) = catalog
            .get_by_name(name)
            .filter(|d| d.collection == target_collection)
        {
            let id = def.id;
            if !ids_to_purge.contains(&id) {
                ids_to_purge.push(id);
            }
            let catalog_key = format!("{CATALOG_PREFIX}{id}");
            let ready_key = format!("{READY_PREFIX}{id}");
            let retired_key = format!("{RETIRED_PREFIX}{id}");
            let retired_value =
                serde_json::to_string(def).map_err(|e| OmniError::IoError(e.to_string()))?;
            let mut meta_batch = WriteBatch::new();
            meta_batch.delete(&catalog_key)?;
            meta_batch.delete(&ready_key)?;
            meta_batch.set(&retired_key, retired_value)?;
            if let Some(receipt_key) = receipt_key {
                let ids_csv = ids_to_purge
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(",");
                meta_batch.set(receipt_key, format!("dropping:{ids_csv}"))?;
            }
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
        } else if ids_to_purge.is_empty() {
            return Err(OmniError::IoError(format!("Index '{}' not found", name)));
        } else if let Some(receipt_key) = receipt_key {
            let ids_csv = ids_to_purge
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",");
            let mut dropping = WriteBatch::new();
            dropping.set(receipt_key, format!("dropping:{ids_csv}"))?;
            self.db.commit_batch(&dropping)?;
        }

        let ids_csv = ids_to_purge
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",");
        for id in ids_to_purge {
            self.purge_retired_index_entries(id)?;
        }
        if let Some(receipt_key) = receipt_key {
            let mut done = WriteBatch::new();
            done.set(receipt_key, format!("done:{ids_csv}"))?;
            self.db.commit_batch(&done)?;
        }
        Ok(())
    }

    fn find_retired_indexes_at(
        &self,
        name: &str,
        expected_collection: Option<&str>,
        seq: u64,
    ) -> Result<Vec<IndexDefinition>, OmniError> {
        let mut matches = Vec::new();
        for (_, value) in self.db.scan_prefix(RETIRED_PREFIX, seq)? {
            let Ok(def) = serde_json::from_str::<IndexDefinition>(&value) else {
                continue;
            };
            if def.name.eq_ignore_ascii_case(name)
                && expected_collection.is_none_or(|expected| def.collection == expected)
            {
                matches.push(def);
            }
        }
        Ok(matches)
    }

    fn cleanup_retired_indexes_for_collection(
        &self,
        collection: &str,
        namespace: IndexNamespace,
    ) -> Result<(), OmniError> {
        let seq = self.db.get_seq();
        for (_, value) in self.db.scan_prefix(RETIRED_PREFIX, seq)? {
            if let Ok(def) = serde_json::from_str::<IndexDefinition>(&value)
                && def.collection == collection
                && def.namespace.matches(namespace)
            {
                self.purge_retired_index_entries(def.id)?;
            }
        }
        Ok(())
    }

    fn purge_retired_index_entries(&self, id: IndexId) -> Result<(), OmniError> {
        const DELETE_BATCH_LIMIT: usize = WriteBatch::MAX_OPS / 2;
        let prefix = build_index_scan_prefix(id);
        let retired_key = format!("{RETIRED_PREFIX}{id}");
        loop {
            let seq = self.db.get_seq();
            let entries =
                self.db
                    .scan_prefix_limit(&prefix, seq, DELETE_BATCH_LIMIT.saturating_add(1))?;
            if entries.is_empty() {
                let mut clear_batch = WriteBatch::new();
                clear_batch.delete(&retired_key)?;
                self.db.commit_batch(&clear_batch)?;
                return Ok(());
            }
            let has_more = entries.len() > DELETE_BATCH_LIMIT;
            let mut batch = WriteBatch::new();
            for (key, _) in entries.into_iter().take(DELETE_BATCH_LIMIT) {
                batch.delete(&key)?;
            }
            if !has_more {
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
        let canonical = self.resolve_collection_name_at(collection, seq)?;
        let catalog = IndexCatalog::load_checked_at(&self.db, seq)?;
        for def in catalog.indexes_for_collection(&canonical) {
            if def.namespace == IndexNamespace::Both
                && catalog.effective_namespace(def) != Some(IndexNamespace::Sql)
            {
                return Err(OmniError::IoError(format!(
                    "cannot drop SQL table '{}' while legacy index '{}' has ambiguous namespace 'Both'; call bind_index_namespace('{}', IndexNamespace::Sql | IndexNamespace::Document) or drop_index('{}') first",
                    canonical, def.name, def.name, def.name
                )));
            }
        }
        let mut entry_keys = Vec::new();
        let mut meta_keys = Vec::new();
        for def in catalog.indexes_for_namespace(&canonical, IndexNamespace::Sql) {
            meta_keys.push(format!("{}{}", CATALOG_PREFIX, def.id));
            meta_keys.push(format!("{READY_PREFIX}{}", def.id));
            let prefix = build_index_scan_prefix(def.id);
            for (key, _) in self.db.scan_prefix(&prefix, seq)? {
                entry_keys.push(key);
            }
        }
        for (ret_key, value) in self.db.scan_prefix(RETIRED_PREFIX, seq)? {
            if let Ok(def) = serde_json::from_str::<IndexDefinition>(&value)
                && def.collection == canonical
                && def.namespace.matches(IndexNamespace::Sql)
            {
                meta_keys.push(ret_key);
                let prefix = build_index_scan_prefix(def.id);
                for (key, _) in self.db.scan_prefix(&prefix, seq)? {
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
        let canonical = self.resolve_collection_name_at(collection, seq)?;
        let catalog = IndexCatalog::load_checked_at(&self.db, seq)?;
        for def in catalog.indexes_for_collection(&canonical) {
            if def.namespace == IndexNamespace::Both
                && catalog.effective_namespace(def) != Some(IndexNamespace::Sql)
            {
                return Err(OmniError::IoError(format!(
                    "cannot drop SQL table '{}' while legacy index '{}' has ambiguous namespace 'Both'; call bind_index_namespace('{}', IndexNamespace::Sql | IndexNamespace::Document) or drop_index('{}') first",
                    canonical, def.name, def.name, def.name
                )));
            }
        }
        for def in catalog.indexes_for_namespace(&canonical, IndexNamespace::Sql) {
            let cat_key = format!("{}{}", CATALOG_PREFIX, def.id);
            let ready_key = format!("{READY_PREFIX}{}", def.id);
            let prefix = build_index_scan_prefix(def.id);
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
            for (key, _) in self.db.scan_prefix(&prefix, seq)? {
                if !batch
                    .buffered_deletes
                    .iter()
                    .any(|deleted| deleted.as_slice() == key.as_bytes())
                {
                    batch.delete(&key)?;
                }
            }
        }
        for (ret_key, value) in self.db.scan_prefix(RETIRED_PREFIX, seq)? {
            if let Ok(def) = serde_json::from_str::<IndexDefinition>(&value)
                && def.collection == canonical
                && def.namespace.matches(IndexNamespace::Sql)
            {
                let prefix = build_index_scan_prefix(def.id);
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
                    key.as_slice() != ret_key.as_bytes() && !key.starts_with(prefix.as_bytes())
                });
                if !batch
                    .buffered_deletes
                    .iter()
                    .any(|deleted| deleted.as_slice() == ret_key.as_bytes())
                {
                    batch.delete(&ret_key)?;
                }
                for key in staged_keys {
                    if !batch
                        .buffered_deletes
                        .iter()
                        .any(|deleted| deleted.as_slice() == key.as_bytes())
                    {
                        batch.delete(&key)?;
                    }
                }
                for (key, _) in self.db.scan_prefix(&prefix, seq)? {
                    if !batch
                        .buffered_deletes
                        .iter()
                        .any(|deleted| deleted.as_slice() == key.as_bytes())
                    {
                        batch.delete(&key)?;
                    }
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

    /// Writes index entries for a document collection INSERT or UPDATE.
    ///
    /// Matches `collection` exactly against document-namespaced indexes; never
    /// falls back to SQL table name normalization.
    pub fn index_document(
        &self,
        collection: &str,
        primary_key: &str,
        document: &serde_json::Value,
        batch: &mut WriteBatch,
    ) -> Result<(), OmniError> {
        let seq = self.db.get_seq();
        let catalog = self.catalog_snapshot_at(seq)?;
        self.index_document_with_catalog_for_namespace(
            &catalog,
            collection,
            IndexNamespace::Document,
            primary_key,
            document,
            batch,
            None,
        )
    }

    pub fn index_document_with_catalog(
        &self,
        catalog: &IndexCatalog,
        collection: &str,
        primary_key: &str,
        document: &serde_json::Value,
        batch: &mut WriteBatch,
    ) -> Result<(), OmniError> {
        self.index_document_with_catalog_for_namespace(
            catalog,
            collection,
            IndexNamespace::Document,
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
        self.index_document_with_catalog_for_namespace(
            catalog,
            collection,
            IndexNamespace::Document,
            primary_key,
            document,
            batch,
            read_collector,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn index_document_with_catalog_for_namespace(
        &self,
        catalog: &IndexCatalog,
        collection: &str,
        namespace: IndexNamespace,
        primary_key: &str,
        document: &serde_json::Value,
        batch: &mut WriteBatch,
        read_collector: Option<&mut crate::volcano::ReadCollector>,
    ) -> Result<(), OmniError> {
        let entries =
            index_entries_for_namespace(catalog, collection, namespace, primary_key, document)?;
        self.write_new_index_entries(&entries, primary_key, batch, read_collector)
    }

    pub(crate) fn validate_document_indexes_with_catalog_for_namespace(
        &self,
        catalog: &IndexCatalog,
        collection: &str,
        namespace: IndexNamespace,
        primary_key: &str,
        document: &serde_json::Value,
    ) -> Result<Vec<(IndexDefinition, String)>, OmniError> {
        index_entries_for_namespace(catalog, collection, namespace, primary_key, document)
    }

    /// Replaces index entries for one document collection row. Skips unchanged
    /// values to avoid delete-after-write. Matches `collection` exactly against
    /// document-namespaced indexes without falling back to SQL tables.
    pub fn replace_document_indexes(
        &self,
        collection: &str,
        old_primary_key: &str,
        old_document: &serde_json::Value,
        new_primary_key: &str,
        new_document: &serde_json::Value,
        batch: &mut WriteBatch,
    ) -> Result<(), OmniError> {
        let seq = self.db.get_seq();
        let catalog = self.catalog_snapshot_at(seq)?;
        self.replace_document_indexes_with_catalog_for_namespace(
            &catalog,
            collection,
            IndexNamespace::Document,
            old_primary_key,
            old_document,
            new_primary_key,
            new_document,
            batch,
            None,
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
        self.replace_document_indexes_with_catalog_for_namespace(
            catalog,
            collection,
            IndexNamespace::Document,
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
        self.replace_document_indexes_with_catalog_for_namespace(
            catalog,
            collection,
            IndexNamespace::Document,
            old_primary_key,
            old_document,
            new_primary_key,
            new_document,
            batch,
            read_collector,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn replace_document_indexes_with_catalog_for_namespace(
        &self,
        catalog: &IndexCatalog,
        collection: &str,
        namespace: IndexNamespace,
        old_primary_key: &str,
        old_document: &serde_json::Value,
        new_primary_key: &str,
        new_document: &serde_json::Value,
        batch: &mut WriteBatch,
        read_collector: Option<&mut crate::volcano::ReadCollector>,
    ) -> Result<(), OmniError> {
        let old_entries = index_entries_for_namespace(
            catalog,
            collection,
            namespace,
            old_primary_key,
            old_document,
        )?;
        let new_entries = index_entries_for_namespace(
            catalog,
            collection,
            namespace,
            new_primary_key,
            new_document,
        )?;
        let new_keys: std::collections::HashSet<&str> =
            new_entries.iter().map(|(_, key)| key.as_str()).collect();

        for (_, old_key) in &old_entries {
            if !new_keys.contains(old_key.as_str()) {
                append_index_delete(batch, old_key)?;
            }
        }
        self.write_new_index_entries(&new_entries, new_primary_key, batch, read_collector)
    }

    /// Removes index entries for a document collection DELETE or pre-UPDATE
    /// cleanup. Matches `collection` exactly against document-namespaced
    /// indexes without falling back to SQL tables.
    pub fn remove_document_indexes(
        &self,
        collection: &str,
        primary_key: &str,
        old_document: &serde_json::Value,
        batch: &mut WriteBatch,
    ) -> Result<(), OmniError> {
        let seq = self.db.get_seq();
        let catalog = self.catalog_snapshot_at(seq)?;
        self.remove_document_indexes_with_catalog_for_namespace(
            &catalog,
            collection,
            IndexNamespace::Document,
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
        self.remove_document_indexes_with_catalog_for_namespace(
            catalog,
            collection,
            IndexNamespace::Document,
            primary_key,
            old_document,
            batch,
        )
    }

    pub(crate) fn remove_document_indexes_with_catalog_for_namespace(
        &self,
        catalog: &IndexCatalog,
        collection: &str,
        namespace: IndexNamespace,
        primary_key: &str,
        old_document: &serde_json::Value,
        batch: &mut WriteBatch,
    ) -> Result<(), OmniError> {
        let entries =
            index_entries_for_namespace(catalog, collection, namespace, primary_key, old_document)?;
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

    /// Computes exact half-open scan ranges covering every stored index key
    /// equivalent to `parts` (treating v0 float `-0.0` and `+0.0` as
    /// equivalent):
    /// - 0 v0 signed-zero fields: 1 exact range (`needs_v0_filter = false`)
    /// - 1 v0 signed-zero field: 2 exact ranges (`-0.0` and `+0.0`, no
    ///   unrelated suffix scanning, `needs_v0_filter = false`)
    /// - 2 or more v0 signed-zero fields: 1 O(fields) bounding range fallback
    ///   avoiding 2^k blowup (`needs_v0_filter = true`)
    fn v0_scan_ranges_for_parts(
        idx_def: &IndexDefinition,
        parts: &[&str],
    ) -> (Vec<(String, String)>, bool) {
        let scan_prefix = build_index_scan_prefix(idx_def.id);
        let is_v0 = idx_def.encoding_version < CURRENT_INDEX_ENCODING_VERSION;
        let zero_positions: Vec<usize> = parts
            .iter()
            .zip(idx_def.fields.iter())
            .enumerate()
            .filter_map(|(i, (segment, (_, field_type)))| {
                (is_v0
                    && *field_type == IndexFieldType::Float
                    && (*segment == V0_NEG_ZERO_HEX || *segment == V0_POS_ZERO_HEX))
                    .then_some(i)
            })
            .collect();

        let build_exact_range = |override_zero: Option<(usize, &str)>| -> (String, String) {
            let mut prefix = scan_prefix.clone();
            for (i, segment) in parts.iter().enumerate() {
                let seg = match override_zero {
                    Some((pos, replacement)) if pos == i => replacement,
                    _ => *segment,
                };
                prefix.push_str(seg);
                prefix.push('\x00');
            }
            let end = format!("{}\x01", &prefix[..prefix.len() - 1]);
            (prefix, end)
        };

        match zero_positions.as_slice() {
            [] => (vec![build_exact_range(None)], false),
            &[pos] => (
                vec![
                    build_exact_range(Some((pos, V0_NEG_ZERO_HEX))),
                    build_exact_range(Some((pos, V0_POS_ZERO_HEX))),
                ],
                false,
            ),
            _ => {
                let mut low = scan_prefix.clone();
                let mut high = scan_prefix;
                for (segment, (_, field_type)) in parts.iter().zip(idx_def.fields.iter()) {
                    if is_v0
                        && *field_type == IndexFieldType::Float
                        && (*segment == V0_NEG_ZERO_HEX || *segment == V0_POS_ZERO_HEX)
                    {
                        low.push_str(V0_NEG_ZERO_HEX);
                        high.push_str(V0_POS_ZERO_HEX);
                    } else {
                        low.push_str(segment);
                        high.push_str(segment);
                    }
                    low.push('\x00');
                    high.push('\x00');
                }
                let high_end = format!("{}\x01", &high[..high.len() - 1]);
                (vec![(low, high_end)], true)
            }
        }
    }

    /// Returns true if stored index `key` has encoded field segments matching
    /// `parts`, treating v0 float `-0.0` (`7fffffffffffffff`) and `+0.0`
    /// (`8000000000000000`) as equivalent in `Float` positions.
    fn v0_index_key_matches_parts(idx_def: &IndexDefinition, key: &str, parts: &[&str]) -> bool {
        let scan_prefix = build_index_scan_prefix(idx_def.id);
        let Some(rest) = key.strip_prefix(&scan_prefix) else {
            return false;
        };
        let mut segments = rest.splitn(idx_def.fields.len() + 1, '\x00');
        let is_v0 = idx_def.encoding_version < CURRENT_INDEX_ENCODING_VERSION;
        for (target, (_, field_type)) in parts.iter().zip(idx_def.fields.iter()) {
            let Some(stored) = segments.next() else {
                return false;
            };
            if is_v0
                && *field_type == IndexFieldType::Float
                && (*target == V0_NEG_ZERO_HEX || *target == V0_POS_ZERO_HEX)
            {
                if stored != V0_NEG_ZERO_HEX && stored != V0_POS_ZERO_HEX {
                    return false;
                }
            } else if stored != *target {
                return false;
            }
        }
        // Must have a primary-key tail after the final field delimiter.
        segments.next().is_some()
    }

    /// Canonicalizes a unique-index value prefix for preflight duplicate sets so
    /// legacy v0 float `-0.0` (`7fffffffffffffff`) and `+0.0` (`8000000000000000`)
    /// collide identically to `check_unique`.
    pub(crate) fn canonical_unique_prefix(
        idx_def: &IndexDefinition,
        idx_key: &str,
        primary_key: &str,
    ) -> String {
        let base_prefix = &idx_key[..idx_key.len() - primary_key.len()];
        if idx_def.encoding_version >= CURRENT_INDEX_ENCODING_VERSION
            || !idx_def
                .fields
                .iter()
                .any(|(_, ft)| *ft == IndexFieldType::Float)
        {
            return base_prefix.to_string();
        }
        let scan_prefix = build_index_scan_prefix(idx_def.id);
        let Some(rest) = base_prefix
            .strip_prefix(&scan_prefix)
            .and_then(|s| s.strip_suffix('\x00'))
        else {
            return base_prefix.to_string();
        };
        let parts: Vec<&str> = rest.split('\x00').collect();
        if parts.len() != idx_def.fields.len() {
            return base_prefix.to_string();
        }
        let mut out = scan_prefix;
        for (segment, (_, field_type)) in parts.iter().zip(idx_def.fields.iter()) {
            let canonical_seg =
                if *field_type == IndexFieldType::Float && *segment == V0_NEG_ZERO_HEX {
                    V0_POS_ZERO_HEX
                } else {
                    *segment
                };
            out.push_str(canonical_seg);
            out.push('\x00');
        }
        out
    }

    fn check_unique(
        &self,
        idx_def: &IndexDefinition,
        idx_key: &str,
        primary_key: &str,
        batch: &WriteBatch,
        read_collector: Option<&mut crate::volcano::ReadCollector>,
    ) -> Result<(), OmniError> {
        let base_prefix = &idx_key[..idx_key.len() - primary_key.len()];
        let scan_prefix = build_index_scan_prefix(idx_def.id);
        let parsed_parts: Option<Vec<&str>> = base_prefix
            .strip_prefix(&scan_prefix)
            .and_then(|s| s.strip_suffix('\x00'))
            .map(|rest| rest.split('\x00').collect::<Vec<_>>())
            .filter(|parts| parts.len() == idx_def.fields.len());

        let (ranges, needs_v0_filter) = match parsed_parts.as_deref() {
            Some(parts) => Self::v0_scan_ranges_for_parts(idx_def, parts),
            None => (
                vec![(
                    base_prefix.to_string(),
                    format!("{}\x01", &base_prefix[..base_prefix.len() - 1]),
                )],
                false,
            ),
        };

        let seq = self.db.get_seq();
        if let Some(collector) = read_collector {
            for (range_start, range_end) in &ranges {
                collector
                    .ranges
                    .push((range_start.clone(), range_end.clone()));
            }
        }
        let key_matches = |candidate: &str| -> bool {
            if needs_v0_filter {
                parsed_parts.as_deref().is_some_and(|parts| {
                    Self::v0_index_key_matches_parts(idx_def, candidate, parts)
                })
            } else {
                ranges
                    .iter()
                    .any(|(start, end)| candidate >= start.as_str() && candidate < end.as_str())
            }
        };
        for (range_start, range_end) in &ranges {
            for (existing_key, existing_pk) in
                self.db.scan_exclusive(range_start, Some(range_end), seq)?
            {
                if !key_matches(&existing_key) {
                    continue;
                }
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
        }
        // Dedup against conflicting staged writes/deletes in the same batch.
        for (pending_key, pending_pk, _) in &batch.buffered_writes {
            let Ok(pending_key) = std::str::from_utf8(pending_key) else {
                continue;
            };
            if key_matches(pending_key)
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
                try_encode_index_value_for_version(value, field_type, idx_def.encoding_version)
                    .ok_or_else(|| OmniError::IoError("invalid index lookup value".into()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let hex_parts: Vec<String> = encoded.iter().map(|part| hex_encode(part)).collect();
        let part_refs: Vec<&str> = hex_parts.iter().map(String::as_str).collect();
        let (ranges, needs_v0_filter) = Self::v0_scan_ranges_for_parts(idx_def, &part_refs);

        let mut results = Vec::new();
        let dedup = ranges.len() > 1 || needs_v0_filter;
        let mut seen = dedup.then(HashSet::new);
        for (range_start, range_end) in &ranges {
            for (key, pk) in self.db.scan_exclusive(range_start, Some(range_end), seq)? {
                if needs_v0_filter && !Self::v0_index_key_matches_parts(idx_def, &key, &part_refs) {
                    continue;
                }
                if let Some(seen) = seen.as_mut()
                    && !seen.insert(pk.clone())
                {
                    continue;
                }
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
            try_encode_index_value_for_version(value, ft, idx_def.encoding_version)
                .map(|encoded| hex_encode(&encoded))
                .ok_or_else(|| OmniError::IoError("invalid index range bound".into()))
        };
        let mut start_hex = start_value.map(encode_bound).transpose()?;
        let mut end_hex = end_value.map(encode_bound).transpose()?;
        let mut widened_v0_zero = false;
        if idx_def.encoding_version < CURRENT_INDEX_ENCODING_VERSION && *ft == IndexFieldType::Float
        {
            // Legacy v0 float indexes stored `-0.0` at `7fffffffffffffff` and
            // `+0.0` at `8000000000000000`. Widen inclusive zero bounds so
            // range scans never drop the opposite-signed zero.
            if start_hex.as_deref() == Some(V0_POS_ZERO_HEX) {
                start_hex = Some(V0_NEG_ZERO_HEX.to_string());
                widened_v0_zero = true;
            }
            if end_hex.as_deref() == Some(V0_NEG_ZERO_HEX) {
                end_hex = Some(V0_POS_ZERO_HEX.to_string());
                widened_v0_zero = true;
            }
        }
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
        let mut seen = widened_v0_zero.then(HashSet::new);
        for (key, pk) in self.db.scan_exclusive(&start_key, Some(&end_key), seq)? {
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
            if let Some(seen) = seen.as_mut()
                && !seen.insert(pk.clone())
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
        let mut idx_def = self.definition(index_name)?;
        let target_matches = idx_def.collection == expected_collection
            || self.resolve_collection_name_at(expected_collection, self.db.get_seq())?
                == idx_def.collection;
        if !target_matches {
            return Err(OmniError::IoError(format!(
                "Index '{}' not found on collection '{}'",
                index_name, expected_collection
            )));
        }
        let upgraded_encoding = idx_def.encoding_version < CURRENT_INDEX_ENCODING_VERSION;
        if upgraded_encoding {
            idx_def.encoding_version = CURRENT_INDEX_ENCODING_VERSION;
        }
        let upgraded_namespace = if idx_def.namespace == IndexNamespace::Both {
            let resolve_seq = self.db.get_seq();
            let resolved =
                resolve_legacy_namespace_at(&self.db, &idx_def.collection, resolve_seq)?
                    .ok_or_else(|| {
                        OmniError::IoError(format!(
                            "legacy index '{}' on collection '{}' has ambiguous namespace ownership; call bind_index_namespace('{}', IndexNamespace::Sql | IndexNamespace::Document) to bind and rebuild it",
                            idx_def.name, idx_def.collection, idx_def.name
                        ))
                    })?;
            if resolved == IndexNamespace::Sql
                && let Some(raw) = self.db.find(
                    &crate::catalog::Catalog::catalog_key(&idx_def.collection),
                    resolve_seq,
                )?
            {
                let table: crate::catalog::TableDef = serde_json::from_str(&raw).map_err(|e| {
                    OmniError::IoError(format!("invalid SQL table definition: {e}"))
                })?;
                validate_sql_index_fields(&table, &idx_def.fields)?;
            }
            idx_def.namespace = resolved;
            true
        } else {
            false
        };
        let mut invalidate = WriteBatch::new();
        invalidate.delete(&format!("{READY_PREFIX}{}", idx_def.id))?;
        self.db.commit_batch(&invalidate)?;

        // Clear any pre-existing or partially built entries for this index ID
        // before scanning the collection at a fresh snapshot.
        let index_prefix = build_index_scan_prefix(idx_def.id);
        loop {
            let clear_seq = self.db.get_seq();
            let stale =
                self.db
                    .scan_prefix_limit(&index_prefix, clear_seq, WriteBatch::MAX_OPS / 2)?;
            if stale.is_empty() {
                break;
            }
            let mut clear_batch = WriteBatch::new();
            for (stale_key, _) in stale {
                clear_batch.delete(&stale_key)?;
            }
            self.db.commit_batch(&clear_batch)?;
        }

        if upgraded_encoding || upgraded_namespace {
            // Publish upgraded encoding/namespace metadata after clearing all
            // legacy entries and BEFORE writing rebuilt entries so a crash
            // mid-rebuild never leaves mismatched catalog metadata on disk.
            let catalog_key = format!("{CATALOG_PREFIX}{}", idx_def.id);
            let catalog_value =
                serde_json::to_string(&idx_def).map_err(|e| OmniError::IoError(e.to_string()))?;
            let mut upgrade = WriteBatch::new();
            upgrade.set(&catalog_key, catalog_value)?;
            let upgrade_seq = self.db.commit_batch(&upgrade)?;
            self.db.ssi_history().record_committed(
                0,
                upgrade_seq,
                &crate::transaction::SsiCommitRecord {
                    txn_id: 0,
                    write_keys: vec![catalog_key],
                    read_keys: Vec::new(),
                    read_ranges: Vec::new(),
                },
            );
        }

        let seq = self.db.get_seq();
        let mut single_catalog = IndexCatalog::empty();
        single_catalog.register_in_memory(idx_def.clone());
        let table_row_format = self
            .db
            .find(
                &format!("\x00CATALOG\x00{}", idx_def.collection.to_lowercase()),
                seq,
            )?
            .and_then(|raw| serde_json::from_str::<crate::catalog::TableDef>(&raw).ok())
            .filter(|table| table.name == idx_def.collection)
            .map(|table| table.row_format)
            .unwrap_or_default();

        let mut count = 0;
        let mut batch = WriteBatch::new();

        for (collection_prefix, collection_end) in
            collection_row_ranges_for_namespace(&idx_def.collection, idx_def.namespace)
        {
            let is_sql_rows = collection_prefix.starts_with("\x00T\x00");
            for (key, value) in
                self.db
                    .scan_exclusive(&collection_prefix, Some(&collection_end), seq)?
            {
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
                self.index_document_with_catalog_for_namespace(
                    &single_catalog,
                    &idx_def.collection,
                    idx_def.namespace,
                    pk,
                    &doc,
                    &mut batch,
                    None,
                )?;
                count += 1;

                if batch.op_count() >= 1000 {
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
    ///
    /// SQL indexes match the resolved canonical SQL table name (case-insensitive),
    /// while document and unbound legacy indexes match `collection` verbatim so
    /// case-differing document collections remain visible and isolated.
    pub fn list_indexes(&self, collection: &str) -> Result<Vec<IndexDefinition>, OmniError> {
        let seq = self.db.get_seq();
        let canonical = self.resolve_collection_name_at(collection, seq)?;
        let catalog = self.catalog_snapshot_at(seq)?;
        Ok(catalog
            .indexes
            .values()
            .filter(|idx| match catalog.effective_namespace(idx) {
                Some(IndexNamespace::Sql) => idx.collection == canonical,
                _ => idx.collection == collection,
            })
            .cloned()
            .collect())
    }
}
