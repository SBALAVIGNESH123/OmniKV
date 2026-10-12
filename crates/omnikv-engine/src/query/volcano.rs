//! Volcano Iterator Model — streaming executor
//!
//! A pull-based iterator pipeline. Each operator implements `next()`
//! returning one row at a time, which means:
//! - O(1) memory for filter, project, limit
//! - Only sort and hash-join buffer rows (unavoidable)
//! - Can process tables larger than RAM
//!
//! ## Architecture
//! ```text
//! Client calls next()
//!    ↓
//! ProjectIter::next()
//!    ↓
//! LimitIter::next()
//!    ↓
//! SortIter::next()  ← buffers all rows (unavoidable for sort)
//!    ↓
//! FilterIter::next()  ← O(1) memory, passes through matching rows
//!    ↓
//! SeqScanIter::next()  ← reads one row at a time from storage
//! ```

#![expect(
    dead_code,
    reason = "The streaming executor contains staged operator variants used by benchmark and planner work that is not fully wired into the public SQL path yet."
)]

use crate::OmniKV;
use crate::catalog::{Catalog, ColumnType, RowFormat, TableDef};
use crate::optimizer::{AccessMethod, PlanNode};
use crate::sql::{AggFunc, CmpOp, JoinType, OrderByItem, SelectColumn, SqlValue, WhereExpr};
use crate::sql_exec::Row;
use std::collections::HashMap;
use std::sync::Arc;

// ─── Iterator Trait ─────────────────────────────────────────────────────────

/// The core volcano iterator trait. Every operator implements this.
///
/// `next_row` is the compatibility primitive for custom operators and
/// complex nodes. Built-in streaming operators also override `next_chunk`, so
/// a caller can use batch dispatch for scan/filter/project/limit without
/// closing the operator set. The SQL dispatch policy is documented in
/// `docs/volcano-dispatch.md`.
pub trait RowIterator {
    /// Returns the next row, or None when exhausted.
    fn next_row(&mut self) -> Option<Row>;

    /// Fill `out` with up to `max_rows` rows and return the number appended.
    ///
    /// Implementations must return `0` only when exhausted or when
    /// `max_rows == 0`. The default preserves compatibility for existing and
    /// third-party row-at-a-time operators.
    fn next_chunk(&mut self, max_rows: usize, out: &mut Vec<Row>) -> usize {
        let start_len = out.len();
        if max_rows == 0 {
            return 0;
        }
        while out.len() - start_len < max_rows {
            match self.next_row() {
                Some(row) => out.push(row),
                None => break,
            }
        }
        out.len() - start_len
    }

    /// Reset the iterator to the beginning (for nested loops).
    fn reset(&mut self) {}

    /// Collect all remaining rows (convenience, used for sort/hash-join).
    fn collect_all(&mut self) -> Vec<Row> {
        let mut rows = Vec::new();
        while let Some(row) = self.next_row() {
            rows.push(row);
        }
        rows
    }
}

/// Default batch size for the chunked volcano path.
///
/// 1024 rows is intentionally conservative: it amortizes vtable dispatch while
/// keeping per-operator scratch buffers modest for typical SQL result rows.
pub const DEFAULT_ROW_CHUNK_SIZE: usize = 1024;

// ─── Sequential Scan Iterator ───────────────────────────────────────────────

/// Streams rows from a table one at a time.
///
/// The scan reads at a caller-provided MVCC sequence and overlays the
/// transaction's own buffered writes: rows the transaction has written or
/// deleted (but not yet committed) appear exactly as the transaction's
/// later statements will see them — PostgreSQL's read-your-own-writes.
/// A delete in the overlay removes the row; a write replaces it.
pub struct SeqScanIter {
    rows: Vec<Row>,
    pos: usize,
}

/// A transaction's uncommitted writes as seen by scans inside that
/// transaction: full storage key → serialized row (Some) for a write,
/// tombstone (None) for a delete. Tombstones shadow (hide) storage rows
/// and writes replace them. Owned keys: an overlay is built per statement
/// from the transaction's staged batch and dropped with the plan.
pub type TxnOverlay = std::collections::HashMap<String, Option<String>>;

impl SeqScanIter {
    pub fn new(db: &Arc<OmniKV>, table: &TableDef) -> Self {
        Self::with_scan(db, table, db.get_seq(), None, None)
    }

    /// Create with column pruning — only deserialize needed columns.
    pub fn new_pruned(db: &Arc<OmniKV>, table: &TableDef, needed: &[String]) -> Self {
        let mut iter = Self::new(db, table);
        if !needed.is_empty() {
            let pruned_rows = std::mem::take(&mut iter.rows)
                .into_iter()
                .map(|full| {
                    full.into_iter()
                        .filter(|(k, _)| needed.iter().any(|c| c.eq_ignore_ascii_case(k)))
                        .collect::<Row>()
                })
                .collect::<Vec<_>>();
            iter.rows = pruned_rows;
        }
        iter
    }

    /// Transaction-aware scan: reads at `scan_seq` (the transaction's
    /// snapshot) and applies `overlay` on top — the transaction's own
    /// buffered writes. Overlay keys are full storage row keys
    /// (`table.row_prefix() + pk`); a `Some` value replaces the stored
    /// row, `None` deletes it. Rows the overlay covers are skipped from
    /// the storage scan so exactly one copy survives.
    pub fn with_scan(
        db: &Arc<OmniKV>,
        table: &TableDef,
        scan_seq: u64,
        overlay: Option<&TxnOverlay>,
        reads: Option<&std::sync::Arc<std::sync::Mutex<ReadCollector>>>,
    ) -> Self {
        let prefix = table.row_prefix();
        if let Some(reads) = reads
            && let Ok(mut collector) = reads.lock()
        {
            collector.record_range(&prefix, &format!("{}\x7F", prefix));
        }
        let mut rows: Vec<Row> = db
            .scan(&prefix, &format!("{}\x7F", prefix), scan_seq)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|(key, value)| {
                if let Some(ov) = overlay {
                    // The overlay owns this key — written or deleted by
                    // this transaction. Skip the stored copy; the overlay
                    // copy is applied below.
                    if ov.contains_key(key.as_str()) {
                        return None;
                    }
                }
                crate::plan_exec::PlanExecutor::deserialize_row(&value, table.row_format)
            })
            .collect();
        if let Some(ov) = overlay {
            for (key, value) in ov {
                let in_table = key.starts_with(prefix.as_str()) && key.len() > prefix.len();
                if let (true, Some(serialized)) = (in_table, value)
                    && let Some(row) = crate::plan_exec::PlanExecutor::deserialize_row(
                        serialized,
                        RowFormat::Typed,
                    )
                {
                    rows.push(row);
                }
            }
        }
        Self { rows, pos: 0 }
    }
}

impl RowIterator for SeqScanIter {
    fn next_row(&mut self) -> Option<Row> {
        if self.pos < self.rows.len() {
            let row = self.rows[self.pos].clone();
            self.pos += 1;
            Some(row)
        } else {
            None
        }
    }

    fn reset(&mut self) {
        self.pos = 0;
    }

    fn next_chunk(&mut self, max_rows: usize, out: &mut Vec<Row>) -> usize {
        let remaining = self.rows.len().saturating_sub(self.pos);
        let take = remaining.min(max_rows);
        if take == 0 {
            return 0;
        }
        out.extend(self.rows[self.pos..self.pos + take].iter().cloned());
        self.pos += take;
        take
    }
}

// ─── PK Lookup Iterator ────────────────────────────────────────────────────

/// Single-row lookup by primary key. O(1).
pub struct PkLookupIter {
    row: Option<Row>,
    consumed: bool,
}

impl PkLookupIter {
    pub fn new(db: &Arc<OmniKV>, table: &TableDef, key_value: &str) -> Self {
        Self::with_scan(db, table, key_value, db.get_seq(), None, None)
    }

    /// Transaction-aware lookup: the transaction's overlay wins over
    /// storage — a buffered write for this key is visible even though it
    /// is not committed, and a buffered delete hides the stored row.
    pub fn with_scan(
        db: &Arc<OmniKV>,
        table: &TableDef,
        key_value: &str,
        scan_seq: u64,
        overlay: Option<&TxnOverlay>,
        reads: Option<&std::sync::Arc<std::sync::Mutex<ReadCollector>>>,
    ) -> Self {
        let key = format!("{}{}", table.row_prefix(), key_value);
        let end = format!("{}{}\x7F", table.row_prefix(), key_value);
        if let Some(ov) = overlay {
            match ov.get(key.as_str()) {
                // Buffered write: read our own uncommitted row.
                Some(Some(serialized)) => {
                    return Self {
                        row: crate::plan_exec::PlanExecutor::deserialize_row(
                            serialized,
                            RowFormat::Typed,
                        ),
                        consumed: false,
                    };
                }
                // Buffered delete: the row is gone for this transaction.
                Some(None) => {
                    return Self {
                        row: None,
                        consumed: false,
                    };
                }
                None => {}
            }
        }
        if let Some(reads) = reads
            && let Ok(mut collector) = reads.lock()
        {
            collector.record_point(&key);
        }
        let row = db
            .scan(&key, &end, scan_seq)
            .unwrap_or_default()
            .into_iter()
            .next()
            .and_then(|(_, value)| {
                crate::plan_exec::PlanExecutor::deserialize_row(&value, table.row_format)
            });
        Self {
            row,
            consumed: false,
        }
    }
}

impl RowIterator for PkLookupIter {
    fn next_row(&mut self) -> Option<Row> {
        if !self.consumed {
            self.consumed = true;
            self.row.clone()
        } else {
            None
        }
    }

    fn reset(&mut self) {
        self.consumed = false;
    }

    fn next_chunk(&mut self, max_rows: usize, out: &mut Vec<Row>) -> usize {
        if max_rows == 0 || self.consumed {
            return 0;
        }
        self.consumed = true;
        if let Some(row) = &self.row {
            out.push(row.clone());
            1
        } else {
            0
        }
    }
}

// ─── Filter Iterator ────────────────────────────────────────────────────────

/// Passes through only rows matching the predicate. O(1) memory.
pub struct FilterIter {
    child: Box<dyn RowIterator>,
    predicate: WhereExpr,
    col_types: ColumnTypeMap,
    scratch: Vec<Row>,
}

impl FilterIter {
    pub fn new(child: Box<dyn RowIterator>, predicate: WhereExpr) -> Self {
        Self::with_types(child, predicate, ColumnTypeMap::default())
    }

    /// Build with the table's declared column types, so ordering follows the
    /// column's affinity rather than the literal's shape.
    pub fn with_types(
        child: Box<dyn RowIterator>,
        predicate: WhereExpr,
        col_types: ColumnTypeMap,
    ) -> Self {
        Self {
            child,
            predicate,
            col_types,
            scratch: Vec::with_capacity(DEFAULT_ROW_CHUNK_SIZE),
        }
    }
}

impl RowIterator for FilterIter {
    fn next_row(&mut self) -> Option<Row> {
        loop {
            let row = self.child.next_row()?;
            if eval_where_typed(&row, &self.predicate, &self.col_types) {
                return Some(row);
            }
        }
    }

    fn next_chunk(&mut self, max_rows: usize, out: &mut Vec<Row>) -> usize {
        let start_len = out.len();
        if max_rows == 0 {
            return 0;
        }

        while out.len() - start_len < max_rows {
            self.scratch.clear();
            let requested = max_rows - (out.len() - start_len);
            let n = self.child.next_chunk(requested, &mut self.scratch);
            if n == 0 {
                break;
            }
            out.extend(
                self.scratch
                    .drain(..)
                    .filter(|row| eval_where_typed(row, &self.predicate, &self.col_types)),
            );
        }

        out.len() - start_len
    }
}

// ─── Project Iterator ───────────────────────────────────────────────────────

/// Projects specific columns from each row. O(1) memory.
pub struct ProjectIter {
    child: Box<dyn RowIterator>,
    columns: Vec<SelectColumn>,
    scratch: Vec<Row>,
}

impl ProjectIter {
    pub fn new(child: Box<dyn RowIterator>, columns: Vec<SelectColumn>) -> Self {
        Self {
            child,
            columns,
            scratch: Vec::with_capacity(DEFAULT_ROW_CHUNK_SIZE),
        }
    }
}

impl RowIterator for ProjectIter {
    fn next_row(&mut self) -> Option<Row> {
        let row = self.child.next_row()?;
        Some(project_row(row, &self.columns))
    }

    fn next_chunk(&mut self, max_rows: usize, out: &mut Vec<Row>) -> usize {
        if max_rows == 0 {
            return 0;
        }
        self.scratch.clear();
        let n = self.child.next_chunk(max_rows, &mut self.scratch);
        out.extend(
            self.scratch
                .drain(..)
                .map(|row| project_row(row, &self.columns)),
        );
        n
    }
}

fn project_row(row: Row, columns: &[SelectColumn]) -> Row {
    if columns.iter().any(|c| matches!(c, SelectColumn::Star)) {
        return row;
    }
    let mut projected = Row::new();
    for col in columns {
        match col {
            SelectColumn::Named(n) => {
                if let Some(v) = row_lookup(&row, n) {
                    projected.insert(n.clone(), Some(v.to_string()));
                }
            }
            SelectColumn::Qualified(t, n) => {
                let key = format!("{}.{}", t, n);
                let val = row_lookup(&row, &key).map(str::to_string);
                projected.insert(key, val.clone());
                projected.entry(n.clone()).or_insert(val);
            }
            SelectColumn::Aggregate(func, target) => {
                let name = format!("{}({})", format!("{:?}", func).to_lowercase(), target);
                if let Some(v) = row.get(&name) {
                    projected.insert(name, v.clone());
                }
            }
            _ => {}
        }
    }
    projected
}

// ─── Limit Iterator ─────────────────────────────────────────────────────────

/// Stops after emitting `count` rows. O(1) memory.
pub struct LimitIter {
    child: Box<dyn RowIterator>,
    limit: usize,
    emitted: usize,
}

impl LimitIter {
    pub fn new(child: Box<dyn RowIterator>, limit: usize) -> Self {
        Self {
            child,
            limit,
            emitted: 0,
        }
    }
}

impl RowIterator for LimitIter {
    fn next_row(&mut self) -> Option<Row> {
        if self.emitted >= self.limit {
            return None;
        }
        let row = self.child.next_row()?;
        self.emitted += 1;
        Some(row)
    }

    fn next_chunk(&mut self, max_rows: usize, out: &mut Vec<Row>) -> usize {
        if max_rows == 0 || self.emitted >= self.limit {
            return 0;
        }
        let remaining = self.limit - self.emitted;
        let requested = max_rows.min(remaining);
        let n = self.child.next_chunk(requested, out);
        self.emitted += n;
        n
    }
}

// ─── Sort Iterator ──────────────────────────────────────────────────────────

/// Materializes all child rows, sorts them, then streams out.
/// Memory: O(N) — unavoidable for general sort.
pub struct SortIter {
    sorted: Vec<Row>,
    pos: usize,
}

impl SortIter {
    /// `types` carries the sort key's declared `ColumnType`, so a TEXT
    /// column orders lexically and a numeric one numerically. Without it
    /// `smart_cmp` would order TEXT "10" before "2", disagreeing with the
    /// type-aware comparison a WHERE clause on the same column uses.
    pub fn new(
        mut child: Box<dyn RowIterator>,
        order_by: Vec<OrderByItem>,
        types: ColumnTypeMap,
    ) -> Self {
        let mut rows = child.collect_all();
        for item in order_by.iter().rev() {
            let col = item.column.clone();
            let desc = item.desc;
            let col_type = type_lookup(&types, &col).cloned();
            rows.sort_by(|a, b| {
                let va = row_lookup(a, &col).map(str::to_string);
                let vb = row_lookup(b, &col).map(str::to_string);
                let cmp = match (va.as_deref(), vb.as_deref()) {
                    (Some(x), Some(y)) => match &col_type {
                        // A declared type decides: numeric columns compare
                        // numerically, everything else lexically.
                        Some(t) if t.is_numeric() => smart_cmp(x, y),
                        Some(_) => x.cmp(y),
                        // No declared type: keep the type-agnostic heuristic.
                        None => smart_cmp(x, y),
                    },
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                };
                if desc { cmp.reverse() } else { cmp }
            });
        }
        Self {
            sorted: rows,
            pos: 0,
        }
    }
}

impl RowIterator for SortIter {
    fn next_row(&mut self) -> Option<Row> {
        if self.pos < self.sorted.len() {
            let row = self.sorted[self.pos].clone();
            self.pos += 1;
            Some(row)
        } else {
            None
        }
    }

    fn next_chunk(&mut self, max_rows: usize, out: &mut Vec<Row>) -> usize {
        let remaining = self.sorted.len().saturating_sub(self.pos);
        let take = remaining.min(max_rows);
        if take == 0 {
            return 0;
        }
        out.extend(self.sorted[self.pos..self.pos + take].iter().cloned());
        self.pos += take;
        take
    }
}

// ─── Hash Join Iterator ─────────────────────────────────────────────────────

/// Build phase: materializes the build (smaller) side into a hash table.
/// Probe phase: streams probe side, looking up matches.
/// Memory: O(build_size) — standard hash join cost.
pub struct HashJoinIter {
    hash_table: HashMap<String, Vec<Row>>,
    probe: Box<dyn RowIterator>,
    build_col: String,
    probe_col: String,
    join_type: JoinType,
    // Probe columns are also stored qualified so a name shared with the
    // build side survives the merge.
    probe_prefix: String,
    // The build side's own qualifier, so it is stored `table.column` too
    // and a qualified predicate binds to the right table on either side.
    build_prefix: String,
    // Buffer for multiple matches on the join
    current_matches: Vec<Row>,
    match_pos: usize,
    // For RIGHT JOIN: track which build keys were matched
    matched_build_keys: std::collections::HashSet<String>,
    null_key_build_rows: Vec<Row>,
    right_unmatched: Vec<Row>,
    right_unmatched_pos: usize,
    probe_exhausted: bool,
    probe_key_type: Option<ColumnType>,
}

impl HashJoinIter {
    #[expect(
        clippy::too_many_arguments,
        reason = "Each join side supplies its iterator, key column, table name, and key type; the sides stay positional so the build/probe pairing is visible at the call site."
    )]
    pub fn new(
        mut build: Box<dyn RowIterator>,
        probe: Box<dyn RowIterator>,
        build_col: String,
        probe_col: String,
        join_type: JoinType,
        build_table: &str,
        probe_table: &str,
        build_key_type: Option<ColumnType>,
        probe_key_type: Option<ColumnType>,
    ) -> Self {
        // Build phase: materialize build side into hash table
        let mut hash_table: HashMap<String, Vec<Row>> = HashMap::new();
        let mut null_key_build_rows: Vec<Row> = Vec::new();
        while let Some(row) = build.next_row() {
            let Some(key) = row.get(&build_col).cloned().flatten() else {
                null_key_build_rows.push(row);
                continue;
            };
            let key = canonicalize_join_key(&key, build_key_type.as_ref());
            hash_table.entry(key).or_default().push(row);
        }
        Self {
            hash_table,
            null_key_build_rows,
            probe,
            build_col,
            probe_col,
            join_type,
            probe_prefix: format!("{probe_table}."),
            build_prefix: format!("{build_table}."),
            current_matches: Vec::new(),
            match_pos: 0,
            matched_build_keys: std::collections::HashSet::new(),
            right_unmatched: Vec::new(),
            right_unmatched_pos: 0,
            probe_exhausted: false,
            probe_key_type,
        }
    }
}

/// Store a row's columns under `table.` as well as the bare name.
///
/// An unmatched outer-join row would otherwise carry only bare keys, and a
/// predicate naming the absent table would rebind to the present table's
/// column of the same name. Carrying its own qualified keys keeps the
/// binding and lets the missing side read as NULL.
pub(crate) fn qualify_row(row: Row, prefix: &str) -> Row {
    let mut out = Row::new();
    for (k, v) in &row {
        out.insert(k.clone(), v.clone());
        out.insert(format!("{prefix}{k}"), v.clone());
    }
    out
}

impl RowIterator for HashJoinIter {
    fn next_row(&mut self) -> Option<Row> {
        loop {
            // First, drain any buffered matches
            if self.match_pos < self.current_matches.len() {
                let row = self.current_matches[self.match_pos].clone();
                self.match_pos += 1;
                return Some(row);
            }

            // For RIGHT JOIN: after probe exhausted, emit unmatched build rows
            if self.probe_exhausted {
                if self.right_unmatched_pos < self.right_unmatched.len() {
                    let row = self.right_unmatched[self.right_unmatched_pos].clone();
                    self.right_unmatched_pos += 1;
                    return Some(row);
                }
                return None;
            }

            // Get next probe row
            let probe_row = match self.probe.next_row() {
                Some(r) => r,
                None => {
                    // Probe exhausted — for RIGHT JOIN, collect unmatched build rows
                    self.probe_exhausted = true;
                    if matches!(self.join_type, JoinType::Right) {
                        for (key, rows) in &self.hash_table {
                            if !self.matched_build_keys.contains(key) {
                                self.right_unmatched.extend(
                                    rows.iter()
                                        .map(|r| qualify_row(r.clone(), &self.build_prefix)),
                                );
                            }
                        }
                        self.right_unmatched.extend(
                            self.null_key_build_rows
                                .iter()
                                .map(|r| qualify_row(r.clone(), &self.build_prefix)),
                        );
                    }
                    continue;
                }
            };
            let key = probe_row
                .get(&self.probe_col)
                .cloned()
                .flatten()
                .map(|k| canonicalize_join_key(&k, self.probe_key_type.as_ref()));

            // A NULL probe key matches nothing: `NULL = x` is UNKNOWN, so it
            // must not fall through to a "" lookup and hit a build row whose
            // key is a real empty string.
            let matched = key
                .as_deref()
                .and_then(|k| self.hash_table.get(k))
                .map(|build_rows| {
                    if matches!(self.join_type, JoinType::Right) {
                        self.matched_build_keys
                            .insert(key.clone().unwrap_or_default());
                    }
                    self.current_matches.clear();
                    self.match_pos = 0;
                    for build_row in build_rows {
                        let mut combined = Row::new();
                        for (k, v) in build_row {
                            combined.insert(k.clone(), v.clone());
                            combined.insert(format!("{}{k}", self.build_prefix), v.clone());
                        }
                        for (k, v) in &probe_row {
                            // Also store it qualified so a shared name isn't dropped.
                            combined.insert(format!("{}{k}", self.probe_prefix), v.clone());
                            combined.entry(k.clone()).or_insert_with(|| v.clone());
                        }
                        self.current_matches.push(combined);
                    }
                });

            if matched.is_none() && matches!(self.join_type, JoinType::Left) {
                self.current_matches = vec![qualify_row(probe_row, &self.probe_prefix)];
                self.match_pos = 0;
            }
        }
    }
}

// ─── Aggregate Iterator ─────────────────────────────────────────────────────

/// Materializes child, groups, computes aggregates, streams result groups.
pub struct AggregateIter {
    result: Vec<Row>,
    pos: usize,
}

impl AggregateIter {
    pub fn new(
        mut child: Box<dyn RowIterator>,
        group_by: Vec<String>,
        agg_columns: Vec<SelectColumn>,
        types: ColumnTypeMap,
    ) -> Self {
        let all_rows = child.collect_all();

        if group_by.is_empty()
            && agg_columns
                .iter()
                .any(|c| matches!(c, SelectColumn::Aggregate(..)))
        {
            // Aggregate without GROUP BY — single result row
            let refs: Vec<&Row> = all_rows.iter().collect();
            let mut row = Row::new();
            for col in &agg_columns {
                if let SelectColumn::Aggregate(func, target) = col {
                    let (name, val) =
                        compute_aggregate(func, target, &refs, type_lookup(&types, target));
                    row.insert(name, val);
                }
            }
            return Self {
                result: vec![row],
                pos: 0,
            };
        }

        let mut groups: HashMap<String, Vec<Row>> = HashMap::new();
        for row in &all_rows {
            let key: String = group_by
                .iter()
                .map(|g| encode_key_part(row_lookup(row, g)))
                .collect::<Vec<_>>()
                .join("\u{0}");
            groups.entry(key).or_default().push(row.clone());
        }

        let mut result = Vec::new();
        for group_rows in groups.values() {
            let refs: Vec<&Row> = group_rows.iter().collect();
            let mut row = Row::new();
            for col in &agg_columns {
                match col {
                    SelectColumn::Named(name) => {
                        if let Some(val) = row_lookup(refs[0], name).map(str::to_string) {
                            row.insert(name.clone(), Some(val));
                        }
                    }
                    SelectColumn::Qualified(t, n) => {
                        let key = format!("{}.{}", t, n);
                        if let Some(val) = row_lookup(refs[0], &key).map(str::to_string) {
                            row.insert(key, Some(val));
                        }
                    }
                    SelectColumn::Aggregate(func, target) => {
                        let (name, val) =
                            compute_aggregate(func, target, &refs, type_lookup(&types, target));
                        row.insert(name, val);
                    }
                    _ => {}
                }
            }
            result.push(row);
        }

        Self { result, pos: 0 }
    }
}

impl RowIterator for AggregateIter {
    fn next_row(&mut self) -> Option<Row> {
        if self.pos < self.result.len() {
            let row = self.result[self.pos].clone();
            self.pos += 1;
            Some(row)
        } else {
            None
        }
    }

    fn next_chunk(&mut self, max_rows: usize, out: &mut Vec<Row>) -> usize {
        let remaining = self.result.len().saturating_sub(self.pos);
        let take = remaining.min(max_rows);
        if take == 0 {
            return 0;
        }
        out.extend(self.result[self.pos..self.pos + take].iter().cloned());
        self.pos += take;
        take
    }
}

// ─── Plan-to-Iterator Compiler ──────────────────────────────────────────────

/// The read context for scans inside a transaction: the MVCC snapshot
/// sequence reads must use, and the transaction's own uncommitted writes
/// overlaid on storage (read-your-own-writes). `None` scans read at
/// `db.get_seq()` with no overlay — the autocommit behavior.
/// Where a transactional plan's scans record their SSI read
/// dependencies: point reads land in `keys`, range scans in `ranges`.
/// The wire core merges both into the transaction alongside its staged
/// writes, so any committed write that invalidates these reads aborts
/// the COMMIT.
#[derive(Default)]
pub struct ReadCollector {
    pub keys: std::collections::HashSet<String>,
    pub ranges: Vec<(String, String)>,
}

impl ReadCollector {
    fn record_point(&mut self, key: &str) {
        self.keys.insert(key.to_string());
    }

    fn record_range(&mut self, start: &str, end: &str) {
        self.ranges.push((start.to_string(), end.to_string()));
    }
}

pub struct ScanContext {
    pub scan_seq: u64,
    pub overlay: TxnOverlay,
    /// SSI read-dependency collector — `None` on autocommit plans.
    pub reads: Option<std::sync::Arc<std::sync::Mutex<ReadCollector>>>,
}

/// Compiles a PlanNode tree into a volcano iterator pipeline.
pub fn compile_plan(
    plan: &PlanNode,
    db: &Arc<OmniKV>,
    catalog: &Arc<Catalog>,
) -> Box<dyn RowIterator> {
    compile_plan_with_scan(plan, db, catalog, None)
}

/// `compile_plan` with a transaction's [`ScanContext`]: every scan in the
/// plan reads at the transaction's snapshot and sees its own buffered
/// writes. Unchanged behavior for plans compiled without one.
pub fn compile_plan_with_scan(
    plan: &PlanNode,
    db: &Arc<OmniKV>,
    catalog: &Arc<Catalog>,
    scan: Option<&ScanContext>,
) -> Box<dyn RowIterator> {
    match plan {
        PlanNode::Scan {
            table,
            access,
            filter,
            ..
        } => {
            let table_def = catalog
                .get_table(table)
                .expect("Table not found in catalog");
            let col_types = table_def
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.col_type.clone()))
                .collect::<ColumnTypeMap>();
            let base: Box<dyn RowIterator> = match access {
                AccessMethod::PkLookup { key_value } => {
                    let key_value = canonicalize_pk_key(&table_def, key_value);
                    if legacy_pk_needs_scan(&table_def) {
                        // A legacy numeric key may be stored as "007", which
                        // the canonical key range can never reach.
                        let base: Box<dyn RowIterator> = match scan {
                            Some(ctx) => Box::new(SeqScanIter::with_scan(
                                db,
                                &table_def,
                                ctx.scan_seq,
                                Some(&ctx.overlay),
                                ctx.reads.as_ref(),
                            )),
                            None => Box::new(SeqScanIter::new(db, &table_def)),
                        };
                        Box::new(FilterIter::with_types(
                            base,
                            pk_equality(&table_def, &key_value),
                            col_types.clone(),
                        ))
                    } else {
                        match scan {
                            Some(ctx) => Box::new(PkLookupIter::with_scan(
                                db,
                                &table_def,
                                &key_value,
                                ctx.scan_seq,
                                Some(&ctx.overlay),
                                ctx.reads.as_ref(),
                            )),
                            None => Box::new(PkLookupIter::new(db, &table_def, &key_value)),
                        }
                    }
                }
                AccessMethod::SeqScan | AccessMethod::IndexScan { .. } => match scan {
                    Some(ctx) => Box::new(SeqScanIter::with_scan(
                        db,
                        &table_def,
                        ctx.scan_seq,
                        Some(&ctx.overlay),
                        ctx.reads.as_ref(),
                    )),
                    None => Box::new(SeqScanIter::new(db, &table_def)),
                },
            };
            match filter {
                Some(pred) => Box::new(FilterIter::with_types(base, pred.clone(), col_types)),
                None => base,
            }
        }
        PlanNode::HashJoin {
            left,
            right,
            join_type,
            on_left_col,
            on_right_col,
            left_table,
            right_table,
            ..
        } => {
            let left_iter = compile_plan_with_scan(left, db, catalog, scan);
            let right_iter = compile_plan_with_scan(right, db, catalog, scan);
            Box::new(HashJoinIter::new(
                left_iter,
                right_iter,
                on_left_col.clone(),
                on_right_col.clone(),
                join_type.clone(),
                left_table,
                right_table,
                join_key_type(catalog, left_table, on_left_col),
                join_key_type(catalog, right_table, on_right_col),
            ))
        }
        PlanNode::Filter {
            child, predicate, ..
        } => {
            let child_iter = compile_plan_with_scan(child, db, catalog, scan);
            let col_types = child.output_types(catalog);
            Box::new(FilterIter::with_types(
                child_iter,
                predicate.clone(),
                col_types,
            ))
        }
        PlanNode::Project { child, columns } => {
            let child_iter = compile_plan_with_scan(child, db, catalog, scan);
            Box::new(ProjectIter::new(child_iter, columns.clone()))
        }
        PlanNode::Sort {
            child, order_by, ..
        } => {
            let child_iter = compile_plan_with_scan(child, db, catalog, scan);
            let col_types = child.output_types(catalog);
            Box::new(SortIter::new(child_iter, order_by.clone(), col_types))
        }
        PlanNode::Limit { child, count } => {
            let child_iter = compile_plan_with_scan(child, db, catalog, scan);
            Box::new(LimitIter::new(child_iter, *count))
        }
        PlanNode::Aggregate {
            child,
            group_by,
            aggregates,
            ..
        } => {
            let child_iter = compile_plan_with_scan(child, db, catalog, scan);
            let types = child.output_types(catalog);
            Box::new(AggregateIter::new(
                child_iter,
                group_by.clone(),
                aggregates.clone(),
                types,
            ))
        }
    }
}

// ─── Shared helpers ─────────────────────────────────────────────────────────

/// Declared column types, keyed by the name a predicate uses. A miss on a
/// join-qualified name falls back to the literal's own type.
pub type ColumnTypeMap = std::collections::HashMap<String, ColumnType>;

/// Ordering by the column's declared affinity, shared by `=` and `IN`. An
/// unknown type falls back to the literal's own type.
fn cmp_by_affinity(
    row_val: &str,
    value: &SqlValue,
    cmp_val: &str,
    col_type: Option<&ColumnType>,
) -> std::cmp::Ordering {
    match col_type {
        Some(t) if t.is_numeric() => smart_cmp(row_val, cmp_val),
        Some(_) => row_val.cmp(cmp_val),
        None => cmp_by_literal(row_val, value, cmp_val),
    }
}

/// Declared type of a join key, resolving either a bare column or a
/// `table.column` reference against the named table.
pub(crate) fn join_key_type(catalog: &Catalog, table: &str, col: &str) -> Option<ColumnType> {
    let table_def = catalog.get_table(table)?;
    let bare = col.rsplit('.').next().unwrap_or(col);
    table_def
        .columns
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(bare))
        .map(|c| c.col_type.clone())
}

/// Canonical form of a numeric join key, so a legacy "007" hashes next to a
/// probe key written as "7".
pub(crate) fn canonicalize_join_key(value: &str, col_type: Option<&ColumnType>) -> String {
    match col_type {
        Some(t) if t.is_numeric() => t.canonicalize(value),
        _ => value.to_string(),
    }
}

/// A legacy numeric primary key may be stored as "007"; the canonical
/// key-range lookup cannot reach it.
pub(crate) fn legacy_pk_needs_scan(table: &TableDef) -> bool {
    table.row_format == RowFormat::Legacy
        && table
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(&table.primary_key))
            .is_some_and(|c| c.col_type.is_numeric())
}

/// `pk = value` for the scan-and-filter fallback, so the comparison runs
/// through the typed predicate path and matches by the column's affinity.
pub(crate) fn pk_equality(table: &TableDef, value: &str) -> WhereExpr {
    WhereExpr::Comparison {
        column: table.primary_key.clone(),
        op: CmpOp::Eq,
        value: SqlValue::Text(value.to_string()),
    }
}

/// Stored form of a primary-key value: canonical for a numeric key, so a
/// lookup for "007" reads the row written as "7".
pub(crate) fn canonicalize_pk_key(table: &TableDef, key_value: &str) -> String {
    let pk_type = table
        .columns
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(&table.primary_key))
        .map(|c| &c.col_type);
    match pk_type {
        Some(t) => t.canonicalize(key_value),
        None => key_value.to_string(),
    }
}

/// Ordering by the literal's type — numeric for a number, lexical for text.
fn cmp_by_literal(row_val: &str, value: &SqlValue, cmp_val: &str) -> std::cmp::Ordering {
    if matches!(value, SqlValue::Integer(_) | SqlValue::Float(_)) {
        smart_cmp(row_val, cmp_val)
    } else {
        row_val.cmp(cmp_val)
    }
}

/// Resolve a column reference to a row value.
///
/// Joins store each side under a `table.column` key as well as the bare
/// name, so a qualified name binds to the table it names even when both
/// sides share the column. On a join row, a qualified name that matches no
/// key is the missing outer-join side and reads as NULL — it must not fall
/// back to the bare key, which belongs to the present table. Only a
/// bare-keyed single-table row falls back. The qualifier is matched
/// case-insensitively because table names are.
///
/// Every read of a qualified name — predicate, projection, ORDER BY, GROUP
/// BY — goes through this, so a column resolves one way everywhere. A stored
/// NULL is `None` in the row and reads as absent.
pub(crate) fn row_lookup<'a>(row: &'a Row, column: &str) -> Option<&'a str> {
    if let Some(Some(v)) = row.get(column) {
        return Some(v.as_str());
    }
    let (qual, bare) = column.split_once('.')?;
    if !row.keys().any(|k| k.contains('.')) {
        return row.get(bare).and_then(|v| v.as_deref());
    }
    row.iter()
        .find(|(k, _)| {
            k.split_once('.')
                .is_some_and(|(q, b)| q.eq_ignore_ascii_case(qual) && b == bare)
        })
        .and_then(|(_, v)| v.as_deref())
}

/// Resolve a predicate column to its declared type, mirroring [`row_lookup`]
/// so a qualified predicate compares by the named column's affinity.
pub(crate) fn type_lookup<'a>(types: &'a ColumnTypeMap, column: &str) -> Option<&'a ColumnType> {
    if let Some(t) = types.get(column) {
        return Some(t);
    }
    let (qual, bare) = column.split_once('.')?;
    if !types.keys().any(|k| k.contains('.')) {
        return types.get(bare);
    }
    types
        .iter()
        .find(|(k, _)| {
            k.split_once('.')
                .is_some_and(|(q, b)| q.eq_ignore_ascii_case(qual) && b == bare)
        })
        .map(|(_, v)| v)
}

/// Evaluate a predicate, preserving SQL three-valued logic: `None` is
/// UNKNOWN, which never selects a row and survives `NOT` unchanged.
fn eval_tri(row: &Row, expr: &WhereExpr, types: &ColumnTypeMap) -> Option<bool> {
    match expr {
        WhereExpr::Comparison { column, op, value } => {
            if matches!(value, SqlValue::Null) {
                return None;
            }
            let row_val = row_lookup(row, column)?;
            let cmp_val = value.as_string();
            let col_type = type_lookup(types, column);
            let ord = cmp_by_affinity(row_val, value, &cmp_val, col_type);
            Some(match op {
                CmpOp::Eq => ord == std::cmp::Ordering::Equal,
                CmpOp::Ne => ord != std::cmp::Ordering::Equal,
                CmpOp::Gt => ord == std::cmp::Ordering::Greater,
                CmpOp::Lt => ord == std::cmp::Ordering::Less,
                CmpOp::Gte => ord != std::cmp::Ordering::Less,
                CmpOp::Lte => ord != std::cmp::Ordering::Greater,
                CmpOp::Like => {
                    let escaped = regex::escape(&cmp_val);
                    let pattern = escaped.replace("%", ".*").replace("_", ".");
                    regex::RegexBuilder::new(&format!("^{}$", pattern))
                        .size_limit(1 << 20)
                        .build()
                        .map(|r| r.is_match(row_val))
                        .unwrap_or(false)
                }
            })
        }
        WhereExpr::And(a, b) => and_tri(eval_tri(row, a, types), eval_tri(row, b, types)),
        WhereExpr::Or(a, b) => or_tri(eval_tri(row, a, types), eval_tri(row, b, types)),
        WhereExpr::Not(inner) => eval_tri(row, inner, types).map(std::ops::Not::not),
        WhereExpr::IsNull(col) => Some(row_lookup(row, col).is_none()),
        WhereExpr::IsNotNull(col) => Some(row_lookup(row, col).is_some()),
        WhereExpr::In(col, vals) => {
            // A NULL element that didn't match makes the result UNKNOWN, not
            // FALSE.
            let row_val = row_lookup(row, col)?;
            let col_type = type_lookup(types, col);
            let mut matched = false;
            let mut saw_null = false;
            for v in vals {
                if matches!(v, SqlValue::Null) {
                    saw_null = true;
                    continue;
                }
                if cmp_by_affinity(row_val, v, &v.as_string(), col_type)
                    == std::cmp::Ordering::Equal
                {
                    matched = true;
                    break;
                }
            }
            match (matched, saw_null) {
                (true, _) => Some(true),
                (false, true) => None,
                (false, false) => Some(false),
            }
        }
        WhereExpr::InSubquery(_, _) => Some(false),
    }
}

fn and_tri(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (None, _) | (_, None) => None,
        _ => Some(true),
    }
}

fn or_tri(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (None, _) | (_, None) => None,
        _ => Some(false),
    }
}

/// Predicate evaluation without the table's column types; the literal's own
/// type picks the ordering.
pub fn eval_where(row: &Row, expr: &WhereExpr) -> bool {
    eval_where_typed(row, expr, &ColumnTypeMap::default())
}

/// Predicate evaluation with the table's declared column types, so ordering
/// follows the column's affinity.
pub fn eval_where_typed(row: &Row, expr: &WhereExpr, types: &ColumnTypeMap) -> bool {
    eval_tri(row, expr, types).unwrap_or(false)
}

/// Integers compare as `i128`, not `f64`: two distinct ids above 2^53 are
/// one `f64` apart and would match the wrong row on `WHERE id = ...`.
/// One part of a composite grouping key.
///
/// The parts are joined with a NUL separator, so a value containing an
/// embedded NUL would blend into its neighbors and two distinct tuples
/// could share one key. Length-prefixing each part makes the split
/// unambiguous, and a NULL gets its own tag so it can never equal a text
/// value that happens to look like the marker.
pub(crate) fn encode_key_part(v: Option<&str>) -> String {
    match v {
        Some(s) => format!("{}:{s}", s.len()),
        None => "\u{2}NULL\u{2}".to_string(),
    }
}

pub(crate) fn smart_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    match (a.parse::<i128>(), b.parse::<i128>()) {
        (Ok(x), Ok(y)) => x.cmp(&y),
        _ => match (a.parse::<f64>(), b.parse::<f64>()) {
            (Ok(x), Ok(y)) if x.is_finite() && y.is_finite() => {
                x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal)
            }
            _ => a.cmp(b),
        },
    }
}

/// MIN/MAX ordering by the column's declared affinity, so a TEXT column
/// orders lexically like it does in WHERE and ORDER BY while a numeric
/// column still orders numerically. An unresolved type keeps the
/// type-agnostic comparison.
pub(crate) fn cmp_by_type(a: &str, b: &str, col_type: Option<&ColumnType>) -> std::cmp::Ordering {
    match col_type {
        Some(t) if t.is_numeric() => smart_cmp(a, b),
        Some(_) => a.cmp(b),
        None => smart_cmp(a, b),
    }
}

/// `sum / n` as a decimal string. A terminating fraction renders in full; a
/// repeating one is rounded half-up at `FRAC_DIGITS` places rather than
/// truncated at an arbitrary fixed point.
fn avg_decimal(sum: i128, n: i128) -> String {
    const FRAC_DIGITS: usize = 15;
    let neg = sum < 0;
    let mut rem = sum.unsigned_abs();
    let n = n.unsigned_abs();
    let int_part = rem / n;
    rem %= n;
    if rem == 0 {
        return format!("{}{int_part}", if neg { "-" } else { "" });
    }
    let mut digits: Vec<u8> = Vec::with_capacity(FRAC_DIGITS);
    while rem != 0 && digits.len() < FRAC_DIGITS {
        rem *= 10;
        digits.push((rem / n) as u8);
        rem %= n;
    }
    // A remainder here means the expansion continues past the kept digits.
    if rem != 0 {
        rem *= 10;
        if rem / n >= 5 {
            for i in (0..FRAC_DIGITS).rev() {
                if digits[i] < 9 {
                    digits[i] += 1;
                    break;
                }
                digits[i] = 0;
                if i == 0 {
                    return format!("{}{}", if neg { "-" } else { "" }, int_part + 1);
                }
            }
        }
    }
    while digits.last() == Some(&0) {
        digits.pop();
    }
    let frac: String = digits.iter().map(|d| (b'0' + d) as char).collect();
    format!("{}{int_part}.{frac}", if neg { "-" } else { "" })
}

pub fn compute_aggregate(
    func: &AggFunc,
    target: &str,
    rows: &[&Row],
    col_type: Option<&ColumnType>,
) -> (String, Option<String>) {
    let name = format!("{}({})", format!("{:?}", func).to_lowercase(), target);
    // COUNT(*) tallies rows; COUNT(col) skips nulls, per the SQL standard.
    let values: Vec<&str> = if target == "*" {
        Vec::new()
    } else {
        rows.iter().filter_map(|r| row_lookup(r, target)).collect()
    };
    match func {
        AggFunc::Count => {
            let n = if target == "*" {
                rows.len()
            } else {
                values.len()
            };
            (name, Some(n.to_string()))
        }
        // SQL: over no non-null values these are NULL, not 0.
        AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max if values.is_empty() => {
            (name, None)
        }
        AggFunc::Sum => {
            if values.iter().all(|v| v.parse::<i64>().is_ok()) {
                // i128, not i64: a full column of i64 values would otherwise
                // overflow the accumulator.
                let sum: i128 = values
                    .iter()
                    .map(|v| v.parse::<i64>().unwrap() as i128)
                    .sum();
                (name, Some(sum.to_string()))
            } else {
                let sum: f64 = values.iter().filter_map(|v| v.parse::<f64>().ok()).sum();
                (name, Some(sum.to_string()))
            }
        }
        AggFunc::Avg => {
            if values.iter().all(|v| v.parse::<i64>().is_ok()) && !values.is_empty() {
                // i128, not i64: a full column of i64 values would otherwise
                // overflow the accumulator.
                let sum: i128 = values
                    .iter()
                    .map(|v| v.parse::<i64>().unwrap() as i128)
                    .sum();
                let n = values.len() as i128;
                (name, Some(avg_decimal(sum, n)))
            } else {
                let avg = values
                    .iter()
                    .filter_map(|v| v.parse::<f64>().ok())
                    .sum::<f64>()
                    / values.len() as f64;
                (name, Some(avg.to_string()))
            }
        }
        AggFunc::Min => {
            // Aggregates skip nulls, so MIN never reports "NULL" as a value.
            let min = values
                .iter()
                .copied()
                .min_by(|a, b| cmp_by_type(a, b, col_type))
                .map(str::to_string);
            (name, min)
        }
        AggFunc::Max => {
            let max = values
                .iter()
                .copied()
                .max_by(|a, b| cmp_by_type(a, b, col_type))
                .map(str::to_string);
            (name, max)
        }
    }
}
