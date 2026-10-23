//! Plan-Driven Query Executor
//!
//! Executes queries using the optimizer's physical plan tree.

use crate::OmniKV;
use crate::catalog::ColumnType;
use crate::catalog::{Catalog, RowFormat, TableDef};
use crate::optimizer::{AccessMethod, PlanNode};
use crate::sql::{AggFunc, JoinType, SelectColumn, WhereExpr};
use crate::sql_exec::Row;
use crate::volcano::{
    ColumnTypeMap, eval_where_typed, legacy_pk_needs_scan, qualify_row, row_lookup, type_lookup,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

type ExplainAnalyzeOutput = (Vec<Row>, Vec<(String, NodeStats)>);

/// Execution statistics collected during EXPLAIN ANALYZE.
#[derive(Debug, Clone)]
pub struct NodeStats {
    pub actual_rows: u64,
    pub actual_time_ms: f64,
    pub estimated_rows: u64,
}

/// Plan-driven executor that walks the optimizer's plan tree.
pub struct PlanExecutor {
    pub db: Arc<OmniKV>,
    pub catalog: Arc<Catalog>,
    types: ColumnTypeMap,
    scan_seq: Option<u64>,
}

impl PlanExecutor {
    pub fn new(db: Arc<OmniKV>, catalog: Arc<Catalog>) -> Self {
        Self {
            db,
            catalog,
            types: ColumnTypeMap::new(),
            scan_seq: None,
        }
    }

    pub fn for_plan(db: Arc<OmniKV>, catalog: Arc<Catalog>, plan: &PlanNode) -> Self {
        let mut types = ColumnTypeMap::new();
        collect_plan_types(plan, &catalog, &mut types);
        Self {
            db,
            catalog,
            types,
            scan_seq: None,
        }
    }

    /// Builds a plan executor pinned to the caller's statement snapshot.
    pub fn for_plan_at(
        db: Arc<OmniKV>,
        catalog: Arc<Catalog>,
        plan: &PlanNode,
        scan_seq: u64,
    ) -> Self {
        let mut executor = Self::for_plan(db, catalog, plan);
        executor.scan_seq = Some(scan_seq);
        executor
    }

    fn read_seq(&self) -> u64 {
        self.scan_seq.unwrap_or_else(|| self.db.get_seq())
    }

    /// Execute a plan node, returning rows.
    pub fn execute_plan(&self, plan: &PlanNode) -> Result<Vec<Row>, String> {
        match plan {
            PlanNode::Scan {
                table,
                access,
                filter,
                ..
            } => self.exec_scan(table, access, filter.as_ref()),
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
                let left_rows = self.execute_plan(left)?;
                let right_rows = self.execute_plan(right)?;
                Ok(self.exec_hash_join(
                    &left_rows,
                    &right_rows,
                    on_left_col,
                    on_right_col,
                    join_type,
                    left_table,
                    right_table,
                ))
            }
            PlanNode::Filter {
                child, predicate, ..
            } => {
                let mut rows = self.execute_plan(child)?;
                rows.retain(|row| eval_where_typed(row, predicate, &self.types));
                Ok(rows)
            }
            PlanNode::Project { child, columns } => {
                let rows = self.execute_plan(child)?;
                Ok(self.exec_project(&rows, columns))
            }
            PlanNode::Sort {
                child, order_by, ..
            } => {
                let mut rows = self.execute_plan(child)?;
                let types = child.output_types(&self.catalog);
                crate::volcano::sort_rows(&mut rows, order_by, &types);
                Ok(rows)
            }
            PlanNode::Limit { child, count } => {
                let mut rows = self.execute_plan(child)?;
                rows.truncate(*count);
                Ok(rows)
            }
            PlanNode::Aggregate {
                child,
                group_by,
                aggregates,
                ..
            } => {
                let rows = self.execute_plan(child)?;
                self.exec_aggregate(&rows, group_by, aggregates)
            }
        }
    }

    /// Execute EXPLAIN ANALYZE — run the plan and collect actual stats.
    pub fn explain_analyze(&self, plan: &PlanNode) -> Result<ExplainAnalyzeOutput, String> {
        let mut stats = Vec::new();
        let rows = self.execute_with_stats(plan, &mut stats)?;
        Ok((rows, stats))
    }

    fn execute_with_stats(
        &self,
        plan: &PlanNode,
        stats: &mut Vec<(String, NodeStats)>,
    ) -> Result<Vec<Row>, String> {
        let start = Instant::now();
        let estimated = plan.estimated_rows();

        let (label, rows) = match plan {
            PlanNode::Scan { table, access, .. } => {
                let label = match access {
                    AccessMethod::SeqScan => format!("Seq Scan on {}", table),
                    AccessMethod::IndexScan { index_name, .. } => {
                        format!("Index Scan ({}) on {}", index_name, table)
                    }
                    AccessMethod::PkLookup { key_value } => {
                        format!("PK Lookup on {} (key={})", table, key_value)
                    }
                };
                (label, self.execute_plan(plan)?)
            }
            PlanNode::HashJoin {
                on_left_col,
                on_right_col,
                left,
                right,
                ..
            } => {
                let _ = self.execute_with_stats(left, stats)?;
                let _ = self.execute_with_stats(right, stats)?;
                let label = format!("Hash Join on {} = {}", on_left_col, on_right_col);
                (label, self.execute_plan(plan)?)
            }
            PlanNode::Filter { child, .. } => {
                let _ = self.execute_with_stats(child, stats)?;
                ("Filter".to_string(), self.execute_plan(plan)?)
            }
            PlanNode::Sort {
                child, order_by, ..
            } => {
                let _ = self.execute_with_stats(child, stats)?;
                let keys: Vec<String> = order_by.iter().map(|o| o.column.clone()).collect();
                (
                    format!("Sort [{}]", keys.join(", ")),
                    self.execute_plan(plan)?,
                )
            }
            PlanNode::Aggregate {
                child, group_by, ..
            } => {
                let _ = self.execute_with_stats(child, stats)?;
                (
                    format!("Aggregate [GROUP BY {}]", group_by.join(", ")),
                    self.execute_plan(plan)?,
                )
            }
            PlanNode::Limit { child, count } => {
                let _ = self.execute_with_stats(child, stats)?;
                (format!("Limit {}", count), self.execute_plan(plan)?)
            }
            PlanNode::Project { child, .. } => {
                let _ = self.execute_with_stats(child, stats)?;
                ("Project".to_string(), self.execute_plan(plan)?)
            }
        };

        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        stats.push((
            label,
            NodeStats {
                actual_rows: rows.len() as u64,
                actual_time_ms: elapsed,
                estimated_rows: estimated,
            },
        ));

        Ok(rows)
    }

    // ─── Access Methods ─────────────────────────────────────────────────

    fn exec_scan(
        &self,
        table_name: &str,
        access: &AccessMethod,
        filter: Option<&WhereExpr>,
    ) -> Result<Vec<Row>, String> {
        let table = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| format!("Table '{}' does not exist", table_name))?;

        let mut rows = match access {
            AccessMethod::PkLookup { key_value } if legacy_pk_needs_scan(&table) => {
                // A legacy numeric key may be stored as "007", which the
                // canonical key range can never reach.
                let key_value = crate::volcano::canonicalize_pk_key(&table, key_value);
                let predicate = crate::volcano::pk_equality(&table, &key_value);
                let mut scanned = self.load_table_rows(&table)?;
                scanned.retain(|row| eval_where_typed(row, &predicate, &self.types));
                scanned
            }
            AccessMethod::PkLookup { key_value } => {
                let key_value = crate::volcano::canonicalize_pk_key(&table, key_value);
                let key = format!("{}{}", table.row_prefix(), key_value);
                let end = format!("{}{}\x7F", table.row_prefix(), key_value);
                let seq = self.read_seq();
                self.db
                    .scan(&key, &end, seq)
                    .map_err(|error| {
                        format!(
                            "storage PK lookup failed for table '{}': {error}",
                            table.name
                        )
                    })?
                    .into_iter()
                    .map(|(_, value)| {
                        Self::deserialize_row_checked(&value, table.row_format).map_err(|error| {
                            format!("table '{}' row decode failed: {error}", table.name)
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?
            }
            AccessMethod::IndexScan { .. } | AccessMethod::SeqScan => {
                self.load_table_rows(&table)?
            }
        };

        if let Some(expr) = filter {
            rows.retain(|row| eval_where_typed(row, expr, &self.types));
        }

        Ok(rows)
    }

    /// Map one stored value to a typed cell, per the table's row format:
    /// [`RowFormat`] is the only signal that tells a stored "NULL" from the
    /// user's literal text.
    fn decode(v: Option<String>, format: RowFormat) -> Option<String> {
        match format {
            // Stored "NULL" is SQL NULL; the typed code never writes it.
            RowFormat::Legacy => v.filter(|s| s != "NULL"),
            // Stored "NULL" is the user's literal text; JSON null is NULL.
            RowFormat::Typed => v,
        }
    }

    pub(crate) fn deserialize_row_checked(value: &str, format: RowFormat) -> Result<Row, String> {
        let row: Row = serde_json::from_str(value)
            .map_err(|error| format!("invalid stored row JSON: {error}"))?;
        Ok(row
            .into_iter()
            .map(|(k, v)| (k, Self::decode(v, format)))
            .collect())
    }

    pub fn deserialize_row(value: &str, format: RowFormat) -> Option<Row> {
        Self::deserialize_row_checked(value, format).ok()
    }

    fn load_table_rows(&self, table: &TableDef) -> Result<Vec<Row>, String> {
        let prefix = table.row_prefix();
        let seq = self.read_seq();
        let stored = self
            .db
            .scan(&prefix, &format!("{}\x7F", prefix), seq)
            .map_err(|error| format!("storage scan failed for table '{}': {error}", table.name))?;
        stored
            .into_iter()
            .map(|(_, value)| {
                Self::deserialize_row_checked(&value, table.row_format)
                    .map_err(|error| format!("table '{}' row decode failed: {error}", table.name))
            })
            .collect()
    }

    /// Load only specific columns (column pruning).
    ///
    /// This compatibility wrapper preserves the original infallible public
    /// API. New SQL execution paths should use
    /// [`Self::load_table_rows_pruned_checked`] so storage and decode errors
    /// remain visible to callers.
    pub fn load_table_rows_pruned(&self, table: &TableDef, needed_cols: &[String]) -> Vec<Row> {
        self.load_table_rows_pruned_checked(table, needed_cols)
            .unwrap_or_default()
    }

    /// Fallible column-pruned load used by correctness-sensitive callers.
    pub fn load_table_rows_pruned_checked(
        &self,
        table: &TableDef,
        needed_cols: &[String],
    ) -> Result<Vec<Row>, String> {
        let prefix = table.row_prefix();
        let seq = self.read_seq();
        let stored = self
            .db
            .scan(&prefix, &format!("{}\x7F", prefix), seq)
            .map_err(|error| format!("storage scan failed for table '{}': {error}", table.name))?;
        stored
            .into_iter()
            .map(|(_, value)| {
                let full =
                    Self::deserialize_row_checked(&value, table.row_format).map_err(|error| {
                        format!("table '{}' row decode failed: {error}", table.name)
                    })?;
                if needed_cols.is_empty() {
                    return Ok(full);
                }
                let pruned: Row = full
                    .into_iter()
                    .filter(|(k, _)| needed_cols.iter().any(|c| c.eq_ignore_ascii_case(k)))
                    .collect();
                Ok(pruned)
            })
            .collect()
    }

    // ─── Hash Join ──────────────────────────────────────────────────────

    #[expect(
        clippy::too_many_arguments,
        reason = "Each join side carries its own rows, key column, and table name; grouping them would hide which side a qualifier belongs to."
    )]
    fn exec_hash_join(
        &self,
        build: &[Row],
        probe: &[Row],
        build_col: &str,
        probe_col: &str,
        join_type: &JoinType,
        build_table: &str,
        probe_table: &str,
    ) -> Vec<Row> {
        // Qualify each side's keys with its table name, exactly as the Volcano
        // hash join does, so on an unmatched outer-join row a qualified lookup
        // like right.id binds to the side that is present instead of falling
        // back to the other table's bare id.
        let build_prefix = format!("{build_table}.");
        let probe_prefix = format!("{probe_table}.");
        let build_key_type = crate::volcano::join_key_type(&self.catalog, build_table, build_col);
        let probe_key_type = crate::volcano::join_key_type(&self.catalog, probe_table, probe_col);
        let mut hash_table: HashMap<String, Vec<&Row>> = HashMap::with_capacity(build.len());
        let mut null_key_build_rows: Vec<&Row> = Vec::new();
        for row in build {
            match row.get(build_col).cloned().flatten() {
                Some(key) => {
                    let key = crate::volcano::canonicalize_join_key(&key, build_key_type.as_ref());
                    hash_table.entry(key).or_default().push(row);
                }
                None => null_key_build_rows.push(row),
            }
        }

        let mut result = Vec::new();
        // Matched keys, so the unmatched build rows can be emitted below.
        let mut matched_keys: std::collections::HashSet<String> = std::collections::HashSet::new();
        for probe_row in probe {
            // A NULL probe key matches nothing, but on a LEFT join the probe
            // row is still preserved.
            let key = probe_row
                .get(probe_col)
                .cloned()
                .flatten()
                .map(|k| crate::volcano::canonicalize_join_key(&k, probe_key_type.as_ref()));
            match (key.as_deref().and_then(|k| hash_table.get(k)), join_type) {
                (Some(matches), _) => {
                    if matches!(join_type, JoinType::Right) {
                        matched_keys.insert(key.unwrap());
                    }
                    for build_row in matches {
                        let mut combined = Row::new();
                        for (k, v) in *build_row {
                            combined.insert(k.clone(), v.clone());
                            combined.insert(format!("{build_prefix}{k}"), v.clone());
                        }
                        for (k, v) in probe_row {
                            // Also store it qualified so a shared name isn't dropped.
                            combined.insert(format!("{probe_prefix}{k}"), v.clone());
                            combined.entry(k.clone()).or_insert_with(|| v.clone());
                        }
                        result.push(combined);
                    }
                }
                (None, JoinType::Left) => {
                    result.push(qualify_row(probe_row.clone(), &probe_prefix));
                }
                _ => {}
            }
        }

        // A RIGHT JOIN preserves build rows that no probe row matched,
        // including those whose key is NULL.
        if matches!(join_type, JoinType::Right) {
            for (key, rows) in &hash_table {
                if matched_keys.contains(key) {
                    continue;
                }
                for build_row in rows {
                    result.push(qualify_row((**build_row).clone(), &build_prefix));
                }
            }
            for build_row in &null_key_build_rows {
                result.push(qualify_row((**build_row).clone(), &build_prefix));
            }
        }
        result
    }

    // ─── Aggregate ──────────────────────────────────────────────────────

    fn exec_aggregate(
        &self,
        rows: &[Row],
        group_by: &[String],
        columns: &[SelectColumn],
    ) -> Result<Vec<Row>, String> {
        if group_by.is_empty()
            && columns
                .iter()
                .any(|c| matches!(c, SelectColumn::Aggregate(..)))
        {
            let mut result = Row::new();
            for col in columns {
                if let SelectColumn::Aggregate(func, target) = col {
                    let refs: Vec<&Row> = rows.iter().collect();
                    let (name, val) =
                        compute_aggregate(func, target, &refs, type_lookup(&self.types, target));
                    result.insert(name, val);
                }
            }
            return Ok(vec![result]);
        }

        let mut groups: HashMap<String, Vec<&Row>> = HashMap::new();
        for row in rows {
            let key: String = group_by
                .iter()
                .map(|g| crate::volcano::encode_key_part(row_lookup(row, g)))
                .collect::<Vec<_>>()
                .join("|");
            groups.entry(key).or_default().push(row);
        }

        let mut result = Vec::new();
        for group_rows in groups.values() {
            let mut row = Row::new();
            for col in columns {
                match col {
                    SelectColumn::Named(name) => {
                        row.insert(
                            name.clone(),
                            row_lookup(group_rows[0], name).map(str::to_string),
                        );
                    }
                    SelectColumn::Qualified(t, n) => {
                        let key = format!("{}.{}", t, n);
                        row.insert(
                            key.clone(),
                            row_lookup(group_rows[0], &key).map(str::to_string),
                        );
                        row.entry(n.clone())
                            .or_insert_with(|| row_lookup(group_rows[0], &key).map(str::to_string));
                    }
                    SelectColumn::Aggregate(func, target) => {
                        let (name, val) = compute_aggregate(
                            func,
                            target,
                            group_rows,
                            type_lookup(&self.types, target),
                        );
                        row.insert(name, val);
                    }
                    _ => {}
                }
            }
            result.push(row);
        }
        Ok(result)
    }

    // ─── Project ────────────────────────────────────────────────────────

    fn exec_project(&self, rows: &[Row], columns: &[SelectColumn]) -> Vec<Row> {
        if columns.iter().any(|c| matches!(c, SelectColumn::Star)) {
            return rows
                .iter()
                .map(|row| {
                    let mut projected = row.clone();
                    // A star keeps the full row, but explicit columns beside
                    // it still need validation and must not disappear from
                    // the plan-driven projection.
                    for col in columns {
                        match col {
                            SelectColumn::Named(name) => {
                                if let Some(value) = row_lookup(row, name) {
                                    projected.insert(name.clone(), Some(value.to_string()));
                                }
                            }
                            SelectColumn::Qualified(table, name) => {
                                let key = format!("{table}.{name}");
                                projected
                                    .insert(key.clone(), row_lookup(row, &key).map(str::to_string));
                            }
                            SelectColumn::Aggregate(func, target) => {
                                let key =
                                    format!("{}({})", format!("{:?}", func).to_lowercase(), target);
                                if let Some(value) = row.get(&key) {
                                    projected.insert(key, value.clone());
                                }
                            }
                            SelectColumn::WindowFunc { func, .. } => {
                                let key = match func {
                                    crate::sql::WindowFuncType::RowNumber => "row_number",
                                    crate::sql::WindowFuncType::Rank => "rank",
                                    crate::sql::WindowFuncType::DenseRank => "dense_rank",
                                };
                                if let Some(value) = row.get(key) {
                                    projected.insert(key.to_string(), value.clone());
                                }
                            }
                            SelectColumn::Star => {}
                        }
                    }
                    projected
                })
                .collect();
        }
        rows.iter()
            .map(|row| {
                let mut projected = Row::new();
                for col in columns {
                    match col {
                        SelectColumn::Named(n) => {
                            if let Some(v) = crate::volcano::row_lookup(row, n) {
                                projected.insert(n.clone(), Some(v.to_string()));
                            }
                        }
                        SelectColumn::Qualified(t, n) => {
                            let key = format!("{}.{}", t, n);
                            let val = crate::volcano::row_lookup(row, &key).map(str::to_string);
                            projected.insert(key, val.clone());
                            projected.entry(n.clone()).or_insert(val);
                        }
                        SelectColumn::Aggregate(func, target) => {
                            let name =
                                format!("{}({})", format!("{:?}", func).to_lowercase(), target);
                            if let Some(v) = row.get(&name) {
                                projected.insert(name, v.clone());
                            }
                        }
                        SelectColumn::WindowFunc { order_by: ob, .. } => {
                            // The window's sort key is read after this
                            // projection, so it has to survive it.
                            if let Some(v) = crate::volcano::row_lookup(row, ob) {
                                projected.insert(ob.clone(), Some(v.to_string()));
                            }
                        }
                        _ => {}
                    }
                }
                projected
            })
            .collect()
    }
}

fn collect_plan_types(plan: &PlanNode, catalog: &Catalog, types: &mut ColumnTypeMap) {
    match plan {
        PlanNode::Scan { table, .. } => {
            if let Some(table_def) = catalog.get_table(table) {
                for c in &table_def.columns {
                    types.insert(c.name.clone(), c.col_type.clone());
                    types.insert(format!("{}.{}", table_def.name, c.name), c.col_type.clone());
                }
            }
        }
        PlanNode::HashJoin { left, right, .. } => {
            collect_plan_types(left, catalog, types);
            collect_plan_types(right, catalog, types);
        }
        PlanNode::Filter { child, .. }
        | PlanNode::Project { child, .. }
        | PlanNode::Sort { child, .. }
        | PlanNode::Limit { child, .. }
        | PlanNode::Aggregate { child, .. } => {
            collect_plan_types(child, catalog, types);
        }
    }
}

// ─── Shared helpers ─────────────────────────────────────────────────────────

fn eval_where(row: &Row, expr: &WhereExpr) -> bool {
    crate::volcano::eval_where(row, expr)
}

fn compute_aggregate(
    func: &AggFunc,
    target: &str,
    rows: &[&Row],
    col_type: Option<&ColumnType>,
) -> (String, Option<String>) {
    crate::volcano::compute_aggregate(func, target, rows, col_type)
}

/// LRU plan cache — avoids re-optimizing identical queries.
pub struct PlanCache {
    cache: Mutex<Vec<(String, PlanNode)>>,
    capacity: usize,
}

impl PlanCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: Mutex::new(Vec::with_capacity(capacity)),
            capacity,
        }
    }

    /// Get a cached plan for a query string.
    pub fn get(&self, query: &str) -> Option<PlanNode> {
        let cache = self.cache.lock().ok()?;
        cache
            .iter()
            .find(|(q, _)| q == query)
            .map(|(_, p)| p.clone())
    }

    /// Insert a plan into the cache, evicting oldest if full.
    pub fn put(&self, query: String, plan: PlanNode) {
        if let Ok(mut cache) = self.cache.lock() {
            cache.retain(|(q, _)| q != &query);
            if cache.len() >= self.capacity {
                cache.remove(0);
            }
            cache.push((query, plan));
        }
    }

    /// Invalidate all cached plans (call on DDL changes).
    pub fn invalidate(&self) {
        if let Ok(mut cache) = self.cache.lock() {
            cache.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::RowFormat;

    #[test]
    fn legacy_sentinel_is_null_but_typed_text_is_text() {
        // The two row formats share the exact same byte pattern for a stored
        // "NULL"; only the table's recorded format decides what it means.
        let stored = r#"{"id":"1","v":"NULL"}"#;
        let legacy = PlanExecutor::deserialize_row(stored, RowFormat::Legacy).unwrap();
        assert!(legacy.get("v").is_some_and(|o| o.is_none()));
        let typed = PlanExecutor::deserialize_row(stored, RowFormat::Typed).unwrap();
        assert_eq!(typed.get("v").and_then(|o| o.as_deref()), Some("NULL"));
    }

    #[test]
    fn json_null_is_null_in_both_formats() {
        let stored = r#"{"id":"2","v":null}"#;
        for fmt in [RowFormat::Legacy, RowFormat::Typed] {
            let row = PlanExecutor::deserialize_row(stored, fmt).unwrap();
            assert!(
                row.get("v").is_some_and(|o| o.is_none()),
                "JSON null is NULL"
            );
        }
    }
}
