//! Plan-Driven Query Executor
//!
//! Executes queries using the optimizer's physical plan tree.

use crate::OmniKV;
use crate::catalog::{Catalog, RowFormat, TableDef};
use crate::optimizer::{AccessMethod, PlanNode};
use crate::sql::{AggFunc, JoinType, OrderByItem, SelectColumn, WhereExpr};
use crate::sql_exec::Row;
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
}

impl PlanExecutor {
    pub fn new(db: Arc<OmniKV>, catalog: Arc<Catalog>) -> Self {
        Self { db, catalog }
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
                ))
            }
            PlanNode::Filter {
                child, predicate, ..
            } => {
                let mut rows = self.execute_plan(child)?;
                rows.retain(|row| eval_where(row, predicate));
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
                self.exec_sort(&mut rows, order_by);
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
            .ok_or_else(|| format!("Table '{}' not found", table_name))?;

        let mut rows = match access {
            AccessMethod::PkLookup { key_value } => {
                let key = format!("{}{}", table.row_prefix(), key_value);
                let end = format!("{}{}\x7F", table.row_prefix(), key_value);
                let seq = self.db.get_seq();
                self.db
                    .scan(&key, &end, seq)
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|(_, value)| Self::deserialize_row(&value, table.row_format))
                    .collect()
            }
            AccessMethod::IndexScan { .. } | AccessMethod::SeqScan => self.load_table_rows(&table),
        };

        if let Some(expr) = filter {
            rows.retain(|row| eval_where(row, expr));
        }

        Ok(rows)
    }

    /// Deserialize a stored row.
    ///
    /// On a [`Legacy`](crate::catalog::RowFormat::Legacy) table a stored
    /// "NULL" is the pre-typed-Row encoding of SQL NULL and becomes `None`;
    /// on a [`Typed`](crate::catalog::RowFormat::Typed) table it is the
    /// user's literal text and is kept. The two byte patterns are identical,
    /// so the value alone cannot distinguish them — the table's recorded
    /// format is the only signal.
    /// Map one stored value to a typed cell, per the table's row format.
    fn decode(v: Option<String>, format: RowFormat) -> Option<String> {
        match format {
            // Stored "NULL" is SQL NULL; the typed code never writes it.
            RowFormat::Legacy => v.filter(|s| s != "NULL"),
            // Stored "NULL" is the user's literal text; JSON null is NULL.
            RowFormat::Typed => v,
        }
    }

    pub fn deserialize_row(value: &str, format: RowFormat) -> Option<Row> {
        let row: Row = serde_json::from_str(value).ok()?;
        Some(
            row.into_iter()
                .map(|(k, v)| (k, Self::decode(v, format)))
                .collect(),
        )
    }

    fn load_table_rows(&self, table: &TableDef) -> Vec<Row> {
        let prefix = table.row_prefix();
        let seq = self.db.get_seq();
        self.db
            .scan(&prefix, &format!("{}\x7F", prefix), seq)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|(_, value)| Self::deserialize_row(&value, table.row_format))
            .collect()
    }

    /// Load only specific columns (column pruning).
    pub fn load_table_rows_pruned(&self, table: &TableDef, needed_cols: &[String]) -> Vec<Row> {
        let prefix = table.row_prefix();
        let seq = self.db.get_seq();
        self.db
            .scan(&prefix, &format!("{}\x7F", prefix), seq)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|(_, value)| {
                let full: Row = Self::deserialize_row(&value, table.row_format)?;
                if needed_cols.is_empty() {
                    return Some(full);
                }
                let pruned: Row = full
                    .into_iter()
                    .filter(|(k, _)| needed_cols.iter().any(|c| c.eq_ignore_ascii_case(k)))
                    .collect();
                Some(pruned)
            })
            .collect()
    }

    // ─── Hash Join ──────────────────────────────────────────────────────

    fn exec_hash_join(
        &self,
        build: &[Row],
        probe: &[Row],
        build_col: &str,
        probe_col: &str,
        join_type: &JoinType,
    ) -> Vec<Row> {
        let mut hash_table: HashMap<String, Vec<&Row>> = HashMap::with_capacity(build.len());
        let mut null_key_build_rows: Vec<&Row> = Vec::new();
        for row in build {
            match row.get(build_col).cloned().flatten() {
                Some(key) => {
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
            let key = probe_row.get(probe_col).cloned().flatten();
            match (key.as_deref().and_then(|k| hash_table.get(k)), join_type) {
                (Some(matches), _) => {
                    if matches!(join_type, JoinType::Right) {
                        matched_keys.insert(key.unwrap());
                    }
                    for build_row in matches {
                        let mut combined = Row::new();
                        for (k, v) in *build_row {
                            combined.insert(k.clone(), v.clone());
                        }
                        for (k, v) in probe_row {
                            combined.entry(k.clone()).or_insert_with(|| v.clone());
                        }
                        result.push(combined);
                    }
                }
                (None, JoinType::Left) => {
                    result.push(probe_row.clone());
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
                    result.push((**build_row).clone());
                }
            }
            for build_row in &null_key_build_rows {
                result.push((**build_row).clone());
            }
        }
        result
    }

    // ─── Sort ───────────────────────────────────────────────────────────

    fn exec_sort(&self, rows: &mut [Row], order_by: &[OrderByItem]) {
        for item in order_by.iter().rev() {
            let col = item.column.clone();
            let desc = item.desc;
            rows.sort_by(|a, b| {
                let va = a.get(&col).cloned().flatten();
                let vb = b.get(&col).cloned().flatten();
                let cmp = match (va.as_deref(), vb.as_deref()) {
                    (Some(x), Some(y)) => smart_cmp(x, y),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                };
                if desc { cmp.reverse() } else { cmp }
            });
        }
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
                    let (name, val) = compute_aggregate(func, target, &refs);
                    result.insert(name, Some(val));
                }
            }
            return Ok(vec![result]);
        }

        let mut groups: HashMap<String, Vec<&Row>> = HashMap::new();
        for row in rows {
            let key: String = group_by
                .iter()
                .map(|g| {
                    let part = row
                        .get(g)
                        .cloned()
                        .flatten()
                        .unwrap_or_else(|| "\x01NULL\x01".to_string());
                    format!("{}:{part}", part.len())
                })
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
                        if let Some(val) = group_rows[0].get(name) {
                            row.insert(name.clone(), val.clone());
                        }
                    }
                    SelectColumn::Aggregate(func, target) => {
                        let (name, val) = compute_aggregate(func, target, group_rows);
                        row.insert(name, Some(val));
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
            return rows.to_vec();
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
                        _ => {}
                    }
                }
                projected
            })
            .collect()
    }
}

// ─── Shared helpers ─────────────────────────────────────────────────────────

fn eval_where(row: &Row, expr: &WhereExpr) -> bool {
    crate::volcano::eval_where(row, expr)
}

fn smart_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    if let (Ok(ai), Ok(bi)) = (a.parse::<f64>(), b.parse::<f64>()) {
        ai.partial_cmp(&bi).unwrap_or(std::cmp::Ordering::Equal)
    } else {
        a.cmp(b)
    }
}

fn compute_aggregate(func: &AggFunc, target: &str, rows: &[&Row]) -> (String, String) {
    crate::volcano::compute_aggregate(func, target, rows)
}

// ─── Plan Cache (LRU) ───────────────────────────────────────────────────

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
