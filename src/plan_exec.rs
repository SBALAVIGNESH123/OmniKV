//! Plan-Driven Query Executor
//!
//! Executes queries using the optimizer's physical plan tree instead of
//! the old hardcoded scan-filter-sort pipeline.

#![allow(dead_code)]

use crate::OmniKV;
use crate::catalog::{Catalog, TableDef};
use crate::optimizer::*;
use crate::sql::*;
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
                    .filter_map(|(_, value)| serde_json::from_str::<Row>(&value).ok())
                    .collect()
            }
            AccessMethod::IndexScan { .. } | AccessMethod::SeqScan => self.load_table_rows(&table),
        };

        if let Some(expr) = filter {
            rows.retain(|row| eval_where(row, expr));
        }

        Ok(rows)
    }

    fn load_table_rows(&self, table: &TableDef) -> Vec<Row> {
        let prefix = table.row_prefix();
        let seq = self.db.get_seq();
        self.db
            .scan(&prefix, &format!("{}\x7F", prefix), seq)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|(_, value)| serde_json::from_str::<Row>(&value).ok())
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
                let full: Row = serde_json::from_str(&value).ok()?;
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
        for row in build {
            let key = row.get(build_col).cloned().unwrap_or_default();
            hash_table.entry(key).or_default().push(row);
        }

        let mut result = Vec::new();
        for probe_row in probe {
            let key = probe_row.get(probe_col).cloned().unwrap_or_default();
            match (hash_table.get(&key), join_type) {
                (Some(matches), _) => {
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
        result
    }

    // ─── Sort ───────────────────────────────────────────────────────────

    fn exec_sort(&self, rows: &mut [Row], order_by: &[OrderByItem]) {
        for item in order_by.iter().rev() {
            let col = item.column.clone();
            let desc = item.desc;
            rows.sort_by(|a, b| {
                let va = a.get(&col).cloned().unwrap_or_default();
                let vb = b.get(&col).cloned().unwrap_or_default();
                let cmp = smart_cmp(&va, &vb);
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
                    result.insert(name, val);
                }
            }
            return Ok(vec![result]);
        }

        let mut groups: HashMap<String, Vec<&Row>> = HashMap::new();
        for row in rows {
            let key: String = group_by
                .iter()
                .map(|g| row.get(g).cloned().unwrap_or_default())
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
            return rows.to_vec();
        }
        rows.iter()
            .map(|row| {
                let mut projected = Row::new();
                for col in columns {
                    match col {
                        SelectColumn::Named(n) => {
                            if let Some(v) = row.get(n) {
                                projected.insert(n.clone(), v.clone());
                            }
                        }
                        SelectColumn::Qualified(t, n) => {
                            let key = format!("{}.{}", t, n);
                            let val = row
                                .get(&key)
                                .or_else(|| row.get(n))
                                .cloned()
                                .unwrap_or_default();
                            projected.insert(n.clone(), val);
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
    match expr {
        WhereExpr::Comparison { column, op, value } => {
            let row_val = row.get(column).cloned().unwrap_or_default();
            let cmp_val = value.as_string();
            match op {
                CmpOp::Eq => row_val == cmp_val,
                CmpOp::Ne => row_val != cmp_val,
                CmpOp::Gt => smart_cmp(&row_val, &cmp_val) == std::cmp::Ordering::Greater,
                CmpOp::Lt => smart_cmp(&row_val, &cmp_val) == std::cmp::Ordering::Less,
                CmpOp::Gte => smart_cmp(&row_val, &cmp_val) != std::cmp::Ordering::Less,
                CmpOp::Lte => smart_cmp(&row_val, &cmp_val) != std::cmp::Ordering::Greater,
                CmpOp::Like => {
                    let pattern = cmp_val.replace('%', ".*").replace('_', ".");
                    regex::Regex::new(&format!("^{}$", pattern))
                        .map(|r| r.is_match(&row_val))
                        .unwrap_or(false)
                }
            }
        }
        WhereExpr::And(a, b) => eval_where(row, a) && eval_where(row, b),
        WhereExpr::Or(a, b) => eval_where(row, a) || eval_where(row, b),
        WhereExpr::Not(inner) => !eval_where(row, inner),
        WhereExpr::IsNull(col) => row
            .get(col)
            .map(|v| v == "NULL" || v.is_empty())
            .unwrap_or(true),
        WhereExpr::IsNotNull(col) => row
            .get(col)
            .map(|v| v != "NULL" && !v.is_empty())
            .unwrap_or(false),
        WhereExpr::In(col, vals) => {
            let row_val = row.get(col).cloned().unwrap_or_default();
            vals.iter().any(|v| v.as_string() == row_val)
        }
        WhereExpr::InSubquery(_, _) => true,
    }
}

fn smart_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    if let (Ok(ai), Ok(bi)) = (a.parse::<f64>(), b.parse::<f64>()) {
        ai.partial_cmp(&bi).unwrap_or(std::cmp::Ordering::Equal)
    } else {
        a.cmp(b)
    }
}

fn compute_aggregate(func: &AggFunc, target: &str, rows: &[&Row]) -> (String, String) {
    let name = format!("{}({})", format!("{:?}", func).to_lowercase(), target);
    match func {
        AggFunc::Count => (name, rows.len().to_string()),
        AggFunc::Sum => {
            let sum: f64 = rows
                .iter()
                .filter_map(|r| r.get(target).and_then(|v| v.parse::<f64>().ok()))
                .sum();
            (name, sum.to_string())
        }
        AggFunc::Avg => {
            let vals: Vec<f64> = rows
                .iter()
                .filter_map(|r| r.get(target).and_then(|v| v.parse::<f64>().ok()))
                .collect();
            let avg = if vals.is_empty() {
                0.0
            } else {
                vals.iter().sum::<f64>() / vals.len() as f64
            };
            (name, format!("{:.2}", avg))
        }
        AggFunc::Min => {
            let min = rows
                .iter()
                .filter_map(|r| r.get(target))
                .min_by(|a, b| smart_cmp(a, b))
                .cloned()
                .unwrap_or_default();
            (name, min)
        }
        AggFunc::Max => {
            let max = rows
                .iter()
                .filter_map(|r| r.get(target))
                .max_by(|a, b| smart_cmp(a, b))
                .cloned()
                .unwrap_or_default();
            (name, max)
        }
    }
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
