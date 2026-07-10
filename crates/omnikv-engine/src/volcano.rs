//! Volcano Iterator Model — pull-based streaming executor
//!
//! Each operator yields one row at a time via next_row().
//! Uses enum dispatch instead of dyn trait objects to avoid
//! vtable overhead in the hot path.

#![allow(dead_code)]

use crate::OmniKV;
use crate::catalog::{Catalog, TableDef};
use crate::optimizer::*;
use crate::sql::*;
use crate::sql_exec::Row;
use std::collections::HashMap;
use std::sync::Arc;

// ─── Iterator Trait ─────────────────────────────────────────────────────────

pub trait RowIterator {
    fn next_row(&mut self) -> Option<Row>;

    fn reset(&mut self) {}

    fn collect_all(&mut self) -> Vec<Row> {
        let mut rows = Vec::new();
        while let Some(row) = self.next_row() {
            rows.push(row);
        }
        rows
    }
}

// ─── Enum Dispatch ──────────────────────────────────────────────────────────
// All known iterator types wrapped in one enum.
// This lets LLVM see through the dispatch — no vtable, just a match.

pub enum RowIter {
    SeqScan(SeqScanIter),
    PkLookup(PkLookupIter),
    Filter(FilterIter),
    Project(ProjectIter),
    Limit(LimitIter),
    Sort(SortIter),
    HashJoin(HashJoinIter),
    Aggregate(AggregateIter),
}

impl RowIterator for RowIter {
    fn next_row(&mut self) -> Option<Row> {
        match self {
            RowIter::SeqScan(it) => it.next_row(),
            RowIter::PkLookup(it) => it.next_row(),
            RowIter::Filter(it) => it.next_row(),
            RowIter::Project(it) => it.next_row(),
            RowIter::Limit(it) => it.next_row(),
            RowIter::Sort(it) => it.next_row(),
            RowIter::HashJoin(it) => it.next_row(),
            RowIter::Aggregate(it) => it.next_row(),
        }
    }

    fn reset(&mut self) {
        match self {
            RowIter::SeqScan(it) => it.reset(),
            RowIter::PkLookup(it) => it.reset(),
            RowIter::Filter(it) => it.reset(),
            RowIter::Project(it) => it.reset(),
            RowIter::Limit(it) => it.reset(),
            RowIter::Sort(it) => it.reset(),
            RowIter::HashJoin(it) => it.reset(),
            RowIter::Aggregate(it) => it.reset(),
        }
    }
}

// ─── Sequential Scan ────────────────────────────────────────────────────────

pub struct SeqScanIter {
    rows: Vec<Row>,
    pos: usize,
}

impl SeqScanIter {
    pub fn new(db: &Arc<OmniKV>, table: &TableDef) -> Self {
        let prefix = table.row_prefix();
        let seq = db.get_seq();
        let rows = db
            .scan(&prefix, &format!("{}\x7F", prefix), seq)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|(_, value)| serde_json::from_str::<Row>(&value).ok())
            .collect();
        Self { rows, pos: 0 }
    }

    pub fn new_pruned(db: &Arc<OmniKV>, table: &TableDef, needed: &[String]) -> Self {
        let prefix = table.row_prefix();
        let seq = db.get_seq();
        let rows = db
            .scan(&prefix, &format!("{}\x7F", prefix), seq)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|(_, value)| {
                let full: Row = serde_json::from_str(&value).ok()?;
                if needed.is_empty() {
                    return Some(full);
                }
                let pruned: Row = full
                    .into_iter()
                    .filter(|(k, _)| needed.iter().any(|c| c.eq_ignore_ascii_case(k)))
                    .collect();
                Some(pruned)
            })
            .collect();
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
}

// ─── PK Lookup ──────────────────────────────────────────────────────────────

pub struct PkLookupIter {
    row: Option<Row>,
    consumed: bool,
}

impl PkLookupIter {
    pub fn new(db: &Arc<OmniKV>, table: &TableDef, key_value: &str) -> Self {
        let key = format!("{}{}", table.row_prefix(), key_value);
        let end = format!("{}{}\x7F", table.row_prefix(), key_value);
        let seq = db.get_seq();
        let row = db
            .scan(&key, &end, seq)
            .unwrap_or_default()
            .into_iter()
            .next()
            .and_then(|(_, value)| serde_json::from_str::<Row>(&value).ok());
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
}

// ─── Filter ─────────────────────────────────────────────────────────────────

pub struct FilterIter {
    child: Box<RowIter>,
    predicate: WhereExpr,
}

impl FilterIter {
    pub fn new(child: Box<RowIter>, predicate: WhereExpr) -> Self {
        Self { child, predicate }
    }
}

impl RowIterator for FilterIter {
    fn next_row(&mut self) -> Option<Row> {
        loop {
            let row = self.child.next_row()?;
            if eval_where(&row, &self.predicate) {
                return Some(row);
            }
        }
    }
}

// ─── Project ────────────────────────────────────────────────────────────────

pub struct ProjectIter {
    child: Box<RowIter>,
    columns: Vec<SelectColumn>,
}

impl ProjectIter {
    pub fn new(child: Box<RowIter>, columns: Vec<SelectColumn>) -> Self {
        Self { child, columns }
    }
}

impl RowIterator for ProjectIter {
    fn next_row(&mut self) -> Option<Row> {
        let row = self.child.next_row()?;
        if self.columns.iter().any(|c| matches!(c, SelectColumn::Star)) {
            return Some(row);
        }
        let mut projected = Row::new();
        for col in &self.columns {
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
                    let name = format!("{}({})", format!("{:?}", func).to_lowercase(), target);
                    if let Some(v) = row.get(&name) {
                        projected.insert(name, v.clone());
                    }
                }
                _ => {}
            }
        }
        Some(projected)
    }
}

// ─── Limit ──────────────────────────────────────────────────────────────────

pub struct LimitIter {
    child: Box<RowIter>,
    limit: usize,
    emitted: usize,
}

impl LimitIter {
    pub fn new(child: Box<RowIter>, limit: usize) -> Self {
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
}

// ─── Sort ───────────────────────────────────────────────────────────────────
// Has to buffer everything — can't sort a stream without materializing it.

pub struct SortIter {
    sorted: Vec<Row>,
    pos: usize,
}

impl SortIter {
    pub fn new(mut child: Box<RowIter>, order_by: Vec<OrderByItem>) -> Self {
        let mut rows = child.collect_all();
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
}

// ─── Hash Join ──────────────────────────────────────────────────────────────

pub struct HashJoinIter {
    hash_table: HashMap<String, Vec<Row>>,
    probe: Box<RowIter>,
    build_col: String,
    probe_col: String,
    join_type: JoinType,
    current_matches: Vec<Row>,
    match_pos: usize,
    matched_build_keys: std::collections::HashSet<String>,
    right_unmatched: Vec<Row>,
    right_unmatched_pos: usize,
    probe_exhausted: bool,
}

impl HashJoinIter {
    pub fn new(
        mut build: Box<RowIter>,
        probe: Box<RowIter>,
        build_col: String,
        probe_col: String,
        join_type: JoinType,
    ) -> Self {
        let mut hash_table: HashMap<String, Vec<Row>> = HashMap::new();
        while let Some(row) = build.next_row() {
            let key = row.get(&build_col).cloned().unwrap_or_default();
            hash_table.entry(key).or_default().push(row);
        }
        Self {
            hash_table,
            probe,
            build_col,
            probe_col,
            join_type,
            current_matches: Vec::new(),
            match_pos: 0,
            matched_build_keys: std::collections::HashSet::new(),
            right_unmatched: Vec::new(),
            right_unmatched_pos: 0,
            probe_exhausted: false,
        }
    }
}

impl RowIterator for HashJoinIter {
    fn next_row(&mut self) -> Option<Row> {
        loop {
            // drain buffered matches first
            if self.match_pos < self.current_matches.len() {
                let row = self.current_matches[self.match_pos].clone();
                self.match_pos += 1;
                return Some(row);
            }

            // RIGHT JOIN: after probe is done, emit unmatched build rows
            if self.probe_exhausted {
                if self.right_unmatched_pos < self.right_unmatched.len() {
                    let row = self.right_unmatched[self.right_unmatched_pos].clone();
                    self.right_unmatched_pos += 1;
                    return Some(row);
                }
                return None;
            }

            let probe_row = match self.probe.next_row() {
                Some(r) => r,
                None => {
                    self.probe_exhausted = true;
                    if matches!(self.join_type, JoinType::Right) {
                        for (key, rows) in &self.hash_table {
                            if !self.matched_build_keys.contains(key) {
                                self.right_unmatched.extend(rows.iter().cloned());
                            }
                        }
                    }
                    continue;
                }
            };
            let key = probe_row.get(&self.probe_col).cloned().unwrap_or_default();

            match self.hash_table.get(&key) {
                Some(build_rows) => {
                    if matches!(self.join_type, JoinType::Right) {
                        self.matched_build_keys.insert(key.clone());
                    }
                    self.current_matches.clear();
                    self.match_pos = 0;
                    for build_row in build_rows {
                        let mut combined = Row::new();
                        for (k, v) in build_row {
                            combined.insert(k.clone(), v.clone());
                        }
                        for (k, v) in &probe_row {
                            combined.entry(k.clone()).or_insert_with(|| v.clone());
                        }
                        self.current_matches.push(combined);
                    }
                }
                None => match self.join_type {
                    JoinType::Left => {
                        self.current_matches = vec![probe_row];
                        self.match_pos = 0;
                    }
                    _ => continue,
                },
            }
        }
    }
}

// ─── Aggregate ──────────────────────────────────────────────────────────────

pub struct AggregateIter {
    result: Vec<Row>,
    pos: usize,
}

impl AggregateIter {
    pub fn new(
        mut child: Box<RowIter>,
        group_by: Vec<String>,
        agg_columns: Vec<SelectColumn>,
    ) -> Self {
        let all_rows = child.collect_all();

        if group_by.is_empty()
            && agg_columns
                .iter()
                .any(|c| matches!(c, SelectColumn::Aggregate(..)))
        {
            let refs: Vec<&Row> = all_rows.iter().collect();
            let mut row = Row::new();
            for col in &agg_columns {
                if let SelectColumn::Aggregate(func, target) = col {
                    let (name, val) = compute_aggregate(func, target, &refs);
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
                .map(|g| row.get(g).cloned().unwrap_or_default())
                .collect::<Vec<_>>()
                .join("\x00");
            groups.entry(key).or_default().push(row.clone());
        }

        let mut result = Vec::new();
        for (_, group_rows) in &groups {
            let refs: Vec<&Row> = group_rows.iter().collect();
            let mut row = Row::new();
            for col in &agg_columns {
                match col {
                    SelectColumn::Named(name) => {
                        if let Some(val) = refs[0].get(name) {
                            row.insert(name.clone(), val.clone());
                        }
                    }
                    SelectColumn::Aggregate(func, target) => {
                        let (name, val) = compute_aggregate(func, target, &refs);
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
}

// ─── Plan Compiler ──────────────────────────────────────────────────────────

/// Turns a PlanNode tree into a RowIter pipeline.
pub fn compile_plan(plan: &PlanNode, db: &Arc<OmniKV>, catalog: &Arc<Catalog>) -> RowIter {
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
            let base = match access {
                AccessMethod::PkLookup { key_value } => {
                    RowIter::PkLookup(PkLookupIter::new(db, &table_def, key_value))
                }
                AccessMethod::SeqScan | AccessMethod::IndexScan { .. } => {
                    RowIter::SeqScan(SeqScanIter::new(db, &table_def))
                }
            };
            match filter {
                Some(pred) => RowIter::Filter(FilterIter::new(Box::new(base), pred.clone())),
                None => base,
            }
        }
        PlanNode::HashJoin {
            left,
            right,
            join_type,
            on_left_col,
            on_right_col,
            ..
        } => {
            let left_iter = compile_plan(left, db, catalog);
            let right_iter = compile_plan(right, db, catalog);
            RowIter::HashJoin(HashJoinIter::new(
                Box::new(left_iter),
                Box::new(right_iter),
                on_left_col.clone(),
                on_right_col.clone(),
                join_type.clone(),
            ))
        }
        PlanNode::Filter {
            child, predicate, ..
        } => {
            let child_iter = compile_plan(child, db, catalog);
            RowIter::Filter(FilterIter::new(Box::new(child_iter), predicate.clone()))
        }
        PlanNode::Project { child, columns } => {
            let child_iter = compile_plan(child, db, catalog);
            RowIter::Project(ProjectIter::new(Box::new(child_iter), columns.clone()))
        }
        PlanNode::Sort {
            child, order_by, ..
        } => {
            let child_iter = compile_plan(child, db, catalog);
            RowIter::Sort(SortIter::new(Box::new(child_iter), order_by.clone()))
        }
        PlanNode::Limit { child, count } => {
            let child_iter = compile_plan(child, db, catalog);
            RowIter::Limit(LimitIter::new(Box::new(child_iter), *count))
        }
        PlanNode::Aggregate {
            child,
            group_by,
            aggregates,
            ..
        } => {
            let child_iter = compile_plan(child, db, catalog);
            RowIter::Aggregate(AggregateIter::new(
                Box::new(child_iter),
                group_by.clone(),
                aggregates.clone(),
            ))
        }
    }
}

// ─── Helpers ────────────────────────────────────────────────────────────────

pub fn eval_where(row: &Row, expr: &WhereExpr) -> bool {
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
                    let escaped = regex::escape(&cmp_val);
                    let pattern = escaped.replace("%", ".*").replace("_", ".");
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
        WhereExpr::InSubquery(_, _) => false,
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
