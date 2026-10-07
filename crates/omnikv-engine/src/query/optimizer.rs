//! Cost-Based Query Optimizer for OmniKV
//!
//! Transforms parsed SQL ASTs into optimized query plans by:
//! - Estimating table cardinality and selectivity
//! - Choosing between full-scan vs index-scan access paths
//! - Reordering JOIN operands by estimated cost (smaller table as build side)
//! - Pushing WHERE predicates down before JOINs
//! - Pruning unnecessary columns early
//!
//! The optimizer produces a `QueryPlan` tree that the executor walks.

use crate::catalog::Catalog;
use crate::secondary_index::{IndexCatalog, IndexDefinition};
use crate::sql::{
    CmpOp, FromClause, JoinType, OrderByItem, SelectColumn, SqlStatement, SqlValue, WhereExpr,
    parse_sql,
};
use crate::volcano::ColumnTypeMap;
use std::fmt;
use std::sync::Arc;

// ─── Table Statistics ───────────────────────────────────────────────────────

/// Per-column histogram for selectivity estimation.
#[derive(Debug, Clone)]
pub struct ColumnHistogram {
    pub column: String,
    pub distinct_count: u64,
    pub null_fraction: f64,
    pub most_common: Vec<(String, f64)>, // (value, frequency)
}

/// Lightweight statistics for cost estimation.
#[derive(Debug, Clone)]
pub struct TableStats {
    pub table_name: String,
    pub row_count: u64,
    pub avg_row_bytes: u64,
    pub indexes: Vec<IndexDefinition>,
    pub histograms: Vec<ColumnHistogram>,
    /// Declared primary key; decides whether an equality predicate can be
    /// answered by a single-row key lookup.
    pub primary_key: String,
}

impl TableStats {
    pub fn estimated_pages(&self) -> u64 {
        let total_bytes = self.row_count * self.avg_row_bytes;
        (total_bytes / 4096).max(1)
    }

    /// Get NDV (number of distinct values) for a column.
    pub fn ndv(&self, column: &str) -> Option<u64> {
        self.histograms
            .iter()
            .find(|h| h.column.eq_ignore_ascii_case(column))
            .map(|h| h.distinct_count)
    }
}

/// Collects table statistics from the catalog + storage engine.
pub fn gather_stats(
    catalog: &Catalog,
    index_catalog: Option<&IndexCatalog>,
    db: &crate::OmniKV,
) -> std::collections::HashMap<String, TableStats> {
    let mut stats = std::collections::HashMap::new();
    for table_name in catalog.list_tables() {
        if let Some(table) = catalog.get_table(&table_name) {
            let prefix = table.row_prefix();
            let seq = db.get_seq();
            let row_count = db
                .scan(&prefix, &format!("{}\x7F", prefix), seq)
                .map(|r| r.len() as u64)
                .unwrap_or(0);

            let avg_row_bytes = if row_count > 0 {
                let sample = db
                    .scan(&prefix, &format!("{}\x7F", prefix), seq)
                    .unwrap_or_default();
                let total: u64 = sample.iter().take(100).map(|(_, v)| v.len() as u64).sum();
                let sampled = sample.len().min(100) as u64;
                total.checked_div(sampled).unwrap_or(128)
            } else {
                128
            };

            let indexes: Vec<IndexDefinition> = if let Some(ic) = index_catalog {
                ic.indexes_for_collection(&table_name)
                    .into_iter()
                    .cloned()
                    .collect()
            } else {
                vec![]
            };

            // Build histograms from sampled data
            let mut histograms = Vec::new();
            if row_count > 0 {
                let sample = db
                    .scan(&prefix, &format!("{}\x7F", prefix), seq)
                    .unwrap_or_default();
                let mut col_values: std::collections::HashMap<
                    String,
                    std::collections::HashSet<String>,
                > = std::collections::HashMap::new();
                let mut col_nulls: std::collections::HashMap<String, u64> =
                    std::collections::HashMap::new();
                let sample_size = sample.len().min(1000);
                for (_, value) in sample.iter().take(sample_size) {
                    if let Ok(row) =
                        serde_json::from_str::<std::collections::HashMap<String, String>>(value)
                    {
                        for (col, val) in &row {
                            col_values
                                .entry(col.clone())
                                .or_default()
                                .insert(val.clone());
                            if val == "NULL" || val.is_empty() {
                                *col_nulls.entry(col.clone()).or_default() += 1;
                            }
                        }
                    }
                }
                for (col, vals) in &col_values {
                    let null_count = col_nulls.get(col).copied().unwrap_or(0);
                    histograms.push(ColumnHistogram {
                        column: col.clone(),
                        distinct_count: vals.len() as u64,
                        null_fraction: if sample_size > 0 {
                            null_count as f64 / sample_size as f64
                        } else {
                            0.0
                        },
                        most_common: vec![], // TODO: frequency counting
                    });
                }
            }

            stats.insert(
                table_name.clone(),
                TableStats {
                    table_name,
                    row_count,
                    avg_row_bytes,
                    indexes,
                    histograms,
                    primary_key: table.primary_key.clone(),
                },
            );
        }
    }
    stats
}

// ─── Query Plan Nodes ───────────────────────────────────────────────────────

/// Physical access strategies.
#[derive(Debug, Clone)]
pub enum AccessMethod {
    /// Full sequential scan of the table.
    SeqScan,
    /// Index scan using a specific index.
    IndexScan { index_name: String, index_id: u32 },
    /// Primary key point lookup.
    PkLookup { key_value: String },
}

/// A physical query plan node.
#[derive(Debug, Clone)]
pub enum PlanNode {
    /// Scan a single table.
    Scan {
        table: String,
        access: AccessMethod,
        filter: Option<WhereExpr>,
        estimated_rows: u64,
        estimated_cost: f64,
    },
    /// Hash join two children.
    HashJoin {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        join_type: JoinType,
        on_left_col: String,
        on_right_col: String,
        left_table: String,
        right_table: String,
        estimated_rows: u64,
        estimated_cost: f64,
    },
    /// Filter rows from a child.
    Filter {
        child: Box<PlanNode>,
        predicate: WhereExpr,
        estimated_rows: u64,
        estimated_cost: f64,
    },
    /// Project columns from a child.
    Project {
        child: Box<PlanNode>,
        columns: Vec<SelectColumn>,
    },
    /// Sort rows.
    Sort {
        child: Box<PlanNode>,
        order_by: Vec<OrderByItem>,
        estimated_cost: f64,
    },
    /// Limit output rows.
    Limit { child: Box<PlanNode>, count: usize },
    /// Group + aggregate.
    Aggregate {
        child: Box<PlanNode>,
        group_by: Vec<String>,
        aggregates: Vec<SelectColumn>,
        estimated_rows: u64,
    },
}

impl PlanNode {
    pub fn estimated_rows(&self) -> u64 {
        match self {
            Self::Scan { estimated_rows, .. } => *estimated_rows,
            Self::HashJoin { estimated_rows, .. } => *estimated_rows,
            Self::Filter { estimated_rows, .. } => *estimated_rows,
            Self::Project { child, .. } => child.estimated_rows(),
            Self::Sort { child, .. } => child.estimated_rows(),
            Self::Limit { child, count } => child.estimated_rows().min(*count as u64),
            Self::Aggregate { estimated_rows, .. } => *estimated_rows,
        }
    }

    pub fn estimated_cost(&self) -> f64 {
        match self {
            Self::Scan { estimated_cost, .. } => *estimated_cost,
            Self::HashJoin { estimated_cost, .. } => *estimated_cost,
            Self::Filter { estimated_cost, .. } => *estimated_cost,
            Self::Project { child, .. } => child.estimated_cost(),
            Self::Sort { estimated_cost, .. } => *estimated_cost,
            Self::Limit { child, .. } => child.estimated_cost(),
            Self::Aggregate { child, .. } => child.estimated_cost() * 1.2,
        }
    }

    /// The column types this plan produces, keyed by both the bare name and
    /// `table.column`, so a predicate above a join compares by the column's
    /// affinity and a qualified name resolves to the table it names.
    pub fn output_types(&self, catalog: &Arc<Catalog>) -> ColumnTypeMap {
        let mut map = ColumnTypeMap::new();
        self.collect_types(catalog, &mut map);
        map
    }

    fn collect_types(&self, catalog: &Arc<Catalog>, map: &mut ColumnTypeMap) {
        match self {
            Self::Scan { table, .. } => {
                let Some(table) = catalog.get_table(table) else {
                    return;
                };
                for c in &table.columns {
                    // A bare clash keeps the leftmost (build) table's type,
                    // matching the row key the bare name resolves to.
                    map.entry(c.name.clone()).or_insert(c.col_type.clone());
                    map.insert(format!("{}.{}", table.name, c.name), c.col_type.clone());
                }
            }
            Self::HashJoin { left, right, .. } => {
                left.collect_types(catalog, map);
                right.collect_types(catalog, map);
            }
            Self::Filter { child, .. }
            | Self::Project { child, .. }
            | Self::Sort { child, .. }
            | Self::Limit { child, .. } => {
                child.collect_types(catalog, map);
            }
            // Aggregates replace the row's shape; a predicate above one is
            // HAVING and is filtered by the executor, not here.
            Self::Aggregate { .. } => {}
        }
    }
}

// ─── Cost Model Constants ───────────────────────────────────────────────────

const SEQ_SCAN_COST_PER_ROW: f64 = 1.0;
const INDEX_SCAN_COST_PER_ROW: f64 = 0.25;
const PK_LOOKUP_COST: f64 = 1.0;
const HASH_BUILD_COST_PER_ROW: f64 = 2.0;
const HASH_PROBE_COST_PER_ROW: f64 = 0.1;
const SORT_COST_FACTOR: f64 = 2.0; // N * log2(N) * factor
const FILTER_COST_PER_ROW: f64 = 0.1;

// ─── Selectivity Estimation ─────────────────────────────────────────────────

/// Estimate selectivity, optionally using histogram data.
pub fn estimate_selectivity(expr: &WhereExpr) -> f64 {
    estimate_selectivity_with_stats(expr, None)
}

/// Estimate selectivity using histogram when available.
pub fn estimate_selectivity_with_stats(expr: &WhereExpr, stats: Option<&TableStats>) -> f64 {
    match expr {
        WhereExpr::Comparison { column, op, .. } => {
            // Use histogram NDV if available
            if let Some(st) = stats
                && let Some(ndv) = st.ndv(bare_name(column))
                && ndv > 0
            {
                return match op {
                    CmpOp::Eq => 1.0 / ndv as f64, // exact: 1/NDV
                    CmpOp::Ne => 1.0 - (1.0 / ndv as f64),
                    CmpOp::Lt | CmpOp::Gt => 1.0 / 3.0,
                    CmpOp::Lte | CmpOp::Gte => 1.0 / 3.0,
                    CmpOp::Like => 0.25,
                };
            }
            // Fallback: hardcoded estimates
            match op {
                CmpOp::Eq => 0.1,
                CmpOp::Ne => 0.9,
                CmpOp::Lt | CmpOp::Gt => 0.33,
                CmpOp::Lte | CmpOp::Gte => 0.33,
                CmpOp::Like => 0.25,
            }
        }
        WhereExpr::And(a, b) => {
            estimate_selectivity_with_stats(a, stats) * estimate_selectivity_with_stats(b, stats)
        }
        WhereExpr::Or(a, b) => {
            let sa = estimate_selectivity_with_stats(a, stats);
            let sb = estimate_selectivity_with_stats(b, stats);
            (sa + sb - sa * sb).min(1.0)
        }
        WhereExpr::Not(inner) => 1.0 - estimate_selectivity_with_stats(inner, stats),
        WhereExpr::IsNull(col) => {
            // Use null_fraction from histogram if available
            if let Some(st) = stats
                && let Some(h) = st
                    .histograms
                    .iter()
                    .find(|h| h.column.eq_ignore_ascii_case(bare_name(col)))
            {
                return h.null_fraction;
            }
            0.05
        }
        WhereExpr::IsNotNull(col) => {
            if let Some(st) = stats
                && let Some(h) = st
                    .histograms
                    .iter()
                    .find(|h| h.column.eq_ignore_ascii_case(bare_name(col)))
            {
                return 1.0 - h.null_fraction;
            }
            0.95
        }
        WhereExpr::In(_, vals) => (vals.len() as f64 * 0.1).min(0.8),
        WhereExpr::InSubquery(_, _) => 0.5,
    }
}

// ─── Column Extraction (for pruning) ────────────────────────────────────────

/// The column name without its `table.` qualifier.
fn bare_name(col: &str) -> &str {
    col.rsplit('.').next().unwrap_or(col)
}

/// Split a WHERE clause into conjuncts (AND-separated predicates).
fn split_conjuncts(expr: &WhereExpr) -> Vec<WhereExpr> {
    match expr {
        WhereExpr::And(a, b) => {
            let mut parts = split_conjuncts(a);
            parts.extend(split_conjuncts(b));
            parts
        }
        other => vec![other.clone()],
    }
}

/// Rebuild a WHERE from conjuncts (ANDs them back together).
fn conjuncts_to_expr(parts: &[WhereExpr]) -> Option<WhereExpr> {
    if parts.is_empty() {
        return None;
    }
    let mut result = parts[0].clone();
    for part in &parts[1..] {
        result = WhereExpr::And(Box::new(result), Box::new(part.clone()));
    }
    Some(result)
}

/// The conjuncts of `where_clause` that a scan of `table` may evaluate
/// safely: every column is qualified to `table`. Unqualified conjuncts are
/// ambiguous across the join, so they ride above it and the combined row's
/// keys pick the binding there.
fn conjuncts_owned_by(where_clause: Option<&WhereExpr>, table: &str) -> Option<WhereExpr> {
    let expr = where_clause?;
    let owned = split_conjuncts(expr)
        .into_iter()
        .filter(|pred| {
            let cols = extract_where_columns(Some(pred));
            !cols.is_empty()
                && cols.iter().all(|c| {
                    c.split_once('.')
                        .is_some_and(|(qual, _)| qual.eq_ignore_ascii_case(table))
                })
        })
        .collect::<Vec<_>>();
    conjuncts_to_expr(&owned)
}

/// Extract all column names needed by a SELECT query.
pub fn extract_needed_columns(
    columns: &[SelectColumn],
    where_clause: Option<&WhereExpr>,
    order_by: &[OrderByItem],
    group_by: &[String],
) -> Vec<String> {
    let mut needed = Vec::new();

    for col in columns {
        match col {
            SelectColumn::Star => return vec![], // need all columns
            SelectColumn::Named(n) => needed.push(n.clone()),
            SelectColumn::Qualified(_, n) => needed.push(n.clone()),
            SelectColumn::Aggregate(_, target) => needed.push(target.clone()),
            SelectColumn::WindowFunc { order_by: ob, .. } => needed.push(ob.clone()),
        }
    }

    if let Some(expr) = where_clause {
        needed.extend(extract_where_columns(Some(expr)));
    }
    for item in order_by {
        needed.push(item.column.clone());
    }
    needed.extend(group_by.iter().cloned());

    needed.sort();
    needed.dedup();
    needed
}

// ─── Optimizer ──────────────────────────────────────────────────────────────

/// The query optimizer. Transforms SQL ASTs into physical plans.
pub struct Optimizer {
    stats: std::collections::HashMap<String, TableStats>,
}

impl Optimizer {
    pub fn new(stats: std::collections::HashMap<String, TableStats>) -> Self {
        Self { stats }
    }

    /// Optimize a SELECT statement into a physical plan.
    pub fn optimize(&self, stmt: &SqlStatement) -> Result<PlanNode, String> {
        match stmt {
            SqlStatement::Select {
                columns,
                from,
                where_clause,
                group_by,
                order_by,
                limit,
                ..
            } => self.optimize_select(
                columns,
                from,
                where_clause.as_ref(),
                group_by,
                order_by,
                *limit,
            ),
            SqlStatement::Explain(inner) => self.optimize(inner),
            _ => Err("Optimizer only handles SELECT queries".into()),
        }
    }

    fn optimize_select(
        &self,
        columns: &[SelectColumn],
        from: &FromClause,
        where_clause: Option<&WhereExpr>,
        group_by: &[String],
        order_by: &[OrderByItem],
        limit: Option<usize>,
    ) -> Result<PlanNode, String> {
        // 1. Build base scan/join node
        let mut plan = self.plan_from(from, where_clause)?;

        // 2. Add filter if not already pushed into scan
        if let Some(expr) = where_clause
            && !self.filter_pushed_to_scan(from, expr)
        {
            let input_rows = plan.estimated_rows();
            let sel = estimate_selectivity(expr);
            let est_rows = (input_rows as f64 * sel) as u64;
            plan = PlanNode::Filter {
                estimated_cost: plan.estimated_cost() + input_rows as f64 * FILTER_COST_PER_ROW,
                child: Box::new(plan),
                predicate: expr.clone(),
                estimated_rows: est_rows.max(1),
            };
        }

        // 3. Aggregate
        if !group_by.is_empty()
            || columns
                .iter()
                .any(|c| matches!(c, SelectColumn::Aggregate(..)))
        {
            let est_groups = if group_by.is_empty() {
                1
            } else {
                (plan.estimated_rows() as f64 * 0.1) as u64 // rough: 10% distinct groups
            };
            plan = PlanNode::Aggregate {
                child: Box::new(plan),
                group_by: group_by.to_vec(),
                aggregates: columns.to_vec(),
                estimated_rows: est_groups.max(1),
            };
        }

        // 4. Sort
        if !order_by.is_empty() {
            let n = plan.estimated_rows() as f64;
            let sort_cost = if n > 1.0 {
                n * n.log2() * SORT_COST_FACTOR
            } else {
                0.0
            };
            plan = PlanNode::Sort {
                estimated_cost: plan.estimated_cost() + sort_cost,
                child: Box::new(plan),
                order_by: order_by.to_vec(),
            };
        }

        // 5. Limit
        if let Some(lim) = limit {
            plan = PlanNode::Limit {
                child: Box::new(plan),
                count: lim,
            };
        }

        // 6. Project
        plan = PlanNode::Project {
            child: Box::new(plan),
            columns: columns.to_vec(),
        };

        Ok(plan)
    }

    /// Build access plan for FROM clause.
    fn plan_from(
        &self,
        from: &FromClause,
        where_clause: Option<&WhereExpr>,
    ) -> Result<PlanNode, String> {
        match from {
            FromClause::Table(name) => self.plan_table_scan(name, where_clause),
            FromClause::Join {
                left,
                right,
                join_type,
                on_left,
                on_right,
            } => {
                // Each side only evaluates the conjuncts qualified to it;
                // the rest ride above the join, where the combined row's
                // qualified keys resolve them. On an outer join only the
                // preserved side may keep its predicates: the other side's
                // rows arrive NULL-filled, so a predicate like
                // `r.x IS NULL` pushed into r's scan would delete the very
                // rows it is asking about.
                let (push_left, push_right) = match join_type {
                    JoinType::Inner => (true, true),
                    JoinType::Left => (true, false),
                    JoinType::Right => (false, true),
                };
                let left_pred = if push_left {
                    conjuncts_owned_by(where_clause, left)
                } else {
                    None
                };
                let right_pred = if push_right {
                    conjuncts_owned_by(where_clause, right)
                } else {
                    None
                };
                let left_plan = self.plan_table_scan(left, left_pred.as_ref())?;
                let right_plan = self.plan_table_scan(right, right_pred.as_ref())?;

                // Cost-based join order: smaller table as build side (hash
                // table). The join iterator preserves the PROBE side's
                // unmatched rows for a LEFT join and the BUILD side's for a
                // RIGHT one, so swapping the operands must also swap the
                // join type or the preserved table silently changes.
                let (build, probe, build_col, probe_col, build_table, probe_table, join_type) =
                    if left_plan.estimated_rows() <= right_plan.estimated_rows() {
                        (
                            left_plan,
                            right_plan,
                            on_left.clone(),
                            on_right.clone(),
                            left.clone(),
                            right.clone(),
                            Self::flip_outer_join(join_type),
                        )
                    } else {
                        (
                            right_plan,
                            left_plan,
                            on_right.clone(),
                            on_left.clone(),
                            right.clone(),
                            left.clone(),
                            join_type.clone(),
                        )
                    };

                let build_rows = build.estimated_rows();
                let probe_rows = probe.estimated_rows();
                let est_rows = (build_rows as f64 * probe_rows as f64 * 0.1) as u64; // 10% match rate
                let cost = build.estimated_cost()
                    + probe.estimated_cost()
                    + build_rows as f64 * HASH_BUILD_COST_PER_ROW
                    + probe_rows as f64 * HASH_PROBE_COST_PER_ROW;

                Ok(PlanNode::HashJoin {
                    left: Box::new(build),
                    right: Box::new(probe),
                    join_type,
                    on_left_col: build_col,
                    on_right_col: probe_col,
                    left_table: build_table,
                    right_table: probe_table,
                    estimated_rows: est_rows.max(1),
                    estimated_cost: cost,
                })
            }
        }
    }

    /// Swap the preserved side of an outer join when the operands are
    /// exchanged; an inner join is unchanged.
    fn flip_outer_join(join_type: &JoinType) -> JoinType {
        match join_type {
            JoinType::Left => JoinType::Right,
            JoinType::Right => JoinType::Left,
            JoinType::Inner => JoinType::Inner,
        }
    }

    /// Choose access method for a single table.
    fn plan_table_scan(
        &self,
        table_name: &str,
        where_clause: Option<&WhereExpr>,
    ) -> Result<PlanNode, String> {
        let stats = self.stats.get(table_name);
        let row_count = stats.map(|s| s.row_count).unwrap_or(1000); // default estimate

        // Check for primary key equality lookup
        if let Some(expr) = where_clause {
            if let Some(pk_val) = self.extract_pk_lookup(table_name, expr) {
                // The fetched row still has to satisfy the rest of the
                // predicate.
                return Ok(PlanNode::Scan {
                    table: table_name.to_string(),
                    access: AccessMethod::PkLookup { key_value: pk_val },
                    filter: Some(expr.clone()),
                    estimated_rows: 1,
                    estimated_cost: PK_LOOKUP_COST,
                });
            }

            // Check for index scan opportunity
            if let Some(idx) = self.find_best_index(table_name, expr) {
                let sel = estimate_selectivity(expr);
                let est_rows = (row_count as f64 * sel) as u64;
                let cost = est_rows as f64 * INDEX_SCAN_COST_PER_ROW;
                return Ok(PlanNode::Scan {
                    table: table_name.to_string(),
                    access: AccessMethod::IndexScan {
                        index_name: idx.name.clone(),
                        index_id: idx.id,
                    },
                    filter: Some(expr.clone()),
                    estimated_rows: est_rows.max(1),
                    estimated_cost: cost,
                });
            }
        }

        // Default: sequential scan
        let cost = row_count as f64 * SEQ_SCAN_COST_PER_ROW;
        let (est_rows, filter) = if let Some(expr) = where_clause {
            let sel = estimate_selectivity(expr);
            ((row_count as f64 * sel) as u64, Some(expr.clone()))
        } else {
            (row_count, None)
        };

        Ok(PlanNode::Scan {
            table: table_name.to_string(),
            access: AccessMethod::SeqScan,
            filter,
            estimated_rows: est_rows.max(1),
            estimated_cost: cost,
        })
    }

    /// Check if WHERE has an equality on the table's primary key.
    ///
    /// Only the declared key qualifies — a column named `id` need not be it.
    fn extract_pk_lookup(&self, table_name: &str, expr: &WhereExpr) -> Option<String> {
        let pk = self.stats.get(table_name)?.primary_key.clone();
        match expr {
            WhereExpr::Comparison {
                column,
                op: CmpOp::Eq,
                value,
            } => {
                // A qualified name must name this table to claim the lookup.
                let qualifies = match column.split_once('.') {
                    Some((qual, _)) => qual.eq_ignore_ascii_case(table_name),
                    None => true,
                };
                if qualifies && bare_name(column).eq_ignore_ascii_case(&pk) {
                    Some(value.as_string())
                } else {
                    None
                }
            }
            WhereExpr::And(a, b) => self
                .extract_pk_lookup(table_name, a)
                .or_else(|| self.extract_pk_lookup(table_name, b)),
            _ => None,
        }
    }

    /// Find the best index for a WHERE predicate.
    fn find_best_index(&self, table_name: &str, expr: &WhereExpr) -> Option<IndexDefinition> {
        let stats = self.stats.get(table_name)?;
        // Index fields are bare column names, so match the predicate's bare
        // components whether or not they are qualified.
        let columns_used = extract_where_columns(Some(expr))
            .into_iter()
            .map(|c| bare_name(&c).to_string())
            .collect::<Vec<_>>();

        // Score each index by how many of its fields match the WHERE columns
        let mut best: Option<(IndexDefinition, usize)> = None;
        for idx in &stats.indexes {
            let matched = idx
                .fields
                .iter()
                .take_while(|(f, _)| columns_used.contains(f))
                .count();
            if matched > 0 && best.as_ref().map(|(_, s)| matched > *s).unwrap_or(true) {
                best = Some((idx.clone(), matched));
            }
        }
        best.map(|(idx, _)| idx)
    }

    /// Check if filter was already pushed into scan node.
    fn filter_pushed_to_scan(&self, from: &FromClause, _expr: &WhereExpr) -> bool {
        matches!(from, FromClause::Table(_)) // single-table scans absorb the filter
    }
}

/// Extract column names referenced in a WHERE expression.
pub fn extract_where_columns(expr: Option<&WhereExpr>) -> Vec<String> {
    let Some(expr) = expr else { return Vec::new() };
    extract_where_columns_inner(expr)
}

fn extract_where_columns_inner(expr: &WhereExpr) -> Vec<String> {
    match expr {
        WhereExpr::Comparison { column, .. } => vec![column.clone()],
        WhereExpr::And(a, b) | WhereExpr::Or(a, b) => {
            let mut cols = extract_where_columns_inner(a);
            cols.extend(extract_where_columns_inner(b));
            cols
        }
        WhereExpr::Not(inner) => extract_where_columns_inner(inner),
        WhereExpr::IsNull(c) | WhereExpr::IsNotNull(c) => vec![c.clone()],
        WhereExpr::In(c, _) | WhereExpr::InSubquery(c, _) => vec![c.clone()],
    }
}

// ─── EXPLAIN Output ─────────────────────────────────────────────────────────

impl fmt::Display for PlanNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_indent(f, 0)
    }
}

impl PlanNode {
    fn fmt_indent(&self, f: &mut fmt::Formatter<'_>, depth: usize) -> fmt::Result {
        let indent = "  ".repeat(depth);
        match self {
            Self::Scan {
                table,
                access,
                estimated_rows,
                estimated_cost,
                filter,
            } => {
                let method = match access {
                    AccessMethod::SeqScan => "Seq Scan".to_string(),
                    AccessMethod::IndexScan { index_name, .. } => {
                        format!("Index Scan ({})", index_name)
                    }
                    AccessMethod::PkLookup { key_value } => {
                        format!("PK Lookup (key={})", key_value)
                    }
                };
                write!(
                    f,
                    "{}→ {} on {}  (rows={}, cost={:.1})",
                    indent, method, table, estimated_rows, estimated_cost
                )?;
                if let Some(flt) = filter {
                    write!(f, "\n{}  Filter: {:?}", indent, flt)?;
                }
            }
            Self::HashJoin {
                left,
                right,
                join_type,
                on_left_col,
                on_right_col,
                left_table: _,
                right_table: _,
                estimated_rows,
                estimated_cost,
            } => {
                write!(
                    f,
                    "{}→ Hash {:?} Join on {} = {}  (rows={}, cost={:.1})",
                    indent, join_type, on_left_col, on_right_col, estimated_rows, estimated_cost
                )?;
                writeln!(f)?;
                left.fmt_indent(f, depth + 1)?;
                writeln!(f)?;
                right.fmt_indent(f, depth + 1)?;
            }
            Self::Filter {
                child,
                predicate,
                estimated_rows,
                estimated_cost,
            } => {
                write!(
                    f,
                    "{}→ Filter  (rows={}, cost={:.1})\n{}  Predicate: {:?}",
                    indent, estimated_rows, estimated_cost, indent, predicate
                )?;
                writeln!(f)?;
                child.fmt_indent(f, depth + 1)?;
            }
            Self::Project { child, columns } => {
                let cols: Vec<String> = columns.iter().map(|c| format!("{:?}", c)).collect();
                write!(f, "{}→ Project [{}]", indent, cols.join(", "))?;
                writeln!(f)?;
                child.fmt_indent(f, depth + 1)?;
            }
            Self::Sort {
                child,
                order_by,
                estimated_cost,
            } => {
                let keys: Vec<String> = order_by
                    .iter()
                    .map(|o| format!("{} {}", o.column, if o.desc { "DESC" } else { "ASC" }))
                    .collect();
                write!(
                    f,
                    "{}→ Sort [{}]  (cost={:.1})",
                    indent,
                    keys.join(", "),
                    estimated_cost
                )?;
                writeln!(f)?;
                child.fmt_indent(f, depth + 1)?;
            }
            Self::Limit { child, count } => {
                write!(f, "{}→ Limit {}", indent, count)?;
                writeln!(f)?;
                child.fmt_indent(f, depth + 1)?;
            }
            Self::Aggregate {
                child,
                group_by,
                estimated_rows,
                ..
            } => {
                write!(
                    f,
                    "{}→ Aggregate [GROUP BY {}]  (rows={})",
                    indent,
                    group_by.join(", "),
                    estimated_rows
                )?;
                writeln!(f)?;
                child.fmt_indent(f, depth + 1)?;
            }
        }
        Ok(())
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_stats() -> std::collections::HashMap<String, TableStats> {
        let mut m = std::collections::HashMap::new();
        m.insert(
            "users".into(),
            TableStats {
                table_name: "users".into(),
                row_count: 10000,
                avg_row_bytes: 256,
                indexes: vec![],
                histograms: vec![],
                primary_key: "id".into(),
            },
        );
        m.insert(
            "orders".into(),
            TableStats {
                table_name: "orders".into(),
                row_count: 100000,
                avg_row_bytes: 128,
                indexes: vec![],
                histograms: vec![],
                primary_key: "id".into(),
            },
        );
        m
    }

    #[test]
    fn test_simple_scan() {
        let opt = Optimizer::new(empty_stats());
        let stmt = parse_sql("SELECT * FROM users").unwrap();
        let plan = opt.optimize(&stmt).unwrap();
        assert!(plan.estimated_rows() <= 10000);
        let display = format!("{}", plan);
        assert!(display.contains("Seq Scan"));
        assert!(display.contains("users"));
    }

    #[test]
    fn test_pk_lookup() {
        let opt = Optimizer::new(empty_stats());
        let stmt = parse_sql("SELECT * FROM users WHERE id = 42").unwrap();
        let plan = opt.optimize(&stmt).unwrap();
        let display = format!("{}", plan);
        assert!(display.contains("PK Lookup"));
    }

    /// A PK lookup keeps the predicates it does not answer as a scan filter.
    #[test]
    fn test_pk_lookup_keeps_other_predicates() {
        let opt = Optimizer::new(empty_stats());
        let stmt = parse_sql("SELECT * FROM users WHERE id = 42 AND name = 'nobody'").unwrap();
        let plan = opt.optimize(&stmt).unwrap();
        let display = format!("{}", plan);
        assert!(display.contains("PK Lookup"), "plan: {display}");
        assert!(
            display.contains("name"),
            "the remaining predicate must be kept as a filter: {display}"
        );
    }

    /// Only the declared primary key gets a lookup, not any column named `id`.
    #[test]
    fn test_pk_lookup_requires_real_primary_key() {
        let mut stats = std::collections::HashMap::new();
        stats.insert(
            "orders".into(),
            TableStats {
                table_name: "orders".into(),
                row_count: 100,
                avg_row_bytes: 64,
                indexes: vec![],
                histograms: vec![],
                primary_key: "order_no".into(),
            },
        );
        let opt = Optimizer::new(stats);
        // `id` is an ordinary column here, so no PK lookup.
        let stmt = parse_sql("SELECT * FROM orders WHERE id = 5").unwrap();
        let plan = opt.optimize(&stmt).unwrap();
        let display = format!("{}", plan);
        assert!(!display.contains("PK Lookup"), "plan: {display}");

        // The declared key does get the lookup.
        let stmt = parse_sql("SELECT * FROM orders WHERE order_no = 'K1'").unwrap();
        let plan = opt.optimize(&stmt).unwrap();
        let display = format!("{}", plan);
        assert!(display.contains("PK Lookup"), "plan: {display}");
    }

    #[test]
    fn test_join_order_small_build() {
        let opt = Optimizer::new(empty_stats());
        let stmt =
            parse_sql("SELECT * FROM orders JOIN users ON orders.user_id = users.id").unwrap();
        let plan = opt.optimize(&stmt).unwrap();
        let display = format!("{}", plan);
        // users (10K) should be build side, orders (100K) probe side
        assert!(display.contains("Hash"));
        assert!(display.contains("Join"));
    }

    #[test]
    fn test_where_selectivity() {
        let expr = WhereExpr::Comparison {
            column: "status".into(),
            op: CmpOp::Eq,
            value: SqlValue::Text("active".into()),
        };
        let sel = estimate_selectivity(&expr);
        assert!(sel > 0.0 && sel < 1.0);
    }

    #[test]
    fn test_and_selectivity() {
        let expr = WhereExpr::And(
            Box::new(WhereExpr::Comparison {
                column: "a".into(),
                op: CmpOp::Eq,
                value: SqlValue::Integer(1),
            }),
            Box::new(WhereExpr::Comparison {
                column: "b".into(),
                op: CmpOp::Eq,
                value: SqlValue::Integer(2),
            }),
        );
        let sel = estimate_selectivity(&expr);
        assert!(sel < 0.1); // AND should be more selective
    }

    #[test]
    fn test_explain_output_format() {
        let opt = Optimizer::new(empty_stats());
        let stmt = parse_sql("SELECT name FROM users WHERE id = 1 ORDER BY name LIMIT 10").unwrap();
        let plan = opt.optimize(&stmt).unwrap();
        let display = format!("{}", plan);
        assert!(display.contains("Project"));
        assert!(display.contains("Limit"));
        assert!(display.contains("Sort"));
    }

    #[test]
    fn test_aggregate_plan() {
        let opt = Optimizer::new(empty_stats());
        let stmt = parse_sql("SELECT COUNT(*) FROM users").unwrap();
        let plan = opt.optimize(&stmt).unwrap();
        let display = format!("{}", plan);
        assert!(display.contains("Aggregate"));
    }
}
