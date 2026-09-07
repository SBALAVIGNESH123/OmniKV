//! SQL Executor — Runs parsed SQL statements against OmniKV
//!
//! Handles table row storage, JOIN execution, WHERE filtering,
//! GROUP BY aggregation, and ORDER BY sorting.

use crate::catalog::{Catalog, Column, ColumnType, TableDef};
use crate::sql::{
    AggFunc, CmpOp, FromClause, JoinType, OrderByItem, SelectColumn, SetOpType, SqlColumnDef,
    SqlStatement, SqlValue, WhereExpr, WindowFuncType,
};
use crate::{OmniKV, WriteBatch};
use std::collections::HashMap;
use std::sync::Arc;

/// Result row: column_name → value
pub type Row = HashMap<String, String>;

/// Execution result
pub enum ExecResult {
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
    },
    Modified {
        count: usize,
        command: String,
    },
    Ok(String),
}

pub struct SqlExecutor {
    pub db: Arc<OmniKV>,
    pub catalog: Arc<Catalog>,
    /// If set, all reads use this MVCC snapshot instead of the current seq.
    /// Used by PgWire transaction blocks for snapshot isolation.
    snapshot_seq: Option<u64>,
    /// Uncommitted writes staged inside an explicit transaction. DML
    /// buffers here instead of hitting `commit_batch`, so ROLLBACK
    /// discards it and COMMIT applies it atomically through the SSI
    /// transaction engine; later statements in the same transaction read
    /// the staged rows back (read-your-own-writes). `None` (or a `None`
    /// inside the RefCell) on autocommit executors, which keep committing
    /// each statement directly. Interior mutability keeps `execute(&self)`
    /// while a transaction is open — only this executor's own thread
    /// touches the batch.
    pending_batch: std::cell::RefCell<Option<WriteBatch>>,
    /// Query timeout duration. None = no timeout.
    query_timeout: Option<std::time::Duration>,
    /// Slow query log threshold. Queries exceeding this are logged.
    slow_query_threshold: std::time::Duration,
}

impl SqlExecutor {
    pub fn new(db: Arc<OmniKV>, catalog: Arc<Catalog>) -> Self {
        Self {
            db,
            catalog,
            snapshot_seq: None,
            pending_batch: std::cell::RefCell::new(None),
            query_timeout: Some(std::time::Duration::from_secs(30)),
            slow_query_threshold: std::time::Duration::from_millis(100),
        }
    }

    /// Creates a SqlExecutor that reads at a specific MVCC snapshot.
    /// Used by PgWire when inside an explicit transaction block (BEGIN...COMMIT).
    pub fn with_snapshot(db: Arc<OmniKV>, catalog: Arc<Catalog>, seq: u64) -> Self {
        Self {
            db,
            catalog,
            snapshot_seq: Some(seq),
            pending_batch: std::cell::RefCell::new(None),
            query_timeout: Some(std::time::Duration::from_secs(30)),
            slow_query_threshold: std::time::Duration::from_millis(100),
        }
    }

    /// Creates a transaction-aware executor: reads at the transaction's
    /// snapshot, DML staged into an internal pending batch instead of
    /// committed, and later statements in the same transaction see the
    /// staged rows (read-your-own-writes). `staged_writes` is the
    /// transaction's write set so far — seeded into the batch so this
    /// statement reads earlier statements' writes too. Durability is the
    /// caller's: [`Self::flush_batch`] hands the accumulated writes to the
    /// SSI transaction engine — `TransactionManager::commit` applies them
    /// atomically, a ROLLBACK discards them untouched.
    pub fn with_transaction(
        db: Arc<OmniKV>,
        catalog: Arc<Catalog>,
        read_seq: u64,
        staged_writes: &HashMap<String, (Option<String>, u64)>,
    ) -> Self {
        // Seed the pending batch with everything the transaction has
        // staged so far, so this statement's reads see earlier
        // statements' uncommitted writes (read-your-own-writes across
        // statements — each statement runs on its own executor).
        let mut pending = WriteBatch::new();
        for (key, (value, ttl)) in staged_writes {
            match value {
                Some(value) if *ttl > 0 => {
                    let _ = pending.set_with_ttl(key, value.clone(), *ttl);
                }
                Some(value) => {
                    let _ = pending.set(key, value.clone());
                }
                None => {
                    let _ = pending.delete(key);
                }
            }
        }
        Self {
            db,
            catalog,
            snapshot_seq: Some(read_seq),
            pending_batch: std::cell::RefCell::new(Some(pending)),
            query_timeout: Some(std::time::Duration::from_secs(30)),
            slow_query_threshold: std::time::Duration::from_millis(100),
        }
    }

    /// Whether this executor stages writes into a pending batch (created
    /// via [`Self::with_transaction`]) instead of committing directly.
    pub fn is_transactional(&self) -> bool {
        self.pending_batch.borrow().is_some()
    }

    /// Drains the staged writes of a transactional executor. The wire
    /// protocol core merges them into the open transaction's write set;
    /// the SSI engine then applies them atomically at COMMIT and
    /// discards them on ROLLBACK. Returns `None` for autocommit
    /// executors and after an earlier flush.
    pub fn flush_batch(&mut self) -> Option<WriteBatch> {
        self.pending_batch.get_mut().take()
    }

    /// Stages one DML `batch` into the pending batch: a later write of
    /// the same key replaces an earlier one, a delete removes an earlier
    /// write — the merged batch is exactly what COMMIT must apply and
    /// what reads-in-transaction must see. Autocommit executors commit
    /// immediately, exactly as before this transaction support existed.
    fn stage_or_commit(&self, batch: WriteBatch) -> Result<(), String> {
        let mut pending_slot = self.pending_batch.borrow_mut();
        match pending_slot.as_mut() {
            Some(pending) => {
                for (key, value, ttl) in batch.buffered_writes {
                    pending.buffered_writes.retain(|(k, _, _)| *k != key);
                    pending.buffered_deletes.retain(|k| *k != key);
                    let key_str = String::from_utf8_lossy(&key).into_owned();
                    if ttl > 0 {
                        pending
                            .set_with_ttl(&key_str, value, ttl)
                            .map_err(|e| format!("{:?}", e))?;
                    } else {
                        pending
                            .set(&key_str, value)
                            .map_err(|e| format!("{:?}", e))?;
                    }
                }
                for key in batch.buffered_deletes {
                    pending.buffered_writes.retain(|(k, _, _)| *k != key);
                    let key_str = String::from_utf8_lossy(&key).into_owned();
                    pending.delete(&key_str).map_err(|e| format!("{:?}", e))?;
                }
                Ok(())
            }
            None => self
                .db
                .commit_batch(&batch)
                .map(|_| ())
                .map_err(|e| format!("{:?}", e)),
        }
    }

    /// The staged writes of this transaction as an overlay storage key →
    /// serialized row (Some) or tombstone (None), consumed by reads.
    fn pending_overlay(&self) -> Option<std::collections::HashMap<String, Option<String>>> {
        let pending = self.pending_batch.borrow();
        let pending = pending.as_ref()?;
        let mut ov = std::collections::HashMap::new();
        for (key, value, _ttl) in &pending.buffered_writes {
            // Later writes of the same key already replaced earlier ones
            // in the staged batch, so insertion cannot lose data.
            ov.insert(
                String::from_utf8_lossy(key).into_owned(),
                Some(value.clone()),
            );
        }
        for key in &pending.buffered_deletes {
            ov.insert(String::from_utf8_lossy(key).into_owned(), None);
        }
        Some(ov)
    }

    /// Set query timeout. None = no timeout.
    pub fn set_query_timeout(&mut self, timeout: Option<std::time::Duration>) {
        self.query_timeout = timeout;
    }

    /// Set slow query log threshold.
    pub fn set_slow_query_threshold(&mut self, threshold: std::time::Duration) {
        self.slow_query_threshold = threshold;
    }

    pub fn execute(&self, stmt: &SqlStatement) -> Result<ExecResult, String> {
        let start = std::time::Instant::now();

        // A transactional executor's catalog overlay: staged DDL from
        // earlier statements in the block becomes visible to this
        // statement's catalog lookups. The overlay is rebuilt from the
        // pending batch (seeded from the transaction's write set), so
        // CREATEs and DROPs from earlier statements appear here.
        if self.is_transactional()
            && let Some(overlay) = self.pending_overlay()
        {
            let staged: HashMap<String, (Option<String>, u64)> =
                overlay.into_iter().map(|(k, v)| (k, (v, 0))).collect();
            self.catalog.apply_txn_overlay(&staged);
        }

        // Check timeout before execution
        if let Some(timeout) = self.query_timeout
            && timeout.is_zero()
        {
            return Err("Query timeout is zero".into());
        }

        let result = self.execute_inner(stmt);

        // Slow query logging
        let elapsed = start.elapsed();
        if elapsed > self.slow_query_threshold {
            eprintln!(
                "[SLOW QUERY] {:.1}ms | {:?}",
                elapsed.as_secs_f64() * 1000.0,
                match stmt {
                    SqlStatement::Select { .. } => "SELECT ...",
                    SqlStatement::Insert { .. } => "INSERT ...",
                    SqlStatement::Update { .. } => "UPDATE ...",
                    SqlStatement::Delete { .. } => "DELETE ...",
                    _ => "OTHER",
                }
            );
        }

        result
    }

    fn execute_inner(&self, stmt: &SqlStatement) -> Result<ExecResult, String> {
        match stmt {
            SqlStatement::CreateTable {
                name,
                columns,
                if_not_exists,
            } => self.exec_create_table(name, columns, *if_not_exists),
            SqlStatement::DropTable { name, if_exists } => self.exec_drop_table(name, *if_exists),
            SqlStatement::Insert {
                table,
                columns,
                values,
            } => self.exec_insert(table, columns, values),
            SqlStatement::Select {
                columns,
                from,
                where_clause,
                group_by,
                having,
                order_by,
                limit,
                offset,
            } => self.exec_select(
                columns,
                from,
                where_clause.as_ref(),
                group_by,
                having.as_ref(),
                order_by,
                *limit,
                *offset,
            ),
            SqlStatement::Update {
                table,
                assignments,
                where_clause,
            } => self.exec_update(table, assignments, where_clause.as_ref()),
            SqlStatement::Delete {
                table,
                where_clause,
            } => self.exec_delete(table, where_clause.as_ref()),
            SqlStatement::ShowTables => {
                let tables = self.catalog.list_tables();
                let rows: Vec<Vec<String>> = tables.into_iter().map(|t| vec![t]).collect();
                Ok(ExecResult::Rows {
                    columns: vec!["table_name".into()],
                    rows,
                })
            }
            SqlStatement::Explain(inner) => {
                // Use the cost-based optimizer for real EXPLAIN output
                let stats = crate::optimizer::gather_stats(&self.catalog, None, &self.db);
                let optimizer = crate::optimizer::Optimizer::new(stats);
                match optimizer.optimize(inner) {
                    Ok(plan) => {
                        let plan_text = format!("{}", plan);
                        let rows: Vec<Vec<String>> = plan_text
                            .lines()
                            .map(|line| vec![line.to_string()])
                            .collect();
                        Ok(ExecResult::Rows {
                            columns: vec!["QUERY PLAN".into()],
                            rows,
                        })
                    }
                    Err(_) => Ok(ExecResult::Rows {
                        columns: vec!["QUERY PLAN".into()],
                        rows: vec![vec![format!("{:?}", inner)]],
                    }),
                }
            }
            SqlStatement::ExplainAnalyze(inner) => {
                // Run the query through the plan executor and collect stats
                let stats = crate::optimizer::gather_stats(&self.catalog, None, &self.db);
                let optimizer = crate::optimizer::Optimizer::new(stats);
                match optimizer.optimize(inner) {
                    Ok(plan) => {
                        let plan_exec = crate::plan_exec::PlanExecutor::new(
                            self.db.clone(),
                            self.catalog.clone(),
                        );
                        match plan_exec.explain_analyze(&plan) {
                            Ok((_rows, node_stats)) => {
                                let mut output = Vec::new();
                                for (label, ns) in &node_stats {
                                    output.push(vec![format!(
                                        "{} (est. rows={}, actual rows={}, time={:.3}ms)",
                                        label, ns.estimated_rows, ns.actual_rows, ns.actual_time_ms
                                    )]);
                                }
                                Ok(ExecResult::Rows {
                                    columns: vec!["QUERY PLAN (ANALYZE)".into()],
                                    rows: output,
                                })
                            }
                            Err(e) => Ok(ExecResult::Rows {
                                columns: vec!["QUERY PLAN (ANALYZE)".into()],
                                rows: vec![vec![format!("Error: {}", e)]],
                            }),
                        }
                    }
                    Err(_) => Ok(ExecResult::Rows {
                        columns: vec!["QUERY PLAN (ANALYZE)".into()],
                        rows: vec![vec![format!("{:?}", inner)]],
                    }),
                }
            }
            SqlStatement::SetOp {
                op,
                left,
                right,
                all,
            } => {
                let left_result = self.execute(left)?;
                let right_result = self.execute(right)?;

                let (left_cols, left_rows) = match left_result {
                    ExecResult::Rows { columns, rows } => (columns, rows),
                    _ => return Err("SET operation requires SELECT on left side".into()),
                };
                let (_right_cols, right_rows) = match right_result {
                    ExecResult::Rows { columns, rows } => (columns, rows),
                    _ => return Err("SET operation requires SELECT on right side".into()),
                };

                let combined_rows = match op {
                    SetOpType::Union => {
                        let mut result = left_rows;
                        result.extend(right_rows);
                        if !all {
                            // Remove duplicates
                            let mut seen = std::collections::HashSet::new();
                            result.retain(|row| {
                                let key = row.join("\0");
                                seen.insert(key)
                            });
                        }
                        result
                    }
                    SetOpType::Intersect => {
                        let right_set: std::collections::HashSet<String> =
                            right_rows.iter().map(|r| r.join("\0")).collect();
                        let mut result: Vec<Vec<String>> = left_rows
                            .into_iter()
                            .filter(|r| right_set.contains(&r.join("\0")))
                            .collect();
                        if !all {
                            let mut seen = std::collections::HashSet::new();
                            result.retain(|row| seen.insert(row.join("\0")));
                        }
                        result
                    }
                    SetOpType::Except => {
                        let right_set: std::collections::HashSet<String> =
                            right_rows.iter().map(|r| r.join("\0")).collect();
                        let mut result: Vec<Vec<String>> = left_rows
                            .into_iter()
                            .filter(|r| !right_set.contains(&r.join("\0")))
                            .collect();
                        if !all {
                            let mut seen = std::collections::HashSet::new();
                            result.retain(|row| seen.insert(row.join("\0")));
                        }
                        result
                    }
                };

                Ok(ExecResult::Rows {
                    columns: left_cols,
                    rows: combined_rows,
                })
            }
        }
    }

    fn exec_create_table(
        &self,
        name: &str,
        cols: &[SqlColumnDef],
        if_not_exists: bool,
    ) -> Result<ExecResult, String> {
        if if_not_exists && self.catalog.get_table(name).is_some() {
            return Ok(ExecResult::Ok("Table already exists".into()));
        }

        let mut pk = None;
        let mut columns = Vec::new();
        for c in cols {
            let col_type = c.col_type.parse::<ColumnType>()?;
            if c.primary_key {
                pk = Some(c.name.clone());
            }
            columns.push(Column {
                name: c.name.clone(),
                col_type,
                nullable: c.nullable,
                primary_key: c.primary_key,
                default: c.default.clone(),
            });
        }

        let primary_key = pk.unwrap_or_else(|| columns[0].name.clone());
        let table = TableDef {
            name: name.to_string(),
            columns,
            primary_key,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };

        // Inside a transaction the CREATE stages in the pending batch —
        // the SSI engine applies it at COMMIT, a ROLLBACK discards it —
        // and the statement's own catalog cache learns the table so
        // later statements in the block see it. Outside one, the
        // catalog's own autocommit path (which also updates its cache)
        // handles it exactly as before.
        if self.is_transactional() {
            // The staging path must reject a duplicate exactly like the
            // autocommit path (Catalog::create_table checks the cache):
            // staging would silently overwrite the existing definition
            // at COMMIT instead of failing the statement.
            if self.catalog.get_table(name).is_some() {
                return Err(format!("Table '{}' already exists", name));
            }
            let (cat_key, cat_value) = Catalog::staged_create_entry(&table)?;
            let mut batch = WriteBatch::new();
            batch
                .set(&cat_key, cat_value)
                .map_err(|e| format!("{:?}", e))?;
            self.stage_or_commit(batch)?;
            self.catalog.insert_staged(table);
            return Ok(ExecResult::Ok(format!("CREATE TABLE {}", name)));
        }

        self.catalog.create_table(table)?;
        Ok(ExecResult::Ok(format!("CREATE TABLE {}", name)))
    }

    fn exec_drop_table(&self, name: &str, if_exists: bool) -> Result<ExecResult, String> {
        if if_exists && self.catalog.get_table(name).is_none() {
            return Ok(ExecResult::Ok("Table does not exist".into()));
        }
        // Inside a transaction the DROP stages: the catalog entry's
        // delete and the row deletes all land in the pending batch —
        // applied atomically at COMMIT, discarded by ROLLBACK — and the
        // statement's own cache forgets the table for this block's later
        // statements. Outside one, the catalog's autocommit path handles
        // rows + entry + cache exactly as before.
        if self.is_transactional() {
            let table = self
                .catalog
                .get_table(name)
                .ok_or_else(|| format!("Table '{}' does not exist", name))?;
            // The table's own rows go through the normal DML staging: the
            // row reads use this statement's snapshot+overlay, so only
            // rows that exist for this transaction are deleted.
            let rows = self.load_table_rows(&table);
            let mut batch = WriteBatch::new();
            for row in &rows {
                let pk = row.get(&table.primary_key).cloned().unwrap_or_default();
                let key = format!("{}{}", table.row_prefix(), pk);
                batch.delete(&key).map_err(|e| format!("{:?}", e))?;
            }
            let cat_key = Catalog::staged_drop_key(name);
            batch.delete(&cat_key).map_err(|e| format!("{:?}", e))?;
            self.stage_or_commit(batch)?;
            self.catalog.remove_staged(name);
            return Ok(ExecResult::Ok(format!("DROP TABLE {}", name)));
        }
        self.catalog.drop_table(name)?;
        Ok(ExecResult::Ok(format!("DROP TABLE {}", name)))
    }

    fn exec_insert(
        &self,
        table_name: &str,
        col_names: &[String],
        values: &[Vec<SqlValue>],
    ) -> Result<ExecResult, String> {
        let table = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| format!("Table '{}' does not exist", table_name))?;

        let columns = if col_names.is_empty() {
            table
                .column_names()
                .into_iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        } else {
            col_names.to_vec()
        };

        let mut batch = WriteBatch::new();
        let mut count = 0;

        for row_vals in values {
            if row_vals.len() != columns.len() {
                return Err(format!(
                    "Column count mismatch: expected {}, got {}",
                    columns.len(),
                    row_vals.len()
                ));
            }

            let mut row_map = HashMap::new();
            let mut pk_val = String::new();
            for (i, col) in columns.iter().enumerate() {
                let val = row_vals[i].as_string();
                if col.eq_ignore_ascii_case(&table.primary_key) {
                    pk_val = val.clone();
                }
                row_map.insert(col.clone(), val);
            }

            if pk_val.is_empty() {
                return Err("Primary key value is required".into());
            }

            let key = format!("{}{}", table.row_prefix(), pk_val);
            let value = serde_json::to_string(&row_map).map_err(|e| format!("Serialize: {}", e))?;
            batch.set(&key, value).map_err(|e| format!("{:?}", e))?;
            count += 1;
        }

        self.stage_or_commit(batch)?;
        Ok(ExecResult::Modified {
            count,
            command: format!("INSERT 0 {}", count),
        })
    }

    fn load_table_rows(&self, table: &TableDef) -> Vec<Row> {
        let prefix = table.row_prefix();
        // Use transaction snapshot if available, otherwise current seq (autocommit)
        let seq = self.snapshot_seq.unwrap_or_else(|| self.db.get_seq());
        let mut results = self
            .db
            .scan(&prefix, &format!("{}\x7F", prefix), seq)
            .unwrap_or_default();

        // Read-your-own-writes: staged rows replace their stored copies,
        // staged deletes hide stored rows. Only this table's key range is
        // overlaid — the pending batch may span tables.
        if let Some(overlay) = self.pending_overlay() {
            results.retain(|(key, _)| !overlay.contains_key(key.as_str()));
            for (key, value) in &overlay {
                let in_table = key.starts_with(prefix.as_str()) && key.len() > prefix.len();
                if let (true, Some(serialized)) = (in_table, value) {
                    // Validate the staged row still deserializes; invalid
                    // staged data is skipped (it could never have been
                    // produced by a successful DML statement).
                    if serde_json::from_str::<Row>(serialized).is_ok() {
                        results.push((key.clone(), serialized.clone()));
                    }
                }
            }
        }

        results
            .into_iter()
            .filter_map(|(_key, value)| serde_json::from_str::<Row>(&value).ok())
            .collect()
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "The SELECT executor mirrors SQL clauses explicitly; refactoring into an execution context is planned separately to avoid query semantics churn."
    )]
    fn exec_select(
        &self,
        columns: &[SelectColumn],
        from: &FromClause,
        where_clause: Option<&WhereExpr>,
        group_by: &[String],
        having: Option<&WhereExpr>,
        order_by: &[OrderByItem],
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<ExecResult, String> {
        // ═══ Pre-process: resolve subqueries in WHERE clause ═══
        let resolved_where = if let Some(expr) = where_clause {
            Some(self.resolve_subqueries(expr)?)
        } else {
            None
        };
        let where_clause = resolved_where.as_ref();

        // ═══ Production path: Optimizer → Volcano iterators ╀══
        let has_window = columns
            .iter()
            .any(|c| matches!(c, SelectColumn::WindowFunc { .. }));

        // Every table referenced by the statement must resolve before the
        // plan is built: the optimizer estimates rows for unknown tables
        // with a default, so a missing table only surfaces as a panic deep
        // inside plan compilation. Fail here with the catalog's error
        // instead — including tables that were dropped (and not re-created)
        // earlier in this transaction.
        self.validate_from_tables(from)?;

        // When OFFSET is present, fetch limit+offset rows from the pipeline,
        // then skip offset rows in post-processing.
        let effective_limit = match (limit, offset) {
            (Some(l), Some(o)) => Some(l + o),
            _ => limit,
        };

        let stmt = SqlStatement::Select {
            columns: columns.to_vec(),
            from: from.clone(),
            where_clause: where_clause.cloned(),
            group_by: group_by.to_vec(),
            having: having.cloned(),
            order_by: order_by.to_vec(),
            limit: effective_limit,
            offset: None, // offset handled in post-processing below
        };

        let stats = crate::optimizer::gather_stats(&self.catalog, None, &self.db);
        let optimizer = crate::optimizer::Optimizer::new(stats);

        match optimizer.optimize(&stmt) {
            Ok(plan) => {
                use crate::volcano::{RowIterator, compile_plan_with_scan, eval_where};
                // Inside a transaction, scans read at the snapshot and
                // overlay this transaction's staged writes (read-your-
                // own-writes); outside one, plain autocommit scans.
                let scan_ctx = self
                    .pending_overlay()
                    .map(|overlay| crate::volcano::ScanContext {
                        scan_seq: self.snapshot_seq.unwrap_or_else(|| self.db.get_seq()),
                        overlay,
                    });
                let mut iter =
                    compile_plan_with_scan(&plan, &self.db, &self.catalog, scan_ctx.as_ref());

                let mut rows: Vec<Row> = Vec::new();
                while let Some(row) = iter.next_row() {
                    rows.push(row);
                }

                // HAVING: post-aggregate filter
                if let Some(having_expr) = having {
                    rows.retain(|row| eval_where(row, having_expr));
                }

                if has_window {
                    self.apply_window_functions(&mut rows, columns);
                }

                let (col_names, mut result_rows) = self.project(&rows, columns)?;

                // OFFSET: skip first N rows, then re-apply original LIMIT
                if let Some(off) = offset {
                    if off < result_rows.len() {
                        result_rows = result_rows.into_iter().skip(off).collect();
                    } else {
                        result_rows.clear();
                    }
                }
                // Re-apply original limit (optimizer got limit+offset)
                if let Some(lim) = limit {
                    result_rows.truncate(lim);
                }

                Ok(ExecResult::Rows {
                    columns: col_names,
                    rows: result_rows,
                })
            }
            Err(_) => {
                self.exec_select_legacy(columns, from, where_clause, group_by, order_by, limit)
            }
        }
    }

    /// Resolve InSubquery expressions by executing the inner SELECT
    /// and converting to a plain IN(values) expression.
    fn resolve_subqueries(&self, expr: &WhereExpr) -> Result<WhereExpr, String> {
        match expr {
            WhereExpr::InSubquery(col, sub_stmt) => {
                // Execute the subquery
                match self.execute(sub_stmt)? {
                    ExecResult::Rows { rows, .. } => {
                        // Extract first column from each row
                        let values: Vec<SqlValue> = rows
                            .into_iter()
                            .filter_map(|row| row.into_iter().next())
                            .map(|v| {
                                if let Ok(n) = v.parse::<i64>() {
                                    SqlValue::Integer(n)
                                } else if let Ok(f) = v.parse::<f64>() {
                                    SqlValue::Float(f)
                                } else {
                                    SqlValue::Text(v)
                                }
                            })
                            .collect();
                        Ok(WhereExpr::In(col.clone(), values))
                    }
                    _ => Ok(WhereExpr::In(col.clone(), vec![])),
                }
            }
            WhereExpr::And(left, right) => {
                let l = self.resolve_subqueries(left)?;
                let r = self.resolve_subqueries(right)?;
                Ok(WhereExpr::And(Box::new(l), Box::new(r)))
            }
            WhereExpr::Or(left, right) => {
                let l = self.resolve_subqueries(left)?;
                let r = self.resolve_subqueries(right)?;
                Ok(WhereExpr::Or(Box::new(l), Box::new(r)))
            }
            WhereExpr::Not(inner) => {
                let resolved = self.resolve_subqueries(inner)?;
                Ok(WhereExpr::Not(Box::new(resolved)))
            }
            // All other variants pass through unchanged
            other => Ok(other.clone()),
        }
    }

    /// Window function post-processing (ROW_NUMBER, RANK, DENSE_RANK).
    fn apply_window_functions(&self, rows: &mut [Row], columns: &[SelectColumn]) {
        for col in columns {
            if let SelectColumn::WindowFunc {
                order_by: ob, desc, ..
            } = col
            {
                let ob = ob.clone();
                rows.sort_by(|a, b| {
                    let va = a.get(&ob).cloned().unwrap_or_default();
                    let vb = b.get(&ob).cloned().unwrap_or_default();
                    let cmp = smart_cmp(&va, &vb);
                    if *desc { cmp.reverse() } else { cmp }
                });
                break;
            }
        }
        for col in columns {
            if let SelectColumn::WindowFunc {
                func, order_by: ob, ..
            } = col
            {
                let mut prev_val = String::new();
                let mut rank = 0usize;
                let mut dense_rank = 0usize;
                for (i, row) in rows.iter_mut().enumerate() {
                    let cur_val = row.get(ob).cloned().unwrap_or_default();
                    match func {
                        WindowFuncType::RowNumber => {
                            row.insert("row_number".into(), (i + 1).to_string());
                        }
                        WindowFuncType::Rank => {
                            if cur_val != prev_val {
                                rank = i + 1;
                            }
                            row.insert("rank".into(), rank.to_string());
                        }
                        WindowFuncType::DenseRank => {
                            if cur_val != prev_val {
                                dense_rank += 1;
                            }
                            row.insert("dense_rank".into(), dense_rank.to_string());
                        }
                    }
                    prev_val = cur_val;
                }
                break;
            }
        }
    }

    /// Legacy execution path — fallback for queries the optimizer can't handle.
    /// Fails with the catalog's error when any table in the FROM clause
    /// (or a join's side) is unknown to this statement's catalog view.
    /// The optimizer would happily plan scans over unknown tables and the
    /// panic would surface far from the cause.
    fn validate_from_tables(&self, from: &FromClause) -> Result<(), String> {
        match from {
            FromClause::Table(name) => {
                if self.catalog.get_table(name).is_none() {
                    return Err(format!("Table '{}' does not exist", name));
                }
                Ok(())
            }
            FromClause::Join { left, right, .. } => {
                for side in [left, right] {
                    if self.catalog.get_table(side).is_none() {
                        return Err(format!("Table '{}' does not exist", side));
                    }
                }
                Ok(())
            }
        }
    }

    fn exec_select_legacy(
        &self,
        columns: &[SelectColumn],
        from: &FromClause,
        where_clause: Option<&WhereExpr>,
        group_by: &[String],
        order_by: &[OrderByItem],
        limit: Option<usize>,
    ) -> Result<ExecResult, String> {
        let mut rows = match from {
            FromClause::Table(name) => {
                let table = self
                    .catalog
                    .get_table(name)
                    .ok_or_else(|| format!("Table '{}' not found", name))?;
                self.load_table_rows(&table)
            }
            FromClause::Join {
                left,
                right,
                join_type,
                on_left,
                on_right,
            } => {
                let lt = self
                    .catalog
                    .get_table(left)
                    .ok_or_else(|| format!("Table '{}' not found", left))?;
                let rt = self
                    .catalog
                    .get_table(right)
                    .ok_or_else(|| format!("Table '{}' not found", right))?;
                let left_rows = self.load_table_rows(&lt);
                let right_rows = self.load_table_rows(&rt);
                self.execute_join(
                    &left_rows,
                    &right_rows,
                    left,
                    right,
                    on_left,
                    on_right,
                    join_type,
                )
            }
        };

        if let Some(expr) = where_clause {
            rows.retain(|row| eval_where(row, expr));
        }

        if !group_by.is_empty() {
            return self.exec_group_by(&rows, columns, group_by, order_by, limit);
        }

        let has_agg = columns
            .iter()
            .any(|c| matches!(c, SelectColumn::Aggregate(..)));
        if has_agg {
            return self.exec_aggregate_no_group(&rows, columns);
        }

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

        if let Some(lim) = limit {
            rows.truncate(lim);
        }

        let (col_names, result_rows) = self.project(&rows, columns)?;
        Ok(ExecResult::Rows {
            columns: col_names,
            rows: result_rows,
        })
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Join execution keeps the parsed join shape explicit; collapsing into a context struct is a later planner cleanup."
    )]
    fn execute_join(
        &self,
        left: &[Row],
        right: &[Row],
        left_name: &str,
        right_name: &str,
        on_left: &str,
        on_right: &str,
        join_type: &JoinType,
    ) -> Vec<Row> {
        let mut result = Vec::new();

        // Build hash index on right table
        let mut right_index: HashMap<String, Vec<&Row>> = HashMap::new();
        for r in right {
            let key = r.get(on_right).cloned().unwrap_or_default();
            right_index.entry(key).or_default().push(r);
        }

        for lr in left {
            let join_key = lr.get(on_left).cloned().unwrap_or_default();
            let matches = right_index.get(&join_key);

            match (matches, join_type) {
                (Some(rights), _) => {
                    for rr in rights {
                        let mut combined = Row::new();
                        for (k, v) in lr {
                            combined.insert(format!("{}.{}", left_name, k), v.clone());
                            combined.insert(k.clone(), v.clone());
                        }
                        for (k, v) in *rr {
                            combined.insert(format!("{}.{}", right_name, k), v.clone());
                            if !combined.contains_key(k) {
                                combined.insert(k.clone(), v.clone());
                            }
                        }
                        result.push(combined);
                    }
                }
                (None, JoinType::Left) => {
                    let mut combined = Row::new();
                    for (k, v) in lr {
                        combined.insert(format!("{}.{}", left_name, k), v.clone());
                        combined.insert(k.clone(), v.clone());
                    }
                    result.push(combined);
                }
                _ => {}
            }
        }
        result
    }

    fn exec_group_by(
        &self,
        rows: &[Row],
        columns: &[SelectColumn],
        group_by: &[String],
        order_by: &[OrderByItem],
        limit: Option<usize>,
    ) -> Result<ExecResult, String> {
        let mut groups: HashMap<String, Vec<&Row>> = HashMap::new();
        for row in rows {
            let key: String = group_by
                .iter()
                .map(|g| row.get(g).cloned().unwrap_or_default())
                .collect::<Vec<_>>()
                .join("|");
            groups.entry(key).or_default().push(row);
        }

        let mut col_names = Vec::new();
        let mut result_rows = Vec::new();

        for group_rows in groups.values() {
            let mut result_row = Vec::new();
            for col in columns {
                match col {
                    SelectColumn::Named(name) => {
                        col_names.push(name.clone());
                        result_row.push(group_rows[0].get(name).cloned().unwrap_or_default());
                    }
                    SelectColumn::Aggregate(func, target) => {
                        let (name, val) = compute_aggregate(func, target, group_rows);
                        col_names.push(name);
                        result_row.push(val);
                    }
                    _ => {}
                }
            }
            result_rows.push(result_row);
        }
        col_names.truncate(columns.len());

        if let Some(lim) = limit {
            result_rows.truncate(lim);
        }
        Ok(ExecResult::Rows {
            columns: col_names,
            rows: result_rows,
        })
    }

    fn exec_aggregate_no_group(
        &self,
        rows: &[Row],
        columns: &[SelectColumn],
    ) -> Result<ExecResult, String> {
        let refs: Vec<&Row> = rows.iter().collect();
        let mut col_names = Vec::new();
        let mut result_row = Vec::new();

        for col in columns {
            if let SelectColumn::Aggregate(func, target) = col {
                let (name, val) = compute_aggregate(func, target, &refs);
                col_names.push(name);
                result_row.push(val);
            }
        }
        Ok(ExecResult::Rows {
            columns: col_names,
            rows: vec![result_row],
        })
    }

    fn project(
        &self,
        rows: &[Row],
        columns: &[SelectColumn],
    ) -> Result<(Vec<String>, Vec<Vec<String>>), String> {
        if columns.iter().any(|c| matches!(c, SelectColumn::Star)) {
            if rows.is_empty() {
                return Ok((vec![], vec![]));
            }
            let mut names: Vec<String> = rows[0]
                .keys()
                .filter(|k| !k.contains('.'))
                .cloned()
                .collect();
            names.sort();
            let result: Vec<Vec<String>> = rows
                .iter()
                .map(|r| {
                    names
                        .iter()
                        .map(|n| r.get(n).cloned().unwrap_or("NULL".into()))
                        .collect()
                })
                .collect();
            return Ok((names, result));
        }

        let mut names = Vec::new();
        for c in columns {
            match c {
                SelectColumn::Named(n) => names.push(n.clone()),
                SelectColumn::Qualified(_, n) => names.push(n.clone()),
                SelectColumn::Aggregate(f, t) => {
                    names.push(format!("{:?}({})", f, t).to_lowercase())
                }
                SelectColumn::WindowFunc { func, .. } => {
                    let name = match func {
                        WindowFuncType::RowNumber => "row_number",
                        WindowFuncType::Rank => "rank",
                        WindowFuncType::DenseRank => "dense_rank",
                    };
                    names.push(name.into());
                }
                SelectColumn::Star => {}
            }
        }

        let result: Vec<Vec<String>> = rows
            .iter()
            .map(|r| {
                columns
                    .iter()
                    .map(|c| match c {
                        SelectColumn::Named(n) => r.get(n).cloned().unwrap_or("NULL".into()),
                        SelectColumn::Qualified(t, n) => r
                            .get(&format!("{}.{}", t, n))
                            .or_else(|| r.get(n))
                            .cloned()
                            .unwrap_or("NULL".into()),
                        SelectColumn::WindowFunc { func, .. } => {
                            let key = match func {
                                WindowFuncType::RowNumber => "row_number",
                                WindowFuncType::Rank => "rank",
                                WindowFuncType::DenseRank => "dense_rank",
                            };
                            r.get(key).cloned().unwrap_or("NULL".into())
                        }
                        _ => "NULL".into(),
                    })
                    .collect()
            })
            .collect();

        Ok((names, result))
    }

    fn exec_update(
        &self,
        table_name: &str,
        assignments: &[(String, SqlValue)],
        where_clause: Option<&WhereExpr>,
    ) -> Result<ExecResult, String> {
        let table = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| format!("Table '{}' not found", table_name))?;
        let mut rows = self.load_table_rows(&table);

        if let Some(expr) = where_clause {
            rows.retain(|row| eval_where(row, expr));
        }

        let mut batch = WriteBatch::new();
        let count = rows.len();
        for row in &mut rows {
            for (col, val) in assignments {
                row.insert(col.clone(), val.as_string());
            }
            let pk = row.get(&table.primary_key).cloned().unwrap_or_default();
            let key = format!("{}{}", table.row_prefix(), pk);
            let value = serde_json::to_string(&row).map_err(|e| format!("{}", e))?;
            batch.set(&key, value).map_err(|e| format!("{:?}", e))?;
        }

        if count > 0 {
            self.stage_or_commit(batch)?;
        }
        Ok(ExecResult::Modified {
            count,
            command: format!("UPDATE {}", count),
        })
    }

    fn exec_delete(
        &self,
        table_name: &str,
        where_clause: Option<&WhereExpr>,
    ) -> Result<ExecResult, String> {
        let table = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| format!("Table '{}' not found", table_name))?;
        let mut rows = self.load_table_rows(&table);

        if let Some(expr) = where_clause {
            rows.retain(|row| eval_where(row, expr));
        }

        let mut batch = WriteBatch::new();
        let count = rows.len();
        for row in &rows {
            let pk = row.get(&table.primary_key).cloned().unwrap_or_default();
            let key = format!("{}{}", table.row_prefix(), pk);
            batch.delete(&key).map_err(|e| format!("{:?}", e))?;
        }

        if count > 0 {
            self.stage_or_commit(batch)?;
        }
        Ok(ExecResult::Modified {
            count,
            command: format!("DELETE {}", count),
        })
    }
}

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
        WhereExpr::InSubquery(_col, _sub) => {
            // Subquery evaluation requires executor context;
            // for simple eval_where we return true (handled at exec_select level)
            true
        }
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
