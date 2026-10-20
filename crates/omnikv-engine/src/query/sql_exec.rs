//! SQL Executor — Runs parsed SQL statements against OmniKV
//!
//! Handles table row storage, JOIN execution, WHERE filtering,
//! GROUP BY aggregation, and ORDER BY sorting.

use crate::catalog::{Catalog, Column, ColumnType, TableDef};
use crate::optimizer::extract_where_columns;
use crate::sql::{
    AggFunc, FromClause, JoinType, OrderByItem, SelectColumn, SetOpType, SqlColumnDef,
    SqlStatement, SqlValue, WhereExpr, WindowFuncType,
};
use crate::volcano::{ColumnTypeMap, eval_where_typed};
use crate::{OmniKV, WriteBatch};
use std::collections::HashMap;
use std::sync::Arc;

/// `None` is a SQL NULL; `Some("NULL")` is the literal text.
pub type Row = HashMap<String, Option<String>>;

#[cfg(test)]
struct MigrationCatalogHook {
    table: String,
    armed: std::sync::atomic::AtomicBool,
    entered: std::sync::mpsc::Sender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}

#[cfg(test)]
static MIGRATION_CATALOG_HOOK: std::sync::OnceLock<
    std::sync::Mutex<Option<Arc<MigrationCatalogHook>>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
static MIGRATION_TEST_LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();

#[cfg(test)]
fn migration_catalog_hook_slot() -> &'static std::sync::Mutex<Option<Arc<MigrationCatalogHook>>> {
    MIGRATION_CATALOG_HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn migration_test_lock() -> &'static std::sync::Mutex<()> {
    MIGRATION_TEST_LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

#[cfg(test)]
fn pause_after_migration_catalog_scan(table: &str) {
    let hook = migration_catalog_hook_slot()
        .lock()
        .expect("migration catalog hook lock")
        .clone();
    let Some(hook) = hook else {
        return;
    };
    if hook.table.eq_ignore_ascii_case(table)
        && hook.armed.swap(false, std::sync::atomic::Ordering::SeqCst)
    {
        hook.entered
            .send(())
            .expect("migration hook entry receiver");
        hook.release
            .lock()
            .expect("migration hook release lock")
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("migration hook release signal");
    }
}

/// One cell of an output row. NULL stays `None` until the wire boundary
/// renders it, so subqueries and set operations never confuse the two.
pub type Cell = Option<String>;

/// Execution result
pub enum ExecResult {
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<Cell>>,
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
    /// Read point captured for the outermost statement. Nested subqueries
    /// reuse it so one SQL statement cannot mix MVCC snapshots.
    statement_seq: std::cell::RefCell<Option<u64>>,
    /// Retention pin paired with `statement_seq` for autocommit statements.
    /// Explicit transaction snapshots are owned by `TransactionManager`.
    statement_snapshot: std::cell::RefCell<Option<crate::SnapshotGuard>>,
    /// SSI read dependencies collected by this statement's scans — the
    /// read counterpart of `pending_batch`. The wire core drains them
    /// into the open transaction alongside the staged writes, so any
    /// committed write that invalidates a read this statement made
    /// aborts the COMMIT (write-skew prevention). `None` on autocommit
    /// executors.
    pending_reads: std::cell::RefCell<Option<crate::volcano::ReadCollector>>,
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

struct StatementScope<'a> {
    executor: &'a SqlExecutor,
}

impl Drop for StatementScope<'_> {
    fn drop(&mut self) {
        *self.executor.statement_seq.borrow_mut() = None;
        self.executor.statement_snapshot.borrow_mut().take();
    }
}

impl SqlExecutor {
    pub fn new(db: Arc<OmniKV>, catalog: Arc<Catalog>) -> Self {
        Self {
            db,
            catalog,
            snapshot_seq: None,
            statement_seq: std::cell::RefCell::new(None),
            statement_snapshot: std::cell::RefCell::new(None),
            pending_reads: std::cell::RefCell::new(None),
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
            statement_seq: std::cell::RefCell::new(None),
            statement_snapshot: std::cell::RefCell::new(None),
            pending_reads: std::cell::RefCell::new(None),
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
            statement_seq: std::cell::RefCell::new(None),
            statement_snapshot: std::cell::RefCell::new(None),
            pending_reads: std::cell::RefCell::new(Some(crate::volcano::ReadCollector::default())),
            pending_batch: std::cell::RefCell::new(Some(pending)),
            query_timeout: Some(std::time::Duration::from_secs(30)),
            slow_query_threshold: std::time::Duration::from_millis(100),
        }
    }

    /// Drains this statement's collected read dependencies. The wire
    /// core merges them into the open transaction's read set and read
    /// ranges — the read counterpart of [`Self::flush_batch`].
    pub fn drain_reads(&mut self) -> Option<crate::volcano::ReadCollector> {
        self.pending_reads.get_mut().take()
    }

    /// Records a point read dependency (storage key).
    fn record_point_read(&self, key: &str) {
        let mut slot = self.pending_reads.borrow_mut();
        if let Some(collector) = slot.as_mut() {
            collector.keys.insert(key.to_string());
        }
    }

    /// Records a range read dependency ([start, end)) — a predicate
    /// lock over every key in the range.
    fn record_range_read(&self, start: &str, end: &str) {
        let mut slot = self.pending_reads.borrow_mut();
        if let Some(collector) = slot.as_mut() {
            collector.ranges.push((start.to_string(), end.to_string()));
        }
    }

    /// Records the catalog key of a table this statement depends on, so
    /// a concurrent CREATE/DROP of the same name aborts this COMMIT.
    fn record_catalog_read(&self, table_name: &str) {
        self.record_point_read(&Catalog::staged_drop_key(table_name));
    }

    /// The one MVCC read point for the current statement. Explicit
    /// transactions already carry a fixed snapshot; autocommit statements
    /// capture the current sequence once and thread it through all reads.
    fn statement_read_seq(&self) -> u64 {
        if let Some(snapshot) = self.statement_snapshot.borrow().as_ref() {
            snapshot.sequence()
        } else {
            self.snapshot_seq.unwrap_or_else(|| self.db.get_seq())
        }
    }

    /// Begins the outermost statement scope. The RAII scope clears the
    /// per-statement state on both normal `Result` errors and panics.
    fn begin_statement_scope(&self) -> StatementScope<'_> {
        if let Some(seq) = self.snapshot_seq {
            *self.statement_seq.borrow_mut() = Some(seq);
        } else {
            let snapshot = self.db.snapshot_guard();
            let seq = snapshot.sequence();
            *self.statement_snapshot.borrow_mut() = Some(snapshot);
            *self.statement_seq.borrow_mut() = Some(seq);
        }
        StatementScope { executor: self }
    }

    /// After waiting for another legacy migration, refresh an autocommit
    /// statement's read point so it cannot apply a stale migration batch over
    /// the migration that just completed. Explicit transactions retain their
    /// original snapshot by design.
    fn refresh_statement_seq_after_migration_lock(&self) -> u64 {
        if self.snapshot_seq.is_some() {
            return self.statement_read_seq();
        }
        let snapshot = self.db.snapshot_guard();
        let seq = snapshot.sequence();
        *self.statement_snapshot.borrow_mut() = Some(snapshot);
        *self.statement_seq.borrow_mut() = Some(seq);
        seq
    }

    /// Acquires the SQL-layer lock for one table. Autocommit DML holds this
    /// from its first visibility check through commit; DDL uses the same lock
    /// so a stale cached definition cannot resurrect a dropped table.
    fn table_lock(
        &self,
        table_name: &str,
    ) -> Result<Arc<crate::storage::core::SqlTableLock>, String> {
        self.db
            .sql_table_lock(table_name)
            .map_err(|e| format!("{:?}", e))
    }

    /// Reconcile a cached legacy definition with the catalog at this
    /// statement's read point before decoding rows. A stale executor may have
    /// been created before another executor completed the migration; using the
    /// persisted definition here prevents the preflight scan from decoding
    /// typed rows with legacy NULL rules.
    fn effective_dml_table(&self, table: &TableDef, seq: u64) -> Result<TableDef, String> {
        let persisted = self
            .effective_table(&table.name, seq)?
            .ok_or_else(|| format!("Table '{}' does not exist", table.name))?;
        self.catalog.insert_staged(persisted.clone());
        Ok(persisted)
    }

    /// Resolves a DML target after the per-table SQL lock is held. The cached
    /// catalog is only a hint: an executor can outlive a CREATE or DROP done
    /// by another executor, so the persisted definition at this statement's
    /// read point is authoritative.
    fn resolve_dml_table(&self, table_name: &str, seq: u64) -> Result<TableDef, String> {
        if let Some(cached) = self.catalog.get_table(table_name) {
            return self.effective_dml_table(&cached, seq);
        }
        let persisted = self
            .effective_table(table_name, seq)?
            .ok_or_else(|| format!("Table '{}' does not exist", table_name))?;
        self.catalog.insert_staged(persisted.clone());
        Ok(persisted)
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
    /// immediately.
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
        let _statement_scope = if self.statement_seq.borrow().is_none() {
            Some(self.begin_statement_scope())
        } else {
            None
        };

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

        let result = if let Some(timeout) = self.query_timeout
            && timeout.is_zero()
        {
            Err("Query timeout is zero".into())
        } else {
            self.execute_inner(stmt)
        };

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
                // Read storage even inside transactions: the cache cannot
                // represent read/decode errors. Merge staged DDL for the
                // transaction's read-your-own-CREATE/DROP visibility.
                let tables = self.catalog.list_tables_with_overlay_at(
                    self.statement_read_seq(),
                    self.pending_overlay().as_ref(),
                )?;
                let rows: Vec<Vec<Cell>> = tables.into_iter().map(|t| vec![Some(t)]).collect();
                Ok(ExecResult::Rows {
                    columns: vec!["table_name".into()],
                    rows,
                })
            }
            SqlStatement::Explain(inner) => {
                self.refresh_statement_catalog_for(inner, self.statement_read_seq())?;
                // Use the cost-based optimizer for real EXPLAIN output
                let stats = crate::optimizer::gather_stats_at(
                    &self.catalog,
                    None,
                    &self.db,
                    self.statement_read_seq(),
                );
                let optimizer = crate::optimizer::Optimizer::new(stats);
                match optimizer.optimize(inner) {
                    Ok(plan) => {
                        let plan_text = format!("{}", plan);
                        let rows: Vec<Vec<Cell>> = plan_text
                            .lines()
                            .map(|line| vec![Some(line.to_string())])
                            .collect();
                        Ok(ExecResult::Rows {
                            columns: vec!["QUERY PLAN".into()],
                            rows,
                        })
                    }
                    Err(_) => Ok(ExecResult::Rows {
                        columns: vec!["QUERY PLAN".into()],
                        rows: vec![vec![Some(format!("{:?}", inner))]],
                    }),
                }
            }
            SqlStatement::ExplainAnalyze(inner) => {
                self.refresh_statement_catalog_for(inner, self.statement_read_seq())?;
                // Run the query through the plan executor and collect stats
                let stats = crate::optimizer::gather_stats_at(
                    &self.catalog,
                    None,
                    &self.db,
                    self.statement_read_seq(),
                );
                let optimizer = crate::optimizer::Optimizer::new(stats);
                match optimizer.optimize(inner) {
                    Ok(plan) => {
                        let plan_exec = crate::plan_exec::PlanExecutor::for_plan_at(
                            self.db.clone(),
                            self.catalog.clone(),
                            &plan,
                            self.statement_read_seq(),
                        );
                        match plan_exec.explain_analyze(&plan) {
                            Ok((_rows, node_stats)) => {
                                let mut output = Vec::new();
                                for (label, ns) in &node_stats {
                                    output.push(vec![Some(format!(
                                        "{} (est. rows={}, actual rows={}, time={:.3}ms)",
                                        label, ns.estimated_rows, ns.actual_rows, ns.actual_time_ms
                                    ))]);
                                }
                                Ok(ExecResult::Rows {
                                    columns: vec!["QUERY PLAN (ANALYZE)".into()],
                                    rows: output,
                                })
                            }
                            Err(e) => Ok(ExecResult::Rows {
                                columns: vec!["QUERY PLAN (ANALYZE)".into()],
                                rows: vec![vec![Some(format!("Error: {}", e))]],
                            }),
                        }
                    }
                    Err(_) => Ok(ExecResult::Rows {
                        columns: vec!["QUERY PLAN (ANALYZE)".into()],
                        rows: vec![vec![Some(format!("{:?}", inner))]],
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
                            let mut seen = std::collections::HashSet::new();
                            result.retain(|row| seen.insert(row.clone()));
                        }
                        result
                    }
                    SetOpType::Intersect => {
                        let right_set: std::collections::HashSet<Vec<Cell>> =
                            right_rows.into_iter().collect();
                        let mut result: Vec<Vec<Cell>> = left_rows
                            .into_iter()
                            .filter(|r| right_set.contains(r))
                            .collect();
                        if !all {
                            let mut seen = std::collections::HashSet::new();
                            result.retain(|row| seen.insert(row.clone()));
                        }
                        result
                    }
                    SetOpType::Except => {
                        let right_set: std::collections::HashSet<Vec<Cell>> =
                            right_rows.into_iter().collect();
                        let mut result: Vec<Vec<Cell>> = left_rows
                            .into_iter()
                            .filter(|r| !right_set.contains(r))
                            .collect();
                        if !all {
                            let mut seen = std::collections::HashSet::new();
                            result.retain(|row| seen.insert(row.clone()));
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
        let table_lock = self.table_lock(name)?;
        let _table_guard = table_lock
            .lock()
            .map_err(|_| "SQL table lock poisoned".to_string())?;
        let seq = self.refresh_statement_seq_after_migration_lock();
        let persisted = self.effective_table(name, seq)?;
        if persisted.is_some() {
            if if_not_exists {
                return Ok(ExecResult::Ok("Table already exists".into()));
            }
            return Err(format!("Table '{}' already exists", name));
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
            // This table is created by the typed-Row code, so its NULLs are
            // JSON `null` and "NULL" is literal text. Old manifests lack the
            // field and default to legacy.
            row_format: crate::catalog::RowFormat::Typed,
        };

        // Inside a transaction the CREATE stages in the pending batch —
        // the SSI engine applies it at COMMIT, a ROLLBACK discards it —
        // and the statement's own catalog cache learns the table so
        // later statements in the block see it. Outside one, the
        // catalog's own autocommit path (which also updates its cache).
        if self.is_transactional() {
            // The staging path must reject a duplicate exactly like the
            // autocommit path (Catalog::create_table checks the cache):
            // staging would silently overwrite the existing definition
            // at COMMIT instead of failing the statement.
            self.record_catalog_read(name);
            let (cat_key, cat_value) = Catalog::staged_create_entry(&table)?;
            let mut batch = WriteBatch::new();
            batch
                .set(&cat_key, cat_value)
                .map_err(|e| format!("{:?}", e))?;
            self.stage_or_commit(batch)?;
            self.catalog.insert_staged(table);
            return Ok(ExecResult::Ok(format!("CREATE TABLE {}", name)));
        }

        // The cache may still contain a definition from a table that another
        // executor dropped. We resolved the persisted catalog under the
        // table lock above, so clear that stale entry before Catalog's own
        // duplicate check.
        self.catalog.remove_staged(name);
        self.catalog.create_table(table)?;
        Ok(ExecResult::Ok(format!("CREATE TABLE {}", name)))
    }

    fn exec_drop_table(&self, name: &str, if_exists: bool) -> Result<ExecResult, String> {
        let table_lock = self.table_lock(name)?;
        let _table_guard = table_lock
            .lock()
            .map_err(|_| "SQL table lock poisoned".to_string())?;
        let seq = self.refresh_statement_seq_after_migration_lock();
        let table = self.effective_table(name, seq)?;
        let Some(table) = table else {
            if if_exists {
                return Ok(ExecResult::Ok("Table does not exist".into()));
            }
            return Err(format!("Table '{}' does not exist", name));
        };
        self.catalog.insert_staged(table.clone());
        // Inside a transaction the DROP stages: the catalog entry's
        // delete and the row deletes all land in the pending batch —
        // applied atomically at COMMIT, discarded by ROLLBACK — and the
        // statement's own cache forgets the table for this block's later
        // statements. Outside one, the catalog's autocommit path handles.
        if self.is_transactional() {
            self.record_catalog_read(name);
            // The table's own rows go through the normal DML staging: the
            // row reads use this statement's snapshot+overlay, so only
            // rows that exist for this transaction are deleted.
            let rows = self.load_table_rows(&table);
            let mut batch = WriteBatch::new();
            for row in &rows {
                let pk = row
                    .get(&table.primary_key)
                    .cloned()
                    .flatten()
                    .unwrap_or_default();
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

    /// The table definition as recorded at this statement's read point. The
    /// catalog cache can lag a migration, and a stale `row_format` would
    /// re-migrate rows that are already typed.
    fn effective_table(&self, name: &str, seq: u64) -> Result<Option<TableDef>, String> {
        let key = Catalog::catalog_key(name);
        if let Some(overlay) = self.pending_overlay()
            && let Some(staged) = overlay.get(&key)
        {
            return Ok(staged
                .as_deref()
                .and_then(|v| serde_json::from_str::<TableDef>(v).ok()));
        }
        self.record_point_read(&key);
        let end = format!("{key}\x7F");
        Ok(self
            .db
            .scan(&key, &end, seq)
            .map_err(|e| format!("{:?}", e))?
            .into_iter()
            .next()
            .and_then(|(_, value)| serde_json::from_str::<TableDef>(&value).ok()))
    }

    /// Builds the batch that rewrites a legacy table's rows to the typed
    /// encoding and records the new format in the catalog. The caller merges
    /// this with its own writes and commits both together, so a statement
    /// that fails validation leaves the table exactly as it was.
    fn migration_batch(&self, table: &TableDef, seq: u64) -> Result<WriteBatch, String> {
        // Use one read point for the catalog format and the rows. An
        // autocommit executor can otherwise observe a concurrent migration
        // between these two reads and decode typed rows with legacy rules.
        let persisted = self
            .effective_table(&table.name, seq)?
            .ok_or_else(|| format!("Table '{}' does not exist", table.name))?;
        #[cfg(test)]
        pause_after_migration_catalog_scan(&table.name);
        // Already migrated: nothing to rewrite. Refresh this executor's
        // cached copy so its own later reads decode rows as typed.
        if persisted.row_format == crate::catalog::RowFormat::Typed {
            self.catalog.insert_staged(persisted);
            return Ok(WriteBatch::new());
        }

        let prefix = table.row_prefix();
        let end = format!("{}\u{7F}", prefix);
        self.record_range_read(&prefix, &end);
        let mut stored = self
            .db
            .scan(&prefix, &end, seq)
            .map_err(|e| format!("{:?}", e))?;

        let overlay = self.pending_overlay();
        if let Some(ov) = &overlay {
            stored.retain(|(key, _)| !ov.contains_key(key.as_str()));
            for (key, value) in ov {
                let in_table = key.starts_with(prefix.as_str()) && key.len() > prefix.len();
                if let (true, Some(serialized)) = (in_table, value)
                    && serde_json::from_str::<Row>(serialized).is_ok()
                {
                    stored.push((key.clone(), serialized.clone()));
                }
            }
        }

        let mut batch = WriteBatch::new();
        // The typed path keys and compares a numeric primary key canonically,
        // so a legacy row stored at ...007 is re-keyed to ...7.
        let mut canonical_keys: HashMap<String, &str> = HashMap::new();
        for (key, serialized) in &stored {
            let mut row =
                crate::plan_exec::PlanExecutor::deserialize_row(serialized, table.row_format)
                    .ok_or_else(|| {
                        format!(
                            "table '{}' holds a row that cannot be decoded; refusing to migrate it",
                            table.name
                        )
                    })?;

            let stored_suffix = key.strip_prefix(prefix.as_str()).unwrap_or_default();
            let canonical_suffix = crate::volcano::canonicalize_pk_key(table, stored_suffix);
            if let Some(first) = canonical_keys.insert(canonical_suffix.clone(), stored_suffix) {
                return Err(format!(
                    "primary key values '{first}' and '{stored_suffix}' in table '{}' both canonicalize to '{canonical_suffix}'",
                    table.name
                ));
            }
            if let Some(slot) = row.get_mut(&table.primary_key)
                && let Some(pk) = slot
            {
                *pk = crate::volcano::canonicalize_pk_key(table, pk);
            }

            let typed = serde_json::to_string(&row).map_err(|e| format!("{}", e))?;
            let canonical_key = format!("{prefix}{canonical_suffix}");
            if &canonical_key != key {
                batch.delete(key).map_err(|e| format!("{:?}", e))?;
                batch
                    .set(&canonical_key, typed)
                    .map_err(|e| format!("{:?}", e))?;
            } else if &typed != serialized {
                batch.set(key, typed).map_err(|e| format!("{:?}", e))?;
            }
        }

        let mut updated = table.clone();
        updated.row_format = crate::catalog::RowFormat::Typed;
        let (cat_key, cat_value) = Catalog::staged_create_entry(&updated)?;
        batch
            .set(&cat_key, cat_value)
            .map_err(|e| format!("{:?}", e))?;
        Ok(batch)
    }

    fn exec_insert(
        &self,
        table_name: &str,
        col_names: &[String],
        values: &[Vec<SqlValue>],
    ) -> Result<ExecResult, String> {
        let table_lock = self.table_lock(table_name)?;
        let _table_guard = table_lock
            .lock()
            .map_err(|_| "SQL table lock poisoned".to_string())?;
        self.record_catalog_read(table_name);
        let read_seq = self.refresh_statement_seq_after_migration_lock();
        let table = self.resolve_dml_table(table_name, read_seq)?;

        // Resolve and validate every column before touching storage: a
        // failed statement must not leave a migrated table behind.
        let columns = if col_names.is_empty() {
            table
                .column_names()
                .into_iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        } else {
            let mut resolved = Vec::with_capacity(col_names.len());
            for c in col_names {
                resolved.push(resolve_column_name(&table, c)?.to_string());
            }
            resolved
        };
        for row_vals in values {
            if row_vals.len() != columns.len() {
                return Err(format!(
                    "Column count mismatch: expected {}, got {}",
                    columns.len(),
                    row_vals.len()
                ));
            }
        }

        // Check canonical primary keys before building the batch. Numeric
        // aliases such as `7` and `'007'` address the same typed key, and a
        // multi-row INSERT must reject duplicates within the statement as
        // well as conflicts with rows already visible to this executor.
        let mut occupied_keys = std::collections::HashSet::new();
        for row in self.load_table_rows_at(&table, read_seq) {
            let Some(pk) = row.get(&table.primary_key).cloned().flatten() else {
                return Err(format!(
                    "table '{}' contains a row without a primary key; refusing to insert",
                    table.name
                ));
            };
            let canonical = crate::volcano::canonicalize_pk_key(&table, &pk);
            occupied_keys.insert(format!("{}{}", table.row_prefix(), canonical));
        }

        // The migration and the inserts commit in one batch, so a ROLLBACK
        // (or a later failure) drops the row rewrites and the format flip
        // together.
        let mut batch = if table.row_format == crate::catalog::RowFormat::Legacy {
            self.migration_batch(&table, read_seq)?
        } else {
            WriteBatch::new()
        };
        let mut count = 0;

        for row_vals in values {
            let mut row_map = Row::new();
            let mut pk_val = String::new();
            for (i, col) in columns.iter().enumerate() {
                let val = match &row_vals[i] {
                    SqlValue::Null => None,
                    v => {
                        let col_type = table
                            .columns
                            .iter()
                            .find(|c| c.name.eq_ignore_ascii_case(col))
                            .map(|c| &c.col_type);
                        Some(match col_type {
                            Some(t) => t.canonicalize(&v.as_string()),
                            None => v.as_string(),
                        })
                    }
                };
                if col.eq_ignore_ascii_case(&table.primary_key) {
                    pk_val = val.clone().unwrap_or_default();
                }
                row_map.insert(col.clone(), val);
            }

            if pk_val.is_empty() {
                return Err("Primary key value is required".into());
            }

            let canonical_pk = crate::volcano::canonicalize_pk_key(&table, &pk_val);
            let key = format!("{}{}", table.row_prefix(), canonical_pk);
            if !occupied_keys.insert(key.clone()) {
                return Err(format!(
                    "duplicate key value violates primary key of table '{}'",
                    table.name
                ));
            }
            let value = serde_json::to_string(&row_map).map_err(|e| format!("Serialize: {}", e))?;
            batch.set(&key, value).map_err(|e| format!("{:?}", e))?;
            count += 1;
        }

        self.stage_or_commit(batch)?;
        if table.row_format == crate::catalog::RowFormat::Legacy {
            let mut migrated = table.clone();
            migrated.row_format = crate::catalog::RowFormat::Typed;
            self.catalog.insert_staged(migrated);
        }
        Ok(ExecResult::Modified {
            count,
            command: format!("INSERT 0 {}", count),
        })
    }

    fn load_table_rows(&self, table: &TableDef) -> Vec<Row> {
        self.load_table_rows_at(table, self.statement_read_seq())
    }

    fn load_table_rows_at(&self, table: &TableDef, seq: u64) -> Vec<Row> {
        let prefix = table.row_prefix();
        // Every read of a table's rows is a range read of its prefix —
        // recorded for SSI so a concurrent write inside the prefix
        // aborts this transaction's COMMIT (phantom/write-skew
        // protection). Autocommit executors collect nothing (their
        // collector is None — nothing to abort).
        self.record_range_read(&prefix, &format!("{}\x7F", prefix));
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
            .filter_map(|(_key, value)| {
                crate::plan_exec::PlanExecutor::deserialize_row(&value, table.row_format)
            })
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
        self.refresh_from_tables(from, self.statement_read_seq())?;

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
        self.validate_where_columns(from, where_clause)?;
        self.validate_output_columns(from, columns, order_by, group_by)?;
        self.validate_having_columns(from, group_by, having)?;

        // When OFFSET is present, fetch limit+offset rows from the pipeline,
        // then skip offset rows in post-processing.
        let effective_limit = match (limit, offset) {
            (Some(l), Some(o)) => Some(l.saturating_add(o)),
            _ => limit,
        };

        let has_aggregation = !group_by.is_empty()
            || columns
                .iter()
                .any(|column| matches!(column, SelectColumn::Aggregate(..)))
            || having.is_some();

        // The physical plan normally applies ORDER BY/LIMIT before its final
        // projection. Window functions must see the complete filtered input,
        // and HAVING must run before either ORDER BY or LIMIT. Aggregate rows
        // also need to retain group/order/HAVING expressions that are not part
        // of the visible SELECT list. Keep those values in the internal
        // projection and defer the relevant post-processing until below.
        let mut plan_columns = columns.to_vec();
        if has_window || has_aggregation {
            for item in order_by {
                append_plan_column(&mut plan_columns, &item.column);
            }
        }
        if has_aggregation {
            for group in group_by {
                append_plan_column(&mut plan_columns, group);
            }
            if let Some(having_expr) = having {
                append_having_plan_columns(&mut plan_columns, having_expr);
            }
        }

        let stmt = SqlStatement::Select {
            columns: plan_columns,
            from: from.clone(),
            where_clause: where_clause.cloned(),
            group_by: group_by.to_vec(),
            having: having.cloned(),
            order_by: if having.is_some() {
                Vec::new()
            } else {
                order_by.to_vec()
            },
            limit: if has_window || having.is_some() {
                None
            } else {
                effective_limit
            },
            offset: None, // offset handled in post-processing below
        };

        let stats = crate::optimizer::gather_stats_at(
            &self.catalog,
            None,
            &self.db,
            self.statement_read_seq(),
        );
        let optimizer = crate::optimizer::Optimizer::new(stats);

        match optimizer.optimize(&stmt) {
            Ok(plan) => {
                use crate::volcano::{RowIterator, compile_plan_with_scan};
                // Every scan in one SQL statement uses the same read point.
                // Explicit transactions use their fixed snapshot; autocommit
                // statements use the sequence captured by `execute`. The
                // empty overlay is harmless and lets the same scan context
                // serve both modes.
                let scan_ctx = crate::volcano::ScanContext {
                    scan_seq: self.statement_read_seq(),
                    overlay: self.pending_overlay().unwrap_or_default(),
                    reads: self.pending_reads.borrow().as_ref().map(|_| {
                        std::sync::Arc::new(std::sync::Mutex::new(
                            crate::volcano::ReadCollector::default(),
                        ))
                    }),
                };
                let iter_reads = scan_ctx.reads.clone();
                let mut iter =
                    compile_plan_with_scan(&plan, &self.db, &self.catalog, Some(&scan_ctx));

                let mut rows: Vec<Row> = Vec::new();
                while let Some(row) = iter.next_row() {
                    rows.push(row);
                }

                // The plan's scans recorded their read dependencies into
                // the shared collector; fold them into the executor's
                // pending reads so the wire core drains one set.
                if let Some(reads) = iter_reads
                    && let Ok(collected) = reads.lock()
                {
                    let mut slot = self.pending_reads.borrow_mut();
                    if let Some(collector) = slot.as_mut() {
                        collector.keys.extend(collected.keys.iter().cloned());
                        collector.ranges.extend(collected.ranges.iter().cloned());
                    }
                }

                let plan_types = plan.output_types(&self.catalog);

                // HAVING is evaluated against the materialized aggregate rows
                // before the outer ORDER BY/LIMIT. Aggregate and group
                // expressions are retained in `plan_columns` above for this
                // purpose, including expressions absent from SELECT.
                if let Some(having_expr) = having {
                    rows.retain(|row| {
                        crate::volcano::eval_where_typed(row, having_expr, &plan_types)
                    });
                }

                if has_window {
                    self.apply_window_functions(&mut rows, columns, from);
                    // Window evaluation temporarily orders the input by each
                    // window's own key. SQL's outer ORDER BY is a separate
                    // clause and must be restored after all window values
                    // have been computed, before projection/limit are applied.
                    crate::volcano::sort_rows(&mut rows, order_by, &plan_types);
                } else if having.is_some() {
                    crate::volcano::sort_rows(&mut rows, order_by, &plan_types);
                }

                let (col_names, mut result_rows) = self.project(&rows, columns, from)?;

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
            WhereExpr::InSubquery(col, sub_stmt) => match self.execute(sub_stmt)? {
                ExecResult::Rows { rows, .. } => {
                    let values: Vec<SqlValue> = rows
                        .into_iter()
                        .filter_map(|row| row.into_iter().next())
                        .map(|cell| match cell {
                            None => SqlValue::Null,
                            Some(v) => {
                                if let Ok(n) = v.parse::<i64>() {
                                    SqlValue::Integer(n)
                                } else if let Ok(f) = v.parse::<f64>() {
                                    SqlValue::Float(f)
                                } else {
                                    SqlValue::Text(v)
                                }
                            }
                        })
                        .collect();
                    Ok(WhereExpr::In(col.clone(), values))
                }
                _ => Ok(WhereExpr::In(col.clone(), vec![])),
            },
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
    fn apply_window_functions(
        &self,
        rows: &mut [Row],
        columns: &[SelectColumn],
        from: &FromClause,
    ) {
        let col_types = self.column_types_for_from(from);

        // Each window expression has its own ordering. Sort immediately
        // before evaluating that expression, rather than sorting once and
        // applying every function to the first window's order. The final
        // query ORDER BY is restored by exec_select after this pass.
        for col in columns {
            if let SelectColumn::WindowFunc {
                func,
                order_by: ob,
                desc,
            } = col
            {
                crate::volcano::sort_rows(
                    rows,
                    std::slice::from_ref(&OrderByItem {
                        column: ob.clone(),
                        desc: *desc,
                    }),
                    &col_types,
                );
                let mut prev_val: Option<String> = None;
                // The first row always starts a new peer group, even when its
                // value is NULL: `None != None` would otherwise call it a tie
                // with the nonexistent previous row and rank it zero.
                let mut have_prev = false;
                let mut rank = 0usize;
                let mut dense_rank = 0usize;
                for (i, row) in rows.iter_mut().enumerate() {
                    let cur_val = crate::volcano::row_lookup(row, ob).map(str::to_string);
                    let new_peer = !have_prev || cur_val != prev_val;
                    match func {
                        WindowFuncType::RowNumber => {
                            row.insert("row_number".into(), Some((i + 1).to_string()));
                        }
                        WindowFuncType::Rank => {
                            if new_peer {
                                rank = i + 1;
                            }
                            row.insert("rank".into(), Some(rank.to_string()));
                        }
                        WindowFuncType::DenseRank => {
                            if new_peer {
                                dense_rank += 1;
                            }
                            row.insert("dense_rank".into(), Some(dense_rank.to_string()));
                        }
                    }
                    prev_val = cur_val;
                    have_prev = true;
                }
            }
        }
    }

    /// Build the declared type map used by filtering and sorting for every
    /// table referenced by a statement. Bare names resolve to the leftmost
    /// table, while qualified names retain their table prefix.
    fn column_types_for_from(&self, from: &FromClause) -> crate::volcano::ColumnTypeMap {
        let tables: Vec<String> = match from {
            FromClause::Table(table) => vec![table.clone()],
            FromClause::Join { left, right, .. } => vec![left.clone(), right.clone()],
        };
        let mut col_types = crate::volcano::ColumnTypeMap::new();
        for table_name in tables {
            if let Some(table) = self.catalog.get_table(&table_name) {
                for column in &table.columns {
                    col_types
                        .entry(column.name.clone())
                        .or_insert(column.col_type.clone());
                    col_types.insert(
                        format!("{table_name}.{}", column.name),
                        column.col_type.clone(),
                    );
                }
            }
        }
        col_types
    }

    /// Refreshes every table definition used by this statement from the same
    /// MVCC point as its row scans. A long-lived executor's cached catalog is
    /// intentionally not authoritative for autocommit SQL.
    fn refresh_from_tables(&self, from: &FromClause, seq: u64) -> Result<(), String> {
        let tables: Vec<&str> = match from {
            FromClause::Table(table) => vec![table],
            FromClause::Join { left, right, .. } => vec![left, right],
        };
        for table_name in tables {
            self.record_catalog_read(table_name);
            let table = self
                .effective_table(table_name, seq)?
                .ok_or_else(|| format!("Table '{}' does not exist", table_name))?;
            self.catalog.insert_staged(table);
        }
        Ok(())
    }

    /// Refreshes all table definitions needed by a plan-shaped statement,
    /// including nested set-operation branches used by EXPLAIN. This keeps
    /// planning and EXPLAIN ANALYZE on the same catalog snapshot as execution.
    fn refresh_statement_catalog_for(&self, stmt: &SqlStatement, seq: u64) -> Result<(), String> {
        match stmt {
            SqlStatement::Select { from, .. } => self.refresh_from_tables(from, seq),
            SqlStatement::SetOp { left, right, .. } => {
                self.refresh_statement_catalog_for(left, seq)?;
                self.refresh_statement_catalog_for(right, seq)
            }
            SqlStatement::Explain(inner) | SqlStatement::ExplainAnalyze(inner) => {
                self.refresh_statement_catalog_for(inner, seq)
            }
            _ => Ok(()),
        }
    }

    /// Fallback execution path for queries the optimizer cannot handle.
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
                self.record_catalog_read(name);
                Ok(())
            }
            FromClause::Join {
                left,
                right,
                on_left,
                on_right,
                ..
            } => {
                for side in [left, right] {
                    if self.catalog.get_table(side).is_none() {
                        return Err(format!("Table '{}' does not exist", side));
                    }
                    self.record_catalog_read(side);
                }
                // An unknown ON column would otherwise read as a missing key
                // and yield an empty or wrongly-bound outer join.
                for (table, col) in [(left, on_left), (right, on_right)] {
                    let table_def = self
                        .catalog
                        .get_table(table)
                        .ok_or_else(|| format!("Table '{table}' does not exist"))?;
                    let bare = col.rsplit('.').next().unwrap_or(col);
                    if let Some((qual, _)) = col.split_once('.')
                        && !qual.eq_ignore_ascii_case(table)
                    {
                        return Err(format!(
                            "table \"{qual}\" is not in the FROM clause of this statement"
                        ));
                    }
                    if !table_def.column_names().contains(&bare) {
                        return Err(format!("column \"{col}\" does not exist"));
                    }
                }
                Ok(())
            }
        }
    }

    fn validate_where_columns_of(
        &self,
        table_name: &str,
        where_clause: Option<&WhereExpr>,
    ) -> Result<(), String> {
        let table = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| format!("Table '{}' does not exist", table_name))?;
        let cols: Vec<String> = table
            .column_names()
            .into_iter()
            .map(str::to_string)
            .collect();
        for col in extract_where_columns(where_clause) {
            // A qualified name must name THIS table.
            if let Some((qual, bare)) = col.split_once('.') {
                if !qual.eq_ignore_ascii_case(table_name) {
                    return Err(format!(
                        "table \"{qual}\" is not in the FROM clause of this statement"
                    ));
                }
                if !cols.iter().any(|c| c == bare) {
                    return Err(format!("column \"{col}\" does not exist"));
                }
            } else if !cols.iter().any(|c| c == &col) {
                return Err(format!("column \"{col}\" does not exist"));
            }
        }
        Ok(())
    }

    /// A WHERE clause naming a column the table does not have must be an
    /// error, not an empty result: comparing a missing key yields "" and
    /// silently matches nothing, which looks like a legitimate answer.
    fn validate_where_columns(
        &self,
        from: &FromClause,
        where_clause: Option<&WhereExpr>,
    ) -> Result<(), String> {
        let per_table = self.columns_per_table(from)?;
        for col in extract_where_columns(where_clause) {
            self.check_column_reference(&per_table, &col)?;
        }
        Ok(())
    }

    /// Validate HAVING references against the grouped result, not just the
    /// base table. A grouped query may use a GROUP BY key or an aggregate
    /// expression, but an unknown/non-grouped base column must fail rather
    /// than silently evaluating against a missing aggregate-row key.
    fn validate_having_columns(
        &self,
        from: &FromClause,
        group_by: &[String],
        having: Option<&WhereExpr>,
    ) -> Result<(), String> {
        let Some(having) = having else {
            return Ok(());
        };
        let per_table = self.columns_per_table(from)?;
        self.validate_having_expr(&per_table, group_by, having)
    }

    fn validate_having_expr(
        &self,
        per_table: &[(String, Vec<String>)],
        group_by: &[String],
        expr: &WhereExpr,
    ) -> Result<(), String> {
        match expr {
            WhereExpr::Comparison { column, .. }
            | WhereExpr::IsNull(column)
            | WhereExpr::IsNotNull(column)
            | WhereExpr::In(column, _)
            | WhereExpr::InSubquery(column, _) => {
                if let Some((_, target)) = parse_aggregate_reference(column) {
                    if target != "*" {
                        self.check_column_reference(per_table, &target)?;
                    }
                    return Ok(());
                }

                self.check_column_reference(per_table, column)?;
                if !group_by
                    .iter()
                    .any(|group| same_column_reference(group, column))
                {
                    return Err(format!(
                        "column \"{column}\" must appear in the GROUP BY clause or be used in an aggregate function"
                    ));
                }
                Ok(())
            }
            WhereExpr::And(left, right) | WhereExpr::Or(left, right) => {
                self.validate_having_expr(per_table, group_by, left)?;
                self.validate_having_expr(per_table, group_by, right)
            }
            WhereExpr::Not(inner) => self.validate_having_expr(per_table, group_by, inner),
        }
    }

    /// Reject an unqualified column that exists on both sides of a join.
    ///
    /// A bare name binds to whichever side hashed as the build table, so the
    /// same query can read a different column when the planner swaps the join
    /// order. Standard SQL calls this ambiguous; the error names both tables.
    fn check_column_reference(
        &self,
        per_table: &[(String, Vec<String>)],
        col: &str,
    ) -> Result<(), String> {
        let Some((qual, bare)) = col.split_once('.') else {
            let owners: Vec<&str> = per_table
                .iter()
                .filter(|(_, cols)| cols.iter().any(|c| c == col))
                .map(|(t, _)| t.as_str())
                .collect();
            if owners.len() > 1 {
                return Err(format!(
                    "column \"{}\" is ambiguous — it exists in tables {} and {}; qualify it as \"{}.{}\" or \"{}.{}\"",
                    col, owners[0], owners[1], owners[0], col, owners[1], col
                ));
            }
            if owners.is_empty() {
                return Err(format!("column \"{col}\" does not exist"));
            }
            return Ok(());
        };
        match per_table.iter().find(|(t, _)| t.eq_ignore_ascii_case(qual)) {
            Some((_, cols)) => {
                if !cols.iter().any(|c| c == bare) {
                    return Err(format!("column \"{col}\" does not exist"));
                }
                Ok(())
            }
            None => Err(format!(
                "table \"{qual}\" is not in the FROM clause of this statement"
            )),
        }
    }

    /// Column lists per table in FROM order.
    fn columns_per_table(&self, from: &FromClause) -> Result<Vec<(String, Vec<String>)>, String> {
        let tables = match from {
            FromClause::Table(t) => vec![t.clone()],
            FromClause::Join { left, right, .. } => vec![left.clone(), right.clone()],
        };
        let mut per_table = Vec::with_capacity(tables.len());
        for t in &tables {
            let table = self
                .catalog
                .get_table(t)
                .ok_or_else(|| format!("Table '{t}' does not exist"))?;
            let names: Vec<String> = table
                .column_names()
                .into_iter()
                .map(str::to_string)
                .collect();
            per_table.push((t.clone(), names));
        }
        Ok(per_table)
    }

    /// Validate the columns named by the SELECT list, ORDER BY and GROUP BY.
    fn validate_output_columns(
        &self,
        from: &FromClause,
        columns: &[SelectColumn],
        order_by: &[OrderByItem],
        group_by: &[String],
    ) -> Result<(), String> {
        let per_table = self.columns_per_table(from)?;
        // `*` expands to every column, but it does not erase explicit
        // columns that appear beside it. Validate every non-star item so
        // `SELECT *, no_such_column` fails instead of silently dropping the
        // bad reference.
        for col in columns {
            match col {
                SelectColumn::Named(n) => self.check_column_reference(&per_table, n)?,
                SelectColumn::Qualified(t, n) => {
                    self.check_column_reference(&per_table, &format!("{t}.{n}"))?
                }
                SelectColumn::Aggregate(_, target) => {
                    // COUNT(*) tallies rows and names no column.
                    if target != "*" {
                        self.check_column_reference(&per_table, target)?
                    }
                }
                SelectColumn::WindowFunc { order_by: ob, .. } => {
                    self.check_column_reference(&per_table, ob)?
                }
                SelectColumn::Star => {}
            }
        }
        for item in order_by {
            if let Some((_, target)) = parse_aggregate_reference(&item.column) {
                if target != "*" {
                    self.check_column_reference(&per_table, &target)?;
                }
            } else {
                self.check_column_reference(&per_table, &item.column)?;
            }
        }
        for g in group_by {
            self.check_column_reference(&per_table, g)?;
        }
        Ok(())
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
                    .ok_or_else(|| format!("Table '{}' does not exist", name))?;
                self.record_catalog_read(name);
                self.load_table_rows(&table)
            }
            FromClause::Join {
                left,
                right,
                join_type,
                on_left,
                on_right,
            } => {
                self.record_catalog_read(left);
                self.record_catalog_read(right);
                let lt = self
                    .catalog
                    .get_table(left)
                    .ok_or_else(|| format!("Table '{}' does not exist", left))?;
                let rt = self
                    .catalog
                    .get_table(right)
                    .ok_or_else(|| format!("Table '{}' does not exist", right))?;
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

        // Predicates compare by the column's affinity, and a qualified name
        // resolves through the `table.column` key the join stores. The same
        // types govern ORDER BY below, so they are built once here.
        let from_tables: Vec<String> = match from {
            FromClause::Table(t) => vec![t.clone()],
            FromClause::Join { left, right, .. } => vec![left.clone(), right.clone()],
        };
        let mut col_types = crate::volcano::ColumnTypeMap::new();
        for t in &from_tables {
            if let Some(table) = self.catalog.get_table(t) {
                for c in &table.columns {
                    // Left wins a bare-key clash, matching execute_join.
                    col_types
                        .entry(c.name.clone())
                        .or_insert(c.col_type.clone());
                    col_types.insert(format!("{t}.{}", c.name), c.col_type.clone());
                }
            }
        }
        if let Some(expr) = where_clause {
            rows.retain(|row| eval_where_typed(row, expr, &col_types));
        }

        if !group_by.is_empty() {
            return self.exec_group_by(&rows, columns, group_by, order_by, limit, &col_types);
        }

        let has_agg = columns
            .iter()
            .any(|c| matches!(c, SelectColumn::Aggregate(..)));
        if has_agg {
            return self.exec_aggregate_no_group(&rows, columns, &col_types);
        }

        if columns
            .iter()
            .any(|column| matches!(column, SelectColumn::WindowFunc { .. }))
        {
            self.apply_window_functions(&mut rows, columns, from);
        }

        // Window evaluation temporarily orders rows by each window key. Apply
        // the statement's outer ORDER BY after that pass, just as the
        // optimized executor does.
        crate::volcano::sort_rows(&mut rows, order_by, &col_types);

        if let Some(lim) = limit {
            rows.truncate(lim);
        }

        let (col_names, result_rows) = self.project(&rows, columns, from)?;
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
        let mut null_key_right_rows: Vec<&Row> = Vec::new();
        for r in right {
            match r.get(on_right).cloned().flatten() {
                Some(key) => {
                    right_index.entry(key).or_default().push(r);
                }
                None => null_key_right_rows.push(r),
            }
        }

        let mut matched_right_keys: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        for lr in left {
            // A NULL left key matches nothing, but on a LEFT join the row is
            // still preserved.
            let join_key = lr.get(on_left).cloned().flatten();
            let matches = join_key.as_deref().and_then(|k| right_index.get(k));

            match (matches, join_type) {
                (Some(rights), _) => {
                    if let Some(k) = join_key {
                        matched_right_keys.insert(k);
                    }
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

        // A RIGHT JOIN preserves right rows that no left row matched,
        // including those whose key is NULL.
        if matches!(join_type, JoinType::Right) {
            for (key, rows) in &right_index {
                if matched_right_keys.contains(key) {
                    continue;
                }
                for rr in rows {
                    let mut combined = Row::new();
                    for (k, v) in *rr {
                        combined.insert(format!("{}.{}", right_name, k), v.clone());
                        combined.insert(k.clone(), v.clone());
                    }
                    result.push(combined);
                }
            }
            for rr in &null_key_right_rows {
                let mut combined = Row::new();
                for (k, v) in *rr {
                    combined.insert(format!("{}.{}", right_name, k), v.clone());
                    combined.insert(k.clone(), v.clone());
                }
                result.push(combined);
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
        col_types: &ColumnTypeMap,
    ) -> Result<ExecResult, String> {
        let mut groups: HashMap<String, Vec<&Row>> = HashMap::new();
        for row in rows {
            let key: String = group_by
                .iter()
                .map(|g| crate::volcano::encode_key_part(crate::volcano::row_lookup(row, g)))
                .collect::<Vec<_>>()
                .join("|");
            groups.entry(key).or_default().push(row);
        }

        let mut col_names = Vec::new();
        let mut result_rows: Vec<Vec<Cell>> = Vec::new();

        for group_rows in groups.values() {
            let mut result_row = Vec::new();
            for col in columns {
                match col {
                    SelectColumn::Named(name) => {
                        col_names.push(name.clone());
                        result_row.push(
                            crate::volcano::row_lookup(group_rows[0], name).map(str::to_string),
                        );
                    }
                    SelectColumn::Qualified(t, n) => {
                        col_names.push(n.clone());
                        let key = format!("{}.{}", t, n);
                        result_row.push(
                            crate::volcano::row_lookup(group_rows[0], &key).map(str::to_string),
                        );
                    }
                    SelectColumn::Aggregate(func, target) => {
                        let (name, val) = compute_aggregate(
                            func,
                            target,
                            group_rows,
                            crate::volcano::type_lookup(col_types, target),
                        );
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
        col_types: &ColumnTypeMap,
    ) -> Result<ExecResult, String> {
        let refs: Vec<&Row> = rows.iter().collect();
        let mut col_names = Vec::new();
        let mut result_row: Vec<Cell> = Vec::new();

        for col in columns {
            if let SelectColumn::Aggregate(func, target) = col {
                let (name, val) = compute_aggregate(
                    func,
                    target,
                    &refs,
                    crate::volcano::type_lookup(col_types, target),
                );
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
        from: &FromClause,
    ) -> Result<(Vec<String>, Vec<Vec<Cell>>), String> {
        if columns.iter().any(|c| matches!(c, SelectColumn::Star)) {
            // Expand per table in FROM order, reading each column by its
            // qualified key. A join row carries both tables under
            // `table.column`, so this emits every column of both tables
            // instead of collapsing two same-named ones into a single
            // bare key.
            let mut names: Vec<String> = Vec::new();
            let mut keys: Vec<String> = Vec::new();
            for column in columns {
                match column {
                    SelectColumn::Star => {
                        for (table, cols) in &self.columns_per_table(from)? {
                            for col in cols {
                                names.push(col.clone());
                                keys.push(format!("{table}.{col}"));
                            }
                        }
                    }
                    SelectColumn::Named(name) => {
                        names.push(name.clone());
                        keys.push(name.clone());
                    }
                    SelectColumn::Qualified(table, name) => {
                        names.push(name.clone());
                        keys.push(format!("{table}.{name}"));
                    }
                    SelectColumn::Aggregate(func, target) => {
                        let key = format!("{}({})", format!("{:?}", func).to_lowercase(), target);
                        names.push(key.clone());
                        keys.push(key);
                    }
                    SelectColumn::WindowFunc { func, .. } => {
                        let name = match func {
                            WindowFuncType::RowNumber => "row_number",
                            WindowFuncType::Rank => "rank",
                            WindowFuncType::DenseRank => "dense_rank",
                        };
                        names.push(name.to_string());
                        keys.push(name.to_string());
                    }
                }
            }
            let result: Vec<Vec<Cell>> = rows
                .iter()
                .map(|r| {
                    keys.iter()
                        .map(|k| crate::volcano::row_lookup(r, k).map(str::to_string))
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
                    names.push(format!("{}({})", format!("{:?}", f).to_lowercase(), t))
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

        let result: Vec<Vec<Cell>> = rows
            .iter()
            .map(|r| {
                columns
                    .iter()
                    .map(|c| match c {
                        SelectColumn::Named(n) => r.get(n).cloned().flatten(),
                        SelectColumn::Qualified(t, n) => {
                            crate::volcano::row_lookup(r, &format!("{}.{}", t, n))
                                .map(str::to_string)
                        }
                        SelectColumn::Aggregate(func, target) => r
                            .get(&format!(
                                "{}({})",
                                format!("{:?}", func).to_lowercase(),
                                target
                            ))
                            .cloned()
                            .flatten(),
                        SelectColumn::WindowFunc { func, .. } => {
                            let key = match func {
                                WindowFuncType::RowNumber => "row_number",
                                WindowFuncType::Rank => "rank",
                                WindowFuncType::DenseRank => "dense_rank",
                            };
                            r.get(key).cloned().flatten()
                        }
                        _ => None,
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
        let table_lock = self.table_lock(table_name)?;
        let _table_guard = table_lock
            .lock()
            .map_err(|_| "SQL table lock poisoned".to_string())?;
        self.record_catalog_read(table_name);
        let read_seq = self.refresh_statement_seq_after_migration_lock();
        let table = self.resolve_dml_table(table_name, read_seq)?;
        self.validate_where_columns_of(table_name, where_clause)?;
        // Validate the assignments before filtering: a statement that
        // matches no rows would otherwise skip the loop and silently accept
        // an unknown column.
        let assignments: Vec<(&str, &SqlValue)> = assignments
            .iter()
            .map(|(col, val)| Ok((resolve_column_name(&table, col)?, val)))
            .collect::<Result<_, String>>()?;
        let mut rows = self.load_table_rows_at(&table, read_seq);
        let mut available_keys = std::collections::HashSet::new();
        for row in &rows {
            let Some(pk) = row.get(&table.primary_key).cloned().flatten() else {
                return Err(format!(
                    "table '{}' contains a row without a primary key; refusing to update",
                    table.name
                ));
            };
            available_keys.insert(format!(
                "{}{}",
                table.row_prefix(),
                crate::volcano::canonicalize_pk_key(&table, &pk)
            ));
        }

        if let Some(expr) = where_clause {
            let col_types = column_types_of_table(&table);
            rows.retain(|row| eval_where_typed(row, expr, &col_types));
        }

        // Remove the keys of rows this statement will rewrite. This permits a
        // no-op update of a row's existing key while still detecting a move
        // onto a key owned by an untouched row or another updated row.
        let mut old_keys = Vec::with_capacity(rows.len());
        for row in &rows {
            let pk = row
                .get(&table.primary_key)
                .cloned()
                .flatten()
                .ok_or_else(|| format!("primary key is NULL in table '{}'", table.name))?;
            let old_key = format!(
                "{}{}",
                table.row_prefix(),
                crate::volcano::canonicalize_pk_key(&table, &pk)
            );
            available_keys.remove(&old_key);
            old_keys.push(old_key);
        }

        // The migration batch lands first so its rewrite of an untouched
        // row loses to this statement's write of the same (canonical) key.
        let mut batch = if table.row_format == crate::catalog::RowFormat::Legacy {
            self.migration_batch(&table, read_seq)?
        } else {
            WriteBatch::new()
        };
        let count = rows.len();
        for (row, old_key) in rows.iter_mut().zip(old_keys) {
            for (resolved, val) in &assignments {
                let stored = match val {
                    SqlValue::Null => None,
                    v => {
                        let col = table.columns.iter().find(|c| c.name == *resolved);
                        Some(match col {
                            Some(c) => c.col_type.canonicalize(&v.as_string()),
                            None => v.as_string(),
                        })
                    }
                };
                row.insert((*resolved).to_string(), stored);
            }
            let pk = row
                .get(&table.primary_key)
                .cloned()
                .flatten()
                .ok_or_else(|| format!("primary key cannot be NULL in table '{}'", table.name))?;
            // A legacy row may be stored under a non-canonical key ("007");
            // the typed path addresses it as "7".
            let key = format!(
                "{}{}",
                table.row_prefix(),
                crate::volcano::canonicalize_pk_key(&table, &pk)
            );
            if !available_keys.insert(key.clone()) {
                return Err(format!(
                    "duplicate key value violates primary key of table '{}'",
                    table.name
                ));
            }
            if old_key != key {
                batch.delete(&old_key).map_err(|e| format!("{:?}", e))?;
            }
            let value = serde_json::to_string(&row).map_err(|e| format!("{}", e))?;
            batch.set(&key, value).map_err(|e| format!("{:?}", e))?;
        }

        if !batch.is_empty() {
            self.stage_or_commit(batch)?;
            if table.row_format == crate::catalog::RowFormat::Legacy {
                let mut migrated = table.clone();
                migrated.row_format = crate::catalog::RowFormat::Typed;
                self.catalog.insert_staged(migrated);
            }
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
        let table_lock = self.table_lock(table_name)?;
        let _table_guard = table_lock
            .lock()
            .map_err(|_| "SQL table lock poisoned".to_string())?;
        self.record_catalog_read(table_name);
        let read_seq = self.refresh_statement_seq_after_migration_lock();
        let table = self.resolve_dml_table(table_name, read_seq)?;
        self.validate_where_columns_of(table_name, where_clause)?;
        let mut rows = self.load_table_rows_at(&table, read_seq);

        if let Some(expr) = where_clause {
            let col_types = column_types_of_table(&table);
            rows.retain(|row| eval_where_typed(row, expr, &col_types));
        }

        // Deletes commit after the migration's rewrites inside one batch, so
        // a row the statement removes disappears even if migration moved it.
        let mut batch = if table.row_format == crate::catalog::RowFormat::Legacy {
            self.migration_batch(&table, read_seq)?
        } else {
            WriteBatch::new()
        };
        let count = rows.len();
        for row in &rows {
            let pk = row
                .get(&table.primary_key)
                .cloned()
                .flatten()
                .unwrap_or_default();
            let key = format!(
                "{}{}",
                table.row_prefix(),
                crate::volcano::canonicalize_pk_key(&table, &pk)
            );
            batch.delete(&key).map_err(|e| format!("{:?}", e))?;
        }

        if !batch.is_empty() {
            self.stage_or_commit(batch)?;
            if table.row_format == crate::catalog::RowFormat::Legacy {
                let mut migrated = table.clone();
                migrated.row_format = crate::catalog::RowFormat::Typed;
                self.catalog.insert_staged(migrated);
            }
        }
        Ok(ExecResult::Modified {
            count,
            command: format!("DELETE {}", count),
        })
    }
}

/// A table's column types keyed by both bare and qualified name, so a
/// `WHERE t.col` predicate compares by the column's affinity rather than
/// the literal's shape.
fn column_types_of_table(table: &TableDef) -> crate::volcano::ColumnTypeMap {
    let mut map = crate::volcano::ColumnTypeMap::new();
    for c in &table.columns {
        map.insert(c.name.clone(), c.col_type.clone());
        map.insert(format!("{}.{}", table.name, c.name), c.col_type.clone());
    }
    map
}

/// Parse the canonical internal spelling used for aggregate result columns,
/// such as `count(*)` or `sum(amount)` in HAVING and ORDER BY expressions.
fn parse_aggregate_reference(reference: &str) -> Option<(AggFunc, String)> {
    let (func, rest) = reference.split_once('(')?;
    if !rest.ends_with(')') {
        return None;
    }
    let target = &rest[..rest.len() - 1];
    let func = match func.to_ascii_lowercase().as_str() {
        "count" => AggFunc::Count,
        "sum" => AggFunc::Sum,
        "avg" => AggFunc::Avg,
        "min" => AggFunc::Min,
        "max" => AggFunc::Max,
        _ => return None,
    };
    if target.is_empty() {
        return None;
    }
    Some((func, target.to_string()))
}

/// Add a reference to the internal aggregate projection once. The final
/// projection still uses the user's original SELECT list, so these entries
/// are execution-only state for HAVING, sorting, and grouping.
fn append_plan_column(columns: &mut Vec<SelectColumn>, reference: &str) {
    let column = if let Some((func, target)) = parse_aggregate_reference(reference) {
        SelectColumn::Aggregate(func, target)
    } else if let Some((table, name)) = reference.split_once('.') {
        SelectColumn::Qualified(table.to_string(), name.to_string())
    } else {
        SelectColumn::Named(reference.to_string())
    };
    if !columns.iter().any(|existing| existing == &column) {
        columns.push(column);
    }
}

fn append_having_plan_columns(columns: &mut Vec<SelectColumn>, expr: &WhereExpr) {
    match expr {
        WhereExpr::Comparison { column, .. }
        | WhereExpr::IsNull(column)
        | WhereExpr::IsNotNull(column)
        | WhereExpr::In(column, _)
        | WhereExpr::InSubquery(column, _) => append_plan_column(columns, column),
        WhereExpr::And(left, right) | WhereExpr::Or(left, right) => {
            append_having_plan_columns(columns, left);
            append_having_plan_columns(columns, right);
        }
        WhereExpr::Not(inner) => append_having_plan_columns(columns, inner),
    }
}

fn same_column_reference(left: &str, right: &str) -> bool {
    if left == right {
        return true;
    }
    let left_bare = left.rsplit('.').next().unwrap_or(left);
    let right_bare = right.rsplit('.').next().unwrap_or(right);
    left_bare == right_bare
        && (left.split_once('.').is_none()
            || right.split_once('.').is_none()
            || left.split_once('.').zip(right.split_once('.')).is_some_and(
                |((left_table, _), (right_table, _))| left_table.eq_ignore_ascii_case(right_table),
            ))
}

/// Resolve a user-supplied column name to the table's declared spelling.
/// Rows are keyed by the declared name; an unknown column is an error.
fn resolve_column_name<'t>(table: &'t TableDef, col: &str) -> Result<&'t str, String> {
    table
        .columns
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(col))
        .map(|c| c.name.as_str())
        .ok_or_else(|| {
            format!(
                "column \"{}\" does not exist in table \"{}\"",
                col, table.name
            )
        })
}

/// Renders one result row as protocol strings. This is the boundary where
/// a SQL NULL becomes the text "NULL"; everything upstream keeps them apart.
pub fn render_row(row: &[Cell]) -> Vec<String> {
    row.iter()
        .map(|cell| cell.clone().unwrap_or_else(|| "NULL".into()))
        .collect()
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Column, RowFormat};
    use crate::sql::parse_sql;
    use std::sync::{Arc, Barrier, mpsc::channel};
    use std::time::Duration;

    fn legacy_table() -> TableDef {
        TableDef {
            name: "legacy_t".into(),
            columns: vec![
                Column {
                    name: "id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                    primary_key: true,
                    default: None,
                },
                Column {
                    name: "v".into(),
                    col_type: ColumnType::Text,
                    nullable: true,
                    primary_key: false,
                    default: None,
                },
            ],
            primary_key: "id".into(),
            created_at: 1,
            row_format: RowFormat::Legacy,
        }
    }

    fn rows(executor: &SqlExecutor, sql: &str) -> Vec<Vec<Cell>> {
        match executor.execute(&parse_sql(sql).expect("parse test query")) {
            Ok(ExecResult::Rows { rows, .. }) => rows,
            _ => panic!("expected rows for {sql:?}"),
        }
    }

    /// The catalog and row scan in one migration must use the same MVCC
    /// sequence. A commit that lands between them may contain typed rows;
    /// decoding those rows with legacy rules silently turns literal "NULL"
    /// text into SQL NULL.
    #[test]
    fn migration_uses_one_read_sequence_for_catalog_and_rows() {
        let _test_guard = migration_test_lock().lock().expect("migration test lock");
        let dir = tempfile::tempdir().expect("temporary database directory");
        let manifest = dir.path().join("manifest.json");
        let wal = dir.path().join("wal.bin");
        let db = OmniKV::open(
            manifest.to_str().expect("manifest path"),
            wal.to_str().expect("wal path"),
        )
        .expect("open database");

        let legacy = legacy_table();
        let typed = TableDef {
            row_format: RowFormat::Typed,
            ..legacy.clone()
        };
        let mut initial = WriteBatch::new();
        let (catalog_key, legacy_manifest) =
            Catalog::staged_create_entry(&legacy).expect("serialize legacy catalog");
        initial
            .set(&catalog_key, legacy_manifest)
            .expect("stage legacy catalog");
        initial
            .set(
                "\u{0}T\u{0}legacy_t\u{0}1",
                r#"{"id":"1","v":"NULL"}"#.to_string(),
            )
            .expect("stage legacy row");
        db.commit_batch(&initial).expect("commit legacy database");

        // Pause the migrating executor after it has read Legacy from the
        // catalog but before it scans rows. The direct batch below simulates
        // another executor completing the migration in that exact window.
        let (entered_tx, entered_rx) = channel();
        let (release_tx, release_rx) = channel();
        let hook = Arc::new(MigrationCatalogHook {
            table: "legacy_t".into(),
            armed: std::sync::atomic::AtomicBool::new(true),
            entered: entered_tx,
            release: std::sync::Mutex::new(release_rx),
        });
        *migration_catalog_hook_slot()
            .lock()
            .expect("migration hook lock") = Some(hook);

        let worker_db = db.clone();
        let worker = std::thread::spawn(move || {
            let executor =
                SqlExecutor::new(worker_db.clone(), Arc::new(Catalog::new(worker_db.clone())));
            executor.execute(
                &parse_sql("INSERT INTO legacy_t (id, v) VALUES (3, 'x')")
                    .expect("parse migration write"),
            )
        });

        let entered = entered_rx.recv_timeout(Duration::from_secs(1)).is_ok();

        // This is a fully typed row: under RowFormat::Typed, the literal
        // string "NULL" must remain text rather than becoming JSON null.
        if entered {
            let mut concurrent = WriteBatch::new();
            let (_, typed_manifest) =
                Catalog::staged_create_entry(&typed).expect("serialize typed catalog");
            concurrent
                .set(&catalog_key, typed_manifest)
                .expect("stage typed catalog");
            concurrent
                .set(
                    "\u{0}T\u{0}legacy_t\u{0}2",
                    r#"{"id":"2","v":"NULL"}"#.to_string(),
                )
                .expect("stage typed row");
            db.commit_batch(&concurrent)
                .expect("commit concurrent typed migration");
        }
        release_tx.send(()).expect("release migration worker");
        worker
            .join()
            .expect("migration worker did not panic")
            .expect("migration write failed");
        *migration_catalog_hook_slot()
            .lock()
            .expect("migration hook lock") = None;
        assert!(
            entered,
            "migration worker did not reach the synchronization hook"
        );

        let verifier = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db)));
        assert_eq!(
            rows(&verifier, "SELECT id FROM legacy_t WHERE v = 'NULL'"),
            vec![vec![Some("2".into())]],
            "the concurrent typed literal must not be decoded with legacy NULL rules"
        );
        assert_eq!(
            rows(&verifier, "SELECT id FROM legacy_t WHERE v IS NULL"),
            vec![vec![Some("1".into())]],
            "the original legacy NULL must remain SQL NULL after migration"
        );
    }

    /// Two SQL writers with stale legacy catalogs must serialize the first
    /// migration. The second writer waits for the table lock, refreshes its
    /// snapshot, and writes a typed literal instead of attempting a second
    /// legacy rewrite.
    #[test]
    fn concurrent_legacy_migrations_serialize_per_table() {
        let _test_guard = migration_test_lock().lock().expect("migration test lock");
        let dir = tempfile::tempdir().expect("temporary database directory");
        let manifest = dir.path().join("manifest.json");
        let wal = dir.path().join("wal.bin");
        let db = OmniKV::open(
            manifest.to_str().expect("manifest path"),
            wal.to_str().expect("wal path"),
        )
        .expect("open database");

        let legacy = legacy_table();
        let mut initial = WriteBatch::new();
        let (catalog_key, legacy_manifest) =
            Catalog::staged_create_entry(&legacy).expect("serialize legacy catalog");
        initial
            .set(&catalog_key, legacy_manifest)
            .expect("stage legacy catalog");
        initial
            .set(
                "\u{0}T\u{0}legacy_t\u{0}1",
                r#"{"id":"1","v":"NULL"}"#.to_string(),
            )
            .expect("stage legacy row");
        db.commit_batch(&initial).expect("commit legacy database");

        let first = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db.clone())));
        let second = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db.clone())));
        let (entered_tx, entered_rx) = channel();
        let (release_tx, release_rx) = channel();
        let hook = Arc::new(MigrationCatalogHook {
            table: "legacy_t".into(),
            armed: std::sync::atomic::AtomicBool::new(true),
            entered: entered_tx,
            release: std::sync::Mutex::new(release_rx),
        });
        *migration_catalog_hook_slot()
            .lock()
            .expect("migration hook lock") = Some(hook.clone());

        let first_thread = std::thread::spawn(move || {
            first.execute(
                &parse_sql("INSERT INTO legacy_t (id, v) VALUES (3, 'x')")
                    .expect("parse first migration"),
            )
        });
        let first_entered = entered_rx.recv_timeout(Duration::from_secs(1)).is_ok();

        let (ready_tx, ready_rx) = channel();
        let (done_tx, done_rx) = channel();
        let second_thread = std::thread::spawn(move || {
            ready_tx.send(()).expect("signal second writer");
            let result = second.execute(
                &parse_sql("INSERT INTO legacy_t (id, v) VALUES (4, 'NULL')")
                    .expect("parse second migration"),
            );
            done_tx.send(()).expect("signal second completion");
            result
        });
        ready_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("second writer did not start");
        let completed_before_release =
            first_entered && done_rx.recv_timeout(Duration::from_millis(100)).is_ok();

        release_tx.send(()).expect("release first migration");
        first_thread
            .join()
            .expect("first migration worker panicked")
            .expect("first migration failed");
        second_thread
            .join()
            .expect("second migration worker panicked")
            .expect("second migration failed");
        *migration_catalog_hook_slot()
            .lock()
            .expect("migration hook lock") = None;

        assert!(
            first_entered,
            "first migration worker did not reach the synchronization hook"
        );
        assert!(
            !completed_before_release,
            "the second writer completed while the first writer held the table lock"
        );
        let verifier = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db)));
        assert_eq!(
            rows(&verifier, "SELECT id FROM legacy_t WHERE v = 'NULL'"),
            vec![vec![Some("4".into())]],
            "the second writer must use typed NULL-literal semantics"
        );
        assert_eq!(
            rows(&verifier, "SELECT id FROM legacy_t WHERE v IS NULL"),
            vec![vec![Some("1".into())]],
            "the original legacy NULL must remain SQL NULL"
        );
    }

    /// Autocommit primary-key validation is serialized with the commit, so
    /// two concurrent inserts cannot both observe the key as unoccupied.
    #[test]
    fn concurrent_autocommit_inserts_reject_duplicate_primary_key() {
        let dir = tempfile::tempdir().expect("temporary database directory");
        let manifest = dir.path().join("manifest.json");
        let wal = dir.path().join("wal.bin");
        let db = OmniKV::open(
            manifest.to_str().expect("manifest path"),
            wal.to_str().expect("wal path"),
        )
        .expect("open database");
        let setup = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db.clone())));
        setup
            .execute(
                &parse_sql("CREATE TABLE unique_t (id INTEGER PRIMARY KEY, v TEXT)")
                    .expect("parse create"),
            )
            .expect("create table");

        let first = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db.clone())));
        let second = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db.clone())));
        let start = Arc::new(Barrier::new(3));
        let first_start = start.clone();
        let first_thread = std::thread::spawn(move || {
            first_start.wait();
            first.execute(
                &parse_sql("INSERT INTO unique_t (id, v) VALUES (1, 'a')")
                    .expect("parse first insert"),
            )
        });
        let second_start = start.clone();
        let second_thread = std::thread::spawn(move || {
            second_start.wait();
            second.execute(
                &parse_sql("INSERT INTO unique_t (id, v) VALUES (1, 'b')")
                    .expect("parse second insert"),
            )
        });
        start.wait();

        let first_result = first_thread
            .join()
            .expect("first duplicate insert worker panicked");
        let second_result = second_thread
            .join()
            .expect("second duplicate insert worker panicked");
        let error = match (first_result, second_result) {
            (Err(error), Ok(_)) | (Ok(_), Err(error)) => error,
            (Ok(_), Ok(_)) => panic!("both duplicate inserts committed"),
            (Err(first_error), Err(second_error)) => {
                panic!("both duplicate inserts failed: {first_error}; {second_error}")
            }
        };
        assert!(
            error.contains("duplicate key"),
            "the losing insert must report a duplicate key: {error}"
        );

        let verifier = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db)));
        assert_eq!(
            rows(&verifier, "SELECT id FROM unique_t"),
            vec![vec![Some("1".into())]],
            "the primary key must have one visible row"
        );
    }

    /// A storage failure after migration planning must not make the executor's
    /// cache claim that the legacy catalog was committed as typed.
    #[test]
    fn failed_migration_commit_does_not_update_catalog_cache() {
        let dir = tempfile::tempdir().expect("temporary database directory");
        let manifest = dir.path().join("manifest.json");
        let wal = dir.path().join("wal.bin");
        let db = OmniKV::open(
            manifest.to_str().expect("manifest path"),
            wal.to_str().expect("wal path"),
        )
        .expect("open database");

        let legacy = legacy_table();
        let mut initial = WriteBatch::new();
        let (catalog_key, legacy_manifest) =
            Catalog::staged_create_entry(&legacy).expect("serialize legacy catalog");
        initial
            .set(&catalog_key, legacy_manifest)
            .expect("stage legacy catalog");
        initial
            .set(
                "\u{0}T\u{0}legacy_t\u{0}1",
                r#"{"id":"1","v":"NULL"}"#.to_string(),
            )
            .expect("stage legacy row");
        for i in 0..3 {
            let mut batch = WriteBatch::new();
            batch
                .set(&format!("\u{0}ZSTALL\u{0}{i}"), format!("value-{i}"))
                .expect("stage stall marker");
            db.commit_batch(&batch).expect("commit stall marker");
            db.compact_sstables().expect("flush stall marker");
        }
        assert_eq!(db.sstable_count(), 3, "the test must reach write stall");
        db.set_compaction_policy(crate::CompactionPolicy {
            l0_compaction_trigger: 1,
            l1_compaction_trigger: 1,
            l0_write_stall_threshold: 4,
            write_stall_wait_attempts: 0,
            write_stall_wait_ms: 0,
        })
        .expect("set preflight write policy");
        db.commit_batch(&initial).expect("commit legacy database");
        db.set_compaction_policy(crate::CompactionPolicy {
            l0_compaction_trigger: 1,
            l1_compaction_trigger: 1,
            l0_write_stall_threshold: 3,
            write_stall_wait_attempts: 0,
            write_stall_wait_ms: 0,
        })
        .expect("set write-stall test policy");

        let executor = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db.clone())));
        let error = match executor.execute(
            &parse_sql("INSERT INTO legacy_t (id, v) VALUES (2, 'x')")
                .expect("parse failed migration"),
        ) {
            Err(error) => error,
            Ok(_) => panic!("the configured write stall must reject the migration commit"),
        };
        assert!(
            error.contains("WriteStall"),
            "unexpected storage error: {error}"
        );
        assert_eq!(
            executor
                .catalog
                .get_table("legacy_t")
                .expect("cached legacy table")
                .row_format,
            RowFormat::Legacy,
            "a failed commit must not update the catalog cache"
        );
    }
}
