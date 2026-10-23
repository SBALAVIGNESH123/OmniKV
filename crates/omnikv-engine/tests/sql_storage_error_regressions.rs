use omni_engine::OmniKV;
use omni_engine::catalog::{Catalog, TableDef};
use omni_engine::optimizer::{Optimizer, gather_stats};
use omni_engine::sql::parse_sql;
use omni_engine::sql_exec::{ExecResult, SqlExecutor};
use std::sync::Arc;

fn create_sql_env() -> (Arc<OmniKV>, Arc<omni_engine::catalog::Catalog>, SqlExecutor) {
    let dir = tempfile::tempdir().expect("temporary database directory");
    let manifest = dir.path().join("manifest.json");
    let wal = dir.path().join("wal.bin");
    let db = OmniKV::open(
        manifest.to_str().expect("manifest path"),
        wal.to_str().expect("wal path"),
    )
    .expect("open database");
    let catalog = Arc::new(Catalog::new(db.clone()));
    let executor = SqlExecutor::new(db.clone(), catalog.clone());
    std::mem::forget(dir);
    (db, catalog, executor)
}

fn execute(executor: &SqlExecutor, sql: &str) -> Result<ExecResult, String> {
    executor.execute(&parse_sql(sql).expect("parse SQL"))
}

fn inject_unreadable_row(db: &OmniKV, table: &TableDef, primary_key: &str) {
    let sequence = db.get_seq();
    let key = format!("{}{}", table.row_prefix(), primary_key);
    // The record is valid in the memtable metadata but points past the heap,
    // so any all-or-error scan must return the heap I/O failure.
    db.insert_replica_record(key.into_bytes(), sequence, u64::MAX, 1, 0, 0);
}

#[test]
fn select_full_scan_reports_unreadable_heap_record() {
    let (db, catalog, executor) = create_sql_env();
    execute(
        &executor,
        "CREATE TABLE corrupt (id INTEGER PRIMARY KEY, value TEXT)",
    )
    .expect("create table");
    let table = catalog.get_table("corrupt").expect("table definition");
    inject_unreadable_row(&db, &table, "7");

    let result = execute(&executor, "SELECT id FROM corrupt");
    assert!(result.is_err(), "a corrupt full scan must not look empty");
}

#[test]
fn select_limit_zero_does_not_hide_scan_error() {
    let (db, catalog, executor) = create_sql_env();
    execute(
        &executor,
        "CREATE TABLE corrupt (id INTEGER PRIMARY KEY, value TEXT)",
    )
    .expect("create table");
    let table = catalog.get_table("corrupt").expect("table definition");
    inject_unreadable_row(&db, &table, "7");

    let result = execute(&executor, "SELECT id FROM corrupt LIMIT 0");
    assert!(result.is_err(), "LIMIT 0 must not hide a scan error");
}

#[test]
fn select_primary_key_lookup_reports_unreadable_heap_record() {
    let (db, catalog, executor) = create_sql_env();
    execute(
        &executor,
        "CREATE TABLE corrupt (id INTEGER PRIMARY KEY, value TEXT)",
    )
    .expect("create table");
    let table = catalog.get_table("corrupt").expect("table definition");
    inject_unreadable_row(&db, &table, "7");

    let result = execute(&executor, "SELECT value FROM corrupt WHERE id = 7");
    assert!(result.is_err(), "a corrupt PK lookup must not look missing");
}

#[test]
fn explain_analyze_reports_unreadable_heap_record() {
    let (db, catalog, executor) = create_sql_env();
    execute(
        &executor,
        "CREATE TABLE corrupt (id INTEGER PRIMARY KEY, value TEXT)",
    )
    .expect("create table");
    let table = catalog.get_table("corrupt").expect("table definition");
    inject_unreadable_row(&db, &table, "7");

    let result = execute(&executor, "EXPLAIN ANALYZE SELECT id FROM corrupt");
    assert!(
        result.is_err(),
        "EXPLAIN ANALYZE must return the scan error"
    );
}

#[test]
fn explain_reports_unreadable_heap_record() {
    let (db, catalog, executor) = create_sql_env();
    execute(
        &executor,
        "CREATE TABLE corrupt (id INTEGER PRIMARY KEY, value TEXT)",
    )
    .expect("create table");
    let table = catalog.get_table("corrupt").expect("table definition");
    inject_unreadable_row(&db, &table, "7");

    let result = execute(&executor, "EXPLAIN SELECT id FROM corrupt");
    assert!(
        result.is_err(),
        "EXPLAIN must return the statistics scan error"
    );
}

#[test]
fn update_read_reports_unreadable_heap_record() {
    let (db, catalog, executor) = create_sql_env();
    execute(
        &executor,
        "CREATE TABLE corrupt (id INTEGER PRIMARY KEY, value TEXT)",
    )
    .expect("create table");
    let table = catalog.get_table("corrupt").expect("table definition");
    inject_unreadable_row(&db, &table, "7");

    let result = execute(&executor, "UPDATE corrupt SET value = 'fixed' WHERE id = 7");
    assert!(
        result.is_err(),
        "an update must not report zero rows after a corrupt read"
    );
}

#[test]
fn sql_drop_reports_unreadable_heap_record_and_keeps_schema() {
    let (db, catalog, executor) = create_sql_env();
    execute(
        &executor,
        "CREATE TABLE corrupt (id INTEGER PRIMARY KEY, value TEXT)",
    )
    .expect("create table");
    let table = catalog.get_table("corrupt").expect("table definition");
    inject_unreadable_row(&db, &table, "7");

    let result = execute(&executor, "DROP TABLE corrupt");
    assert!(result.is_err(), "DROP must not hide a corrupt row scan");
    assert!(
        catalog.get_table("corrupt").is_some(),
        "a failed DROP must retain the cached schema"
    );
    assert!(
        db.find(&Catalog::catalog_key("corrupt"), db.get_seq())
            .expect("catalog lookup")
            .is_some(),
        "a failed DROP must retain the persisted schema"
    );
}

#[test]
fn plan_executor_reports_unreadable_heap_record() {
    let (db, catalog, executor) = create_sql_env();
    execute(
        &executor,
        "CREATE TABLE corrupt (id INTEGER PRIMARY KEY, value TEXT)",
    )
    .expect("create table");
    let table = catalog.get_table("corrupt").expect("table definition");
    inject_unreadable_row(&db, &table, "7");

    let statement = parse_sql("SELECT id FROM corrupt").expect("parse SQL");
    let plan = Optimizer::new(gather_stats(&catalog, None, &db))
        .optimize(&statement)
        .expect("build plan");
    let result =
        omni_engine::plan_exec::PlanExecutor::for_plan(db, catalog, &plan).execute_plan(&plan);
    assert!(
        result.is_err(),
        "plan execution must not hide a corrupt heap record"
    );
}
