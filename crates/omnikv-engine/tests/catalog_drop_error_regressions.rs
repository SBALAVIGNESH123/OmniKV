use omni_engine::OmniKV;
use omni_engine::catalog::Catalog;
use omni_engine::sql::parse_sql;
use omni_engine::sql_exec::SqlExecutor;
use std::sync::Arc;
use tempfile::TempDir;

fn environment() -> (TempDir, Arc<OmniKV>, Arc<Catalog>, SqlExecutor) {
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
    (dir, db, catalog, executor)
}

fn execute(executor: &SqlExecutor, sql: &str) {
    executor
        .execute(&parse_sql(sql).expect("parse SQL"))
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
}

fn inject_unreadable_row(
    db: &OmniKV,
    catalog: &Catalog,
    table_name: &str,
    primary_key: &str,
) -> String {
    let table = catalog
        .get_table(table_name)
        .expect("table definition should exist");
    let row_key = format!("{}{}", table.row_prefix(), primary_key);
    // Insert a selected record whose payload lies past EOF. The original row
    // was created through the normal SQL write path above.
    db.insert_replica_record(row_key.into_bytes(), db.get_seq(), u64::MAX, 1, 0, 0);
    format!("{}{}", table.row_prefix(), primary_key)
}

#[test]
fn drop_table_returns_row_scan_error_and_keeps_catalog_entry() {
    let (_dir, db, catalog, executor) = environment();
    execute(
        &executor,
        "CREATE TABLE corrupt (id INTEGER PRIMARY KEY, value TEXT)",
    );
    execute(
        &executor,
        "INSERT INTO corrupt (id, value) VALUES (7, 'present')",
    );
    let row_key = inject_unreadable_row(&db, &catalog, "corrupt", "7");

    let error = catalog
        .drop_table("corrupt")
        .expect_err("DROP TABLE must return a row-scan error");
    assert_ne!(error, "");
    assert!(
        catalog.get_table("corrupt").is_some(),
        "failed DROP TABLE must retain the cached catalog entry"
    );
    assert!(
        db.find(&Catalog::catalog_key("corrupt"), db.get_seq())
            .expect("catalog entry should remain readable")
            .is_some(),
        "failed DROP TABLE must not delete the persisted catalog entry"
    );
    assert!(
        db.find(&row_key, db.get_seq()).is_err(),
        "failed DROP TABLE must not delete rows after a scan error"
    );
}

#[test]
fn drop_table_still_removes_rows_and_catalog_entry_after_successful_scan() {
    let (_dir, db, catalog, executor) = environment();
    execute(
        &executor,
        "CREATE TABLE clean (id INTEGER PRIMARY KEY, value TEXT)",
    );
    execute(
        &executor,
        "INSERT INTO clean (id, value) VALUES (7, 'present')",
    );
    let table = catalog.get_table("clean").expect("table definition");
    let row_key = format!("{}{}", table.row_prefix(), "7");

    catalog
        .drop_table("clean")
        .expect("DROP TABLE should succeed");

    assert!(catalog.get_table("clean").is_none());
    assert_eq!(
        db.find(&Catalog::catalog_key("clean"), db.get_seq())
            .expect("catalog tombstone should be readable"),
        None
    );
    assert_eq!(
        db.find(&row_key, db.get_seq())
            .expect("row tombstone should be readable"),
        None
    );
}
