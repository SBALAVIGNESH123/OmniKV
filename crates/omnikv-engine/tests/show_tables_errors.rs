//! SHOW TABLES must either return the complete visible catalog or an error.

use omni_engine::catalog::Catalog;
use omni_engine::sql::{SqlStatement, parse_sql};
use omni_engine::sql_exec::{ExecResult, SqlExecutor};
use omni_engine::{OmniKV, WriteBatch};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

fn environment() -> (TempDir, SqlExecutor) {
    let dir = tempfile::tempdir().expect("tempdir");
    let manifest = dir.path().join("manifest.json");
    let wal = dir.path().join("wal.bin");
    let db = OmniKV::open(manifest.to_str().unwrap(), wal.to_str().unwrap()).expect("open");
    let catalog = Arc::new(Catalog::new(db.clone()));
    (dir, SqlExecutor::new(db, catalog))
}

fn execute(executor: &SqlExecutor, sql: &str) {
    executor
        .execute(&parse_sql(sql).expect("parse SQL"))
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
}

fn show_tables(executor: &SqlExecutor) -> Vec<String> {
    match executor.execute(&SqlStatement::ShowTables) {
        Ok(ExecResult::Rows { columns, rows }) => {
            assert_eq!(columns, ["table_name"]);
            let mut names: Vec<_> = rows
                .into_iter()
                .map(|row| {
                    assert_eq!(row.len(), 1);
                    row.into_iter().next().unwrap().expect("table name")
                })
                .collect();
            names.sort();
            names
        }
        Ok(_) => panic!("SHOW TABLES must return rows"),
        Err(error) => panic!("SHOW TABLES failed: {error}"),
    }
}

fn show_tables_error(executor: &SqlExecutor) -> String {
    match executor.execute(&SqlStatement::ShowTables) {
        Err(error) => error,
        Ok(ExecResult::Rows { rows, .. }) => {
            panic!("SHOW TABLES must fail, not return a partial or empty catalog: {rows:?}")
        }
        Ok(_) => panic!("SHOW TABLES must fail"),
    }
}

#[derive(Clone, Copy, Debug)]
enum Damage {
    UnreadableHeap,
    PayloadCrc,
    InvalidJson,
}

fn damage_catalog(db: &OmniKV, name: &str, damage: Damage) {
    let key = Catalog::catalog_key(name);
    match damage {
        Damage::UnreadableHeap => {
            // A real selected storage record, whose payload lies past EOF.
            db.insert_replica_record(key.into_bytes(), db.get_seq() + 1, u64::MAX / 2, 1, 0, 0);
        }
        Damage::PayloadCrc => {
            let payload = b"catalog payload with a corrupt checksum";
            let offset = db.append_to_heap(payload).expect("append heap payload");
            db.insert_replica_record(
                key.into_bytes(),
                db.get_seq() + 1,
                offset,
                payload.len() as u64,
                crc32fast::hash(payload) ^ u32::MAX,
                0,
            );
        }
        Damage::InvalidJson => {
            let mut batch = WriteBatch::new();
            batch.set(&key, "{broken catalog JSON".into()).unwrap();
            db.commit_batch(&batch)
                .expect("commit malformed definition");
        }
    }
}

#[test]
fn show_tables_excludes_non_catalog_values_after_compaction() {
    let (_dir, executor) = environment();
    execute(&executor, "CREATE TABLE valid (id INTEGER PRIMARY KEY)");
    let mut batch = WriteBatch::new();
    batch
        .set("\x00BEFORE_CATALOG", "not a table definition".into())
        .unwrap();
    executor.db.commit_batch(&batch).unwrap();
    assert_eq!(show_tables(&executor), ["valid"]);

    // Sparse-index seeks can rewind to an earlier key in the same block.
    // No persisted tier may pass that unrelated value to the catalog decoder.
    executor.db.compact_sstables().unwrap();
    assert_eq!(show_tables(&executor), ["valid"]);
    executor.db.compact_l0_to_l1().unwrap();
    assert_eq!(show_tables(&executor), ["valid"]);
    executor.db.compact_l1_to_l2().unwrap();
    assert_eq!(show_tables(&executor), ["valid"]);
}

#[test]
fn show_tables_empty_valid_and_snapshot_catalogs() {
    let (_dir, executor) = environment();
    let no_tables = Vec::<String>::new();
    let empty = executor.db.snapshot();
    assert_eq!(show_tables(&executor), no_tables);
    assert_eq!(executor.catalog.list_tables_at(empty).unwrap(), no_tables);

    execute(&executor, "CREATE TABLE Alpha (id INTEGER PRIMARY KEY)");
    execute(&executor, "CREATE TABLE beta (id INTEGER PRIMARY KEY)");
    let populated = executor.db.snapshot();
    assert_eq!(show_tables(&executor), ["alpha", "beta"]);
    assert_eq!(
        executor.catalog.list_tables_at(populated).unwrap(),
        ["alpha", "beta"]
    );
    assert_eq!(executor.catalog.list_tables_at(empty).unwrap(), no_tables);

    execute(&executor, "DROP TABLE alpha");
    execute(&executor, "DROP TABLE beta");
    assert_eq!(show_tables(&executor), no_tables);
    assert_eq!(
        executor
            .catalog
            .list_tables_at(executor.db.get_seq())
            .unwrap(),
        no_tables
    );
    assert_eq!(
        executor.catalog.list_tables_at(populated).unwrap(),
        ["alpha", "beta"]
    );
    executor.db.unregister_snapshot(empty);
    executor.db.unregister_snapshot(populated);
}

#[test]
fn list_tables_at_rejects_unreadable_or_corrupt_catalog_without_partial_success() {
    for damage in [
        Damage::UnreadableHeap,
        Damage::PayloadCrc,
        Damage::InvalidJson,
    ] {
        // Exercise a bad entry both before and after a valid one. The latter
        // catches returning a successful prefix after a lazy heap read fails.
        for name in ["a_broken", "z_broken"] {
            let (_dir, executor) = environment();
            execute(&executor, "CREATE TABLE middle (id INTEGER PRIMARY KEY)");
            damage_catalog(&executor.db, name, damage);

            let stored = executor
                .db
                .find(&Catalog::catalog_key(name), executor.db.get_seq());
            match damage {
                Damage::UnreadableHeap | Damage::PayloadCrc => assert!(stored.is_err()),
                Damage::InvalidJson => assert!(stored.unwrap().is_some()),
            }
            let error = executor
                .catalog
                .list_tables_at(executor.db.get_seq())
                .expect_err("catalog listing must not succeed with missing tables");
            assert!(error.contains("Catalog"), "{damage:?}: {error}");
        }
    }
}

#[test]
fn show_tables_rejects_unreadable_or_corrupt_catalog_without_partial_success() {
    for damage in [
        Damage::UnreadableHeap,
        Damage::PayloadCrc,
        Damage::InvalidJson,
    ] {
        for name in ["a_broken", "z_broken"] {
            let (_dir, executor) = environment();
            execute(&executor, "CREATE TABLE middle (id INTEGER PRIMARY KEY)");
            damage_catalog(&executor.db, name, damage);

            // This executor's cache predates the corrupt entry: SHOW must
            // inspect storage, rather than successfully returning that cache.
            let error = show_tables_error(&executor);
            assert!(error.contains("Catalog"), "{damage:?}: {error}");

            let snapshot = executor.db.snapshot();
            let transactional = SqlExecutor::with_transaction(
                executor.db.clone(),
                Arc::new(Catalog::at_snapshot(executor.db.clone(), snapshot)),
                snapshot,
                &HashMap::new(),
            );
            let error = show_tables_error(&transactional);
            assert!(error.contains("Catalog"), "transaction {damage:?}: {error}");
            executor.db.unregister_snapshot(snapshot);
        }
    }
}

#[test]
fn show_tables_transaction_preserves_staged_create_drop_and_snapshot_visibility() {
    let (_dir, executor) = environment();
    execute(&executor, "CREATE TABLE keep (id INTEGER PRIMARY KEY)");
    execute(&executor, "CREATE TABLE remove_me (id INTEGER PRIMARY KEY)");
    let snapshot = executor.db.snapshot();
    let mut transaction = SqlExecutor::with_transaction(
        executor.db.clone(),
        Arc::new(Catalog::at_snapshot(executor.db.clone(), snapshot)),
        snapshot,
        &HashMap::new(),
    );
    execute(&transaction, "CREATE TABLE staged (id INTEGER PRIMARY KEY)");
    execute(&transaction, "DROP TABLE remove_me");
    assert_eq!(show_tables(&transaction), ["keep", "staged"]);
    assert_eq!(show_tables(&executor), ["keep", "remove_me"]);

    // Rebuild the next statement's executor as PgWire does, with staged DDL
    // from earlier statements in the transaction's pending write set.
    let pending = transaction.flush_batch().expect("staged transaction batch");
    let mut staged = HashMap::new();
    for (key, value, ttl) in pending.buffered_writes {
        staged.insert(String::from_utf8(key).unwrap(), (Some(value), ttl));
    }
    for key in pending.buffered_deletes {
        staged.insert(String::from_utf8(key).unwrap(), (None, 0));
    }
    execute(&executor, "DROP TABLE keep");
    execute(&executor, "CREATE TABLE later (id INTEGER PRIMARY KEY)");
    let next_statement = SqlExecutor::with_transaction(
        executor.db.clone(),
        Arc::new(Catalog::at_snapshot(executor.db.clone(), snapshot)),
        snapshot,
        &staged,
    );
    assert_eq!(show_tables(&next_statement), ["keep", "staged"]);
    assert_eq!(show_tables(&executor), ["later", "remove_me"]);
    assert_eq!(
        executor.catalog.list_tables_at(snapshot).unwrap(),
        ["keep", "remove_me"]
    );
    executor.db.unregister_snapshot(snapshot);
}

#[test]
fn show_tables_rejects_malformed_staged_catalog_definition() {
    let (_dir, executor) = environment();
    execute(&executor, "CREATE TABLE valid (id INTEGER PRIMARY KEY)");
    let staged = HashMap::from([(
        Catalog::catalog_key("broken"),
        (Some("not a table definition".into()), 0),
    )]);
    let transaction = SqlExecutor::with_transaction(
        executor.db.clone(),
        Arc::new(Catalog::at_snapshot(
            executor.db.clone(),
            executor.db.get_seq(),
        )),
        executor.db.get_seq(),
        &staged,
    );
    let error = show_tables_error(&transaction);
    assert!(error.contains("Catalog"), "{error}");
}
