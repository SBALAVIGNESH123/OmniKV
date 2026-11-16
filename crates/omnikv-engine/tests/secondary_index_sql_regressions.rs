use omni_engine::catalog::Catalog;
use omni_engine::secondary_index::{IndexFieldType, IndexManager};
use omni_engine::sql::parse_sql;
use omni_engine::sql_exec::{ExecResult, SqlExecutor};
use omni_engine::transaction::TransactionManager;
use omni_engine::{OmniKV, WriteBatch};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

fn setup() -> (Arc<OmniKV>, TempDir, SqlExecutor) {
    let dir = TempDir::new().unwrap();
    let db = OmniKV::open(
        dir.path().join("manifest.json").to_str().unwrap(),
        dir.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();
    let executor = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db.clone())));
    execute(
        &executor,
        "CREATE TABLE items (id TEXT PRIMARY KEY, label TEXT, n INTEGER)",
    );
    execute(
        &executor,
        "INSERT INTO items VALUES ('a', 'red', 20), ('ab', 'red', 30), ('b', NULL, NULL), ('c', '12', 40)",
    );
    (db, dir, executor)
}

fn execute(executor: &SqlExecutor, sql: &str) {
    executor
        .execute(&parse_sql(sql).unwrap())
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
}

fn rows(executor: &SqlExecutor, sql: &str) -> Vec<Vec<Option<String>>> {
    let ExecResult::Rows { rows, .. } = executor
        .execute(&parse_sql(sql).unwrap())
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
    else {
        panic!("expected rows for {sql}");
    };
    rows
}

fn create_index(manager: &IndexManager, field: &str, field_type: IndexFieldType, unique: bool) {
    let name = format!("items_{field}_idx");
    manager
        .create_index(&name, "items", vec![(field.into(), field_type)], unique)
        .unwrap();
    manager.rebuild_index(&name).unwrap();
}

#[test]
fn indexed_queries_match_sequential_queries_for_nulls_disjunctions_and_prefix_keys() {
    let (db, _dir, executor) = setup();
    let queries = [
        "SELECT id FROM items WHERE label = 'red' ORDER BY id",
        "SELECT id FROM items WHERE label IS NULL ORDER BY id",
        "SELECT id FROM items WHERE label = 'red' OR label IS NULL ORDER BY id",
        "SELECT id FROM items WHERE label = 12 ORDER BY id",
        "SELECT id FROM items WHERE n >= 25 ORDER BY id",
        "SELECT id FROM items WHERE n >= 25 OR n IS NULL ORDER BY id",
        "SELECT id FROM items WHERE n = 20.5 ORDER BY id",
    ];
    let expected: Vec<_> = queries.iter().map(|query| rows(&executor, query)).collect();
    let manager = IndexManager::new(db);
    create_index(&manager, "label", IndexFieldType::String, false);
    create_index(&manager, "n", IndexFieldType::Integer, false);
    for (query, expected) in queries.iter().zip(expected) {
        assert_eq!(rows(&executor, query), expected, "{query}");
    }
}

#[test]
fn incomplete_index_build_cannot_hide_existing_sql_rows() {
    let (db, _dir, executor) = setup();
    let query = "SELECT id FROM items WHERE label = 'red' ORDER BY id";
    let expected = rows(&executor, query);
    let manager = IndexManager::new(db);
    manager
        .create_index(
            "items_label_idx",
            "items",
            vec![("label".into(), IndexFieldType::String)],
            false,
        )
        .unwrap();
    assert_eq!(
        rows(&executor, query),
        expected,
        "an unbuilt index is not a valid access path"
    );
    manager.rebuild_index("items_label_idx").unwrap();
    assert_eq!(rows(&executor, query), expected);
}

#[test]
fn sql_unique_index_rejects_duplicates_in_one_multirow_statement() {
    let (db, _dir, executor) = setup();
    let manager = IndexManager::new(db);
    create_index(&manager, "n", IndexFieldType::Integer, true);
    let result = executor.execute(
        &parse_sql("INSERT INTO items VALUES ('d', 'first', 50), ('e', 'second', 50)").unwrap(),
    );
    assert!(
        result.is_err(),
        "duplicate indexed values must fail atomically"
    );
    assert_eq!(
        rows(&executor, "SELECT id FROM items WHERE n = 50"),
        Vec::<Vec<Option<String>>>::new()
    );
}

#[test]
fn legacy_table_index_rebuild_and_dml_migration_preserve_index_lookup() {
    let dir = TempDir::new().unwrap();
    let db = OmniKV::open(
        dir.path().join("manifest.json").to_str().unwrap(),
        dir.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();
    let manifest = "{\"name\":\"legacy_idx\",\"columns\":[{\"name\":\"id\",\"col_type\":\"Integer\",\"nullable\":false,\"primary_key\":true,\"default\":null},{\"name\":\"email\",\"col_type\":\"Text\",\"nullable\":true,\"primary_key\":false,\"default\":null}],\"primary_key\":\"id\",\"created_at\":1000}";
    let mut wb = WriteBatch::new();
    wb.set("\u{0}CATALOG\u{0}legacy_idx", manifest.to_string())
        .unwrap();
    wb.set(
        "\u{0}T\u{0}legacy_idx\u{0}007",
        "{\"id\":\"007\",\"email\":\"bond@mi6.test\"}".to_string(),
    )
    .unwrap();
    db.commit_batch(&wb).unwrap();

    let manager = IndexManager::new(db.clone());
    manager
        .create_index(
            "legacy_idx_email_idx",
            "legacy_idx",
            vec![("email".into(), IndexFieldType::String)],
            true,
        )
        .unwrap();
    manager.rebuild_index("legacy_idx_email_idx").unwrap();

    let executor = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db)));
    execute(
        &executor,
        "INSERT INTO legacy_idx (id, email) VALUES (8, 'eight@mi6.test')",
    );
    assert_eq!(
        rows(
            &executor,
            "SELECT id FROM legacy_idx WHERE email = 'bond@mi6.test'"
        ),
        vec![vec![Some("7".to_string())]]
    );
    assert_eq!(
        rows(
            &executor,
            "SELECT id FROM legacy_idx WHERE email = 'eight@mi6.test'"
        ),
        vec![vec![Some("8".to_string())]]
    );

    execute(
        &executor,
        "UPDATE legacy_idx SET email = '007@mi6.test' WHERE id = 7",
    );
    assert_eq!(
        rows(
            &executor,
            "SELECT id FROM legacy_idx WHERE email = 'bond@mi6.test'"
        ),
        Vec::<Vec<Option<String>>>::new()
    );
    assert_eq!(
        rows(
            &executor,
            "SELECT id FROM legacy_idx WHERE email = '007@mi6.test'"
        ),
        vec![vec![Some("7".to_string())]]
    );
    assert_eq!(
        manager
            .lookup("legacy_idx_email_idx", &["bond@mi6.test".into()])
            .unwrap(),
        Vec::<String>::new()
    );
    assert_eq!(
        manager
            .lookup("legacy_idx_email_idx", &["007@mi6.test".into()])
            .unwrap(),
        vec!["7".to_string()]
    );
}

#[test]
fn concurrent_ssi_unique_index_inserts_with_non_ascii_pk_abort_conflicting_txn() {
    let (db, _dir, executor) = setup();
    let manager = IndexManager::new(db.clone());
    create_index(&manager, "n", IndexFieldType::Integer, true);

    let mgr = TransactionManager::new(db.clone());
    let mut t1 = mgr.begin();
    let mut t2 = mgr.begin();

    let mut exec1 = SqlExecutor::with_transaction(
        db.clone(),
        Arc::new(Catalog::at_snapshot(db.clone(), t1.read_seq)),
        t1.read_seq,
        &HashMap::new(),
    );
    let mut exec2 = SqlExecutor::with_transaction(
        db.clone(),
        Arc::new(Catalog::at_snapshot(db, t2.read_seq)),
        t2.read_seq,
        &HashMap::new(),
    );

    execute(&exec1, "INSERT INTO items VALUES ('α-clé', 'first', 999)");
    execute(&exec2, "INSERT INTO items VALUES ('β-clé', 'second', 999)");

    if let Some(reads) = exec1.drain_reads() {
        t1.read_set.extend(reads.keys);
        t1.read_ranges.extend(reads.ranges);
    }
    if let Some(batch) = exec1.flush_batch() {
        for (k, v, ttl) in batch.buffered_writes {
            t1.write_set
                .insert(String::from_utf8(k).unwrap(), (Some(v), ttl));
        }
        for k in batch.buffered_deletes {
            t1.write_set
                .insert(String::from_utf8(k).unwrap(), (None, 0));
        }
    }

    if let Some(reads) = exec2.drain_reads() {
        t2.read_set.extend(reads.keys);
        t2.read_ranges.extend(reads.ranges);
    }
    if let Some(batch) = exec2.flush_batch() {
        for (k, v, ttl) in batch.buffered_writes {
            t2.write_set
                .insert(String::from_utf8(k).unwrap(), (Some(v), ttl));
        }
        for k in batch.buffered_deletes {
            t2.write_set
                .insert(String::from_utf8(k).unwrap(), (None, 0));
        }
    }

    mgr.commit(&mut t1)
        .expect("first concurrent unique insert commits");
    let second = mgr.commit(&mut t2);
    assert!(
        second.is_err(),
        "second concurrent unique insert must abort under SSI: {second:?}"
    );
    assert_eq!(
        rows(&executor, "SELECT id FROM items WHERE n = 999"),
        vec![vec![Some("α-clé".to_string())]]
    );
}
