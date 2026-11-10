use omni_engine::OmniKV;
use omni_engine::catalog::Catalog;
use omni_engine::secondary_index::{IndexFieldType, IndexManager};
use omni_engine::sql::parse_sql;
use omni_engine::sql_exec::{ExecResult, SqlExecutor};
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
