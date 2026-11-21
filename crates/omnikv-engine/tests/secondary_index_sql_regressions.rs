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

#[test]
fn transactional_update_primary_key_shift_retains_moved_rows() {
    let (db, _dir, _executor) = setup();
    let mgr = TransactionManager::new(db.clone());
    let mut txn = mgr.begin();
    let mut tx_exec = SqlExecutor::with_transaction(
        db.clone(),
        Arc::new(Catalog::at_snapshot(db, txn.read_seq)),
        txn.read_seq,
        &HashMap::new(),
    );
    execute(
        &tx_exec,
        "CREATE TABLE shift_pk (id INTEGER PRIMARY KEY, label TEXT)",
    );
    execute(
        &tx_exec,
        "INSERT INTO shift_pk VALUES (1, 'one'), (2, 'two')",
    );
    execute(&tx_exec, "UPDATE shift_pk SET id = 3 WHERE id = 2");
    execute(&tx_exec, "UPDATE shift_pk SET id = 2 WHERE id = 1");
    assert_eq!(
        rows(&tx_exec, "SELECT id, label FROM shift_pk ORDER BY id"),
        vec![
            vec![Some("2".to_string()), Some("one".to_string())],
            vec![Some("3".to_string()), Some("two".to_string())],
        ]
    );
    let _ = (&mut txn, &mut tx_exec);
}

#[test]
fn remove_field_resolves_case_insensitive_sql_column_and_2pc_tracks_plain_writes() {
    use omni_engine::dist_txn::{DistWrite, TwoPhaseParticipant, Vote};
    use omni_engine::schema::{Migration, SchemaManager, SchemaOp};

    let (db, _dir, executor) = setup();
    let schema = SchemaManager::new(db.clone(), Arc::new(IndexManager::new(db.clone())));
    schema
        .migrate_up(&Migration {
            version: 1,
            description: "remove label with different case".into(),
            up: vec![SchemaOp::RemoveField {
                collection: "items".into(),
                field_name: "LABEL".into(),
            }],
            down: vec![],
        })
        .expect("case-insensitive RemoveField should succeed");

    let catalog = Catalog::new(db.clone());
    let table = catalog.get_table("items").expect("items table");
    assert!(
        !table
            .columns
            .iter()
            .any(|c| c.name.eq_ignore_ascii_case("label"))
    );
    let _ = executor;

    let participant = TwoPhaseParticipant::new(1, db.clone());
    let snap = db.get_seq().saturating_sub(1);
    let mut plain_batch = WriteBatch::new();
    plain_batch
        .set("plain_conflict_k", "plain_v".into())
        .unwrap();
    db.commit_batch(&plain_batch).unwrap();

    let res = participant.prepare_with_snapshot(
        (1, 100),
        &[DistWrite {
            node_id: 1,
            key: "plain_conflict_k".into(),
            value: Some("2pc_v".into()),
            ttl: 0,
        }],
        snap,
    );
    assert!(
        matches!(res.vote, Vote::Abort(_)),
        "prepare_with_snapshot must abort when a plain write modified the key after read_seq"
    );
}

#[test]
fn remove_field_rejects_indexed_sql_column_until_index_is_dropped() {
    use omni_engine::schema::{Migration, SchemaManager, SchemaOp};

    let (db, _dir, _executor) = setup();
    let idx_mgr = Arc::new(IndexManager::new(db.clone()));
    create_index(&idx_mgr, "n", IndexFieldType::Integer, false);
    let schema = SchemaManager::new(db.clone(), idx_mgr.clone());

    let err = schema
        .migrate_up(&Migration {
            version: 1,
            description: "attempt removing indexed column".into(),
            up: vec![SchemaOp::RemoveField {
                collection: "ITEMS".into(),
                field_name: "N".into(),
            }],
            down: vec![],
        })
        .unwrap_err();
    assert!(
        err.to_string().contains("items_n_idx"),
        "expected dependent index rejection, got: {err}"
    );

    idx_mgr.drop_index("items_n_idx").unwrap();
    schema
        .migrate_up(&Migration {
            version: 1,
            description: "remove unindexed column and recreate as text".into(),
            up: vec![
                SchemaOp::RemoveField {
                    collection: "ITEMS".into(),
                    field_name: "N".into(),
                },
                SchemaOp::AddField {
                    collection: "ITEMS".into(),
                    field_name: "n".into(),
                    default_value: serde_json::Value::String("new".into()),
                },
            ],
            down: vec![],
        })
        .expect("removing and recreating column as text succeeds once index is dropped");

    let executor = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db)));
    assert_eq!(
        rows(&executor, "SELECT n FROM items WHERE id = 'a'"),
        vec![vec![Some("new".to_string())]]
    );
}

#[test]
fn add_field_rejects_literal_null_default_on_legacy_sql_table() {
    use omni_engine::schema::{Migration, SchemaManager, SchemaOp};

    let dir = TempDir::new().unwrap();
    let db = OmniKV::open(
        dir.path().join("manifest.json").to_str().unwrap(),
        dir.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();
    let manifest = "{\"name\":\"legacy_null_def\",\"columns\":[{\"name\":\"id\",\"col_type\":\"Text\",\"nullable\":false,\"primary_key\":true,\"default\":null}],\"primary_key\":\"id\",\"created_at\":1000}";
    let mut wb = WriteBatch::new();
    wb.set("\u{0}CATALOG\u{0}legacy_null_def", manifest.to_string())
        .unwrap();
    wb.set(
        "\u{0}T\u{0}legacy_null_def\u{0}r1",
        "{\"id\":\"r1\"}".to_string(),
    )
    .unwrap();
    db.commit_batch(&wb).unwrap();

    let schema = SchemaManager::new(db.clone(), Arc::new(IndexManager::new(db.clone())));
    let err = schema
        .migrate_up(&Migration {
            version: 1,
            description: "literal NULL default on legacy table".into(),
            up: vec![SchemaOp::AddField {
                collection: "LEGACY_NULL_DEF".into(),
                field_name: "note".into(),
                default_value: serde_json::Value::String("NULL".into()),
            }],
            down: vec![],
        })
        .unwrap_err();
    assert!(
        err.to_string().contains("NULL"),
        "expected literal NULL default to be rejected on legacy table, got: {err}"
    );
    let raw = db
        .find("\u{0}T\u{0}legacy_null_def\u{0}r1", db.get_seq())
        .unwrap()
        .unwrap();
    assert_eq!(raw, "{\"id\":\"r1\"}");
}

#[test]
fn rename_field_commits_rows_and_catalog_together() {
    use omni_engine::schema::{Migration, SchemaManager, SchemaOp};

    let (db, _dir, _executor) = setup();
    let schema = SchemaManager::new(db.clone(), Arc::new(IndexManager::new(db.clone())));
    schema
        .migrate_up(&Migration {
            version: 1,
            description: "single-batch rename".into(),
            up: vec![SchemaOp::RenameField {
                collection: "ITEMS".into(),
                old_name: "label".into(),
                new_name: "title".into(),
            }],
            down: vec![SchemaOp::RenameField {
                collection: "ITEMS".into(),
                old_name: "title".into(),
                new_name: "label".into(),
            }],
        })
        .unwrap();

    let row_seqs: Vec<u64> = ["a", "ab", "b", "c"]
        .into_iter()
        .map(|id| db.get_seq_for_key(&format!("\u{0}T\u{0}items\u{0}{id}"), u64::MAX))
        .collect();
    let first_seq = row_seqs[0];
    let cat_seq = db.get_seq_for_key("\u{0}CATALOG\u{0}items", u64::MAX);
    assert_eq!(
        row_seqs,
        vec![first_seq, first_seq + 1, first_seq + 2, first_seq + 3]
    );
    assert_eq!(cat_seq, first_seq + 4);
    for id in ["a", "ab", "b", "c"] {
        let raw = db
            .find(&format!("\u{0}T\u{0}items\u{0}{id}"), u64::MAX)
            .unwrap()
            .unwrap();
        let doc: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert!(doc.get("title").is_some(), "row {id} missing title: {raw}");
        assert!(
            doc.get("label").is_none(),
            "row {id} still has label: {raw}"
        );
    }

    let mut doc_batch = WriteBatch::new();
    doc_batch
        .set("cased_docs:1", "{\"A\":1,\"a\":2}".into())
        .unwrap();
    db.commit_batch(&doc_batch).unwrap();
    schema
        .migrate_up(&Migration {
            version: 2,
            description: "case-sensitive two-step rename".into(),
            up: vec![
                SchemaOp::RenameField {
                    collection: "cased_docs".into(),
                    old_name: "A".into(),
                    new_name: "X".into(),
                },
                SchemaOp::RenameField {
                    collection: "cased_docs".into(),
                    old_name: "a".into(),
                    new_name: "x".into(),
                },
            ],
            down: vec![],
        })
        .unwrap();
    let cased_raw = db.find("cased_docs:1", u64::MAX).unwrap().unwrap();
    let cased_doc: serde_json::Value = serde_json::from_str(&cased_raw).unwrap();
    assert_eq!(cased_doc.get("X"), Some(&serde_json::json!(1)));
    assert_eq!(cased_doc.get("x"), Some(&serde_json::json!(2)));
}

#[test]
fn large_online_rename_recovers_staged_rows_and_completed_steps() {
    use omni_engine::schema::{Migration, SchemaManager, SchemaOp};

    let (db, _dir, executor) = setup();
    let schema = SchemaManager::new(db.clone(), Arc::new(IndexManager::new(db.clone())));
    execute(
        &executor,
        "CREATE TABLE big_rename (id TEXT PRIMARY KEY, old_col TEXT)",
    );
    let mut seed = WriteBatch::new();
    for i in 0..1100 {
        if seed.op_count() >= 500 {
            db.commit_batch(&seed).unwrap();
            seed = WriteBatch::new();
        }
        seed.set(
            &format!("\u{0}T\u{0}big_rename\u{0}k{i:04}"),
            format!("{{\"id\":\"k{i:04}\",\"old_col\":\"v{i}\"}}"),
        )
        .unwrap();
    }
    if !seed.is_empty() {
        db.commit_batch(&seed).unwrap();
    }

    let stage_key = test_rename_stage_key("big_rename", "old_col", "new_col");
    let row_marker_k0001 = test_rename_row_stage_key(&stage_key, "\u{0}T\u{0}big_rename\u{0}k0001");
    let mut partial = WriteBatch::new();
    for (k, v) in [
        (stage_key.as_str(), "old_col"),
        (
            "\u{0}T\u{0}big_rename\u{0}k0000",
            "{\"id\":\"k0000\",\"old_col\":\"v0\",\"new_col\":\"v0\"}",
        ),
        (row_marker_k0001.as_str(), "1"),
        (
            "\u{0}T\u{0}big_rename\u{0}k0001",
            "{\"id\":\"k0001\",\"old_col\":\"v1\",\"new_col\":\"v1\"}",
        ),
    ] {
        partial.set(k, v.into()).unwrap();
    }
    db.commit_batch(&partial).unwrap();
    execute(
        &executor,
        "UPDATE big_rename SET id = 'k1100', old_col = 'v1_moved' WHERE id = 'k0001'",
    );

    let rename_op = SchemaOp::RenameField {
        collection: "BIG_RENAME".into(),
        old_name: "old_col".into(),
        new_name: "new_col".into(),
    };
    let err = schema
        .migrate_up(&Migration {
            version: 1,
            description: "multi-op migration where second op fails".into(),
            up: vec![
                rename_op.clone(),
                SchemaOp::DropIndex {
                    name: "nonexistent_idx_after_rename".into(),
                },
            ],
            down: vec![],
        })
        .unwrap_err();
    assert!(err.to_string().contains("nonexistent_idx_after_rename"));

    schema
        .migrate_up(&Migration {
            version: 1,
            description: "retry after rename already finished".into(),
            up: vec![rename_op],
            down: vec![],
        })
        .unwrap();

    let refreshed = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db.clone())));
    assert_eq!(
        rows(
            &refreshed,
            "SELECT new_col FROM big_rename WHERE id = 'k1100'"
        ),
        vec![vec![Some("v1_moved".to_string())]]
    );
    assert_eq!(
        rows(
            &refreshed,
            "SELECT new_col FROM big_rename WHERE id = 'k1099'"
        ),
        vec![vec![Some("v1099".to_string())]]
    );
    assert_eq!(
        db.find(
            "\u{0}SCHEMA\u{0}rename_done:000001:up:000000:big_rename:old_col:new_col",
            db.get_seq()
        )
        .unwrap(),
        None
    );
}

#[test]
fn failed_unique_index_rebuild_over_batch_limit_cleans_up_and_allows_retry() {
    use omni_engine::schema::{Migration, SchemaManager, SchemaOp};

    let dir = TempDir::new().unwrap();
    let db = OmniKV::open(
        dir.path().join("manifest.json").to_str().unwrap(),
        dir.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();
    let executor = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db.clone())));
    execute(
        &executor,
        "CREATE TABLE big_idx (id TEXT PRIMARY KEY, code TEXT)",
    );

    let mut batch = WriteBatch::new();
    for i in 0..6000 {
        if batch.op_count() >= 500 {
            db.commit_batch(&batch).unwrap();
            batch = WriteBatch::new();
        }
        batch
            .set(
                &format!("\u{0}T\u{0}big_idx\u{0}k{i:04}"),
                format!("{{\"id\":\"k{i:04}\",\"code\":\"c{i:04}\"}}"),
            )
            .unwrap();
    }
    batch
        .set(
            "\u{0}T\u{0}big_idx\u{0}k9999",
            "{\"id\":\"k9999\",\"code\":\"c0000\"}".into(),
        )
        .unwrap();
    db.commit_batch(&batch).unwrap();

    let idx_mgr = Arc::new(IndexManager::new(db.clone()));
    let schema = SchemaManager::new(db.clone(), idx_mgr.clone());
    let migration = Migration {
        version: 1,
        description: "unique index on big_idx.code".into(),
        up: vec![SchemaOp::CreateIndex {
            name: "big_idx_code_uq".into(),
            collection: "big_idx".into(),
            fields: vec![("code".into(), IndexFieldType::String)],
            unique: true,
        }],
        down: vec![],
    };

    let err = schema.migrate_up(&migration).unwrap_err();
    assert!(err.to_string().contains("UNIQUE CONSTRAINT VIOLATION"));
    assert!(idx_mgr.definition("big_idx_code_uq").is_err());
    assert_eq!(
        db.scan("\u{0}IDX\u{0}", "\u{0}IDX\u{0}\u{10ffff}", db.get_seq())
            .unwrap()
            .len(),
        0
    );

    let mut fix = WriteBatch::new();
    fix.set(
        "\u{0}T\u{0}big_idx\u{0}k9999",
        "{\"id\":\"k9999\",\"code\":\"c9999\"}".into(),
    )
    .unwrap();
    db.commit_batch(&fix).unwrap();

    schema.migrate_up(&migration).unwrap();
    assert_eq!(
        idx_mgr
            .lookup(
                "big_idx_code_uq",
                &[serde_json::Value::String("c9999".into())]
            )
            .unwrap(),
        vec!["k9999".to_string()]
    );

    let mut retired_conflict = WriteBatch::new();
    retired_conflict
        .set(
            "\u{0}IDX_RETIRED\u{0}999",
            serde_json::to_string(&omni_engine::secondary_index::IndexDefinition {
                id: 999,
                name: "big_idx_code_uq".into(),
                collection: "other_tbl".into(),
                fields: vec![("code".into(), IndexFieldType::String)],
                unique: false,
            })
            .unwrap(),
        )
        .unwrap();
    db.commit_batch(&retired_conflict).unwrap();

    let ambig_err = idx_mgr.drop_index("big_idx_code_uq").unwrap_err();
    assert!(ambig_err.to_string().contains("ambiguous"));
    idx_mgr.purge_retired_index(999).unwrap();
    assert!(idx_mgr.definition("big_idx_code_uq").is_ok());
}

fn test_rename_stage_key(collection: &str, old_name: &str, new_name: &str) -> String {
    let col = collection.to_lowercase();
    let old = old_name.to_lowercase();
    let new = new_name.to_lowercase();
    format!(
        "\u{0}SCHEMA\u{0}rename_stage:{}:{}:{}:{}:{}:{}",
        col.len(),
        col,
        old.len(),
        old,
        new.len(),
        new
    )
}

fn test_rename_row_stage_key(stage_key: &str, row_key: &str) -> String {
    let suffix = stage_key
        .strip_prefix("\u{0}SCHEMA\u{0}rename_stage:")
        .unwrap_or(stage_key);
    format!(
        "\u{0}SCHEMA\u{0}rename_row:{}:{suffix}\u{0}{row_key}",
        suffix.len()
    )
}

#[test]
fn legacy_table_online_rename_propagates_stage_markers_across_lazy_migration_update_and_delete() {
    use omni_engine::schema::{Migration, SchemaManager, SchemaOp};

    let dir = TempDir::new().unwrap();
    let db = OmniKV::open(
        dir.path().join("manifest.json").to_str().unwrap(),
        dir.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();

    let stage_key = test_rename_stage_key("legacy_rename", "old_col", "new_col");
    let orphan_marker = test_rename_row_stage_key(&stage_key, "\u{0}T\u{0}legacy_rename\u{0}999");
    let manifest = "{\"name\":\"legacy_rename\",\"columns\":[{\"name\":\"id\",\"col_type\":\"Integer\",\"nullable\":false,\"primary_key\":true,\"default\":null},{\"name\":\"old_col\",\"col_type\":\"Text\",\"nullable\":true,\"primary_key\":false,\"default\":null}],\"primary_key\":\"id\",\"created_at\":1000}";
    let mut seed = WriteBatch::new();
    seed.set("\u{0}CATALOG\u{0}legacy_rename", manifest.to_string())
        .unwrap();
    for (raw_id, val) in [("001", "v1"), ("002", "v2"), ("003", "v3")] {
        let row_key = format!("\u{0}T\u{0}legacy_rename\u{0}{raw_id}");
        seed.set(
            &row_key,
            format!("{{\"id\":\"{raw_id}\",\"old_col\":\"{val}\",\"new_col\":\"{val}\"}}"),
        )
        .unwrap();
        seed.set(&test_rename_row_stage_key(&stage_key, &row_key), "1".into())
            .unwrap();
    }
    seed.set(&orphan_marker, "1".into()).unwrap();
    seed.set(&stage_key, "old_col".into()).unwrap();
    db.commit_batch(&seed).unwrap();

    let executor = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db.clone())));
    // Triggers lazy Legacy -> Typed migration (001->1, 002->2, 003->3) AND shifts 1 -> 100
    execute(
        &executor,
        "UPDATE legacy_rename SET id = 100, old_col = 'v1_updated' WHERE id = 1",
    );
    execute(&executor, "DELETE FROM legacy_rename WHERE id = 2");

    // Verify markers migrated cleanly: 001 -> 100, 003 -> 3, and 002/2 deleted
    for old_raw in ["001", "002", "003", "1", "2"] {
        let row_key = format!("\u{0}T\u{0}legacy_rename\u{0}{old_raw}");
        assert_eq!(
            db.find(
                &test_rename_row_stage_key(&stage_key, &row_key),
                db.get_seq()
            )
            .unwrap(),
            None,
            "stale marker for key {old_raw} must not remain"
        );
    }
    for live_id in ["100", "3"] {
        let row_key = format!("\u{0}T\u{0}legacy_rename\u{0}{live_id}");
        assert_eq!(
            db.find(
                &test_rename_row_stage_key(&stage_key, &row_key),
                db.get_seq()
            )
            .unwrap(),
            Some("1".into()),
            "canonical marker for key {live_id} must be preserved"
        );
    }

    let idx_mgr = Arc::new(IndexManager::new(db.clone()));
    let schema = SchemaManager::new(db.clone(), idx_mgr);
    schema
        .migrate_up(&Migration {
            version: 1,
            description: "complete interrupted rename on legacy table".into(),
            up: vec![SchemaOp::RenameField {
                collection: "legacy_rename".into(),
                old_name: "old_col".into(),
                new_name: "new_col".into(),
            }],
            down: vec![],
        })
        .unwrap();

    assert_eq!(
        db.find(&orphan_marker, db.get_seq()).unwrap(),
        None,
        "orphan row marker with no live row must be swept during rename cleanup"
    );

    let refreshed = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db)));
    assert_eq!(
        rows(
            &refreshed,
            "SELECT id, new_col FROM legacy_rename ORDER BY id"
        ),
        vec![
            vec![Some("3".into()), Some("v3".into())],
            vec![Some("100".into()), Some("v1_updated".into())],
        ]
    );
}

#[test]
fn online_rename_cleanup_does_not_delete_markers_of_prefix_overlapping_field_rename() {
    use omni_engine::schema::{Migration, SchemaManager, SchemaOp};

    let dir = TempDir::new().unwrap();
    let db = OmniKV::open(
        dir.path().join("manifest.json").to_str().unwrap(),
        dir.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();

    let stage_one = test_rename_stage_key("docs", "a:b", "c");
    let stage_two = test_rename_stage_key("docs", "a", "b:c\u{0}tail");
    let row_key = "docs:1";
    let marker_one = test_rename_row_stage_key(&stage_one, row_key);
    let marker_two = test_rename_row_stage_key(&stage_two, row_key);

    let mut seed = WriteBatch::new();
    seed.set(
        row_key,
        "{\"a:b\":\"v1\",\"c\":\"v1\",\"a\":\"v2\",\"b:c\\u0000tail\":\"v2_old\"}".into(),
    )
    .unwrap();
    seed.set(&stage_one, "a:b".into()).unwrap();
    seed.set(&marker_one, "1".into()).unwrap();
    seed.set(&stage_two, "a".into()).unwrap();
    seed.set(&marker_two, "1".into()).unwrap();
    db.commit_batch(&seed).unwrap();

    let schema = SchemaManager::new(db.clone(), Arc::new(IndexManager::new(db.clone())));
    schema
        .migrate_up(&Migration {
            version: 1,
            description: "complete first rename with colon in field name".into(),
            up: vec![SchemaOp::RenameField {
                collection: "docs".into(),
                old_name: "a:b".into(),
                new_name: "c".into(),
            }],
            down: vec![],
        })
        .unwrap();

    assert_eq!(db.find(&marker_one, db.get_seq()).unwrap(), None);
    assert_eq!(
        db.find(&marker_two, db.get_seq()).unwrap(),
        Some("1".into()),
        "cleanup of a:b -> c must not delete markers belonging to a -> b:c\\0tail"
    );

    // Retry of the second rename must still recognize its staged row marker
    // even though `a` changed after staging.
    schema
        .migrate_up(&Migration {
            version: 2,
            description: "retry second rename with embedded NUL in destination".into(),
            up: vec![SchemaOp::RenameField {
                collection: "docs".into(),
                old_name: "a".into(),
                new_name: "b:c\u{0}tail".into(),
            }],
            down: vec![],
        })
        .unwrap();
    assert_eq!(db.find(&marker_two, db.get_seq()).unwrap(), None);
}

#[test]
fn legacy_table_migration_with_rename_markers_over_batch_limit_completes_insert() {
    let dir = TempDir::new().unwrap();
    let db = OmniKV::open(
        dir.path().join("manifest.json").to_str().unwrap(),
        dir.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();

    let stage_key = test_rename_stage_key("big_legacy", "old_col", "new_col");
    let manifest = "{\"name\":\"big_legacy\",\"columns\":[{\"name\":\"id\",\"col_type\":\"Integer\",\"nullable\":false,\"primary_key\":true,\"default\":null},{\"name\":\"old_col\",\"col_type\":\"Text\",\"nullable\":true,\"primary_key\":false,\"default\":null}],\"primary_key\":\"id\",\"created_at\":1000}";
    let mut seed = WriteBatch::new();
    seed.set("\u{0}CATALOG\u{0}big_legacy", manifest.to_string())
        .unwrap();
    seed.set(&stage_key, "old_col".into()).unwrap();
    db.commit_batch(&seed).unwrap();

    // MAX_OPS / 3 zero-padded keys + 1 rename marker each = 4 ops/row (> MAX_OPS total).
    let row_count = WriteBatch::MAX_OPS / 3 + 10;
    let mut batch = WriteBatch::new();
    for i in 1..=row_count {
        if batch.op_count() >= 500 {
            db.commit_batch(&batch).unwrap();
            batch = WriteBatch::new();
        }
        let raw_id = format!("{i:06}");
        let row_key = format!("\u{0}T\u{0}big_legacy\u{0}{raw_id}");
        batch
            .set(
                &row_key,
                format!("{{\"id\":\"{raw_id}\",\"old_col\":\"v{i}\",\"new_col\":\"v{i}\"}}"),
            )
            .unwrap();
        batch
            .set(&test_rename_row_stage_key(&stage_key, &row_key), "1".into())
            .unwrap();
    }
    if !batch.is_empty() {
        db.commit_batch(&batch).unwrap();
    }

    let executor = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db.clone())));
    execute(
        &executor,
        "INSERT INTO big_legacy (id, old_col) VALUES (999999, 'fresh_row')",
    );

    assert_eq!(
        rows(
            &executor,
            "SELECT old_col FROM big_legacy WHERE id = 999999"
        ),
        vec![vec![Some("fresh_row".into())]]
    );
    assert_eq!(
        db.find(
            &test_rename_row_stage_key(&stage_key, "\u{0}T\u{0}big_legacy\u{0}1"),
            db.get_seq()
        )
        .unwrap(),
        Some("1".into()),
        "canonical rename marker must survive bounded lazy migration"
    );
    assert_eq!(
        db.find(
            &test_rename_row_stage_key(&stage_key, "\u{0}T\u{0}big_legacy\u{0}000001"),
            db.get_seq()
        )
        .unwrap(),
        None,
        "old non-canonical rename marker must be removed atomically with its row"
    );
}
