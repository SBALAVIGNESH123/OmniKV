use omni_engine::catalog::Catalog;
use omni_engine::schema::{Migration, SchemaManager, SchemaOp};
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

fn select_rows(executor: &SqlExecutor, sql: &str) -> Vec<Vec<String>> {
    rows(executor, sql)
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|cell| cell.unwrap_or_else(|| "NULL".into()))
                .collect()
        })
        .collect()
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
                encoding_version: omni_engine::secondary_index::CURRENT_INDEX_ENCODING_VERSION,
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
    format!(
        "\u{0}SCHEMA\u{0}rename_stage:{}:{}:{}:{}:{}:{}",
        col.len(),
        col,
        old_name.len(),
        old_name,
        new_name.len(),
        new_name
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

fn seed_large_legacy_table(db: &OmniKV, table_name: &str, row_count: usize) -> String {
    let stage_key = test_rename_stage_key(table_name, "old_col", "new_col");
    let manifest = format!(
        "{{\"name\":\"{table_name}\",\"columns\":[{{\"name\":\"id\",\"col_type\":\"Integer\",\"nullable\":false,\"primary_key\":true,\"default\":null}},{{\"name\":\"old_col\",\"col_type\":\"Text\",\"nullable\":true,\"primary_key\":false,\"default\":null}}],\"primary_key\":\"id\",\"created_at\":1000}}"
    );
    let mut seed = WriteBatch::new();
    seed.set(&format!("\u{0}CATALOG\u{0}{table_name}"), manifest)
        .unwrap();
    seed.set(&stage_key, "old_col".into()).unwrap();
    db.commit_batch(&seed).unwrap();

    let mut batch = WriteBatch::new();
    for i in 1..=row_count {
        if batch.op_count() >= 500 {
            db.commit_batch(&batch).unwrap();
            batch = WriteBatch::new();
        }
        let raw_id = format!("{i:06}");
        let row_key = format!("\u{0}T\u{0}{table_name}\u{0}{raw_id}");
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
    stage_key
}

#[test]
fn legacy_table_migration_with_rename_markers_over_batch_limit_completes_insert() {
    let dir = TempDir::new().unwrap();
    let db = OmniKV::open(
        dir.path().join("manifest.json").to_str().unwrap(),
        dir.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();

    // MAX_OPS / 3 zero-padded keys + 1 rename marker each = 4 ops/row (> MAX_OPS total).
    let stage_key = seed_large_legacy_table(&db, "big_legacy", WriteBatch::MAX_OPS / 3 + 10);

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

fn assert_legacy_row_untouched(db: &OmniKV, table_name: &str, stage_key: &str, context: &str) {
    let seq = db.get_seq();
    let legacy_key = format!("\u{0}T\u{0}{table_name}\u{0}000001");
    let canonical_key = format!("\u{0}T\u{0}{table_name}\u{0}1");
    assert!(
        db.find(&legacy_key, seq).unwrap().is_some(),
        "{context}: legacy row 000001 must remain untouched"
    );
    assert_eq!(
        db.find(&canonical_key, seq).unwrap(),
        None,
        "{context}: canonical row 1 must not be written on a failed statement"
    );
    assert_eq!(
        db.find(&test_rename_row_stage_key(stage_key, &legacy_key), seq)
            .unwrap(),
        Some("1".into()),
        "{context}: legacy rename marker must remain untouched"
    );
    assert_eq!(
        db.find(&test_rename_row_stage_key(stage_key, &canonical_key), seq)
            .unwrap(),
        None,
        "{context}: canonical rename marker must not be written on a failed statement"
    );
}

#[test]
fn failed_insert_and_update_on_large_legacy_table_leave_storage_untouched() {
    let dir = TempDir::new().unwrap();
    let db = OmniKV::open(
        dir.path().join("manifest.json").to_str().unwrap(),
        dir.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();

    let stage_key = seed_large_legacy_table(&db, "big_legacy_atomic", WriteBatch::MAX_OPS / 3 + 10);

    let idx_mgr = IndexManager::new(db.clone());
    idx_mgr
        .create_index(
            "big_legacy_atomic_old_col_uq",
            "big_legacy_atomic",
            vec![("old_col".into(), IndexFieldType::String)],
            true,
        )
        .unwrap();
    idx_mgr
        .rebuild_index("big_legacy_atomic_old_col_uq")
        .unwrap();

    let executor = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db.clone())));
    for (label, sql) in [
        (
            "duplicate PK insert",
            "INSERT INTO big_legacy_atomic (id, old_col) VALUES (1, 'fresh')",
        ),
        (
            "missing PK insert",
            "INSERT INTO big_legacy_atomic (old_col) VALUES ('fresh')",
        ),
        (
            "unique index violation insert",
            "INSERT INTO big_legacy_atomic (id, old_col) VALUES (999999, 'v1')",
        ),
        (
            "duplicate PK update",
            "UPDATE big_legacy_atomic SET id = 2 WHERE id = 1",
        ),
        (
            "NULL PK update",
            "UPDATE big_legacy_atomic SET id = NULL WHERE id = 1",
        ),
        (
            "unique index violation update",
            "UPDATE big_legacy_atomic SET old_col = 'v2' WHERE id = 1",
        ),
    ] {
        let stmt = parse_sql(sql).unwrap();
        assert!(executor.execute(&stmt).is_err(), "{label} must be rejected");
        assert_legacy_row_untouched(&db, "big_legacy_atomic", &stage_key, label);
    }

    // A subsequent valid UPDATE migrates cleanly and applies the update.
    execute(
        &executor,
        "UPDATE big_legacy_atomic SET old_col = 'v1_updated' WHERE id = 1",
    );
    assert_eq!(
        rows(
            &executor,
            "SELECT old_col FROM big_legacy_atomic WHERE id = 1"
        ),
        vec![vec![Some("v1_updated".into())]]
    );
    assert_eq!(
        db.find("\u{0}T\u{0}big_legacy_atomic\u{0}000001", db.get_seq())
            .unwrap(),
        None
    );
    assert_eq!(
        db.find(
            &test_rename_row_stage_key(&stage_key, "\u{0}T\u{0}big_legacy_atomic\u{0}1"),
            db.get_seq()
        )
        .unwrap(),
        Some("1".into())
    );
}

#[test]
fn legacy_update_canonicalizes_stored_pk_and_drop_table_cleans_stage_markers_and_retired_indexes() {
    let dir = TempDir::new().unwrap();
    let db = OmniKV::open(
        dir.path().join("manifest.json").to_str().unwrap(),
        dir.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();

    let stage_key = seed_large_legacy_table(&db, "legacy_pk_canon", 1);
    let retired_idx_key = "\u{0}IDX_RETIRED\u{0}99";
    let retired_idx_entry = "\u{0}IDX\u{0}00000099\u{0}v1\u{0}1";
    let retired_idx_json = "{\"id\":99,\"name\":\"old_retired_idx\",\"collection\":\"legacy_pk_canon\",\"fields\":[[\"old_col\",\"String\"]],\"unique\":false}";
    let mut retired_seed = WriteBatch::new();
    retired_seed
        .set(retired_idx_key, retired_idx_json.into())
        .unwrap();
    retired_seed.set(retired_idx_entry, "1".into()).unwrap();
    db.commit_batch(&retired_seed).unwrap();

    let executor = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db.clone())));
    execute(
        &executor,
        "UPDATE legacy_pk_canon SET old_col = 'updated' WHERE id = 1",
    );

    assert_eq!(
        rows(
            &executor,
            "SELECT id, old_col FROM legacy_pk_canon WHERE id = 1"
        ),
        vec![vec![Some("1".into()), Some("updated".into())]],
        "UPDATE on a legacy non-canonical PK row must canonicalize stored row[primary_key]"
    );

    let canonical_marker =
        test_rename_row_stage_key(&stage_key, "\u{0}T\u{0}legacy_pk_canon\u{0}1");
    assert_eq!(
        db.find(&canonical_marker, db.get_seq()).unwrap(),
        Some("1".into())
    );

    execute(&executor, "DROP TABLE legacy_pk_canon");
    assert_eq!(
        db.find(&stage_key, db.get_seq()).unwrap(),
        None,
        "DROP TABLE must clean up rename_stage markers for the table"
    );
    assert_eq!(
        db.find(&canonical_marker, db.get_seq()).unwrap(),
        None,
        "DROP TABLE must clean up rename_row markers for the table"
    );
    assert_eq!(
        db.find(retired_idx_key, db.get_seq()).unwrap(),
        None,
        "DROP TABLE must clean up retired index metadata for the table"
    );
    assert_eq!(
        db.find(retired_idx_entry, db.get_seq()).unwrap(),
        None,
        "DROP TABLE must clean up retired index entries for the table"
    );
}

#[test]
fn schema_add_field_stores_unquoted_string_and_none_for_null_sql_column_defaults() {
    use omni_engine::schema::{Migration, SchemaManager, SchemaOp};

    let dir = TempDir::new().unwrap();
    let db = OmniKV::open(
        dir.path().join("manifest.json").to_str().unwrap(),
        dir.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();
    let catalog = Arc::new(Catalog::new(db.clone()));
    let executor = SqlExecutor::new(db.clone(), catalog);
    execute(
        &executor,
        "CREATE TABLE add_field_defaults (id INTEGER PRIMARY KEY)",
    );
    execute(&executor, "INSERT INTO add_field_defaults (id) VALUES (1)");

    let schema = SchemaManager::new(db.clone(), Arc::new(IndexManager::new(db.clone())));
    schema
        .migrate_up(&Migration {
            version: 1,
            description: "add string and null defaults".into(),
            up: vec![
                SchemaOp::AddField {
                    collection: "add_field_defaults".into(),
                    field_name: "label".into(),
                    default_value: serde_json::Value::String("hello".into()),
                },
                SchemaOp::AddField {
                    collection: "add_field_defaults".into(),
                    field_name: "note".into(),
                    default_value: serde_json::Value::Null,
                },
            ],
            down: vec![],
        })
        .unwrap();

    let refreshed_catalog = Arc::new(Catalog::new(db.clone()));
    let table_def = refreshed_catalog
        .get_table("add_field_defaults")
        .expect("table metadata must exist after AddField");
    let label_col = table_def
        .columns
        .iter()
        .find(|c| c.name == "label")
        .expect("label column must exist");
    let note_col = table_def
        .columns
        .iter()
        .find(|c| c.name == "note")
        .expect("note column must exist");
    assert_eq!(
        label_col.default.as_deref(),
        Some("hello"),
        "AddField must store unquoted string default in SQL catalog"
    );
    assert_eq!(
        note_col.default, None,
        "AddField must store None (not \"null\") for JSON null default in SQL catalog"
    );

    let refreshed = SqlExecutor::new(db, refreshed_catalog);
    assert_eq!(
        rows(
            &refreshed,
            "SELECT label, note FROM add_field_defaults WHERE id = 1"
        ),
        vec![vec![Some("hello".into()), None]]
    );
}

fn seed_differential_oracle_tables(exec: &SqlExecutor) {
    for sql in [
        "CREATE TABLE users (id INTEGER PRIMARY KEY, age INTEGER, score FLOAT, tag TEXT)",
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, age INTEGER, label TEXT)",
        "CREATE TABLE codes (code TEXT PRIMARY KEY, priority INTEGER)",
        "INSERT INTO users (id, age, score, tag) VALUES (1, 2, 1.5, 'a'), (2, 9, 2.5, 'ab'), (10, 10, 10.0, 'b'), (11, 20, 20.5, 'ba'), (100, 30, 30.0, 'c'), (101, NULL, NULL, NULL), (200, -5, -1.25, '')",
        "INSERT INTO orders (id, user_id, age, label) VALUES (1, 1, 99, 'x'), (2, 10, 99, 'y'), (3, 11, 99, 'z'), (4, 100, 5, 'w')",
        "INSERT INTO codes (code, priority) VALUES ('a1', 1), ('ab', 2), ('b', 3)",
    ] {
        execute(exec, sql);
    }
}

fn build_differential_oracle_indexes(db: &Arc<OmniKV>) {
    let idx_mgr = IndexManager::new(db.clone());
    for (name, table, col, field_type) in [
        ("users_age_int_idx", "users", "age", IndexFieldType::Integer),
        (
            "users_score_float_idx",
            "users",
            "score",
            IndexFieldType::Float,
        ),
        ("users_tag_str_idx", "users", "tag", IndexFieldType::String),
    ] {
        idx_mgr
            .create_index(name, table, vec![(col.into(), field_type)], false)
            .unwrap();
        idx_mgr.rebuild_index(name).unwrap();
    }

    // Creating a mismatched String index on INTEGER column orders.age must be rejected.
    assert!(
        idx_mgr
            .create_index(
                "orders_age_wrong_str_idx",
                "orders",
                vec![("age".into(), IndexFieldType::String)],
                false,
            )
            .is_err(),
        "create_index must reject mismatched IndexFieldType::String on SQL INTEGER column"
    );

    // Even if a pre-existing mismatched catalog entry is present on disk, the SQL
    // optimizer must refuse to use it for range/equality scans.
    let mut legacy_idx = WriteBatch::new();
    legacy_idx
        .set(
            "\u{0}IDX_CATALOG\u{0}900",
            "{\"id\":900,\"name\":\"legacy_wrong_orders_age_idx\",\"collection\":\"orders\",\"fields\":[[\"age\",\"String\"]],\"unique\":false}".into(),
        )
        .unwrap();
    legacy_idx
        .set("\u{0}IDX_READY\u{0}900", "ready".into())
        .unwrap();
    db.commit_batch(&legacy_idx).unwrap();
}

fn explain_plan(exec: &SqlExecutor, sql: &str) -> String {
    rows(exec, sql)
        .into_iter()
        .filter_map(|r| r.into_iter().next().flatten())
        .collect::<Vec<_>>()
        .join("\n")
}

fn assert_differential_oracle_plans(indexed_exec: &SqlExecutor) {
    assert!(
        explain_plan(indexed_exec, "EXPLAIN SELECT id FROM users WHERE age >= 20")
            .contains("users_age_int_idx"),
        "ready INTEGER index must be selected by optimizer"
    );
    assert!(
        explain_plan(
            indexed_exec,
            "EXPLAIN SELECT id FROM users WHERE score >= 1.5"
        )
        .contains("users_score_float_idx"),
        "ready FLOAT index must be selected by optimizer"
    );
    assert!(
        explain_plan(
            indexed_exec,
            "EXPLAIN SELECT id FROM users WHERE tag >= 'a'"
        )
        .contains("users_tag_str_idx"),
        "ready String index must be selected by optimizer"
    );
    let orders_plan = explain_plan(indexed_exec, "EXPLAIN SELECT id FROM orders WHERE age >= 2");
    assert!(
        !orders_plan.contains("legacy_wrong_orders_age_idx"),
        "mismatched String index on INTEGER column must never be selected: {orders_plan}"
    );
    assert!(
        explain_plan(indexed_exec, "EXPLAIN SELECT id FROM USERS WHERE id = 1")
            .contains("PK Lookup"),
        "case-insensitive table lookup must still select PK Lookup"
    );
    assert!(
        !explain_plan(indexed_exec, "EXPLAIN SELECT id FROM users WHERE id = NULL")
            .contains("PK Lookup"),
        "WHERE pk = NULL must not plan a PK Lookup"
    );
}

#[test]
fn sql_differential_oracle_indexed_vs_unindexed_matches_across_types_bounds_joins_and_pks() {
    let dir_oracle = TempDir::new().unwrap();
    let db_oracle = OmniKV::open(
        dir_oracle.path().join("manifest.json").to_str().unwrap(),
        dir_oracle.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();
    let oracle_exec = SqlExecutor::new(db_oracle.clone(), Arc::new(Catalog::new(db_oracle)));

    let dir_indexed = TempDir::new().unwrap();
    let db_indexed = OmniKV::open(
        dir_indexed.path().join("manifest.json").to_str().unwrap(),
        dir_indexed.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();
    let indexed_exec = SqlExecutor::new(
        db_indexed.clone(),
        Arc::new(Catalog::new(db_indexed.clone())),
    );

    seed_differential_oracle_tables(&oracle_exec);
    seed_differential_oracle_tables(&indexed_exec);
    build_differential_oracle_indexes(&db_indexed);
    assert_differential_oracle_plans(&indexed_exec);

    for sql in [
        "SELECT id, age FROM users WHERE age >= 2 AND age <= 10 ORDER BY id",
        "SELECT id, age FROM users WHERE age >= 30 AND age >= 10 ORDER BY id",
        "SELECT id, age FROM users WHERE age >= 10 AND age >= 30 ORDER BY id",
        "SELECT id, age FROM users WHERE age = 20 AND age >= 10 ORDER BY id",
        "SELECT id, age FROM users WHERE age >= 10 AND age = 20 ORDER BY id",
        "SELECT id, age FROM users WHERE age <= 20 AND age <= 50 ORDER BY id",
        "SELECT id, age FROM users WHERE age = 20 AND age = 30 ORDER BY id",
        "SELECT id, age FROM users WHERE age > 25 AND age < 15 ORDER BY id",
        "SELECT id, age FROM users WHERE age < 0 ORDER BY id",
        "SELECT id, score FROM users WHERE score >= -2.0 AND score <= 10.0 ORDER BY id",
        "SELECT id, tag FROM users WHERE tag >= 'a' AND tag <= 'ab' ORDER BY id",
        "SELECT id, tag FROM users WHERE tag >= '' AND tag <= 'b' ORDER BY id",
        "SELECT id, age FROM orders WHERE age >= 2 AND age <= 10 ORDER BY id",
        "SELECT id, age FROM orders WHERE age >= 10 ORDER BY id",
        "SELECT users.id, orders.id FROM users JOIN orders ON users.id = orders.user_id WHERE users.age >= 2 AND orders.age = 99 ORDER BY users.id",
        "SELECT users.id, orders.id FROM users JOIN orders ON users.id = orders.user_id WHERE users.age >= 20 AND orders.age = 99 ORDER BY users.id",
        "SELECT id, age FROM users WHERE id = 1",
        "SELECT id, age FROM USERS WHERE id = 1",
        "SELECT id, age FROM users WHERE id = '1.0'",
        "SELECT id, age FROM users WHERE id = 10",
        "SELECT id, age FROM users WHERE id = 20",
        "SELECT id, age FROM users WHERE id = NULL",
        "SELECT code, priority FROM codes WHERE code = 'a'",
        "SELECT code, priority FROM codes WHERE code = 'a1'",
    ] {
        assert_eq!(
            rows(&indexed_exec, sql),
            rows(&oracle_exec, sql),
            "indexed SQL result diverged from sequential oracle for query: {sql}"
        );
    }

    // FLOAT PRIMARY KEY with -0.0 must match stored 0.0 via PK Lookup.
    for exec in [&oracle_exec, &indexed_exec] {
        execute(exec, "CREATE TABLE coords (x FLOAT PRIMARY KEY, v TEXT)");
        execute(exec, "INSERT INTO coords (x, v) VALUES (0.0, 'origin')");
    }
    assert_eq!(
        rows(&indexed_exec, "SELECT x, v FROM coords WHERE x = -0.0"),
        vec![vec![Some("0".into()), Some("origin".into())]]
    );

    // Corrupt neighbor prefix key `codes.a1` and verify BOTH PkLookupIter AND
    // PlanExecutor (plan_exec.rs) perform exact O(1) lookups without touching `a1`.
    let mut corrupt = WriteBatch::new();
    corrupt
        .set("\u{0}T\u{0}codes\u{0}a1", "{not-valid-json".into())
        .unwrap();
    db_indexed.commit_batch(&corrupt).unwrap();
    let catalog = Arc::new(Catalog::new(db_indexed.clone()));
    let codes_table = catalog.get_table("codes").expect("codes table");
    let mut pk_iter = omni_engine::volcano::PkLookupIter::new(&db_indexed, &codes_table, "a");
    assert_eq!(
        omni_engine::volcano::RowIterator::try_next_row(&mut pk_iter).unwrap(),
        None
    );
    let pk_plan = omni_engine::optimizer::PlanNode::Scan {
        table: "codes".into(),
        access: omni_engine::optimizer::AccessMethod::PkLookup {
            key_value: "a".into(),
        },
        filter: None,
        estimated_rows: 1,
        estimated_cost: 1.0,
    };
    let plan_exec = omni_engine::plan_exec::PlanExecutor::for_plan_at(
        db_indexed.clone(),
        catalog,
        &pk_plan,
        db_indexed.get_seq(),
    );
    assert_eq!(
        plan_exec.execute_plan(&pk_plan).unwrap().len(),
        0,
        "PlanExecutor PkLookup for missing 'a' must not scan or decode neighbor 'a1'"
    );
}

#[test]
fn index_scan_bounds_tighten_conjuncts_and_ignore_foreign_table_qualifiers() {
    use omni_engine::sql::{CmpOp, SqlValue, WhereExpr};
    use omni_engine::volcano::{IndexScanIter, RowIterator};

    let dir = TempDir::new().unwrap();
    let db = OmniKV::open(
        dir.path().join("manifest.json").to_str().unwrap(),
        dir.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();
    let catalog = Arc::new(Catalog::new(db.clone()));
    let executor = SqlExecutor::new(db.clone(), catalog.clone());
    seed_differential_oracle_tables(&executor);
    build_differential_oracle_indexes(&db);

    // Corrupt rows with age=10 (id=10) and age=30 (id=100) in storage.
    // Tightened index bounds must skip those out-of-range rows completely!
    let mut corrupt = WriteBatch::new();
    corrupt
        .set("\u{0}T\u{0}users\u{0}10", "{corrupt-age-10".into())
        .unwrap();
    corrupt
        .set("\u{0}T\u{0}users\u{0}100", "{corrupt-age-30".into())
        .unwrap();
    db.commit_batch(&corrupt).unwrap();

    let users_table = catalog.get_table("users").expect("users table");
    let seq = db.get_seq();

    // age = 20 AND age >= 10 must keep lower=20, upper=20 (never touching age=10 or age=30).
    let pred_eq_and_ge = WhereExpr::And(
        Box::new(WhereExpr::Comparison {
            column: "age".into(),
            op: CmpOp::Eq,
            value: SqlValue::Integer(20),
        }),
        Box::new(WhereExpr::Comparison {
            column: "age".into(),
            op: CmpOp::Gte,
            value: SqlValue::Integer(10),
        }),
    );
    let mut iter = IndexScanIter::with_scan(
        &db,
        &catalog,
        &users_table,
        "users_age_int_idx",
        Some(&pred_eq_and_ge),
        seq,
        None,
    );
    let mut found_ids = Vec::new();
    while let Some(row) = iter.try_next_row().unwrap() {
        found_ids.push(row.get("id").cloned().flatten());
    }
    assert_eq!(found_ids, vec![Some("11".into())]);

    // age = 20 AND age = 30 must tighten to lower=30 > upper=20 -> 0 rows (never touching age=30).
    let pred_contradiction = WhereExpr::And(
        Box::new(WhereExpr::Comparison {
            column: "age".into(),
            op: CmpOp::Eq,
            value: SqlValue::Integer(20),
        }),
        Box::new(WhereExpr::Comparison {
            column: "age".into(),
            op: CmpOp::Eq,
            value: SqlValue::Integer(30),
        }),
    );
    let mut empty_iter = IndexScanIter::with_scan(
        &db,
        &catalog,
        &users_table,
        "users_age_int_idx",
        Some(&pred_contradiction),
        seq,
        None,
    );
    assert_eq!(empty_iter.try_next_row().unwrap(), None);

    // Foreign qualifier `orders.age = 99` must NOT overwrite `users.age = 20`.
    let pred_foreign = WhereExpr::And(
        Box::new(WhereExpr::Comparison {
            column: "users.age".into(),
            op: CmpOp::Eq,
            value: SqlValue::Integer(20),
        }),
        Box::new(WhereExpr::Comparison {
            column: "orders.age".into(),
            op: CmpOp::Eq,
            value: SqlValue::Integer(99),
        }),
    );
    let mut foreign_iter = IndexScanIter::with_scan(
        &db,
        &catalog,
        &users_table,
        "users_age_int_idx",
        Some(&pred_foreign),
        seq,
        None,
    );
    let mut foreign_ids = Vec::new();
    while let Some(row) = foreign_iter.try_next_row().unwrap() {
        foreign_ids.push(row.get("id").cloned().flatten());
    }
    assert_eq!(foreign_ids, vec![Some("11".into())]);
}

#[test]
fn stale_partial_index_entries_never_hide_rows_and_are_purged_on_rebuild() {
    let dir = TempDir::new().unwrap();
    let db = OmniKV::open(
        dir.path().join("manifest.json").to_str().unwrap(),
        dir.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();
    let executor = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db.clone())));
    execute(
        &executor,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, sku TEXT)",
    );
    execute(
        &executor,
        "INSERT INTO items (id, sku) VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')",
    );

    let idx_mgr = IndexManager::new(db.clone());
    let index_id = idx_mgr
        .create_index(
            "items_sku_idx",
            "items",
            vec![("sku".into(), IndexFieldType::String)],
            true,
        )
        .unwrap();

    // Simulate stale/partial index entries left behind by an earlier crashed build
    // (pointing to a non-existent phantom PK '999' for sku = 'alpha', while index is NOT ready).
    let stale_key = format!("\u{0}IDX\u{0}{index_id:08}\u{0}616c706861\u{0}999");
    let mut seed_stale = WriteBatch::new();
    seed_stale.set(&stale_key, "999".into()).unwrap();
    db.commit_batch(&seed_stale).unwrap();

    assert!(
        !explain_plan(
            &executor,
            "EXPLAIN SELECT id, sku FROM items WHERE sku = 'alpha'"
        )
        .contains("items_sku_idx"),
        "unready index must not be chosen by EXPLAIN"
    );
    assert_eq!(
        rows(
            &executor,
            "SELECT id, sku FROM items WHERE sku = 'alpha' ORDER BY id"
        ),
        vec![vec![Some("1".into()), Some("alpha".into())]]
    );

    let rebuilt = idx_mgr
        .rebuild_index("items_sku_idx")
        .expect("rebuild_index must purge stale entries before unique backfill");
    assert_eq!(rebuilt, 3);
    assert_eq!(
        db.find(&stale_key, db.get_seq()).unwrap(),
        None,
        "stale partial index entry must be purged by rebuild_index"
    );
    assert!(
        explain_plan(
            &executor,
            "EXPLAIN SELECT id, sku FROM items WHERE sku = 'alpha'"
        )
        .contains("items_sku_idx"),
        "rebuilt ready index must be chosen by EXPLAIN"
    );
    assert_eq!(
        rows(
            &executor,
            "SELECT id, sku FROM items WHERE sku = 'alpha' ORDER BY id"
        ),
        vec![vec![Some("1".into()), Some("alpha".into())]]
    );
}

#[test]
fn unbuilt_unique_index_on_large_legacy_table_rejects_duplicates_without_partial_migration() {
    let dir = TempDir::new().unwrap();
    let db = OmniKV::open(
        dir.path().join("manifest.json").to_str().unwrap(),
        dir.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();

    let stage_key =
        seed_large_legacy_table(&db, "big_legacy_unbuilt", WriteBatch::MAX_OPS / 3 + 10);

    // Deliberately create a unique index WITHOUT calling rebuild_index yet.
    let idx_mgr = IndexManager::new(db.clone());
    idx_mgr
        .create_index(
            "big_legacy_unbuilt_old_col_uq",
            "big_legacy_unbuilt",
            vec![("old_col".into(), IndexFieldType::String)],
            true,
        )
        .unwrap();

    let executor = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db.clone())));
    for (label, sql) in [
        (
            "unbuilt unique index insert violation",
            "INSERT INTO big_legacy_unbuilt (id, old_col) VALUES (999999, 'v1')",
        ),
        (
            "unbuilt unique index update violation",
            "UPDATE big_legacy_unbuilt SET old_col = 'v2' WHERE id = 1",
        ),
    ] {
        let stmt = parse_sql(sql).unwrap();
        assert!(executor.execute(&stmt).is_err(), "{label} must be rejected");
        assert_legacy_row_untouched(&db, "big_legacy_unbuilt", &stage_key, label);
    }
}

#[test]
fn transactional_drop_table_cleans_stage_markers_retired_indexes_and_updates_explain() {
    let dir = TempDir::new().unwrap();
    let db = OmniKV::open(
        dir.path().join("manifest.json").to_str().unwrap(),
        dir.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();

    let stage_key = seed_large_legacy_table(&db, "tx_drop_tbl", 1);
    let retired_idx_key = "\u{0}IDX_RETIRED\u{0}88";
    let retired_idx_entry = "\u{0}IDX\u{0}00000088\u{0}v1\u{0}1";
    let retired_idx_json = "{\"id\":88,\"name\":\"tx_retired_idx\",\"collection\":\"tx_drop_tbl\",\"fields\":[[\"old_col\",\"String\"]],\"unique\":false}";
    let mut retired_seed = WriteBatch::new();
    retired_seed
        .set(retired_idx_key, retired_idx_json.into())
        .unwrap();
    retired_seed.set(retired_idx_entry, "1".into()).unwrap();
    db.commit_batch(&retired_seed).unwrap();

    let tm = TransactionManager::new(db.clone());
    let mut txn = tm.begin();
    let mut tx_exec = SqlExecutor::with_transaction(
        db.clone(),
        Arc::new(Catalog::at_snapshot(db.clone(), txn.read_seq)),
        txn.read_seq,
        &HashMap::new(),
    );
    execute(
        &tx_exec,
        "UPDATE tx_drop_tbl SET id = 50, old_col = 'moved' WHERE id = 1",
    );
    execute(&tx_exec, "DROP TABLE tx_drop_tbl");
    execute(
        &tx_exec,
        "CREATE TABLE tx_drop_tbl (id INTEGER PRIMARY KEY, old_col TEXT)",
    );
    let plan_after_recreate = explain_plan(
        &tx_exec,
        "EXPLAIN SELECT id FROM tx_drop_tbl WHERE old_col = 'moved'",
    );
    assert!(
        !plan_after_recreate.contains("Index Scan"),
        "transactional EXPLAIN must not reference dropped indexes: {plan_after_recreate}"
    );
    if let Some(reads) = tx_exec.drain_reads() {
        txn.read_set.extend(reads.keys);
        txn.read_ranges.extend(reads.ranges);
    }
    if let Some(staged) = tx_exec.flush_batch() {
        for (k, v, ttl) in staged.buffered_writes {
            txn.write_set
                .insert(String::from_utf8(k).unwrap(), (Some(v), ttl));
        }
        for k in staged.buffered_deletes {
            txn.write_set
                .insert(String::from_utf8(k).unwrap(), (None, 0));
        }
    }
    tm.commit(&mut txn).unwrap();

    let seq = db.get_seq();
    assert_eq!(db.find(&stage_key, seq).unwrap(), None);
    assert_eq!(
        db.find(
            &test_rename_row_stage_key(&stage_key, "\u{0}T\u{0}tx_drop_tbl\u{0}000001"),
            seq
        )
        .unwrap(),
        None
    );
    assert_eq!(
        db.find(
            &test_rename_row_stage_key(&stage_key, "\u{0}T\u{0}tx_drop_tbl\u{0}50"),
            seq
        )
        .unwrap(),
        None
    );
    assert_eq!(db.find(retired_idx_key, seq).unwrap(), None);
    assert_eq!(db.find(retired_idx_entry, seq).unwrap(), None);
}

#[test]
fn transactional_drop_empty_typed_table_updates_explain_for_active_index() {
    let dir = TempDir::new().unwrap();
    let db = OmniKV::open(
        dir.path().join("manifest.json").to_str().unwrap(),
        dir.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();
    let setup_exec = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db.clone())));
    execute(
        &setup_exec,
        "CREATE TABLE tx_drop_active (id INTEGER PRIMARY KEY, email TEXT)",
    );
    let idx_mgr = IndexManager::new(db.clone());
    idx_mgr
        .create_index(
            "tx_drop_active_email_idx",
            "tx_drop_active",
            vec![("email".into(), IndexFieldType::String)],
            false,
        )
        .unwrap();
    idx_mgr.rebuild_index("tx_drop_active_email_idx").unwrap();

    let tm = TransactionManager::new(db.clone());
    let mut txn = tm.begin();
    let tx_exec = SqlExecutor::with_transaction(
        db.clone(),
        Arc::new(Catalog::at_snapshot(db, txn.read_seq)),
        txn.read_seq,
        &HashMap::new(),
    );
    let plan_before_drop = explain_plan(
        &tx_exec,
        "EXPLAIN SELECT id FROM tx_drop_active WHERE email = 'alice@example.com'",
    );
    assert!(
        plan_before_drop.contains("Index Scan"),
        "EXPLAIN on empty typed table with ready active index must select Index Scan before DROP: {plan_before_drop}"
    );
    execute(&tx_exec, "DROP TABLE tx_drop_active");
    execute(
        &tx_exec,
        "CREATE TABLE tx_drop_active (id INTEGER PRIMARY KEY, email TEXT)",
    );
    let plan_after_active_recreate = explain_plan(
        &tx_exec,
        "EXPLAIN SELECT id FROM tx_drop_active WHERE email = 'alice@example.com'",
    );
    assert!(
        !plan_after_active_recreate.contains("Index Scan"),
        "EXPLAIN after transactional DROP + CREATE must not select dropped active index: {plan_after_active_recreate}"
    );
    tm.abort(&mut txn);
}

#[test]
fn legacy_typed_negative_zero_float_pk_is_addressable_and_updatable() {
    let dir = TempDir::new().unwrap();
    let db = OmniKV::open(
        dir.path().join("manifest.json").to_str().unwrap(),
        dir.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();
    let catalog = Arc::new(Catalog::new(db.clone()));
    let executor = SqlExecutor::new(db.clone(), catalog.clone());
    execute(
        &executor,
        "CREATE TABLE coords (x FLOAT PRIMARY KEY, label TEXT)",
    );

    let idx_mgr = IndexManager::new(db.clone());
    idx_mgr
        .create_index(
            "coords_label_idx",
            "coords",
            vec![("label".into(), IndexFieldType::String)],
            false,
        )
        .unwrap();
    idx_mgr
        .create_index(
            "coords_x_uq",
            "coords",
            vec![("x".into(), IndexFieldType::Float)],
            true,
        )
        .unwrap();

    let mut seed = WriteBatch::new();
    seed.set(
        "\u{0}T\u{0}coords\u{0}-0",
        "{\"x\":\"-0\",\"label\":\"origin\"}".into(),
    )
    .unwrap();
    db.commit_batch(&seed).unwrap();
    idx_mgr.rebuild_index("coords_label_idx").unwrap();
    idx_mgr.rebuild_index("coords_x_uq").unwrap();

    for query in [
        "SELECT label FROM coords WHERE x = 0",
        "SELECT label FROM coords WHERE x = -0.0",
    ] {
        assert_eq!(select_rows(&executor, query), vec![vec!["origin"]]);
        let stmt = parse_sql(query).unwrap();
        let idx_cat = idx_mgr.catalog_snapshot().unwrap();
        let stats = omni_engine::optimizer::gather_stats(&catalog, Some(&idx_cat), &db);
        let plan = omni_engine::optimizer::Optimizer::new(stats)
            .optimize(&stmt)
            .unwrap();
        let plan_exec = omni_engine::plan_exec::PlanExecutor::new(db.clone(), catalog.clone());
        let plan_rows = plan_exec.execute_plan(&plan).unwrap();
        assert_eq!(plan_rows.len(), 1);
        assert_eq!(
            plan_rows[0].get("label").and_then(|v| v.as_deref()),
            Some("origin")
        );
    }

    let dup_stmt = parse_sql("INSERT INTO coords (x, label) VALUES (0.0, 'dup')").unwrap();
    assert!(executor.execute(&dup_stmt).is_err());

    execute(
        &executor,
        "UPDATE coords SET label = 'migrated' WHERE x = -0.0",
    );
    let seq = db.get_seq();
    assert_eq!(db.find("\u{0}T\u{0}coords\u{0}-0", seq).unwrap(), None);
    assert!(db.find("\u{0}T\u{0}coords\u{0}0", seq).unwrap().is_some());
    assert_eq!(
        select_rows(&executor, "SELECT label FROM coords WHERE x = 0"),
        vec![vec!["migrated"]]
    );
    assert_eq!(
        select_rows(
            &executor,
            "SELECT label FROM coords WHERE label = 'migrated'"
        ),
        vec![vec!["migrated"]]
    );
    assert_eq!(
        select_rows(&executor, "SELECT label FROM coords WHERE label = 'origin'").len(),
        0
    );
}

#[test]
fn legacy_typed_negative_zero_float_pk_is_deletable_with_indexes() {
    let dir = TempDir::new().unwrap();
    let db = OmniKV::open(
        dir.path().join("manifest.json").to_str().unwrap(),
        dir.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();
    let executor = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db.clone())));
    execute(
        &executor,
        "CREATE TABLE coords_del (x FLOAT PRIMARY KEY, label TEXT)",
    );
    let idx_mgr = IndexManager::new(db.clone());
    idx_mgr
        .create_index(
            "coords_del_label_idx",
            "coords_del",
            vec![("label".into(), IndexFieldType::String)],
            false,
        )
        .unwrap();
    let mut seed = WriteBatch::new();
    seed.set(
        "\u{0}T\u{0}coords_del\u{0}-0",
        "{\"x\":\"-0\",\"label\":\"remove_me\"}".into(),
    )
    .unwrap();
    db.commit_batch(&seed).unwrap();
    idx_mgr.rebuild_index("coords_del_label_idx").unwrap();

    execute(&executor, "DELETE FROM coords_del WHERE x = 0");
    assert_eq!(
        db.find("\u{0}T\u{0}coords_del\u{0}-0", db.get_seq())
            .unwrap(),
        None
    );
    assert_eq!(
        idx_mgr
            .lookup(
                "coords_del_label_idx",
                &[serde_json::Value::String("remove_me".into())]
            )
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn integer_pk_non_i128_literal_matches_eq_in_both_executors() {
    let dir = TempDir::new().unwrap();
    let db = OmniKV::open(
        dir.path().join("manifest.json").to_str().unwrap(),
        dir.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();
    let catalog = Arc::new(Catalog::new(db.clone()));
    let executor = SqlExecutor::new(db.clone(), catalog.clone());
    execute(
        &executor,
        "CREATE TABLE int_pk_tbl (id INTEGER PRIMARY KEY, v TEXT)",
    );
    execute(
        &executor,
        "INSERT INTO int_pk_tbl (id, v) VALUES ('1.0', 'float_like')",
    );

    assert_eq!(
        select_rows(&executor, "SELECT v FROM int_pk_tbl WHERE id = 1"),
        vec![vec!["float_like"]]
    );
    let stmt = parse_sql("SELECT v FROM int_pk_tbl WHERE id = 1").unwrap();
    let stats = omni_engine::optimizer::gather_stats(&catalog, None, &db);
    let plan = omni_engine::optimizer::Optimizer::new(stats)
        .optimize(&stmt)
        .unwrap();
    let plan_exec = omni_engine::plan_exec::PlanExecutor::new(db, catalog);
    let rows = plan_exec.execute_plan(&plan).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("v").and_then(|v| v.as_deref()),
        Some("float_like")
    );
}

#[test]
fn staged_document_rename_preserves_case() {
    let dir = TempDir::new().unwrap();
    let db = OmniKV::open(
        dir.path().join("manifest.json").to_str().unwrap(),
        dir.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();
    let schema = SchemaManager::new(db.clone(), Arc::new(IndexManager::new(db.clone())));

    let doc_key = "docs:1";
    let stage_upper = test_rename_stage_key("docs", "A", "X");
    let stage_lower = test_rename_stage_key("docs", "a", "x");
    assert_ne!(stage_upper, stage_lower);
    let row_marker_upper = test_rename_row_stage_key(&stage_upper, doc_key);
    let mut seed = WriteBatch::new();
    seed.set(
        doc_key,
        "{\"A\":\"from_A\",\"X\":\"from_A\",\"a\":\"from_a\",\"x\":\"keep_x\"}".into(),
    )
    .unwrap();
    seed.set(&stage_upper, "A".into()).unwrap();
    seed.set(&row_marker_upper, "1".into()).unwrap();
    db.commit_batch(&seed).unwrap();

    let colliding_lower = Migration {
        version: 1,
        description: "lower a -> x must not reuse A -> X stage".into(),
        up: vec![SchemaOp::RenameField {
            collection: "docs".into(),
            old_name: "a".into(),
            new_name: "x".into(),
        }],
        down: vec![],
    };
    let err = schema.migrate_up(&colliding_lower).unwrap_err();
    assert!(
        err.to_string().contains("destination exists"),
        "a -> x must fail because x already exists: {err}"
    );
    let seq = db.get_seq();
    let stored: serde_json::Value =
        serde_json::from_str(&db.find(doc_key, seq).unwrap().unwrap()).unwrap();
    assert_eq!(stored["x"], "keep_x");
    assert_eq!(db.find(&stage_upper, seq).unwrap(), Some("A".into()));
    assert_eq!(db.find(&row_marker_upper, seq).unwrap(), Some("1".into()));
}

#[test]
fn staged_sql_rename_resolves_canonical_column_on_retry() {
    let dir = TempDir::new().unwrap();
    let db = OmniKV::open(
        dir.path().join("manifest.json").to_str().unwrap(),
        dir.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();
    let schema = SchemaManager::new(db.clone(), Arc::new(IndexManager::new(db.clone())));
    let executor = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db.clone())));
    execute(
        &executor,
        "CREATE TABLE mixed_cols (id INTEGER PRIMARY KEY, DisplayName TEXT)",
    );
    execute(
        &executor,
        "INSERT INTO mixed_cols (id, DisplayName) VALUES (1, 'Alice')",
    );
    let sql_stage = test_rename_stage_key("mixed_cols", "DisplayName", "FullName");
    let sql_row_key = "\u{0}T\u{0}mixed_cols\u{0}1";
    let mut table_def: omni_engine::catalog::TableDef = serde_json::from_str(
        &db.find("\u{0}CATALOG\u{0}mixed_cols", db.get_seq())
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    for col in &mut table_def.columns {
        if col.name == "DisplayName" {
            col.name = "FullName".into();
        }
    }
    let mut sim_crash = WriteBatch::new();
    sim_crash
        .set(
            sql_row_key,
            "{\"id\":\"1\",\"DisplayName\":\"Alice\",\"FullName\":\"Alice\"}".into(),
        )
        .unwrap();
    sim_crash.set(&sql_stage, "DisplayName".into()).unwrap();
    sim_crash
        .set(
            &test_rename_row_stage_key(&sql_stage, sql_row_key),
            "1".into(),
        )
        .unwrap();
    sim_crash
        .set(
            "\u{0}CATALOG\u{0}mixed_cols",
            serde_json::to_string(&table_def).unwrap(),
        )
        .unwrap();
    db.commit_batch(&sim_crash).unwrap();

    let sql_rename = Migration {
        version: 1,
        description: "retry case-insensitive SQL column rename".into(),
        up: vec![SchemaOp::RenameField {
            collection: "mixed_cols".into(),
            old_name: "displayname".into(),
            new_name: "FullName".into(),
        }],
        down: vec![],
    };
    schema
        .migrate_up(&sql_rename)
        .expect("retrying SQL rename after catalog cutover must find staged canonical column");
    let seq = db.get_seq();
    assert_eq!(db.find(&sql_stage, seq).unwrap(), None);
    let cleaned_row: serde_json::Value =
        serde_json::from_str(&db.find(sql_row_key, seq).unwrap().unwrap()).unwrap();
    assert_eq!(cleaned_row.get("DisplayName"), None);
    assert_eq!(cleaned_row["FullName"], "Alice");
}

#[test]
fn legacy_update_preflight_counts_unchanged_index_writes_before_migration_commit() {
    let dir = TempDir::new().unwrap();
    let db = OmniKV::open(
        dir.path().join("manifest.json").to_str().unwrap(),
        dir.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();

    let row_count = WriteBatch::MAX_OPS / 2; // 5,000 rows + 5,000 unchanged index writes + 1 catalog flip = 10,001 > MAX_OPS
    let stage_key = seed_large_legacy_table(&db, "big_legacy_idx_cap", row_count);
    let idx_mgr = IndexManager::new(db.clone());
    idx_mgr
        .create_index(
            "big_legacy_idx_cap_old_col_idx",
            "big_legacy_idx_cap",
            vec![("old_col".into(), IndexFieldType::String)],
            false,
        )
        .unwrap();
    idx_mgr
        .rebuild_index("big_legacy_idx_cap_old_col_idx")
        .unwrap();

    let executor = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db.clone())));
    // Update an unindexed column (actually old_col is indexed, wait: add/update a non-indexed column?
    // Even updating old_col to the SAME value or adding a non-indexed column leaves the index key unchanged).
    // Here old_col is indexed with values v1..v5000; updating `id = id` or updating via no-op value keeps
    // every index key unchanged while rewriting all 5,000 rows + 5,000 index sets + 1 catalog flip.
    // Let's seed a 3-column legacy table or update old_col on row 1? No, update `old_col = 'same'` changes old_col.
    // Instead, re-seed table schema with an extra unindexed `note` column so old_col stays identical!
    let mut table_def: omni_engine::catalog::TableDef = serde_json::from_str(
        &db.find("\u{0}CATALOG\u{0}big_legacy_idx_cap", db.get_seq())
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    table_def.columns.push(omni_engine::catalog::Column {
        name: "note".into(),
        col_type: omni_engine::catalog::ColumnType::Text,
        nullable: true,
        primary_key: false,
        default: None,
    });
    let mut cat_wb = WriteBatch::new();
    cat_wb
        .set(
            "\u{0}CATALOG\u{0}big_legacy_idx_cap",
            serde_json::to_string(&table_def).unwrap(),
        )
        .unwrap();
    db.commit_batch(&cat_wb).unwrap();

    let stmt = parse_sql("UPDATE big_legacy_idx_cap SET note = 'touched'").unwrap();
    let Err(err) = executor.execute(&stmt) else {
        panic!(
            "5,000 rows + 5,000 unchanged index writes + 1 catalog flip must fail BatchTooLarge"
        );
    };
    assert!(
        err.contains("BatchTooLarge"),
        "expected BatchTooLarge before any migration chunk commits, got: {err}"
    );
    assert_legacy_row_untouched(
        &db,
        "big_legacy_idx_cap",
        &stage_key,
        "oversized legacy UPDATE with unchanged index writes",
    );
}

#[test]
fn sql_remove_field_small_atomic_and_replayable_via_step_receipt() {
    let dir = TempDir::new().unwrap();
    let db = OmniKV::open(
        dir.path().join("manifest.json").to_str().unwrap(),
        dir.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();
    let executor = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db.clone())));
    let schema = SchemaManager::new(db.clone(), Arc::new(IndexManager::new(db.clone())));

    execute(
        &executor,
        "CREATE TABLE drop_col_tbl (id INTEGER PRIMARY KEY, obsolete TEXT, keep_col TEXT)",
    );
    execute(
        &executor,
        "INSERT INTO drop_col_tbl (id, obsolete, keep_col) VALUES (1, 'old_val', 'kept')",
    );

    let failing_migration = Migration {
        version: 1,
        description: "remove obsolete then fail".into(),
        up: vec![
            SchemaOp::RemoveField {
                collection: "drop_col_tbl".into(),
                field_name: "obsolete".into(),
            },
            SchemaOp::RemoveField {
                collection: "drop_col_tbl".into(),
                field_name: "does_not_exist".into(),
            },
        ],
        down: vec![],
    };
    let err = schema.migrate_up(&failing_migration).unwrap_err();
    assert!(err.to_string().contains("does not exist"));
    assert_eq!(schema.current_version().unwrap(), 0);
    let row_json: serde_json::Value = serde_json::from_str(
        &db.find("\u{0}T\u{0}drop_col_tbl\u{0}1", db.get_seq())
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(row_json.get("obsolete"), None);
    assert_eq!(row_json["keep_col"], "kept");

    let fixed_migration = Migration {
        version: 1,
        description: "remove obsolete then add extra".into(),
        up: vec![
            SchemaOp::RemoveField {
                collection: "drop_col_tbl".into(),
                field_name: "obsolete".into(),
            },
            SchemaOp::AddField {
                collection: "drop_col_tbl".into(),
                field_name: "extra".into(),
                default_value: serde_json::Value::String("ok".into()),
            },
        ],
        down: vec![],
    };
    schema.migrate_up(&fixed_migration).expect(
        "replaying migration after step 0 RemoveField succeeded must skip step 0 via receipt",
    );
    assert_eq!(schema.current_version().unwrap(), 1);

    let missing_col = Migration {
        version: 2,
        description: "remove absent column without receipt".into(),
        up: vec![SchemaOp::RemoveField {
            collection: "drop_col_tbl".into(),
            field_name: "obsolete".into(),
        }],
        down: vec![],
    };
    let missing_err = schema.migrate_up(&missing_col).unwrap_err();
    assert!(missing_err.to_string().contains("does not exist"));
}

#[test]
fn sql_remove_field_large_cutover_and_chunked_cleanup() {
    let dir = TempDir::new().unwrap();
    let db = OmniKV::open(
        dir.path().join("manifest.json").to_str().unwrap(),
        dir.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();
    let executor = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db.clone())));
    let schema = SchemaManager::new(db.clone(), Arc::new(IndexManager::new(db.clone())));

    execute(
        &executor,
        "CREATE TABLE big_remove_tbl (id INTEGER PRIMARY KEY, drop_me TEXT)",
    );
    let total_rows = WriteBatch::MAX_OPS + 50;
    let mut seed = WriteBatch::new();
    for i in 1..=total_rows {
        seed.set(
            &format!("\u{0}T\u{0}big_remove_tbl\u{0}{i}"),
            format!("{{\"id\":\"{i}\",\"drop_me\":\"val_{i}\"}}"),
        )
        .unwrap();
        if seed.op_count() >= WriteBatch::MAX_OPS / 2 {
            db.commit_batch(&seed).unwrap();
            seed = WriteBatch::new();
        }
    }
    if !seed.is_empty() {
        db.commit_batch(&seed).unwrap();
    }
    let big_remove = Migration {
        version: 1,
        description: "large column removal".into(),
        up: vec![SchemaOp::RemoveField {
            collection: "big_remove_tbl".into(),
            field_name: "drop_me".into(),
        }],
        down: vec![],
    };
    schema
        .migrate_up(&big_remove)
        .expect("large SQL RemoveField must succeed via durable cutover + chunked cleanup");
    assert_eq!(schema.current_version().unwrap(), 1);
    let check_row: serde_json::Value = serde_json::from_str(
        &db.find(
            &format!("\u{0}T\u{0}big_remove_tbl\u{0}{total_rows}"),
            db.get_seq(),
        )
        .unwrap()
        .unwrap(),
    )
    .unwrap();
    assert_eq!(check_row.get("drop_me"), None);
}

#[test]
fn integer_secondary_index_supports_full_sql_i128_domain() {
    let dir = TempDir::new().unwrap();
    let db = OmniKV::open(
        dir.path().join("manifest.json").to_str().unwrap(),
        dir.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();
    let executor = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db.clone())));
    let idx_mgr = IndexManager::new(db);

    execute(
        &executor,
        "CREATE TABLE bigints (id INTEGER PRIMARY KEY, n INTEGER)",
    );
    execute(
        &executor,
        "INSERT INTO bigints (id, n) VALUES (1, '-9223372036854775809')",
    );
    execute(&executor, "INSERT INTO bigints (id, n) VALUES (2, '42')");
    execute(
        &executor,
        "INSERT INTO bigints (id, n) VALUES (3, '9223372036854775808')",
    );

    idx_mgr
        .create_index(
            "bigints_n_idx",
            "bigints",
            vec![("n".into(), IndexFieldType::Integer)],
            false,
        )
        .unwrap();
    assert_eq!(idx_mgr.rebuild_index("bigints_n_idx").unwrap(), 3);

    let plan = explain_plan(
        &executor,
        "EXPLAIN SELECT id FROM bigints WHERE n = '9223372036854775808'",
    );
    assert!(plan.contains("Index Scan"), "plan: {plan}");
    assert_eq!(
        select_rows(
            &executor,
            "SELECT id FROM bigints WHERE n = '9223372036854775808'"
        ),
        vec![vec!["3"]]
    );
    assert_eq!(
        select_rows(
            &executor,
            "SELECT id FROM bigints WHERE n >= '42' ORDER BY id ASC"
        ),
        vec![vec!["2"], vec!["3"]]
    );
    assert_eq!(
        select_rows(&executor, "SELECT id FROM bigints WHERE n < '42'"),
        vec![vec!["1"]]
    );
    execute(
        &executor,
        "DELETE FROM bigints WHERE n = '9223372036854775808'",
    );
    assert_eq!(
        select_rows(
            &executor,
            "SELECT id FROM bigints WHERE n = '9223372036854775808'"
        )
        .len(),
        0
    );
}

#[test]
fn integer_secondary_index_upgrades_legacy_v0_on_rebuild() {
    let dir = TempDir::new().unwrap();
    let db = OmniKV::open(
        dir.path().join("manifest.json").to_str().unwrap(),
        dir.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();
    let executor = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db.clone())));
    let idx_mgr = IndexManager::new(db.clone());

    execute(
        &executor,
        "CREATE TABLE legacy_int_idx_tbl (id INTEGER PRIMARY KEY, n INTEGER)",
    );
    execute(
        &executor,
        "INSERT INTO legacy_int_idx_tbl (id, n) VALUES (1, '7')",
    );
    let mut seed_v0 = WriteBatch::new();
    let legacy_def_json = "{\"id\":77,\"name\":\"legacy_v0_n_idx\",\"collection\":\"legacy_int_idx_tbl\",\"fields\":[[\"n\",\"Integer\"]],\"unique\":false}";
    let encoded_7_i64 = format!("{:016x}", (7i64 ^ i64::MIN).cast_unsigned());
    seed_v0
        .set("\u{0}IDX_CATALOG\u{0}77", legacy_def_json.into())
        .unwrap();
    seed_v0
        .set("\u{0}IDX_READY\u{0}77", "ready".into())
        .unwrap();
    seed_v0.set("\u{0}IDX_NEXT_ID\u{0}", "78".into()).unwrap();
    seed_v0
        .set(
            &format!("\u{0}IDX\u{0}00000077\u{0}{encoded_7_i64}\u{0}1"),
            "1".into(),
        )
        .unwrap();
    db.commit_batch(&seed_v0).unwrap();

    assert_eq!(
        idx_mgr
            .definition("legacy_v0_n_idx")
            .unwrap()
            .encoding_version,
        0
    );
    assert_eq!(
        select_rows(&executor, "SELECT id FROM legacy_int_idx_tbl WHERE n = 7"),
        vec![vec!["1"]]
    );
    idx_mgr.rebuild_index("legacy_v0_n_idx").unwrap();
    assert_eq!(
        idx_mgr
            .definition("legacy_v0_n_idx")
            .unwrap()
            .encoding_version,
        omni_engine::secondary_index::CURRENT_INDEX_ENCODING_VERSION
    );
    execute(
        &executor,
        "INSERT INTO legacy_int_idx_tbl (id, n) VALUES (2, '9223372036854775808')",
    );
    assert_eq!(
        select_rows(
            &executor,
            "SELECT id FROM legacy_int_idx_tbl WHERE n = '9223372036854775808'"
        ),
        vec![vec!["2"]]
    );
}

#[test]
fn unpinned_compile_plan_and_plan_executor_do_not_omit_rows_when_index_not_ready() {
    let dir = TempDir::new().unwrap();
    let db = OmniKV::open(
        dir.path().join("manifest.json").to_str().unwrap(),
        dir.path().join("wal.bin").to_str().unwrap(),
    )
    .unwrap();
    let catalog = Arc::new(Catalog::new(db.clone()));
    let executor = SqlExecutor::new(db.clone(), catalog.clone());
    let idx_mgr = IndexManager::new(db.clone());

    execute(
        &executor,
        "CREATE TABLE rebuild_scan_tbl (id INTEGER PRIMARY KEY, tag TEXT)",
    );
    execute(
        &executor,
        "INSERT INTO rebuild_scan_tbl (id, tag) VALUES (1, 'hit'), (2, 'miss'), (3, 'hit')",
    );
    let idx_id = idx_mgr
        .create_index(
            "rebuild_scan_tag_idx",
            "rebuild_scan_tbl",
            vec![("tag".into(), IndexFieldType::String)],
            false,
        )
        .unwrap();
    idx_mgr.rebuild_index("rebuild_scan_tag_idx").unwrap();

    // Plan the IndexScan while the index is ready, then simulate rebuild_index_locked's
    // invalidation + clear window (ready marker removed and index entries cleared).
    let stmt = parse_sql("SELECT id FROM rebuild_scan_tbl WHERE tag = 'hit'").unwrap();
    let idx_cat = idx_mgr.catalog_snapshot().unwrap();
    let stats = omni_engine::optimizer::gather_stats(&catalog, Some(&idx_cat), &db);
    let plan = omni_engine::optimizer::Optimizer::new(stats)
        .optimize(&stmt)
        .unwrap();
    assert!(
        plan.to_string().contains("Index Scan"),
        "precondition: plan must contain Index Scan: {plan}"
    );

    let mut clear_wb = WriteBatch::new();
    clear_wb
        .delete(&format!("\u{0}IDX_READY\u{0}{idx_id}"))
        .unwrap();
    let idx_prefix = format!("\u{0}IDX\u{0}{idx_id:08}\u{0}");
    for (k, _) in db
        .scan(
            &idx_prefix,
            &format!("{idx_prefix}\u{10ffff}"),
            db.get_seq(),
        )
        .unwrap()
    {
        clear_wb.delete(&k).unwrap();
    }
    db.commit_batch(&clear_wb).unwrap();

    // Unpinned compile_plan must detect the unready index at the same seq and fall back
    // to SeqScan rather than returning zero rows.
    let mut volcano_iter = omni_engine::volcano::compile_plan(&plan, &db, &catalog);
    let volcano_rows = volcano_iter.try_collect_all().unwrap();
    assert_eq!(
        volcano_rows.len(),
        2,
        "unpinned compile_plan must not omit matching rows while index is not ready"
    );

    let plan_exec = omni_engine::plan_exec::PlanExecutor::new(db, catalog);
    let exec_rows = plan_exec.execute_plan(&plan).unwrap();
    assert_eq!(
        exec_rows.len(),
        2,
        "PlanExecutor must not omit matching rows while index is not ready"
    );
}
