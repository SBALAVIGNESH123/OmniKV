use omni_engine::{EmbeddedBatch, EmbeddedConfig, EmbeddedOmniKv, EmbeddedSqlResult, KeyValue};

#[test]
fn embedded_api_round_trips_namespaced_data_and_reopens() {
    let dir = tempfile::tempdir().expect("embedded db dir");
    let store = EmbeddedOmniKv::open(EmbeddedConfig::new(dir.path()).namespace("sketchlog"))
        .expect("open embedded store");

    store
        .put("streams/api/p95", "42.5")
        .expect("put percentile");
    store
        .write_batch(
            EmbeddedBatch::new()
                .put("streams/api/cardinality", "128")
                .put("streams/worker/p95", "80.0"),
        )
        .expect("write batch");

    assert_eq!(
        store.get("streams/api/p95").expect("get percentile"),
        Some("42.5".to_string())
    );

    let stream_rows = store
        .scan_prefix("streams/api/", None)
        .expect("scan stream prefix");
    assert_eq!(
        stream_rows,
        vec![
            KeyValue {
                key: "streams/api/cardinality".to_string(),
                value: "128".to_string(),
            },
            KeyValue {
                key: "streams/api/p95".to_string(),
                value: "42.5".to_string(),
            },
        ]
    );

    let snapshot = store.snapshot();
    store
        .put("streams/api/p95", "44.0")
        .expect("overwrite percentile");
    assert_eq!(
        store
            .get_at("streams/api/p95", &snapshot)
            .expect("snapshot read"),
        Some("42.5".to_string())
    );
    drop(snapshot);

    store.compact().expect("compact embedded store");
    let stats = store.stats();
    assert!(stats.sequence >= 3, "expected committed sequence growth");
    drop(store);

    let reopened = EmbeddedOmniKv::open(EmbeddedConfig::new(dir.path()).namespace("sketchlog"))
        .expect("reopen embedded store");
    assert_eq!(
        reopened.get("streams/api/p95").expect("read after reopen"),
        Some("44.0".to_string())
    );
}

#[test]
fn embedded_scopes_isolate_same_key_for_sketchlog_tenants() {
    let dir = tempfile::tempdir().expect("embedded db dir");
    let sketchlog = EmbeddedOmniKv::open(EmbeddedConfig::new(dir.path()).namespace("sketchlog"))
        .expect("open sketchlog scope");
    let tenant_a = sketchlog.scoped("tenant_a").expect("tenant a scope");
    let tenant_b = sketchlog.scoped("tenant_b").expect("tenant b scope");

    tenant_a.put("latest", "a").expect("put tenant a");
    tenant_b.put("latest", "b").expect("put tenant b");

    assert_eq!(
        tenant_a.get("latest").expect("read tenant a"),
        Some("a".to_string())
    );
    assert_eq!(
        tenant_b.get("latest").expect("read tenant b"),
        Some("b".to_string())
    );
    assert!(
        sketchlog
            .get("latest")
            .expect("read product namespace")
            .is_none(),
        "parent product namespace must not see tenant-local key"
    );
}

#[test]
fn embedded_backup_restore_roundtrip() {
    let source_dir = tempfile::tempdir().expect("source dir");
    let restore_dir = tempfile::tempdir().expect("restore dir");
    let backup_path = source_dir.path().join("omnikv-backup.tar.gz");
    let store = EmbeddedOmniKv::open(EmbeddedConfig::new(source_dir.path()).namespace("sketchlog"))
        .expect("open source store");

    store
        .put("telemetry/api/000001", r#"{"latency_ms":42}"#)
        .expect("put telemetry");
    store
        .create_backup(&backup_path)
        .expect("create embedded backup");
    drop(store);

    let restored = EmbeddedOmniKv::restore_from_backup(&backup_path, restore_dir.path())
        .expect("restore backup")
        .scoped("sketchlog")
        .expect("restore sketchlog scope");
    assert_eq!(
        restored
            .get("telemetry/api/000001")
            .expect("read restored telemetry"),
        Some(r#"{"latency_ms":42}"#.to_string())
    );
}

#[test]
fn embedded_encrypted_backup_restore_roundtrip() {
    let source_dir = tempfile::tempdir().expect("source dir");
    let restore_dir = tempfile::tempdir().expect("restore dir");
    let backup_path = source_dir.path().join("omnikv-backup.enc");
    let store = EmbeddedOmniKv::open(EmbeddedConfig::new(source_dir.path()).namespace("sketchlog"))
        .expect("open source store");

    store
        .put("sketches/api/p99", "91.4")
        .expect("put sketch state");
    store
        .create_encrypted_backup(&backup_path, "correct horse battery staple")
        .expect("create encrypted backup");
    drop(store);

    let restored = EmbeddedOmniKv::restore_from_encrypted_backup(
        &backup_path,
        restore_dir.path(),
        "correct horse battery staple",
    )
    .expect("restore encrypted backup")
    .scoped("sketchlog")
    .expect("restore sketchlog scope");

    assert_eq!(
        restored
            .get("sketches/api/p99")
            .expect("read restored sketch state"),
        Some("91.4".to_string())
    );
}

#[test]
fn embedded_sql_execution_uses_engine_global_tables() {
    let dir = tempfile::tempdir().expect("embedded db dir");
    let store = EmbeddedOmniKv::open_dir(dir.path()).expect("open embedded store");

    assert!(matches!(
        store
            .execute_sql("CREATE TABLE metrics (id INT PRIMARY KEY, name TEXT)")
            .expect("create table"),
        EmbeddedSqlResult::Ok(_)
    ));
    assert!(matches!(
        store
            .execute_sql("INSERT INTO metrics (id, name) VALUES (1, 'api')")
            .expect("insert row"),
        EmbeddedSqlResult::Modified { count: 1, .. }
    ));

    let result = store
        .execute_sql("SELECT name FROM metrics WHERE id = 1")
        .expect("select row");
    assert_eq!(
        result,
        EmbeddedSqlResult::Rows {
            columns: vec!["name".to_string()],
            rows: vec![vec!["api".to_string()]],
        }
    );
}
