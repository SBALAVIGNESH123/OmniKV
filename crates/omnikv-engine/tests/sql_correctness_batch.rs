//! Regression coverage for the SQL-correctness review findings: each test
//! reproduces a defect the review reported and pins the fixed behavior.

use omni_engine::OmniKV;
use omni_engine::catalog::Catalog;
use omni_engine::sql::*;
use omni_engine::sql_exec::*;
use std::sync::Arc;

fn create_env(prefix: &str) -> SqlExecutor {
    let dir = tempfile::tempdir().unwrap();
    let m = dir.path().join(format!("{prefix}_m.json"));
    let w = dir.path().join(format!("{prefix}_w.bin"));
    let db = OmniKV::open(m.to_str().unwrap(), w.to_str().unwrap()).unwrap();
    let catalog = Arc::new(Catalog::new(db.clone()));
    let exec = SqlExecutor::new(db, catalog);
    std::mem::forget(dir);
    exec
}

fn exec(executor: &SqlExecutor, sql: &str) {
    let stmt = parse_sql(sql).unwrap_or_else(|e| panic!("Parse error for '{sql}': {e}"));
    executor
        .execute(&stmt)
        .unwrap_or_else(|e| panic!("Exec error for '{sql}': {e}"));
}

fn rows(executor: &SqlExecutor, sql: &str) -> Vec<Vec<String>> {
    let stmt = parse_sql(sql).unwrap_or_else(|e| panic!("Parse error for '{sql}': {e}"));
    match executor.execute(&stmt) {
        Ok(ExecResult::Rows { rows, .. }) => rows,
        Ok(_) => panic!("expected rows for '{sql}'"),
        Err(e) => panic!("Exec error for '{sql}': {e}"),
    }
}

fn err(executor: &SqlExecutor, sql: &str) -> String {
    let stmt = parse_sql(sql).unwrap_or_else(|e| panic!("Parse error for '{sql}': {e}"));
    match executor.execute(&stmt) {
        Err(e) => e,
        Ok(_) => panic!("expected '{sql}' to fail"),
    }
}

fn sorted(mut rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.sort();
    rows
}

fn no_rows(executor: &SqlExecutor, sql: &str) {
    assert_eq!(
        rows(executor, sql),
        Vec::<Vec<String>>::new(),
        "expected '{sql}' to return no rows"
    );
}

/// A hand-written manifest with no `row_format` field, which is what the
/// pre-typed code wrote: serde reads it back as [`RowFormat::Legacy`].
fn legacy_env(prefix: &str, table: &str, extra_columns: &str) -> SqlExecutor {
    let dir = tempfile::tempdir().unwrap();
    let m = dir.path().join(format!("{prefix}_m.json"));
    let w = dir.path().join(format!("{prefix}_w.bin"));
    let db = OmniKV::open(m.to_str().unwrap(), w.to_str().unwrap()).unwrap();
    let manifest = format!(
        "{{\"name\":\"{table}\",\"columns\":[{{\"name\":\"id\",\"col_type\":\"Integer\",\"nullable\":false,\"primary_key\":true,\"default\":null}},{{\"name\":\"v\",\"col_type\":\"Text\",\"nullable\":true,\"primary_key\":false,\"default\":null}}{extra_columns}],\"primary_key\":\"id\",\"created_at\":1000}}"
    );
    let mut wb = omni_engine::WriteBatch::new();
    let catalog_key = format!("\u{0}CATALOG\u{0}{table}");
    wb.set(&catalog_key, manifest).unwrap();
    db.commit_batch(&wb).unwrap();
    let catalog = Arc::new(Catalog::new(db.clone()));
    let exec = SqlExecutor::new(db, catalog);
    std::mem::forget(dir);
    exec
}

// ═══════════════════════════════════════════════════════════════════════════
// Legacy row encoding
// ═══════════════════════════════════════════════════════════════════════════

/// A legacy table's stored "NULL" stays a SQL NULL, and a typed write of the
/// literal text 'NULL' stays text: the two never swap after a migration.
#[test]
fn legacy_null_still_null_after_literal_write() {
    let e = legacy_env("legacy_null", "legacy_t", "");
    let mut wb = omni_engine::WriteBatch::new();
    wb.set(
        "\u{0}T\u{0}legacy_t\u{0}1",
        "{\"id\":\"1\",\"v\":\"NULL\"}".to_string(),
    )
    .unwrap();
    wb.set(
        "\u{0}T\u{0}legacy_t\u{0}2",
        "{\"id\":\"2\",\"v\":\"hello\"}".to_string(),
    )
    .unwrap();
    e.db.commit_batch(&wb).unwrap();

    assert_eq!(
        rows(&e, "SELECT id FROM legacy_t WHERE v IS NULL"),
        vec![["1"]]
    );
    assert_eq!(
        rows(&e, "SELECT v FROM legacy_t WHERE v IS NULL"),
        vec![["NULL"]]
    );

    exec(&e, "INSERT INTO legacy_t (id, v) VALUES (3, 'NULL')");
    assert_eq!(
        sorted(rows(&e, "SELECT id FROM legacy_t WHERE v = 'NULL'")),
        vec![["3"]]
    );
    assert_eq!(
        rows(&e, "SELECT id FROM legacy_t WHERE v IS NULL"),
        vec![["1"]]
    );

    // UPDATE through the same migrated table keeps the distinction.
    exec(&e, "UPDATE legacy_t SET v = 'NULL' WHERE id = 2");
    assert_eq!(
        sorted(rows(&e, "SELECT id FROM legacy_t WHERE v = 'NULL'")),
        vec![["2"], ["3"]]
    );
    assert_eq!(
        rows(&e, "SELECT id FROM legacy_t WHERE v IS NULL"),
        vec![["1"]]
    );
}

/// A legacy numeric primary key stored non-canonically ("007") is still
/// reachable by a canonical lookup, on both the scan and plan paths.
#[test]
fn legacy_noncanonical_pk_is_reachable() {
    let e = legacy_env("legacy_pk", "legacy_t", "");
    let mut wb = omni_engine::WriteBatch::new();
    wb.set(
        "\u{0}T\u{0}legacy_t\u{0}007",
        "{\"id\":\"007\",\"v\":\"x\"}".to_string(),
    )
    .unwrap();
    e.db.commit_batch(&wb).unwrap();

    assert_eq!(rows(&e, "SELECT v FROM legacy_t WHERE id = 7"), vec![["x"]]);
    assert_eq!(
        rows(&e, "SELECT v FROM legacy_t WHERE id = '007'"),
        vec![["x"]]
    );
    let plan = rows(&e, "EXPLAIN ANALYZE SELECT v FROM legacy_t WHERE id = 7");
    assert!(
        plan.iter().any(|r| r[0].contains("actual rows=1")),
        "plan must find the legacy row: {plan:?}"
    );
}

/// A legacy row's non-canonical join key still joins a canonical partner key.
#[test]
fn legacy_noncanonical_join_key() {
    let dir = tempfile::tempdir().unwrap();
    let m = dir.path().join("legacy_join_m.json");
    let w = dir.path().join("legacy_join_w.bin");
    let db = OmniKV::open(m.to_str().unwrap(), w.to_str().unwrap()).unwrap();
    let mk = |name: &str| {
        format!(
            "{{\"name\":\"{name}\",\"columns\":[{{\"name\":\"id\",\"col_type\":\"Integer\",\"nullable\":false,\"primary_key\":true,\"default\":null}},{{\"name\":\"k\",\"col_type\":\"Integer\",\"nullable\":false,\"primary_key\":false,\"default\":null}}],\"primary_key\":\"id\",\"created_at\":1000}}"
        )
    };
    let mut wb = omni_engine::WriteBatch::new();
    wb.set("\u{0}CATALOG\u{0}a", mk("a")).unwrap();
    wb.set("\u{0}CATALOG\u{0}b", mk("b")).unwrap();
    wb.set(
        "\u{0}T\u{0}a\u{0}1",
        "{\"id\":\"1\",\"k\":\"7\"}".to_string(),
    )
    .unwrap();
    wb.set(
        "\u{0}T\u{0}b\u{0}9",
        "{\"id\":\"9\",\"k\":\"007\"}".to_string(),
    )
    .unwrap();
    db.commit_batch(&wb).unwrap();
    let catalog = Arc::new(Catalog::new(db.clone()));
    let e = SqlExecutor::new(db, catalog);
    std::mem::forget(dir);

    assert_eq!(
        rows(&e, "SELECT a.id, b.id FROM a JOIN b ON a.k = b.k"),
        vec![["1", "9"]]
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Three-valued logic
// ═══════════════════════════════════════════════════════════════════════════

/// IN with a NULL element is UNKNOWN when nothing matches, so neither IN nor
/// NOT IN returns the row; a match is still TRUE.
#[test]
fn in_not_in_three_valued_logic() {
    let e = create_env("in3vl");
    exec(&e, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    exec(
        &e,
        "INSERT INTO t (id, v) VALUES (1, 'a'), (2, 'b'), (3, NULL)",
    );

    assert_eq!(
        rows(&e, "SELECT id FROM t WHERE v IN ('a', NULL)"),
        vec![["1"]]
    );
    no_rows(&e, "SELECT id FROM t WHERE v IN ('z', NULL)");
    no_rows(&e, "SELECT id FROM t WHERE NOT (v IN ('z', NULL))");
    assert_eq!(
        sorted(rows(&e, "SELECT id FROM t WHERE NOT (v IN ('z'))")),
        vec![["1"], ["2"]]
    );
}

/// A subquery producing a NULL propagates UNKNOWN to IN, not a text match.
#[test]
fn in_subquery_null_is_unknown() {
    let e = create_env("insubq");
    exec(&e, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    exec(
        &e,
        "INSERT INTO t (id, v) VALUES (1, 'a'), (2, 'b'), (3, NULL)",
    );
    exec(&e, "CREATE TABLE s (id INTEGER PRIMARY KEY, w TEXT)");
    exec(&e, "INSERT INTO s (id, w) VALUES (1, 'a'), (2, NULL)");

    assert_eq!(
        rows(&e, "SELECT id FROM t WHERE v IN (SELECT w FROM s)"),
        vec![["1"]]
    );
    no_rows(&e, "SELECT id FROM t WHERE NOT (v IN (SELECT w FROM s))");
}

// ═══════════════════════════════════════════════════════════════════════════
// Aggregates
// ═══════════════════════════════════════════════════════════════════════════

/// An aggregate over no non-null values is SQL NULL, so HAVING ... IS NULL
/// keeps the row.
#[test]
fn aggregate_null_is_sql_null() {
    let e = create_env("aggnull");
    exec(&e, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    exec(&e, "INSERT INTO t (id, v) VALUES (1, NULL)");
    assert_eq!(
        rows(&e, "SELECT SUM(v) FROM t HAVING SUM(v) IS NULL"),
        vec![["NULL"]]
    );
}

/// MIN/MAX follow the column's declared affinity: TEXT orders lexically, so
/// MAX('2', '10') is "2", while an INTEGER column still orders numerically.
#[test]
fn min_max_follow_declared_affinity() {
    let e = create_env("minmax");
    exec(
        &e,
        "CREATE TABLE q (id INTEGER PRIMARY KEY, v TEXT, n INTEGER)",
    );
    exec(
        &e,
        "INSERT INTO q (id, v, n) VALUES (1, '2', 2), (2, '10', 10), (3, '1', 1)",
    );

    assert_eq!(rows(&e, "SELECT MIN(v) FROM q"), vec![["1"]]);
    assert_eq!(rows(&e, "SELECT MAX(v) FROM q"), vec![["2"]]);
    assert_eq!(rows(&e, "SELECT MIN(n) FROM q"), vec![["1"]]);
    assert_eq!(rows(&e, "SELECT MAX(n) FROM q"), vec![["10"]]);
    assert_eq!(rows(&e, "SELECT MAX(q.v) FROM q"), vec![["2"]]);
    assert_eq!(
        rows(&e, "SELECT id, MAX(v) FROM q GROUP BY id ORDER BY id"),
        vec![["1", "2"], ["2", "10"], ["3", "1"]]
    );
    // The legacy executor path agrees.
    assert_eq!(rows(&e, "SELECT MAX(v) FROM q WHERE id > 0"), vec![["2"]]);
}

/// A repeating integer average is rounded, not truncated at six places, and a
/// terminating one renders exactly.
#[test]
fn integer_avg_rounds_instead_of_truncating() {
    let e = create_env("avgprec");
    exec(&e, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    exec(&e, "INSERT INTO t (id, v) VALUES (1, 1), (2, 0), (3, 0)");
    assert_eq!(
        rows(&e, "SELECT AVG(v) FROM t"),
        vec![["0.333333333333333"]]
    );

    let e = create_env("avground");
    exec(&e, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    exec(&e, "INSERT INTO t (id, v) VALUES (1, 2)");
    for i in 2..=7 {
        exec(&e, &format!("INSERT INTO t (id, v) VALUES ({i}, 0)"));
    }
    // 2/7 = 0.2857142857142857... rounds half-up at the last kept place.
    assert_eq!(
        rows(&e, "SELECT AVG(v) FROM t"),
        vec![["0.285714285714286"]]
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Column resolution
// ═══════════════════════════════════════════════════════════════════════════

/// A qualified reference on a single table resolves through every operator,
/// not just the predicate: ORDER BY, GROUP BY, COUNT, MIN.
#[test]
fn qualified_reference_resolves_everywhere() {
    let e = create_env("qualref");
    exec(&e, "CREATE TABLE q (id INTEGER PRIMARY KEY, v TEXT)");
    exec(
        &e,
        "INSERT INTO q (id, v) VALUES (1, '2'), (2, '10'), (3, '1')",
    );

    assert_eq!(
        rows(&e, "SELECT v FROM q ORDER BY q.v"),
        vec![["1"], ["10"], ["2"]]
    );
    assert_eq!(
        sorted(rows(&e, "SELECT q.v FROM q GROUP BY q.v")),
        vec![["1"], ["10"], ["2"]]
    );
    assert_eq!(rows(&e, "SELECT COUNT(q.v) FROM q"), vec![["3"]]);
    assert_eq!(rows(&e, "SELECT MIN(q.v) FROM q"), vec![["1"]]);
}

/// Two rows whose grouped values differ only in where a NUL byte sits are two
/// groups, not one.
#[test]
fn group_by_key_cannot_collide_on_embedded_nul() {
    let e = create_env("grpby_nul");
    exec(
        &e,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT)",
    );
    exec(&e, "INSERT INTO t (id, a, b) VALUES (1, 'x\u{0}y', 'z')");
    exec(&e, "INSERT INTO t (id, a, b) VALUES (2, 'x', 'y\u{0}z')");
    assert_eq!(
        rows(&e, "SELECT COUNT(*) FROM t GROUP BY a, b"),
        vec![["1"], ["1"]]
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Join and plan consistency
// ═══════════════════════════════════════════════════════════════════════════

/// An unmatched outer-join row fills the optional side with NULLs under the
/// optional table's qualified names, on both the Volcano and plan executors.
#[test]
fn unmatched_outer_join_row_is_null_qualified() {
    let e = create_env("outerjoin");
    exec(&e, "CREATE TABLE a (id INTEGER PRIMARY KEY, k TEXT)");
    exec(
        &e,
        "CREATE TABLE b (id INTEGER PRIMARY KEY, k TEXT, w TEXT)",
    );
    exec(&e, "INSERT INTO a (id, k) VALUES (1, 'x'), (2, 'y')");
    exec(&e, "INSERT INTO b (id, k, w) VALUES (1, 'x', 'match')");

    assert_eq!(
        rows(
            &e,
            "SELECT a.id, b.id FROM a LEFT JOIN b ON a.k = b.k ORDER BY a.id"
        ),
        vec![["1", "1"], ["2", "NULL"]]
    );
    assert_eq!(
        rows(
            &e,
            "SELECT a.id, b.id FROM b RIGHT JOIN a ON a.k = b.k ORDER BY a.id"
        ),
        vec![["1", "1"], ["2", "NULL"]]
    );
}

/// The plan executor binds the optional side's qualified key to NULL, not to
/// the preserved side's bare value.
#[test]
fn plan_executor_null_fills_optional_join_side() {
    let dir = tempfile::tempdir().unwrap();
    let m = dir.path().join("planjoin_m.json");
    let w = dir.path().join("planjoin_w.bin");
    let db = OmniKV::open(m.to_str().unwrap(), w.to_str().unwrap()).unwrap();
    let catalog = Arc::new(Catalog::new(db.clone()));
    let e = SqlExecutor::new(db.clone(), catalog.clone());
    std::mem::forget(dir);
    exec(&e, "CREATE TABLE a (id INTEGER PRIMARY KEY, k TEXT)");
    exec(
        &e,
        "CREATE TABLE b (id INTEGER PRIMARY KEY, k TEXT, w TEXT)",
    );
    exec(&e, "INSERT INTO a (id, k) VALUES (1, 'x'), (2, 'y')");
    exec(&e, "INSERT INTO b (id, k, w) VALUES (1, 'x', 'match')");

    let stmt = parse_sql("SELECT a.id, b.id FROM a LEFT JOIN b ON a.k = b.k").unwrap();
    let stats = omni_engine::optimizer::gather_stats(&catalog, None, &db);
    let plan = omni_engine::optimizer::Optimizer::new(stats)
        .optimize(&stmt)
        .expect("plan");
    let rows = omni_engine::plan_exec::PlanExecutor::for_plan(db, catalog, &plan)
        .execute_plan(&plan)
        .expect("execute");

    let mut out: Vec<String> = rows
        .iter()
        .map(|row| {
            let aid = row
                .get("a.id")
                .cloned()
                .flatten()
                .unwrap_or_else(|| "NULL".into());
            let bid = row
                .get("b.id")
                .cloned()
                .flatten()
                .unwrap_or_else(|| "NULL".into());
            format!("{aid}/{bid}")
        })
        .collect();
    out.sort();
    assert_eq!(out, vec!["1/1", "2/NULL"]);
}

/// EXPLAIN ANALYZE filters by the column's declared affinity, so it reports
/// the same row count the query returns.
#[test]
fn explain_analyze_matches_typed_query() {
    let e = create_env("explain_typed");
    exec(&e, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    exec(&e, "INSERT INTO t (id, v) VALUES (1, '10'), (2, '2')");

    // TEXT '10' is not > '2' lexically, so no rows qualify.
    no_rows(&e, "SELECT id FROM t WHERE v > 2");
    let plan = rows(&e, "EXPLAIN ANALYZE SELECT id FROM t WHERE v > 2");
    assert!(
        plan.iter().any(|r| r[0].contains("actual rows=0")),
        "EXPLAIN ANALYZE must report 0 rows: {plan:?}"
    );
}

/// A JOIN ON column that does not exist is an error, not an empty result.
#[test]
fn join_on_unknown_column_errors() {
    let e = create_env("join_bad_col");
    exec(&e, "CREATE TABLE a (id INTEGER PRIMARY KEY, k TEXT)");
    exec(&e, "CREATE TABLE b (id INTEGER PRIMARY KEY, k TEXT)");
    let err = err(&e, "SELECT a.id FROM a JOIN b ON a.no_such_column = b.id");
    assert!(
        err.contains("does not exist"),
        "bad ON column must error: {err}"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Values and parameters
// ═══════════════════════════════════════════════════════════════════════════

/// An integer literal above the i64 range keeps its exact digits: f64 would
/// round it, and a bound parameter of the same magnitude behaves the same.
#[test]
fn i64_overflow_literal_stays_exact() {
    let e = create_env("i128_literal");
    exec(&e, "CREATE TABLE t (id INTEGER PRIMARY KEY, v BIGINT)");
    let big = "9223372036854775808"; // i64::MAX + 1
    exec(&e, &format!("INSERT INTO t (id, v) VALUES (1, {big})"));

    assert_eq!(rows(&e, "SELECT v FROM t"), vec![["9223372036854775808"]]);
    assert_eq!(
        rows(&e, "SELECT v FROM t WHERE v > 9223372036854775807"),
        vec![["9223372036854775808"]]
    );
    assert_eq!(
        rows(&e, "SELECT v FROM t WHERE v = 9223372036854775808"),
        vec![["9223372036854775808"]]
    );

    let stmt = parse_sql("SELECT v FROM t WHERE v = $1").unwrap();
    let bound = bind_statement_params(stmt, &[Some(big.to_string())]).unwrap();
    match e.execute(&bound) {
        Ok(ExecResult::Rows { rows, .. }) => assert_eq!(rows, vec![["9223372036854775808"]]),
        Ok(_) => panic!("bound i128 lookup returned no rows"),
        Err(e) => panic!("bound i128 lookup failed: {e}"),
    }
}

/// A bound numeric write is canonicalized at write time, so the row is
/// findable by a canonical lookup from any path.
#[test]
fn bound_numeric_write_is_canonical() {
    let e = create_env("bound_num");
    exec(
        &e,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER, f FLOAT)",
    );
    let stmt = parse_sql("INSERT INTO t (id, v, f) VALUES ($1, $2, $3)").unwrap();
    let bound = bind_statement_params(
        stmt,
        &[Some("007".into()), Some("42".into()), Some("1.50".into())],
    )
    .unwrap();
    e.execute(&bound).unwrap();

    assert_eq!(rows(&e, "SELECT id FROM t WHERE id = 7"), vec![["7"]]);
    assert_eq!(rows(&e, "SELECT id FROM t WHERE id = '007'"), vec![["7"]]);
    assert_eq!(rows(&e, "SELECT f FROM t"), vec![["1.5"]]);
    let plan = rows(&e, "EXPLAIN ANALYZE SELECT id FROM t WHERE id = 7");
    assert!(
        plan.iter().any(|r| r[0].contains("actual rows=1")),
        "PK lookup must find the canonical key: {plan:?}"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Error reporting
// ═══════════════════════════════════════════════════════════════════════════

/// UPDATE and DELETE on a missing table report the same wording as SELECT,
/// which is what maps the error to SQLSTATE 42P01.
#[test]
fn dml_missing_table_wording_matches_select() {
    let e = create_env("dml_missing");
    exec(&e, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    for q in [
        "SELECT id FROM nope",
        "UPDATE nope SET v = 1",
        "DELETE FROM nope",
    ] {
        let err = err(&e, q);
        assert!(
            err.contains("does not exist"),
            "'{q}' must say the table does not exist: {err}"
        );
    }
}
