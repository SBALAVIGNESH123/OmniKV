//! Regression coverage for the SQL-correctness review findings: each test
//! reproduces a defect the review reported and pins the fixed behavior.

use omni_engine::OmniKV;
use omni_engine::catalog::Catalog;
use omni_engine::sql::*;
use omni_engine::sql_exec::*;
use std::sync::{Arc, Barrier};
use std::thread;

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
        Ok(ExecResult::Rows { rows, .. }) => rows
            .into_iter()
            .map(|r| omni_engine::sql_exec::render_row(&r))
            .collect(),
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

    // A typed write migrates the table, re-keying the legacy row at ...7.
    // The typed PK lookup and the keys UPDATE/DELETE build both expect it
    // there, so the row must stay reachable by either form.
    exec(&e, "INSERT INTO legacy_t (id, v) VALUES (9, 'y')");
    assert_eq!(rows(&e, "SELECT v FROM legacy_t WHERE id = 7"), vec![["x"]]);
    assert_eq!(
        rows(&e, "SELECT v FROM legacy_t WHERE id = '007'"),
        vec![["x"]]
    );
    assert_eq!(
        sorted(rows(&e, "SELECT id FROM legacy_t")),
        vec![["7"], ["9"]]
    );

    exec(&e, "UPDATE legacy_t SET v = 'z' WHERE id = 7");
    assert_eq!(rows(&e, "SELECT v FROM legacy_t WHERE id = 7"), vec![["z"]]);
    exec(&e, "DELETE FROM legacy_t WHERE id = 7");
    no_rows(&e, "SELECT v FROM legacy_t WHERE id = 7");
    assert_eq!(rows(&e, "SELECT v FROM legacy_t WHERE id = 9"), vec![["y"]]);
}

/// Two legacy keys that canonicalize to one are reported instead of one
/// silently overwriting the other, and the table is left as it was.
#[test]
fn legacy_pk_collision_blocks_migration() {
    let e = legacy_env("legacy_pk_collide", "legacy_t", "");
    let mut wb = omni_engine::WriteBatch::new();
    wb.set(
        "\u{0}T\u{0}legacy_t\u{0}007",
        "{\"id\":\"007\",\"v\":\"x\"}".to_string(),
    )
    .unwrap();
    wb.set(
        "\u{0}T\u{0}legacy_t\u{0}7",
        "{\"id\":\"7\",\"v\":\"y\"}".to_string(),
    )
    .unwrap();
    e.db.commit_batch(&wb).unwrap();

    let err = err(&e, "INSERT INTO legacy_t (id, v) VALUES (9, 'z')");
    assert!(
        err.contains("canonicalize"),
        "colliding legacy keys must be reported: {err}"
    );
    assert!(
        err.contains("'007'") && err.contains("'7'"),
        "both colliding values must be named: {err}"
    );
    // The table stays legacy, so both rows are still reachable.
    assert_eq!(
        sorted(rows(&e, "SELECT v FROM legacy_t")),
        vec![["x"], ["y"]]
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

/// A window function's ORDER BY resolves a qualified join column, so the
/// ranking reflects that column's values rather than reading every row as a
/// tie.
#[test]
fn window_order_by_resolves_a_qualified_join_column() {
    let e = create_env("window_join_order");
    exec(&e, "CREATE TABLE a (id INTEGER PRIMARY KEY)");
    exec(
        &e,
        "CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER, v INTEGER)",
    );
    exec(&e, "INSERT INTO a (id) VALUES (1), (2), (3)");
    exec(
        &e,
        "INSERT INTO b (id, a_id, v) VALUES (1, 1, 5), (2, 2, 15), (3, 3, 25)",
    );

    assert_eq!(
        rows(
            &e,
            "SELECT b.id, ROW_NUMBER() OVER (ORDER BY b.v DESC) \
             FROM a JOIN b ON a.id = b.a_id"
        ),
        vec![["3", "1"], ["2", "2"], ["1", "3"]]
    );

    exec(&e, "INSERT INTO b (id, a_id, v) VALUES (4, 1, 15)");
    assert_eq!(
        sorted(rows(
            &e,
            "SELECT b.id, RANK() OVER (ORDER BY b.v) FROM a JOIN b ON a.id = b.a_id"
        )),
        vec![["1", "1"], ["2", "2"], ["3", "4"], ["4", "2"]]
    );
    assert_eq!(
        sorted(rows(
            &e,
            "SELECT b.id, DENSE_RANK() OVER (ORDER BY b.v) \
             FROM a JOIN b ON a.id = b.a_id"
        )),
        vec![["1", "1"], ["2", "2"], ["3", "3"], ["4", "2"]]
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
        Ok(ExecResult::Rows { rows, .. }) => {
            assert_eq!(rows, vec![vec![Some("9223372036854775808".to_string())]]);
        }
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

// ═══════════════════════════════════════════════════════════════════════════
// Migration atomicity and catalog currency
// ═══════════════════════════════════════════════════════════════════════════

/// The row format recorded on disk for a table, independent of any
/// executor's cached copy.
fn persisted_row_format(e: &SqlExecutor, table: &str) -> omni_engine::catalog::RowFormat {
    let key = format!("\u{0}CATALOG\u{0}{table}");
    let seq = e.db.get_seq();
    let stored =
        e.db.scan(&key, &format!("{key}\u{7F}"), seq)
            .expect("catalog scan");
    let (_, value) = &stored[0];
    serde_json::from_str::<omni_engine::catalog::TableDef>(value)
        .expect("catalog manifest")
        .row_format
}

/// A statement that fails validation must not leave the table migrated.
#[test]
fn failed_legacy_write_does_not_flip_format() {
    let e = legacy_env("failed_legacy_write", "legacy_t", "");
    let mut wb = omni_engine::WriteBatch::new();
    wb.set(
        "\u{0}T\u{0}legacy_t\u{0}1",
        "{\"id\":\"1\",\"v\":\"NULL\"}".to_string(),
    )
    .unwrap();
    e.db.commit_batch(&wb).unwrap();

    let err = err(&e, "INSERT INTO legacy_t (no_such_column) VALUES (2)");
    assert!(
        err.contains("does not exist"),
        "the unknown column must be rejected: {err}"
    );
    assert_eq!(
        persisted_row_format(&e, "legacy_t"),
        omni_engine::catalog::RowFormat::Legacy,
        "a failed statement must not migrate the table"
    );
    assert_eq!(
        rows(&e, "SELECT id FROM legacy_t WHERE v IS NULL"),
        vec![["1"]]
    );
}

/// A legacy row that cannot be decoded must block migration, not be skipped.
#[test]
fn malformed_legacy_row_blocks_migration() {
    let e = legacy_env("malformed_legacy", "legacy_t", "");
    let mut wb = omni_engine::WriteBatch::new();
    wb.set("\u{0}T\u{0}legacy_t\u{0}1", "this is not json".to_string())
        .unwrap();
    e.db.commit_batch(&wb).unwrap();

    let err = err(&e, "INSERT INTO legacy_t (id, v) VALUES (2, 'x')");
    assert!(
        err.contains("cannot be decoded"),
        "an undecodable row must abort migration: {err}"
    );
    assert_eq!(
        persisted_row_format(&e, "legacy_t"),
        omni_engine::catalog::RowFormat::Legacy,
        "the table must stay legacy when migration fails"
    );
}

/// A transaction's snapshot pins the schema it saw: a migration committed
/// by another connection must not change how the snapshot decodes its rows.
#[test]
fn legacy_migration_respects_transaction_snapshot() {
    let e = legacy_env("legacy_snap", "legacy_t", "");
    let mut wb = omni_engine::WriteBatch::new();
    wb.set(
        "\u{0}T\u{0}legacy_t\u{0}1",
        "{\"id\":\"1\",\"v\":\"NULL\"}".to_string(),
    )
    .unwrap();
    e.db.commit_batch(&wb).unwrap();
    let snap_seq = e.db.get_seq();

    // Another connection migrates the table after the snapshot was taken.
    let other = SqlExecutor::new(e.db.clone(), Arc::new(Catalog::new(e.db.clone())));
    exec(&other, "INSERT INTO legacy_t (id, v) VALUES (2, 'x')");

    let snap_catalog = Arc::new(Catalog::at_snapshot(e.db.clone(), snap_seq));
    let snap = SqlExecutor::with_snapshot(e.db, snap_catalog, snap_seq);
    assert_eq!(
        rows(&snap, "SELECT id FROM legacy_t WHERE v IS NULL"),
        vec![["1"]],
        "the snapshot must still read its legacy NULL as a SQL NULL"
    );
}

/// An executor holding a stale legacy catalog must not re-migrate rows that
/// another executor already wrote as typed.
#[test]
fn stale_catalog_does_not_remigrate_typed_rows() {
    let e = legacy_env("stale_catalog", "legacy_t", "");

    // A second executor migrates the table and stores the literal text 'NULL'.
    let other = SqlExecutor::new(e.db.clone(), Arc::new(Catalog::new(e.db.clone())));
    exec(&other, "INSERT INTO legacy_t (id, v) VALUES (2, 'NULL')");

    // This executor's cached copy still says legacy.
    exec(&e, "INSERT INTO legacy_t (id, v) VALUES (3, 'x')");

    assert_eq!(
        sorted(rows(&e, "SELECT id FROM legacy_t WHERE v = 'NULL'")),
        vec![["2"]],
        "the typed literal text must survive a stale executor's write"
    );
}

/// A stale executor must refresh the persisted row format before a read too;
/// otherwise a typed literal "NULL" is decoded as a legacy SQL NULL.
#[test]
fn stale_catalog_select_uses_persisted_row_format() {
    let e = legacy_env("stale_catalog_select", "legacy_t", "");

    let other = SqlExecutor::new(e.db.clone(), Arc::new(Catalog::new(e.db.clone())));
    exec(&other, "INSERT INTO legacy_t (id, v) VALUES (2, 'NULL')");

    assert_eq!(
        rows(&e, "SELECT id FROM legacy_t WHERE v = 'NULL'"),
        vec![["2"]],
        "a stale SELECT must preserve typed literal text"
    );
    assert_eq!(
        rows(&e, "SELECT id FROM legacy_t WHERE v IS NULL"),
        Vec::<Vec<String>>::new(),
        "a typed literal must not match IS NULL through a stale catalog"
    );
}

/// The per-table SQL lock must cover the uniqueness check and commit, not
/// merely the legacy migration. Two concurrent autocommit INSERTs for one
/// typed primary key therefore produce one success and one duplicate-key
/// error rather than silently letting the later version win.
#[test]
fn concurrent_autocommit_insert_preserves_primary_key_uniqueness() {
    let e = create_env("concurrent_pk");
    exec(&e, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");

    let barrier = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for value in ["first", "second"] {
        let db = e.db.clone();
        let catalog = Arc::new(Catalog::new(db.clone()));
        let executor = SqlExecutor::new(db, catalog);
        let barrier = barrier.clone();
        let value = value.to_string();
        workers.push(thread::spawn(move || {
            barrier.wait();
            executor.execute(
                &parse_sql(&format!("INSERT INTO t (id, v) VALUES (1, '{value}')"))
                    .expect("parse concurrent insert"),
            )
        }));
    }
    barrier.wait();

    let results: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().expect("concurrent insert worker panicked"))
        .collect();
    let summaries: Vec<String> = results
        .iter()
        .map(|result| match result {
            Ok(ExecResult::Modified { command, .. }) => command.clone(),
            Ok(ExecResult::Rows { .. }) => "rows".into(),
            Ok(ExecResult::Ok(message)) => message.clone(),
            Err(error) => error.clone(),
        })
        .collect();
    assert_eq!(
        results.iter().filter(|result| result.is_ok()).count(),
        1,
        "exactly one insert may claim the primary key: {summaries:?}"
    );
    assert_eq!(
        results
            .iter()
            .filter(|result| result
                .as_ref()
                .is_err_and(|error| error.contains("duplicate key")))
            .count(),
        1,
        "the losing insert must report a duplicate-key error: {summaries:?}"
    );
    assert_eq!(rows(&e, "SELECT COUNT(*) FROM t"), vec![vec!["1"]]);
}

/// Two SQL writers that both see a legacy catalog must serialize the
/// migration itself. Both inserts should succeed, while the old sentinel
/// remains SQL NULL and a typed literal "NULL" remains text.
#[test]
fn concurrent_legacy_migrations_are_serialized() {
    let e = legacy_env("concurrent_legacy", "legacy_t", "");
    let mut seed = omni_engine::WriteBatch::new();
    seed.set(
        "\u{0}T\u{0}legacy_t\u{0}1",
        "{\"id\":\"1\",\"v\":\"NULL\"}".to_string(),
    )
    .unwrap();
    e.db.commit_batch(&seed).unwrap();

    let barrier = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for (id, value) in [("2", "NULL"), ("3", "other")] {
        let db = e.db.clone();
        let catalog = Arc::new(Catalog::new(db.clone()));
        let executor = SqlExecutor::new(db, catalog);
        let barrier = barrier.clone();
        workers.push(thread::spawn(move || {
            barrier.wait();
            executor.execute(
                &parse_sql(&format!(
                    "INSERT INTO legacy_t (id, v) VALUES ({id}, '{value}')"
                ))
                .expect("parse concurrent legacy insert"),
            )
        }));
    }
    barrier.wait();

    for worker in workers {
        assert!(
            worker
                .join()
                .expect("concurrent legacy worker panicked")
                .is_ok(),
            "both serialized legacy migrations must succeed"
        );
    }

    assert_eq!(
        sorted(rows(&e, "SELECT id FROM legacy_t WHERE v = 'NULL'")),
        vec![vec!["2"]]
    );
    assert_eq!(
        rows(&e, "SELECT id FROM legacy_t WHERE v IS NULL"),
        vec![vec!["1"]]
    );
}

/// A long-lived executor must not resurrect a table after another executor
/// drops it. The DML target is re-resolved while holding the same per-table
/// lock used by DROP TABLE.
#[test]
fn stale_catalog_dml_rejects_dropped_table() {
    let e = legacy_env("stale_after_drop", "legacy_t", "");
    let other = SqlExecutor::new(e.db.clone(), Arc::new(Catalog::new(e.db.clone())));
    exec(&other, "DROP TABLE legacy_t");

    let error = err(&e, "INSERT INTO legacy_t (id, v) VALUES (1, 'x')");
    assert!(
        error.contains("does not exist"),
        "a stale executor must not recreate a dropped table: {error}"
    );
    assert_eq!(
        rows(&e, "SHOW TABLES"),
        Vec::<Vec<String>>::new(),
        "the dropped table must remain absent"
    );

    // The same stale executor must be able to create the name again after the
    // persisted DROP, rather than failing against its obsolete cache entry.
    exec(&e, "CREATE TABLE legacy_t (id INTEGER PRIMARY KEY, v TEXT)");
    assert_eq!(rows(&e, "SHOW TABLES"), vec![vec!["legacy_t"]]);
}

// ═══════════════════════════════════════════════════════════════════════════
// NULL at the result boundary
// ═══════════════════════════════════════════════════════════════════════════

/// A subquery returning the literal text 'NULL' must not turn it into a SQL
/// NULL on the way out.
#[test]
fn subquery_literal_null_text_is_not_sql_null() {
    let e = create_env("subq_literal_null");
    exec(&e, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    exec(&e, "CREATE TABLE s (id INTEGER PRIMARY KEY, v TEXT)");
    exec(&e, "INSERT INTO t (id, v) VALUES (1, 'NULL')");
    exec(&e, "INSERT INTO s (id, v) VALUES (1, 'NULL')");

    assert_eq!(
        rows(&e, "SELECT id FROM t WHERE v IN (SELECT v FROM s)"),
        vec![["1"]]
    );
}

/// SQL NULL and the literal text "NULL" are two distinct values, so a UNION
/// keeps both rows.
#[test]
fn set_operation_keeps_null_separate_from_literal_text() {
    let e = create_env("set_null_text");
    exec(&e, "CREATE TABLE a (id INTEGER PRIMARY KEY, v TEXT)");
    exec(&e, "CREATE TABLE b (id INTEGER PRIMARY KEY, v TEXT)");
    exec(&e, "INSERT INTO a (id, v) VALUES (1, NULL)");
    exec(&e, "INSERT INTO b (id, v) VALUES (2, 'NULL')");

    assert_eq!(
        rows(&e, "SELECT v FROM a UNION SELECT v FROM b"),
        vec![["NULL"], ["NULL"]]
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Aggregate and window semantics
// ═══════════════════════════════════════════════════════════════════════════

/// SUM over values past the i64 limit stays exact: the integer domain is
/// i128, so accumulation must not fall back to f64.
#[test]
fn big_integer_sum_is_exact() {
    let e = create_env("big_int_sum");
    exec(&e, "CREATE TABLE t (id INTEGER PRIMARY KEY, v BIGINT)");
    exec(&e, "INSERT INTO t (id, v) VALUES (1, 9223372036854775808)");
    exec(&e, "INSERT INTO t (id, v) VALUES (2, 9223372036854775809)");

    assert_eq!(
        rows(&e, "SELECT SUM(v) FROM t"),
        vec![["18446744073709551617"]]
    );
}

/// Aggregating two valid i128 values must not panic in debug or wrap in
/// release when the sum exceeds the width of one stored integer.
#[test]
fn aggregate_sum_and_avg_are_safe_past_i128() {
    let e = create_env("big_int_overflow");
    exec(&e, "CREATE TABLE t (id INTEGER PRIMARY KEY, v BIGINT)");
    let max = "170141183460469231731687303715884105727";
    exec(&e, &format!("INSERT INTO t (id, v) VALUES (1, {max})"));
    exec(&e, &format!("INSERT INTO t (id, v) VALUES (2, {max})"));

    assert_eq!(
        rows(&e, "SELECT SUM(v) FROM t"),
        vec![vec!["340282366920938463463374607431768211454".to_string()]]
    );
    assert_eq!(
        rows(&e, "SELECT AVG(v) FROM t"),
        vec![vec![max.to_string()]]
    );
}

/// Canonical numeric primary-key aliases are duplicates, both across
/// statements and within one multi-row INSERT; a failed statement is atomic.
#[test]
fn insert_rejects_canonical_primary_key_duplicates() {
    let e = create_env("insert_pk_dupes");
    exec(&e, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    exec(&e, "INSERT INTO t (id, v) VALUES (7, 'first')");

    let duplicate = err(&e, "INSERT INTO t (id, v) VALUES ('007', 'second')");
    assert!(
        duplicate.contains("duplicate key"),
        "duplicate error: {duplicate}"
    );
    assert_eq!(
        rows(&e, "SELECT v FROM t WHERE id = 7"),
        vec![vec!["first".to_string()]]
    );

    let duplicate_in_batch = err(&e, "INSERT INTO t (id, v) VALUES (8, 'a'), (8, 'b')");
    assert!(
        duplicate_in_batch.contains("duplicate key"),
        "in-statement duplicate error: {duplicate_in_batch}"
    );
    assert_eq!(
        rows(&e, "SELECT id FROM t WHERE id = 8"),
        Vec::<Vec<String>>::new()
    );
}

/// HAVING and ORDER BY can reference hidden group/aggregate expressions, and
/// HAVING must filter before an outer LIMIT consumes a group.
#[test]
fn grouped_having_retains_hidden_expressions_and_filters_before_limit() {
    let e = create_env("having_hidden");
    exec(&e, "CREATE TABLE t (id INTEGER PRIMARY KEY, region TEXT)");
    exec(
        &e,
        "INSERT INTO t (id, region) VALUES (1, 'a'), (2, 'b'), (3, 'b')",
    );

    assert_eq!(
        rows(
            &e,
            "SELECT region FROM t GROUP BY region HAVING COUNT(*) > 1"
        ),
        vec![vec!["b".to_string()]]
    );
    assert_eq!(
        rows(
            &e,
            "SELECT COUNT(*) FROM t GROUP BY region HAVING region = 'b'"
        ),
        vec![vec!["2".to_string()]]
    );
    assert_eq!(
        rows(
            &e,
            "SELECT region, COUNT(*) FROM t GROUP BY region \
             HAVING COUNT(*) > 1 ORDER BY region LIMIT 1"
        ),
        vec![vec!["b".to_string(), "2".to_string()]]
    );

    let invalid = err(
        &e,
        "SELECT region, COUNT(*) FROM t GROUP BY region HAVING no_such_column = 1",
    );
    assert!(
        invalid.contains("does not exist"),
        "invalid HAVING: {invalid}"
    );
}

/// A star is an expansion item, not a replacement for the rest of the SELECT
/// list. Explicit columns remain visible and are still validated.
#[test]
fn star_mixed_with_explicit_columns_is_preserved_and_validated() {
    let e = create_env("star_mixed");
    exec(&e, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    exec(&e, "INSERT INTO t (id, v) VALUES (1, 'x')");

    assert_eq!(
        rows(&e, "SELECT *, id FROM t"),
        vec![vec!["1".to_string(), "x".to_string(), "1".to_string(),]]
    );
    let invalid = err(&e, "SELECT *, no_such_column FROM t");
    assert!(
        invalid.contains("does not exist"),
        "invalid mixed star: {invalid}"
    );
}

/// JOIN ON uses the same exact-case read contract as WHERE and projection;
/// accepting a wrong-case key must not silently produce an empty join.
#[test]
fn join_on_wrong_case_is_rejected() {
    let e = create_env("join_case");
    exec(&e, "CREATE TABLE a (id INTEGER PRIMARY KEY, k TEXT)");
    exec(&e, "CREATE TABLE b (id INTEGER PRIMARY KEY, k TEXT)");
    exec(&e, "INSERT INTO a (id, k) VALUES (1, 'x')");
    exec(&e, "INSERT INTO b (id, k) VALUES (1, 'x')");

    let invalid = err(&e, "SELECT a.id, b.id FROM a JOIN b ON a.K = b.K");
    assert!(
        invalid.contains("does not exist"),
        "invalid JOIN key: {invalid}"
    );
}

/// The first row of a window always starts its own peer group, so a leading
/// NULL is ranked 1 rather than tied with nothing.
#[test]
fn rank_of_leading_null_is_one() {
    let e = create_env("rank_leading_null");
    exec(&e, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    exec(&e, "INSERT INTO t (id, v) VALUES (1, NULL)");
    exec(&e, "INSERT INTO t (id, v) VALUES (2, 5)");

    assert_eq!(
        rows(&e, "SELECT v, RANK() OVER (ORDER BY v DESC) FROM t"),
        vec![["NULL", "1"], ["5", "2"]]
    );
    assert_eq!(
        rows(&e, "SELECT v, DENSE_RANK() OVER (ORDER BY v DESC) FROM t"),
        vec![["NULL", "1"], ["5", "2"]]
    );
}

/// A grouped TEXT column keeps lexical ordering above the aggregate: the
/// sort must not fall back to the numeric-looking heuristic.
#[test]
fn group_by_preserves_text_sort_affinity() {
    let e = create_env("group_text_order");
    exec(&e, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    exec(&e, "INSERT INTO t (id, v) VALUES (1, '2')");
    exec(&e, "INSERT INTO t (id, v) VALUES (2, '10')");
    exec(&e, "INSERT INTO t (id, v) VALUES (3, '1')");

    assert_eq!(
        rows(&e, "SELECT v, COUNT(*) FROM t GROUP BY v ORDER BY v"),
        vec![["1", "1"], ["10", "1"], ["2", "1"]]
    );
}

/// `SELECT *` expands the whole row, but its ORDER BY still names a column
/// and an unqualified name shared by both join sides is still ambiguous.
#[test]
fn star_does_not_skip_ambiguous_order_validation() {
    let e = create_env("star_ambiguous");
    exec(&e, "CREATE TABLE a (id INTEGER PRIMARY KEY, k INTEGER)");
    exec(&e, "CREATE TABLE b (id INTEGER PRIMARY KEY, k INTEGER)");
    exec(&e, "INSERT INTO a (id, k) VALUES (1, 1)");
    exec(&e, "INSERT INTO b (id, k) VALUES (1, 1)");

    let err = err(&e, "SELECT * FROM a JOIN b ON a.k = b.k ORDER BY id");
    assert!(
        err.contains("ambiguous"),
        "SELECT * must not skip ORDER BY validation: {err}"
    );
}

/// Statistics decode rows with the table's own format, so a typed table's
/// real NULLs are counted in the histogram.
#[test]
fn stats_decode_typed_null_rows() {
    let e = create_env("stats_typed_null");
    exec(&e, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    exec(&e, "INSERT INTO t (id, v) VALUES (1, NULL)");
    exec(&e, "INSERT INTO t (id, v) VALUES (2, 'x')");

    let stats = omni_engine::optimizer::gather_stats(&e.catalog, None, &e.db);
    let table = &stats["t"];
    let v = table
        .histograms
        .iter()
        .find(|h| h.column == "v")
        .expect("a histogram for v");
    assert!(
        (v.null_fraction - 0.5).abs() < 1e-9,
        "one of two rows is NULL: {:?}",
        table.histograms
    );
    assert_eq!(v.distinct_count, 1, "the literal 'x' is the only value");
}
