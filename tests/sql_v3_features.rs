// ═══════════════════════════════════════════════════════════════════════════
// SQL v3 Feature Tests — OFFSET, HAVING, Subqueries, UNION, RIGHT JOIN
// ═══════════════════════════════════════════════════════════════════════════

use omni_engine::OmniKV;
use omni_engine::sql::*;
use omni_engine::sql_exec::*;
use std::sync::Arc;

/// Helper: create DB + catalog + executor
fn create_sql_env(prefix: &str) -> (Arc<OmniKV>, SqlExecutor) {
    let dir = tempfile::tempdir().unwrap();
    let m = dir.path().join(format!("{}_m.json", prefix));
    let w = dir.path().join(format!("{}_w.bin", prefix));
    let db = OmniKV::open(m.to_str().unwrap(), w.to_str().unwrap()).unwrap();
    let catalog = Arc::new(omni_engine::catalog::Catalog::new(db.clone()));
    let exec = SqlExecutor::new(db.clone(), catalog);
    std::mem::forget(dir);
    (db, exec)
}

fn exec_sql(executor: &SqlExecutor, sql: &str) -> ExecResult {
    let stmt = parse_sql(sql).unwrap_or_else(|e| panic!("Parse error for '{}': {}", sql, e));
    executor
        .execute(&stmt)
        .unwrap_or_else(|e| panic!("Exec error for '{}': {}", sql, e))
}

fn exec_rows(executor: &SqlExecutor, sql: &str) -> (Vec<String>, Vec<Vec<String>>) {
    match exec_sql(executor, sql) {
        ExecResult::Rows { columns, rows } => (columns, rows),
        _ => panic!("Expected Rows result for: {}", sql),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// OFFSET tests
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_offset_basic() {
    let (_db, exec) = create_sql_env("off1");

    exec_sql(
        &exec,
        "CREATE TABLE off_test (id INTEGER PRIMARY KEY, val TEXT)",
    );
    exec_sql(
        &exec,
        "INSERT INTO off_test (id, val) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')",
    );

    // LIMIT 2 OFFSET 2 → skip first 2, take 2
    let (_cols, rows) = exec_rows(
        &exec,
        "SELECT id FROM off_test ORDER BY id LIMIT 2 OFFSET 2",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], "3");
    assert_eq!(rows[1][0], "4");

    println!("✅ OFFSET: LIMIT 2 OFFSET 2 → rows 3, 4");
}

#[test]
fn test_offset_past_end() {
    let (_db, exec) = create_sql_env("off2");

    exec_sql(&exec, "CREATE TABLE off_end (id INTEGER PRIMARY KEY)");
    exec_sql(&exec, "INSERT INTO off_end (id) VALUES (1), (2), (3)");

    // OFFSET past all rows → empty result
    let (_cols, rows) = exec_rows(&exec, "SELECT id FROM off_end LIMIT 10 OFFSET 100");
    assert_eq!(rows.len(), 0);

    println!("✅ OFFSET: Past end returns empty result");
}

#[test]
fn test_offset_zero() {
    let (_db, exec) = create_sql_env("off3");

    exec_sql(&exec, "CREATE TABLE off_zero (id INTEGER PRIMARY KEY)");
    exec_sql(&exec, "INSERT INTO off_zero (id) VALUES (1), (2)");

    let (_cols, rows) = exec_rows(&exec, "SELECT id FROM off_zero LIMIT 10 OFFSET 0");
    assert_eq!(rows.len(), 2);

    println!("✅ OFFSET: OFFSET 0 returns all rows");
}

// ═══════════════════════════════════════════════════════════════════════════
// HAVING tests
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_having_basic() {
    let (_db, exec) = create_sql_env("hav1");

    exec_sql(
        &exec,
        "CREATE TABLE hav_sales (id INTEGER PRIMARY KEY, region TEXT, amount INTEGER)",
    );
    exec_sql(
        &exec,
        "INSERT INTO hav_sales (id, region, amount) VALUES (1, 'East', 100)",
    );
    exec_sql(
        &exec,
        "INSERT INTO hav_sales (id, region, amount) VALUES (2, 'East', 200)",
    );
    exec_sql(
        &exec,
        "INSERT INTO hav_sales (id, region, amount) VALUES (3, 'West', 50)",
    );
    exec_sql(
        &exec,
        "INSERT INTO hav_sales (id, region, amount) VALUES (4, 'North', 300)",
    );

    // Parse HAVING
    let stmt =
        parse_sql("SELECT region, COUNT(*) FROM hav_sales GROUP BY region HAVING COUNT(*) > 1");
    assert!(stmt.is_ok(), "Should parse HAVING clause");

    println!("✅ HAVING: GROUP BY ... HAVING parsed correctly");
}

#[test]
fn test_offset_parse() {
    let stmt = parse_sql("SELECT * FROM t LIMIT 10 OFFSET 5");
    assert!(stmt.is_ok(), "Should parse OFFSET");

    if let Ok(SqlStatement::Select { offset, limit, .. }) = stmt {
        assert_eq!(limit, Some(10));
        assert_eq!(offset, Some(5));
    }

    println!("✅ OFFSET: LIMIT 10 OFFSET 5 parsed correctly");
}

// ═══════════════════════════════════════════════════════════════════════════
// SHOW TABLES test
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_show_tables() {
    let (_db, exec) = create_sql_env("show");

    exec_sql(&exec, "CREATE TABLE alpha (id INTEGER PRIMARY KEY)");
    exec_sql(&exec, "CREATE TABLE beta (id INTEGER PRIMARY KEY)");

    let (cols, rows) = exec_rows(&exec, "SHOW TABLES");
    assert_eq!(cols[0], "table_name");
    assert!(rows.len() >= 2);

    println!("✅ SHOW TABLES: Listed {} tables", rows.len());
}

// ═══════════════════════════════════════════════════════════════════════════
// NOT operator test
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_where_not() {
    let stmt = parse_sql("SELECT * FROM t WHERE NOT name = 'test'");
    assert!(stmt.is_ok(), "Should parse NOT");

    if let Ok(SqlStatement::Select {
        where_clause: Some(WhereExpr::Not(_)),
        ..
    }) = stmt
    {
        // correct
    } else {
        panic!("Expected NOT wrapper");
    }

    println!("✅ NOT: WHERE NOT parsed correctly");
}

// ═══════════════════════════════════════════════════════════════════════════
// Complex query: ORDER BY + LIMIT + OFFSET
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_pagination_workflow() {
    let (_db, exec) = create_sql_env("page");

    exec_sql(
        &exec,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)",
    );
    for i in 1..=20 {
        exec_sql(
            &exec,
            &format!(
                "INSERT INTO items (id, name, price) VALUES ({}, 'item{}', {})",
                i,
                i,
                i * 10
            ),
        );
    }

    // Page 1: items 1-5
    let (_cols, page1) = exec_rows(&exec, "SELECT id FROM items ORDER BY id LIMIT 5 OFFSET 0");
    assert_eq!(page1.len(), 5);
    assert_eq!(page1[0][0], "1");

    // Page 2: items 6-10
    let (_cols, page2) = exec_rows(&exec, "SELECT id FROM items ORDER BY id LIMIT 5 OFFSET 5");
    assert_eq!(page2.len(), 5);
    assert_eq!(page2[0][0], "6");

    // Page 3: items 11-15
    let (_cols, page3) = exec_rows(&exec, "SELECT id FROM items ORDER BY id LIMIT 5 OFFSET 10");
    assert_eq!(page3.len(), 5);
    assert_eq!(page3[0][0], "11");

    // Page 5: items 21-25 (only 20 items, so past end)
    let (_cols, page5) = exec_rows(&exec, "SELECT id FROM items ORDER BY id LIMIT 5 OFFSET 20");
    assert_eq!(page5.len(), 0);

    println!("✅ PAGINATION: 4 pages verified (5 items per page, 20 total)");
}

// ═══════════════════════════════════════════════════════════════════════════
// Multiple aggregates in one query
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_multiple_aggregates() {
    let (_db, exec) = create_sql_env("magg");

    exec_sql(
        &exec,
        "CREATE TABLE stats (id INTEGER PRIMARY KEY, val INTEGER)",
    );
    exec_sql(
        &exec,
        "INSERT INTO stats (id, val) VALUES (1, 10), (2, 20), (3, 30)",
    );

    let (cols, rows) = exec_rows(
        &exec,
        "SELECT COUNT(*), SUM(val), MIN(val), MAX(val) FROM stats",
    );
    assert_eq!(cols.len(), 4);
    assert_eq!(rows.len(), 1);
    // Aggregates without GROUP BY: verify non-empty results
    assert!(!rows[0][0].is_empty(), "COUNT should have a value");
    assert!(!rows[0][1].is_empty(), "SUM should have a value");

    println!("✅ AGGREGATES: Multiple aggregates in one query");
}

// ═══════════════════════════════════════════════════════════════════════════
// EXPLAIN ANALYZE test
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_explain_analyze() {
    let (_db, exec) = create_sql_env("ea");

    exec_sql(
        &exec,
        "CREATE TABLE ea_t (id INTEGER PRIMARY KEY, val TEXT)",
    );
    exec_sql(&exec, "INSERT INTO ea_t (id, val) VALUES (1, 'test')");

    let (cols, rows) = exec_rows(&exec, "EXPLAIN ANALYZE SELECT * FROM ea_t WHERE id = 1");
    assert!(
        cols[0].contains("QUERY PLAN"),
        "Column should contain QUERY PLAN"
    );
    assert!(!rows.is_empty());

    // Should contain timing info
    let plan_text: String = rows
        .iter()
        .map(|r| r[0].clone())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan_text.contains("actual"),
        "EXPLAIN ANALYZE should show actual timing"
    );

    println!("✅ EXPLAIN ANALYZE: Plan with actual timing produced");
}

// ═══════════════════════════════════════════════════════════════════════════
// Subquery execution: WHERE id IN (SELECT ...)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_subquery_execution() {
    let (_db, exec) = create_sql_env("subq");

    exec_sql(
        &exec,
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, tier TEXT)",
    );
    exec_sql(
        &exec,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)",
    );

    exec_sql(
        &exec,
        "INSERT INTO customers (id, name, tier) VALUES (1, 'Alice', 'gold')",
    );
    exec_sql(
        &exec,
        "INSERT INTO customers (id, name, tier) VALUES (2, 'Bob', 'silver')",
    );
    exec_sql(
        &exec,
        "INSERT INTO customers (id, name, tier) VALUES (3, 'Charlie', 'gold')",
    );

    exec_sql(
        &exec,
        "INSERT INTO orders (id, customer_id, amount) VALUES (1, 1, 100)",
    );
    exec_sql(
        &exec,
        "INSERT INTO orders (id, customer_id, amount) VALUES (2, 1, 200)",
    );
    exec_sql(
        &exec,
        "INSERT INTO orders (id, customer_id, amount) VALUES (3, 2, 50)",
    );

    // Subquery: get orders from gold-tier customers only
    let (_cols, rows) = exec_rows(
        &exec,
        "SELECT amount FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE tier = 'gold')",
    );
    // Gold customers are id=1 and id=3. Orders for id=1: 100, 200. No orders for id=3.
    assert_eq!(rows.len(), 2);

    println!(
        "✅ SUBQUERY: WHERE customer_id IN (SELECT id FROM ...) returned {} rows",
        rows.len()
    );
}

#[test]
fn test_subquery_empty_result() {
    let (_db, exec) = create_sql_env("subq2");

    exec_sql(&exec, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)");
    exec_sql(
        &exec,
        "CREATE TABLE t2 (id INTEGER PRIMARY KEY, ref_id INTEGER)",
    );

    exec_sql(&exec, "INSERT INTO t1 (id, val) VALUES (1, 'a')");
    exec_sql(&exec, "INSERT INTO t2 (id, ref_id) VALUES (1, 99)"); // ref_id=99 doesn't exist in t1

    // Subquery returns id=1, but t2.ref_id=99 doesn't match
    let (_cols, rows) = exec_rows(
        &exec,
        "SELECT ref_id FROM t2 WHERE ref_id IN (SELECT id FROM t1)",
    );
    assert_eq!(rows.len(), 0);

    println!("✅ SUBQUERY: Empty result when subquery doesn't match");
}

// ═══════════════════════════════════════════════════════════════════════════
// Config module test
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_config_defaults() {
    let config = omni_engine::config::ServerConfig::load_dev();
    assert!(config.http_addr.contains(':'), "http_addr must be host:port");
    assert!(config.pgwire_addr.contains(':'), "pgwire_addr must be host:port");
    assert!(!config.jwt_secret.is_empty(), "jwt_secret must not be empty");
    println!("\u2705 CONFIG: Default config has sensible values");
}

// ═══════════════════════════════════════════════════════════════════════════
// UNION / INTERSECT / EXCEPT
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_union() {
    let (_db, exec) = create_sql_env("union1");

    exec_sql(
        &exec,
        "CREATE TABLE t_a (id INTEGER PRIMARY KEY, name TEXT)",
    );
    exec_sql(
        &exec,
        "CREATE TABLE t_b (id INTEGER PRIMARY KEY, name TEXT)",
    );

    exec_sql(
        &exec,
        "INSERT INTO t_a (id, name) VALUES (1, 'Alice'), (2, 'Bob')",
    );
    exec_sql(
        &exec,
        "INSERT INTO t_b (id, name) VALUES (2, 'Bob'), (3, 'Charlie')",
    );

    // UNION removes duplicates: Alice, Bob, Charlie (Bob appears in both)
    let (_cols, rows) = exec_rows(&exec, "SELECT name FROM t_a UNION SELECT name FROM t_b");
    assert_eq!(rows.len(), 3);

    println!("✅ UNION: 3 unique rows from 2+2 with overlap");
}

#[test]
fn test_union_all() {
    let (_db, exec) = create_sql_env("union2");

    exec_sql(&exec, "CREATE TABLE ua (id INTEGER PRIMARY KEY, val TEXT)");
    exec_sql(&exec, "CREATE TABLE ub (id INTEGER PRIMARY KEY, val TEXT)");

    exec_sql(&exec, "INSERT INTO ua (id, val) VALUES (1, 'x'), (2, 'y')");
    exec_sql(&exec, "INSERT INTO ub (id, val) VALUES (3, 'y'), (4, 'z')");

    // UNION ALL keeps duplicates: x, y, y, z
    let (_cols, rows) = exec_rows(&exec, "SELECT val FROM ua UNION ALL SELECT val FROM ub");
    assert_eq!(rows.len(), 4);

    println!("✅ UNION ALL: 4 rows (keeps duplicates)");
}

#[test]
fn test_intersect() {
    let (_db, exec) = create_sql_env("isect");

    exec_sql(&exec, "CREATE TABLE ia (id INTEGER PRIMARY KEY, val TEXT)");
    exec_sql(&exec, "CREATE TABLE ib (id INTEGER PRIMARY KEY, val TEXT)");

    exec_sql(
        &exec,
        "INSERT INTO ia (id, val) VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    );
    exec_sql(
        &exec,
        "INSERT INTO ib (id, val) VALUES (4, 'b'), (5, 'c'), (6, 'd')",
    );

    // INTERSECT: only b, c are in both
    let (_cols, rows) = exec_rows(&exec, "SELECT val FROM ia INTERSECT SELECT val FROM ib");
    assert_eq!(rows.len(), 2);

    println!("✅ INTERSECT: 2 common rows");
}

#[test]
fn test_except() {
    let (_db, exec) = create_sql_env("except");

    exec_sql(&exec, "CREATE TABLE ea (id INTEGER PRIMARY KEY, val TEXT)");
    exec_sql(&exec, "CREATE TABLE eb (id INTEGER PRIMARY KEY, val TEXT)");

    exec_sql(
        &exec,
        "INSERT INTO ea (id, val) VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    );
    exec_sql(&exec, "INSERT INTO eb (id, val) VALUES (4, 'b'), (5, 'c')");

    // EXCEPT: a is in ea but not eb
    let (_cols, rows) = exec_rows(&exec, "SELECT val FROM ea EXCEPT SELECT val FROM eb");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "a");

    println!("✅ EXCEPT: 1 row (a only in left)");
}
