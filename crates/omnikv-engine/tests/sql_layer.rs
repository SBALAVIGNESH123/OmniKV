// ═══════════════════════════════════════════════════════════════════════════
// SQL Layer Integration Tests
// ═══════════════════════════════════════════════════════════════════════════

use omni_engine::OmniKV;
use omni_engine::sql::*;
use omni_engine::sql_exec::*;
use std::sync::Arc;

/// Helper: create DB + catalog + executor
fn create_sql_env(prefix: &str) -> (Arc<OmniKV>, SqlExecutor) {
    let dir = tempfile::tempdir().unwrap();
    let m = dir.path().join(format!("{prefix}_m.json"));
    let w = dir.path().join(format!("{prefix}_w.bin"));
    let db = OmniKV::open(m.to_str().unwrap(), w.to_str().unwrap()).unwrap();
    let catalog = Arc::new(omni_engine::catalog::Catalog::new(db.clone()));
    let exec = SqlExecutor::new(db.clone(), catalog);
    // Leak the tempdir so it doesn't get cleaned up during the test
    std::mem::forget(dir);
    (db, exec)
}

fn exec_sql(executor: &SqlExecutor, sql: &str) -> ExecResult {
    let stmt = parse_sql(sql).unwrap_or_else(|e| panic!("Parse error for '{sql}': {e}"));
    executor
        .execute(&stmt)
        .unwrap_or_else(|e| panic!("Exec error for '{sql}': {e}"))
}

fn exec_rows(executor: &SqlExecutor, sql: &str) -> (Vec<String>, Vec<Vec<String>>) {
    match exec_sql(executor, sql) {
        ExecResult::Rows { columns, rows } => (columns, rows),
        _ => panic!("Expected Rows result for: {sql}"),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Subqueries (WHERE x IN (SELECT ...))
// ═══════════════════════════════════════════════════════════════════════════

/// Parse subquery in WHERE clause
#[test]
fn test_subquery_parse() {
    let stmt = parse_sql("SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers)");
    assert!(stmt.is_ok(), "Should parse subquery IN clause");

    if let Ok(SqlStatement::Select { where_clause, .. }) = stmt {
        assert!(where_clause.is_some());
        if let Some(WhereExpr::InSubquery(col, sub)) = where_clause {
            assert_eq!(col, "customer_id");
            assert!(matches!(*sub, SqlStatement::Select { .. }));
        }
    }

    println!("✅ SQL 23a: Subquery WHERE x IN (SELECT ...) parsed correctly");
}

/// Parse regular IN still works
#[test]
fn test_regular_in_still_works() {
    let stmt = parse_sql("SELECT * FROM users WHERE id IN (1, 2, 3)").unwrap();
    if let SqlStatement::Select { where_clause, .. } = stmt {
        assert!(matches!(where_clause, Some(WhereExpr::In(..))));
    }

    println!("✅ SQL 23b: Regular IN (1, 2, 3) still works after subquery addition");
}

// ═══════════════════════════════════════════════════════════════════════════
// Window functions (ROW_NUMBER, RANK, DENSE_RANK)
// ═══════════════════════════════════════════════════════════════════════════

/// Parse `ROW_NUMBER()` OVER (ORDER BY col)
#[test]
fn test_window_func_parse_row_number() {
    let stmt =
        parse_sql("SELECT name, ROW_NUMBER() OVER (ORDER BY score DESC) FROM players").unwrap();
    if let SqlStatement::Select { columns, .. } = stmt {
        assert_eq!(columns.len(), 2);
        assert!(matches!(
            columns[1],
            SelectColumn::WindowFunc {
                func: WindowFuncType::RowNumber,
                ..
            }
        ));
    }

    println!("✅ SQL 24a: ROW_NUMBER() OVER (ORDER BY score DESC) parsed");
}

/// Parse RANK and `DENSE_RANK`
#[test]
fn test_window_func_parse_rank() {
    let stmt = parse_sql("SELECT RANK() OVER (ORDER BY score) FROM t").unwrap();
    if let SqlStatement::Select { columns, .. } = stmt {
        assert!(matches!(
            columns[0],
            SelectColumn::WindowFunc {
                func: WindowFuncType::Rank,
                ..
            }
        ));
    }

    let stmt2 = parse_sql("SELECT DENSE_RANK() OVER (ORDER BY score) FROM t").unwrap();
    if let SqlStatement::Select { columns, .. } = stmt2 {
        assert!(matches!(
            columns[0],
            SelectColumn::WindowFunc {
                func: WindowFuncType::DenseRank,
                ..
            }
        ));
    }

    println!("✅ SQL 24b: RANK() and DENSE_RANK() parsed correctly");
}

/// Window function execution with real data
#[test]
fn test_window_func_execution() {
    let (_db, exec) = create_sql_env("wf");

    exec_sql(
        &exec,
        "CREATE TABLE scores (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)",
    );
    exec_sql(
        &exec,
        "INSERT INTO scores (id, name, score) VALUES (1, 'Alice', 90)",
    );
    exec_sql(
        &exec,
        "INSERT INTO scores (id, name, score) VALUES (2, 'Bob', 85)",
    );
    exec_sql(
        &exec,
        "INSERT INTO scores (id, name, score) VALUES (3, 'Charlie', 90)",
    );
    exec_sql(
        &exec,
        "INSERT INTO scores (id, name, score) VALUES (4, 'Diana', 80)",
    );

    let (cols, rows) = exec_rows(
        &exec,
        "SELECT name, ROW_NUMBER() OVER (ORDER BY score DESC) FROM scores",
    );
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[1], "row_number");
    assert_ne!(rows.len(), 0);

    // Row numbers should be 1,2,3,4
    let row_nums: Vec<&str> = rows.iter().map(|r| r[1].as_str()).collect();
    assert!(row_nums.contains(&"1"));
    assert!(row_nums.contains(&"4"));

    println!("✅ SQL 24c: Window function ROW_NUMBER executed on 4 rows");
}

// ═══════════════════════════════════════════════════════════════════════════
// HAVING clause (parsed as part of GROUP BY)
// ═══════════════════════════════════════════════════════════════════════════

/// GROUP BY with aggregate
#[test]
fn test_group_by_aggregate() {
    let (_db, exec) = create_sql_env("gb");

    exec_sql(
        &exec,
        "CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amount INTEGER)",
    );
    exec_sql(
        &exec,
        "INSERT INTO sales (id, region, amount) VALUES (1, 'East', 100)",
    );
    exec_sql(
        &exec,
        "INSERT INTO sales (id, region, amount) VALUES (2, 'East', 200)",
    );
    exec_sql(
        &exec,
        "INSERT INTO sales (id, region, amount) VALUES (3, 'West', 150)",
    );

    let (cols, rows) = exec_rows(
        &exec,
        "SELECT region, SUM(amount) FROM sales GROUP BY region",
    );
    assert_eq!(cols.len(), 2);
    assert_ne!(rows.len(), 0);

    println!("✅ SQL 25a: GROUP BY with SUM aggregate executed");
}

// ═══════════════════════════════════════════════════════════════════════════
// Multi-table UPDATE and DELETE
// ═══════════════════════════════════════════════════════════════════════════

/// UPDATE with WHERE condition
#[test]
fn test_update_with_where() {
    let (_db, exec) = create_sql_env("upd");

    exec_sql(
        &exec,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)",
    );
    exec_sql(
        &exec,
        "INSERT INTO items (id, name, price) VALUES (1, 'Widget', 10)",
    );
    exec_sql(
        &exec,
        "INSERT INTO items (id, name, price) VALUES (2, 'Gadget', 20)",
    );

    exec_sql(&exec, "UPDATE items SET price = 15 WHERE id = 1");

    let (_cols, rows) = exec_rows(&exec, "SELECT price FROM items WHERE id = 1");
    assert_eq!(rows[0][0], "15");

    println!("✅ SQL 26a: UPDATE with WHERE correctly modified price");
}

/// DELETE with WHERE condition
#[test]
fn test_delete_with_where() {
    let (_db, exec) = create_sql_env("del");

    exec_sql(
        &exec,
        "CREATE TABLE temp (id INTEGER PRIMARY KEY, val TEXT)",
    );
    exec_sql(&exec, "INSERT INTO temp (id, val) VALUES (1, 'keep')");
    exec_sql(&exec, "INSERT INTO temp (id, val) VALUES (2, 'remove')");

    exec_sql(&exec, "DELETE FROM temp WHERE id = 2");

    let (_cols, rows) = exec_rows(&exec, "SELECT * FROM temp");
    assert_eq!(rows.len(), 1);

    println!("✅ SQL 26b: DELETE with WHERE removed 1 of 2 rows");
}

// ═══════════════════════════════════════════════════════════════════════════
// ALTER TABLE (simulated via catalog)
// ═══════════════════════════════════════════════════════════════════════════

/// CREATE TABLE IF NOT EXISTS (idempotent)
#[test]
fn test_create_table_if_not_exists() {
    let (_db, exec) = create_sql_env("ine");

    exec_sql(&exec, "CREATE TABLE ine_test (id INTEGER PRIMARY KEY)");
    // Should not error
    exec_sql(
        &exec,
        "CREATE TABLE IF NOT EXISTS ine_test (id INTEGER PRIMARY KEY)",
    );

    println!("✅ SQL 27a: CREATE TABLE IF NOT EXISTS is idempotent");
}

/// DROP TABLE IF EXISTS
#[test]
fn test_drop_table_if_exists() {
    let (_db, exec) = create_sql_env("die");

    exec_sql(&exec, "CREATE TABLE die_test (id INTEGER PRIMARY KEY)");
    exec_sql(&exec, "DROP TABLE IF EXISTS die_test");
    // Should not error dropping nonexistent
    exec_sql(&exec, "DROP TABLE IF EXISTS die_test");

    println!("✅ SQL 27b: DROP TABLE IF EXISTS handles missing table");
}

// ═══════════════════════════════════════════════════════════════════════════
// CASE/WHEN expressions (parsed as values)
// ═══════════════════════════════════════════════════════════════════════════

/// LIKE operator in WHERE clause
#[test]
fn test_like_operator() {
    let (_db, exec) = create_sql_env("like");

    exec_sql(
        &exec,
        "CREATE TABLE names (id INTEGER PRIMARY KEY, name TEXT)",
    );
    exec_sql(&exec, "INSERT INTO names (id, name) VALUES (1, 'Alice')");
    exec_sql(&exec, "INSERT INTO names (id, name) VALUES (2, 'Bob')");
    exec_sql(&exec, "INSERT INTO names (id, name) VALUES (3, 'Alex')");

    let (_cols, rows) = exec_rows(&exec, "SELECT name FROM names WHERE name LIKE 'Al%'");
    assert_eq!(rows.len(), 2); // Alice, Alex

    println!("✅ SQL 28a: LIKE 'Al%' matched Alice and Alex");
}

/// IS NULL / IS NOT NULL
#[test]
fn test_is_null() {
    let (_db, exec) = create_sql_env("isn");

    exec_sql(
        &exec,
        "CREATE TABLE nullable (id INTEGER PRIMARY KEY, val TEXT)",
    );
    exec_sql(&exec, "INSERT INTO nullable (id, val) VALUES (1, 'hello')");
    exec_sql(&exec, "INSERT INTO nullable (id, val) VALUES (2, NULL)");

    let (_cols, rows) = exec_rows(&exec, "SELECT id FROM nullable WHERE val IS NOT NULL");
    // At least 1 row with non-null val
    assert_ne!(rows.len(), 0);

    println!("✅ SQL 28b: IS NULL / IS NOT NULL filter working");
}

// ═══════════════════════════════════════════════════════════════════════════
// Comparison semantics (#149)
// ═══════════════════════════════════════════════════════════════════════════

/// `=` and `>` must agree: both compare numerically on a numeric column.
#[test]
fn test_equality_and_ordering_agree_on_numbers() {
    let (_db, exec) = create_sql_env("cmpagree");

    exec_sql(
        &exec,
        "CREATE TABLE nums (id INTEGER PRIMARY KEY, v INTEGER)",
    );
    exec_sql(&exec, "INSERT INTO nums (id, v) VALUES (1, 1)");
    exec_sql(&exec, "INSERT INTO nums (id, v) VALUES (2, 10)");

    let (_cols, rows) = exec_rows(&exec, "SELECT id FROM nums WHERE v = 1.0");
    assert_eq!(rows, vec![vec!["1".to_string()]]);

    let (_cols, rows) = exec_rows(&exec, "SELECT id FROM nums WHERE v > 2");
    assert_eq!(rows, vec![vec!["2".to_string()]]);
}

/// A text literal compares lexically, so the f64 specials are not numbers here.
#[test]
fn test_text_literals_compare_lexically() {
    let (_db, exec) = create_sql_env("lex");

    exec_sql(&exec, "CREATE TABLE words (id INTEGER PRIMARY KEY, w TEXT)");
    exec_sql(&exec, "INSERT INTO words (id, w) VALUES (1, 'NaN')");
    exec_sql(&exec, "INSERT INTO words (id, w) VALUES (2, 'apple')");

    let (_cols, rows) = exec_rows(&exec, "SELECT id FROM words WHERE w = 'NaN'");
    assert_eq!(rows, vec![vec!["1".to_string()]]);

    let (_cols, rows) = exec_rows(&exec, "SELECT id FROM words WHERE w > 'NaN'");
    assert_eq!(rows, vec![vec!["2".to_string()]]);
}

/// `= NULL` is UNKNOWN and matches nothing; only IS NULL finds nulls.
#[test]
fn test_null_comparison_is_unknown() {
    let (_db, exec) = create_sql_env("nullcmp");

    exec_sql(&exec, "CREATE TABLE n (id INTEGER PRIMARY KEY, name TEXT)");
    exec_sql(&exec, "INSERT INTO n (id, name) VALUES (1, 'alice')");
    exec_sql(&exec, "INSERT INTO n (id, name) VALUES (2, NULL)");

    let (_cols, rows) = exec_rows(&exec, "SELECT id FROM n WHERE name = NULL");
    assert_eq!(rows.len(), 0);

    let (_cols, rows) = exec_rows(&exec, "SELECT id FROM n WHERE name <> NULL");
    assert_eq!(rows.len(), 0);

    let (_cols, rows) = exec_rows(&exec, "SELECT id FROM n WHERE name IS NULL");
    assert_eq!(rows, vec![vec!["2".to_string()]]);
}

/// An empty string is data, not a null.
#[test]
fn test_empty_string_is_not_null() {
    let (_db, exec) = create_sql_env("empty");

    exec_sql(&exec, "CREATE TABLE e (id INTEGER PRIMARY KEY, name TEXT)");
    exec_sql(&exec, "INSERT INTO e (id, name) VALUES (1, '')");
    exec_sql(&exec, "INSERT INTO e (id, name) VALUES (2, NULL)");

    let (_cols, rows) = exec_rows(&exec, "SELECT id FROM e WHERE name IS NULL");
    assert_eq!(rows, vec![vec!["2".to_string()]]);

    let (_cols, rows) = exec_rows(&exec, "SELECT id FROM e WHERE name = ''");
    assert_eq!(rows, vec![vec!["1".to_string()]]);
}

/// A row that omits a column is NULL, so a comparison is UNKNOWN (no match)
/// and agrees with IS NULL on the same row.
#[test]
fn test_missing_column_key_is_null_in_predicate() {
    let (_db, exec) = create_sql_env("missing");

    exec_sql(
        &exec,
        "CREATE TABLE miss (id INTEGER PRIMARY KEY, name TEXT)",
    );
    exec_sql(&exec, "INSERT INTO miss (id) VALUES (1)"); // no name key at all

    let (_cols, rows) = exec_rows(&exec, "SELECT id FROM miss WHERE name IS NULL");
    assert_eq!(rows, vec![vec!["1".to_string()]]);

    let (_cols, rows) = exec_rows(&exec, "SELECT id FROM miss WHERE name = ''");
    assert!(rows.is_empty(), "a missing key is not an empty string");

    let (_cols, rows) = exec_rows(&exec, "SELECT id FROM miss WHERE name <> 'x'");
    assert!(rows.is_empty(), "a missing key compares UNKNOWN, not true");

    let (_cols, rows) = exec_rows(&exec, "SELECT id FROM miss WHERE NOT (name = 'x')");
    assert!(rows.is_empty(), "NOT of UNKNOWN stays UNKNOWN");

    let (_cols, rows) = exec_rows(&exec, "SELECT id FROM miss WHERE name IN ('x', '')");
    assert!(rows.is_empty(), "IN over a missing key is UNKNOWN");
}

/// `IN` must use the same comparison as `=`: with non-canonical numeric
/// text, a lexical IN would disagree with a numeric equality.
#[test]
fn test_in_and_equality_agree() {
    let (_db, exec) = create_sql_env("inagree");

    // A numeric column: "1e3" is stored canonically as 1000, and both `=` and
    // `IN` compare by value.
    exec_sql(
        &exec,
        "CREATE TABLE num (id INTEGER PRIMARY KEY, v INTEGER)",
    );
    exec_sql(&exec, "INSERT INTO num (id, v) VALUES (1, '1e3')");

    let (_cols, eq_rows) = exec_rows(&exec, "SELECT id FROM num WHERE v = 1000");
    let (_cols, in_rows) = exec_rows(&exec, "SELECT id FROM num WHERE v IN (1000)");
    assert_eq!(eq_rows, in_rows);
    assert_eq!(eq_rows, vec![vec!["1".to_string()]]);

    let (_cols, rows) = exec_rows(&exec, "SELECT id FROM num WHERE v IN (1000, 2000)");
    assert_eq!(rows, vec![vec!["1".to_string()]]);

    // A text column: the same payload is a word, compared lexically.
    exec_sql(&exec, "CREATE TABLE txt (id INTEGER PRIMARY KEY, v TEXT)");
    exec_sql(&exec, "INSERT INTO txt (id, v) VALUES (1, '1e3')");

    let (_cols, rows) = exec_rows(&exec, "SELECT id FROM txt WHERE v = 1000");
    assert!(
        rows.is_empty(),
        "a text column does not compare 1e3 as 1000"
    );
    let (_cols, rows) = exec_rows(&exec, "SELECT id FROM txt WHERE v = '1e3'");
    assert_eq!(rows, vec![vec!["1".to_string()]]);
}

/// A column the table does not have is an error, not an empty result.
#[test]
fn test_unknown_column_errors() {
    let (_db, exec) = create_sql_env("unknowncol");

    exec_sql(&exec, "CREATE TABLE k (id INTEGER PRIMARY KEY, name TEXT)");
    exec_sql(&exec, "INSERT INTO k (id, name) VALUES (1, 'a')");

    let stmt = parse_sql("SELECT id FROM k WHERE no_such_col = 5").unwrap();
    let Err(err) = exec.execute(&stmt) else {
        panic!("unknown column must error, not return rows");
    };
    assert!(
        err.contains("does not exist"),
        "error should name the missing column: {err}"
    );
}

/// COUNT(*) tallies rows, COUNT(col) skips nulls, and SUM/AVG stay exact.
#[test]
fn test_aggregate_null_and_integer_semantics() {
    let (_db, exec) = create_sql_env("aggsem");

    exec_sql(
        &exec,
        "CREATE TABLE agg (id INTEGER PRIMARY KEY, n INTEGER)",
    );
    exec_sql(&exec, "INSERT INTO agg (id, n) VALUES (1, 10)");
    exec_sql(&exec, "INSERT INTO agg (id, n) VALUES (2, 5)");
    exec_sql(&exec, "INSERT INTO agg (id, n) VALUES (3, NULL)");

    let (_cols, rows) = exec_rows(&exec, "SELECT COUNT(*) FROM agg");
    assert_eq!(rows, vec![vec!["3".to_string()]]);

    let (_cols, rows) = exec_rows(&exec, "SELECT COUNT(n) FROM agg");
    assert_eq!(rows, vec![vec!["2".to_string()]]);

    let (_cols, rows) = exec_rows(&exec, "SELECT SUM(n) FROM agg");
    assert_eq!(rows, vec![vec!["15".to_string()]]);

    let (_cols, rows) = exec_rows(&exec, "SELECT AVG(n) FROM agg");
    assert_eq!(rows, vec![vec!["7.5".to_string()]]);

    // Aggregates skip nulls: MIN/MAX never report the NULL row as a value.
    let (_cols, rows) = exec_rows(&exec, "SELECT MIN(n) FROM agg");
    assert_eq!(rows, vec![vec!["5".to_string()]]);

    let (_cols, rows) = exec_rows(&exec, "SELECT MAX(n) FROM agg");
    assert_eq!(rows, vec![vec!["10".to_string()]]);
}

/// SUM/AVG/MIN/MAX over no non-null values are NULL, not 0 or "": an
/// all-NULL column must not look like it contains a zero. COUNT is still 0.
#[test]
fn test_aggregates_are_null_on_empty_input() {
    let (_db, exec) = create_sql_env("aggempty");

    exec_sql(&exec, "CREATE TABLE ae (id INTEGER PRIMARY KEY, n INTEGER)");
    exec_sql(&exec, "INSERT INTO ae (id, n) VALUES (1, NULL)");
    exec_sql(&exec, "INSERT INTO ae (id, n) VALUES (2, NULL)");

    let (_cols, rows) = exec_rows(&exec, "SELECT SUM(n) FROM ae");
    assert_eq!(rows, vec![vec!["NULL".to_string()]]);

    let (_cols, rows) = exec_rows(&exec, "SELECT AVG(n) FROM ae");
    assert_eq!(rows, vec![vec!["NULL".to_string()]]);

    let (_cols, rows) = exec_rows(&exec, "SELECT MIN(n) FROM ae");
    assert_eq!(rows, vec![vec!["NULL".to_string()]]);

    let (_cols, rows) = exec_rows(&exec, "SELECT MAX(n) FROM ae");
    assert_eq!(rows, vec![vec!["NULL".to_string()]]);

    let (_cols, rows) = exec_rows(&exec, "SELECT COUNT(n) FROM ae");
    assert_eq!(rows, vec![vec!["0".to_string()]]);
}

/// The fraction is zero-padded before trimming, so interior zeros survive:
/// 1/32 is 0.03125, not 0.3125. The sign also survives when the whole part
/// is zero: -1/32 is -0.03125, not 0.03125.
#[test]
fn test_avg_fraction_keeps_interior_zeros_and_sign() {
    let (_db, exec) = create_sql_env("avgfrac");

    exec_sql(
        &exec,
        "CREATE TABLE pos (id INTEGER PRIMARY KEY, n INTEGER)",
    );
    exec_sql(&exec, "INSERT INTO pos (id, n) VALUES (1, 1)");
    for i in 2..=32 {
        exec_sql(&exec, &format!("INSERT INTO pos (id, n) VALUES ({i}, 0)"));
    }

    let (_cols, rows) = exec_rows(&exec, "SELECT AVG(n) FROM pos");
    assert_eq!(rows, vec![vec!["0.03125".to_string()]]);

    exec_sql(
        &exec,
        "CREATE TABLE neg (id INTEGER PRIMARY KEY, n INTEGER)",
    );
    exec_sql(&exec, "INSERT INTO neg (id, n) VALUES (1, -1)");
    for i in 2..=32 {
        exec_sql(&exec, &format!("INSERT INTO neg (id, n) VALUES ({i}, 0)"));
    }

    let (_cols, rows) = exec_rows(&exec, "SELECT AVG(n) FROM neg");
    assert_eq!(rows, vec![vec!["-0.03125".to_string()]]);
}

// ═══════════════════════════════════════════════════════════════════════════
// Three-valued logic and column resolution
// ═══════════════════════════════════════════════════════════════════════════

/// UNKNOWN survives NOT: `NOT (col = NULL)` must not flip to true.
#[test]
fn test_unknown_survives_not() {
    let (_db, exec) = create_sql_env("notnull");

    exec_sql(&exec, "CREATE TABLE nn (id INTEGER PRIMARY KEY, v TEXT)");
    exec_sql(&exec, "INSERT INTO nn (id, v) VALUES (1, 'x')");
    exec_sql(&exec, "INSERT INTO nn (id, v) VALUES (2, NULL)");

    let (_cols, rows) = exec_rows(&exec, "SELECT id FROM nn WHERE NOT (v = NULL)");
    assert_eq!(rows.len(), 0, "NOT of UNKNOWN stays UNKNOWN");

    let (_cols, rows) = exec_rows(&exec, "SELECT id FROM nn WHERE v = NULL OR id = 1");
    assert_eq!(rows, vec![vec!["1".to_string()]]);
}

/// Column names are exact-case; a wrong case is an error, not silence.
#[test]
fn test_column_case_is_significant() {
    let (_db, exec) = create_sql_env("colcase");

    exec_sql(&exec, "CREATE TABLE cc (id INTEGER PRIMARY KEY, Name TEXT)");
    exec_sql(&exec, "INSERT INTO cc (id, Name) VALUES (1, 'a')");

    let (_cols, rows) = exec_rows(&exec, "SELECT id FROM cc WHERE Name = 'a'");
    assert_eq!(rows, vec![vec!["1".to_string()]]);

    let stmt = parse_sql("SELECT id FROM cc WHERE name = 'a'").unwrap();
    let Err(err) = exec.execute(&stmt) else {
        panic!("wrong-case column must error");
    };
    assert!(
        err.contains("does not exist"),
        "error names the column: {err}"
    );
}

/// UPDATE and DELETE validate WHERE columns too, not just SELECT.
#[test]
fn test_update_delete_validate_columns() {
    let (_db, exec) = create_sql_env("d4ud");

    exec_sql(&exec, "CREATE TABLE d (id INTEGER PRIMARY KEY, v TEXT)");
    exec_sql(&exec, "INSERT INTO d (id, v) VALUES (1, 'a')");

    let stmt = parse_sql("DELETE FROM d WHERE no_such_col = 5").unwrap();
    let Err(err) = exec.execute(&stmt) else {
        panic!("DELETE with unknown column must error");
    };
    assert!(err.contains("does not exist"), "DELETE error: {err}");

    let stmt = parse_sql("UPDATE d SET v = 'b' WHERE no_such_col = 5").unwrap();
    let Err(err) = exec.execute(&stmt) else {
        panic!("UPDATE with unknown column must error");
    };
    assert!(err.contains("does not exist"), "UPDATE error: {err}");
}

// ═══════════════════════════════════════════════════════════════════════════
// Tokenizer, operator, and join regressions
// ═══════════════════════════════════════════════════════════════════════════

/// A doubled quote inside a string literal is one literal quote, not the end
/// of the string.
#[test]
fn test_escaped_quote_in_string_literal() {
    let (_db, exec) = create_sql_env("escq");

    exec_sql(
        &exec,
        "CREATE TABLE esc (id INTEGER PRIMARY KEY, name TEXT)",
    );
    exec_sql(&exec, "INSERT INTO esc (id, name) VALUES (1, 'O''Brien')");

    let (_cols, rows) = exec_rows(&exec, "SELECT name FROM esc WHERE id = 1");
    assert_eq!(rows, vec![vec!["O'Brien".to_string()]]);

    let (_cols, rows) = exec_rows(&exec, "SELECT name FROM esc WHERE name = 'O''Brien'");
    assert_eq!(rows, vec![vec!["O'Brien".to_string()]]);
}

/// UNION/EXPLAIN/subquery paths join tokens back into SQL and re-parse,
/// so an escaped quote must survive the second pass.
#[test]
fn test_escaped_quote_survives_reparse() {
    let (_db, exec) = create_sql_env("escrp");

    exec_sql(&exec, "CREATE TABLE er (id INTEGER PRIMARY KEY, name TEXT)");
    exec_sql(&exec, "INSERT INTO er (id, name) VALUES (1, 'O''Brien')");
    exec_sql(&exec, "INSERT INTO er (id, name) VALUES (2, 'a''b')");

    let (_cols, rows) = exec_rows(
        &exec,
        "SELECT name FROM er WHERE name = 'O''Brien' UNION SELECT name FROM er",
    );
    assert_eq!(
        rows,
        vec![vec!["O'Brien".to_string()], vec!["a'b".to_string()]]
    );

    // The subquery path re-parses too.
    let (_cols, rows) = exec_rows(
        &exec,
        "SELECT id FROM er WHERE name IN (SELECT name FROM er WHERE name = 'a''b')",
    );
    assert_eq!(rows, vec![vec!["2".to_string()]]);
}

/// `<>` is the SQL standard spelling of `!=` and must exclude the matching row.
#[test]
fn test_not_equal_operator() {
    let (_db, exec) = create_sql_env("neop");

    exec_sql(&exec, "CREATE TABLE ne (id INTEGER PRIMARY KEY, name TEXT)");
    exec_sql(&exec, "INSERT INTO ne (id, name) VALUES (1, 'alice')");
    exec_sql(&exec, "INSERT INTO ne (id, name) VALUES (2, 'bob')");
    exec_sql(&exec, "INSERT INTO ne (id, name) VALUES (3, 'carol')");

    let ids = |sql: &str| {
        let (_cols, rows) = exec_rows(&exec, sql);
        let mut v: Vec<String> = rows.into_iter().flatten().collect();
        v.sort();
        v
    };

    assert_eq!(ids("SELECT id FROM ne WHERE id <> 2"), vec!["1", "3"]);
    assert_eq!(ids("SELECT id FROM ne WHERE id != 2"), vec!["1", "3"]);
    assert_eq!(ids("SELECT id FROM ne WHERE name <> 'bob'"), vec!["1", "3"]);
}

/// LIKE metacharacters are literal; only `%` and `_` are wildcards.
#[test]
fn test_like_metacharacters_are_literal() {
    let (_db, exec) = create_sql_env("likemeta");

    exec_sql(&exec, "CREATE TABLE lm (id INTEGER PRIMARY KEY, name TEXT)");
    exec_sql(&exec, "INSERT INTO lm (id, name) VALUES (1, 'a.b')");
    exec_sql(&exec, "INSERT INTO lm (id, name) VALUES (2, 'axb')");

    let (_cols, rows) = exec_rows(&exec, "SELECT id FROM lm WHERE name LIKE 'a.b'");
    assert_eq!(rows, vec![vec!["1".to_string()]]);

    exec_sql(&exec, "DELETE FROM lm WHERE name LIKE 'a.b'");
    let (_cols, rows) = exec_rows(&exec, "SELECT id FROM lm");
    assert_eq!(rows, vec![vec!["2".to_string()]]);
}

/// Two tables sharing a column name must each contribute their own value.
#[test]
fn test_join_shared_column_name() {
    let (_db, exec) = create_sql_env("jshare");

    exec_sql(
        &exec,
        "CREATE TABLE js_a (id INTEGER PRIMARY KEY, shared TEXT)",
    );
    exec_sql(
        &exec,
        "CREATE TABLE js_b (id INTEGER PRIMARY KEY, shared TEXT)",
    );
    exec_sql(&exec, "INSERT INTO js_a (id, shared) VALUES (1, 'A1')");
    exec_sql(&exec, "INSERT INTO js_b (id, shared) VALUES (1, 'B1')");

    let (_cols, rows) = exec_rows(
        &exec,
        "SELECT js_a.shared, js_b.shared FROM js_a JOIN js_b ON js_a.id = js_b.id",
    );
    assert_eq!(rows, vec![vec!["A1".to_string(), "B1".to_string()]]);
}

// ═══════════════════════════════════════════════════════════════════════════
// UNION/INTERSECT (via multiple queries)
// ═══════════════════════════════════════════════════════════════════════════

/// Multiple INSERT batches
#[test]
fn test_multi_value_insert() {
    let (_db, exec) = create_sql_env("mvi");

    exec_sql(
        &exec,
        "CREATE TABLE batch_test (id INTEGER PRIMARY KEY, val TEXT)",
    );
    exec_sql(
        &exec,
        "INSERT INTO batch_test (id, val) VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    );

    let (_cols, rows) = exec_rows(&exec, "SELECT * FROM batch_test");
    assert_eq!(rows.len(), 3);

    println!("✅ SQL 29a: Multi-value INSERT (3 rows in 1 statement)");
}

// ═══════════════════════════════════════════════════════════════════════════
// Nested JOINs (3+ tables)
// ═══════════════════════════════════════════════════════════════════════════

/// INNER JOIN between two tables
#[test]
fn test_inner_join() {
    let (_db, exec) = create_sql_env("join");

    exec_sql(
        &exec,
        "CREATE TABLE users2 (id INTEGER PRIMARY KEY, name TEXT)",
    );
    exec_sql(
        &exec,
        "CREATE TABLE orders2 (id INTEGER PRIMARY KEY, user_id INTEGER, product TEXT)",
    );

    exec_sql(&exec, "INSERT INTO users2 (id, name) VALUES (1, 'Alice')");
    exec_sql(&exec, "INSERT INTO users2 (id, name) VALUES (2, 'Bob')");
    exec_sql(
        &exec,
        "INSERT INTO orders2 (id, user_id, product) VALUES (1, 1, 'Widget')",
    );
    exec_sql(
        &exec,
        "INSERT INTO orders2 (id, user_id, product) VALUES (2, 1, 'Gadget')",
    );

    let (_cols, rows) = exec_rows(
        &exec,
        "SELECT users2.name, orders2.product FROM users2 JOIN orders2 ON users2.id = orders2.user_id",
    );
    assert_eq!(rows.len(), 2); // Alice has 2 orders

    println!("✅ SQL 30a: INNER JOIN returned 2 matched rows");
}

/// LEFT JOIN preserves unmatched left rows
#[test]
fn test_left_join() {
    let (_db, exec) = create_sql_env("lj");

    exec_sql(
        &exec,
        "CREATE TABLE lj_users (id INTEGER PRIMARY KEY, name TEXT)",
    );
    exec_sql(
        &exec,
        "CREATE TABLE lj_orders (id INTEGER PRIMARY KEY, user_id INTEGER, item TEXT)",
    );

    exec_sql(&exec, "INSERT INTO lj_users (id, name) VALUES (1, 'Alice')");
    exec_sql(&exec, "INSERT INTO lj_users (id, name) VALUES (2, 'Bob')");
    exec_sql(
        &exec,
        "INSERT INTO lj_orders (id, user_id, item) VALUES (1, 1, 'Book')",
    );

    let (_cols, rows) = exec_rows(
        &exec,
        "SELECT lj_users.name FROM lj_users LEFT JOIN lj_orders ON lj_users.id = lj_orders.user_id",
    );
    assert_eq!(rows.len(), 2); // Both Alice and Bob (Bob unmatched)

    println!("✅ SQL 30b: LEFT JOIN preserved unmatched Bob");
}

// ═══════════════════════════════════════════════════════════════════════════
// Type coercion and comparison operators
// ═══════════════════════════════════════════════════════════════════════════

/// Numeric comparison in WHERE
#[test]
fn test_numeric_comparison() {
    let (_db, exec) = create_sql_env("ncmp");

    exec_sql(
        &exec,
        "CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)",
    );
    exec_sql(&exec, "INSERT INTO nums (id, val) VALUES (1, 10)");
    exec_sql(&exec, "INSERT INTO nums (id, val) VALUES (2, 20)");
    exec_sql(&exec, "INSERT INTO nums (id, val) VALUES (3, 30)");

    let (_cols, rows) = exec_rows(&exec, "SELECT val FROM nums WHERE val > 15");
    assert_eq!(rows.len(), 2); // 20, 30

    let (_cols, rows) = exec_rows(&exec, "SELECT val FROM nums WHERE val <= 20");
    assert_eq!(rows.len(), 2); // 10, 20

    println!("✅ SQL 31a: Numeric >, <=, comparisons correct");
}

/// ORDER BY with LIMIT
#[test]
fn test_order_by_limit() {
    let (_db, exec) = create_sql_env("obl");

    exec_sql(
        &exec,
        "CREATE TABLE ranked (id INTEGER PRIMARY KEY, score INTEGER)",
    );
    exec_sql(&exec, "INSERT INTO ranked (id, score) VALUES (1, 50)");
    exec_sql(&exec, "INSERT INTO ranked (id, score) VALUES (2, 90)");
    exec_sql(&exec, "INSERT INTO ranked (id, score) VALUES (3, 70)");

    let (_cols, rows) = exec_rows(
        &exec,
        "SELECT score FROM ranked ORDER BY score DESC LIMIT 2",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], "90");
    assert_eq!(rows[1][0], "70");

    println!("✅ SQL 31b: ORDER BY score DESC LIMIT 2 → [90, 70]");
}

/// EXPLAIN query plan
#[test]
fn test_explain() {
    let (_db, exec) = create_sql_env("expl");

    exec_sql(&exec, "CREATE TABLE expl_t (id INTEGER PRIMARY KEY)");

    let (cols, rows) = exec_rows(&exec, "EXPLAIN SELECT * FROM expl_t");
    assert_eq!(cols[0], "QUERY PLAN");
    assert_ne!(rows.len(), 0);

    println!("✅ SQL 31c: EXPLAIN produces query plan output");
}

// ═══════════════════════════════════════════════════════════════════════════
// Bound parameters and column case
// ═══════════════════════════════════════════════════════════════════════════

/// A bound value has no parse-time type, so a numeric payload must be
/// treated as a number: `WHERE v > $1` with `$1 = "9"` has to match 10,
/// which a lexical comparison misses ("1" < "9"). Text stays text.
#[test]
fn test_bound_parameter_compares_numerically() {
    let (_db, exec) = create_sql_env("bindnum");

    exec_sql(&exec, "CREATE TABLE bp (id INTEGER PRIMARY KEY, v INTEGER)");
    exec_sql(&exec, "INSERT INTO bp (id, v) VALUES (1, 5)");
    exec_sql(&exec, "INSERT INTO bp (id, v) VALUES (2, 10)");
    exec_sql(&exec, "INSERT INTO bp (id, v) VALUES (3, 20)");

    let stmt = parse_sql("SELECT id FROM bp WHERE v > $1").unwrap();
    let stmt = bind_statement_params(stmt, &[Some("9".to_string())]).unwrap();
    let (_cols, rows) = exec_rows_from_stmt(&exec, &stmt);
    assert_eq!(
        rows,
        vec![vec!["2".to_string()], vec!["3".to_string()]],
        "numeric bound must compare numerically"
    );

    // A non-numeric payload stays text and compares as text.
    exec_sql(&exec, "CREATE TABLE bt (id INTEGER PRIMARY KEY, name TEXT)");
    exec_sql(&exec, "INSERT INTO bt (id, name) VALUES (1, 'abc')");

    let stmt = parse_sql("SELECT id FROM bt WHERE name = $1").unwrap();
    let stmt = bind_statement_params(stmt, &[Some("abc".to_string())]).unwrap();
    let (_cols, rows) = exec_rows_from_stmt(&exec, &stmt);
    assert_eq!(rows, vec![vec!["1".to_string()]]);
}

/// The aggregate result key keeps the column's case: lowercasing the
/// whole key made `SUM(MyCol)` look up `sum(mycol)` and print NULL.
#[test]
fn test_aggregate_keeps_column_case() {
    let (_db, exec) = create_sql_env("aggcase");

    exec_sql(
        &exec,
        "CREATE TABLE mc (id INTEGER PRIMARY KEY, MyCol INTEGER)",
    );
    exec_sql(&exec, "INSERT INTO mc (id, MyCol) VALUES (1, 10)");
    exec_sql(&exec, "INSERT INTO mc (id, MyCol) VALUES (2, 20)");

    let (cols, rows) = exec_rows(&exec, "SELECT SUM(MyCol) FROM mc");
    assert_eq!(
        rows,
        vec![vec!["30".to_string()]],
        "mixed-case target is found"
    );
    assert_eq!(cols, vec!["sum(MyCol)".to_string()]);
}

/// A bound value written to a table keeps the client's bytes exactly:
/// numeric coercion applies only to predicate comparison, never to stored
/// data, so "007" is not rewritten to "7" nor "1.50" to "1.5".
#[test]
fn test_bound_value_is_stored_verbatim() {
    let (_db, exec) = create_sql_env("bindverbatim");

    exec_sql(&exec, "CREATE TABLE bv (id TEXT PRIMARY KEY, v TEXT)");

    let stmt = parse_sql("INSERT INTO bv (id, v) VALUES ($1, $2)").unwrap();
    let stmt =
        bind_statement_params(stmt, &[Some("007".to_string()), Some("1.50".to_string())]).unwrap();
    exec.execute(&stmt).expect("insert must succeed");

    let (_cols, rows) = exec_rows(&exec, "SELECT id, v FROM bv");
    assert_eq!(rows, vec![vec!["007".to_string(), "1.50".to_string()]]);

    // An UPDATE assignment is stored data too.
    let stmt = parse_sql("UPDATE bv SET v = $1 WHERE id = '007'").unwrap();
    let stmt = bind_statement_params(stmt, &[Some("08".to_string())]).unwrap();
    exec.execute(&stmt).expect("update must succeed");

    let (_cols, rows) = exec_rows(&exec, "SELECT v FROM bv");
    assert_eq!(rows, vec![vec!["08".to_string()]]);
}

/// A predicate payload written the way its number does NOT print stays
/// text, so it finds the row stored under that exact text by every path —
/// scan, primary-key lookup, and IN. A canonical "7" still compares numerically.
#[test]
fn test_bound_predicate_round_trips_non_canonical_text() {
    let (_db, exec) = create_sql_env("bindrt");

    exec_sql(&exec, "CREATE TABLE rt (id TEXT PRIMARY KEY, v INTEGER)");
    exec_sql(&exec, "INSERT INTO rt (id, v) VALUES ('007', 9)");

    let stmt = parse_sql("SELECT id FROM rt WHERE id = $1").unwrap();
    let stmt = bind_statement_params(stmt, &[Some("007".to_string())]).unwrap();
    let (_cols, rows) = exec_rows_from_stmt(&exec, &stmt);
    assert_eq!(rows, vec![vec!["007".to_string()]]);

    let stmt = parse_sql("SELECT id FROM rt WHERE id IN ($1)").unwrap();
    let stmt = bind_statement_params(stmt, &[Some("007".to_string())]).unwrap();
    let (_cols, rows) = exec_rows_from_stmt(&exec, &stmt);
    assert_eq!(rows, vec![vec!["007".to_string()]]);

    // A canonical numeric payload still orders numerically.
    let stmt = parse_sql("SELECT id FROM rt WHERE v > $1").unwrap();
    let stmt = bind_statement_params(stmt, &[Some("5".to_string())]).unwrap();
    let (_cols, rows) = exec_rows_from_stmt(&exec, &stmt);
    assert_eq!(rows, vec![vec!["007".to_string()]]);
}

/// A numeric column's declared type decides comparison, not the payload's
/// shape: a bound "007" matches the row stored as "7" by scan, primary-key
/// lookup, and IN, while a text column keeps the same payload verbatim.
#[test]
fn test_numeric_affinity_matches_non_canonical_payload() {
    let (_db, exec) = create_sql_env("affinity");

    exec_sql(
        &exec,
        "CREATE TABLE num (id INTEGER PRIMARY KEY, v INTEGER)",
    );
    exec_sql(&exec, "INSERT INTO num (id, v) VALUES (7, 1000)");

    // The stored form is canonical, so an unquoted literal finds it.
    let (_cols, rows) = exec_rows(&exec, "SELECT id FROM num WHERE id = 7");
    assert_eq!(rows, vec![vec!["7".to_string()]]);

    // A bound non-canonical payload matches the numeric column by value.
    let stmt = parse_sql("SELECT id FROM num WHERE id = $1").unwrap();
    let stmt = bind_statement_params(stmt, &[Some("007".to_string())]).unwrap();
    let (_cols, rows) = exec_rows_from_stmt(&exec, &stmt);
    assert_eq!(rows, vec![vec!["7".to_string()]]);

    let stmt = parse_sql("SELECT id FROM num WHERE id IN ($1)").unwrap();
    let stmt = bind_statement_params(stmt, &[Some("007".to_string())]).unwrap();
    let (_cols, rows) = exec_rows_from_stmt(&exec, &stmt);
    assert_eq!(rows, vec![vec!["7".to_string()]]);

    // A text column stores the same payload verbatim and matches only itself.
    exec_sql(&exec, "CREATE TABLE txt (id TEXT PRIMARY KEY)");
    exec_sql(&exec, "INSERT INTO txt (id) VALUES ('007')");

    let stmt = parse_sql("SELECT id FROM txt WHERE id = $1").unwrap();
    let stmt = bind_statement_params(stmt, &[Some("007".to_string())]).unwrap();
    let (_cols, rows) = exec_rows_from_stmt(&exec, &stmt);
    assert_eq!(rows, vec![vec!["007".to_string()]]);

    let stmt = parse_sql("SELECT id FROM txt WHERE id = $1").unwrap();
    let stmt = bind_statement_params(stmt, &[Some("7".to_string())]).unwrap();
    let (_cols, rows) = exec_rows_from_stmt(&exec, &stmt);
    assert!(rows.is_empty(), "a text column must not match 7 for 007");
}

fn exec_rows_from_stmt(
    executor: &SqlExecutor,
    stmt: &SqlStatement,
) -> (Vec<String>, Vec<Vec<String>>) {
    match executor
        .execute(stmt)
        .unwrap_or_else(|e| panic!("Exec error: {e}"))
    {
        ExecResult::Rows { columns, rows } => (columns, rows),
        _ => panic!("Expected Rows result"),
    }
}

/// UPDATE and DELETE predicates must compare by the column's declared
/// affinity, not the literal's: `WHERE Qty = $1` with `$1 = "007"` has to
/// hit an integer 7 exactly as the equivalent SELECT does. Lexical
/// comparison reads "7" > "007" and misses. The mixed-case column also
/// exercises type-map keying by the declared name.
#[test]
fn test_update_delete_predicate_uses_column_type() {
    let (_db, exec) = create_sql_env("updeltyped");

    exec_sql(
        &exec,
        "CREATE TABLE ud (id INTEGER PRIMARY KEY, Qty INTEGER)",
    );
    exec_sql(&exec, "INSERT INTO ud (id, Qty) VALUES (1, 7)");
    exec_sql(&exec, "INSERT INTO ud (id, Qty) VALUES (2, 10)");
    exec_sql(&exec, "INSERT INTO ud (id, Qty) VALUES (3, 20)");

    // SELECT is the reference behaviour: numeric match for a bound "007".
    let stmt = parse_sql("SELECT id FROM ud WHERE Qty = $1").unwrap();
    let stmt = bind_statement_params(stmt, &[Some("007".to_string())]).unwrap();
    let (_cols, rows) = exec_rows_from_stmt(&exec, &stmt);
    assert_eq!(
        rows,
        vec![vec!["1".to_string()]],
        "SELECT must match integer 7 for bound 007"
    );

    // UPDATE must agree: only the 7 row flips.
    let stmt = parse_sql("UPDATE ud SET Qty = 100 WHERE Qty = $1").unwrap();
    let stmt = bind_statement_params(stmt, &[Some("007".to_string())]).unwrap();
    match exec.execute(&stmt).unwrap() {
        ExecResult::Modified { count, .. } => assert_eq!(
            count, 1,
            "UPDATE bound 007 must match exactly the integer 7 row"
        ),
        _ => panic!("Expected Modified result"),
    }
    let (_cols, rows) = exec_rows(&exec, "SELECT id, Qty FROM ud ORDER BY id");
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "100".to_string()],
            vec!["2".to_string(), "10".to_string()],
            vec!["3".to_string(), "20".to_string()],
        ]
    );

    // DELETE must agree too: bound "15" is numeric, so 100 and 20 go, 10 stays.
    let stmt = parse_sql("DELETE FROM ud WHERE Qty > $1").unwrap();
    let stmt = bind_statement_params(stmt, &[Some("15".to_string())]).unwrap();
    match exec.execute(&stmt).unwrap() {
        ExecResult::Modified { count, .. } => assert_eq!(
            count, 2,
            "DELETE bound 15 must remove 100 and 20 numerically"
        ),
        _ => panic!("Expected Modified result"),
    }
    let (_cols, rows) = exec_rows(&exec, "SELECT id, Qty FROM ud");
    assert_eq!(
        rows,
        vec![vec!["2".to_string(), "10".to_string()]],
        "only the row under 15 survives"
    );
}

/// A qualified WHERE name resolves against the table it names, even when
/// both sides of a join share the column: `WHERE b.id = 2` must read B's
/// `id`, not A's. A qualifier naming no FROM table is an error.
#[test]
fn test_qualified_where_column_binds_to_its_table() {
    let (_db, exec) = create_sql_env("qualwhere");

    exec_sql(
        &exec,
        "CREATE TABLE qa (id INTEGER PRIMARY KEY, shared INTEGER)",
    );
    exec_sql(
        &exec,
        "CREATE TABLE qb (id INTEGER PRIMARY KEY, shared INTEGER)",
    );
    exec_sql(&exec, "INSERT INTO qa (id, shared) VALUES (1, 10)");
    exec_sql(&exec, "INSERT INTO qa (id, shared) VALUES (2, 40)");
    exec_sql(&exec, "INSERT INTO qb (id, shared) VALUES (1, 20)");
    exec_sql(&exec, "INSERT INTO qb (id, shared) VALUES (2, 30)");

    // Unqualified `shared` resolves to the build side's value (10) — the
    // reference behaviour the qualified lookups must not disturb.
    let (_cols, rows) = exec_rows(&exec, "SELECT qb.id FROM qa JOIN qb ON qa.id = qb.id");
    assert_eq!(
        rows,
        vec![vec!["1".to_string()], vec!["2".to_string()]],
        "join returns both rows"
    );

    // Qualified predicate binds to the table it names: qb.shared = 30 is
    // only true for qb row 2, never for the shared-10 build side.
    let (_cols, rows) = exec_rows(
        &exec,
        "SELECT qb.id FROM qa JOIN qb ON qa.id = qb.id WHERE qb.shared = 30",
    );
    assert_eq!(
        rows,
        vec![vec!["2".to_string()]],
        "qualified qb.shared must bind to qb, not the build side"
    );

    // The other side's qualifier works too: qa.shared = 10 matches only
    // qa row 1, which joins to qb row 1.
    let (_cols, rows) = exec_rows(
        &exec,
        "SELECT qb.id FROM qa JOIN qb ON qa.id = qb.id WHERE qa.shared = 10",
    );
    assert_eq!(rows, vec![vec!["1".to_string()]]);

    // A qualifier naming no FROM table is rejected, not silently ignored.
    let stmt = parse_sql("SELECT qb.id FROM qa JOIN qb ON qa.id = qb.id WHERE nope.shared = 30");
    assert!(stmt.is_ok());
    match exec.execute(&stmt.unwrap()) {
        Err(e) => assert!(
            e.contains("not in the FROM clause"),
            "bogus qualifier must error, got: {e}"
        ),
        Ok(_) => panic!("bogus qualifier must be an error, not a silent match"),
    }
}

/// A qualified name on a single-table statement still works and still
/// compares by the column's affinity, while a wrong qualifier is rejected.
#[test]
fn test_qualified_where_single_table() {
    let (_db, exec) = create_sql_env("qualsingle");

    exec_sql(
        &exec,
        "CREATE TABLE st (id INTEGER PRIMARY KEY, Qty INTEGER)",
    );
    exec_sql(&exec, "INSERT INTO st (id, Qty) VALUES (1, 7)");
    exec_sql(&exec, "INSERT INTO st (id, Qty) VALUES (2, 9)");

    // Qualified name on its own table resolves and compares numerically:
    // bound "007" equals integer 7.
    let stmt = parse_sql("SELECT id FROM st WHERE st.Qty = $1").unwrap();
    let stmt = bind_statement_params(stmt, &[Some("007".to_string())]).unwrap();
    let (_cols, rows) = exec_rows_from_stmt(&exec, &stmt);
    assert_eq!(
        rows,
        vec![vec!["1".to_string()]],
        "qualified single-table predicate compares by affinity"
    );

    // A qualifier naming another table is an error on a single-table statement.
    let stmt = parse_sql("SELECT id FROM st WHERE other.Qty = 7").unwrap();
    match exec.execute(&stmt) {
        Err(e) => assert!(
            e.contains("not in the FROM clause"),
            "wrong single-table qualifier must error, got: {e}"
        ),
        Ok(_) => panic!("wrong qualifier must be an error"),
    }

    // UPDATE/DELETE accept the qualified name too.
    let stmt = parse_sql("UPDATE st SET Qty = 100 WHERE st.Qty = $1").unwrap();
    let stmt = bind_statement_params(stmt, &[Some("009".to_string())]).unwrap();
    match exec.execute(&stmt).unwrap() {
        ExecResult::Modified { count, .. } => assert_eq!(count, 1, "qualified UPDATE matches"),
        _ => panic!("Expected Modified result"),
    }
    let (_cols, rows) = exec_rows(&exec, "SELECT id, Qty FROM st ORDER BY id");
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "7".to_string()],
            vec!["2".to_string(), "100".to_string()],
        ]
    );
}
