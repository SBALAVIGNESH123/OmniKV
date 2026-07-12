use omni_engine::OmniKV;
use omni_engine::optimizer::{AccessMethod, Optimizer, PlanNode, TableStats};
use omni_engine::plan_exec::PlanCache;
use omni_engine::prepared::QueryEngine;
use omni_engine::secondary_index::{IndexDefinition, IndexFieldType};
use omni_engine::sql::parse_sql;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

fn table_stats(indexes: Vec<IndexDefinition>) -> HashMap<String, TableStats> {
    HashMap::from([(
        "users".to_string(),
        TableStats {
            table_name: "users".to_string(),
            row_count: 10_000,
            avg_row_bytes: 256,
            indexes,
            histograms: vec![],
        },
    )])
}

fn secondary_index(
    id: u32,
    name: &str,
    field: &str,
    field_type: IndexFieldType,
) -> IndexDefinition {
    IndexDefinition {
        id,
        name: name.to_string(),
        collection: "users".to_string(),
        fields: vec![(field.to_string(), field_type)],
        unique: false,
    }
}

fn optimize(sql: &str, indexes: Vec<IndexDefinition>) -> PlanNode {
    let statement = parse_sql(sql).unwrap_or_else(|err| panic!("parse failed for {sql}: {err}"));
    Optimizer::new(table_stats(indexes))
        .optimize(&statement)
        .unwrap_or_else(|err| panic!("optimize failed for {sql}: {err}"))
}

fn scan_access(plan: &PlanNode) -> &AccessMethod {
    match plan {
        PlanNode::Scan { access, .. } => access,
        PlanNode::Filter { child, .. }
        | PlanNode::Project { child, .. }
        | PlanNode::Sort { child, .. }
        | PlanNode::Limit { child, .. }
        | PlanNode::Aggregate { child, .. } => scan_access(child),
        PlanNode::HashJoin { .. } => panic!("expected a single-table scan plan"),
    }
}

fn open_temp_db() -> (Arc<OmniKV>, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let manifest = dir.path().join("manifest.json");
    let wal = dir.path().join("data.wal");
    let db = OmniKV::open(
        manifest.to_str().expect("manifest path"),
        wal.to_str().expect("wal path"),
    )
    .expect("open temp database");
    (db, dir)
}

#[test]
fn sql_ast_golden_outputs_cover_common_statement_paths() {
    let cases = [
        (
            "INSERT INTO users (id, name, active) VALUES (1, 'Alice', TRUE)",
            r#"Insert { table: "users", columns: ["id", "name", "active"], values: [[Integer(1), Text("Alice"), Boolean(true)]] }"#,
        ),
        (
            "SELECT id, name FROM users WHERE status = 'active' AND age >= 18 ORDER BY name DESC LIMIT 10 OFFSET 5",
            r#"Select { columns: [Named("id"), Named("name")], from: Table("users"), where_clause: Some(And(Comparison { column: "status", op: Eq, value: Text("active") }, Comparison { column: "age", op: Gte, value: Integer(18) })), group_by: [], having: None, order_by: [OrderByItem { column: "name", desc: true }], limit: Some(10), offset: Some(5) }"#,
        ),
        (
            "UPDATE users SET name = 'Bob' WHERE id = 1",
            r#"Update { table: "users", assignments: [("name", Text("Bob"))], where_clause: Some(Comparison { column: "id", op: Eq, value: Integer(1) }) }"#,
        ),
        (
            "DELETE FROM users WHERE id = 1",
            r#"Delete { table: "users", where_clause: Some(Comparison { column: "id", op: Eq, value: Integer(1) }) }"#,
        ),
    ];

    for (sql, expected_debug) in cases {
        let statement =
            parse_sql(sql).unwrap_or_else(|err| panic!("parse failed for {sql}: {err}"));
        assert_eq!(format!("{statement:?}"), expected_debug, "{sql}");
    }
}

#[test]
fn unsupported_sql_returns_clear_contract_errors() {
    let cases = [
        (
            "ALTER TABLE users ADD COLUMN age INTEGER",
            "Unsupported: ALTER",
        ),
        (
            "SELECT * FROM users WHERE age BETWEEN 18 AND 65",
            "Unknown operator: BETWEEN",
        ),
        ("SELECT id users", "Expected FROM"),
    ];

    for (sql, expected_error) in cases {
        let error = parse_sql(sql).expect_err(sql);
        assert!(
            error.contains(expected_error),
            "expected {expected_error:?} in error {error:?} for {sql}"
        );
    }
}

#[test]
fn planner_selects_primary_key_lookup_for_id_equality() {
    let plan = optimize("SELECT * FROM users WHERE id = 42", vec![]);

    match scan_access(&plan) {
        AccessMethod::PkLookup { key_value } => assert_eq!(key_value, "42"),
        other => panic!("expected primary-key lookup, got {other:?}"),
    }
}

#[test]
fn planner_selects_secondary_index_scan_for_indexed_predicate() {
    let email_index = secondary_index(7, "users_email_idx", "email", IndexFieldType::String);
    let plan = optimize(
        "SELECT * FROM users WHERE email = 'alice@example.com'",
        vec![email_index],
    );

    match scan_access(&plan) {
        AccessMethod::IndexScan {
            index_name,
            index_id,
        } => {
            assert_eq!(index_name, "users_email_idx");
            assert_eq!(*index_id, 7);
        }
        other => panic!("expected secondary-index scan, got {other:?}"),
    }
}

#[test]
fn planner_selects_index_scan_for_range_predicate_when_index_exists() {
    let age_index = secondary_index(8, "users_age_idx", "age", IndexFieldType::Integer);
    let plan = optimize("SELECT * FROM users WHERE age >= 18", vec![age_index]);

    match scan_access(&plan) {
        AccessMethod::IndexScan {
            index_name,
            index_id,
        } => {
            assert_eq!(index_name, "users_age_idx");
            assert_eq!(*index_id, 8);
        }
        other => panic!("expected range predicate to use age index, got {other:?}"),
    }
}

#[test]
fn planner_falls_back_to_seq_scan_without_matching_index() {
    let name_index = secondary_index(9, "users_name_idx", "name", IndexFieldType::String);
    let plan = optimize(
        "SELECT * FROM users WHERE email = 'missing@example.com'",
        vec![name_index],
    );

    assert!(
        matches!(scan_access(&plan), AccessMethod::SeqScan),
        "expected sequential scan fallback, got {:?}",
        scan_access(&plan)
    );
}

#[test]
fn optimized_plan_cache_invalidation_drops_cached_plans() {
    let cache = PlanCache::new(4);
    let plan = optimize("SELECT * FROM users WHERE id = 42", vec![]);
    let query = "SELECT * FROM users WHERE id = 42";

    cache.put(query.to_string(), plan);
    assert!(cache.get(query).is_some(), "plan should be cached");

    cache.invalidate();
    assert!(cache.get(query).is_none(), "plan should be invalidated");
}

#[test]
fn prepared_query_cache_clear_resets_plans_and_statistics() {
    let (db, _dir) = open_temp_db();
    let engine = QueryEngine::new(db, 4);
    let query = "SELECT * WHERE key = $1";

    let first = engine.prepare(query).expect("first prepare");
    let second = engine.prepare(query).expect("second prepare");

    assert_ne!(first.id, second.id);
    assert_eq!(engine.cache_stats(), (1, 1, 1));

    engine.clear_cache();
    assert_eq!(engine.cache_stats(), (0, 0, 0));

    let third = engine.prepare(query).expect("third prepare after clear");
    assert_ne!(second.id, third.id);
    assert_eq!(engine.cache_stats(), (0, 1, 1));
}
