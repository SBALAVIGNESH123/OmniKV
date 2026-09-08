//! Table Management and SQL API
//!
//! Exposes routes for CRUD operations, raw SQL execution, and serving the admin dashboard.

use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse},
};
use omni_engine::sql::parse_sql;
use omni_engine::sql_exec::{ExecResult, SqlExecutor};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;

use crate::api::{ApiResponse, AppState};

/// Paginated result struct for `GET /api/v1/data/:table`
#[derive(Serialize)]
pub struct PaginatedData {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub total: usize,
    pub page: usize,
    pub per_page: usize,
}

/// Dynamic response for SQL execution
#[derive(Serialize)]
#[serde(untagged)]
pub enum SqlResponseData {
    Select {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
        execution_time_ms: f64,
    },
    Dml {
        count: usize,
        command: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        execution_time_ms: Option<f64>,
    },
    Ok {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        execution_time_ms: Option<f64>,
    },
}

/// Request for raw SQL execution
#[derive(Deserialize)]
pub struct SqlRequest {
    pub query: String,
}

/// Pagination parameters
#[derive(Deserialize)]
pub struct PaginationQuery {
    pub page: Option<usize>,
    pub per_page: Option<usize>,
    pub order_by: Option<String>,
    pub desc: Option<bool>,
}

/// Row insertion request
#[derive(Deserialize)]
pub struct InsertRequest {
    pub row: HashMap<String, String>,
}

/// Row update request
#[derive(Deserialize)]
pub struct UpdateRequest {
    pub row: HashMap<String, String>,
}

/// List all tables with metadata
pub async fn list_tables(State(state): State<AppState>) -> impl IntoResponse {
    let tables = state.catalog.list_tables();
    (StatusCode::OK, ApiResponse::ok(tables)).into_response()
}

/// Get details of a single table schema
pub async fn get_table(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Some(table) = state.catalog.get_table(&name) {
        (StatusCode::OK, ApiResponse::ok(table)).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            ApiResponse::<()>::err("Table not found"),
        )
            .into_response()
    }
}

/// Serve the embedded HTML admin dashboard
pub async fn dashboard() -> impl IntoResponse {
    Html(include_str!("../dashboard/index.html"))
}

/// Execute arbitrary SQL statement
pub async fn execute_sql(
    State(state): State<AppState>,
    Json(req): Json<SqlRequest>,
) -> impl IntoResponse {
    let start = Instant::now();
    let stmt = match parse_sql(&req.query) {
        Ok(s) => s,
        Err(e) => return (StatusCode::BAD_REQUEST, ApiResponse::<()>::err(&e)).into_response(),
    };

    let executor = SqlExecutor::new(state.db, state.catalog);
    let result = executor.execute(&stmt);
    let execution_time_ms = start.elapsed().as_secs_f64() * 1000.0;

    match result {
        Ok(ExecResult::Rows { columns, rows }) => (
            StatusCode::OK,
            ApiResponse::ok(SqlResponseData::Select {
                columns,
                rows,
                execution_time_ms,
            }),
        )
            .into_response(),
        Ok(ExecResult::Modified { count, command }) => (
            StatusCode::OK,
            ApiResponse::ok(SqlResponseData::Dml {
                count,
                command,
                execution_time_ms: Some(execution_time_ms),
            }),
        )
            .into_response(),
        Ok(ExecResult::Ok(message)) => (
            StatusCode::OK,
            ApiResponse::ok(SqlResponseData::Ok {
                message,
                execution_time_ms: Some(execution_time_ms),
            }),
        )
            .into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, ApiResponse::<()>::err(&e)).into_response(),
    }
}

/// Query table with pagination
pub async fn query_table(
    State(state): State<AppState>,
    Path(table): Path<String>,
    Query(q): Query<PaginationQuery>,
) -> impl IntoResponse {
    let table_def = match state.catalog.get_table(&table) {
        Some(t) => t,
        None => {
            return (
                StatusCode::NOT_FOUND,
                ApiResponse::<()>::err("Table not found"),
            )
                .into_response();
        }
    };

    let page = q.page.unwrap_or(1).max(1);
    let per_page = q.per_page.unwrap_or(50).clamp(1, 1000);
    let offset = match (page - 1).checked_mul(per_page) {
        Some(o) => o,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                ApiResponse::<()>::err("Page value too large"),
            )
                .into_response();
        }
    };

    // Validate order_by against table columns to prevent SQL injection
    let order_clause = if let Some(ref order_by) = q.order_by {
        let valid = table_def
            .columns
            .iter()
            .any(|c| c.name.eq_ignore_ascii_case(order_by));
        if !valid {
            return (
                StatusCode::BAD_REQUEST,
                ApiResponse::<()>::err(&format!("Invalid order_by column: '{}'", order_by)),
            )
                .into_response();
        }
        let desc_str = if q.desc.unwrap_or(false) { " DESC" } else { "" };
        format!(" ORDER BY {}{}", order_by, desc_str)
    } else {
        String::new()
    };

    let sql = format!(
        "SELECT * FROM {}{} LIMIT {} OFFSET {}",
        table, order_clause, per_page, offset
    );

    let count_sql = format!("SELECT COUNT(*) FROM {}", table);
    let count_stmt = match parse_sql(&count_sql) {
        Ok(s) => s,
        Err(e) => return (StatusCode::BAD_REQUEST, ApiResponse::<()>::err(&e)).into_response(),
    };

    let executor = SqlExecutor::new(state.db, state.catalog);
    let total = match executor.execute(&count_stmt) {
        Ok(ExecResult::Rows { rows, .. }) => rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0),
        _ => 0,
    };

    let stmt = match parse_sql(&sql) {
        Ok(s) => s,
        Err(e) => return (StatusCode::BAD_REQUEST, ApiResponse::<()>::err(&e)).into_response(),
    };

    match executor.execute(&stmt) {
        Ok(ExecResult::Rows { columns, rows }) => (
            StatusCode::OK,
            ApiResponse::ok(PaginatedData {
                columns,
                rows,
                total,
                page,
                per_page,
            }),
        )
            .into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, ApiResponse::<()>::err(&e)).into_response(),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiResponse::<()>::err("Unexpected result"),
        )
            .into_response(),
    }
}

/// Select a single row by primary key
pub async fn get_row(
    State(state): State<AppState>,
    Path((table_name, id)): Path<(String, String)>,
) -> impl IntoResponse {
    let table = match state.catalog.get_table(&table_name) {
        Some(t) => t,
        None => {
            return (
                StatusCode::NOT_FOUND,
                ApiResponse::<()>::err("Table not found"),
            )
                .into_response();
        }
    };

    let sql = format!(
        "SELECT * FROM {} WHERE {} = '{}'",
        table_name,
        table.primary_key,
        id.replace('\'', "''")
    );

    let stmt = match parse_sql(&sql) {
        Ok(s) => s,
        Err(e) => return (StatusCode::BAD_REQUEST, ApiResponse::<()>::err(&e)).into_response(),
    };

    let executor = SqlExecutor::new(state.db, state.catalog);
    match executor.execute(&stmt) {
        Ok(ExecResult::Rows { columns, mut rows }) => {
            if rows.is_empty() {
                (
                    StatusCode::NOT_FOUND,
                    ApiResponse::<()>::err("Row not found"),
                )
                    .into_response()
            } else {
                let row = rows.remove(0);
                let mut data = HashMap::new();
                for (i, col) in columns.into_iter().enumerate() {
                    data.insert(col, row[i].clone());
                }
                (StatusCode::OK, ApiResponse::ok(data)).into_response()
            }
        }
        Err(e) => (StatusCode::BAD_REQUEST, ApiResponse::<()>::err(&e)).into_response(),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiResponse::<()>::err("Unexpected result"),
        )
            .into_response(),
    }
}

/// Insert a new row
pub async fn insert_row(
    State(state): State<AppState>,
    Path(table_name): Path<String>,
    Json(req): Json<InsertRequest>,
) -> impl IntoResponse {
    if state.catalog.get_table(&table_name).is_none() {
        return (
            StatusCode::NOT_FOUND,
            ApiResponse::<()>::err("Table not found"),
        )
            .into_response();
    }

    let mut cols = Vec::new();
    let mut vals = Vec::new();
    for (k, v) in req.row {
        cols.push(k);
        vals.push(format!("'{}'", v.replace('\'', "''")));
    }

    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table_name,
        cols.join(", "),
        vals.join(", ")
    );

    let stmt = match parse_sql(&sql) {
        Ok(s) => s,
        Err(e) => return (StatusCode::BAD_REQUEST, ApiResponse::<()>::err(&e)).into_response(),
    };

    let executor = SqlExecutor::new(state.db, state.catalog);
    match executor.execute(&stmt) {
        Ok(ExecResult::Modified { count, command }) => (
            StatusCode::CREATED,
            ApiResponse::ok(SqlResponseData::Dml {
                count,
                command,
                execution_time_ms: None,
            }),
        )
            .into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, ApiResponse::<()>::err(&e)).into_response(),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiResponse::<()>::err("Unexpected result"),
        )
            .into_response(),
    }
}

/// Update a row by primary key
pub async fn update_row(
    State(state): State<AppState>,
    Path((table_name, id)): Path<(String, String)>,
    Json(req): Json<UpdateRequest>,
) -> impl IntoResponse {
    let table = match state.catalog.get_table(&table_name) {
        Some(t) => t,
        None => {
            return (
                StatusCode::NOT_FOUND,
                ApiResponse::<()>::err("Table not found"),
            )
                .into_response();
        }
    };

    let mut sets = Vec::new();
    for (k, v) in req.row {
        sets.push(format!("{} = '{}'", k, v.replace('\'', "''")));
    }

    if sets.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            ApiResponse::<()>::err("Empty update"),
        )
            .into_response();
    }

    let sql = format!(
        "UPDATE {} SET {} WHERE {} = '{}'",
        table_name,
        sets.join(", "),
        table.primary_key,
        id.replace('\'', "''")
    );

    let stmt = match parse_sql(&sql) {
        Ok(s) => s,
        Err(e) => return (StatusCode::BAD_REQUEST, ApiResponse::<()>::err(&e)).into_response(),
    };

    let executor = SqlExecutor::new(state.db, state.catalog);
    match executor.execute(&stmt) {
        Ok(ExecResult::Modified { count, command }) => {
            if count == 0 {
                (
                    StatusCode::NOT_FOUND,
                    ApiResponse::<()>::err("Row not found"),
                )
                    .into_response()
            } else {
                (
                    StatusCode::OK,
                    ApiResponse::ok(SqlResponseData::Dml {
                        count,
                        command,
                        execution_time_ms: None,
                    }),
                )
                    .into_response()
            }
        }
        Err(e) => (StatusCode::BAD_REQUEST, ApiResponse::<()>::err(&e)).into_response(),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiResponse::<()>::err("Unexpected result"),
        )
            .into_response(),
    }
}

/// Delete a row by primary key
pub async fn delete_row(
    State(state): State<AppState>,
    Path((table_name, id)): Path<(String, String)>,
) -> impl IntoResponse {
    let table = match state.catalog.get_table(&table_name) {
        Some(t) => t,
        None => {
            return (
                StatusCode::NOT_FOUND,
                ApiResponse::<()>::err("Table not found"),
            )
                .into_response();
        }
    };

    let sql = format!(
        "DELETE FROM {} WHERE {} = '{}'",
        table_name,
        table.primary_key,
        id.replace('\'', "''")
    );

    let stmt = match parse_sql(&sql) {
        Ok(s) => s,
        Err(e) => return (StatusCode::BAD_REQUEST, ApiResponse::<()>::err(&e)).into_response(),
    };

    let executor = SqlExecutor::new(state.db, state.catalog);
    match executor.execute(&stmt) {
        Ok(ExecResult::Modified { count, command }) => {
            if count == 0 {
                (
                    StatusCode::NOT_FOUND,
                    ApiResponse::<()>::err("Row not found"),
                )
                    .into_response()
            } else {
                (
                    StatusCode::OK,
                    ApiResponse::ok(SqlResponseData::Dml {
                        count,
                        command,
                        execution_time_ms: None,
                    }),
                )
                    .into_response()
            }
        }
        Err(e) => (StatusCode::BAD_REQUEST, ApiResponse::<()>::err(&e)).into_response(),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiResponse::<()>::err("Unexpected result"),
        )
            .into_response(),
    }
}
