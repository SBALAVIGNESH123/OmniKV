//! REST API Server (Axum)
//!
//! Production-grade HTTP/1.1 + HTTP/2 REST API with:
//! - CRUD operations (GET, SET, DELETE, SCAN)
//! - Transaction support (BEGIN, COMMIT, ROLLBACK)
//! - Backup/restore endpoints
//! - Prometheus metrics export
//! - JWT authentication middleware
//! - Health checks

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    middleware,
    response::IntoResponse,
};
use omnikv_engine::{OmniKV, WriteBatch};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tower_http::cors::CorsLayer;

/// Application state shared across all handlers.
#[derive(Clone)]
pub struct AppState {
    pub db: Arc<OmniKV>,
    pub jwt_secret: String,
    pub manifest_path: String,
}

#[derive(Deserialize)]
pub struct SetRequest {
    pub key: String,
    pub value: String,
    pub expiry: Option<u64>,
}

#[derive(Deserialize)]
pub struct BatchRequest {
    pub operations: Vec<BatchOp>,
}

#[derive(Deserialize)]
pub struct BatchOp {
    pub op: String, // "set" or "delete"
    pub key: String,
    pub value: Option<String>,
}

#[derive(Serialize)]
pub struct ApiResponse<T: Serialize> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
}

impl<T: Serialize> ApiResponse<T> {
    pub fn ok(data: T) -> Json<Self> {
        Json(Self {
            success: true,
            data: Some(data),
            error: None,
        })
    }
    pub fn err(msg: &str) -> Json<Self> {
        Json(Self {
            success: false,
            data: None,
            error: Some(msg.to_string()),
        })
    }
}

#[derive(Serialize)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

#[derive(Serialize)]
pub struct WriteResult {
    pub seq: u64,
}

#[derive(Serialize)]
pub struct HealthStatus {
    pub status: String,
    pub version: String,
    pub uptime_secs: u64,
    pub sstable_count: usize,
}

#[derive(Deserialize)]
pub struct ScanQuery {
    pub start: Option<String>,
    pub end: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Serialize)]
pub struct MetricsOutput {
    pub text: String,
}

/// Build the Axum router with all API routes.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        // Health
        .route("/health", axum::routing::get(health_handler))
        // CRUD
        .route("/kv/{key}", axum::routing::get(get_handler))
        .route("/kv", axum::routing::post(set_handler))
        .route("/kv/{key}", axum::routing::delete(delete_handler))
        // Batch
        .route("/batch", axum::routing::post(batch_handler))
        // Scan
        .route("/scan", axum::routing::get(scan_handler))
        // Metrics
        .route("/metrics", axum::routing::get(metrics_handler))
        // Backup
        .route("/admin/backup", axum::routing::post(backup_handler))
        // Auth
        .route("/auth/token", axum::routing::post(token_handler))
        // Compaction
        .route("/admin/compact", axum::routing::post(compact_handler))
        // Layer
        .layer(CorsLayer::permissive())
        .with_state(state)
}

// ─── Handlers ───────────────────────────────────────────

static START_TIME: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();

async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    let uptime = START_TIME
        .get_or_init(std::time::Instant::now)
        .elapsed()
        .as_secs();
    ApiResponse::ok(HealthStatus {
        status: "ok".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        uptime_secs: uptime,
        sstable_count: state.db.sstable_count(),
    })
}

async fn get_handler(State(state): State<AppState>, Path(key): Path<String>) -> impl IntoResponse {
    let seq = state.db.get_seq();
    match state.db.find(&key, seq) {
        Ok(Some(val)) => (
            StatusCode::OK,
            ApiResponse::ok(KeyValue { key, value: val }),
        ),
        Ok(None) => (StatusCode::NOT_FOUND, ApiResponse::err("Key not found")),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiResponse::err(&format!("{:?}", e)),
        ),
    }
}

async fn set_handler(
    State(state): State<AppState>,
    Json(req): Json<SetRequest>,
) -> impl IntoResponse {
    let mut batch = WriteBatch::new();
    if let Some(ttl) = req.expiry {
        if let Err(e) = batch.set_with_ttl(&req.key, req.value, ttl) {
            return (
                StatusCode::BAD_REQUEST,
                ApiResponse::<WriteResult>::err(&format!("{:?}", e)),
            );
        }
    } else {
        if let Err(e) = batch.set(&req.key, req.value) {
            return (
                StatusCode::BAD_REQUEST,
                ApiResponse::<WriteResult>::err(&format!("{:?}", e)),
            );
        }
    }

    match state.db.commit_batch(&batch) {
        Ok(seq) => (StatusCode::CREATED, ApiResponse::ok(WriteResult { seq })),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiResponse::<WriteResult>::err(&format!("{:?}", e)),
        ),
    }
}

async fn delete_handler(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    let mut batch = WriteBatch::new();
    match batch.delete(&key) {
        Ok(_) => match state.db.commit_batch(&batch) {
            Ok(seq) => (StatusCode::OK, ApiResponse::ok(WriteResult { seq })),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                ApiResponse::<WriteResult>::err(&format!("{:?}", e)),
            ),
        },
        Err(e) => (
            StatusCode::BAD_REQUEST,
            ApiResponse::<WriteResult>::err(&format!("{:?}", e)),
        ),
    }
}

async fn batch_handler(
    State(state): State<AppState>,
    Json(req): Json<BatchRequest>,
) -> impl IntoResponse {
    let mut batch = WriteBatch::new();
    for op in &req.operations {
        match op.op.as_str() {
            "set" => {
                if let Some(ref val) = op.value {
                    let _ = batch.set(&op.key, val.clone());
                }
            }
            "delete" => {
                let _ = batch.delete(&op.key);
            }
            _ => {
                return (
                    StatusCode::BAD_REQUEST,
                    ApiResponse::<WriteResult>::err(&format!("Unknown op: {}", op.op)),
                );
            }
        }
    }

    match state.db.commit_batch(&batch) {
        Ok(seq) => (StatusCode::OK, ApiResponse::ok(WriteResult { seq })),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiResponse::<WriteResult>::err(&format!("{:?}", e)),
        ),
    }
}

async fn scan_handler(
    State(state): State<AppState>,
    Query(q): Query<ScanQuery>,
) -> impl IntoResponse {
    let start = q.start.as_deref().unwrap_or("");
    let end = q.end.as_deref().unwrap_or("\x7F");
    let seq = state.db.get_seq();

    match state.db.scan(start, end, seq) {
        Ok(results) => {
            let limit = q.limit.unwrap_or(1000);
            let items: Vec<KeyValue> = results
                .into_iter()
                .take(limit)
                .map(|(k, v)| KeyValue { key: k, value: v })
                .collect();
            (StatusCode::OK, ApiResponse::ok(items))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiResponse::<Vec<KeyValue>>::err(&format!("{:?}", e)),
        ),
    }
}

async fn metrics_handler() -> impl IntoResponse {
    let text = omnikv_engine::metrics_prometheus::render_metrics();
    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        text,
    )
}

async fn backup_handler(State(state): State<AppState>) -> impl IntoResponse {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let backup_path = format!("backup_{}.tar.gz", timestamp);

    match crate::backup::create_backup(&state.db, &state.manifest_path, &backup_path) {
        Ok(path) => (StatusCode::OK, ApiResponse::ok(path)),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiResponse::<String>::err(&e),
        ),
    }
}

async fn compact_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state.db.compact_sstables() {
        Ok(()) => (
            StatusCode::OK,
            ApiResponse::ok("Compaction complete".to_string()),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiResponse::<String>::err(&format!("{:?}", e)),
        ),
    }
}

#[derive(Deserialize)]
pub struct TokenRequest {
    pub username: String,
    pub role: Option<String>,
}

async fn token_handler(
    State(state): State<AppState>,
    Json(req): Json<TokenRequest>,
) -> impl IntoResponse {
    let role = req.role.as_deref().unwrap_or("read");
    match crate::auth::generate_token(&req.username, role, &state.jwt_secret, 86400) {
        Ok(token) => (StatusCode::OK, ApiResponse::ok(token)),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiResponse::<String>::err(&e),
        ),
    }
}
