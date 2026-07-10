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
    body::Body,
    extract::Request,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    middleware,
    response::{IntoResponse, Response},
};
use omni_engine::{OmniError, OmniKV, WriteBatch};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tower_http::cors::CorsLayer;

/// Application state shared across all handlers.
#[derive(Clone)]
pub struct AppState {
    pub db: Arc<OmniKV>,
    pub jwt_secret: String,
    pub bootstrap_admin_key: String,
    pub manifest_path: String,
    pub wal_path: String,
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

#[derive(Serialize)]
pub struct ReadyStatus {
    pub ready: bool,
    pub checks: ReadyChecks,
}

#[derive(Serialize)]
pub struct ReadyChecks {
    pub storage: bool,
    pub sequence_advancing: bool,
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

#[derive(Clone, Copy)]
enum RequiredRole {
    Read,
    Write,
    Admin,
}

impl RequiredRole {
    fn allows(self, role: &str) -> bool {
        match self {
            Self::Read => matches!(role, "read" | "write" | "admin"),
            Self::Write => matches!(role, "write" | "admin"),
            Self::Admin => role == "admin",
        }
    }
}

/// Map internal storage errors to stable, sanitized client-facing error codes.
/// Internal error details are never exposed to clients — they are logged server-side.
fn sanitize_storage_err(e: &OmniError) -> String {
    // Log full error server-side for operators
    tracing::error!(error = ?e, "internal storage error");
    // Return stable, opaque code to client
    match e {
        OmniError::KeyNotFound => "NOT_FOUND".to_string(),
        OmniError::BatchTooLarge(_) => "BATCH_TOO_LARGE".to_string(),
        OmniError::ValueTooLarge(_) => "VALUE_TOO_LARGE".to_string(),
        OmniError::UnsupportedVersion { .. } => "UNSUPPORTED_VERSION".to_string(),
        _ => "STORAGE_ERROR".to_string(),
    }
}

/// Map a generic std::error::Error to a sanitized client-facing message.
fn sanitize_err(e: &impl std::fmt::Debug) -> String {
    tracing::error!(error = ?e, "internal error");
    "INTERNAL_ERROR".to_string()
}

/// Derive correct HTTP status code from OmniError variant.
fn http_status_for_storage_err(e: &OmniError) -> StatusCode {
    match e {
        OmniError::KeyNotFound => StatusCode::NOT_FOUND,
        OmniError::BatchTooLarge(_) | OmniError::ValueTooLarge(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

pub fn build_router(state: AppState) -> Router {
    let read_routes = Router::new()
        .route("/kv/{key}", axum::routing::get(get_handler))
        .route("/scan", axum::routing::get(scan_handler))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_read));

    let write_routes = Router::new()
        .route("/kv", axum::routing::post(set_handler))
        .route("/kv/{key}", axum::routing::delete(delete_handler))
        .route("/batch", axum::routing::post(batch_handler))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_write));

    let admin_routes = Router::new()
        .route("/metrics", axum::routing::get(metrics_handler))
        .route("/admin/backup", axum::routing::post(backup_handler))
        .route("/admin/compact", axum::routing::post(compact_handler))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_admin));

    Router::new()
        .route("/health", axum::routing::get(health_handler))
        .route("/ready", axum::routing::get(ready_handler))
        .route("/auth/token", axum::routing::post(token_handler))
        .merge(read_routes)
        .merge(write_routes)
        .merge(admin_routes)
        .layer(CorsLayer::permissive())
        .with_state(state)
}

async fn require_read(
    State(state): State<AppState>,
    req: Request<Body>,
    next: middleware::Next,
) -> Result<Response, Response> {
    require_role(state, req, next, RequiredRole::Read).await
}

async fn require_write(
    State(state): State<AppState>,
    req: Request<Body>,
    next: middleware::Next,
) -> Result<Response, Response> {
    require_role(state, req, next, RequiredRole::Write).await
}

async fn require_admin(
    State(state): State<AppState>,
    req: Request<Body>,
    next: middleware::Next,
) -> Result<Response, Response> {
    require_role(state, req, next, RequiredRole::Admin).await
}

async fn require_role(
    state: AppState,
    req: Request<Body>,
    next: middleware::Next,
    required: RequiredRole,
) -> Result<Response, Response> {
    let Some(header_value) = req.headers().get(header::AUTHORIZATION) else {
        return Err(auth_error(StatusCode::UNAUTHORIZED, "missing bearer token"));
    };
    let Ok(header_value) = header_value.to_str() else {
        return Err(auth_error(
            StatusCode::UNAUTHORIZED,
            "invalid authorization header",
        ));
    };
    let Some(token) = crate::auth::extract_bearer(header_value) else {
        return Err(auth_error(
            StatusCode::UNAUTHORIZED,
            "authorization header must use Bearer token",
        ));
    };

    match crate::auth::verify_token(token, &state.jwt_secret) {
        Ok(claims) if required.allows(&claims.role) => Ok(next.run(req).await),
        Ok(_) => Err(auth_error(StatusCode::FORBIDDEN, "insufficient role")),
        Err(_) => Err(auth_error(StatusCode::UNAUTHORIZED, "invalid bearer token")),
    }
}

fn auth_error(status: StatusCode, message: &str) -> Response {
    (status, ApiResponse::<()>::err(message)).into_response()
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

async fn ready_handler(State(state): State<AppState>) -> impl IntoResponse {
    let seq = state.db.get_seq();
    let storage_ok = state.db.sstable_count() < 10_000;
    let ready = storage_ok;
    let status = ReadyStatus {
        ready,
        checks: ReadyChecks {
            storage: storage_ok,
            sequence_advancing: seq > 0,
        },
    };
    if ready {
        (StatusCode::OK, ApiResponse::ok(status))
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, ApiResponse::ok(status))
    }
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
            ApiResponse::err(&sanitize_storage_err(&e)),
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
                http_status_for_storage_err(&e),
                ApiResponse::<WriteResult>::err(&sanitize_storage_err(&e)),
            );
        }
    } else if let Err(e) = batch.set(&req.key, req.value) {
        return (
            http_status_for_storage_err(&e),
            ApiResponse::<WriteResult>::err(&sanitize_storage_err(&e)),
        );
    }

    match state.db.commit_batch(&batch) {
        Ok(seq) => (StatusCode::CREATED, ApiResponse::ok(WriteResult { seq })),
        Err(e) => (
            http_status_for_storage_err(&e),
            ApiResponse::<WriteResult>::err(&sanitize_storage_err(&e)),
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
                http_status_for_storage_err(&e),
                ApiResponse::<WriteResult>::err(&sanitize_storage_err(&e)),
            ),
        },
        Err(e) => (
            http_status_for_storage_err(&e),
            ApiResponse::<WriteResult>::err(&sanitize_storage_err(&e)),
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
            http_status_for_storage_err(&e),
            ApiResponse::<WriteResult>::err(&sanitize_storage_err(&e)),
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
            ApiResponse::<Vec<KeyValue>>::err(&sanitize_storage_err(&e)),
        ),
    }
}

async fn metrics_handler() -> impl IntoResponse {
    let text = omni_engine::metrics_prometheus::render_metrics();
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

    match omni_engine::backup::create_backup_with_wal(
        &state.db,
        &state.manifest_path,
        &state.wal_path,
        &backup_path,
    ) {
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
            http_status_for_storage_err(&e),
            ApiResponse::<String>::err(&sanitize_storage_err(&e)),
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
    headers: HeaderMap,
    Json(req): Json<TokenRequest>,
) -> impl IntoResponse {
    let Some(header_value) = headers.get("x-omni-admin-key") else {
        return (
            StatusCode::UNAUTHORIZED,
            ApiResponse::<String>::err("missing bootstrap admin key"),
        );
    };
    let Ok(provided_key) = header_value.to_str() else {
        return (
            StatusCode::UNAUTHORIZED,
            ApiResponse::<String>::err("invalid bootstrap admin key"),
        );
    };
    if !crate::auth::validate_api_key(provided_key, &state.bootstrap_admin_key) {
        return (
            StatusCode::UNAUTHORIZED,
            ApiResponse::<String>::err("invalid bootstrap admin key"),
        );
    }

    let role = req.role.as_deref().unwrap_or("read");
    if !matches!(role, "read" | "write" | "admin") {
        return (
            StatusCode::BAD_REQUEST,
            ApiResponse::<String>::err("role must be read, write, or admin"),
        );
    }

    match crate::auth::generate_token(&req.username, role, &state.jwt_secret, 86400) {
        Ok(token) => (StatusCode::OK, ApiResponse::ok(token)),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiResponse::<String>::err(&e),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Method, Request};
    use serde_json::json;
    use tower::ServiceExt;

    fn test_router() -> (Router, tempfile::TempDir, String) {
        let dir = tempfile::tempdir().expect("tempdir");
        let manifest = dir.path().join("manifest.json");
        let wal = dir.path().join("wal.bin");
        let db = OmniKV::open(
            manifest.to_str().expect("manifest path"),
            wal.to_str().expect("wal path"),
        )
        .expect("open db");
        let jwt_secret = "0123456789abcdef0123456789abcdef".to_string();
        let bootstrap_admin_key = "bootstrap-admin-key-0123456789abcdef".to_string();
        let router = build_router(AppState {
            db,
            jwt_secret: jwt_secret.clone(),
            bootstrap_admin_key,
            manifest_path: manifest.to_string_lossy().to_string(),
            wal_path: wal.to_string_lossy().to_string(),
        });
        (router, dir, jwt_secret)
    }

    #[tokio::test]
    async fn unauthenticated_write_is_rejected() {
        let (router, _dir, _secret) = test_router();
        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/kv")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(json!({"key":"a","value":"b"}).to_string()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn read_token_cannot_write() {
        let (router, _dir, secret) = test_router();
        let token = crate::auth::generate_token("reader", "read", &secret, 60).expect("read token");
        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/kv")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(json!({"key":"a","value":"b"}).to_string()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn write_token_can_write() {
        let (router, _dir, secret) = test_router();
        let token =
            crate::auth::generate_token("writer", "write", &secret, 60).expect("write token");
        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/kv")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(json!({"key":"a","value":"b"}).to_string()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn token_endpoint_requires_bootstrap_key() {
        let (router, _dir, _secret) = test_router();
        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/auth/token")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({"username":"admin","role":"admin"}).to_string(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
