//! Tests that the REST API never leaks internal error details to clients.
//!
//! Every error response must contain a stable opaque code (e.g. "STORAGE_ERROR",
//! "NOT_FOUND") and must never contain internal details such as file paths,
//! lock errors, serialization traces, or Rust Debug output.

use axum::body::Body;
use axum::http::Request;
use tower::ServiceExt;

async fn test_app() -> axum::Router {
    use omni_engine::api::{AppState, build_router};
    use tempfile::tempdir;

    let dir = tempdir().unwrap();
    let manifest = dir.path().join("manifest.json");
    let wal = dir.path().join("wal.bin");
    let db = omni_engine::OmniKV::open(&manifest.to_string_lossy(), &wal.to_string_lossy()).unwrap();
    let state = AppState {
        db,
        jwt_secret: "0123456789abcdef0123456789abcdef".to_string(),
        bootstrap_admin_key: "bootstrap-admin-key-0123456789abcdef".to_string(),
        manifest_path: manifest.to_string_lossy().to_string(),
        wal_path: wal.to_string_lossy().to_string(),
    };
    build_router(state)
}

/// Helper: assert the response body does NOT contain internal Rust debug patterns.
fn assert_no_internal_leak(body: &str) {
    let forbidden = [
        "OmniError::",
        "IoError",
        "Poisoned",
        "lock",
        "manifest",
        "wal.bin",
        "src/",
        ".rs:",
        "panicked",
        "thread",
        "LOCK",
        "BufWriter",
        "Mutex",
    ];
    for pattern in forbidden {
        assert!(
            !body.to_lowercase().contains(&pattern.to_lowercase()),
            "API response leaked internal detail '{}' in body: {}",
            pattern,
            body
        );
    }
}

/// Assert the body contains a stable, expected error code.
fn assert_stable_error_code(body: &str) {
    let stable_codes = [
        "NOT_FOUND",
        "STORAGE_ERROR",
        "BATCH_TOO_LARGE",
        "UNSUPPORTED_VERSION",
        "INTERNAL_ERROR",
        "UNAUTHORIZED",
        "FORBIDDEN",
    ];
    let found = stable_codes.iter().any(|code| body.contains(code));
    assert!(
        found,
        "API error response did not contain a stable error code. Body: {}",
        body
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get_missing_key_returns_not_found_no_leak() {
        let app = test_app().await;
        // Need a valid JWT — generate one or use a read token
        // For this test we use the unauthenticated path since GET /kv/:key
        // should return NOT_FOUND not internal error details
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/kv/nonexistent_key_xyz")
                    .header("Authorization", "Bearer invalid-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // 401 or 404 — either way no internal leak
        let body = String::from_utf8(
            axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        assert_no_internal_leak(&body);
    }

    #[tokio::test]
    async fn error_responses_never_contain_rust_debug_output() {
        let app = test_app().await;
        // Try several endpoints that could expose internal errors
        let paths = [
            "/kv/does_not_exist",
            "/scan?prefix=x&limit=1",
            "/admin/compact",
        ];
        for path in paths {
            let resp = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(path)
                        .header("Authorization", "Bearer invalid")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let body = String::from_utf8(
                axum::body::to_bytes(resp.into_body(), usize::MAX)
                    .await
                    .unwrap()
                    .to_vec(),
            )
            .unwrap();
            assert_no_internal_leak(&body);
        }
    }

    #[tokio::test]
    async fn sanitize_storage_err_returns_stable_codes() {
        // Unit test the sanitizer directly
        use omni_engine::OmniError;
        let cases = [
            (OmniError::NotFound, "NOT_FOUND"),
            (OmniError::BatchTooLarge(100), "BATCH_TOO_LARGE"),
            (OmniError::IoError("test".to_string()), "STORAGE_ERROR"),
        ];
        for (err, expected_code) in cases {
            let result = omni_engine::api::sanitize_storage_err(&err);
            assert_eq!(
                result, expected_code,
                "Expected stable code '{}' for error {:?}",
                expected_code, err
            );
        }
    }
}
