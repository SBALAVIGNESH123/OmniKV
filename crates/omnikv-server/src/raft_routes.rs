//! Raft RPC HTTP Handlers
//!
//! These routes handle the Raft consensus protocol RPCs:
//! - POST /raft/append  — AppendEntries RPC
//! - POST /raft/vote    — RequestVote RPC
//! - POST /raft/snapshot — InstallSnapshot RPC
//!
//! Each handler deserializes the request, forwards it to the local Raft node,
//! and returns the serialized response.

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;

use omni_engine::raft_impl::{OmniRaft, TypeConfig};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use std::sync::Arc;

/// Shared state for Raft RPC routes.
#[derive(Clone)]
pub struct RaftState {
    pub raft: Arc<OmniRaft>,
    pub auth_token: String,
}

pub fn build_raft_router(state: RaftState) -> axum::Router {
    axum::Router::new()
        .route("/raft/append", axum::routing::post(raft_append_handler))
        .route("/raft/vote", axum::routing::post(raft_vote_handler))
        .route("/raft/snapshot", axum::routing::post(raft_snapshot_handler))
        .with_state(state)
}

async fn raft_append_handler(
    State(state): State<RaftState>,
    headers: HeaderMap,
    Json(req): Json<AppendEntriesRequest<TypeConfig>>,
) -> impl IntoResponse {
    if let Err(response) = authorize_peer(&headers, &state.auth_token) {
        return response;
    }
    match state.raft.append_entries(req).await {
        Ok(resp) => Json(resp).into_response(),
        Err(e) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("append_entries error: {}", e),
        )
            .into_response(),
    }
}

async fn raft_vote_handler(
    State(state): State<RaftState>,
    headers: HeaderMap,
    Json(req): Json<VoteRequest<u64>>,
) -> impl IntoResponse {
    if let Err(response) = authorize_peer(&headers, &state.auth_token) {
        return response;
    }
    match state.raft.vote(req).await {
        Ok(resp) => Json(resp).into_response(),
        Err(e) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("vote error: {}", e),
        )
            .into_response(),
    }
}

async fn raft_snapshot_handler(
    State(state): State<RaftState>,
    headers: HeaderMap,
    Json(req): Json<InstallSnapshotRequest<TypeConfig>>,
) -> impl IntoResponse {
    if let Err(response) = authorize_peer(&headers, &state.auth_token) {
        return response;
    }
    match state.raft.install_snapshot(req).await {
        Ok(resp) => Json(resp).into_response(),
        Err(e) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("install_snapshot error: {}", e),
        )
            .into_response(),
    }
}

#[expect(
    clippy::result_large_err,
    reason = "The helper feeds Axum handlers directly; boxing would add an allocation to every rejected peer RPC without changing the response contract."
)]
fn authorize_peer(headers: &HeaderMap, expected: &str) -> Result<(), axum::response::Response> {
    let Some(provided) = headers
        .get("x-omnikv-raft-auth")
        .and_then(|value| value.to_str().ok())
    else {
        return Err((StatusCode::UNAUTHORIZED, "missing raft peer credentials").into_response());
    };
    if !crate::auth::validate_api_key(provided, expected) {
        return Err((StatusCode::FORBIDDEN, "invalid raft peer credentials").into_response());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_auth_rejects_missing_and_wrong_credentials() {
        let empty = HeaderMap::new();
        assert_eq!(
            authorize_peer(&empty, "shared-peer-secret")
                .unwrap_err()
                .status(),
            StatusCode::UNAUTHORIZED
        );

        let mut wrong = HeaderMap::new();
        wrong.insert("x-omnikv-raft-auth", "wrong-secret".parse().unwrap());
        assert_eq!(
            authorize_peer(&wrong, "shared-peer-secret")
                .unwrap_err()
                .status(),
            StatusCode::FORBIDDEN
        );
    }

    #[test]
    fn peer_auth_accepts_the_configured_shared_secret() {
        let mut headers = HeaderMap::new();
        headers.insert("x-omnikv-raft-auth", "shared-peer-secret".parse().unwrap());
        assert!(authorize_peer(&headers, "shared-peer-secret").is_ok());
    }
}
