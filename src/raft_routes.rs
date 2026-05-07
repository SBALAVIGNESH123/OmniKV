//! Raft RPC HTTP Handlers
//!
//! These routes handle the Raft consensus protocol RPCs:
//! - POST /raft/append  — AppendEntries RPC
//! - POST /raft/vote    — RequestVote RPC
//! - POST /raft/snapshot — InstallSnapshot RPC
//!
//! Each handler deserializes the request, forwards it to the local Raft node,
//! and returns the serialized response.

use axum::extract::State;
use axum::response::IntoResponse;
use axum::Json;

use crate::raft_impl::{OmniRaft, TypeConfig};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse,
    InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use std::sync::Arc;

/// Shared state for Raft RPC routes.
#[derive(Clone)]
pub struct RaftState {
    pub raft: Arc<OmniRaft>,
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
    Json(req): Json<AppendEntriesRequest<TypeConfig>>,
) -> impl IntoResponse {
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
    Json(req): Json<VoteRequest<u64>>,
) -> impl IntoResponse {
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
    Json(req): Json<InstallSnapshotRequest<TypeConfig>>,
) -> impl IntoResponse {
    match state.raft.install_snapshot(req).await {
        Ok(resp) => Json(resp).into_response(),
        Err(e) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("install_snapshot error: {}", e),
        )
            .into_response(),
    }
}
