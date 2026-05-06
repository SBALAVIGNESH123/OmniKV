//! Cluster State Management
//!
//! Manages the distributed cluster topology and coordinates
//! write forwarding between leader and follower nodes.

use omni_engine::WriteBatch;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::{mpsc, oneshot};

/// A write request forwarded to the Raft leader.
pub struct WriteRequest {
    pub batch: WriteBatch,
    pub reply_to: oneshot::Sender<Result<u64, String>>,
}

/// Cluster-wide state shared across all async tasks.
pub struct ClusterState {
    pub is_leader: AtomicBool,
    pub leader_id: AtomicU64,
    pub node_id: u64,
    pub peers: Vec<String>,
    pub expected_quorum: usize,
    pub write_tx: mpsc::Sender<WriteRequest>,
}

impl ClusterState {
    pub fn new(node_id: u64, peers: Vec<String>, write_tx: mpsc::Sender<WriteRequest>) -> Self {
        Self {
            is_leader: AtomicBool::new(false),
            leader_id: AtomicU64::new(0),
            node_id,
            expected_quorum: (peers.len() / 2) + 1,
            peers,
            write_tx,
        }
    }

    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::Relaxed)
    }

    pub fn set_leader(&self, leader: u64) {
        self.leader_id.store(leader, Ordering::Relaxed);
        self.is_leader
            .store(leader == self.node_id, Ordering::Relaxed);
    }
}

/// Load the current epoch from a JSON file (or default to 0).
pub fn load_epoch(path: &str) -> u64 {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .and_then(|v| v["epoch"].as_u64())
        .unwrap_or(0)
}
