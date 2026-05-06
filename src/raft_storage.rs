//! Raft Storage Adapter
//!
//! Maps OpenRaft's RaftStorage trait to OmniKV's storage engine.
//! Raft log entries and state machine snapshots are stored in OmniKV itself.

use crate::{OmniKV, WriteBatch};
use std::sync::Arc;
use std::sync::Mutex;

/// Raft log stored in OmniKV with prefix `__raft_log__/`.
const RAFT_LOG_PREFIX: &str = "__raft_log__/";
/// Raft vote stored at this key.
const RAFT_VOTE_KEY: &str = "__raft_vote__";
/// Raft committed index.
const RAFT_COMMITTED_KEY: &str = "__raft_committed__";

/// OmniKV-backed Raft storage.
pub struct OmniRaftStorage {
    db: Arc<OmniKV>,
    /// Last applied log index.
    last_applied: Mutex<u64>,
}

impl OmniRaftStorage {
    pub fn new(db: Arc<OmniKV>) -> Self {
        let last_applied = Self::read_committed(&db);
        Self {
            db,
            last_applied: Mutex::new(last_applied),
        }
    }

    /// Append a Raft log entry.
    pub fn append_log(&self, index: u64, entry: &str) -> Result<u64, String> {
        let key = format!("{}{:020}", RAFT_LOG_PREFIX, index);
        let mut batch = WriteBatch::new();
        batch
            .set(&key, entry.to_string())
            .map_err(|e| format!("Raft log set: {:?}", e))?;
        self.db
            .commit_batch(&batch)
            .map_err(|e| format!("Raft log commit: {:?}", e))
    }

    /// Read a Raft log entry.
    pub fn read_log(&self, index: u64) -> Option<String> {
        let key = format!("{}{:020}", RAFT_LOG_PREFIX, index);
        let seq = self.db.get_seq();
        self.db.find(&key, seq).ok().flatten()
    }

    /// Get log entries in range [start, end).
    pub fn get_log_range(&self, start: u64, end: u64) -> Vec<(u64, String)> {
        let start_key = format!("{}{:020}", RAFT_LOG_PREFIX, start);
        let end_key = format!("{}{:020}", RAFT_LOG_PREFIX, end);
        let seq = self.db.get_seq();
        self.db
            .scan(&start_key, &end_key, seq)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|(k, v)| {
                let idx_str = k.strip_prefix(RAFT_LOG_PREFIX)?;
                let idx = idx_str.parse::<u64>().ok()?;
                Some((idx, v))
            })
            .collect()
    }

    /// Delete log entries from start to end (for log compaction).
    pub fn delete_log_range(&self, start: u64, end: u64) -> Result<(), String> {
        let mut batch = WriteBatch::new();
        for idx in start..end {
            let key = format!("{}{:020}", RAFT_LOG_PREFIX, idx);
            let _ = batch.delete(&key);
        }
        self.db
            .commit_batch(&batch)
            .map_err(|e| format!("Raft log delete: {:?}", e))?;
        Ok(())
    }

    /// Save the current vote.
    pub fn save_vote(&self, vote_json: &str) -> Result<(), String> {
        let mut batch = WriteBatch::new();
        batch
            .set(RAFT_VOTE_KEY, vote_json.to_string())
            .map_err(|e| format!("Vote set: {:?}", e))?;
        self.db
            .commit_batch(&batch)
            .map_err(|e| format!("Vote commit: {:?}", e))?;
        Ok(())
    }

    /// Read the saved vote.
    pub fn read_vote(&self) -> Option<String> {
        let seq = self.db.get_seq();
        self.db.find(RAFT_VOTE_KEY, seq).ok().flatten()
    }

    /// Mark an index as applied (committed).
    pub fn mark_applied(&self, index: u64) -> Result<(), String> {
        let mut last = self.last_applied.lock().map_err(|_| "Lock error")?;
        if index > *last {
            *last = index;
            let mut batch = WriteBatch::new();
            batch
                .set(RAFT_COMMITTED_KEY, index.to_string())
                .map_err(|e| format!("Committed set: {:?}", e))?;
            self.db
                .commit_batch(&batch)
                .map_err(|e| format!("Committed commit: {:?}", e))?;
        }
        Ok(())
    }

    /// Get the last applied index.
    pub fn last_applied_index(&self) -> u64 {
        *self.last_applied.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Read committed index from DB on startup.
    fn read_committed(db: &Arc<OmniKV>) -> u64 {
        let seq = db.get_seq();
        db.find(RAFT_COMMITTED_KEY, seq)
            .ok()
            .flatten()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0)
    }

    /// Apply a client write (the state machine apply).
    pub fn apply_write(&self, data: &str) -> Result<String, String> {
        // Data format: "SET key value" or "DELETE key"
        let parts: Vec<&str> = data.splitn(3, ' ').collect();
        match parts.first().map(|s| s.to_uppercase()).as_deref() {
            Some("SET") if parts.len() == 3 => {
                let mut batch = WriteBatch::new();
                batch
                    .set(parts[1], parts[2].to_string())
                    .map_err(|e| format!("{:?}", e))?;
                let seq = self
                    .db
                    .commit_batch(&batch)
                    .map_err(|e| format!("{:?}", e))?;
                Ok(format!("OK:{}", seq))
            }
            Some("DELETE") if parts.len() >= 2 => {
                let mut batch = WriteBatch::new();
                let _ = batch.delete(parts[1]);
                self.db
                    .commit_batch(&batch)
                    .map_err(|e| format!("{:?}", e))?;
                Ok("DELETED".to_string())
            }
            _ => Err(format!("Unknown raft command: {}", data)),
        }
    }
}
