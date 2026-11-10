//! Two-Phase Commit (2PC) Distributed Transaction Coordinator
//!
//! Extends OmniKV's single-node SSI transactions to span multiple nodes
//! in the Raft cluster. Guarantees atomic commit across all participants.
//!
//! ## How 2PC Works
//!
//! ```text
//!  Coordinator                  Participant A          Participant B
//!      |                              |                       |
//!      |--- PREPARE (txn, writes) --->|                       |
//!      |--- PREPARE (txn, writes) --------------------------->|
//!      |                              |                       |
//!      |<-- VOTE_COMMIT --------------|                       |
//!      |<-- VOTE_COMMIT --------------------------------------|
//!      |                              |                       |
//!      |  (all voted COMMIT)          |                       |
//!      |                              |                       |
//!      |--- COMMIT ------------------>|                       |
//!      |--- COMMIT ------------------------------------------>|
//!      |                              |                       |
//!      |<-- ACK ----------------------|                       |
//!      |<-- ACK ------------------------------------------- --|
//! ```
//!
//! ## Recovery Guarantees
//!
//! - If coordinator crashes after writing PREPARE to its log → abort on recovery
//! - If coordinator crashes after writing COMMIT to its log → re-send commits
//! - If participant crashes after voting COMMIT → it MUST commit on recovery
//! - Participants write PREPARE/COMMIT records to WAL before responding
//!
//! ## Integration with SSI
//!
//! Each participant runs its own SSI conflict detection during PREPARE.
//! If any participant detects a conflict → VOTE_ABORT → entire txn aborts.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::{OmniError, OmniKV, WriteBatch};

/// Global transaction ID for distributed transactions.
/// Format: (coordinator_node_id, local_txn_sequence)
pub type GlobalTxnId = (u64, u64);

/// State of a distributed transaction.
#[derive(Debug, Clone, PartialEq)]
pub enum DistTxnState {
    /// Transaction is being assembled (writes buffered).
    Preparing,
    /// PREPARE messages sent to all participants, awaiting votes.
    WaitingForVotes,
    /// All participants voted COMMIT — sending COMMIT messages.
    Committing,
    /// Transaction successfully committed on all participants.
    Committed,
    /// Transaction aborted (at least one participant voted ABORT or timeout).
    Aborted,
    /// Unknown state (used during recovery).
    InDoubt,
}

/// A write operation targeted at a specific node.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DistWrite {
    /// Target node ID.
    pub node_id: u64,
    /// Key to write.
    pub key: String,
    /// Value (None = delete).
    pub value: Option<String>,
    /// TTL in seconds (0 = no expiry).
    pub ttl: u64,
}

/// The coordinator's view of a distributed transaction.
#[derive(Debug, Clone)]
pub struct DistTransaction {
    /// Global transaction ID.
    pub id: GlobalTxnId,
    /// Current state.
    pub state: DistTxnState,
    /// Writes grouped by participant node.
    pub writes_by_node: HashMap<u64, Vec<DistWrite>>,
    /// Votes received from participants.
    pub votes: HashMap<u64, Vote>,
    /// When the transaction was created.
    pub created_at: u64,
    /// Timeout for vote collection (milliseconds).
    pub timeout_ms: u64,
}

/// A participant's vote in the 2PC protocol.
#[derive(Debug, Clone, PartialEq)]
pub enum Vote {
    /// Participant can commit — it has acquired locks and written PREPARE to WAL.
    Commit,
    /// Participant cannot commit — SSI conflict or resource unavailable.
    Abort(String),
}

/// Result of a PREPARE request on a participant.
#[derive(Debug, Clone)]
pub struct PrepareResult {
    pub node_id: u64,
    pub txn_id: GlobalTxnId,
    pub vote: Vote,
    /// The participant's prepare sequence (for recovery).
    pub prepare_seq: u64,
}

/// Recovery log entry for 2PC coordinator.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TwoPhaseLogEntry {
    pub txn_id: (u64, u64),
    pub state: String, // "PREPARE", "COMMIT", "ABORT"
    pub participants: Vec<u64>,
    pub timestamp: u64,
}

/// ═══════════════════════════════════════════════════════════════════════
/// 2PC COORDINATOR
/// ═══════════════════════════════════════════════════════════════════════
///
/// The coordinator lives on the node that initiated the distributed
/// transaction. It drives the 2PC protocol and handles recovery.
pub struct TwoPhaseCoordinator {
    /// This node's ID.
    node_id: u64,
    /// Local OmniKV instance (used for coordinator log storage).
    db: Arc<OmniKV>,
    /// Next local transaction sequence.
    next_seq: AtomicU64,
    /// Active distributed transactions.
    active_txns: Mutex<HashMap<GlobalTxnId, DistTransaction>>,
    /// Vote collection timeout (milliseconds).
    default_timeout_ms: u64,
}

impl TwoPhaseCoordinator {
    /// Creates a new 2PC coordinator.
    pub fn new(node_id: u64, db: Arc<OmniKV>, timeout_ms: u64) -> Self {
        let next_seq = db
            .scan("__2PC_LOG__/", "__2PC_LOG__/~", db.get_seq())
            .ok()
            .into_iter()
            .flatten()
            .filter_map(|(key, _)| {
                let txn = key.strip_prefix("__2PC_LOG__/")?.split('/').next()?;
                let (coordinator, sequence) = txn.split_once('_')?;
                let coordinator = coordinator.parse::<u64>().ok()?;
                let sequence = sequence.parse::<u64>().ok()?;
                (coordinator == node_id).then_some(sequence)
            })
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        Self {
            node_id,
            db,
            next_seq: AtomicU64::new(next_seq),
            active_txns: Mutex::new(HashMap::new()),
            default_timeout_ms: timeout_ms,
        }
    }

    /// BEGIN DISTRIBUTED — creates a new distributed transaction.
    pub fn begin(&self) -> GlobalTxnId {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        let txn_id = (self.node_id, seq);

        let txn = DistTransaction {
            id: txn_id,
            state: DistTxnState::Preparing,
            writes_by_node: HashMap::new(),
            votes: HashMap::new(),
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            timeout_ms: self.default_timeout_ms,
        };

        let mut active = self.active_txns.lock().expect("coordinator lock");
        active.insert(txn_id, txn);
        txn_id
    }

    /// Buffer a write for a specific participant node.
    pub fn add_write(
        &self,
        txn_id: GlobalTxnId,
        node_id: u64,
        key: String,
        value: Option<String>,
        ttl: u64,
    ) -> Result<(), OmniError> {
        let mut active = self
            .active_txns
            .lock()
            .map_err(|_| OmniError::LockPoisoned("coordinator".into()))?;

        let txn = active
            .get_mut(&txn_id)
            .ok_or_else(|| OmniError::IoError("Transaction not found".into()))?;

        if txn.state != DistTxnState::Preparing {
            return Err(OmniError::IoError(
                "Transaction is not in PREPARING state".into(),
            ));
        }

        let write = DistWrite {
            node_id,
            key,
            value,
            ttl,
        };
        txn.writes_by_node.entry(node_id).or_default().push(write);
        Ok(())
    }

    /// PREPARE PHASE — sends prepare requests to all participants.
    /// Returns the list of participant node IDs that need to be contacted.
    pub fn prepare(&self, txn_id: GlobalTxnId) -> Result<Vec<u64>, OmniError> {
        let mut active = self
            .active_txns
            .lock()
            .map_err(|_| OmniError::LockPoisoned("coordinator".into()))?;

        let txn = active
            .get_mut(&txn_id)
            .ok_or_else(|| OmniError::IoError("Transaction not found".into()))?;

        if txn.state != DistTxnState::Preparing {
            return Err(OmniError::IoError(
                "Transaction is not in PREPARING state".into(),
            ));
        }

        // Write PREPARE record to coordinator's WAL
        self.log_state(
            txn_id,
            "PREPARE",
            &txn.writes_by_node.keys().copied().collect::<Vec<_>>(),
        )?;

        txn.state = DistTxnState::WaitingForVotes;

        Ok(txn.writes_by_node.keys().copied().collect())
    }

    /// Record a participant's vote.
    pub fn receive_vote(
        &self,
        txn_id: GlobalTxnId,
        result: PrepareResult,
    ) -> Result<DistTxnState, OmniError> {
        let mut active = self
            .active_txns
            .lock()
            .map_err(|_| OmniError::LockPoisoned("coordinator".into()))?;

        let txn = active
            .get_mut(&txn_id)
            .ok_or_else(|| OmniError::IoError("Transaction not found".into()))?;

        if txn.state != DistTxnState::WaitingForVotes {
            return Err(OmniError::IoError("Not waiting for votes".into()));
        }

        txn.votes.insert(result.node_id, result.vote);

        // Check if all votes are in
        let all_participants: HashSet<u64> = txn.writes_by_node.keys().copied().collect();
        let voted: HashSet<u64> = txn.votes.keys().copied().collect();

        if voted == all_participants {
            // All votes received — decide
            let all_commit = txn.votes.values().all(|v| *v == Vote::Commit);

            if all_commit {
                txn.state = DistTxnState::Committing;
                let participants: Vec<u64> = all_participants.into_iter().collect();
                drop(active); // release lock before WAL write
                self.log_state(txn_id, "COMMIT", &participants)?;
                return Ok(DistTxnState::Committing);
            } else {
                txn.state = DistTxnState::Aborted;
                let abort_reasons: Vec<String> = txn
                    .votes
                    .values()
                    .filter_map(|v| match v {
                        Vote::Abort(reason) => Some(reason.clone()),
                        _ => None,
                    })
                    .collect();
                let participants: Vec<u64> = all_participants.into_iter().collect();
                drop(active);
                self.log_state(txn_id, "ABORT", &participants)?;
                return Err(OmniError::IoError(format!(
                    "Distributed transaction aborted: {}",
                    abort_reasons.join("; ")
                )));
            }
        }

        Ok(DistTxnState::WaitingForVotes)
    }

    /// COMMIT PHASE — marks the transaction as committed after all ACKs.
    pub fn finalize_commit(&self, txn_id: GlobalTxnId) -> Result<(), OmniError> {
        let mut active = self
            .active_txns
            .lock()
            .map_err(|_| OmniError::LockPoisoned("coordinator".into()))?;

        if let Some(txn) = active.get_mut(&txn_id) {
            txn.state = DistTxnState::Committed;
            // Remove from active set
            active.remove(&txn_id);
        }
        Ok(())
    }

    /// ABORT — aborts a transaction at any stage.
    pub fn abort(&self, txn_id: GlobalTxnId) -> Result<Vec<u64>, OmniError> {
        let mut active = self
            .active_txns
            .lock()
            .map_err(|_| OmniError::LockPoisoned("coordinator".into()))?;

        let txn = active
            .get_mut(&txn_id)
            .ok_or_else(|| OmniError::IoError("Transaction not found".into()))?;

        let participants: Vec<u64> = txn.writes_by_node.keys().copied().collect();
        txn.state = DistTxnState::Aborted;

        drop(active);
        self.log_state(txn_id, "ABORT", &participants)?;

        let mut active = self
            .active_txns
            .lock()
            .map_err(|_| OmniError::LockPoisoned("coordinator".into()))?;
        active.remove(&txn_id);

        Ok(participants)
    }

    /// Check for timed-out transactions.
    pub fn check_timeouts(&self) -> Vec<GlobalTxnId> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let active = self.active_txns.lock().expect("coordinator lock");
        let mut timed_out = Vec::new();

        for (id, txn) in active.iter() {
            if txn.state == DistTxnState::WaitingForVotes {
                let elapsed_ms = now.saturating_sub(txn.created_at);
                if elapsed_ms > txn.timeout_ms {
                    timed_out.push(*id);
                }
            }
        }
        timed_out
    }

    /// Get the state of a distributed transaction.
    pub fn get_state(&self, txn_id: GlobalTxnId) -> Option<DistTxnState> {
        let active = self.active_txns.lock().ok()?;
        active.get(&txn_id).map(|t| t.state.clone())
    }

    /// Get the writes for a specific participant.
    pub fn get_participant_writes(
        &self,
        txn_id: GlobalTxnId,
        node_id: u64,
    ) -> Option<Vec<DistWrite>> {
        let active = self.active_txns.lock().ok()?;
        active
            .get(&txn_id)
            .and_then(|t| t.writes_by_node.get(&node_id))
            .cloned()
    }

    /// Returns the number of active distributed transactions.
    pub fn active_count(&self) -> usize {
        self.active_txns.lock().map(|a| a.len()).unwrap_or(0)
    }

    /// Recover in-doubt distributed transactions after a coordinator crash.
    ///
    /// Scans the WAL for 2PC log entries and identifies transactions that
    /// were PREPARED but never reached a final COMMIT or ABORT state.
    /// Returns a list of `(GlobalTxnId, state, participants)` tuples:
    /// - `"PREPARE"` → transaction is in-doubt, needs resolution
    /// - `"COMMIT"` → commit was decided but may not have been delivered
    ///
    /// The caller should:
    /// - For PREPARE-only records: decide ABORT (safe default) or re-query participants.
    /// - For COMMIT records: re-send COMMIT to all participants.
    pub fn recover(&self) -> Result<Vec<(GlobalTxnId, String, Vec<u64>)>, OmniError> {
        let seq = self.db.get_seq();
        let prefix = "__2PC_LOG__/";
        let end = "__2PC_LOG__/~"; // '~' is after all alphanumerics in ASCII

        let entries = self.db.scan(prefix, end, seq)?;

        // Group log entries by txn_id
        let mut txn_states: HashMap<GlobalTxnId, HashMap<String, TwoPhaseLogEntry>> =
            HashMap::new();

        for (key, value) in &entries {
            if let Ok(entry) = serde_json::from_str::<TwoPhaseLogEntry>(value) {
                txn_states
                    .entry(entry.txn_id)
                    .or_default()
                    .insert(entry.state.clone(), entry);
            }
        }

        let mut in_doubt = Vec::new();

        for (txn_id, states) in &txn_states {
            if states.contains_key("COMMIT") {
                // COMMIT was decided — need to re-deliver to participants
                if let Some(entry) = states.get("COMMIT") {
                    in_doubt.push((*txn_id, "COMMIT".to_string(), entry.participants.clone()));
                }
            } else if states.contains_key("PREPARE") && !states.contains_key("ABORT") {
                // PREPARE but no decision — in-doubt, default to ABORT
                if let Some(entry) = states.get("PREPARE") {
                    in_doubt.push((*txn_id, "PREPARE".to_string(), entry.participants.clone()));
                }
            }
            // ABORT records are already resolved — skip
        }

        Ok(in_doubt)
    }

    /// Write a 2PC state change to the coordinator's recovery log.
    fn log_state(
        &self,
        txn_id: GlobalTxnId,
        state: &str,
        participants: &[u64],
    ) -> Result<(), OmniError> {
        let entry = TwoPhaseLogEntry {
            txn_id,
            state: state.to_string(),
            participants: participants.to_vec(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };

        let log_key = format!("__2PC_LOG__/{}_{}/{}", txn_id.0, txn_id.1, state);
        let log_value = serde_json::to_string(&entry)
            .map_err(|e| OmniError::IoError(format!("2PC log serialize: {}", e)))?;

        let mut batch = WriteBatch::new();
        batch.set(&log_key, log_value)?;
        self.db.commit_2pc_batch(&batch)?;
        Ok(())
    }
}

/// ═══════════════════════════════════════════════════════════════════════
/// 2PC PARTICIPANT
/// ═══════════════════════════════════════════════════════════════════════
///
/// The participant runs on each node that holds data involved in the
/// distributed transaction. It validates writes using local SSI and
/// votes COMMIT or ABORT.
pub struct TwoPhaseParticipant {
    /// This node's ID.
    node_id: u64,
    /// Local OmniKV instance.
    db: Arc<OmniKV>,
    /// Prepared transactions awaiting COMMIT/ABORT decision.
    prepared: Mutex<HashMap<GlobalTxnId, PreparedState>>,
    /// In-process key reservations prevent two concurrent prepares from
    /// voting COMMIT for the same key. Durable prepared records are rebuilt
    /// into this map when recovery is requested.
    key_locks: Mutex<HashMap<String, GlobalTxnId>>,
}

/// State of a prepared transaction on a participant.
struct PreparedState {
    /// The prepared write batch (ready to commit).
    batch: WriteBatch,
    /// Original writes retained so the prepared record can be reconstructed
    /// after a process restart.
    writes: Vec<DistWrite>,
    /// Sequence at which this was prepared (for recovery).
    prepare_seq: u64,
    /// When this was prepared.
    prepared_at: Instant,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ParticipantLogEntry {
    txn_id: GlobalTxnId,
    node_id: u64,
    state: String,
    writes: Vec<DistWrite>,
    prepare_seq: u64,
    commit_seq: Option<u64>,
}

fn participant_prepare_key(node_id: u64, txn_id: GlobalTxnId) -> String {
    format!("__2PC_PREPARE__/{node_id}/{}_{}", txn_id.0, txn_id.1)
}

fn legacy_participant_prepare_key(txn_id: GlobalTxnId) -> String {
    format!("__2PC_PREPARE__/{}_{}", txn_id.0, txn_id.1)
}

fn build_write_batch(writes: &[DistWrite]) -> Result<WriteBatch, String> {
    let mut batch = WriteBatch::new();
    for write in writes {
        match &write.value {
            Some(value) if write.ttl > 0 => batch
                .set_with_ttl(&write.key, value.clone(), write.ttl)
                .map_err(|error| error.to_string())?,
            Some(value) => batch
                .set(&write.key, value.clone())
                .map_err(|error| error.to_string())?,
            None => batch
                .delete(&write.key)
                .map_err(|error| error.to_string())?,
        }
    }
    crate::raft_command::RaftCommand::from_batch(&batch).protects_system_keys()?;
    Ok(batch)
}

impl TwoPhaseParticipant {
    pub fn new(node_id: u64, db: Arc<OmniKV>) -> Self {
        let participant = Self {
            node_id,
            db,
            prepared: Mutex::new(HashMap::new()),
            key_locks: Mutex::new(HashMap::new()),
        };
        if let Err(error) = participant.recover() {
            tracing::error!(node_id, ?error, "2PC participant recovery failed");
        }
        participant
    }

    /// PREPARE — validate writes and vote.
    ///
    /// This is the critical path: the participant validates that the writes
    /// don't conflict with any locally committed transactions (SSI check),
    /// then writes a PREPARE record to its WAL and votes COMMIT.
    ///
    /// If validation fails (SSI conflict, disk full, etc.) → votes ABORT.
    pub fn prepare(&self, txn_id: GlobalTxnId, writes: &[DistWrite]) -> PrepareResult {
        let read_seq = self.db.get_seq().saturating_sub(1);
        self.prepare_with_snapshot(txn_id, writes, read_seq)
    }

    /// PREPARE with an explicit coordinator snapshot. This is the SSI-aware
    /// entry point; the compatibility `prepare` method uses the participant's
    /// current committed point.
    pub fn prepare_with_snapshot(
        &self,
        txn_id: GlobalTxnId,
        writes: &[DistWrite],
        read_seq: u64,
    ) -> PrepareResult {
        let result = (|| -> Result<PrepareResult, OmniError> {
            let lock = self
                .db
                .sql_table_lock(&format!("\x002PC\x00{}", self.node_id))?;
            let _guard = lock
                .lock()
                .map_err(|_| OmniError::LockPoisoned("2PC participant".into()))?;
            self.recover_locked()?;
            if let Some(existing) = self.load_participant_log(txn_id)? {
                if existing.writes != writes && existing.state != "ABORTED" {
                    return Err(OmniError::IoError(
                        "transaction ID was reused with different writes".into(),
                    ));
                }
                return Ok(match existing.state.as_str() {
                    "PREPARED" | "COMMITTED" => PrepareResult {
                        node_id: self.node_id,
                        txn_id,
                        vote: Vote::Commit,
                        prepare_seq: existing.prepare_seq,
                    },
                    "ABORTED" => PrepareResult {
                        node_id: self.node_id,
                        txn_id,
                        vote: Vote::Abort("transaction was already aborted".into()),
                        prepare_seq: existing.prepare_seq,
                    },
                    _ => PrepareResult {
                        node_id: self.node_id,
                        txn_id,
                        vote: Vote::Abort("unknown participant recovery state".into()),
                        prepare_seq: 0,
                    },
                });
            }
            build_write_batch(writes).map_err(OmniError::IoError)?;
            Ok(self.prepare_locked(txn_id, writes, read_seq))
        })();
        result.unwrap_or_else(|error| PrepareResult {
            node_id: self.node_id,
            txn_id,
            vote: Vote::Abort(error.to_string()),
            prepare_seq: 0,
        })
    }

    fn prepare_locked(
        &self,
        txn_id: GlobalTxnId,
        writes: &[DistWrite],
        read_seq: u64,
    ) -> PrepareResult {
        let write_keys: Vec<String> = writes.iter().map(|write| write.key.clone()).collect();
        if let Some(reason) = self
            .db
            .ssi_history
            .check_write_conflicts(read_seq, &write_keys)
        {
            return PrepareResult {
                node_id: self.node_id,
                txn_id,
                vote: Vote::Abort(reason),
                prepare_seq: 0,
            };
        }

        {
            let mut key_locks = match self.key_locks.lock() {
                Ok(locks) => locks,
                Err(_) => {
                    return PrepareResult {
                        node_id: self.node_id,
                        txn_id,
                        vote: Vote::Abort("participant key-lock registry poisoned".into()),
                        prepare_seq: 0,
                    };
                }
            };
            if let Some(conflict) = write_keys.iter().find_map(|key| {
                key_locks
                    .get(key)
                    .copied()
                    .filter(|owner| *owner != txn_id)
                    .map(|owner| (key, owner))
            }) {
                return PrepareResult {
                    node_id: self.node_id,
                    txn_id,
                    vote: Vote::Abort(format!(
                        "prepared key '{}' is reserved by transaction {:?}",
                        conflict.0, conflict.1
                    )),
                    prepare_seq: 0,
                };
            }
            for key in &write_keys {
                key_locks.insert(key.clone(), txn_id);
            }
        }

        // Build the write batch
        let mut batch = WriteBatch::new();
        for write in writes {
            match &write.value {
                Some(val) => {
                    if write.ttl > 0 {
                        if let Err(e) = batch.set_with_ttl(&write.key, val.clone(), write.ttl) {
                            self.release_key_locks(txn_id, &write_keys);
                            return PrepareResult {
                                node_id: self.node_id,
                                txn_id,
                                vote: Vote::Abort(format!("Batch error: {}", e)),
                                prepare_seq: 0,
                            };
                        }
                    } else {
                        if let Err(e) = batch.set(&write.key, val.clone()) {
                            self.release_key_locks(txn_id, &write_keys);
                            return PrepareResult {
                                node_id: self.node_id,
                                txn_id,
                                vote: Vote::Abort(format!("Batch error: {}", e)),
                                prepare_seq: 0,
                            };
                        }
                    }
                }
                None => {
                    if let Err(e) = batch.delete(&write.key) {
                        self.release_key_locks(txn_id, &write_keys);
                        return PrepareResult {
                            node_id: self.node_id,
                            txn_id,
                            vote: Vote::Abort(format!("Delete error: {}", e)),
                            prepare_seq: 0,
                        };
                    }
                }
            }
        }

        // Write the complete prepared record durably before voting COMMIT.
        let prepare_key = participant_prepare_key(self.node_id, txn_id);
        let log_entry = ParticipantLogEntry {
            txn_id,
            node_id: self.node_id,
            state: "PREPARED".into(),
            writes: writes.to_vec(),
            prepare_seq: 0,
            commit_seq: None,
        };
        let mut prepare_batch = WriteBatch::new();
        let encoded = match serde_json::to_string(&log_entry) {
            Ok(encoded) => encoded,
            Err(error) => {
                self.release_key_locks(txn_id, &write_keys);
                return PrepareResult {
                    node_id: self.node_id,
                    txn_id,
                    vote: Vote::Abort(format!("prepare log serialization failed: {error}")),
                    prepare_seq: 0,
                };
            }
        };
        if let Err(error) = prepare_batch.set(&prepare_key, encoded) {
            self.release_key_locks(txn_id, &write_keys);
            return PrepareResult {
                node_id: self.node_id,
                txn_id,
                vote: Vote::Abort(format!("prepare log construction failed: {error}")),
                prepare_seq: 0,
            };
        }
        let _ = prepare_batch.set(
            &legacy_participant_prepare_key(txn_id),
            format!("PREPARED:{}", self.node_id),
        );
        match self.db.commit_2pc_batch(&prepare_batch) {
            Ok(prepare_seq) => {
                let persisted = ParticipantLogEntry {
                    txn_id,
                    node_id: self.node_id,
                    state: "PREPARED".into(),
                    writes: writes.to_vec(),
                    prepare_seq,
                    commit_seq: None,
                };
                let mut update = WriteBatch::new();
                if let Ok(encoded) = serde_json::to_string(&persisted)
                    && update.set(&prepare_key, encoded).is_ok()
                {
                    let _ = self.db.commit_2pc_batch(&update);
                }
                // Store the prepared batch for later commit
                let mut prepared = self.prepared.lock().expect("participant lock");
                prepared.insert(
                    txn_id,
                    PreparedState {
                        batch,
                        writes: writes.to_vec(),
                        prepare_seq,
                        prepared_at: Instant::now(),
                    },
                );

                PrepareResult {
                    node_id: self.node_id,
                    txn_id,
                    vote: Vote::Commit,
                    prepare_seq,
                }
            }
            Err(e) => {
                self.release_key_locks(txn_id, &write_keys);
                PrepareResult {
                    node_id: self.node_id,
                    txn_id,
                    vote: Vote::Abort(format!("WAL write failed: {}", e)),
                    prepare_seq: 0,
                }
            }
        }
    }

    fn load_participant_log(
        &self,
        txn_id: GlobalTxnId,
    ) -> Result<Option<ParticipantLogEntry>, OmniError> {
        let key = participant_prepare_key(self.node_id, txn_id);
        let value = self.db.find(&key, self.db.get_seq())?;
        value
            .map(|value| {
                serde_json::from_str(&value)
                    .map_err(|error| OmniError::IoError(format!("2PC participant log: {error}")))
            })
            .transpose()
    }

    fn release_key_locks(&self, txn_id: GlobalTxnId, keys: &[String]) {
        if let Ok(mut locks) = self.key_locks.lock() {
            for key in keys {
                if locks.get(key).is_some_and(|owner| *owner == txn_id) {
                    locks.remove(key);
                }
            }
        }
    }

    /// Rebuilds in-memory prepared batches after a participant restart. The
    /// durable records remain authoritative; this method is safe to call more
    /// than once and leaves committed/aborted records out of the prepared set.
    pub fn recover(&self) -> Result<usize, OmniError> {
        let lock = self
            .db
            .sql_table_lock(&format!("\x002PC\x00{}", self.node_id))?;
        let _guard = lock
            .lock()
            .map_err(|_| OmniError::LockPoisoned("2PC participant".into()))?;
        self.recover_locked()
    }

    fn recover_locked(&self) -> Result<usize, OmniError> {
        let prefix = format!("__2PC_PREPARE__/{}/", self.node_id);
        let entries = self
            .db
            .scan(&prefix, &format!("{prefix}~"), self.db.get_seq())?;
        let mut recovered = 0;
        let mut recovered_prepared = HashMap::new();
        let mut recovered_locks = HashMap::new();
        let mut prepared = self
            .prepared
            .lock()
            .map_err(|_| OmniError::LockPoisoned("participant prepared".into()))?;
        let mut key_locks = self
            .key_locks
            .lock()
            .map_err(|_| OmniError::LockPoisoned("participant key locks".into()))?;
        for (_, value) in entries {
            let entry: ParticipantLogEntry = serde_json::from_str(&value).map_err(|error| {
                OmniError::IoError(format!("invalid prepared transaction log: {error}"))
            })?;
            if entry.node_id != self.node_id || entry.state != "PREPARED" {
                continue;
            }
            let batch = build_write_batch(&entry.writes)
                .map_err(|error| OmniError::IoError(format!("recover prepared batch: {error}")))?;
            for write in &entry.writes {
                if let Some(owner) = recovered_locks.insert(write.key.clone(), entry.txn_id)
                    && owner != entry.txn_id
                {
                    return Err(OmniError::IoError(
                        "conflicting durable prepared reservations".into(),
                    ));
                }
            }
            recovered_prepared.insert(
                entry.txn_id,
                PreparedState {
                    batch,
                    writes: entry.writes,
                    prepare_seq: entry.prepare_seq,
                    prepared_at: Instant::now(),
                },
            );
            recovered += 1;
        }
        *prepared = recovered_prepared;
        *key_locks = recovered_locks;
        Ok(recovered)
    }

    /// COMMIT — apply the previously prepared writes.
    pub fn commit(&self, txn_id: GlobalTxnId) -> Result<u64, OmniError> {
        let lock = self
            .db
            .sql_table_lock(&format!("\x002PC\x00{}", self.node_id))?;
        let _guard = lock
            .lock()
            .map_err(|_| OmniError::LockPoisoned("2PC participant".into()))?;
        let state = self
            .load_participant_log(txn_id)?
            .ok_or_else(|| OmniError::IoError(format!("No prepared transaction {txn_id:?}")))?;
        match state.state.as_str() {
            "COMMITTED" => {
                // A crash can occur after the atomic data+decision batch but
                // before its optional marker annotation. Return a current
                // read point in that case; never replay the committed data.
                return Ok(state.commit_seq.unwrap_or_else(|| self.db.get_seq()));
            }
            "PREPARED" => {}
            _ => {
                return Err(OmniError::IoError(
                    "cannot commit an aborted or invalid transaction".into(),
                ));
            }
        }

        let mut commit_batch = build_write_batch(&state.writes)
            .map_err(|error| OmniError::IoError(format!("commit prepared batch: {error}")))?;
        let log_entry = ParticipantLogEntry {
            txn_id,
            node_id: self.node_id,
            state: "COMMITTED".into(),
            writes: state.writes.clone(),
            prepare_seq: state.prepare_seq,
            commit_seq: None,
        };
        commit_batch.set(
            &participant_prepare_key(self.node_id, txn_id),
            serde_json::to_string(&log_entry)
                .map_err(|error| OmniError::IoError(format!("commit log: {error}")))?,
        )?;
        // Keep the historical marker in sync for existing recovery tooling
        // while the node-qualified record remains the authoritative format.
        commit_batch.set(
            &legacy_participant_prepare_key(txn_id),
            format!("COMMITTED:{}", self.node_id),
        )?;
        let commit_seq = self.db.commit_2pc_batch(&commit_batch)?;
        let mut final_log = WriteBatch::new();
        final_log.set(
            &participant_prepare_key(self.node_id, txn_id),
            serde_json::to_string(&ParticipantLogEntry {
                commit_seq: Some(commit_seq),
                ..log_entry
            })
            .map_err(|error| OmniError::IoError(format!("commit log update: {error}")))?,
        )?;
        // This annotation is advisory: the data and COMMITTED decision above
        // are already durable. Its failure must not turn a committed write
        // into an error or leave its keys reserved indefinitely.
        let _ = self.db.commit_2pc_batch(&final_log);
        self.prepared
            .lock()
            .map_err(|_| OmniError::LockPoisoned("participant prepared".into()))?
            .remove(&txn_id);
        self.release_key_locks(
            txn_id,
            &state
                .writes
                .iter()
                .map(|write| write.key.clone())
                .collect::<Vec<_>>(),
        );

        Ok(commit_seq)
    }

    /// ABORT — discard the prepared writes.
    pub fn abort(&self, txn_id: GlobalTxnId) -> Result<(), OmniError> {
        let lock = self
            .db
            .sql_table_lock(&format!("\x002PC\x00{}", self.node_id))?;
        let _guard = lock
            .lock()
            .map_err(|_| OmniError::LockPoisoned("2PC participant".into()))?;
        let writes = match self.load_participant_log(txn_id)? {
            Some(entry) if entry.state == "COMMITTED" => {
                return Err(OmniError::IoError(
                    "cannot abort a committed transaction".into(),
                ));
            }
            Some(entry) if entry.state == "ABORTED" => return Ok(()),
            Some(entry) if entry.state == "PREPARED" => entry.writes,
            Some(_) => return Err(OmniError::IoError("invalid participant decision".into())),
            None => Vec::new(),
        };

        // Write ABORT record durably.
        let abort_key = participant_prepare_key(self.node_id, txn_id);
        let abort_entry = ParticipantLogEntry {
            txn_id,
            node_id: self.node_id,
            state: "ABORTED".into(),
            writes: writes.clone(),
            prepare_seq: 0,
            commit_seq: None,
        };
        let mut log_batch = WriteBatch::new();
        log_batch.set(
            &abort_key,
            serde_json::to_string(&abort_entry)
                .map_err(|error| OmniError::IoError(format!("abort log: {error}")))?,
        )?;
        let _ = log_batch.set(
            &legacy_participant_prepare_key(txn_id),
            format!("ABORTED:{}", self.node_id),
        );
        self.db.commit_2pc_batch(&log_batch)?;
        self.prepared
            .lock()
            .map_err(|_| OmniError::LockPoisoned("participant prepared".into()))?
            .remove(&txn_id);
        self.release_key_locks(
            txn_id,
            &writes
                .iter()
                .map(|write| write.key.clone())
                .collect::<Vec<_>>(),
        );

        Ok(())
    }

    /// Returns the number of prepared (in-doubt) transactions.
    pub fn prepared_count(&self) -> usize {
        self.prepared.lock().map(|p| p.len()).unwrap_or(0)
    }

    /// Check for stale prepared transactions (for recovery).
    pub fn stale_prepared(&self, max_age: Duration) -> Vec<GlobalTxnId> {
        let prepared = self.prepared.lock().expect("participant lock");
        prepared
            .iter()
            .filter(|(_, state)| state.prepared_at.elapsed() > max_age)
            .map(|(id, _)| *id)
            .collect()
    }
}
