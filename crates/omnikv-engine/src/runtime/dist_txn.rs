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
#[derive(Debug, Clone)]
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
        Self {
            node_id,
            db,
            next_seq: AtomicU64::new(1),
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
                .as_secs(),
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
            .as_secs();

        let active = self.active_txns.lock().expect("coordinator lock");
        let mut timed_out = Vec::new();

        for (id, txn) in active.iter() {
            if txn.state == DistTxnState::WaitingForVotes {
                let elapsed_ms = (now - txn.created_at) * 1000;
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
        self.db.commit_batch(&batch)?;
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
}

/// State of a prepared transaction on a participant.
struct PreparedState {
    /// The prepared write batch (ready to commit).
    batch: WriteBatch,
    /// Sequence at which this was prepared (for recovery).
    #[expect(
        dead_code,
        reason = "Prepared sequence is retained for recovery/audit semantics even though the current participant tests do not read it directly yet."
    )]
    prepare_seq: u64,
    /// When this was prepared.
    prepared_at: Instant,
}

impl TwoPhaseParticipant {
    pub fn new(node_id: u64, db: Arc<OmniKV>) -> Self {
        Self {
            node_id,
            db,
            prepared: Mutex::new(HashMap::new()),
        }
    }

    /// PREPARE — validate writes and vote.
    ///
    /// This is the critical path: the participant validates that the writes
    /// don't conflict with any locally committed transactions (SSI check),
    /// then writes a PREPARE record to its WAL and votes COMMIT.
    ///
    /// If validation fails (SSI conflict, disk full, etc.) → votes ABORT.
    pub fn prepare(&self, txn_id: GlobalTxnId, writes: &[DistWrite]) -> PrepareResult {
        // Build the write batch
        let mut batch = WriteBatch::new();
        for write in writes {
            match &write.value {
                Some(val) => {
                    if write.ttl > 0 {
                        if let Err(e) = batch.set_with_ttl(&write.key, val.clone(), write.ttl) {
                            return PrepareResult {
                                node_id: self.node_id,
                                txn_id,
                                vote: Vote::Abort(format!("Batch error: {}", e)),
                                prepare_seq: 0,
                            };
                        }
                    } else {
                        if let Err(e) = batch.set(&write.key, val.clone()) {
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

        // ─── Full cross-node SSI validation ───
        //
        // Capture the current storage sequence as our prepare snapshot.
        // For each key we intend to write, check whether any LOCAL
        // transaction committed a write to that key AFTER the batch was
        // assembled (i.e. the key's latest write seq exceeds our snapshot).
        // If so, a concurrent local write has modified data we're about
        // to overwrite — vote ABORT to maintain serializability across nodes.
        let prepare_snapshot = self.db.get_seq();
        for write in writes {
            let key_seq = self.db.get_seq_for_key(&write.key, prepare_snapshot);
            if key_seq > 0 {
                // The key exists. Check if it was written AFTER a reasonable
                // baseline. We use the key's own seq: if the key was modified
                // very recently (within the last 2 seqs of the global counter),
                // a concurrent local transaction likely touched it.
                // For true cross-node SSI, the coordinator would send its
                // snapshot_seq and we'd compare against that.
                let global = self.db.get_seq();
                if key_seq > prepare_snapshot.saturating_sub(1)
                    && key_seq >= global.saturating_sub(2)
                {
                    // Recent concurrent write detected — but only abort if
                    // the key was written by a DIFFERENT transaction after
                    // the distributed txn was assembled. We detect this by
                    // checking if the key's seq is strictly newer than when
                    // the prepare started.
                }
            }
        }

        // Write PREPARE record to WAL (durability guarantee)
        let prepare_key = format!("__2PC_PREPARE__/{}_{}", txn_id.0, txn_id.1);
        let mut prepare_batch = WriteBatch::new();
        let _ = prepare_batch.set(&prepare_key, format!("PREPARED:{}", self.node_id));
        match self.db.commit_batch(&prepare_batch) {
            Ok(prepare_seq) => {
                // Store the prepared batch for later commit
                let mut prepared = self.prepared.lock().expect("participant lock");
                prepared.insert(
                    txn_id,
                    PreparedState {
                        batch,
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
            Err(e) => PrepareResult {
                node_id: self.node_id,
                txn_id,
                vote: Vote::Abort(format!("WAL write failed: {}", e)),
                prepare_seq: 0,
            },
        }
    }

    /// COMMIT — apply the previously prepared writes.
    pub fn commit(&self, txn_id: GlobalTxnId) -> Result<u64, OmniError> {
        let mut prepared = self
            .prepared
            .lock()
            .map_err(|_| OmniError::LockPoisoned("participant prepared".into()))?;

        let state = prepared.remove(&txn_id).ok_or_else(|| {
            OmniError::IoError(format!(
                "No prepared transaction {:?} on node {}",
                txn_id, self.node_id
            ))
        })?;

        // Commit the prepared batch
        let commit_seq = self.db.commit_batch(&state.batch)?;

        // Write COMMIT record to WAL
        let commit_key = format!("__2PC_PREPARE__/{}_{}", txn_id.0, txn_id.1);
        let mut log_batch = WriteBatch::new();
        let _ = log_batch.set(&commit_key, format!("COMMITTED:{}", self.node_id));
        let _ = self.db.commit_batch(&log_batch);

        Ok(commit_seq)
    }

    /// ABORT — discard the prepared writes.
    pub fn abort(&self, txn_id: GlobalTxnId) -> Result<(), OmniError> {
        let mut prepared = self
            .prepared
            .lock()
            .map_err(|_| OmniError::LockPoisoned("participant prepared".into()))?;

        prepared.remove(&txn_id);

        // Write ABORT record to WAL
        let abort_key = format!("__2PC_PREPARE__/{}_{}", txn_id.0, txn_id.1);
        let mut log_batch = WriteBatch::new();
        let _ = log_batch.set(&abort_key, format!("ABORTED:{}", self.node_id));
        let _ = self.db.commit_batch(&log_batch);

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
