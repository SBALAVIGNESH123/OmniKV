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
        let next_seq = match Self::load_next_seq(node_id, &db) {
            Ok(next_seq) => next_seq,
            Err(error) => {
                tracing::error!(node_id, ?error, "2PC coordinator sequence recovery failed");
                0
            }
        };
        Self {
            node_id,
            db,
            next_seq: AtomicU64::new(next_seq),
            active_txns: Mutex::new(HashMap::new()),
            default_timeout_ms: timeout_ms,
        }
    }

    fn read_persisted_hwm(node_id: u64, db: &OmniKV, seq: u64) -> Result<Option<u64>, OmniError> {
        let hwm_key = format!("__2PC_LOG__/NEXT_SEQ/{node_id}");
        match db.find(&hwm_key, seq)? {
            Some(v) => v.parse::<u64>().map(Some).map_err(|error| {
                OmniError::IoError(format!("invalid 2PC NEXT_SEQ watermark: {error}"))
            }),
            None => Ok(None),
        }
    }

    fn load_next_seq(node_id: u64, db: &OmniKV) -> Result<u64, OmniError> {
        let seq = db.get_seq();
        let persisted_hwm = Self::read_persisted_hwm(node_id, db, seq)?.unwrap_or(0);
        let scan_max = db
            .scan("__2PC_LOG__/", "__2PC_LOG__/~", seq)?
            .into_iter()
            .filter_map(|(key, _)| {
                let txn = key.strip_prefix("__2PC_LOG__/")?.split('/').next()?;
                let (coordinator, sequence) = txn.split_once('_')?;
                let coordinator = coordinator.parse::<u64>().ok()?;
                let sequence = sequence.parse::<u64>().ok()?;
                (coordinator == node_id).then_some(sequence)
            })
            .max()
            .unwrap_or(0);
        persisted_hwm
            .max(scan_max)
            .checked_add(1)
            .ok_or_else(|| OmniError::IoError("2PC transaction sequence exhausted".into()))
    }

    /// BEGIN DISTRIBUTED — creates a new distributed transaction.
    pub fn begin(&self) -> Result<GlobalTxnId, OmniError> {
        let txn_id = {
            let coord_lock = self
                .db
                .sql_table_lock(&format!("\x002PC_COORD\x00{}", self.node_id))?;
            let _coord_guard = coord_lock
                .lock()
                .map_err(|_| OmniError::LockPoisoned("coordinator sequence".into()))?;
            let current = self.next_seq.load(Ordering::SeqCst);
            let durable_seq = if current == 0 {
                Self::load_next_seq(self.node_id, &self.db)?
            } else {
                match Self::read_persisted_hwm(self.node_id, &self.db, self.db.get_seq())? {
                    Some(hwm) => hwm.checked_add(1).ok_or_else(|| {
                        OmniError::IoError("2PC transaction sequence exhausted".into())
                    })?,
                    None => Self::load_next_seq(self.node_id, &self.db)?,
                }
            };
            let seq = current.max(durable_seq);

            let hwm_key = format!("__2PC_LOG__/NEXT_SEQ/{}", self.node_id);
            let mut hwm_batch = WriteBatch::new();
            hwm_batch.set(&hwm_key, seq.to_string())?;
            self.db.commit_2pc_batch(&hwm_batch)?;

            self.next_seq.store(seq.saturating_add(1), Ordering::SeqCst);
            (self.node_id, seq)
        };

        let mut active = self
            .active_txns
            .lock()
            .map_err(|_| OmniError::LockPoisoned("coordinator".into()))?;

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

        active.insert(txn_id, txn);
        Ok(txn_id)
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

    fn has_durable_state(&self, txn_id: GlobalTxnId, state: &str) -> Result<bool, OmniError> {
        let key = format!("__2PC_LOG__/{}_{}/{}", txn_id.0, txn_id.1, state);
        Ok(self.db.find(&key, self.db.get_seq())?.is_some())
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
        if matches!(
            active.get(&txn_id).map(|t| &t.state),
            Some(DistTxnState::InDoubt | DistTxnState::Committing)
        ) {
            self.db.ensure_cluster_linearizable()?;
        }
        let durable_commit = self.has_durable_state(txn_id, "COMMIT")?;
        let durable_abort = self.has_durable_state(txn_id, "ABORT")?;

        let txn = active
            .get_mut(&txn_id)
            .ok_or_else(|| OmniError::IoError("Transaction not found".into()))?;

        if durable_commit {
            txn.state = DistTxnState::Committing;
            return Ok(DistTxnState::Committing);
        }
        if durable_abort {
            txn.state = DistTxnState::Aborted;
            return Err(OmniError::IoError(
                "Distributed transaction was already aborted".into(),
            ));
        }

        if !matches!(
            txn.state,
            DistTxnState::WaitingForVotes | DistTxnState::InDoubt | DistTxnState::Committing
        ) {
            return Err(OmniError::IoError("Not waiting for votes".into()));
        }

        txn.votes.insert(result.node_id, result.vote);

        let all_participants: HashSet<u64> = txn.writes_by_node.keys().copied().collect();
        let voted: HashSet<u64> = txn.votes.keys().copied().collect();

        if voted == all_participants {
            let all_commit = txn.votes.values().all(|v| *v == Vote::Commit);

            if all_commit {
                txn.state = DistTxnState::InDoubt;
                let participants: Vec<u64> = all_participants.into_iter().collect();
                if let Err(err) = self.log_state(txn_id, "COMMIT", &participants) {
                    let _ = self.db.ensure_cluster_linearizable();
                    if matches!(self.has_durable_state(txn_id, "COMMIT"), Ok(true)) {
                        txn.state = DistTxnState::Committing;
                        return Ok(DistTxnState::Committing);
                    }
                    return Err(err);
                }
                txn.state = DistTxnState::Committing;
                return Ok(DistTxnState::Committing);
            }

            let abort_reasons: Vec<String> = txn
                .votes
                .values()
                .filter_map(|v| match v {
                    Vote::Abort(reason) => Some(reason.clone()),
                    _ => None,
                })
                .collect();
            let participants: Vec<u64> = all_participants.into_iter().collect();
            self.log_state(txn_id, "ABORT", &participants)?;
            txn.state = DistTxnState::Aborted;
            return Err(OmniError::IoError(format!(
                "Distributed transaction aborted: {}",
                abort_reasons.join("; ")
            )));
        }

        Ok(DistTxnState::WaitingForVotes)
    }

    /// COMMIT PHASE — marks the transaction as committed after all ACKs.
    pub fn finalize_commit(&self, txn_id: GlobalTxnId) -> Result<(), OmniError> {
        let mut active = self
            .active_txns
            .lock()
            .map_err(|_| OmniError::LockPoisoned("coordinator".into()))?;
        if self.has_durable_state(txn_id, "ABORT")? {
            return Err(OmniError::IoError(
                "cannot finalize commit for an aborted transaction".into(),
            ));
        }
        let participants = if let Some(txn) = active.get(&txn_id) {
            let all_participants: HashSet<u64> = txn.writes_by_node.keys().copied().collect();
            let voted: HashSet<u64> = txn.votes.keys().copied().collect();
            let all_commit =
                voted == all_participants && txn.votes.values().all(|v| *v == Vote::Commit);
            if !all_commit
                && !matches!(
                    txn.state,
                    DistTxnState::Committing | DistTxnState::Committed
                )
            {
                return Err(OmniError::IoError(
                    "cannot finalize commit before all participants vote COMMIT".into(),
                ));
            }
            let list: Vec<u64> = txn.writes_by_node.keys().copied().collect();
            if !self.has_durable_state(txn_id, "COMMIT")? {
                self.log_state(txn_id, "COMMIT", &list)?;
            }
            list
        } else {
            let commit_key = format!("__2PC_LOG__/{}_{}/COMMIT", txn_id.0, txn_id.1);
            self.db
                .find(&commit_key, self.db.get_seq())?
                .and_then(|raw| serde_json::from_str::<TwoPhaseLogEntry>(&raw).ok())
                .map(|entry| entry.participants)
                .ok_or_else(|| {
                    OmniError::IoError(format!("No durable COMMIT decision for {txn_id:?}"))
                })?
        };
        self.log_state(txn_id, "COMPLETE", &participants)?;
        if let Some(txn) = active.get_mut(&txn_id) {
            txn.state = DistTxnState::Committed;
            active.remove(&txn_id);
        }
        Ok(())
    }

    /// Marks an aborted transaction as completed once all participants acknowledge the abort.
    pub fn finalize_abort(&self, txn_id: GlobalTxnId) -> Result<(), OmniError> {
        let mut active = self
            .active_txns
            .lock()
            .map_err(|_| OmniError::LockPoisoned("coordinator".into()))?;
        if matches!(
            active.get(&txn_id).map(|t| &t.state),
            Some(DistTxnState::InDoubt | DistTxnState::Committing)
        ) {
            self.db.ensure_cluster_linearizable()?;
        }
        if self.has_durable_state(txn_id, "COMMIT")? {
            return Err(OmniError::IoError(
                "cannot finalize abort after a durable commit decision".into(),
            ));
        }
        let participants = if let Some(txn) = active.get(&txn_id) {
            if txn.state == DistTxnState::Committed {
                return Err(OmniError::IoError(
                    "cannot finalize abort for a committed transaction".into(),
                ));
            }
            let list: Vec<u64> = txn.writes_by_node.keys().copied().collect();
            if !self.has_durable_state(txn_id, "ABORT")? {
                self.log_state(txn_id, "ABORT", &list)?;
            }
            list
        } else {
            let abort_key = format!("__2PC_LOG__/{}_{}/ABORT", txn_id.0, txn_id.1);
            self.db
                .find(&abort_key, self.db.get_seq())?
                .and_then(|raw| serde_json::from_str::<TwoPhaseLogEntry>(&raw).ok())
                .map(|entry| entry.participants)
                .ok_or_else(|| {
                    OmniError::IoError(format!("No durable ABORT decision for {txn_id:?}"))
                })?
        };
        self.log_state(txn_id, "COMPLETE", &participants)?;
        active.remove(&txn_id);
        Ok(())
    }

    /// ABORT — aborts a transaction if no commit decision has been durably recorded.
    pub fn abort(&self, txn_id: GlobalTxnId) -> Result<Vec<u64>, OmniError> {
        let mut active = self
            .active_txns
            .lock()
            .map_err(|_| OmniError::LockPoisoned("coordinator".into()))?;
        if matches!(
            active.get(&txn_id).map(|t| &t.state),
            Some(DistTxnState::InDoubt | DistTxnState::Committing)
        ) {
            self.db.ensure_cluster_linearizable()?;
        }
        let durable_commit = self.has_durable_state(txn_id, "COMMIT")?;

        let txn = active
            .get_mut(&txn_id)
            .ok_or_else(|| OmniError::IoError("Transaction not found".into()))?;
        if txn.state == DistTxnState::Committed || durable_commit {
            if txn.state != DistTxnState::Committed {
                txn.state = DistTxnState::Committing;
            }
            return Err(OmniError::IoError(
                "cannot abort a transaction after its commit decision".into(),
            ));
        }

        let participants: Vec<u64> = txn.writes_by_node.keys().copied().collect();
        if !self.has_durable_state(txn_id, "ABORT")? {
            self.log_state(txn_id, "ABORT", &participants)?;
        }
        txn.state = DistTxnState::Aborted;
        active.remove(&txn_id);

        Ok(participants)
    }

    /// Check for timed-out transactions.
    pub fn check_timeouts(&self) -> Vec<GlobalTxnId> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let active = self.active_txns.lock().unwrap_or_else(|e| e.into_inner());
        let mut timed_out = Vec::new();

        for (id, txn) in active.iter() {
            let can_timeout = match txn.state {
                DistTxnState::WaitingForVotes => true,
                DistTxnState::InDoubt | DistTxnState::Committing => {
                    !self.has_durable_state(*id, "COMMIT").unwrap_or(true)
                }
                _ => false,
            };
            if can_timeout {
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

    /// Recover uncompleted distributed transactions after a coordinator crash.
    pub fn recover(&self) -> Result<Vec<(GlobalTxnId, String, Vec<u64>)>, OmniError> {
        let seq = self.db.get_seq();
        let prefix = "__2PC_LOG__/";
        let end = "__2PC_LOG__/~";

        let entries = self.db.scan(prefix, end, seq)?;

        let mut txn_states: HashMap<GlobalTxnId, HashMap<String, TwoPhaseLogEntry>> =
            HashMap::new();

        for (key, value) in &entries {
            if key.starts_with("__2PC_LOG__/NEXT_SEQ/") {
                continue;
            }
            if let Ok(entry) = serde_json::from_str::<TwoPhaseLogEntry>(value) {
                txn_states
                    .entry(entry.txn_id)
                    .or_default()
                    .insert(entry.state.clone(), entry);
            }
        }

        let mut in_doubt = Vec::new();

        for (txn_id, states) in &txn_states {
            if states.contains_key("COMPLETE") {
                continue;
            }
            if let Some(entry) = states.get("COMMIT") {
                in_doubt.push((*txn_id, "COMMIT".to_string(), entry.participants.clone()));
            } else if let Some(entry) = states.get("ABORT") {
                in_doubt.push((*txn_id, "ABORT".to_string(), entry.participants.clone()));
            } else if let Some(entry) = states.get("PREPARE") {
                in_doubt.push((*txn_id, "PREPARE".to_string(), entry.participants.clone()));
            }
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

    /// GC completed coordinator log entries older than `max_age_secs`.
    pub fn gc_completed_logs(&self, max_age_secs: u64) -> Result<usize, OmniError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let entries = self
            .db
            .scan("__2PC_LOG__/", "__2PC_LOG__/~", self.db.get_seq())?;
        let mut by_txn: HashMap<GlobalTxnId, Vec<(String, TwoPhaseLogEntry)>> = HashMap::new();
        let mut keys_to_delete = Vec::new();
        for (key, value) in entries {
            if key.starts_with("__2PC_LOG__/NEXT_SEQ/") {
                continue;
            }
            match serde_json::from_str::<TwoPhaseLogEntry>(&value) {
                Ok(entry) => {
                    by_txn.entry(entry.txn_id).or_default().push((key, entry));
                }
                Err(_) => {
                    tracing::warn!(
                        key,
                        "2PC coordinator log entry is malformed; retaining for manual review"
                    );
                }
            }
        }
        for records in by_txn.into_values() {
            let terminal_entry = records
                .iter()
                .find(|(_, e)| e.state == "COMPLETE")
                .map(|(_, e)| e);
            if let Some(terminal) = terminal_entry
                && now.saturating_sub(terminal.timestamp) > max_age_secs
            {
                keys_to_delete.push(records.into_iter().map(|(key, _)| key).collect::<Vec<_>>());
            }
        }
        let purged = keys_to_delete.iter().map(Vec::len).sum();
        let mut batch = WriteBatch::new();
        for txn_keys in keys_to_delete {
            if batch.op_count() + txn_keys.len() > 1000 && !batch.is_empty() {
                self.db.commit_2pc_batch(&batch)?;
                batch.clear();
            }
            for key in txn_keys {
                batch.delete(&key)?;
            }
        }
        if !batch.is_empty() {
            self.db.commit_2pc_batch(&batch)?;
        }
        Ok(purged)
    }
}

pub struct TwoPhaseParticipant {
    node_id: u64,
    db: Arc<OmniKV>,
    prepared: Mutex<HashMap<GlobalTxnId, PreparedState>>,
    key_locks: Mutex<HashMap<String, GlobalTxnId>>,
    needs_recovery: std::sync::atomic::AtomicBool,
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
    if writes.len().saturating_add(2) > WriteBatch::MAX_OPS {
        return Err("2PC batch exceeds maximum size after decision records".into());
    }
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

fn opposite_2pc_decision_key(key: &str) -> Option<String> {
    if !key.starts_with("__2PC_LOG__/") {
        return None;
    }
    if let Some(prefix) = key.strip_suffix("/COMMIT") {
        return Some(format!("{prefix}/ABORT"));
    }
    if let Some(prefix) = key.strip_suffix("/ABORT") {
        return Some(format!("{prefix}/COMMIT"));
    }
    None
}

fn parse_participant_entry(key: &str, value: &str) -> Option<ParticipantLogEntry> {
    let rest = key.strip_prefix("__2PC_PREPARE__/")?;
    if !rest.contains('/') {
        return None;
    }
    serde_json::from_str(value).ok()
}

fn is_illegal_participant_transition(prev_state: &str, next_state: &str) -> bool {
    matches!(prev_state, "COMMITTED" | "ABORTED") && prev_state != next_state
}

fn latest_participant_entry(
    db: &OmniKV,
    staged: Option<&WriteBatch>,
    key: &str,
) -> Result<Option<ParticipantLogEntry>, OmniError> {
    if let Some(staged) = staged {
        let key_bytes = key.as_bytes();
        if staged
            .buffered_deletes
            .iter()
            .any(|k| k.as_slice() == key_bytes)
        {
            return Ok(None);
        }
        if let Some((_, value, _)) = staged
            .buffered_writes
            .iter()
            .rev()
            .find(|(k, _, _)| k.as_slice() == key_bytes)
        {
            return Ok(parse_participant_entry(key, value));
        }
    }
    Ok(db
        .find_latest_internal(key)?
        .and_then(|value| parse_participant_entry(key, &value)))
}

fn check_2pc_participant_writes(
    db: &OmniKV,
    staged: Option<&WriteBatch>,
    incoming_writes: &[(&str, &str)],
    incoming_deletes: &[&str],
) -> Result<Option<String>, OmniError> {
    let mut has_incoming_prepared = false;
    let mut seen_entries: HashMap<&str, ParticipantLogEntry> = HashMap::new();
    for &(key, value) in incoming_writes {
        let Some(entry) = parse_participant_entry(key, value) else {
            continue;
        };
        if participant_prepare_key(entry.node_id, entry.txn_id) != key {
            return Ok(Some(format!(
                "mismatched 2PC participant record key '{key}' for node {} txn {:?}",
                entry.node_id, entry.txn_id
            )));
        }
        let prev_entry = match seen_entries.get(key) {
            Some(prev) => Some(prev.clone()),
            None => latest_participant_entry(db, staged, key)?,
        };
        if let Some(prev) = prev_entry {
            if is_illegal_participant_transition(&prev.state, &entry.state) {
                return Ok(Some(format!(
                    "conflicting 2PC participant decision for key '{key}': {} -> {}",
                    prev.state, entry.state
                )));
            }
            if prev.state == "PREPARED"
                && prev.writes != entry.writes
                && !(entry.state == "ABORTED" && entry.writes.is_empty())
            {
                return Ok(Some(format!(
                    "conflicting 2PC participant writes for key '{key}'"
                )));
            }
        }
        if entry.state == "PREPARED" {
            has_incoming_prepared = true;
        }
        seen_entries.insert(key, entry);
    }
    if !has_incoming_prepared {
        return Ok(None);
    }

    let mut active_records: HashMap<String, String> = db
        .scan("__2PC_PREPARE__/", "__2PC_PREPARE__/~", u64::MAX)?
        .into_iter()
        .collect();
    if let Some(staged) = staged {
        for (key_bytes, val, _) in &staged.buffered_writes {
            if let Ok(key) = std::str::from_utf8(key_bytes)
                && key.starts_with("__2PC_PREPARE__/")
            {
                active_records.insert(key.to_string(), val.clone());
            }
        }
        for del in &staged.buffered_deletes {
            if let Ok(key) = std::str::from_utf8(del) {
                active_records.remove(key);
            }
        }
    }
    for &(key, val) in incoming_writes {
        if key.starts_with("__2PC_PREPARE__/") {
            active_records.insert(key.to_string(), val.to_string());
        }
    }
    for &del in incoming_deletes {
        active_records.remove(del);
    }

    let mut owners: HashMap<String, (u64, GlobalTxnId)> = HashMap::new();
    for (key, val) in active_records {
        let Some(entry) = parse_participant_entry(&key, &val) else {
            continue;
        };
        if entry.state != "PREPARED" {
            continue;
        }
        let owner = (entry.node_id, entry.txn_id);
        for write in entry.writes {
            if let Some(&existing) = owners.get(&write.key)
                && existing != owner
            {
                return Ok(Some(format!(
                    "conflicting 2PC prepared reservation for key '{}'",
                    write.key
                )));
            }
            owners.insert(write.key, owner);
        }
    }
    Ok(None)
}

pub(crate) fn validate_2pc_decision_writes(
    db: &OmniKV,
    staged: Option<&WriteBatch>,
    batch: &WriteBatch,
) -> Result<(), OmniError> {
    let mut touches_prepare = false;
    for (key_bytes, _, _) in &batch.buffered_writes {
        let Ok(key) = std::str::from_utf8(key_bytes) else {
            continue;
        };
        if key.starts_with("__2PC_PREPARE__/") {
            touches_prepare = true;
        }
        let Some(opposite_key) = opposite_2pc_decision_key(key) else {
            continue;
        };
        let opp_bytes = opposite_key.as_bytes();
        let conflict_in_batch = batch
            .buffered_writes
            .iter()
            .any(|(w, _, _)| w.as_slice() == opp_bytes)
            && !batch
                .buffered_deletes
                .iter()
                .any(|k| k.as_slice() == opp_bytes);
        let conflict_in_staged = staged.is_some_and(|s| {
            s.buffered_writes
                .iter()
                .any(|(w, _, _)| w.as_slice() == opp_bytes)
                && !s.buffered_deletes.iter().any(|k| k.as_slice() == opp_bytes)
        });
        let deleted_in_batch_or_staged = batch
            .buffered_deletes
            .iter()
            .any(|k| k.as_slice() == opp_bytes)
            || staged.is_some_and(|s| s.buffered_deletes.iter().any(|k| k.as_slice() == opp_bytes));
        let conflict_in_db =
            !deleted_in_batch_or_staged && db.find_latest_internal(&opposite_key)?.is_some();
        if conflict_in_batch || conflict_in_staged || conflict_in_db {
            return Err(OmniError::IoError(format!(
                "conflicting 2PC coordinator decision for {key}"
            )));
        }
    }
    if touches_prepare {
        let incoming_writes: Vec<(&str, &str)> = batch
            .buffered_writes
            .iter()
            .filter_map(|(k, v, _)| std::str::from_utf8(k).ok().map(|s| (s, v.as_str())))
            .collect();
        let incoming_deletes: Vec<&str> = batch
            .buffered_deletes
            .iter()
            .filter_map(|k| std::str::from_utf8(k).ok())
            .collect();
        if let Some(reason) =
            check_2pc_participant_writes(db, staged, &incoming_writes, &incoming_deletes)?
        {
            return Err(OmniError::IoError(reason));
        }
    }
    Ok(())
}

pub(crate) fn conflicts_with_2pc_decision(
    db: &OmniKV,
    staged: &WriteBatch,
    cmd: &crate::raft_command::RaftCommand,
) -> Result<bool, OmniError> {
    let mut touches_prepare = false;
    for op in &cmd.sets {
        if op.key.starts_with("__2PC_PREPARE__/") {
            touches_prepare = true;
        }
        let Some(opposite_key) = opposite_2pc_decision_key(&op.key) else {
            continue;
        };
        let opp_bytes = opposite_key.as_bytes();
        let conflict_in_cmd = cmd.sets.iter().any(|w| w.key == opposite_key)
            && !cmd.dels.iter().any(|k| k == &opposite_key);
        let conflict_in_staged = staged
            .buffered_writes
            .iter()
            .any(|(w, _, _)| w.as_slice() == opp_bytes)
            && !staged
                .buffered_deletes
                .iter()
                .any(|k| k.as_slice() == opp_bytes);
        let deleted_in_cmd_or_staged = cmd.dels.iter().any(|k| k == &opposite_key)
            || staged
                .buffered_deletes
                .iter()
                .any(|k| k.as_slice() == opp_bytes);
        let conflict_in_db =
            !deleted_in_cmd_or_staged && db.find_latest_internal(&opposite_key)?.is_some();
        if conflict_in_cmd || conflict_in_staged || conflict_in_db {
            return Ok(true);
        }
    }
    if touches_prepare {
        let incoming_writes: Vec<(&str, &str)> = cmd
            .sets
            .iter()
            .map(|op| (op.key.as_str(), op.value.as_str()))
            .collect();
        let incoming_deletes: Vec<&str> = cmd.dels.iter().map(String::as_str).collect();
        if check_2pc_participant_writes(db, Some(staged), &incoming_writes, &incoming_deletes)?
            .is_some()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn stamp_2pc_commit_seq(
    key_bytes: &[u8],
    value: &str,
    commit_seq: u64,
) -> Option<String> {
    let rest = key_bytes.strip_prefix(b"__2PC_PREPARE__/")?;
    if !rest.contains(&b'/') {
        return None;
    }
    let mut entry: ParticipantLogEntry = serde_json::from_str(value).ok()?;
    if entry.state != "COMMITTED" || entry.commit_seq.is_some() {
        return None;
    }
    entry.commit_seq = Some(commit_seq);
    serde_json::to_string(&entry).ok()
}

pub(crate) fn sync_all_prepared_reservations(db: &OmniKV) -> Result<(), OmniError> {
    let entries = db.scan("__2PC_PREPARE__/", "__2PC_PREPARE__/~", u64::MAX)?;
    let mut reserved = HashMap::new();
    for (key, value) in entries {
        let Some(entry) = parse_participant_entry(&key, &value) else {
            continue;
        };
        if entry.state != "PREPARED" {
            continue;
        }
        for write in entry.writes {
            reserved.insert(write.key, (entry.node_id, entry.txn_id));
        }
    }
    db.ssi_history.replace_all_prepared_keys(reserved);
    Ok(())
}

pub(crate) fn sync_prepared_reservations_from_batch(
    db: &OmniKV,
    batch: &WriteBatch,
) -> Result<(), OmniError> {
    if batch
        .buffered_deletes
        .iter()
        .any(|key| key.starts_with(b"__2PC_PREPARE__/"))
    {
        return sync_all_prepared_reservations(db);
    }
    for (key_bytes, value, _) in &batch.buffered_writes {
        let Ok(key) = std::str::from_utf8(key_bytes) else {
            continue;
        };
        let Some(entry) = parse_participant_entry(key, value) else {
            continue;
        };
        match entry.state.as_str() {
            "PREPARED" => {
                let mut reservations = HashMap::new();
                for write in &entry.writes {
                    reservations.insert(write.key.clone(), (entry.node_id, entry.txn_id));
                }
                db.ssi_history.merge_prepared_keys(reservations);
            }
            "COMMITTED" | "ABORTED" => {
                db.ssi_history
                    .release_prepared_keys_for_txn(entry.node_id, entry.txn_id);
            }
            _ => {}
        }
    }
    Ok(())
}

impl TwoPhaseParticipant {
    pub fn new(node_id: u64, db: Arc<OmniKV>) -> Self {
        let participant = Self {
            node_id,
            db,
            prepared: Mutex::new(HashMap::new()),
            key_locks: Mutex::new(HashMap::new()),
            needs_recovery: std::sync::atomic::AtomicBool::new(true),
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
            if self.needs_recovery.load(Ordering::SeqCst) {
                self.db.ensure_cluster_linearizable()?;
            }
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
        if let Err(reason) =
            self.db
                .ssi_history
                .reserve_prepared_keys(self.node_id, txn_id, &write_keys)
        {
            self.release_key_locks(txn_id, &write_keys);
            return PrepareResult {
                node_id: self.node_id,
                txn_id,
                vote: Vote::Abort(reason),
                prepare_seq: 0,
            };
        }
        if let Err(error) = self.db.fence_write_reservation() {
            self.release_key_locks(txn_id, &write_keys);
            return PrepareResult {
                node_id: self.node_id,
                txn_id,
                vote: Vote::Abort(error.to_string()),
                prepare_seq: 0,
            };
        }
        if let Some(reason) = self
            .db
            .ssi_history
            .check_write_conflicts(read_seq, &write_keys)
            .or_else(|| {
                write_keys.iter().find_map(|key| {
                    let latest_seq = self.db.get_seq_for_key(key, u64::MAX);
                    (latest_seq > read_seq).then(|| {
                        format!(
                            "SSI CONFLICT (WW): key '{}' written at seq {}",
                            key, latest_seq
                        )
                    })
                })
            })
        {
            self.release_key_locks(txn_id, &write_keys);
            return PrepareResult {
                node_id: self.node_id,
                txn_id,
                vote: Vote::Abort(reason),
                prepare_seq: 0,
            };
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
        // Reject if the COMMITTED version would exceed MAX_VALUE_SIZE.
        let max_value = 10 * 1024 * 1024;
        if encoded.len() + 32 > max_value {
            self.release_key_locks(txn_id, &write_keys);
            return PrepareResult {
                node_id: self.node_id,
                txn_id,
                vote: Vote::Abort("decision record would exceed value size limit".into()),
                prepare_seq: 0,
            };
        }
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
                let mut prepared = self.prepared.lock().unwrap_or_else(|e| e.into_inner());
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
                self.reconcile_failed_prepare(txn_id, writes, &write_keys, &e);
                PrepareResult {
                    node_id: self.node_id,
                    txn_id,
                    vote: Vote::Abort(format!("WAL write failed: {}", e)),
                    prepare_seq: 0,
                }
            }
        }
    }

    fn reconcile_failed_prepare(
        &self,
        txn_id: GlobalTxnId,
        writes: &[DistWrite],
        write_keys: &[String],
        error: &OmniError,
    ) {
        if matches!(error, OmniError::NotLeader { .. }) {
            self.release_key_locks(txn_id, write_keys);
            return;
        }
        if !self.db.is_clustered() && matches!(self.load_participant_log(txn_id), Ok(None)) {
            self.release_key_locks(txn_id, write_keys);
            return;
        }
        let abort_entry = ParticipantLogEntry {
            txn_id,
            node_id: self.node_id,
            state: "ABORTED".into(),
            writes: writes.to_vec(),
            prepare_seq: 0,
            commit_seq: None,
        };
        let mut abort_batch = WriteBatch::new();
        let abort_committed = serde_json::to_string(&abort_entry)
            .ok()
            .and_then(|encoded| {
                abort_batch
                    .set(&participant_prepare_key(self.node_id, txn_id), encoded)
                    .ok()?;
                let _ = abort_batch.set(
                    &legacy_participant_prepare_key(txn_id),
                    format!("ABORTED:{}", self.node_id),
                );
                self.db.commit_2pc_batch(&abort_batch).ok()
            })
            .is_some();
        if abort_committed {
            self.release_key_locks(txn_id, write_keys);
        } else {
            self.needs_recovery.store(true, Ordering::SeqCst);
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
        let mut locks = self.key_locks.lock().unwrap_or_else(|e| e.into_inner());
        for key in keys {
            if locks.get(key).is_some_and(|owner| *owner == txn_id) {
                locks.remove(key);
            }
        }
        drop(locks);
        self.db
            .ssi_history
            .release_prepared_keys(self.node_id, txn_id, keys);
    }

    fn cleanup_terminal_state(&self, txn_id: GlobalTxnId) -> Result<(), OmniError> {
        self.key_locks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|_, owner| *owner != txn_id);
        self.db
            .ssi_history
            .release_prepared_keys_for_txn(self.node_id, txn_id);
        self.prepared
            .lock()
            .map_err(|_| OmniError::LockPoisoned("participant prepared".into()))?
            .remove(&txn_id);
        Ok(())
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
            let prepared_at = prepared
                .get(&entry.txn_id)
                .map_or_else(Instant::now, |existing| existing.prepared_at);
            recovered_prepared.insert(
                entry.txn_id,
                PreparedState {
                    batch,
                    writes: entry.writes,
                    prepare_seq: entry.prepare_seq,
                    prepared_at,
                },
            );
            recovered += 1;
        }
        self.db
            .ssi_history
            .replace_prepared_keys_for_node(self.node_id, &recovered_locks)
            .map_err(OmniError::IoError)?;
        *prepared = recovered_prepared;
        *key_locks = recovered_locks;
        self.needs_recovery.store(false, Ordering::SeqCst);
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
        if self.needs_recovery.load(Ordering::SeqCst) {
            self.db.ensure_cluster_linearizable()?;
            self.recover_locked()?;
        }
        let state = self
            .load_participant_log(txn_id)?
            .ok_or_else(|| OmniError::IoError(format!("No prepared transaction {txn_id:?}")))?;
        let write_keys: Vec<String> = state.writes.iter().map(|w| w.key.clone()).collect();
        match state.state.as_str() {
            "COMMITTED" => {
                let ssi_txn_id = txn_id.0.wrapping_mul(1_000_000).wrapping_add(txn_id.1);
                let commit_seq = match state.commit_seq {
                    Some(seq) => seq,
                    None => {
                        let recovered_seq = self
                            .db
                            .ssi_history()
                            .committed_seq_for_txn(ssi_txn_id)
                            .ok_or_else(|| {
                                OmniError::IoError(format!(
                                    "Missing commit sequence for committed transaction {txn_id:?}"
                                ))
                            })?;
                        let prep_key = participant_prepare_key(self.node_id, txn_id);
                        let mut final_log = WriteBatch::new();
                        final_log.set(
                            &prep_key,
                            serde_json::to_string(&ParticipantLogEntry {
                                commit_seq: Some(recovered_seq),
                                ..state
                            })
                            .map_err(|error| {
                                OmniError::IoError(format!("commit log update: {error}"))
                            })?,
                        )?;
                        let _ = self.db.commit_2pc_batch(&final_log);
                        recovered_seq
                    }
                };
                self.cleanup_terminal_state(txn_id)?;
                return Ok(commit_seq);
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
        commit_batch.set(
            &legacy_participant_prepare_key(txn_id),
            format!("COMMITTED:{}", self.node_id),
        )?;
        let ssi_txn_id = txn_id.0.wrapping_mul(1_000_000).wrapping_add(txn_id.1);
        let commit_seq = self.db.commit_2pc_batch_with_ssi(
            &commit_batch,
            crate::transaction::SsiCommitRecord {
                txn_id: ssi_txn_id,
                write_keys: write_keys.clone(),
                read_keys: Vec::new(),
                read_ranges: Vec::new(),
            },
        )?;
        self.cleanup_terminal_state(txn_id)?;

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
        if self.needs_recovery.load(Ordering::SeqCst) {
            self.db.ensure_cluster_linearizable()?;
            self.recover_locked()?;
        }
        let writes = match self.load_participant_log(txn_id)? {
            Some(entry) if entry.state == "COMMITTED" => {
                return Err(OmniError::IoError(
                    "cannot abort a committed transaction".into(),
                ));
            }
            Some(entry) if entry.state == "ABORTED" => {
                self.cleanup_terminal_state(txn_id)?;
                return Ok(());
            }
            Some(entry) if entry.state == "PREPARED" => entry.writes,
            Some(_) => return Err(OmniError::IoError("invalid participant decision".into())),
            None => Vec::new(),
        };

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
        self.cleanup_terminal_state(txn_id)?;

        Ok(())
    }

    /// Returns the number of prepared (in-doubt) transactions.
    pub fn prepared_count(&self) -> usize {
        self.prepared.lock().map(|p| p.len()).unwrap_or(0)
    }

    /// Check for stale prepared transactions (for recovery).
    pub fn stale_prepared(&self, max_age: Duration) -> Vec<GlobalTxnId> {
        let prepared = self.prepared.lock().unwrap_or_else(|e| e.into_inner());
        prepared
            .iter()
            .filter(|(_, state)| state.prepared_at.elapsed() > max_age)
            .map(|(id, _)| *id)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_db() -> (Arc<OmniKV>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db = OmniKV::open(
            dir.path().join("manifest.json").to_str().unwrap(),
            dir.path().join("wal.bin").to_str().unwrap(),
        )
        .unwrap();
        (db, dir)
    }

    #[test]
    fn corrupt_next_seq_watermark_fails_closed_until_repaired() {
        let (db, _dir) = test_db();
        let mut batch = WriteBatch::new();
        batch
            .set("__2PC_LOG__/NEXT_SEQ/5", "not-a-u64".into())
            .unwrap();
        db.commit_2pc_batch(&batch).unwrap();

        let coord = TwoPhaseCoordinator::new(5, db.clone(), 5000);
        assert!(coord.begin().is_err());

        let mut fix = WriteBatch::new();
        fix.set("__2PC_LOG__/NEXT_SEQ/5", "41".into()).unwrap();
        db.commit_2pc_batch(&fix).unwrap();

        assert_eq!(coord.begin().unwrap(), (5, 42));
    }

    #[test]
    fn unacknowledged_abort_survives_gc_until_complete() {
        let (db, _dir) = test_db();
        let txn_id = (1, 77);
        let prepare = TwoPhaseLogEntry {
            txn_id,
            state: "PREPARE".into(),
            participants: vec![2, 3],
            timestamp: 10,
        };
        let abort = TwoPhaseLogEntry {
            txn_id,
            state: "ABORT".into(),
            participants: vec![2, 3],
            timestamp: 11,
        };
        let mut batch = WriteBatch::new();
        batch
            .set(
                "__2PC_LOG__/1_77/PREPARE",
                serde_json::to_string(&prepare).unwrap(),
            )
            .unwrap();
        batch
            .set(
                "__2PC_LOG__/1_77/ABORT",
                serde_json::to_string(&abort).unwrap(),
            )
            .unwrap();
        db.commit_2pc_batch(&batch).unwrap();

        let coord = TwoPhaseCoordinator::new(1, db.clone(), 5000);
        assert_eq!(coord.gc_completed_logs(0).unwrap(), 0);
        assert_eq!(
            coord.recover().unwrap(),
            vec![(txn_id, "ABORT".to_string(), vec![2, 3])]
        );

        let complete = TwoPhaseLogEntry {
            txn_id,
            state: "COMPLETE".into(),
            participants: vec![2, 3],
            timestamp: 12,
        };
        let mut fin = WriteBatch::new();
        fin.set(
            "__2PC_LOG__/1_77/COMPLETE",
            serde_json::to_string(&complete).unwrap(),
        )
        .unwrap();
        db.commit_2pc_batch(&fin).unwrap();

        assert_eq!(coord.recover().unwrap().len(), 0);
        assert_eq!(coord.gc_completed_logs(0).unwrap(), 3);
    }

    #[test]
    fn multiple_coordinators_on_same_node_allocate_distinct_sequences() {
        let (db, _dir) = test_db();
        let coord_a = TwoPhaseCoordinator::new(7, db.clone(), 5000);
        let coord_b = TwoPhaseCoordinator::new(7, db, 5000);

        assert_eq!(coord_a.begin().unwrap(), (7, 1));
        assert_eq!(coord_b.begin().unwrap(), (7, 2));
        assert_eq!(coord_a.begin().unwrap(), (7, 3));
    }

    #[test]
    fn finalize_commit_rejects_missing_or_aborted_decision_and_conflicting_writes_fail() {
        let (db, _dir) = test_db();
        let coord = TwoPhaseCoordinator::new(3, db.clone(), 5000);

        assert!(coord.finalize_commit((3, 999)).is_err());

        let txn_id = coord.begin().unwrap();
        coord
            .add_write(txn_id, 2, "k".into(), Some("v".into()), 0)
            .unwrap();
        coord.prepare(txn_id).unwrap();
        coord.abort(txn_id).unwrap();
        assert!(coord.finalize_commit(txn_id).is_err());

        let conflict_entry = TwoPhaseLogEntry {
            txn_id,
            state: "COMMIT".into(),
            participants: vec![2],
            timestamp: 1,
        };
        let mut conflicting = WriteBatch::new();
        conflicting
            .set(
                &format!("__2PC_LOG__/{}_{}/COMMIT", txn_id.0, txn_id.1),
                serde_json::to_string(&conflict_entry).unwrap(),
            )
            .unwrap();
        assert!(db.commit_2pc_batch(&conflicting).is_err());
    }

    #[test]
    fn participant_commit_retry_recovers_original_commit_seq_after_later_writes() {
        let (db, _dir) = test_db();
        let participant = TwoPhaseParticipant::new(2, db.clone());
        let txn_id = (1, 55);
        let writes = vec![
            DistWrite {
                node_id: 2,
                key: "k_keep".into(),
                value: Some("v_keep".into()),
                ttl: 0,
            },
            DistWrite {
                node_id: 2,
                key: "k_del".into(),
                value: None,
                ttl: 0,
            },
        ];

        assert_eq!(participant.prepare(txn_id, &writes).vote, Vote::Commit);

        let mut commit_batch = build_write_batch(&writes).unwrap();
        let log_entry = ParticipantLogEntry {
            txn_id,
            node_id: 2,
            state: "COMMITTED".into(),
            writes: writes.clone(),
            prepare_seq: 1,
            commit_seq: None,
        };
        commit_batch
            .set(
                &participant_prepare_key(2, txn_id),
                serde_json::to_string(&log_entry).unwrap(),
            )
            .unwrap();
        commit_batch
            .set(
                &legacy_participant_prepare_key(txn_id),
                "COMMITTED:2".into(),
            )
            .unwrap();
        let cmd1 = crate::raft_command::RaftCommand::from_batch_with_ssi(
            &commit_batch,
            crate::transaction::SsiCommitRecord {
                txn_id: 1_000_055,
                write_keys: vec!["k_keep".into(), "k_del".into()],
                read_keys: Vec::new(),
                read_ranges: Vec::new(),
            },
        );
        let mut follow_up_batch = WriteBatch::new();
        follow_up_batch
            .set("grouped_follow_up", "after".into())
            .unwrap();
        let cmd2 = crate::raft_command::RaftCommand::from_batch(&follow_up_batch);

        let mut raft_storage = crate::raft_storage::OmniRaftStorage::new(db.clone());
        let leader = openraft::CommittedLeaderId::new(1, 1);
        let entries = [
            openraft::Entry {
                log_id: openraft::LogId::new(leader, 1),
                payload: openraft::EntryPayload::Normal(cmd1.encode()),
            },
            openraft::Entry {
                log_id: openraft::LogId::new(leader, 2),
                payload: openraft::EntryPayload::Normal(cmd2.encode()),
            },
        ];
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let responses = rt
            .block_on(openraft::RaftStorage::apply_to_state_machine(
                &mut raft_storage,
                &entries,
            ))
            .unwrap();
        let original_seq: u64 = responses[0].parse().unwrap();
        let prep_seq = db.get_seq_for_key(&participant_prepare_key(2, txn_id), u64::MAX);
        assert!(
            original_seq > prep_seq + 3,
            "grouped Raft apply must assign a shared batch commit marker beyond the participant's own ops"
        );
        let durable: ParticipantLogEntry = serde_json::from_str(
            &db.find(&participant_prepare_key(2, txn_id), u64::MAX)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(durable.commit_seq, Some(original_seq));

        for i in 0..10 {
            let mut unrelated = WriteBatch::new();
            unrelated
                .set(&format!("unrelated_{i}"), "val".into())
                .unwrap();
            db.commit_batch(&unrelated).unwrap();
        }
        assert!(db.get_seq() > original_seq);

        assert_eq!(participant.commit(txn_id).unwrap(), original_seq);
    }

    #[test]
    fn terminal_participant_retries_release_lingering_ssi_reservations() {
        let (db, _dir) = test_db();
        let participant = TwoPhaseParticipant::new(2, db.clone());
        let committed_txn = (1, 10);
        let aborted_txn = (1, 11);

        assert_eq!(
            participant
                .prepare(
                    committed_txn,
                    &[DistWrite {
                        node_id: 2,
                        key: "k_committed".into(),
                        value: Some("v1".into()),
                        ttl: 0,
                    }]
                )
                .vote,
            Vote::Commit
        );
        participant.commit(committed_txn).unwrap();

        db.ssi_history
            .reserve_prepared_keys(2, committed_txn, &["k_committed".to_string()])
            .unwrap();
        let mut plain_c = WriteBatch::new();
        plain_c.set("k_committed", "blocked".into()).unwrap();
        assert!(db.commit_batch(&plain_c).is_err());

        participant.commit(committed_txn).unwrap();
        assert!(db.commit_batch(&plain_c).is_ok());

        assert_eq!(
            participant
                .prepare(
                    aborted_txn,
                    &[DistWrite {
                        node_id: 2,
                        key: "k_aborted".into(),
                        value: Some("v2".into()),
                        ttl: 0,
                    }]
                )
                .vote,
            Vote::Commit
        );
        participant.abort(aborted_txn).unwrap();

        db.ssi_history
            .reserve_prepared_keys(2, aborted_txn, &["k_aborted".to_string()])
            .unwrap();
        let mut plain_a = WriteBatch::new();
        plain_a.set("k_aborted", "blocked".into()).unwrap();
        assert!(db.commit_batch(&plain_a).is_err());

        participant.abort(aborted_txn).unwrap();
        assert!(db.commit_batch(&plain_a).is_ok());
    }

    #[test]
    fn prepared_participant_records_enforce_exclusive_ownership_and_terminal_transitions() {
        let (db, _dir) = test_db();
        let make_batch = |node_id: u64, txn_id: GlobalTxnId, state: &str, key: &str| {
            let entry = ParticipantLogEntry {
                txn_id,
                node_id,
                state: state.into(),
                writes: vec![DistWrite {
                    node_id,
                    key: key.into(),
                    value: Some("v".into()),
                    ttl: 0,
                }],
                prepare_seq: 1,
                commit_seq: None,
            };
            let mut batch = WriteBatch::new();
            batch
                .set(
                    &participant_prepare_key(node_id, txn_id),
                    serde_json::to_string(&entry).unwrap(),
                )
                .unwrap();
            batch
        };

        let t1 = (1, 1);
        let t2 = (1, 2);
        let t3 = (1, 3);
        let t4 = (1, 4);

        db.commit_2pc_batch(&make_batch(2, t1, "PREPARED", "shared_k"))
            .unwrap();
        db.commit_2pc_batch(&make_batch(2, t1, "PREPARED", "shared_k"))
            .unwrap();
        assert!(
            db.commit_2pc_batch(&make_batch(2, t2, "PREPARED", "shared_k"))
                .is_err(),
            "local 2PC commit must reject overlapping PREPARED key ownership"
        );

        let mut raft_storage = crate::raft_storage::OmniRaftStorage::new(db.clone());
        let leader = openraft::CommittedLeaderId::new(1, 1);
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let t2_cmd = crate::raft_command::RaftCommand::from_batch(&make_batch(
            2, t2, "PREPARED", "shared_k",
        ));
        let t3_cmd = crate::raft_command::RaftCommand::from_batch(&make_batch(
            2,
            t3,
            "PREPARED",
            "grouped_k",
        ));
        let t4_cmd = crate::raft_command::RaftCommand::from_batch(&make_batch(
            2,
            t4,
            "PREPARED",
            "grouped_k",
        ));
        let entries = [
            openraft::Entry {
                log_id: openraft::LogId::new(leader, 1),
                payload: openraft::EntryPayload::Normal(t2_cmd.encode()),
            },
            openraft::Entry {
                log_id: openraft::LogId::new(leader, 2),
                payload: openraft::EntryPayload::Normal(t3_cmd.encode()),
            },
            openraft::Entry {
                log_id: openraft::LogId::new(leader, 3),
                payload: openraft::EntryPayload::Normal(t4_cmd.encode()),
            },
        ];
        let responses = rt
            .block_on(openraft::RaftStorage::apply_to_state_machine(
                &mut raft_storage,
                &entries,
            ))
            .unwrap();
        assert_eq!(
            responses[0], "ERR",
            "Raft apply must reject PREPARED record overlapping durable PREPARED owner"
        );
        assert!(
            responses[1].parse::<u64>().is_ok(),
            "non-overlapping PREPARED record in grouped apply must succeed"
        );
        assert_eq!(
            responses[2], "ERR",
            "Raft apply must reject second PREPARED record overlapping staged PREPARED owner"
        );

        let restarted = TwoPhaseParticipant::new(2, db.clone());
        assert_eq!(
            restarted.recover().unwrap(),
            2,
            "participant recovery must see only non-overlapping durable PREPARED records"
        );

        db.commit_2pc_batch(&make_batch(2, t1, "ABORTED", "shared_k"))
            .unwrap();
        assert!(
            db.commit_2pc_batch(&make_batch(2, t1, "PREPARED", "shared_k"))
                .is_err(),
            "ABORTED participant record must not regress to PREPARED"
        );
        db.commit_2pc_batch(&make_batch(2, t2, "PREPARED", "shared_k"))
            .unwrap();
    }

    #[test]
    fn reconcile_failed_prepare_aborts_durable_prepared_record_before_releasing_locks() {
        let (db, _dir) = test_db();
        let participant = TwoPhaseParticipant::new(2, db.clone());
        let t1 = (1, 100);
        let t2 = (1, 101);
        let writes = vec![DistWrite {
            node_id: 2,
            key: "indeterminate_k".into(),
            value: Some("v1".into()),
            ttl: 0,
        }];
        let write_keys = vec!["indeterminate_k".to_string()];

        participant
            .key_locks
            .lock()
            .unwrap()
            .insert("indeterminate_k".into(), t1);
        db.ssi_history
            .reserve_prepared_keys(2, t1, &write_keys)
            .unwrap();
        let durable_prepare = ParticipantLogEntry {
            txn_id: t1,
            node_id: 2,
            state: "PREPARED".into(),
            writes: writes.clone(),
            prepare_seq: 0,
            commit_seq: None,
        };
        let mut prepare_batch = WriteBatch::new();
        prepare_batch
            .set(
                &participant_prepare_key(2, t1),
                serde_json::to_string(&durable_prepare).unwrap(),
            )
            .unwrap();
        db.commit_2pc_batch(&prepare_batch).unwrap();

        participant.reconcile_failed_prepare(
            t1,
            &writes,
            &write_keys,
            &OmniError::IoError("cluster write applied but await failed".into()),
        );

        let after = participant.load_participant_log(t1).unwrap().unwrap();
        assert_eq!(after.state, "ABORTED");
        assert_eq!(
            participant.prepare(t2, &writes).vote,
            Vote::Commit,
            "second transaction must prepare cleanly after indeterminate prepare is aborted"
        );
        let restarted = TwoPhaseParticipant::new(2, db);
        assert_eq!(restarted.recover().unwrap(), 1);
    }

    #[test]
    fn participant_record_validation_rejects_mismatched_keys_mutated_writes_and_syncs_deletes() {
        let (db, _dir) = test_db();
        let make_entry = |node_id: u64, txn_id: GlobalTxnId, state: &str, key: &str| {
            serde_json::to_string(&ParticipantLogEntry {
                txn_id,
                node_id,
                state: state.into(),
                writes: vec![DistWrite {
                    node_id,
                    key: key.into(),
                    value: Some("v".into()),
                    ttl: 0,
                }],
                prepare_seq: 1,
                commit_seq: None,
            })
            .unwrap()
        };

        // 1. Mismatched key vs payload (key says node 2 txn 1_1, payload says node 2 txn 1_99)
        let mut spoofed = WriteBatch::new();
        spoofed
            .set(
                &participant_prepare_key(2, (1, 1)),
                make_entry(2, (1, 99), "PREPARED", "k_spoof"),
            )
            .unwrap();
        assert!(
            db.commit_2pc_batch(&spoofed).is_err(),
            "mismatched key vs ParticipantLogEntry payload must be rejected"
        );

        // 2. Valid PREPARED record, followed by an attempt to mutate its write set
        let t1 = (1, 1);
        let mut valid_prep = WriteBatch::new();
        valid_prep
            .set(
                &participant_prepare_key(2, t1),
                make_entry(2, t1, "PREPARED", "k_orig"),
            )
            .unwrap();
        db.commit_2pc_batch(&valid_prep).unwrap();

        for bad_state in ["PREPARED", "COMMITTED", "ABORTED"] {
            let mut mutated = WriteBatch::new();
            mutated
                .set(
                    &participant_prepare_key(2, t1),
                    make_entry(2, t1, bad_state, "k_different"),
                )
                .unwrap();
            assert!(
                db.commit_2pc_batch(&mutated).is_err(),
                "transition from PREPARED to {bad_state} must reject mutated write set"
            );
        }

        // 3. Raft apply deleting a __2PC_PREPARE__ record must release its SSI reservation
        db.ssi_history
            .reserve_prepared_keys(2, t1, &["k_orig".to_string()])
            .unwrap();
        let mut plain = WriteBatch::new();
        plain.set("k_orig", "plain_val".into()).unwrap();
        assert!(db.commit_batch(&plain).is_err());

        let mut del_batch = WriteBatch::new();
        del_batch.delete(&participant_prepare_key(2, t1)).unwrap();
        let del_cmd = crate::raft_command::RaftCommand::from_batch(&del_batch);
        let mut raft_storage = crate::raft_storage::OmniRaftStorage::new(db.clone());
        let leader = openraft::CommittedLeaderId::new(1, 1);
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let responses = rt
            .block_on(openraft::RaftStorage::apply_to_state_machine(
                &mut raft_storage,
                &[openraft::Entry {
                    log_id: openraft::LogId::new(leader, 1),
                    payload: openraft::EntryPayload::Normal(del_cmd.encode()),
                }],
            ))
            .unwrap();
        assert!(responses[0].parse::<u64>().is_ok());
        assert!(
            db.commit_batch(&plain).is_ok(),
            "deleting a durable PREPARED record through Raft apply must release its SSI reservation"
        );

        // 4. NotLeader error in reconcile_failed_prepare releases locks without writing ABORTED
        let participant = TwoPhaseParticipant::new(2, db.clone());
        let t_nl = (1, 200);
        let nl_keys = vec!["k_not_leader".to_string()];
        participant
            .key_locks
            .lock()
            .unwrap()
            .insert("k_not_leader".into(), t_nl);
        db.ssi_history
            .reserve_prepared_keys(2, t_nl, &nl_keys)
            .unwrap();
        participant.reconcile_failed_prepare(
            t_nl,
            &[DistWrite {
                node_id: 2,
                key: "k_not_leader".into(),
                value: Some("v".into()),
                ttl: 0,
            }],
            &nl_keys,
            &OmniError::NotLeader { leader_id: Some(1) },
        );
        assert!(participant.load_participant_log(t_nl).unwrap().is_none());
        let mut nl_plain = WriteBatch::new();
        nl_plain.set("k_not_leader", "ok".into()).unwrap();
        assert!(db.commit_batch(&nl_plain).is_ok());

        // 5. Presumed-abort (ABORTED with empty writes) applied through Raft after PREPARED
        //    must succeed and release all SSI prepared reservations for that (node_id, txn_id).
        let t_pa = (1, 201);
        let mut pa_prep = WriteBatch::new();
        pa_prep
            .set(
                &participant_prepare_key(2, t_pa),
                make_entry(2, t_pa, "PREPARED", "k_presumed_abort"),
            )
            .unwrap();
        db.commit_2pc_batch(&pa_prep).unwrap();
        db.ssi_history
            .reserve_prepared_keys(2, t_pa, &["k_presumed_abort".to_string()])
            .unwrap();

        let presumed_abort = ParticipantLogEntry {
            txn_id: t_pa,
            node_id: 2,
            state: "ABORTED".into(),
            writes: Vec::new(),
            prepare_seq: 0,
            commit_seq: None,
        };
        let mut pa_abort = WriteBatch::new();
        pa_abort
            .set(
                &participant_prepare_key(2, t_pa),
                serde_json::to_string(&presumed_abort).unwrap(),
            )
            .unwrap();
        let pa_cmd = crate::raft_command::RaftCommand::from_batch(&pa_abort);
        let pa_resp = rt
            .block_on(openraft::RaftStorage::apply_to_state_machine(
                &mut raft_storage,
                &[openraft::Entry {
                    log_id: openraft::LogId::new(leader, 2),
                    payload: openraft::EntryPayload::Normal(pa_cmd.encode()),
                }],
            ))
            .unwrap();
        assert!(
            pa_resp[0].parse::<u64>().is_ok(),
            "presumed-abort with empty writes must be accepted against PREPARED"
        );
        let mut pa_plain = WriteBatch::new();
        pa_plain.set("k_presumed_abort", "freed".into()).unwrap();
        assert!(
            db.commit_batch(&pa_plain).is_ok(),
            "presumed-abort with empty writes must still release all SSI prepared keys for the txn"
        );

        // 6. abort() when no durable log exists still clears lingering in-memory locks/SSI reservations
        let t_ghost = (1, 202);
        participant
            .key_locks
            .lock()
            .unwrap()
            .insert("k_ghost".into(), t_ghost);
        db.ssi_history
            .reserve_prepared_keys(2, t_ghost, &["k_ghost".to_string()])
            .unwrap();
        participant.abort(t_ghost).unwrap();
        assert!(
            !participant
                .key_locks
                .lock()
                .unwrap()
                .contains_key("k_ghost")
        );
        let mut ghost_plain = WriteBatch::new();
        ghost_plain.set("k_ghost", "freed".into()).unwrap();
        assert!(db.commit_batch(&ghost_plain).is_ok());
    }
}
