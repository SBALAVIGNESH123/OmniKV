//! Serializable Snapshot Isolation (SSI) Transaction Engine
//!
//! Provides multi-key ACID transactions with full snapshot isolation and
//! write-write conflict detection. Inspired by PostgreSQL's SSI and
//! CockroachDB's transaction model.
//!
//! ## How It Works
//!
//! 1. **BEGIN**: Transaction acquires a read snapshot (`read_seq`).
//!    All reads see a consistent point-in-time view.
//!
//! 2. **READ**: Reads go through the snapshot — invisible to concurrent writers.
//!    Read keys are tracked in the `read_set` for conflict detection.
//!
//! 3. **WRITE**: Writes are buffered in the `write_set` (not yet visible to others).
//!
//! 4. **COMMIT**:
//!    a. Acquire the global transaction lock (serialization point).
//!    b. For each key in our write_set, check if any other transaction
//!       committed a write to that key AFTER our snapshot. If yes → ABORT.
//!    c. If no conflicts, commit all buffered writes atomically via WriteBatch.
//!    d. Release the lock.
//!
//! This guarantees Serializable isolation — the strongest level.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::{OmniError, OmniKV, WriteBatch};

/// Unique transaction identifier.
pub type TxnId = u64;

/// Transaction state.
#[derive(Debug, Clone, PartialEq)]
pub enum TxnState {
    Active,
    Committed,
    Aborted,
}

/// A single in-flight transaction with read/write tracking.
#[derive(Debug)]
pub struct Transaction {
    /// Unique transaction ID.
    pub id: TxnId,
    /// The MVCC snapshot sequence number (all reads see data ≤ this seq).
    pub read_seq: u64,
    /// State of this transaction.
    pub state: TxnState,
    /// Keys read during this transaction (for SSI conflict detection).
    pub read_set: HashSet<String>,
    /// Buffered writes: key → (value, ttl). None value = delete.
    pub write_set: HashMap<String, (Option<String>, u64)>,
}

impl Transaction {
    fn new(id: TxnId, read_seq: u64) -> Self {
        Self {
            id,
            read_seq,
            state: TxnState::Active,
            read_set: HashSet::new(),
            write_set: HashMap::new(),
        }
    }
}

/// Record of a committed transaction, used for conflict detection.
#[derive(Debug, Clone)]
struct CommittedTxn {
    /// The transaction ID.
    txn_id: TxnId,
    /// The commit sequence number.
    commit_seq: u64,
    /// Keys written by this transaction.
    write_keys: HashSet<String>,
    /// Keys read by this transaction.
    read_keys: HashSet<String>,
}

/// RW-dependency edge: T_from read a key that T_to later wrote.
/// PostgreSQL calls these "rw-antidependencies" or "SIREAD locks".
#[derive(Debug, Clone)]
struct RWDependency {
    from_txn: TxnId,
    to_txn: TxnId,
}

/// The Transaction Manager — coordinates all in-flight and recently
/// committed transactions for SSI conflict detection.
///
/// ## Dangerous Structure Detection (PostgreSQL-style)
///
/// A "dangerous structure" is a cycle of rw-dependencies:
///   T1 →rw→ T2 →rw→ T3
/// where T1 committed before T2 started, and T2 committed before T3 started.
/// This indicates a potential serialization anomaly and the middle
/// transaction (T2) must be aborted.
pub struct TransactionManager {
    db: Arc<OmniKV>,
    /// Monotonically increasing transaction ID counter.
    next_txn_id: AtomicU64,
    /// Global commit lock — ensures only one transaction commits at a time.
    commit_lock: Mutex<()>,
    /// Recently committed transactions, kept for conflict detection.
    committed_txns: Mutex<Vec<CommittedTxn>>,
    /// Active transactions, indexed by TxnId.
    active_txns: Mutex<HashMap<TxnId, u64>>,
    /// RW-dependency graph edges for dangerous structure detection.
    rw_deps: Mutex<Vec<RWDependency>>,
}

impl TransactionManager {
    /// Creates a new TransactionManager bound to the given OmniKV instance.
    pub fn new(db: Arc<OmniKV>) -> Self {
        Self {
            db,
            next_txn_id: AtomicU64::new(1),
            commit_lock: Mutex::new(()),
            committed_txns: Mutex::new(Vec::new()),
            active_txns: Mutex::new(HashMap::new()),
            rw_deps: Mutex::new(Vec::new()),
        }
    }

    /// BEGIN — starts a new transaction with a consistent snapshot.
    pub fn begin(&self) -> Transaction {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let read_seq = self.db.snapshot();

        let mut active = self.active_txns.lock().expect("active_txns lock");
        active.insert(txn_id, read_seq);

        Transaction::new(txn_id, read_seq)
    }

    /// GET — reads a key within a transaction's snapshot.
    /// Checks the write_set first (read-your-own-writes), then falls through
    /// to the storage engine at the transaction's snapshot seq.
    pub fn get(&self, txn: &mut Transaction, key: &str) -> Result<Option<String>, OmniError> {
        if txn.state != TxnState::Active {
            return Err(OmniError::IoError("Transaction is not active".into()));
        }

        // Read-your-own-writes: check local write buffer first
        if let Some((value, _ttl)) = txn.write_set.get(key) {
            txn.read_set.insert(key.to_string());
            return Ok(value.clone());
        }

        // Read from storage at our snapshot
        txn.read_set.insert(key.to_string());
        self.db.find(key, txn.read_seq)
    }

    /// SET — buffers a write in the transaction (not yet committed).
    pub fn set(&self, txn: &mut Transaction, key: &str, value: String) -> Result<(), OmniError> {
        if txn.state != TxnState::Active {
            return Err(OmniError::IoError("Transaction is not active".into()));
        }
        txn.write_set.insert(key.to_string(), (Some(value), 0));
        Ok(())
    }

    /// SET_WITH_TTL — buffers a write with TTL.
    pub fn set_with_ttl(
        &self,
        txn: &mut Transaction,
        key: &str,
        value: String,
        ttl: u64,
    ) -> Result<(), OmniError> {
        if txn.state != TxnState::Active {
            return Err(OmniError::IoError("Transaction is not active".into()));
        }
        txn.write_set.insert(key.to_string(), (Some(value), ttl));
        Ok(())
    }

    /// DELETE — buffers a deletion in the transaction.
    pub fn delete(&self, txn: &mut Transaction, key: &str) -> Result<(), OmniError> {
        if txn.state != TxnState::Active {
            return Err(OmniError::IoError("Transaction is not active".into()));
        }
        txn.write_set.insert(key.to_string(), (None, 0));
        Ok(())
    }

    /// COMMIT — validates the transaction and atomically applies all writes.
    ///
    /// Returns the commit sequence number on success, or an error if a
    /// write-write conflict is detected (the caller should retry).
    pub fn commit(&self, txn: &mut Transaction) -> Result<u64, OmniError> {
        if txn.state != TxnState::Active {
            return Err(OmniError::IoError("Transaction is not active".into()));
        }

        if txn.write_set.is_empty() {
            // Read-only transaction — no conflicts possible
            txn.state = TxnState::Committed;
            self.cleanup_txn(txn.id, txn.read_seq);
            return Ok(txn.read_seq);
        }

        // ═══════════════════════════════════════════════════════════════
        // SERIALIZATION POINT: acquire commit lock
        // ═══════════════════════════════════════════════════════════════
        let _guard = self
            .commit_lock
            .lock()
            .map_err(|_| OmniError::LockPoisoned("txn commit lock".into()))?;

        // ═══════════════════════════════════════════════════════════════
        // SSI Conflict Detection with Dangerous Structure Analysis
        // ═══════════════════════════════════════════════════════════════
        let conflict: Option<String> = {
            let committed = self
                .committed_txns
                .lock()
                .map_err(|_| OmniError::LockPoisoned("committed_txns".into()))?;

            let mut rw_deps = self
                .rw_deps
                .lock()
                .map_err(|_| OmniError::LockPoisoned("rw_deps".into()))?;

            let mut found = None;
            'outer: for committed_txn in committed.iter() {
                if committed_txn.commit_seq > txn.read_seq {
                    // Write-write conflict check
                    for key in txn.write_set.keys() {
                        if committed_txn.write_keys.contains(key) {
                            found = Some(format!(
                                "SSI CONFLICT (WW): key '{}' written by txn {} at seq {}",
                                key, committed_txn.txn_id, committed_txn.commit_seq
                            ));
                            break 'outer;
                        }
                    }

                    // Read-write anti-dependency: we read key, they wrote it
                    // This alone is a conflict — abort (PostgreSQL-compatible)
                    for key in &txn.read_set {
                        if committed_txn.write_keys.contains(key) {
                            rw_deps.push(RWDependency {
                                from_txn: txn.id,
                                to_txn: committed_txn.txn_id,
                            });
                            found = Some(format!(
                                "SSI CONFLICT (RW): key '{}' read by us, written by txn {} at seq {}",
                                key, committed_txn.txn_id, committed_txn.commit_seq
                            ));
                            break 'outer;
                        }
                    }

                    // Write-read anti-dependency: we wrote key, they read it
                    for key in txn.write_set.keys() {
                        if committed_txn.read_keys.contains(key) {
                            // Record rw-dependency: committed_txn →rw→ txn
                            rw_deps.push(RWDependency {
                                from_txn: committed_txn.txn_id,
                                to_txn: txn.id,
                            });
                        }
                    }
                }
            }

            // Dangerous structure detection:
            // If txn has BOTH an incoming AND outgoing rw-dependency,
            // it's the "pivot" in T1→rw→Txn→rw→T3 — must abort.
            if found.is_none() {
                let has_incoming = rw_deps.iter().any(|d| d.to_txn == txn.id);
                let has_outgoing = rw_deps.iter().any(|d| d.from_txn == txn.id);
                if has_incoming && has_outgoing {
                    found = Some(format!(
                        "SSI CONFLICT (DANGEROUS STRUCTURE): txn {} is pivot in rw-dependency cycle",
                        txn.id
                    ));
                }
            }

            found
        }; // locks released here

        if let Some(conflict_msg) = conflict {
            txn.state = TxnState::Aborted;
            self.cleanup_txn(txn.id, txn.read_seq);
            return Err(OmniError::IoError(conflict_msg));
        }

        // No conflicts! Build and commit the WriteBatch atomically.
        let mut batch = WriteBatch::new();
        for (key, (value, ttl)) in &txn.write_set {
            match value {
                Some(val) => {
                    if *ttl > 0 {
                        batch.set_with_ttl(key, val.clone(), *ttl)?;
                    } else {
                        batch.set(key, val.clone())?;
                    }
                }
                None => {
                    batch.delete(key)?;
                }
            }
        }

        let commit_seq = self.db.commit_batch(&batch)?;

        // Record this transaction in the committed set for future conflict detection
        {
            let mut committed = self
                .committed_txns
                .lock()
                .map_err(|_| OmniError::LockPoisoned("committed_txns".into()))?;
            committed.push(CommittedTxn {
                txn_id: txn.id,
                commit_seq,
                write_keys: txn.write_set.keys().cloned().collect(),
                read_keys: txn.read_set.clone(),
            });
        }

        txn.state = TxnState::Committed;
        self.cleanup_txn(txn.id, txn.read_seq);

        // Prune old committed transaction records
        self.prune_committed_txns();

        Ok(commit_seq)
    }

    /// ABORT — discards all buffered writes without applying them.
    pub fn abort(&self, txn: &mut Transaction) {
        txn.state = TxnState::Aborted;
        txn.write_set.clear();
        txn.read_set.clear();
        self.cleanup_txn(txn.id, txn.read_seq);
    }

    /// Removes a transaction from the active set and unregisters its snapshot.
    fn cleanup_txn(&self, txn_id: TxnId, read_seq: u64) {
        let mut active = self.active_txns.lock().expect("active_txns");
        active.remove(&txn_id);
        self.db.unregister_snapshot(read_seq);
    }

    /// Prunes committed transaction records that are no longer needed for
    /// conflict detection (all active transactions started after them).
    fn prune_committed_txns(&self) {
        let min_active_seq = {
            let active = self.active_txns.lock().expect("active_txns");
            active.values().copied().min().unwrap_or(u64::MAX)
        };

        let mut committed = self.committed_txns.lock().expect("committed_txns");
        committed.retain(|c| c.commit_seq >= min_active_seq);
    }
}
