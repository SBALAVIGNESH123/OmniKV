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
//!    committed a write to that key AFTER our snapshot. If yes → ABORT.
//!    c. If no conflicts, commit all buffered writes atomically via WriteBatch.
//!    d. Release the lock.
//!
//! This guarantees Serializable isolation — the strongest level.
//!
//! ## Production Features
//!
//! - **Transaction timeouts**: Long-running transactions are automatically aborted.
//! - **Savepoints**: Partial rollback to named checkpoints within a transaction.
//! - **Metrics**: Commit/abort/conflict counters for observability.
//! - **RW-dependency pruning**: Bounded memory usage for the dependency graph.
//! - **Dangerous structure detection**: PostgreSQL-compatible SSI cycle detection.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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

/// A savepoint within a transaction — captures the write_set and read_set
/// at a specific point so the transaction can partially roll back.
#[derive(Debug, Clone)]
pub struct Savepoint {
    /// Name of the savepoint.
    pub name: String,
    /// Snapshot of the write_set at the time of the savepoint.
    write_set_snapshot: HashMap<String, (Option<String>, u64)>,
    /// Snapshot of the read_set at the time of the savepoint.
    read_set_snapshot: HashSet<String>,
    /// Snapshot of the read ranges at the time of the savepoint.
    read_ranges_snapshot: Vec<(String, String)>,
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
    /// Ranges (start, end) scanned during this transaction — predicate
    /// locks. A key written by a concurrently committed transaction
    /// that falls in one of our read ranges aborts our COMMIT exactly
    /// like a point-read conflict: this is what stops phantom writes
    /// from sneaking past a scan — and a concurrently inserted row from
    /// surviving a DROP TABLE that scanned the same range earlier.
    pub read_ranges: Vec<(String, String)>,
    /// Buffered writes: key → (value, ttl). None value = delete.
    pub write_set: HashMap<String, (Option<String>, u64)>,
    /// When this transaction was started.
    pub started_at: Instant,
    /// Stack of savepoints for partial rollback.
    pub savepoints: Vec<Savepoint>,
}

impl Transaction {
    fn new(id: TxnId, read_seq: u64) -> Self {
        Self {
            id,
            read_seq,
            state: TxnState::Active,
            read_set: HashSet::new(),
            read_ranges: Vec::new(),
            write_set: HashMap::new(),
            started_at: Instant::now(),
            savepoints: Vec::new(),
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
    /// Ranges (start, end) scanned by this transaction — its predicate
    /// locks, kept in the committed history so transactions that write
    /// inside a range scanned by an EARLIER committed transaction can
    /// be caught at their own COMMIT.
    read_ranges: Vec<(String, String)>,
}

/// RW-dependency edge: T_from read a key that T_to later wrote.
/// PostgreSQL calls these "rw-antidependencies" or "SIREAD locks".
#[derive(Debug, Clone)]
struct RWDependency {
    from_txn: TxnId,
    to_txn: TxnId,
}

/// Observable metrics for the transaction engine.
/// All counters are monotonically increasing atomics.
pub struct TxnMetrics {
    /// Total transactions started (begin() calls).
    pub txns_started: AtomicU64,
    /// Total transactions successfully committed.
    pub txns_committed: AtomicU64,
    /// Total transactions aborted (explicit or conflict).
    pub txns_aborted: AtomicU64,
    /// Total SSI conflict detections (WW, RW, or dangerous structure).
    pub conflicts_detected: AtomicU64,
    /// Total savepoints created.
    pub savepoints_created: AtomicU64,
    /// Total savepoint rollbacks performed.
    pub savepoints_rolled_back: AtomicU64,
    /// Total transactions timed out.
    pub txns_timed_out: AtomicU64,
}

impl TxnMetrics {
    fn new() -> Self {
        Self {
            txns_started: AtomicU64::new(0),
            txns_committed: AtomicU64::new(0),
            txns_aborted: AtomicU64::new(0),
            conflicts_detected: AtomicU64::new(0),
            savepoints_created: AtomicU64::new(0),
            savepoints_rolled_back: AtomicU64::new(0),
            txns_timed_out: AtomicU64::new(0),
        }
    }

    /// Returns a snapshot of all metrics as a HashMap for export.
    pub fn snapshot(&self) -> HashMap<String, u64> {
        let mut m = HashMap::new();
        m.insert(
            "txns_started".into(),
            self.txns_started.load(Ordering::Relaxed),
        );
        m.insert(
            "txns_committed".into(),
            self.txns_committed.load(Ordering::Relaxed),
        );
        m.insert(
            "txns_aborted".into(),
            self.txns_aborted.load(Ordering::Relaxed),
        );
        m.insert(
            "conflicts_detected".into(),
            self.conflicts_detected.load(Ordering::Relaxed),
        );
        m.insert(
            "savepoints_created".into(),
            self.savepoints_created.load(Ordering::Relaxed),
        );
        m.insert(
            "savepoints_rolled_back".into(),
            self.savepoints_rolled_back.load(Ordering::Relaxed),
        );
        m.insert(
            "txns_timed_out".into(),
            self.txns_timed_out.load(Ordering::Relaxed),
        );
        m
    }
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
/// Number of stripes in the commit lock array.
/// Must be a power of 2 for fast modular hashing.
const COMMIT_STRIPE_COUNT: usize = 64;

pub struct TransactionManager {
    db: Arc<OmniKV>,
    /// Monotonically increasing transaction ID counter.
    next_txn_id: AtomicU64,
    /// Striped commit locks — transactions lock only the stripes that cover
    /// their write keys, allowing non-overlapping transactions to commit
    /// in parallel. Each stripe is a Mutex guarding a logical key-space
    /// partition. This replaces the former single global commit lock.
    commit_stripes: Vec<Mutex<()>>,
    /// Recently committed transactions, kept for conflict detection.
    committed_txns: Mutex<Vec<CommittedTxn>>,
    /// Active transactions, indexed by TxnId.
    active_txns: Mutex<HashMap<TxnId, u64>>,
    /// RW-dependency graph edges for dangerous structure detection.
    rw_deps: Mutex<Vec<RWDependency>>,
    /// Transaction timeout duration. Transactions older than this are
    /// rejected at commit time and can be detected via check_timeouts().
    txn_timeout: Duration,
    /// Observable metrics for monitoring and alerting.
    pub metrics: Arc<TxnMetrics>,
}

impl TransactionManager {
    /// Creates a new TransactionManager bound to the given OmniKV instance.
    /// Uses a default transaction timeout of 30 seconds.
    pub fn new(db: Arc<OmniKV>) -> Self {
        Self::with_timeout(db, Duration::from_secs(30))
    }

    /// Creates a new TransactionManager with a custom transaction timeout.
    pub fn with_timeout(db: Arc<OmniKV>, timeout: Duration) -> Self {
        let stripes = (0..COMMIT_STRIPE_COUNT).map(|_| Mutex::new(())).collect();
        Self {
            db,
            next_txn_id: AtomicU64::new(1),
            commit_stripes: stripes,
            committed_txns: Mutex::new(Vec::new()),
            active_txns: Mutex::new(HashMap::new()),
            rw_deps: Mutex::new(Vec::new()),
            txn_timeout: timeout,
            metrics: Arc::new(TxnMetrics::new()),
        }
    }

    /// BEGIN — starts a new transaction with a consistent snapshot.
    pub fn begin(&self) -> Transaction {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let read_seq = self.db.snapshot();

        let mut active = self.active_txns.lock().expect("active_txns lock");
        active.insert(txn_id, read_seq);

        self.metrics.txns_started.fetch_add(1, Ordering::Relaxed);

        Transaction::new(txn_id, read_seq)
    }

    /// GET — reads a key within a transaction's snapshot.
    /// Checks the write_set first (read-your-own-writes), then falls through
    /// to the storage engine at the transaction's snapshot seq.
    pub fn get(&self, txn: &mut Transaction, key: &str) -> Result<Option<String>, OmniError> {
        if txn.state != TxnState::Active {
            return Err(OmniError::IoError("Transaction is not active".into()));
        }

        // Enforce transaction timeout on read operations
        if txn.started_at.elapsed() > self.txn_timeout {
            self.metrics.txns_timed_out.fetch_add(1, Ordering::Relaxed);
            txn.state = TxnState::Aborted;
            self.cleanup_txn(txn.id, txn.read_seq);
            return Err(OmniError::IoError(format!(
                "Transaction {} timed out after {:?}",
                txn.id, self.txn_timeout
            )));
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

    /// Records a range read ([start, end)) for SSI conflict detection —
    /// a predicate lock. Called for every scan serving a statement in an
    /// explicit transaction (SQL table scans, legacy KV range selects,
    /// DROP TABLE's row collection), so a write committed inside the
    /// range after this transaction's snapshot aborts its COMMIT:
    /// phantom-write protection, the range counterpart of read_set.
    pub fn record_read_range(
        &self,
        txn: &mut Transaction,
        start_key: &str,
        end_key: &str,
    ) -> Result<(), OmniError> {
        if txn.state != TxnState::Active {
            return Err(OmniError::IoError("Transaction is not active".into()));
        }
        txn.read_ranges
            .push((start_key.to_string(), end_key.to_string()));
        Ok(())
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

    /// SAVEPOINT — creates a named savepoint that captures the current
    /// transaction state. You can later rollback to this savepoint to
    /// undo writes made after it, without aborting the entire transaction.
    pub fn savepoint(&self, txn: &mut Transaction, name: &str) -> Result<(), OmniError> {
        if txn.state != TxnState::Active {
            return Err(OmniError::IoError("Transaction is not active".into()));
        }

        txn.savepoints.push(Savepoint {
            name: name.to_string(),
            write_set_snapshot: txn.write_set.clone(),
            read_set_snapshot: txn.read_set.clone(),
            read_ranges_snapshot: txn.read_ranges.clone(),
        });

        self.metrics
            .savepoints_created
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// ROLLBACK TO SAVEPOINT — rolls back the transaction's write_set
    /// and read_set to the state captured at the named savepoint.
    /// Savepoints created after the target are discarded.
    pub fn rollback_to_savepoint(
        &self,
        txn: &mut Transaction,
        name: &str,
    ) -> Result<(), OmniError> {
        if txn.state != TxnState::Active {
            return Err(OmniError::IoError("Transaction is not active".into()));
        }

        // Find the savepoint by name (search from most recent)
        let pos = txn
            .savepoints
            .iter()
            .rposition(|sp| sp.name == name)
            .ok_or_else(|| OmniError::IoError(format!("Savepoint '{}' not found", name)))?;

        // Restore write_set and read_set from the savepoint
        let savepoint = txn.savepoints[pos].clone();
        txn.write_set = savepoint.write_set_snapshot;
        txn.read_set = savepoint.read_set_snapshot;
        txn.read_ranges = savepoint.read_ranges_snapshot;

        // Discard all savepoints after (and including) the target
        txn.savepoints.truncate(pos);

        self.metrics
            .savepoints_rolled_back
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// RELEASE SAVEPOINT — removes a savepoint without rolling back.
    /// This is an optimization — the savepoint's state is no longer needed.
    pub fn release_savepoint(&self, txn: &mut Transaction, name: &str) -> Result<(), OmniError> {
        if txn.state != TxnState::Active {
            return Err(OmniError::IoError("Transaction is not active".into()));
        }

        let pos = txn
            .savepoints
            .iter()
            .rposition(|sp| sp.name == name)
            .ok_or_else(|| OmniError::IoError(format!("Savepoint '{}' not found", name)))?;

        txn.savepoints.remove(pos);
        Ok(())
    }

    /// COMMIT — validates the transaction and atomically applies all writes.
    ///
    /// Returns the commit sequence number on success, or an error if a
    /// write-write conflict is detected (the caller should retry).
    ///
    /// Clustered mode: consensus is the commit. The gateway serializes
    /// validate → propose → local apply → record under its flight lock,
    /// and the returned number is a STORAGE sequence number
    /// (db.get_seq() after the local apply) — the same number space as
    /// single-node commits and as the read_seq the conflict checks
    /// compare against.
    pub fn commit(&self, txn: &mut Transaction) -> Result<u64, OmniError> {
        if txn.state != TxnState::Active {
            return Err(OmniError::IoError("Transaction is not active".into()));
        }

        // Enforce transaction timeout
        if txn.started_at.elapsed() > self.txn_timeout {
            self.metrics.txns_timed_out.fetch_add(1, Ordering::Relaxed);
            self.metrics.txns_aborted.fetch_add(1, Ordering::Relaxed);
            txn.state = TxnState::Aborted;
            self.cleanup_txn(txn.id, txn.read_seq);
            return Err(OmniError::IoError(format!(
                "Transaction {} timed out after {:?}",
                txn.id, self.txn_timeout
            )));
        }

        if txn.write_set.is_empty() {
            // Read-only transaction — no conflicts possible
            txn.state = TxnState::Committed;
            self.cleanup_txn(txn.id, txn.read_seq);
            self.metrics.txns_committed.fetch_add(1, Ordering::Relaxed);
            return Ok(txn.read_seq);
        }

        // ═══════════════════════════════════════════════════════════════
        // Clustered: consensus IS the commit — and the SERIALIZATION
        // POINT moves with it. Everything (stripes, conflict check,
        // propose, local apply, record) runs inside the gateway's single
        // flight lock, so no other proposal can land between this
        // transaction's validation and its commit record — the clustered
        // counterpart of holding the committed-set lock across
        // check-and-insert in single-node mode below. Engine locks are
        // taken and released INSIDE that lock, never held across the
        // await-able propose, so the global lock order is always
        // flight → stripes → committed → rw_deps with no cycle.
        // ═══════════════════════════════════════════════════════════════
        if let Some(gateway) = self.db.cluster_gateway() {
            let txn_ref: &Transaction = txn;
            let outcome = gateway.commit_ssi_blocking(
                move || {
                    // Full re-validation under the flight lock, against
                    // the LIVE committed set: the gateway serializes all
                    // clustered writers behind this lock, so this check
                    // sees every commit that landed before ours. Nothing
                    // here holds a lock across the propose that follows.
                    let _stripe_guards = self.acquire_commit_stripes(txn_ref);
                    let committed = self.committed_txns.lock().map_err(|_| {
                        OmniError::LockPoisoned("committed_txns".into()).to_string()
                    })?;
                    if let Some(conflict) = self.ssi_detect_conflicts(txn_ref, &committed) {
                        return Err(conflict);
                    }
                    drop(committed);
                    Self::build_write_batch(txn_ref).map_err(|e| e.to_string())
                },
                move |_raft_index| {
                    // Post-consensus, still under the flight lock: the
                    // local apply just completed, so db.get_seq() is past
                    // every write in our batch. Recording that STORAGE
                    // seq (not the raft log index) keeps the committed
                    // history in the same number space as read_seq — the
                    // space the conflict checks compare in.
                    let commit_seq = self.db.get_seq();
                    let mut committed = self
                        .committed_txns
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    committed.push(CommittedTxn {
                        txn_id: txn_ref.id,
                        commit_seq,
                        write_keys: txn_ref.write_set.keys().cloned().collect(),
                        read_keys: txn_ref.read_set.clone(),
                        read_ranges: txn_ref.read_ranges.clone(),
                    });
                    drop(committed);
                    self.prune_committed_txns();
                    commit_seq
                },
            );

            return match outcome {
                Ok(commit_seq) => {
                    txn.state = TxnState::Committed;
                    self.cleanup_txn(txn.id, txn.read_seq);
                    self.metrics.txns_committed.fetch_add(1, Ordering::Relaxed);
                    Ok(commit_seq)
                }
                Err(msg) => {
                    txn.state = TxnState::Aborted;
                    self.cleanup_txn(txn.id, txn.read_seq);
                    if msg.starts_with("SSI CONFLICT") {
                        self.metrics
                            .conflicts_detected
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    self.metrics.txns_aborted.fetch_add(1, Ordering::Relaxed);
                    Err(OmniError::IoError(msg))
                }
            };
        }

        // ═══════════════════════════════════════════════════════════════
        // SERIALIZATION POINT (single-node): acquire striped commit locks
        //
        // Sorted stripe indices over the txn's write keys + read keys +
        // read-range endpoints (deadlock prevention via lock ordering);
        // held across validation+commit. Non-overlapping transactions
        // proceed fully in parallel.
        // ═══════════════════════════════════════════════════════════════
        let _guards = self.acquire_commit_stripes(txn);

        // ═══════════════════════════════════════════════════════════════
        // SSI Conflict Detection with Dangerous Structure Analysis
        //
        // CRITICAL: We hold committed_txns lock across BOTH the conflict
        // check AND the insertion of our own commit record. This prevents
        // a TOCTOU race where two concurrent txns on the same key both
        // pass validation before either records its commit.
        // ═══════════════════════════════════════════════════════════════
        let mut committed = self
            .committed_txns
            .lock()
            .map_err(|_| OmniError::LockPoisoned("committed_txns".into()))?;

        let conflict: Option<String> = self.ssi_detect_conflicts(txn, &committed);

        if let Some(conflict_msg) = conflict {
            drop(committed); // release before cleanup
            txn.state = TxnState::Aborted;
            self.cleanup_txn(txn.id, txn.read_seq);
            self.metrics
                .conflicts_detected
                .fetch_add(1, Ordering::Relaxed);
            self.metrics.txns_aborted.fetch_add(1, Ordering::Relaxed);
            return Err(OmniError::IoError(conflict_msg));
        }

        // No conflicts! Build the batch the SSI engine would apply.
        let batch = Self::build_write_batch(txn)?;
        let commit_seq = self.db.commit_batch(&batch)?;

        // Record this transaction in the committed set — pushing into
        // the guard we still hold, so no other txn can sneak between
        // our validation and our record.
        committed.push(CommittedTxn {
            txn_id: txn.id,
            commit_seq,
            write_keys: txn.write_set.keys().cloned().collect(),
            read_keys: txn.read_set.clone(),
            read_ranges: txn.read_ranges.clone(),
        });
        drop(committed); // release committed_txns lock

        txn.state = TxnState::Committed;
        self.cleanup_txn(txn.id, txn.read_seq);

        // Prune old committed transaction records AND stale rw-dependencies
        self.prune_committed_txns();

        self.metrics.txns_committed.fetch_add(1, Ordering::Relaxed);

        Ok(commit_seq)
    }

    /// ABORT — discards all buffered writes without applying them.
    pub fn abort(&self, txn: &mut Transaction) {
        txn.state = TxnState::Aborted;
        txn.write_set.clear();
        txn.read_set.clear();
        txn.read_ranges.clear();
        txn.savepoints.clear();
        self.cleanup_txn(txn.id, txn.read_seq);
        self.metrics.txns_aborted.fetch_add(1, Ordering::Relaxed);
    }

    /// CHECK_TIMEOUTS — returns a list of timed-out active transaction IDs.
    /// Production systems should call this periodically to detect and log
    /// stuck transactions. The transactions are NOT automatically aborted;
    /// they will be rejected at the next get() or commit() call.
    pub fn check_timeouts(&self) -> Vec<TxnId> {
        let active = self.active_txns.lock().expect("active_txns lock");
        // We can't check started_at from here (it's on the Transaction struct),
        // but we track active txn IDs. The timeout enforcement happens in
        // get() and commit() which have access to the Transaction.
        // This method returns all active txn IDs for external monitoring.
        active.keys().copied().collect()
    }

    /// Returns the number of currently active transactions.
    pub fn active_count(&self) -> usize {
        self.active_txns.lock().map(|a| a.len()).unwrap_or(0)
    }

    /// Returns the number of committed transaction records being held
    /// for conflict detection.
    pub fn committed_record_count(&self) -> usize {
        self.committed_txns.lock().map(|c| c.len()).unwrap_or(0)
    }

    /// Returns the number of RW-dependency edges in the graph.
    pub fn rw_dep_count(&self) -> usize {
        self.rw_deps.lock().map(|d| d.len()).unwrap_or(0)
    }

    /// Acquires the striped commit locks covering a transaction's write
    /// keys, read keys, and read-range endpoints, in sorted order
    /// (deadlock prevention via lock ordering). The single-node path
    /// holds them across validate+commit+record; the clustered path
    /// takes them inside the gateway's flight lock and drops them
    /// before the propose — they must never be held across an
    /// await-able call.
    fn acquire_commit_stripes(&self, txn: &Transaction) -> Vec<std::sync::MutexGuard<'_, ()>> {
        let mut stripe_indices: Vec<usize> = txn
            .write_set
            .keys()
            .chain(txn.read_set.iter())
            .chain(txn.read_ranges.iter().flat_map(|(s, e)| [s, e]))
            .map(|k| Self::stripe_index_for_key(k))
            .collect();
        stripe_indices.sort_unstable();
        stripe_indices.dedup();
        stripe_indices
            .iter()
            .map(|&i| {
                self.commit_stripes[i]
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
            })
            .collect()
    }

    /// The stripe a key belongs to (djb2-style hash). The mapping only
    /// has to be stable within a process: stripes serialize concurrent
    /// commits, they persist nothing.
    fn stripe_index_for_key(key: &str) -> usize {
        let mut h: u64 = 5381;
        for b in key.bytes() {
            h = h.wrapping_mul(33).wrapping_add(b as u64);
        }
        (h as usize) % COMMIT_STRIPE_COUNT
    }

    /// The SSI conflict scan over committed history: write-write,
    /// read-write anti-dependencies (point and range/predicate), and
    /// the dangerous-structure pivot check. Soft rw-edges are recorded
    /// into the dependency graph for later pivot detection. The caller
    /// holds the serialization point — the committed_txns lock in
    /// single-node mode, the gateway's flight lock in clustered mode —
    /// so check-and-record is atomic against other committers, and
    /// `committed` is always the LIVE set, never a pre-lock snapshot.
    fn ssi_detect_conflicts(
        &self,
        txn: &Transaction,
        committed: &[CommittedTxn],
    ) -> Option<String> {
        // Poison-tolerant like the stripes: rw_deps is append-only, so
        // a panicked holder never leaves it torn.
        let mut rw_deps = self.rw_deps.lock().unwrap_or_else(|e| e.into_inner()); // released on return

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

                // ── Range (predicate) conflicts ─────────────────
                // A write they committed inside one of OUR read
                // ranges: we scanned the range, they changed it after
                // our snapshot — the phantom-write conflict, caught
                // at COMMIT. This is the DROP TABLE vs. concurrent
                // INSERT race and every seq-scan write-skew.
                for key in committed_txn.write_keys.iter() {
                    for (start, end) in &txn.read_ranges {
                        if key.as_str() >= start.as_str() && key.as_str() < end.as_str() {
                            rw_deps.push(RWDependency {
                                from_txn: txn.id,
                                to_txn: committed_txn.txn_id,
                            });
                            found = Some(format!(
                                "SSI CONFLICT (RANGE): key '{}' written by txn {} at seq {} inside our read range [{}, {}]",
                                key, committed_txn.txn_id, committed_txn.commit_seq, start, end
                            ));
                            break 'outer;
                        }
                    }
                }

                // Our writes inside one of THEIR committed read
                // ranges: they scanned the range, we changed it
                // after their snapshot — the mirror image, so the
                // race cannot slip through by ordering alone.
                for key in txn.write_set.keys() {
                    for (start, end) in &committed_txn.read_ranges {
                        if key.as_str() >= start.as_str() && key.as_str() < end.as_str() {
                            rw_deps.push(RWDependency {
                                from_txn: committed_txn.txn_id,
                                to_txn: txn.id,
                            });
                            found = Some(format!(
                                "SSI CONFLICT (RANGE): our key '{}' falls in txn {}'s read range [{}, {}] (seq {})",
                                key, committed_txn.txn_id, start, end, committed_txn.commit_seq
                            ));
                            break 'outer;
                        }
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
    }

    /// Builds the atomic WriteBatch from a transaction's buffered
    /// writes — the unit of apply in single-node mode and the single
    /// replicated raft entry in clustered mode.
    fn build_write_batch(txn: &Transaction) -> Result<WriteBatch, OmniError> {
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
        Ok(batch)
    }

    /// Removes a transaction from the active set and unregisters its snapshot.
    fn cleanup_txn(&self, txn_id: TxnId, read_seq: u64) {
        let mut active = self.active_txns.lock().expect("active_txns");
        active.remove(&txn_id);
        self.db.unregister_snapshot(read_seq);
    }

    /// Prunes committed transaction records that are no longer needed for
    /// conflict detection (all active transactions started after them).
    /// Also prunes RW-dependency edges that reference pruned transactions,
    /// preventing unbounded memory growth.
    fn prune_committed_txns(&self) {
        let min_active_seq = {
            let active = self.active_txns.lock().expect("active_txns");
            active.values().copied().min().unwrap_or(u64::MAX)
        };

        // Collect txn_ids that will be pruned
        let pruned_txn_ids: HashSet<TxnId> = {
            let committed = self.committed_txns.lock().expect("committed_txns");
            committed
                .iter()
                .filter(|c| c.commit_seq < min_active_seq)
                .map(|c| c.txn_id)
                .collect()
        };

        // Prune committed transaction records
        {
            let mut committed = self.committed_txns.lock().expect("committed_txns");
            committed.retain(|c| c.commit_seq >= min_active_seq);
        }

        // Prune RW-dependency edges that reference pruned transactions.
        // This is critical — without this, rw_deps grows unboundedly.
        if !pruned_txn_ids.is_empty() {
            let mut rw_deps = self.rw_deps.lock().expect("rw_deps");
            rw_deps.retain(|dep| {
                !pruned_txn_ids.contains(&dep.from_txn) && !pruned_txn_ids.contains(&dep.to_txn)
            });
        }
    }
}
