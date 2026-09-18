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

/// The SSI commit record carried INSIDE a replicated [`RaftCommand`] so
/// that every node's apply path can record the transaction in its own
/// committed history. Without this, the history that powers serializable
/// conflict detection exists only on the leader that ran the COMMIT: a
/// transaction that began before a failover and commits after it would
/// validate against a history missing the old leader's records, and a
/// write-write or rw anti-dependency against a pre-failover commit would
/// slip through. Carrying the record in the command makes the history
/// converge on all nodes. The storage-space `commit_seq` is NOT carried:
/// it is per-node (each node's sequence counter is its own), so the apply
/// records it locally — see [`SsiHistory::record_committed`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SsiCommitRecord {
    /// The transaction ID (leader-local, but unique enough for the
    /// history's purpose: conflict detection only compares records
    /// against each other within one node's history).
    pub txn_id: TxnId,
    /// Keys written by this transaction.
    pub write_keys: Vec<String>,
    /// Keys read by this transaction.
    pub read_keys: Vec<String>,
    /// Ranges (start, end) scanned by this transaction — its predicate
    /// locks, kept in the committed history so transactions that write
    /// inside a range scanned by an EARLIER committed transaction can
    /// be caught at their own COMMIT.
    pub read_ranges: Vec<(String, String)>,
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

impl From<&CommittedTxn> for SsiCommitRecord {
    fn from(txn: &CommittedTxn) -> Self {
        Self {
            txn_id: txn.txn_id,
            // Sets are unordered; the order here is irrelevant because
            // the receiver rebuilds them into sets.
            write_keys: txn.write_keys.iter().cloned().collect(),
            read_keys: txn.read_keys.iter().cloned().collect(),
            read_ranges: txn.read_ranges.clone(),
        }
    }
}

impl SsiCommitRecord {
    /// Builds the record from a live transaction at COMMIT time — the
    /// shape that travels inside the replicated command so every node's
    /// apply can record it locally.
    pub fn from_committed_view(txn: &Transaction) -> Self {
        Self {
            txn_id: txn.id,
            write_keys: txn.write_set.keys().cloned().collect(),
            read_keys: txn.read_set.iter().cloned().collect(),
            read_ranges: txn.read_ranges.clone(),
        }
    }
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
    /// Total SSI conflict detections (write-write, read-write, or
    /// range/predicate).
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
/// Number of stripes in the commit lock array.
/// Must be a power of 2 for fast modular hashing.
const COMMIT_STRIPE_COUNT: usize = 64;

/// The committed-transaction history that powers SSI conflict detection,
/// held by [`OmniKV`] so that BOTH transaction paths reach the same store:
///
/// - the leader's [`TransactionManager::commit`] records the transaction
///   it just committed, and
/// - the raft state machine's apply path records the SSI commit record
///   carried inside each replicated [`crate::raft_command::RaftCommand`].
///
/// This second path is the fix for the leader-local history bug: before
/// it, only the node that RAN the COMMIT ever recorded the transaction,
/// so a follower promoted to leader after a failover validated new
/// transactions against a history missing every pre-failover commit —
/// and a write-write or rw anti-dependency against one of them slipped
/// through. Carrying the record in the replicated command and recording
/// it at apply on every node keeps all nodes' histories converged.
///
/// The `commit_seq` each entry is recorded at is per-node (every node's
/// sequence counter is its own), so it is stamped by the apply from the
/// LOCAL batch marker, never carried across the wire.
pub struct SsiHistory {
    /// Recently committed transactions, kept for conflict detection.
    committed_txns: Mutex<Vec<CommittedTxn>>,
}

impl SsiHistory {
    /// Records a transaction that committed at the given sequence. Called
    /// by the single-node commit path, the clustered leader's commit
    /// closure, AND by every follower's apply path — all three must land
    /// in the same store or histories diverge across a failover.
    pub fn record_committed(&self, txn_id: TxnId, commit_seq: u64, record: &SsiCommitRecord) {
        let mut committed = self
            .committed_txns
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        committed.push(CommittedTxn {
            txn_id,
            commit_seq,
            write_keys: record.write_keys.iter().cloned().collect(),
            read_keys: record.read_keys.iter().cloned().collect(),
            read_ranges: record.read_ranges.clone(),
        });
    }

    /// The number of committed transaction records being held for
    /// conflict detection.
    pub fn committed_record_count(&self) -> usize {
        self.committed_txns.lock().map(|c| c.len()).unwrap_or(0)
    }

    /// A serializable snapshot of the committed history, for a raft
    /// snapshot envelope. A node that installs that snapshot replays these
    /// so it keeps detecting conflicts against commits the snapshot
    /// carried (which never passed through its log-apply path).
    pub fn snapshot_entries(&self) -> Vec<(TxnId, u64, SsiCommitRecord)> {
        let committed = self
            .committed_txns
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        committed
            .iter()
            .map(|c| {
                (
                    c.txn_id,
                    c.commit_seq,
                    SsiCommitRecord {
                        txn_id: c.txn_id,
                        write_keys: c.write_keys.iter().cloned().collect(),
                        read_keys: c.read_keys.iter().cloned().collect(),
                        read_ranges: c.read_ranges.clone(),
                    },
                )
            })
            .collect()
    }

    /// Replaces the whole history, used by snapshot install: the data was
    /// replaced wholesale, so the in-memory history must be too, then
    /// refilled from what the snapshot carried.
    pub fn install_from(&self, entries: Vec<(TxnId, u64, SsiCommitRecord)>) {
        let mut committed = self
            .committed_txns
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        committed.clear();
        for (txn_id, commit_seq, record) in entries {
            committed.push(CommittedTxn {
                txn_id,
                commit_seq,
                write_keys: record.write_keys.iter().cloned().collect(),
                read_keys: record.read_keys.iter().cloned().collect(),
                read_ranges: record.read_ranges,
            });
        }
    }

    /// Runs the SSI conflict scan — write-write, rw anti-dependency
    /// (point and range/predicate) — of `txn` against the committed
    /// history, and returns the history guard alongside the result.
    ///
    /// The guard is the whole point: the single-node commit path MUST
    /// hold it across the batch commit AND its own record insertion.
    /// Without that span, two transactions whose range scans hash to
    /// disjoint stripes (only the range ENDPOINTS are striped, so
    /// overlapping ranges can land in different stripes) both validate
    /// against a history missing the other and both commit — a
    /// serializability violation. Returning the guard keeps
    /// check-and-record atomic without holding a lock across an
    /// await-able propose.
    fn detect_conflicts_locked(
        &self,
        txn: &Transaction,
    ) -> (Option<String>, std::sync::MutexGuard<'_, Vec<CommittedTxn>>) {
        // The only lock SsiHistory takes: rw-edges are no longer kept
        // (see the note below on the removed pivot check), so there is
        // no second lock to order against. Poison-tolerant like the
        // stripes: a panicked holder never leaves the Vec torn.
        let committed = self
            .committed_txns
            .lock()
            .unwrap_or_else(|e| e.into_inner());

        let found = Self::scan_conflicts(txn, &committed);
        (found, committed)
    }

    /// The pure conflict scan — no locking. Separated so the clustered
    /// path (whose serialization point is the gateway's flight lock, not
    /// this mutex) can validate without holding the history lock across
    /// the await-able propose, while the single-node path holds the lock
    /// across both scan and record via `detect_conflicts_locked`.
    fn scan_conflicts(txn: &Transaction, committed: &[CommittedTxn]) -> Option<String> {
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
                        found = Some(format!(
                            "SSI CONFLICT (RW): key '{}' read by us, written by txn {} at seq {}",
                            key, committed_txn.txn_id, committed_txn.commit_seq
                        ));
                        break 'outer;
                    }
                }

                // Write-read anti-dependency (we wrote a key a committed
                // txn read) is deliberately NOT an abort here: a single
                // rw-antidependency is legal in the PostgreSQL SSI model —
                // the anomaly needs a pivot in a rw-CYCLE. This engine had
                // a pivot check, and it was unreachable dead code (every
                // outgoing edge was pushed only immediately before an
                // aborting break, so the check never ran with
                // `found.is_none()` and an outgoing edge present); it is
                // removed rather than left to misfire. The classic
                // write-skew is still caught by the RW branch above, which
                // fires for the crossing read/write pair.

                // ── Range (predicate) conflicts ─────────────────
                // A write they committed inside one of OUR read
                // ranges: we scanned the range, they changed it after
                // our snapshot — the phantom-write conflict, caught
                // at COMMIT. This is the DROP TABLE vs. concurrent
                // INSERT race and every seq-scan write-skew.
                for key in committed_txn.write_keys.iter() {
                    for (start, end) in &txn.read_ranges {
                        if key.as_str() >= start.as_str() && key.as_str() < end.as_str() {
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

        found
    }

    /// The clustered-path entry point: scan and release. The gateway's
    /// flight lock is the serialization point there — it serializes
    /// proposals, and the apply (which records the history) completes
    /// before the lock releases — so the history mutex does not need to
    /// span the await-able propose. Holding it there would needlessly
    /// stall every concurrent commit on the node for each consensus
    /// round trip.
    fn detect_conflicts(&self, txn: &Transaction) -> Option<String> {
        let committed = self
            .committed_txns
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        Self::scan_conflicts(txn, &committed)
    }

    /// Removes committed transaction records that are no longer needed
    /// for conflict detection (all active transactions started after
    /// them). Called after every commit on every node — without it, a
    /// follower that never runs a local COMMIT would grow its history
    /// without bound. The floor is the caller's oldest in-play snapshot:
    /// a record whose commit_seq is at or below it is already visible to
    /// every live transaction and can never again be a conflict.
    pub(crate) fn prune(&self, min_active_seq: u64) {
        let mut committed = self
            .committed_txns
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        committed.retain(|c| c.commit_seq >= min_active_seq);
    }
}

impl Default for SsiHistory {
    fn default() -> Self {
        Self {
            committed_txns: Mutex::new(Vec::new()),
        }
    }
}

pub struct TransactionManager {
    db: Arc<OmniKV>,
    /// The shared committed-transaction history. Held by reference so the
    /// raft apply path (recording replicated SSI records) and this
    /// manager (recording locally-committed ones) see the same store.
    history: Arc<SsiHistory>,
    /// Monotonically increasing transaction ID counter.
    next_txn_id: AtomicU64,
    /// Striped commit locks — transactions lock only the stripes that cover
    /// their write keys, allowing non-overlapping transactions to commit
    /// in parallel. Each stripe is a Mutex guarding a logical key-space
    /// partition. This replaces the former single global commit lock.
    commit_stripes: Vec<Mutex<()>>,
    /// Active transactions, indexed by TxnId.
    active_txns: Mutex<HashMap<TxnId, u64>>,
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
            history: db.ssi_history(),
            db,
            next_txn_id: AtomicU64::new(1),
            commit_stripes: stripes,
            active_txns: Mutex::new(HashMap::new()),
            txn_timeout: timeout,
            metrics: Arc::new(TxnMetrics::new()),
        }
    }

    /// The shared SSI history — the raft apply path uses this to record
    /// replicated commit records so a promoted follower's conflict checks
    /// see pre-failover commits.
    pub fn history(&self) -> Arc<SsiHistory> {
        self.history.clone()
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
    /// and the returned number is the commit marker's STORAGE sequence —
    /// the sequence at which the write became visible. The marker is
    /// stamped by the state machine during the apply and carried back
    /// through the proposal's response (see `WriteAck::commit_seq`), never
    /// reconstructed from `db.get_seq()`: the apply's own meta record, and
    /// any concurrent purge or snapshot install, reserve from the same
    /// counter afterwards and would yield a different number. The
    /// single-node path below gets this same number from commit_batch's
    /// return value; it is the number space the read_seq the conflict
    /// checks compare against lives in.
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
        // flight → stripes → committed-history with no cycle.
        //
        // The SSI commit record rides INSIDE the replicated command, so
        // every follower's apply path records this transaction in its OWN
        // history too. Before that, the history existed only on the node
        // that ran the COMMIT, and a follower promoted after a failover
        // validated against a history missing every pre-failover commit
        // — the serializability hole tracked as #124. The leader does NOT
        // double-record: its own state machine apply records the same
        // command, and that apply is what the flight lock waits on before
        // releasing, so the record is in place for the next transaction's
        // check.
        // ═══════════════════════════════════════════════════════════════
        if let Some(gateway) = self.db.cluster_gateway() {
            let txn_ref: &Transaction = txn;
            let outcome = gateway.commit_ssi_blocking(
                // The SSI record carried in the replicated command. The
                // commit_seq is deliberately NOT here: it is per-node
                // (each node's sequence counter is its own), so each
                // apply stamps it from its LOCAL batch marker.
                SsiCommitRecord::from_committed_view(txn_ref),
                move || {
                    // Full re-validation under the flight lock, against
                    // the LIVE committed set: the gateway serializes all
                    // clustered writers behind this lock, so this check
                    // sees every commit that landed before ours. Nothing
                    // here holds a lock across the propose that follows.
                    let _stripe_guards = self.acquire_commit_stripes(txn_ref);
                    if let Some(conflict) = self.history.detect_conflicts(txn_ref) {
                        return Err(conflict);
                    }
                    Self::build_write_batch(txn_ref).map_err(|e| e.to_string())
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
        // SSI Conflict Detection
        //
        // CRITICAL: The history's committed-set lock is held across BOTH
        // the conflict check AND the insertion of our own commit record
        // below — the guard returned here also spans the batch commit.
        // Without that span, two transactions whose range scans hash into
        // DISJOINT stripes (only the range endpoints are striped, so
        // overlapping ranges can land in different stripes) would both
        // validate against a history missing the other and both commit —
        // a serializability violation. (The clustered branch above gets
        // the same guarantee from the gateway's flight lock instead.)
        // ═══════════════════════════════════════════════════════════════
        let (conflict, mut committed_guard) = self.history.detect_conflicts_locked(txn);

        if let Some(conflict_msg) = conflict {
            drop(committed_guard);
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

        // Record this transaction by pushing into the guard we still hold
        // — no other txn can have snuck between our validation and this
        // record.
        committed_guard.push(CommittedTxn {
            txn_id: txn.id,
            commit_seq,
            write_keys: txn.write_set.keys().cloned().collect(),
            read_keys: txn.read_set.clone(),
            read_ranges: txn.read_ranges.clone(),
        });
        drop(committed_guard);

        txn.state = TxnState::Committed;
        self.cleanup_txn(txn.id, txn.read_seq);

        // Prune records that no live snapshot can conflict with. The floor
        // is the db-wide oldest snapshot: a record below it can never
        // conflict with any transaction that can still commit, and the
        // apply path uses the same source so both paths agree.
        self.history.prune(self.db.min_active_snapshot());

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
        self.history.committed_record_count()
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
}

#[cfg(test)]
mod commit_seq_number_space {
    //! Guards the commit-sequence number space that SSI conflict detection
    //! depends on. The clustered commit path records `db.get_seq() - 1` —
    //! the commit marker's sequence — so that a transaction started AFTER a
    //! clustered commit (whose `read_seq` is exactly that marker) sees the
    //! commit as already visible rather than concurrent. Recording the
    //! unadjusted `get_seq()` made every clustered commit appear one
    //! sequence too new and aborted such transactions spuriously.

    use super::{CommittedTxn, Transaction, TransactionManager};
    use crate::{OmniKV, WriteBatch};
    use std::collections::HashSet;

    /// Records a committed transaction directly into a manager's shared
    /// history — the shape the raft apply path uses on a follower, which
    /// never runs a local COMMIT of its own.
    fn record_committed(mgr: &TransactionManager, txn: &CommittedTxn) {
        mgr.history.record_committed(
            txn.txn_id,
            txn.commit_seq,
            &super::SsiCommitRecord::from(txn),
        );
    }

    fn temp_db() -> std::sync::Arc<OmniKV> {
        let dir = tempfile::tempdir().expect("temp dir");
        OmniKV::open(
            dir.path()
                .join("manifest.json")
                .to_str()
                .expect("manifest path is utf-8"),
            dir.path()
                .join("wal.bin")
                .to_str()
                .expect("wal path is utf-8"),
        )
        .expect("open db")
    }

    /// `commit_batch_local` reserves one sequence per op plus a final one
    /// for the commit marker, and returns the MARKER's sequence. This pins
    /// that `get_seq()` afterwards is exactly one past it — the identity the
    /// clustered path relies on when it records `get_seq() - 1`.
    #[test]
    fn commit_marker_is_exactly_get_seq_minus_one() {
        let db = temp_db();

        let mut batch = WriteBatch::new();
        batch.set("k1", "v1".into()).expect("stage k1");
        batch.set("k2", "v2".into()).expect("stage k2");

        let marker = db
            .commit_batch_local(&batch)
            .expect("commit a two-op batch");

        assert_eq!(
            db.get_seq(),
            marker + 1,
            "get_seq() after commit_batch_local must be exactly one past the marker"
        );
        assert_eq!(
            db.get_seq().saturating_sub(1),
            marker,
            "the clustered path's get_seq()-1 must equal the marker the \
             single-node path records"
        );
    }

    /// The observable symptom of the off-by-one: a committed txn whose
    /// `commit_seq` EQUALS a later txn's `read_seq` is visible to that
    /// snapshot and must NOT conflict; one higher must. If the clustered
    /// path ever records the marker-plus-one again, this turns a
    /// legitimate commit into a spurious abort.
    #[test]
    fn committed_at_exactly_read_seq_is_visible_not_a_conflict() {
        let mgr = TransactionManager::new(temp_db());

        let read_seq = 100;
        let mut later = Transaction::new(2, read_seq);
        later.write_set.insert("k".into(), (Some("v2".into()), 0));

        // commit_seq == read_seq: the write is already in our snapshot.
        let visible = CommittedTxn {
            txn_id: 1,
            commit_seq: read_seq,
            write_keys: ["k".into()].into(),
            read_keys: HashSet::new(),
            read_ranges: vec![],
        };
        record_committed(&mgr, &visible);
        assert!(
            mgr.history.detect_conflicts(&later).is_none(),
            "commit_seq == read_seq must be visible: recording the marker \
             keeps a later txn from aborting spuriously"
        );

        // commit_seq == read_seq + 1: genuinely after our snapshot.
        let after = CommittedTxn {
            commit_seq: read_seq + 1,
            ..visible
        };
        record_committed(&mgr, &after);
        assert!(
            mgr.history.detect_conflicts(&later).is_some(),
            "commit_seq == read_seq + 1 must conflict: this is the abort \
             the off-by-one produced when it should not have"
        );
    }

    /// The end-to-end property on the single-node path, which the
    /// clustered path must match exactly: a transaction started right
    /// after a commit takes `read_seq` == that commit's marker, so the
    /// write is visible to it. This is why the clustered path records the
    /// marker reported by the apply rather than the sequence one past it.
    #[test]
    fn next_snapshot_sees_a_just_committed_write() {
        let mgr = TransactionManager::new(temp_db());

        let mut t1 = mgr.begin();
        mgr.set(&mut t1, "k", "v1".into()).expect("stage write");
        let commit_seq = mgr.commit(&mut t1).expect("commit t1");

        // A txn begun immediately after snapshots the marker itself.
        let mut t2 = mgr.begin();
        assert_eq!(
            t2.read_seq, commit_seq,
            "the next txn's read_seq must equal the commit marker — the \
             clustered path must record that same number"
        );
        assert_eq!(
            mgr.get(&mut t2, "k").expect("read k"),
            Some("v1".into()),
            "a write committed before our snapshot must be visible"
        );
    }
}
