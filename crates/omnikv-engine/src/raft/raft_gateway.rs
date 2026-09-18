//! The cluster write gateway — the single funnel every mutating client
//! request goes through in clustered mode.
//!
//! etcd's model, on openraft: a client write is serialized through ONE
//! in-flight proposal on the leader. Holding the flight lock across
//! propose → commit → local apply means the next write's validation (SSI
//! conflict checks, uniqueness, everything the single-node engine does)
//! always runs against fully-applied committed state — there is no
//! window where a proposal is accepted but not yet visible to the next
//! transaction's checks. The ack returned to the client therefore means
//! exactly what it means in single-node mode: the write is durable on a
//! quorum AND applied everywhere, including here.
//!
//! Followers do not accept client writes; the proposal error carries the
//! leader's identity so the client can reconnect there.

use crate::raft_command::RaftCommand;
use crate::raft_impl::OmniRaft;

/// What a successful proposal tells the caller.
#[derive(Debug, Clone, Copy)]
pub struct WriteAck {
    /// The log index the command was replicated and applied at.
    pub index: u64,
    /// The storage commit marker the entry's local apply reported — the
    /// sequence at which the write became visible. Carried from the
    /// state-machine apply response so callers never reconstruct it from
    /// the mutable global counter afterwards (a concurrent purge or
    /// snapshot install can shift that counter in between).
    pub commit_seq: u64,
}

/// The server-wide handle for cluster writes. Cheap to clone via `Arc`.
pub struct ClusterGateway {
    raft: OmniRaft,
    /// One mutating command in flight: proposal, commit, and local apply
    /// complete before the next proposal starts. This is what makes the
    /// leader's committed view gap-free for the next write's checks.
    flight: tokio::sync::Mutex<()>,
    /// The DEDICATED consensus runtime. Every openraft task (heartbeats,
    /// elections, replication, the state-machine apply loop), the raft
    /// RPC listener, and every proposal run here — never on the
    /// client-facing server runtime. This is the deadlock fix: async
    /// REST/QUIC handlers that call the blocking facades park their own
    /// runtime's workers on a channel while consensus proceeds on this
    /// separate runtime, so no number of concurrent client writes can
    /// starve the tasks that must complete their proposals.
    ///
    /// Larger than the old 2-thread gateway pool: it also carries the
    /// RPC listener and openraft's replication workers now.
    consensus_rt: tokio::runtime::Runtime,
    /// The consensus runtime's handle — how the server boots the openraft
    /// node (its internal tasks adopt the runtime context current during
    /// `Raft::new`) and spawns the raft listener onto the same runtime.
    pub consensus_handle: tokio::runtime::Handle,
}

impl ClusterGateway {
    /// Builds the shared consensus runtime — the same configuration
    /// [`Self::new`] bakes in. The server's cluster boot calls this, runs
    /// the openraft node construction ON the returned runtime (so
    /// openraft's internal tasks adopt it), and hands the runtime back
    /// via [`Self::with_runtime`]; the runtime is never dropped in
    /// between, so the tasks keep their home.
    pub fn consensus_runtime() -> tokio::runtime::Runtime {
        Self::build_consensus_rt()
    }

    /// Builds the consensus runtime. One place so every constructor
    /// path gets identical settings.
    fn build_consensus_rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .thread_name("omnikv-consensus")
            .enable_all()
            .build()
            .expect("consensus runtime")
    }

    /// Adopts an ALREADY-RUNNING consensus runtime — the one the node
    /// was constructed on (see [`Self::consensus_runtime`]). This is how
    /// the boot sequence guarantees openraft's internal tasks, the raft
    /// RPC listener, and every proposal share one runtime that no
    /// client-facing server worker can starve.
    pub fn with_runtime(consensus_rt: tokio::runtime::Runtime, raft: OmniRaft) -> Self {
        let consensus_handle = consensus_rt.handle().clone();
        Self {
            raft,
            flight: tokio::sync::Mutex::new(()),
            consensus_rt,
            consensus_handle,
        }
    }

    pub fn new(raft: OmniRaft) -> Self {
        Self::with_runtime(Self::build_consensus_rt(), raft)
    }

    /// Proposes one command, waits until it is applied locally, returns
    /// the log index. Errors when this node is not the leader (with the
    /// current leader's id in the message) or when the cluster rejects
    /// the proposal. Async callers (REST/QUIC handlers) use this.
    pub async fn propose(&self, cmd: RaftCommand) -> Result<WriteAck, String> {
        let _flight = self.flight.lock().await;
        self.propose_locked(cmd).await
    }

    /// The propose core — the caller already holds the flight lock.
    /// Everything between a caller's SSI validation and this proposal
    /// being committed+applied is hidden from other writers by that
    /// lock; this is what makes the leader's committed view gap-free.
    async fn propose_locked(&self, cmd: RaftCommand) -> Result<WriteAck, String> {
        cmd.protects_system_keys()?;
        if cmd.is_empty() {
            // Nothing to replicate — nothing to ack either. Callers that
            // need read-only round-trip semantics use `probe()`.
            return Ok(WriteAck {
                index: 0,
                commit_seq: 0,
            });
        }
        let resp = self
            .raft
            .client_write(cmd.encode())
            .await
            .map_err(|e| match e.api_error() {
                Some(openraft::error::ClientWriteError::ForwardToLeader(fwd)) => {
                    match fwd.leader_id {
                        Some(id) => format!("not the leader; the leader is node {id}"),
                        None => "not the leader; no leader elected yet".to_string(),
                    }
                }
                _ => format!("cluster write failed: {e}"),
            })?;
        let index = resp.log_id.index;
        if index > 0 {
            self.raft
                .wait(None)
                .applied_index_at_least(Some(index), "omnikv-cluster-write-apply")
                .await
                .map_err(|e| format!("cluster write applied but await failed: {e}"))?;
        }
        // The state machine reports the commit marker it stamped for this
        // entry. Parsing it (rather than reading db.get_seq() now) is what
        // keeps the SSI commit record exact against concurrent bookkeeping
        // commits — see OmniRaftStorage::apply_to_state_machine.
        let commit_seq = resp.data.parse::<u64>().map_err(|_| {
            format!(
                "apply at index {index} returned no commit marker (got {:?}) — \
                 the entry was rejected or the response is malformed",
                resp.data
            )
        })?;
        Ok(WriteAck { index, commit_seq })
    }

    /// A quorum round trip that confirms this node is the leader and its
    /// state machine is current — the clustered readiness check.
    pub async fn probe(&self) -> Result<(), String> {
        self.raft
            .ensure_linearizable()
            .await
            .map(|_| ())
            .map_err(|e| format!("leadership check failed: {e}"))
    }

    /// The node id this cluster currently believes is the leader, if any.
    pub async fn leader_id(&self) -> Option<u64> {
        self.raft.current_leader().await
    }

    /// The last log index this node has applied to its state machine.
    pub fn applied_index(&self) -> u64 {
        self.raft
            .metrics()
            .borrow()
            .last_applied
            .map(|l| l.index)
            .unwrap_or_default()
    }

    /// Whether this node is the leader right now (quorum-checked).
    pub async fn is_leader(&self) -> bool {
        self.probe().await.is_ok()
    }

    /// Current openraft metrics — term, leader, applied index,
    /// membership — the shape monitoring and the failover tests watch.
    pub fn metrics(
        &self,
    ) -> tokio::sync::watch::Receiver<openraft::metrics::RaftMetrics<u64, openraft::BasicNode>>
    {
        self.raft.metrics()
    }

    /// Blocking facade over [`Self::propose`] for sync-thread callers
    /// (pgwire connection threads, the TCP debug server). Takes the
    /// flight lock, then runs the locked core.
    pub fn propose_blocking(&self, cmd: RaftCommand) -> Result<WriteAck, String> {
        let _flight = self.flight_blocking();
        self.block_on_ctx(async { self.propose_locked(cmd).await })
    }

    /// Blocking facade over [`Self::probe`].
    pub fn probe_blocking(&self) -> Result<(), String> {
        self.block_on_ctx(async { self.probe().await })
    }

    /// The `commit_batch` facade for sync-thread callers when clustered:
    /// serializes the batch into one [`RaftCommand`], proposes, waits for
    /// local apply. Reached through the [`crate::OmniKV::commit_batch`]
    /// engine hook — pgwire threads and SQL autocommit land here with no
    /// code changes of their own.
    pub fn commit_batch(&self, batch: &crate::WriteBatch) -> Result<u64, crate::OmniError> {
        let _flight = self.flight_blocking();
        let cmd = RaftCommand::from_batch(batch);
        if cmd.is_empty() {
            return Ok(0);
        }
        let ack = self
            .block_on_ctx(async { self.propose_locked(cmd).await })
            .map_err(crate::OmniError::IoError)?;
        Ok(ack.index)
    }

    /// The clustered SSI COMMIT: runs the transaction's validation
    /// under the flight lock (so no other proposal can land between
    /// its conflict checks and its consensus commit), then proposes the
    /// validated batch — with the SSI commit record riding inside the
    /// command — as ONE raft entry, the atomicity unit across the
    /// cluster. Returns the COMMIT MARKER the state machine reported for
    /// that entry: the caller's own number space (the SSI engine's
    /// storage seq — NOT the raft index), straight from the apply rather
    /// than the global counter, so a concurrent bookkeeping commit
    /// cannot shift it.
    ///
    /// The history record is NOT made here: the state machine's apply
    /// records it, on every node, when it stamps the marker — including
    /// on this leader, whose own apply `propose_locked` waits for before
    /// returning. So the record is in place before the flight lock
    /// releases, and the next transaction's validation sees it. That is
    /// the fix for the leader-local history: before the record rode in
    /// the command, only the node that ran the COMMIT ever recorded it,
    /// and a node promoted after a failover validated against a history
    /// missing every pre-failover commit.
    pub fn commit_ssi_blocking<F>(
        &self,
        ssi: crate::transaction::SsiCommitRecord,
        validate: F,
    ) -> Result<u64, String>
    where
        F: FnOnce() -> Result<crate::WriteBatch, String>,
    {
        let _flight = self.flight_blocking();
        let batch = validate()?;
        let cmd = RaftCommand::from_batch_with_ssi(&batch, ssi);
        if cmd.is_empty() {
            return Ok(0);
        }
        let ack = self.block_on_ctx(async { self.propose_locked(cmd).await })?;
        Ok(ack.commit_seq)
    }

    /// Runs `fut` to completion for a blocking caller on the CONSENSUS
    /// runtime. On a plain thread (pgwire connection threads) this is a
    /// direct `Runtime::block_on`. Inside an async execution context
    /// (axum REST / QUIC handler tasks on the client-facing server
    /// runtime) `block_on` would panic — "cannot start a runtime from
    /// within a runtime" — so the future is handed to the consensus
    /// runtime on a helper thread and the caller parks on a channel
    /// until it finishes. Parking is safe no matter HOW many client
    /// workers do it at once: consensus (openraft tasks, the raft RPC
    /// listener, the proposal being awaited) runs on the dedicated
    /// consensus runtime, never on the caller's — the parked workers
    /// cannot starve the very tasks that must finish to unpark them.
    /// (The flight lock does NOT bound parked workers: it is acquired
    /// inside the helper, after the caller is already parked.)
    fn block_on_ctx<F>(&self, fut: F) -> F::Output
    where
        F: std::future::Future + Send,
        F::Output: Send,
    {
        if tokio::runtime::Handle::try_current().is_err() {
            return self.consensus_rt.block_on(fut);
        }
        let handle = self.consensus_handle.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        // A scoped thread lets the future keep borrowing `self`.
        std::thread::scope(|scope| {
            scope.spawn(move || {
                let out = handle.block_on(fut);
                let _ = tx.send(out);
            });
            rx.recv().expect("consensus runtime runner finished")
        })
    }

    /// Blocking flight-lock acquisition for sync-thread callers. Like
    /// the other facades this is async-context safe: the guard is
    /// produced inside the helper thread and released by the caller
    /// once consensus completes.
    fn flight_blocking(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.block_on_ctx(async { self.flight.lock().await })
    }
}
