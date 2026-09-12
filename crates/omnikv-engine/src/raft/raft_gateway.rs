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
}

/// The server-wide handle for cluster writes. Cheap to clone via `Arc`.
pub struct ClusterGateway {
    raft: OmniRaft,
    /// One mutating command in flight: proposal, commit, and local apply
    /// complete before the next proposal starts. This is what makes the
    /// leader's committed view gap-free for the next write's checks.
    flight: tokio::sync::Mutex<()>,
    /// Runtime for the blocking facades — the wire servers (pgwire,
    /// TCP) run one OS thread per connection and cannot `.await`.
    /// Blocking facades entered from an async context (REST/QUIC
    /// handlers) hop through a helper thread, because
    /// `Runtime::block_on` inside a runtime worker panics.
    block_on: tokio::runtime::Runtime,
}

impl ClusterGateway {
    pub fn new(raft: OmniRaft) -> Self {
        Self {
            raft,
            flight: tokio::sync::Mutex::new(()),
            block_on: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("omnikv-cluster-gateway")
                .build()
                .expect("cluster gateway runtime"),
        }
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
            return Ok(WriteAck { index: 0 });
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
        Ok(WriteAck { index })
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
    /// validated batch as ONE raft entry — the atomicity unit across
    /// the cluster — and runs `on_committed` with the log index while
    /// still holding the lock, so the caller's committed-history record
    /// lands in cluster commit order. `on_committed` returns the
    /// commit number in the caller's own number space (the SSI engine's
    /// storage seq — NOT the raft index), which becomes this call's
    /// result. Called from
    /// [`crate::storage::transaction::TransactionManager::commit`] with
    /// its own locks dropped; consensus may take arbitrarily long.
    pub fn commit_ssi_blocking<F, G>(&self, validate: F, on_committed: G) -> Result<u64, String>
    where
        F: FnOnce() -> Result<crate::WriteBatch, String>,
        G: FnOnce(u64) -> u64,
    {
        let _flight = self.flight_blocking();
        let batch = validate()?;
        let cmd = RaftCommand::from_batch(&batch);
        if cmd.is_empty() {
            return Ok(0);
        }
        let ack = self.block_on_ctx(async { self.propose_locked(cmd).await })?;
        Ok(on_committed(ack.index))
    }

    /// Runs `fut` to completion for a blocking caller. On a plain
    /// thread (pgwire connection threads) this is a direct
    /// `Runtime::block_on`. Inside an async execution context (axum
    /// REST / QUIC handler tasks on the server runtime) `block_on`
    /// would panic — "cannot start a runtime from within a runtime" —
    /// so the future is handed to the gateway's own runtime on a
    /// helper thread and the caller parks on a channel until it
    /// finishes. The parked worker is safe: the server runtime is
    /// multi-threaded, and the flight lock bounds how many callers can
    /// be inside here at once.
    fn block_on_ctx<F>(&self, fut: F) -> F::Output
    where
        F: std::future::Future + Send,
        F::Output: Send,
    {
        if tokio::runtime::Handle::try_current().is_err() {
            return self.block_on.block_on(fut);
        }
        let handle = self.block_on.handle().clone();
        let (tx, rx) = std::sync::mpsc::channel();
        // A scoped thread lets the future keep borrowing `self`; the
        // parked caller is a single async worker at most, because the
        // flight lock serializes every other clustered writer behind
        // the same hop.
        std::thread::scope(|scope| {
            scope.spawn(move || {
                let out = handle.block_on(fut);
                let _ = tx.send(out);
            });
            rx.recv().expect("cluster gateway runner finished")
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
