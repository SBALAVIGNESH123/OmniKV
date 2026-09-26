//! Production Hardening Utilities
//!
//! This module contains critical production infrastructure:
//!
//! 1. **Group Commit** — coalesces multiple concurrent fsyncs into one,
//!    reducing I/O syscalls by 10-50x under high write load.
//!
//! 2. **Per-User Rate Limiter** — token bucket rate limiting keyed by
//!    user/IP, preventing any single client from monopolizing the database.
//!
//! 3. **Connection Pool Config** — reqwest client tuning for Raft RPC.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::OmniError;

/// Coalesces concurrent fsync calls into one fsync per batch.
///
/// ## Coverage rule
///
/// A leader's fsync covers a writer only if the writer appended to the
/// heap/WAL *before* that fsync began. Writers append before calling
/// `join_group`, so the engine never lets a writer piggyback on a sync that
/// was already in flight when it arrived:
///
/// - A writer that finds the engine **idle** leads a new group; its own
///   fsync trivially covers its own append.
/// - A writer that arrives while epoch `E` is syncing cannot be covered by
///   `E` (the fsync may already have flushed past its append), so it queues
///   for epoch `E + 1`. That sync is guaranteed to start after the writer
///   queued, hence after its append.
pub struct GroupCommitEngine {
    state: Mutex<GroupState>,
    cond: Condvar,
}

struct GroupState {
    /// Followers waiting for a covering sync, keyed by the epoch they need
    /// completed. A leader is never counted here.
    waiters: BTreeMap<u64, usize>,
    /// Epoch of the sync currently in flight; 0 when the engine is idle.
    current_epoch: u64,
    /// Highest epoch whose sync finished, on any outcome — failures advance
    /// it too, so waiters waiting on a failed epoch are released.
    completed_epoch: u64,
    next_epoch: u64,
    /// fsync failures, keyed by epoch. An entry lives only while that epoch
    /// still has waiters to deliver the error to.
    failures: HashMap<u64, OmniError>,
    /// Set once a sync fails. A later sync would flush the failed batch's
    /// already-appended WAL bytes and make a rejected write durable.
    poisoned: Option<OmniError>,
}

impl GroupState {
    fn begin_next_sync(&mut self) -> u64 {
        let epoch = self.next_epoch;
        self.next_epoch += 1;
        self.current_epoch = epoch;
        epoch
    }

    /// Withdraws one waiter for `epoch` and drops the bucket when it empties.
    /// The failure is cloned per withdrawal so a failed sync reaches every
    /// waiter of the epoch, not just the first.
    fn release(&mut self, epoch: u64) -> Option<OmniError> {
        let failure = self.failures.get(&epoch).cloned();
        if let Some(count) = self.waiters.get_mut(&epoch) {
            *count -= 1;
            if *count == 0 {
                self.waiters.remove(&epoch);
                self.failures.remove(&epoch);
            }
        }
        failure
    }
}

impl GroupCommitEngine {
    pub fn new(_max_wait_us: u64) -> Self {
        Self {
            state: Mutex::new(GroupState {
                waiters: BTreeMap::new(),
                current_epoch: 0,
                completed_epoch: 0,
                next_epoch: 1,
                failures: HashMap::new(),
                poisoned: None,
            }),
            cond: Condvar::new(),
        }
    }

    /// Join the current write group.
    ///
    /// - **Leader** (`is_leader == true`): fsync, then call
    ///   `guard.mark_synced(result)` so the group's followers are released
    ///   with the same outcome.
    /// - **Follower** (`is_leader == false`): the covering sync already
    ///   completed successfully; proceed without fsyncing.
    ///
    /// Returns `Err` if the covering sync failed, or if the engine is
    /// poisoned by an earlier failure.
    pub fn join_group(&self) -> Result<GroupCommitGuard<'_>, OmniError> {
        let mut state = self.state.lock().expect("group state");

        if let Some(err) = state.poisoned.clone() {
            return Err(err);
        }

        if state.current_epoch == 0 {
            let epoch = state.begin_next_sync();
            drop(state);
            return Ok(GroupCommitGuard {
                engine: self,
                is_leader: true,
                epoch,
            });
        }

        // The in-flight sync began before this writer's append, so it may
        // already have flushed past these bytes. Wait for the next epoch,
        // which cannot start until this writer is queued.
        let needed = state.current_epoch + 1;
        *state.waiters.entry(needed).or_default() += 1;

        loop {
            state = self.cond.wait(state).expect("condvar wait");

            if state.completed_epoch >= needed {
                let failure = state.release(needed);
                drop(state);
                if let Some(err) = failure {
                    return Err(err);
                }
                return Ok(GroupCommitGuard {
                    engine: self,
                    is_leader: false,
                    epoch: needed,
                });
            }

            if state.current_epoch == 0 {
                // Epochs are handed out in order, so the next one is the
                // epoch this writer needs.
                let epoch = state.begin_next_sync();
                debug_assert_eq!(epoch, needed, "epochs are sequential");
                // Leaving the queue for an epoch that has not synced yet can
                // only withdraw this writer's own slot.
                let _ = state.release(needed);
                drop(state);
                return Ok(GroupCommitGuard {
                    engine: self,
                    is_leader: true,
                    epoch: needed,
                });
            }
        }
    }

    /// Publishes the leader's fsync outcome so the group's followers are
    /// released with the same result.
    fn complete_sync(&self, epoch: u64, result: Result<(), OmniError>) {
        let mut state = self.state.lock().expect("group state");
        state.completed_epoch = state.completed_epoch.max(epoch);
        state.current_epoch = 0;
        if let Err(err) = result {
            state.poisoned = Some(err.clone());
            if state.waiters.contains_key(&epoch) {
                state.failures.insert(epoch, err);
            }
        }
        drop(state);

        self.cond.notify_all();
    }

    /// Returns (completed_epoch, waiting follower count).
    pub fn stats(&self) -> (u64, usize) {
        let state = self.state.lock().expect("group state");
        (state.completed_epoch, state.waiters.values().sum())
    }
}

/// Guard returned by `join_group()`.
/// If `is_leader` is true, perform fsync then call `mark_synced(result)`.
/// If `is_leader` is false, the sync is already done — just proceed.
pub struct GroupCommitGuard<'a> {
    engine: &'a GroupCommitEngine,
    /// If true, this writer must perform the fsync.
    pub is_leader: bool,
    epoch: u64,
}

impl GroupCommitGuard<'_> {
    /// Leader only: publishes the fsync outcome so the group's followers are
    /// released with the same result.
    pub fn mark_synced(self, result: Result<(), OmniError>) {
        if self.is_leader {
            self.engine.complete_sync(self.epoch, result);
        }
    }
}

/// ═══════════════════════════════════════════════════════════════════════
/// PER-USER TOKEN BUCKET RATE LIMITER
/// ═══════════════════════════════════════════════════════════════════════
///
/// Each user/IP gets their own token bucket with configurable rate and burst.
/// This prevents a single client from overwhelming the database while allowing
/// aggregate throughput to remain high.
///
/// ## Token Bucket Algorithm:
///
/// - Each user starts with `burst` tokens.
/// - Tokens refill at `rate_per_sec` tokens per second.
/// - Each request consumes 1 token.
/// - If no tokens available → request is rejected (HTTP 429).
pub struct RateLimiter {
    /// Per-user buckets: user_id → bucket state.
    buckets: Mutex<HashMap<String, TokenBucket>>,
    /// Maximum tokens per user (burst capacity).
    burst: u32,
    /// Token refill rate (tokens per second).
    rate_per_sec: f64,
    /// Maximum number of tracked users (LRU eviction after this).
    max_users: usize,
}

struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
}

impl RateLimiter {
    /// Creates a new per-user rate limiter.
    ///
    /// `rate_per_sec` — sustained request rate per user
    /// `burst` — maximum burst capacity per user
    /// `max_users` — maximum tracked users (prevents memory exhaustion)
    pub fn new(rate_per_sec: f64, burst: u32, max_users: usize) -> Self {
        Self {
            buckets: Mutex::new(HashMap::new()),
            burst,
            rate_per_sec,
            max_users,
        }
    }

    /// Attempts to consume one token for the given user.
    ///
    /// Returns `Ok(remaining_tokens)` if allowed, or `Err(retry_after_ms)` if rate limited.
    pub fn try_acquire(&self, user_id: &str) -> Result<u32, u64> {
        let mut buckets = self.buckets.lock().expect("rate limiter");
        let now = Instant::now();
        let burst = self.burst;
        let rate = self.rate_per_sec;

        // Evict oldest bucket if at capacity
        if buckets.len() >= self.max_users && !buckets.contains_key(user_id) {
            let oldest = buckets
                .iter()
                .min_by_key(|(_, b)| b.last_refill)
                .map(|(k, _)| k.clone());
            if let Some(key) = oldest {
                buckets.remove(&key);
            }
        }

        let bucket = buckets.entry(user_id.to_string()).or_insert(TokenBucket {
            tokens: burst as f64,
            last_refill: now,
        });

        // Refill tokens based on elapsed time
        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * rate).min(burst as f64);
        bucket.last_refill = now;

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            Ok(bucket.tokens as u32)
        } else {
            let deficit = 1.0 - bucket.tokens;
            let retry_ms = (deficit / rate * 1000.0) as u64;
            Err(retry_ms.max(1))
        }
    }

    /// Resets a specific user's rate limit bucket.
    pub fn reset_user(&self, user_id: &str) {
        let mut buckets = self.buckets.lock().expect("rate limiter");
        buckets.remove(user_id);
    }

    /// Returns the number of tracked users.
    pub fn tracked_users(&self) -> usize {
        let buckets = self.buckets.lock().expect("rate limiter");
        buckets.len()
    }
}

/// ═══════════════════════════════════════════════════════════════════════
/// CONNECTION POOL CONFIGURATION
/// ═══════════════════════════════════════════════════════════════════════
///
/// Creates a properly tuned reqwest::Client for Raft RPC communication.
/// Default `reqwest::Client::new()` has no connection pool limits, causing
/// TCP connection storms under high Raft traffic.
///
/// Creates a production-grade reqwest client with connection pooling.
pub fn create_pooled_client(
    pool_max_idle: usize,
    timeout_secs: u64,
    pool_idle_timeout_secs: u64,
) -> reqwest::Client {
    reqwest::Client::builder()
        .pool_max_idle_per_host(pool_max_idle)
        .pool_idle_timeout(Duration::from_secs(pool_idle_timeout_secs))
        .timeout(Duration::from_secs(timeout_secs))
        .connect_timeout(Duration::from_secs(5))
        .tcp_keepalive(Duration::from_secs(30))
        .tcp_nodelay(true)
        .build()
        .expect("Failed to build HTTP client")
}

/// Default production client: 32 idle connections, 10s timeout.
pub fn default_raft_client() -> reqwest::Client {
    create_pooled_client(32, 10, 90)
}
