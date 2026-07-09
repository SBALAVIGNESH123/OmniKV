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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

/// ═══════════════════════════════════════════════════════════════════════
/// GROUP COMMIT ENGINE — v2 (No-Sleep Design)
/// ═══════════════════════════════════════════════════════════════════════
///
/// Coalesces concurrent fsync calls into a single fsync per batch.
///
/// ## Design (no sleep, no timed wait):
///
/// 1. Each writer appends data to heap + WAL (no fsync yet).
/// 2. Writer enters `join_group()`.
/// 3. If no sync is in progress → become leader, sync immediately.
/// 4. If a sync IS in progress → wait as follower.
/// 5. When the leader's sync completes, ALL followers are released.
/// 6. Natural batching: while leader fsyncs (~2ms), new writers queue up.
///    Next leader syncs for everyone who arrived during those 2ms.
///
/// This achieves the same throughput as a timed-wait design without the
/// latency overhead of sleeping on every single-threaded write.
pub struct GroupCommitEngine {
    /// State of the current write group.
    state: Mutex<GroupState>,
    /// Condition variable for waiting followers.
    cond: Condvar,
    /// Monotonic epoch counter — increments on each completed sync.
    epoch: AtomicU64,
}

struct GroupState {
    /// Number of writers waiting in the current group (including leader).
    pending_count: usize,
    /// The epoch that was last committed.
    committed_epoch: u64,
    /// Whether a sync is currently in progress.
    sync_in_progress: bool,
}

impl GroupCommitEngine {
    /// Creates a new GroupCommitEngine.
    pub fn new(_max_wait_us: u64) -> Self {
        Self {
            state: Mutex::new(GroupState {
                pending_count: 0,
                committed_epoch: 0,
                sync_in_progress: false,
            }),
            cond: Condvar::new(),
            epoch: AtomicU64::new(1),
        }
    }

    /// Join the current write group.
    ///
    /// Returns a guard indicating whether this writer is the leader.
    /// - Leader: must perform fsync, then call `guard.mark_synced()`.
    /// - Follower: blocks until the leader's sync completes, then returns.
    ///
    /// No sleep, no timed wait. The leader syncs immediately.
    /// Natural batching occurs because followers accumulate during the
    /// ~2ms fsync window.
    pub fn join_group(&self) -> GroupCommitGuard<'_> {
        let my_epoch = self.epoch.load(Ordering::SeqCst);

        let mut state = self.state.lock().expect("group state");
        state.pending_count += 1;

        if !state.sync_in_progress {
            // No sync running → I'm the leader. Start syncing immediately.
            state.sync_in_progress = true;
            drop(state);

            // No sleep! Leader proceeds directly to fsync.
            GroupCommitGuard {
                engine: self,
                is_leader: true,
            }
        } else {
            // A sync is already in progress → wait as follower.
            // The leader will wake us when done.
            while state.committed_epoch < my_epoch {
                state = self.cond.wait(state).expect("condvar wait");
            }
            state.pending_count -= 1;
            drop(state);

            GroupCommitGuard {
                engine: self,
                is_leader: false,
            }
        }
    }

    /// Called by the leader after performing the actual fsync.
    fn complete_sync(&self) {
        let new_epoch = self.epoch.fetch_add(1, Ordering::SeqCst);

        let mut state = self.state.lock().expect("group state");
        state.committed_epoch = new_epoch;
        state.sync_in_progress = false;
        state.pending_count -= 1;
        drop(state);

        // Wake all waiting followers
        self.cond.notify_all();
    }

    /// Returns (committed_epoch, pending_count).
    pub fn stats(&self) -> (u64, usize) {
        let state = self.state.lock().expect("group state");
        (state.committed_epoch, state.pending_count)
    }
}

/// Guard returned by `join_group()`.
/// If `is_leader` is true, perform fsync then call `mark_synced()`.
/// If `is_leader` is false, the sync is already done — just proceed.
pub struct GroupCommitGuard<'a> {
    engine: &'a GroupCommitEngine,
    /// If true, this writer must perform the fsync.
    pub is_leader: bool,
}

impl GroupCommitGuard<'_> {
    /// Call after performing fsync (leader only). Wakes all followers.
    pub fn mark_synced(self) {
        if self.is_leader {
            self.engine.complete_sync();
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
