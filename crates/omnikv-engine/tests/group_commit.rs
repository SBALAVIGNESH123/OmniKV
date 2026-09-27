//! Group-commit durability and coalescing tests.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use omni_engine::hardening::GroupCommitEngine;

/// Writers arriving while a sync is in flight must be covered by a single
/// following sync: one leader, the rest followers.
#[test]
fn concurrent_joiners_coalesce_into_one_sync() {
    let engine = Arc::new(GroupCommitEngine::new(100));
    let leaders = Arc::new(AtomicUsize::new(0));
    let followers = Arc::new(AtomicUsize::new(0));
    let n = 8;
    let all_arrived = Arc::new(Barrier::new(n + 1));

    let g1 = engine.join_group().expect("first leader");
    assert!(g1.is_leader);

    let mut handles = Vec::new();
    for _ in 0..n {
        let engine = engine.clone();
        let leaders = leaders.clone();
        let followers = followers.clone();
        let all_arrived = all_arrived.clone();
        handles.push(thread::spawn(move || {
            all_arrived.wait();
            let guard = engine.join_group().expect("covered sync");
            if guard.is_leader {
                leaders.fetch_add(1, Ordering::SeqCst);
                guard.mark_synced(Ok(()));
            } else {
                followers.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }
    // Hold the first sync open until every joiner is queued as a waiter,
    // then release. Polling the waiter count keeps this deterministic under
    // CI load: a joiner that has not reached join_group() yet would find the
    // engine idle and lead a separate sync of its own.
    all_arrived.wait();
    wait_for_pending(&engine, n);
    g1.mark_synced(Ok(()));

    for h in handles {
        h.join().expect("joiner thread");
    }

    let leaders = leaders.load(Ordering::SeqCst);
    let followers = followers.load(Ordering::SeqCst);
    println!("leaders={leaders} followers={followers}");
    assert_eq!(leaders + followers, n);
    assert_eq!(
        leaders, 1,
        "the {n} concurrent joiners must coalesce into one leader, but {leaders} led",
    );
    assert_eq!(followers, n - 1);

    let (_, pending) = engine.stats();
    assert_eq!(pending, 0);
}

/// A failed leader fsync must fail every follower it covered.
#[test]
fn a_failed_leader_sync_fails_its_followers() {
    let engine = Arc::new(GroupCommitEngine::new(100));
    let n = 6;
    let all_arrived = Arc::new(Barrier::new(n + 1));

    let g1 = engine.join_group().expect("first leader");
    assert!(g1.is_leader);

    let outcomes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut handles = Vec::new();
    for _ in 0..n {
        let engine = engine.clone();
        let outcomes = outcomes.clone();
        let all_arrived = all_arrived.clone();
        handles.push(thread::spawn(move || {
            all_arrived.wait();
            let failed = match engine.join_group() {
                Ok(guard) if guard.is_leader => {
                    guard.mark_synced(Err(omni_engine::OmniError::IoError(
                        "simulated fsync failure".into(),
                    )));
                    true
                }
                Ok(_) => false,
                Err(_) => true,
            };
            outcomes.lock().unwrap().push(failed);
        }));
    }
    all_arrived.wait();
    wait_for_pending(&engine, n);
    g1.mark_synced(Err(omni_engine::OmniError::IoError(
        "simulated fsync failure".into(),
    )));

    for h in handles {
        h.join().expect("joiner thread");
    }

    // Clone out of the guard before asserting.
    let outcomes = outcomes.lock().unwrap().clone();
    let failures = outcomes.iter().filter(|&&f| f).count();
    println!("failures among {n} joiners: {failures}/{n}");
    assert_eq!(
        failures, n,
        "every writer covered by a failed sync must report failure, got {outcomes:?}"
    );

    let (_, pending) = engine.stats();
    assert_eq!(pending, 0, "no waiter may be stranded after a failed sync");
}

/// A follower whose covering sync succeeds is released without fsyncing and
/// without error.
#[test]
fn a_successful_sync_releases_followers_cleanly() {
    let engine = Arc::new(GroupCommitEngine::new(100));
    let g1 = engine.join_group().expect("first leader");

    let engine2 = engine.clone();
    let handle = thread::spawn(move || {
        // The guard borrows the engine, so it must be consumed here.
        let guard = engine2.join_group().expect("covered sync");
        let was_leader = guard.is_leader;
        guard.mark_synced(Ok(()));
        was_leader
    });

    thread::sleep(std::time::Duration::from_millis(100));
    g1.mark_synced(Ok(()));

    let was_leader = handle.join().expect("joiner thread");
    assert!(was_leader, "lone late joiner leads the next epoch");

    let (epoch, pending) = engine.stats();
    assert_eq!(epoch, 2);
    assert_eq!(pending, 0);
}

/// A failed sync poisons the engine: no later writer may lead a sync that
/// would flush the rejected batch's appended bytes.
#[test]
fn a_failed_sync_poisons_the_engine() {
    let engine = Arc::new(GroupCommitEngine::new(100));
    let g1 = engine.join_group().expect("first leader");
    g1.mark_synced(Err(omni_engine::OmniError::IoError(
        "simulated fsync failure".into(),
    )));

    assert!(
        engine.join_group().is_err(),
        "a poisoned engine must not accept a new sync"
    );
    assert!(
        engine.join_group().is_err(),
        "poison is sticky, not a one-shot"
    );
}

/// A waiter queued for the next epoch when a sync fails must not be promoted
/// to leader afterwards: it would sync a truncated WAL and get acknowledged
/// for bytes that are no longer there.
#[test]
fn a_waiter_is_not_promoted_after_a_poisoned_sync() {
    let engine = Arc::new(GroupCommitEngine::new(100));
    let g1 = engine.join_group().expect("first leader");

    let engine2 = engine.clone();
    let handle = thread::spawn(move || {
        // The guard borrows the engine, so it must be consumed here.
        engine2.join_group().map(|g| {
            let was_leader = g.is_leader;
            g.mark_synced(Ok(()));
            was_leader
        })
    });

    wait_for_pending(&engine, 1);
    g1.mark_synced(Err(omni_engine::OmniError::IoError(
        "simulated fsync failure".into(),
    )));

    let outcome = handle.join().expect("joiner thread");
    assert!(
        outcome.is_err(),
        "a waiter queued before the failure must be failed, not promoted"
    );
    assert!(engine.is_poisoned());
    assert!(engine.join_group().is_err());
    let (_, pending) = engine.stats();
    assert_eq!(pending, 0, "the waiter must not be stranded");
}

/// After a failed fsync, the undurable WAL bytes must be physically gone, not
/// merely unf-synced: the kernel's own writeback could otherwise flush them.
#[test]
fn discard_undurable_removes_unfsynced_batches() {
    use omni_engine::wal::WriteAheadLog;

    let dir = tempfile::tempdir().expect("wal tempdir");
    let path = dir.path().join("wal.bin");
    let p = path.to_string_lossy().to_string();

    let mut wal = WriteAheadLog::new(&p).expect("open wal");
    let marker = vec![(make_marker(), None)];
    wal.append_batch_nosync(&marker).expect("append nosync");

    let len_before = std::fs::metadata(&p).unwrap().len();
    assert!(len_before > 0, "the batch must have reached the file");

    wal.discard_undurable().expect("truncate");

    let len_after = std::fs::metadata(&p).unwrap().len();
    assert_eq!(len_after, 0, "undurable bytes must be truncated");
    assert!(
        WriteAheadLog::replay(&p, "").unwrap().is_empty(),
        "replay must not restore a discarded batch"
    );
}

fn make_marker() -> omni_engine::OmniRecord {
    omni_engine::OmniRecord {
        seq: 1,
        key: b"__COMMIT_MARKER__".to_vec(),
        offset: 0,
        length: 0,
        crc32: 0,
        payload_crc32: 0,
        expiry: 0,
    }
}

/// Blocks until exactly `n` writers are queued as waiters. Used instead of a
/// fixed sleep so the tests do not depend on thread-scheduling timing.
fn wait_for_pending(engine: &GroupCommitEngine, n: usize) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        match engine.stats().1 {
            found if found == n => return,
            found if std::time::Instant::now() > deadline => {
                panic!("expected {n} waiters queued, found {found}")
            }
            _ => std::thread::sleep(std::time::Duration::from_millis(2)),
        }
    }
}
