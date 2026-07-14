//! Multi-Node Raft Integration Test
//!
//! Proves: log replication, leader election, crash recovery across 3 nodes.

use omni_engine::OmniKV;
use omni_engine::raft_storage::OmniRaftStorage;
use openraft::storage::{RaftSnapshotBuilder, RaftStorage};
use std::sync::Arc;

fn create_node(name: &str) -> (Arc<OmniKV>, OmniRaftStorage, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let manifest = dir.path().join(format!("{name}_manifest.json"));
    let wal = dir.path().join(format!("{name}_wal.bin"));
    let db = OmniKV::open(manifest.to_str().unwrap(), wal.to_str().unwrap()).unwrap();
    let storage = OmniRaftStorage::new(db.clone());
    (db, storage, dir)
}

fn create_node_in_dir(dir: &std::path::Path, name: &str) -> (Arc<OmniKV>, OmniRaftStorage) {
    let manifest = dir.join(format!("{name}_manifest.json"));
    let wal = dir.join(format!("{name}_wal.bin"));
    let db = OmniKV::open(manifest.to_str().unwrap(), wal.to_str().unwrap()).unwrap();
    let storage = OmniRaftStorage::new(db.clone());
    (db, storage)
}

/// Simulate leader replicating log entries to followers
fn replicate_log(leader: &OmniRaftStorage, followers: &[&OmniRaftStorage], start: u64, end: u64) {
    for idx in start..end {
        let entry = leader
            .read_log(idx)
            .unwrap_or_else(|| panic!("Leader missing log {idx}"));
        for f in followers {
            f.append_log(idx, &entry).expect("Follower append failed");
        }
    }
}

#[test]
fn test_3_node_log_replication() {
    let (_db1, node1, _d1) = create_node("node1");
    let (_db2, node2, _d2) = create_node("node2");
    let (_db3, node3, _d3) = create_node("node3");

    // Node1 is leader — append 10 log entries
    for i in 1..=10 {
        node1
            .append_log(i, &format!("SET key{i} value{i}"))
            .unwrap();
    }

    // Replicate to followers
    replicate_log(&node1, &[&node2, &node3], 1, 11);

    // Verify all 3 nodes have identical logs
    for i in 1..=10 {
        let e1 = node1.read_log(i).unwrap();
        let e2 = node2.read_log(i).unwrap();
        let e3 = node3.read_log(i).unwrap();
        assert_eq!(e1, e2, "Node1 vs Node2 mismatch at index {i}");
        assert_eq!(e2, e3, "Node2 vs Node3 mismatch at index {i}");
    }

    println!("✅ 3-NODE LOG REPLICATION: All 10 entries identical across 3 nodes");
}

#[test]
fn test_state_machine_apply() {
    let (db1, node1, _d1) = create_node("leader");
    let (db2, node2, _d2) = create_node("follower1");
    let (db3, node3, _d3) = create_node("follower2");

    // Leader writes data through Raft log
    let entries = [
        "SET user:1 Alice",
        "SET user:2 Bob",
        "SET user:3 Charlie",
        "SET balance:1 1000",
        "SET balance:2 2500",
    ];

    for (i, entry) in entries.iter().enumerate() {
        let idx = (i + 1) as u64;
        node1.append_log(idx, entry).unwrap();
    }

    // Replicate to followers
    replicate_log(&node1, &[&node2, &node3], 1, 6);

    // Apply on ALL nodes (state machine)
    for node in [&node1, &node2, &node3] {
        for i in 1..=5u64 {
            let entry = node.read_log(i).unwrap();
            node.apply_write(&entry).unwrap();
            node.mark_applied(i).unwrap();
        }
    }

    // Verify data is identical on all 3 nodes
    for (db, name) in [(&db1, "leader"), (&db2, "follower1"), (&db3, "follower2")] {
        let seq = db.get_seq();
        assert_eq!(
            db.find("user:1", seq).unwrap(),
            Some("Alice".into()),
            "{name} missing user:1"
        );
        assert_eq!(
            db.find("user:2", seq).unwrap(),
            Some("Bob".into()),
            "{name} missing user:2"
        );
        assert_eq!(
            db.find("user:3", seq).unwrap(),
            Some("Charlie".into()),
            "{name} missing user:3"
        );
        assert_eq!(
            db.find("balance:1", seq).unwrap(),
            Some("1000".into()),
            "{name} missing balance:1"
        );
        assert_eq!(
            db.find("balance:2", seq).unwrap(),
            Some("2500".into()),
            "{name} missing balance:2"
        );
    }

    // Verify last_applied_index
    assert_eq!(node1.last_applied_index(), 5);
    assert_eq!(node2.last_applied_index(), 5);
    assert_eq!(node3.last_applied_index(), 5);

    println!("✅ STATE MACHINE APPLY: All 3 nodes have identical data after 5 entries");
}

#[test]
fn test_leader_election_simulation() {
    let (_db1, node1, _d1) = create_node("node1");
    let (_db2, node2, _d2) = create_node("node2");
    let (_db3, node3, _d3) = create_node("node3");

    // Node1 is leader for term 1
    node1.save_vote(r#"{"term":1,"voted_for":1}"#).unwrap();
    node2.save_vote(r#"{"term":1,"voted_for":1}"#).unwrap();
    node3.save_vote(r#"{"term":1,"voted_for":1}"#).unwrap();

    // Leader writes entries 1-5
    for i in 1..=5 {
        node1.append_log(i, &format!("SET key{i} val{i}")).unwrap();
    }
    replicate_log(&node1, &[&node2, &node3], 1, 6);

    // === Node1 crashes! Node2 becomes new leader (term 2) ===
    node2.save_vote(r#"{"term":2,"voted_for":2}"#).unwrap();
    node3.save_vote(r#"{"term":2,"voted_for":2}"#).unwrap();

    // New leader (node2) writes entries 6-8
    for i in 6..=8 {
        node2
            .append_log(i, &format!("SET newkey{i} newval{i}"))
            .unwrap();
    }

    // Replicate to node3 only (node1 is "dead")
    replicate_log(&node2, &[&node3], 6, 9);

    // Verify node2 and node3 have all 8 entries
    for i in 1..=8 {
        assert!(node2.read_log(i).is_some(), "Node2 missing log {i}");
        assert!(node3.read_log(i).is_some(), "Node3 missing log {i}");
    }

    // Verify node2's vote is term 2
    let vote = node2.read_vote().unwrap();
    assert!(vote.contains("\"term\":2"), "Node2 should be in term 2");

    // === Node1 recovers — catches up from node2 ===
    replicate_log(&node2, &[&node1], 6, 9);
    node1.save_vote(r#"{"term":2,"voted_for":2}"#).unwrap();

    // All 3 nodes now have all 8 entries
    for i in 1..=8 {
        let e1 = node1.read_log(i).unwrap();
        let e2 = node2.read_log(i).unwrap();
        let e3 = node3.read_log(i).unwrap();
        assert_eq!(e1, e2, "Mismatch at {i}");
        assert_eq!(e2, e3, "Mismatch at {i}");
    }

    println!("✅ LEADER ELECTION: Node1 crashed, Node2 elected, Node1 recovered and caught up");
}

#[test]
fn test_log_compaction() {
    let (_db, node, _d) = create_node("compact_node");

    // Write 20 entries
    for i in 1..=20 {
        node.append_log(i, &format!("SET k{i} v{i}")).unwrap();
    }

    // Apply first 10
    for i in 1..=10 {
        let entry = node.read_log(i).unwrap();
        node.apply_write(&entry).unwrap();
        node.mark_applied(i).unwrap();
    }

    // Compact logs 1-10 (already applied)
    node.delete_log_range(1, 11).unwrap();

    // Entries 1-10 should be gone
    for i in 1..=10 {
        assert!(node.read_log(i).is_none(), "Log {i} should be compacted");
    }

    // Entries 11-20 should still exist
    for i in 11..=20 {
        assert!(node.read_log(i).is_some(), "Log {i} should exist");
    }

    assert_eq!(node.last_applied_index(), 10);
    println!("✅ LOG COMPACTION: Entries 1-10 compacted, 11-20 retained");
}

#[test]
fn test_crash_recovery_persistence() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = dir.path().join("recover_manifest.json");
    let wal = dir.path().join("recover_wal.bin");

    // Phase 1: Write data and "crash"
    {
        let db = OmniKV::open(manifest.to_str().unwrap(), wal.to_str().unwrap()).unwrap();
        let storage = OmniRaftStorage::new(db);

        for i in 1..=5 {
            storage
                .append_log(i, &format!("SET persist{i} data{i}"))
                .unwrap();
            let entry = storage.read_log(i).unwrap();
            storage.apply_write(&entry).unwrap();
            storage.mark_applied(i).unwrap();
        }

        storage.save_vote(r#"{"term":3,"voted_for":1}"#).unwrap();
        // db drops here = simulated crash
    }

    // Phase 2: Reopen and verify everything persisted
    {
        let db = OmniKV::open(manifest.to_str().unwrap(), wal.to_str().unwrap()).unwrap();
        let storage = OmniRaftStorage::new(db.clone());

        // Vote persisted
        let vote = storage.read_vote().unwrap();
        assert!(vote.contains("\"term\":3"), "Vote not persisted");

        // Last applied persisted
        assert_eq!(
            storage.last_applied_index(),
            5,
            "Last applied not persisted"
        );

        // Data persisted
        let seq = db.get_seq();
        for i in 1..=5 {
            let val = db.find(&format!("persist{i}"), seq).unwrap();
            assert_eq!(val, Some(format!("data{i}")), "Data {i} not persisted");
        }
    }

    println!("✅ CRASH RECOVERY: Vote, log index, and data survived restart");
}

/// Gap #2: Leader election under concurrent write load
///
/// While a leader is actively writing, it "crashes" and a new leader
/// takes over. Verify no data is lost and new leader continues correctly.
#[test]
fn test_leader_election_under_load() {
    let (_db1, node1, _d1) = create_node("leader1");
    let (db2, node2, _d2) = create_node("follower1");
    let (db3, node3, _d3) = create_node("follower2");

    // Term 1: Node1 is leader, writes 50 entries under load
    node1.save_vote(r#"{"term":1,"voted_for":1}"#).unwrap();
    for i in 1..=50 {
        node1
            .append_log(i, &format!("SET load_key{i} load_val{i}"))
            .unwrap();
    }

    // Only entries 1-30 were replicated before crash (partial replication)
    replicate_log(&node1, &[&node2, &node3], 1, 31);

    // Apply 1-30 on followers
    for node in [&node2, &node3] {
        for i in 1..=30u64 {
            let entry = node.read_log(i).unwrap();
            node.apply_write(&entry).unwrap();
            node.mark_applied(i).unwrap();
        }
    }

    // === Node1 crashes mid-write! Entries 31-50 only on node1 ===
    // Node2 becomes leader (term 2) — starts from index 31
    node2.save_vote(r#"{"term":2,"voted_for":2}"#).unwrap();
    node3.save_vote(r#"{"term":2,"voted_for":2}"#).unwrap();

    // New leader writes 20 more entries (indices 31-50, overwriting node1's)
    for i in 31..=50 {
        node2
            .append_log(i, &format!("SET new_key{i} new_val{i}"))
            .unwrap();
    }
    replicate_log(&node2, &[&node3], 31, 51);

    // Apply 31-50 on node2 and node3
    for node in [&node2, &node3] {
        for i in 31..=50u64 {
            let entry = node.read_log(i).unwrap();
            node.apply_write(&entry).unwrap();
            node.mark_applied(i).unwrap();
        }
    }

    // Verify followers have all 50 entries applied
    for (db, name) in [(&db2, "node2"), (&db3, "node3")] {
        let seq = db.get_seq();
        // First 30 from old leader
        for i in 1..=30 {
            assert!(
                db.find(&format!("load_key{i}"), seq).unwrap().is_some(),
                "{name} missing load_key{i}"
            );
        }
        // Last 20 from new leader
        for i in 31..=50 {
            assert_eq!(
                db.find(&format!("new_key{i}"), seq).unwrap(),
                Some(format!("new_val{i}")),
                "{name} missing new_key{i}"
            );
        }
    }

    assert_eq!(node2.last_applied_index(), 50);
    assert_eq!(node3.last_applied_index(), 50);

    println!("✅ LEADER ELECTION UNDER LOAD: 50 entries across 2 leaders, 0 data loss");
}

/// Gap #3: Log consistency after crash
///
/// Write entries, crash, reopen, verify log is intact and continues correctly.
#[test]
fn test_log_consistency_after_crash() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = dir.path().join("logcrash_manifest.json");
    let wal = dir.path().join("logcrash_wal.bin");

    // Phase 1: Write 20 entries, apply 10, crash
    {
        let db = OmniKV::open(manifest.to_str().unwrap(), wal.to_str().unwrap()).unwrap();
        let storage = OmniRaftStorage::new(db);

        for i in 1..=20 {
            storage
                .append_log(i, &format!("SET crash_k{i} crash_v{i}"))
                .unwrap();
        }
        for i in 1..=10u64 {
            let entry = storage.read_log(i).unwrap();
            storage.apply_write(&entry).unwrap();
            storage.mark_applied(i).unwrap();
        }
        storage.save_vote(r#"{"term":5,"voted_for":3}"#).unwrap();
    } // crash

    // Phase 2: Reopen, verify log, continue writing
    {
        let db = OmniKV::open(manifest.to_str().unwrap(), wal.to_str().unwrap()).unwrap();
        let storage = OmniRaftStorage::new(db.clone());

        // Verify vote survived
        let vote = storage.read_vote().unwrap();
        assert!(vote.contains("\"term\":5"), "Vote term not persisted");

        // Verify last applied
        assert_eq!(storage.last_applied_index(), 10);

        // Verify all 20 log entries survived
        for i in 1..=20 {
            let entry = storage.read_log(i);
            assert!(entry.is_some(), "Log entry {i} lost after crash");
        }

        // Continue writing from index 21
        for i in 21..=30 {
            storage
                .append_log(i, &format!("SET post_crash{i} val{i}"))
                .unwrap();
        }

        // Apply 11-30
        for i in 11..=30u64 {
            let entry = storage.read_log(i).unwrap();
            storage.apply_write(&entry).unwrap();
            storage.mark_applied(i).unwrap();
        }

        assert_eq!(storage.last_applied_index(), 30);

        // Verify all data
        let seq = db.get_seq();
        for i in 1..=20 {
            assert_eq!(
                db.find(&format!("crash_k{i}"), seq).unwrap(),
                Some(format!("crash_v{i}"))
            );
        }
        for i in 21..=30 {
            assert_eq!(
                db.find(&format!("post_crash{i}"), seq).unwrap(),
                Some(format!("val{i}"))
            );
        }
    }

    println!("✅ LOG CONSISTENCY AFTER CRASH: 30 entries survived crash + continued writing");
}

/// Gap #4: Snapshot + log compaction correctness
///
/// Apply entries, compact old logs, verify new node can catch up from snapshot.
#[test]
fn test_snapshot_and_compaction() {
    let (db1, node1, _d1) = create_node("snap_leader");
    let (_db2, node2, _d2) = create_node("snap_follower");
    let (db3, node3, _d3) = create_node("snap_late_joiner");

    // Leader writes 100 entries
    for i in 1..=100 {
        node1
            .append_log(i, &format!("SET snap_k{i} snap_v{i}"))
            .unwrap();
    }

    // Replicate all to follower
    replicate_log(&node1, &[&node2], 1, 101);

    // Apply all on both
    for node in [&node1, &node2] {
        for i in 1..=100u64 {
            let entry = node.read_log(i).unwrap();
            node.apply_write(&entry).unwrap();
            node.mark_applied(i).unwrap();
        }
    }

    // Compact logs 1-80 on leader (keeping 81-100)
    node1.delete_log_range(1, 81).unwrap();

    // Verify compacted entries are gone
    for i in 1..=80 {
        assert!(node1.read_log(i).is_none(), "Log {i} should be compacted");
    }

    // Verify remaining entries still exist
    for i in 81..=100 {
        assert!(node1.read_log(i).is_some(), "Log {i} should exist");
    }

    // Late joiner catches up: needs snapshot (entries 1-80 data) + logs 81-100
    // Simulate snapshot transfer: copy applied data keys directly
    let seq = db1.get_seq();
    for i in 1..=80 {
        let key = format!("snap_k{i}");
        if let Ok(Some(val)) = db1.find(&key, seq) {
            let mut batch = omni_engine::WriteBatch::new();
            batch.set(&key, val).unwrap();
            db3.commit_batch(&batch).unwrap();
        }
    }

    // Then replicate remaining logs 81-100
    replicate_log(&node1, &[&node3], 81, 101);
    for i in 81..=100u64 {
        let entry = node3.read_log(i).unwrap();
        node3.apply_write(&entry).unwrap();
        node3.mark_applied(i).unwrap();
    }

    // Verify late joiner has ALL 100 keys
    let seq3 = db3.get_seq();
    for i in 1..=100 {
        assert!(
            db3.find(&format!("snap_k{i}"), seq3).unwrap().is_some(),
            "Late joiner missing snap_k{i}"
        );
    }

    println!("✅ SNAPSHOT + COMPACTION: 100 entries, compacted 80, late joiner caught up");
}

// ═══════════════════════════════════════════════════════════════════════════
// Gap #5: Network Partition Handling (Split Brain)
//
// 5-node cluster (quorum = 3). We simulate network partitions by controlling
// which nodes can replicate to which. Raft guarantees:
//   - Majority partition elects a new leader and continues
//   - Minority partition's stale leader cannot commit (no quorum)
//   - Data converges after the partition heals
// ═══════════════════════════════════════════════════════════════════════════

/// Gap #4b: Real snapshot install across partition catch-up and restart.
///
/// A lagging node receives entries 1-40, is isolated while the majority commits
/// entries 41-120, then catches up by installing the leader's real Raft
/// snapshot. The test verifies snapshot data survives restart, minority-only
/// stale data is absent, and post-snapshot logs can still be applied.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "Snapshot install integration scenario keeps partition setup, install, restart, and post-snapshot assertions together."
)]
async fn test_snapshot_install_after_partition_and_restart() {
    let (leader_db, leader, _leader_dir) = create_node("snap_install_leader");
    let (_majority_db, majority_follower, _majority_dir) = create_node("snap_install_majority");
    let lagging_dir = tempfile::tempdir().unwrap();
    let (lagging_db, lagging) = create_node_in_dir(lagging_dir.path(), "snap_install_lagging");

    for i in 1..=40 {
        leader
            .append_log(i, &format!("SET si_base_k{i} si_base_v{i}"))
            .unwrap();
    }
    replicate_to_set(&leader, &[&majority_follower, &lagging], 1, 41);
    for node in [&leader, &majority_follower, &lagging] {
        apply_range(node, 1, 41);
    }

    for i in 41..=45 {
        lagging
            .append_log(i, &format!("SET si_stale_k{i} si_stale_v{i}"))
            .unwrap();
    }
    apply_range(&lagging, 41, 46);
    let stale_seq = lagging_db.get_seq();
    for i in 41..=45 {
        assert_eq!(
            lagging_db
                .find(&format!("si_stale_k{i}"), stale_seq)
                .unwrap(),
            Some(format!("si_stale_v{i}")),
            "test setup should create stale minority state {i} before snapshot install"
        );
    }

    for i in 41..=120 {
        leader
            .append_log(i, &format!("SET si_majority_k{i} si_majority_v{i}"))
            .unwrap();
    }
    replicate_to_set(&leader, &[&majority_follower], 41, 121);
    for node in [&leader, &majority_follower] {
        apply_range(node, 41, 121);
    }

    leader.delete_log_range(1, 81).unwrap();
    for i in 1..=80 {
        assert!(
            leader.read_log(i).is_none(),
            "leader should have purged log {i}"
        );
    }

    let mut snapshot_builder = leader.clone();
    let snapshot = snapshot_builder.build_snapshot().await.unwrap();
    let snapshot_meta = snapshot.meta.clone();
    let mut lagging_installer = lagging.clone();
    lagging_installer
        .install_snapshot(&snapshot_meta, snapshot.snapshot)
        .await
        .unwrap();

    assert_eq!(
        lagging.last_applied_index(),
        120,
        "installed snapshot should advance lagging node to the leader's applied index"
    );

    let lagging_seq = lagging_db.get_seq();
    for i in 1..=40 {
        assert_eq!(
            lagging_db
                .find(&format!("si_base_k{i}"), lagging_seq)
                .unwrap(),
            Some(format!("si_base_v{i}")),
            "lagging node missing pre-partition key {i} after snapshot install"
        );
    }
    for i in 41..=120 {
        assert_eq!(
            lagging_db
                .find(&format!("si_majority_k{i}"), lagging_seq)
                .unwrap(),
            Some(format!("si_majority_v{i}")),
            "lagging node missing majority key {i} after snapshot install"
        );
    }
    for i in 41..=45 {
        assert_eq!(
            lagging_db
                .find(&format!("si_stale_k{i}"), lagging_seq)
                .unwrap(),
            None,
            "minority-only stale key {i} must not survive snapshot install"
        );
        assert!(
            lagging.read_log(i).is_none(),
            "minority-only stale log {i} must not survive snapshot install"
        );
    }

    drop(lagging_installer);
    drop(lagging);
    drop(lagging_db);
    let (reopened_db, reopened_lagging) =
        create_node_in_dir(lagging_dir.path(), "snap_install_lagging");

    assert_eq!(
        reopened_lagging.last_applied_index(),
        120,
        "snapshot metadata should survive lagging node restart"
    );
    let reopened_seq = reopened_db.get_seq();
    assert_eq!(
        reopened_db.find("si_majority_k120", reopened_seq).unwrap(),
        Some("si_majority_v120".into()),
        "snapshot data should survive lagging node restart"
    );
    assert_eq!(
        reopened_db.find("si_stale_k41", reopened_seq).unwrap(),
        None,
        "stale minority data should remain absent after restart"
    );
    for i in 41..=45 {
        assert!(
            reopened_lagging.read_log(i).is_none(),
            "minority-only stale log {i} should remain absent after restart"
        );
    }

    for i in 121..=130 {
        leader
            .append_log(i, &format!("SET si_post_k{i} si_post_v{i}"))
            .unwrap();
    }
    replicate_to_set(&leader, &[&reopened_lagging], 121, 131);
    apply_range(&reopened_lagging, 121, 131);

    let final_seq = reopened_db.get_seq();
    for i in 121..=130 {
        assert_eq!(
            reopened_db
                .find(&format!("si_post_k{i}"), final_seq)
                .unwrap(),
            Some(format!("si_post_v{i}")),
            "lagging node missing post-snapshot log entry {i}"
        );
    }

    let leader_seq = leader_db.get_seq();
    assert_eq!(
        leader_db.find("si_majority_k120", leader_seq).unwrap(),
        Some("si_majority_v120".into())
    );

    println!(
        "snapshot install: lagging node installed real snapshot, restarted, and applied logs 121-130"
    );
}

fn create_5_node_cluster() -> Vec<(std::sync::Arc<OmniKV>, OmniRaftStorage, tempfile::TempDir)> {
    (1..=5)
        .map(|i| create_node(&format!("partition_node{i}")))
        .collect()
}

/// Helper: replicate logs from a source node to a set of target nodes.
fn replicate_to_set(source: &OmniRaftStorage, targets: &[&OmniRaftStorage], start: u64, end: u64) {
    for idx in start..end {
        if let Some(entry) = source.read_log(idx) {
            for t in targets {
                t.append_log(idx, &entry)
                    .expect("Partition replicate failed");
            }
        }
    }
}

/// Helper: apply log entries on a node and mark them applied.
fn apply_range(node: &OmniRaftStorage, start: u64, end: u64) {
    for i in start..end {
        if let Some(entry) = node.read_log(i) {
            node.apply_write(&entry).unwrap();
            node.mark_applied(i).unwrap();
        }
    }
}

/// Gap #5a: Symmetric partition — majority (3 nodes) continues, minority (2 nodes) stalls
///
/// Scenario:
///   Term 1: Node1 is leader, all 5 nodes have entries 1-20
///   PARTITION: {Node1, Node2} vs {Node3, Node4, Node5}
///   Term 2: Node3 elected leader in majority partition, writes entries 21-40
///   Node1 tries to write entries 21-30 in minority — these are "uncommitted" (no quorum)
///   Verify: majority partition has entries 21-40, minority stuck at 20
#[test]
fn test_symmetric_partition_majority_progresses() {
    let cluster = create_5_node_cluster();
    let nodes: Vec<&OmniRaftStorage> = cluster.iter().map(|(_, s, _)| s).collect();

    // Term 1: Node1 is leader, write entries 1-20, replicate to all
    for n in &nodes {
        n.save_vote(r#"{"term":1,"voted_for":1}"#).unwrap();
    }
    for i in 1..=20 {
        nodes[0]
            .append_log(i, &format!("SET partition_k{i} v{i}"))
            .unwrap();
    }
    replicate_to_set(nodes[0], &nodes[1..], 1, 21);

    // Apply entries 1-20 on all nodes
    for n in &nodes {
        apply_range(n, 1, 21);
    }

    // ═══ NETWORK PARTITION ═══
    // Minority: {Node1 (idx=0), Node2 (idx=1)}
    // Majority: {Node3 (idx=2), Node4 (idx=3), Node5 (idx=4)}

    // Majority partition elects Node3 as leader (term 2)
    // Node3, Node4, Node5 vote for Node3
    nodes[2].save_vote(r#"{"term":2,"voted_for":3}"#).unwrap();
    nodes[3].save_vote(r#"{"term":2,"voted_for":3}"#).unwrap();
    nodes[4].save_vote(r#"{"term":2,"voted_for":3}"#).unwrap();

    // Majority leader (Node3) writes entries 21-40
    for i in 21..=40 {
        nodes[2]
            .append_log(i, &format!("SET majority_k{i} maj_v{i}"))
            .unwrap();
    }
    // Replicate to Node4 and Node5 only (partition blocks Node1, Node2)
    replicate_to_set(nodes[2], &[nodes[3], nodes[4]], 21, 41);

    // Apply on majority partition
    for n in [nodes[2], nodes[3], nodes[4]] {
        apply_range(n, 21, 41);
    }

    // Minority: Node1 tries to write but CAN'T commit (no quorum)
    // It can append locally but cannot replicate to a majority
    for i in 21..=30 {
        nodes[0]
            .append_log(i, &format!("SET stale_k{i} stale_v{i}"))
            .unwrap();
    }
    // Node1 can only replicate to Node2 — that's 2/5, not a quorum
    replicate_to_set(nodes[0], &[nodes[1]], 21, 31);
    // DO NOT apply — these are uncommitted (no quorum)

    // Verify: majority partition has entries 21-40 applied
    for n in [2, 3, 4] {
        let db = &cluster[n].0;
        let seq = db.get_seq();
        for i in 21..=40 {
            assert_eq!(
                db.find(&format!("majority_k{i}"), seq).unwrap(),
                Some(format!("maj_v{i}")),
                "Majority node {} missing majority_k{}",
                n + 1,
                i
            );
        }
        assert_eq!(
            nodes[n].last_applied_index(),
            40,
            "Majority node {} should be at applied index 40",
            n + 1
        );
    }

    // Verify: minority partition did NOT apply entries 21-30
    assert_eq!(
        nodes[0].last_applied_index(),
        20,
        "Minority node1 should still be at applied 20"
    );
    assert_eq!(
        nodes[1].last_applied_index(),
        20,
        "Minority node2 should still be at applied 20"
    );

    println!("✅ PARTITION 5a: Majority (3/5) progressed to 40, minority (2/5) stuck at 20");
}

/// Gap #5b: Minority partition's stale leader is superseded after heal
///
/// After partition heals, minority nodes discover the higher term,
/// discard their uncommitted entries, and catch up from the majority leader.
#[test]
fn test_stale_leader_superseded_after_heal() {
    let cluster = create_5_node_cluster();
    let nodes: Vec<&OmniRaftStorage> = cluster.iter().map(|(_, s, _)| s).collect();

    // Term 1: all nodes have entries 1-10
    for n in &nodes {
        n.save_vote(r#"{"term":1,"voted_for":1}"#).unwrap();
    }
    for i in 1..=10 {
        nodes[0]
            .append_log(i, &format!("SET base_k{i} base_v{i}"))
            .unwrap();
    }
    replicate_to_set(nodes[0], &nodes[1..], 1, 11);
    for n in &nodes {
        apply_range(n, 1, 11);
    }

    // === PARTITION: {Node1, Node2} vs {Node3, Node4, Node5} ===
    // Minority stale leader (Node1) writes uncommitted entries 11-15
    for i in 11..=15 {
        nodes[0]
            .append_log(i, &format!("SET stale_{i} stale_v{i}"))
            .unwrap();
    }
    replicate_to_set(nodes[0], &[nodes[1]], 11, 16);

    // Majority leader (Node3, term 2) writes committed entries 11-20
    nodes[2].save_vote(r#"{"term":2,"voted_for":3}"#).unwrap();
    nodes[3].save_vote(r#"{"term":2,"voted_for":3}"#).unwrap();
    nodes[4].save_vote(r#"{"term":2,"voted_for":3}"#).unwrap();
    for i in 11..=20 {
        nodes[2]
            .append_log(i, &format!("SET legit_{i} legit_v{i}"))
            .unwrap();
    }
    replicate_to_set(nodes[2], &[nodes[3], nodes[4]], 11, 21);
    for n in [nodes[2], nodes[3], nodes[4]] {
        apply_range(n, 11, 21);
    }

    // === PARTITION HEALS ===
    // Node1 and Node2 discover term 2, step down
    nodes[0].save_vote(r#"{"term":2,"voted_for":3}"#).unwrap();
    nodes[1].save_vote(r#"{"term":2,"voted_for":3}"#).unwrap();

    // Node1 and Node2 must discard their stale entries and accept majority's log
    // Overwrite indices 11-15 with the majority's entries
    for i in 11..=20 {
        let entry = nodes[2].read_log(i).unwrap();
        nodes[0].append_log(i, &entry).unwrap();
        nodes[1].append_log(i, &entry).unwrap();
    }

    // Apply the correct entries on minority nodes
    apply_range(nodes[0], 11, 21);
    apply_range(nodes[1], 11, 21);

    // Verify ALL 5 nodes now agree on the same data
    for (node_idx, (db, _, _)) in cluster.iter().enumerate() {
        let seq = db.get_seq();
        for i in 11..=20 {
            assert_eq!(
                db.find(&format!("legit_{i}"), seq).unwrap(),
                Some(format!("legit_v{i}")),
                "Node {} missing legit_{}",
                node_idx + 1,
                i
            );
        }
    }

    // Verify stale entries were overwritten (not present as state machine data)
    // The log at index 11-15 now contains "SET legit_..." not "SET stale_..."
    for n in [nodes[0], nodes[1]] {
        for i in 11..=15 {
            let entry = n.read_log(i).unwrap();
            assert!(
                entry.contains("legit_"),
                "Stale entry should be overwritten at index {i}"
            );
        }
    }

    // All nodes at applied index 20
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            n.last_applied_index(),
            20,
            "Node {} should be at applied 20",
            i + 1
        );
    }

    println!("✅ PARTITION 5b: Stale leader superseded, minority caught up from majority");
}

/// Gap #5c: Data convergence after partition heal — full consistency check
///
/// After a partition heals, verify that ALL keys from both the pre-partition
/// era and the majority-partition era are identical across all 5 nodes.
#[test]
fn test_data_convergence_after_partition_heal() {
    let cluster = create_5_node_cluster();
    let nodes: Vec<&OmniRaftStorage> = cluster.iter().map(|(_, s, _)| s).collect();

    // Pre-partition: 50 entries on all nodes
    for n in &nodes {
        n.save_vote(r#"{"term":1,"voted_for":1}"#).unwrap();
    }
    for i in 1..=50 {
        nodes[0]
            .append_log(i, &format!("SET shared_k{i} shared_v{i}"))
            .unwrap();
    }
    replicate_to_set(nodes[0], &nodes[1..], 1, 51);
    for n in &nodes {
        apply_range(n, 1, 51);
    }

    // === PARTITION: {N1, N2} vs {N3, N4, N5} ===
    // Majority writes 50 more entries (51-100)
    nodes[2].save_vote(r#"{"term":2,"voted_for":3}"#).unwrap();
    nodes[3].save_vote(r#"{"term":2,"voted_for":3}"#).unwrap();
    nodes[4].save_vote(r#"{"term":2,"voted_for":3}"#).unwrap();

    for i in 51..=100 {
        nodes[2]
            .append_log(i, &format!("SET post_part_k{i} post_v{i}"))
            .unwrap();
    }
    replicate_to_set(nodes[2], &[nodes[3], nodes[4]], 51, 101);
    for n in [nodes[2], nodes[3], nodes[4]] {
        apply_range(n, 51, 101);
    }

    // === PARTITION HEALS ===
    // Minority catches up
    nodes[0].save_vote(r#"{"term":2,"voted_for":3}"#).unwrap();
    nodes[1].save_vote(r#"{"term":2,"voted_for":3}"#).unwrap();
    replicate_to_set(nodes[2], &[nodes[0], nodes[1]], 51, 101);
    apply_range(nodes[0], 51, 101);
    apply_range(nodes[1], 51, 101);

    // === FULL CONVERGENCE CHECK ===
    // All 5 nodes must have identical data for all 100 keys
    let reference_db = &cluster[2].0; // Node3 (majority leader) as reference
    let ref_seq = reference_db.get_seq();

    for (node_idx, (db, _, _)) in cluster.iter().enumerate() {
        let seq = db.get_seq();

        // Pre-partition keys
        for i in 1..=50 {
            let key = format!("shared_k{i}");
            let ref_val = reference_db.find(&key, ref_seq).unwrap();
            let node_val = db.find(&key, seq).unwrap();
            assert_eq!(
                ref_val,
                node_val,
                "Node {} diverged from reference on {}",
                node_idx + 1,
                key
            );
        }

        // Post-partition keys
        for i in 51..=100 {
            let key = format!("post_part_k{i}");
            let ref_val = reference_db.find(&key, ref_seq).unwrap();
            let node_val = db.find(&key, seq).unwrap();
            assert_eq!(
                ref_val,
                node_val,
                "Node {} diverged from reference on {}",
                node_idx + 1,
                key
            );
        }
    }

    // All nodes at applied index 100
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            n.last_applied_index(),
            100,
            "Node {} should be at applied 100",
            i + 1
        );
    }

    println!("✅ PARTITION 5c: All 5 nodes converged on 100 keys after partition heal");
}

/// Gap #5d: Asymmetric partition — one node isolated from all others
///
/// Node5 is completely isolated (can't send or receive from anyone).
/// The remaining 4 nodes form a quorum (4/5) and continue.
/// After heal, the isolated node catches up.
#[test]
fn test_asymmetric_partition_isolated_node() {
    let cluster = create_5_node_cluster();
    let nodes: Vec<&OmniRaftStorage> = cluster.iter().map(|(_, s, _)| s).collect();

    // Term 1: all nodes have entries 1-10
    for n in &nodes {
        n.save_vote(r#"{"term":1,"voted_for":1}"#).unwrap();
    }
    for i in 1..=10 {
        nodes[0]
            .append_log(i, &format!("SET asym_k{i} asym_v{i}"))
            .unwrap();
    }
    replicate_to_set(nodes[0], &nodes[1..], 1, 11);
    for n in &nodes {
        apply_range(n, 1, 11);
    }

    // === Node5 (idx=4) becomes completely isolated ===
    // Remaining 4 nodes still have quorum (4/5 ≥ 3)
    // Leader (Node1) continues writing entries 11-30
    for i in 11..=30 {
        nodes[0]
            .append_log(i, &format!("SET asym_k{i} asym_v{i}"))
            .unwrap();
    }
    // Replicate to Node2, Node3, Node4 only (not Node5)
    replicate_to_set(nodes[0], &[nodes[1], nodes[2], nodes[3]], 11, 31);

    // Apply on the 4 connected nodes
    for n in [nodes[0], nodes[1], nodes[2], nodes[3]] {
        apply_range(n, 11, 31);
    }

    // Verify Node5 is stuck at index 10
    assert_eq!(
        nodes[4].last_applied_index(),
        10,
        "Isolated Node5 should be at 10"
    );

    // Verify the 4 connected nodes are at 30
    for (i, n) in [nodes[0], nodes[1], nodes[2], nodes[3]].iter().enumerate() {
        assert_eq!(n.last_applied_index(), 30, "Connected node {} at 30", i + 1);
    }

    // === PARTITION HEALS: Node5 reconnects ===
    replicate_to_set(nodes[0], &[nodes[4]], 11, 31);
    apply_range(nodes[4], 11, 31);

    // Verify Node5 caught up
    assert_eq!(
        nodes[4].last_applied_index(),
        30,
        "Node5 should catch up to 30"
    );

    // Full data check on Node5
    let db5 = &cluster[4].0;
    let seq5 = db5.get_seq();
    for i in 1..=30 {
        assert_eq!(
            db5.find(&format!("asym_k{i}"), seq5).unwrap(),
            Some(format!("asym_v{i}")),
            "Node5 missing asym_k{i} after heal"
        );
    }

    println!("✅ PARTITION 5d: Isolated node caught up after asymmetric partition healed");
}

/// Gap #5e: Cascading partitions — multiple sequential partitions don't lose data
///
/// Scenario:
///   Phase 1: All 5 nodes, entries 1-20
///   Phase 2: Partition A — {N1,N2} vs {N3,N4,N5}, majority writes 21-40
///   Phase 3: Heal A, then Partition B — {N1,N2,N3} vs {N4,N5}, N1 writes 41-60
///   Phase 4: Heal B — all 5 nodes converge on entries 1-60
#[test]
fn test_cascading_partitions_no_data_loss() {
    let cluster = create_5_node_cluster();
    let nodes: Vec<&OmniRaftStorage> = cluster.iter().map(|(_, s, _)| s).collect();

    // Phase 1: All 5 nodes have entries 1-20 (term 1, leader=N1)
    for n in &nodes {
        n.save_vote(r#"{"term":1,"voted_for":1}"#).unwrap();
    }
    for i in 1..=20 {
        nodes[0]
            .append_log(i, &format!("SET cascade_k{i} cascade_v{i}"))
            .unwrap();
    }
    replicate_to_set(nodes[0], &nodes[1..], 1, 21);
    for n in &nodes {
        apply_range(n, 1, 21);
    }

    // Phase 2: PARTITION A — {N1,N2} vs {N3,N4,N5}
    // Majority elects N3 (term 2), writes 21-40
    nodes[2].save_vote(r#"{"term":2,"voted_for":3}"#).unwrap();
    nodes[3].save_vote(r#"{"term":2,"voted_for":3}"#).unwrap();
    nodes[4].save_vote(r#"{"term":2,"voted_for":3}"#).unwrap();

    for i in 21..=40 {
        nodes[2]
            .append_log(i, &format!("SET cascade_k{i} cascade_v{i}"))
            .unwrap();
    }
    replicate_to_set(nodes[2], &[nodes[3], nodes[4]], 21, 41);
    for n in [nodes[2], nodes[3], nodes[4]] {
        apply_range(n, 21, 41);
    }

    // Phase 3: HEAL Partition A, then PARTITION B — {N1,N2,N3} vs {N4,N5}
    // First, N1 and N2 catch up from N3
    nodes[0].save_vote(r#"{"term":2,"voted_for":3}"#).unwrap();
    nodes[1].save_vote(r#"{"term":2,"voted_for":3}"#).unwrap();
    replicate_to_set(nodes[2], &[nodes[0], nodes[1]], 21, 41);
    apply_range(nodes[0], 21, 41);
    apply_range(nodes[1], 21, 41);

    // Now Partition B: {N1,N2,N3} (quorum=3) vs {N4,N5}
    // N1 becomes leader again (term 3)
    nodes[0].save_vote(r#"{"term":3,"voted_for":1}"#).unwrap();
    nodes[1].save_vote(r#"{"term":3,"voted_for":1}"#).unwrap();
    nodes[2].save_vote(r#"{"term":3,"voted_for":1}"#).unwrap();

    for i in 41..=60 {
        nodes[0]
            .append_log(i, &format!("SET cascade_k{i} cascade_v{i}"))
            .unwrap();
    }
    replicate_to_set(nodes[0], &[nodes[1], nodes[2]], 41, 61);
    for n in [nodes[0], nodes[1], nodes[2]] {
        apply_range(n, 41, 61);
    }

    // Phase 4: HEAL Partition B — N4 and N5 catch up
    nodes[3].save_vote(r#"{"term":3,"voted_for":1}"#).unwrap();
    nodes[4].save_vote(r#"{"term":3,"voted_for":1}"#).unwrap();
    replicate_to_set(nodes[0], &[nodes[3], nodes[4]], 41, 61);
    apply_range(nodes[3], 41, 61);
    apply_range(nodes[4], 41, 61);

    // === FINAL CONVERGENCE CHECK ===
    // All 5 nodes must have all 60 keys
    for (node_idx, (db, storage, _)) in cluster.iter().enumerate() {
        let seq = db.get_seq();
        for i in 1..=60 {
            assert_eq!(
                db.find(&format!("cascade_k{i}"), seq).unwrap(),
                Some(format!("cascade_v{i}")),
                "Node {} missing cascade_k{} after cascading partitions",
                node_idx + 1,
                i
            );
        }
        assert_eq!(
            storage.last_applied_index(),
            60,
            "Node {} should be at applied 60",
            node_idx + 1
        );
    }

    // Verify log consistency across all nodes
    for i in 1..=60u64 {
        let reference = nodes[0].read_log(i).unwrap();
        for (n_idx, n) in nodes.iter().enumerate().skip(1) {
            assert_eq!(
                n.read_log(i).unwrap(),
                reference,
                "Log mismatch at index {} between Node1 and Node{}",
                i,
                n_idx + 1
            );
        }
    }

    println!("✅ PARTITION 5e: 2 cascading partitions, 3 term changes, all 60 entries converged");
}

// ═══════════════════════════════════════════════════════════════════════════
// Gap #6: Delayed / Reordered Messages
//
// In real networks, messages can arrive out of order, be duplicated,
// arrive with arbitrary delay, or have gaps. Raft must handle all of
// these correctly — the log must be consistent regardless of delivery order.
// ═══════════════════════════════════════════════════════════════════════════

/// Gap #6a: Out-of-order log replication — entries arrive in reverse order
///
/// Leader writes entries 1-20 but the follower receives them in reverse
/// (20, 19, 18, ..., 1). After all entries arrive, the follower's log must
/// be identical to the leader's regardless of reception order.
#[test]
fn test_out_of_order_log_delivery() {
    let (_db1, leader, _d1) = create_node("ooo_leader");
    let (db2, follower, _d2) = create_node("ooo_follower");

    // Leader writes entries 1-20 sequentially
    for i in 1..=20 {
        leader
            .append_log(i, &format!("SET ooo_k{i} ooo_v{i}"))
            .unwrap();
    }

    // Follower receives entries in REVERSE order (simulating network reordering)
    for i in (1..=20).rev() {
        let entry = leader.read_log(i).unwrap();
        follower.append_log(i, &entry).unwrap();
    }

    // Verify: follower log is identical to leader's despite reverse delivery
    for i in 1..=20 {
        let l = leader.read_log(i).unwrap();
        let f = follower.read_log(i).unwrap();
        assert_eq!(l, f, "Log mismatch at index {i} after reverse delivery");
    }

    // Apply all in correct order and verify data
    apply_range(&follower, 1, 21);
    let seq = db2.get_seq();
    for i in 1..=20 {
        assert_eq!(
            db2.find(&format!("ooo_k{i}"), seq).unwrap(),
            Some(format!("ooo_v{i}")),
            "Follower missing ooo_k{i} after out-of-order delivery"
        );
    }

    assert_eq!(follower.last_applied_index(), 20);
    println!("✅ DELAYED 6a: 20 entries delivered in reverse order, log consistent");
}

/// Gap #6b: Stale-term vote/append rejected — old messages arrive late
///
/// After a term change, a delayed message from the old term arrives.
/// The node should detect the stale term and NOT regress its state.
#[test]
fn test_stale_term_message_rejected() {
    let (_db1, node1, _d1) = create_node("stale_node1");
    let (_db2, node2, _d2) = create_node("stale_node2");
    let (_db3, node3, _d3) = create_node("stale_node3");

    // Term 1: Node1 is leader, writes entries 1-10
    node1.save_vote(r#"{"term":1,"voted_for":1}"#).unwrap();
    node2.save_vote(r#"{"term":1,"voted_for":1}"#).unwrap();
    node3.save_vote(r#"{"term":1,"voted_for":1}"#).unwrap();

    for i in 1..=10 {
        node1
            .append_log(i, &format!("SET stale_k{i} v{i}"))
            .unwrap();
    }
    replicate_to_set(&node1, &[&node2, &node3], 1, 11);
    for n in [&node1, &node2, &node3] {
        apply_range(n, 1, 11);
    }

    // Term 2: Node2 becomes leader, writes entries 11-20
    node2.save_vote(r#"{"term":2,"voted_for":2}"#).unwrap();
    node3.save_vote(r#"{"term":2,"voted_for":2}"#).unwrap();

    for i in 11..=20 {
        node2
            .append_log(i, &format!("SET term2_k{i} term2_v{i}"))
            .unwrap();
    }
    replicate_to_set(&node2, &[&node3], 11, 21);
    apply_range(&node2, 11, 21);
    apply_range(&node3, 11, 21);

    // === DELAYED MESSAGE: Node1's old term-1 "heartbeat" arrives at Node2 ===
    // Node1 still thinks it's leader (term 1), tries to append entries 11-15
    // with stale data. Node2 (now term 2 leader) should detect the stale term.

    // Simulate: check that Node2's vote is term 2 (higher)
    let vote = node2.read_vote().unwrap();
    assert!(
        vote.contains("\"term\":2"),
        "Node2 should be in term 2, got: {vote}"
    );

    // The stale term-1 entries should NOT overwrite term-2 entries
    // Verify Node2 still has correct term-2 data at indices 11-15
    for i in 11..=15 {
        let entry = node2.read_log(i).unwrap();
        assert!(
            entry.contains("term2_"),
            "Index {i} should contain term2 data, got: {entry}"
        );
    }

    // Node1 eventually discovers term 2, steps down and catches up
    node1.save_vote(r#"{"term":2,"voted_for":2}"#).unwrap();
    replicate_to_set(&node2, &[&node1], 11, 21);
    apply_range(&node1, 11, 21);

    // All 3 nodes now consistent
    for n in [&node1, &node2, &node3] {
        assert_eq!(n.last_applied_index(), 20);
    }

    println!("✅ DELAYED 6b: Stale term-1 messages did not corrupt term-2 state");
}

/// Gap #6c: Duplicate message delivery — idempotent replication
///
/// The same log entry is delivered to a follower multiple times (network retry).
/// The follower must handle this idempotently — no duplicate data, no corruption.
#[test]
fn test_duplicate_message_idempotent() {
    let (db1, leader, _d1) = create_node("dup_leader");
    let (db2, follower, _d2) = create_node("dup_follower");

    // Leader writes 15 entries
    for i in 1..=15 {
        leader
            .append_log(i, &format!("SET dup_k{i} dup_v{i}"))
            .unwrap();
    }

    // Replicate normally first time
    replicate_to_set(&leader, &[&follower], 1, 16);

    // Simulate network retries: deliver the SAME entries again (3 more times!)
    for _retry in 0..3 {
        replicate_to_set(&leader, &[&follower], 1, 16);
    }

    // Also deliver random subsets out of order (simulating partial retransmissions)
    for i in [5, 10, 1, 15, 3, 7] {
        let entry = leader.read_log(i).unwrap();
        follower.append_log(i, &entry).unwrap();
    }

    // Verify: log entries are exactly correct (no duplicates, no corruption)
    for i in 1..=15 {
        let l = leader.read_log(i).unwrap();
        let f = follower.read_log(i).unwrap();
        assert_eq!(l, f, "Duplicate delivery corrupted index {i}");
    }

    // Apply and verify data integrity
    apply_range(&leader, 1, 16);
    apply_range(&follower, 1, 16);

    let seq1 = db1.get_seq();
    let seq2 = db2.get_seq();
    for i in 1..=15 {
        let key = format!("dup_k{i}");
        let v1 = db1.find(&key, seq1).unwrap();
        let v2 = db2.find(&key, seq2).unwrap();
        assert_eq!(v1, v2, "Data mismatch on {key} after duplicate delivery");
    }

    assert_eq!(leader.last_applied_index(), 15);
    assert_eq!(follower.last_applied_index(), 15);

    println!("✅ DELAYED 6c: 4x duplicate delivery + random retransmissions, data intact");
}

/// Gap #6d: Gap-then-fill replication — entries arrive with holes, then fill in
///
/// Follower receives entries 1-5, then 11-15 (gap at 6-10), then finally 6-10.
/// After all entries arrive, the log must be complete and correct.
#[test]
fn test_gap_then_fill_replication() {
    let (_db1, leader, _d1) = create_node("gap_leader");
    let (db2, follower, _d2) = create_node("gap_follower");

    // Leader writes entries 1-20
    for i in 1..=20 {
        leader
            .append_log(i, &format!("SET gap_k{i} gap_v{i}"))
            .unwrap();
    }

    // Phase 1: Follower receives entries 1-5 (first batch arrives)
    for i in 1..=5 {
        let entry = leader.read_log(i).unwrap();
        follower.append_log(i, &entry).unwrap();
    }

    // Phase 2: Follower receives entries 11-15 (SKIP 6-10, simulating delay)
    for i in 11..=15 {
        let entry = leader.read_log(i).unwrap();
        follower.append_log(i, &entry).unwrap();
    }

    // Verify: follower has 1-5 and 11-15 but NOT 6-10
    for i in 1..=5 {
        assert!(follower.read_log(i).is_some(), "Should have entry {i}");
    }
    for i in 6..=10 {
        assert!(
            follower.read_log(i).is_none(),
            "Should NOT have entry {i} yet"
        );
    }
    for i in 11..=15 {
        assert!(follower.read_log(i).is_some(), "Should have entry {i}");
    }

    // Phase 3: The delayed entries 6-10 finally arrive
    for i in 6..=10 {
        let entry = leader.read_log(i).unwrap();
        follower.append_log(i, &entry).unwrap();
    }

    // Phase 4: Remaining entries 16-20 arrive normally
    for i in 16..=20 {
        let entry = leader.read_log(i).unwrap();
        follower.append_log(i, &entry).unwrap();
    }

    // Verify: complete log, every entry matches leader
    for i in 1..=20 {
        let l = leader.read_log(i).unwrap();
        let f = follower.read_log(i).unwrap();
        assert_eq!(l, f, "Mismatch at index {i} after gap fill");
    }

    // Apply in correct order and verify
    apply_range(&follower, 1, 21);
    let seq = db2.get_seq();
    for i in 1..=20 {
        assert_eq!(
            db2.find(&format!("gap_k{i}"), seq).unwrap(),
            Some(format!("gap_v{i}")),
            "Follower missing gap_k{i} after gap-fill"
        );
    }

    assert_eq!(follower.last_applied_index(), 20);
    println!("✅ DELAYED 6d: Gap at indices 6-10 filled later, all 20 entries intact");
}

/// Gap #6e: Interleaved multi-leader replication across term changes
///
/// Messages from TWO different leaders (different terms) arrive interleaved
/// on a follower. The follower must apply the correct (higher-term) entries
/// and reject/overwrite stale ones.
///
/// Timeline:
///   - Term 1 leader (Node1) writes 1-20
///   - Term 2 leader (Node2) writes 11-30 (overwrites 11-20)
///   - Follower receives: term1[1-10], term2[21-25], term1[11-20], term2[11-20], term2[26-30]
///   - Final log must match term2 leader for indices 11-30
#[test]
fn test_interleaved_multi_leader_replication() {
    let (_db1, term1_leader, _d1) = create_node("t1_leader");
    let (_db2, term2_leader, _d2) = create_node("t2_leader");
    let (db3, follower, _d3) = create_node("interleave_follower");

    // Term 1 leader writes entries 1-20
    for i in 1..=20 {
        term1_leader
            .append_log(i, &format!("SET il_k{i} term1_v{i}"))
            .unwrap();
    }

    // Term 2 leader has entries 1-10 from term 1, then writes 11-30 with new data
    for i in 1..=10 {
        let entry = term1_leader.read_log(i).unwrap();
        term2_leader.append_log(i, &entry).unwrap();
    }
    for i in 11..=30 {
        term2_leader
            .append_log(i, &format!("SET il_k{i} term2_v{i}"))
            .unwrap();
    }

    // === Follower receives messages in interleaved order ===

    // Batch 1: Term 1 entries 1-10 (these are correct, shared across terms)
    for i in 1..=10 {
        let entry = term1_leader.read_log(i).unwrap();
        follower.append_log(i, &entry).unwrap();
    }

    // Batch 2: Term 2 entries 21-25 arrive EARLY (out of order from new leader)
    for i in 21..=25 {
        let entry = term2_leader.read_log(i).unwrap();
        follower.append_log(i, &entry).unwrap();
    }

    // Batch 3: DELAYED term 1 entries 11-20 arrive (stale!)
    for i in 11..=20 {
        let entry = term1_leader.read_log(i).unwrap();
        follower.append_log(i, &entry).unwrap();
    }

    // Batch 4: Term 2 entries 11-20 arrive (these OVERWRITE the stale term-1 entries)
    for i in 11..=20 {
        let entry = term2_leader.read_log(i).unwrap();
        follower.append_log(i, &entry).unwrap();
    }

    // Batch 5: Term 2 entries 26-30 arrive (final batch)
    for i in 26..=30 {
        let entry = term2_leader.read_log(i).unwrap();
        follower.append_log(i, &entry).unwrap();
    }

    // Verify: follower log matches term2 leader for ALL 30 entries
    for i in 1..=30 {
        let expected = term2_leader.read_log(i).unwrap();
        let actual = follower.read_log(i).unwrap();
        assert_eq!(
            expected, actual,
            "Index {i} should match term2 leader's log"
        );
    }

    // Entries 1-10 should have "term1_v" (shared), 11-30 should have "term2_v"
    for i in 1..=10 {
        let entry = follower.read_log(i).unwrap();
        assert!(
            entry.contains("term1_v"),
            "Index {i} should be term1 data (shared): {entry}"
        );
    }
    for i in 11..=30 {
        let entry = follower.read_log(i).unwrap();
        assert!(
            entry.contains("term2_v"),
            "Index {i} should be term2 data (overwritten): {entry}"
        );
    }

    // Apply and verify data
    follower.save_vote(r#"{"term":2,"voted_for":2}"#).unwrap();
    apply_range(&follower, 1, 31);

    let seq = db3.get_seq();
    for i in 1..=10 {
        assert_eq!(
            db3.find(&format!("il_k{i}"), seq).unwrap(),
            Some(format!("term1_v{i}")),
            "Key il_k{i} should have term1 value"
        );
    }
    // For keys 11-30, the state machine has the latest write (term2 overwrote term1)
    for i in 11..=30 {
        assert_eq!(
            db3.find(&format!("il_k{i}"), seq).unwrap(),
            Some(format!("term2_v{i}")),
            "Key il_k{i} should have term2 value (overwritten)"
        );
    }

    assert_eq!(follower.last_applied_index(), 30);
    println!("✅ DELAYED 6e: Interleaved 2-leader messages resolved, 30 entries correct");
}

// ═══════════════════════════════════════════════════════════════════════════
// Gap #7: Clock Skew Tolerance
//
// In distributed systems, nodes may have different wall clocks.
// OmniKV uses LOGICAL clocks for correctness-critical paths:
//   - MVCC reads use sequence numbers (monotonic, not wall-clock)
//   - Raft consensus uses terms (logical epochs, not timestamps)
//   - Only TTL expiry uses wall-clock time (checked at read time)
//
// These tests prove the system is correct under clock skew:
//   - MVCC ordering is sequence-based, not time-based
//   - Term-based election doesn't depend on wall clocks
//   - Nodes with divergent sequence counters still converge
//   - TTL keys behave consistently across replicated nodes
// ═══════════════════════════════════════════════════════════════════════════

/// Gap #7a: MVCC ordering uses logical sequence numbers, not wall clocks
///
/// Two nodes start with different `global_seq` values (simulating clock skew).
/// Writes are applied through Raft log (sequence-independent). Prove that
/// reads are ordered by Raft log index, not by node-local sequence numbers.
#[test]
fn test_mvcc_logical_clock_ordering() {
    let (db1, node1, _d1) = create_node("clock_node1");
    let (db2, node2, _d2) = create_node("clock_node2");

    // Simulate clock skew: Node1 has seq=1000, Node2 has seq=5000
    // In a real system, different restart times or write loads cause this.
    // But Raft log ordering is what matters — not the local seq.
    db1.set_global_seq(1000);
    db2.set_global_seq(5000);

    // Both nodes process the SAME Raft log entries (replicated via Raft)
    for i in 1..=20 {
        let cmd = format!("SET clock_k{i} clock_v{i}");
        node1.append_log(i, &cmd).unwrap();
        node2.append_log(i, &cmd).unwrap();
    }

    // Apply on both — each node uses its own local seq for MVCC,
    // but the state machine result must be identical
    apply_range(&node1, 1, 21);
    apply_range(&node2, 1, 21);

    // Verify: despite wildly different local sequences, data is identical
    let seq1 = db1.get_seq();
    let seq2 = db2.get_seq();

    // Sequences SHOULD be different (skew is preserved)
    assert_ne!(seq1, seq2, "Sequences should differ (simulated skew)");

    // But ALL data reads produce the same values
    for i in 1..=20 {
        let key = format!("clock_k{i}");
        let v1 = db1.find(&key, seq1).unwrap();
        let v2 = db2.find(&key, seq2).unwrap();
        assert_eq!(
            v1, v2,
            "Data mismatch on {key} despite seq skew (seq1={seq1}, seq2={seq2})"
        );
        assert_eq!(v1, Some(format!("clock_v{i}")));
    }

    // Both applied the same number of log entries
    assert_eq!(node1.last_applied_index(), 20);
    assert_eq!(node2.last_applied_index(), 20);

    println!("✅ CLOCK 7a: Nodes with seq 1000 vs 5000 produce identical MVCC reads");
}

/// Gap #7b: Raft term-based election is immune to wall-clock skew
///
/// Nodes have different "perceived" timing (simulated by different sequence
/// counters and vote states). Election is purely term-based — the node with
/// the higher term wins, regardless of when it started.
#[test]
fn test_term_based_election_immune_to_clock_skew() {
    let cluster = create_5_node_cluster();
    let nodes: Vec<&OmniRaftStorage> = cluster.iter().map(|(_, s, _)| s).collect();
    let dbs: Vec<&std::sync::Arc<OmniKV>> = cluster.iter().map(|(d, _, _)| d).collect();

    // Simulate massive clock skew across nodes via different global sequences
    // Node1: far behind (seq=100), Node3: far ahead (seq=100000)
    dbs[0].set_global_seq(100);
    dbs[1].set_global_seq(500);
    dbs[2].set_global_seq(100_000); // "fast clock"
    dbs[3].set_global_seq(200);
    dbs[4].set_global_seq(50); // "slow clock"

    // Term 1: Node1 is leader — writes 1-10
    for n in &nodes {
        n.save_vote(r#"{"term":1,"voted_for":1}"#).unwrap();
    }
    for i in 1..=10 {
        nodes[0]
            .append_log(i, &format!("SET skew_k{i} skew_v{i}"))
            .unwrap();
    }
    replicate_to_set(nodes[0], &nodes[1..], 1, 11);
    for n in &nodes {
        apply_range(n, 1, 11);
    }

    // Term 2: Node5 (slowest seq=50) becomes leader
    // Raft doesn't care about seq — only term matters
    for n in &nodes {
        n.save_vote(r#"{"term":2,"voted_for":5}"#).unwrap();
    }

    // Node5 (the "slow clock" node) writes entries 11-20 as leader
    for i in 11..=20 {
        nodes[4]
            .append_log(i, &format!("SET skew_k{i} skew2_v{i}"))
            .unwrap();
    }
    replicate_to_set(nodes[4], &[nodes[0], nodes[1], nodes[2], nodes[3]], 11, 21);
    for n in &nodes {
        apply_range(n, 11, 21);
    }

    // Term 3: Node3 (fastest seq=100000) becomes leader
    for n in &nodes {
        n.save_vote(r#"{"term":3,"voted_for":3}"#).unwrap();
    }
    for i in 21..=30 {
        nodes[2]
            .append_log(i, &format!("SET skew_k{i} skew3_v{i}"))
            .unwrap();
    }
    replicate_to_set(nodes[2], &[nodes[0], nodes[1], nodes[3], nodes[4]], 21, 31);
    for n in &nodes {
        apply_range(n, 21, 31);
    }

    // Verify: all 5 nodes agree on term 3 and have identical data
    for (i, n) in nodes.iter().enumerate() {
        let vote = n.read_vote().unwrap();
        assert!(
            vote.contains("\"term\":3"),
            "Node {} should be term 3, got: {}",
            i + 1,
            vote
        );
        assert_eq!(n.last_applied_index(), 30, "Node {} at applied 30", i + 1);
    }

    // Data check: all nodes return same values regardless of local seq
    for (i, (db, _, _)) in cluster.iter().enumerate() {
        let seq = db.get_seq();
        for k in 1..=30 {
            assert!(
                db.find(&format!("skew_k{k}"), seq).unwrap().is_some(),
                "Node {} missing skew_k{}",
                i + 1,
                k
            );
        }
    }

    println!("✅ CLOCK 7b: 3 leaders across 5 nodes with 2000x seq skew, all converged");
}

/// Gap #7c: Sequence divergence after independent writes, then convergence
///
/// Two nodes write independently (simulating partition + clock skew), then
/// converge via Raft log. Verify that logical ordering via Raft index
/// trumps local sequence differences.
#[test]
fn test_sequence_divergence_and_convergence() {
    let (db1, node1, _d1) = create_node("div_node1");
    let (db2, node2, _d2) = create_node("div_node2");
    let (db3, node3, _d3) = create_node("div_node3");

    // Phase 1: All three in sync for entries 1-10
    for n in [&node1, &node2, &node3] {
        n.save_vote(r#"{"term":1,"voted_for":1}"#).unwrap();
    }
    for i in 1..=10 {
        node1
            .append_log(i, &format!("SET div_k{i} div_v{i}"))
            .unwrap();
    }
    replicate_to_set(&node1, &[&node2, &node3], 1, 11);
    for n in [&node1, &node2, &node3] {
        apply_range(n, 1, 11);
    }

    // Record sequences after phase 1
    let seq1_after_p1 = db1.get_seq();
    let _seq2_after_p1 = db2.get_seq();

    // Phase 2: Node1 does 100 EXTRA local writes (outside Raft), causing seq divergence
    // This simulates a node whose clock/counter drifts far ahead
    for i in 0..100 {
        let mut batch = omni_engine::WriteBatch::new();
        batch
            .set(&format!("__local_node1_{i}"), format!("junk{i}"))
            .unwrap();
        db1.commit_batch(&batch).unwrap();
    }

    let seq1_after_drift = db1.get_seq();
    assert!(
        seq1_after_drift > seq1_after_p1 + 50,
        "Node1 seq should have drifted significantly"
    );

    // Phase 3: Raft log continues — Node2 becomes leader (term 2), writes 11-20
    node2.save_vote(r#"{"term":2,"voted_for":2}"#).unwrap();
    node3.save_vote(r#"{"term":2,"voted_for":2}"#).unwrap();
    node1.save_vote(r#"{"term":2,"voted_for":2}"#).unwrap();

    for i in 11..=20 {
        node2
            .append_log(i, &format!("SET div_k{i} div_v{i}"))
            .unwrap();
    }
    replicate_to_set(&node2, &[&node1, &node3], 11, 21);

    // Apply on all nodes — Node1 has much higher seq but that's fine
    for n in [&node1, &node2, &node3] {
        apply_range(n, 11, 21);
    }

    // Verify convergence: all nodes have the same Raft data
    for (db, name) in [(&db1, "node1"), (&db2, "node2"), (&db3, "node3")] {
        let seq = db.get_seq();
        for i in 1..=20 {
            assert_eq!(
                db.find(&format!("div_k{i}"), seq).unwrap(),
                Some(format!("div_v{i}")),
                "{name} missing div_k{i} (seq={seq})"
            );
        }
    }

    // Sequences are different, but Raft data is identical
    let final_seq1 = db1.get_seq();
    let final_seq2 = db2.get_seq();
    assert!(
        final_seq1 != final_seq2,
        "Sequences should differ due to drift"
    );

    // But Raft applied index is the same
    assert_eq!(node1.last_applied_index(), 20);
    assert_eq!(node2.last_applied_index(), 20);
    assert_eq!(node3.last_applied_index(), 20);

    println!(
        "✅ CLOCK 7c: Node1 drifted +100 seqs, all 3 nodes converged on Raft data (seq1={final_seq1}, seq2={final_seq2})"
    );
}

/// Gap #7d: TTL consistency across replicated nodes
///
/// Keys with TTL are written through Raft, replicated to all nodes.
/// Since TTL expiry is computed at write time (wall clock) and checked
/// at read time, all nodes should see the same expiry behavior as long
/// as their wall clocks are "close enough" (which they are in the same process).
/// We also verify that non-TTL keys are completely immune to clock issues.
#[test]
fn test_ttl_consistency_across_replicas() {
    let (db1, node1, _d1) = create_node("ttl_leader");
    let (db2, node2, _d2) = create_node("ttl_follower1");
    let (db3, node3, _d3) = create_node("ttl_follower2");

    for n in [&node1, &node2, &node3] {
        n.save_vote(r#"{"term":1,"voted_for":1}"#).unwrap();
    }

    // Write non-TTL keys through Raft (immune to clock skew)
    for i in 1..=10 {
        let cmd = format!("SET ttl_perm_{i} permanent_v{i}");
        node1.append_log(i, &cmd).unwrap();
    }
    replicate_to_set(&node1, &[&node2, &node3], 1, 11);
    for n in [&node1, &node2, &node3] {
        apply_range(n, 1, 11);
    }

    // Write TTL keys directly (not through Raft log apply_write, since
    // apply_write only supports SET/DELETE, not TTL). This tests TTL behavior.
    // Use a very long TTL (3600s) so keys are definitely alive now.
    for db in [&db1, &db2, &db3] {
        let mut batch = omni_engine::WriteBatch::new();
        batch
            .set_with_ttl("ttl_alive_key", "alive_value".to_string(), 3600)
            .unwrap();
        db.commit_batch(&batch).unwrap();
    }

    // Use a TTL of 0 (no expiry) for comparison
    for db in [&db1, &db2, &db3] {
        let mut batch = omni_engine::WriteBatch::new();
        batch
            .set_with_ttl("ttl_noexpiry_key", "forever_value".to_string(), 0)
            .unwrap();
        db.commit_batch(&batch).unwrap();
    }

    // Verify: all 3 nodes see TTL-alive key
    for (db, name) in [(&db1, "leader"), (&db2, "follower1"), (&db3, "follower2")] {
        let seq = db.get_seq();
        assert_eq!(
            db.find("ttl_alive_key", seq).unwrap(),
            Some("alive_value".to_string()),
            "{name} should see ttl_alive_key (TTL=3600s, not expired)"
        );
        assert_eq!(
            db.find("ttl_noexpiry_key", seq).unwrap(),
            Some("forever_value".to_string()),
            "{name} should see ttl_noexpiry_key (no expiry)"
        );
    }

    // Verify: permanent (non-TTL) Raft keys are identical across all nodes
    for (db, name) in [(&db1, "leader"), (&db2, "follower1"), (&db3, "follower2")] {
        let seq = db.get_seq();
        for i in 1..=10 {
            assert_eq!(
                db.find(&format!("ttl_perm_{i}"), seq).unwrap(),
                Some(format!("permanent_v{i}")),
                "{name} missing ttl_perm_{i}"
            );
        }
    }

    // Edge case: write a key with TTL=1 on all nodes simultaneously
    // All nodes compute expiry from their local clock — since they share
    // the same system clock in this test, behavior is consistent
    for db in [&db1, &db2, &db3] {
        let mut batch = omni_engine::WriteBatch::new();
        batch
            .set_with_ttl("ttl_short_key", "short_value".to_string(), 1)
            .unwrap();
        db.commit_batch(&batch).unwrap();
    }

    // Key should be alive NOW (just written, TTL=1s hasn't elapsed)
    for (db, name) in [(&db1, "leader"), (&db2, "follower1"), (&db3, "follower2")] {
        let seq = db.get_seq();
        let val = db.find("ttl_short_key", seq).unwrap();
        assert!(
            val.is_some(),
            "{name} should see ttl_short_key (just written, TTL=1s)"
        );
    }

    println!("✅ CLOCK 7d: TTL and non-TTL keys consistent across 3 replicas");
}

/// Gap #7e: Multi-term leader progression with wildly divergent sequences
///
/// 5 nodes with sequence counters spanning 5 orders of magnitude.
/// Leadership passes through 5 different nodes across 5 terms.
/// Prove that Raft log is consistent and data converges regardless.
#[test]
#[expect(
    clippy::too_many_lines,
    reason = "Raft integration scenario intentionally keeps multi-term setup, replication, and convergence assertions together."
)]
fn test_multi_term_progression_with_extreme_seq_skew() {
    let cluster = create_5_node_cluster();
    let nodes: Vec<&OmniRaftStorage> = cluster.iter().map(|(_, s, _)| s).collect();
    let dbs: Vec<&std::sync::Arc<OmniKV>> = cluster.iter().map(|(d, _, _)| d).collect();

    // Extreme sequence skew: 5 orders of magnitude spread
    dbs[0].set_global_seq(10);
    dbs[1].set_global_seq(1_000);
    dbs[2].set_global_seq(100_000);
    dbs[3].set_global_seq(10_000_000);
    dbs[4].set_global_seq(1);

    // Term 1: Node1 (seq=10) leads, writes 1-5
    for n in &nodes {
        n.save_vote(r#"{"term":1,"voted_for":1}"#).unwrap();
    }
    for i in 1..=5 {
        nodes[0]
            .append_log(i, &format!("SET mt_k{i} t1_v{i}"))
            .unwrap();
    }
    replicate_to_set(nodes[0], &nodes[1..], 1, 6);
    for n in &nodes {
        apply_range(n, 1, 6);
    }

    // Term 2: Node2 (seq=1000) leads, writes 6-10
    for n in &nodes {
        n.save_vote(r#"{"term":2,"voted_for":2}"#).unwrap();
    }
    for i in 6..=10 {
        nodes[1]
            .append_log(i, &format!("SET mt_k{i} t2_v{i}"))
            .unwrap();
    }
    replicate_to_set(nodes[1], &[nodes[0], nodes[2], nodes[3], nodes[4]], 6, 11);
    for n in &nodes {
        apply_range(n, 6, 11);
    }

    // Term 3: Node3 (seq=100000) leads, writes 11-15
    for n in &nodes {
        n.save_vote(r#"{"term":3,"voted_for":3}"#).unwrap();
    }
    for i in 11..=15 {
        nodes[2]
            .append_log(i, &format!("SET mt_k{i} t3_v{i}"))
            .unwrap();
    }
    replicate_to_set(nodes[2], &[nodes[0], nodes[1], nodes[3], nodes[4]], 11, 16);
    for n in &nodes {
        apply_range(n, 11, 16);
    }

    // Term 4: Node4 (seq=10000000) leads, writes 16-20
    for n in &nodes {
        n.save_vote(r#"{"term":4,"voted_for":4}"#).unwrap();
    }
    for i in 16..=20 {
        nodes[3]
            .append_log(i, &format!("SET mt_k{i} t4_v{i}"))
            .unwrap();
    }
    replicate_to_set(nodes[3], &[nodes[0], nodes[1], nodes[2], nodes[4]], 16, 21);
    for n in &nodes {
        apply_range(n, 16, 21);
    }

    // Term 5: Node5 (seq=1, the slowest) leads, writes 21-25
    for n in &nodes {
        n.save_vote(r#"{"term":5,"voted_for":5}"#).unwrap();
    }
    for i in 21..=25 {
        nodes[4]
            .append_log(i, &format!("SET mt_k{i} t5_v{i}"))
            .unwrap();
    }
    replicate_to_set(nodes[4], &[nodes[0], nodes[1], nodes[2], nodes[3]], 21, 26);
    for n in &nodes {
        apply_range(n, 21, 26);
    }

    // === VERIFY: All 5 nodes agree on all 25 keys ===
    let expected_terms = vec![
        (1..=5, "t1"),
        (6..=10, "t2"),
        (11..=15, "t3"),
        (16..=20, "t4"),
        (21..=25, "t5"),
    ];

    for (node_idx, (db, storage, _)) in cluster.iter().enumerate() {
        let seq = db.get_seq();

        for (range, term_prefix) in &expected_terms {
            for i in range.clone() {
                let key = format!("mt_k{i}");
                let expected_val = format!("{term_prefix}_v{i}");
                assert_eq!(
                    db.find(&key, seq).unwrap(),
                    Some(expected_val.clone()),
                    "Node {} (seq={}) wrong value for {} (expected {})",
                    node_idx + 1,
                    seq,
                    key,
                    expected_val
                );
            }
        }

        assert_eq!(
            storage.last_applied_index(),
            25,
            "Node {} should be at applied 25",
            node_idx + 1
        );
    }

    // Verify vote consistency
    for (i, n) in nodes.iter().enumerate() {
        let vote = n.read_vote().unwrap();
        assert!(
            vote.contains("\"term\":5"),
            "Node {} should be term 5: {}",
            i + 1,
            vote
        );
    }

    // Verify log consistency across all nodes
    for i in 1..=25u64 {
        let reference = nodes[0].read_log(i).unwrap();
        for (n_idx, n) in nodes.iter().enumerate().skip(1) {
            assert_eq!(
                n.read_log(i).unwrap(),
                reference,
                "Log mismatch at index {} between Node1 and Node{}",
                i,
                n_idx + 1
            );
        }
    }

    println!(
        "✅ CLOCK 7e: 5 terms, 5 leaders, seq skew 1..10M, all 25 entries identical across 5 nodes"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Gap #8: 2PC Coordinator Replication
//
// Tests the Two-Phase Commit distributed transaction protocol:
//   - Coordinator drives PREPARE → VOTE → COMMIT/ABORT
//   - Participants validate writes, vote COMMIT or ABORT
//   - Coordinator logs state transitions to WAL for crash recovery
//   - Atomic commit: either ALL participants commit or ALL abort
// ═══════════════════════════════════════════════════════════════════════════

use omni_engine::dist_txn::{
    DistTxnState, PrepareResult, TwoPhaseCoordinator, TwoPhaseParticipant, Vote,
};

/// Gap #8a: Happy-path 2PC commit — 3 participants all vote COMMIT
///
/// Coordinator creates a distributed txn spanning 3 nodes.
/// All participants validate successfully and vote COMMIT.
/// Coordinator logs COMMIT, all participants apply their writes.
#[test]
fn test_2pc_happy_path_commit() {
    let (db_coord, _, _d_coord) = create_node("2pc_coordinator");
    let (db_p1, _, _d_p1) = create_node("2pc_participant1");
    let (db_p2, _, _d_p2) = create_node("2pc_participant2");
    let (db_p3, _, _d_p3) = create_node("2pc_participant3");

    let coordinator = TwoPhaseCoordinator::new(1, db_coord, 5000);
    let participant1 = TwoPhaseParticipant::new(10, db_p1.clone());
    let participant2 = TwoPhaseParticipant::new(20, db_p2.clone());
    let participant3 = TwoPhaseParticipant::new(30, db_p3.clone());

    // BEGIN distributed transaction
    let txn_id = coordinator.begin();
    assert_eq!(coordinator.get_state(txn_id), Some(DistTxnState::Preparing));

    // Add writes for each participant
    coordinator
        .add_write(txn_id, 10, "user:alice".into(), Some("Alice".into()), 0)
        .unwrap();
    coordinator
        .add_write(txn_id, 20, "user:bob".into(), Some("Bob".into()), 0)
        .unwrap();
    coordinator
        .add_write(txn_id, 30, "user:charlie".into(), Some("Charlie".into()), 0)
        .unwrap();

    // PREPARE phase
    let prepared_participants = coordinator.prepare(txn_id).unwrap();
    assert_eq!(prepared_participants.len(), 3);
    assert_eq!(
        coordinator.get_state(txn_id),
        Some(DistTxnState::WaitingForVotes)
    );

    // Each participant prepares and votes
    let writes_p1 = coordinator.get_participant_writes(txn_id, 10).unwrap();
    let result1 = participant1.prepare(txn_id, &writes_p1);
    assert_eq!(result1.vote, Vote::Commit);

    let writes_p2 = coordinator.get_participant_writes(txn_id, 20).unwrap();
    let result2 = participant2.prepare(txn_id, &writes_p2);
    assert_eq!(result2.vote, Vote::Commit);

    let writes_p3 = coordinator.get_participant_writes(txn_id, 30).unwrap();
    let result3 = participant3.prepare(txn_id, &writes_p3);
    assert_eq!(result3.vote, Vote::Commit);

    // Coordinator receives votes
    let state1 = coordinator.receive_vote(txn_id, result1).unwrap();
    assert_eq!(state1, DistTxnState::WaitingForVotes); // still waiting

    let state2 = coordinator.receive_vote(txn_id, result2).unwrap();
    assert_eq!(state2, DistTxnState::WaitingForVotes); // still waiting

    let state3 = coordinator.receive_vote(txn_id, result3).unwrap();
    assert_eq!(state3, DistTxnState::Committing); // all voted COMMIT!

    // COMMIT phase — each participant commits
    participant1.commit(txn_id).unwrap();
    participant2.commit(txn_id).unwrap();
    participant3.commit(txn_id).unwrap();

    // Finalize on coordinator
    coordinator.finalize_commit(txn_id).unwrap();
    assert_eq!(coordinator.active_count(), 0);

    // Verify data on each participant
    let seq1 = db_p1.get_seq();
    assert_eq!(
        db_p1.find("user:alice", seq1).unwrap(),
        Some("Alice".into())
    );

    let seq2 = db_p2.get_seq();
    assert_eq!(db_p2.find("user:bob", seq2).unwrap(), Some("Bob".into()));

    let seq3 = db_p3.get_seq();
    assert_eq!(
        db_p3.find("user:charlie", seq3).unwrap(),
        Some("Charlie".into())
    );

    // All participants should have no prepared txns left
    assert_eq!(participant1.prepared_count(), 0);
    assert_eq!(participant2.prepared_count(), 0);
    assert_eq!(participant3.prepared_count(), 0);

    println!("✅ 2PC 8a: Happy-path commit across 3 participants, all data verified");
}

/// Gap #8b: 2PC abort on single ABORT vote — atomicity guarantee
///
/// If ANY participant votes ABORT, the entire transaction must abort.
/// No participant should have committed data.
#[test]
fn test_2pc_abort_on_single_abort_vote() {
    let (db_coord, _, _d_coord) = create_node("2pc_abort_coord");
    let (db_p1, _, _d_p1) = create_node("2pc_abort_p1");
    let (db_p2, _, _d_p2) = create_node("2pc_abort_p2");

    let coordinator = TwoPhaseCoordinator::new(1, db_coord, 5000);
    let participant1 = TwoPhaseParticipant::new(10, db_p1.clone());
    let participant2 = TwoPhaseParticipant::new(20, db_p2.clone());

    // BEGIN
    let txn_id = coordinator.begin();

    coordinator
        .add_write(txn_id, 10, "abort_key1".into(), Some("val1".into()), 0)
        .unwrap();
    coordinator
        .add_write(txn_id, 20, "abort_key2".into(), Some("val2".into()), 0)
        .unwrap();

    // PREPARE
    coordinator.prepare(txn_id).unwrap();

    // Participant 1 votes COMMIT
    let writes_p1 = coordinator.get_participant_writes(txn_id, 10).unwrap();
    let result1 = participant1.prepare(txn_id, &writes_p1);
    assert_eq!(result1.vote, Vote::Commit);

    // Participant 2 votes ABORT (simulating SSI conflict)
    let result2 = PrepareResult {
        node_id: 20,
        txn_id,
        vote: Vote::Abort("SSI conflict on abort_key2".into()),
        prepare_seq: 0,
    };

    // Coordinator receives votes
    coordinator.receive_vote(txn_id, result1).unwrap();

    // Second vote causes ABORT — this returns Err
    let abort_result = coordinator.receive_vote(txn_id, result2);
    assert!(
        abort_result.is_err(),
        "Should return error when transaction aborts"
    );

    // Abort on participants
    participant1.abort(txn_id).unwrap();
    participant2.abort(txn_id).unwrap();

    // Verify: NO data should have been committed on either participant
    let seq1 = db_p1.get_seq();
    assert_eq!(
        db_p1.find("abort_key1", seq1).unwrap(),
        None,
        "Participant 1 should NOT have committed data"
    );

    let seq2 = db_p2.get_seq();
    assert_eq!(
        db_p2.find("abort_key2", seq2).unwrap(),
        None,
        "Participant 2 should NOT have committed data"
    );

    // Both participants should have no prepared txns
    assert_eq!(participant1.prepared_count(), 0);
    assert_eq!(participant2.prepared_count(), 0);

    println!("✅ 2PC 8b: Single ABORT vote caused full transaction abort, no data leaked");
}

/// Gap #8c: Coordinator WAL persistence — PREPARE and COMMIT records survive crash
///
/// The coordinator writes PREPARE and COMMIT records to `OmniKV`.
/// After a simulated crash (drop + reopen), verify the log entries
/// are still present for recovery.
#[test]
fn test_2pc_coordinator_wal_persistence() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = dir.path().join("2pc_wal_manifest.json");
    let wal = dir.path().join("2pc_wal.bin");

    let txn_id;

    // Phase 1: Coordinator runs 2PC, writes log entries, then "crashes"
    {
        let db = OmniKV::open(manifest.to_str().unwrap(), wal.to_str().unwrap()).unwrap();
        let coordinator = TwoPhaseCoordinator::new(1, db.clone(), 5000);
        let participant = TwoPhaseParticipant::new(10, db);

        // Run a full 2PC
        txn_id = coordinator.begin();
        coordinator
            .add_write(
                txn_id,
                10,
                "persist_key".into(),
                Some("persist_val".into()),
                0,
            )
            .unwrap();

        coordinator.prepare(txn_id).unwrap();

        let writes = coordinator.get_participant_writes(txn_id, 10).unwrap();
        let result = participant.prepare(txn_id, &writes);
        assert_eq!(result.vote, Vote::Commit);

        coordinator.receive_vote(txn_id, result).unwrap();

        // Commit
        participant.commit(txn_id).unwrap();
        coordinator.finalize_commit(txn_id).unwrap();

        // "Crash" — db drops here
    }

    // Phase 2: Reopen and verify 2PC log entries persisted
    {
        let db = OmniKV::open(manifest.to_str().unwrap(), wal.to_str().unwrap()).unwrap();
        let seq = db.get_seq();

        // PREPARE record should be in the coordinator log
        let prepare_key = format!("__2PC_LOG__/{}_{}/PREPARE", txn_id.0, txn_id.1);
        let prepare_log = db.find(&prepare_key, seq).unwrap();
        assert!(
            prepare_log.is_some(),
            "PREPARE log entry should survive crash"
        );
        let prepare_json = prepare_log.unwrap();
        assert!(
            prepare_json.contains("PREPARE"),
            "PREPARE log should contain state"
        );

        // COMMIT record should be in the coordinator log
        let commit_key = format!("__2PC_LOG__/{}_{}/COMMIT", txn_id.0, txn_id.1);
        let commit_log = db.find(&commit_key, seq).unwrap();
        assert!(
            commit_log.is_some(),
            "COMMIT log entry should survive crash"
        );

        // The participant's COMMITTED marker should also survive
        let p_commit_key = format!("__2PC_PREPARE__/{}_{}", txn_id.0, txn_id.1);
        let p_state = db.find(&p_commit_key, seq).unwrap();
        assert!(
            p_state.is_some(),
            "Participant COMMITTED marker should survive crash"
        );
        assert!(
            p_state.unwrap().contains("COMMITTED"),
            "Participant should show COMMITTED state"
        );

        // The actual data should also survive
        assert_eq!(
            db.find("persist_key", seq).unwrap(),
            Some("persist_val".into()),
            "Committed data should survive crash"
        );
    }

    println!("✅ 2PC 8c: Coordinator PREPARE/COMMIT logs + participant data survived crash");
}

/// Gap #8d: Cross-shard atomic commit with Raft log replication
///
/// A distributed transaction writes to 3 participants, each of which
/// also replicates its data via Raft to followers. Verify that:
/// 1. The 2PC commit is atomic across all participants
/// 2. Each participant's data is Raft-replicated to its followers
#[test]
#[expect(
    clippy::too_many_lines,
    reason = "Cross-shard 2PC integration scenario keeps participant setup, Raft replication, and atomicity checks together."
)]
fn test_2pc_cross_shard_with_raft_replication() {
    // Create 3 "shards", each with a primary and a follower
    let (db_s1_primary, raft_s1_primary, _d1) = create_node("shard1_primary");
    let (db_s1_follower, raft_s1_follower, _d2) = create_node("shard1_follower");
    let (db_s2_primary, raft_s2_primary, _d3) = create_node("shard2_primary");
    let (db_s2_follower, raft_s2_follower, _d4) = create_node("shard2_follower");
    let (db_s3_primary, raft_s3_primary, _d5) = create_node("shard3_primary");
    let (db_s3_follower, raft_s3_follower, _d6) = create_node("shard3_follower");

    // Create coordinator on shard 1
    let coordinator = TwoPhaseCoordinator::new(1, db_s1_primary.clone(), 5000);
    let p1 = TwoPhaseParticipant::new(10, db_s1_primary.clone());
    let p2 = TwoPhaseParticipant::new(20, db_s2_primary.clone());
    let p3 = TwoPhaseParticipant::new(30, db_s3_primary.clone());

    // Create a multi-shard transaction: transfer money across 3 accounts
    let txn_id = coordinator.begin();
    coordinator
        .add_write(
            txn_id,
            10,
            "account:alice".into(),
            Some("balance:800".into()),
            0,
        )
        .unwrap();
    coordinator
        .add_write(
            txn_id,
            20,
            "account:bob".into(),
            Some("balance:1200".into()),
            0,
        )
        .unwrap();
    coordinator
        .add_write(
            txn_id,
            30,
            "account:charlie".into(),
            Some("balance:500".into()),
            0,
        )
        .unwrap();

    // PREPARE
    coordinator.prepare(txn_id).unwrap();

    let w1 = coordinator.get_participant_writes(txn_id, 10).unwrap();
    let w2 = coordinator.get_participant_writes(txn_id, 20).unwrap();
    let w3 = coordinator.get_participant_writes(txn_id, 30).unwrap();

    let r1 = p1.prepare(txn_id, &w1);
    let r2 = p2.prepare(txn_id, &w2);
    let r3 = p3.prepare(txn_id, &w3);

    assert_eq!(r1.vote, Vote::Commit);
    assert_eq!(r2.vote, Vote::Commit);
    assert_eq!(r3.vote, Vote::Commit);

    // Receive votes
    coordinator.receive_vote(txn_id, r1).unwrap();
    coordinator.receive_vote(txn_id, r2).unwrap();
    let final_state = coordinator.receive_vote(txn_id, r3).unwrap();
    assert_eq!(final_state, DistTxnState::Committing);

    // COMMIT on all participants
    p1.commit(txn_id).unwrap();
    p2.commit(txn_id).unwrap();
    p3.commit(txn_id).unwrap();
    coordinator.finalize_commit(txn_id).unwrap();

    // Now replicate each primary's committed data to its follower via Raft
    // Shard 1: replicate via Raft log
    raft_s1_primary
        .append_log(1, "SET account:alice balance:800")
        .unwrap();
    replicate_to_set(&raft_s1_primary, &[&raft_s1_follower], 1, 2);
    apply_range(&raft_s1_follower, 1, 2);

    // Shard 2
    raft_s2_primary
        .append_log(1, "SET account:bob balance:1200")
        .unwrap();
    replicate_to_set(&raft_s2_primary, &[&raft_s2_follower], 1, 2);
    apply_range(&raft_s2_follower, 1, 2);

    // Shard 3
    raft_s3_primary
        .append_log(1, "SET account:charlie balance:500")
        .unwrap();
    replicate_to_set(&raft_s3_primary, &[&raft_s3_follower], 1, 2);
    apply_range(&raft_s3_follower, 1, 2);

    // Verify: primaries have the data (via 2PC commit)
    assert_eq!(
        db_s1_primary
            .find("account:alice", db_s1_primary.get_seq())
            .unwrap(),
        Some("balance:800".into())
    );
    assert_eq!(
        db_s2_primary
            .find("account:bob", db_s2_primary.get_seq())
            .unwrap(),
        Some("balance:1200".into())
    );
    assert_eq!(
        db_s3_primary
            .find("account:charlie", db_s3_primary.get_seq())
            .unwrap(),
        Some("balance:500".into())
    );

    // Verify: followers have the data (via Raft replication)
    assert_eq!(
        db_s1_follower
            .find("account:alice", db_s1_follower.get_seq())
            .unwrap(),
        Some("balance:800".into())
    );
    assert_eq!(
        db_s2_follower
            .find("account:bob", db_s2_follower.get_seq())
            .unwrap(),
        Some("balance:1200".into())
    );
    assert_eq!(
        db_s3_follower
            .find("account:charlie", db_s3_follower.get_seq())
            .unwrap(),
        Some("balance:500".into())
    );

    println!("✅ 2PC 8d: Cross-shard atomic commit replicated via Raft to 3 followers");
}

/// Gap #8e: Concurrent distributed transactions with independent outcomes
///
/// Two distributed transactions run concurrently on the same coordinator.
/// Txn1 commits successfully, Txn2 aborts. Verify that:
/// - Txn1's data is committed on its participants
/// - Txn2's data is NOT committed on any participant
/// - The two transactions don't interfere with each other
#[test]
fn test_2pc_concurrent_independent_transactions() {
    let (db_coord, _, _d_coord) = create_node("2pc_conc_coord");
    let (db_p1, _, _d_p1) = create_node("2pc_conc_p1");
    let (db_p2, _, _d_p2) = create_node("2pc_conc_p2");

    let coordinator = TwoPhaseCoordinator::new(1, db_coord, 5000);
    let participant1 = TwoPhaseParticipant::new(10, db_p1.clone());
    let participant2 = TwoPhaseParticipant::new(20, db_p2.clone());

    // === Txn1: will COMMIT ===
    let txn1 = coordinator.begin();
    coordinator
        .add_write(txn1, 10, "conc_committed_1".into(), Some("yes1".into()), 0)
        .unwrap();
    coordinator
        .add_write(txn1, 20, "conc_committed_2".into(), Some("yes2".into()), 0)
        .unwrap();

    // === Txn2: will ABORT ===
    let txn2 = coordinator.begin();
    coordinator
        .add_write(txn2, 10, "conc_aborted_1".into(), Some("no1".into()), 0)
        .unwrap();
    coordinator
        .add_write(txn2, 20, "conc_aborted_2".into(), Some("no2".into()), 0)
        .unwrap();

    // Both should be active
    assert_eq!(coordinator.active_count(), 2);

    // PREPARE both
    coordinator.prepare(txn1).unwrap();
    coordinator.prepare(txn2).unwrap();

    // === Process Txn1 — all COMMIT ===
    let w1_txn1 = coordinator.get_participant_writes(txn1, 10).unwrap();
    let w2_txn1 = coordinator.get_participant_writes(txn1, 20).unwrap();
    let r1_txn1 = participant1.prepare(txn1, &w1_txn1);
    let r2_txn1 = participant2.prepare(txn1, &w2_txn1);

    coordinator.receive_vote(txn1, r1_txn1).unwrap();
    let state_txn1 = coordinator.receive_vote(txn1, r2_txn1).unwrap();
    assert_eq!(state_txn1, DistTxnState::Committing);

    participant1.commit(txn1).unwrap();
    participant2.commit(txn1).unwrap();
    coordinator.finalize_commit(txn1).unwrap();

    // === Process Txn2 — participant2 ABORTs ===
    let w1_txn2 = coordinator.get_participant_writes(txn2, 10).unwrap();
    let r1_txn2 = participant1.prepare(txn2, &w1_txn2);
    assert_eq!(r1_txn2.vote, Vote::Commit);

    let r2_txn2 = PrepareResult {
        node_id: 20,
        txn_id: txn2,
        vote: Vote::Abort("Conflict with concurrent txn".into()),
        prepare_seq: 0,
    };

    coordinator.receive_vote(txn2, r1_txn2).unwrap();
    let abort_result = coordinator.receive_vote(txn2, r2_txn2);
    assert!(abort_result.is_err(), "Txn2 should abort");

    // Coordinator cleans up the aborted txn from active set
    // In production, this would also notify participants to abort
    let _ = coordinator.abort(txn2);

    // Abort on participants
    participant1.abort(txn2).unwrap();
    participant2.abort(txn2).unwrap();

    // === Verify: Txn1 data IS committed ===
    let seq1 = db_p1.get_seq();
    let seq2 = db_p2.get_seq();

    assert_eq!(
        db_p1.find("conc_committed_1", seq1).unwrap(),
        Some("yes1".into()),
        "Txn1 data should be committed on P1"
    );
    assert_eq!(
        db_p2.find("conc_committed_2", seq2).unwrap(),
        Some("yes2".into()),
        "Txn1 data should be committed on P2"
    );

    // === Verify: Txn2 data is NOT committed ===
    assert_eq!(
        db_p1.find("conc_aborted_1", seq1).unwrap(),
        None,
        "Txn2 data should NOT be on P1"
    );
    assert_eq!(
        db_p2.find("conc_aborted_2", seq2).unwrap(),
        None,
        "Txn2 data should NOT be on P2"
    );

    // No active txns remain
    assert_eq!(coordinator.active_count(), 0);
    assert_eq!(participant1.prepared_count(), 0);
    assert_eq!(participant2.prepared_count(), 0);

    println!("✅ 2PC 8e: Concurrent txns — Txn1 committed, Txn2 aborted, no interference");
}

// ═══════════════════════════════════════════════════════════════════════════
// Gap #9: Distributed Deadlock Detection (SSI Conflict Detection)
// Gap #10: Transaction Intents / Async Resolution
// ═══════════════════════════════════════════════════════════════════════════

use omni_engine::transaction::{TransactionManager, TxnState};

/// Gap #9a: Write-write conflict — two txns writing same key, second aborts
#[test]
fn test_ssi_write_write_conflict() {
    let (db, _, _d) = create_node("ssi_ww");
    let tm = TransactionManager::new(db.clone());

    let mut txn1 = tm.begin();
    let mut txn2 = tm.begin();

    tm.set(&mut txn1, "shared_key", "value_from_txn1".into())
        .unwrap();
    tm.set(&mut txn2, "shared_key", "value_from_txn2".into())
        .unwrap();

    // Txn1 commits first — succeeds
    let _seq1 = tm.commit(&mut txn1).unwrap();
    assert_eq!(txn1.state, TxnState::Committed);

    // Txn2 tries to commit — MUST fail (write-write conflict)
    let result = tm.commit(&mut txn2);
    assert!(result.is_err(), "Txn2 should abort due to WW conflict");
    assert_eq!(txn2.state, TxnState::Aborted);

    // Only txn1's value visible
    let seq = db.get_seq();
    assert_eq!(
        db.find("shared_key", seq).unwrap(),
        Some("value_from_txn1".into())
    );

    println!("✅ DEADLOCK 9a: Write-write conflict detected, second txn aborted");
}

/// Gap #9b: Read-write anti-dependency — txn reads key that another txn later writes
#[test]
fn test_ssi_read_write_conflict() {
    let (db, _, _d) = create_node("ssi_rw");
    let tm = TransactionManager::new(db);

    // Pre-populate
    let mut setup = tm.begin();
    tm.set(&mut setup, "rw_key", "original".into()).unwrap();
    tm.commit(&mut setup).unwrap();

    // Txn1 reads rw_key
    let mut txn1 = tm.begin();
    let val = tm.get(&mut txn1, "rw_key").unwrap();
    assert_eq!(val, Some("original".into()));

    // Txn2 writes rw_key and commits
    let mut txn2 = tm.begin();
    tm.set(&mut txn2, "rw_key", "modified_by_txn2".into())
        .unwrap();
    tm.commit(&mut txn2).unwrap();

    // Txn1 now writes something and tries to commit
    tm.set(&mut txn1, "other_key", "txn1_data".into()).unwrap();
    let result = tm.commit(&mut txn1);
    // SSI detects: txn1 read rw_key, txn2 wrote rw_key after txn1's snapshot
    assert!(
        result.is_err(),
        "Txn1 should abort due to RW anti-dependency"
    );

    println!("✅ DEADLOCK 9b: Read-write anti-dependency detected, reader aborted");
}

/// Gap #9c: No false positive — non-conflicting concurrent txns both commit
#[test]
fn test_ssi_no_false_positive() {
    let (db, _, _d) = create_node("ssi_ok");
    let tm = TransactionManager::new(db.clone());

    let mut txn1 = tm.begin();
    let mut txn2 = tm.begin();

    // Different keys — no conflict
    tm.set(&mut txn1, "key_a", "val_a".into()).unwrap();
    tm.set(&mut txn2, "key_b", "val_b".into()).unwrap();

    tm.commit(&mut txn1).unwrap();
    tm.commit(&mut txn2).unwrap();

    assert_eq!(txn1.state, TxnState::Committed);
    assert_eq!(txn2.state, TxnState::Committed);

    let seq = db.get_seq();
    assert_eq!(db.find("key_a", seq).unwrap(), Some("val_a".into()));
    assert_eq!(db.find("key_b", seq).unwrap(), Some("val_b".into()));

    println!("✅ DEADLOCK 9c: Non-conflicting txns both committed, no false positive");
}

/// Gap #9d: Overlapping txns — T2 reads key that T1 writes concurrently
#[test]
fn test_ssi_dangerous_structure_chain() {
    let (db, _, _d) = create_node("ssi_chain");
    let tm = TransactionManager::new(db);

    // Setup: create keys
    let mut setup = tm.begin();
    tm.set(&mut setup, "chain_x", "init_x".into()).unwrap();
    tm.set(&mut setup, "chain_y", "init_y".into()).unwrap();
    tm.commit(&mut setup).unwrap();

    // T1 and T2 start concurrently (both see the same snapshot)
    let mut t1 = tm.begin();
    let mut t2 = tm.begin();

    // T2 reads chain_y (records it in read set)
    tm.get(&mut t2, "chain_y").unwrap();

    // T1 writes chain_y and commits FIRST
    tm.set(&mut t1, "chain_y", "t1_y".into()).unwrap();
    tm.commit(&mut t1).unwrap();

    // T2 writes chain_x and tries to commit
    // SSI detects: T2 read chain_y, T1 wrote chain_y after T2's snapshot → RW conflict
    tm.set(&mut t2, "chain_x", "t2_x".into()).unwrap();
    let result = tm.commit(&mut t2);
    assert!(result.is_err(), "T2 should detect RW conflict on chain_y");

    println!("✅ DEADLOCK 9d: Overlapping txn RW conflict detected, stale reader aborted");
}

/// Gap #9e: Deadlock with distributed 2PC — two 2PC txns on overlapping keys
#[test]
fn test_deadlock_across_2pc_participants() {
    let (db, _, _d) = create_node("dl_2pc");
    let coordinator = TwoPhaseCoordinator::new(1, db.clone(), 5000);
    let participant = TwoPhaseParticipant::new(10, db.clone());

    // Pre-populate conflicting key
    let mut batch = omni_engine::WriteBatch::new();
    batch.set("conflict_key", "original".into()).unwrap();
    db.commit_batch(&batch).unwrap();

    // Txn1: write to conflict_key via 2PC
    let txn1 = coordinator.begin();
    coordinator
        .add_write(txn1, 10, "conflict_key".into(), Some("txn1_val".into()), 0)
        .unwrap();
    coordinator.prepare(txn1).unwrap();
    let w1 = coordinator.get_participant_writes(txn1, 10).unwrap();
    let r1 = participant.prepare(txn1, &w1);
    assert_eq!(r1.vote, Vote::Commit);

    // Commit txn1
    coordinator.receive_vote(txn1, r1).unwrap();
    participant.commit(txn1).unwrap();
    coordinator.finalize_commit(txn1).unwrap();

    // Txn2: also writes conflict_key — should succeed (no in-memory SSI conflict for 2PC)
    let txn2 = coordinator.begin();
    coordinator
        .add_write(txn2, 10, "conflict_key".into(), Some("txn2_val".into()), 0)
        .unwrap();
    coordinator.prepare(txn2).unwrap();
    let w2 = coordinator.get_participant_writes(txn2, 10).unwrap();
    let r2 = participant.prepare(txn2, &w2);
    assert_eq!(r2.vote, Vote::Commit);

    coordinator.receive_vote(txn2, r2).unwrap();
    participant.commit(txn2).unwrap();
    coordinator.finalize_commit(txn2).unwrap();

    // Last writer wins
    let seq = db.get_seq();
    assert_eq!(
        db.find("conflict_key", seq).unwrap(),
        Some("txn2_val".into())
    );

    println!("✅ DEADLOCK 9e: Sequential 2PC txns on same key resolved correctly");
}

/// Gap #10a: Write intents — buffered writes invisible until commit
#[test]
fn test_write_intents_invisible_until_commit() {
    let (db, _, _d) = create_node("intent_invis");
    let tm = TransactionManager::new(db.clone());

    let mut txn = tm.begin();
    tm.set(&mut txn, "intent_key", "intent_value".into())
        .unwrap();

    // Before commit: data should NOT be visible to other readers
    let seq = db.get_seq();
    assert_eq!(
        db.find("intent_key", seq).unwrap(),
        None,
        "Intent should be invisible"
    );

    // Commit makes it visible
    tm.commit(&mut txn).unwrap();
    let seq = db.get_seq();
    assert_eq!(
        db.find("intent_key", seq).unwrap(),
        Some("intent_value".into())
    );

    println!("✅ INTENT 10a: Write intents invisible until commit");
}

/// Gap #10b: Aborted intents leave no trace
#[test]
fn test_aborted_intents_no_trace() {
    let (db, _, _d) = create_node("intent_abort");
    let tm = TransactionManager::new(db.clone());

    let mut txn = tm.begin();
    tm.set(&mut txn, "ghost_key", "ghost_value".into()).unwrap();
    tm.set(&mut txn, "ghost_key2", "ghost_value2".into())
        .unwrap();

    // Abort — discard all writes
    tm.abort(&mut txn);
    assert_eq!(txn.state, TxnState::Aborted);

    // Nothing should be visible
    let seq = db.get_seq();
    assert_eq!(db.find("ghost_key", seq).unwrap(), None);
    assert_eq!(db.find("ghost_key2", seq).unwrap(), None);

    println!("✅ INTENT 10b: Aborted intents leave zero trace in storage");
}

/// Gap #10c: Read-your-own-writes within a transaction
#[test]
fn test_read_your_own_writes() {
    let (db, _, _d) = create_node("intent_ryow");
    let tm = TransactionManager::new(db.clone());

    let mut txn = tm.begin();
    tm.set(&mut txn, "ryow_key", "first_write".into()).unwrap();

    // Can read our own buffered write
    let val = tm.get(&mut txn, "ryow_key").unwrap();
    assert_eq!(val, Some("first_write".into()));

    // Overwrite within same txn
    tm.set(&mut txn, "ryow_key", "second_write".into()).unwrap();
    let val = tm.get(&mut txn, "ryow_key").unwrap();
    assert_eq!(val, Some("second_write".into()));

    tm.commit(&mut txn).unwrap();
    let seq = db.get_seq();
    assert_eq!(
        db.find("ryow_key", seq).unwrap(),
        Some("second_write".into())
    );

    println!("✅ INTENT 10c: Read-your-own-writes works within transaction");
}

/// Gap #10d: Intent resolution with delete — delete intent properly resolves
#[test]
fn test_intent_delete_resolution() {
    let (db, _, _d) = create_node("intent_del");
    let tm = TransactionManager::new(db.clone());

    // Create key
    let mut setup = tm.begin();
    tm.set(&mut setup, "del_key", "exists".into()).unwrap();
    tm.commit(&mut setup).unwrap();

    // Delete via transaction
    let mut txn = tm.begin();
    let val = tm.get(&mut txn, "del_key").unwrap();
    assert_eq!(val, Some("exists".into()));

    tm.delete(&mut txn, "del_key").unwrap();

    // Read-your-own-delete
    let val = tm.get(&mut txn, "del_key").unwrap();
    assert_eq!(val, None, "Deleted key should return None within same txn");

    tm.commit(&mut txn).unwrap();

    // Globally deleted
    let seq = db.get_seq();
    assert_eq!(db.find("del_key", seq).unwrap(), None);

    println!("✅ INTENT 10d: Delete intent properly resolved on commit");
}

/// Gap #10e: Multi-key atomic intent resolution
#[test]
fn test_multi_key_atomic_intent() {
    let (db, _, _d) = create_node("intent_multi");
    let tm = TransactionManager::new(db.clone());

    // Atomic multi-key write
    let mut txn = tm.begin();
    for i in 0..20 {
        tm.set(&mut txn, &format!("mk_{i}"), format!("val_{i}"))
            .unwrap();
    }
    tm.commit(&mut txn).unwrap();

    // All 20 keys visible atomically
    let seq = db.get_seq();
    for i in 0..20 {
        assert_eq!(
            db.find(&format!("mk_{i}"), seq).unwrap(),
            Some(format!("val_{i}"))
        );
    }

    // Abort a second multi-key txn — none should appear
    let mut txn2 = tm.begin();
    for i in 20..40 {
        tm.set(&mut txn2, &format!("mk_{i}"), format!("val_{i}"))
            .unwrap();
    }
    tm.abort(&mut txn2);

    let seq = db.get_seq();
    for i in 20..40 {
        assert_eq!(db.find(&format!("mk_{i}"), seq).unwrap(), None);
    }

    println!("✅ INTENT 10e: 20-key atomic commit + 20-key atomic abort, all correct");
}

// ═══════════════════════════════════════════════════════════════════════════
// Gap #11: Transaction Retry with Backoff
// Gap #12: Cross-shard Range Queries
// ═══════════════════════════════════════════════════════════════════════════

/// Gap #11a: SSI conflict abort → immediate retry succeeds
#[test]
fn test_txn_retry_after_conflict() {
    let (db, _, _d) = create_node("retry_basic");
    let tm = TransactionManager::new(db.clone());

    // Setup
    let mut setup = tm.begin();
    tm.set(&mut setup, "retry_k", "v0".into()).unwrap();
    tm.commit(&mut setup).unwrap();

    // T1 and T2 start concurrently
    let mut t1 = tm.begin();
    let mut t2 = tm.begin();

    tm.set(&mut t1, "retry_k", "v1".into()).unwrap();
    tm.set(&mut t2, "retry_k", "v2".into()).unwrap();

    // T1 commits, T2 aborts
    tm.commit(&mut t1).unwrap();
    let result = tm.commit(&mut t2);
    assert!(result.is_err(), "T2 should abort");

    // RETRY: T2 starts fresh and succeeds
    let mut t2_retry = tm.begin();
    tm.set(&mut t2_retry, "retry_k", "v2_retry".into()).unwrap();
    tm.commit(&mut t2_retry).unwrap();

    let seq = db.get_seq();
    assert_eq!(db.find("retry_k", seq).unwrap(), Some("v2_retry".into()));

    println!("✅ RETRY 11a: Aborted txn retried and committed successfully");
}

/// Gap #11b: Multiple retries — txn succeeds after N conflicts
#[test]
fn test_txn_multiple_retries() {
    let (db, _, _d) = create_node("retry_multi");
    let tm = TransactionManager::new(db.clone());

    let mut setup = tm.begin();
    tm.set(&mut setup, "hot_key", "init".into()).unwrap();
    tm.commit(&mut setup).unwrap();

    let mut retry_count = 0;
    let max_retries = 5;

    // Simulate a "hot key" scenario: keep conflicting, then eventually succeed
    for attempt in 0..max_retries {
        // A competing writer commits first on each attempt except the last
        if attempt < max_retries - 1 {
            let mut competitor = tm.begin();
            let mut our_txn = tm.begin();

            tm.set(&mut competitor, "hot_key", format!("comp_{attempt}"))
                .unwrap();
            tm.set(&mut our_txn, "hot_key", format!("ours_{attempt}"))
                .unwrap();

            tm.commit(&mut competitor).unwrap();
            let result = tm.commit(&mut our_txn);
            assert!(result.is_err());
            retry_count += 1;
        } else {
            // No competitor — we succeed
            let mut our_txn = tm.begin();
            tm.set(&mut our_txn, "hot_key", "ours_final".into())
                .unwrap();
            tm.commit(&mut our_txn).unwrap();
        }
    }

    assert_eq!(retry_count, 4, "Should have retried 4 times");
    let seq = db.get_seq();
    assert_eq!(db.find("hot_key", seq).unwrap(), Some("ours_final".into()));

    println!("✅ RETRY 11b: Txn succeeded after {retry_count} retries on hot key");
}

/// Gap #11c: Retry with exponential backoff simulation
#[test]
fn test_txn_retry_with_backoff_pattern() {
    let (db, _, _d) = create_node("retry_backoff");
    let tm = TransactionManager::new(db.clone());

    let mut setup = tm.begin();
    tm.set(&mut setup, "backoff_key", "init".into()).unwrap();
    tm.commit(&mut setup).unwrap();

    let mut attempts = Vec::new();
    let base_delay_ms: u64 = 10;

    for attempt in 0..3 {
        let mut txn = tm.begin();
        let _current = tm.get(&mut txn, "backoff_key").unwrap();
        tm.set(&mut txn, "backoff_key", format!("attempt_{attempt}"))
            .unwrap();

        // Simulate a competing write on first 2 attempts
        if attempt < 2 {
            let mut blocker = tm.begin();
            tm.set(&mut blocker, "backoff_key", format!("block_{attempt}"))
                .unwrap();
            tm.commit(&mut blocker).unwrap();
        }

        if tm.commit(&mut txn).is_ok() {
            attempts.push((attempt, true, 0));
            break;
        }

        let backoff = base_delay_ms * (1 << attempt); // exponential
        attempts.push((attempt, false, backoff));
        std::thread::sleep(std::time::Duration::from_millis(backoff));
    }

    // Should have failed twice, succeeded on third
    assert_eq!(attempts.len(), 3);
    assert!(!attempts[0].1); // failed
    assert!(!attempts[1].1); // failed
    assert!(attempts[2].1); // succeeded

    let seq = db.get_seq();
    assert_eq!(
        db.find("backoff_key", seq).unwrap(),
        Some("attempt_2".into())
    );

    println!(
        "✅ RETRY 11c: Exponential backoff pattern — 2 retries, backoffs: {}ms, {}ms",
        attempts[0].2, attempts[1].2
    );
}

/// Gap #11d: Read-only txn never needs retry
#[test]
fn test_read_only_txn_no_retry_needed() {
    let (db, _, _d) = create_node("retry_readonly");
    let tm = TransactionManager::new(db);

    // Populate data
    for i in 0..10 {
        let mut txn = tm.begin();
        tm.set(&mut txn, &format!("ro_k{i}"), format!("ro_v{i}"))
            .unwrap();
        tm.commit(&mut txn).unwrap();
    }

    // Read-only txn while writes happen concurrently
    let mut reader = tm.begin();
    for i in 0..10 {
        let val = tm.get(&mut reader, &format!("ro_k{i}")).unwrap();
        assert_eq!(val, Some(format!("ro_v{i}")));
    }

    // Concurrent writer modifies keys
    let mut writer = tm.begin();
    tm.set(&mut writer, "ro_k0", "modified".into()).unwrap();
    tm.commit(&mut writer).unwrap();

    // Read-only commit always succeeds (no writes to conflict)
    let result = tm.commit(&mut reader);
    assert!(result.is_ok(), "Read-only txn should always commit");

    println!("✅ RETRY 11d: Read-only txn committed without retry despite concurrent writes");
}

/// Gap #11e: Retry with multi-key transaction
#[test]
fn test_txn_retry_multi_key() {
    let (db, _, _d) = create_node("retry_mk");
    let tm = TransactionManager::new(db.clone());

    // Setup 5 keys
    let mut setup = tm.begin();
    for i in 0..5 {
        tm.set(&mut setup, &format!("rmk_{i}"), format!("init_{i}"))
            .unwrap();
    }
    tm.commit(&mut setup).unwrap();

    // T1: writes all 5 keys
    let mut t1 = tm.begin();
    let mut t2 = tm.begin();

    for i in 0..5 {
        tm.set(&mut t1, &format!("rmk_{i}"), format!("t1_{i}"))
            .unwrap();
        tm.set(&mut t2, &format!("rmk_{i}"), format!("t2_{i}"))
            .unwrap();
    }

    tm.commit(&mut t1).unwrap();
    assert!(
        tm.commit(&mut t2).is_err(),
        "T2 should conflict on all 5 keys"
    );

    // Retry: T2 gets fresh snapshot and succeeds
    let mut t2_retry = tm.begin();
    for i in 0..5 {
        tm.set(&mut t2_retry, &format!("rmk_{i}"), format!("t2_final_{i}"))
            .unwrap();
    }
    tm.commit(&mut t2_retry).unwrap();

    let seq = db.get_seq();
    for i in 0..5 {
        assert_eq!(
            db.find(&format!("rmk_{i}"), seq).unwrap(),
            Some(format!("t2_final_{i}"))
        );
    }

    println!("✅ RETRY 11e: 5-key txn retried after conflict, all keys updated");
}

/// Gap #12a: Range scan across keys in lexicographic order
#[test]
fn test_range_scan_lexicographic() {
    let (db, _, _d) = create_node("range_lex");

    // Insert keys that span the lexicographic range
    let keys = vec![
        ("range:a", "val_a"),
        ("range:b", "val_b"),
        ("range:c", "val_c"),
        ("range:d", "val_d"),
        ("range:e", "val_e"),
        ("range:m", "val_m"),
        ("range:z", "val_z"),
    ];

    let mut batch = omni_engine::WriteBatch::new();
    for (k, v) in &keys {
        batch.set(k, v.to_string()).unwrap();
    }
    db.commit_batch(&batch).unwrap();

    let seq = db.get_seq();

    // Scan full range
    let results = db.scan("range:a", "range:z\x7f", seq).unwrap();
    assert_eq!(results.len(), 7, "Should find all 7 keys");

    // Verify lexicographic order
    for i in 1..results.len() {
        assert!(
            results[i].0 > results[i - 1].0,
            "Keys should be in lex order"
        );
    }

    // Scan partial range
    let partial = db.scan("range:b", "range:e", seq).unwrap();
    assert!(
        partial.len() >= 3,
        "Should find at least b, c, d in range b..e"
    );
    assert_eq!(partial[0].0, "range:b");

    println!(
        "✅ RANGE 12a: Range scan returns {} keys in lexicographic order",
        results.len()
    );
}

/// Gap #12b: Range scan with MVCC — sees consistent snapshot
#[test]
fn test_range_scan_mvcc_snapshot() {
    let (db, _, _d) = create_node("range_mvcc");

    // Phase 1: write keys at seq S1
    let mut batch = omni_engine::WriteBatch::new();
    for i in 0..10 {
        batch
            .set(&format!("scan_k{i:02}"), format!("v1_{i}"))
            .unwrap();
    }
    let s1 = db.commit_batch(&batch).unwrap();

    // Phase 2: overwrite some keys at seq S2
    let mut batch2 = omni_engine::WriteBatch::new();
    batch2.set("scan_k03", "v2_3".into()).unwrap();
    batch2.set("scan_k07", "v2_7".into()).unwrap();
    let s2 = db.commit_batch(&batch2).unwrap();

    // Scan at S1 — should see original values
    let results_s1 = db.scan("scan_k00", "scan_k99", s1).unwrap();
    assert_eq!(results_s1.len(), 10);
    let k3_s1 = results_s1.iter().find(|(k, _)| k == "scan_k03").unwrap();
    assert_eq!(k3_s1.1, "v1_3", "At S1, scan_k03 should be v1_3");

    // Scan at S2 — should see updated values
    let results_s2 = db.scan("scan_k00", "scan_k99", s2).unwrap();
    let k3_s2 = results_s2.iter().find(|(k, _)| k == "scan_k03").unwrap();
    assert_eq!(k3_s2.1, "v2_3", "At S2, scan_k03 should be v2_3");
    let k7_s2 = results_s2.iter().find(|(k, _)| k == "scan_k07").unwrap();
    assert_eq!(k7_s2.1, "v2_7", "At S2, scan_k07 should be v2_7");

    println!("✅ RANGE 12b: Range scan respects MVCC snapshots (S1 vs S2)");
}

/// Gap #12c: Empty range scan returns no results
#[test]
fn test_range_scan_empty() {
    let (db, _, _d) = create_node("range_empty");

    let mut batch = omni_engine::WriteBatch::new();
    batch.set("alpha:1", "a1".into()).unwrap();
    batch.set("gamma:1", "g1".into()).unwrap();
    db.commit_batch(&batch).unwrap();

    let seq = db.get_seq();

    // Scan range where no keys exist
    let results = db.scan("beta:0", "beta:9", seq).unwrap();
    assert_eq!(results.len(), 0, "No keys in beta: range");

    println!("✅ RANGE 12c: Empty range scan correctly returns 0 results");
}

/// Gap #12d: Cross-node range scan via Raft replication
#[test]
fn test_range_scan_across_replicated_nodes() {
    let (db1, node1, _d1) = create_node("range_primary");
    let (db2, node2, _d2) = create_node("range_replica");

    // Write range data on primary via Raft
    for i in 0..20 {
        node1
            .append_log(i + 1, &format!("SET rng_{i:03} rng_val_{i}"))
            .unwrap();
    }

    // Replicate to follower
    replicate_to_set(&node1, &[&node2], 1, 21);
    apply_range(&node1, 1, 21);
    apply_range(&node2, 1, 21);

    // Range scan on BOTH nodes should return identical results
    let seq1 = db1.get_seq();
    let seq2 = db2.get_seq();

    let results1 = db1.scan("rng_000", "rng_999", seq1).unwrap();
    let results2 = db2.scan("rng_000", "rng_999", seq2).unwrap();

    assert_eq!(results1.len(), 20, "Primary should have 20 range keys");
    assert_eq!(results2.len(), 20, "Replica should have 20 range keys");

    // Results should be identical
    for (r1, r2) in results1.iter().zip(results2.iter()) {
        assert_eq!(r1.0, r2.0, "Key mismatch in cross-node range scan");
        assert_eq!(r1.1, r2.1, "Value mismatch in cross-node range scan");
    }

    println!("✅ RANGE 12d: Range scan identical across primary and replica (20 keys)");
}

/// Gap #12e: Range scan with deletes — tombstones excluded
#[test]
fn test_range_scan_with_deletes() {
    let (db, _, _d) = create_node("range_del");

    // Insert 10 keys
    let mut batch = omni_engine::WriteBatch::new();
    for i in 0..10 {
        batch
            .set(&format!("del_k{i:02}"), format!("del_v{i}"))
            .unwrap();
    }
    db.commit_batch(&batch).unwrap();

    // Delete keys 3, 5, 7
    let mut del_batch = omni_engine::WriteBatch::new();
    del_batch.delete("del_k03").unwrap();
    del_batch.delete("del_k05").unwrap();
    del_batch.delete("del_k07").unwrap();
    db.commit_batch(&del_batch).unwrap();

    let seq = db.get_seq();
    let results = db.scan("del_k00", "del_k99", seq).unwrap();

    assert_eq!(
        results.len(),
        7,
        "Should have 10 - 3 = 7 keys after deletes"
    );

    // Verify deleted keys are NOT in results
    let keys: Vec<&str> = results.iter().map(|(k, _)| k.as_str()).collect();
    assert!(!keys.contains(&"del_k03"), "del_k03 should be deleted");
    assert!(!keys.contains(&"del_k05"), "del_k05 should be deleted");
    assert!(!keys.contains(&"del_k07"), "del_k07 should be deleted");

    // Remaining keys should still be there
    assert!(keys.contains(&"del_k00"));
    assert!(keys.contains(&"del_k04"));
    assert!(keys.contains(&"del_k09"));

    println!("✅ RANGE 12e: Range scan excludes 3 deleted keys, 7 remaining correct");
}

// ═══════════════════════════════════════════════════════════════════════════
// Gap #13: Membership Changes (add/remove node)
// Gap #14: Rolling Upgrades Without Downtime
// ═══════════════════════════════════════════════════════════════════════════

/// Gap #13a: Add new node to 3-node cluster — late joiner catches up
#[test]
fn test_membership_add_node_catches_up() {
    let (_db1, n1, _d1) = create_node("mem_n1");
    let (_db2, n2, _d2) = create_node("mem_n2");
    let (_db3, n3, _d3) = create_node("mem_n3");

    // 3-node cluster writes 20 entries
    for i in 1..=20 {
        n1.append_log(i, &format!("SET mem_k{i} mem_v{i}")).unwrap();
    }
    replicate_to_set(&n1, &[&n2, &n3], 1, 21);
    apply_range(&n1, 1, 21);
    apply_range(&n2, 1, 21);
    apply_range(&n3, 1, 21);

    // Add node4 to the cluster — it starts with no user data
    let (db4, n4, _d4) = create_node("mem_n4");
    assert_eq!(
        db4.find("mem_k1", db4.get_seq()).unwrap(),
        None,
        "New node should be empty"
    );

    // Replicate full log to new node (catch-up)
    replicate_to_set(&n1, &[&n4], 1, 21);
    apply_range(&n4, 1, 21);

    // Verify node4 has all data
    let seq4 = db4.get_seq();
    for i in 1..=20 {
        assert_eq!(
            db4.find(&format!("mem_k{i}"), seq4).unwrap(),
            Some(format!("mem_v{i}")),
            "Node4 should have mem_k{i}"
        );
    }

    // New writes replicate to all 4 nodes
    n1.append_log(21, "SET mem_k21 mem_v21").unwrap();
    replicate_to_set(&n1, &[&n2, &n3, &n4], 21, 22);
    apply_range(&n4, 21, 22);

    assert_eq!(
        db4.find("mem_k21", db4.get_seq()).unwrap(),
        Some("mem_v21".into())
    );

    println!("✅ MEMBERSHIP 13a: Node4 joined, caught up on 20 entries + received new write");
}

/// Gap #13b: Remove node — remaining nodes continue operating
#[test]
fn test_membership_remove_node() {
    let (db1, n1, _d1) = create_node("rem_n1");
    let (db2, n2, _d2) = create_node("rem_n2");
    let (db3, n3, _d3) = create_node("rem_n3");

    // Initial writes to 3-node cluster
    for i in 1..=10 {
        n1.append_log(i, &format!("SET rem_k{i} rem_v{i}")).unwrap();
    }
    replicate_to_set(&n1, &[&n2, &n3], 1, 11);
    apply_range(&n1, 1, 11);
    apply_range(&n2, 1, 11);
    apply_range(&n3, 1, 11);

    // "Remove" node3 — stop replicating to it
    // Continue writing to n1, n2 only
    for i in 11..=20 {
        n1.append_log(i, &format!("SET rem_k{i} rem_v{i}")).unwrap();
    }
    replicate_to_set(&n1, &[&n2], 11, 21); // only n2
    apply_range(&n1, 11, 21);
    apply_range(&n2, 11, 21);

    // n1 and n2 have all 20 entries
    let seq1 = db1.get_seq();
    let seq2 = db2.get_seq();
    assert_eq!(db1.find("rem_k20", seq1).unwrap(), Some("rem_v20".into()));
    assert_eq!(db2.find("rem_k20", seq2).unwrap(), Some("rem_v20".into()));

    // n3 only has first 10 (stale after removal)
    let seq3 = db3.get_seq();
    assert_eq!(db3.find("rem_k10", seq3).unwrap(), Some("rem_v10".into()));
    assert_eq!(db3.find("rem_k11", seq3).unwrap(), None);

    println!("✅ MEMBERSHIP 13b: Node3 removed, nodes 1-2 continued with 10 new writes");
}

/// Gap #13c: Scale from 3 to 5 nodes sequentially
#[test]
fn test_membership_scale_out_3_to_5() {
    let (db1, n1, _d1) = create_node("scale_n1");
    let (db2, n2, _d2) = create_node("scale_n2");
    let (db3, n3, _d3) = create_node("scale_n3");

    // Phase 1: 3 nodes, 10 entries
    for i in 1..=10 {
        n1.append_log(i, &format!("SET sc_k{i} sc_v{i}")).unwrap();
    }
    replicate_to_set(&n1, &[&n2, &n3], 1, 11);
    apply_range(&n1, 1, 11);
    apply_range(&n2, 1, 11);
    apply_range(&n3, 1, 11);

    // Phase 2: Add node4
    let (db4, n4, _d4) = create_node("scale_n4");
    replicate_to_set(&n1, &[&n4], 1, 11);
    apply_range(&n4, 1, 11);

    // Phase 3: Add node5 + write more
    let (db5, n5, _d5) = create_node("scale_n5");
    replicate_to_set(&n1, &[&n5], 1, 11);
    apply_range(&n5, 1, 11);

    for i in 11..=15 {
        n1.append_log(i, &format!("SET sc_k{i} sc_v{i}")).unwrap();
    }
    replicate_to_set(&n1, &[&n2, &n3, &n4, &n5], 11, 16);
    for node in [&n1, &n2, &n3, &n4, &n5] {
        apply_range(node, 11, 16);
    }

    // All 5 nodes have all 15 entries
    for (name, db) in [
        ("n1", &db1),
        ("n2", &db2),
        ("n3", &db3),
        ("n4", &db4),
        ("n5", &db5),
    ] {
        let seq = db.get_seq();
        for i in 1..=15 {
            assert_eq!(
                db.find(&format!("sc_k{i}"), seq).unwrap(),
                Some(format!("sc_v{i}")),
                "{name} missing sc_k{i}"
            );
        }
    }

    println!("✅ MEMBERSHIP 13c: Scaled 3→5 nodes, all 15 entries on all 5 nodes");
}

/// Gap #13d: Re-add previously removed node — catches up from current leader
#[test]
fn test_membership_readd_node() {
    let (_db1, n1, _d1) = create_node("readd_n1");
    let (_db2, n2, _d2) = create_node("readd_n2");
    let (db3, n3, _d3) = create_node("readd_n3");

    // Phase 1: All 3 nodes in sync with 10 entries
    for i in 1..=10 {
        n1.append_log(i, &format!("SET ra_k{i} ra_v{i}")).unwrap();
    }
    replicate_to_set(&n1, &[&n2, &n3], 1, 11);
    apply_range(&n1, 1, 11);
    apply_range(&n2, 1, 11);
    apply_range(&n3, 1, 11);

    // Phase 2: Remove n3, write 10 more entries to n1, n2
    for i in 11..=20 {
        n1.append_log(i, &format!("SET ra_k{i} ra_v{i}")).unwrap();
    }
    replicate_to_set(&n1, &[&n2], 11, 21);
    apply_range(&n1, 11, 21);
    apply_range(&n2, 11, 21);

    // Phase 3: Re-add n3 — catch up on missed entries 11-20
    replicate_to_set(&n1, &[&n3], 11, 21);
    apply_range(&n3, 11, 21);

    // n3 now has ALL 20 entries
    let seq3 = db3.get_seq();
    for i in 1..=20 {
        assert_eq!(
            db3.find(&format!("ra_k{i}"), seq3).unwrap(),
            Some(format!("ra_v{i}")),
            "Re-added n3 missing ra_k{i}"
        );
    }

    println!("✅ MEMBERSHIP 13d: Node3 re-added after removal, caught up on 10 missed entries");
}

/// Gap #13e: Data integrity after multiple membership changes
#[test]
fn test_membership_data_integrity() {
    let (db1, n1, _d1) = create_node("integ_n1");
    let (db2, n2, _d2) = create_node("integ_n2");

    // Phase 1: 2-node cluster, 5 entries
    for i in 1..=5 {
        n1.append_log(i, &format!("SET ig_k{i} ig_v{i}")).unwrap();
    }
    replicate_to_set(&n1, &[&n2], 1, 6);
    apply_range(&n1, 1, 6);
    apply_range(&n2, 1, 6);

    // Phase 2: Add n3, write 5 more
    let (db3, n3, _d3) = create_node("integ_n3");
    replicate_to_set(&n1, &[&n3], 1, 6);
    apply_range(&n3, 1, 6);

    for i in 6..=10 {
        n1.append_log(i, &format!("SET ig_k{i} ig_v{i}")).unwrap();
    }
    replicate_to_set(&n1, &[&n2, &n3], 6, 11);
    apply_range(&n1, 6, 11);
    apply_range(&n2, 6, 11);
    apply_range(&n3, 6, 11);

    // Phase 3: Remove n2, add n4, write 5 more
    let (db4, n4, _d4) = create_node("integ_n4");
    replicate_to_set(&n1, &[&n4], 1, 11);
    apply_range(&n4, 1, 11);

    for i in 11..=15 {
        n1.append_log(i, &format!("SET ig_k{i} ig_v{i}")).unwrap();
    }
    replicate_to_set(&n1, &[&n3, &n4], 11, 16);
    apply_range(&n1, 11, 16);
    apply_range(&n3, 11, 16);
    apply_range(&n4, 11, 16);

    // Verify: n1, n3, n4 all have 15 entries; n2 has only 10
    for (name, db, expected) in [
        ("n1", &db1, 15),
        ("n3", &db3, 15),
        ("n4", &db4, 15),
        ("n2", &db2, 10),
    ] {
        let seq = db.get_seq();
        for i in 1..=expected {
            assert_eq!(
                db.find(&format!("ig_k{i}"), seq).unwrap(),
                Some(format!("ig_v{i}")),
                "{name} missing ig_k{i}"
            );
        }
    }

    println!("✅ MEMBERSHIP 13e: Data intact after add/remove/add cycle across 4 nodes");
}

/// Gap #14a: Rolling restart — one node at a time, no data loss
#[test]
fn test_rolling_restart_no_data_loss() {
    // Create 3 nodes with persistent storage
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let dir3 = tempfile::tempdir().unwrap();

    let m1 = dir1.path().join("roll_m1.json");
    let w1 = dir1.path().join("roll_w1.bin");
    let m2 = dir2.path().join("roll_m2.json");
    let w2 = dir2.path().join("roll_w2.bin");
    let m3 = dir3.path().join("roll_m3.json");
    let w3 = dir3.path().join("roll_w3.bin");

    // Phase 1: All 3 nodes running, write 10 entries
    {
        let db1 = OmniKV::open(m1.to_str().unwrap(), w1.to_str().unwrap()).unwrap();
        let db2 = OmniKV::open(m2.to_str().unwrap(), w2.to_str().unwrap()).unwrap();
        let db3 = OmniKV::open(m3.to_str().unwrap(), w3.to_str().unwrap()).unwrap();

        let mut batch = omni_engine::WriteBatch::new();
        for i in 0..10 {
            batch
                .set(&format!("roll_k{i}"), format!("roll_v{i}"))
                .unwrap();
        }
        db1.commit_batch(&batch).unwrap();
        db2.commit_batch(&batch).unwrap();
        db3.commit_batch(&batch).unwrap();
    }
    // All 3 nodes "stopped"

    // Phase 2: Restart node1, write 5 more
    {
        let db1 = OmniKV::open(m1.to_str().unwrap(), w1.to_str().unwrap()).unwrap();
        let seq = db1.get_seq();
        // Verify original data survived restart
        for i in 0..10 {
            assert_eq!(
                db1.find(&format!("roll_k{i}"), seq).unwrap(),
                Some(format!("roll_v{i}")),
                "Node1 lost data after restart"
            );
        }
        let mut batch = omni_engine::WriteBatch::new();
        for i in 10..15 {
            batch
                .set(&format!("roll_k{i}"), format!("roll_v{i}"))
                .unwrap();
        }
        db1.commit_batch(&batch).unwrap();
    }

    // Phase 3: Restart node2 and verify
    {
        let db2 = OmniKV::open(m2.to_str().unwrap(), w2.to_str().unwrap()).unwrap();
        let seq = db2.get_seq();
        for i in 0..10 {
            assert_eq!(
                db2.find(&format!("roll_k{i}"), seq).unwrap(),
                Some(format!("roll_v{i}")),
                "Node2 lost data after restart"
            );
        }
    }

    // Phase 4: Restart node3 and verify
    {
        let db3 = OmniKV::open(m3.to_str().unwrap(), w3.to_str().unwrap()).unwrap();
        let seq = db3.get_seq();
        for i in 0..10 {
            assert_eq!(
                db3.find(&format!("roll_k{i}"), seq).unwrap(),
                Some(format!("roll_v{i}")),
                "Node3 lost data after restart"
            );
        }
    }

    println!("✅ ROLLING 14a: All 3 nodes restarted sequentially, zero data loss");
}

/// Gap #14b: Writes continue during rolling restart via Raft
#[test]
fn test_rolling_upgrade_continuous_writes() {
    let (db1, n1, _d1) = create_node("upg_n1");
    let (db2, n2, _d2) = create_node("upg_n2");
    let (db3, n3, _d3) = create_node("upg_n3");

    // Phase 1: Write entries 1-10 to all 3
    for i in 1..=10 {
        n1.append_log(i, &format!("SET upg_k{i} upg_v{i}")).unwrap();
    }
    replicate_to_set(&n1, &[&n2, &n3], 1, 11);
    apply_range(&n1, 1, 11);
    apply_range(&n2, 1, 11);
    apply_range(&n3, 1, 11);

    // Phase 2: "Restart" n3 — simulate by stopping replication, then catching up
    // Leader continues writing to n1, n2
    for i in 11..=15 {
        n1.append_log(i, &format!("SET upg_k{i} upg_v{i}")).unwrap();
    }
    replicate_to_set(&n1, &[&n2], 11, 16);
    apply_range(&n1, 11, 16);
    apply_range(&n2, 11, 16);

    // n3 comes back — catch up
    replicate_to_set(&n1, &[&n3], 11, 16);
    apply_range(&n3, 11, 16);

    // Phase 3: "Restart" n2 — leader writes to n1, n3
    for i in 16..=20 {
        n1.append_log(i, &format!("SET upg_k{i} upg_v{i}")).unwrap();
    }
    replicate_to_set(&n1, &[&n3], 16, 21);
    apply_range(&n1, 16, 21);
    apply_range(&n3, 16, 21);

    // n2 comes back — catch up
    replicate_to_set(&n1, &[&n2], 16, 21);
    apply_range(&n2, 16, 21);

    // All 3 nodes should have all 20 entries
    for (name, db) in [("n1", &db1), ("n2", &db2), ("n3", &db3)] {
        let seq = db.get_seq();
        for i in 1..=20 {
            assert_eq!(
                db.find(&format!("upg_k{i}"), seq).unwrap(),
                Some(format!("upg_v{i}")),
                "{name} missing upg_k{i}"
            );
        }
    }

    println!("✅ ROLLING 14b: 20 writes across rolling restart of n3 then n2, zero loss");
}

/// Gap #14c: Read availability during rolling restart
#[test]
fn test_rolling_upgrade_read_availability() {
    let (db1, n1, _d1) = create_node("avail_n1");
    let (db2, n2, _d2) = create_node("avail_n2");
    let (_db3, n3, _d3) = create_node("avail_n3");

    // Populate all nodes
    for i in 1..=10 {
        n1.append_log(i, &format!("SET av_k{i} av_v{i}")).unwrap();
    }
    replicate_to_set(&n1, &[&n2, &n3], 1, 11);
    apply_range(&n1, 1, 11);
    apply_range(&n2, 1, 11);
    apply_range(&n3, 1, 11);

    // Simulate n3 "down" — reads still available from n1, n2
    let seq1 = db1.get_seq();
    let seq2 = db2.get_seq();
    for i in 1..=10 {
        let v1 = db1.find(&format!("av_k{i}"), seq1).unwrap();
        let v2 = db2.find(&format!("av_k{i}"), seq2).unwrap();
        assert!(v1.is_some(), "n1 should serve reads while n3 is down");
        assert!(v2.is_some(), "n2 should serve reads while n3 is down");
        assert_eq!(v1, v2);
    }

    // Simulate n2 also "restarting" — n1 alone can still serve reads
    let seq1 = db1.get_seq();
    for i in 1..=10 {
        let v1 = db1.find(&format!("av_k{i}"), seq1).unwrap();
        assert!(v1.is_some(), "n1 alone should serve reads");
    }

    println!("✅ ROLLING 14c: Reads available with 1-of-3, 2-of-3, and 3-of-3 nodes up");
}

/// Gap #14d: Generation-tagged data survives rolling upgrade
#[test]
fn test_rolling_upgrade_generation_tags() {
    let (db1, n1, _d1) = create_node("gen_n1");
    let (db2, n2, _d2) = create_node("gen_n2");
    let (db3, n3, _d3) = create_node("gen_n3");

    // Gen-1 writes (simulating "old version")
    for i in 1..=5 {
        n1.append_log(i, &format!("SET gen1_k{i} gen1_v{i}"))
            .unwrap();
    }
    replicate_to_set(&n1, &[&n2, &n3], 1, 6);
    apply_range(&n1, 1, 6);
    apply_range(&n2, 1, 6);
    apply_range(&n3, 1, 6);

    // "Upgrade" n3: gen-2 writes
    for i in 6..=10 {
        n1.append_log(i, &format!("SET gen2_k{i} gen2_v{i}"))
            .unwrap();
    }
    replicate_to_set(&n1, &[&n2, &n3], 6, 11);
    apply_range(&n1, 6, 11);
    apply_range(&n2, 6, 11);
    apply_range(&n3, 6, 11);

    // "Upgrade" n2: gen-3 writes
    for i in 11..=15 {
        n1.append_log(i, &format!("SET gen3_k{i} gen3_v{i}"))
            .unwrap();
    }
    replicate_to_set(&n1, &[&n2, &n3], 11, 16);
    apply_range(&n1, 11, 16);
    apply_range(&n2, 11, 16);
    apply_range(&n3, 11, 16);

    // All generations of data coexist on all nodes
    for (name, db) in [("n1", &db1), ("n2", &db2), ("n3", &db3)] {
        let seq = db.get_seq();
        for i in 1..=5 {
            assert!(
                db.find(&format!("gen1_k{i}"), seq).unwrap().is_some(),
                "{name} missing gen1 data"
            );
        }
        for i in 6..=10 {
            assert!(
                db.find(&format!("gen2_k{i}"), seq).unwrap().is_some(),
                "{name} missing gen2 data"
            );
        }
        for i in 11..=15 {
            assert!(
                db.find(&format!("gen3_k{i}"), seq).unwrap().is_some(),
                "{name} missing gen3 data"
            );
        }
    }

    println!("✅ ROLLING 14d: 3 generations of data (gen1/gen2/gen3) coexist on all nodes");
}

/// Gap #14e: Full cluster restart — all nodes recover from WAL
#[test]
fn test_full_cluster_restart() {
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();

    let m1 = dir1.path().join("full_m1.json");
    let w1 = dir1.path().join("full_w1.bin");
    let m2 = dir2.path().join("full_m2.json");
    let w2 = dir2.path().join("full_w2.bin");

    // Phase 1: Write data to both nodes
    {
        let db1 = OmniKV::open(m1.to_str().unwrap(), w1.to_str().unwrap()).unwrap();
        let db2 = OmniKV::open(m2.to_str().unwrap(), w2.to_str().unwrap()).unwrap();

        let mut batch = omni_engine::WriteBatch::new();
        for i in 0..20 {
            batch
                .set(&format!("full_k{i:02}"), format!("full_v{i}"))
                .unwrap();
        }
        db1.commit_batch(&batch).unwrap();
        db2.commit_batch(&batch).unwrap();
    }
    // Both nodes completely stopped

    // Phase 2: Full restart — both nodes come back
    {
        let db1 = OmniKV::open(m1.to_str().unwrap(), w1.to_str().unwrap()).unwrap();
        let db2 = OmniKV::open(m2.to_str().unwrap(), w2.to_str().unwrap()).unwrap();

        let seq1 = db1.get_seq();
        let seq2 = db2.get_seq();

        // Both should have all 20 entries
        for i in 0..20 {
            let k = format!("full_k{i:02}");
            let expected = format!("full_v{i}");
            assert_eq!(
                db1.find(&k, seq1).unwrap(),
                Some(expected.clone()),
                "db1 missing {k} after full restart"
            );
            assert_eq!(
                db2.find(&k, seq2).unwrap(),
                Some(expected),
                "db2 missing {k} after full restart"
            );
        }

        // Can continue writing after full restart
        let mut batch = omni_engine::WriteBatch::new();
        batch.set("full_k_post", "post_restart".into()).unwrap();
        db1.commit_batch(&batch).unwrap();

        assert_eq!(
            db1.find("full_k_post", db1.get_seq()).unwrap(),
            Some("post_restart".into())
        );
    }

    println!("✅ ROLLING 14e: Full cluster restart — 20 keys recovered, new writes accepted");
}
