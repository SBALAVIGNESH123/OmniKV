//! Multi-Node Raft Integration Test
//!
//! Proves: log replication, leader election, crash recovery across 3 nodes.

use omni_engine::OmniKV;
use omni_engine::raft_storage::OmniRaftStorage;
use std::sync::Arc;

fn create_node(name: &str) -> (Arc<OmniKV>, OmniRaftStorage, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let manifest = dir.path().join(format!("{}_manifest.json", name));
    let wal = dir.path().join(format!("{}_wal.bin", name));
    let db = OmniKV::open(manifest.to_str().unwrap(), wal.to_str().unwrap()).unwrap();
    let storage = OmniRaftStorage::new(db.clone());
    (db, storage, dir)
}

/// Simulate leader replicating log entries to followers
fn replicate_log(leader: &OmniRaftStorage, followers: &[&OmniRaftStorage], start: u64, end: u64) {
    for idx in start..end {
        let entry = leader
            .read_log(idx)
            .expect(&format!("Leader missing log {}", idx));
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
            .append_log(i, &format!("SET key{} value{}", i, i))
            .unwrap();
    }

    // Replicate to followers
    replicate_log(&node1, &[&node2, &node3], 1, 11);

    // Verify all 3 nodes have identical logs
    for i in 1..=10 {
        let e1 = node1.read_log(i).unwrap();
        let e2 = node2.read_log(i).unwrap();
        let e3 = node3.read_log(i).unwrap();
        assert_eq!(e1, e2, "Node1 vs Node2 mismatch at index {}", i);
        assert_eq!(e2, e3, "Node2 vs Node3 mismatch at index {}", i);
    }

    println!("✅ 3-NODE LOG REPLICATION: All 10 entries identical across 3 nodes");
}

#[test]
fn test_state_machine_apply() {
    let (db1, node1, _d1) = create_node("leader");
    let (db2, node2, _d2) = create_node("follower1");
    let (db3, node3, _d3) = create_node("follower2");

    // Leader writes data through Raft log
    let entries = vec![
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
            "{} missing user:1",
            name
        );
        assert_eq!(
            db.find("user:2", seq).unwrap(),
            Some("Bob".into()),
            "{} missing user:2",
            name
        );
        assert_eq!(
            db.find("user:3", seq).unwrap(),
            Some("Charlie".into()),
            "{} missing user:3",
            name
        );
        assert_eq!(
            db.find("balance:1", seq).unwrap(),
            Some("1000".into()),
            "{} missing balance:1",
            name
        );
        assert_eq!(
            db.find("balance:2", seq).unwrap(),
            Some("2500".into()),
            "{} missing balance:2",
            name
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
        node1
            .append_log(i, &format!("SET key{} val{}", i, i))
            .unwrap();
    }
    replicate_log(&node1, &[&node2, &node3], 1, 6);

    // === Node1 crashes! Node2 becomes new leader (term 2) ===
    node2.save_vote(r#"{"term":2,"voted_for":2}"#).unwrap();
    node3.save_vote(r#"{"term":2,"voted_for":2}"#).unwrap();

    // New leader (node2) writes entries 6-8
    for i in 6..=8 {
        node2
            .append_log(i, &format!("SET newkey{} newval{}", i, i))
            .unwrap();
    }

    // Replicate to node3 only (node1 is "dead")
    replicate_log(&node2, &[&node3], 6, 9);

    // Verify node2 and node3 have all 8 entries
    for i in 1..=8 {
        assert!(node2.read_log(i).is_some(), "Node2 missing log {}", i);
        assert!(node3.read_log(i).is_some(), "Node3 missing log {}", i);
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
        assert_eq!(e1, e2, "Mismatch at {}", i);
        assert_eq!(e2, e3, "Mismatch at {}", i);
    }

    println!("✅ LEADER ELECTION: Node1 crashed, Node2 elected, Node1 recovered and caught up");
}

#[test]
fn test_log_compaction() {
    let (_db, node, _d) = create_node("compact_node");

    // Write 20 entries
    for i in 1..=20 {
        node.append_log(i, &format!("SET k{} v{}", i, i)).unwrap();
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
        assert!(node.read_log(i).is_none(), "Log {} should be compacted", i);
    }

    // Entries 11-20 should still exist
    for i in 11..=20 {
        assert!(node.read_log(i).is_some(), "Log {} should exist", i);
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
        let storage = OmniRaftStorage::new(db.clone());

        for i in 1..=5 {
            storage
                .append_log(i, &format!("SET persist{} data{}", i, i))
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
            let val = db.find(&format!("persist{}", i), seq).unwrap();
            assert_eq!(val, Some(format!("data{}", i)), "Data {} not persisted", i);
        }
    }

    println!("✅ CRASH RECOVERY: Vote, log index, and data survived restart");
}
