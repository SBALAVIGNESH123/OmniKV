//! Multi-process cluster failover test — the issue #113 Definition of
//! Done evidence: three REAL server processes form a Raft cluster, a
//! write replicates to every node, and killing the leader (hard-kill,
//! no graceful shutdown) elects a new leader with zero data loss.
//!
//! Runs anywhere the test suite runs (Linux CI, Windows dev): it spawns
//! the `omnikv-server` binary three times with distinct ports, data
//! dirs, and raft node ids, drives writes through the TCP command
//! interface (AUTH pipelined ahead of each command — see issue #117),
//! and observes leadership by write-probing (a write succeeds only where
//! the leader is).

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// JWT secret the nodes boot with (see `spawn_node`) and the tests sign
/// tokens with. A node with a mismatched secret refuses every command.
const TEST_JWT_SECRET: &str = "test-jwt-secret-0123456789abcdef";

/// One spawned cluster node. `kill()`ed tests read `ports` after the
/// child is gone, so the connection endpoints live here, not on Child.
struct Node {
    child: Child,
    /// (tcp, http, quic, pgwire, raft)
    ports: (u16, u16, u16, u16, u16),
    id: u64,
    /// Where the node keeps WAL/manifest/raft state. Held so `drop` can
    /// clean it up — a hard-killed node leaves a lock and a log behind.
    dir: std::path::PathBuf,
}

impl Node {
    const fn tcp_port(&self) -> u16 {
        self.ports.0
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        // Test hygiene: never leak server processes, even on failure.
        let _ = self.child.kill();
        let _ = self.child.wait();
        // ...and never leak their state. A stale raft log left behind is
        // not just clutter: the next test to land on this directory
        // inherits it, and openraft then refuses to initialize ("not
        // allowed to initialize due to current raft state"), which shows
        // up from the test as a listener that never comes up.
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Sign a JWT the TCP interface will accept (issue #117): it refuses all
/// data commands until a valid token arrives, and the token's role has to
/// cover the command.
fn test_token_role(role: &str) -> String {
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
    use serde::Serialize;

    #[derive(Serialize)]
    struct Claims {
        sub: String,
        role: String,
        exp: u64,
        iat: u64,
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    let claims = Claims {
        sub: "cluster-test".into(),
        role: role.into(),
        exp: now + 3600,
        iat: now,
    };
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(TEST_JWT_SECRET.as_bytes()),
    )
    .expect("sign test token")
}

/// The admin token the failover check drives writes with.
fn test_token() -> String {
    test_token_role("admin")
}

/// Sends one command to the TCP command interface, returns the
/// response. `AUTH <token>` is pipelined ahead of the command on the
/// same connection; the interface authenticates the session and then
/// runs the command, replying with one line per request.
fn tcp_cmd(port: u16, cmd: &str) -> Result<String, String> {
    tcp_cmd_raw_lines(port, &format!("AUTH {}\n{cmd}\n", test_token()), 2)
}

/// Same wire, a token scoped to `role`: for checking that the interface
/// authorizes per command, not just per session.
fn tcp_cmd_role(port: u16, role: &str, cmd: &str) -> Result<String, String> {
    tcp_cmd_raw_lines(port, &format!("AUTH {}\n{cmd}\n", test_token_role(role)), 2)
}

/// Sends a command with NO auth header — the interface must refuse it.
/// Used to prove the regression fixed in issue #117 stays fixed.
fn tcp_cmd_unauthenticated(port: u16, cmd: &str) -> Result<String, String> {
    tcp_cmd_raw_lines(port, &format!("{cmd}\n"), 1)
}

/// Write `request`, read back until `expect_lines` complete lines have
/// arrived, return the last non-empty one. Reading a fixed number of
/// lines matters: a single `read()` can return a prefix of the response
/// when it arrives in multiple TCP segments, and the exact-match
/// assertions would flake without a product defect. For a pipelined
/// AUTH+command the last line is the command's own reply.
fn tcp_cmd_raw_lines(port: u16, request: &str, expect_lines: usize) -> Result<String, String> {
    let mut stream =
        TcpStream::connect(("127.0.0.1", port)).map_err(|e| format!("connect: {e}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .map_err(|e| format!("timeout set: {e}"))?;
    stream
        .write_all(request.as_bytes())
        .map_err(|e| format!("write: {e}"))?;

    // Peel whole lines out of the buffer as they arrive; a partial line
    // stays in `out` until the next read completes it.
    let mut out = Vec::new();
    let mut lines = Vec::new();
    let mut buf = [0u8; 4096];
    while lines.len() < expect_lines {
        let n = stream.read(&mut buf).map_err(|e| format!("read: {e}"))?;
        if n == 0 {
            break;
        }
        out.extend_from_slice(&buf[..n]);
        while let Some(nl) = out.iter().position(|&b| b == b'\n') {
            let line = String::from_utf8_lossy(&out[..nl]).trim().to_string();
            out.drain(..=nl);
            if !line.is_empty() {
                lines.push(line);
            }
        }
    }
    Ok(lines.pop().unwrap_or_default())
}

/// A free port for a listener the test will bind later. Racy in theory,
/// fine in practice: the window between drop and re-bind is test-local.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral")
        .local_addr()
        .expect("local addr")
        .port()
}

/// Hands out a fresh data dir for every node, in every test. The naive
/// `omnikv-cluster-{id}-{pid}` collides: two tests in one test binary
/// share a pid, so the second cluster boots on the first one's raft log.
/// openraft then refuses to initialize a node that already has one ("not
/// allowed to initialize due to current raft state"), and the failure
/// surfaces only as a listener that never comes up inside a 30s timeout.
fn fresh_node_dir(id: u64) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let dir =
        std::env::temp_dir().join(format!("omnikv-cluster-{id}-{}-{seq}", std::process::id()));
    // A previous run may have died without dropping its nodes; booting on
    // that state is the exact failure this function exists to prevent.
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create node dir");
    dir
}

/// The command for one cluster node: distinct TCP/HTTP/QUIC/pgwire/
/// raft ports, a private data dir, fast election timers (the test must
/// converge in seconds, not minutes).
fn spawn_node(id: u64, ports: &[(u16, u16, u16, u16, u16)]) -> Node {
    let idx = usize::try_from(id - 1).expect("node id fits usize");
    let (tcp, http, quic, pgwire, raft) = ports[idx];
    let dir = fresh_node_dir(id);
    let child = Command::new(env!("CARGO_BIN_EXE_omnikv-server"))
        .env("OMNIKV_MODE", "development")
        .env("OMNIKV_TCP_ADDR", format!("127.0.0.1:{tcp}"))
        .env("OMNIKV_HTTP_ADDR", format!("127.0.0.1:{http}"))
        .env("OMNIKV_QUIC_ADDR", format!("127.0.0.1:{quic}"))
        .env("OMNIKV_PGWIRE_ADDR", format!("127.0.0.1:{pgwire}"))
        .env("OMNIKV_RAFT_ADDR", format!("127.0.0.1:{raft}"))
        .env("OMNIKV_NODE_ID", id.to_string())
        // Only node 1's peer list is consumed (initial membership);
        // peer ids are 2.. in declaration order — see raft_node.rs.
        .env(
            "OMNIKV_RAFT_PEERS",
            if id == 1 {
                ports
                    .iter()
                    .skip(1)
                    .map(|p| format!("127.0.0.1:{}", p.4))
                    .collect::<Vec<_>>()
                    .join(",")
            } else {
                String::new()
            },
        )
        .env(
            "OMNIKV_MANIFEST_PATH",
            dir.join("manifest.json").to_string_lossy().to_string(),
        )
        .env(
            "OMNIKV_WAL_PATH",
            dir.join("wal.bin").to_string_lossy().to_string(),
        )
        .env("OMNIKV_JWT_SECRET", TEST_JWT_SECRET)
        .env(
            "OMNIKV_BOOTSTRAP_ADMIN_KEY",
            "test-bootstrap-key-0123456789",
        )
        .env("OMNIKV_TLS_INSECURE_SKIP", "true")
        .env("OMNIKV_RAFT_HEARTBEAT_MS", "50")
        .env("OMNIKV_RAFT_ELECTION_MIN_MS", "150")
        .env("OMNIKV_RAFT_ELECTION_MAX_MS", "300")
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        // Server stderr goes to the node dir, not /dev/null: a node that
        // never binds its listener is otherwise invisible from the test,
        // and the failure shows up only as a 30s timeout.
        .stderr(std::process::Stdio::from(
            std::fs::File::create(dir.join("stderr.log")).expect("create stderr log"),
        ))
        .spawn()
        .expect("spawn omnikv-server");
    Node {
        child,
        ports: (tcp, http, quic, pgwire, raft),
        id,
        dir,
    }
}

/// Spawns the three-node cluster and waits for every TCP listener.
fn boot_cluster() -> Vec<Node> {
    let ports: Vec<(u16, u16, u16, u16, u16)> = (0..3)
        .map(|_| {
            (
                free_port(),
                free_port(),
                free_port(),
                free_port(),
                free_port(),
            )
        })
        .collect();
    let nodes: Vec<Node> = (1..=3).map(|id| spawn_node(id, &ports)).collect();

    let deadline = Instant::now() + Duration::from_secs(30);
    for node in &nodes {
        loop {
            assert!(
                Instant::now() <= deadline,
                "node {} TCP listener never came up",
                node.id
            );
            if TcpStream::connect(("127.0.0.1", node.tcp_port())).is_ok() {
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    nodes
}

/// The leadership oracle: a SET through the write gateway succeeds only
/// on the leader (followers answer "not the leader; ...").
fn find_leader(nodes: &[&Node]) -> Option<usize> {
    for (i, node) in nodes.iter().enumerate() {
        if let Ok(resp) = tcp_cmd(node.tcp_port(), "SET __probe__ x")
            && resp.starts_with("OK")
        {
            return Some(i);
        }
    }
    None
}

fn retry_until<T>(deadline: Instant, f: impl Fn() -> Option<T>) -> Option<T> {
    while Instant::now() < deadline {
        if let Some(v) = f() {
            return Some(v);
        }
        std::thread::sleep(Duration::from_millis(150));
    }
    None
}

/// 16 simultaneous writers against the leader — the PR #127 review's
/// deadlock finding. Async REST/QUIC handlers park their own runtime's
/// workers waiting for consensus; before the dedicated consensus
/// runtime, enough concurrent writes could park every worker and starve
/// the very openraft tasks that had to finish their proposals to release
/// them. 16 writers at once (more than typical CI worker counts) must
/// all get answers — serialized by the flight lock, but NEVER stuck —
/// and every write must replicate to every node. Extracted to a helper
/// to keep the failover test under clippy's function-length limit.
fn concurrent_writers_do_not_deadlock(leader_tcp: u16, nodes: &[Node]) {
    let acknowledged = std::sync::atomic::AtomicUsize::new(0);
    // A shared ref is Copy, so each `move` writer copies the ref
    // instead of fighting over ownership of the atomic itself.
    let ack = &acknowledged;
    // thread::scope joins every writer before it returns, so the count
    // is complete by the time we read it back below.
    std::thread::scope(|scope| {
        for w in 0..16u32 {
            scope.spawn(move || {
                let resp = tcp_cmd(leader_tcp, &format!("SET conc:w{w} v{w}")).unwrap_or_default();
                if resp.starts_with("OK") {
                    ack.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            });
        }
    });
    let ok = acknowledged.into_inner();
    assert_eq!(ok, 16, "concurrent writes: {ok}/16 acknowledged");
    // Every concurrent write must replicate to every node.
    for node in nodes {
        let seen = retry_until(Instant::now() + Duration::from_secs(10), || {
            (0..16u32)
                .all(|w| {
                    matches!(tcp_cmd(node.tcp_port(), &format!("GET conc:w{w}")),
                             Ok(r) if r == format!("OK: v{w}"))
                })
                .then_some(())
        });
        assert!(seen.is_some(), "node {} missed a concurrent write", node.id);
    }
    println!("16 concurrent writers all acknowledged and replicated");
}

/// Several commands in ONE write segment must each get a reply. This is
/// the framing contract the interface's line buffer has to keep: an early
/// version of the bounded reader consumed the whole segment, answered the
/// first line, and discarded the rest — the client hung waiting for
/// replies that were never coming, with no error anywhere.
#[test]
fn tcp_pipelined_commands_all_get_replies() {
    let nodes = boot_cluster();
    let refs: Vec<&Node> = nodes.iter().collect();

    // The SET has to land on the leader: boot_cluster waits for the
    // listeners, not for an election, and a pre-election batch comes back
    // NOT_LEADER with nothing to DELETE. Whichever node won does not
    // matter, only that one has by the time the batch goes out.
    let deadline = Instant::now() + Duration::from_secs(30);
    let leader =
        retry_until(deadline, || find_leader(&refs)).expect("no leader elected within 30s");
    let port = refs[leader].tcp_port();

    // AUTH plus three data commands, written as one segment.
    let request = format!(
        "AUTH {}\nSET pipe:k one\nGET pipe:k\nDELETE pipe:k\n",
        test_token()
    );
    let got = tcp_cmd_raw_lines(port, &request, 4).expect("pipelined round trip");

    // The LAST reply is the one the assertions care about, but every line
    // must have arrived — a dropped command is the failure mode.
    assert!(got.starts_with("DELETED"), "DELETE reply missing: {got}");

    // And the write/read really happened, not just replies returned:
    assert_eq!(
        tcp_cmd(port, "GET pipe:k").unwrap_or_default(),
        "NOT_FOUND",
        "pipelined delete did not take effect"
    );
}

/// A valid token is not a blanket grant: a read-scoped token may look
/// around but must not mutate, and a write token may. Authentication
/// without per-command authorization is a bypass (CWE-862). Extracted to
/// a helper to keep the failover test under clippy's function-length
/// limit. The refusals would hold on any node — the role gate fires
/// before dispatch — but the write token's `SET` has to reach the leader
/// to come back `OK`, so the caller passes a leader's port.
fn roles_are_enforced_on_the_command_interface(port: u16) {
    let read_get = tcp_cmd_role(port, "read", "GET failover:key");
    assert!(
        matches!(&read_get, Ok(r) if r.starts_with("OK:") || r == "NOT_FOUND"),
        "read token must be able to GET (got: {read_get:?})"
    );
    let denied = tcp_cmd_role(port, "read", "SET role-check nope").unwrap_or_default();
    assert!(
        denied.contains("FORBIDDEN"),
        "read token accepted a write: {denied}"
    );
    let denied_del = tcp_cmd_role(port, "read", "DELETE role-check").unwrap_or_default();
    assert!(
        denied_del.contains("FORBIDDEN"),
        "read token accepted a delete: {denied_del}"
    );
    assert!(
        tcp_cmd_role(port, "write", "SET role-check ok")
            .unwrap_or_default()
            .starts_with("OK"),
        "write token must be able to SET"
    );
    println!("roles enforced: read cannot write, write can");
}

/// THE test: three real processes, one replicated write, kill the
/// leader, a new leader is elected, and no data is lost.
#[test]
fn cluster_failover_kill_leader_no_data_loss() {
    let nodes = boot_cluster();
    let refs: Vec<&Node> = nodes.iter().collect();

    // ── 0. The command interface refuses work before AUTH (issue #117) ──
    // Before the fix, anyone who could reach the port had unrestricted
    // read/write. Now every data command is gated behind a verified JWT.
    for node in &refs {
        let refused = tcp_cmd_unauthenticated(node.tcp_port(), "GET failover:key");
        assert!(
            matches!(&refused, Ok(r) if r.contains("AUTH_REQUIRED")),
            "node {} accepted an unauthenticated command: {refused:?}",
            node.id
        );
    }
    println!("all nodes refuse unauthenticated commands");

    // ── 1. A leader is elected and accepts writes ──
    let deadline = Instant::now() + Duration::from_secs(30);
    let leader_idx =
        retry_until(deadline, || find_leader(&refs)).expect("no leader elected within 30s");
    let leader_id = refs[leader_idx].id;
    let leader_tcp = refs[leader_idx].tcp_port();
    println!("leader elected: node {leader_id}");

    // ── 1b. A token's role gates each command. Runs here, after the
    // leader is known: the refusals hold on any node (the role gate fires
    // before dispatch), but the write token's SET only comes back OK from
    // the leader — a follower answers NOT_LEADER, which would make this
    // assertion flake on which node won the election.
    roles_are_enforced_on_the_command_interface(leader_tcp);

    // ── 2. A write on the leader replicates to every node ──
    let write = tcp_cmd(leader_tcp, "SET failover:key hello-cluster");
    assert!(
        write.unwrap_or_default().starts_with("OK"),
        "leader write failed"
    );
    for node in &nodes {
        let seen = retry_until(Instant::now() + Duration::from_secs(10), || {
            matches!(tcp_cmd(node.tcp_port(), "GET failover:key"),
                     Ok(r) if r.starts_with("OK: hello-cluster"))
            .then_some(())
        });
        assert!(
            seen.is_some(),
            "node {} never saw the replicated write",
            node.id
        );
    }
    println!("write replicated to all 3 nodes");

    // ── 3. Followers reject writes with the leader's identity ──
    let follower = refs
        .iter()
        .enumerate()
        .find(|(i, _)| *i != leader_idx)
        .map(|(_, n)| *n)
        .expect("a follower exists in a 3-node cluster");
    let resp = tcp_cmd(follower.tcp_port(), "SET failover:other v").unwrap_or_default();
    assert!(
        resp.to_lowercase().contains("leader"),
        "follower accepted a write: {resp}"
    );
    println!("follower correctly rejected a write: {resp}");

    // ── 3b. CONCURRENT writers do not deadlock the cluster ──
    // The PR #127 review's P1: async handlers on the server runtime
    // park their workers waiting for consensus; before the dedicated
    // consensus runtime, enough concurrent writes starved the very
    // openraft tasks that had to complete their proposals.
    concurrent_writers_do_not_deadlock(leader_tcp, &nodes);

    // ── 4. Kill the leader — hard kill, no graceful shutdown ──
    // Capture the survivors' endpoints BEFORE consuming `nodes`.
    let survivors: Vec<(u16, u64)> = nodes
        .iter()
        .filter(|n| n.id != leader_id)
        .map(|n| (n.tcp_port(), n.id))
        .collect();
    assert_eq!(survivors.len(), 2, "quorum must survive");
    drop(refs); // release the borrows before consuming `nodes`

    let mut nodes = nodes;
    let leader_pos = nodes
        .iter()
        .position(|n| n.id == leader_id)
        .expect("leader in nodes");
    let mut killed = nodes.swap_remove(leader_pos);
    killed.child.kill().expect("kill leader");
    killed.child.wait().expect("reap leader");
    drop(killed);
    println!("leader node {leader_id} killed");

    // ── 5. Survivors elect a new leader; the data survived ──
    let new_leader = retry_until(Instant::now() + Duration::from_secs(30), || {
        find_leader_among(&survivors)
    })
    .expect("no new leader elected within 30s after killing the leader");
    println!("new leader elected: node {}", new_leader.1);

    // No data loss: the replicated key is still there, and a NEW write
    // on the new leader (the strongest form of "cluster is alive")
    // replicates to the other survivor.
    let read = tcp_cmd(new_leader.0, "GET failover:key").unwrap_or_default();
    assert!(
        read.starts_with("OK: hello-cluster"),
        "DATA LOSS: new leader lost failover:key (got: {read})"
    );
    let write = tcp_cmd(new_leader.0, "SET failover:post value-after-failover");
    assert!(
        write.unwrap_or_default().starts_with("OK"),
        "write on new leader failed"
    );
    let other = survivors
        .iter()
        .find(|(_, id)| *id != new_leader.1)
        .expect("other survivor");
    let seen = retry_until(Instant::now() + Duration::from_secs(10), || {
        matches!(tcp_cmd(other.0, "GET failover:post"),
                 Ok(r) if r.starts_with("OK: value-after-failover"))
        .then_some(())
    });
    assert!(
        seen.is_some(),
        "post-failover write never reached the other survivor"
    );
    println!("post-failover write replicated — no data loss, cluster fully alive");

    // Nodes are dropped here: Drop kills the survivors (the leader is
    // already dead) and removes their data dirs.
}

/// Leadership among a subset of (`tcp_port`, `node_id`) pairs.
fn find_leader_among(nodes: &[(u16, u64)]) -> Option<(u16, u64)> {
    for (port, id) in nodes {
        if let Ok(resp) = tcp_cmd(*port, "SET __probe_after__ x")
            && resp.starts_with("OK")
        {
            return Some((*port, *id));
        }
    }
    None
}
