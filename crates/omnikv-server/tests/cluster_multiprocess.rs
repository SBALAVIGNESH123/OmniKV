//! Multi-process Raft cluster failover and TCP command interface tests.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const TEST_JWT_SECRET: &str = "test-jwt-secret-0123456789abcdef";

struct Node {
    child: Child,
    /// (tcp, http, quic, pgwire, raft)
    ports: (u16, u16, u16, u16, u16),
    id: u64,
    dir: std::path::PathBuf,
}

impl Node {
    const fn tcp_port(&self) -> u16 {
        self.ports.0
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

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

fn test_token() -> String {
    test_token_role("admin")
}

fn tcp_cmd(port: u16, cmd: &str) -> Result<String, String> {
    tcp_cmd_raw_lines(port, &format!("AUTH {}\n{cmd}\n", test_token()), 2)
}

fn tcp_cmd_role(port: u16, role: &str, cmd: &str) -> Result<String, String> {
    tcp_cmd_raw_lines(port, &format!("AUTH {}\n{cmd}\n", test_token_role(role)), 2)
}

fn tcp_cmd_unauthenticated(port: u16, cmd: &str) -> Result<String, String> {
    tcp_cmd_raw_lines(port, &format!("{cmd}\n"), 1)
}

fn tcp_cmd_raw_lines(port: u16, request: &str, expect_lines: usize) -> Result<String, String> {
    let mut stream =
        TcpStream::connect(("127.0.0.1", port)).map_err(|e| format!("connect: {e}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .map_err(|e| format!("timeout set: {e}"))?;
    stream
        .write_all(request.as_bytes())
        .map_err(|e| format!("write: {e}"))?;

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
    if lines.len() != expect_lines {
        return Err(format!(
            "peer closed after {}/{} replies (got: {lines:?})",
            lines.len(),
            expect_lines
        ));
    }
    Ok(lines.pop().expect("checked len above"))
}

fn allocate_port_set(count: usize) -> Vec<(u16, u16, u16, u16, u16)> {
    const MAX_UDP_BIND_RETRIES: usize = 64;
    let mut tcp_guards = Vec::with_capacity(count * 5);
    let mut udp_guards = Vec::with_capacity(count);
    let mut ports = Vec::with_capacity(count);
    for _ in 0..count {
        let mut tuple = [0u16; 5];
        for (slot_idx, slot) in tuple.iter_mut().enumerate() {
            for attempt in 0..MAX_UDP_BIND_RETRIES {
                let tcp = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral tcp");
                let port = tcp.local_addr().expect("local addr").port();
                if slot_idx == 2 {
                    match std::net::UdpSocket::bind(("127.0.0.1", port)) {
                        Ok(udp) => {
                            udp_guards.push(udp);
                            tcp_guards.push(tcp);
                            *slot = port;
                            break;
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::AddrInUse => {
                            assert!(
                                attempt + 1 < MAX_UDP_BIND_RETRIES,
                                "exhausted UDP port allocation retries due to AddrInUse on port {port}: {err}"
                            );
                        }
                        Err(err) => {
                            panic!("bind ephemeral udp on 127.0.0.1:{port} failed: {err}");
                        }
                    }
                } else {
                    tcp_guards.push(tcp);
                    *slot = port;
                    break;
                }
            }
        }
        ports.push(tuple.into());
    }
    drop(udp_guards);
    drop(tcp_guards);
    ports
}

fn fresh_node_dir(id: u64) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let dir =
        std::env::temp_dir().join(format!("omnikv-cluster-{id}-{}-{seq}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create node dir");
    dir
}

fn spawn_node(id: u64, ports: &[(u16, u16, u16, u16, u16)], tcp_tls: bool) -> Node {
    let idx = usize::try_from(id - 1).expect("node id fits usize");
    let (tcp, http, quic, pgwire, raft) = ports[idx];
    let dir = fresh_node_dir(id);
    let child = Command::new(env!("CARGO_BIN_EXE_omnikv-server"))
        .env("OMNIKV_MODE", "development")
        .env("OMNIKV_TCP_ADDR", format!("127.0.0.1:{tcp}"))
        .env("OMNIKV_TCP_TLS", if tcp_tls { "true" } else { "false" })
        .env("OMNIKV_HTTP_ADDR", format!("127.0.0.1:{http}"))
        .env("OMNIKV_QUIC_ADDR", format!("127.0.0.1:{quic}"))
        .env("OMNIKV_PGWIRE_ADDR", format!("127.0.0.1:{pgwire}"))
        .env("OMNIKV_RAFT_ADDR", format!("127.0.0.1:{raft}"))
        .env("OMNIKV_NODE_ID", id.to_string())
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

fn stderr_excerpt(dir: &std::path::Path) -> String {
    const MAX_CHARS: usize = 1024;
    let Ok(raw) = std::fs::read_to_string(dir.join("stderr.log")) else {
        return "<stderr.log unavailable>".into();
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return "<stderr empty>".into();
    }
    if trimmed.chars().count() <= MAX_CHARS {
        trimmed.to_string()
    } else {
        let tail: String = trimmed
            .chars()
            .rev()
            .take(MAX_CHARS)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        format!("...{tail}")
    }
}

fn boot_cluster() -> Vec<Node> {
    let mut attempt_failures = Vec::new();
    for attempt in 1..=3 {
        let ports = allocate_port_set(3);
        let n2 = spawn_node(2, &ports, false);
        let n3 = spawn_node(3, &ports, false);
        let n1 = spawn_node(1, &ports, false);
        let mut nodes = vec![n1, n2, n3];

        let mut failed_reason = None;
        for node in &mut nodes {
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                match node.child.try_wait() {
                    Ok(Some(status)) => {
                        let log = stderr_excerpt(&node.dir);
                        failed_reason = Some(format!(
                            "attempt {attempt}: node {} exited during startup with {status} (stderr: {log})",
                            node.id
                        ));
                        break;
                    }
                    Err(err) => {
                        let log = stderr_excerpt(&node.dir);
                        failed_reason = Some(format!(
                            "attempt {attempt}: node {} try_wait failed: {err} (stderr: {log})",
                            node.id
                        ));
                        break;
                    }
                    Ok(None) => {}
                }
                if Instant::now() > deadline {
                    let log = stderr_excerpt(&node.dir);
                    failed_reason = Some(format!(
                        "attempt {attempt}: node {} timed out after 30s waiting for TCP listener on port {} (stderr: {log})",
                        node.id,
                        node.tcp_port()
                    ));
                    break;
                }
                match tcp_cmd_unauthenticated(node.tcp_port(), "GET failover:key") {
                    Ok(resp) if resp.contains("AUTH_REQUIRED") => break,
                    _ => std::thread::sleep(Duration::from_millis(100)),
                }
            }
            if failed_reason.is_some() {
                break;
            }
        }
        let Some(failure) = failed_reason else {
            return nodes;
        };
        attempt_failures.push(failure);
        drop(nodes);
        assert!(
            attempt < 3,
            "cluster nodes failed to become ready after 3 attempts:\n{}",
            attempt_failures.join("\n")
        );
    }
    unreachable!()
}

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

fn concurrent_writers_do_not_deadlock(leader_tcp: u16, nodes: &[Node]) {
    let acknowledged = std::sync::atomic::AtomicUsize::new(0);
    let ack = &acknowledged;
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

#[test]
fn tcp_pipelined_commands_all_get_replies() {
    let nodes = boot_cluster();
    let refs: Vec<&Node> = nodes.iter().collect();

    let deadline = Instant::now() + Duration::from_secs(30);
    let leader =
        retry_until(deadline, || find_leader(&refs)).expect("no leader elected within 30s");
    let port = refs[leader].tcp_port();

    let request = format!(
        "AUTH {}\nSET pipe:k one\nGET pipe:k\nDELETE pipe:k\n",
        test_token()
    );
    let got = tcp_cmd_raw_lines(port, &request, 4).expect("pipelined round trip");

    assert!(got.starts_with("DELETED"), "DELETE reply missing: {got}");

    assert_eq!(
        tcp_cmd(port, "GET pipe:k").unwrap_or_default(),
        "NOT_FOUND",
        "pipelined delete did not take effect"
    );
}

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

#[test]
fn cluster_failover_kill_leader_no_data_loss() {
    let nodes = boot_cluster();
    let refs: Vec<&Node> = nodes.iter().collect();

    for node in &refs {
        let refused = tcp_cmd_unauthenticated(node.tcp_port(), "GET failover:key");
        assert!(
            matches!(&refused, Ok(r) if r.contains("AUTH_REQUIRED")),
            "node {} accepted an unauthenticated command: {refused:?}",
            node.id
        );
    }
    println!("all nodes refuse unauthenticated commands");

    let deadline = Instant::now() + Duration::from_secs(30);
    let leader_idx =
        retry_until(deadline, || find_leader(&refs)).expect("no leader elected within 30s");
    let leader_id = refs[leader_idx].id;
    let leader_tcp = refs[leader_idx].tcp_port();
    println!("leader elected: node {leader_id}");

    roles_are_enforced_on_the_command_interface(leader_tcp);

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

    concurrent_writers_do_not_deadlock(leader_tcp, &nodes);

    let survivors: Vec<(u16, u64)> = nodes
        .iter()
        .filter(|n| n.id != leader_id)
        .map(|n| (n.tcp_port(), n.id))
        .collect();
    assert_eq!(survivors.len(), 2, "quorum must survive");
    drop(refs);

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

    let new_leader = retry_until(Instant::now() + Duration::from_secs(30), || {
        find_leader_among(&survivors)
    })
    .expect("no new leader elected within 30s after killing the leader");
    println!("new leader elected: node {}", new_leader.1);

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
}

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

fn boot_tls_node() -> Node {
    let ports = allocate_port_set(1);
    let node = spawn_node(1, &ports, true);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        assert!(
            Instant::now() <= deadline,
            "TLS node listener never came up"
        );
        if TcpStream::connect(("127.0.0.1", node.tcp_port())).is_ok() {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    node
}

#[test]
fn tls_enabled_port_refuses_plaintext_commands() {
    let node = boot_tls_node();
    let mut stream =
        TcpStream::connect(("127.0.0.1", node.tcp_port())).expect("connect to TLS node");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set read timeout");
    stream.write_all(b"PING\n").expect("send plaintext probe");

    let mut buf = [0u8; 128];
    match stream.read(&mut buf) {
        Ok(0) | Err(_) => {}
        Ok(n) => {
            let got = &buf[..n];
            assert!(
                !got.contains(&b'\n'),
                "TLS port answered a plaintext command: {:?}",
                String::from_utf8_lossy(got)
            );
        }
    }
}
