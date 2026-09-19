//! OmniKV — Embeddable + Distributed KV Engine
//!
//! Production binary that starts:
//! 1. HTTP/1.1 + HTTP/2 REST API (Axum + axum-server with ALPN TLS)
//! 2. QUIC/HTTP3 binary protocol (Quinn)
//! 3. PostgreSQL wire protocol v3 (PgWire)
//! 4. Prometheus metrics on /metrics

#![expect(
    dead_code,
    unused_mut,
    reason = "The server crate keeps staged QUIC client helpers that are built in CI before every protocol surface is enabled by the binary."
)]
#![expect(
    clippy::doc_markdown,
    clippy::format_push_string,
    clippy::ignored_unit_patterns,
    clippy::manual_let_else,
    clippy::match_same_arms,
    clippy::missing_const_for_fn,
    clippy::option_if_let_else,
    clippy::single_match_else,
    clippy::trait_duplication_in_bounds,
    clippy::uninlined_format_args,
    reason = "Strict clippy lint groups are enabled. These server style findings are documented legacy debt while protocol hardening work continues."
)]

mod api;
mod auth;
mod quic_server;
mod raft_node;
mod raft_routes;

use std::sync::Arc;

use omni_engine::{OmniKV, config::ServerConfig, hardening::RateLimiter};

fn print_banner(cfg: &ServerConfig, cluster_mode: Option<u64>) {
    // The honesty rules this banner follows (issue #113): "Distributed"
    // only when consensus is actually wired (a raft node booted), and
    // the build credit names openraft instead of claiming every byte.
    let dist_line = match cluster_mode {
        Some(node_id) => format!("Raft cluster (node {node_id})"),
        None => "Single-node".to_string(),
    };
    let feature_line = format!("Embeddable · Transactional KV · {dist_line}");
    println!();
    println!("  ╔════════════════════════════════════════════════════╗");
    println!(
        "  ║        ⚡ OmniKV v{}                       ║",
        env!("CARGO_PKG_VERSION")
    );
    println!("  ║  {feature_line:<48}      ║");
    println!("  ╠════════════════════════════════════════════════════╣");
    println!(
        "  ║  HTTP/1.1 + HTTP/2 (TLS)  → {}           ║",
        cfg.http_addr
    );
    println!(
        "  ║  QUIC/HTTP3 (binary)      → {}           ║",
        cfg.quic_addr
    );
    println!(
        "  ║  PostgreSQL Wire Protocol → {}           ║",
        cfg.pgwire_addr
    );
    println!(
        "  ║  TCP Command Interface    → {}           ║",
        cfg.tcp_addr
    );
    println!("  ╠════════════════════════════════════════════════════╣");
    println!("  ║  Storage · SQL · wire protocols built from scratch ║");
    println!("  ║  Consensus: openraft (Raft)                         ║");
    println!("  ╚════════════════════════════════════════════════════╝");
    println!();
}

fn start_storage_maintenance(
    db: &Arc<OmniKV>,
    cfg: &ServerConfig,
) -> Result<std::thread::JoinHandle<()>, omni_engine::OmniError> {
    db.set_compaction_policy(cfg.storage.compaction_policy())?;
    Ok(db.start_background_compaction(
        cfg.storage.compaction_check_interval_ms,
        cfg.storage.memtable_flush_threshold,
    ))
}

fn log_database_opened(db: &OmniKV, cfg: &ServerConfig) {
    tracing::info!(
        seq = db.get_seq(),
        sstables = db.sstable_count(),
        l0_compaction_trigger = cfg.storage.l0_compaction_trigger,
        l1_compaction_trigger = cfg.storage.l1_compaction_trigger,
        l0_write_stall_threshold = cfg.storage.l0_write_stall_threshold,
        "Database opened"
    );
}

fn install_rustls_crypto_provider() {
    match rustls::crypto::ring::default_provider().install_default() {
        Ok(()) => {}
        Err(_) => {
            tracing::debug!("rustls crypto provider was already installed");
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize structured logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,omni_engine=debug".parse().unwrap()),
        )
        .json()
        .init();

    install_rustls_crypto_provider();

    // Load the single authoritative server runtime config.
    //
    // Precedence is: defaults < config file < environment variables.
    // `--config <path>` selects the config file ahead of OMNIKV_CONFIG /
    // legacy OMNI_CONFIG. Production mode then fails closed on invalid or
    // unsafe settings.
    let cfg = ServerConfig::load_server_from_args(std::env::args().skip(1))?;
    tracing::info!(
        mode = ?cfg.mode,
        http_addr = %cfg.http_addr,
        manifest = %cfg.storage.manifest_path,
        "Configuration loaded"
    );

    // Clone paths before opening so they remain available for AppState.
    let manifest_path = cfg.storage.manifest_path.clone();
    let wal_path = cfg.storage.wal_path.clone();

    // Open the database using configured paths.
    let db = OmniKV::open(&manifest_path, &wal_path)?;
    let _compaction_handle = start_storage_maintenance(&db, &cfg)?;
    log_database_opened(&db, &cfg);

    // ─── Cluster boot (issue #113): a real raft node when configured ──
    // OMNIKV_RAFT_ADDR + OMNIKV_NODE_ID make this server a cluster
    // member: every client write routes through consensus before it is
    // acknowledged, and this node serves consensus RPCs for its peers.
    // Absent, the process stays a fully independent single-node engine
    // and none of the cluster machinery runs.
    //
    // Called from async main, but the boot hops off the runtime worker
    // internally (see boot_cluster_node): the openraft node and its
    // consensus listener are constructed on the gateway's DEDICATED
    // consensus runtime, never the client-facing server runtime —
    // concurrent client writes can park server workers, but never
    // starve openraft's tasks.
    let cluster = raft_node::boot_cluster_node(&cfg, &db)?;
    let cluster_mode = if cluster.is_some() {
        cfg.raft.node_id
    } else {
        None
    };
    print_banner(&cfg, cluster_mode);

    let rate_limiter = Arc::new(RateLimiter::new(
        cfg.rate_limit_per_sec,
        cfg.rate_limit_burst,
        cfg.rate_limit_max_users,
    ));
    tracing::info!(
        rate_limit_per_sec = cfg.rate_limit_per_sec,
        rate_limit_burst = cfg.rate_limit_burst,
        rate_limit_max_users = cfg.rate_limit_max_users,
        "Shared protocol rate limiter configured"
    );

    let app_state = api::AppState {
        db: db.clone(),
        jwt_secret: cfg.jwt_secret.clone(),
        bootstrap_admin_key: cfg.bootstrap_admin_key.clone(),
        manifest_path,
        wal_path,
        rate_limiter: rate_limiter.clone(),
        cluster_gateway: cluster.as_ref().map(|c| c.gateway.clone()),
        cluster_node_id: cfg.raft.node_id,
    };

    let (http_handle, quic_handle, tcp_handle) =
        spawn_protocol_servers(db, &cfg, rate_limiter, app_state).await?;

    // ─── 5. Raft consensus listener (cluster mode only) ───────
    // Serves /raft/{append,vote,snapshot} for this node's peers on the
    // dedicated plaintext port — ON the consensus runtime (never the
    // client-facing server runtime), so parked client workers can't
    // starve peer RPCs either. A dead listener means a deaf cluster
    // member, so its exit is handled like any other server exit below.
    let raft_handle = cluster.map(|node| {
        let handle = node.gateway.consensus_handle.clone();
        handle.spawn(async move {
            if let Err(e) = raft_node::serve_raft_rpc(node).await {
                tracing::error!("Raft consensus listener exited: {e}");
            }
        })
    });

    tracing::info!("All servers started. OmniKV is ready.");

    // Wait for any server to exit (they should not)
    tokio::select! {
        _ = http_handle => tracing::error!("HTTP server exited"),
        _ = quic_handle => tracing::error!("QUIC server exited"),
        _ = tcp_handle => tracing::error!("TCP server exited"),
        _ = async {
            match raft_handle {
                Some(handle) => handle.await.expect("raft listener task"),
                // Single-node mode: never resolves; the other branches
                // still decide the outcome.
                None => std::future::pending::<()>().await,
            }
        } => tracing::error!("Raft consensus listener exited"),
    }

    Ok(())
}

/// Starts the four protocol servers (HTTP/2, QUIC, PgWire, TCP) and
/// returns their supervision handles. Each logs its own fatal error;
/// the caller's select treats any exit as a broken server.
async fn spawn_protocol_servers(
    db: Arc<OmniKV>,
    cfg: &ServerConfig,
    rate_limiter: Arc<RateLimiter>,
    app_state: api::AppState,
) -> Result<
    (
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<()>,
    ),
    Box<dyn std::error::Error>,
> {
    let router = api::build_router(app_state);

    // ─── 1. HTTP/1.1 + HTTP/2 (TLS, ALPN) ──────────────────────
    let (certs, key) = quic_server::generate_self_signed_cert()?;
    let tls_config = axum_server::tls_rustls::RustlsConfig::from_der(
        certs.iter().map(|c| c.as_ref().to_vec()).collect(),
        key.secret_der().to_vec(),
    )
    .await?;

    // Clone addr strings before async move closures consume cfg.
    let http_addr_str = cfg.http_addr.clone();
    let quic_addr_str = cfg.quic_addr.clone();
    let pgwire_addr_str = cfg.pgwire_addr.clone();
    let tcp_addr_str = cfg.tcp_addr.clone();
    let http_addr: std::net::SocketAddr = http_addr_str.parse()?;

    let http_handle = tokio::spawn(async move {
        tracing::info!("HTTP/1.1 + HTTP/2 server starting on {http_addr_str}");
        if let Err(e) = axum_server::bind_rustls(http_addr, tls_config)
            .serve(router.into_make_service_with_connect_info::<std::net::SocketAddr>())
            .await
        {
            tracing::error!("HTTP server error: {e}");
        }
    });

    // ─── 2. QUIC/HTTP3 Binary Protocol ─────────────────────────
    let (quic_certs, quic_key) = quic_server::generate_self_signed_cert()?;
    let quic_endpoint = quic_server::create_server_endpoint(&quic_addr_str, quic_certs, quic_key)?;
    let quic_db = db.clone();
    let quic_rate_limiter = rate_limiter.clone();
    let quic_handle = tokio::spawn(async move {
        quic_server::run_quic_server(quic_endpoint, quic_db, quic_rate_limiter).await;
    });

    // ─── 3. PostgreSQL Wire Protocol ───────────────────────────
    let pgwire_db = db.clone();
    let pgwire_rate_limiter = rate_limiter.clone();
    let _pgwire_handle = std::thread::spawn(move || {
        // Log before moving pgwire_addr_str into PgWireServer::new.
        tracing::info!("PostgreSQL wire protocol starting on {pgwire_addr_str}");
        let server = omni_engine::pgwire::PgWireServer::with_rate_limiter(
            pgwire_db,
            &pgwire_addr_str,
            pgwire_rate_limiter,
        );
        if let Err(e) = server.start() {
            tracing::error!("PgWire server error: {e}");
        }
    });

    // ─── 4. TCP Command Interface (for telnet/debug) ──────────
    let tcp_db = db.clone();
    let tcp_secret = cfg.jwt_secret.clone();
    let tcp_rate_limiter = rate_limiter.clone();
    let tcp_handle = tokio::spawn(async move {
        if let Err(e) = run_tcp_server(tcp_db, &tcp_addr_str, tcp_secret, tcp_rate_limiter).await {
            tracing::error!("TCP server error: {e}");
        }
    });

    Ok((http_handle, quic_handle, tcp_handle))
}

/// Upper bound on one buffered command line. Sits above `max_value_size`
/// (10 MiB) so a legitimate single-line SET still fits; beyond it the
/// connection is dropped rather than absorbing unbounded memory per peer.
const MAX_TCP_LINE: usize = 12 * 1024 * 1024;

/// Failed AUTH attempts tolerated on one connection before it is dropped.
/// Slows token grinding on a publicly bound interface; a legitimate client
/// that authenticates never reaches it.
const MAX_TCP_AUTH_FAILURES: u32 = 5;

/// Simple TCP command interface for telnet/debugging.
///
/// # Authentication
/// Every command other than `AUTH` and `QUIT` is rejected with
/// `ERR AUTH_REQUIRED` until the connection authenticates:
///
/// ```text
/// AUTH <jwt-token>
/// OK: authenticated as <subject>
/// GET some-key
/// OK: <value>
/// ```
///
/// The token is the same JWT the REST and QUIC paths accept (`sub` +
/// `role` + expiry, HS256-signed with the configured secret), verified
/// through [`crate::auth::verify_token`]. This closes the hole where the
/// interface granted unauthenticated full read/write to anyone who could
/// reach the port, bypassing the auth layer guarding every other protocol.
/// Like the QUIC listener, a connection to a node with no secret configured
/// cannot authenticate at all (`ERR AUTH_NOT_CONFIGURED`) — bind loopback
/// and configure a secret, or leave the interface off. Authenticated
/// commands share the QUIC path's per-identity rate limiter, and a session
/// that fails `AUTH` five times is dropped (`ERR TOO_MANY_FAILURES`) to slow
/// token grinding. Error replies are sanitized: engine internals never reach
/// the client (see [`tcp_err_response`]).
///
/// Commands are framed on newlines, so a client may pipeline several
/// commands per segment; lines above [`MAX_TCP_LINE`] are rejected and the
/// connection closed.
async fn run_tcp_server(
    db: Arc<OmniKV>,
    addr: &str,
    jwt_secret: String,
    rate_limiter: Arc<RateLimiter>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("TCP command interface on {addr}");

    loop {
        let (socket, peer) = listener.accept().await?;
        tokio::spawn(handle_tcp_connection(
            socket,
            peer,
            db.clone(),
            jwt_secret.clone(),
            rate_limiter.clone(),
        ));
    }
}

/// One authenticated client session. See [`run_tcp_server`] for the
/// protocol's security model; this is the per-connection state machine.
async fn handle_tcp_connection(
    socket: tokio::net::TcpStream,
    peer: std::net::SocketAddr,
    db: Arc<OmniKV>,
    jwt_secret: String,
    rate_limiter: Arc<RateLimiter>,
) {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

    // Frame on newlines: a client may pipeline several commands in
    // one segment, and a single command may straddle a read
    // boundary. Buffering by line (instead of consuming one command
    // per 4096-byte read) keeps both cases intact.
    let (read_half, mut write_half) = socket.into_split();
    let mut reader = tokio::io::BufReader::new(read_half);
    let mut raw = Vec::with_capacity(512);

    // A connection is unauthenticated until a valid AUTH lands.
    // Every data command before then is refused, so an open port
    // never doubles as an open database.
    let mut authenticated = false;
    let mut identity = String::new();
    let mut auth_failures = 0u32;

    loop {
        raw.clear();
        match reader.read_until(b'\n', &mut raw).await {
            Ok(0) => return,
            Ok(_) => {}
            Err(_) => return,
        }

        // Cap the line length: without it, a client can make the
        // server buffer an unbounded "value" per connection. The
        // limit sits above max_value_size so legitimate writes fit.
        if raw.len() > MAX_TCP_LINE {
            let _ = write_half.write_all(b"ERROR: LINE_TOO_LONG\n").await;
            return;
        }

        let request = String::from_utf8_lossy(&raw);
        let request = request.trim();
        if request.is_empty() {
            continue;
        }

        let mut parts = request.splitn(3, char::is_whitespace);
        let cmd = parts.next().unwrap_or("");

        let response = match cmd.to_uppercase().as_str() {
            "AUTH" => match handle_tcp_auth(&mut parts, &jwt_secret, peer) {
                AuthOutcome::Accepted { subject } => {
                    authenticated = true;
                    identity = format!("tcp:user:{subject}");
                    format!("OK: authenticated as {subject}\n")
                }
                AuthOutcome::Rejected { message } => {
                    auth_failures += 1;
                    if auth_failures >= MAX_TCP_AUTH_FAILURES {
                        tracing::warn!(
                            peer = %peer,
                            "dropping TCP session after \
                             {MAX_TCP_AUTH_FAILURES} failed AUTH attempts"
                        );
                        let _ = write_half.write_all(b"ERR TOO_MANY_FAILURES\n").await;
                        return;
                    }
                    message
                }
                AuthOutcome::Unconfigured => "ERR AUTH_NOT_CONFIGURED\n".to_string(),
            },
            "QUIT" | "EXIT" => {
                let _ = write_half.write_all(b"Goodbye.\n").await;
                return;
            }
            // Everything else requires an authenticated session.
            _ if !authenticated => "ERR AUTH_REQUIRED\n".to_string(),
            _ => {
                // Same per-identity limiter the QUIC path uses: one
                // authenticated client cannot starve the node.
                match rate_limiter.try_acquire(&identity) {
                    Ok(_) => dispatch_tcp_command(cmd, &mut parts, &db),
                    Err(retry_after_ms) => {
                        omni_engine::metrics_prometheus::record_rate_limit_rejection("tcp");
                        format!("ERR RATE_LIMITED retry_after_ms={retry_after_ms}\n")
                    }
                }
            }
        };

        if write_half.write_all(response.as_bytes()).await.is_err() {
            return;
        }
    }
}

/// What `AUTH` decided about a session.
enum AuthOutcome {
    /// The token verified; `subject` is the principal to rate-limit under.
    Accepted { subject: String },
    /// The token was missing or invalid; `message` is the reply to send.
    Rejected { message: String },
    /// No JWT secret is configured, so no token could ever verify.
    Unconfigured,
}

/// Verify one `AUTH <token>` line. The token carries no whitespace, so the
/// whole remainder of the line is it.
fn handle_tcp_auth(
    parts: &mut std::str::SplitN<'_, impl Fn(char) -> bool>,
    jwt_secret: &str,
    peer: std::net::SocketAddr,
) -> AuthOutcome {
    if jwt_secret.is_empty() {
        tracing::error!(
            peer = %peer,
            "TCP AUTH attempted but no JWT secret is configured"
        );
        return AuthOutcome::Unconfigured;
    }
    let token = parts.next().unwrap_or("").trim();
    if token.is_empty() {
        return AuthOutcome::Rejected {
            message: "ERR MISSING_AUTH_TOKEN\n".into(),
        };
    }
    match crate::auth::verify_token(token, jwt_secret) {
        Ok(claims) => {
            tracing::info!(
                peer = %peer,
                sub = %claims.sub,
                role = %claims.role,
                "TCP session authenticated"
            );
            AuthOutcome::Accepted {
                subject: claims.sub,
            }
        }
        Err(_) => {
            tracing::warn!(peer = %peer, "TCP JWT verification failed");
            AuthOutcome::Rejected {
                message: "ERR INVALID_TOKEN\n".into(),
            }
        }
    }
}

/// Run one authenticated data command against the database and return the
/// client-facing response line. Extracted from the connection loop so the
/// command surface (and its error sanitization) is unit-testable without
/// a socket.
fn dispatch_tcp_command(
    cmd: &str,
    parts: &mut std::str::SplitN<'_, impl Fn(char) -> bool>,
    db: &Arc<OmniKV>,
) -> String {
    use omni_engine::WriteBatch;

    match cmd {
        "GET" => {
            if let Some(key) = parts.next() {
                let seq = db.get_seq();
                match db.find(key, seq) {
                    Ok(Some(val)) => format!("OK: {val}\n"),
                    Ok(None) => "NOT_FOUND\n".to_string(),
                    Err(e) => tcp_err_response(&e),
                }
            } else {
                "ERROR: Missing key\n".to_string()
            }
        }
        "SET" => {
            if let (Some(key), Some(value)) = (parts.next(), parts.next()) {
                let mut batch = WriteBatch::new();
                match batch.set(key, value.to_string()) {
                    Ok(_) => match db.commit_batch(&batch) {
                        Ok(seq) => format!("OK: seq={seq}\n"),
                        Err(e) => tcp_err_response(&e),
                    },
                    Err(e) => tcp_err_response(&e),
                }
            } else {
                "ERROR: SET <key> <value>\n".to_string()
            }
        }
        "DELETE" => {
            if let Some(key) = parts.next() {
                let mut batch = WriteBatch::new();
                match batch.delete(key) {
                    Ok(_) => match db.commit_batch(&batch) {
                        Ok(seq) => format!("DELETED: seq={seq}\n"),
                        Err(e) => tcp_err_response(&e),
                    },
                    Err(e) => tcp_err_response(&e),
                }
            } else {
                "ERROR: Missing key\n".to_string()
            }
        }
        "SCAN" => {
            let start = parts.next().unwrap_or("");
            let end = parts.next().unwrap_or("\x7F");
            let seq = db.get_seq();
            match db.scan(start, end, seq) {
                Ok(results) => {
                    let mut out = format!("{} results:\n", results.len());
                    for (k, v) in results.iter().take(50) {
                        out.push_str(&format!("  {k} = {v}\n"));
                    }
                    out
                }
                Err(e) => tcp_err_response(&e),
            }
        }
        _ => "ERROR: Unknown command (AUTH, GET, SET, DELETE, SCAN, QUIT)\n".to_string(),
    }
}

/// Map a storage error to a safe client-facing TCP response line.
///
/// Client-caused errors get a specific, safe message; everything else is
/// logged server-side and reported as a generic `INTERNAL`. The previous
/// `{e:?}` formatting leaked engine internals (paths, lock names, batch
/// state) to anyone who could reach the port.
fn tcp_err_response(e: &omni_engine::OmniError) -> String {
    match e {
        omni_engine::OmniError::KeyNotFound => "ERROR: NOT_FOUND\n".into(),
        omni_engine::OmniError::BatchTooLarge(_) | omni_engine::OmniError::ValueTooLarge(_) => {
            "ERROR: REQUEST_TOO_LARGE\n".into()
        }
        omni_engine::OmniError::WriteStall => "ERROR: BUSY\n".into(),
        // Client-caused and safe to expose: in a cluster the caller hit a
        // follower, and it needs the leader's identity to retry there.
        // Answering INTERNAL for the most common clustered-write failure
        // would make the interface unusable for real clients.
        omni_engine::OmniError::NotLeader { leader_id } => match leader_id {
            Some(id) => format!("ERROR: NOT_LEADER the leader is node {id}\n"),
            None => "ERROR: NOT_LEADER no leader elected yet\n".into(),
        },
        _ => {
            tracing::error!(error = ?e, "TCP command internal error");
            "ERROR: INTERNAL\n".into()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A scratch database in a temp dir for one test.
    fn test_db() -> Arc<OmniKV> {
        let dir = tempfile::tempdir().expect("temp dir");
        let manifest = dir.path().join("manifest.json");
        let wal = dir.path().join("wal.bin");
        OmniKV::open(&manifest.to_string_lossy(), &wal.to_string_lossy())
            .expect("open test database")
    }

    fn run(db: &Arc<OmniKV>, line: &str) -> String {
        let mut parts = line.splitn(3, char::is_whitespace);
        let cmd = parts.next().unwrap_or("");
        dispatch_tcp_command(cmd, &mut parts, db)
    }

    #[test]
    fn tcp_dispatch_set_get_delete_round_trip() {
        let db = test_db();

        let set = run(&db, "SET k1 hello");
        assert!(set.starts_with("OK: seq="), "SET replied: {set}");

        assert_eq!(run(&db, "GET k1"), "OK: hello\n");
        assert_eq!(run(&db, "GET missing"), "NOT_FOUND\n");

        let del = run(&db, "DELETE k1");
        assert!(del.starts_with("DELETED: seq="), "DELETE replied: {del}");
        assert_eq!(run(&db, "GET k1"), "NOT_FOUND\n");
    }

    #[test]
    fn tcp_dispatch_rejects_missing_arguments() {
        let db = test_db();
        assert_eq!(run(&db, "GET"), "ERROR: Missing key\n");
        assert_eq!(run(&db, "SET only-a-key"), "ERROR: SET <key> <value>\n");
        assert_eq!(run(&db, "DELETE"), "ERROR: Missing key\n");
    }

    #[test]
    fn tcp_dispatch_unknown_command_lists_the_surface() {
        let db = test_db();
        let resp = run(&db, "DROP TABLE users");
        assert!(resp.starts_with("ERROR: Unknown command"), "got: {resp}");
        // The listing is what a legitimate operator needs to discover
        // AUTH; it must not hint at anything beyond the public surface.
        for cmd in ["AUTH", "GET", "SET", "DELETE", "SCAN", "QUIT"] {
            assert!(
                resp.contains(cmd),
                "unknown-command help omits {cmd}: {resp}"
            );
        }
    }

    #[test]
    fn tcp_scan_reports_the_count() {
        let db = test_db();
        run(&db, "SET scan:a 1");
        run(&db, "SET scan:b 2");
        let resp = run(&db, "SCAN scan:a scan:z");
        assert!(resp.starts_with("2 results:"), "got: {resp}");
        assert!(resp.contains("scan:a = 1") && resp.contains("scan:b = 2"));
    }

    #[test]
    fn tcp_error_sanitization_hides_engine_internals() {
        // Client-caused errors get a specific, safe message.
        assert_eq!(
            tcp_err_response(&omni_engine::OmniError::KeyNotFound),
            "ERROR: NOT_FOUND\n"
        );
        assert_eq!(
            tcp_err_response(&omni_engine::OmniError::ValueTooLarge(999)),
            "ERROR: REQUEST_TOO_LARGE\n"
        );
        assert_eq!(
            tcp_err_response(&omni_engine::OmniError::WriteStall),
            "ERROR: BUSY\n"
        );

        // A clustered write to a follower: the client needs the leader's
        // identity to retry there, so it is exposed rather than collapsed
        // to INTERNAL.
        assert_eq!(
            tcp_err_response(&omni_engine::OmniError::NotLeader { leader_id: Some(2) }),
            "ERROR: NOT_LEADER the leader is node 2\n"
        );
        assert_eq!(
            tcp_err_response(&omni_engine::OmniError::NotLeader { leader_id: None }),
            "ERROR: NOT_LEADER no leader elected yet\n"
        );

        // Everything else collapses to a generic INTERNAL. The old
        // `{e:?}` formatting handed paths and lock names to any client
        // who could reach the port (issue #117).
        let sensitive = omni_engine::OmniError::DatabaseAlreadyOpen {
            lock_path: "/var/lib/omnikv/.lock".into(),
        };
        let resp = tcp_err_response(&sensitive);
        assert_eq!(resp, "ERROR: INTERNAL\n");
        assert!(!resp.contains("/var/lib"), "leaked a path: {resp}");
        assert!(!resp.contains("lock"), "leaked a lock name: {resp}");
    }
}
