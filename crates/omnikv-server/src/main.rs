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

/// Failed AUTH attempts tolerated from one peer address within
/// [`TCP_AUTH_FAILURE_WINDOW`] before that address is refused outright.
/// The per-connection limit above is trivially reset by reconnecting, so
/// without a per-peer budget a grinding client gets five fresh guesses
/// per TCP handshake.
const MAX_TCP_PEER_AUTH_FAILURES: u32 = 25;

/// How long a peer's AUTH failures are counted before its budget resets.
const TCP_AUTH_FAILURE_WINDOW: std::time::Duration = std::time::Duration::from_secs(60);

/// Upper bound on simultaneous TCP sessions. Each one can buffer up to
/// [`MAX_TCP_LINE`] before it authenticates, so an unbounded accept loop
/// lets a farm of unauthenticated peers reserve a bounded-but-large
/// chunk of memory each. This caps the aggregate at a level that serves
/// every realistic debugging workload.
const MAX_TCP_SESSIONS: usize = 256;

/// How long a session may remain unauthenticated. Permits are bounded,
/// so a session holding one forever is a denial-of-service vector: 256
/// idle strangers would drain the pool and the accept loop would stop
/// serving real clients. AUTH is a single round trip for a legitimate
/// client, so this is generous for a machine and firm for a loiterer.
const TCP_AUTH_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);

/// How long an authenticated session may idle between reads. Deliberately
/// generous — the interface is a debugging tool, and an operator's
/// telnet session should not be cut for thinking — but still finite, so
/// a forgotten terminal cannot squat on a permit indefinitely. It resets
/// on any received bytes, so a slow-but-working client is never cut.
const TCP_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

/// How long a session may accumulate one incomplete command line, counting
/// from the first byte of that line. The idle timeout alone does not bound
/// this: a peer that dribbles one byte every few minutes is never idle by
/// that measure, yet never finishes a line, holding its permit for as long
/// as it keeps dribbling. This is the absolute backstop for that case —
/// generous enough for a large legitimate value over a slow link, firm
/// enough that a stalled permit comes back to the pool.
const TCP_LINE_DEADLINE: std::time::Duration = std::time::Duration::from_secs(600);

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

    // Bound concurrent sessions. Each one can buffer up to MAX_TCP_LINE
    // before authenticating, so without a ceiling the memory an
    // unauthenticated peer farm can reserve grows with the connection
    // count. Acquiring before spawn (rather than inside the task) makes a
    // saturated interface apply backpressure to the accept loop: a peer
    // that arrives when every permit is out waits, and is served when a
    // session ends, instead of adding to the pile.
    let session_limit = Arc::new(tokio::sync::Semaphore::new(MAX_TCP_SESSIONS));
    let auth_failures = Arc::new(TcpAuthFailures::new());

    loop {
        let (socket, peer) = listener.accept().await?;
        let permit = match session_limit.clone().acquire_owned().await {
            Ok(permit) => permit,
            // Only reachable at shutdown, when the semaphore is closed.
            Err(_) => break,
        };
        // Clone before the move closure: `db.clone()` inside an
        // `async move` captures `db` itself, and the accept loop would
        // move it on the first iteration.
        let db = db.clone();
        let jwt_secret = jwt_secret.clone();
        let rate_limiter = rate_limiter.clone();
        let auth_tracker = auth_failures.clone();
        tokio::spawn(async move {
            // Held for the session: released on drop, when the task ends.
            let _permit = permit;
            handle_tcp_connection(socket, peer, db, jwt_secret, rate_limiter, auth_tracker).await;
        });
    }
    Ok(())
}

/// Failed `AUTH` attempts by peer address, so a client cannot reset its
/// budget by reconnecting. Entries are counts over a rolling window (see
/// [`TCP_AUTH_FAILURE_WINDOW`]); an address that burns through
/// [`MAX_TCP_PEER_AUTH_FAILURES`] inside one is refused until the window
/// closes. Lock contention is negligible — the map is touched only on the
/// AUTH failure path, which the rate limiter already made the slow path.
struct TcpAuthFailures {
    by_peer:
        std::sync::Mutex<std::collections::HashMap<std::net::IpAddr, (u32, std::time::Instant)>>,
}

impl TcpAuthFailures {
    fn new() -> Self {
        Self {
            by_peer: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Whether this address is currently over its AUTH budget. Failure to
    /// take the lock fails open rather than dropping a legitimate client
    /// because bookkeeping is wedged.
    fn is_banned(&self, peer: &std::net::IpAddr) -> bool {
        let Ok(by_peer) = self.by_peer.lock() else {
            return false;
        };
        match by_peer.get(peer) {
            Some((count, first_at)) => {
                *count >= MAX_TCP_PEER_AUTH_FAILURES && first_at.elapsed() < TCP_AUTH_FAILURE_WINDOW
            }
            None => false,
        }
    }

    /// Record one failure and return the address's count within the
    /// current window. The window starts at the first failure and does not
    /// slide per attempt, so a slow grind over minutes does not evade it.
    fn record(&self, peer: std::net::IpAddr) -> u32 {
        let Ok(mut by_peer) = self.by_peer.lock() else {
            return 0;
        };
        // Prune when the map grows, so a rotating farm of source
        // addresses cannot make bookkeeping unbounded.
        if by_peer.len() > 4096 {
            by_peer.retain(|_, (_, first_at)| first_at.elapsed() < TCP_AUTH_FAILURE_WINDOW);
        }
        let entry = by_peer
            .entry(peer)
            .or_insert_with(|| (0, std::time::Instant::now()));
        if entry.1.elapsed() >= TCP_AUTH_FAILURE_WINDOW {
            *entry = (1, std::time::Instant::now());
            1
        } else {
            entry.0 += 1;
            entry.0
        }
    }

    /// A successful AUTH clears the address's budget: the client proved
    /// who it is, and stale failures from before should not follow a
    /// legitimate session around.
    fn clear(&self, peer: &std::net::IpAddr) {
        if let Ok(mut by_peer) = self.by_peer.lock() {
            by_peer.remove(peer);
        }
    }

    /// Test-only: force every peer's window closed, so the reset branch
    /// can be exercised without waiting out a real minute.
    #[cfg(test)]
    fn expire_all(&self) {
        if let Ok(mut by_peer) = self.by_peer.lock() {
            // checked_sub: a monotonic-clock skew can make now < window,
            // and panicking test bookkeeping for it is not worth it.
            let expired = std::time::Instant::now()
                .checked_sub(TCP_AUTH_FAILURE_WINDOW + std::time::Duration::from_secs(1))
                .unwrap_or_else(std::time::Instant::now);
            for (_, first_at) in by_peer.values_mut() {
                *first_at = expired;
            }
        }
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
    auth_tracker: Arc<TcpAuthFailures>,
) {
    use tokio::io::AsyncWriteExt;

    // Frame on newlines: a client may pipeline several commands in
    // one segment, and a single command may straddle a read
    // boundary. Buffering by line (instead of consuming one command
    // per 4096-byte read) keeps both cases intact.
    let (read_half, mut write_half) = socket.into_split();
    let mut reader = tokio::io::BufReader::new(read_half);

    // An address that already burned its AUTH budget does not get another
    // session — checking at entry is what makes the per-peer budget mean
    // something, since the per-connection counter resets on reconnect.
    if auth_tracker.is_banned(&peer.ip()) {
        tracing::warn!(peer = %peer, "refusing TCP session: peer is over its AUTH failure budget");
        let _ = write_half.write_all(b"ERR TOO_MANY_FAILURES\n").await;
        return;
    }

    // A connection is unauthenticated until a valid AUTH lands.
    // Every data command before then is refused, so an open port
    // never doubles as an open database.
    let mut authenticated = false;
    let mut identity = String::new();
    let mut session_role = String::new();
    let mut auth_failures = 0u32;

    // The session gets a bounded window to authenticate (a permit is
    // held the whole time, and the pool is finite). Once authenticated,
    // the much longer idle timeout governs. Both keep a parked peer from
    // squatting on a permit that a real client is waiting for.
    let auth_deadline = tokio::time::Instant::now() + TCP_AUTH_DEADLINE;

    // Holds partial input between reads: a client may pipeline several
    // commands in one segment, and a single command may straddle a read
    // boundary. Both cases need the leftover preserved, not discarded.
    let mut buffer = Vec::with_capacity(512);
    let mut chunk = [0u8; 8192];
    // How many leading bytes are already known to be newline-free, so a
    // scan covers only what the last read appended — linear, not
    // quadratic, as a single line grows toward the cap.
    let mut scanned = 0usize;
    // Two clocks, because they protect against different things:
    // - `idle_deadline` resets whenever any bytes arrive, so a slow but
    //   working client is never cut.
    // - `line_deadline` is an absolute backstop on accumulating one
    //   incomplete line, so a peer dribbling a byte every few minutes —
    //   active by the idle measure, but never finishing a line — cannot
    //   hold its permit indefinitely.
    let mut idle_deadline = tokio::time::Instant::now() + TCP_IDLE_TIMEOUT;
    let mut line_deadline = tokio::time::Instant::now() + TCP_LINE_DEADLINE;

    loop {
        // Read until a complete line is buffered. Three bounds apply, and
        // each protects something different: the length cap stops a
        // newline-free peer growing the buffer toward OOM (CWE-400), the
        // auth deadline bounds a loitering stranger, and the idle/line
        // clocks stop a parked or dribbling peer holding a permit forever
        // — the pool is finite, and 256 such sessions would drain it.
        // Unauthenticated, the auth deadline governs and the idle clock
        // does not: no partial command is worth waiting past AUTH for.
        let absolute = if authenticated {
            line_deadline
        } else {
            auth_deadline
        };
        match read_bounded_line(
            &mut reader,
            &mut buffer,
            &mut chunk,
            &mut scanned,
            &mut idle_deadline,
            absolute,
        )
        .await
        {
            LineRead::Ready => {}
            LineRead::Closed => return,
            LineRead::Timeout => {
                write_timeout_reply(&mut write_half, authenticated, peer).await;
                return;
            }
            LineRead::TooLong => {
                let _ = write_half.write_all(b"ERROR: LINE_TOO_LONG\n").await;
                return;
            }
        }

        // Peel exactly one line; the rest stays buffered for next pass.
        let Some(request) = peel_line(&mut buffer, &mut scanned, &mut line_deadline) else {
            // A completed line resets the idle clock too: the client just
            // proved it is present, even if the line was empty.
            idle_deadline = tokio::time::Instant::now() + TCP_IDLE_TIMEOUT;
            continue;
        };
        idle_deadline = tokio::time::Instant::now() + TCP_IDLE_TIMEOUT;

        let mut parts = request.splitn(3, char::is_whitespace);
        let cmd = parts.next().unwrap_or("");
        // Normalize once: the gate and the dispatch both match on the
        // uppercase form, so a client's casing cannot route around the
        // role check into a command that the raw form happens not to
        // require a role for.
        let cmd = cmd.to_uppercase();

        let response = match cmd.as_str() {
            "AUTH" => {
                match handle_tcp_auth_outcome(
                    &mut parts,
                    &jwt_secret,
                    peer,
                    &auth_tracker,
                    &mut auth_failures,
                    &mut write_half,
                )
                .await
                {
                    // The peer went over budget: the session is already
                    // closed and the reply already sent.
                    AuthResolution::Dropped => return,
                    AuthResolution::Reply(reply) => reply,
                    AuthResolution::Accepted {
                        subject,
                        role,
                        reply,
                    } => {
                        authenticated = true;
                        identity = format!("tcp:user:{subject}");
                        session_role = role;
                        reply
                    }
                }
            }
            "QUIT" | "EXIT" => {
                let _ = write_half.write_all(b"Goodbye.\n").await;
                return;
            }
            // Everything else requires an authenticated session.
            _ if !authenticated => "ERR AUTH_REQUIRED\n".to_string(),
            _ => run_authenticated_command(
                &cmd,
                &mut parts,
                &db,
                &identity,
                &session_role,
                &rate_limiter,
            ),
        };

        if write_half.write_all(response.as_bytes()).await.is_err() {
            return;
        }
    }
}

/// What an AUTH attempt resolved to: the session was dropped over its
/// failure budget, a reply was owed (accepted or a routine rejection), or
/// the token verified and the session is now authenticated.
enum AuthResolution {
    /// Over the failure budget: the session is closed and the reply
    /// already written, so the caller stops rather than sends another.
    Dropped,
    /// A reply to send, with the session's auth state unchanged — a
    /// routine rejection or an unconfigured secret.
    Reply(String),
    /// The token verified; the caller records the identity and role.
    Accepted {
        subject: String,
        role: String,
        reply: String,
    },
}

/// Handle one AUTH attempt and its bookkeeping: the failure counters, the
/// per-peer budget, and the reply. Only a verified token authenticates —
/// a rejection replies but leaves the session unauthenticated, so a
/// failed AUTH never grants access by accident.
async fn handle_tcp_auth_outcome(
    parts: &mut std::str::SplitN<'_, impl Fn(char) -> bool>,
    jwt_secret: &str,
    peer: std::net::SocketAddr,
    auth_tracker: &Arc<TcpAuthFailures>,
    auth_failures: &mut u32,
    write_half: &mut tokio::net::tcp::OwnedWriteHalf,
) -> AuthResolution {
    use tokio::io::AsyncWriteExt;
    match handle_tcp_auth(parts, jwt_secret, peer) {
        AuthOutcome::Accepted { subject, role } => {
            // The client proved who it is; forget any failures it
            // accumulated getting here.
            auth_tracker.clear(&peer.ip());
            AuthResolution::Accepted {
                reply: format!("OK: authenticated as {subject}\n"),
                subject,
                role,
            }
        }
        AuthOutcome::Rejected { message } => {
            *auth_failures += 1;
            // The per-connection limit drops a grinding session fast; the
            // per-peer limit is what stops it from reconnecting for five
            // more guesses.
            let peer_count = auth_tracker.record(peer.ip());
            if *auth_failures >= MAX_TCP_AUTH_FAILURES || peer_count >= MAX_TCP_PEER_AUTH_FAILURES {
                tracing::warn!(
                    peer = %peer,
                    connection_failures = *auth_failures,
                    peer_failures = peer_count,
                    "dropping TCP session after failed AUTH"
                );
                let _ = write_half.write_all(b"ERR TOO_MANY_FAILURES\n").await;
                return AuthResolution::Dropped;
            }
            AuthResolution::Reply(message)
        }
        AuthOutcome::Unconfigured => AuthResolution::Reply("ERR AUTH_NOT_CONFIGURED\n".to_string()),
    }
}

/// Why [`read_bounded_line`] stopped: a line is ready, the peer went away,
/// a deadline elapsed, or the peer tried to buffer past the line cap. The
/// caller owns the reply for the last two — it knows whether the session
/// had authenticated yet, which changes what is worth saying.
enum LineRead {
    Ready,
    Closed,
    Timeout,
    TooLong,
}

/// Reply to a session whose deadline elapsed, then the caller drops it.
/// Whether the auth deadline or the idle/line clock fired is a detail the
/// reply does not leak — either way the session is being cut for stalling,
/// and the peer's remedy is the same.
async fn write_timeout_reply(
    write_half: &mut tokio::net::tcp::OwnedWriteHalf,
    authenticated: bool,
    peer: std::net::SocketAddr,
) {
    use tokio::io::AsyncWriteExt;
    if authenticated {
        tracing::warn!(peer = %peer, "dropping TCP session: stalled past the idle or line deadline");
        let _ = write_half.write_all(b"ERR IDLE_TIMEOUT\n").await;
    } else {
        tracing::warn!(peer = %peer, "dropping TCP session: no AUTH within the deadline");
        let _ = write_half.write_all(b"ERR AUTH_TIMEOUT\n").await;
    }
}

/// Read into `buffer` until it holds a complete line, bounding memory and
/// time as it goes. `read_until` would buffer an entire newline-free line
/// before any check, letting an unauthenticated peer grow the buffer until
/// the process exhausted memory (CWE-400); a plain read loop, in turn,
/// would let a silent peer hold its permit forever. Both bounds are
/// enforced here, one read at a time.
///
/// Two clocks: `idle` resets whenever bytes arrive, so a slow but working
/// client is never cut; `line_deadline` is absolute, so a peer dribbling a
/// byte every few minutes without ever finishing a line cannot hold its
/// permit indefinitely. The effective wait each pass is the earlier one.
///
/// `scanned` is how many leading bytes of `buffer` are already known to be
/// newline-free, carried across calls: each byte is compared once instead of
/// re-scanning the whole buffer after every read, which keeps a maxed-out
/// 12 MiB line linear rather than quadratic in CPU.
async fn read_bounded_line(
    reader: &mut tokio::io::BufReader<tokio::net::tcp::OwnedReadHalf>,
    buffer: &mut Vec<u8>,
    chunk: &mut [u8],
    scanned: &mut usize,
    idle: &mut tokio::time::Instant,
    line_deadline: tokio::time::Instant,
) -> LineRead {
    use tokio::io::AsyncReadExt;

    loop {
        // Look only at what the last read appended.
        if buffer[*scanned..].contains(&b'\n') {
            return LineRead::Ready;
        }
        // Everything so far is now known to be newline-free. Only true
        // when no newline was found — a Ready return leaves the rest of
        // a pipelined batch unexamined for the next call.
        *scanned = buffer.len();
        let now = tokio::time::Instant::now();
        // Recomputed each pass, so the wait is bounded by whichever clock
        // fires first, no matter how many reads the line takes.
        let limit = std::cmp::min(*idle, line_deadline).saturating_duration_since(now);
        match tokio::time::timeout(limit, reader.read(chunk)).await {
            // EOF or a socket error: nothing more is coming.
            Ok(Ok(0) | Err(_)) => return LineRead::Closed,
            Ok(Ok(n)) => {
                buffer.extend_from_slice(&chunk[..n]);
                // Activity: this is a working client, however slow, and
                // the idle clock starts over. The absolute line deadline
                // does not, which is what makes the dribble case finite.
                *idle = tokio::time::Instant::now() + TCP_IDLE_TIMEOUT;
            }
            // The deadline elapsed. Which one is the caller's business.
            Err(_) => return LineRead::Timeout,
        }
        // The cap sits above max_value_size so a legitimate single-line
        // SET still fits; a peer past it is dropped, not humored.
        if buffer.len() > MAX_TCP_LINE {
            return LineRead::TooLong;
        }
    }
}

/// Peel exactly one complete line off `buffer`, trimmed. Returns `None`
/// for a blank line, which the caller skips: it is not a command, and it
/// gets no reply. Anything after the line's newline stays buffered for
/// the next pass, so pipelined commands are never lost.
///
/// The drain shifts every surviving byte to the front, so `scanned` (an
/// offset into `buffer`) restarts at zero; completing a line also restarts
/// the idle deadline, which is what stops a dribbling peer from holding a
/// permit across arbitrarily many reads.
fn peel_line(
    buffer: &mut Vec<u8>,
    scanned: &mut usize,
    line_deadline: &mut tokio::time::Instant,
) -> Option<String> {
    let newline = buffer.iter().position(|&b| b == b'\n')?;
    let line = buffer.drain(..=newline).collect::<Vec<u8>>();
    *scanned = 0;
    *line_deadline = tokio::time::Instant::now() + TCP_LINE_DEADLINE;
    let request = String::from_utf8_lossy(&line);
    let request = request.trim();
    if request.is_empty() {
        None
    } else {
        Some(request.to_string())
    }
}

/// Run one authenticated data command: the role gate first, then the
/// shared rate limiter, then dispatch. Extracted from the connection
/// loop so the authorization sequence has one definition and the loop
/// stays readable.
fn run_authenticated_command(
    cmd: &str,
    parts: &mut std::str::SplitN<'_, impl Fn(char) -> bool>,
    db: &Arc<OmniKV>,
    identity: &str,
    session_role: &str,
    rate_limiter: &Arc<RateLimiter>,
) -> String {
    // The same role model the REST middleware uses: the token's role has
    // to cover the command. Anything less and a `read` token could DELETE.
    match required_role_for(cmd) {
        Some(required) if !required.allows(session_role) => {
            tracing::warn!(
                role = session_role,
                required = required.as_str(),
                "TCP command refused: insufficient role"
            );
            "ERR FORBIDDEN insufficient role\n".to_string()
        }
        _ => {
            // Same per-identity limiter the QUIC path uses: one
            // authenticated client cannot starve the node.
            match rate_limiter.try_acquire(identity) {
                Ok(_) => dispatch_tcp_command(cmd, parts, db),
                Err(retry_after_ms) => {
                    omni_engine::metrics_prometheus::record_rate_limit_rejection("tcp");
                    format!("ERR RATE_LIMITED retry_after_ms={retry_after_ms}\n")
                }
            }
        }
    }
}

/// What `AUTH` decided about a session.
enum AuthOutcome {
    /// The token verified; `subject` is the principal to rate-limit under
    /// and `role` is what it is authorized to do.
    Accepted { subject: String, role: String },
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
                role: claims.role,
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

/// Which role a data command requires, mirroring the REST route guards:
/// `GET`/`SCAN` need read, `SET`/`DELETE` need write. `None` for commands
/// with no data-plane effect.
fn required_role_for(cmd: &str) -> Option<crate::auth::RequiredRole> {
    match cmd {
        "GET" | "SCAN" => Some(crate::auth::RequiredRole::Read),
        "SET" | "DELETE" => Some(crate::auth::RequiredRole::Write),
        _ => None,
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
    fn tcp_role_requirements_match_the_rest_guards() {
        // The token a client holds has to cover the command: a read token
        // must not mutate, and the mapping has to agree with the REST
        // route guards or the two protocols drift apart.
        use crate::auth::RequiredRole;

        assert_eq!(required_role_for("GET"), Some(RequiredRole::Read));
        assert_eq!(required_role_for("SCAN"), Some(RequiredRole::Read));
        assert_eq!(required_role_for("SET"), Some(RequiredRole::Write));
        assert_eq!(required_role_for("DELETE"), Some(RequiredRole::Write));

        // A read token reads, and may not write; admin covers everything.
        assert!(RequiredRole::Read.allows("read"));
        assert!(!RequiredRole::Write.allows("read"));
        assert!(RequiredRole::Write.allows("write"));
        assert!(RequiredRole::Write.allows("admin"));
        assert!(RequiredRole::Read.allows("admin"));
    }

    /// A session over a socket pair, for exercising the connection loop
    /// without spawning a listener: read replies as whole lines.
    struct Session {
        client: tokio::net::TcpStream,
        handle: tokio::task::JoinHandle<()>,
    }

    impl Session {
        /// Drive one connection: `lines` are written at once, as a client
        /// pipelining them in one segment would.
        async fn send(&mut self, lines: &str) -> Vec<String> {
            use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
            self.client.write_all(lines.as_bytes()).await.unwrap();
            let mut reader = tokio::io::BufReader::new(&mut self.client);
            let mut out = Vec::new();
            // One reply per non-empty request line sent.
            for _ in 0..lines.lines().filter(|l| !l.is_empty()).count() {
                let mut buf = String::new();
                if reader.read_line(&mut buf).await.unwrap() == 0 {
                    break;
                }
                let trimmed = buf.trim().to_string();
                if !trimmed.is_empty() {
                    out.push(trimmed);
                }
            }
            out
        }
    }

    /// The peer address a synthetic session reports. Real or not, it is
    /// only ever a logging key and the per-peer ban key, so a fixed one
    /// lets a test speak as the same client across "reconnections".
    const TEST_PEER: &str = "127.0.0.1:65530";

    fn test_limiter() -> Arc<RateLimiter> {
        Arc::new(RateLimiter::new(100.0, 10, 100))
    }

    /// A token signed with `secret`, carrying `role`.
    fn token_for(secret: &str, role: &str) -> String {
        crate::auth::generate_token("test", role, secret, 3600).expect("sign token")
    }

    /// One connection driven by the real handler, talking to a synthetic
    /// peer. The task ends when the client side drops.
    async fn session(db: Arc<OmniKV>, secret: &str, tracker: Arc<TcpAuthFailures>) -> Session {
        // A loopback socket pair. The peer the handler is told is
        // TEST_PEER regardless of the real ephemeral port: it is only a
        // logging and ban key, and a fixed one lets a test speak as the
        // same client across "reconnections".
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let secret = secret.to_string();
        let limiter = test_limiter();
        let handle = tokio::spawn(async move {
            handle_tcp_connection(
                server,
                TEST_PEER.parse().unwrap(),
                db,
                secret,
                limiter,
                tracker,
            )
            .await;
        });
        Session { client, handle }
    }

    #[tokio::test]
    async fn tcp_command_casing_cannot_route_around_the_role_gate() {
        // The gate and dispatch both match the uppercase command, so a
        // client's casing cannot dodge the role check: a read-scoped
        // token must be refused a write, whatever case it sends it in.
        let secret = "unit-test-secret-0123456789abcdef";
        let db = test_db();
        let mut s = session(db, secret, Arc::new(TcpAuthFailures::new())).await;

        let token = token_for(secret, "read");
        let replies = s
            .send(&format!(
                "AUTH {token}\nset lowercase-key v\nSET upper-key v\n"
            ))
            .await;

        assert_eq!(replies.len(), 3, "every command gets a reply: {replies:?}");
        assert!(
            replies[1].contains("FORBIDDEN"),
            "lowercase SET slipped past the role gate: {replies:?}"
        );
        assert!(
            replies[2].contains("FORBIDDEN"),
            "uppercase SET was allowed for a read token: {replies:?}"
        );
    }

    #[tokio::test]
    async fn tcp_peer_failure_budget_survives_reconnects() {
        // The per-connection counter resets on reconnect; the per-peer
        // budget is what makes grinding expensive. A client that fails
        // AUTH over and over from one address must eventually be refused
        // at the door, before it can spend another connection's guesses.
        let secret = "unit-test-secret-0123456789abcdef";
        let db = test_db();
        let tracker = Arc::new(TcpAuthFailures::new());
        let peer: std::net::IpAddr = "127.0.0.1".parse().unwrap();

        // Burn the per-connection budget repeatedly: the handler drops a
        // session at MAX_TCP_AUTH_FAILURES, so each round is a reconnect.
        let rounds = (MAX_TCP_PEER_AUTH_FAILURES / MAX_TCP_AUTH_FAILURES) as usize + 1;
        for _ in 0..rounds {
            let mut s = session(test_db(), secret, tracker.clone()).await;
            let bad = "AUTH not-a-valid-token\n".repeat(MAX_TCP_AUTH_FAILURES as usize);
            let replies = s.send(&bad).await;
            // The session is dropped once the budget is spent; whatever
            // replies arrived are all rejections.
            for r in &replies {
                assert!(
                    r.contains("INVALID_TOKEN") || r.contains("TOO_MANY_FAILURES"),
                    "unexpected reply: {r}"
                );
            }
        }
        assert!(
            tracker.is_banned(&peer),
            "peer should be over its AUTH budget"
        );

        // A fresh connection from the same address is refused at entry,
        // without consuming another guess.
        let mut s = session(db, secret, tracker.clone()).await;
        let replies = s.send("GET anything\n").await;
        assert!(
            replies.iter().any(|r| r.contains("TOO_MANY_FAILURES")),
            "banned peer was served: {replies:?}"
        );
    }

    #[tokio::test]
    async fn tcp_successful_auth_clears_the_peer_budget() {
        // A client that failed a few times then presents a valid token is
        // who it claims: its failures should not follow it around.
        let secret = "unit-test-secret-0123456789abcdef";
        let tracker = Arc::new(TcpAuthFailures::new());
        let peer: std::net::IpAddr = "127.0.0.1".parse().unwrap();

        assert_eq!(tracker.record(peer), 1);
        assert_eq!(tracker.record(peer), 2);
        tracker.clear(&peer);
        assert!(!tracker.is_banned(&peer));

        // The same is true through the wire: a valid AUTH forgets the
        // bad attempts that preceded it on the connection.
        let mut s = session(test_db(), secret, tracker).await;
        let token = token_for(secret, "admin");
        let replies = s.send(&format!("AUTH bad-token\nAUTH {token}\n")).await;
        assert_eq!(replies.len(), 2);
        assert!(replies[0].contains("INVALID_TOKEN"));
        assert!(
            replies[1].starts_with("OK: authenticated"),
            "valid AUTH after a failure was rejected: {replies:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn tcp_idle_unauthenticated_session_is_deadlined() {
        use tokio::io::AsyncReadExt;

        // A session that connects and says nothing still holds a permit,
        // and the pool is finite — without a deadline, 256 idle strangers
        // drain it and the accept loop stops serving anyone real. The
        // session has to be cut once its AUTH window elapses.
        // start_paused advances the mock clock while no task is runnable,
        // so the 10s deadline passes in milliseconds of wall clock.
        let secret = "unit-test-secret-0123456789abcdef";
        let mut s = session(test_db(), secret, Arc::new(TcpAuthFailures::new())).await;

        // Send nothing. Read until the handler gives up and closes.
        let mut buf = Vec::new();
        s.client.read_to_end(&mut buf).await.unwrap();
        let replied = String::from_utf8_lossy(&buf);
        assert!(
            replied.contains("AUTH_TIMEOUT"),
            "idle unauthenticated session was not deadlined: {replied:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn tcp_authenticated_session_dribbling_a_line_is_deadlined() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // The idle clock resets on any received bytes, so a peer who sends
        // one byte every few minutes is never idle by that measure — yet
        // never finishes a line, holding its permit for as long as it
        // cares to keep dribbling. The absolute line deadline is what
        // bounds that case, and it does not reset on activity.
        let secret = "unit-test-secret-0123456789abcdef";
        let db = test_db();
        let mut s = session(db, secret, Arc::new(TcpAuthFailures::new())).await;

        // Authenticate first, so the clock in play is the idle/line pair,
        // not the AUTH deadline.
        let token = token_for(secret, "write");
        s.send(&format!("AUTH {token}\n")).await;

        // Start a line and deliberately never finish it, then go quiet.
        // The write resets the idle clock, so only the absolute line
        // deadline can cut this session.
        s.client.write_all(b"SET k v").await.unwrap();

        // The mock clock advances while the runtime is idle, past the
        // line deadline, and the session is cut.
        let mut buf = Vec::new();
        s.client.read_to_end(&mut buf).await.unwrap();
        let replied = String::from_utf8_lossy(&buf);
        assert!(
            replied.contains("IDLE_TIMEOUT"),
            "dribbling authenticated session was not deadlined: {replied:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn tcp_authenticated_session_completing_lines_is_not_cut() {
        // The counterpart of the dribble test: a client that keeps
        // finishing lines keeps getting served. The paused clock
        // auto-advances whenever the runtime is idle, so this has to send
        // the whole batch in one segment — the moment the handler parks
        // between commands the mock clock would jump to the idle
        // deadline regardless of the reset, which makes an await-per-
        // command arrangement unable to tell "cut for stalling" from
        // "clock moved while parked". A single pipelined write keeps the
        // handler busy and lets every line complete.
        let secret = "unit-test-secret-0123456789abcdef";
        let db = test_db();
        let mut s = session(db, secret, Arc::new(TcpAuthFailures::new())).await;

        let token = token_for(secret, "write");
        s.send(&format!("AUTH {token}\n")).await;

        // More commands than one idle window would tolerate, all
        // completed: every one earns its reply, and the session survives
        // to serve the next.
        let batch: String = (0..8).fold(String::new(), |mut acc, i| {
            acc.push_str(&format!("SET k{i} v{i}\n"));
            acc
        });
        let replies = s.send(&batch).await;
        assert_eq!(
            replies.len(),
            8,
            "completing client lost replies: {replies:?}"
        );
        for r in &replies {
            assert!(
                r.starts_with("OK"),
                "completing client was errored: {replies:?}"
            );
        }

        // The session is still alive: a later command still gets served.
        let later = s.send("GET k0\n").await;
        assert_eq!(
            later,
            vec!["OK: v0"],
            "session died after completing lines: {later:?}"
        );
    }

    #[test]
    fn tcp_failure_window_resets_when_it_elapses() {
        // The budget is per window, not lifetime: a slow grind spread
        // across minutes does not accumulate toward a ban forever.
        let tracker = TcpAuthFailures::new();
        let peer: std::net::IpAddr = "127.0.0.1".parse().unwrap();

        for _ in 0..MAX_TCP_PEER_AUTH_FAILURES {
            tracker.record(peer);
        }
        assert!(
            tracker.is_banned(&peer),
            "peer should be banned within the window"
        );

        tracker.expire_all();
        assert!(!tracker.is_banned(&peer), "ban outlived the window");

        // The next failure starts a fresh window, not a continuation.
        assert_eq!(tracker.record(peer), 1);
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
