//! PostgreSQL Wire Protocol (v3) Implementation
//!
//! Enables any PostgreSQL client (psql, pgAdmin, JDBC, Python psycopg2, etc.)
//! to connect to OmniKV and execute queries.
//!
//! ## Protocol Flow
//!
//! ```text
//! Client                          OmniKV
//!   |--- StartupMessage ----------->|
//!   |<-- AuthenticationOk ----------|
//!   |<-- ReadyForQuery -------------|
//!   |--- Query (SQL) -------------->|
//!   |<-- RowDescription ------------|
//!   |<-- DataRow (per row) -------->|
//!   |<-- CommandComplete -----------|
//!   |<-- ReadyForQuery -------------|
//!   |--- Terminate ---------------->|
//! ```

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;

use crate::hardening::RateLimiter;
use crate::metrics_prometheus;
use crate::query;
use crate::transaction::{Transaction, TransactionManager, TxnState};
use crate::{OmniKV, WriteBatch};

/// Maximum allowed PGWire packet body size (4 MiB) to prevent DoS.
const MAX_PGWIRE_PACKET_BYTES: usize = 4 * 1024 * 1024;

/// Maximum number of rows a PgWire SELECT may emit by default.
///
/// PostgreSQL clients can otherwise issue an unbounded SELECT over a large key
/// range and force the server to allocate or stream a huge response. Explicit
/// LIMIT clauses above this cap are rejected with a protocol error.
const MAX_PGWIRE_RESULT_ROWS: usize = 10_000;

const DEFAULT_PGWIRE_RATE_LIMIT_PER_SEC: f64 = 1000.0;
const DEFAULT_PGWIRE_RATE_LIMIT_BURST: u32 = 100;
const DEFAULT_PGWIRE_RATE_LIMIT_MAX_USERS: usize = 10_000;

/// PostgreSQL wire protocol message types (server -> client)
const AUTH_OK: u8 = b'R';
const READY_FOR_QUERY: u8 = b'Z';
const ROW_DESCRIPTION: u8 = b'T';
const DATA_ROW: u8 = b'D';
const COMMAND_COMPLETE: u8 = b'C';
const ERROR_RESPONSE: u8 = b'E';
const PARAMETER_STATUS: u8 = b'S';

/// PostgreSQL wire protocol message types (client -> server)
const QUERY_MSG: u8 = b'Q';
const TERMINATE_MSG: u8 = b'X';

/// Per-connection transaction state for PgWire sessions.
/// Tracks whether the client is in an explicit transaction block.
struct ConnectionState {
    /// Active transaction, if any (set by BEGIN, cleared by COMMIT/ROLLBACK).
    txn: Option<Transaction>,
    /// Set to true when an error occurs inside a transaction block.
    /// All subsequent commands (except ROLLBACK) return error until
    /// the client issues ROLLBACK.
    txn_failed: bool,
}

impl ConnectionState {
    fn new() -> Self {
        Self {
            txn: None,
            txn_failed: false,
        }
    }

    /// Returns the ReadyForQuery status byte:
    /// 'I' = idle (no transaction)
    /// 'T' = in a transaction block
    /// 'E' = in a failed transaction block
    fn ready_status(&self) -> u8 {
        if self.txn_failed {
            b'E'
        } else if self.txn.is_some() {
            b'T'
        } else {
            b'I'
        }
    }
}

/// Cleartext-auth exposure policy for the PgWire listener.
///
/// Until the PgWire listener gains TLS (tracked separately), authentication is
/// cleartext-password and must not be offered on externally reachable
/// addresses. Production defaults fail closed: cleartext auth is only served
/// on loopback or private (RFC 1918 / ULA) binds. Development mode allows any
/// bind for local experiments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgWireSecurityPolicy {
    /// Serve cleartext auth on any bind address (development default).
    AllowCleartextAnywhere,
    /// Serve cleartext auth only on loopback or private-network binds
    /// (production default). Other binds fail closed at startup.
    RequirePrivateBind,
}

/// Returns true when a bind address is safe for cleartext authentication:
/// loopback (any family) or a private/unique-local network address.
fn is_cleartext_safe_bind(bind_addr: &str) -> bool {
    let Ok(addr) = bind_addr.parse::<std::net::SocketAddr>() else {
        // Unparseable bind (e.g. a host name to be resolved later) cannot be
        // proven private; treat as unsafe.
        return false;
    };
    match addr.ip() {
        std::net::IpAddr::V4(ip) => {
            ip.is_loopback()
                || ip.is_private() // RFC 1918: 10/8, 172.16/12, 192.168/16
                || ip.is_link_local() // 169.254/16
        }
        std::net::IpAddr::V6(ip) => {
            ip.is_loopback() || (ip.segments()[0] & 0xfe00) == 0xfc00 // fc00::/7 ULA
        }
    }
}

/// Represents a PostgreSQL wire protocol server with connection pooling.
pub struct PgWireServer {
    db: Arc<OmniKV>,
    bind_addr: String,
    max_connections: usize,
    rate_limiter: Arc<RateLimiter>,
    /// Cleartext password required from PgWire clients. Read from
    /// OMNI_PGWIRE_PASSWORD at construction time so tests can inject a
    /// deterministic value without mutating process-global environment.
    pgwire_password: String,
    /// Whether cleartext auth may be served on non-private binds.
    security_policy: PgWireSecurityPolicy,
}

/// Reads the PgWire cleartext password from OMNI_PGWIRE_PASSWORD.
fn pgwire_password_from_env() -> String {
    std::env::var("OMNI_PGWIRE_PASSWORD").unwrap_or_default()
}

/// Production mode is env-driven because the engine crate does not own the
/// server configuration pipeline; the server binary constructs the policy.
fn default_security_policy() -> PgWireSecurityPolicy {
    match std::env::var("OMNIKV_MODE").as_deref() {
        Ok("production" | "prod") => PgWireSecurityPolicy::RequirePrivateBind,
        _ => PgWireSecurityPolicy::AllowCleartextAnywhere,
    }
}

impl PgWireServer {
    pub fn new(db: Arc<OmniKV>, bind_addr: &str) -> Self {
        Self {
            db,
            bind_addr: bind_addr.to_string(),
            max_connections: 32,
            rate_limiter: default_pgwire_rate_limiter(),
            pgwire_password: pgwire_password_from_env(),
            security_policy: default_security_policy(),
        }
    }

    /// Creates a PgWireServer with an explicit cleartext password, for callers
    /// and tests that manage the credential outside the process environment.
    pub fn with_password(db: Arc<OmniKV>, bind_addr: &str, pgwire_password: &str) -> Self {
        Self {
            db,
            bind_addr: bind_addr.to_string(),
            max_connections: 32,
            rate_limiter: default_pgwire_rate_limiter(),
            pgwire_password: pgwire_password.to_string(),
            security_policy: default_security_policy(),
        }
    }

    /// Creates a PgWireServer with an explicit cleartext-auth exposure policy,
    /// overriding the environment-derived default.
    pub fn with_security_policy(
        db: Arc<OmniKV>,
        bind_addr: &str,
        pgwire_password: &str,
        security_policy: PgWireSecurityPolicy,
    ) -> Self {
        Self {
            db,
            bind_addr: bind_addr.to_string(),
            max_connections: 32,
            rate_limiter: default_pgwire_rate_limiter(),
            pgwire_password: pgwire_password.to_string(),
            security_policy,
        }
    }

    /// Creates a PgWireServer with a custom connection pool size.
    pub fn with_pool_size(db: Arc<OmniKV>, bind_addr: &str, max_connections: usize) -> Self {
        Self {
            db,
            bind_addr: bind_addr.to_string(),
            max_connections,
            rate_limiter: default_pgwire_rate_limiter(),
            pgwire_password: pgwire_password_from_env(),
            security_policy: default_security_policy(),
        }
    }

    /// Creates a PgWireServer with caller-supplied rate limiter state.
    pub fn with_rate_limiter(
        db: Arc<OmniKV>,
        bind_addr: &str,
        rate_limiter: Arc<RateLimiter>,
    ) -> Self {
        Self {
            db,
            bind_addr: bind_addr.to_string(),
            max_connections: 32,
            rate_limiter,
            pgwire_password: pgwire_password_from_env(),
            security_policy: default_security_policy(),
        }
    }

    /// Returns the configured max connections.
    pub fn max_connections(&self) -> usize {
        self.max_connections
    }

    /// Returns the active cleartext-auth exposure policy.
    pub fn security_policy(&self) -> PgWireSecurityPolicy {
        self.security_policy
    }

    /// Starts the PostgreSQL wire protocol server with connection pooling.
    /// Uses a bounded thread pool to prevent resource exhaustion.
    pub fn start(&self) -> std::io::Result<()> {
        self.validate_security_policy()?;
        let listener = TcpListener::bind(&self.bind_addr)?;
        eprintln!(
            "[OmniKV] PostgreSQL wire protocol listening on {} (pool: {} max connections)",
            self.bind_addr, self.max_connections
        );
        self.serve(listener)
    }

    /// Checks the cleartext-auth exposure policy against the configured bind
    /// without binding or entering the accept loop, so callers can validate
    /// configuration before startup and tests can exercise the policy.
    pub fn validate_security_policy(&self) -> std::io::Result<()> {
        if self.security_policy == PgWireSecurityPolicy::RequirePrivateBind
            && !is_cleartext_safe_bind(&self.bind_addr)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!(
                    "PgWire cleartext authentication refused on non-private bind {}; \
                     bind loopback/private or wait for PgWire TLS support",
                    self.bind_addr
                ),
            ));
        }
        Ok(())
    }

    /// Accept loop over an already-bound listener.
    ///
    /// Split from [`Self::start`] so tests can bind to an OS-assigned port
    /// (`127.0.0.1:0`), learn it from [`TcpListener::local_addr`], and drive
    /// real connections against the same accept path production uses.
    pub fn serve(&self, listener: TcpListener) -> std::io::Result<()> {
        // Bounded connection semaphore using a channel
        let (permit_tx, permit_rx) = std::sync::mpsc::sync_channel::<()>(self.max_connections);
        // Pre-fill permits
        for _ in 0..self.max_connections {
            let _ = permit_tx.send(());
        }

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    // Wait for a permit (blocks if pool is full)
                    let permit = permit_rx.recv();
                    if permit.is_err() {
                        break;
                    }
                    let db = self.db.clone();
                    let rate_limiter = self.rate_limiter.clone();
                    let pgwire_password = self.pgwire_password.clone();
                    let release_tx = permit_tx.clone();
                    std::thread::spawn(move || {
                        if let Err(e) =
                            handle_connection(db, stream, rate_limiter, &pgwire_password)
                        {
                            eprintln!("[OmniKV] Connection error: {}", e);
                        }
                        // Release permit back to pool
                        let _ = release_tx.send(());
                    });
                }
                Err(e) => eprintln!("[OmniKV] Accept error: {}", e),
            }
        }
        Ok(())
    }
}

fn default_pgwire_rate_limiter() -> Arc<RateLimiter> {
    Arc::new(RateLimiter::new(
        DEFAULT_PGWIRE_RATE_LIMIT_PER_SEC,
        DEFAULT_PGWIRE_RATE_LIMIT_BURST,
        DEFAULT_PGWIRE_RATE_LIMIT_MAX_USERS,
    ))
}

/// Handle a single PostgreSQL client connection.
fn handle_connection(
    db: Arc<OmniKV>,
    mut stream: std::net::TcpStream,
    rate_limiter: Arc<RateLimiter>,
    pgwire_password: &str,
) -> std::io::Result<()> {
    // Phase 1: Startup handshake
    if pgwire_password.is_empty() {
        tracing::error!("PgWire password is empty — PGWire connections will be rejected");
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "OMNI_PGWIRE_PASSWORD not configured",
        ));
    }
    handle_startup(&mut stream, pgwire_password)?;

    // Per-connection state: transaction manager and session state
    let tm = Arc::new(TransactionManager::new(db.clone()));
    let mut conn = ConnectionState::new();
    let client_id = stream
        .peer_addr()
        .map(|addr| format!("pgwire:ip:{}", addr.ip()))
        .unwrap_or_else(|_| "pgwire:unknown".to_string());

    // Phase 2: Query loop
    loop {
        let mut msg_type = [0u8; 1];
        if stream.read_exact(&mut msg_type).is_err() {
            // Client disconnected — clean up any open transaction
            if let Some(txn) = conn.txn.take() {
                // Implicit rollback: unregister the snapshot
                db.unregister_snapshot(txn.read_seq);
            }
            break;
        }

        match msg_type[0] {
            QUERY_MSG => {
                let sql = read_query_message(&mut stream)?;
                if let Err(retry_after_ms) = acquire_pgwire_query_permit(&rate_limiter, &client_id)
                {
                    send_error(
                        &mut stream,
                        "ERROR",
                        "53300",
                        &format!("rate limit exceeded; retry after {retry_after_ms}ms"),
                    )?;
                    send_ready_for_query_status(&mut stream, conn.ready_status())?;
                    continue;
                }
                handle_query(&db, &tm, &mut conn, &mut stream, &sql)?;
            }
            TERMINATE_MSG => {
                let _ = read_message_body(&mut stream)?;
                // Clean up any open transaction
                if let Some(txn) = conn.txn.take() {
                    db.unregister_snapshot(txn.read_seq);
                }
                break;
            }
            _ => {
                let _ = read_message_body(&mut stream)?;
                send_error(&mut stream, "ERROR", "XX000", "Unsupported message type")?;
                send_ready_for_query_status(&mut stream, conn.ready_status())?;
            }
        }
    }
    Ok(())
}

fn acquire_pgwire_query_permit(rate_limiter: &RateLimiter, client_id: &str) -> Result<(), u64> {
    rate_limiter
        .try_acquire(client_id)
        .map(|_| ())
        .inspect_err(|_| {
            metrics_prometheus::record_rate_limit_rejection("pgwire");
        })
}

/// PostgreSQL wire protocol negotiation codes sent as the first Int32 after
/// the 4-byte length prefix.
/// 196608 (0x00030000) is protocol version 3.0 (a StartupMessage).
/// 80877103 (0x04D2162F) is the SSLRequest negotiation packet.
/// 80877104 (0x04D21630) is the GSSENCRequest negotiation packet.
/// 80877102 (0x04D2162E) is the CancelRequest packet (16 bytes total).
const PROTOCOL_VERSION_3_0: u32 = 196_608;
const SSL_REQUEST_CODE: u32 = 80_877_103;
const GSS_ENC_REQUEST_CODE: u32 = 80_877_104;
const CANCEL_REQUEST_CODE: u32 = 80_877_102;

/// Maximum number of negotiation packets (SSLRequest / GSSENCRequest) accepted
/// before the StartupMessage. PostgreSQL treats repeated negotiation as a
/// protocol violation; bounding it here prevents a trivial pre-auth spin loop.
/// The bound applies only to negotiation packets — the StartupMessage itself
/// is always readable after any number of negotiations within the bound.
const MAX_STARTUP_NEGOTIATION_MESSAGES: usize = 8;

/// Handle the startup handshake with password authentication.
///
/// Reads the startup message, sends AuthenticationCleartextPassword,
/// reads the PasswordMessage, validates against OMNI_PGWIRE_PASSWORD,
/// and only sends AuthenticationOk on success.
///
/// Clients running libpq defaults (psql, JDBC, psycopg2, pg8000, node-postgres)
/// send an SSLRequest before the StartupMessage. Per the PostgreSQL protocol,
/// the reply is a single byte: 'S' to upgrade to TLS, 'N' to stay on the
/// current (plaintext) connection. OmniKV has no PgWire TLS yet, so the reply
/// is always 'N', and the handshake then continues with the real
/// StartupMessage on the same connection.
fn handle_startup(
    stream: &mut std::net::TcpStream,
    expected_password: &str,
) -> std::io::Result<()> {
    let mut negotiation_packets = 0usize;
    loop {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf)?;
        let len = u32::from_be_bytes(len_buf) as usize;
        let body_len = validate_pgwire_body_len(len, "startup message")?;
        if body_len < 4 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "startup message too short for a protocol version",
            ));
        }
        let mut code_buf = [0u8; 4];
        stream.read_exact(&mut code_buf)?;
        let protocol_code = u32::from_be_bytes(code_buf);

        match protocol_code {
            SSL_REQUEST_CODE | GSS_ENC_REQUEST_CODE => {
                negotiation_packets += 1;
                if negotiation_packets > MAX_STARTUP_NEGOTIATION_MESSAGES {
                    send_error_response(
                        stream,
                        "08P01",
                        "too many negotiation requests before startup",
                    )?;
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "too many negotiation requests before startup",
                    ));
                }
                // Single-byte 'N': no TLS or GSS upgrade on this listener.
                // The next message from the client is the real StartupMessage.
                stream.write_all(b"N")?;
            }
            CANCEL_REQUEST_CODE => {
                // A dedicated connection carrying a query-cancel request
                // (process ID + secret key follow the code). OmniKV does not
                // issue backend keys, so a cancel can never match; drain the
                // frame and close the connection, as PostgreSQL does for an
                // unknown backend key.
                let mut rest = vec![0u8; body_len - 4];
                stream.read_exact(&mut rest)?;
                return Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionAborted,
                    "cancel request for unknown backend key",
                ));
            }
            PROTOCOL_VERSION_3_0 => {
                // Real StartupMessage: the protocol code is consumed, drain
                // the remaining key/value parameters to stay framing-aligned.
                let mut params = vec![0u8; body_len - 4];
                stream.read_exact(&mut params)?;
                break;
            }
            _ => {
                send_error_response(
                    stream,
                    "08P01",
                    &format!("unsupported protocol version {protocol_code}"),
                )?;
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "unsupported startup protocol version",
                ));
            }
        }
    }

    // Request cleartext password
    // AuthenticationCleartextPassword: 'R' + int32(8) + int32(3)
    let mut auth_req = Vec::with_capacity(9);
    auth_req.push(b'R');
    auth_req.extend_from_slice(&8u32.to_be_bytes());
    auth_req.extend_from_slice(&3u32.to_be_bytes());
    stream.write_all(&auth_req)?;

    // Read PasswordMessage: 'p' + int32(len) + password + '\0'
    let mut msg_type = [0u8; 1];
    stream.read_exact(&mut msg_type)?;
    if msg_type[0] != b'p' {
        send_error_response(stream, "28P01", "password message expected")?;
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "password message expected",
        ));
    }
    let mut plen_buf = [0u8; 4];
    stream.read_exact(&mut plen_buf)?;
    let plen = u32::from_be_bytes(plen_buf) as usize;
    let password_len = validate_pgwire_body_len(plen, "password message")?;
    let mut pw_buf = vec![0u8; password_len];
    stream.read_exact(&mut pw_buf)?;
    // Password is null-terminated
    let supplied = pw_buf.split(|&b| b == 0).next().unwrap_or(&[]);
    let supplied = std::str::from_utf8(supplied).unwrap_or("");

    if supplied != expected_password {
        tracing::warn!("PGWire authentication failed — bad password");
        send_error_response(stream, "28P01", "password authentication failed")?;
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "password authentication failed",
        ));
    }

    // AuthenticationOk: 'R' + int32(8) + int32(0)
    let mut ok = Vec::with_capacity(9);
    ok.push(b'R');
    ok.extend_from_slice(&8u32.to_be_bytes());
    ok.extend_from_slice(&0u32.to_be_bytes());
    stream.write_all(&ok)?;

    send_parameter_status(stream, "server_version", "15.0 (OmniKV)")?;
    send_parameter_status(stream, "server_encoding", "UTF8")?;
    send_parameter_status(stream, "client_encoding", "UTF8")?;
    send_parameter_status(stream, "DateStyle", "ISO, MDY")?;
    send_parameter_status(stream, "integer_datetimes", "on")?;
    send_ready_for_query_status(stream, b'I')?;
    Ok(())
}

/// Send an ErrorResponse message to the client.
fn send_error_response(
    stream: &mut std::net::TcpStream,
    code: &str,
    message: &str,
) -> std::io::Result<()> {
    // 'E' + int32(len) + 'S' + "ERROR\0" + 'C' + code + '\0' + 'M' + message + '\0' + '\0'
    let mut payload = Vec::new();
    payload.push(b'S');
    payload.extend_from_slice(b"ERROR\0");
    payload.push(b'C');
    payload.extend_from_slice(code.as_bytes());
    payload.push(0);
    payload.push(b'M');
    payload.extend_from_slice(message.as_bytes());
    payload.push(0);
    payload.push(0); // terminator
    let total_len = (payload.len() + 4) as u32;
    let mut msg = Vec::with_capacity(1 + 4 + payload.len());
    msg.push(b'E');
    msg.extend_from_slice(&total_len.to_be_bytes());
    msg.extend_from_slice(&payload);
    stream.write_all(&msg)
}

/// Read a Query ('Q') message body.
fn read_query_message(stream: &mut std::net::TcpStream) -> std::io::Result<String> {
    let body = read_message_body(stream)?;
    let end = body.iter().position(|&b| b == 0).unwrap_or(body.len());
    Ok(String::from_utf8_lossy(&body[..end]).to_string())
}

/// Read a message body (length-prefixed).
fn read_message_body(stream: &mut std::net::TcpStream) -> std::io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let body_len = validate_pgwire_body_len(len, "message")?;
    let mut body = vec![0u8; body_len];
    stream.read_exact(&mut body)?;
    Ok(body)
}

fn validate_pgwire_body_len(len: usize, context: &str) -> std::io::Result<usize> {
    if len < 4 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{context} too short"),
        ));
    }

    let body_len = len - 4;
    if body_len > MAX_PGWIRE_PACKET_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{context} packet too large"),
        ));
    }

    Ok(body_len)
}

/// Handle a SQL query and send results back.
fn handle_query(
    db: &Arc<OmniKV>,
    tm: &Arc<TransactionManager>,
    conn: &mut ConnectionState,
    stream: &mut std::net::TcpStream,
    sql: &str,
) -> std::io::Result<()> {
    let sql_trimmed = sql.trim().trim_end_matches(';');

    if sql_trimmed.is_empty() {
        send_command_complete(stream, "EMPTY")?;
        send_ready_for_query_status(stream, conn.ready_status())?;
        return Ok(());
    }

    // Uppercased whitespace-normalized form for multi-word transaction
    // statements: DBAPI drivers (psycopg2, pg8000) send lowercase
    // `begin transaction`, `commit`, `rollback work`, etc.
    let normalized = sql_trimmed
        .to_uppercase()
        .split_whitespace()
        .collect::<Vec<&str>>()
        .join(" ");

    // ── SET — always accept silently ──
    if normalized == "SET" || normalized.starts_with("SET ") {
        send_command_complete(stream, "SET")?;
        send_ready_for_query_status(stream, conn.ready_status())?;
        return Ok(());
    }

    // ── BEGIN — start an explicit transaction block ──
    // PostgreSQL accepts BEGIN [WORK|TRANSACTION] and START TRANSACTION.
    if normalized == "BEGIN"
        || normalized == "BEGIN WORK"
        || normalized == "BEGIN TRANSACTION"
        || normalized == "START TRANSACTION"
    {
        if conn.txn.is_some() {
            // Already in a transaction — PostgreSQL sends a WARNING but doesn't fail
            send_error(
                stream,
                "WARNING",
                "25001",
                "there is already a transaction in progress",
            )?;
        } else {
            let txn = tm.begin();
            conn.txn = Some(txn);
            conn.txn_failed = false;
        }
        send_command_complete(stream, "BEGIN")?;
        send_ready_for_query_status(stream, conn.ready_status())?;
        return Ok(());
    }

    // ── COMMIT — commit the current transaction ──
    // PostgreSQL accepts COMMIT [WORK] and END [WORK].
    if normalized == "COMMIT"
        || normalized == "COMMIT WORK"
        || normalized == "END"
        || normalized == "END WORK"
    {
        if let Some(mut txn) = conn.txn.take() {
            if conn.txn_failed {
                // Failed transaction — COMMIT acts as ROLLBACK
                db.unregister_snapshot(txn.read_seq);
                conn.txn_failed = false;
                send_command_complete(stream, "ROLLBACK")?;
            } else {
                match tm.commit(&mut txn) {
                    Ok(_) => {
                        send_command_complete(stream, "COMMIT")?;
                    }
                    Err(e) => {
                        send_error(stream, "ERROR", "40001", &format!("COMMIT failed: {}", e))?;
                    }
                }
            }
        } else {
            // No transaction — PostgreSQL sends a WARNING
            send_error(
                stream,
                "WARNING",
                "25P01",
                "there is no transaction in progress",
            )?;
            send_command_complete(stream, "COMMIT")?;
        }
        send_ready_for_query_status(stream, conn.ready_status())?;
        return Ok(());
    }

    // ── ROLLBACK — abort the current transaction ──
    // PostgreSQL accepts ROLLBACK [WORK] and ABORT [WORK].
    if normalized == "ROLLBACK"
        || normalized == "ROLLBACK WORK"
        || normalized == "ABORT"
        || normalized == "ABORT WORK"
    {
        if let Some(txn) = conn.txn.take() {
            db.unregister_snapshot(txn.read_seq);
            conn.txn_failed = false;
            send_command_complete(stream, "ROLLBACK")?;
        } else {
            send_error(
                stream,
                "WARNING",
                "25P01",
                "there is no transaction in progress",
            )?;
            send_command_complete(stream, "ROLLBACK")?;
        }
        send_ready_for_query_status(stream, conn.ready_status())?;
        return Ok(());
    }

    // ── If in a failed transaction, reject all commands until ROLLBACK ──
    if conn.txn_failed {
        send_error(
            stream,
            "ERROR",
            "25P02",
            "current transaction is aborted, commands ignored until end of transaction block",
        )?;
        send_ready_for_query_status(stream, conn.ready_status())?;
        return Ok(());
    }

    // ── SELECT 1 / SELECT VERSION — compatibility shortcuts ──
    // #110 tracks real literal-SELECT support in the parser; until then these
    // health-check shortcuts answer any case and spacing combination.
    if normalized == "SELECT 1" || normalized.starts_with("SELECT VERSION") {
        send_row_description(stream, &[("version", 25)])?;
        send_data_row(stream, &["OmniKV 0.1.0 — Distributed KV Engine"])?;
        send_command_complete(stream, "SELECT 1")?;
        send_ready_for_query_status(stream, conn.ready_status())?;
        return Ok(());
    }

    // ── Execute SQL (with transaction context if inside BEGIN block) ──
    match crate::sql::parse_sql(sql_trimmed) {
        Ok(stmt) => {
            let stmt = match enforce_pgwire_statement_limits(stmt) {
                Ok(stmt) => stmt,
                Err(msg) => {
                    if conn.txn.is_some() {
                        conn.txn_failed = true;
                    }
                    send_error(stream, "ERROR", "54000", &msg)?;
                    send_ready_for_query_status(stream, conn.ready_status())?;
                    return Ok(());
                }
            };
            let catalog = std::sync::Arc::new(crate::catalog::Catalog::new(db.clone()));

            // If inside an explicit transaction, use the transaction's read_seq
            // for snapshot isolation. Otherwise use autocommit (current seq).
            let executor = if let Some(ref mut txn) = conn.txn {
                crate::sql_exec::SqlExecutor::with_snapshot(db.clone(), catalog, txn.read_seq)
            } else {
                crate::sql_exec::SqlExecutor::new(db.clone(), catalog)
            };

            match executor.execute(&stmt) {
                Ok(crate::sql_exec::ExecResult::Rows { columns, rows }) => {
                    let col_defs: Vec<(&str, i32)> =
                        columns.iter().map(|c| (c.as_str(), 25i32)).collect();
                    send_row_description(stream, &col_defs)?;
                    for row in &rows {
                        let refs: Vec<&str> = row.iter().map(|s| s.as_str()).collect();
                        send_data_row(stream, &refs)?;
                    }
                    send_command_complete(stream, &format!("SELECT {}", rows.len()))?;
                }
                Ok(crate::sql_exec::ExecResult::Modified { count, command }) => {
                    // Track writes in the transaction if inside BEGIN block
                    if let Some(ref mut txn) = conn.txn {
                        // For DML inside a transaction, buffer through the txn manager.
                        // Note: full write buffering requires deeper SqlExecutor integration.
                        // For now, we track that the transaction has performed writes.
                        let _ = count; // Writes are committed directly for now
                    }
                    send_command_complete(stream, &command)?;
                }
                Ok(crate::sql_exec::ExecResult::Ok(msg)) => {
                    send_command_complete(stream, &msg)?;
                }
                Err(e) => {
                    if conn.txn.is_some() {
                        conn.txn_failed = true;
                    }
                    send_error(stream, "ERROR", "XX000", &format!("Exec error: {}", e))?;
                }
            }
        }
        Err(_) => {
            // Fall back to legacy KV query parser
            match query::parse_query(sql_trimmed) {
                Ok(parsed) => {
                    execute_query(db, stream, &parsed)?;
                }
                Err(e) => {
                    if conn.txn.is_some() {
                        conn.txn_failed = true;
                    }
                    send_error(stream, "ERROR", "42601", &format!("Parse error: {}", e))?;
                }
            }
        }
    }

    send_ready_for_query_status(stream, conn.ready_status())?;
    Ok(())
}

/// Execute a parsed query and stream results.
fn execute_query(
    db: &Arc<OmniKV>,
    stream: &mut std::net::TcpStream,
    parsed: &query::Query,
) -> std::io::Result<()> {
    let seq = db.get_seq();

    match &parsed.action {
        query::Action::SelectAll => {
            // Build scan range from conditions
            let (start_key, end_key) = build_scan_range(&parsed.conditions);

            let results = db.scan(&start_key, &end_key, seq).unwrap_or_default();

            send_row_description(stream, &[("key", 25), ("value", 25)])?;

            let limit = match bounded_pgwire_query_limit(parsed.limit) {
                Ok(limit) => limit,
                Err(msg) => {
                    send_error(stream, "ERROR", "54000", &msg)?;
                    return Ok(());
                }
            };
            let mut count = 0;

            let iter: Box<dyn Iterator<Item = &(String, String)>> = if parsed.order_desc {
                Box::new(results.iter().rev())
            } else {
                Box::new(results.iter())
            };

            for (key, value) in iter {
                if count >= limit {
                    break;
                }
                send_data_row(stream, &[key, value])?;
                count += 1;
            }

            send_command_complete(stream, &format!("SELECT {}", count))?;
        }

        query::Action::SelectCount => {
            let (start_key, end_key) = build_scan_range(&parsed.conditions);
            let results = db.scan(&start_key, &end_key, seq).unwrap_or_default();

            send_row_description(stream, &[("count", 20)])?;
            send_data_row(stream, &[&results.len().to_string()])?;
            send_command_complete(stream, "SELECT 1")?;
        }

        query::Action::Insert(key, value) => {
            let mut batch = WriteBatch::new();
            match batch.set(key, value.clone()) {
                Ok(_) => match db.commit_batch(&batch) {
                    Ok(_) => send_command_complete(stream, "INSERT 0 1")?,
                    Err(e) => {
                        send_error(stream, "ERROR", "XX000", &format!("Insert failed: {}", e))?
                    }
                },
                Err(e) => send_error(stream, "ERROR", "XX000", &format!("Batch error: {}", e))?,
            }
        }

        query::Action::Update(key, value) => {
            let mut batch = WriteBatch::new();
            match batch.set(key, value.clone()) {
                Ok(_) => match db.commit_batch(&batch) {
                    Ok(_) => send_command_complete(stream, "UPDATE 1")?,
                    Err(e) => {
                        send_error(stream, "ERROR", "XX000", &format!("Update failed: {}", e))?
                    }
                },
                Err(e) => send_error(stream, "ERROR", "XX000", &format!("Batch error: {}", e))?,
            }
        }

        query::Action::Delete => {
            let (start_key, end_key) = build_scan_range(&parsed.conditions);
            let results = db.scan(&start_key, &end_key, seq).unwrap_or_default();

            let mut batch = WriteBatch::new();
            let mut deleted = 0;
            for (key, _) in &results {
                if batch.delete(key).is_ok() {
                    deleted += 1;
                }
            }

            if deleted > 0 {
                let _ = db.commit_batch(&batch);
            }

            send_command_complete(stream, &format!("DELETE {}", deleted))?;
        }
    }

    Ok(())
}

fn bounded_pgwire_query_limit(limit: Option<usize>) -> Result<usize, String> {
    match limit {
        Some(limit) if limit > MAX_PGWIRE_RESULT_ROWS => Err(format!(
            "SELECT LIMIT {limit} exceeds PgWire maximum of {MAX_PGWIRE_RESULT_ROWS} rows"
        )),
        Some(limit) => Ok(limit),
        None => Ok(MAX_PGWIRE_RESULT_ROWS),
    }
}

fn bounded_pgwire_sql_limit(limit: Option<usize>, offset: Option<usize>) -> Result<usize, String> {
    let offset = offset.unwrap_or(0);
    if offset > MAX_PGWIRE_RESULT_ROWS {
        return Err(format!(
            "SELECT OFFSET {offset} exceeds PgWire maximum query window of {MAX_PGWIRE_RESULT_ROWS} rows"
        ));
    }

    match limit {
        Some(limit) if limit > MAX_PGWIRE_RESULT_ROWS => Err(format!(
            "SELECT LIMIT {limit} exceeds PgWire maximum of {MAX_PGWIRE_RESULT_ROWS} rows"
        )),
        Some(limit) if limit.saturating_add(offset) > MAX_PGWIRE_RESULT_ROWS => Err(format!(
            "SELECT LIMIT/OFFSET window exceeds PgWire maximum of {MAX_PGWIRE_RESULT_ROWS} rows"
        )),
        Some(limit) => Ok(limit),
        None => Ok(MAX_PGWIRE_RESULT_ROWS - offset),
    }
}

fn enforce_pgwire_statement_limits(
    stmt: crate::sql::SqlStatement,
) -> Result<crate::sql::SqlStatement, String> {
    match stmt {
        crate::sql::SqlStatement::Select {
            columns,
            from,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
        } => {
            let limit = Some(bounded_pgwire_sql_limit(limit, offset)?);
            Ok(crate::sql::SqlStatement::Select {
                columns,
                from,
                where_clause,
                group_by,
                having,
                order_by,
                limit,
                offset,
            })
        }
        crate::sql::SqlStatement::SetOp {
            op,
            left,
            right,
            all,
        } => Ok(crate::sql::SqlStatement::SetOp {
            op,
            left: Box::new(enforce_pgwire_statement_limits(*left)?),
            right: Box::new(enforce_pgwire_statement_limits(*right)?),
            all,
        }),
        other => Ok(other),
    }
}

/// Build scan range from WHERE conditions.
fn build_scan_range(conditions: &[query::Condition]) -> (String, String) {
    let mut start = String::new();
    let mut end = String::from("\x7F"); // High ASCII

    for cond in conditions {
        match cond {
            query::Condition::Key(query::Operator::Eq, val) => {
                start = val.clone();
                // For exact match, end is key + 1 byte
                end = format!("{}~", val);
            }
            query::Condition::Key(query::Operator::Gte, val) => {
                start = val.clone();
            }
            query::Condition::Key(query::Operator::Lte, val) => {
                end = format!("{}~", val);
            }
        }
    }

    (start, end)
}

// ═══════════════════════════════════════════════════════════════════════
// Wire Protocol Message Builders
// ═══════════════════════════════════════════════════════════════════════

fn send_auth_ok(stream: &mut std::net::TcpStream) -> std::io::Result<()> {
    let mut buf = Vec::new();
    buf.push(AUTH_OK);
    buf.extend_from_slice(&8i32.to_be_bytes());
    buf.extend_from_slice(&0i32.to_be_bytes());
    stream.write_all(&buf)
}

/// Send ReadyForQuery with the correct transaction status byte.
/// 'I' = idle (no transaction), 'T' = in transaction, 'E' = failed transaction
fn send_ready_for_query_status(
    stream: &mut std::net::TcpStream,
    status: u8,
) -> std::io::Result<()> {
    stream.write_all(&ready_for_query_bytes(status))
}

fn ready_for_query_bytes(status: u8) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(READY_FOR_QUERY);
    buf.extend_from_slice(&5i32.to_be_bytes());
    buf.push(status);
    buf
}

fn send_parameter_status(
    stream: &mut std::net::TcpStream,
    key: &str,
    value: &str,
) -> std::io::Result<()> {
    let mut body = Vec::new();
    body.extend_from_slice(key.as_bytes());
    body.push(0);
    body.extend_from_slice(value.as_bytes());
    body.push(0);

    let mut buf = Vec::new();
    buf.push(PARAMETER_STATUS);
    buf.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    buf.extend_from_slice(&body);
    stream.write_all(&buf)
}

fn send_row_description(
    stream: &mut std::net::TcpStream,
    columns: &[(&str, i32)],
) -> std::io::Result<()> {
    let mut body = Vec::new();
    body.extend_from_slice(&(columns.len() as i16).to_be_bytes());

    for (name, oid) in columns {
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(&0i32.to_be_bytes()); // table OID
        body.extend_from_slice(&0i16.to_be_bytes()); // column #
        body.extend_from_slice(&oid.to_be_bytes()); // type OID
        body.extend_from_slice(&(-1i16).to_be_bytes()); // type size
        body.extend_from_slice(&0i32.to_be_bytes()); // type modifier
        body.extend_from_slice(&0i16.to_be_bytes()); // format (text)
    }

    let mut buf = Vec::new();
    buf.push(ROW_DESCRIPTION);
    buf.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    buf.extend_from_slice(&body);
    stream.write_all(&buf)
}

fn send_data_row(stream: &mut std::net::TcpStream, values: &[&str]) -> std::io::Result<()> {
    let mut body = Vec::new();
    body.extend_from_slice(&(values.len() as i16).to_be_bytes());

    for val in values {
        let bytes = val.as_bytes();
        body.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
        body.extend_from_slice(bytes);
    }

    let mut buf = Vec::new();
    buf.push(DATA_ROW);
    buf.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    buf.extend_from_slice(&body);
    stream.write_all(&buf)
}

fn send_command_complete(stream: &mut std::net::TcpStream, tag: &str) -> std::io::Result<()> {
    stream.write_all(&command_complete_bytes(tag))
}

fn command_complete_bytes(tag: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(tag.as_bytes());
    body.push(0);

    let mut buf = Vec::new();
    buf.push(COMMAND_COMPLETE);
    buf.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    buf.extend_from_slice(&body);
    buf
}

fn send_error(
    stream: &mut std::net::TcpStream,
    severity: &str,
    code: &str,
    message: &str,
) -> std::io::Result<()> {
    stream.write_all(&error_response_bytes(severity, code, message))
}

fn error_response_bytes(severity: &str, code: &str, message: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.push(b'S');
    body.extend_from_slice(severity.as_bytes());
    body.push(0);
    body.push(b'V');
    body.extend_from_slice(severity.as_bytes());
    body.push(0);
    body.push(b'C');
    body.extend_from_slice(code.as_bytes());
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(message.as_bytes());
    body.push(0);
    body.push(0); // Terminator

    let mut buf = Vec::new();
    buf.push(ERROR_RESPONSE);
    buf.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    buf.extend_from_slice(&body);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::{SqlStatement, parse_sql};

    fn pgwire_frame_length(frame: &[u8]) -> usize {
        let length_bytes: [u8; 4] = frame[1..5].try_into().expect("length bytes");
        let encoded = u32::from_be_bytes(length_bytes);
        usize::try_from(encoded).expect("frame length fits usize")
    }

    #[test]
    fn pgwire_contract_ready_for_query_frames_are_stable() {
        assert_eq!(ready_for_query_bytes(b'I'), vec![b'Z', 0, 0, 0, 5, b'I']);
        assert_eq!(ready_for_query_bytes(b'T'), vec![b'Z', 0, 0, 0, 5, b'T']);
        assert_eq!(ready_for_query_bytes(b'E'), vec![b'Z', 0, 0, 0, 5, b'E']);
    }

    #[test]
    fn pgwire_contract_command_complete_frame_is_stable() {
        let frame = command_complete_bytes("SELECT 1");

        assert_eq!(frame[0], b'C');
        assert_eq!(pgwire_frame_length(&frame), frame.len() - 1);
        assert_eq!(&frame[5..], b"SELECT 1\0");
    }

    #[test]
    fn pgwire_contract_error_response_fields_are_stable() {
        let frame = error_response_bytes("ERROR", "42601", "Parse error: invalid query");

        assert_eq!(frame[0], b'E');
        assert_eq!(pgwire_frame_length(&frame), frame.len() - 1);
        assert_eq!(
            &frame[5..],
            b"SERROR\0VERROR\0C42601\0MParse error: invalid query\0\0"
        );
    }

    #[test]
    fn pgwire_frame_length_validation_rejects_invalid_lengths_before_allocation() {
        let short = validate_pgwire_body_len(3, "startup message").expect_err("short frame");
        assert_eq!(short.kind(), std::io::ErrorKind::InvalidData);
        assert!(short.to_string().contains("too short"));

        let huge = validate_pgwire_body_len(MAX_PGWIRE_PACKET_BYTES + 5, "message")
            .expect_err("huge frame");
        assert_eq!(huge.kind(), std::io::ErrorKind::InvalidData);
        assert!(huge.to_string().contains("too large"));

        assert_eq!(
            validate_pgwire_body_len(4 + MAX_PGWIRE_PACKET_BYTES, "message")
                .expect("maximum legal frame"),
            MAX_PGWIRE_PACKET_BYTES
        );
    }

    #[test]
    fn pgwire_legacy_query_limit_defaults_and_rejects_oversized_limits() {
        assert_eq!(
            bounded_pgwire_query_limit(None).expect("default"),
            MAX_PGWIRE_RESULT_ROWS
        );
        assert_eq!(bounded_pgwire_query_limit(Some(25)).expect("explicit"), 25);
        assert!(bounded_pgwire_query_limit(Some(MAX_PGWIRE_RESULT_ROWS + 1)).is_err());
    }

    #[test]
    fn pgwire_sql_limit_caps_unbounded_selects_before_execution() {
        let stmt = parse_sql("SELECT * FROM users").expect("parse");
        let limited = enforce_pgwire_statement_limits(stmt).expect("limited");

        match limited {
            SqlStatement::Select { limit, .. } => assert_eq!(limit, Some(MAX_PGWIRE_RESULT_ROWS)),
            other => panic!("expected select, got {other:?}"),
        }
    }

    #[test]
    fn pgwire_sql_limit_rejects_oversized_explicit_windows() {
        let too_large = parse_sql("SELECT * FROM users LIMIT 10001").expect("parse");
        assert!(enforce_pgwire_statement_limits(too_large).is_err());

        let too_wide = parse_sql("SELECT * FROM users LIMIT 10000 OFFSET 1").expect("parse");
        assert!(enforce_pgwire_statement_limits(too_wide).is_err());
    }

    #[test]
    fn pgwire_rate_limiter_rejects_abusive_client_identity() {
        let rate_limiter = RateLimiter::new(0.01, 1, 10);
        assert!(acquire_pgwire_query_permit(&rate_limiter, "pgwire:ip:127.0.0.1").is_ok());
        assert!(acquire_pgwire_query_permit(&rate_limiter, "pgwire:ip:127.0.0.1").is_err());
        assert!(acquire_pgwire_query_permit(&rate_limiter, "pgwire:ip:127.0.0.2").is_ok());
    }
}
