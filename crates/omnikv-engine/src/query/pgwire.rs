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

use std::collections::HashMap;
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

/// Maximum number of named prepared statements a single connection may
/// retain. Distinct names accumulate until disconnect and each pins its
/// SQL text, so without a cap one authenticated client can grow session
/// memory without bound; exceeding the cap is a `54000` protocol error.
/// Replacing an existing name (and the unnamed statement slot) is free.
const MAX_PGWIRE_PREPARED_STATEMENTS: usize = 1_000;

/// Maximum number of named portals a single connection may retain. A
/// suspended portal additionally retains its full result set, so this
/// bounds retained rows too; exceeding it is a `54000` protocol error.
const MAX_PGWIRE_PORTALS: usize = 1_000;

/// PostgreSQL wire protocol message types (server -> client)
const AUTH_OK: u8 = b'R';
const READY_FOR_QUERY: u8 = b'Z';
const ROW_DESCRIPTION: u8 = b'T';
const DATA_ROW: u8 = b'D';
const COMMAND_COMPLETE: u8 = b'C';
const ERROR_RESPONSE: u8 = b'E';
const PARAMETER_STATUS: u8 = b'S';
const NOTICE_RESPONSE: u8 = b'N';

/// PostgreSQL wire protocol message types (client -> server)
const QUERY_MSG: u8 = b'Q';
const TERMINATE_MSG: u8 = b'X';
const PARSE_MSG: u8 = b'P';
const BIND_MSG: u8 = b'B';
const DESCRIBE_MSG: u8 = b'D';
const EXECUTE_MSG: u8 = b'E';
const CLOSE_MSG: u8 = b'C';
const FLUSH_MSG: u8 = b'H';
const SYNC_MSG: u8 = b'S';

/// PostgreSQL wire protocol message types (server -> client, extended only)
const PARSE_COMPLETE: u8 = b'1';
const BIND_COMPLETE: u8 = b'2';
const CLOSE_COMPLETE: u8 = b'3';
const NO_DATA: u8 = b'n';
const PARAMETER_DESCRIPTION: u8 = b't';
const PORTAL_SUSPENDED: u8 = b's';

/// A Parse-created prepared statement: the query text plus the
/// parameter type OIDs the client declared, which Describe(statement)
/// echoes back in ParameterDescription (the true `$n` count is derived
/// from the statement text itself at Describe time).
struct PreparedStatement {
    sql: String,
    param_oids: Vec<u32>,
}

/// Per-connection transaction state for PgWire sessions.
/// Tracks whether the client is in an explicit transaction block.
struct ConnectionState {
    /// Active transaction, if any (set by BEGIN, cleared by COMMIT/ROLLBACK).
    txn: Option<Transaction>,
    /// Set to true when an error occurs inside a transaction block.
    /// All subsequent commands (except ROLLBACK) return error until
    /// the client issues ROLLBACK.
    txn_failed: bool,
    /// Named prepared statements from the extended protocol's Parse
    /// message. The unnamed statement is stored under the empty name and,
    /// per PostgreSQL semantics, is overwritten by every Parse with an
    /// empty name and lasts only until the next Parse of any kind.
    /// Each entry retains the parameter type OIDs the Parse carried, so
    /// Describe(statement) can echo them in ParameterDescription.
    named_statements: HashMap<String, PreparedStatement>,
    /// Bound portals awaiting Execute. The unnamed portal is stored under
    /// the empty name and is destroyed by any Bind (not only an unnamed
    /// one), matching PostgreSQL.
    portals: HashMap<String, Portal>,
    /// After an extended-protocol error, the pipeline is aborted: every
    /// message except Sync (and Terminate) is skipped without execution
    /// until the next Sync, exactly like PostgreSQL's error rule.
    extended_error_pending: bool,
}

/// A bound portal: a parsed statement with its Bind-parameter values,
/// ready to Execute. Values are already resolved to text (format code 0)
/// or rejected at Bind time.
struct Portal {
    sql: String,
    /// Name of the prepared statement this portal was bound from, so
    /// Close(statement) can implicitly destroy it — PostgreSQL: "closing
    /// a prepared statement implicitly closes any open portals that were
    /// constructed from that statement."
    source_statement: String,
    params: Vec<Option<String>>,
    /// Rows retained from a suspended execution, awaiting further Execute
    /// rounds. PostgreSQL's portal holds the executor's output the same
    /// way: a suspended portal resumes by streaming retained rows, never
    /// by re-running the statement (which could re-apply DML or read a
    /// different snapshot).
    suspended_rows: Option<SuspendedRows>,
}

/// The retained result of a partially-consumed portal.
struct SuspendedRows {
    columns: Vec<(String, i32)>,
    rows: Vec<Vec<String>>,
    tag: String,
    streamed: usize,
}

impl ConnectionState {
    fn new() -> Self {
        Self {
            txn: None,
            txn_failed: false,
            named_statements: HashMap::new(),
            portals: HashMap::new(),
            extended_error_pending: false,
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

/// The structured outcome of executing one statement, produced by the
/// protocol-neutral execution core and rendered differently by the simple
/// ('Q') and extended (Parse/Bind/Execute) protocol writers.
#[derive(Debug, Clone, PartialEq)]
enum StepOutcome {
    /// Statement executed; the tag is the CommandComplete tag (e.g.
    /// "BEGIN", "COMMIT", "SELECT 3", "INSERT 0 1").
    Complete(String),
    /// Statement produced a row set: (columns, rows) with the final
    /// CommandComplete tag appended by the writer.
    Rows {
        columns: Vec<(String, i32)>,
        rows: Vec<Vec<String>>,
        tag: String,
    },
    /// Benign warning (e.g. COMMIT with no open transaction) — the frame is
    /// WARNING-severity, followed by the completion tag, matching
    /// PostgreSQL's notice-then-complete sequence.
    Warning {
        code: String,
        message: String,
        tag: String,
    },
    /// Hard error (SQLSTATE + message). In the simple protocol the writer
    /// sends an ErrorResponse; in the extended protocol the pipeline is
    /// aborted and the connection skips messages until Sync.
    Error { code: String, message: String },
}

impl StepOutcome {
    fn complete(tag: impl Into<String>) -> Self {
        StepOutcome::Complete(tag.into())
    }

    fn error(code: &str, message: impl Into<String>) -> Self {
        StepOutcome::Error {
            code: code.to_string(),
            message: message.into(),
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

    /// Creates a PgWireServer with caller-supplied password and rate
    /// limiter state — for tests that need a deterministic limiter budget
    /// and production setups that provision their own limiter.
    pub fn with_password_and_rate_limiter(
        db: Arc<OmniKV>,
        bind_addr: &str,
        pgwire_password: &str,
        rate_limiter: Arc<RateLimiter>,
    ) -> Self {
        Self {
            db,
            bind_addr: bind_addr.to_string(),
            max_connections: 32,
            rate_limiter,
            pgwire_password: pgwire_password.to_string(),
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
                conn.extended_error_pending = false;
                handle_query(&db, &tm, &mut conn, &mut stream, &sql)?;
            }
            PARSE_MSG => {
                let body = read_message_body(&mut stream)?;
                handle_parse(&mut conn, &mut stream, &body, &rate_limiter, &client_id)?;
            }
            BIND_MSG => {
                let body = read_message_body(&mut stream)?;
                handle_bind(&mut conn, &mut stream, &body)?;
            }
            DESCRIBE_MSG => {
                let body = read_message_body(&mut stream)?;
                handle_describe(&mut conn, &mut stream, &body)?;
            }
            EXECUTE_MSG => {
                let body = read_message_body(&mut stream)?;
                handle_execute(
                    &db,
                    &tm,
                    &mut conn,
                    &mut stream,
                    &body,
                    &rate_limiter,
                    &client_id,
                )?;
            }
            CLOSE_MSG => {
                let body = read_message_body(&mut stream)?;
                handle_close(&mut conn, &mut stream, &body)?;
            }
            FLUSH_MSG => {
                let _ = read_message_body(&mut stream)?;
                // Flush has no body beyond the length; just drain the socket.
                let _ = stream.flush();
            }
            SYNC_MSG => {
                let _ = read_message_body(&mut stream)?;
                conn.extended_error_pending = false;
                send_ready_for_query_status(&mut stream, conn.ready_status())?;
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

// =====================================================================
// Extended Query Protocol (Parse / Bind / Describe / Execute / Sync)
// =====================================================================

/// Reads a null-terminated string from the front of `buf`, returning the
/// string and the rest of the buffer. Errors are protocol violations.
fn take_cstr<'a>(buf: &'a [u8], what: &str) -> Result<(&'a str, &'a [u8]), String> {
    let end = buf
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| format!("{what} is missing its null terminator"))?;
    let s = std::str::from_utf8(&buf[..end]).map_err(|e| format!("{what} is not UTF-8: {e}"))?;
    Ok((s, &buf[end + 1..]))
}

/// Whether the shared execution core accepts `sql_trimmed` — mirrors
/// its acceptance order exactly: empty statements, the whitespace-
/// normalized transaction keywords (with the `AND [NO] CHAIN` suffix
/// stripped only for the termination commands, as the core does), `SET`,
/// the `SELECT 1` / `SELECT VERSION` shortcuts, the SQL grammar, and
/// finally the legacy KV grammar. Parse rejects anything this returns
/// false for, at Parse time — the same text the core would reject with
/// `42601` at first Execute, which is how PostgreSQL reports it.
fn statement_is_accepted_by_core(sql_trimmed: &str) -> bool {
    let normalized = sql_trimmed
        .to_uppercase()
        .split_whitespace()
        .collect::<Vec<&str>>()
        .join(" ");
    if normalized.is_empty() {
        return true;
    }
    // Termination commands accept the chain suffix; nothing else does —
    // ordinary SQL may end in the word CHAIN as a column name.
    let mut stripped = normalized.as_str();
    if normalized
        .split(' ')
        .next()
        .is_some_and(|first| matches!(first, "COMMIT" | "END" | "ROLLBACK" | "ABORT"))
    {
        if let Some(rest) = normalized.strip_suffix(" AND NO CHAIN") {
            stripped = rest;
        } else if let Some(rest) = normalized.strip_suffix(" AND CHAIN") {
            stripped = rest;
        }
    }
    if matches!(
        stripped,
        "BEGIN"
            | "BEGIN WORK"
            | "BEGIN TRANSACTION"
            | "START TRANSACTION"
            | "COMMIT"
            | "COMMIT WORK"
            | "COMMIT TRANSACTION"
            | "END"
            | "END WORK"
            | "END TRANSACTION"
            | "ROLLBACK"
            | "ROLLBACK WORK"
            | "ROLLBACK TRANSACTION"
            | "ABORT"
            | "ABORT WORK"
            | "ABORT TRANSACTION"
    ) {
        return true;
    }
    if normalized == "SET" || normalized.starts_with("SET ") {
        return true;
    }
    if normalized == "SELECT 1" || normalized.starts_with("SELECT VERSION") {
        return true;
    }
    crate::sql::parse_sql(sql_trimmed).is_ok() || query::parse_query(sql_trimmed).is_ok()
}

/// Handles a Parse ('P') message: parse-check the statement (syntax
/// errors are reported HERE, at Parse time, like PostgreSQL), store it
/// under its name (the unnamed statement lives under ""), and reply
/// ParseComplete. Per PostgreSQL, the unnamed statement is destroyed by
/// any Parse; named statements are replaced on name collision and capped
/// per connection. Parameter type OIDs sent by the client are accepted
/// but not enforced (parameters bind as text); Describe echoes them.
fn handle_parse(
    conn: &mut ConnectionState,
    stream: &mut std::net::TcpStream,
    body: &[u8],
    rate_limiter: &RateLimiter,
    client_id: &str,
) -> std::io::Result<()> {
    if conn.extended_error_pending {
        return Ok(());
    }
    if let Err(retry_after_ms) = acquire_pgwire_query_permit(rate_limiter, client_id) {
        conn.extended_error_pending = true;
        send_error(
            stream,
            "ERROR",
            "53300",
            &format!("rate limit exceeded; retry after {retry_after_ms}ms"),
        )?;
        return Ok(());
    }

    let (name, rest) = match take_cstr(body, "statement name") {
        Ok(v) => v,
        Err(e) => return extended_protocol_error(stream, conn, "08P01", &e),
    };
    let (sql, rest) = match take_cstr(rest, "query text") {
        Ok(v) => v,
        Err(e) => return extended_protocol_error(stream, conn, "08P01", &e),
    };
    // rest is: int16 parameter-type count, then that many int32 OIDs.
    if rest.len() < 2 {
        return extended_protocol_error(
            stream,
            conn,
            "08P01",
            "Parse message is missing the parameter type count",
        );
    }
    let param_count = u16::from_be_bytes([rest[0], rest[1]]) as usize;
    if rest.len() < 2 + param_count * 4 {
        return extended_protocol_error(
            stream,
            conn,
            "08P01",
            "Parse message parameter type list is truncated",
        );
    }
    let mut param_oids = Vec::with_capacity(param_count);
    for i in 0..param_count {
        let at = 2 + i * 4;
        param_oids.push(u32::from_be_bytes([
            rest[at],
            rest[at + 1],
            rest[at + 2],
            rest[at + 3],
        ]));
    }

    // Parse-check NOW: PostgreSQL reports statement syntax errors at
    // Parse time, not at first Execute. Acceptance mirrors the shared
    // execution core exactly (see statement_is_accepted_by_core), so
    // nothing Parse accepts here fails there, and anything rejected
    // here is the exact text the core would reject with 42601.
    let sql_trimmed = sql.trim().trim_end_matches(';');
    if !statement_is_accepted_by_core(sql_trimmed) {
        let near = sql_trimmed.split_whitespace().next().unwrap_or_default();
        let msg = format!("syntax error at or near \"{near}\"");
        return extended_protocol_error(stream, conn, "42601", &msg);
    }

    // The execution core parses again at Execute time, which keeps
    // transaction semantics identical between protocols. The unnamed
    // statement is destroyed by ANY Parse, including a named one. The
    // cap counts DISTINCT names only — replacing an existing name or
    // the unnamed slot never grows the session's retained state.
    if name.is_empty() {
        conn.named_statements.remove("");
    }
    if !name.is_empty()
        && !conn.named_statements.contains_key(name)
        && conn.named_statements.len() >= MAX_PGWIRE_PREPARED_STATEMENTS
    {
        let msg = format!(
            "too many prepared statements (max {MAX_PGWIRE_PREPARED_STATEMENTS}); close some first"
        );
        return extended_protocol_error(stream, conn, "54000", &msg);
    }
    conn.named_statements.insert(
        name.to_string(),
        PreparedStatement {
            sql: sql.to_string(),
            param_oids,
        },
    );

    // Reply ParseComplete: '1' + int32(4).
    let msg = [PARSE_COMPLETE, 0, 0, 0, 4];
    stream.write_all(&msg)
}

/// Handles a Bind ('B') message: resolve the statement (by name or the
/// unnamed statement), bind the parameter values into a portal, and reply
/// BindComplete. Per PostgreSQL, the unnamed portal is destroyed by ANY
/// Bind, and a Bind of an unknown statement is a 26000 error that aborts
/// the pipeline until Sync.
fn handle_bind(
    conn: &mut ConnectionState,
    stream: &mut std::net::TcpStream,
    body: &[u8],
) -> std::io::Result<()> {
    if conn.extended_error_pending {
        return Ok(());
    }

    let (portal_name, rest) = match take_cstr(body, "portal name") {
        Ok(v) => v,
        Err(e) => return extended_protocol_error(stream, conn, "08P01", &e),
    };
    let (stmt_name, rest) = match take_cstr(rest, "statement name") {
        Ok(v) => v,
        Err(e) => return extended_protocol_error(stream, conn, "08P01", &e),
    };

    let sql = match conn.named_statements.get(stmt_name) {
        Some(stmt) => stmt.sql.clone(),
        None => {
            let msg = format!("prepared statement \"{stmt_name}\" does not exist");
            return extended_protocol_error(stream, conn, "26000", &msg);
        }
    };

    // The three Bind lists are each: int16 count, then entries. Format
    // codes are int16 each (0 = text, 1 = binary); values are int32 length
    // + bytes with -1 meaning NULL. A format-code count of 0 means "all
    // text", and a count of 1 means "applies to all parameters".
    let (format_codes, rest) = match read_bind_list(rest, "parameter format codes", true) {
        Ok(v) => v,
        Err(e) => return extended_protocol_error(stream, conn, "08P01", &e),
    };
    let (raw_values, rest) = match read_bind_list(rest, "parameter values", false) {
        Ok(v) => v,
        Err(e) => return extended_protocol_error(stream, conn, "08P01", &e),
    };
    let (result_formats, _rest) = match read_bind_list(rest, "result format codes", true) {
        Ok(v) => v,
        Err(e) => return extended_protocol_error(stream, conn, "08P01", &e),
    };
    // Only text result format (0) is implemented. Accepting a binary
    // request and then emitting text frames would make clients decode
    // garbage; reject at Bind with a protocol error naming the gap,
    // which is exactly the failure PostgreSQL gives for an unsupported
    // format rather than a silent mismatch.
    if result_formats.iter().any(Option::is_some) {
        return extended_protocol_error(
            stream,
            conn,
            "08P01",
            "binary result format is not supported; request text format (0)",
        );
    }

    if format_codes.len() > 1 && format_codes.len() != raw_values.len() {
        return extended_protocol_error(
            stream,
            conn,
            "08P01",
            "parameter format code count does not match parameter count",
        );
    }
    if format_codes.iter().any(Option::is_some) {
        return extended_protocol_error(
            stream,
            conn,
            "08P01",
            "binary parameter format is not supported; use text format (0)",
        );
    }

    let params: Vec<Option<String>> = raw_values
        .into_iter()
        .map(|v| v.map(|bytes| String::from_utf8_lossy(bytes.as_slice()).into_owned()))
        .collect();

    // Any Bind destroys the unnamed portal (PostgreSQL semantics).
    // The cap counts DISTINCT names only — re-Binding an existing portal
    // (the unnamed round-trip pattern drivers use) never grows state.
    conn.portals.remove("");
    if !portal_name.is_empty()
        && !conn.portals.contains_key(portal_name)
        && conn.portals.len() >= MAX_PGWIRE_PORTALS
    {
        let msg = format!("too many portals (max {MAX_PGWIRE_PORTALS}); close some first");
        return extended_protocol_error(stream, conn, "54000", &msg);
    }
    conn.portals.insert(
        portal_name.to_string(),
        Portal {
            sql,
            source_statement: stmt_name.to_string(),
            params,
            suspended_rows: None,
        },
    );

    // Reply BindComplete: '2' + int32(4).
    let msg = [BIND_COMPLETE, 0, 0, 0, 4];
    stream.write_all(&msg)
}

/// One decoded Bind list entry: `None` is a NULL value (or the "all text"
/// format-code slot), `Some(bytes)` is a raw value (or a non-text format).
type BindList = Vec<Option<Vec<u8>>>;

/// Reads one of Bind's int16-count-prefixed lists. When `is_format` is set,
/// entries are int16 format codes (0 text, 1 binary) and are returned as
/// Some(code) / None for "all text"; value entries are int32 length + bytes
/// with -1 as NULL.
fn read_bind_list<'a>(
    buf: &'a [u8],
    what: &str,
    is_format: bool,
) -> Result<(BindList, &'a [u8]), String> {
    if buf.len() < 2 {
        return Err(format!("{what} is missing its count"));
    }
    let count = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    let mut rest = &buf[2..];
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        if is_format {
            if rest.len() < 2 {
                return Err(format!("{what} entry is truncated"));
            }
            let code = i16::from_be_bytes([rest[0], rest[1]]);
            rest = &rest[2..];
            out.push(if code == 0 { None } else { Some(vec![1]) });
        } else {
            if rest.len() < 4 {
                return Err(format!("{what} entry is truncated"));
            }
            let len = i32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]);
            rest = &rest[4..];
            if len < 0 {
                out.push(None);
            } else {
                let n = usize::try_from(len).map_err(|_| format!("{what} length is huge"))?;
                if rest.len() < n {
                    return Err(format!("{what} entry is truncated"));
                }
                out.push(Some(rest[..n].to_vec()));
                rest = &rest[n..];
            }
        }
    }
    Ok((out, rest))
}

/// Derives a display label for a SELECT column for RowDescription, the
/// way PostgreSQL names result columns. `Star` and aggregates get their
/// canonical function labels; named columns keep their name.
fn select_column_label(col: &crate::sql::SelectColumn) -> String {
    use crate::sql::{AggFunc, SelectColumn, WindowFuncType};
    let func_name = |agg: &AggFunc| match agg {
        AggFunc::Count => "count",
        AggFunc::Sum => "sum",
        AggFunc::Avg => "avg",
        AggFunc::Min => "min",
        AggFunc::Max => "max",
    };
    let window_name = |w: &WindowFuncType| match w {
        WindowFuncType::RowNumber => "row_number",
        WindowFuncType::Rank => "rank",
        WindowFuncType::DenseRank => "dense_rank",
    };
    match col {
        SelectColumn::Star => "?column?".to_string(),
        SelectColumn::Named(name) => name.clone(),
        SelectColumn::Qualified(table, column) => format!("{table}.{column}"),
        SelectColumn::Aggregate(func, arg) => format!("{}({arg})", func_name(func)),
        SelectColumn::WindowFunc { func, .. } => window_name(func).to_string(),
    }
}

/// Handles Describe ('D'): 'S' describes a statement (ParameterDescription,
/// then the row shape), 'P' a portal (row shape only). Both reply
/// RowDescription for row-returning statements, NoData otherwise.
/// Describe is side-effect-free, exactly
/// like PostgreSQL's plan-based Describe: pg8000's execute_unnamed sends
/// Parse/Describe/Sync before every unnamed statement, so executing the
/// statement here would double-run every query (committing at Describe
/// time and failing at Execute time). Instead the shape is derived from
/// the parsed statement alone.
fn handle_describe(
    conn: &mut ConnectionState,
    stream: &mut std::net::TcpStream,
    body: &[u8],
) -> std::io::Result<()> {
    if conn.extended_error_pending {
        return Ok(());
    }
    if body.len() < 2 {
        return extended_protocol_error(
            stream,
            conn,
            "08P01",
            "Describe message is missing its kind byte",
        );
    }
    let kind = body[0];
    let (name, _rest) = match take_cstr(&body[1..], "describe target name") {
        Ok(v) => v,
        Err(e) => return extended_protocol_error(stream, conn, "08P01", &e),
    };

    let (sql, stmt_param_oids): (String, Vec<u32>) = match kind {
        b'S' => match conn.named_statements.get(name) {
            Some(stmt) => (stmt.sql.clone(), stmt.param_oids.clone()),
            None => {
                let msg = format!("prepared statement \"{name}\" does not exist");
                return extended_protocol_error(stream, conn, "26000", &msg);
            }
        },
        b'P' => match conn.portals.get(name) {
            Some(portal) => (portal.sql.clone(), Vec::new()),
            None => {
                let msg = format!("portal \"{name}\" does not exist");
                return extended_protocol_error(stream, conn, "34000", &msg);
            }
        },
        other => {
            let msg = format!("unknown Describe kind {other:#x}");
            return extended_protocol_error(stream, conn, "08P01", &msg);
        }
    };

    // Derive the row shape from the statement text without executing it.
    // Only SELECT-shaped statements (SQL SELECT, the SELECT 1/VERSION
    // shortcuts, and legacy KV SELECT forms) produce rows; a column name
    // cannot be derived without executing for arbitrary expressions, so
    // row statements reply a single text column like the simple path's
    // shortcuts do. Everything else replies NoData.
    let sql_trimmed = sql.trim().trim_end_matches(';');
    let normalized = sql_trimmed
        .to_uppercase()
        .split_whitespace()
        .collect::<Vec<&str>>()
        .join(" ");

    let row_columns: Option<Vec<(String, i32)>> =
        if normalized == "SELECT 1" || normalized.starts_with("SELECT VERSION") {
            Some(vec![("version".to_string(), 25)])
        } else if normalized.starts_with("SELECT") || normalized.starts_with("SHOW") {
            // Row-returning statements: derive column names from the parse when
            // possible; fall back to a placeholder text column. Parameters are
            // irrelevant to shape (they bind as text).
            match crate::sql::parse_sql(sql_trimmed) {
                Ok(crate::sql::SqlStatement::Select { columns, .. }) => Some(
                    columns
                        .iter()
                        .map(|c| (select_column_label(c), 25))
                        .collect(),
                ),
                _ => Some(vec![("column".to_string(), 25)]),
            }
        } else {
            // Legacy KV SELECT forms (SELECT * WHERE key = ...) also return rows.
            match query::parse_query(sql_trimmed) {
                Ok(parsed) => match parsed.action {
                    query::Action::SelectAll => {
                        Some(vec![("key".to_string(), 25), ("value".to_string(), 25)])
                    }
                    query::Action::SelectCount => Some(vec![("count".to_string(), 20)]),
                    _ => None,
                },
                Err(_) => None,
            }
        };

    // Describe(statement) replies ParameterDescription first — even for
    // zero parameters — exactly like PostgreSQL: "The response is a
    // ParameterDescription message describing the parameters needed by
    // the statement, followed by a RowDescription ... (or NoData)".
    // The count is what the STATEMENT needs: the highest `$n` its text
    // references. Client-declared Parse OIDs are echoed where provided;
    // the rest stay 0 (unspecified — parameters bind as text).
    if kind == b'S' {
        let n_params = crate::sql::count_statement_params(&sql);
        let oids: Vec<u32> = (1..=n_params)
            .map(|i| stmt_param_oids.get(i - 1).copied().unwrap_or(0))
            .collect();
        send_parameter_description(stream, &oids)?;
    }

    match row_columns {
        Some(cols) => {
            let refs: Vec<(&str, i32)> = cols
                .iter()
                .map(|(name, oid)| (name.as_str(), *oid))
                .collect();
            send_row_description(stream, &refs)
        }
        None => {
            // Reply NoData: 'n' + int32(4).
            let msg = [NO_DATA, 0, 0, 0, 4];
            stream.write_all(&msg)
        }
    }
}

/// Handles Execute ('E'): run the portal's statement with its bound
/// parameters and stream DataRows + CommandComplete (or PortalSuspended) —
/// never RowDescription, which only Describe sends. A fresh execution
/// consumes a rate-limit permit like a simple-protocol Query; a resume
/// of retained rows does not. Unlike the simple protocol, Execute
/// errors abort the pipeline: the ErrorResponse is sent and the
/// connection skips every message until Sync (PostgreSQL's error rule).
fn handle_execute(
    db: &Arc<OmniKV>,
    tm: &Arc<TransactionManager>,
    conn: &mut ConnectionState,
    stream: &mut std::net::TcpStream,
    body: &[u8],
    rate_limiter: &RateLimiter,
    client_id: &str,
) -> std::io::Result<()> {
    if conn.extended_error_pending {
        return Ok(());
    }
    let (portal_name, rest) = match take_cstr(body, "portal name") {
        Ok(v) => v,
        Err(e) => return extended_protocol_error(stream, conn, "08P01", &e),
    };
    // int32 max rows: 0 = unlimited (all rows, then CommandComplete);
    // a nonzero bound streams at most that many rows, and if the portal
    // has more, the round ends with PortalSuspended instead of
    // CommandComplete — the client resumes with another Execute of the
    // same portal. This is the frame contract cursor/fetch-size clients
    // (JDBC fetch size, psycopg2 server-side cursors) rely on.
    if rest.len() < 4 {
        return extended_protocol_error(
            stream,
            conn,
            "08P01",
            "Execute message is missing its row-count field",
        );
    }
    let max_rows = u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]) as usize;

    // A suspended portal resumes from its RETAINED rows — the statement
    // is never re-run (re-running DML would double-apply it, and a read
    // could see a snapshot that no longer exists).
    let suspended = {
        let portal = match conn.portals.get_mut(portal_name) {
            Some(p) => p,
            None => {
                let msg = format!("portal \"{portal_name}\" does not exist");
                return extended_protocol_error(stream, conn, "34000", &msg);
            }
        };
        portal.suspended_rows.take()
    };

    // Either the retained rows of a suspended portal, or a fresh run.
    // (columns, rows, tag, resume_offset)
    let (columns, rows, tag, resume_at) = match suspended {
        Some(SuspendedRows {
            columns,
            rows,
            tag,
            streamed,
        }) => (columns, rows, tag, streamed),
        None => {
            // A fresh execution consumes a rate-limit permit, exactly like
            // a simple-protocol Query — otherwise a client could re-Execute
            // a bound portal in a tight loop and bypass throttling. A
            // resume streams retained rows only and consumes nothing.
            if let Err(retry_after_ms) = acquire_pgwire_query_permit(rate_limiter, client_id) {
                return extended_protocol_error(
                    stream,
                    conn,
                    "53300",
                    &format!("rate limit exceeded; retry after {retry_after_ms}ms"),
                );
            }
            let portal_sql = conn
                .portals
                .get(portal_name)
                .map(|p| (p.sql.clone(), p.params.clone()))
                .expect("portal exists (checked above)");
            let (sql, params) = portal_sql;
            match execute_statement_with_params(db, tm, conn, &sql, &params) {
                StepOutcome::Rows { columns, rows, tag } => (columns, rows, tag, 0),
                StepOutcome::Complete(tag) => {
                    return send_command_complete(stream, &tag);
                }
                StepOutcome::Warning { code, message, tag } => {
                    send_notice(stream, &code, &message)?;
                    return send_command_complete(stream, &tag);
                }
                StepOutcome::Error { code, message } => {
                    return extended_protocol_error(stream, conn, &code, &message);
                }
            }
        }
    };

    // Apply the max-rows bound on top of the statement's rows, starting
    // at the offset a suspended round reached.
    let start = resume_at.min(rows.len());
    let end = if max_rows == 0 {
        rows.len()
    } else {
        (start + max_rows).min(rows.len())
    };
    let suspended_now = end < rows.len();

    // Execute NEVER sends RowDescription — PostgreSQL: "Execute doesn't
    // cause ReadyForQuery or RowDescription to be issued". The row shape
    // came from Describe(portal) or Describe(statement)+Bind; Execute
    // streams DataRows only, on every round.
    for row in &rows[start..end] {
        let refs: Vec<&str> = row.iter().map(String::as_str).collect();
        send_data_row(stream, &refs)?;
    }
    if suspended_now {
        let portal = conn.portals.get_mut(portal_name);
        if let Some(p) = portal {
            p.suspended_rows = Some(SuspendedRows {
                columns,
                rows,
                tag,
                streamed: end,
            });
        }
        // PortalSuspended: 's' + int32(4). No CommandComplete this round —
        // the client continues with another Execute of the same portal.
        let msg = [PORTAL_SUSPENDED, 0, 0, 0, 4];
        stream.write_all(&msg)
    } else {
        send_command_complete(stream, &tag)
    }
}

/// Handles Close ('C'): destroy a named statement or portal and reply
/// CloseComplete. Closing a statement implicitly closes the portals
/// constructed from it. Closing a nonexistent name is NOT an error in
/// PostgreSQL.
fn handle_close(
    conn: &mut ConnectionState,
    stream: &mut std::net::TcpStream,
    body: &[u8],
) -> std::io::Result<()> {
    if conn.extended_error_pending {
        return Ok(());
    }
    if body.len() < 2 {
        return extended_protocol_error(
            stream,
            conn,
            "08P01",
            "Close message is missing its kind byte",
        );
    }
    let kind = body[0];
    let (name, _rest) = match take_cstr(&body[1..], "close target name") {
        Ok(v) => v,
        Err(e) => return extended_protocol_error(stream, conn, "08P01", &e),
    };
    match kind {
        b'S' => {
            // Closing a prepared statement implicitly closes every open
            // portal constructed from it — PostgreSQL's documented
            // behavior. Portals remember their source statement name, so
            // only this statement's portals disappear; portals of other
            // statements (even ones with identical SQL text) survive.
            // Closing a nonexistent name stays a silent no-op.
            if conn.named_statements.remove(name).is_some() {
                conn.portals
                    .retain(|_, portal| portal.source_statement != name);
            }
        }
        b'P' => {
            conn.portals.remove(name);
        }
        other => {
            let msg = format!("unknown Close kind {other:#x}");
            return extended_protocol_error(stream, conn, "08P01", &msg);
        }
    }
    // Reply CloseComplete: '3' + int32(4).
    let msg = [CLOSE_COMPLETE, 0, 0, 0, 4];
    stream.write_all(&msg)
}

/// Sends an ERROR-severity ErrorResponse and marks the extended-protocol
/// pipeline as aborted: subsequent Parse/Bind/Describe/Execute/Close are
/// skipped until the next Sync, which clears the flag and sends
/// ReadyForQuery. This is PostgreSQL's skip-until-Sync error rule. Errors
/// inside a transaction block also fail the transaction, matching the
/// simple-protocol behavior.
fn extended_protocol_error(
    stream: &mut std::net::TcpStream,
    conn: &mut ConnectionState,
    code: &str,
    message: &str,
) -> std::io::Result<()> {
    conn.extended_error_pending = true;
    if conn.txn.is_some() {
        conn.txn_failed = true;
    }
    send_error(stream, "ERROR", code, message)
}

/// Handle a simple-protocol ('Q') Query: run the shared execution core and
/// render its outcome as simple-protocol frames.
fn handle_query(
    db: &Arc<OmniKV>,
    tm: &Arc<TransactionManager>,
    conn: &mut ConnectionState,
    stream: &mut std::net::TcpStream,
    sql: &str,
) -> std::io::Result<()> {
    write_step_outcome_simple(stream, execute_statement_core(db, tm, conn, sql))?;
    send_ready_for_query_status(stream, conn.ready_status())
}

/// Render one core outcome as simple-protocol frames. A WARNING-severity
/// ErrorResponse is followed by the completion tag, matching PostgreSQL.
fn write_step_outcome_simple(
    stream: &mut std::net::TcpStream,
    outcome: StepOutcome,
) -> std::io::Result<()> {
    match outcome {
        StepOutcome::Complete(tag) => send_command_complete(stream, &tag)?,
        StepOutcome::Rows { columns, rows, tag } => {
            let col_defs: Vec<(&str, i32)> =
                columns.iter().map(|(c, oid)| (c.as_str(), *oid)).collect();
            send_row_description(stream, &col_defs)?;
            for row in &rows {
                let refs: Vec<&str> = row.iter().map(String::as_str).collect();
                send_data_row(stream, &refs)?;
            }
            send_command_complete(stream, &tag)?;
        }
        // NoticeResponse, never ErrorResponse — drivers raise on 'E' frames.
        StepOutcome::Warning { code, message, tag } => {
            send_notice(stream, &code, &message)?;
            send_command_complete(stream, &tag)?;
        }
        StepOutcome::Error { code, message } => {
            send_error(stream, "ERROR", &code, &message)?;
        }
    }
    Ok(())
}

/// Protocol-neutral statement execution: runs one SQL string against the
/// engine, mutating connection transaction state, and returns the
/// structured outcome for the protocol writer to render. Both the simple
/// ('Q') and extended (Parse/Bind/Execute) protocols run this core so
/// transaction semantics can never drift between them.
fn execute_statement_core(
    db: &Arc<OmniKV>,
    tm: &Arc<TransactionManager>,
    conn: &mut ConnectionState,
    sql: &str,
) -> StepOutcome {
    execute_statement_with_params(db, tm, conn, sql, &[])
}

/// Params-aware execution core: the extended protocol's Execute path.
/// `$n` placeholders are parsed into the AST as marker nodes and the bound
/// values are substituted AS DATA after parsing - parameter bytes are never
/// re-parsed as SQL, so a bound value like `x OR 1=1` compares as a plain
/// text value and cannot alter the statement structure.
fn execute_statement_with_params(
    db: &Arc<OmniKV>,
    tm: &Arc<TransactionManager>,
    conn: &mut ConnectionState,
    sql: &str,
    params: &[Option<String>],
) -> StepOutcome {
    let sql_trimmed = sql.trim().trim_end_matches(';');

    if sql_trimmed.is_empty() {
        return StepOutcome::complete("EMPTY");
    }

    // Uppercased whitespace-normalized form for multi-word transaction
    // statements: DBAPI drivers (psycopg2, pg8000) send lowercase
    // `begin transaction`, `commit`, `rollback work`, etc.
    let normalized = sql_trimmed
        .to_uppercase()
        .split_whitespace()
        .collect::<Vec<&str>>()
        .join(" ");

    // PostgreSQL's transaction-control grammar allows an optional
    // `AND [NO] CHAIN` suffix on the termination commands only —
    // COMMIT/ROLLBACK and their END/ABORT aliases. BEGIN/START
    // TRANSACTION's synopsis has no chain suffix (`BEGIN AND CHAIN` is a
    // 42601 syntax error there), and ordinary SQL may legally end in the
    // word CHAIN as a column name (`WHERE admin AND chain`), so the
    // suffix is stripped only for statements led by a termination
    // keyword. `AND CHAIN` starts a new transaction with the same
    // characteristics immediately after COMMIT/ROLLBACK; `AND NO CHAIN`
    // (the default) does not.
    let mut chain = false;
    let normalized = if normalized
        .split(' ')
        .next()
        .is_some_and(|first| matches!(first, "COMMIT" | "END" | "ROLLBACK" | "ABORT"))
    {
        if let Some(rest) = normalized.strip_suffix(" AND NO CHAIN") {
            rest
        } else if let Some(rest) = normalized.strip_suffix(" AND CHAIN") {
            chain = true;
            rest
        } else {
            normalized.as_str()
        }
    } else {
        normalized.as_str()
    };

    // ── SET — always accept silently ──
    if normalized == "SET" || normalized.starts_with("SET ") {
        return StepOutcome::complete("SET");
    }

    // ── BEGIN — start an explicit transaction block ──
    // PostgreSQL accepts BEGIN [WORK|TRANSACTION] and START TRANSACTION;
    // a `AND CHAIN` suffix is a syntax error there (42601), which happens
    // naturally because the suffix is only ever stripped for the
    // termination commands: `BEGIN AND CHAIN` reaches the SQL parser
    // unstripped and is rejected.
    if matches!(
        normalized,
        "BEGIN" | "BEGIN WORK" | "BEGIN TRANSACTION" | "START TRANSACTION"
    ) {
        if conn.txn.is_some() {
            // Already in a transaction — PostgreSQL sends a WARNING but doesn't fail
            return StepOutcome::Warning {
                code: "25001".into(),
                message: "there is already a transaction in progress".into(),
                tag: "BEGIN".into(),
            };
        }
        let txn = tm.begin();
        conn.txn = Some(txn);
        conn.txn_failed = false;
        return StepOutcome::complete("BEGIN");
    }

    // ── COMMIT — commit the current transaction ──
    // PostgreSQL accepts COMMIT [WORK|TRANSACTION] and END [WORK|TRANSACTION]
    // (COMMIT TRANSACTION / END TRANSACTION are PostgreSQL extensions).
    if matches!(
        normalized,
        "COMMIT" | "COMMIT WORK" | "COMMIT TRANSACTION" | "END" | "END WORK" | "END TRANSACTION"
    ) {
        if let Some(mut txn) = conn.txn.take() {
            if conn.txn_failed {
                // Failed transaction — COMMIT acts as ROLLBACK
                db.unregister_snapshot(txn.read_seq);
                conn.txn_failed = false;
                if chain {
                    let txn = tm.begin();
                    conn.txn = Some(txn);
                    conn.txn_failed = false;
                }
                return StepOutcome::complete("ROLLBACK");
            }
            match tm.commit(&mut txn) {
                Ok(_) => {
                    // AND CHAIN: open a new transaction with the same
                    // characteristics, exactly like PostgreSQL. There
                    // is always a transaction to chain from here, so
                    // this cannot fail.
                    if chain {
                        let txn = tm.begin();
                        conn.txn = Some(txn);
                        conn.txn_failed = false;
                    }
                    StepOutcome::complete("COMMIT")
                }
                Err(e) => StepOutcome::error("40001", format!("COMMIT failed: {e}")),
            }
        } else if chain {
            // `COMMIT AND CHAIN` outside a transaction block is an error in
            // PostgreSQL, unlike plain COMMIT which only warns.
            StepOutcome::error("25P01", "there is no transaction in progress")
        } else {
            // No transaction — PostgreSQL sends a WARNING then completes.
            StepOutcome::Warning {
                code: "25P01".into(),
                message: "there is no transaction in progress".into(),
                tag: "COMMIT".into(),
            }
        }
    } else if matches!(
        normalized,
        "ROLLBACK"
            | "ROLLBACK WORK"
            | "ROLLBACK TRANSACTION"
            | "ABORT"
            | "ABORT WORK"
            | "ABORT TRANSACTION"
    ) {
        // ── ROLLBACK — abort the current transaction ──
        // PostgreSQL accepts ROLLBACK [WORK|TRANSACTION] and ABORT [WORK|
        // TRANSACTION] (the TRANSACTION spellings are PostgreSQL extensions).
        if let Some(txn) = conn.txn.take() {
            db.unregister_snapshot(txn.read_seq);
            conn.txn_failed = false;
            if chain {
                let txn = tm.begin();
                conn.txn = Some(txn);
                conn.txn_failed = false;
            }
            StepOutcome::complete("ROLLBACK")
        } else if chain {
            // `ROLLBACK AND CHAIN` outside a transaction block is an error in
            // PostgreSQL, unlike plain ROLLBACK which only warns.
            StepOutcome::error("25P01", "there is no transaction in progress")
        } else {
            StepOutcome::Warning {
                code: "25P01".into(),
                message: "there is no transaction in progress".into(),
                tag: "ROLLBACK".into(),
            }
        }
    } else {
        execute_non_transactional_statement(db, conn, sql_trimmed, normalized, params)
    }
}

/// Executes everything that is not a transaction-control statement: the
/// failed-transaction guard, health-check shortcuts, the SQL path, and the
/// legacy KV fallback. Shares the connection's transaction snapshot when a
/// BEGIN block is open.
fn execute_non_transactional_statement(
    db: &Arc<OmniKV>,
    conn: &mut ConnectionState,
    sql_trimmed: &str,
    normalized: &str,
    params: &[Option<String>],
) -> StepOutcome {
    // ── If in a failed transaction, reject all commands until ROLLBACK ──
    if conn.txn_failed {
        return StepOutcome::error(
            "25P02",
            "current transaction is aborted, commands ignored until end of transaction block",
        );
    }

    // ── SELECT 1 / SELECT VERSION — compatibility shortcuts ──
    // #110 tracks real literal-SELECT support in the parser; until then these
    // health-check shortcuts answer any case and spacing combination.
    if normalized == "SELECT 1" || normalized.starts_with("SELECT VERSION") {
        return StepOutcome::Rows {
            columns: vec![("version".into(), 25)],
            rows: vec![vec!["OmniKV 0.1.0 — Distributed KV Engine".into()]],
            tag: "SELECT 1".into(),
        };
    }

    // Binding ALWAYS runs: a statement carrying $n with an empty Bind
    // value list must fail with the missing-parameter error here, never
    // reach the executor with raw placeholder markers. Bind errors are
    // distinct from parse errors: a parse failure falls back to the
    // legacy KV grammar below, but a bind failure is a hard error about
    // the caller's parameters and must not be re-interpreted by another
    // parser.
    match crate::sql::parse_sql(sql_trimmed) {
        Ok(parsed) => {
            let stmt = match crate::sql::bind_statement_params(parsed, params) {
                Ok(bound) => bound,
                Err(msg) => {
                    if conn.txn.is_some() {
                        conn.txn_failed = true;
                    }
                    return StepOutcome::error("08P01", msg);
                }
            };
            let stmt = match enforce_pgwire_statement_limits(stmt) {
                Ok(stmt) => stmt,
                Err(msg) => {
                    if conn.txn.is_some() {
                        conn.txn_failed = true;
                    }
                    return StepOutcome::error("54000", msg);
                }
            };
            let catalog = std::sync::Arc::new(crate::catalog::Catalog::new(db.clone()));

            // If inside an explicit transaction, use the transaction's read_seq
            // for snapshot isolation. Otherwise use autocommit (current seq).
            let executor = if let Some(ref txn) = conn.txn {
                crate::sql_exec::SqlExecutor::with_snapshot(db.clone(), catalog, txn.read_seq)
            } else {
                crate::sql_exec::SqlExecutor::new(db.clone(), catalog)
            };

            match executor.execute(&stmt) {
                Ok(crate::sql_exec::ExecResult::Rows { columns, rows }) => {
                    let tag = format!("SELECT {}", rows.len());
                    StepOutcome::Rows {
                        columns: columns.into_iter().map(|c| (c, 25)).collect(),
                        rows,
                        tag,
                    }
                }
                Ok(crate::sql_exec::ExecResult::Modified { count, command }) => {
                    let _ = count; // Writes are committed directly for now (see txn note above)
                    StepOutcome::complete(command)
                }
                Ok(crate::sql_exec::ExecResult::Ok(msg)) => StepOutcome::complete(msg),
                Err(e) => {
                    if conn.txn.is_some() {
                        conn.txn_failed = true;
                    }
                    StepOutcome::error("XX000", format!("Exec error: {e}"))
                }
            }
        }
        Err(_) => {
            // Fall back to legacy KV query parser. Parameterized
            // statements never take this path: a bound value with
            // whitespace would split into multiple tokens here, so params
            // + legacy grammar is rejected as unsupported instead of
            // silently mis-parsed.
            if !params.is_empty() {
                return StepOutcome::error(
                    "0A000",
                    "parameterized statements require the SQL grammar; the legacy KV grammar does not support parameters",
                );
            }
            match query::parse_query(sql_trimmed) {
                Ok(parsed) => execute_parsed_kv_query(db, &parsed),
                Err(e) => {
                    if conn.txn.is_some() {
                        conn.txn_failed = true;
                    }
                    StepOutcome::error("42601", format!("Parse error: {e}"))
                }
            }
        }
    }
}

/// Execute a parsed legacy-KV query and build its outcome. Used by both
/// wire protocols after the SQL parser declines a statement.
fn execute_parsed_kv_query(db: &Arc<OmniKV>, parsed: &query::Query) -> StepOutcome {
    let seq = db.get_seq();

    match &parsed.action {
        query::Action::SelectAll => {
            // Build scan range from conditions
            let (start_key, end_key) = build_scan_range(&parsed.conditions);

            let results = db.scan(&start_key, &end_key, seq).unwrap_or_default();

            let limit = match bounded_pgwire_query_limit(parsed.limit) {
                Ok(limit) => limit,
                Err(msg) => return StepOutcome::error("54000", msg),
            };
            let mut rows = Vec::new();

            let iter: Box<dyn Iterator<Item = &(String, String)>> = if parsed.order_desc {
                Box::new(results.iter().rev())
            } else {
                Box::new(results.iter())
            };

            for (key, value) in iter {
                if rows.len() >= limit {
                    break;
                }
                rows.push(vec![key.clone(), value.clone()]);
            }

            let count = rows.len();
            StepOutcome::Rows {
                columns: vec![("key".into(), 25), ("value".into(), 25)],
                rows,
                tag: format!("SELECT {count}"),
            }
        }

        query::Action::SelectCount => {
            let (start_key, end_key) = build_scan_range(&parsed.conditions);
            let results = db.scan(&start_key, &end_key, seq).unwrap_or_default();

            StepOutcome::Rows {
                columns: vec![("count".into(), 20)],
                rows: vec![vec![results.len().to_string()]],
                tag: "SELECT 1".into(),
            }
        }

        query::Action::Insert(key, value) => {
            let mut batch = WriteBatch::new();
            match batch.set(key, value.clone()) {
                Ok(()) => match db.commit_batch(&batch) {
                    Ok(_) => StepOutcome::complete("INSERT 0 1"),
                    Err(e) => StepOutcome::error("XX000", format!("Insert failed: {e}")),
                },
                Err(e) => StepOutcome::error("XX000", format!("Batch error: {e}")),
            }
        }

        query::Action::Update(key, value) => {
            let mut batch = WriteBatch::new();
            match batch.set(key, value.clone()) {
                Ok(()) => match db.commit_batch(&batch) {
                    Ok(_) => StepOutcome::complete("UPDATE 1"),
                    Err(e) => StepOutcome::error("XX000", format!("Update failed: {e}")),
                },
                Err(e) => StepOutcome::error("XX000", format!("Batch error: {e}")),
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

            StepOutcome::complete(format!("DELETE {deleted}"))
        }
    }
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

/// Sends a ParameterDescription ('t') frame: int16 parameter count,
/// then one int32 type OID per parameter.
fn send_parameter_description(
    stream: &mut std::net::TcpStream,
    oids: &[u32],
) -> std::io::Result<()> {
    let count = i16::try_from(oids.len()).expect("parameter count fits i16");
    let mut body = Vec::with_capacity(2 + oids.len() * 4);
    body.extend_from_slice(&count.to_be_bytes());
    for oid in oids {
        body.extend_from_slice(&oid.to_be_bytes());
    }
    let mut buf = Vec::with_capacity(1 + 4 + body.len());
    buf.push(PARAMETER_DESCRIPTION);
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

/// Sends a WARNING/NOTICE as a NoticeResponse ('N') frame, the frame real
/// drivers (psql prints it, pg8000 ignores it) expect for benign warnings.
/// ErrorResponse ('E') is reserved for errors: DBAPI drivers raise on any
/// ErrorResponse, so a benign COMMIT-outside-transaction warning must never
/// travel as 'E'.
fn send_notice(stream: &mut std::net::TcpStream, code: &str, message: &str) -> std::io::Result<()> {
    let mut payload = Vec::new();
    payload.push(b'S');
    payload.extend_from_slice(b"WARNING ");
    payload.push(b'C');
    payload.extend_from_slice(code.as_bytes());
    payload.push(0);
    payload.push(b'M');
    payload.extend_from_slice(message.as_bytes());
    payload.push(0);
    payload.push(0);
    let total_len = u32::try_from(payload.len() + 4).expect("notice length fits u32");
    let mut msg = Vec::with_capacity(1 + 4 + payload.len());
    msg.push(NOTICE_RESPONSE);
    msg.extend_from_slice(&total_len.to_be_bytes());
    msg.extend_from_slice(&payload);
    stream.write_all(&msg)
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
