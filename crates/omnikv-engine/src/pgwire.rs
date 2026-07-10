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

use crate::query;
use crate::transaction::{Transaction, TransactionManager, TxnState};
use crate::{OmniKV, WriteBatch};

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

/// Represents a PostgreSQL wire protocol server with connection pooling.
pub struct PgWireServer {
    db: Arc<OmniKV>,
    bind_addr: String,
    max_connections: usize,
}

impl PgWireServer {
    pub fn new(db: Arc<OmniKV>, bind_addr: &str) -> Self {
        Self {
            db,
            bind_addr: bind_addr.to_string(),
            max_connections: 32,
        }
    }

    /// Creates a PgWireServer with a custom connection pool size.
    pub fn with_pool_size(db: Arc<OmniKV>, bind_addr: &str, max_connections: usize) -> Self {
        Self {
            db,
            bind_addr: bind_addr.to_string(),
            max_connections,
        }
    }

    /// Returns the configured max connections.
    pub fn max_connections(&self) -> usize {
        self.max_connections
    }

    /// Starts the PostgreSQL wire protocol server with connection pooling.
    /// Uses a bounded thread pool to prevent resource exhaustion.
    pub fn start(&self) -> std::io::Result<()> {
        let listener = TcpListener::bind(&self.bind_addr)?;
        eprintln!(
            "[OmniKV] PostgreSQL wire protocol listening on {} (pool: {} max connections)",
            self.bind_addr, self.max_connections
        );

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
                    let release_tx = permit_tx.clone();
                    std::thread::spawn(move || {
                        if let Err(e) = handle_connection(db, stream) {
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

/// Handle a single PostgreSQL client connection.
fn handle_connection(db: Arc<OmniKV>, mut stream: std::net::TcpStream) -> std::io::Result<()> {
    // Phase 1: Startup handshake
    handle_startup(&mut stream)?;

    // Per-connection state: transaction manager and session state
    let tm = Arc::new(TransactionManager::new(db.clone()));
    let mut conn = ConnectionState::new();

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

/// Handle the startup handshake.
fn handle_startup(stream: &mut std::net::TcpStream) -> std::io::Result<()> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut body = vec![0u8; len - 4];
    stream.read_exact(&mut body)?;

    send_auth_ok(stream)?;
    send_parameter_status(stream, "server_version", "15.0 (OmniKV)")?;
    send_parameter_status(stream, "server_encoding", "UTF8")?;
    send_parameter_status(stream, "client_encoding", "UTF8")?;
    send_parameter_status(stream, "DateStyle", "ISO, MDY")?;
    send_parameter_status(stream, "integer_datetimes", "on")?;
    send_ready_for_query_status(stream, b'I')?;
    Ok(())
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
    let mut body = vec![0u8; len - 4];
    stream.read_exact(&mut body)?;
    Ok(body)
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

    let upper = sql_trimmed.to_uppercase();

    // ── SET — always accept silently ──
    if upper.starts_with("SET ") {
        send_command_complete(stream, "SET")?;
        send_ready_for_query_status(stream, conn.ready_status())?;
        return Ok(());
    }

    // ── BEGIN — start an explicit transaction block ──
    if upper == "BEGIN" || upper == "START TRANSACTION" {
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
    if upper == "COMMIT" || upper == "END" {
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
    if upper == "ROLLBACK" || upper == "ABORT" {
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
    if upper == "SELECT 1" || upper.starts_with("SELECT VERSION") {
        send_row_description(stream, &[("version", 25)])?;
        send_data_row(stream, &["OmniKV 0.1.0 — Distributed KV Engine"])?;
        send_command_complete(stream, "SELECT 1")?;
        send_ready_for_query_status(stream, conn.ready_status())?;
        return Ok(());
    }

    // ── Execute SQL (with transaction context if inside BEGIN block) ──
    match crate::sql::parse_sql(sql_trimmed) {
        Ok(stmt) => {
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

            let limit = parsed.limit.unwrap_or(usize::MAX);
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
    let mut buf = Vec::new();
    buf.push(READY_FOR_QUERY);
    buf.extend_from_slice(&5i32.to_be_bytes());
    buf.push(status);
    stream.write_all(&buf)
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
    let mut body = Vec::new();
    body.extend_from_slice(tag.as_bytes());
    body.push(0);

    let mut buf = Vec::new();
    buf.push(COMMAND_COMPLETE);
    buf.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    buf.extend_from_slice(&body);
    stream.write_all(&buf)
}

fn send_error(
    stream: &mut std::net::TcpStream,
    severity: &str,
    code: &str,
    message: &str,
) -> std::io::Result<()> {
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
    stream.write_all(&buf)
}
