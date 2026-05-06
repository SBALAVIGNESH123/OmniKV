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

/// Represents a PostgreSQL wire protocol server.
pub struct PgWireServer {
    db: Arc<OmniKV>,
    bind_addr: String,
}

impl PgWireServer {
    pub fn new(db: Arc<OmniKV>, bind_addr: &str) -> Self {
        Self {
            db,
            bind_addr: bind_addr.to_string(),
        }
    }

    /// Starts the PostgreSQL wire protocol server.
    /// This blocks and accepts connections in a loop.
    pub fn start(&self) -> std::io::Result<()> {
        let listener = TcpListener::bind(&self.bind_addr)?;
        eprintln!(
            "[OmniKV] PostgreSQL wire protocol listening on {}",
            self.bind_addr
        );

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let db = self.db.clone();
                    std::thread::spawn(move || {
                        if let Err(e) = handle_connection(db, stream) {
                            eprintln!("[OmniKV] Connection error: {}", e);
                        }
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

    // Phase 2: Query loop
    loop {
        let mut msg_type = [0u8; 1];
        if stream.read_exact(&mut msg_type).is_err() {
            break; // Client disconnected
        }

        match msg_type[0] {
            QUERY_MSG => {
                let sql = read_query_message(&mut stream)?;
                handle_query(&db, &mut stream, &sql)?;
            }
            TERMINATE_MSG => {
                let _ = read_message_body(&mut stream)?;
                break;
            }
            _ => {
                let _ = read_message_body(&mut stream)?;
                send_error(&mut stream, "ERROR", "XX000", "Unsupported message type")?;
                send_ready_for_query(&mut stream)?;
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
    send_ready_for_query(stream)?;
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
    stream: &mut std::net::TcpStream,
    sql: &str,
) -> std::io::Result<()> {
    let sql_trimmed = sql.trim().trim_end_matches(';');

    if sql_trimmed.is_empty() {
        send_command_complete(stream, "EMPTY")?;
        send_ready_for_query(stream)?;
        return Ok(());
    }

    // Handle meta-commands that our parser doesn't understand
    let upper = sql_trimmed.to_uppercase();
    if upper.starts_with("SET ") || upper == "BEGIN" || upper == "COMMIT" || upper == "ROLLBACK" {
        send_command_complete(stream, "OK")?;
        send_ready_for_query(stream)?;
        return Ok(());
    }
    if upper == "SELECT 1" || upper.starts_with("SELECT VERSION") {
        send_row_description(stream, &[("version", 25)])?;
        send_data_row(stream, &["OmniKV 0.1.0 — Distributed KV Engine"])?;
        send_command_complete(stream, "SELECT 1")?;
        send_ready_for_query(stream)?;
        return Ok(());
    }

    // Try SQL v2 parser first (CREATE TABLE, JOIN, aggregates)
    match crate::sql::parse_sql(sql_trimmed) {
        Ok(stmt) => {
            let catalog = std::sync::Arc::new(crate::catalog::Catalog::new(db.clone()));
            let executor = crate::sql_exec::SqlExecutor::new(db.clone(), catalog);
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
                    send_command_complete(stream, &command)?;
                }
                Ok(crate::sql_exec::ExecResult::Ok(msg)) => {
                    send_command_complete(stream, &msg)?;
                }
                Err(e) => {
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
                    send_error(stream, "ERROR", "42601", &format!("Parse error: {}", e))?;
                }
            }
        }
    }

    send_ready_for_query(stream)?;
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

fn send_ready_for_query(stream: &mut std::net::TcpStream) -> std::io::Result<()> {
    let mut buf = Vec::new();
    buf.push(READY_FOR_QUERY);
    buf.extend_from_slice(&5i32.to_be_bytes());
    buf.push(b'I');
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
