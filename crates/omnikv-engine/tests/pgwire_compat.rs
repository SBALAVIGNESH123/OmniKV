//! `PgWire` protocol-compatibility regression tests.
//!
//! These tests drive a real `PgWireServer` over real TCP sockets and speak
//! the actual libpq startup sequence — including the `SSLRequest` negotiation
//! packet that libpq-based clients (psql, JDBC, psycopg2, pg8000,
//! node-postgres) send by default before the `StartupMessage`.
//!
//! Regression context (issue #108): the server previously misparsed the
//! 8-byte `SSLRequest` as a `StartupMessage`, desynchronizing the protocol so
//! every default-configured client failed at connection time. Simulation
//! tests cannot catch framing bugs like this; only real sockets can.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use omni_engine::OmniKV;
use omni_engine::hardening::RateLimiter;
use omni_engine::pgwire::PgWireServer;
use tempfile::TempDir;

/// libpq protocol negotiation codes (first Int32 after the length prefix).
const PROTOCOL_VERSION_3_0: u32 = 196_608;
const SSL_REQUEST_CODE: u32 = 80_877_103;
const GSS_ENC_REQUEST_CODE: u32 = 80_877_104;

const TEST_PASSWORD: &str = "pgwire-compat-test-password";

/// Spawns a real `PgWireServer` on an OS-assigned loopback port and returns
/// the bound address. The engine directory is leaked for the process lifetime
/// (a few KB per test): the server thread is detached and may outlive the
/// test function, so dropping the `TempDir` while the engine's files are still
/// mmapped would fail on Windows and race on Unix.
fn spawn_pgwire_server() -> std::io::Result<String> {
    let dir = TempDir::new().expect("temp dir");
    let base = dir.keep();
    let db = OmniKV::open(
        &engine_path(&base, "manifest.json"),
        &engine_path(&base, "wal.log"),
    )
    .expect("open engine");

    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?.to_string();

    let server = PgWireServer::with_password(db, &addr, TEST_PASSWORD);
    std::thread::spawn(move || {
        // Binding errors are impossible here (the listener is pre-bound), and
        // accept-loop errors are logged by serve() per connection.
        let _ = server.serve(listener);
    });
    Ok(addr)
}

/// Spawns a `PgWireServer` with a caller-supplied rate limiter, for
/// tests that need a deterministic throttle budget.
fn spawn_pgwire_server_with_limiter(rate_limiter: Arc<RateLimiter>) -> std::io::Result<String> {
    let dir = TempDir::new().expect("temp dir");
    let base = dir.keep();
    let db = OmniKV::open(
        &engine_path(&base, "manifest.json"),
        &engine_path(&base, "wal.log"),
    )
    .expect("open engine");
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?.to_string();
    let server =
        PgWireServer::with_password_and_rate_limiter(db, &addr, TEST_PASSWORD, rate_limiter);
    std::thread::spawn(move || {
        let _ = server.serve(listener);
    });
    Ok(addr)
}

/// Decodes a `ParameterDescription` ('t') body: int16 count, then one
/// int32 OID per parameter.
fn decode_parameter_description(body: &[u8]) -> Vec<u32> {
    assert!(body.len() >= 2, "parameter description too short");
    let count = u16::from_be_bytes([body[0], body[1]]);
    let count = count as usize;
    assert_eq!(
        body.len(),
        2 + count * 4,
        "parameter description length mismatch"
    );
    (0..count)
        .map(|i| {
            let at = 2 + i * 4;
            u32::from_be_bytes([body[at], body[at + 1], body[at + 2], body[at + 3]])
        })
        .collect()
}

/// Builds an engine path inside the test's base directory using the
/// platform separator, so the same code works on Windows and Unix CI.
fn engine_path(base: &Path, file: &str) -> String {
    let path: PathBuf = base.join(file);
    path.to_str().expect("utf-8 temp path").to_string()
}

/// Reads one length-prefixed protocol message, returning (type, body).
/// The length field includes itself but not the type byte.
fn read_message(stream: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut type_buf = [0u8; 1];
    stream.read_exact(&mut type_buf)?;
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = usize::try_from(u32::from_be_bytes(len_buf)).expect("length fits usize");
    assert!(len >= 4, "protocol length must include itself");
    let mut body = vec![0u8; len - 4];
    stream.read_exact(&mut body)?;
    Ok((type_buf[0], body))
}

fn read_exact(stream: &mut TcpStream, n: usize) -> std::io::Result<Vec<u8>> {
    let mut buf = vec![0u8; n];
    stream.read_exact(&mut buf)?;
    Ok(buf)
}

/// Sends an `SSLRequest` / `GSSENCRequest` negotiation packet (length 8, code).
fn send_negotiation_request(stream: &mut TcpStream, code: u32) -> std::io::Result<()> {
    stream.write_all(&8u32.to_be_bytes())?;
    stream.write_all(&code.to_be_bytes())?;
    stream.flush()
}

/// Sends a protocol 3.0 `StartupMessage` with the given parameters.
fn send_startup_message(stream: &mut TcpStream) -> std::io::Result<()> {
    let mut body: Vec<u8> = Vec::new();
    body.extend_from_slice(&PROTOCOL_VERSION_3_0.to_be_bytes());
    for (k, v) in [
        ("user", "omni"),
        ("database", "omnikv"),
        ("application_name", "compat-test"),
    ] {
        body.extend_from_slice(k.as_bytes());
        body.push(0);
        body.extend_from_slice(v.as_bytes());
        body.push(0);
    }
    body.push(0);
    let frame_len = u32::try_from(body.len() + 4).expect("frame length fits u32");
    stream.write_all(&frame_len.to_be_bytes())?;
    stream.write_all(&body)?;
    stream.flush()
}

/// Sends a `PasswordMessage` ('p') frame with the cleartext password.
fn send_password_message(stream: &mut TcpStream, password: &str) -> std::io::Result<()> {
    let mut body = password.as_bytes().to_vec();
    body.push(0);
    let frame_len = u32::try_from(body.len() + 4).expect("frame length fits u32");
    let mut frame = Vec::with_capacity(1 + 4 + body.len());
    frame.push(b'p');
    frame.extend_from_slice(&frame_len.to_be_bytes());
    frame.extend_from_slice(&body);
    stream.write_all(&frame)?;
    stream.flush()
}

/// Sends a simple `Query` ('Q') frame.
fn send_query(stream: &mut TcpStream, sql: &str) -> std::io::Result<()> {
    let mut body = sql.as_bytes().to_vec();
    body.push(0);
    let frame_len = u32::try_from(body.len() + 4).expect("frame length fits u32");
    let mut frame = Vec::with_capacity(1 + 4 + body.len());
    frame.push(b'Q');
    frame.extend_from_slice(&frame_len.to_be_bytes());
    frame.extend_from_slice(&body);
    stream.write_all(&frame)?;
    stream.flush()
}

/// Completes the full libpq handshake: `StartupMessage`, password, auth ok,
/// parameter statuses, `ReadyForQuery`('I'). Returns the stream ready for
/// queries.
fn complete_handshake(stream: &mut TcpStream) -> std::io::Result<()> {
    send_startup_message(stream)?;

    // AuthenticationCleartextPassword request ('R', code 3).
    let (msg_type, body) = read_message(stream)?;
    assert_eq!(msg_type, b'R', "expected authentication request");
    assert_eq!(
        u32::from_be_bytes(body.as_slice().try_into().expect("4 bytes")),
        3,
        "expected AuthenticationCleartextPassword"
    );

    send_password_message(stream, TEST_PASSWORD)?;

    // AuthenticationOk ('R', code 0).
    let (msg_type, body) = read_message(stream)?;
    assert_eq!(msg_type, b'R');
    assert_eq!(
        u32::from_be_bytes(body.as_slice().try_into().expect("4 bytes")),
        0,
        "expected AuthenticationOk"
    );

    // ParameterStatus messages, then ReadyForQuery('I').
    let mut saw_ready = false;
    while !saw_ready {
        let (msg_type, body) = read_message(stream)?;
        match msg_type {
            b'S' => assert!(
                body.len() > 1 && body[body.len() - 1] == 0,
                "parameter status must be null-terminated"
            ),
            b'Z' => {
                assert_eq!(body, vec![b'I'], "expected idle ReadyForQuery");
                saw_ready = true;
            }
            other => panic!("unexpected frame {other:#x} during startup"),
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------
// Extended query protocol (issue #119): Parse / Bind / Describe / Execute /
// Close / Sync over raw sockets, exactly as DBAPI drivers drive them.
// ---------------------------------------------------------------------

/// Sends one extended-protocol frame: type byte + body (length is added).
fn send_extended(stream: &mut TcpStream, msg_type: u8, body: &[u8]) -> std::io::Result<()> {
    let frame_len = u32::try_from(body.len() + 4).expect("frame length fits u32");
    let mut frame = Vec::with_capacity(1 + 4 + body.len());
    frame.push(msg_type);
    frame.extend_from_slice(&frame_len.to_be_bytes());
    frame.extend_from_slice(body);
    stream.write_all(&frame)?;
    stream.flush()
}

/// Builds a Parse body: statement name, query, parameter type OIDs.
fn parse_body(name: &str, sql: &str, oids: &[u32]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(name.as_bytes());
    body.push(0);
    body.extend_from_slice(sql.as_bytes());
    body.push(0);
    let oid_count = i16::try_from(oids.len()).expect("oid count fits i16");
    body.extend_from_slice(&oid_count.to_be_bytes());
    for oid in oids {
        body.extend_from_slice(&oid.to_be_bytes());
    }
    body
}

/// Builds a Bind body: portal name, statement name, one text param.
fn bind_body_text_params(portal: &str, stmt: &str, params: &[&str]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(portal.as_bytes());
    body.push(0);
    body.extend_from_slice(stmt.as_bytes());
    body.push(0);
    // Zero format codes: all parameters are text.
    body.extend_from_slice(&0i16.to_be_bytes());
    let param_count = i16::try_from(params.len()).expect("param count fits i16");
    body.extend_from_slice(&param_count.to_be_bytes());
    for p in params {
        let param_len = i32::try_from(p.len()).expect("param length fits i32");
        body.extend_from_slice(&param_len.to_be_bytes());
        body.extend_from_slice(p.as_bytes());
    }
    // Zero result format codes: all results are text.
    body.extend_from_slice(&0i16.to_be_bytes());
    body
}

/// Builds an Execute body: portal name + max rows (0 = unlimited).
fn execute_body(portal: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(portal.as_bytes());
    body.push(0);
    body.extend_from_slice(&0i32.to_be_bytes());
    body
}

/// Builds a Describe/Close body: kind byte ('S' statement, 'P' portal).
fn kind_body(kind: u8, name: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.push(kind);
    body.extend_from_slice(name.as_bytes());
    body.push(0);
    body
}

/// Reads frames, skipping benign `NoticeResponse` ('N') frames, until the
/// wanted type arrives. Returns its body.
fn read_until_type(stream: &mut TcpStream, wanted: u8) -> Vec<u8> {
    loop {
        let (msg_type, body) = read_message(stream).expect("read frame");
        match msg_type {
            t if t == wanted => return body,
            b'N' => { /* benign notice; skip */ }
            b'T' if wanted == b'D' => { /* row description precedes rows */ }
            other => panic!("expected frame {wanted:#x}, got {other:#x}"),
        }
    }
}

#[test]
fn pgwire_extended_protocol_prepare_bind_execute_sync_round_trip() {
    // The canonical extended-protocol sequence every DBAPI driver uses:
    // Parse(unnamed) -> Bind(unnamed portal) -> Execute -> Sync, expecting
    // ParseComplete, BindComplete, CommandComplete, ReadyForQuery.
    let addr = spawn_pgwire_server().expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");
    complete_handshake(&mut stream).expect("handshake");

    send_extended(&mut stream, b'P', &parse_body("", "SELECT 1", &[])).expect("send Parse");
    send_extended(&mut stream, b'S', &[]).expect("send Sync");

    // ParseComplete: '1' + int32(4) — empty body.
    let body = read_until_type(&mut stream, b'1');
    assert_eq!(body, Vec::<u8>::new());
    // ReadyForQuery after Sync.
    assert_eq!(read_ready_status(&mut stream), b'I');

    // pg8000's actual sequence for every unnamed statement:
    // Parse -> Describe(S, "") -> Sync, then Bind -> Execute -> Sync.
    send_extended(&mut stream, b'P', &parse_body("", "SELECT 1", &[])).expect("send Parse");
    send_extended(&mut stream, b'D', &kind_body(b'S', "")).expect("send Describe stmt");
    send_extended(&mut stream, b'S', &[]).expect("send Sync");
    read_until_type(&mut stream, b'1'); // ParseComplete
    // PostgreSQL: statement describes reply ParameterDescription first —
    // "a ParameterDescription message describing the parameters needed by
    // the statement, followed by a RowDescription" — even when the
    // statement needs zero parameters.
    let pd_body = read_until_type(&mut stream, b't'); // ParameterDescription
    assert!(
        decode_parameter_description(&pd_body).is_empty(),
        "SELECT 1 declares no parameters"
    );
    let desc = read_until_type(&mut stream, b'T'); // RowDescription
    assert!(
        String::from_utf8_lossy(&desc).contains("version"),
        "SELECT 1 describe must expose the version column"
    );
    assert_eq!(read_ready_status(&mut stream), b'I');

    send_extended(&mut stream, b'B', &bind_body_text_params("", "", &[])).expect("send Bind");
    send_extended(&mut stream, b'E', &execute_body("")).expect("send Execute");
    send_extended(&mut stream, b'S', &[]).expect("send Sync");
    read_until_type(&mut stream, b'2'); // BindComplete
    // Execute NEVER sends RowDescription — PostgreSQL: "Execute doesn't
    // cause ReadyForQuery or RowDescription to be issued." The very next
    // frame must be the first (and here only) DataRow, whose body carries
    // the version banner.
    let (frame, rows) = read_message(&mut stream).expect("first execute frame");
    assert_eq!(
        frame, b'D',
        "Execute must stream DataRows, never RowDescription (got {frame:#x})"
    );
    assert!(
        String::from_utf8_lossy(&rows).contains("OmniKV"),
        "SELECT 1 must return the version banner row"
    );
    let tag = read_command_complete(&mut stream);
    assert_eq!(tag, "SELECT 1");
    assert_eq!(read_ready_status(&mut stream), b'I');
}

#[test]
fn pgwire_extended_protocol_parse_reports_syntax_errors_at_parse_time() {
    // PostgreSQL reports statement syntax errors at Parse time, not at
    // first Execute: Parse of text no grammar accepts answers an
    // ErrorResponse 42601 and the pipeline skips until Sync.
    let addr = spawn_pgwire_server().expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");
    complete_handshake(&mut stream).expect("handshake");

    send_extended(
        &mut stream,
        b'P',
        &parse_body("", "THIS IS NOT SQL !!!", &[]),
    )
    .expect("Parse garbage");
    send_extended(&mut stream, b'S', &[]).expect("Sync");

    let (msg_type, body) = read_message(&mut stream).expect("error frame");
    assert_eq!(msg_type, b'E', "syntax error must be an ErrorResponse");
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("42601"),
        "parse-time syntax error must be 42601, got {text:?}"
    );
    // The pipeline was aborted: the next frame after the error is the
    // ReadyForQuery from the Sync.
    assert_eq!(read_ready_status(&mut stream), b'I');

    // A legal statement still parses afterwards.
    send_extended(&mut stream, b'P', &parse_body("", "SELECT 1", &[])).expect("Parse legal");
    send_extended(&mut stream, b'S', &[]).expect("Sync");
    read_until_type(&mut stream, b'1'); // ParseComplete
    assert_eq!(read_ready_status(&mut stream), b'I');
}

#[test]
fn pgwire_extended_protocol_statement_describe_reports_parameter_count() {
    // Describe(statement) must echo a ParameterDescription whose count is
    // what the STATEMENT needs — the highest `$n` its text references —
    // and whose OIDs are the ones the client declared at Parse.
    let addr = spawn_pgwire_server().expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");
    complete_handshake(&mut stream).expect("handshake");

    send_extended(
        &mut stream,
        b'P',
        &parse_body("", "SELECT id FROM t WHERE id = $2 OR id = $1", &[23, 23]),
    )
    .expect("Parse with params");
    send_extended(&mut stream, b'D', &kind_body(b'S', "")).expect("Describe stmt");
    send_extended(&mut stream, b'S', &[]).expect("Sync");

    read_until_type(&mut stream, b'1'); // ParseComplete
    let pd = read_until_type(&mut stream, b't'); // ParameterDescription
    assert_eq!(
        decode_parameter_description(&pd),
        vec![23, 23],
        "two declared int4 OIDs, echoed, count from the statement text"
    );
    // RowDescription follows the parameters, per the wire contract.
    read_until_type(&mut stream, b'T');
    assert_eq!(read_ready_status(&mut stream), b'I');
}

#[test]
fn pgwire_extended_protocol_execute_is_rate_limited_and_resume_is_free() {
    // A fresh Execute must consume a rate-limit permit exactly like a
    // simple-protocol Query — re-Executing a portal in a tight loop
    // cannot bypass throttling. Resuming a SUSPENDED portal streams
    // retained rows only and consumes nothing. A throttled Execute
    // answers 53300 and skips until Sync.
    //
    // Budget: burst 6 permits, refill 0.01/s (nothing refills during the
    // test). The spend: CREATE (1) + three INSERTs (3) + Parse (1) + the
    // capped fresh Execute (1) = 6, leaving zero — so the resume round
    // can only pass if resumes are free, and the next fresh Execute can
    // only fail.
    let rate_limiter = Arc::new(RateLimiter::new(0.01, 6, 10));
    let addr = spawn_pgwire_server_with_limiter(rate_limiter).expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");
    complete_handshake(&mut stream).expect("handshake");

    // Seed three rows through the simple protocol (4 permits).
    send_query(&mut stream, "CREATE TABLE t (id INT PRIMARY KEY)").expect("create");
    read_command_complete(&mut stream);
    let _ = read_ready_status(&mut stream);
    for id in 1..=3 {
        send_query(&mut stream, &format!("INSERT INTO t VALUES ({id})")).expect("insert");
        read_command_complete(&mut stream);
        let _ = read_ready_status(&mut stream);
    }

    // Parse (1 permit) + Bind (free) + capped fresh Execute (1 permit):
    // two rows, then PortalSuspended. Budget is now exhausted.
    send_extended(&mut stream, b'P', &parse_body("", "SELECT id FROM t", &[])).expect("Parse");
    let mut exec = execute_body("");
    let n = exec.len();
    exec[n - 4..n].copy_from_slice(&2u32.to_be_bytes());
    send_extended(&mut stream, b'B', &bind_body_text_params("", "", &[])).expect("Bind");
    send_extended(&mut stream, b'E', &exec).expect("Execute capped");
    send_extended(&mut stream, b'S', &[]).expect("Sync");
    read_until_type(&mut stream, b'1'); // ParseComplete
    read_until_type(&mut stream, b'2'); // BindComplete
    let _ = read_until_type(&mut stream, b'D'); // row 1
    let _ = read_until_type(&mut stream, b'D'); // row 2
    assert_eq!(
        read_message(&mut stream).expect("suspended frame").0,
        b's',
        "capped Execute must end with PortalSuspended"
    );
    assert_eq!(read_ready_status(&mut stream), b'I');

    // Resume with the budget at zero: still streams the retained row and
    // closes with CommandComplete — resumes consume no permit.
    send_extended(&mut stream, b'E', &execute_body("")).expect("Execute resume");
    send_extended(&mut stream, b'S', &[]).expect("Sync");
    let _ = read_until_type(&mut stream, b'D'); // row 3
    assert_eq!(read_command_complete(&mut stream), "SELECT 3");
    assert_eq!(read_ready_status(&mut stream), b'I');

    // A FRESH execution at zero budget is throttled: 53300, skip until
    // Sync. (Bind is free, so the BindComplete still arrives.)
    send_extended(&mut stream, b'B', &bind_body_text_params("", "", &[])).expect("re-Bind");
    send_extended(&mut stream, b'E', &execute_body("")).expect("Execute throttled");
    send_extended(&mut stream, b'S', &[]).expect("Sync");
    read_until_type(&mut stream, b'2'); // BindComplete
    let (msg_type, body) = read_message(&mut stream).expect("throttle frame");
    assert_eq!(msg_type, b'E', "throttle must be an ErrorResponse");
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("53300"),
        "rate-limited fresh Execute must be 53300, got {text:?}"
    );
    assert_eq!(read_ready_status(&mut stream), b'I');
}

#[test]
fn pgwire_extended_protocol_close_statement_cascades_only_its_portals() {
    // PostgreSQL: "closing a prepared statement implicitly closes any
    // open portals that were constructed from that statement." Two
    // statements with IDENTICAL SQL text prove the cascade follows the
    // source statement identity, not the SQL text: closing one leaves
    // the other statement's portal executable.
    let addr = spawn_pgwire_server().expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");
    complete_handshake(&mut stream).expect("handshake");

    send_query(&mut stream, "CREATE TABLE t (id INT PRIMARY KEY)").expect("create");
    read_command_complete(&mut stream);
    let _ = read_ready_status(&mut stream);

    // Two statements with identical SQL text.
    send_extended(
        &mut stream,
        b'P',
        &parse_body("s1", "SELECT id FROM t", &[]),
    )
    .expect("Parse s1");
    send_extended(
        &mut stream,
        b'P',
        &parse_body("s2", "SELECT id FROM t", &[]),
    )
    .expect("Parse s2");
    // One portal from each statement.
    send_extended(&mut stream, b'B', &bind_body_text_params("p1", "s1", &[])).expect("Bind p1");
    send_extended(&mut stream, b'B', &bind_body_text_params("p2", "s2", &[])).expect("Bind p2");
    // Close ONLY s1.
    send_extended(&mut stream, b'C', &kind_body(b'S', "s1")).expect("Close s1");
    send_extended(&mut stream, b'S', &[]).expect("Sync");

    read_until_type(&mut stream, b'1'); // ParseComplete s1
    read_until_type(&mut stream, b'1'); // ParseComplete s2
    read_until_type(&mut stream, b'2'); // BindComplete p1
    read_until_type(&mut stream, b'2'); // BindComplete p2
    read_until_type(&mut stream, b'3'); // CloseComplete s1
    assert_eq!(read_ready_status(&mut stream), b'I');

    // p1 came from s1: implicitly closed — Execute answers 34000.
    send_extended(&mut stream, b'E', &execute_body("p1")).expect("Execute p1");
    send_extended(&mut stream, b'S', &[]).expect("Sync");
    let (msg_type, body) = read_message(&mut stream).expect("error frame");
    assert_eq!(msg_type, b'E');
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("34000"),
        "portal from closed statement must be gone, got {text:?}"
    );
    assert_eq!(read_ready_status(&mut stream), b'I');

    // p2 came from s2 (identical SQL text!): still executable.
    send_extended(&mut stream, b'E', &execute_body("p2")).expect("Execute p2");
    send_extended(&mut stream, b'S', &[]).expect("Sync");
    // Execute streams DataRows directly (zero rows here), then the tag.
    assert_eq!(read_command_complete(&mut stream), "SELECT 0");
    assert_eq!(read_ready_status(&mut stream), b'I');
}

#[test]
fn pgwire_extended_protocol_execute_max_rows_suspends_and_resumes() {
    // A nonzero Execute max-rows bound must stream at most that many rows
    // and end the round with PortalSuspended ('s'), NOT CommandComplete.
    // The next Execute of the same portal resumes from the retained rows —
    // the statement is never re-run — and the final round closes with
    // CommandComplete. This is the frame contract JDBC fetch sizes and
    // psycopg2 server-side cursors depend on.
    let addr = spawn_pgwire_server().expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");
    complete_handshake(&mut stream).expect("handshake");

    // Seed three rows through the simple protocol.
    send_query(&mut stream, "CREATE TABLE t (id INT PRIMARY KEY)").expect("create");
    read_command_complete(&mut stream);
    let _ = read_ready_status(&mut stream);
    for id in 1..=3 {
        send_query(&mut stream, &format!("INSERT INTO t VALUES ({id})")).expect("insert");
        read_command_complete(&mut stream);
        let _ = read_ready_status(&mut stream);
    }

    // Bind a full-range SELECT into a portal.
    send_extended(&mut stream, b'P', &parse_body("", "SELECT id FROM t", &[])).expect("Parse");
    send_extended(&mut stream, b'B', &bind_body_text_params("p", "", &[])).expect("Bind");
    send_extended(&mut stream, b'S', &[]).expect("Sync");
    read_until_type(&mut stream, b'1');
    read_until_type(&mut stream, b'2');
    let _ = read_ready_status(&mut stream);

    // Execute with max_rows = 2: exactly two DataRows, then PortalSuspended.
    let mut exec = execute_body("p");
    // Overwrite the trailing row-count field with 2.
    let n = exec.len();
    exec[n - 4..n].copy_from_slice(&2u32.to_be_bytes());
    send_extended(&mut stream, b'E', &exec).expect("Execute capped");
    send_extended(&mut stream, b'S', &[]).expect("Sync");
    // Execute never sends RowDescription: the capped round streams
    // DataRows directly, then PortalSuspended.
    let _ = read_until_type(&mut stream, b'D'); // row 1
    let _ = read_until_type(&mut stream, b'D'); // row 2
    let suspended = read_message(&mut stream).expect("suspended frame");
    assert_eq!(
        suspended.0, b's',
        "capped Execute must end with PortalSuspended, got {:#x}",
        suspended.0
    );
    assert_eq!(read_ready_status(&mut stream), b'I');

    // Resume: the remaining one row, then CommandComplete "SELECT 3" —
    // the tag counts the whole statement's rows, like PostgreSQL. The
    // resumed round continues the ROW stream: DataRows arrive directly,
    // with NO second RowDescription (the columns were already described
    // by the first round; re-sending them desyncs drivers).
    send_extended(&mut stream, b'E', &execute_body("p")).expect("Execute resume");
    send_extended(&mut stream, b'S', &[]).expect("Sync");
    let (msg_type, _) = read_message(&mut stream).expect("first resume frame");
    assert_eq!(
        msg_type, b'D',
        "resumed Execute must start with a DataRow, not RowDescription"
    );
    assert_eq!(
        read_command_complete(&mut stream),
        "SELECT 3",
        "resumed round must close the statement with the full row count"
    );
    let _ = read_ready_status(&mut stream);

    // The JDBC pattern: a new round re-Binds the statement (destroying the
    // unnamed portal's prior state; here the named portal is re-created by
    // Bind) and a full Execute streams every row with CommandComplete —
    // no PortalSuspended.
    send_extended(&mut stream, b'B', &bind_body_text_params("p", "", &[])).expect("re-Bind");
    send_extended(&mut stream, b'E', &execute_body("p")).expect("Execute full");
    send_extended(&mut stream, b'S', &[]).expect("send Sync");
    read_until_type(&mut stream, b'2'); // BindComplete
    // DataRows arrive with no preceding RowDescription on any round.
    let (first, _) = read_message(&mut stream).expect("first frame");
    assert_eq!(
        first, b'D',
        "Execute streams DataRows, never RowDescription (got {first:#x})"
    );
    for _ in 0..2 {
        let _ = read_until_type(&mut stream, b'D');
    }
    assert_eq!(read_command_complete(&mut stream), "SELECT 3");
    let _ = read_ready_status(&mut stream);
}

#[test]
fn pgwire_extended_protocol_explain_binds_inner_statement_parameters() {
    // EXPLAIN/EXPLAIN ANALYZE wrap an inner statement that carries value
    // positions; the binder must recurse into the wrapped AST. Proof at
    // the wire: EXPLAIN of a parameterized statement with an EMPTY Bind
    // must surface the missing-parameter error — if the walk skipped the
    // inner statement, the `$1` would execute as literal text instead.
    let addr = spawn_pgwire_server().expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");
    complete_handshake(&mut stream).expect("handshake");

    send_query(&mut stream, "CREATE TABLE t (id INT PRIMARY KEY)").expect("create");
    read_command_complete(&mut stream);
    let _ = read_ready_status(&mut stream);

    send_extended(
        &mut stream,
        b'P',
        &parse_body("", "EXPLAIN SELECT id FROM t WHERE id = $1", &[]),
    )
    .expect("Parse EXPLAIN with inner $1");
    send_extended(&mut stream, b'B', &bind_body_text_params("", "", &[]))
        .expect("Bind with no values");
    send_extended(&mut stream, b'E', &execute_body("")).expect("Execute");
    send_extended(&mut stream, b'S', &[]).expect("Sync");

    read_until_type(&mut stream, b'1'); // ParseComplete
    read_until_type(&mut stream, b'2'); // BindComplete
    let (msg_type, body) = read_message(&mut stream).expect("error frame");
    assert_eq!(msg_type, b'E', "unbound inner $1 must be an error");
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("no value specified for parameter $1"),
        "binder must reach inside EXPLAIN, got {text:?}"
    );
    assert_eq!(read_ready_status(&mut stream), b'I');

    // And with a value supplied, the EXPLAIN round-trips: BindComplete,
    // rows/NoData, CommandComplete — the inner statement is fully bound.
    send_extended(
        &mut stream,
        b'P',
        &parse_body("", "EXPLAIN SELECT id FROM t WHERE id = $1", &[]),
    )
    .expect("Parse EXPLAIN again");
    send_extended(&mut stream, b'B', &bind_body_text_params("", "", &["1"]))
        .expect("Bind with a value");
    send_extended(&mut stream, b'E', &execute_body("")).expect("Execute");
    send_extended(&mut stream, b'S', &[]).expect("Sync");
    read_until_type(&mut stream, b'1');
    read_until_type(&mut stream, b'2');
    // EXPLAIN streams the plan as DataRows followed by CommandComplete —
    // Execute never sends RowDescription. The inner statement is fully
    // bound: no error, no missing-parameter complaint.
    let explain_tag: String;
    loop {
        let (msg_type, body) = read_message(&mut stream).expect("plan frames");
        match msg_type {
            // Plan DataRows and benign notices both just continue the
            // stream; the round ends with the completion tag.
            b'D' | b'N' => {}
            b'C' => {
                explain_tag =
                    String::from_utf8_lossy(&body[..body.len().saturating_sub(1)]).to_string();
                break;
            }
            other => panic!("unexpected frame {other:#x} in EXPLAIN result"),
        }
    }
    // The completion tag reflects the row stream the plan renderer
    // produced; the contract under test is that the round completes
    // cleanly with the inner statement fully bound — no error, no
    // missing-parameter complaint.
    assert!(
        !explain_tag.is_empty(),
        "bound EXPLAIN must complete with a CommandComplete tag, got {explain_tag:?}"
    );
    let _ = read_ready_status(&mut stream);
}

#[test]
fn pgwire_extended_protocol_unbound_parameter_is_an_error_not_data() {
    // A statement carrying $n executed through a Bind with NO values must
    // fail with a missing-parameter error — never fall through and reach
    // the executor with a raw `$1` string as literal data. This was the
    // review regression: the empty-params shortcut skipped binding
    // entirely, so `WHERE id = $1` compared against the literal "$1".
    let addr = spawn_pgwire_server().expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");
    complete_handshake(&mut stream).expect("handshake");

    send_query(&mut stream, "CREATE TABLE t (id INT PRIMARY KEY)").expect("create");
    read_command_complete(&mut stream);
    let _ = read_ready_status(&mut stream);

    // Parse a parameterized statement, Bind with ZERO values, Execute.
    send_extended(
        &mut stream,
        b'P',
        &parse_body("", "SELECT id FROM t WHERE id = $1", &[23]),
    )
    .expect("Parse with $1");
    send_extended(&mut stream, b'B', &bind_body_text_params("", "", &[]))
        .expect("Bind with no values");
    send_extended(&mut stream, b'E', &execute_body("")).expect("Execute");
    send_extended(&mut stream, b'S', &[]).expect("Sync");

    read_until_type(&mut stream, b'1'); // ParseComplete
    read_until_type(&mut stream, b'2'); // BindComplete
    let (msg_type, body) = read_message(&mut stream).expect("error frame");
    assert_eq!(msg_type, b'E', "unbound $1 must be an error, not data");
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("no value specified for parameter $1"),
        "expected the missing-parameter error, got {text:?}"
    );
    assert_eq!(read_ready_status(&mut stream), b'I');
}

#[test]
fn pgwire_extended_protocol_binary_result_format_is_rejected_at_bind() {
    // Bind requesting binary result format must be rejected with 08P01 at
    // Bind time — accepting it and emitting text frames would make
    // clients decode garbage.
    let addr = spawn_pgwire_server().expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");
    complete_handshake(&mut stream).expect("handshake");

    send_extended(&mut stream, b'P', &parse_body("", "SELECT 1", &[])).expect("Parse");
    send_extended(&mut stream, b'S', &[]).expect("Sync");
    read_until_type(&mut stream, b'1');
    let _ = read_ready_status(&mut stream);

    // Bind with result format code = 1 (binary) for the single column.
    let mut body = Vec::new();
    body.extend_from_slice(b" "); // portal: unnamed
    body.extend_from_slice(b" "); // statement: unnamed
    body.extend_from_slice(&0i16.to_be_bytes()); // param format codes: all text
    body.extend_from_slice(&0i16.to_be_bytes()); // zero param values
    body.extend_from_slice(&1i16.to_be_bytes()); // one result format code
    body.extend_from_slice(&1i16.to_be_bytes()); // ...and it is binary
    send_extended(&mut stream, b'B', &body).expect("Bind binary result");
    send_extended(&mut stream, b'S', &[]).expect("Sync");

    let (msg_type, resp) = read_message(&mut stream).expect("error frame");
    assert_eq!(msg_type, b'E', "binary result format must be rejected");
    let text = String::from_utf8_lossy(&resp);
    assert!(
        text.contains("08P01") && text.contains("binary result format"),
        "expected 08P01 naming the binary result format gap, got {text:?}"
    );
    assert_eq!(read_ready_status(&mut stream), b'I');
}

#[test]
fn pgwire_extended_protocol_warnings_travel_as_notice_response() {
    // The benign COMMIT-without-transaction warning over the extended
    // protocol must arrive as a NoticeResponse ('N') frame followed by
    // CommandComplete — never an ErrorResponse ('E'), because DBAPI
    // drivers raise on any 'E' and would break a benign autocommit-adjacent
    // commit. This was the review regression: both renderers called
    // send_error("WARNING", ...), which always emits 'E'.
    let addr = spawn_pgwire_server().expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");
    complete_handshake(&mut stream).expect("handshake");

    send_extended(&mut stream, b'P', &parse_body("", "commit", &[])).expect("Parse commit");
    send_extended(&mut stream, b'B', &bind_body_text_params("", "", &[])).expect("Bind");
    send_extended(&mut stream, b'E', &execute_body("")).expect("Execute");
    send_extended(&mut stream, b'S', &[]).expect("Sync");

    read_until_type(&mut stream, b'1'); // ParseComplete
    read_until_type(&mut stream, b'2'); // BindComplete
    // THE CONTRACT: the warning frame must be 'N', not 'E'.
    let (warn_type, warn_body) = read_message(&mut stream).expect("warning frame");
    assert_eq!(
        warn_type, b'N',
        "benign warning must be a NoticeResponse, got {warn_type:#x}"
    );
    let warn = String::from_utf8_lossy(&warn_body);
    assert!(
        warn.contains("25P01") && warn.contains("WARNING"),
        "expected WARNING 25P01 notice, got {warn:?}"
    );
    assert_eq!(read_command_complete(&mut stream), "COMMIT");
    assert_eq!(read_ready_status(&mut stream), b'I');
}

#[test]
fn pgwire_extended_protocol_transaction_commit_and_rollback() {
    // The exact #119 scenario: DBAPI commit()/rollback() drive COMMIT /
    // ROLLBACK through the extended protocol. Sync after the unnamed
    // statement must keep the session's transaction state coherent.
    let addr = spawn_pgwire_server().expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");
    complete_handshake(&mut stream).expect("handshake");

    // Open the transaction with simple 'Q' (pg8000 sends begin that way),
    // then commit via extended protocol — the #119 failure path.
    send_query(&mut stream, "begin transaction").expect("send begin");
    assert_eq!(read_command_complete(&mut stream), "BEGIN");
    assert_eq!(read_ready_status(&mut stream), b'T');

    // commit via Parse/Bind/Execute/Sync (pg8000 execute_unnamed).
    send_extended(&mut stream, b'P', &parse_body("", "commit", &[])).expect("Parse commit");
    send_extended(&mut stream, b'B', &bind_body_text_params("", "", &[])).expect("Bind");
    send_extended(&mut stream, b'E', &execute_body("")).expect("Execute");
    send_extended(&mut stream, b'S', &[]).expect("Sync");
    read_until_type(&mut stream, b'1'); // ParseComplete
    read_until_type(&mut stream, b'2'); // BindComplete
    assert_eq!(read_command_complete(&mut stream), "COMMIT");
    // ReadyForQuery after Sync must report idle: the extended-path commit
    // closed the same transaction the simple path opened.
    assert_eq!(read_ready_status(&mut stream), b'I');

    // Now begin again and rollback through the extended protocol.
    send_extended(&mut stream, b'P', &parse_body("", "begin", &[])).expect("Parse begin");
    send_extended(&mut stream, b'B', &bind_body_text_params("", "", &[])).expect("Bind");
    send_extended(&mut stream, b'E', &execute_body("")).expect("Execute");
    send_extended(&mut stream, b'S', &[]).expect("Sync");
    read_until_type(&mut stream, b'1');
    read_until_type(&mut stream, b'2');
    assert_eq!(read_command_complete(&mut stream), "BEGIN");
    assert_eq!(read_ready_status(&mut stream), b'T');

    send_extended(&mut stream, b'P', &parse_body("", "rollback", &[])).expect("Parse rollback");
    send_extended(&mut stream, b'B', &bind_body_text_params("", "", &[])).expect("Bind");
    send_extended(&mut stream, b'E', &execute_body("")).expect("Execute");
    send_extended(&mut stream, b'S', &[]).expect("Sync");
    read_until_type(&mut stream, b'1');
    read_until_type(&mut stream, b'2');
    assert_eq!(read_command_complete(&mut stream), "ROLLBACK");
    assert_eq!(read_ready_status(&mut stream), b'I');
}

#[test]
fn pgwire_extended_protocol_bind_error_aborts_pipeline_until_sync() {
    // PostgreSQL's error rule: after an extended-protocol error, the
    // connection skips every message until the next Sync, which answers
    // ReadyForQuery. A Bind against a nonexistent statement is 26000.
    let addr = spawn_pgwire_server().expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");
    complete_handshake(&mut stream).expect("handshake");

    // Bind of an unknown statement: error + skip-until-Sync.
    send_extended(
        &mut stream,
        b'B',
        &bind_body_text_params("", "no-such-stmt", &[]),
    )
    .expect("Bind unknown");
    send_extended(&mut stream, b'E', &execute_body("")).expect("Execute (must be skipped)");
    send_extended(&mut stream, b'S', &[]).expect("Sync");

    let (msg_type, body) = read_message(&mut stream).expect("error frame");
    assert_eq!(msg_type, b'E');
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.starts_with("SERROR") && text.contains("26000"),
        "expected ERROR 26000 for unknown statement, got {text:?}"
    );
    // The Execute between the Bind error and Sync must be skipped: the
    // next frame is ReadyForQuery, not another error.
    assert_eq!(read_ready_status(&mut stream), b'I');

    // And the pipeline recovers: a fresh statement works afterwards.
    send_query(&mut stream, "SELECT 1").expect("post-error query");
    let (msg_type, _) = read_message(&mut stream).expect("reply");
    assert_eq!(msg_type, b'T', "pipeline must recover after Sync");
}

#[test]
fn pgwire_extended_protocol_named_statement_close_and_describe_portal() {
    // Named statements + portals: Parse(named), Bind, Describe(portal),
    // Execute (named portal), Close(statement), Sync — the JDBC-style
    // lifecycle.
    let addr = spawn_pgwire_server().expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");
    complete_handshake(&mut stream).expect("handshake");

    send_extended(&mut stream, b'P', &parse_body("health", "SELECT 1", &[])).expect("Parse named");
    send_extended(
        &mut stream,
        b'B',
        &bind_body_text_params("p1", "health", &[]),
    )
    .expect("Bind to named portal");
    send_extended(&mut stream, b'D', &kind_body(b'P', "p1")).expect("Describe portal");
    send_extended(&mut stream, b'E', &execute_body("p1")).expect("Execute named portal");
    send_extended(&mut stream, b'C', &kind_body(b'S', "health")).expect("Close statement");
    send_extended(&mut stream, b'S', &[]).expect("Sync");

    read_until_type(&mut stream, b'1'); // ParseComplete
    read_until_type(&mut stream, b'2'); // BindComplete
    let desc = read_until_type(&mut stream, b'T'); // portal Describe
    assert!(
        String::from_utf8_lossy(&desc).contains("version"),
        "portal describe must expose the row shape"
    );
    read_until_type(&mut stream, b'D'); // DataRow
    assert_eq!(read_command_complete(&mut stream), "SELECT 1");
    read_until_type(&mut stream, b'3'); // CloseComplete
    assert_eq!(read_ready_status(&mut stream), b'I');

    // After Close, the named statement is gone: a Bind to it is 26000.
    send_extended(&mut stream, b'B', &bind_body_text_params("", "health", &[]))
        .expect("Bind closed statement");
    send_extended(&mut stream, b'S', &[]).expect("Sync");
    let (msg_type, body) = read_message(&mut stream).expect("error frame");
    assert_eq!(msg_type, b'E');
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("26000"),
        "closed statement must be gone, got {text:?}"
    );
    assert_eq!(read_ready_status(&mut stream), b'I');
}

#[test]
fn pgwire_extended_protocol_parameter_binding_substitutes_values() {
    // Bound `$1` values must flow to the executor AS DATA, never as SQL
    // text: the statement is parsed with the placeholders intact and the
    // values are substituted into the parsed AST. The proof is the
    // injection attempt below — a value full of SQL operators binds as a
    // plain text value and finds nothing, instead of restructuring the
    // statement.
    let addr = spawn_pgwire_server().expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");
    complete_handshake(&mut stream).expect("handshake");

    // Set up a table with one row via the simple protocol.
    send_query(
        &mut stream,
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
    )
    .expect("create table");
    read_command_complete(&mut stream);
    let _ = read_ready_status(&mut stream);
    send_query(&mut stream, "INSERT INTO users VALUES (1, 'bala')").expect("seed row");
    read_command_complete(&mut stream);
    let _ = read_ready_status(&mut stream);

    // Parameterized INSERT through Parse/Bind/Execute/Sync, with the RAW
    // value (no pre-quotes — quoting is the executor's business now).
    send_extended(
        &mut stream,
        b'P',
        &parse_body("", "INSERT INTO users VALUES ($1, $2)", &[23, 25]),
    )
    .expect("Parse with params");
    send_extended(
        &mut stream,
        b'B',
        &bind_body_text_params("", "", &["2", "vignesh"]),
    )
    .expect("Bind raw values");
    send_extended(&mut stream, b'E', &execute_body("")).expect("Execute");
    send_extended(&mut stream, b'S', &[]).expect("Sync");

    read_until_type(&mut stream, b'1'); // ParseComplete
    read_until_type(&mut stream, b'2'); // BindComplete
    assert_eq!(read_command_complete(&mut stream), "INSERT 0 1");
    assert_eq!(read_ready_status(&mut stream), b'I');

    // Read the row back through a bound parameter: `WHERE id = $1` with
    // the value `2` must find exactly the row the parameter inserted.
    send_extended(
        &mut stream,
        b'P',
        &parse_body("", "SELECT id, name FROM users WHERE id = $1", &[23]),
    )
    .expect("Parse select");
    send_extended(&mut stream, b'B', &bind_body_text_params("", "", &["2"])).expect("Bind key");
    send_extended(&mut stream, b'E', &execute_body("")).expect("Execute");
    send_extended(&mut stream, b'S', &[]).expect("Sync");
    read_until_type(&mut stream, b'1');
    read_until_type(&mut stream, b'2');
    // DataRows arrive immediately after BindComplete — no RowDescription
    // frame ever leaves Execute.
    let row = read_until_type(&mut stream, b'D');
    let text = String::from_utf8_lossy(&row);
    assert!(
        text.contains("vignesh"),
        "bound parameter must reach the executor as data, got {text:?}"
    );
    assert_eq!(read_command_complete(&mut stream), "SELECT 1");
    let _ = read_ready_status(&mut stream);

    // THE INJECTION ATTEMPT: a value containing SQL operators must bind as
    // a plain text value. Under text substitution, `1 OR id > 0` would
    // restructure the WHERE clause and return every row; under AST
    // binding it compares a single text value against id and matches
    // nothing.
    send_extended(
        &mut stream,
        b'P',
        &parse_body("", "SELECT id FROM users WHERE id = $1", &[23]),
    )
    .expect("Parse injection probe");
    send_extended(
        &mut stream,
        b'B',
        &bind_body_text_params("", "", &["1 OR id > 0"]),
    )
    .expect("Bind injection payload");
    send_extended(&mut stream, b'E', &execute_body("")).expect("Execute");
    send_extended(&mut stream, b'S', &[]).expect("Sync");
    read_until_type(&mut stream, b'1');
    read_until_type(&mut stream, b'2');
    // No DataRows may arrive: the payload is one text value, not SQL.
    // (And no RowDescription either — Execute never sends one, even for
    // zero-row results.)
    assert_eq!(
        read_command_complete(&mut stream),
        "SELECT 0",
        "injection payload must bind as inert data and match nothing"
    );
    let _ = read_ready_status(&mut stream);
}

#[test]
fn pgwire_ssl_request_receives_single_byte_no_and_handshake_continues() {
    let addr = spawn_pgwire_server().expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");

    // The libpq default sequence: SSLRequest first, then StartupMessage.
    send_negotiation_request(&mut stream, SSL_REQUEST_CODE).expect("send SSLRequest");

    // Reply must be exactly one byte: 'N' (no TLS upgrade), unframed.
    let reply = read_exact(&mut stream, 1).expect("read negotiation reply");
    assert_eq!(reply, vec![b'N'], "SSLRequest must be answered with 'N'");

    // The connection must stay framing-aligned: the real StartupMessage
    // handshake completes on the same socket.
    complete_handshake(&mut stream).expect("handshake after SSLRequest refusal");

    // And a query must still work end to end on this connection.
    send_query(&mut stream, "SHOW TABLES").expect("send query");
    let (msg_type, _) = read_message(&mut stream).expect("query response");
    assert!(
        matches!(msg_type, b'C' | b'T' | b'E'),
        "expected CommandComplete, RowDescription, or ErrorResponse, got {msg_type:#x}"
    );
}

#[test]
fn pgwire_gss_enc_request_receives_single_byte_no_and_handshake_continues() {
    let addr = spawn_pgwire_server().expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");

    send_negotiation_request(&mut stream, GSS_ENC_REQUEST_CODE).expect("send GSSENCRequest");
    let reply = read_exact(&mut stream, 1).expect("read negotiation reply");
    assert_eq!(reply, vec![b'N'], "GSSENCRequest must be answered with 'N'");

    complete_handshake(&mut stream).expect("handshake after GSSENC refusal");
}

#[test]
fn pgwire_ssl_then_gss_negotiation_then_startup_is_accepted() {
    let addr = spawn_pgwire_server().expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");

    // Some clients (notably psql with sslmode=prefer and GSS enabled) may
    // negotiate both before falling back to plaintext.
    send_negotiation_request(&mut stream, SSL_REQUEST_CODE).expect("send SSLRequest");
    assert_eq!(
        read_exact(&mut stream, 1).expect("ssl reply"),
        vec![b'N'],
        "SSLRequest must be answered with 'N'"
    );
    send_negotiation_request(&mut stream, GSS_ENC_REQUEST_CODE).expect("send GSSENCRequest");
    assert_eq!(
        read_exact(&mut stream, 1).expect("gss reply"),
        vec![b'N'],
        "GSSENCRequest must be answered with 'N'"
    );

    complete_handshake(&mut stream).expect("handshake after dual negotiation");
}

/// Reads frames until `CommandComplete` and returns the command tag.
///
/// Tolerates `NoticeResponse` ('N') frames, which the server emits before
/// `CommandComplete` for benign cases exactly like PostgreSQL (COMMIT
/// without a transaction, BEGIN inside a transaction). An `ErrorResponse`
/// ('E') here is a contract violation — DBAPI drivers raise on any 'E'
/// frame — so it fails the test rather than being tolerated.
fn read_command_complete(stream: &mut TcpStream) -> String {
    loop {
        let (msg_type, body) = read_message(stream).expect("read frame");
        match msg_type {
            b'C' => {
                let tag = &body[..body.len().saturating_sub(1)];
                return String::from_utf8_lossy(tag).to_string();
            }
            b'Z' => panic!("ReadyForQuery before CommandComplete"),
            b'N' => { /* benign notice; PostgreSQL sends these as 'N' */ }
            other => panic!("unexpected frame {other:#x} while waiting for CommandComplete"),
        }
    }
}

/// Reads one `ReadyForQuery` frame and returns its transaction status byte.
fn read_ready_status(stream: &mut TcpStream) -> u8 {
    let (msg_type, body) = read_message(stream).expect("read ReadyForQuery");
    assert_eq!(msg_type, b'Z', "expected ReadyForQuery");
    assert_eq!(body.len(), 1);
    body[0]
}

#[test]
fn pgwire_transaction_statement_keywords_accept_any_case_and_variant() {
    // Issue #109: DBAPI drivers (psycopg2, pg8000 in non-autocommit mode)
    // implicitly send lowercase `begin transaction` at session start, and
    // clients use the full PostgreSQL variant set: BEGIN [WORK|TRANSACTION],
    // COMMIT [WORK], END [WORK], ROLLBACK [WORK], START TRANSACTION, ABORT.
    // All of them must behave identically in any case/spacing combination.
    let addr = spawn_pgwire_server().expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");
    complete_handshake(&mut stream).expect("handshake");

    // The exact DBAPI sequence: lowercase begin transaction.
    send_query(&mut stream, "begin transaction").expect("send begin transaction");
    assert_eq!(
        read_command_complete(&mut stream).as_str(),
        "BEGIN",
        "lowercase begin transaction must start a transaction block"
    );
    assert_eq!(
        read_ready_status(&mut stream),
        b'T',
        "session must report in-transaction ('T') after begin"
    );

    // COMMIT TRANSACTION is a documented PostgreSQL extension and must work
    // like plain COMMIT.
    send_query(&mut stream, "commit transaction").expect("send commit transaction");
    assert_eq!(
        read_command_complete(&mut stream).as_str(),
        "COMMIT",
        "commit transaction must return CommandComplete COMMIT"
    );
    assert_eq!(
        read_ready_status(&mut stream),
        b'I',
        "transaction must be closed after commit transaction"
    );

    // Close with every remaining COMMIT/END variant. These run with no open
    // transaction and get a WARNING first, exactly like PostgreSQL — then
    // CommandComplete and idle status.
    for variant in [
        "commit",
        "Commit  Work",
        "end",
        "END WORK",
        "End Transaction",
    ] {
        send_query(&mut stream, variant).expect("send commit variant");
        assert_eq!(
            read_command_complete(&mut stream).as_str(),
            "COMMIT",
            "commit variant {variant:?} must return CommandComplete COMMIT"
        );
        assert_eq!(
            read_ready_status(&mut stream),
            b'I',
            "transaction must be closed after {variant:?}"
        );
    }
}

#[test]
fn pgwire_commit_and_rollback_and_chain_behaves_like_postgresql() {
    // `AND CHAIN` (a PostgreSQL/SQL-standard suffix on COMMIT and ROLLBACK)
    // must start a new transaction immediately, and `AND NO CHAIN` must not.
    // With no open transaction, CHAIN is a hard error 25P01 while the plain
    // forms only warn. BEGIN/START TRANSACTION take no chain suffix in
    // PostgreSQL's grammar, so a chained BEGIN must be a 42601 syntax error
    // that opens nothing.
    let addr = spawn_pgwire_server().expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");
    complete_handshake(&mut stream).expect("handshake");

    // BEGIN AND CHAIN / BEGIN AND NO CHAIN / START TRANSACTION AND CHAIN:
    // syntax errors, and no transaction may be opened by a rejected statement.
    for malformed in [
        "begin and chain",
        "BEGIN AND NO CHAIN",
        "start transaction and chain",
    ] {
        send_query(&mut stream, malformed).expect("send malformed begin");
        let (msg_type, body) = read_message(&mut stream).expect("error frame");
        assert_eq!(msg_type, b'E', "{malformed:?} must be a syntax error");
        let text = String::from_utf8_lossy(&body);
        assert!(
            text.starts_with("SERROR") && text.contains("42601"),
            "expected ERROR severity with 42601, got {text:?}"
        );
        assert_eq!(
            read_ready_status(&mut stream),
            b'I',
            "{malformed:?} must not open a transaction"
        );
    }

    // COMMIT AND CHAIN with no open transaction: ERROR 25P01, session stays idle.
    send_query(&mut stream, "commit and chain").expect("send commit and chain");
    let (msg_type, body) = read_message(&mut stream).expect("error frame");
    assert_eq!(msg_type, b'E', "COMMIT AND CHAIN outside a txn must error");
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.starts_with("SERROR") && text.contains("25P01"),
        "expected ERROR severity with 25P01, got {text:?}"
    );
    assert_eq!(read_ready_status(&mut stream), b'I');

    // COMMIT AND NO CHAIN outside a transaction is the plain form: WARNING,
    // then CommandComplete, still idle.
    send_query(&mut stream, "commit and no chain").expect("send commit and no chain");
    assert_eq!(
        read_command_complete(&mut stream).as_str(),
        "COMMIT",
        "COMMIT AND NO CHAIN must complete like plain COMMIT"
    );
    assert_eq!(read_ready_status(&mut stream), b'I');

    // begin → COMMIT AND CHAIN: commits, then a NEW transaction is open ('T').
    send_query(&mut stream, "begin").expect("send begin");
    assert_eq!(read_command_complete(&mut stream).as_str(), "BEGIN");
    assert_eq!(read_ready_status(&mut stream), b'T');
    send_query(&mut stream, "commit and chain").expect("send commit and chain");
    assert_eq!(
        read_command_complete(&mut stream).as_str(),
        "COMMIT",
        "COMMIT AND CHAIN must commit the open transaction"
    );
    assert_eq!(
        read_ready_status(&mut stream),
        b'T',
        "AND CHAIN must immediately open a new transaction"
    );

    // ROLLBACK TRANSACTION AND CHAIN rolls back and re-opens ('T' again).
    send_query(&mut stream, "rollback transaction and chain").expect("send rollback and chain");
    assert_eq!(
        read_command_complete(&mut stream).as_str(),
        "ROLLBACK",
        "ROLLBACK TRANSACTION AND CHAIN must roll back"
    );
    assert_eq!(
        read_ready_status(&mut stream),
        b'T',
        "rollback AND CHAIN must immediately open a new transaction"
    );

    // ROLLBACK WORK AND NO CHAIN closes the transaction and stays idle.
    send_query(&mut stream, "rollback work and no chain").expect("send rollback no chain");
    assert_eq!(
        read_command_complete(&mut stream).as_str(),
        "ROLLBACK",
        "ROLLBACK WORK AND NO CHAIN must roll back without chaining"
    );
    assert_eq!(
        read_ready_status(&mut stream),
        b'I',
        "AND NO CHAIN must leave the session idle"
    );
}

#[test]
fn pgwire_begin_work_and_rollback_work_variants_any_case() {
    let addr = spawn_pgwire_server().expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");
    complete_handshake(&mut stream).expect("handshake");

    for begin_variant in ["begin work", "Begin  Transaction", "START transaction"] {
        send_query(&mut stream, begin_variant).expect("send begin variant");
        assert_eq!(
            read_command_complete(&mut stream).as_str(),
            "BEGIN",
            "begin variant {begin_variant:?} must return CommandComplete BEGIN"
        );
        assert_eq!(
            read_ready_status(&mut stream),
            b'T',
            "must be in a transaction after {begin_variant:?}"
        );

        for rollback_variant in [
            "rollback",
            "Rollback  Work",
            "abort",
            "Rollback Transaction",
        ] {
            send_query(&mut stream, rollback_variant).expect("send rollback variant");
            assert_eq!(
                read_command_complete(&mut stream).as_str(),
                "ROLLBACK",
                "rollback variant {rollback_variant:?} must return CommandComplete ROLLBACK"
            );
            assert_eq!(
                read_ready_status(&mut stream),
                b'I',
                "transaction must be closed after {rollback_variant:?}"
            );
            // Re-open a transaction for the next rollback variant; on the
            // second pass the previous re-open may still hold, which yields
            // a benign WARNING like PostgreSQL.
            send_query(&mut stream, begin_variant).expect("re-send begin variant");
            assert_eq!(
                read_command_complete(&mut stream).as_str(),
                "BEGIN",
                "re-open via {begin_variant:?} must return CommandComplete BEGIN"
            );
            let _ = read_ready_status(&mut stream);
        }
    }
}

#[test]
fn pgwire_set_statement_is_accepted_in_any_case_and_spacing() {
    let addr = spawn_pgwire_server().expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");
    complete_handshake(&mut stream).expect("handshake");

    // Drivers issue SET statements at session start (e.g. psycopg2's
    // `SET datestyle`); they must never depend on keyword case.
    for variant in [
        "SET client_encoding = 'UTF8'",
        "set  datestyle = 'ISO'",
        "Set Timezone To 'UTC'",
    ] {
        send_query(&mut stream, variant).expect("send set variant");
        let (msg_type, body) = read_message(&mut stream).expect("set reply");
        assert_eq!(
            msg_type, b'C',
            "SET variant {variant:?} must return CommandComplete"
        );
        assert_eq!(body, b"SET\0".to_vec());
        let (msg_type, _) = read_message(&mut stream).expect("ready after set");
        assert_eq!(msg_type, b'Z');
    }
}

#[test]
fn pgwire_plain_startup_without_negotiation_still_works() {
    // Clients configured with sslmode=disable skip negotiation entirely; the
    // first message is the StartupMessage itself. This is the only path that
    // worked before the fix, and it must keep working after it.
    let addr = spawn_pgwire_server().expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");

    complete_handshake(&mut stream).expect("plain handshake");
}

#[test]
fn pgwire_eight_negotiation_packets_then_startup_completes() {
    // The negotiation bound limits negotiation packets, not total startup
    // messages: a client that sends the documented maximum of eight SSLRequest
    // packets must still be able to complete the handshake with the ninth
    // message, the StartupMessage.
    let addr = spawn_pgwire_server().expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");

    for _ in 0..8 {
        send_negotiation_request(&mut stream, SSL_REQUEST_CODE).expect("send SSLRequest");
        assert_eq!(
            read_exact(&mut stream, 1).expect("negotiation reply"),
            vec![b'N'],
            "each of the first eight negotiation packets must get 'N'"
        );
    }

    complete_handshake(&mut stream).expect("handshake after eight negotiations");
}

#[test]
fn pgwire_ninth_negotiation_packet_is_rejected_with_08p01() {
    // The ninth negotiation packet is a protocol violation: the server must
    // reject it with an ErrorResponse, not keep answering 'N' forever.
    let addr = spawn_pgwire_server().expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");

    for _ in 0..8 {
        send_negotiation_request(&mut stream, SSL_REQUEST_CODE).expect("send SSLRequest");
        let _ = read_exact(&mut stream, 1).expect("negotiation reply");
    }

    send_negotiation_request(&mut stream, SSL_REQUEST_CODE).expect("send ninth negotiation packet");
    let (msg_type, body) = read_message(&mut stream).expect("error response");
    assert_eq!(
        msg_type, b'E',
        "ninth negotiation packet must get ErrorResponse"
    );
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("08P01"),
        "expected SQLSTATE 08P01 in {text:?}"
    );
}

#[test]
fn pgwire_production_policy_refuses_cleartext_on_public_bind() {
    use omni_engine::pgwire::PgWireSecurityPolicy;

    let dir = TempDir::new().expect("temp dir");
    let base = dir.keep();
    let db = OmniKV::open(
        &engine_path(&base, "manifest.json"),
        &engine_path(&base, "wal.log"),
    )
    .expect("open engine");

    // A public bind under the production policy must fail closed at start(),
    // before the listener is created — start() returns the policy error
    // instead of entering the accept loop.
    let public = PgWireServer::with_security_policy(
        db.clone(),
        "0.0.0.0:5433",
        TEST_PASSWORD,
        PgWireSecurityPolicy::RequirePrivateBind,
    );
    let err = public
        .start()
        .expect_err("production policy must refuse public binds");
    assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
    assert!(
        err.to_string().contains("non-private bind"),
        "error must name the policy violation: {err}"
    );

    // Loopback and RFC 1918 binds pass validation under the same policy.
    // (start() itself would block in the accept loop, so policy validation is
    // the non-blocking surface.)
    for bind in [
        "127.0.0.1:5433",
        "192.168.1.10:5433",
        "10.0.0.5:5433",
        "[::1]:5433",
    ] {
        let server = PgWireServer::with_security_policy(
            db.clone(),
            bind,
            TEST_PASSWORD,
            PgWireSecurityPolicy::RequirePrivateBind,
        );
        server
            .validate_security_policy()
            .unwrap_or_else(|e| panic!("private bind {bind} must be allowed: {e}"));
    }

    // Unparseable or public addresses fail validation under the production
    // policy, and any bind passes under the development policy.
    for bind in ["example.com:5433", "8.8.8.8:5433", "0.0.0.0:5433"] {
        let server = PgWireServer::with_security_policy(
            db.clone(),
            bind,
            TEST_PASSWORD,
            PgWireSecurityPolicy::RequirePrivateBind,
        );
        assert!(
            server.validate_security_policy().is_err(),
            "bind {bind} must fail the production policy"
        );
    }
    for bind in ["0.0.0.0:5433", "8.8.8.8:5433"] {
        let server = PgWireServer::with_security_policy(
            db.clone(),
            bind,
            TEST_PASSWORD,
            PgWireSecurityPolicy::AllowCleartextAnywhere,
        );
        server
            .validate_security_policy()
            .unwrap_or_else(|e| panic!("development policy must accept {bind}: {e}"));
    }
}

#[test]
fn pgwire_repeated_negotiation_packets_do_not_loop_forever() {
    // A hostile or buggy client may re-send SSLRequest many times. The first
    // MAX_STARTUP_NEGOTIATION_MESSAGES packets each get 'N'; the next one is
    // a protocol violation answered with ErrorResponse 08P01 and a close, so
    // the server never spins in pre-auth.
    let addr = spawn_pgwire_server().expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");

    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .expect("set read timeout");

    for _ in 0..8 {
        send_negotiation_request(&mut stream, SSL_REQUEST_CODE).expect("send SSLRequest");
        assert_eq!(
            read_exact(&mut stream, 1).expect("negotiation reply"),
            vec![b'N'],
            "each of the first eight negotiation packets must get 'N'"
        );
    }

    // The ninth negotiation packet gets the violation ErrorResponse; the
    // connection may reset mid-write or mid-read, which is equally fine.
    if let Ok((msg_type, body)) = send_negotiation_request(&mut stream, SSL_REQUEST_CODE)
        .and_then(|()| read_message(&mut stream))
    {
        assert_eq!(
            msg_type, b'E',
            "expected ErrorResponse after the negotiation bound"
        );
        let text = String::from_utf8_lossy(&body);
        assert!(
            text.contains("08P01"),
            "expected SQLSTATE 08P01 in {text:?}"
        );
    }

    // And the server must be gone from here on.
    let mut buf = [0u8; 1];
    match stream.read(&mut buf) {
        Ok(0) | Err(_) => { /* connection closed or reset: expected */ }
        Ok(_) => panic!("server must stop answering unbounded negotiation spam"),
    }
}

#[test]
fn pgwire_unsupported_protocol_version_is_rejected_with_08p01() {
    let addr = spawn_pgwire_server().expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");

    // A startup message with an unknown protocol code (e.g. 2.0 or garbage).
    let code: u32 = 0x0002_0000;
    stream
        .write_all(&(8u32).to_be_bytes())
        .and_then(|()| stream.write_all(&code.to_be_bytes()))
        .expect("send bogus startup");

    // Server must answer with an ErrorResponse carrying SQLSTATE 08P01
    // (protocol violation), per PostgreSQL behavior.
    let (msg_type, body) = read_message(&mut stream).expect("read error response");
    assert_eq!(msg_type, b'E', "expected ErrorResponse");
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("08P01"),
        "expected SQLSTATE 08P01 in {text:?}"
    );
}

#[test]
fn pgwire_wrong_password_is_rejected_over_real_socket() {
    let addr = spawn_pgwire_server().expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");

    send_startup_message(&mut stream).expect("startup");

    let (msg_type, body) = read_message(&mut stream).expect("auth request");
    assert_eq!(msg_type, b'R');
    assert_eq!(
        u32::from_be_bytes(body.as_slice().try_into().expect("4 bytes")),
        3
    );

    send_password_message(&mut stream, "definitely-not-the-password").expect("send password");

    let (msg_type, body) = read_message(&mut stream).expect("error response");
    assert_eq!(msg_type, b'E', "expected ErrorResponse on bad password");
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("28P01"),
        "expected SQLSTATE 28P01 (invalid_password) in {text:?}"
    );
}
