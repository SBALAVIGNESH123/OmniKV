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

use omni_engine::OmniKV;
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
/// Tolerates WARNING-severity `ErrorResponse` frames, which the server emits
/// before `CommandComplete` for benign cases exactly like PostgreSQL (COMMIT
/// without a transaction, BEGIN inside a transaction). Any ERROR-severity
/// frame fails the test.
fn read_command_complete(stream: &mut TcpStream) -> String {
    loop {
        let (msg_type, body) = read_message(stream).expect("read frame");
        match msg_type {
            b'C' => {
                let tag = &body[..body.len().saturating_sub(1)];
                return String::from_utf8_lossy(tag).to_string();
            }
            b'Z' => panic!("ReadyForQuery before CommandComplete"),
            b'E' => {
                let text = String::from_utf8_lossy(&body);
                assert!(
                    text.starts_with("SWARNING"),
                    "unexpected error frame: {text:?}"
                );
            }
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

    // Close with every COMMIT/END variant. The first is a real commit; the
    // rest run with no open transaction and get a WARNING first, exactly
    // like PostgreSQL — then CommandComplete and idle status.
    for variant in ["commit", "Commit  Work", "end", "END WORK"] {
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

        for rollback_variant in ["rollback", "Rollback  Work", "abort"] {
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
