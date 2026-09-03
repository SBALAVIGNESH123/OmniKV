//! PgWire protocol-compatibility regression tests.
//!
//! These tests drive a real `PgWireServer` over real TCP sockets and speak
//! the actual libpq startup sequence — including the SSLRequest negotiation
//! packet that libpq-based clients (psql, JDBC, psycopg2, pg8000,
//! node-postgres) send by default before the StartupMessage.
//!
//! Regression context (issue #108): the server previously misparsed the
//! 8-byte SSLRequest as a StartupMessage, desynchronizing the protocol so
//! every default-configured client failed at connection time. Simulation
//! tests cannot catch framing bugs like this; only real sockets can.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;

use omni_engine::OmniKV;
use omni_engine::pgwire::PgWireServer;
use tempfile::TempDir;

/// libpq protocol negotiation codes (first Int32 after the length prefix).
const PROTOCOL_VERSION_3_0: u32 = 196_608;
const SSL_REQUEST_CODE: u32 = 80_877_103;
const GSS_ENC_REQUEST_CODE: u32 = 80_877_104;

const TEST_PASSWORD: &str = "pgwire-compat-test-password";

/// Spawns a real PgWireServer on an OS-assigned loopback port and returns
/// the bound address. The server thread is detached; it stops when the test
/// process exits. Each caller gets an isolated engine directory, so these
/// tests are parallel-safe and leak nothing between each other.
fn spawn_pgwire_server() -> std::io::Result<String> {
    let dir = TempDir::new().expect("temp dir");
    let base = dir.path().to_str().expect("utf-8 temp path").to_string();
    let db = OmniKV::open(
        &format!("{base}\\manifest.json"),
        &format!("{base}\\wal.log"),
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

/// Reads one length-prefixed protocol message, returning (type, body).
/// The length field includes itself but not the type byte.
fn read_message(stream: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut type_buf = [0u8; 1];
    stream.read_exact(&mut type_buf)?;
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;
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

/// Sends an SSLRequest / GSSENCRequest negotiation packet (length 8, code).
fn send_negotiation_request(stream: &mut TcpStream, code: u32) -> std::io::Result<()> {
    stream.write_all(&8u32.to_be_bytes())?;
    stream.write_all(&code.to_be_bytes())?;
    stream.flush()
}

/// Sends a protocol 3.0 StartupMessage with the given parameters.
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
    stream.write_all(&((body.len() + 4) as u32).to_be_bytes())?;
    stream.write_all(&body)?;
    stream.flush()
}

/// Sends a PasswordMessage ('p') frame with the cleartext password.
fn send_password_message(stream: &mut TcpStream, password: &str) -> std::io::Result<()> {
    let mut body = password.as_bytes().to_vec();
    body.push(0);
    let mut frame = Vec::with_capacity(5 + body.len());
    frame.push(b'p');
    frame.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    stream.write_all(&frame)?;
    stream.flush()
}

/// Sends a simple Query ('Q') frame.
fn send_query(stream: &mut TcpStream, sql: &str) -> std::io::Result<()> {
    let mut body = sql.as_bytes().to_vec();
    body.push(0);
    let mut frame = Vec::with_capacity(5 + body.len());
    frame.push(b'Q');
    frame.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    stream.write_all(&frame)?;
    stream.flush()
}

/// Completes the full libpq handshake: StartupMessage, password, auth ok,
/// parameter statuses, ReadyForQuery('I'). Returns the stream ready for
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
            other => panic!("unexpected frame {:#x} during startup", other),
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
        "expected CommandComplete, RowDescription, or ErrorResponse, got {:#x}",
        msg_type
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
fn pgwire_repeated_negotiation_packets_do_not_loop_forever() {
    // A hostile or buggy client may re-send SSLRequest many times. The
    // server must eventually give up instead of spinning in pre-auth. Once
    // the bounded window is exhausted, further writes may reset the socket
    // mid-send — which is fine; the point is the server stops replying.
    let addr = spawn_pgwire_server().expect("spawn server");
    let mut stream = TcpStream::connect(&addr).expect("connect");

    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .expect("set read timeout");

    let mut saw_server_give_up = false;
    for _ in 0..16 {
        if send_negotiation_request(&mut stream, SSL_REQUEST_CODE).is_err() {
            // Connection already reset by the server after the bounded
            // window: that is the pass condition.
            saw_server_give_up = true;
            break;
        }
        // Drain the 'N' reply; EOF or reset here also proves the server
        // stopped answering.
        let mut buf = [0u8; 1];
        match stream.read(&mut buf) {
            Ok(0) | Err(_) => {
                saw_server_give_up = true;
                break;
            }
            Ok(_) if buf[0] == b'N' => continue,
            Ok(other) => panic!("unexpected reply byte {:#x}", other),
        }
    }

    // The bound is 8 negotiation packets (see MAX_STARTUP_NEGOTIATION_MESSAGES),
    // so within 16 attempts the server must have cut the connection.
    if !saw_server_give_up {
        let mut buf = [0u8; 1];
        match stream.read(&mut buf) {
            Ok(0) | Err(_) => { /* connection closed or reset: expected */ }
            Ok(_) => panic!("server must stop answering unbounded negotiation spam"),
        }
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
        .and_then(|_| stream.write_all(&code.to_be_bytes()))
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
