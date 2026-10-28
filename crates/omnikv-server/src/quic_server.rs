//! QUIC/HTTP3 Binary Transport
//!
//! High-performance binary protocol over QUIC for client data operations.
//! Consensus RPCs use the authenticated Raft HTTP transport instead. Uses
//! Quinn for UDP-based, TLS 1.3 encrypted transport.

use omni_engine::hardening::RateLimiter;
use omni_engine::metrics_prometheus;
use omni_engine::{OmniKV, WriteBatch};
use quinn::{ClientConfig, Endpoint, RecvStream, ServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::sync::Arc;

/// Binary protocol command opcodes.
#[repr(u8)]
pub enum OpCode {
    Get = 0x01,
    Set = 0x02,
    Delete = 0x03,
    Scan = 0x04,
    Ping = 0x10,
    Pong = 0x11,
}

impl OpCode {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(Self::Get),
            0x02 => Some(Self::Set),
            0x03 => Some(Self::Delete),
            0x04 => Some(Self::Scan),
            0x10 => Some(Self::Ping),
            0x11 => Some(Self::Pong),
            _ => None,
        }
    }
}

/// Maximum bytes in one QUIC request frame, including the length-prefixed
/// authentication header and binary command payload. This is deliberately
/// above the engine's 10 MiB value limit so a valid maximum-sized SET can be
/// represented without making the transport an accidental second limit.
pub const MAX_QUIC_FRAME_BYTES: usize = 16 * 1024 * 1024;
const QUIC_FRAME_LENGTH_BYTES: usize = 4;
const QUIC_AUTH_LENGTH_BYTES: usize = 2;
const MAX_QUIC_AUTH_BYTES: usize = 4096;

/// The certificate and private key a listener presents. Operator-supplied
/// Generate self-signed TLS certificates for QUIC.
pub fn generate_self_signed_cert()
-> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), String> {
    let key_pair =
        rcgen::KeyPair::generate().map_err(|e| format!("Key generation failed: {}", e))?;

    let mut params = rcgen::CertificateParams::new(vec!["localhost".into()])
        .map_err(|e| format!("Cert params failed: {}", e))?;

    let cert = params
        .self_signed(&key_pair)
        .map_err(|e| format!("Self-signed cert failed: {}", e))?;

    let cert_der = cert.der().clone();
    let key_der =
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_pair.serialized_der().to_vec()));

    Ok((vec![cert_der], key_der))
}

/// Build a TLS 1.3 acceptor for the TCP command interface. The caller
/// supplies its own freshly generated key pair — this is not the QUIC
/// endpoint's certificate, and a restart mints a new one, so a client that
/// pins the leaf must re-pin after every boot.
pub fn build_tcp_tls_acceptor(
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> Result<tokio_rustls::TlsAcceptor, String> {
    // TLS 1.3 only: 1.2 has no reason to exist on a single-vendor interface
    // and accepting it widens the downgrade surface.
    let mut config =
        rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| format!("TLS config: {}", e))?;
    config.alpn_protocols = vec![b"omnikv/1".to_vec()];
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

/// Build the server-side TLS configuration for the PgWire listener.
///
/// Unlike the TCP command interface this accepts TLS 1.2 as well as 1.3:
/// PgWire exists for client compatibility, and older libpq builds negotiate
/// 1.2 only. PostgreSQL clients do not use ALPN, so none is advertised.
pub fn pgwire_tls_config(
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> Result<Arc<rustls::ServerConfig>, String> {
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("TLS config: {}", e))?;
    Ok(Arc::new(config))
}

/// Create a QUIC server endpoint.
pub fn create_server_endpoint(
    bind_addr: &str,
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> Result<Endpoint, String> {
    let mut server_crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("TLS config: {}", e))?;

    server_crypto.alpn_protocols = vec![b"omnikv/1".to_vec()];

    let server_config = ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto)
            .map_err(|e| format!("QUIC server config: {}", e))?,
    ));

    let addr = bind_addr
        .parse()
        .map_err(|e| format!("Parse addr: {}", e))?;
    let endpoint =
        Endpoint::server(server_config, addr).map_err(|e| format!("QUIC bind: {}", e))?;

    Ok(endpoint)
}

/// Create a QUIC client endpoint for connecting to peers.
pub fn create_client_endpoint(insecure_skip_verify: bool) -> Result<Endpoint, String> {
    // Production guard: reject insecure mode in release builds
    #[cfg(not(debug_assertions))]
    if insecure_skip_verify {
        return Err(
            "QUIC insecure_skip_verify is not permitted in production (release) builds".to_string(),
        );
    }

    // Development warning when cert verification is disabled
    #[cfg(debug_assertions)]
    if insecure_skip_verify {
        tracing::warn!(
            "SECURITY WARNING: QUIC certificate verification is DISABLED. \
             Only acceptable in development/test mode."
        );
    }

    let mut crypto = if insecure_skip_verify {
        rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
            .with_no_client_auth()
    } else {
        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth()
    };
    crypto.alpn_protocols = vec![b"omnikv/1".to_vec()];

    let client_config = ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
            .map_err(|e| format!("QUIC client config: {}", e))?,
    ));

    let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap())
        .map_err(|e| format!("Client endpoint: {}", e))?;

    endpoint.set_default_client_config(client_config);
    Ok(endpoint)
}

/// Run the QUIC server loop, handling binary protocol requests.
pub async fn run_quic_server(endpoint: Endpoint, db: Arc<OmniKV>, rate_limiter: Arc<RateLimiter>) {
    tracing::info!(
        "QUIC/HTTP3 server listening on {}",
        endpoint.local_addr().unwrap()
    );

    while let Some(incoming) = endpoint.accept().await {
        let db = db.clone();
        let rate_limiter = rate_limiter.clone();
        tokio::spawn(async move {
            match incoming.await {
                Ok(conn) => {
                    tracing::debug!("QUIC connection from {}", conn.remote_address());
                    let jwt_secret = std::env::var("OMNI_JWT_SECRET").unwrap_or_default();
                    while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                        let db = db.clone();
                        let secret = jwt_secret.clone();
                        let rate_limiter = rate_limiter.clone();
                        tokio::spawn(async move {
                            let response = match read_quic_frame(&mut recv).await {
                                Ok(frame) => handle_authenticated_request(
                                    &db,
                                    &frame,
                                    &secret,
                                    &rate_limiter,
                                ),
                                Err(error) => format!("ERR {error}\n").into_bytes(),
                            };
                            let _ = send.write_all(&response).await;
                            let _ = send.finish();
                        });
                    }
                }
                Err(e) => {
                    tracing::warn!("QUIC connection error: {}", e);
                }
            }
        });
    }
}

/// Read exactly one length-prefixed request frame from a QUIC stream.
///
/// QUIC stream reads are allowed to split an application frame at any byte
/// boundary. The old one-read implementation treated a partial read as a
/// complete request and silently capped every request at 64 KiB. This helper
/// reads the fixed length first, validates the explicit cap before allocating,
/// and then loops until the whole frame arrives.
async fn read_quic_frame(recv: &mut RecvStream) -> Result<Vec<u8>, String> {
    let mut length_bytes = [0u8; QUIC_FRAME_LENGTH_BYTES];
    read_quic_exact(recv, &mut length_bytes).await?;
    let frame_len = u32::from_le_bytes(length_bytes) as usize;

    validate_quic_frame_length(frame_len)?;

    let mut frame = vec![0u8; frame_len];
    read_quic_exact(recv, &mut frame).await?;
    Ok(frame)
}

fn validate_quic_frame_length(frame_len: usize) -> Result<(), String> {
    if frame_len < QUIC_AUTH_LENGTH_BYTES + 1 {
        return Err("INVALID_FRAME".to_string());
    }
    if frame_len > MAX_QUIC_FRAME_BYTES {
        return Err("FRAME_TOO_LARGE".to_string());
    }
    Ok(())
}

async fn read_quic_exact(recv: &mut RecvStream, buf: &mut [u8]) -> Result<(), String> {
    let mut offset = 0;
    while offset < buf.len() {
        match recv.read(&mut buf[offset..]).await {
            Ok(Some(0) | None) => return Err("TRUNCATED_FRAME".to_string()),
            Ok(Some(read)) => offset += read,
            Err(error) => return Err(format!("READ_ERROR: {error}")),
        }
    }
    Ok(())
}

/// Handle a single length-prefixed binary protocol request body.
///
/// The body format is `[auth_len:u16][auth_token][opcode][payload...]`.
/// Length-prefixing the token keeps authentication framing independent from
/// binary payload bytes; a token or value can never be split on an accidental
/// newline.
fn handle_authenticated_request(
    db: &Arc<OmniKV>,
    frame: &[u8],
    jwt_secret: &str,
    rate_limiter: &RateLimiter,
) -> Vec<u8> {
    if jwt_secret.is_empty() {
        tracing::error!("OMNI_JWT_SECRET not set — rejecting QUIC request");
        return b"ERR AUTH_NOT_CONFIGURED\n".to_vec();
    }

    if frame.len() < QUIC_AUTH_LENGTH_BYTES + 1 {
        return b"ERR INVALID_FRAME\n".to_vec();
    }

    let auth_len = u16::from_le_bytes([frame[0], frame[1]]) as usize;
    if auth_len == 0 || auth_len > MAX_QUIC_AUTH_BYTES {
        return b"ERR INVALID_AUTH_HEADER\n".to_vec();
    }

    let auth_start = QUIC_AUTH_LENGTH_BYTES;
    let payload_start = auth_start + auth_len;
    if payload_start >= frame.len() {
        return b"ERR INVALID_FRAME\n".to_vec();
    }

    let token = match std::str::from_utf8(&frame[auth_start..payload_start]) {
        Ok(token) if !token.is_empty() => token,
        Ok(_) => return b"ERR MISSING_AUTH_TOKEN\n".to_vec(),
        Err(_) => return b"ERR INVALID_AUTH_HEADER\n".to_vec(),
    };

    let claims = match crate::auth::verify_token(token, jwt_secret) {
        Ok(claims) => claims,
        Err(_) => {
            tracing::warn!("QUIC JWT verification failed");
            return b"ERR INVALID_TOKEN\n".to_vec();
        }
    };

    let identity = format!("quic:user:{}", claims.sub);
    if let Err(retry_after_ms) = acquire_quic_request_permit(rate_limiter, &identity) {
        return format!("ERR RATE_LIMITED retry_after_ms={retry_after_ms}\n").into_bytes();
    }

    handle_binary_request(db, &frame[payload_start..])
}

fn invalid_utf8_response() -> Vec<u8> {
    b"ERR INVALID_UTF8\n".to_vec()
}

fn invalid_frame_response() -> Vec<u8> {
    b"ERR INVALID_FRAME\n".to_vec()
}

fn decode_utf8_payload(payload: &[u8]) -> Result<&str, Vec<u8>> {
    std::str::from_utf8(payload).map_err(|_| invalid_utf8_response())
}

fn handle_binary_request(db: &Arc<OmniKV>, data: &[u8]) -> Vec<u8> {
    if data.is_empty() {
        return invalid_frame_response();
    }

    let op = data[0];
    let payload = &data[1..];

    match OpCode::from_u8(op) {
        Some(OpCode::Ping) => vec![OpCode::Pong as u8],

        Some(OpCode::Get) => {
            let key = match decode_utf8_payload(payload) {
                Ok(key) => key,
                Err(response) => return response,
            };
            let seq = db.get_seq();
            match db.find(key, seq) {
                Ok(Some(val)) => {
                    let mut resp = vec![0x00]; // Success
                    resp.extend_from_slice(val.as_bytes());
                    resp
                }
                Ok(None) => vec![0x01], // Not found
                Err(_) => vec![0xFF],   // Error
            }
        }

        Some(OpCode::Set) => {
            // Payload format: [key_len: u16][key][value]
            if payload.len() < 2 {
                return invalid_frame_response();
            }
            let key_len = u16::from_le_bytes([payload[0], payload[1]]) as usize;
            if payload.len() < 2 + key_len {
                return invalid_frame_response();
            }

            let key = match decode_utf8_payload(&payload[2..2 + key_len]) {
                Ok(key) => key,
                Err(response) => return response,
            };
            let value = match decode_utf8_payload(&payload[2 + key_len..]) {
                Ok(value) => value,
                Err(response) => return response,
            };

            let mut batch = WriteBatch::new();
            if batch.set(key, value.to_string()).is_ok() {
                match db.commit_batch(&batch) {
                    Ok(seq) => {
                        let mut resp = vec![0x00];
                        resp.extend_from_slice(&seq.to_le_bytes());
                        resp
                    }
                    Err(_) => vec![0xFF],
                }
            } else {
                vec![0xFF]
            }
        }

        Some(OpCode::Delete) => {
            let key = match decode_utf8_payload(payload) {
                Ok(key) => key,
                Err(response) => return response,
            };
            let mut batch = WriteBatch::new();
            if batch.delete(key).is_ok() {
                match db.commit_batch(&batch) {
                    Ok(_) => vec![0x00],
                    Err(_) => vec![0xFF],
                }
            } else {
                vec![0xFF]
            }
        }

        Some(OpCode::Scan) => b"ERR UNSUPPORTED_OPCODE\n".to_vec(),
        None => b"ERR UNKNOWN_OPCODE\n".to_vec(),
        Some(OpCode::Pong) => b"ERR INVALID_OPCODE\n".to_vec(),
    }
}

fn acquire_quic_request_permit(rate_limiter: &RateLimiter, identity: &str) -> Result<(), u64> {
    rate_limiter
        .try_acquire(identity)
        .map(|_| ())
        .inspect_err(|_| {
            metrics_prometheus::record_rate_limit_rejection("quic");
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_temp_db() -> (Arc<OmniKV>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let manifest = dir.path().join("manifest.json");
        let wal = dir.path().join("wal.bin");
        let db = OmniKV::open(
            manifest.to_str().expect("manifest path"),
            wal.to_str().expect("wal path"),
        )
        .expect("open db");
        (db, dir)
    }

    fn authenticated_frame(token: &str, payload: &[u8]) -> Vec<u8> {
        let token = token.as_bytes();
        let token_len = u16::try_from(token.len()).expect("test token must fit in u16");
        let mut frame = Vec::with_capacity(2 + token.len() + payload.len());
        frame.extend_from_slice(&token_len.to_le_bytes());
        frame.extend_from_slice(token);
        frame.extend_from_slice(payload);
        frame
    }

    #[test]
    fn quic_rate_limiter_rejects_abusive_authenticated_streams() {
        let (db, _dir) = open_temp_db();
        let secret = "0123456789abcdef0123456789abcdef";
        let token = crate::auth::generate_token("client-a", "write", secret, 60).expect("token");
        let frame = authenticated_frame(&token, &[OpCode::Ping as u8]);
        let rate_limiter = RateLimiter::new(0.01, 1, 10);

        assert_eq!(
            handle_authenticated_request(&db, &frame, secret, &rate_limiter),
            vec![OpCode::Pong as u8]
        );
        let second = handle_authenticated_request(&db, &frame, secret, &rate_limiter);
        assert!(String::from_utf8_lossy(&second).starts_with("ERR RATE_LIMITED"));
    }

    #[test]
    fn invalid_utf8_is_rejected_instead_of_lossily_rewritten() {
        let (db, _dir) = open_temp_db();
        let secret = "0123456789abcdef0123456789abcdef";
        let token = crate::auth::generate_token("client-a", "write", secret, 60).expect("token");
        let frame = authenticated_frame(&token, &[OpCode::Get as u8, 0xff]);
        let rate_limiter = RateLimiter::new(100.0, 10, 10);

        assert_eq!(
            handle_authenticated_request(&db, &frame, secret, &rate_limiter),
            b"ERR INVALID_UTF8\n"
        );
    }

    #[test]
    fn raft_opcodes_are_rejected_as_unknown_on_the_client_transport() {
        let (db, _dir) = open_temp_db();
        let secret = "0123456789abcdef0123456789abcdef";
        let token = crate::auth::generate_token("client-a", "write", secret, 60).expect("token");
        let frame = authenticated_frame(&token, &[0x20]);
        let rate_limiter = RateLimiter::new(100.0, 10, 10);

        assert_eq!(
            handle_authenticated_request(&db, &frame, secret, &rate_limiter),
            b"ERR UNKNOWN_OPCODE\n"
        );
    }

    #[test]
    fn frame_length_validation_is_explicit_and_bounded() {
        assert_eq!(
            validate_quic_frame_length(2),
            Err("INVALID_FRAME".to_string())
        );
        assert_eq!(
            validate_quic_frame_length(MAX_QUIC_FRAME_BYTES + 1),
            Err("FRAME_TOO_LARGE".to_string())
        );
        assert!(validate_quic_frame_length(3).is_ok());
    }
}

/// Skip server certificate verification (for self-signed certs in cluster).
#[derive(Debug)]
struct SkipServerVerification;

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
}
