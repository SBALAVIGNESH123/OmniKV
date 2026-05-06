//! QUIC/HTTP3 Binary Transport
//!
//! High-performance binary protocol over QUIC for inter-node
//! Raft consensus and cluster communication. Uses Quinn for
//! UDP-based, TLS 1.3 encrypted transport.

use std::sync::Arc;
use quinn::{Endpoint, ServerConfig, ClientConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use omni_engine::{OmniKV, WriteBatch};

/// Binary protocol command opcodes.
#[repr(u8)]
pub enum OpCode {
    Get = 0x01,
    Set = 0x02,
    Delete = 0x03,
    Scan = 0x04,
    Ping = 0x10,
    Pong = 0x11,
    RaftAppend = 0x20,
    RaftVote = 0x21,
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
            0x20 => Some(Self::RaftAppend),
            0x21 => Some(Self::RaftVote),
            _ => None,
        }
    }
}

/// Generate self-signed TLS certificates for QUIC.
pub fn generate_self_signed_cert() -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), String> {
    let key_pair = rcgen::KeyPair::generate()
        .map_err(|e| format!("Key generation failed: {}", e))?;
    
    let mut params = rcgen::CertificateParams::new(vec!["localhost".into()])
        .map_err(|e| format!("Cert params failed: {}", e))?;
    
    let cert = params.self_signed(&key_pair)
        .map_err(|e| format!("Self-signed cert failed: {}", e))?;
    
    let cert_der = cert.der().clone();
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_pair.serialized_der().to_vec()));

    Ok((vec![cert_der], key_der))
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

    let addr = bind_addr.parse().map_err(|e| format!("Parse addr: {}", e))?;
    let endpoint = Endpoint::server(server_config, addr)
        .map_err(|e| format!("QUIC bind: {}", e))?;

    Ok(endpoint)
}

/// Create a QUIC client endpoint for connecting to peers.
pub fn create_client_endpoint() -> Result<Endpoint, String> {
    let mut crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();

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
pub async fn run_quic_server(endpoint: Endpoint, db: Arc<OmniKV>) {
    tracing::info!("QUIC/HTTP3 server listening on {}", endpoint.local_addr().unwrap());

    while let Some(incoming) = endpoint.accept().await {
        let db = db.clone();
        tokio::spawn(async move {
            match incoming.await {
                Ok(conn) => {
                    tracing::debug!("QUIC connection from {}", conn.remote_address());
                    loop {
                        match conn.accept_bi().await {
                            Ok((mut send, mut recv)) => {
                                let db = db.clone();
                                tokio::spawn(async move {
                                    let mut buf = vec![0u8; 65536];
                                    match recv.read(&mut buf).await {
                                        Ok(Some(n)) if n > 0 => {
                                            let response = handle_binary_request(&db, &buf[..n]);
                                            let _ = send.write_all(&response).await;
                                            let _ = send.finish();
                                        }
                                        _ => {}
                                    }
                                });
                            }
                            Err(_) => break,
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("QUIC connection error: {}", e);
                }
            }
        });
    }
}

/// Handle a single binary protocol request.
fn handle_binary_request(db: &Arc<OmniKV>, data: &[u8]) -> Vec<u8> {
    if data.is_empty() {
        return vec![0xFF]; // Error: empty request
    }

    let op = data[0];
    let payload = &data[1..];

    match OpCode::from_u8(op) {
        Some(OpCode::Ping) => vec![OpCode::Pong as u8],

        Some(OpCode::Get) => {
            let key = String::from_utf8_lossy(payload);
            let seq = db.get_seq();
            match db.find(&key, seq) {
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
            if payload.len() < 2 { return vec![0xFF]; }
            let key_len = u16::from_le_bytes([payload[0], payload[1]]) as usize;
            if payload.len() < 2 + key_len { return vec![0xFF]; }

            let key = String::from_utf8_lossy(&payload[2..2 + key_len]).to_string();
            let value = String::from_utf8_lossy(&payload[2 + key_len..]).to_string();

            let mut batch = WriteBatch::new();
            if batch.set(&key, value).is_ok() {
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
            let key = String::from_utf8_lossy(payload);
            let mut batch = WriteBatch::new();
            if batch.delete(&key).is_ok() {
                match db.commit_batch(&batch) {
                    Ok(_) => vec![0x00],
                    Err(_) => vec![0xFF],
                }
            } else {
                vec![0xFF]
            }
        }

        _ => vec![0xFF], // Unknown opcode
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
