//! TLS identity for the HTTP/2 and QUIC listeners.
//!
//! Operator material is used when configured; the self-signed fallback needs
//! development mode or an explicit opt-in, and production with no certificate
//! fails closed rather than presenting an identity nobody can verify.

use crate::quic_server;
use omni_engine::config::{ServerConfig, ServerMode};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// TLS material for the listeners. Both listeners present the same identity.
pub struct ServerTls {
    /// Certificate chain, leaf first.
    pub certs: Vec<CertificateDer<'static>>,
    pub key: PrivateKeyDer<'static>,
    pub posture: TlsPosture,
}

/// The identity the listeners present, and why it is acceptable.
#[derive(Clone, Debug)]
pub enum TlsPosture {
    /// Operator-supplied PEM files from the configuration.
    Operator {
        cert_path: String,
        key_path: String,
        n_certs: usize,
    },
    /// Generated in this process. Safe only for development, or when the
    /// operator explicitly accepted unverifiable TLS.
    EphemeralSelfSigned { permitted_by: &'static str },
}

impl ServerTls {
    /// Clone for a second listener. `clone_key` is spelled out so secret
    /// material is never copied by an auto-derived `Clone`.
    pub fn for_second_listener(&self) -> Self {
        Self {
            certs: self.certs.clone(),
            key: self.key.clone_key(),
            posture: self.posture.clone(),
        }
    }
}

/// Resolve the TLS identity from the configuration.
///
/// A server that silently falls back to an untrusted identity is worse than
/// one that does not start.
pub fn resolve_server_tls(cfg: &ServerConfig) -> Result<ServerTls, String> {
    match (&cfg.tls_cert_path, &cfg.tls_key_path) {
        (Some(cert_path), Some(key_path)) => {
            let certs = load_certificate_chain(cert_path)?;
            let key = load_private_key(key_path)?;
            Ok(ServerTls {
                posture: TlsPosture::Operator {
                    cert_path: cert_path.clone(),
                    key_path: key_path.clone(),
                    n_certs: certs.len(),
                },
                certs,
                key,
            })
        }
        // Half-configured TLS is a misconfiguration, not a fallback case.
        (Some(_), None) | (None, Some(_)) => Err(
            "tls_cert_path and tls_key_path must be set together: one is configured without the other".into(),
        ),
        (None, None) => {
            let permitted_by = if cfg.tls_insecure_skip {
                "tls_insecure_skip=true"
            } else if cfg.mode == ServerMode::Development {
                "development mode"
            } else {
                return Err(
                    "production mode requires tls_cert_path + tls_key_path (or OMNIKV_TLS_INSECURE_SKIP=true): \
                     refusing to generate an unverifiable self-signed certificate"
                        .into(),
                );
            };
            let (certs, key) = quic_server::generate_self_signed_cert()?;
            Ok(ServerTls {
                posture: TlsPosture::EphemeralSelfSigned { permitted_by },
                certs,
                key,
            })
        }
    }
}

/// Log the active TLS posture. An ephemeral certificate in production is a
/// silent security regression, so it is visible on every boot.
pub fn log_tls_posture(posture: &TlsPosture) {
    match posture {
        TlsPosture::Operator {
            cert_path,
            key_path,
            n_certs,
        } => {
            tracing::info!(
                "TLS: operator certificate ({n_certs} cert(s) in chain) loaded from {cert_path}, key from {key_path}"
            );
        }
        TlsPosture::EphemeralSelfSigned { permitted_by } => {
            tracing::warn!(
                "TLS: EPHEMERAL SELF-SIGNED certificate generated at boot ({permitted_by}) — \
                 NOT production-safe: clients cannot verify this server's identity"
            );
        }
    }
}

/// Read a PEM certificate chain (leaf first, then intermediates).
fn load_certificate_chain(path: &str) -> Result<Vec<CertificateDer<'static>>, String> {
    let iter = CertificateDer::pem_file_iter(path)
        .map_err(|e| format!("cannot read TLS certificate chain from {path}: {e}"))?;
    let mut certs = Vec::new();
    for item in iter {
        let der = item.map_err(|e| format!("malformed PEM in TLS certificate file {path}: {e}"))?;
        certs.push(der);
    }
    if certs.is_empty() {
        return Err(format!(
            "TLS certificate file {path} contains no CERTIFICATE PEM sections"
        ));
    }
    Ok(certs)
}

/// Read a PEM private key (PKCS#8, PKCS#1, or SEC1).
fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>, String> {
    PrivateKeyDer::from_pem_file(path)
        .map_err(|e| format!("cannot load TLS private key from {path}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Build a test CA and a server certificate it signs, with `localhost`
    /// as the SAN, and write the server material to temp PEM files.
    fn write_operator_material() -> (tempfile::TempPath, tempfile::TempPath, Vec<u8>) {
        let ca_params = {
            let mut p = rcgen::CertificateParams::new(Vec::new()).unwrap();
            p.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
            p.distinguished_name
                .push(rcgen::DnType::CommonName, "OmniKV Test CA");
            p
        };
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        let issuer = rcgen::Issuer::from_params(&ca_params, &ca_key);

        let server_params = {
            let mut p = rcgen::CertificateParams::new(vec!["localhost".into()]).unwrap();
            p.distinguished_name
                .push(rcgen::DnType::CommonName, "localhost");
            p
        };
        let server_key = rcgen::KeyPair::generate().unwrap();
        let server_cert = server_params.signed_by(&server_key, &issuer).unwrap();

        let mut cert_file = tempfile::NamedTempFile::new().unwrap();
        cert_file.write_all(server_cert.pem().as_bytes()).unwrap();
        let mut key_file = tempfile::NamedTempFile::new().unwrap();
        key_file
            .write_all(server_key.serialize_pem().as_bytes())
            .unwrap();

        (
            cert_file.into_temp_path(),
            key_file.into_temp_path(),
            ca_cert.der().to_vec(),
        )
    }

    fn cfg_with_paths(cert: &str, key: &str) -> ServerConfig {
        let mut cfg = ServerConfig::load_dev().unwrap();
        cfg.tls_cert_path = Some(cert.to_string());
        cfg.tls_key_path = Some(key.to_string());
        cfg
    }

    /// Configured cert paths must produce the operator's certificate, not a
    /// freshly generated one. The bytes on the wire are asserted by
    /// [`client_anchored_on_operator_ca_handshakes_and_sees_operator_cert`].
    #[test]
    fn operator_certs_are_loaded_not_generated() {
        let (cert_path, key_path, _ca_der) = write_operator_material();
        let cfg = cfg_with_paths(cert_path.to_str().unwrap(), key_path.to_str().unwrap());
        let tls = resolve_server_tls(&cfg).expect("operator material should load");

        match &tls.posture {
            TlsPosture::Operator { n_certs, .. } => assert_eq!(*n_certs, 1),
            other @ TlsPosture::EphemeralSelfSigned { .. } => {
                panic!("expected operator posture, got {other:?}")
            }
        }
        assert_eq!(tls.certs.len(), 1);
    }

    /// Development with no operator material falls back to self-signed.
    #[test]
    fn dev_mode_without_certs_falls_back_ephemerally() {
        let cfg = ServerConfig::load_dev().unwrap();
        let tls = resolve_server_tls(&cfg).expect("dev fallback should be permitted");
        match &tls.posture {
            TlsPosture::EphemeralSelfSigned { permitted_by } => {
                assert!(permitted_by.contains("development mode"));
            }
            other @ TlsPosture::Operator { .. } => {
                panic!("expected ephemeral posture in dev, got {other:?}")
            }
        }
        assert!(!tls.certs.is_empty());
    }

    /// Production with no cert and no opt-in must fail closed — never
    /// silently serve an unverifiable identity.
    #[test]
    fn production_without_certs_refuses_to_start() {
        let mut cfg = ServerConfig::load_dev().unwrap();
        cfg.mode = ServerMode::Production;
        cfg.tls_insecure_skip = false;
        let err = match resolve_server_tls(&cfg) {
            Ok(_) => panic!("production with no cert material must not start"),
            Err(e) => e,
        };
        assert!(
            err.contains("refusing to generate an unverifiable self-signed certificate"),
            "unexpected error: {err}"
        );
    }

    /// `tls_insecure_skip` is an explicit operator opt-in, so it permits
    /// the ephemeral fallback even in production.
    #[test]
    fn insecure_skip_permits_ephemeral_in_production() {
        let mut cfg = ServerConfig::load_dev().unwrap();
        cfg.mode = ServerMode::Production;
        cfg.tls_insecure_skip = true;
        let tls = resolve_server_tls(&cfg).expect("explicit opt-in should be honored");
        match &tls.posture {
            TlsPosture::EphemeralSelfSigned { permitted_by } => {
                assert!(permitted_by.contains("tls_insecure_skip"));
            }
            other @ TlsPosture::Operator { .. } => {
                panic!("expected ephemeral posture, got {other:?}")
            }
        }
    }

    /// Configuring a cert without a key (or the reverse) is a misconfiguration
    /// that must be rejected rather than papered over with a fallback.
    #[test]
    fn half_configured_tls_is_rejected() {
        let (cert_path, _key_path, _ca) = write_operator_material();
        let mut cfg = ServerConfig::load_dev().unwrap();
        cfg.tls_cert_path = Some(cert_path.to_str().unwrap().to_string());
        match resolve_server_tls(&cfg) {
            Ok(_) => panic!("a cert path without a key path must be rejected"),
            Err(e) => assert!(e.contains("set together"), "unexpected error: {e}"),
        }
    }

    /// A client anchored on the operator's CA completes a real TLS handshake
    /// with material loaded by [`resolve_server_tls`] and sees the operator's
    /// certificate.
    #[test]
    fn client_anchored_on_operator_ca_handshakes_and_sees_operator_cert() {
        use rustls::{ClientConfig, RootCertStore, ServerConfig};
        use std::io::Read;

        // rustls cannot auto-pick a provider: the dependency graph pulls in
        // both ring and aws-lc-rs (jsonwebtoken). Pin the one the server uses.
        let _ = rustls::crypto::ring::default_provider().install_default();

        let (cert_path, key_path, ca_der) = write_operator_material();
        let cfg = cfg_with_paths(cert_path.to_str().unwrap(), key_path.to_str().unwrap());
        let tls = resolve_server_tls(&cfg).expect("operator material should load");

        let expected_der = tls.certs[0].as_ref().to_vec();

        let server_cfg = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(tls.certs, tls.key)
            .expect("loaded material must build a usable server config");

        // The client trusts only the operator's CA.
        let mut roots = RootCertStore::empty();
        roots
            .add(rustls::pki_types::CertificateDer::from(ca_der))
            .unwrap();
        let client_cfg = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();

        // Handshake over a loopback socket, both sides driven in lockstep
        // from this thread.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        // Connect before accept, or this thread would block forever.
        let client_io = std::net::TcpStream::connect(addr).unwrap();
        let server_io = listener.accept().unwrap().0;
        let (mut client_io, mut server_io) = (client_io, server_io);
        let mut server_conn =
            rustls::ServerConnection::new(std::sync::Arc::new(server_cfg)).unwrap();
        let mut client_conn = rustls::ClientConnection::new(
            std::sync::Arc::new(client_cfg),
            "localhost".try_into().unwrap(),
        )
        .unwrap();

        // ClientHello -> server
        client_conn.write_tls(&mut client_io).unwrap();
        server_conn.read_tls(&mut server_io).unwrap();
        server_conn.process_new_packets().unwrap();
        // Server flight (cert + Finished) -> client
        server_conn.write_tls(&mut server_io).unwrap();
        client_conn.read_tls(&mut client_io).unwrap();
        client_conn.process_new_packets().unwrap();
        // Client Finished -> server
        client_conn.write_tls(&mut client_io).unwrap();
        server_conn.read_tls(&mut server_io).unwrap();
        server_conn.process_new_packets().unwrap();

        // A client that trusted the operator CA got past the handshake.
        assert!(
            !client_conn.is_handshaking(),
            "client anchored on the operator CA failed to complete the handshake"
        );
        assert!(
            !server_conn.is_handshaking(),
            "server failed to complete the handshake with the operator material"
        );
        // And it saw exactly the operator's certificate on the wire.
        let presented = client_conn
            .peer_certificates()
            .expect("client should have the server certificate");
        assert_eq!(
            presented[0].as_ref(),
            expected_der.as_slice(),
            "the certificate on the wire is not the operator's certificate"
        );

        // Application data. rustls' Reader returns WouldBlock when no
        // plaintext is buffered, so read one record, not to EOF.
        client_conn
            .writer()
            .write_all(b"ping")
            .expect("client should send app data");
        client_conn.write_tls(&mut client_io).unwrap();
        server_conn.read_tls(&mut server_io).unwrap();
        server_conn.process_new_packets().unwrap();
        let mut got = [0u8; 8];
        let n = server_conn.reader().read(&mut got).unwrap();
        assert_eq!(&got[..n], b"ping");
    }
}
