use super::*;
use axum::{Json, Router, routing::post};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{VoteRequest, VoteResponse};
use std::time::Duration;

struct TestCa {
    params: rcgen::CertificateParams,
    key: rcgen::KeyPair,
    pem: Vec<u8>,
}

impl TestCa {
    fn new() -> Self {
        let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let key = rcgen::KeyPair::generate().unwrap();
        let pem = params.self_signed(&key).unwrap().pem().into_bytes();
        Self { params, key, pem }
    }

    fn peer(&self, host: &str) -> (ServerTls, Vec<u8>) {
        let mut params = rcgen::CertificateParams::new(vec![host.to_string()]).unwrap();
        params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let key = rcgen::KeyPair::generate().unwrap();
        let issuer = rcgen::Issuer::from_params(&self.params, &self.key);
        let cert = params.signed_by(&key, &issuer).unwrap();
        let identity = format!("{}{}", cert.pem(), key.serialize_pem()).into_bytes();
        let tls = ServerTls {
            certs: vec![cert.der().clone()],
            key: rustls::pki_types::PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
            posture: crate::tls::TlsPosture::Operator {
                cert_path: "test certificate".into(),
                key_path: "test key".into(),
                n_certs: 1,
            },
        };
        (tls, identity)
    }
}

fn mock_peer(tls: ServerTls, ca: &[u8]) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let config = raft_server_tls_config(tls, Some(ca)).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new().route(
        "/raft/vote",
        post(|Json(request): Json<VoteRequest<u64>>| async move {
            Json(VoteResponse::new(request.vote, None, true))
        }),
    );
    let server = axum_server::from_tcp_rustls(listener, config).unwrap();
    let handle = tokio::spawn(async move {
        server.serve(app.into_make_service()).await.unwrap();
    });
    (addr, handle)
}

async fn vote_succeeds(mut network: OmniNetwork, addr: SocketAddr) -> bool {
    let mut client = network
        .new_client(
            2,
            &BasicNode {
                addr: addr.to_string(),
            },
        )
        .await;
    matches!(
        client
            .vote(
                VoteRequest::new(openraft::Vote::new(1, 1), None),
                RPCOption::new(Duration::from_secs(2)),
            )
            .await,
        Ok(response) if response.vote_granted
    )
}

#[tokio::test]
async fn raft_mutual_tls_accepts_trusted_peers_and_rejects_untrusted_peers() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let ca = TestCa::new();
    let (server_tls, client_identity) = ca.peer("127.0.0.1");
    let (addr, handle) = mock_peer(server_tls, &ca.pem);
    let trusted =
        OmniNetwork::new_tls("peer-secret", Some(&ca.pem), Some(&client_identity), false).unwrap();
    assert!(
        vote_succeeds(trusted, addr).await,
        "trusted peer RPC must complete"
    );

    let anonymous = OmniNetwork::new_tls("peer-secret", Some(&ca.pem), None, false).unwrap();
    assert!(
        !vote_succeeds(anonymous, addr).await,
        "client certificate is required"
    );
    let other_ca = TestCa::new();
    let (_, untrusted_identity) = other_ca.peer("127.0.0.1");
    let untrusted = OmniNetwork::new_tls(
        "peer-secret",
        Some(&ca.pem),
        Some(&untrusted_identity),
        false,
    )
    .unwrap();
    assert!(
        !vote_succeeds(untrusted, addr).await,
        "foreign client CA must be rejected"
    );
    let wrong_trust = OmniNetwork::new_tls(
        "peer-secret",
        Some(&other_ca.pem),
        Some(&client_identity),
        false,
    )
    .unwrap();
    assert!(
        !vote_succeeds(wrong_trust, addr).await,
        "foreign server CA must be rejected"
    );
    handle.abort();
}

#[tokio::test]
async fn raft_mutual_tls_verifies_the_advertised_peer_name() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let ca = TestCa::new();
    let (server_tls, client_identity) = ca.peer("wrong-peer.example");
    let (addr, handle) = mock_peer(server_tls, &ca.pem);
    let network =
        OmniNetwork::new_tls("peer-secret", Some(&ca.pem), Some(&client_identity), false).unwrap();
    assert!(
        !vote_succeeds(network, addr).await,
        "a valid CA cannot substitute the wrong host"
    );
    handle.abort();
}

#[test]
fn raft_network_from_rejects_non_header_safe_tokens_without_leaking_value() {
    for bad_token in [
        "secret-raft-token-0123456789abcdef\n",
        "secret-raft-token-0123456789abcdef\r\n",
        " secret-raft-token-0123456789abcdef",
        "secret-raft-token-0123456789abcdef ",
        "secret-raft-token-\x07-0123456789abcdef",
    ] {
        let mut cfg = ServerConfig::default();
        cfg.raft.auth_token = bad_token.to_string();
        let err = match raft_network_from(&cfg) {
            Ok(_) => panic!("expected invalid raft token to be rejected"),
            Err(err) => err.to_string(),
        };
        assert!(
            err.contains("raft.auth_token") || err.contains("OMNIKV_RAFT_AUTH_TOKEN"),
            "error must name the setting: {err}"
        );
        assert!(
            !err.contains("secret-raft-token"),
            "error must not print the token value: {err}"
        );
    }

    let mut valid_cfg = ServerConfig::default();
    valid_cfg.raft.auth_token = "header-safe-raft-token-0123456789abcdef".into();
    assert!(raft_network_from(&valid_cfg).is_ok());
}

#[tokio::test]
async fn raft_network_from_separates_cert_and_key_pem_blocks_without_trailing_newline() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let dir = tempfile::tempdir().unwrap();
    let ca = TestCa::new();

    let mut params = rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
    params.extended_key_usages = vec![
        rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        rcgen::ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let key = rcgen::KeyPair::generate().unwrap();
    let issuer = rcgen::Issuer::from_params(&ca.params, &ca.key);
    let cert = params.signed_by(&key, &issuer).unwrap();

    let ca_path = dir.path().join("ca.pem");
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&ca_path, &ca.pem).unwrap();
    std::fs::write(&cert_path, cert.pem().trim_end()).unwrap();
    std::fs::write(&key_path, key.serialize_pem().trim_end()).unwrap();

    let server_tls = ServerTls {
        certs: vec![cert.der().clone()],
        key: rustls::pki_types::PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
        posture: crate::tls::TlsPosture::Operator {
            cert_path: cert_path.to_string_lossy().into_owned(),
            key_path: key_path.to_string_lossy().into_owned(),
            n_certs: 1,
        },
    };
    let (addr, handle) = mock_peer(server_tls, &ca.pem);

    let mut cfg = ServerConfig::default();
    cfg.raft.tls = true;
    cfg.raft.auth_token = "peer-secret".into();
    cfg.raft.tls_ca_path = Some(ca_path.to_string_lossy().into_owned());
    cfg.tls_cert_path = Some(cert_path.to_string_lossy().into_owned());
    cfg.tls_key_path = Some(key_path.to_string_lossy().into_owned());

    let network = raft_network_from(&cfg).expect("PEM blocks without trailing newline must load");
    assert!(vote_succeeds(network, addr).await);
    handle.abort();
}
