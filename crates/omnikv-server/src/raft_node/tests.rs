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
