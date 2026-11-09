use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::network::{RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use crate::raft_impl::{OmniNode, OmniTypeConfig};

/// Consensus transport for openraft RPCs. Production clusters use HTTPS with
/// a configured CA bundle so peer traffic is confidential and the target
/// server is authenticated; the bearer token remains the application-level
/// authorization check. Plain HTTP remains available for development and
/// compatibility tests only.
pub struct OmniNetwork {
    client: reqwest::Client,
    auth_token: String,
    tls: bool,
}

impl Default for OmniNetwork {
    fn default() -> Self {
        Self::new(crate::config::DEV_RAFT_AUTH_TOKEN)
    }
}

impl OmniNetwork {
    pub fn new(auth_token: impl Into<String>) -> Self {
        Self::build_client(auth_token.into(), false, None, None, false)
            .expect("Failed to build Raft HTTP client")
    }

    /// Builds a TLS-protected peer client. `ca_pem` is the operator's trust
    /// bundle; omitting it is only valid with the explicit development
    /// `insecure_skip` opt-in.
    pub fn new_tls(
        auth_token: impl Into<String>,
        ca_pem: Option<&[u8]>,
        identity_pem: Option<&[u8]>,
        insecure_skip: bool,
    ) -> Result<Self, String> {
        if ca_pem.is_none() && !insecure_skip {
            return Err(
                "Raft TLS requires a CA bundle unless insecure verification is explicitly enabled"
                    .into(),
            );
        }
        Self::build_client(auth_token.into(), true, ca_pem, identity_pem, insecure_skip)
    }

    fn build_client(
        auth_token: String,
        tls: bool,
        ca_pem: Option<&[u8]>,
        identity_pem: Option<&[u8]>,
        insecure_skip: bool,
    ) -> Result<Self, String> {
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .https_only(tls)
            .pool_max_idle_per_host(32)
            .pool_idle_timeout(std::time::Duration::from_secs(90))
            .timeout(std::time::Duration::from_secs(10))
            .connect_timeout(std::time::Duration::from_secs(5))
            .tcp_keepalive(std::time::Duration::from_secs(30))
            .tcp_nodelay(true);
        if let Some(ca_pem) = ca_pem {
            let certs = reqwest::Certificate::from_pem_bundle(ca_pem)
                .map_err(|error| format!("invalid Raft TLS CA bundle: {error}"))?;
            if certs.is_empty() {
                return Err("Raft TLS CA bundle contains no certificates".into());
            }
            builder = builder.tls_certs_only(certs);
        }
        if let Some(identity_pem) = identity_pem {
            let identity = reqwest::Identity::from_pem(identity_pem)
                .map_err(|error| format!("invalid Raft TLS client identity: {error}"))?;
            builder = builder.identity(identity);
        }
        if insecure_skip {
            builder = builder.danger_accept_invalid_certs(true);
        }
        let client = builder
            .build()
            .map_err(|error| format!("failed to build Raft HTTP client: {error}"))?;
        Ok(Self {
            client,
            auth_token,
            tls,
        })
    }
}

pub struct OmniNetworkConnection {
    client: reqwest::Client,
    target: OmniNode,
    auth_token: String,
    tls: bool,
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "openraft's storage/network traits declare async fns; the storage and network adapters are synchronous internally and async is required by the trait signatures."
)]
impl RaftNetworkFactory<OmniTypeConfig> for OmniNetwork {
    type Network = OmniNetworkConnection;

    async fn new_client(&mut self, _target: u64, node: &OmniNode) -> Self::Network {
        OmniNetworkConnection {
            client: self.client.clone(),
            target: node.clone(),
            auth_token: self.auth_token.clone(),
            tls: self.tls,
        }
    }
}

impl RaftNetwork<OmniTypeConfig> for OmniNetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<OmniTypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, OmniNode, RaftError<u64>>> {
        let scheme = if self.tls { "https" } else { "http" };
        let url = format!("{scheme}://{}/raft/append", self.target.addr);
        let resp = self
            .client
            .post(&url)
            .header("x-omnikv-raft-auth", &self.auth_token)
            .json(&req)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let res = resp
            .json()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        Ok(res)
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<OmniTypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, OmniNode, RaftError<u64, InstallSnapshotError>>,
    > {
        let scheme = if self.tls { "https" } else { "http" };
        let url = format!("{scheme}://{}/raft/snapshot", self.target.addr);
        let resp = self
            .client
            .post(&url)
            .header("x-omnikv-raft-auth", &self.auth_token)
            .json(&req)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let res = resp
            .json()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        Ok(res)
    }

    async fn vote(
        &mut self,
        req: VoteRequest<u64>,
        _option: openraft::network::RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, OmniNode, RaftError<u64>>> {
        let scheme = if self.tls { "https" } else { "http" };
        let url = format!("{scheme}://{}/raft/vote", self.target.addr);
        let resp = self
            .client
            .post(&url)
            .header("x-omnikv-raft-auth", &self.auth_token)
            .json(&req)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let res = resp
            .json()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        Ok(res)
    }
}
