use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::network::{RaftNetwork, RaftNetworkFactory};
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse};

use crate::raft_impl::{OmniNode, OmniTypeConfig};

pub struct OmniNetwork {
    client: reqwest::Client,
}

impl OmniNetwork {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .pool_max_idle_per_host(32)
                .pool_idle_timeout(std::time::Duration::from_secs(90))
                .timeout(std::time::Duration::from_secs(10))
                .connect_timeout(std::time::Duration::from_secs(5))
                .tcp_keepalive(std::time::Duration::from_secs(30))
                .tcp_nodelay(true)
                .build()
                .expect("Failed to build Raft HTTP client"),
        }
    }
}

pub struct OmniNetworkConnection {
    client: reqwest::Client,
    target: OmniNode,
}

impl RaftNetworkFactory<OmniTypeConfig> for OmniNetwork {
    type Network = OmniNetworkConnection;

    async fn new_client(&mut self, _target: u64, node: &OmniNode) -> Self::Network {
        OmniNetworkConnection {
            client: self.client.clone(),
            target: node.clone(),
        }
    }
}

impl RaftNetwork<OmniTypeConfig> for OmniNetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<OmniTypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, OmniNode, RaftError<u64>>> {
        let url = format!("https://{}/raft/append", self.target.addr);
        let resp = self.client.post(&url).json(&req).send().await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        
        let res = resp.json().await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        Ok(res)
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<OmniTypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<InstallSnapshotResponse<u64>, RPCError<u64, OmniNode, RaftError<u64, InstallSnapshotError>>> {
        let url = format!("https://{}/raft/snapshot", self.target.addr);
        let resp = self.client.post(&url).json(&req).send().await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        
        let res = resp.json().await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        Ok(res)
    }

    async fn vote(
        &mut self,
        req: VoteRequest<u64>,
        _option: openraft::network::RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, OmniNode, RaftError<u64>>> {
        let url = format!("https://{}/raft/vote", self.target.addr);
        let resp = self.client.post(&url).json(&req).send().await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        
        let res = resp.json().await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        Ok(res)
    }
}
