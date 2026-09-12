//! Cluster boot: constructs the real openraft node when the config says
//! this server is a cluster member.
//!
//! `OMNIKV_RAFT_ADDR` + `OMNIKV_NODE_ID` present → the server boots an
//! openraft node, serves consensus RPCs on a dedicated plaintext
//! listener, and routes every client write through consensus before
//! acknowledging it. Absent → the server stays an independent
//! single-node engine and none of this runs.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use omni_engine::raft_gateway::ClusterGateway;
use omni_engine::raft_impl::OmniRaft;
use omni_engine::raft_network::OmniNetwork;
use omni_engine::raft_storage::OmniRaftStorage;
use omni_engine::{OmniKV, config::ServerConfig};
use openraft::BasicNode;

use crate::raft_routes::{RaftState, build_raft_router};

/// A booted cluster node: the openraft handle, the write gateway, and
/// the bound consensus listener (bound here so an unusable raft port
/// fails the boot, not the first RPC).
pub struct ClusterNode {
    pub raft: Arc<OmniRaft>,
    pub gateway: Arc<ClusterGateway>,
    pub raft_listener: tokio::net::TcpListener,
    pub raft_addr: SocketAddr,
}

/// Builds the openraft config with the engine defaults plus the
/// config-file/env overrides applied. Constructed directly (not via
/// `build_raft_config`) so the overrides can never be silently dropped
/// by an `Arc` clone.
fn raft_config_from(
    cfg: &ServerConfig,
) -> Result<Arc<openraft::Config>, Box<dyn std::error::Error>> {
    let mut c = openraft::Config {
        // Defaults that suit tests and LANs; env/config overrides below.
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        ..Default::default()
    };
    if let Some(h) = cfg.raft.heartbeat_interval_ms {
        c.heartbeat_interval = h;
    }
    if let Some(min) = cfg.raft.election_timeout_min_ms {
        c.election_timeout_min = min;
    }
    if let Some(max) = cfg.raft.election_timeout_max_ms {
        c.election_timeout_max = max;
    }
    if c.election_timeout_min > c.election_timeout_max {
        return Err(format!(
            "raft election timeout min ({}) > max ({})",
            c.election_timeout_min, c.election_timeout_max
        )
        .into());
    }
    Ok(Arc::new(
        c.validate().map_err(|e| format!("raft config: {e}"))?,
    ))
}

/// Boots the cluster node described by `cfg.raft`. Returns `None` when
/// the config has no raft section — single-node mode, nothing started.
///
/// Membership bootstrap: node 1 of a FRESH cluster (no prior state)
/// initializes the cluster with the full initial peer set — the
/// openraft pattern where blank follower nodes adopt the cluster as the
/// initialized leader replicates the membership entry to them. A node
/// with persisted membership (a restart) recognizes it and re-joins;
/// openraft errors on double-init, which is exactly the guard here.
pub async fn boot_cluster_node(
    cfg: &ServerConfig,
    db: &Arc<OmniKV>,
) -> Result<Option<ClusterNode>, Box<dyn std::error::Error>> {
    let Some(node_id) = cfg.raft.node_id else {
        return Ok(None);
    };
    let Some(raft_addr_str) = cfg.raft.raft_addr.clone() else {
        return Ok(None);
    };
    let raft_addr: SocketAddr = raft_addr_str.parse()?;

    let config = raft_config_from(cfg)?;

    // One storage instance split by openraft's Adaptor into the
    // log-store and state-machine roles.
    let storage = OmniRaftStorage::new(db.clone());
    let (log_store, state_machine) = openraft::storage::Adaptor::new(storage);

    let raft = OmniRaft::new(
        node_id,
        config,
        OmniNetwork::new(),
        log_store,
        state_machine,
    )
    .await?;
    let raft = Arc::new(raft);

    // ── Membership bootstrap ──
    let metrics = raft.metrics().borrow().clone();
    let already_initialized = metrics
        .membership_config
        .membership()
        .nodes()
        .next()
        .is_some()
        || raft.current_leader().await.is_some();
    if already_initialized {
        tracing::info!("Node {node_id} re-joining existing cluster");
    } else if node_id == 1 {
        // Node 1 of a fresh cluster carries the complete initial
        // membership (itself + every peer as voters). Other nodes start
        // blank: they adopt the cluster as the leader's replication
        // (heartbeats, votes, the membership log entry) reaches them —
        // the same path a learner joins through.
        let mut members = BTreeMap::new();
        members.insert(
            node_id,
            BasicNode {
                addr: raft_addr_str.clone(),
            },
        );
        for (i, peer) in cfg.raft.peers.iter().enumerate() {
            // Peer node ids are 2.. in declaration order — the
            // compose file pairs OMNI_NODE_ID with OMNI_PEERS, so
            // every node declares the same ordered peer list.
            let peer_id: u64 = i as u64 + 2;
            members.insert(peer_id, BasicNode { addr: peer.clone() });
        }
        raft.initialize(members).await?;
        tracing::info!(
            members = cfg.raft.peers.len() + 1,
            "Raft cluster initialized"
        );
    } else {
        // A fresh non-1 node: no local state, cluster not reachable
        // yet. openraft leaves it a follower; it joins when the
        // leader's RPCs (it is already in the initial membership)
        // arrive.
        tracing::info!(
            "Node {node_id} starting blank; it joins the cluster as the leader reaches it"
        );
    }

    // ── Write gateway + engine hooks ──
    let gateway = Arc::new(ClusterGateway::new((*raft).clone()));
    db.set_cluster_gateway(gateway.clone());

    // Bind the consensus listener up front: a taken port must fail the
    // boot (the node would look alive but never hear a vote otherwise).
    let raft_listener = tokio::net::TcpListener::bind(raft_addr).await?;
    tracing::info!(%raft_addr, "Raft consensus listener bound");

    Ok(Some(ClusterNode {
        raft,
        gateway,
        raft_listener,
        raft_addr,
    }))
}

/// Serves the consensus RPC routes on the node's dedicated plaintext
/// listener. Never returns under normal operation.
pub async fn serve_raft_rpc(node: ClusterNode) -> std::io::Result<()> {
    let app = build_raft_router(RaftState {
        raft: node.raft.clone(),
    });
    tracing::info!("Raft consensus listener on {}", node.raft_addr);
    axum::serve(node.raft_listener, app).await
}
