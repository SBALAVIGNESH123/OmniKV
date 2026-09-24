//! Cluster boot: constructs the real openraft node when the config says
//! this server is a cluster member.
//!
//! `OMNIKV_RAFT_ADDR` + `OMNIKV_NODE_ID` present → the server boots an
//! openraft node, serves consensus RPCs on a dedicated plaintext
//! listener, and routes every client write through consensus before
//! acknowledging it. Absent → the server stays an independent
//! single-node engine and none of this runs.
//!
//! Bind vs advertised address: `OMNIKV_RAFT_ADDR` is
//! what the listener BINDS to (0.0.0.0 is fine there);
//! `OMNIKV_RAFT_ADVERTISE_ADDR` is what peers DIAL to reach this node
//! (0.0.0.0 is not — it resolves to the dialer itself). The advertised
//! address is what goes into cluster membership; without it, a wildcard
//! bind address would poison every peer's routing table for this node.

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

/// A booted cluster node: the openraft handle, the write gateway (whose
/// consensus runtime hosts every openraft task and the RPC listener
/// bound here), and the bound consensus listener.
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

/// Boots the cluster node described by `cfg.raft`, or returns `None` in
/// single-node mode.
///
/// Node 1 of a fresh cluster initializes membership; other nodes start
/// blank and join as the leader reaches them (openraft errors on
/// double-init, which is the guard below).
///
/// The node and listener are built ON the consensus runtime and the
/// runtime is then adopted by the gateway, so openraft's tasks can never
/// be starved by client load on the server runtime.
fn boot_rt(
    cfg: &ServerConfig,
    db: &Arc<OmniKV>,
    rt: tokio::runtime::Runtime,
) -> Result<Option<ClusterNode>, Box<dyn std::error::Error>> {
    let Some(node_id) = cfg.raft.node_id else {
        return Ok(None);
    };
    let Some(raft_addr_str) = cfg.raft.raft_addr.clone() else {
        return Ok(None);
    };
    let raft_addr: SocketAddr = raft_addr_str.parse()?;
    // What peers dial to reach this node. Falls back to the bind
    // address for the common single-host case (127.0.0.1:port); the
    // config validator already refuses a wildcard ADVERTISED address,
    // so this can never poison membership with 0.0.0.0.
    let advertise_addr = cfg
        .raft
        .advertise_addr
        .clone()
        .unwrap_or_else(|| raft_addr_str.clone());

    let config = raft_config_from(cfg)?;

    let storage = OmniRaftStorage::new(db.clone());
    let (log_store, state_machine) = openraft::storage::Adaptor::new(storage);

    let raft = rt.block_on(async {
        OmniRaft::new(
            node_id,
            config,
            OmniNetwork::new(),
            log_store,
            state_machine,
        )
        .await
    })?;
    let raft = Arc::new(raft);

    // ── Membership bootstrap ──
    let metrics = raft.metrics().borrow().clone();
    let already_initialized = metrics
        .membership_config
        .membership()
        .nodes()
        .next()
        .is_some()
        || rt.block_on(raft.current_leader()).is_some();
    if already_initialized {
        tracing::info!("Node {node_id} re-joining existing cluster");
    } else if node_id == 1 {
        // Node 1 seeds the full membership; peers start blank and join
        // here.
        let mut members = BTreeMap::new();
        members.insert(
            node_id,
            BasicNode {
                addr: advertise_addr,
            },
        );
        for (i, peer) in cfg.raft.peers.iter().enumerate() {
            // Peer node ids are 2.. in declaration order — the
            // compose file pairs OMNI_NODE_ID with OMNI_PEERS, so
            // every node declares the same ordered peer list.
            let peer_id: u64 = i as u64 + 2;
            members.insert(peer_id, BasicNode { addr: peer.clone() });
        }
        rt.block_on(raft.initialize(members))?;
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
    let gateway = Arc::new(ClusterGateway::with_runtime(rt, (*raft).clone()));
    db.set_cluster_gateway(gateway.clone());

    // Bind the consensus listener up front: a taken port must fail the
    // boot (the node would look alive but never hear a vote otherwise).
    // Uses the gateway's runtime handle — `rt` has been adopted above.
    let raft_listener = gateway
        .consensus_handle
        .block_on(tokio::net::TcpListener::bind(raft_addr))?;
    tracing::info!(%raft_addr, "Raft consensus listener bound");

    Ok(Some(ClusterNode {
        raft,
        gateway,
        raft_listener,
        raft_addr,
    }))
}

/// Builds the consensus runtime, then constructs the node ON it (see
/// `boot_rt`). Called from an async context, where a nested
/// `Runtime::block_on` would panic, so the boot hops through a plain OS
/// thread and the caller parks on a channel until it finishes.
pub fn boot_cluster_node(
    cfg: &ServerConfig,
    db: &Arc<OmniKV>,
) -> Result<Option<ClusterNode>, Box<dyn std::error::Error>> {
    let rt = ClusterGateway::consensus_runtime();
    if tokio::runtime::Handle::try_current().is_err() {
        // Plain thread (some tests): block_on is safe directly.
        return boot_rt(cfg, db, rt);
    }
    // Async context (main): hop off the runtime worker. The error
    // crosses the channel as a String — a boxed `dyn Error` is not
    // `Send`, and a boot failure is fatal anyway (only its message
    // matters).
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        scope.spawn(move || {
            let _ = tx.send(boot_rt(cfg, db, rt).map_err(|e| e.to_string()));
        });
        rx.recv()
            .expect("cluster boot thread finished")
            .map_err(|msg: String| -> Box<dyn std::error::Error> { msg.into() })
    })
}

/// Serves the consensus RPC routes on the node's dedicated plaintext
/// listener. Call from a task spawned on the gateway's consensus
/// runtime — never returns under normal operation.
pub async fn serve_raft_rpc(node: ClusterNode) -> std::io::Result<()> {
    let app = build_raft_router(RaftState {
        raft: node.raft.clone(),
    });
    tracing::info!("Raft consensus listener on {}", node.raft_addr);
    axum::serve(node.raft_listener, app).await
}
