//! Raft Cluster Initialization
//!
//! Bootstrap a new Raft cluster or join an existing one.

use crate::raft_impl::TypeConfig;
use openraft::{BasicNode, Config, Raft};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Initialize a new single-node Raft cluster.
pub async fn init_single_node(
    raft: &Raft<TypeConfig>,
    node_id: u64,
    addr: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut members = BTreeMap::new();
    members.insert(
        node_id,
        BasicNode {
            addr: addr.to_string(),
        },
    );

    raft.initialize(members).await?;
    tracing::info!("Raft cluster initialized with single node {}", node_id);
    Ok(())
}

/// Add a learner node to the cluster (called on the leader).
pub async fn add_learner(
    raft: &Raft<TypeConfig>,
    node_id: u64,
    addr: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let node = BasicNode {
        addr: addr.to_string(),
    };
    raft.add_learner(node_id, node, true).await?;
    tracing::info!("Added learner node {} at {}", node_id, addr);
    Ok(())
}

/// Promote learners to full voting members.
pub async fn change_membership(
    raft: &Raft<TypeConfig>,
    member_ids: Vec<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let members: std::collections::BTreeSet<u64> = member_ids.into_iter().collect();
    raft.change_membership(members, false).await?;
    tracing::info!("Membership changed successfully");
    Ok(())
}

/// Build the Raft configuration.
pub fn build_raft_config() -> Arc<Config> {
    let config = Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        ..Default::default()
    };
    Arc::new(config.validate().expect("Invalid Raft config"))
}
