//! Raft Type Definitions
//!
//! Core type aliases for OpenRaft integration.
//! All other raft_* modules import types from here.

use openraft::BasicNode;
use std::io::Cursor;

// The Raft type configuration for OmniKV.
openraft::declare_raft_types!(
    pub TypeConfig:
        D = String,
        R = String,
        Node = BasicNode,
);

// Re-export so all raft modules use consistent names
pub type OmniTypeConfig = TypeConfig;
pub type OmniNode = BasicNode;
pub type OmniRaft = openraft::Raft<TypeConfig>;
