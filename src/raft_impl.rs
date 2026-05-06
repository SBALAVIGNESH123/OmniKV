//! Raft Type Definitions
//!
//! Core type aliases for OpenRaft integration.

use std::io::Cursor;
use openraft::BasicNode;

/// The Raft type configuration for OmniKV.
openraft::declare_raft_types!(
    pub TypeConfig:
        D = String,
        R = String,
        Node = BasicNode,
);
