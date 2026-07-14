//! Library surface for `OmniKV` server protocol contracts.
//!
//! The production binary owns process startup. This library exposes the JSON
//! request contracts that integration tests and fuzz targets need to parse
//! without widening the public surface of the server internals.

/// JSON request bodies and query shapes accepted by the HTTP API.
pub mod api_contracts {
    use serde::Deserialize;

    /// Request body for `POST /kv`.
    #[derive(Deserialize)]
    pub struct SetRequest {
        /// Key to write.
        pub key: String,
        /// Value to store.
        pub value: String,
        /// Optional time-to-live in seconds.
        pub expiry: Option<u64>,
    }

    /// Request body for `POST /batch`.
    #[derive(Deserialize)]
    pub struct BatchRequest {
        /// Ordered operations to apply.
        pub operations: Vec<BatchOp>,
    }

    /// One operation inside a batch write request.
    #[derive(Deserialize)]
    pub struct BatchOp {
        /// Operation name, normally `set` or `delete`.
        pub op: String,
        /// Key affected by the operation.
        pub key: String,
        /// Optional value for set-like operations.
        pub value: Option<String>,
    }

    /// Query parameters for a key-range scan.
    #[derive(Deserialize)]
    pub struct ScanQuery {
        /// Inclusive scan start key.
        pub start: Option<String>,
        /// Exclusive scan end key.
        pub end: Option<String>,
        /// Optional maximum result count.
        pub limit: Option<usize>,
    }

    /// Request body for issuing an API token.
    #[derive(Deserialize)]
    pub struct TokenRequest {
        /// Principal name to encode in the token.
        pub username: String,
        /// Optional authorization role.
        pub role: Option<String>,
        /// Optional token lifetime in seconds.
        pub ttl_seconds: Option<u64>,
    }
}
