//! OmniKV Client Library
//!
//! Production-grade Rust client for OmniKV's REST API.
//! Supports CRUD, batch operations, scans, health checks, and metrics.

use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// OmniKV REST API client.
#[derive(Clone)]
pub struct OmniClient {
    base_url: String,
    client: Client,
    token: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KvPair {
    pub key: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WriteResult {
    pub seq: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct HealthStatus {
    pub status: String,
    pub version: String,
    pub uptime_secs: u64,
    pub sstable_count: usize,
}

#[derive(Serialize, Debug)]
pub struct BatchOp {
    pub op: String,
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

#[derive(Serialize, Debug)]
struct BatchRequest {
    operations: Vec<BatchOp>,
}

#[derive(Serialize, Debug)]
struct SetRequest {
    key: String,
    value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    expiry: Option<u64>,
}

#[derive(Debug)]
pub enum OmniClientError {
    Http(reqwest::Error),
    Api(String),
}

impl std::fmt::Display for OmniClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Http(e) => write!(f, "HTTP error: {}", e),
            Self::Api(msg) => write!(f, "API error: {}", msg),
        }
    }
}

impl std::error::Error for OmniClientError {}

impl From<reqwest::Error> for OmniClientError {
    fn from(e: reqwest::Error) -> Self {
        Self::Http(e)
    }
}

impl OmniClient {
    /// Create a new client pointing to the given base URL.
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: Client::builder()
                .timeout(Duration::from_secs(30))
                .pool_max_idle_per_host(10)
                .danger_accept_invalid_certs(true) // for self-signed TLS
                .build()
                .expect("Failed to build HTTP client"),
            token: None,
        }
    }

    /// Set the JWT bearer token for authenticated requests.
    pub fn with_token(mut self, token: &str) -> Self {
        self.token = Some(token.to_string());
        self
    }

    /// GET a single key.
    pub async fn get(&self, key: &str) -> Result<Option<String>, OmniClientError> {
        let url = format!("{}/kv/{}", self.base_url, key);
        let mut req = self.client.get(&url);
        if let Some(ref t) = self.token {
            req = req.bearer_auth(t);
        }
        let resp = req.send().await?;
        if resp.status().is_success() {
            let body: ApiResponse<KvPair> = resp.json().await?;
            Ok(body.data.map(|kv| kv.value))
        } else if resp.status().as_u16() == 404 {
            Ok(None)
        } else {
            let body: ApiResponse<()> = resp.json().await?;
            Err(OmniClientError::Api(body.error.unwrap_or_default()))
        }
    }

    /// SET a key-value pair with optional TTL (seconds).
    pub async fn set(&self, key: &str, value: &str) -> Result<u64, OmniClientError> {
        self.set_with_ttl(key, value, None).await
    }

    /// SET with optional TTL.
    pub async fn set_with_ttl(&self, key: &str, value: &str, ttl: Option<u64>) -> Result<u64, OmniClientError> {
        let url = format!("{}/kv", self.base_url);
        let body = SetRequest {
            key: key.to_string(),
            value: value.to_string(),
            expiry: ttl,
        };
        let mut req = self.client.post(&url).json(&body);
        if let Some(ref t) = self.token {
            req = req.bearer_auth(t);
        }
        let resp = req.send().await?;
        let body: ApiResponse<WriteResult> = resp.json().await?;
        if body.success {
            Ok(body.data.map(|r| r.seq).unwrap_or(0))
        } else {
            Err(OmniClientError::Api(body.error.unwrap_or_default()))
        }
    }

    /// DELETE a key.
    pub async fn delete(&self, key: &str) -> Result<(), OmniClientError> {
        let url = format!("{}/kv/{}", self.base_url, key);
        let mut req = self.client.delete(&url);
        if let Some(ref t) = self.token {
            req = req.bearer_auth(t);
        }
        req.send().await?;
        Ok(())
    }

    /// SCAN a key range.
    pub async fn scan(&self, start: &str, end: &str, limit: Option<usize>) -> Result<Vec<KvPair>, OmniClientError> {
        let mut url = format!("{}/scan?start={}&end={}", self.base_url, start, end);
        if let Some(l) = limit {
            url.push_str(&format!("&limit={}", l));
        }
        let mut req = self.client.get(&url);
        if let Some(ref t) = self.token {
            req = req.bearer_auth(t);
        }
        let resp = req.send().await?;
        let body: ApiResponse<Vec<KvPair>> = resp.json().await?;
        Ok(body.data.unwrap_or_default())
    }

    /// Execute a batch of operations atomically.
    pub async fn batch(&self, ops: Vec<BatchOp>) -> Result<u64, OmniClientError> {
        let url = format!("{}/batch", self.base_url);
        let body = BatchRequest { operations: ops };
        let mut req = self.client.post(&url).json(&body);
        if let Some(ref t) = self.token {
            req = req.bearer_auth(t);
        }
        let resp = req.send().await?;
        let body: ApiResponse<WriteResult> = resp.json().await?;
        Ok(body.data.map(|r| r.seq).unwrap_or(0))
    }

    /// Check server health.
    pub async fn health(&self) -> Result<HealthStatus, OmniClientError> {
        let url = format!("{}/health", self.base_url);
        let resp = self.client.get(&url).send().await?;
        let body: ApiResponse<HealthStatus> = resp.json().await?;
        body.data.ok_or_else(|| OmniClientError::Api("No health data".into()))
    }

    /// Fetch Prometheus metrics as raw text.
    pub async fn metrics(&self) -> Result<String, OmniClientError> {
        let url = format!("{}/metrics", self.base_url);
        let resp = self.client.get(&url).send().await?;
        Ok(resp.text().await?)
    }
}
