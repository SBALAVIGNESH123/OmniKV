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

/// Builder for [`OmniClient`].
pub struct OmniClientBuilder {
    base_url: String,
    timeout: Duration,
    accept_invalid_certs: bool,
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
        Self::builder(base_url)
            .build()
            .expect("Failed to build OmniKV HTTP client")
    }

    /// Create a new client that accepts self-signed TLS certificates.
    ///
    /// This is intended for local development only. Production clients should
    /// use [`OmniClient::new`] or [`OmniClient::builder`] so certificates are
    /// verified by default.
    pub fn new_insecure_for_local_dev(base_url: &str) -> Self {
        Self::builder(base_url)
            .accept_invalid_certs_for_local_dev(true)
            .build()
            .expect("Failed to build OmniKV HTTP client")
    }

    /// Start building a client with explicit options.
    pub fn builder(base_url: &str) -> OmniClientBuilder {
        OmniClientBuilder {
            base_url: base_url.trim_end_matches('/').to_string(),
            timeout: Duration::from_secs(30),
            accept_invalid_certs: false,
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
    pub async fn set_with_ttl(
        &self,
        key: &str,
        value: &str,
        ttl: Option<u64>,
    ) -> Result<u64, OmniClientError> {
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
    pub async fn scan(
        &self,
        start: &str,
        end: &str,
        limit: Option<usize>,
    ) -> Result<Vec<KvPair>, OmniClientError> {
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
        body.data
            .ok_or_else(|| OmniClientError::Api("No health data".into()))
    }

    /// Fetch Prometheus metrics as raw text.
    pub async fn metrics(&self) -> Result<String, OmniClientError> {
        let url = format!("{}/metrics", self.base_url);
        let resp = self.client.get(&url).send().await?;
        Ok(resp.text().await?)
    }
}

impl OmniClientBuilder {
    /// Set the request timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the JWT bearer token for authenticated requests.
    pub fn token(mut self, token: &str) -> Self {
        self.token = Some(token.to_string());
        self
    }

    /// Accept invalid TLS certificates for local development.
    ///
    /// Do not enable this in production.
    pub fn accept_invalid_certs_for_local_dev(mut self, accept: bool) -> Self {
        self.accept_invalid_certs = accept;
        self
    }

    /// Build the configured client.
    pub fn build(self) -> Result<OmniClient, OmniClientError> {
        let client = Client::builder()
            .timeout(self.timeout)
            .pool_max_idle_per_host(10)
            .danger_accept_invalid_certs(self.accept_invalid_certs)
            .build()?;

        Ok(OmniClient {
            base_url: self.base_url,
            client,
            token: self.token,
        })
    }
}
