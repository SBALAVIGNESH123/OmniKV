//! OmniKV Client Library
//!
//! Provides a Rust client for connecting to OmniKV's REST API.

use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct OmniClient {
    base_url: String,
    client: Client,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct KvPair {
    pub key: String,
    pub value: String,
}

impl OmniClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.to_string(),
            client: Client::new(),
        }
    }

    pub async fn get(&self, key: &str) -> Result<Option<String>, reqwest::Error> {
        let url = format!("{}/kv/{}", self.base_url, key);
        let resp = self.client.get(&url).send().await?;
        if resp.status().is_success() {
            let body = resp.text().await?;
            Ok(Some(body))
        } else {
            Ok(None)
        }
    }

    pub async fn set(&self, key: &str, value: &str) -> Result<(), reqwest::Error> {
        let url = format!("{}/kv", self.base_url);
        let pair = KvPair {
            key: key.to_string(),
            value: value.to_string(),
        };
        self.client.post(&url).json(&pair).send().await?;
        Ok(())
    }

    pub async fn delete(&self, key: &str) -> Result<(), reqwest::Error> {
        let url = format!("{}/kv/{}", self.base_url, key);
        self.client.delete(&url).send().await?;
        Ok(())
    }
}
