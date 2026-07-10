//! WriteBatch — buffered write operations for atomic commits.

use crate::record::{string_to_key, OmniError, MAX_BATCH_SIZE, MAX_VALUE_SIZE};

#[derive(Default)]
pub struct WriteBatch {
    pub buffered_writes: Vec<(Vec<u8>, String, u64)>,
    pub buffered_deletes: Vec<Vec<u8>>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn set(&mut self, key: &str, value: String) -> Result<(), OmniError> {
        self.set_with_ttl(key, value, 0)
    }
    pub fn set_with_ttl(
        &mut self,
        key: &str,
        value: String,
        ttl_secs: u64,
    ) -> Result<(), OmniError> {
        if self.buffered_writes.len() + self.buffered_deletes.len() >= MAX_BATCH_SIZE {
            return Err(OmniError::BatchTooLarge(MAX_BATCH_SIZE));
        }
        if value.len() > MAX_VALUE_SIZE {
            return Err(OmniError::ValueTooLarge(MAX_VALUE_SIZE));
        }
        let expiry = if ttl_secs > 0 {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                + ttl_secs
        } else {
            0
        };
        self.buffered_writes
            .push((string_to_key(key), value, expiry));
        Ok(())
    }
    pub fn delete(&mut self, key: &str) -> Result<(), OmniError> {
        if self.buffered_writes.len() + self.buffered_deletes.len() >= MAX_BATCH_SIZE {
            return Err(OmniError::BatchTooLarge(MAX_BATCH_SIZE));
        }
        self.buffered_deletes.push(string_to_key(key));
        Ok(())
    }
    pub fn is_empty(&self) -> bool {
        self.buffered_writes.is_empty() && self.buffered_deletes.is_empty()
    }
}
