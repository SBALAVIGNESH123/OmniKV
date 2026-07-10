//! Bloom filter and FNV hashing utilities for probabilistic key lookups.

use std::hash::{Hash, Hasher};

use crate::OmniError;

/// Fowler–Noll–Vo (FNV-1a) hash function.
pub struct FnvHasher(u64);
impl Default for FnvHasher {
    fn default() -> Self {
        Self(0xcbf29ce484222325)
    }
}
impl Hasher for FnvHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 ^= b as u64;
            self.0 = self.0.wrapping_mul(0x100000001b3);
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct BloomFilter {
    bitset: Vec<u64>,
    num_bits: usize,
    num_hashes: usize,
}

impl BloomFilter {
    pub fn save(&self, path: &str) -> Result<(), OmniError> {
        let content = serde_json::to_string(self)
            .map_err(|e| OmniError::IoError(format!("Bloom filter serialize: {}", e)))?;
        std::fs::write(path, content)?;
        Ok(())
    }

    pub fn load(path: &str) -> Result<Self, OmniError> {
        let content = std::fs::read_to_string(path)?;
        serde_json::from_str(&content).map_err(|e| OmniError::IoError(e.to_string()))
    }

    pub fn new(expected_elements: usize) -> Self {
        let p = 0.01_f64;
        let num_bits =
            (-(expected_elements as f64) * p.ln() / (2.0_f64.ln().powi(2))).ceil() as usize;
        let num_hashes =
            (2.0_f64.ln() * (num_bits as f64) / (expected_elements as f64)).ceil() as usize;
        let bitset = vec![0; num_bits.div_ceil(64)];
        let num_bits_total = bitset.len() * 64;
        Self {
            bitset,
            num_bits: num_bits_total,
            num_hashes,
        }
    }

    fn hashes(&self, key: u64) -> Vec<usize> {
        let mut h = FnvHasher::default();
        key.hash(&mut h);
        let hash1 = h.finish();

        let mut h2 = FnvHasher::default();
        hash1.hash(&mut h2);
        let hash2 = h2.finish();

        let mut res = Vec::with_capacity(self.num_hashes);
        for i in 0..self.num_hashes {
            let combined = hash1.wrapping_add((i as u64).wrapping_mul(hash2));
            res.push((combined % self.num_bits as u64) as usize);
        }
        res
    }

    pub fn add(&mut self, key: u64) {
        if self.num_bits == 0 {
            return;
        }
        for idx in self.hashes(key) {
            self.bitset[idx / 64] |= 1u64 << (idx % 64);
        }
    }

    pub fn might_contain(&self, key: u64) -> bool {
        if self.num_bits == 0 {
            return false;
        }
        for idx in self.hashes(key) {
            if self.bitset[idx / 64] & (1u64 << (idx % 64)) == 0 {
                return false;
            }

        }
        true
    }
}
