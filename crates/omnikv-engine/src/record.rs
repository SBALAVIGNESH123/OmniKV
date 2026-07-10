//! Core record types, error handling, and key conversion utilities.

use std::hash::{Hash, Hasher};
use crate::bloom::FnvHasher;

/// Core error type for OmniKV operations.
#[derive(Debug, Clone)]
pub enum OmniError {
    IoError(String),
    BatchTooLarge(usize),
    ValueTooLarge(usize),
    KeyNotFound,
    HashCollision,
    LockPoisoned(String),
    WriteStall,
}

impl std::fmt::Display for OmniError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for OmniError {}

impl From<std::io::Error> for OmniError {
    fn from(e: std::io::Error) -> Self {
        OmniError::IoError(e.to_string())
    }
}

pub fn string_to_key(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

pub fn key_to_string(key: &[u8]) -> String {
    String::from_utf8_lossy(key).to_string()
}

pub const MAX_BATCH_SIZE: usize = 10_000;
pub const MAX_VALUE_SIZE: usize = 10 * 1024 * 1024; // 10 MB
pub const UNCOMPRESSED_FLAG: u64 = 1 << 63;
pub const NUM_SHARDS: usize = 16;

#[inline]
pub fn shard_idx(key: &[u8]) -> usize {
    let mut h = FnvHasher::default();
    key.hash(&mut h);
    (h.finish() as usize) % NUM_SHARDS
}

/// A single record in the OmniKV storage engine.
/// Contains key, sequence number, heap offset/length, CRC32 integrity, and TTL expiry.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct OmniRecord {
    pub seq: u64,
    pub key: Vec<u8>,
    pub offset: u64,
    pub length: u64,
    pub crc32: u32,
    pub payload_crc32: u32,
    pub expiry: u64,
}

impl OmniRecord {
    pub fn new(
        seq: u64,
        key: Vec<u8>,
        offset: u64,
        length: u64,
        payload_crc32: u32,
        expiry: u64,
    ) -> Self {
        let mut rec = Self {
            seq,
            key,
            offset,
            length,
            crc32: 0,
            payload_crc32,
            expiry,
        };
        rec.crc32 = rec.compute_crc();
        rec
    }

    pub fn compute_crc(&self) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&self.seq.to_le_bytes());
        hasher.update(&(self.key.len() as u16).to_le_bytes());
        hasher.update(&self.key);
        hasher.update(&self.offset.to_le_bytes());
        hasher.update(&self.length.to_le_bytes());
        hasher.update(&self.payload_crc32.to_le_bytes());
        hasher.update(&self.expiry.to_le_bytes());
        hasher.finalize()
    }

    pub fn is_valid(&self) -> bool {
        self.crc32 == self.compute_crc()
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(42 + self.key.len());
        buf.extend_from_slice(&self.seq.to_le_bytes());
        buf.extend_from_slice(&(self.key.len() as u16).to_le_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.offset.to_le_bytes());
        buf.extend_from_slice(&self.length.to_le_bytes());
        buf.extend_from_slice(&self.payload_crc32.to_le_bytes());
        buf.extend_from_slice(&self.expiry.to_le_bytes());
        buf.extend_from_slice(&self.crc32.to_le_bytes());
        buf
    }

    pub fn decode(buf: &[u8]) -> Option<(Self, usize)> {
        if buf.len() < 42 {
            return None;
        }
        let key_len = u16::from_le_bytes(buf[8..10].try_into().unwrap()) as usize;
        if buf.len() < 42 + key_len {
            return None;
        }

        let seq = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let key = buf[10..10 + key_len].to_vec();
        let off = 10 + key_len;
        let offset = u64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
        let length = u64::from_le_bytes(buf[off + 8..off + 16].try_into().unwrap());
        let payload_crc32 = u32::from_le_bytes(buf[off + 16..off + 20].try_into().unwrap());
        let expiry = u64::from_le_bytes(buf[off + 20..off + 28].try_into().unwrap());
        let crc32 = u32::from_le_bytes(buf[off + 28..off + 32].try_into().unwrap());
        let rec = Self {
            seq,
            key,
            offset,
            length,
            crc32,
            payload_crc32,
            expiry,
        };

        if !rec.is_valid() {
            return None; // Invalid CRC
        }

        Some((rec, 42 + key_len))
    }
}
