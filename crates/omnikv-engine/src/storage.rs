//! Storage topology types — the atomic snapshot of all read-visible state.

use std::cmp::Reverse;
use std::fs::File;
use std::sync::{Arc, Mutex};

use crossbeam_skiplist::SkipMap;
use memmap2::Mmap;

use crate::bloom::BloomFilter;
use crate::manifest::Manifest;
use crate::record::NUM_SHARDS;
use crate::wal;

pub struct Metrics {
    pub read_latencies: Mutex<hdrhistogram::Histogram<u64>>,
    pub commit_latencies: Mutex<hdrhistogram::Histogram<u64>>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            read_latencies: Mutex::new(
                hdrhistogram::Histogram::<u64>::new_with_bounds(1, 10_000_000, 3)
                    .unwrap_or_else(|_| hdrhistogram::Histogram::new(3).unwrap()),
            ),
            commit_latencies: Mutex::new(
                hdrhistogram::Histogram::<u64>::new_with_bounds(1, 10_000_000, 3)
                    .unwrap_or_else(|_| hdrhistogram::Histogram::new(3).unwrap()),
            ),
        }
    }
}

/// Single atomic snapshot of all read-visible storage topology.
/// Readers load this once and operate on stable Arc-owned handles — zero locking.
/// Snapshot install replaces this with a single `roots.store(Arc::new(new_roots))`.
/// No mixed-topology window is possible.
#[derive(Clone)]
pub struct StorageRoots {
    pub base_mmap: Arc<Mmap>,
    pub base_bloom: Arc<BloomFilter>,
    pub sstables: Arc<Vec<(Arc<Mmap>, Arc<BloomFilter>, String)>>,   // L0
    pub l1_sstables: Arc<Vec<(Arc<Mmap>, Arc<BloomFilter>, String)>>, // L1
    pub memtable: Arc<[SkipMap<(Vec<u8>, Reverse<u64>), (u64, u64, u32, u64)>; NUM_SHARDS]>,
    pub frozen_memtables: Arc<Vec<Arc<[SkipMap<(Vec<u8>, Reverse<u64>), (u64, u64, u32, u64)>; NUM_SHARDS]>>>,
    pub manifest: Arc<Manifest>,
    pub heap_reader: Arc<File>,
}

/// Pure storage recovery result — returned by recover_storage_from_path().
/// All topology data lives here; no background threads are started.
pub(crate) struct RecoveredStorage {
    pub base_mmap: Arc<Mmap>,
    pub base_bloom: Arc<BloomFilter>,
    pub sstables: Arc<Vec<(Arc<Mmap>, Arc<BloomFilter>, String)>>,
    pub l1_sstables: Arc<Vec<(Arc<Mmap>, Arc<BloomFilter>, String)>>,
    pub manifest: Arc<Manifest>,
    pub heap_reader: Arc<File>,
    pub heap_file: File,
    pub heap_offset: u64,
    pub wal: wal::WriteAheadLog,
    pub memtable: Arc<[SkipMap<(Vec<u8>, Reverse<u64>), (u64, u64, u32, u64)>; NUM_SHARDS]>,
    pub max_seq: u64,
}
