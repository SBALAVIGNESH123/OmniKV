use std::collections::{BTreeMap, BinaryHeap};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, BufWriter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::cmp::Reverse;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;
use arc_swap::ArcSwap;
use crossbeam_skiplist::SkipMap;
use memmap2::{Mmap, MmapOptions};
use std::hash::{Hash, Hasher};

pub struct FnvHasher(u64);
impl Default for FnvHasher { fn default() -> Self { Self(0xcbf29ce484222325) } }
impl Hasher for FnvHasher {
    fn finish(&self) -> u64 { self.0 }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 ^= b as u64;
            self.0 = self.0.wrapping_mul(0x100000001b3);
        }
    }
}
pub mod wal;
pub mod generator;
pub mod query;
pub mod metrics_prometheus;
pub mod transaction;
pub mod secondary_index;
pub mod prepared;
pub mod schema;
pub mod hardening;
pub mod chaos;
pub mod pgwire;
pub mod dist_txn;

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
    pub fn new(seq: u64, key: Vec<u8>, offset: u64, length: u64, payload_crc32: u32, expiry: u64) -> Self {
        let mut rec = Self { seq, key, offset, length, crc32: 0, payload_crc32, expiry };
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
        if buf.len() < 42 { return None; }
        let key_len = u16::from_le_bytes(buf[8..10].try_into().unwrap()) as usize;
        if buf.len() < 42 + key_len { return None; }
        
        let seq = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let key = buf[10..10+key_len].to_vec();
        let off = 10 + key_len;
        let offset = u64::from_le_bytes(buf[off..off+8].try_into().unwrap());
        let length = u64::from_le_bytes(buf[off+8..off+16].try_into().unwrap());
        let payload_crc32 = u32::from_le_bytes(buf[off+16..off+20].try_into().unwrap());
        let expiry = u64::from_le_bytes(buf[off+20..off+28].try_into().unwrap());
        let crc32 = u32::from_le_bytes(buf[off+28..off+32].try_into().unwrap());
        let rec = Self { seq, key, offset, length, crc32, payload_crc32, expiry };
        
        if !rec.is_valid() {
            return None; // Invalid CRC
        }
        
        Some((rec, 42 + key_len))
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
        let num_bits = (-(expected_elements as f64) * p.ln() / (2.0_f64.ln().powi(2))).ceil() as usize;
        let num_hashes = (2.0_f64.ln() * (num_bits as f64) / (expected_elements as f64)).ceil() as usize;
        let bitset = vec![0; num_bits.div_ceil(64)];
        let num_bits_total = bitset.len() * 64;
        Self { bitset, num_bits: num_bits_total, num_hashes }
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
        if self.num_bits == 0 { return; }
        for idx in self.hashes(key) {
            self.bitset[idx / 64] |= 1 << (idx % 64);
        }
    }

    pub fn might_contain(&self, key: u64) -> bool {
        if self.num_bits == 0 { return false; }
        for idx in self.hashes(key) {
            if self.bitset[idx / 64] & (1 << (idx % 64)) == 0 {
                return false;
            }
        }
        true
    }
}

const MAX_BATCH_SIZE: usize = 10_000;
const MAX_VALUE_SIZE: usize = 10 * 1024 * 1024; // 10 MB
const UNCOMPRESSED_FLAG: u64 = 1 << 63;
const NUM_SHARDS: usize = 16;

#[inline]
fn shard_idx(key: &[u8]) -> usize {
    let mut h = FnvHasher::default();
    key.hash(&mut h);
    (h.finish() as usize) % NUM_SHARDS
}

#[derive(Default)]
pub struct WriteBatch {
    pub buffered_writes: Vec<(Vec<u8>, String, u64)>,
    pub buffered_deletes: Vec<Vec<u8>>,
}

impl WriteBatch {
    pub fn new() -> Self { Self::default() }
    pub fn set(&mut self, key: &str, value: String) -> Result<(), OmniError> {
        self.set_with_ttl(key, value, 0)
    }
    pub fn set_with_ttl(&mut self, key: &str, value: String, ttl_secs: u64) -> Result<(), OmniError> {
        if self.buffered_writes.len() + self.buffered_deletes.len() >= MAX_BATCH_SIZE {
            return Err(OmniError::BatchTooLarge(MAX_BATCH_SIZE));
        }
        if value.len() > MAX_VALUE_SIZE {
            return Err(OmniError::ValueTooLarge(MAX_VALUE_SIZE));
        }
        let expiry = if ttl_secs > 0 {
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() + ttl_secs
        } else { 0 };
        self.buffered_writes.push((string_to_key(key), value, expiry));
        Ok(())
    }
    pub fn delete(&mut self, key: &str) -> Result<(), OmniError> {
        if self.buffered_writes.len() + self.buffered_deletes.len() >= MAX_BATCH_SIZE {
            return Err(OmniError::BatchTooLarge(MAX_BATCH_SIZE));
        }
        self.buffered_deletes.push(string_to_key(key));
        Ok(())
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct Manifest {
    pub heap_path: String,
    pub base_path: String,
    pub sstables: Vec<String>,
    #[serde(default)]
    pub l1_sstables: Vec<String>,
}

impl Manifest {
    pub fn load(path: &str) -> Result<Self, OmniError> {
        let content = std::fs::read_to_string(path)?;
        serde_json::from_str(&content).map_err(|e| OmniError::IoError(e.to_string()))
    }
    pub fn save(&self, path: &str) -> Result<(), OmniError> {
        let content = serde_json::to_string(self)
            .map_err(|e| OmniError::IoError(format!("Manifest serialize: {}", e)))?;
        let tmp_path = format!("{}.tmp", path);
        std::fs::write(&tmp_path, content)?;
        if let Ok(file) = std::fs::OpenOptions::new().write(true).open(&tmp_path) {
            let _ = file.sync_all();
        }
        std::fs::rename(&tmp_path, path)?;
        Ok(())
    }
}

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
            read_latencies: Mutex::new(hdrhistogram::Histogram::<u64>::new_with_bounds(1, 10_000_000, 3).unwrap_or_else(|_| hdrhistogram::Histogram::new(3).unwrap())),
            commit_latencies: Mutex::new(hdrhistogram::Histogram::<u64>::new_with_bounds(1, 10_000_000, 3).unwrap_or_else(|_| hdrhistogram::Histogram::new(3).unwrap())),
        }
    }
}

pub struct SSTableReader<'a> {
    data: &'a [u8],
}
impl<'a> SSTableReader<'a> {
    pub fn new(data: &'a [u8]) -> Self { Self { data } }
    
    fn get_block_for_key(&self, target: &[u8]) -> Option<usize> {
        if self.data.len() < 16 { return Some(0); }
        let magic = &self.data[self.data.len()-8..];
        if magic != b"OMNIV2**" { return Some(0); }
        
        let index_offset = u64::from_le_bytes(self.data[self.data.len()-16..self.data.len()-8].try_into().unwrap()) as usize;
        if index_offset >= self.data.len() { return Some(0); }
        
        let index_data = &self.data[index_offset..self.data.len()-16];
        if index_data.len() < 8 { return Some(0); }
        let num_entries = u64::from_le_bytes(index_data[0..8].try_into().unwrap()) as usize;
        
        let mut entries = Vec::with_capacity(num_entries);
        let mut curr = 8;
        for _ in 0..num_entries {
            if curr + 2 > index_data.len() { break; }
            let key_len = u16::from_le_bytes(index_data[curr..curr+2].try_into().unwrap()) as usize;
            curr += 2;
            if curr + key_len + 8 > index_data.len() { break; }
            let key = &index_data[curr..curr+key_len];
            curr += key_len;
            let offset = u64::from_le_bytes(index_data[curr..curr+8].try_into().unwrap()) as usize;
            curr += 8;
            entries.push((key, offset));
        }
        
        let idx = entries.partition_point(|&(k, _)| k <= target);
        if idx == 0 {
            Some(0)
        } else {
            Some(entries[idx - 1].1)
        }
    }

    pub fn find(&self, target_key: &[u8], read_seq: u64) -> Option<(u64, u64, u32, u64)> {
        let mut offset = self.get_block_for_key(target_key).unwrap_or(0);
        let mut best = None;
        let mut best_seq = 0;
        
        while offset < self.data.len() {
            if self.data.len() >= 16 && &self.data[self.data.len()-8..] == b"OMNIV2**" {
                let index_offset = u64::from_le_bytes(self.data[self.data.len()-16..self.data.len()-8].try_into().unwrap()) as usize;
                if offset >= index_offset { break; }
            }
            
            if let Some((rec, len)) = OmniRecord::decode(&self.data[offset..]) {
                offset += len;
                if rec.key.as_slice() > target_key { break; }
                if rec.key.as_slice() == target_key && rec.is_valid() && rec.seq <= read_seq {
                    if best.is_none() || rec.seq > best_seq {
                        best_seq = rec.seq;
                        best = Some((rec.offset, rec.length, rec.payload_crc32, rec.expiry));
                    }
                }
            } else {
                break;
            }
        }
        best
    }

    pub fn iter_from(&self, start_key: &[u8]) -> SSTableIterator<'a> {
        let offset = self.get_block_for_key(start_key).unwrap_or(0);
        SSTableIterator { data: self.data, offset }
    }
}

pub struct SSTableIterator<'a> {
    data: &'a [u8],
    offset: usize,
}
impl<'a> Iterator for SSTableIterator<'a> {
    type Item = OmniRecord;
    fn next(&mut self) -> Option<Self::Item> {
        if self.data.len() >= 16 && &self.data[self.data.len()-8..] == b"OMNIV2**" {
            let index_offset = u64::from_le_bytes(self.data[self.data.len()-16..self.data.len()-8].try_into().unwrap()) as usize;
            if self.offset >= index_offset { return None; }
        }
        if let Some((rec, len)) = OmniRecord::decode(&self.data[self.offset..]) {
            self.offset += len;
            Some(rec)
        } else {
            None
        }
    }
}

pub struct SSTableWriter<'a> {
    writer: BufWriter<&'a File>,
    offset: usize,
    index: Vec<(Vec<u8>, u64)>,
    current_block_start_key: Option<Vec<u8>>,
    block_size: usize,
}

impl<'a> SSTableWriter<'a> {
    pub fn new(file: &'a File) -> Self {
        Self {
            writer: BufWriter::new(file),
            offset: 0,
            index: Vec::new(),
            current_block_start_key: None,
            block_size: 4096,
        }
    }

    pub fn append(&mut self, record: &OmniRecord) -> Result<(), OmniError> {
        let bytes = record.encode();
        
        if self.current_block_start_key.is_none() {
            self.current_block_start_key = Some(record.key.clone());
            self.index.push((record.key.clone(), self.offset as u64));
        }
        
        self.writer.write_all(&bytes)?;
        self.offset += bytes.len();
        
        if let Some(_) = &self.current_block_start_key {
            if self.offset as u64 - self.index.last().unwrap().1 >= self.block_size as u64 {
                self.current_block_start_key = None;
            }
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<(), OmniError> {
        let index_offset = self.offset as u64;
        self.writer.write_all(&(self.index.len() as u64).to_le_bytes())?;
        for (k, off) in &self.index {
            self.writer.write_all(&(k.len() as u16).to_le_bytes())?;
            self.writer.write_all(k)?;
            self.writer.write_all(&off.to_le_bytes())?;
        }
        self.writer.write_all(&index_offset.to_le_bytes())?;
        self.writer.write_all(b"OMNIV2**")?;
        self.writer.flush()?;
        Ok(())
    }
}

pub struct OmniKV {
    base_mmap: ArcSwap<Mmap>,
    base_bloom: ArcSwap<BloomFilter>,
    sstables: ArcSwap<Vec<(Arc<Mmap>, Arc<BloomFilter>, String)>>, // L0
    l1_sstables: ArcSwap<Vec<(Arc<Mmap>, Arc<BloomFilter>, String)>>, // L1
    memtable: ArcSwap<[SkipMap<(Vec<u8>, Reverse<u64>), (u64, u64, u32, u64)>; NUM_SHARDS]>, // key -> (offset, length, crc, expiry)
    frozen_memtables: ArcSwap<Vec<Arc<[SkipMap<(Vec<u8>, Reverse<u64>), (u64, u64, u32, u64)>; NUM_SHARDS]>>>,
    global_seq: AtomicU64,
    heap_file: Mutex<File>,
    heap_offset: AtomicU64,
    wal: Mutex<wal::WriteAheadLog>,
    write_mutex: Mutex<()>,
    manifest: ArcSwap<Manifest>,
    manifest_path: String,
    pub metrics: Arc<Metrics>,
    active_snapshots: Mutex<std::collections::BTreeMap<u64, usize>>,
    block_cache: moka::sync::Cache<u64, String>,
    heap_reader: ArcSwap<File>,
}

impl OmniKV {
    /// Opens an OmniKV database from the given manifest and WAL paths.
    /// Recovers state from WAL and initializes the storage engine.
    pub fn open(manifest_path: &str, wal_path: &str) -> Result<Arc<Self>, OmniError> {
        let manifest = match Manifest::load(manifest_path) {
            Ok(m) => m,
            Err(_) => {
                let m = Manifest {
                    heap_path: format!("{}_heap.bin", manifest_path),
                    base_path: format!("{}_base.bin", manifest_path),
                    sstables: vec![],
                    l1_sstables: vec![],
                };
                m.save(manifest_path)?;
                m
            }
        };

        let base_file = OpenOptions::new().create(true).read(true).write(true).open(&manifest.base_path)?;
        if base_file.metadata()?.len() == 0 {
            base_file.set_len(4096)?; // dummy length to allow mmap
        }
        let base_mmap = unsafe { MmapOptions::new().map(&base_file)? };
        
        let reader = SSTableReader::new(&base_mmap);
        let base_bloom_path = manifest.base_path.replace(".bin", ".bloom");
        let base_bloom = match BloomFilter::load(&base_bloom_path) {
            Ok(b) => b,
            Err(_) => {
                let mut count = 0;
                for _ in reader.iter_from(b"") { count += 1; }
                let mut bloom = BloomFilter::new(count.max(100));
                for rec in reader.iter_from(b"") { 
                    let mut h = FnvHasher::default();
                    rec.key.hash(&mut h);
                    bloom.add(h.finish());
                }
                let _ = bloom.save(&base_bloom_path);
                bloom
            }
        };

        let mut loaded_sstables = Vec::new();
        for sst_path in &manifest.sstables {
            if let Ok(file) = OpenOptions::new().read(true).open(sst_path)
                && let Ok(mmap) = unsafe { MmapOptions::new().map(&file) } {
                    let reader = SSTableReader::new(&mmap);
                    let bloom_path = sst_path.replace(".sst", ".bloom");
                    let bloom = match BloomFilter::load(&bloom_path) {
                        Ok(b) => b,
                        Err(_) => {
                            let mut count = 0;
                            for _ in reader.iter_from(b"") { count += 1; }
                            let mut b = BloomFilter::new(count.max(100));
                            for rec in reader.iter_from(b"") {
                                if rec.is_valid() { 
                                    let mut h = FnvHasher::default();
                                    rec.key.hash(&mut h);
                                    b.add(h.finish());
                                }
                            }
                            let _ = b.save(&bloom_path);
                            b
                        }
                    };
                    loaded_sstables.push((Arc::new(mmap), Arc::new(bloom), sst_path.clone()));
                }
        }

        let mut loaded_l1_sstables = Vec::new();
        for sst_path in &manifest.l1_sstables {
            if let Ok(file) = OpenOptions::new().read(true).open(sst_path)
                && let Ok(mmap) = unsafe { MmapOptions::new().map(&file) } {
                    let reader = SSTableReader::new(&mmap);
                    let mut count = 0;
                    for _ in reader.iter_from(b"") { count += 1; }
                    let mut bloom = BloomFilter::new(count.max(100));
                    for rec in reader.iter_from(b"") {
                        if rec.is_valid() { 
                            let mut h = FnvHasher::default();
                            rec.key.hash(&mut h);
                            bloom.add(h.finish());
                        }
                    }
                    loaded_l1_sstables.push((Arc::new(mmap), Arc::new(bloom), sst_path.clone()));
                }
        }

        let memtable = Arc::new(std::array::from_fn(|_| SkipMap::new()));
        let mut global_seq = 0;
        
        let recovered_records = wal::WriteAheadLog::replay(wal_path, &manifest.heap_path)?;
        if !recovered_records.is_empty() {
            for rec in recovered_records {
                let shard = shard_idx(&rec.key);
                memtable[shard].insert((rec.key, Reverse(rec.seq)), (rec.offset, rec.length, rec.payload_crc32, rec.expiry));
                if rec.seq > global_seq { global_seq = rec.seq; }
            }
        }

        let heap_file = OpenOptions::new().create(true).read(true).append(true).open(&manifest.heap_path)?;
        let heap_offset = heap_file.metadata()?.len();

        let wal = wal::WriteAheadLog::new(wal_path)?;

        Ok(Arc::new(Self {
            base_mmap: ArcSwap::from_pointee(base_mmap),
            base_bloom: ArcSwap::from_pointee(base_bloom),
            sstables: ArcSwap::from_pointee(loaded_sstables),
            l1_sstables: ArcSwap::from_pointee(loaded_l1_sstables),
            memtable: ArcSwap::from_pointee(Arc::into_inner(memtable).unwrap()),
            frozen_memtables: ArcSwap::from_pointee(Vec::new()),
            global_seq: AtomicU64::new(global_seq + 1),
            heap_file: Mutex::new(heap_file),
            heap_offset: AtomicU64::new(heap_offset),
            wal: Mutex::new(wal),
            write_mutex: Mutex::new(()),
            heap_reader: ArcSwap::from_pointee(OpenOptions::new().read(true).open(&manifest.heap_path)?),
            manifest: ArcSwap::from_pointee(manifest),
            manifest_path: manifest_path.to_string(),
            metrics: Arc::new(Metrics::new()),
            active_snapshots: Mutex::new(std::collections::BTreeMap::new()),
            block_cache: moka::sync::Cache::builder().max_capacity(100_000).build(),
        }))
    }

    /// Returns the current global sequence number.
    pub fn get_seq(&self) -> u64 {
        self.global_seq.load(Ordering::SeqCst)
    }

    pub fn snapshot(&self) -> u64 {
        let seq = self.get_seq();
        let mut snaps = self.active_snapshots.lock().expect("FATAL: snapshot lock poisoned");
        *snaps.entry(seq).or_insert(0) += 1;
        seq
    }

    pub fn unregister_snapshot(&self, seq: u64) {
        let mut snaps = self.active_snapshots.lock().expect("FATAL: snapshot lock poisoned");
        if let Some(count) = snaps.get_mut(&seq) {
            *count -= 1;
            if *count == 0 {
                snaps.remove(&seq);
            }
        }
    }

    pub fn min_active_snapshot(&self) -> u64 {
        self.active_snapshots.lock().expect("FATAL: snapshot lock poisoned").keys().next().copied().unwrap_or(self.get_seq())
    }

    pub fn memtable_size(&self) -> usize {
        let mut sum = self.memtable.load().iter().map(|shard| shard.len()).sum::<usize>();
        for frozen in &**self.frozen_memtables.load() {
            sum += frozen.iter().map(|shard| shard.len()).sum::<usize>();
        }
        sum
    }

    pub fn min_memtable_seq(&self) -> u64 {
        let mut min_seq = self.get_seq();
        let memtable = self.memtable.load_full();
        for shard in memtable.iter() {
            if let Some(entry) = shard.front() {
                let seq = entry.key().1.0;
                if seq < min_seq {
                    min_seq = seq;
                }
            }
        }
        min_seq
    }

    pub fn scan(&self, start_key: &str, end_key: &str, read_seq: u64) -> Result<Vec<(String, String)>, OmniError> {
        let mut results = Vec::new();
        for (k_str, val) in self.scan_iter(start_key, end_key, read_seq)? {
            results.push((k_str, val));
        }
        Ok(results)
    }

    pub fn set_global_seq(&self, seq: u64) {
        self.global_seq.store(seq, Ordering::SeqCst);
    }

    pub fn total_records(&self) -> usize {
        self.memtable_size() // + other counts if needed
    }

    pub fn sstable_count(&self) -> usize {
        self.sstables.load().len()
    }

    pub fn l1_sstable_count(&self) -> usize {
        self.l1_sstables.load().len()
    }

    pub fn flush_memtable_to_disk(&self, _id: u64) -> Result<(), OmniError> {
        self.compact_sstables()
    }

    pub fn append_to_heap(&self, data: &[u8]) -> Result<u64, OmniError> {
        let _guard = self.write_mutex.lock().map_err(|_| OmniError::LockPoisoned("heap write mutex".into()))?;
        let mut heap = self.heap_file.lock().map_err(|_| OmniError::LockPoisoned("heap_file".into()))?;
        let offset = self.heap_offset.load(Ordering::SeqCst);
        heap.write_all(data)?;
        heap.sync_data()?;
        self.heap_offset.store(offset + data.len() as u64, Ordering::SeqCst);
        Ok(offset)
    }

    pub fn insert_replica_record(&self, key: Vec<u8>, seq: u64, offset: u64, length: u64, crc: u32, expiry: u64) {
        let shard = shard_idx(&key);
        let memtable = self.memtable.load_full();
        memtable[shard].insert((key, Reverse(seq)), (offset, length, crc, expiry));
        self.global_seq.fetch_max(seq + 1, Ordering::SeqCst);
    }

    pub fn sync_all(&self) -> Result<(), OmniError> {
        let _guard = self.write_mutex.lock().map_err(|_| OmniError::LockPoisoned("sync write mutex".into()))?;
        let heap = self.heap_file.lock().map_err(|_| OmniError::LockPoisoned("heap_file".into()))?;
        heap.sync_all()?;
        Ok(())
    }

    /// Commits a batch of write operations atomically.
    /// 
    /// Uses a 3-phase pipelined write (inspired by RocksDB):
    ///   Phase 1: Compression + CRC computation (no lock, fully parallel)
    ///   Phase 2: Sequence assignment + offset reservation (write_mutex, microseconds)
    ///   Phase 3: Heap I/O + WAL append + memtable insert (heap_file lock, no write_mutex)
    /// 
    /// This allows multiple concurrent batches to overlap their CPU-intensive
    /// compression work and only serialize briefly for sequence numbering.
    pub fn commit_batch(&self, tx: &WriteBatch) -> Result<u64, OmniError> {
        let start_time = std::time::Instant::now();
        
        // Write Backpressure: If L0 SSTables exceed 12, stall to let compaction catch up
        if self.sstable_count() >= 12 {
            return Err(OmniError::WriteStall);
        }
        
        // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        // PHASE 1: CPU-intensive work â€” NO LOCK (fully parallelizable)
        // Compression + CRC computation for all values in the batch.
        // Multiple threads can execute this phase simultaneously.
        // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        let mut prepped_writes = Vec::with_capacity(tx.buffered_writes.len());
        for (key, value, expiry) in &tx.buffered_writes {
            let payload_bytes = value.as_bytes();
            let (final_bytes, length) = if payload_bytes.len() < 64 {
                (payload_bytes.to_vec(), (payload_bytes.len() as u64) | UNCOMPRESSED_FLAG)
            } else {
                let comp = lz4_flex::compress_prepend_size(payload_bytes);
                let len = comp.len() as u64;
                (comp, len)
            };
            
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&final_bytes);
            let payload_crc32 = hasher.finalize();
            
            prepped_writes.push((key.clone(), final_bytes, length, payload_crc32, *expiry));
        }

        // Calculate total byte size needed in the heap for offset reservation
        let total_heap_bytes: u64 = prepped_writes.iter()
            .map(|(_, _, length, _, _)| length & !UNCOMPRESSED_FLAG)
            .sum();
        let num_records = prepped_writes.len() as u64 + tx.buffered_deletes.len() as u64;

        // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        // PHASE 2: Sequence + Offset reservation â€” WRITE_MUTEX (microseconds)
        // This is the ONLY serialization point. We assign monotonic sequence
        // numbers and reserve a contiguous region in the heap file.
        // No I/O happens here â€” just atomic integer operations.
        // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        let (base_seq, reserved_offset) = {
            let _guard = self.write_mutex.lock().map_err(|_| OmniError::LockPoisoned("commit_seq".into()))?;
            let seq = self.global_seq.fetch_add(num_records + 1, Ordering::SeqCst);
            let offset = self.heap_offset.fetch_add(total_heap_bytes, Ordering::SeqCst);
            (seq, offset)
        };
        // write_mutex is RELEASED here â€” other batches can now reserve their sequences

        // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        // PHASE 3: I/O + Memtable â€” HEAP_FILE lock (separate from write_mutex)
        // Write to heap at our reserved offset, then WAL, then memtable.
        // Multiple batches can interleave here (they write to non-overlapping
        // heap regions) though the heap_file Mutex serializes actual writes.
        // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        let mut wal_records = Vec::with_capacity(prepped_writes.len() + tx.buffered_deletes.len() + 1);
        let mut current_seq = base_seq;
        let mut current_offset = reserved_offset;
        
        for (key, final_bytes, length, payload_crc32, expiry) in prepped_writes {
            wal_records.push((OmniRecord::new(current_seq, key, current_offset, length, payload_crc32, expiry), Some(final_bytes)));
            current_offset += length & !UNCOMPRESSED_FLAG;
            current_seq += 1;
        }

        for key in &tx.buffered_deletes {
            wal_records.push((OmniRecord::new(current_seq, key.clone(), 0, 0, 0, 0), None));
            current_seq += 1;
        }

        let commit_marker = OmniRecord::new(current_seq, string_to_key("__COMMIT_MARKER__"), wal_records.len() as u64, 0, 0, 0);
        wal_records.push((commit_marker, None));

        // Heap write (serialized by heap_file Mutex, NOT by write_mutex)
        {
            let mut heap_writer = self.heap_file.lock().map_err(|_| OmniError::LockPoisoned("heap lock".into()))?;
            for (_, payload) in &wal_records {
                if let Some(bytes) = payload {
                    heap_writer.write_all(bytes)?;
                }
            }
            heap_writer.flush()?;
            heap_writer.sync_data()?;
        }

        // WAL write (serialized by wal Mutex, NOT by write_mutex)
        {
            let wal = self.wal.lock().map_err(|_| OmniError::LockPoisoned("wal lock".into()))?;
            wal.append_batch(&wal_records)?;
        }

        // Memtable insertion (SkipMap is lock-free for concurrent inserts)
        let memtable = self.memtable.load_full();
        for (rec, _) in &wal_records {
            if key_to_string(&rec.key) != "__COMMIT_MARKER__" {
                let shard = shard_idx(&rec.key);
                memtable[shard].insert((rec.key.clone(), Reverse(rec.seq)), (rec.offset, rec.length, rec.payload_crc32, rec.expiry));
            }
        }

        // Metrics
        if let Ok(mut lock) = self.metrics.commit_latencies.lock() {
            let _ = lock.record(start_time.elapsed().as_micros() as u64);
        }
        
        metrics_prometheus::WRITE_LATENCY.observe(start_time.elapsed().as_secs_f64());
        metrics_prometheus::MEMTABLE_SIZE.set(self.memtable_size() as i64);
        metrics_prometheus::SSTABLE_COUNT.set((self.sstable_count() + self.l1_sstable_count()) as i64);
        metrics_prometheus::COMMIT_RATE.inc();

        Ok(current_seq)
    }

    fn read_from_heap(&self, offset: u64, length_with_flag: u64, expected_crc32: u32) -> Result<String, OmniError> {
        if let Some(cached) = self.block_cache.get(&offset) {
            return Ok(cached);
        }
        
        let is_uncompressed = (length_with_flag & UNCOMPRESSED_FLAG) != 0;
        let length = length_with_flag & !UNCOMPRESSED_FLAG;
        
        let mut buf = vec![0u8; length as usize];
        let file = self.heap_reader.load();
        
        #[cfg(unix)]
        file.read_exact_at(&mut buf, offset)?;
        #[cfg(windows)]
        file.seek_read(&mut buf, offset)?;

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        if hasher.finalize() != expected_crc32 {
            return Err(OmniError::IoError("Payload CRC32 mismatch!".into()));
        }

        let result = if is_uncompressed {
            Ok(String::from_utf8_lossy(&buf).to_string())
        } else {
            let decompressed = lz4_flex::decompress_size_prepended(&buf).map_err(|e| OmniError::IoError(format!("LZ4 Error: {:?}", e)))?;
            Ok(String::from_utf8_lossy(&decompressed).to_string())
        };
        
        if let Ok(ref val) = result {
            self.block_cache.insert(offset, val.clone());
        }
        
        result
    }

    // Removed binary_search_records

    /// Finds a value by its key, up to the specified read sequence number (MVCC).
    /// Returns `Ok(None)` if the key does not exist or has been deleted.
    pub fn find(&self, target_key: &str, read_seq: u64) -> Result<Option<String>, OmniError> {
        let start_time = std::time::Instant::now();
        let target_key = string_to_key(target_key);
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();

        let shard = shard_idx(&target_key);
        
        let memtable = self.memtable.load_full();
        for entry in memtable[shard].range((target_key.clone(), Reverse(read_seq))..) {
            if entry.key().0 != target_key { break; }
            if entry.key().1.0 <= read_seq {
                let (_, length, _, expiry) = entry.value();
                if *length == 0 { return Ok(None); }
                if *expiry > 0 && *expiry <= now { return Ok(None); }
                let val = self.read_from_heap(entry.value().0, *length, entry.value().2)?;
                if let Ok(mut lock) = self.metrics.read_latencies.lock() { let _ = lock.record(start_time.elapsed().as_micros() as u64); }
                return Ok(Some(val));
            }
        }
        
        let frozen_memtables = self.frozen_memtables.load();
        for frozen in frozen_memtables.iter().rev() {
            for entry in frozen[shard].range((target_key.clone(), Reverse(read_seq))..) {
                if entry.key().0 != target_key { break; }
                if entry.key().1.0 <= read_seq {
                    let (_, length, _, expiry) = entry.value();
                    if *length == 0 { return Ok(None); }
                    if *expiry > 0 && *expiry <= now { return Ok(None); }
                    let val = self.read_from_heap(entry.value().0, *length, entry.value().2)?;
                    if let Ok(mut lock) = self.metrics.read_latencies.lock() { let _ = lock.record(start_time.elapsed().as_micros() as u64); }
                    return Ok(Some(val));
                }
            }
        }

        let mut pointer = None;
        let sstables = self.sstables.load();
        for (mmap, bloom, _) in sstables.iter().rev() {
            let mut h = FnvHasher::default();
            target_key.hash(&mut h);
            if bloom.might_contain(h.finish()) {
                let reader = SSTableReader::new(mmap);
                if let Some(ptr) = reader.find(&target_key, read_seq) {
                    pointer = Some(ptr);
                    break;
                }
            }
        }

        if pointer.is_none() {
            let l1_sstables = self.l1_sstables.load();
            for (mmap, bloom, _) in l1_sstables.iter().rev() {
                let mut h = FnvHasher::default();
                target_key.hash(&mut h);
                if bloom.might_contain(h.finish()) {
                    let reader = SSTableReader::new(mmap);
                    if let Some(ptr) = reader.find(&target_key, read_seq) {
                        pointer = Some(ptr);
                        break;
                    }
                }
            }
        }

        if pointer.is_none() {
            let base_bloom = self.base_bloom.load();
            let mut h = FnvHasher::default();
            target_key.hash(&mut h);
            if base_bloom.might_contain(h.finish()) {
                let base_mmap = self.base_mmap.load();
                let reader = SSTableReader::new(&base_mmap);
                pointer = reader.find(&target_key, read_seq);
            }
        }

        if let Some((offset, length, crc, expiry)) = pointer {
            if length == 0 { return Ok(None); }
            if expiry > 0 && expiry <= now { return Ok(None); }
            let val = self.read_from_heap(offset, length, crc)?;
            if let Ok(mut lock) = self.metrics.read_latencies.lock() { let _ = lock.record(start_time.elapsed().as_micros() as u64); }
            metrics_prometheus::READ_LATENCY.observe(start_time.elapsed().as_secs_f64());
            return Ok(Some(val));
        }

        metrics_prometheus::READ_LATENCY.observe(start_time.elapsed().as_secs_f64());
        Ok(None)
    }

    /// Returns an iterator over all key-value pairs within the given lexical range,
    /// up to the specified read sequence number (MVCC).
    /// 
    /// Uses a merge-sort approach: collects candidate records from all levels
    /// (memtable, frozen memtables, L0, L1, base), deduplicates by key keeping
    /// only the newest version visible at `read_seq`, then lazily reads values
    /// from the heap only when consumed.
    pub fn scan_iter(&self, start_key: &str, end_key: &str, read_seq: u64) -> Result<impl Iterator<Item = (String, String)> + '_, OmniError> {
        let start_key = string_to_key(start_key);
        let end_key = string_to_key(end_key);
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();

        // Phase 1: Collect candidate records from all levels into a single Vec.
        // Each entry: (key, seq, offset, length, crc, expiry)
        // We use a Vec instead of BTreeMap to avoid per-insert O(log N) overhead.
        let mut candidates: Vec<(Vec<u8>, u64, u64, u64, u32, u64)> = Vec::new();

        // Memtable (highest priority â€” newest data)
        let active_mem = self.memtable.load_full();
        for shard in active_mem.iter() {
            for entry in shard.range((start_key.clone(), Reverse(read_seq))..) {
                let key = entry.key().0.clone();
                if key > end_key { break; }
                let seq = entry.key().1.0;
                if seq <= read_seq {
                    candidates.push((key, seq, entry.value().0, entry.value().1, entry.value().2, entry.value().3));
                }
            }
        }

        // Frozen memtables (second priority)
        let frozen = self.frozen_memtables.load();
        for f_mem in frozen.iter().rev() {
            for shard in f_mem.iter() {
                for entry in shard.range((start_key.clone(), Reverse(read_seq))..) {
                    let key = entry.key().0.clone();
                    if key > end_key { break; }
                    let seq = entry.key().1.0;
                    if seq <= read_seq {
                        candidates.push((key, seq, entry.value().0, entry.value().1, entry.value().2, entry.value().3));
                    }
                }
            }
        }

        // L0 SSTables (newest first)
        let sstables = self.sstables.load();
        for (mmap, _, _) in sstables.iter().rev() {
            let reader = SSTableReader::new(mmap);
            for rec in reader.iter_from(&start_key) {
                if rec.key > end_key { break; }
                if rec.is_valid() && rec.seq <= read_seq {
                    candidates.push((rec.key, rec.seq, rec.offset, rec.length, rec.payload_crc32, rec.expiry));
                }
            }
        }

        // L1 SSTables
        let l1_sstables = self.l1_sstables.load();
        for (mmap, _, _) in &**l1_sstables {
            let reader = SSTableReader::new(mmap);
            for rec in reader.iter_from(&start_key) {
                if rec.key > end_key { break; }
                if rec.is_valid() && rec.seq <= read_seq {
                    candidates.push((rec.key, rec.seq, rec.offset, rec.length, rec.payload_crc32, rec.expiry));
                }
            }
        }

        // Base file (oldest data, lowest priority)
        let base_mmap = self.base_mmap.load();
        let reader = SSTableReader::new(&base_mmap);
        for rec in reader.iter_from(&start_key) {
            if rec.key > end_key { break; }
            if rec.is_valid() && rec.seq <= read_seq {
                candidates.push((rec.key, rec.seq, rec.offset, rec.length, rec.payload_crc32, rec.expiry));
            }
        }

        // Phase 2: Sort by (key ASC, seq DESC) â€” this groups duplicates together
        // with the newest version first, enabling O(N) deduplication in one pass.
        candidates.sort_unstable_by(|a, b| {
            a.0.cmp(&b.0).then(b.1.cmp(&a.1))
        });

        // Phase 3: Deduplicate â€” keep only the first (newest) entry per key.
        candidates.dedup_by(|a, b| a.0 == b.0);

        // Phase 4: Lazy heap I/O â€” only read values when the iterator is consumed.
        Ok(candidates.into_iter().filter_map(move |(k, _seq, offset, length, crc, expiry)| {
            if length == 0 || (expiry > 0 && expiry <= now) { return None; }
            let k_str = key_to_string(&k);
            self.read_from_heap(offset, length, crc).ok().map(|val| (k_str, val))
        }))
    }

    /// Returns the sequence number of the latest version of a key visible at `read_seq`.
    /// Used for Optimistic Concurrency Control (OCC) conflict detection.
    pub fn get_seq_for_key(&self, target_key: &str, read_seq: u64) -> u64 {
        let target_key = target_key.as_bytes().to_vec();
        let shard = shard_idx(&target_key);

        // 1. Active memtable
        let memtable = self.memtable.load_full();
        for entry in memtable[shard].range((target_key.clone(), Reverse(read_seq))..) {
            if entry.key().0 != target_key { break; }
            if entry.key().1.0 <= read_seq { return entry.key().1.0; }
        }

        // 2. Frozen memtables
        let frozen_memtables = self.frozen_memtables.load();
        for frozen in frozen_memtables.iter().rev() {
            for entry in frozen[shard].range((target_key.clone(), Reverse(read_seq))..) {
                if entry.key().0 != target_key { break; }
                if entry.key().1.0 <= read_seq { return entry.key().1.0; }
            }
        }

        // Helper: search SSTables via iter_from (returns OmniRecord with .seq)
        let search_sstable = |mmap: &Mmap, bloom: &BloomFilter| -> Option<u64> {
            let mut h = FnvHasher::default();
            target_key.hash(&mut h);
            if !bloom.might_contain(h.finish()) { return None; }
            let reader = SSTableReader::new(mmap);
            let mut best_seq = 0u64;
            for rec in reader.iter_from(&target_key) {
                if rec.key > target_key { break; }
                if rec.key == target_key && rec.is_valid() && rec.seq <= read_seq {
                    if rec.seq > best_seq { best_seq = rec.seq; }
                }
            }
            if best_seq > 0 { Some(best_seq) } else { None }
        };

        // 3. L0 SSTables (newest first)
        let sstables = self.sstables.load();
        for (mmap, bloom, _) in sstables.iter().rev() {
            if let Some(seq) = search_sstable(mmap, bloom) { return seq; }
        }

        // 4. L1 SSTables
        let l1_sstables = self.l1_sstables.load();
        for (mmap, bloom, _) in l1_sstables.iter().rev() {
            if let Some(seq) = search_sstable(mmap, bloom) { return seq; }
        }

        // 5. Base file
        {
            let base_bloom = self.base_bloom.load();
            let mut h = FnvHasher::default();
            target_key.hash(&mut h);
            if !base_bloom.might_contain(h.finish()) { return 0; }
        }
        {
            let base_mmap = self.base_mmap.load();
            let reader = SSTableReader::new(&base_mmap);
            let mut best_seq = 0u64;
            for rec in reader.iter_from(&target_key) {
                if rec.key > target_key { break; }
                if rec.key == target_key && rec.is_valid() && rec.seq <= read_seq {
                    if rec.seq > best_seq { best_seq = rec.seq; }
                }
            }
            if best_seq > 0 { return best_seq; }
        }
        0
    }

    /// Flushes the active memtable to disk as an L0 SSTable.
    pub fn compact_sstables(&self) -> Result<(), OmniError> {
        let (frozen_idx, frozen_arc) = {
            let _guard = self.write_mutex.lock().map_err(|_| OmniError::LockPoisoned("compact".into()))?;
            let old_memtable = self.memtable.swap(Arc::new(std::array::from_fn(|_| SkipMap::new())));
            let mut frozen = self.frozen_memtables.load().as_ref().clone();
            frozen.push(old_memtable.clone());
            self.frozen_memtables.store(Arc::new(frozen.clone()));
            if let Ok(mut wal) = self.wal.lock() {
                let _ = wal.rotate_segment();
            }
            (frozen.len() - 1, old_memtable)
        };
        
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
        let mut records = Vec::new();
        
        for shard in frozen_arc.iter() {
            for entry in shard.iter() {
                let key = entry.key().0.clone();
                let seq = entry.key().1.0;
                let val = entry.value();
                if val.3 == 0 || val.3 > now {
                    records.push(OmniRecord::new(seq, key, val.0, val.1, val.2, val.3));
                }
            }
        }
        
        if records.is_empty() { return Ok(()); }
        records.sort_by_key(|a| a.key.clone());
        
        let mut bloom = BloomFilter::new(records.len().max(100));
        for rec in &records {
            let mut h = FnvHasher::default();
            rec.key.hash(&mut h);
            bloom.add(h.finish());
        }
        
        let sst_path = format!("{}_l0_0_{}.sst", self.manifest_path, std::time::UNIX_EPOCH.elapsed().unwrap_or_default().as_millis());
        let _ = bloom.save(&sst_path.replace(".sst", ".bloom"));
        let file = OpenOptions::new().create(true).write(true).read(true).open(&sst_path)?;
        
        let mut writer = SSTableWriter::new(&file);
        for rec in &records {
            writer.append(rec)?;
        }
        writer.finish()?;
        
        file.sync_all()?;
        
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        
        let mut manifest = self.manifest.load().as_ref().clone();
        manifest.sstables.push(sst_path.clone());
        manifest.save(&self.manifest_path)?;
        self.manifest.store(Arc::new(manifest));
        
        let mut sstables = self.sstables.load().as_ref().clone();
        sstables.push((Arc::new(mmap), Arc::new(bloom), sst_path));
        self.sstables.store(Arc::new(sstables));
        
        let _guard = self.write_mutex.lock().map_err(|_| OmniError::LockPoisoned("compact".into()))?;
        let mut frozen = self.frozen_memtables.load().as_ref().clone();
        if frozen_idx < frozen.len() {
            frozen.remove(frozen_idx);
            self.frozen_memtables.store(Arc::new(frozen));
        }
        
        Ok(())
    }

    /// Compacts multiple L0 SSTables into a single L1 SSTable,
    /// dropping expired or obsolete records.
    pub fn compact_l0_to_l1(&self) -> Result<(), OmniError> {
        let _guard = self.write_mutex.lock().map_err(|_| OmniError::LockPoisoned("compact_l0".into()))?;
        let sstables = self.sstables.load();
        if sstables.is_empty() { return Ok(()); }
        
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
        let mut merged: BTreeMap<Vec<u8>, (u64, u64, u64, u32, u64)> = BTreeMap::new();
        
        for (mmap, _, _) in &**sstables {
            let reader = SSTableReader::new(mmap);
            for rec in reader.iter_from(b"") {
                if rec.is_valid() {
                    let existing = merged.get(&rec.key);
                    if existing.is_none() || existing.unwrap().0 < rec.seq {
                        merged.insert(rec.key.clone(), (rec.seq, rec.offset, rec.length, rec.payload_crc32, rec.expiry));
                    }
                }
            }
        }
        
        let mut records = Vec::new();
        for (key, (seq, offset, length, crc, expiry)) in merged {
            if expiry == 0 || expiry > now {
                records.push(OmniRecord::new(seq, key, offset, length, crc, expiry));
            }
        }
        
        if records.is_empty() { 
            let mut manifest = self.manifest.load().as_ref().clone();
            manifest.sstables.clear();
            manifest.save(&self.manifest_path)?;
            self.manifest.store(Arc::new(manifest));
            self.sstables.store(Arc::new(Vec::new()));
            
            let old_paths: Vec<String> = sstables.iter().map(|(_, _, p)| p.clone()).collect();
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_secs(5));
                for path in old_paths {
                    let _ = std::fs::remove_file(&path);
                    let _ = std::fs::remove_file(path.replace(".sst", ".bloom"));
                }
            });
            
            return Ok(());
        }
        
        records.sort_by_key(|a| a.key.clone());
        
        let mut bloom = BloomFilter::new(records.len().max(100));
        for rec in &records {
            let mut h = FnvHasher::default();
            rec.key.hash(&mut h);
            bloom.add(h.finish());
        }
        
        let sst_path = format!("{}_l1_{}.sst", self.manifest_path, std::time::UNIX_EPOCH.elapsed().unwrap_or_default().as_millis());
        let _ = bloom.save(&sst_path.replace(".sst", ".bloom"));
        let file = OpenOptions::new().create(true).write(true).read(true).open(&sst_path)?;
        let mut writer = SSTableWriter::new(&file);
        for rec in &records {
            writer.append(rec)?;
        }
        writer.finish()?;
        file.sync_all()?;
        
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        
        let mut manifest = self.manifest.load().as_ref().clone();
        manifest.sstables.clear();
        manifest.l1_sstables.push(sst_path.clone());
        manifest.save(&self.manifest_path)?;
        self.manifest.store(Arc::new(manifest));
        
        let mut l1_sstables = self.l1_sstables.load().as_ref().clone();
        l1_sstables.push((Arc::new(mmap), Arc::new(bloom), sst_path));
        self.l1_sstables.store(Arc::new(l1_sstables));
        
        self.sstables.store(Arc::new(Vec::new()));
        
        let old_paths: Vec<String> = sstables.iter().map(|(_, _, p)| p.clone()).collect();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(5));
            for path in old_paths {
                let _ = std::fs::remove_file(&path);
                let _ = std::fs::remove_file(path.replace(".sst", ".bloom"));
            }
        });
        
        Ok(())
    }

    pub fn compact_l1_to_l2(&self) -> Result<(), OmniError> {
        let _guard = self.write_mutex.lock().map_err(|_| OmniError::LockPoisoned("compact_l1_l2".into()))?;
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
        let mut merged: BTreeMap<Vec<u8>, (u64, u64, u64, u32, u64)> = BTreeMap::new();
        
        let base_mmap = self.base_mmap.load();
        let reader = SSTableReader::new(&base_mmap);
        for rec in reader.iter_from(b"") {
            if rec.is_valid() {
                let existing = merged.get(&rec.key);
                if existing.is_none() || existing.unwrap().0 < rec.seq {
                    merged.insert(rec.key, (rec.seq, rec.offset, rec.length, rec.payload_crc32, rec.expiry));
                }
            }
        }
        
        let l1_sstables_snap = self.l1_sstables.load();
        if l1_sstables_snap.is_empty() { return Ok(()); }
        let num_l1_merged = l1_sstables_snap.len();
        
        for (mmap, _, _) in &**l1_sstables_snap {
            let reader = SSTableReader::new(mmap);
            for rec in reader.iter_from(b"") {
                if rec.is_valid() {
                    let existing = merged.get(&rec.key);
                    if existing.is_none() || existing.unwrap().0 < rec.seq {
                        merged.insert(rec.key, (rec.seq, rec.offset, rec.length, rec.payload_crc32, rec.expiry));
                    }
                }
            }
        }
        
        let mut records = Vec::new();
        for (key, (seq, offset, length, crc, expiry)) in merged {
            // Drop tombstones and expired entries
            if length > 0 && (expiry == 0 || expiry > now) {
                records.push(OmniRecord::new(seq, key, offset, length, crc, expiry));
            }
        }
        
        records.sort_by_key(|a| a.key.clone());
        
        let mut bloom = BloomFilter::new(records.len().max(100));
        for rec in &records {
            let mut h = FnvHasher::default();
            rec.key.hash(&mut h);
            bloom.add(h.finish());
        }
        
        let new_base_path = format!("{}_base_{}.sst", self.manifest_path, std::time::UNIX_EPOCH.elapsed().unwrap_or_default().as_millis());
        let file = OpenOptions::new().create(true).write(true).read(true).open(&new_base_path)?;
        
        let mut writer = SSTableWriter::new(&file);
        for rec in &records {
            writer.append(rec)?;
        }
        writer.finish()?;
        file.sync_all()?;
        
        let new_base_mmap = unsafe { MmapOptions::new().map(&file)? };
        
        // Critical Section: Swap active files
        let old_base_path = self.manifest.load().base_path.clone();
        self.base_mmap.store(Arc::new(new_base_mmap));
        self.base_bloom.store(Arc::new(bloom));
        
        let current_l1 = self.l1_sstables.load();
        let remaining_l1 = current_l1[num_l1_merged..].to_vec();
        self.l1_sstables.store(Arc::new(remaining_l1.clone()));
        
        let mut manifest = self.manifest.load().as_ref().clone();
        manifest.base_path = new_base_path;
        manifest.l1_sstables = remaining_l1.into_iter().map(|(_,_,path)| path).collect();
        manifest.save(&self.manifest_path)?;
        self.manifest.store(Arc::new(manifest));
        
        let old_paths: Vec<String> = l1_sstables_snap[..num_l1_merged].iter().map(|(_, _, p)| p.clone()).collect();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(5));
            let _ = std::fs::remove_file(&old_base_path);
            for path in old_paths {
                let _ = std::fs::remove_file(&path);
                let _ = std::fs::remove_file(path.replace(".sst", ".bloom"));
            }
        });
        
        Ok(())
    }

    /// Runs the garbage collection process on the heap file, compacting it
    /// and freeing space taken up by obsolete values.
    pub fn run_garbage_collection(&self) -> Result<(), OmniError> {
        let _guard = self.write_mutex.lock().map_err(|_| OmniError::LockPoisoned("GC write mutex".into()))?;
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
        let min_seq = self.min_active_snapshot();
        let mut merged: BTreeMap<Vec<u8>, std::collections::BTreeMap<u64, (u64, u64, u32, u64)>> = BTreeMap::new();
        
        let mut add_to_merged = |key: Vec<u8>, seq: u64, offset: u64, length: u64, crc: u32, expiry: u64| {
            let versions = merged.entry(key).or_default();
            versions.insert(seq, (offset, length, crc, expiry));
            
            // keep the latest version PLUS any versions >= min_seq
            let max_seq = versions.keys().next_back().copied().unwrap_or(0);
            let mut keys_to_remove = Vec::new();
            for &s in versions.keys() {
                if s < min_seq && s < max_seq {
                    keys_to_remove.push(s);
                }
            }
            for k in keys_to_remove {
                versions.remove(&k);
            }
        };

        let base_mmap = self.base_mmap.load();
        let reader = SSTableReader::new(&base_mmap);
        for rec in reader.iter_from(b"") {
            if rec.is_valid() {
                add_to_merged(rec.key, rec.seq, rec.offset, rec.length, rec.payload_crc32, rec.expiry);
            }
        }
        
        let sstables = self.sstables.load();
        for (mmap, _, _) in &**sstables {
            let reader = SSTableReader::new(mmap);
            for rec in reader.iter_from(b"") {
                if rec.is_valid() {
                    add_to_merged(rec.key, rec.seq, rec.offset, rec.length, rec.payload_crc32, rec.expiry);
                }
            }
        }
        
        let l1_sstables = self.l1_sstables.load();
        for (mmap, _, _) in &**l1_sstables {
            let reader = SSTableReader::new(mmap);
            for rec in reader.iter_from(b"") {
                if rec.is_valid() {
                    add_to_merged(rec.key, rec.seq, rec.offset, rec.length, rec.payload_crc32, rec.expiry);
                }
            }
        }
        
        let memtable = self.memtable.load_full();
        for shard in memtable.iter() {
            for entry in shard.iter() {
                let key = entry.key().0.clone();
                let seq = entry.key().1.0;
                add_to_merged(key, seq, entry.value().0, entry.value().1, entry.value().2, entry.value().3);
            }
        }
        
        let new_heap_path = format!("{}_heap_compacted_{}.bin", self.manifest_path, std::time::UNIX_EPOCH.elapsed().unwrap_or_default().as_millis());
        let new_base_path = format!("{}_database_compacted_{}.bin", self.manifest_path, std::time::UNIX_EPOCH.elapsed().unwrap_or_default().as_millis());
        
        let new_heap_file = OpenOptions::new().create(true).write(true).read(true).open(&new_heap_path)?;
        let new_base_file = OpenOptions::new().create(true).write(true).read(true).open(&new_base_path)?;
        
        let mut new_heap_writer = BufWriter::new(&new_heap_file);
        let mut base_sst_writer = SSTableWriter::new(&new_base_file);
        
        let mut final_records = Vec::new();
        let mut bloom = BloomFilter::new(merged.len().max(100));
        let mut current_offset = 0;
        
        for (k, versions) in merged {
            for (seq, (old_offset, length, crc, expiry)) in versions {
            if length == 0 || (expiry > 0 && expiry <= now) { continue; }
            
            // Raw read, NO decompression!
            let manifest = self.manifest.load();
            let mut file = File::open(&manifest.heap_path)?;
            use std::io::Seek;
            file.seek(std::io::SeekFrom::Start(old_offset))?;
            let mut final_bytes = vec![0u8; (length & !UNCOMPRESSED_FLAG) as usize];
            file.read_exact(&mut final_bytes)?;
            
            // Validate CRC just to be safe
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&final_bytes);
            if hasher.finalize() != crc {
                return Err(OmniError::IoError("GC CRC32 mismatch!".into()));
            }
            let new_crc = crc;
            let new_length = length;
            
            new_heap_writer.write_all(&final_bytes)?;
            
            let mut h = FnvHasher::default();
            k.hash(&mut h);
            bloom.add(h.finish());
            
            let rec = OmniRecord::new(seq, k.clone(), current_offset, new_length, new_crc, expiry);
            base_sst_writer.append(&rec)?;
            final_records.push(rec);
            current_offset += new_length & !UNCOMPRESSED_FLAG;
            }
        }
        
        new_heap_writer.flush()?;
        base_sst_writer.finish()?;
        new_heap_file.sync_all()?;
        new_base_file.sync_all()?;
        
        drop(new_heap_writer);

        
        let new_base_mmap = unsafe { MmapOptions::new().map(&new_base_file)? };
        
        self.base_mmap.store(Arc::new(new_base_mmap));
        let _ = bloom.save(&new_base_path.replace(".bin", ".bloom"));
        self.base_bloom.store(Arc::new(bloom));
        self.sstables.store(Arc::new(Vec::new()));
        self.l1_sstables.store(Arc::new(Vec::new()));
        self.block_cache.invalidate_all();
        
        let mut heap_lock = self.heap_file.lock().map_err(|_| OmniError::LockPoisoned("heap_file".into()))?;
        *heap_lock = new_heap_file;
        self.heap_reader.store(Arc::new(OpenOptions::new().read(true).open(&new_heap_path)?));
        self.heap_offset.store(current_offset, Ordering::SeqCst);
        let memtable = self.memtable.load_full();
        for shard in memtable.iter() {
            shard.clear();
        }
        
        let old_manifest = self.manifest.load().as_ref().clone();
        let mut manifest = old_manifest.clone();
        manifest.heap_path = new_heap_path;
        manifest.base_path = new_base_path;
        manifest.sstables.clear();
        manifest.l1_sstables.clear();
        manifest.save(&self.manifest_path)?;
        self.manifest.store(Arc::new(manifest));
        
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(5));
            let _ = std::fs::remove_file(&old_manifest.heap_path);
            let _ = std::fs::remove_file(&old_manifest.base_path);
            for p in old_manifest.sstables.iter().chain(old_manifest.l1_sstables.iter()) {
                let _ = std::fs::remove_file(p);
                let _ = std::fs::remove_file(p.replace(".sst", ".bloom"));
            }
        });
        
        if let Ok(mut wal) = self.wal.lock() {
            let _ = wal.clear();
        }
        
        Ok(())
    }
}
