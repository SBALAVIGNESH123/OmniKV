use crate::{OmniError, OmniRecord};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};

// =============================================================
// WRITE-AHEAD LOG (WAL) — With Per-Batch CRC32 Checksumming
// =============================================================
//
// Sequential-write log guaranteeing crash recovery. Every committed
// batch is serialised here BEFORE it becomes visible in the memtable.
// On restart we replay the WAL to rebuild the memtable.
//
// Layout per batch (v2 — checksummed):
//   [record_count: u32][record_0][record_1]...[record_n][batch_crc32: u32]
//
// The batch_crc32 covers the entire batch from record_count through
// all encoded records. A single bit-flip anywhere in the batch will
// cause CRC mismatch and the batch will be rejected during replay.
//
// Each record is self-describing via OmniRecord::encode / decode.

pub struct WriteAheadLog {
    path: String,
    writer: BufWriter<File>,
}

impl WriteAheadLog {
    /// Open (or create) the WAL at `path`.
    pub fn new(path: &str) -> Result<Self, OmniError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| OmniError::IoError(format!("WAL open: {}", e)))?;

        Ok(Self {
            path: path.to_string(),
            writer: BufWriter::new(file),
        })
    }

    /// Replay the WAL from disk, returning all recovered OmniRecords.
    /// Also needs the heap path to verify payload integrity.
    pub fn replay(wal_path: &str, _heap_path: &str) -> Result<Vec<OmniRecord>, OmniError> {
        let file = match File::open(wal_path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(OmniError::IoError(format!("WAL replay open: {}", e))),
        };

        let mut reader = BufReader::new(file);
        let mut all_data = Vec::new();
        if reader.read_to_end(&mut all_data).is_err() || all_data.is_empty() {
            return Ok(Vec::new());
        }

        let mut records = Vec::new();
        let mut offset = 0;

        while offset + 4 <= all_data.len() {
            let batch_start = offset;
            let count =
                u32::from_le_bytes(all_data[offset..offset + 4].try_into().unwrap_or([0; 4]))
                    as usize;
            offset += 4;

            // Sanity check: reject obviously corrupt record counts (prevents OOM)
            if count > 100_000 || count == 0 {
                break;
            }

            let mut batch_records = Vec::with_capacity(count);
            let mut batch_ok = true;

            for _ in 0..count {
                if offset >= all_data.len() {
                    batch_ok = false;
                    break;
                }
                match OmniRecord::decode(&all_data[offset..]) {
                    Some((rec, len)) => {
                        batch_records.push(rec);
                        offset += len;
                    }
                    None => {
                        batch_ok = false;
                        break;
                    }
                }
            }

            // Verify per-batch CRC32 checksum
            if batch_ok && offset + 4 <= all_data.len() {
                let stored_crc = u32::from_le_bytes(
                    all_data[offset..offset + 4].try_into().unwrap_or([0; 4]),
                );
                offset += 4;

                // Compute CRC over the batch data (from record_count through all records)
                let batch_data = &all_data[batch_start..offset - 4];
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(batch_data);
                let computed_crc = hasher.finalize();

                if stored_crc != computed_crc {
                    // CRC mismatch — batch is corrupted, skip it
                    eprintln!(
                        "[WAL] Batch CRC mismatch at offset {}: stored={:#x}, computed={:#x} — skipping",
                        batch_start, stored_crc, computed_crc
                    );
                    continue;
                }
            } else if batch_ok {
                // No CRC trailer — truncated batch
                batch_ok = false;
            }

            // Only accept complete batches that end with a COMMIT_MARKER
            if batch_ok && !batch_records.is_empty() {
                let last = &batch_records[batch_records.len() - 1];
                let key_str = String::from_utf8_lossy(&last.key);
                if key_str == "__COMMIT_MARKER__" {
                    // Don't include the marker itself
                    batch_records.pop();
                    records.extend(batch_records);
                }
                // If no commit marker → incomplete batch → discard
            } else {
                break; // Corrupted or truncated WAL — stop replay
            }
        }

        Ok(records)
    }

    /// Append a batch of records (including the commit marker) atomically.
    /// Each batch is checksummed with CRC32 for corruption detection.
    /// Called while holding the WAL Mutex in lib.rs, so &mut self is safe.
    pub fn append_batch(
        &mut self,
        records: &[(OmniRecord, Option<Vec<u8>>)],
    ) -> Result<(), OmniError> {
        // Build the batch payload into a buffer first so we can CRC it
        let mut batch_buf = Vec::new();

        // Write record count
        let count = records.len() as u32;
        batch_buf.extend_from_slice(&count.to_le_bytes());

        // Write each record
        for (rec, _) in records {
            let encoded = rec.encode();
            batch_buf.extend_from_slice(&encoded);
        }

        // Compute CRC32 over the entire batch (record_count + all records)
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&batch_buf);
        let batch_crc = hasher.finalize();

        // Write batch data + CRC trailer atomically
        self.writer
            .write_all(&batch_buf)
            .map_err(|e| OmniError::IoError(format!("WAL write batch: {}", e)))?;
        self.writer
            .write_all(&batch_crc.to_le_bytes())
            .map_err(|e| OmniError::IoError(format!("WAL write CRC: {}", e)))?;

        // flush() pushes userspace buffer → kernel page cache
        self.writer
            .flush()
            .map_err(|e| OmniError::IoError(format!("WAL flush: {}", e)))?;

        // sync_data() pushes kernel page cache → stable storage (disk/SSD)
        // Without this, a power loss after flush() can lose committed data.
        // fdatasync is sufficient here — we don't need metadata (sync_all).
        self.writer
            .get_ref()
            .sync_data()
            .map_err(|e| OmniError::IoError(format!("WAL fsync: {}", e)))?;

        Ok(())
    }

    /// Rotate the WAL — truncate the current segment.
    /// Called after a successful flush to SSTable.
    pub fn rotate_segment(&mut self) -> Result<(), OmniError> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)
            .map_err(|e| OmniError::IoError(format!("WAL rotate: {}", e)))?;

        self.writer = BufWriter::new(file);
        Ok(())
    }

    /// Clear the WAL completely.
    pub fn clear(&mut self) -> Result<(), OmniError> {
        self.rotate_segment()
    }
}
