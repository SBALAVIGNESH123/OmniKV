use crate::{OmniError, OmniRecord};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};

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
    durable_offset: u64,
    /// Bytes appended since `durable_offset` was last advanced. Tracked
    /// incrementally so a successful fsync records exactly what it
    /// flushed, instead of asking the filesystem how big the file is.
    pending: u64,
    /// Set when a failed batch append also failed to roll back partial bytes,
    /// fencing further appends/syncs/rotations until restart recovery heals WAL.
    write_fenced: bool,
}

impl WriteAheadLog {
    /// Open (or create) the WAL at `path`.
    pub fn new(path: &str) -> Result<Self, OmniError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| OmniError::IoError(format!("WAL open: {}", e)))?;

        // An existing WAL's bytes are already on stable storage. A failure
        // here means the durable offset is unknown, not zero: assuming zero
        // would let a later truncate discard persisted batches.
        let durable_offset = file
            .metadata()
            .map(|m| m.len())
            .map_err(|e| OmniError::IoError(format!("WAL length: {}", e)))?;

        Ok(Self {
            path: path.to_string(),
            writer: BufWriter::new(file),
            durable_offset,
            pending: 0,
            write_fenced: false,
        })
    }

    pub(crate) fn is_fenced(&self) -> bool {
        self.write_fenced
    }

    /// Replay the WAL from disk, returning all recovered OmniRecords without
    /// mutating the WAL file.
    pub fn replay(wal_path: &str, _heap_path: &str) -> Result<Vec<OmniRecord>, OmniError> {
        let (records, _valid_end, _file_len) = Self::replay_prefix(wal_path)?;
        Ok(records)
    }

    /// Replay the WAL from disk and durably truncate any torn or corrupted
    /// tail past the last complete committed batch.
    ///
    /// Without truncating the torn tail before opening the WAL for appends,
    /// new commits would land after the corrupt bytes and be silently dropped
    /// on the next restart.
    pub(crate) fn replay_and_heal(
        wal_path: &str,
        _heap_path: &str,
    ) -> Result<Vec<OmniRecord>, OmniError> {
        let (records, valid_end, file_len) = Self::replay_prefix(wal_path)?;
        if valid_end < file_len {
            let file = OpenOptions::new()
                .write(true)
                .open(wal_path)
                .map_err(|e| OmniError::IoError(format!("WAL heal open: {e}")))?;
            file.set_len(valid_end as u64)
                .map_err(|e| OmniError::IoError(format!("WAL heal truncate: {e}")))?;
            file.sync_all()
                .map_err(|e| OmniError::IoError(format!("WAL heal fsync: {e}")))?;
            eprintln!(
                "[WAL] Truncated torn/corrupt tail at offset {valid_end} (was {file_len} bytes)"
            );
        }
        Ok(records)
    }

    /// Scans the WAL strictly from byte 0, stopping at the first incomplete,
    /// non-committed, or CRC-mismatched batch so later batches can never
    /// replay over a corrupted predecessor. Returns `(records, valid_end, file_len)`.
    fn replay_prefix(wal_path: &str) -> Result<(Vec<OmniRecord>, usize, usize), OmniError> {
        let file = match File::open(wal_path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((Vec::new(), 0, 0)),
            Err(e) => return Err(OmniError::IoError(format!("WAL replay open: {}", e))),
        };

        let mut reader = BufReader::new(file);
        let mut all_data = Vec::new();
        reader
            .read_to_end(&mut all_data)
            .map_err(|e| OmniError::IoError(format!("WAL replay read: {}", e)))?;
        if all_data.is_empty() {
            return Ok((Vec::new(), 0, 0));
        }

        let file_len = all_data.len();
        let mut records = Vec::new();
        let mut offset = 0;
        let mut valid_end = 0;

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

            // Cap preallocation to the maximum number of encoded records that
            // could physically fit in the remaining WAL bytes.
            let remaining = all_data.len().saturating_sub(offset);
            let mut batch_records = Vec::with_capacity(count.min(remaining / 16));
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
                let stored_crc =
                    u32::from_le_bytes(all_data[offset..offset + 4].try_into().unwrap_or([0; 4]));
                offset += 4;

                // Compute CRC over the batch data (from record_count through all records)
                let batch_data = &all_data[batch_start..offset - 4];
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(batch_data);
                let computed_crc = hasher.finalize();

                if stored_crc != computed_crc {
                    // CRC mismatch — stop replay at this atomic prefix boundary so
                    // no later batch can apply out of causal order over corrupt history.
                    eprintln!(
                        "[WAL] Batch CRC mismatch at offset {}: stored={:#x}, computed={:#x} — stopping replay",
                        batch_start, stored_crc, computed_crc
                    );
                    break;
                }
            } else {
                // Truncated batch or missing CRC trailer — stop replay.
                break;
            }

            // Only accept complete batches that end with a COMMIT_MARKER.
            if let Some(last) = batch_records.last()
                && last.key.as_slice() == b"__COMMIT_MARKER__"
            {
                batch_records.pop();
                records.extend(batch_records);
                valid_end = offset;
            } else {
                // Incomplete / uncommitted batch — stop replay at batch_start.
                break;
            }
        }

        Ok((records, valid_end, file_len))
    }

    fn encode_batch(records: &[(OmniRecord, Option<Vec<u8>>)]) -> (Vec<u8>, u32) {
        let mut batch_buf = Vec::new();
        let count = records.len() as u32;
        batch_buf.extend_from_slice(&count.to_le_bytes());
        for (rec, _) in records {
            let encoded = rec.encode();
            batch_buf.extend_from_slice(&encoded);
        }
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&batch_buf);
        let batch_crc = hasher.finalize();
        (batch_buf, batch_crc)
    }

    fn write_batch_payload(&mut self, batch_buf: &[u8], batch_crc: u32) -> Result<(), OmniError> {
        self.writer
            .write_all(batch_buf)
            .map_err(|e| OmniError::IoError(format!("WAL write batch: {}", e)))?;
        self.writer
            .write_all(&batch_crc.to_le_bytes())
            .map_err(|e| OmniError::IoError(format!("WAL write CRC: {}", e)))?;
        self.writer
            .flush()
            .map_err(|e| OmniError::IoError(format!("WAL flush: {}", e)))?;
        Ok(())
    }

    /// Reopens the WAL without append mode, drops any stale buffered writer
    /// BEFORE truncating to `target_offset`, syncs the truncation, and positions
    /// the fresh writer at EOF.
    fn truncate_to_offset(&mut self, target_offset: u64) -> Result<(), OmniError> {
        // Reopened without append mode: on Windows, set_len on an append-mode
        // handle fails with access denied. Replace `self.writer` BEFORE calling
        // `set_len` so dropping the previous `BufWriter` cannot flush stale
        // buffered bytes after truncation.
        let file = OpenOptions::new()
            .write(true)
            .open(&self.path)
            .map_err(|e| OmniError::IoError(format!("WAL reopen for truncate: {}", e)))?;
        self.writer = BufWriter::new(file);
        self.writer
            .get_ref()
            .set_len(target_offset)
            .map_err(|e| OmniError::IoError(format!("WAL truncate: {}", e)))?;
        self.writer
            .get_ref()
            .sync_all()
            .map_err(|e| OmniError::IoError(format!("WAL truncate fsync: {}", e)))?;
        self.writer
            .seek(SeekFrom::End(0))
            .map_err(|e| OmniError::IoError(format!("WAL seek after truncate: {}", e)))?;
        Ok(())
    }

    /// Append a batch of records (including the commit marker) atomically.
    /// Each batch is checksummed with CRC32 for corruption detection.
    /// Called while holding the WAL Mutex in lib.rs, so &mut self is safe.
    pub fn append_batch(
        &mut self,
        records: &[(OmniRecord, Option<Vec<u8>>)],
    ) -> Result<(), OmniError> {
        self.append_batch_nosync(records)?;
        self.sync()
    }

    /// Write a batch to WAL WITHOUT fsync.
    /// Used by group commit: multiple batches are written, then a single
    /// fsync is issued by the group leader via `sync()`.
    ///
    /// On a write or flush failure, truncates back to the last complete batch
    /// boundary (`durable_offset + pending`, preserving complete unsynced
    /// batches in the current group) so a later commit can never land behind
    /// partial bytes. If that rollback also fails, fences the WAL until restart
    /// recovery repairs it.
    pub fn append_batch_nosync(
        &mut self,
        records: &[(OmniRecord, Option<Vec<u8>>)],
    ) -> Result<(), OmniError> {
        if self.write_fenced {
            return Err(OmniError::IoError(
                "WAL is fenced after an unrecovered partial write; reopen to recover".into(),
            ));
        }
        let (batch_buf, batch_crc) = Self::encode_batch(records);
        if let Err(err) = self.write_batch_payload(&batch_buf, batch_crc) {
            let complete_end = self.durable_offset + self.pending;
            return match self.truncate_to_offset(complete_end) {
                Ok(()) => Err(err),
                Err(rollback_err) => {
                    self.write_fenced = true;
                    Err(OmniError::IoError(format!(
                        "{err}; WAL rollback to complete batch offset {complete_end} failed: {rollback_err}"
                    )))
                }
            };
        }
        // Counted only after the bytes actually landed: a write that failed
        // partway must not be reported as durable by a later sync.
        self.pending += batch_buf.len() as u64 + std::mem::size_of::<u32>() as u64;
        Ok(())
    }

    /// A fsync that succeeded made every appended byte durable. Advance the
    /// offset from the byte count rather than re-reading the file length: a
    /// filesystem that cannot report the length must not be answered with
    /// zero, which would let a later truncate discard persisted batches.
    fn mark_pending_durable(&mut self) {
        self.durable_offset += self.pending;
        self.pending = 0;
    }

    /// Fsync the WAL file to stable storage.
    /// Called by group commit leader after all writers in the group have
    /// appended their batches.
    pub fn sync(&mut self) -> Result<(), OmniError> {
        if self.write_fenced {
            return Err(OmniError::IoError(
                "WAL is fenced after an unrecovered partial write; reopen to recover".into(),
            ));
        }
        crate::failpoints::maybe_fail("wal::sync")
            .map_err(|e| OmniError::IoError(format!("WAL failure point: {e}")))?;
        match self.writer.get_ref().sync_data() {
            Ok(()) => {
                self.mark_pending_durable();
                Ok(())
            }
            Err(e) => Err(OmniError::IoError(format!("WAL fsync: {}", e))),
        }
    }

    /// Truncates the WAL to the last offset known to be on stable storage.
    pub fn discard_undurable(&mut self) -> Result<(), OmniError> {
        self.truncate_to_offset(self.durable_offset)?;
        self.pending = 0;
        self.write_fenced = false;
        Ok(())
    }

    /// Rotate the WAL — truncate the current segment.
    /// Must only be called after every record in the WAL has been durably
    /// flushed and published in the manifest.
    pub fn rotate_segment(&mut self) -> Result<(), OmniError> {
        if self.write_fenced {
            return Err(OmniError::IoError(
                "WAL is fenced after an unrecovered partial write; refusing rotation".into(),
            ));
        }
        // Drain any buffered bytes before truncating so dropping the previous
        // `BufWriter` below cannot flush stale bytes back into the empty file.
        self.writer
            .flush()
            .map_err(|e| OmniError::IoError(format!("WAL rotate flush: {}", e)))?;
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)
            .map_err(|e| OmniError::IoError(format!("WAL rotate: {}", e)))?;
        // Once open(truncate=true) succeeds, the on-disk file length is 0.
        // Reconcile the handle and durability offsets BEFORE calling sync_all
        // so a failed sync never leaves a stale non-zero durable_offset behind.
        self.writer = BufWriter::new(file);
        self.durable_offset = 0;
        self.pending = 0;
        self.writer
            .get_ref()
            .sync_all()
            .map_err(|e| OmniError::IoError(format!("WAL rotate fsync: {}", e)))?;
        Ok(())
    }

    /// Clear the WAL completely.
    pub fn clear(&mut self) -> Result<(), OmniError> {
        self.rotate_segment()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn sample_batch(key: &str, seq: u64) -> Vec<(OmniRecord, Option<Vec<u8>>)> {
        vec![
            (
                OmniRecord::new(seq, key.as_bytes().to_vec(), 0, 0, 0, 0),
                None,
            ),
            (
                OmniRecord::new(seq + 1, b"__COMMIT_MARKER__".to_vec(), 1, 0, 0, 0),
                None,
            ),
        ]
    }

    #[test]
    fn truncate_to_offset_preserves_complete_unsynced_batches_and_drops_partial_tail() {
        let dir = TempDir::new().expect("wal test dir");
        let wal_path = dir.path().join("test.wal");
        let wal_str = wal_path.to_string_lossy().to_string();

        let mut wal = WriteAheadLog::new(&wal_str).expect("open wal");
        wal.append_batch(&sample_batch("synced_k", 1))
            .expect("append synced batch");
        wal.append_batch_nosync(&sample_batch("unsynced_complete_k", 3))
            .expect("append complete unsynced batch");

        let complete_end = wal.durable_offset + wal.pending;
        // Simulate partial trailing bytes from a failed large write.
        wal.writer
            .write_all(&[0x01, 0x00, 0x00, 0x00, 0xAA, 0xBB])
            .expect("write simulated partial bytes");
        wal.writer.flush().expect("flush simulated partial bytes");

        wal.truncate_to_offset(complete_end)
            .expect("rollback partial bytes to complete_end");
        wal.append_batch_nosync(&sample_batch("after_rollback_k", 5))
            .expect("append subsequent batch");
        wal.sync().expect("sync retained and new batches");
        drop(wal);

        let recovered = WriteAheadLog::replay(&wal_str, "").expect("replay wal");
        let keys: Vec<String> = recovered
            .into_iter()
            .map(|r| String::from_utf8_lossy(&r.key).to_string())
            .collect();
        assert_eq!(
            keys,
            vec!["synced_k", "unsynced_complete_k", "after_rollback_k"]
        );
    }

    #[test]
    fn fenced_wal_rejects_appends_sync_and_rotation_until_recovery() {
        let dir = TempDir::new().expect("fenced wal dir");
        let wal_path = dir.path().join("fenced.wal");
        let wal_str = wal_path.to_string_lossy().to_string();

        let mut wal = WriteAheadLog::new(&wal_str).expect("open wal");
        wal.write_fenced = true;

        assert!(wal.is_fenced());
        assert!(
            wal.append_batch_nosync(&sample_batch("blocked", 1))
                .is_err()
        );
        assert!(wal.sync().is_err());
        assert!(wal.rotate_segment().is_err());
    }
}
