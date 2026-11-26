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
        })
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

        // Counted only after the bytes actually landed: a write that failed
        // partway must not be reported as durable by a later sync.
        self.pending += batch_buf.len() as u64 + std::mem::size_of::<u32>() as u64;

        // sync_data() pushes kernel page cache → stable storage (disk/SSD)
        // Without this, a power loss after flush() can lose committed data.
        // fdatasync is sufficient here — we don't need metadata (sync_all).
        crate::failpoints::maybe_fail("wal::sync")
            .map_err(|e| OmniError::IoError(format!("WAL failure point: {e}")))?;
        self.writer
            .get_ref()
            .sync_data()
            .map_err(|e| OmniError::IoError(format!("WAL fsync: {}", e)))?;
        self.mark_pending_durable();

        Ok(())
    }

    /// Write a batch to WAL WITHOUT fsync.
    /// Used by group commit: multiple batches are written, then a single
    /// fsync is issued by the group leader via `sync()`.
    pub fn append_batch_nosync(
        &mut self,
        records: &[(OmniRecord, Option<Vec<u8>>)],
    ) -> Result<(), OmniError> {
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

        self.writer
            .write_all(&batch_buf)
            .map_err(|e| OmniError::IoError(format!("WAL write batch: {}", e)))?;
        self.writer
            .write_all(&batch_crc.to_le_bytes())
            .map_err(|e| OmniError::IoError(format!("WAL write CRC: {}", e)))?;
        self.writer
            .flush()
            .map_err(|e| OmniError::IoError(format!("WAL flush: {}", e)))?;
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
        self.writer
            .flush()
            .map_err(|e| OmniError::IoError(format!("WAL flush before truncate: {}", e)))?;
        // Reopened without append mode: on Windows, set_len on an
        // append-mode handle fails with access denied. That means the new
        // writer's cursor sits at zero, so it must be moved to the end
        // before the next append — otherwise the retained batches are
        // overwritten instead of extended.
        let mut file = OpenOptions::new()
            .write(true)
            .open(&self.path)
            .map_err(|e| OmniError::IoError(format!("WAL reopen for truncate: {}", e)))?;
        file.set_len(self.durable_offset)
            .map_err(|e| OmniError::IoError(format!("WAL truncate: {}", e)))?;
        file.sync_all()
            .map_err(|e| OmniError::IoError(format!("WAL truncate fsync: {}", e)))?;
        file.seek(SeekFrom::End(0))
            .map_err(|e| OmniError::IoError(format!("WAL seek after truncate: {}", e)))?;
        self.writer = BufWriter::new(file);
        self.pending = 0;
        Ok(())
    }

    /// Rotate the WAL — truncate the current segment.
    /// Must only be called after every record in the WAL has been durably
    /// flushed and published in the manifest.
    pub fn rotate_segment(&mut self) -> Result<(), OmniError> {
        // Drain any buffered bytes before truncating so dropping the previous
        // `BufWriter` below cannot flush stale bytes back into the empty file.
        let _ = self.writer.flush();
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
