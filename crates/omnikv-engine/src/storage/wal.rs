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

enum WalWriterState {
    Open(BufWriter<File>),
    Fenced,
}

pub struct WriteAheadLog {
    path: String,
    /// Explicit runtime writer state: `WalWriterState::Open` holds the active
    /// handle; `WalWriterState::Fenced` detaches and discards the writer so no
    /// append, sync, rotation, or Drop flush can touch the WAL until recovery
    /// restores a validated handle at EOF.
    writer: WalWriterState,
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
            writer: WalWriterState::Open(BufWriter::new(file)),
            durable_offset,
            pending: 0,
        })
    }

    pub(crate) fn is_fenced(&self) -> bool {
        matches!(self.writer, WalWriterState::Fenced)
    }

    fn detach_and_fence_writer(&mut self) {
        if let WalWriterState::Open(old_writer) =
            std::mem::replace(&mut self.writer, WalWriterState::Fenced)
        {
            // Deliberately disassemble the `BufWriter` with `into_parts()` so
            // any pending unwritten bytes from a failed append are discarded
            // without flushing onto disk on Drop.
            let (_old_file, _discarded_buf) = old_writer.into_parts();
        }
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
        let WalWriterState::Open(writer) = &mut self.writer else {
            return Err(OmniError::IoError(
                "WAL is fenced after an unrecovered partial write; reopen to recover".into(),
            ));
        };
        writer
            .write_all(batch_buf)
            .map_err(|e| OmniError::IoError(format!("WAL write batch: {}", e)))?;
        writer
            .write_all(&batch_crc.to_le_bytes())
            .map_err(|e| OmniError::IoError(format!("WAL write CRC: {}", e)))?;
        writer
            .flush()
            .map_err(|e| OmniError::IoError(format!("WAL flush: {}", e)))?;
        Ok(())
    }

    /// Detaches and discards any existing buffered writer (entering `Fenced`
    /// state BEFORE any fallible I/O), reopens the WAL without append mode,
    /// truncates to `target_offset`, optionally syncs the truncation, seeks to
    /// EOF, and restores `WalWriterState::Open` only after every step succeeds.
    ///
    /// Pass `sync_truncation = true` only when `target_offset <= self.durable_offset`
    /// (every retained byte already has durable heap payloads). When rolling
    /// back a failed append while earlier in-group batches are still unsynced
    /// (`pending > 0`), pass `sync_truncation = false` so WAL bytes are never
    /// force-fsynced ahead of `heap.sync_data()`.
    ///
    /// If any step fails, the WAL remains `WalWriterState::Fenced` and any
    /// rejected buffered bytes were already discarded without flushing on Drop.
    fn truncate_to_offset(
        &mut self,
        target_offset: u64,
        sync_truncation: bool,
    ) -> Result<(), OmniError> {
        self.detach_and_fence_writer();

        crate::failpoints::maybe_fail("wal::truncate_open")
            .map_err(|e| OmniError::IoError(format!("WAL truncate failure point: {e}")))?;

        // Reopen without append mode: on Windows, set_len on an append-mode
        // handle fails with access denied.
        let mut file = OpenOptions::new()
            .write(true)
            .open(&self.path)
            .map_err(|e| OmniError::IoError(format!("WAL reopen for truncate: {}", e)))?;
        file.set_len(target_offset)
            .map_err(|e| OmniError::IoError(format!("WAL truncate: {}", e)))?;
        if sync_truncation {
            file.sync_all()
                .map_err(|e| OmniError::IoError(format!("WAL truncate fsync: {}", e)))?;
        }
        file.seek(SeekFrom::End(0))
            .map_err(|e| OmniError::IoError(format!("WAL seek after truncate: {}", e)))?;

        self.writer = WalWriterState::Open(BufWriter::new(file));
        Ok(())
    }

    /// Append a batch of records (including the commit marker) atomically.
    /// Each batch is checksummed with CRC32 for corruption detection.
    /// If fsync fails after appending, rolls back undurable bytes so the
    /// rejected batch cannot silently commit on a subsequent append.
    pub fn append_batch(
        &mut self,
        records: &[(OmniRecord, Option<Vec<u8>>)],
    ) -> Result<(), OmniError> {
        self.append_batch_nosync(records)?;
        if let Err(err) = self.sync() {
            return match self.discard_undurable() {
                Ok(()) => Err(err),
                Err(rollback_err) => Err(OmniError::IoError(format!(
                    "{err}; WAL rollback to durable offset {} failed: {rollback_err}",
                    self.durable_offset
                ))),
            };
        }
        Ok(())
    }

    /// Write a batch to WAL WITHOUT fsync.
    /// Used by group commit: multiple batches are written, then a single
    /// fsync is issued by the group leader via `sync()`.
    ///
    /// On a write or flush failure, truncates back to the last complete batch
    /// boundary (`durable_offset + pending`, preserving complete unsynced
    /// batches in the current group without force-fsyncing them ahead of their
    /// heap payloads) so a later commit can never land behind partial bytes.
    /// If that rollback also fails, the WAL remains fenced until restart
    /// recovery repairs it.
    pub fn append_batch_nosync(
        &mut self,
        records: &[(OmniRecord, Option<Vec<u8>>)],
    ) -> Result<(), OmniError> {
        if self.is_fenced() {
            return Err(OmniError::IoError(
                "WAL is fenced after an unrecovered partial write; reopen to recover".into(),
            ));
        }
        let (batch_buf, batch_crc) = Self::encode_batch(records);
        if let Err(err) = self.write_batch_payload(&batch_buf, batch_crc) {
            let complete_end = self.durable_offset + self.pending;
            let sync_truncation = self.pending == 0;
            return match self.truncate_to_offset(complete_end, sync_truncation) {
                Ok(()) => Err(err),
                Err(rollback_err) => Err(OmniError::IoError(format!(
                    "{err}; WAL rollback to complete batch offset {complete_end} failed: {rollback_err}"
                ))),
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
        let WalWriterState::Open(writer) = &self.writer else {
            return Err(OmniError::IoError(
                "WAL is fenced after an unrecovered partial write; reopen to recover".into(),
            ));
        };
        crate::failpoints::maybe_fail("wal::sync")
            .map_err(|e| OmniError::IoError(format!("WAL failure point: {e}")))?;
        match writer.get_ref().sync_data() {
            Ok(()) => {
                self.mark_pending_durable();
                Ok(())
            }
            Err(e) => Err(OmniError::IoError(format!("WAL fsync: {}", e))),
        }
    }

    /// Truncates the WAL to the last offset known to be on stable storage.
    ///
    /// Rejects already-fenced WAL instances so a fenced WAL can never be
    /// silently un-fenced by a concurrent leader or direct caller. Otherwise
    /// enters `WalWriterState::Fenced` before attempting rollback and restores
    /// `WalWriterState::Open` (clearing `pending`) only after truncation, sync,
    /// and seek to EOF all succeed.
    pub fn discard_undurable(&mut self) -> Result<(), OmniError> {
        if self.is_fenced() {
            return Err(OmniError::IoError(
                "WAL is fenced after an unrecovered partial write; reopen to recover".into(),
            ));
        }
        self.truncate_to_offset(self.durable_offset, true)?;
        self.pending = 0;
        Ok(())
    }

    /// Rotate the WAL — truncate the current segment.
    /// Must only be called after every record in the WAL has been durably
    /// flushed and published in the manifest.
    pub fn rotate_segment(&mut self) -> Result<(), OmniError> {
        if self.is_fenced() {
            return Err(OmniError::IoError(
                "WAL is fenced after an unrecovered partial write; refusing rotation".into(),
            ));
        }
        // Do not flush buffered bytes before truncating: `detach_and_fence_writer`
        // below disassembles the old `BufWriter` via `into_parts()`, discarding
        // any buffer without flushing (and avoiding unnecessary writes under ENOSPC).
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)
            .map_err(|e| OmniError::IoError(format!("WAL rotate: {}", e)))?;
        // Once open(truncate=true) succeeds, the on-disk file length is 0.
        // Immediately detach the old handle into `WalWriterState::Fenced` and
        // reconcile durability offsets BEFORE `sync_all` so a failed sync
        // never leaves a stale non-zero `durable_offset` or an open writer.
        self.detach_and_fence_writer();
        self.durable_offset = 0;
        self.pending = 0;
        crate::failpoints::maybe_fail("wal::rotate_sync")
            .map_err(|e| OmniError::IoError(format!("WAL rotate failure point: {e}")))?;
        file.sync_all()
            .map_err(|e| OmniError::IoError(format!("WAL rotate fsync: {}", e)))?;
        self.writer = WalWriterState::Open(BufWriter::new(file));
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
        let WalWriterState::Open(writer) = &mut wal.writer else {
            panic!("expected open WAL writer");
        };
        writer
            .write_all(&[0x01, 0x00, 0x00, 0x00, 0xAA, 0xBB])
            .expect("write simulated partial bytes");
        writer.flush().expect("flush simulated partial bytes");

        wal.truncate_to_offset(complete_end, false)
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
    fn failed_truncate_open_fences_wal_and_discards_buffered_bytes_without_drop_flush() {
        let dir = TempDir::new().expect("fenced wal dir");
        let wal_path = dir.path().join("fenced.wal");
        let wal_str = wal_path.to_string_lossy().to_string();

        let mut wal = WriteAheadLog::new(&wal_str).expect("open wal");
        wal.append_batch(&sample_batch("durable_k", 1))
            .expect("append durable batch");
        let durable_len = std::fs::metadata(&wal_path).expect("wal metadata").len();

        // Stage unflushed garbage in the active `BufWriter`, then force
        // `truncate_to_offset` to fail at `OpenOptions::open` by pointing
        // `wal.path` at the directory itself.
        let WalWriterState::Open(writer) = &mut wal.writer else {
            panic!("expected open WAL writer");
        };
        writer
            .write_all(b"UNFLUSHED_GARBAGE_MUST_NOT_REACH_DISK")
            .expect("buffer unflushed bytes");
        wal.path = dir.path().to_string_lossy().to_string();

        assert!(
            wal.discard_undurable().is_err(),
            "truncate_to_offset must fail when reopening directory path"
        );
        assert!(wal.is_fenced(), "failed rollback must fence WAL");
        // Even if the path becomes valid again, a fenced WAL must NEVER be
        // silently un-fenced by `discard_undurable()`, append, sync, or rotate.
        wal.path = wal_str;
        assert!(
            wal.discard_undurable().is_err(),
            "discard_undurable must reject an already-fenced WAL"
        );
        assert!(wal.is_fenced(), "fenced WAL must remain fenced");
        assert!(
            wal.append_batch_nosync(&sample_batch("blocked", 3))
                .is_err()
        );
        assert!(wal.sync().is_err());
        assert!(wal.rotate_segment().is_err());

        // Dropping the fenced WAL must NOT flush the previously buffered bytes.
        drop(wal);
        let final_len = std::fs::metadata(&wal_path).expect("wal metadata").len();
        assert_eq!(
            final_len, durable_len,
            "detached BufWriter must discard buffered bytes without flushing on Drop"
        );
    }
}
