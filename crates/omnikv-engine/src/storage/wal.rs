use crate::{OmniError, OmniRecord};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

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

const MAX_WAL_BATCH_RECORDS: usize = 100_000;
const HEAP_CRC_SCRATCH_BYTES: usize = 64 * 1024;
const MIN_ENCODED_RECORD_BYTES: usize = 42;

fn fenced_wal_error(action: &str) -> OmniError {
    OmniError::IoError(format!(
        "WAL is fenced after an unrecovered write, rollback, or rotation failure; refusing {action} until reopen"
    ))
}

#[cfg(unix)]
fn sync_parent_dir(path: &str) -> Result<(), OmniError> {
    let dir = Path::new(path)
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let handle = File::open(dir)
        .map_err(|e| OmniError::IoError(format!("WAL parent dir open '{}': {e}", dir.display())))?;
    handle
        .sync_all()
        .map_err(|e| OmniError::IoError(format!("WAL parent dir fsync '{}': {e}", dir.display())))
}

#[cfg(windows)]
fn sync_parent_dir(_path: &str) -> Result<(), OmniError> {
    Ok(())
}

#[cfg(unix)]
fn read_exact_at_file(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt as _;
    file.read_exact_at(buf, offset)
}

#[cfg(windows)]
fn read_exact_at_file(file: &File, mut buf: &mut [u8], mut offset: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt as _;
    while !buf.is_empty() {
        let n = file.seek_read(buf, offset)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unexpected EOF reading heap payload during WAL replay",
            ));
        }
        buf = &mut buf[n..];
        offset = offset.saturating_add(n as u64);
    }
    Ok(())
}

struct HeapValidator {
    file: File,
    len: u64,
    scratch: Vec<u8>,
}

impl HeapValidator {
    fn open(heap_path: &str) -> Result<Option<Self>, OmniError> {
        if heap_path.is_empty() {
            return Ok(None);
        }
        let file = File::open(heap_path)
            .map_err(|e| OmniError::IoError(format!("WAL heap open '{heap_path}': {e}")))?;
        let meta = file
            .metadata()
            .map_err(|e| OmniError::IoError(format!("WAL heap metadata '{heap_path}': {e}")))?;
        if !meta.is_file() {
            return Err(OmniError::IoError(format!(
                "WAL heap path '{heap_path}' is not a regular file"
            )));
        }
        Ok(Some(Self {
            file,
            len: meta.len(),
            scratch: vec![0u8; HEAP_CRC_SCRATCH_BYTES],
        }))
    }

    fn verify_record(&mut self, rec: &OmniRecord) -> Result<bool, OmniError> {
        let is_uncompressed = (rec.length & crate::storage::core::UNCOMPRESSED_FLAG) != 0;
        let raw_len = rec.length & !crate::storage::core::UNCOMPRESSED_FLAG;
        if (!is_uncompressed && raw_len < 4)
            || raw_len > crate::storage::core::MAX_ENCODED_VALUE_SIZE
        {
            return Ok(false);
        }
        let Some(end) = rec.offset.checked_add(raw_len) else {
            return Ok(false);
        };
        if end > self.len {
            return Ok(false);
        }

        let mut hasher = crc32fast::Hasher::new();
        let mut remaining = raw_len;
        let mut pos = rec.offset;
        let mut checked_lz4_header = is_uncompressed;

        while remaining > 0 {
            let chunk_len = usize::try_from(remaining.min(self.scratch.len() as u64))
                .expect("chunk length bounded by 64 KiB scratch buffer");
            let buf = &mut self.scratch[..chunk_len];
            match read_exact_at_file(&self.file, buf, pos) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(false),
                Err(e) => return Err(OmniError::IoError(format!("WAL heap read: {e}"))),
            }
            if !checked_lz4_header {
                let declared = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
                if declared > crate::storage::core::MAX_VALUE_SIZE {
                    return Ok(false);
                }
                checked_lz4_header = true;
            }
            hasher.update(buf);
            pos = pos.saturating_add(chunk_len as u64);
            remaining -= chunk_len as u64;
        }
        Ok(hasher.finalize() == rec.payload_crc32)
    }
}

enum HeapCheck<'a> {
    /// Structural WAL check only (used by `WriteAheadLog::new`, which has no
    /// heap path). Validates framing, record CRCs, batch CRCs, and commit markers.
    Skip,
    /// Full recovery check (used by `replay`, `replay_and_heal*`, and
    /// `open_and_heal_since`): verifies every non-tombstone record in active
    /// batches against its referenced heap payload, LZ4 framing, and `payload_crc32`.
    Verify(Option<&'a mut HeapValidator>),
}

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
    /// Open (or create) the WAL at `path` for appending.
    ///
    /// Performs structural validation (record framing, per-record CRC, batch
    /// CRC, and `__COMMIT_MARKER__`) on any existing WAL bytes before returning
    /// an append handle, rejecting a structurally invalid or torn tail.
    ///
    /// Because `new` does not take a heap path, callers using external heap
    /// storage must run [`WriteAheadLog::replay_and_heal`] against the heap
    /// file before calling `new` (as `OmniKV::open` does via
    /// `open_and_heal_since`).
    pub fn new(path: &str) -> Result<Self, OmniError> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)
            .map_err(|e| OmniError::IoError(format!("WAL open: {}", e)))?;

        file.seek(SeekFrom::Start(0))
            .map_err(|e| OmniError::IoError(format!("WAL validate seek: {}", e)))?;
        let mut existing = Vec::new();
        file.read_to_end(&mut existing)
            .map_err(|e| OmniError::IoError(format!("WAL validate read: {}", e)))?;
        let file_len = existing.len();
        let (_records, valid_end) =
            Self::parse_valid_prefix(&existing, &mut HeapCheck::Skip, None, false)?;
        if valid_end < file_len {
            return Err(OmniError::IoError(format!(
                "WAL '{}' has an invalid or torn tail at offset {valid_end} (file length {file_len}); call WriteAheadLog::replay_and_heal before opening for append",
                path
            )));
        }
        // Unconditionally sync file data/metadata first, then parent directory
        // entry, so retries after an earlier failed open/truncate never skip
        // durability barriers based on non-durable dentry/page-cache state.
        file.sync_all()
            .map_err(|e| OmniError::IoError(format!("WAL open fsync: {}", e)))?;
        sync_parent_dir(path)?;
        file.seek(SeekFrom::End(0))
            .map_err(|e| OmniError::IoError(format!("WAL seek EOF: {}", e)))?;

        Ok(Self {
            path: path.to_string(),
            writer: WalWriterState::Open(BufWriter::new(file)),
            durable_offset: file_len as u64,
            pending: 0,
        })
    }

    /// Single-pass crash recovery and WAL open used by `OmniKV::recover_storage_roots`.
    ///
    /// Reads the WAL once, verifies every non-checkpointed batch against
    /// `heap_path`, durably truncates any torn/corrupt/heap-incomplete tail,
    /// syncs the file and parent directory, and returns the ready-to-append
    /// `WriteAheadLog` handle alongside the recovered records.
    pub(crate) fn open_and_heal_since(
        wal_path: &str,
        heap_path: &str,
        wal_checkpoint_seq: Option<u64>,
    ) -> Result<(Self, Vec<OmniRecord>), OmniError> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(wal_path)
            .map_err(|e| OmniError::IoError(format!("WAL open_and_heal: {e}")))?;

        let mut all_data = Vec::new();
        file.read_to_end(&mut all_data)
            .map_err(|e| OmniError::IoError(format!("WAL open_and_heal read: {e}")))?;
        let file_len = all_data.len();

        let (records, valid_end) = if all_data.is_empty() {
            (Vec::new(), 0)
        } else {
            let mut heap_validator = HeapValidator::open(heap_path)?;
            let mut heap_check = HeapCheck::Verify(heap_validator.as_mut());
            Self::parse_valid_prefix(&all_data, &mut heap_check, wal_checkpoint_seq, true)?
        };

        let truncated = valid_end < file_len;
        if truncated {
            file.set_len(valid_end as u64)
                .map_err(|e| OmniError::IoError(format!("WAL heal truncate: {e}")))?;
        }
        // Unconditionally sync file before parent directory so both truncation
        // (including truncation to 0) and file creation are durable even on
        // retry after a previous partial failure.
        file.sync_all()
            .map_err(|e| OmniError::IoError(format!("WAL heal fsync: {e}")))?;
        sync_parent_dir(wal_path)?;
        if truncated {
            tracing::warn!(
                wal_path,
                valid_end,
                file_len,
                "truncated torn, corrupt, or heap-incomplete WAL tail"
            );
        }
        file.seek(SeekFrom::End(0))
            .map_err(|e| OmniError::IoError(format!("WAL open_and_heal seek EOF: {e}")))?;

        Ok((
            Self {
                path: wal_path.to_string(),
                writer: WalWriterState::Open(BufWriter::new(file)),
                durable_offset: valid_end as u64,
                pending: 0,
            },
            records,
        ))
    }

    /// Returns `true` if the WAL writer has been fenced after an unrecovered
    /// rollback or rotation failure and rejects further mutations until reopen.
    pub fn is_fenced(&self) -> bool {
        matches!(self.writer, WalWriterState::Fenced)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.durable_offset == 0 && self.pending == 0
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

    /// Replay the WAL from disk without mutating the WAL file.
    ///
    /// Stops at the first torn, uncommitted, CRC-mismatched, or heap-missing
    /// batch so every returned batch is both atomic and backed by intact heap
    /// payloads.
    pub fn replay(wal_path: &str, heap_path: &str) -> Result<Vec<OmniRecord>, OmniError> {
        let (records, _valid_end, _file_len) = Self::replay_prefix(wal_path, heap_path, None)?;
        Ok(records)
    }

    /// Replay the WAL from disk and durably truncate any torn, corrupted, or
    /// heap-incomplete tail past the last complete valid batch.
    ///
    /// Call this explicit repair path before [`WriteAheadLog::new`] when
    /// recovering from a crash or corrupted WAL tail.
    pub fn replay_and_heal(wal_path: &str, heap_path: &str) -> Result<Vec<OmniRecord>, OmniError> {
        Self::replay_and_heal_since(wal_path, heap_path, None)
    }

    /// Internal recovery helper used by `replay_and_heal` and unit tests that
    /// skips heap validation for fully checkpointed batches (`<= cp`) while
    /// strictly validating every active WAL batch's heap payloads.
    pub(crate) fn replay_and_heal_since(
        wal_path: &str,
        heap_path: &str,
        wal_checkpoint_seq: Option<u64>,
    ) -> Result<Vec<OmniRecord>, OmniError> {
        let (records, valid_end, file_len) =
            Self::replay_prefix(wal_path, heap_path, wal_checkpoint_seq)?;
        if valid_end < file_len {
            let file = OpenOptions::new()
                .write(true)
                .open(wal_path)
                .map_err(|e| OmniError::IoError(format!("WAL heal open: {e}")))?;
            file.set_len(valid_end as u64)
                .map_err(|e| OmniError::IoError(format!("WAL heal truncate: {e}")))?;
            file.sync_all()
                .map_err(|e| OmniError::IoError(format!("WAL heal fsync: {e}")))?;
            tracing::warn!(
                wal_path,
                valid_end,
                file_len,
                "truncated torn, corrupt, or heap-incomplete WAL tail"
            );
        }
        Ok(records)
    }

    /// Scans the WAL strictly from byte 0, stopping at the first incomplete,
    /// non-committed, CRC-mismatched, or heap-unverified batch so later batches
    /// can never replay over corrupt or partially durable history.
    /// Returns `(records, valid_end, file_len)`.
    fn replay_prefix(
        wal_path: &str,
        heap_path: &str,
        wal_checkpoint_seq: Option<u64>,
    ) -> Result<(Vec<OmniRecord>, usize, usize), OmniError> {
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

        let mut heap_validator = HeapValidator::open(heap_path)?;
        let mut heap_check = HeapCheck::Verify(heap_validator.as_mut());
        let file_len = all_data.len();
        let (records, valid_end) =
            Self::parse_valid_prefix(&all_data, &mut heap_check, wal_checkpoint_seq, true)?;
        Ok((records, valid_end, file_len))
    }

    fn batch_heap_payloads_valid(
        batch_records: &[OmniRecord],
        commit_marker_seq: u64,
        heap_check: &mut HeapCheck<'_>,
        wal_checkpoint_seq: Option<u64>,
    ) -> Result<bool, OmniError> {
        let HeapCheck::Verify(heap_validator) = heap_check else {
            return Ok(true);
        };
        if let Some(cp) = wal_checkpoint_seq {
            let all_checkpointed =
                commit_marker_seq <= cp && batch_records.iter().all(|rec| rec.seq <= cp);
            if all_checkpointed {
                return Ok(true);
            }
            let all_active = commit_marker_seq > cp && batch_records.iter().all(|rec| rec.seq > cp);
            if !all_active {
                // Reject a batch that straddles `wal_checkpoint_seq` so partial
                // batch application can never happen on manifest/WAL mismatch.
                return Ok(false);
            }
        }
        for rec in batch_records {
            if rec.length == 0 {
                continue;
            }
            let is_uncompressed = (rec.length & crate::storage::core::UNCOMPRESSED_FLAG) != 0;
            let raw_len = rec.length & !crate::storage::core::UNCOMPRESSED_FLAG;
            if is_uncompressed && raw_len == 0 {
                if crc32fast::hash(b"") != rec.payload_crc32 {
                    return Ok(false);
                }
                continue;
            }
            let Some(validator) = heap_validator.as_deref_mut() else {
                return Err(OmniError::IoError(
                    "cannot validate heap-backed WAL records without a heap path".into(),
                ));
            };
            if !validator.verify_record(rec)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn parse_valid_prefix(
        all_data: &[u8],
        heap_check: &mut HeapCheck<'_>,
        wal_checkpoint_seq: Option<u64>,
        collect_records: bool,
    ) -> Result<(Vec<OmniRecord>, usize), OmniError> {
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
            if count > MAX_WAL_BATCH_RECORDS || count == 0 {
                break;
            }

            // Cap preallocation to the maximum number of encoded records that
            // could physically fit in the remaining WAL bytes.
            let remaining = all_data.len().saturating_sub(offset);
            let mut batch_records =
                Vec::with_capacity(count.min(remaining / MIN_ENCODED_RECORD_BYTES));
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
                    tracing::warn!(
                        batch_start,
                        stored_crc,
                        computed_crc,
                        "WAL batch CRC mismatch — stopping replay"
                    );
                    break;
                }
            } else {
                // Truncated batch or missing CRC trailer — stop replay.
                break;
            }

            // Only accept complete batches that end with a COMMIT_MARKER AND
            // whose referenced heap payloads are durable and CRC-valid.
            if let Some(last) = batch_records.last()
                && last.key.as_slice() == b"__COMMIT_MARKER__"
            {
                let commit_marker_seq = last.seq;
                batch_records.pop();
                if !Self::batch_heap_payloads_valid(
                    &batch_records,
                    commit_marker_seq,
                    heap_check,
                    wal_checkpoint_seq,
                )? {
                    tracing::warn!(
                        batch_start,
                        "WAL batch references missing, corrupt, or checkpoint-straddling heap payload — stopping replay"
                    );
                    break;
                }
                if collect_records {
                    records.extend(batch_records);
                }
                valid_end = offset;
            } else {
                // Incomplete / uncommitted batch — stop replay at batch_start.
                break;
            }
        }

        Ok((records, valid_end))
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
            return Err(fenced_wal_error("append"));
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
    /// Rejects empty or oversized (`> 100_000`) batches before writing so a
    /// direct caller cannot emit a record count that `replay` refuses.
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
            return Err(fenced_wal_error("append"));
        }
        if records.is_empty() || records.len() > MAX_WAL_BATCH_RECORDS {
            return Err(OmniError::IoError(format!(
                "WAL batch record count {} out of valid range 1..={MAX_WAL_BATCH_RECORDS}",
                records.len()
            )));
        }
        if records
            .last()
            .is_none_or(|(rec, _)| rec.key.as_slice() != b"__COMMIT_MARKER__")
        {
            return Err(OmniError::IoError(
                "WAL batch must terminate with __COMMIT_MARKER__".into(),
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
            return Err(fenced_wal_error("sync"));
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
            return Err(fenced_wal_error("rollback"));
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
            return Err(fenced_wal_error("rotation"));
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
        // Verify `durable_offset` stayed strictly at the synced batch boundary
        // (so `unsynced_complete_k` remained pending, NOT prematurely marked durable).
        assert!(wal.pending > 0);
        assert!(wal.durable_offset < complete_end);
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

    #[test]
    fn new_rejects_corrupt_tail_until_explicit_replay_and_heal() {
        let dir = TempDir::new().expect("corrupt tail open dir");
        let wal_path = dir.path().join("tail.wal");
        let wal_str = wal_path.to_string_lossy().to_string();

        let mut wal = WriteAheadLog::new(&wal_str).expect("open fresh wal");
        wal.append_batch(&sample_batch("good_1", 1))
            .expect("append first batch");
        wal.append_batch(&sample_batch("corrupt_2", 3))
            .expect("append second batch");
        drop(wal);

        // Flip a bit in the trailing batch CRC32 so batch 2 fails CRC check.
        let mut bytes = std::fs::read(&wal_path).expect("read wal bytes");
        let last_idx = bytes.len() - 1;
        bytes[last_idx] ^= 0x01;
        std::fs::write(&wal_path, &bytes).expect("write corrupt crc wal");
        let corrupt_len = bytes.len() as u64;

        // Public `replay` must remain strictly read-only (does not mutate file).
        let ro_records = WriteAheadLog::replay(&wal_str, "").expect("read-only replay");
        assert_eq!(ro_records.len(), 1);
        assert_eq!(ro_records[0].key, b"good_1");
        assert_eq!(
            std::fs::metadata(&wal_path).expect("wal metadata").len(),
            corrupt_len,
            "public replay must remain strictly read-only"
        );

        // `WriteAheadLog::new` must reject opening the corrupt tail for append.
        assert!(
            WriteAheadLog::new(&wal_str).is_err(),
            "WriteAheadLog::new must reject opening a WAL with a corrupt tail"
        );

        // Explicit `replay_and_heal` repairs the tail, after which `new` and
        // subsequent `append_batch` succeed and replay returns both batches.
        let healed = WriteAheadLog::replay_and_heal(&wal_str, "").expect("heal tail");
        assert_eq!(healed.len(), 1);
        assert_eq!(healed[0].key, b"good_1");
        assert!(std::fs::metadata(&wal_path).expect("healed metadata").len() < corrupt_len);

        let mut reopened = WriteAheadLog::new(&wal_str).expect("open healed wal");
        reopened
            .append_batch(&sample_batch("good_3", 5))
            .expect("append after heal");
        drop(reopened);

        let final_keys: Vec<String> = WriteAheadLog::replay(&wal_str, "")
            .expect("replay final wal")
            .into_iter()
            .map(|r| String::from_utf8_lossy(&r.key).to_string())
            .collect();
        assert_eq!(final_keys, vec!["good_1", "good_3"]);
    }

    #[test]
    fn replay_and_heal_refuses_to_truncate_heap_backed_wal_when_heap_path_missing_or_empty() {
        let dir = TempDir::new().expect("missing heap wal dir");
        let wal_path = dir.path().join("heap_backed.wal");
        let heap_path = dir.path().join("heap.bin");
        let missing_heap_path = dir.path().join("does_not_exist.bin");
        let wal_str = wal_path.to_string_lossy().to_string();
        let heap_str = heap_path.to_string_lossy().to_string();
        let missing_heap_str = missing_heap_path.to_string_lossy().to_string();

        let payload = b"durable_payload";
        std::fs::write(&heap_path, payload).expect("write heap");
        let heap_batch = vec![
            (
                OmniRecord::new(
                    1,
                    b"k_heap".to_vec(),
                    0,
                    crate::storage::core::UNCOMPRESSED_FLAG | payload.len() as u64,
                    crc32fast::hash(payload),
                    0,
                ),
                None,
            ),
            (
                OmniRecord::new(2, b"__COMMIT_MARKER__".to_vec(), 1, 0, 0, 0),
                None,
            ),
        ];

        let mut wal = WriteAheadLog::new(&wal_str).expect("open wal");
        wal.append_batch(&heap_batch).expect("append heap batch");
        drop(wal);
        let original_len = std::fs::metadata(&wal_path).expect("wal meta").len();

        // Both an empty heap_path and a missing heap_path must fail closed
        // without truncating a single byte of the healthy WAL.
        assert!(WriteAheadLog::replay_and_heal(&wal_str, "").is_err());
        assert_eq!(
            std::fs::metadata(&wal_path).expect("wal meta").len(),
            original_len,
            "replay_and_heal with empty heap path must not truncate healthy WAL"
        );
        assert!(WriteAheadLog::replay_and_heal(&wal_str, &missing_heap_str).is_err());
        assert_eq!(
            std::fs::metadata(&wal_path).expect("wal meta").len(),
            original_len,
            "replay_and_heal with missing heap path must not truncate healthy WAL"
        );

        // Passing the valid heap path recovers the batch cleanly.
        let recovered =
            WriteAheadLog::replay_and_heal(&wal_str, &heap_str).expect("valid heap replay");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].key, b"k_heap");
    }

    #[test]
    fn replay_and_heal_since_skips_checkpointed_batch_and_recovers_later_valid_batch() {
        let dir = TempDir::new().expect("checkpoint wal dir");
        let wal_path = dir.path().join("cp.wal");
        let heap_path = dir.path().join("heap.bin");
        let wal_str = wal_path.to_string_lossy().to_string();
        let heap_str = heap_path.to_string_lossy().to_string();

        let payload2 = b"active_after_gc";
        std::fs::write(&heap_path, payload2).expect("write new gc heap");

        // Batch 1 (seq 1..=2 <= checkpoint 2) references an old pre-GC heap
        // offset far past `heap.bin`'s EOF; Batch 2 (seq 3..=4 > checkpoint 2)
        // references a valid payload in `heap.bin`.
        let pre_gc_payload_len = 12u64;
        let batch1 = vec![
            (
                OmniRecord::new(
                    1,
                    b"pre_gc_k".to_vec(),
                    999_999,
                    crate::storage::core::UNCOMPRESSED_FLAG | pre_gc_payload_len,
                    0xDEAD_BEEF,
                    0,
                ),
                None,
            ),
            (
                OmniRecord::new(2, b"__COMMIT_MARKER__".to_vec(), 1, 0, 0, 0),
                None,
            ),
        ];
        let batch2 = vec![
            (
                OmniRecord::new(
                    3,
                    b"post_gc_k".to_vec(),
                    0,
                    crate::storage::core::UNCOMPRESSED_FLAG | payload2.len() as u64,
                    crc32fast::hash(payload2),
                    0,
                ),
                None,
            ),
            (
                OmniRecord::new(4, b"__COMMIT_MARKER__".to_vec(), 1, 0, 0, 0),
                None,
            ),
        ];

        let mut wal = WriteAheadLog::new(&wal_str).expect("open wal");
        wal.append_batch(&batch1).expect("append pre-gc batch");
        wal.append_batch(&batch2).expect("append post-gc batch");
        drop(wal);
        let original_len = std::fs::metadata(&wal_path).expect("wal meta").len();

        let recovered = WriteAheadLog::replay_and_heal_since(&wal_str, &heap_str, Some(2))
            .expect("replay past checkpointed batch");
        let keys: Vec<&[u8]> = recovered.iter().map(|r| r.key.as_slice()).collect();
        assert_eq!(keys, vec![b"pre_gc_k".as_slice(), b"post_gc_k".as_slice()]);
        assert_eq!(
            std::fs::metadata(&wal_path).expect("wal meta").len(),
            original_len,
            "checkpointed batch must not cause WAL truncation"
        );
    }

    #[test]
    fn replay_rejects_bad_empty_crc_malformed_lz4_overflow_dir_heap_and_empty_append() {
        let dir = TempDir::new().expect("wal validation dir");
        let heap_path = dir.path().join("heap.bin");
        let heap_str = heap_path.to_string_lossy().to_string();

        // Write an 8-byte heap containing an oversized declared LZ4 length (1 GiB).
        let mut bad_lz4 = Vec::new();
        bad_lz4.extend_from_slice(&(1024u32 * 1024 * 1024).to_le_bytes());
        bad_lz4.extend_from_slice(&[1, 2, 3, 4]);
        std::fs::write(&heap_path, &bad_lz4).expect("write bad lz4 heap");

        // Direct WAL API must reject empty and uncommitted batches upfront.
        let guard_wal_path = dir.path().join("guard.wal");
        let guard_wal_str = guard_wal_path.to_string_lossy().to_string();
        let mut guard_wal = WriteAheadLog::new(&guard_wal_str).expect("open guard wal");
        assert!(
            guard_wal.append_batch_nosync(&[]).is_err(),
            "empty WAL batch must be rejected"
        );
        assert!(
            guard_wal
                .append_batch_nosync(&[(
                    OmniRecord::new(1, b"no_marker".to_vec(), 0, 0, 0, 0),
                    None
                )])
                .is_err(),
            "WAL batch without trailing __COMMIT_MARKER__ must be rejected"
        );
        drop(guard_wal);

        let cases = [
            // Corrupt CRC on empty string (`UNCOMPRESSED_FLAG | 0`)
            OmniRecord::new(
                1,
                b"bad_empty".to_vec(),
                0,
                crate::storage::core::UNCOMPRESSED_FLAG,
                0x1234_5678,
                0,
            ),
            // Compressed payload shorter than 4-byte LZ4 size prefix
            OmniRecord::new(
                2,
                b"short_lz4".to_vec(),
                0,
                3,
                crc32fast::hash(&bad_lz4[..3]),
                0,
            ),
            // Compressed payload declaring > MAX_VALUE_SIZE
            OmniRecord::new(
                3,
                b"huge_lz4".to_vec(),
                0,
                bad_lz4.len() as u64,
                crc32fast::hash(&bad_lz4),
                0,
            ),
            // Overflowing `offset + raw_len`
            OmniRecord::new(
                4,
                b"overflow".to_vec(),
                u64::MAX - 2,
                crate::storage::core::UNCOMPRESSED_FLAG | 8,
                0,
                0,
            ),
        ];

        for (idx, bad_rec) in cases.into_iter().enumerate() {
            let wal_path = dir.path().join(format!("bad_{idx}.wal"));
            let wal_str = wal_path.to_string_lossy().to_string();
            let mut wal = WriteAheadLog::new(&wal_str).expect("open case wal");
            let batch = vec![
                (bad_rec, None),
                (
                    OmniRecord::new(100, b"__COMMIT_MARKER__".to_vec(), 1, 0, 0, 0),
                    None,
                ),
            ];
            wal.append_batch(&batch).expect("append test case batch");
            drop(wal);

            let replayed = WriteAheadLog::replay(&wal_str, &heap_str).expect("replay bad case");
            assert!(
                replayed.is_empty(),
                "case {idx} must be rejected during heap-validated WAL replay"
            );
        }

        // Passing a directory as `heap_path` must fail closed without truncating.
        let dir_wal = dir.path().join("dir_heap.wal");
        let dir_wal_str = dir_wal.to_string_lossy().to_string();
        let mut wal = WriteAheadLog::new(&dir_wal_str).expect("open dir_heap wal");
        let batch = vec![
            (
                OmniRecord::new(
                    1,
                    b"k".to_vec(),
                    0,
                    crate::storage::core::UNCOMPRESSED_FLAG | 4,
                    crc32fast::hash(&bad_lz4[..4]),
                    0,
                ),
                None,
            ),
            (
                OmniRecord::new(2, b"__COMMIT_MARKER__".to_vec(), 1, 0, 0, 0),
                None,
            ),
        ];
        wal.append_batch(&batch).expect("append valid batch");
        drop(wal);
        let original_len = std::fs::metadata(&dir_wal).expect("wal meta").len();
        assert!(
            WriteAheadLog::replay_and_heal(&dir_wal_str, &dir.path().to_string_lossy()).is_err(),
            "directory heap_path must fail closed"
        );
        assert_eq!(
            std::fs::metadata(&dir_wal).expect("wal meta").len(),
            original_len,
            "directory heap_path error must not truncate healthy WAL"
        );
    }
}
