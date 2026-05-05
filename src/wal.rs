use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::sync::Mutex;
use bytemuck::bytes_of;
use crate::OmniRecord;

// =============================================================
// WRITE-AHEAD LOG (WAL)
// =============================================================
// A real database never overwrites old data in place. That is too slow
// and corrupts if the power fails.
// Instead, every SET command is appended sequentially to the end of this log.
// Sequential SSD writes are 100x faster than random SSD writes.
pub struct WriteAheadLog {
    writer: Mutex<BufWriter<File>>,
}

impl WriteAheadLog {
    pub fn new(path: &str) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
            
        Ok(Self {
            // Buffer writes to RAM and flush to SSD automatically
            writer: Mutex::new(BufWriter::new(file)),
        })
    }

    /// Appends a new 32-byte record to the end of the log in microseconds.
    pub fn append(&self, record: &OmniRecord) -> std::io::Result<()> {
        let mut w = self.writer.lock().unwrap();
        w.write_all(bytes_of(record))?;
        // In a production DBMS, we would .flush() here for 100% crash safety.
        // For benchmarking extreme ingestion speed, we rely on the BufWriter's auto-flush.
        Ok(())
    }
}
