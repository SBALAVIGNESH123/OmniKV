use crate::OmniRecord;
use bytemuck::bytes_of;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::Instant;

pub fn generate_structured_db(file_path: &str, size_bytes: usize) {
    if Path::new(file_path).exists() {
        let metadata = std::fs::metadata(file_path).unwrap();
        if metadata.len() as usize == size_bytes {
            println!("Database already exists. Skipping generation.");
            return;
        }
    }

    println!("Generating V5 Structured Database (1GB of 32-byte records)...");
    let start = Instant::now();
    let file = File::create(file_path).unwrap();
    let mut writer = BufWriter::new(file);

    let record_size = std::mem::size_of::<OmniRecord>();
    let total_records = size_bytes / record_size;

    // We will inject a specific "Target Key" near the very end of the 1GB file
    // to prove the engine has to scan almost the entire massive database to find it.
    for i in 0..total_records {
        let record = OmniRecord {
            key: i as u64,
            payload: [0xAA; 24], // Dummy data
        };

        // Bytemuck safely casts the struct directly to raw bytes for instant disk writing
        writer.write_all(bytes_of(&record)).unwrap();
    }

    writer.flush().unwrap();
    println!("Database generated in {:.2?}. Total Records: {}", start.elapsed(), total_records);
}
