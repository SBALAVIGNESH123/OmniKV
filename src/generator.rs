//! Database Generator (Legacy Compatibility)
//!
//! Generates sample data for benchmarking and testing.
//! The modern OmniKV uses WriteBatch for data ingestion,
//! but this module is retained for standalone binary compatibility.

use std::path::Path;

/// Generate a sample database file if it doesn't exist.
/// In the modern architecture, data is ingested via WriteBatch + commit_batch.
pub fn generate_structured_db(file_path: &str, _size_bytes: usize) {
    if Path::new(file_path).exists() {
        println!("Database already exists. Skipping generation.");
        return;
    }
    println!("[OmniKV] No existing database at {}. Will create on first write.", file_path);
}
