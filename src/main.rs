mod generator;
pub use omni_engine::{OmniKV, OmniRecord};

use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const FILE_PATH: &str = "database_v5.bin";
const WAL_PATH: &str = "wal.bin";
const DB_SIZE: usize = 1024 * 1024 * 1024; // 1 GB

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║     OMNI-ENGINE V8: THE CLICKHOUSE CHALLENGER            ║");
    println!("║     Zero-Copy | WAL Ingestion | Mutable DBMS             ║");
    println!("╚══════════════════════════════════════════════════════════╝\n");

    generator::generate_structured_db(FILE_PATH, DB_SIZE);

    // Open the Database with the Write-Ahead Log
    let db = Arc::new(OmniKV::open(FILE_PATH, WAL_PATH).expect("Failed to open database"));
    println!("Database initialized. Total records: {}", db.total_records());

    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("🚀 Server listening on 127.0.0.1:8080");
    println!("Try it: `telnet 127.0.0.1 8080`\nCommands: GET <key> | SET <key> <string_value>\n");

    loop {
        let (mut socket, addr) = listener.accept().await?;
        let db_clone = Arc::clone(&db);

        tokio::spawn(async move {
            let mut buf = [0; 1024];

            loop {
                let n = match socket.read(&mut buf).await {
                    Ok(n) if n == 0 => return,
                    Ok(n) => n,
                    Err(_) => return,
                };

                let request = String::from_utf8_lossy(&buf[0..n]);
                let request = request.trim();
                if request.is_empty() { continue; }
                println!("    [REQ] {} -> {}", addr, request);

                let mut parts = request.split_whitespace();
                let cmd = parts.next().unwrap_or("");
                
                let response = if cmd.eq_ignore_ascii_case("GET") {
                    if let Some(key_str) = parts.next() {
                        if let Ok(target_key) = key_str.parse::<u64>() {
                            let start = std::time::Instant::now();
                            // Zero-Copy Direct Network Stream optimization conceptually happens here.
                            // The raw memory slice is converted to a string for standard telnet clients.
                            match db_clone.find(target_key) {
                                Some(payload) => {
                                    format!("OK ({}ms): {:?}\n", start.elapsed().as_micros() as f64 / 1000.0, &payload[0..8])
                                }
                                None => format!("NOT_FOUND ({}ms)\n", start.elapsed().as_micros() as f64 / 1000.0)
                            }
                        } else { "ERROR: Invalid Key.\n".to_string() }
                    } else { "ERROR: Missing Key.\n".to_string() }
                } else if cmd.eq_ignore_ascii_case("SET") {
                    if let Some(key_str) = parts.next() {
                        if let Ok(key) = key_str.parse::<u64>() {
                            let value_str = parts.next().unwrap_or("Default");
                            let mut payload = [0u8; 24];
                            let copy_len = std::cmp::min(value_str.len(), 24);
                            payload[..copy_len].copy_from_slice(&value_str.as_bytes()[..copy_len]);

                            let start = std::time::Instant::now();
                            // Extreme Ingestion: Append to WAL + Write to MemTable
                            if db_clone.set(key, payload).is_ok() {
                                format!("OK ({}ms): Record ingested.\n", start.elapsed().as_micros() as f64 / 1000.0)
                            } else {
                                "ERROR: Disk Write Failed.\n".to_string()
                            }
                        } else { "ERROR: Invalid Key.\n".to_string() }
                    } else { "ERROR: Missing Key.\n".to_string() }
                } else if cmd.eq_ignore_ascii_case("QUIT") {
                    let _ = socket.write_all(b"Goodbye.\n").await;
                    break;
                } else {
                    "ERROR: Unknown Command. (GET, SET, QUIT)\n".to_string()
                };

                if socket.write_all(response.as_bytes()).await.is_err() { return; }
            }
        });
    }
}
