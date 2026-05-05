use std::sync::Arc;
use omni_engine::{OmniKV, WriteBatch};

const MANIFEST_PATH: &str = "manifest.json";
const WAL_PATH: &str = "wal.bin";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║     OmniKV — Embeddable + Distributed KV Engine         ║");
    println!("║     SSI | 2PC | PgWire | HTTP/3 | Rust from scratch     ║");
    println!("╚══════════════════════════════════════════════════════════╝\n");

    let db = OmniKV::open(MANIFEST_PATH, WAL_PATH)?;
    println!("Database initialized.");
    println!("  Global sequence: {}", db.get_seq());

    // Start PostgreSQL wire protocol server
    let db_clone = db.clone();
    let pgwire_handle = std::thread::spawn(move || {
        let server = omni_engine::pgwire::PgWireServer::new(db_clone, "127.0.0.1:5433");
        if let Err(e) = server.start() {
            eprintln!("[OmniKV] PgWire server error: {}", e);
        }
    });

    // Start TCP command interface
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
    println!("🚀 TCP server listening on 127.0.0.1:8080");
    println!("🐘 PostgreSQL wire protocol on 127.0.0.1:5433");
    println!("\nCommands: GET <key> | SET <key> <value> | DELETE <key> | SCAN <start> <end>\n");

    loop {
        let (mut socket, addr) = listener.accept().await?;
        let db = db.clone();

        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = [0u8; 4096];

            loop {
                let n = match socket.read(&mut buf).await {
                    Ok(0) => return,
                    Ok(n) => n,
                    Err(_) => return,
                };

                let request = String::from_utf8_lossy(&buf[..n]);
                let request = request.trim();
                if request.is_empty() { continue; }

                let mut parts = request.splitn(3, char::is_whitespace);
                let cmd = parts.next().unwrap_or("");

                let response = match cmd.to_uppercase().as_str() {
                    "GET" => {
                        if let Some(key) = parts.next() {
                            let seq = db.get_seq();
                            match db.find(key, seq) {
                                Ok(Some(val)) => format!("OK: {}\n", val),
                                Ok(None) => "NOT_FOUND\n".to_string(),
                                Err(e) => format!("ERROR: {:?}\n", e),
                            }
                        } else {
                            "ERROR: Missing key\n".to_string()
                        }
                    }
                    "SET" => {
                        if let (Some(key), Some(value)) = (parts.next(), parts.next()) {
                            let mut batch = WriteBatch::new();
                            match batch.set(key, value.to_string()) {
                                Ok(_) => match db.commit_batch(&batch) {
                                    Ok(seq) => format!("OK: seq={}\n", seq),
                                    Err(e) => format!("ERROR: {:?}\n", e),
                                },
                                Err(e) => format!("ERROR: {:?}\n", e),
                            }
                        } else {
                            "ERROR: SET <key> <value>\n".to_string()
                        }
                    }
                    "DELETE" => {
                        if let Some(key) = parts.next() {
                            let mut batch = WriteBatch::new();
                            match batch.delete(key) {
                                Ok(_) => match db.commit_batch(&batch) {
                                    Ok(seq) => format!("DELETED: seq={}\n", seq),
                                    Err(e) => format!("ERROR: {:?}\n", e),
                                },
                                Err(e) => format!("ERROR: {:?}\n", e),
                            }
                        } else {
                            "ERROR: Missing key\n".to_string()
                        }
                    }
                    "SCAN" => {
                        let start = parts.next().unwrap_or("");
                        let end = parts.next().unwrap_or("\x7F");
                        let seq = db.get_seq();
                        match db.scan(start, end, seq) {
                            Ok(results) => {
                                let mut out = format!("{} results:\n", results.len());
                                for (k, v) in results.iter().take(50) {
                                    out.push_str(&format!("  {} = {}\n", k, v));
                                }
                                out
                            }
                            Err(e) => format!("ERROR: {:?}\n", e),
                        }
                    }
                    "QUIT" | "EXIT" => {
                        let _ = socket.write_all(b"Goodbye.\n").await;
                        return;
                    }
                    _ => "ERROR: Unknown command (GET, SET, DELETE, SCAN, QUIT)\n".to_string(),
                };

                if socket.write_all(response.as_bytes()).await.is_err() {
                    return;
                }
            }
        });
    }
}
