//! OmniKV — Embeddable + Distributed KV Engine
//!
//! Production binary that starts:
//! 1. HTTP/1.1 + HTTP/2 REST API (Axum + axum-server with ALPN TLS)
//! 2. QUIC/HTTP3 binary protocol (Quinn)
//! 3. PostgreSQL wire protocol v3 (PgWire)
//! 4. Prometheus metrics on /metrics

#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]

mod api;
mod auth;
mod backup;
mod cluster;
mod crypto;
mod quic_server;

use omni_engine::OmniKV;
use std::sync::Arc;

const MANIFEST_PATH: &str = "manifest.json";
const WAL_PATH: &str = "wal.bin";
const HTTP_ADDR: &str = "0.0.0.0:8443";
const QUIC_ADDR: &str = "0.0.0.0:4433";
const PGWIRE_ADDR: &str = "0.0.0.0:5433";
const TCP_ADDR: &str = "0.0.0.0:8080";

fn print_banner() {
    println!();
    println!("  ╔════════════════════════════════════════════════════╗");
    println!(
        "  ║        ⚡ OmniKV v{}                       ║",
        env!("CARGO_PKG_VERSION")
    );
    println!("  ║  Embeddable · Distributed · Transactional KV      ║");
    println!("  ╠════════════════════════════════════════════════════╣");
    println!("  ║  HTTP/1.1 + HTTP/2 (TLS)  → {}           ║", HTTP_ADDR);
    println!("  ║  QUIC/HTTP3 (binary)      → {}           ║", QUIC_ADDR);
    println!(
        "  ║  PostgreSQL Wire Protocol → {}           ║",
        PGWIRE_ADDR
    );
    println!("  ║  TCP Command Interface    → {}           ║", TCP_ADDR);
    println!("  ╠════════════════════════════════════════════════════╣");
    println!("  ║  Built from scratch in Rust. Every byte is ours.  ║");
    println!("  ╚════════════════════════════════════════════════════╝");
    println!();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize structured logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,omni_engine=debug".parse().unwrap()),
        )
        .json()
        .init();

    print_banner();

    // Open the database
    let db = OmniKV::open(MANIFEST_PATH, WAL_PATH)?;
    tracing::info!(
        seq = db.get_seq(),
        sstables = db.sstable_count(),
        "Database opened"
    );

    // ─── 1. HTTP/1.1 + HTTP/2 REST API (TLS with ALPN) ────────
    let app_state = api::AppState {
        db: db.clone(),
        jwt_secret: std::env::var("OMNI_JWT_SECRET")
            .unwrap_or_else(|_| "omnikv-dev-secret-change-in-prod".to_string()),
        manifest_path: MANIFEST_PATH.to_string(),
    };

    let router = api::build_router(app_state);

    // Generate self-signed certs for HTTP/2 + QUIC
    let (certs, key) = quic_server::generate_self_signed_cert()?;

    // HTTP/2 server with TLS (ALPN h2 + http/1.1)
    let tls_config = axum_server::tls_rustls::RustlsConfig::from_der(
        certs.iter().map(|c| c.as_ref().to_vec()).collect(),
        key.secret_der().to_vec(),
    )
    .await?;

    let http_addr: std::net::SocketAddr = HTTP_ADDR.parse()?;
    let http_handle = tokio::spawn(async move {
        tracing::info!("HTTP/1.1 + HTTP/2 server starting on {}", HTTP_ADDR);
        if let Err(e) = axum_server::bind_rustls(http_addr, tls_config)
            .serve(router.into_make_service())
            .await
        {
            tracing::error!("HTTP server error: {}", e);
        }
    });

    // ─── 2. QUIC/HTTP3 Binary Protocol ─────────────────────────
    let (quic_certs, quic_key) = quic_server::generate_self_signed_cert()?;
    let quic_endpoint = quic_server::create_server_endpoint(QUIC_ADDR, quic_certs, quic_key)?;
    let quic_db = db.clone();
    let quic_handle = tokio::spawn(async move {
        quic_server::run_quic_server(quic_endpoint, quic_db).await;
    });

    // ─── 3. PostgreSQL Wire Protocol ───────────────────────────
    let pgwire_db = db.clone();
    let pgwire_handle = std::thread::spawn(move || {
        let server = omni_engine::pgwire::PgWireServer::new(pgwire_db, PGWIRE_ADDR);
        tracing::info!("PostgreSQL wire protocol starting on {}", PGWIRE_ADDR);
        if let Err(e) = server.start() {
            tracing::error!("PgWire server error: {}", e);
        }
    });

    // ─── 4. TCP Command Interface (for telnet/debug) ──────────
    let tcp_db = db.clone();
    let tcp_handle = tokio::spawn(async move {
        if let Err(e) = run_tcp_server(tcp_db, TCP_ADDR).await {
            tracing::error!("TCP server error: {}", e);
        }
    });

    tracing::info!("All servers started. OmniKV is ready.");

    // Wait for any server to exit (they shouldn't)
    tokio::select! {
        _ = http_handle => tracing::error!("HTTP server exited"),
        _ = quic_handle => tracing::error!("QUIC server exited"),
        _ = tcp_handle => tracing::error!("TCP server exited"),
    }

    Ok(())
}

/// Simple TCP command interface for telnet/debugging.
async fn run_tcp_server(db: Arc<OmniKV>, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    use omni_engine::WriteBatch;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("TCP command interface on {}", addr);

    loop {
        let (mut socket, _addr) = listener.accept().await?;
        let db = db.clone();

        tokio::spawn(async move {
            let mut buf = [0u8; 4096];

            loop {
                let n = match socket.read(&mut buf).await {
                    Ok(0) => return,
                    Ok(n) => n,
                    Err(_) => return,
                };

                let request = String::from_utf8_lossy(&buf[..n]);
                let request = request.trim();
                if request.is_empty() {
                    continue;
                }

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
