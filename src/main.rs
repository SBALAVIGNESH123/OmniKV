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
mod cluster;
mod quic_server;

use omni_engine::{config::ServerConfig, OmniKV};
use std::sync::Arc;

fn print_banner(cfg: &ServerConfig) {
    println!();
    println!("  ╔════════════════════════════════════════════════════╗");
    println!(
        "  ║        ⚡ OmniKV v{}                       ║",
        env!("CARGO_PKG_VERSION")
    );
    println!("  ║  Embeddable · Distributed · Transactional KV      ║");
    println!("  ╠════════════════════════════════════════════════════╣");
    println!("  ║  HTTP/1.1 + HTTP/2 (TLS)  → {}           ║", cfg.http_addr);
    println!("  ║  QUIC/HTTP3 (binary)      → {}           ║", cfg.quic_addr);
    println!(println!("  ║  TCP Command Interface    → {}           ║", cfg.tcp_addr);, cfg.pgwire_addr);
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

    // Load configuration (development mode; switch to ServerConfig::load_production()
    // before any production deployment).
    let cfg = ServerConfig::load_dev();
    print_banner(&cfg);
    tracing::info!(
        mode = ?cfg.mode,
        http_addr = %cfg.http_addr,
        manifest = %cfg.storage.manifest_path,
        "Configuration loaded"
    );

    // Clone paths before opening so they remain available for AppState.
    let manifest_path = cfg.storage.manifest_path.clone();
    let wal_path = cfg.storage.wal_path.clone();

    // Open the database using configured paths.
    let db = OmniKV::open(&manifest_path, &wal_path)?;
    tracing::info!(
        seq = db.get_seq(),
        sstables = db.sstable_count(),
        "Database opened"
    );

    let jwt_secret = cfg.jwt_secret.clone();

    let app_state = api::AppState {
        db: db.clone(),
        jwt_secret,
        manifest_path,
        wal_path,
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

    // Clone addr strings before async move closures consume cfg.
    let http_addr_str = cfg.http_addr.clone();
    let quic_addr_str = cfg.quic_addr.clone();
    let pgwire_addr_str = cfg.pgwire_addr.clone();
    let tcp_addr_str = cfg.tcp_addr.clone();
    // Use http_addr_str (not cfg.http_addr) to avoid redundant clone warning.
    let http_addr: std::net::SocketAddr = http_addr_str.parse()?;

    let http_handle = tokio::spawn(async move {
        tracing::info!("HTTP/1.1 + HTTP/2 server starting on {http_addr_str}");
        if let Err(e) = axum_server::bind_rustls(http_addr, tls_config)
            .serve(router.into_make_service())
            .await
        {
            tracing::error!("HTTP server error: {e}");
        }
    });

    // ─── 2. QUIC/HTTP3 Binary Protocol ─────────────────────────
    let (quic_certs, quic_key) = quic_server::generate_self_signed_cert()?;
    let quic_endpoint =
        quic_server::create_server_endpoint(quic_addr_str, quic_certs, quic_key)?;
    let quic_db = db.clone();
    let quic_handle = tokio::spawn(async move {
        quic_server::run_quic_server(quic_endpoint, quic_db).await;
    });

    // ─── 3. PostgreSQL Wire Protocol ───────────────────────────
    let pgwire_db = db.clone();
    let _pgwire_handle = std::thread::spawn(move || {
        // Log before moving pgwire_addr_str into PgWireServer::new.
        tracing::info!("PostgreSQL wire protocol starting on {pgwire_addr_str}");
        let server = omni_engine::pgwire::PgWireServer::new(pgwire_db, pgwire_addr_str);
        if let Err(e) = server.start() {
            tracing::error!("PgWire server error: {e}");
        }
    });

    // ─── 4. TCP Command Interface (for telnet/debug) ──────────
    let tcp_db = db.clone();
    let tcp_handle = tokio::spawn(async move {
        if let Err(e) = run_tcp_server(tcp_db, &tcp_addr_str).await {
            tracing::error!("TCP server error: {e}");
        }
    });

    tracing::info!("All servers started. OmniKV is ready.");

    // Wait for any server to exit (they should not)
    tokio::select! {
        _ = http_handle => tracing::error!("HTTP server exited"),
        _ = quic_handle => tracing::error!("QUIC server exited"),
        _ = tcp_handle => tracing::error!("TCP server exited"),
    }

    Ok(())
}

/// Simple TCP command interface for telnet/debugging.
async fn run_tcp_server(
    db: Arc<OmniKV>,
    addr: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use omni_engine::WriteBatch;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("TCP command interface on {addr}");

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
                                Ok(Some(val)) => format!("OK: {val}\n"),
                                Ok(None) => "NOT_FOUND\n".to_string(),
                                Err(e) => format!("ERROR: {e:?}\n"),
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
                                    Ok(seq) => format!("OK: seq={seq}\n"),
                                    Err(e) => format!("ERROR: {e:?}\n"),
                                },
                                Err(e) => format!("ERROR: {e:?}\n"),
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
                                    Ok(seq) => format!("DELETED: seq={seq}\n"),
                                    Err(e) => format!("ERROR: {e:?}\n"),
                                },
                                Err(e) => format!("ERROR: {e:?}\n"),
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
                                    out.push_str(&format!("  {k} = {v}\n"));
                                }
                                out
                            }
                            Err(e) => format!("ERROR: {e:?}\n"),
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
