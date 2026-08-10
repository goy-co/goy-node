//! Goy Node — Automatic Nostr relay mesh agent over VPN
//!
//! Copyright © 2024-2026 The Goy Company. All rights reserved.
//! Licensed under the Goy Source Available License (see LICENSE).

use std::path::PathBuf;

use clap::Parser;
use directories::ProjectDirs;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, fmt};

mod config;
mod event_types;
mod mesh;
mod rate_limiter;
mod registry;
mod relay;
mod tls;

/// Opções CLI para o Goy Node
#[derive(Parser, Debug)]
#[command(
    name = "goy-node",
    version = env!("CARGO_PKG_VERSION"),
    author = "The Goy Company",
    about = "Mesh agent for Goy Node — automatic Nostr relay synchronization"
)]
struct Cli {
    /// Caminho alternativo para o ficheiro de configuração config.toml
    #[arg(short, long, value_name = "PATH")]
    config: Option<PathBuf>,

    /// Caminho alternativo para o diretório de dados (seen_ids, peer_cursors)
    #[arg(short, long, value_name = "PATH")]
    data_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse de argumentos CLI (trata --help e --version automaticamente)
    let cli = Cli::parse();

    // ── Logging estruturado ────────────────────────────────────────────
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    info!("🟢 Goy Node v{} starting", env!("CARGO_PKG_VERSION"));

    // ── Configuração ───────────────────────────────────────────────────
    let project_dirs = ProjectDirs::from("com", "the-goy-company", "goy-node")
        .expect("failed to determine project directories");

    let config_path = cli
        .config
        .or_else(|| std::env::var("GOY_NODE_CONFIG").map(PathBuf::from).ok())
        .unwrap_or_else(|| project_dirs.config_dir().join("config.toml"));

    let data_dir = cli
        .data_dir
        .or_else(|| std::env::var("GOY_NODE_DATA_DIR").map(PathBuf::from).ok())
        .unwrap_or_else(|| project_dirs.data_dir().to_path_buf());

    let cfg = match config::Config::load_or_generate(&config_path) {
        Ok(c) => c,
        Err(e) => {
            error!("❌ Configuration error: {e}");
            std::process::exit(1);
        }
    };

    info!("✔ Config loaded from {}", config_path.display());
    info!("  Relay URL  : {}", cfg.relay.url);
    info!("  Listen addr: {}", cfg.mesh.listen);
    info!("  Data dir   : {}", data_dir.display());

    // ── Graceful shutdown ──────────────────────────────────────────────
    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();

    tokio::spawn(async move {
        match signal::ctrl_c().await {
            Ok(()) => {
                info!("Received SIGINT, shutting down gracefully…");
                cancel_clone.cancel();
            }
            Err(e) => error!("Failed to listen for ctrl-c: {e}"),
        }
    });

    // ── Relay Connection ───────────────────────────────────────────────
    let relay_events = relay::connect(cfg.relay.clone(), cancel.clone()).await?;
    let relay_publisher = relay::create_publisher(&cfg.relay, cancel.clone());
    info!("✔ Relay connection started");

    // ── Mesh Agent ─────────────────────────────────────────────────────
    let mesh_handle = tokio::spawn(mesh::run(
        cfg.mesh,
        cfg.relay.url.clone(),
        Some(data_dir),
        relay_events,
        relay_publisher,
        cancel.clone(),
    ));

    info!("🟢 Goy Node ready (press Ctrl+C to stop)");

    tokio::select! {
        result = mesh_handle => {
            if let Err(e) = result {
                error!("❌ Mesh agent failed: {e}");
            }
        }
        _ = cancel.cancelled() => {
            info!("Shutdown signal received");
        }
    }

    info!("👋 Goy Node stopped");
    Ok(())
}
