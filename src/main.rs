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

mod cli;
mod config;
mod event_types;
mod http;
mod mesh;
mod metrics;
mod rate_limiter;
mod registry;
mod relay;
mod tls;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse de argumentos CLI (trata --help e --version automaticamente)
    let cli = cli::Cli::parse();

    // ── Logging estruturado (silenciar logs normais para subcomandos de consulta se não for Run) ──
    let is_query_cmd = cli.command.as_ref().map_or(false, |c| c != &cli::Commands::Run);
    let default_filter = if is_query_cmd { "warn" } else { "info" };

    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter)),
        )
        .init();

    // ── Configuração ───────────────────────────────────────────────────
    let project_dirs = ProjectDirs::from("com", "the-goy-company", "goy-node")
        .expect("failed to determine project directories");

    let config_path = cli
        .config
        .clone()
        .or_else(|| std::env::var("GOY_NODE_CONFIG").map(PathBuf::from).ok())
        .unwrap_or_else(|| project_dirs.config_dir().join("config.toml"));

    let data_dir = cli
        .data_dir
        .clone()
        .or_else(|| std::env::var("GOY_NODE_DATA_DIR").map(PathBuf::from).ok())
        .unwrap_or_else(|| project_dirs.data_dir().to_path_buf());

    let cfg = match config::Config::load_or_generate(&config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ Configuration error: {e}");
            std::process::exit(1);
        }
    };

    // Tratar subcomandos de consulta (status, peers, info, metrics)
    if cli::handle_cli(&cli, cfg.metrics.listen.as_deref()).await? {
        return Ok(());
    }

    // Subcomando `run` (ou default): iniciar o nó mesh agent
    cmd_run(cfg, config_path, data_dir).await
}

async fn cmd_run(
    cfg: config::Config,
    config_path: PathBuf,
    data_dir: PathBuf,
) -> anyhow::Result<()> {
    info!("🟢 Goy Node v{} starting", env!("CARGO_PKG_VERSION"));
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
    let mesh_handle = tokio::spawn(mesh::run_with_http_listen(
        cfg.mesh,
        cfg.metrics.listen,
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
