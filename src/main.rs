//! Goy Node — Automatic Nostr relay mesh agent over VPN
//!
//! Copyright © 2024-2026 The Goy Company. All rights reserved.
//! Licensed under the Goy Source Available License (see LICENSE).

use std::path::PathBuf;

use directories::ProjectDirs;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, fmt};

mod config;
mod mesh;
mod relay;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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

    let config_path = std::env::var("GOY_NODE_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| project_dirs.config_dir().join("config.toml"));

    let cfg = config::Config::load(&config_path)?;
    info!("✔ Config loaded from {}", config_path.display());
    info!("  Relay URL : {}", cfg.relay.url);
    info!("  Data dir  : {}", project_dirs.data_dir().display());

    // ── Graceful shutdown ──────────────────────────────────────────────
    let cancel = CancellationToken::new(); // ← movido para CIMA
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
