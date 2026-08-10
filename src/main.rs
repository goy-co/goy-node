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
mod consistent_hash;
mod event_types;
mod goy_api;
mod http;
mod mesh;
mod metrics;
mod onboard;
mod rate_limiter;
mod registry;
mod relay;
mod storage;
mod tls;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse de argumentos CLI (trata --help e --version automaticamente)
    let cli = cli::Cli::parse();

    // ── Logging estruturado (silenciar logs normais para subcomandos de consulta se não for Run) ──
    let is_query_cmd = cli
        .command
        .as_ref()
        .is_some_and(|c| c != &cli::Commands::Run);
    let default_filter = if is_query_cmd { "warn" } else { "info" };

    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter)),
        )
        .init();

    // ── Configuração ───────────────────────────────────────────────────
    let project_dirs = ProjectDirs::from("com", "goy-co", "goy-node")
        .expect("failed to determine project directories");

    let config_path = cli
        .config
        .clone()
        .or_else(|| std::env::var("GOY_NODE_CONFIG").map(PathBuf::from).ok())
        .unwrap_or_else(|| project_dirs.config_dir().join("config.toml"));

    let mut cfg = match config::Config::load_or_generate(&config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ Configuration error: {e}");
            std::process::exit(1);
        }
    };

    if let Some(ref cli_data_dir) = cli.data_dir {
        info!(
            "🔧 Override from CLI --data-dir: {}",
            cli_data_dir.display()
        );
        cfg.storage.data_dir = cli_data_dir.clone();
    }
    let data_dir = cfg.storage.data_dir.clone();

    // Tratar subcomandos de consulta (status, peers, info, metrics, onboard, offboard)
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

    // ── 1. Verificação de Storage Reservado (Fail-Fast no Startup) ─────
    let storage_info = match storage::verify_storage(&cfg.storage) {
        Ok(info) => {
            info!("💾 Storage verificado com sucesso");
            info!(
                "   Reservado: {} GB ({} GB mínimo + {} GB extra)",
                info.total_reserved_gb,
                storage::MIN_RESERVED_GB,
                cfg.storage.extra_contribution_gb
            );
            info!(
                "   Disponível: {} GB | Usado: {} GB",
                info.available_gb, info.used_gb
            );
            info!("   Filesystem: {}", info.filesystem_path.display());
            info
        }
        Err(err) => {
            match &err {
                storage::StorageError::InsufficientSpace {
                    available_gb,
                    required_gb,
                } => {
                    error!("❌ Espaço insuficiente para operação do Goy Node");
                    error!(
                        "   Disponível: {} GB | Requerido: {} GB (mínimo obrigatório)",
                        available_gb, required_gb
                    );
                    error!("   Data dir: {}", cfg.storage.data_dir.display());
                    error!("");
                    error!("   O Goy Node requer pelo menos 50 GB de espaço reservado");
                    error!("   para garantir redundância de dados na rede Goy.");
                    error!("");
                    error!("   Ações possíveis:");
                    error!("   • Libertar espaço em disco");
                    error!("   • Escolher outro data_dir com espaço suficiente (--data-dir)");
                    error!("   • Montar volume adicional no data_dir atual");
                }
                storage::StorageError::PermissionDenied(path) => {
                    error!("❌ Permissão negada no diretório de dados");
                    error!("   Data dir: {}", path.display());
                    error!("   Verifique as permissões de leitura e escrita do utilizador.");
                }
                storage::StorageError::DataDirNotFound(path) => {
                    error!("❌ Não foi possível criar o diretório de dados");
                    error!("   Data dir: {}", path.display());
                }
                storage::StorageError::FilesystemError(msg) => {
                    error!("❌ Erro no sistema de ficheiros: {msg}");
                }
            }
            std::process::exit(storage::EXIT_STORAGE_ERROR);
        }
    };

    // ── 2. Verificação de Onboarding (Graceful Degradation) ───────────
    if onboard::check_onboard_status(Some(&data_dir)).is_none() {
        tracing::warn!(
            "⚠️ Node not onboarded. Run 'goy-node onboard' first to join Goy VPN platform."
        );
    } else {
        info!("✅ Node is onboarded on Goy VPN platform");
    }

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
    let mesh_handle = tokio::spawn(mesh::run_with_http_listen_and_storage(
        cfg.mesh,
        cfg.metrics.listen,
        cfg.relay.url.clone(),
        Some(data_dir),
        Some(storage_info),
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
