//! Goy Node — Automatic Nostr relay mesh agent over VPN
//!
//! Copyright © 2024-2026 The Goy Company. All rights reserved.
//! Licensed under the Goy Source Available License (see LICENSE).

use std::path::PathBuf;

use clap::Parser;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, fmt};

mod cli;
mod config;
mod consistent_hash;
mod event_types;
mod goy_api;
mod heartbeat;
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

    // ── Subcomandos `config init`, `config set`, `config get` (executam antes da resolução geral) ──
    if let Some(cli::Commands::Config(ref args)) = cli.command {
        match &args.action {
            cli::ConfigAction::Init(init_args) => {
                let cmd_args = config::commands::InitArgs {
                    coord_url: init_args.coord_url.clone(),
                    admin_api_key: init_args.admin_api_key.clone(),
                    data_dir: init_args.data_dir.clone(),
                    relay_url: init_args.relay_url.clone(),
                    mesh_listen: init_args.mesh_listen.clone(),
                    metrics_listen: init_args.metrics_listen.clone(),
                    log_level: init_args.log_level.clone(),
                    non_interactive: init_args.non_interactive || cli.no_interactive,
                    force: init_args.force,
                };
                config::commands::cmd_init(&cmd_args, cli.config.as_deref())?;
                return Ok(());
            }
            cli::ConfigAction::Set(set_args) => {
                let cmd_args = config::commands::SetArgs {
                    key: set_args.key.clone(),
                    value: set_args.value.clone(),
                };
                config::commands::cmd_set(&cmd_args, cli.config.as_deref())?;
                return Ok(());
            }
            cli::ConfigAction::Get(get_args) => {
                let cmd_args = config::commands::GetArgs {
                    key: get_args.key.clone(),
                };
                config::commands::cmd_get(&cmd_args, cli.config.as_deref())?;
                return Ok(());
            }
            _ => {}
        }
    }

    // ── Resolver configuração ANTES de inicializar logging ─────────────
    let resolve_opts = config::resolver::ResolveOptions::from(&cli);
    let resolved = match config::resolver::resolve(&resolve_opts) {
        Ok(r) => r,
        Err(e) => {
            // Erro de config antes do tracing estar pronto → stderr
            eprintln!("❌ Configuration error: {e}");
            std::process::exit(1);
        }
    };

    // ── Imprimir warnings de resolução ─────────────────────────────────
    for w in &resolved.warnings {
        eprintln!("{w}");
    }

    // ── Subcomando `config` (show / validate) ───────────────────────────
    if let Some(cli::Commands::Config(args)) = &cli.command {
        match &args.action {
            cli::ConfigAction::Show => {
                let mut masked_config = resolved.config.clone();
                masked_config.coord.admin_api_key =
                    config::resolver::mask_secret(&masked_config.coord.admin_api_key);
                println!("{}", toml::to_string_pretty(&masked_config)?);
                return Ok(());
            }
            cli::ConfigAction::Validate => {
                println!("✅ Configuration is valid.");
                println!("Sources:");
                let mut sorted_sources: Vec<_> = resolved.sources.iter().collect();
                sorted_sources.sort_by_key(|(k, _)| *k);
                for (field, source) in sorted_sources {
                    println!("  {field} ← {source}");
                }
                return Ok(());
            }
            _ => {}
        }
    }

    // ── Logging estruturado (silenciar logs normais para subcomandos de consulta se não for Run) ──
    let is_query_cmd = cli
        .command
        .as_ref()
        .is_some_and(|c| !matches!(c, cli::Commands::Run(_)));
    init_tracing(&resolved.config.log, is_query_cmd)?;

    // ── Converter para Config de execução ──────────────────────────────
    let mut cfg = config::Config::from(resolved.config);

    // Merge de seeds passadas via CLI (se subcomando for Run)
    if let Some(cli::Commands::Run(run_args)) = &cli.command {
        cfg.mesh.seeds.extend(run_args.seed.iter().cloned());
    }

    let config_path = cli
        .config
        .clone()
        .unwrap_or_else(config::default_config_path);
    let data_dir = cfg.storage.data_dir.clone();

    // Tratar subcomandos de consulta (status, peers, info, metrics, onboard, offboard)
    if cli::handle_cli(&cli, cfg.metrics.listen.as_deref()).await? {
        return Ok(());
    }

    // Subcomando `run` (ou default): iniciar o nó mesh agent
    cmd_run(cfg, config_path, data_dir).await
}

fn init_tracing(log_config: &config::schema::LogConfig, is_query_cmd: bool) -> anyhow::Result<()> {
    let default_filter = if is_query_cmd {
        "warn"
    } else {
        &log_config.level
    };
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));

    match log_config.format.as_str() {
        "json" => {
            fmt().json().with_env_filter(filter).init();
        }
        _ => {
            fmt().pretty().with_env_filter(filter).init();
        }
    }

    Ok(())
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
    let mesh_handle = tokio::spawn(mesh::run_with_http_listen_and_storage_with_heartbeat(
        cfg.mesh,
        cfg.metrics.listen,
        cfg.relay.url.clone(),
        Some(data_dir),
        Some(storage_info),
        Some(cfg.heartbeat),
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
