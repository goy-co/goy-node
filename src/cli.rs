//! Admin CLI do Goy Node — comandos de gestão local do nó via HTTP e configuração.
//!
//! Subcomandos suportados:
//! - `goy-node run` → inicia o nó mesh agent (comportamento padrão)
//! - `goy-node status` → lê `/health` e apresenta o estado (OK / Degraded, peers, uptime)
//! - `goy-node peers` → lê `/peers` e lista peers conectados em tabela alinhada
//! - `goy-node info` → lê `/info` e lista metadados do nó
//! - `goy-node metrics` → lê `/metrics` e imprime o dump Prometheus raw
//! - `goy-node onboard` → onboarding interativo/automatizado na VPN e API
//! - `goy-node offboard` → desconectar e desregistar da VPN
//! - `goy-node config show` → exibe a configuração resolvida
//! - `goy-node config validate` → valida a configuração e mostra as origens
//!
//! A flag `--json` (global) formata o output de qualquer comando de consulta como JSON.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use reqwest::Client;
use serde_json::Value;

#[derive(Parser, Debug, Clone, Default)]
#[command(
    name = "goy-node",
    version = env!("CARGO_PKG_VERSION"),
    author = "The Goy Company",
    about = "Mesh agent for Goy Node — automatic Nostr relay synchronization"
)]
pub struct Cli {
    /// Path do ficheiro de configuração TOML.
    /// Default: ~/.config/goy-node/config.toml
    #[arg(short, long, value_name = "PATH", global = true, env = "GOY_CONFIG_PATH")]
    pub config: Option<PathBuf>,

    /// Override do URL do coord-server.
    #[arg(long, global = true, env = "GOY_API_URL")]
    pub coord_url: Option<String>,

    /// Override da admin API key do coord-server.
    #[arg(long, global = true, env = "GOY_ADMIN_API_KEY")]
    pub admin_api_key: Option<String>,

    /// Override do diretório de dados (seen_ids, peer_cursors).
    #[arg(short, long, value_name = "PATH", global = true, env = "GOY_DATA_DIR")]
    pub data_dir: Option<PathBuf>,

    /// Override do nível de log (trace, debug, info, warn, error).
    #[arg(long, global = true, env = "GOY_LOG_LEVEL")]
    pub log_level: Option<String>,

    /// Override do formato de log (pretty, json).
    #[arg(long, global = true, env = "GOY_LOG_FORMAT")]
    pub log_format: Option<String>,

    /// Desativar prompts interativos. Falhar com erro se faltar config obrigatória.
    #[arg(long, global = true)]
    pub no_interactive: bool,

    /// Formatar saída dos comandos de consulta como JSON machine-readable
    #[arg(long, global = true)]
    pub json: bool,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand, Debug, PartialEq, Eq, Clone)]
pub enum Commands {
    /// Inicia o nó mesh agent (comportamento padrão)
    Run(RunArgs),
    /// Exibe o estado e saúde atual do nó (health)
    Status,
    /// Lista os peers atualmente conectados ao nó
    Peers,
    /// Exibe metadados e configuração do nó (versão, fingerprint, etc.)
    Info,
    /// Exibe o dump das métricas Prometheus em formato texto
    Metrics,
    /// Onboarding interativo/automatizado do nó na VPN e plataforma Goy Company
    Onboard(OnboardArgs),
    /// Deregistar o nó da plataforma e desconectar da VPN
    Offboard {
        /// Confirmar remoção sem prompt de confirmação
        #[arg(long)]
        force: bool,
    },
    /// Gerir configuração
    Config(ConfigArgs),
}

/// Argumentos do subcomando run
#[derive(Parser, Debug, PartialEq, Eq, Clone, Default)]
pub struct RunArgs {
    /// Seeds adicionais para bootstrap (pode ser repetido)
    #[arg(long)]
    pub seed: Vec<String>,
}

/// Argumentos do subcomando onboard
#[derive(Parser, Debug, PartialEq, Eq, Clone, Default)]
pub struct OnboardArgs {
    /// Chave de autenticação fornecida pela Goy Company (começa por gc_)
    #[arg(long)]
    pub auth_key: Option<String>,
    /// Execução não-interativa (sem prompts, ideal para automação/CI)
    #[arg(long)]
    pub non_interactive: bool,
    /// Configurar apenas a VPN, sem registar na API Goy Company
    #[arg(long)]
    pub vpn_only: bool,
}

/// Argumentos do subcomando config
#[derive(Parser, Debug, PartialEq, Eq, Clone)]
pub struct ConfigArgs {
    #[command(subcommand)]
    pub action: ConfigAction,
}

#[derive(Subcommand, Debug, PartialEq, Eq, Clone)]
pub enum ConfigAction {
    /// Gerar configuração inicial (interativo ou programático)
    Init(ConfigInitArgs),
    /// Mostrar configuração atual (secrets mascarados)
    Show,
    /// Validar configuração sem arrancar
    Validate,
    /// Alterar um campo específico
    Set(ConfigSetArgs),
    /// Ler um campo específico
    Get(ConfigGetArgs),
    /// Migrar env vars deprecated para config.toml
    Migrate(ConfigMigrateArgs),
}

/// Argumentos do subcomando `config migrate`
#[derive(Parser, Debug, PartialEq, Eq, Clone, Default)]
pub struct ConfigMigrateArgs {
    /// Migrar sem confirmação interativa
    #[arg(long, short = 'y')]
    pub yes: bool,
}

/// Argumentos do subcomando `config init`
#[derive(Parser, Debug, PartialEq, Eq, Clone, Default)]
pub struct ConfigInitArgs {
    /// Coord-server URL
    #[arg(long)]
    pub coord_url: Option<String>,

    /// Admin API key
    #[arg(long)]
    pub admin_api_key: Option<String>,

    /// Data directory
    #[arg(long)]
    pub data_dir: Option<PathBuf>,

    /// Relay URL
    #[arg(long)]
    pub relay_url: Option<String>,

    /// Mesh listen address
    #[arg(long)]
    pub mesh_listen: Option<String>,

    /// Metrics listen address
    #[arg(long)]
    pub metrics_listen: Option<String>,

    /// Log level
    #[arg(long)]
    pub log_level: Option<String>,

    /// Não fazer prompts interativos
    #[arg(long)]
    pub non_interactive: bool,

    /// Sobrescrever config existente sem perguntar
    #[arg(long)]
    pub force: bool,
}

/// Argumentos do subcomando `config set`
#[derive(Parser, Debug, PartialEq, Eq, Clone)]
pub struct ConfigSetArgs {
    /// Campo em notação dotted (ex: coord.url)
    pub key: String,
    /// Novo valor
    pub value: String,
}

/// Argumentos do subcomando `config get`
#[derive(Parser, Debug, PartialEq, Eq, Clone)]
pub struct ConfigGetArgs {
    /// Campo em notação dotted (ex: coord.url)
    pub key: String,
}

impl From<&Cli> for crate::config::resolver::ResolveOptions {
    fn from(cli: &Cli) -> Self {
        Self {
            config_path: cli.config.clone(),
            coord_url: cli.coord_url.clone(),
            admin_api_key: cli.admin_api_key.clone(),
            data_dir: cli.data_dir.clone(),
            log_level: cli.log_level.clone(),
            log_format: cli.log_format.clone(),
            no_interactive: cli.no_interactive,
        }
    }
}

/// Executa o subcomando CLI especificado. Retorna `Ok(true)` se um subcomando de consulta
/// foi tratado e o processo deve terminar, ou `Ok(false)` se o comando for `Run` (iniciar o nó).
pub async fn handle_cli(cli: &Cli, metrics_listen: Option<&str>) -> anyhow::Result<bool> {
    let default_run = Commands::Run(RunArgs::default());
    let command = cli.command.as_ref().unwrap_or(&default_run);
    if matches!(command, Commands::Run(_)) {
        return Ok(false);
    }

    match command {
        Commands::Onboard(args) => {
            let non_interactive = args.non_interactive || cli.no_interactive;
            let code = crate::onboard::run_onboard(
                args.auth_key.clone(),
                non_interactive,
                args.vpn_only,
                cli.config.as_deref(),
                cli.data_dir.as_deref(),
            )
            .await?;
            std::process::exit(code);
        }
        Commands::Offboard { force } => {
            let code = crate::onboard::run_offboard(
                *force,
                cli.config.as_deref(),
                cli.data_dir.as_deref(),
            )
            .await?;
            std::process::exit(code);
        }
        Commands::Config(_) => {
            // Tratado no main
            return Ok(false);
        }
        _ => {}
    }

    let listen_addr = metrics_listen.unwrap_or("127.0.0.1:9090");

    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()?;

    let (path, _query_name) = match command {
        Commands::Status => ("/health", "status"),
        Commands::Peers => ("/peers", "peers"),
        Commands::Info => ("/info", "info"),
        Commands::Metrics => ("/metrics", "metrics"),
        _ => unreachable!(),
    };

    let url = format!("http://{listen_addr}{path}");
    let resp = match client.get(&url).send().await {
        Ok(r) => r,
        Err(_) => {
            eprintln!("❌ Goy Node is not running on {listen_addr}");
            std::process::exit(1);
        }
    };

    let status_code = resp.status();
    let text = resp.text().await?;

    if matches!(command, Commands::Metrics) {
        if cli.json {
            let json_val = serde_json::json!({
                "metrics": text,
            });
            println!("{}", serde_json::to_string_pretty(&json_val)?);
        } else {
            print!("{text}");
        }
        return Ok(true);
    }

    let json_val: Value = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(_) => {
            if cli.json {
                println!(r#"{{"raw":"{text}"}}"#);
            } else {
                println!("{text}");
            }
            return Ok(true);
        }
    };

    if cli.json {
        println!("{}", serde_json::to_string_pretty(&json_val)?);
        return Ok(true);
    }

    match command {
        Commands::Status => format_status(&json_val, status_code.as_u16()),
        Commands::Peers => format_peers(&json_val),
        Commands::Info => format_info(&json_val),
        _ => unreachable!(),
    }

    Ok(true)
}

fn format_status(json: &Value, http_code: u16) {
    let status_str = json
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let peers = json.get("peers").and_then(|v| v.as_u64()).unwrap_or(0);
    let uptime = json.get("uptime").and_then(|v| v.as_u64()).unwrap_or(0);

    let symbol = if http_code == 200 { "🟢" } else { "🟡" };
    println!("{symbol} Goy Node Status: {}", status_str.to_uppercase());
    println!("Peers connected : {peers}");
    println!("Uptime          : {} ({uptime}s)", format_duration(uptime));
}

fn format_peers(json: &Value) {
    let Some(arr) = json.as_array() else {
        println!("No peer data returned.");
        return;
    };

    if arr.is_empty() {
        println!("No peers currently connected.");
        return;
    }

    println!(
        "{:<30} {:<10} {:<22} {:<12} {:<12}",
        "PEER ID", "DIRECTION", "ADDRESS", "EVENTS SENT", "EVENTS RECV"
    );
    println!("{}", "-".repeat(88));

    for item in arr {
        let peer_id = item.get("peer_id").and_then(|v| v.as_str()).unwrap_or("-");
        let direction = item
            .get("direction")
            .and_then(|v| v.as_str())
            .unwrap_or("-");
        let address = item.get("address").and_then(|v| v.as_str()).unwrap_or("-");
        let sent = item
            .get("events_sent")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let recv = item
            .get("events_received")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        println!(
            "{:<30} {:<10} {:<22} {:<12} {:<12}",
            peer_id, direction, address, sent, recv
        );
    }
}

fn format_info(json: &Value) {
    let version = json.get("version").and_then(|v| v.as_str()).unwrap_or("-");
    let node_id = json.get("node_id").and_then(|v| v.as_str()).unwrap_or("-");
    let fp = json
        .get("cert_fingerprint")
        .and_then(|v| v.as_str())
        .unwrap_or("none");
    let relay_url = json
        .get("relay_url")
        .and_then(|v| v.as_str())
        .unwrap_or("-");
    let mesh_listen = json
        .get("mesh_listen")
        .and_then(|v| v.as_str())
        .unwrap_or("-");
    let rf = json
        .get("replication_factor")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let tls = json
        .get("tls_enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    println!("Node Version      : {version}");
    println!("Node ID           : {node_id}");
    println!("TLS Fingerprint   : {fp}");
    println!("Relay URL         : {relay_url}");
    println!("Mesh Listen       : {mesh_listen}");
    println!("Replication Factor: {rf}");
    println!("TLS Enabled       : {tls}");
}

fn format_duration(secs: u64) -> String {
    let days = secs / 86400;
    let hours = (secs % 86400) / 3600;
    let mins = (secs % 3600) / 60;
    let s = secs % 60;

    if days > 0 {
        format!("{days}d {hours}h {mins}m")
    } else if hours > 0 {
        format!("{hours}h {mins}m {s}s")
    } else if mins > 0 {
        format!("{mins}m {s}s")
    } else {
        format!("{s}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_parse_subcommands_and_json_flag() {
        let args = vec!["goy-node", "status", "--json"];
        let cli = Cli::parse_from(args);
        assert_eq!(cli.command, Some(Commands::Status));
        assert!(cli.json);

        let args_peers = vec!["goy-node", "peers"];
        let cli_peers = Cli::parse_from(args_peers);
        assert_eq!(cli_peers.command, Some(Commands::Peers));
        assert!(!cli_peers.json);

        let args_run = vec!["goy-node", "run"];
        let cli_run = Cli::parse_from(args_run);
        assert_eq!(cli_run.command, Some(Commands::Run(RunArgs::default())));

        let args_default = vec!["goy-node"];
        let cli_default = Cli::parse_from(args_default);
        assert_eq!(cli_default.command, None);
    }

    #[test]
    fn test_parse_onboard_minimal() {
        let cli = Cli::parse_from(["goy-node", "onboard", "--auth-key", "gc_test_1234567890"]);
        assert!(matches!(cli.command, Some(Commands::Onboard(_))));
        assert!(cli.coord_url.is_none());
        assert!(!cli.no_interactive);
    }

    #[test]
    fn test_parse_global_flags_with_subcommand() {
        let cli = Cli::parse_from([
            "goy-node",
            "--coord-url",
            "http://10.0.0.5:8080",
            "--admin-api-key",
            "secret",
            "--no-interactive",
            "--log-level",
            "debug",
            "onboard",
            "--auth-key",
            "gc_test_1234567890",
        ]);
        assert_eq!(cli.coord_url.as_deref(), Some("http://10.0.0.5:8080"));
        assert_eq!(cli.admin_api_key.as_deref(), Some("secret"));
        assert!(cli.no_interactive);
        assert_eq!(cli.log_level.as_deref(), Some("debug"));
    }

    #[test]
    fn test_parse_run_with_seeds() {
        let cli = Cli::parse_from([
            "goy-node",
            "run",
            "--seed",
            "ws://peer1:8443",
            "--seed",
            "ws://peer2:8443",
        ]);
        if let Some(Commands::Run(args)) = &cli.command {
            assert_eq!(args.seed.len(), 2);
            assert_eq!(args.seed[0], "ws://peer1:8443");
            assert_eq!(args.seed[1], "ws://peer2:8443");
        } else {
            panic!("expected Run command");
        }
    }

    #[test]
    fn test_parse_config_show_and_validate() {
        let cli_show = Cli::parse_from(["goy-node", "config", "show"]);
        assert_eq!(
            cli_show.command,
            Some(Commands::Config(ConfigArgs {
                action: ConfigAction::Show
            }))
        );

        let cli_val = Cli::parse_from(["goy-node", "config", "validate"]);
        assert_eq!(
            cli_val.command,
            Some(Commands::Config(ConfigArgs {
                action: ConfigAction::Validate
            }))
        );
    }

    #[test]
    fn test_resolve_options_from_cli() {
        let cli = Cli::parse_from([
            "goy-node",
            "--coord-url",
            "http://x:8080",
            "--data-dir",
            "/custom/data",
            "--no-interactive",
            "run",
        ]);
        let opts = crate::config::resolver::ResolveOptions::from(&cli);
        assert_eq!(opts.coord_url.as_deref(), Some("http://x:8080"));
        assert_eq!(opts.data_dir, Some(PathBuf::from("/custom/data")));
        assert!(opts.no_interactive);
    }

    #[test]
    fn test_parse_config_init_set_get() {
        let cli_init = Cli::parse_from([
            "goy-node",
            "config",
            "init",
            "--coord-url",
            "http://10.0.0.5:8080",
            "--admin-api-key",
            "secret_key",
            "--non-interactive",
            "--force",
        ]);
        if let Some(Commands::Config(ConfigArgs {
            action: ConfigAction::Init(args),
        })) = cli_init.command
        {
            assert_eq!(args.coord_url.as_deref(), Some("http://10.0.0.5:8080"));
            assert_eq!(args.admin_api_key.as_deref(), Some("secret_key"));
            assert!(args.non_interactive);
            assert!(args.force);
        } else {
            panic!("expected ConfigAction::Init");
        }

        let cli_set = Cli::parse_from(["goy-node", "config", "set", "coord.url", "http://new:8080"]);
        assert_eq!(
            cli_set.command,
            Some(Commands::Config(ConfigArgs {
                action: ConfigAction::Set(ConfigSetArgs {
                    key: "coord.url".to_string(),
                    value: "http://new:8080".to_string(),
                })
            }))
        );

        let cli_get = Cli::parse_from(["goy-node", "config", "get", "coord.url"]);
        assert_eq!(
            cli_get.command,
            Some(Commands::Config(ConfigArgs {
                action: ConfigAction::Get(ConfigGetArgs {
                    key: "coord.url".to_string(),
                })
            }))
        );
    }
}

