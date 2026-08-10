//! Admin CLI do Goy Node — comandos de gestão local do nó via HTTP.
//!
//! Subcomandos suportados:
//! - `goy-node run` → inicia o nó mesh agent (comportamento padrão)
//! - `goy-node status` → lê `/health` e apresenta o estado (OK / Degraded, peers, uptime)
//! - `goy-node peers` → lê `/peers` e lista peers conectados em tabela alinhada
//! - `goy-node info` → lê `/info` e lista metadados do nó
//! - `goy-node metrics` → lê `/metrics` e imprime o dump Prometheus raw
//!
//! A flag `--json` (global) formata o output de qualquer comando de consulta como JSON.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use reqwest::Client;
use serde_json::Value;

#[derive(Parser, Debug)]
#[command(
    name = "goy-node",
    version = env!("CARGO_PKG_VERSION"),
    author = "The Goy Company",
    about = "Mesh agent for Goy Node — automatic Nostr relay synchronization"
)]
pub struct Cli {
    /// Caminho alternativo para o ficheiro de configuração config.toml
    #[arg(short, long, value_name = "PATH", global = true)]
    pub config: Option<PathBuf>,

    /// Caminho alternativo para o diretório de dados (seen_ids, peer_cursors)
    #[arg(short, long, value_name = "PATH", global = true)]
    pub data_dir: Option<PathBuf>,

    /// Formatar saída dos comandos de consulta como JSON machine-readable
    #[arg(long, global = true)]
    pub json: bool,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand, Debug, PartialEq, Eq)]
pub enum Commands {
    /// Inicia o nó mesh agent (comportamento padrão)
    Run,
    /// Exibe o estado e saúde atual do nó (health)
    Status,
    /// Lista os peers atualmente conectados ao nó
    Peers,
    /// Exibe metadados e configuração do nó (versão, fingerprint, etc.)
    Info,
    /// Exibe o dump das métricas Prometheus em formato texto
    Metrics,
    /// Onboarding interativo/automatizado do nó na VPN e plataforma Goy Company
    Onboard {
        /// Chave de autenticação fornecida pela Goy Company (começa por gc_)
        #[arg(long)]
        auth_key: Option<String>,
        /// Execução não-interativa (sem prompts, ideal para automação/CI)
        #[arg(long)]
        non_interactive: bool,
        /// Configurar apenas a VPN, sem registar na API Goy Company
        #[arg(long)]
        vpn_only: bool,
    },
    /// Deregistar o nó da plataforma e desconectar da VPN
    Offboard {
        /// Confirmar remoção sem prompt de confirmação
        #[arg(long)]
        force: bool,
    },
}

/// Executa o subcomando CLI especificado. Retorna `Ok(true)` se um subcomando de consulta
/// foi tratado e o processo deve terminar, ou `Ok(false)` se o comando for `Run` (iniciar o nó).
pub async fn handle_cli(cli: &Cli, metrics_listen: Option<&str>) -> anyhow::Result<bool> {
    let command = cli.command.as_ref().unwrap_or(&Commands::Run);
    if command == &Commands::Run {
        return Ok(false);
    }

    match command {
        Commands::Onboard {
            auth_key,
            non_interactive,
            vpn_only,
        } => {
            let code = crate::onboard::run_onboard(
                auth_key.clone(),
                *non_interactive,
                *vpn_only,
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

    if command == &Commands::Metrics {
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
    let status_str = json.get("status").and_then(|v| v.as_str()).unwrap_or("unknown");
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
        let direction = item.get("direction").and_then(|v| v.as_str()).unwrap_or("-");
        let address = item.get("address").and_then(|v| v.as_str()).unwrap_or("-");
        let sent = item.get("events_sent").and_then(|v| v.as_u64()).unwrap_or(0);
        let recv = item.get("events_received").and_then(|v| v.as_u64()).unwrap_or(0);

        println!("{:<30} {:<10} {:<22} {:<12} {:<12}", peer_id, direction, address, sent, recv);
    }
}

fn format_info(json: &Value) {
    let version = json.get("version").and_then(|v| v.as_str()).unwrap_or("-");
    let node_id = json.get("node_id").and_then(|v| v.as_str()).unwrap_or("-");
    let fp = json.get("cert_fingerprint").and_then(|v| v.as_str()).unwrap_or("none");
    let relay_url = json.get("relay_url").and_then(|v| v.as_str()).unwrap_or("-");
    let mesh_listen = json.get("mesh_listen").and_then(|v| v.as_str()).unwrap_or("-");
    let rf = json.get("replication_factor").and_then(|v| v.as_u64()).unwrap_or(0);
    let tls = json.get("tls_enabled").and_then(|v| v.as_bool()).unwrap_or(false);

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
        assert_eq!(cli_run.command, Some(Commands::Run));

        let args_default = vec!["goy-node"];
        let cli_default = Cli::parse_from(args_default);
        assert_eq!(cli_default.command, None);
    }
}
