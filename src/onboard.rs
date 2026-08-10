//! Lógica de onboarding e offboarding do Goy Node na plataforma e VPN da Goy Company.

use sha2::Digest;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use tracing::{error, info, warn};

use crate::config::DEFAULT_CONFIG_TEMPLATE;
use crate::goy_api::{validate_auth_key, GoyApiClient};

/// Estado persistido de onboarding (`data_dir/onboard_state.json`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnboardState {
    pub node_id: String,
    pub auth_key_hash: String,
    pub vpn_configured_at: u64,
    pub api_registered_at: u64,
    pub bearer_token: Option<String>,
}

/// Estado persistido da VPN (`data_dir/vpn_state.json`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VpnState {
    pub vpn_ip: Option<String>,
    pub magic_dns: Option<String>,
    pub client_type: String,
    pub configured_at: u64,
}

/// Lê o estado de onboarding de `data_dir/onboard_state.json`.
pub fn check_onboard_status(data_dir: Option<&Path>) -> Option<OnboardState> {
    let dir = data_dir?;
    let path = dir.join("onboard_state.json");
    if !path.exists() {
        return None;
    }
    let content = fs::read_to_string(&path).ok()?;
    serde_json::from_str(&content).ok()
}

/// Tenta auto-detectar o `mesh_url` a partir da CLI do Tailscale/Headscale ou interfaces de rede VPN.
pub fn detect_vpn_mesh_url() -> Option<String> {
    // 1. Tentar `tailscale ip -4`
    if let Ok(output) = Command::new("tailscale").arg("ip").arg("-4").output() {
        if output.status.success() {
            let ip = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !ip.is_empty() {
                return Some(format!("ws://{ip}:8443"));
            }
        }
    }

    // 2. Tentar `tailscale status --json` para extrair MagicDNS ou Self IP
    if let Ok(output) = Command::new("tailscale").arg("status").arg("--json").output() {
        if output.status.success() {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&output.stdout) {
                if let Some(dns) = v.get("Self").and_then(|s| s.get("DNSName")).and_then(|d| d.as_str()) {
                    let dns_trimmed = dns.trim_end_matches('.');
                    if !dns_trimmed.is_empty() {
                        return Some(format!("ws://{dns_trimmed}:8443"));
                    }
                }
            }
        }
    }

    None
}

/// Executa o onboarding do nó (subcomando `goy-node onboard`).
pub async fn run_onboard(
    auth_key_flag: Option<String>,
    non_interactive: bool,
    vpn_only: bool,
    config_path: Option<&Path>,
    data_dir: Option<&Path>,
) -> anyhow::Result<i32> {
    info!("🚀 Starting Goy Node Onboarding Wizard...");

    // 1. Obter e validar a auth key
    let auth_key = match auth_key_flag {
        Some(k) => k,
        None => {
            if non_interactive {
                error!("❌ Auth key is required in non-interactive mode (--auth-key gc_xxx)");
                return Ok(2); // Exit code 2: auth key inválida/ausente
            }
            print!("🔑 Enter your Goy Company Auth Key (gc_...): ");
            io::stdout().flush()?;
            let mut input = String::new();
            io::stdin().read_line(&mut input)?;
            input.trim().to_string()
        }
    };

    if !validate_auth_key(&auth_key) {
        error!("❌ Invalid auth key format. Key must start with 'gc_' and have at least 10 characters.");
        return Ok(2);
    }

    // 2. Registar na API da Goy Company (se não for vpn_only)
    let node_id = uuid::Uuid::new_v4().to_string();
    let api_url = std::env::var("GOY_API_URL").ok();
    let api_client = GoyApiClient::new(api_url.as_deref());

    let (registered_node_id, vpn_key_to_use, bearer_token) = if !vpn_only {
        match api_client.register_node(&auth_key, Some(&node_id)).await {
            Ok(res) => {
                info!("✅ Successfully registered node {} with Goy Company API!", res.node_id);
                (
                    res.node_id,
                    res.vpn_auth_key.unwrap_or_else(|| auth_key.clone()),
                    Some(res.bearer_token),
                )
            }
            Err(e) => {
                error!("❌ Goy Company API registration failed: {e}");
                return Ok(4); // Exit code 4: API indisponível
            }
        }
    } else {
        info!("ℹ️ Skipping API registration (--vpn-only mode)");
        (node_id, auth_key.clone(), None)
    };

    // 3. Configurar cliente VPN (Tailscale/Headscale)
    info!("🔒 Configuring VPN connection...");
    let tailscale_available = Command::new("tailscale").arg("version").output().is_ok();
    let mut vpn_configured = false;
    let mut vpn_ip = None;

    if tailscale_available {
        info!("🌀 Found Tailscale CLI. Joining Goy VPN network...");
        let up_res = Command::new("tailscale")
            .arg("up")
            .arg(format!("--authkey={vpn_key_to_use}"))
            .arg("--accept-routes")
            .output();

        match up_res {
            Ok(out) if out.status.success() => {
                info!("✅ Tailscale VPN connected successfully!");
                vpn_configured = true;
                vpn_ip = detect_vpn_mesh_url();
            }
            Ok(out) => {
                let err = String::from_utf8_lossy(&out.stderr);
                warn!("⚠️ Tailscale up completed with warnings: {err}");
                vpn_configured = true;
                vpn_ip = detect_vpn_mesh_url();
            }
            Err(e) => {
                warn!("⚠️ Failed to run tailscale up command: {e}");
            }
        }
    } else {
        warn!("⚠️ Tailscale/Headscale CLI is not installed on this system.");
        if !non_interactive {
            info!("💡 Please install Tailscale manually from https://tailscale.com/download to join the Goy mesh network.");
        }
    }

    if !vpn_configured && vpn_only {
        error!("❌ VPN configuration failed.");
        return Ok(3); // Exit code 3: VPN falhou
    }

    // 4. Auto-detetar `mesh_url` e atualizar / gerar config.toml
    let detected_mesh = vpn_ip.or_else(detect_vpn_mesh_url);
    let target_mesh_url = match detected_mesh {
        Some(url) => {
            info!("🌐 Auto-detected mesh_url: {url}");
            url
        }
        None => {
            warn!("⚠️ Could not auto-detect VPN IP address. Defaulting to loopback.");
            "ws://127.0.0.1:8443".to_string()
        }
    };

    // Garantir criação do data_dir e gravação de estados
    let target_data_dir = data_dir
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("./data"));
    fs::create_dir_all(&target_data_dir)?;

    // Gravar node_id.txt
    fs::write(target_data_dir.join("node_id.txt"), &registered_node_id)?;

    // Persistir onboard_state.json
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let onboard_state = OnboardState {
        node_id: registered_node_id.clone(),
        auth_key_hash: format!("{:x}", sha2::Sha256::digest(auth_key.as_bytes())),
        vpn_configured_at: now,
        api_registered_at: now,
        bearer_token,
    };

    fs::write(
        target_data_dir.join("onboard_state.json"),
        serde_json::to_string_pretty(&onboard_state)?,
    )?;

    // Persistir vpn_state.json
    let vpn_state = VpnState {
        vpn_ip: Some(target_mesh_url.clone()),
        magic_dns: None,
        client_type: if tailscale_available { "tailscale".to_string() } else { "none".to_string() },
        configured_at: now,
    };

    fs::write(
        target_data_dir.join("vpn_state.json"),
        serde_json::to_string_pretty(&vpn_state)?,
    )?;

    // Escrever config.toml se não existir ou atualizar mesh_url
    let cfg_file_path = config_path
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("config.toml"));

    let config_content = if cfg_file_path.exists() {
        let existing = fs::read_to_string(&cfg_file_path)?;
        if !existing.contains("mesh_url") {
            format!("{existing}\nmesh_url = \"{target_mesh_url}\"\nnode_id = \"{registered_node_id}\"\n")
        } else {
            existing
        }
    } else {
        format!("{DEFAULT_CONFIG_TEMPLATE}\nmesh_url = \"{target_mesh_url}\"\nnode_id = \"{registered_node_id}\"\n")
    };

    fs::write(&cfg_file_path, config_content)?;

    info!("🎉 Onboarding completed successfully!");
    info!("✅ Config written to: {}", cfg_file_path.display());
    info!("✅ Node ID: {registered_node_id}");
    info!("✅ Run 'goy-node run' to start the mesh node!");

    Ok(0) // Exit code 0: Sucesso
}

/// Deregista o nó da plataforma e da VPN (subcomando `goy-node offboard`).
pub async fn run_offboard(
    force: bool,
    _config_path: Option<&Path>,
    data_dir: Option<&Path>,
) -> anyhow::Result<i32> {
    info!("👋 Starting Goy Node Offboarding...");

    let target_data_dir = data_dir
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("./data"));

    let onboard_state = check_onboard_status(Some(&target_data_dir));

    if !force {
        print!("⚠️ Are you sure you want to offboard this node? [y/N]: ");
        io::stdout().flush()?;
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        if !input.trim().eq_ignore_ascii_case("y") {
            info!("Offboard operation cancelled.");
            return Ok(0);
        }
    }

    // 1. Deregistar da API Goy Company
    if let Some(ref st) = onboard_state {
        if let Some(ref token) = st.bearer_token {
            let api_url = std::env::var("GOY_API_URL").ok();
            let api_client = GoyApiClient::new(api_url.as_deref());
            let _ = api_client.deregister_node(token, &st.node_id).await;
        }
    }

    // 2. Logout na Tailscale / VPN se disponível
    if Command::new("tailscale").arg("version").output().is_ok() {
        info!("🌀 Running tailscale logout...");
        let _ = Command::new("tailscale").arg("logout").output();
    }

    // 3. Remover ficheiros de estado
    let _ = fs::remove_file(target_data_dir.join("onboard_state.json"));
    let _ = fs::remove_file(target_data_dir.join("vpn_state.json"));

    info!("👋 Node removed from Goy platform. Offboard complete.");
    Ok(0)
}
