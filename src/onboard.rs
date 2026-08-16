//! Lógica de onboarding e offboarding do Goy Node na plataforma e VPN da Goy Company.

use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use tracing::{error, info, warn};

use crate::config::DEFAULT_CONFIG_TEMPLATE;
use crate::goy_api::{GoyApiClient, validate_auth_key};

/// Código de saída do processo (exit code 5) quando ocorre falha de armazenamento durante o onboarding.
pub const EXIT_ONBOARD_STORAGE_ERROR: i32 = 5;

/// Estado persistido de onboarding (`data_dir/onboard_state.json`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnboardState {
    pub node_id: String,
    pub auth_key_hash: String,
    pub vpn_configured_at: u64,
    pub api_registered_at: u64,
    pub bearer_token: Option<String>,
    #[serde(default)]
    pub provider: Option<String>,
}

/// Estado persistido da VPN (`data_dir/vpn_state.json`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VpnState {
    pub vpn_ip: Option<String>,
    pub magic_dns: Option<String>,
    pub client_type: String,
    pub configured_at: u64,
    #[serde(default)]
    pub provider: Option<String>,
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
    if let Ok(output) = Command::new("tailscale").arg("ip").arg("-4").output()
        && output.status.success()
    {
        let ip = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !ip.is_empty() {
            return Some(format!("ws://{ip}:8443"));
        }
    }

    // 2. Tentar `tailscale status --json` para extrair MagicDNS ou Self IP
    if let Ok(output) = Command::new("tailscale")
        .arg("status")
        .arg("--json")
        .output()
        && output.status.success()
        && let Ok(v) = serde_json::from_slice::<serde_json::Value>(&output.stdout)
        && let Some(dns) = v
            .get("Self")
            .and_then(|s| s.get("DNSName"))
            .and_then(|d| d.as_str())
    {
        let dns_trimmed = dns.trim_end_matches('.');
        if !dns_trimmed.is_empty() {
            return Some(format!("ws://{dns_trimmed}:8443"));
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
        error!(
            "❌ Invalid auth key format. Key must start with 'gc_' and have at least 10 characters."
        );
        return Ok(2);
    }

    // 2. Garantir diretórios de dados e verificar storage (Fail-Fast local antes de API/VPN)
    let target_data_dir = data_dir
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("./data"));
    let _ = fs::create_dir_all(&target_data_dir);

    let cfg_file_path = config_path
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("config.toml"));

    let extra_contribution_gb = if cfg_file_path.exists() {
        if let Ok(content) = fs::read_to_string(&cfg_file_path) {
            if let Ok(cfg) = toml::from_str::<crate::config::Config>(&content) {
                cfg.storage.extra_contribution_gb
            } else {
                0
            }
        } else {
            0
        }
    } else {
        0
    };

    let storage_cfg = crate::storage::StorageConfig {
        extra_contribution_gb,
        data_dir: target_data_dir.clone(),
    };

    match crate::storage::verify_storage(&storage_cfg) {
        Ok(info) => {
            println!("🔍 A verificar requisitos de storage...");
            println!("   Disco disponível: {} GB", info.available_gb);
            println!(
                "   Mínimo requerido: {} GB ✅",
                crate::storage::MIN_RESERVED_GB
            );
            if extra_contribution_gb > 0 {
                println!("   Contribuição extra: {extra_contribution_gb} GB");
            }
            println!("   Total reservado: {} GB ✅", info.total_reserved_gb);
        }
        Err(err) => {
            eprintln!("🔍 A verificar requisitos de storage...");
            match &err {
                crate::storage::StorageError::InsufficientSpace {
                    available_gb,
                    required_gb,
                } => {
                    eprintln!("   Disco disponível: {available_gb} GB");
                    eprintln!("   Mínimo requerido: {required_gb} GB ❌");
                    eprintln!();
                    eprintln!("❌ Espaço insuficiente para operar o Goy Node.");
                    eprintln!();
                    eprintln!("   O Goy Node requer pelo menos 50 GB de espaço reservado");
                    eprintln!("   para garantir redundância de dados na rede Goy.");
                    eprintln!();
                    eprintln!("   Ações possíveis:");
                    eprintln!("   • Libertar espaço em disco");
                    eprintln!("   • Escolher outro data_dir (--data-dir /caminho/com/espaco)");
                    eprintln!("   • Montar volume adicional no data_dir atual");
                }
                crate::storage::StorageError::PermissionDenied(path) => {
                    eprintln!("   Data dir: {} ❌", path.display());
                    eprintln!();
                    eprintln!("❌ Permissão negada ao aceder ou escrever no diretório de dados.");
                    eprintln!("   Verifique as permissões de leitura/escrita do utilizador.");
                }
                crate::storage::StorageError::DataDirNotFound(path) => {
                    eprintln!("   Data dir: {} ❌", path.display());
                    eprintln!();
                    eprintln!("❌ Não foi possível encontrar ou criar o diretório de dados.");
                }
                crate::storage::StorageError::FilesystemError(msg) => {
                    eprintln!();
                    eprintln!("❌ Erro no sistema de ficheiros: {msg}");
                }
            }
            if target_data_dir.exists() {
                eprintln!();
                eprintln!(
                    "ℹ️  O diretório '{}' foi mantido sem ficheiros de estado e pode ser removido manualmente.",
                    target_data_dir.display()
                );
            }
            return Ok(EXIT_ONBOARD_STORAGE_ERROR);
        }
    }

    // 3. Registar na API da Goy Company (se não for vpn_only)
    let node_id = uuid::Uuid::new_v4().to_string();
    let api_url = std::env::var("GOY_API_URL").ok();
    let api_client = GoyApiClient::new(api_url.as_deref());

    let (
        registered_node_id,
        vpn_key_to_use,
        vpn_control_url_to_use,
        vpn_provider_to_use,
        bearer_token,
        registry_url_from_api,
    ) = if !vpn_only {
        match api_client.register_node(&auth_key, Some(&node_id)).await {
            Ok(res) => {
                info!(
                    "✅ Successfully registered node {} with Goy Company API!",
                    res.node_id
                );
                let vpn_key = res.get_vpn_auth_key().unwrap_or_else(|| auth_key.clone());
                let control_url = res.get_vpn_control_url();
                let provider = res.get_vpn_provider();
                let bearer = res.bearer_token;
                let reg_url = res.registry_url;
                (res.node_id, vpn_key, control_url, provider, bearer, reg_url)
            }
            Err(e) => {
                error!("❌ Goy Company API registration failed: {e}");
                return Ok(4); // Exit code 4: API indisponível
            }
        }
    } else {
        info!("ℹ️ Skipping API registration (--vpn-only mode)");
        (node_id, auth_key.clone(), None, None, None, None)
    };

    // Determinar modo de VPN efetivo (Tailscale vs Headscale com fallback legacy)
    let (effective_provider, is_fallback) = match vpn_provider_to_use.as_deref() {
        Some("tailscale") => ("tailscale", false),
        Some("headscale") => ("headscale", false),
        Some(other) => {
            warn!("⚠️ Desconhecido VPN provider '{other}'. A recorrer à lógica legacy...");
            if vpn_control_url_to_use
                .as_ref()
                .is_some_and(|u| !u.trim().is_empty())
            {
                ("headscale", true)
            } else {
                ("tailscale", true)
            }
        }
        None => {
            if vpn_control_url_to_use
                .as_ref()
                .is_some_and(|u| !u.trim().is_empty())
            {
                ("headscale", true)
            } else {
                ("tailscale", true)
            }
        }
    };

    // Validação de inputs por provider
    if effective_provider == "tailscale" {
        if vpn_key_to_use.trim().is_empty() {
            error!("❌ Erro na configuração VPN (Tailscale): auth_key está vazia.");
            return Ok(3);
        }
    } else if effective_provider == "headscale" {
        if vpn_key_to_use.trim().is_empty() {
            error!("❌ Erro na configuração VPN (Headscale): auth_key está vazia.");
            return Ok(3);
        }
        if vpn_control_url_to_use
            .as_ref()
            .is_none_or(|u| u.trim().is_empty())
        {
            error!("❌ Erro na configuração VPN (Headscale): control_url está vazio.");
            return Ok(3);
        }
    }

    // 3. Configurar cliente VPN (Tailscale/Headscale)
    info!("🔒 Configuring VPN connection...");
    let tailscale_available = Command::new("tailscale").arg("version").output().is_ok();
    let mut vpn_configured = false;
    let mut vpn_ip = None;

    if tailscale_available {
        let mut cmd = Command::new("tailscale");
        cmd.arg("up")
            .arg("--reset")
            .arg("--timeout=2s")
            .arg(format!("--authkey={vpn_key_to_use}"))
            .arg("--accept-routes");

        if effective_provider == "headscale" {
            let ctrl_url = vpn_control_url_to_use.as_deref().unwrap_or_default();
            if is_fallback {
                info!("🔗 A configurar VPN via Headscale (legacy fallback: {ctrl_url})...");
            } else {
                info!("🔗 A configurar VPN via Headscale (self-hosted: {ctrl_url})...");
            }
            cmd.arg(format!("--login-server={ctrl_url}"));
        } else if is_fallback {
            info!("🔗 A configurar VPN via Tailscale (legacy fallback)...");
        } else {
            info!("🔗 A configurar VPN via Tailscale (SaaS)...");
        }

        let up_res = cmd.output();

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
            info!(
                "💡 Please install Tailscale manually from https://tailscale.com/download to join the Goy mesh network."
            );
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

    // 5. Anunciar relay no registry do coord-server
    if let Some(ref reg_url) = registry_url_from_api {
        info!("📢 Announcing relay to registry at {reg_url}...");
        let registry_client = GoyApiClient::new(Some(reg_url));

        let storage_reserved = crate::storage::MIN_RESERVED_GB + extra_contribution_gb;
        let storage_available = match crate::storage::verify_storage(&storage_cfg) {
            Ok(info) => info.available_gb,
            Err(_) => 0,
        };

        if let Err(e) = registry_client
            .announce_relay(
                &auth_key,
                &registered_node_id,
                &target_mesh_url,
                Some("sha256:pending"),
                storage_reserved,
                storage_available,
            )
            .await
        {
            warn!("⚠️ Failed to announce relay to registry: {e}");
        }
    } else {
        warn!("⚠️ No registry URL available, skipping relay announce");
    }

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
        provider: Some(effective_provider.to_string()),
    };

    fs::write(
        target_data_dir.join("onboard_state.json"),
        serde_json::to_string_pretty(&onboard_state)?,
    )?;

    // Persistir vpn_state.json
    let vpn_state = VpnState {
        vpn_ip: Some(target_mesh_url.clone()),
        magic_dns: None,
        client_type: if tailscale_available {
            effective_provider.to_string()
        } else {
            "none".to_string()
        },
        configured_at: now,
        provider: Some(effective_provider.to_string()),
    };

    fs::write(
        target_data_dir.join("vpn_state.json"),
        serde_json::to_string_pretty(&vpn_state)?,
    )?;

    // Escrever config.toml se não existir ou atualizar mesh_url / registry_url

    let config_content = if cfg_file_path.exists() {
        let mut existing = fs::read_to_string(&cfg_file_path)?;
        if !existing.contains("mesh_url") {
            existing.push_str(&format!(
                "\nmesh_url = \"{target_mesh_url}\"\nnode_id = \"{registered_node_id}\"\n"
            ));
        }
        if let Some(reg_url) = &registry_url_from_api
            && !existing.contains("registry_url")
        {
            existing.push_str(&format!("registry_url = \"{reg_url}\"\n"));
        }
        existing
    } else {
        let mut tpl = format!(
            "{DEFAULT_CONFIG_TEMPLATE}\nmesh_url = \"{target_mesh_url}\"\nnode_id = \"{registered_node_id}\"\n"
        );
        if let Some(reg_url) = &registry_url_from_api {
            tpl.push_str(&format!("registry_url = \"{reg_url}\"\n"));
        }
        tpl
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
    if let Some(ref st) = onboard_state
        && let Some(ref token) = st.bearer_token
    {
        let api_url = std::env::var("GOY_API_URL").ok();
        let api_client = GoyApiClient::new(api_url.as_deref());
        let _ = api_client.deregister_node(token, &st.node_id).await;
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
