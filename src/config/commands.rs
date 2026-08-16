use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Result, bail};

use super::default_config_path;
use super::resolver::mask_secret;
use super::schema::GoyNodeConfig;

/// Argumentos para `config init`
#[derive(Debug, Clone, Default)]
pub struct InitArgs {
    /// Override do coord URL
    pub coord_url: Option<String>,
    /// Override da admin API key
    pub admin_api_key: Option<String>,
    /// Override do data dir
    pub data_dir: Option<PathBuf>,
    /// Override do relay URL
    pub relay_url: Option<String>,
    /// Override do mesh listen
    pub mesh_listen: Option<String>,
    /// Override do metrics listen
    pub metrics_listen: Option<String>,
    /// Override do log level
    pub log_level: Option<String>,
    /// Não fazer prompts interativos
    pub non_interactive: bool,
    /// Sobrescrever config existente sem perguntar
    pub force: bool,
}

/// Argumentos para `config set`
#[derive(Debug, Clone)]
pub struct SetArgs {
    /// Campo em notação dotted (ex: coord.url, mesh.listen)
    pub key: String,
    /// Novo valor (string, será parseado conforme tipo do campo)
    pub value: String,
}

/// Argumentos para `config get`
#[derive(Debug, Clone)]
pub struct GetArgs {
    /// Campo em notação dotted
    pub key: String,
}

/// Executa o subcomando `config init`.
pub fn cmd_init(args: &InitArgs, target_path: Option<&Path>) -> Result<()> {
    let config_path = target_path
        .map(|p| p.to_path_buf())
        .unwrap_or_else(default_config_path);

    // ── Verificar se já existe ─────────────────────────────────────────
    if config_path.exists() && !args.force {
        if args.non_interactive {
            bail!(
                "Config file already exists at {}.\nUse --force to overwrite or 'goy-node config set' to modify individual fields.",
                config_path.display()
            );
        }
        let overwrite = dialoguer::Confirm::new()
            .with_prompt(format!(
                "Config file already exists at {}. Overwrite?",
                config_path.display()
            ))
            .default(false)
            .interact()?;
        if !overwrite {
            println!("Aborted.");
            return Ok(());
        }
    }

    // ── Construir config (CLI flags > prompts > defaults) ──────────────
    let config = if args.non_interactive {
        build_config_non_interactive(args)?
    } else {
        build_config_interactive(args)?
    };

    // ── Validar antes de escrever ──────────────────────────────────────
    config.validate()?;

    // ── Escrever ficheiro ──────────────────────────────────────────────
    write_config(&config_path, &config)?;

    println!("💾 Configuration saved to {}", config_path.display());
    println!("✅ Done! Run 'goy-node onboard --auth-key gc_...' to register this node.");

    Ok(())
}

/// Constrói configuração para modo não interativo.
pub fn build_config_non_interactive(args: &InitArgs) -> Result<GoyNodeConfig> {
    let mut config = default_goy_node_config();

    // Aplicar overrides obrigatórios
    config.coord.url = args
        .coord_url
        .clone()
        .unwrap_or_else(|| "http://localhost:8080".to_string());
    config.coord.admin_api_key = args.admin_api_key.clone().unwrap_or_default();

    // Aplicar overrides opcionais
    if let Some(ref v) = args.data_dir {
        config.storage.data_dir = v.clone();
    }
    if let Some(ref v) = args.relay_url {
        config.relay.url = v.clone();
    }
    if let Some(ref v) = args.mesh_listen {
        config.mesh.listen = v.clone();
    }
    if let Some(ref v) = args.metrics_listen {
        config.metrics.listen = v.clone();
    }
    if let Some(ref v) = args.log_level {
        config.log.level = v.clone();
    }

    // Em modo non-interactive, admin_api_key deve estar presente
    if config.coord.admin_api_key.trim().is_empty() {
        bail!("--admin-api-key is required in non-interactive mode");
    }

    Ok(config)
}

/// Constrói configuração interativamente solicitando inputs do utilizador.
pub fn build_config_interactive(args: &InitArgs) -> Result<GoyNodeConfig> {
    use dialoguer::{Input, Password};

    let mut config = default_goy_node_config();

    // Coord URL
    config.coord.url = match &args.coord_url {
        Some(v) => v.clone(),
        None => Input::new()
            .with_prompt("Coord-server URL")
            .default("http://localhost:8080".to_string())
            .interact_text()?,
    };

    // Admin API Key (input escondido)
    config.coord.admin_api_key = match &args.admin_api_key {
        Some(v) => v.clone(),
        None => Password::new().with_prompt("Admin API Key").interact()?,
    };

    // Data dir
    config.storage.data_dir = match &args.data_dir {
        Some(v) => v.clone(),
        None => {
            let s: String = Input::new()
                .with_prompt("Data directory")
                .default("/var/lib/goy-node".to_string())
                .interact_text()?;
            PathBuf::from(s)
        }
    };

    // Relay URL
    config.relay.url = match &args.relay_url {
        Some(v) => v.clone(),
        None => Input::new()
            .with_prompt("Relay URL")
            .default("ws://127.0.0.1:7777".to_string())
            .interact_text()?,
    };

    // Mesh listen
    config.mesh.listen = match &args.mesh_listen {
        Some(v) => v.clone(),
        None => Input::new()
            .with_prompt("Mesh listen address")
            .default("0.0.0.0:8443".to_string())
            .interact_text()?,
    };

    // Metrics listen
    config.metrics.listen = match &args.metrics_listen {
        Some(v) => v.clone(),
        None => Input::new()
            .with_prompt("Metrics listen address")
            .default("127.0.0.1:9090".to_string())
            .interact_text()?,
    };

    // Log level
    config.log.level = match &args.log_level {
        Some(v) => v.clone(),
        None => Input::new()
            .with_prompt("Log level")
            .default("info".to_string())
            .interact_text()?,
    };

    Ok(config)
}

/// Executa o subcomando `config set`.
pub fn cmd_set(args: &SetArgs, target_path: Option<&Path>) -> Result<()> {
    let config_path = target_path
        .map(|p| p.to_path_buf())
        .unwrap_or_else(default_config_path);
    if !config_path.exists() {
        bail!(
            "Config file not found at {}. Run 'goy-node config init' first.",
            config_path.display()
        );
    }

    // Carregar config atual
    let content = std::fs::read_to_string(&config_path)?;
    let mut config: GoyNodeConfig = toml::from_str(&content)?;

    // Aplicar mudança
    apply_set_field(&mut config, &args.key, &args.value)?;

    // Validar após mudança
    config
        .validate()
        .map_err(|e| anyhow::anyhow!("Validation failed after setting {}: {e}", args.key))?;

    // Reescrever
    write_config(&config_path, &config)?;

    // Feedback com masking se for secret
    let display_value = if args.key.contains("api_key") || args.key.contains("secret") {
        mask_secret(&args.value)
    } else {
        args.value.clone()
    };
    println!("✅ Updated {} = \"{}\"", args.key, display_value);

    Ok(())
}

/// Aplica uma mudança a um campo específico usando notação dotted.
pub fn apply_set_field(config: &mut GoyNodeConfig, key: &str, value: &str) -> Result<()> {
    match key {
        // ── coord ──────────────────────────────────────────────────────
        "coord.url" => config.coord.url = value.to_string(),
        "coord.admin_api_key" => config.coord.admin_api_key = value.to_string(),
        "coord.heartbeat_interval_secs" => {
            config.coord.heartbeat_interval_secs = value
                .parse()
                .map_err(|_| anyhow::anyhow!("coord.heartbeat_interval_secs must be a positive integer"))?;
        }
        "coord.request_timeout_secs" => {
            config.coord.request_timeout_secs = value
                .parse()
                .map_err(|_| anyhow::anyhow!("coord.request_timeout_secs must be a positive integer"))?;
        }

        // ── relay ──────────────────────────────────────────────────────
        "relay.url" => config.relay.url = value.to_string(),
        "relay.import_cmd" => config.relay.import_cmd = Some(value.to_string()),

        // ── mesh ───────────────────────────────────────────────────────
        "mesh.listen" => config.mesh.listen = value.to_string(),
        "mesh.registry_url" => config.mesh.registry_url = Some(value.to_string()),
        "mesh.heartbeat_secs" => {
            config.mesh.heartbeat_secs = value
                .parse()
                .map_err(|_| anyhow::anyhow!("mesh.heartbeat_secs must be a positive integer"))?;
        }
        "mesh.tls_enabled" => {
            config.mesh.tls_enabled = value
                .parse()
                .map_err(|_| anyhow::anyhow!("mesh.tls_enabled must be true or false"))?;
        }
        "mesh.seeds" => {
            let wrapper = format!("seeds = {value}");
            let parsed: toml::Value = toml::from_str(&wrapper)
                .map_err(|e| anyhow::anyhow!("mesh.seeds must be a TOML array: {e}"))?;
            if let Some(arr) = parsed.get("seeds").and_then(|v| v.as_array()) {
                config.mesh.seeds = arr
                    .iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect();
            } else {
                bail!("mesh.seeds must be a TOML array of strings");
            }
        }

        // ── storage ────────────────────────────────────────────────────
        "storage.data_dir" => config.storage.data_dir = PathBuf::from(value),
        "storage.extra_contribution_gb" => {
            config.storage.extra_contribution_gb = value
                .parse()
                .map_err(|_| anyhow::anyhow!("storage.extra_contribution_gb must be a non-negative integer"))?;
        }

        // ── metrics ────────────────────────────────────────────────────
        "metrics.listen" => config.metrics.listen = value.to_string(),

        // ── log ────────────────────────────────────────────────────────
        "log.level" => config.log.level = value.to_string(),
        "log.format" => config.log.format = value.to_string(),

        // ── Desconhecido ───────────────────────────────────────────────
        _ => {
            let available = [
                "coord.url",
                "coord.admin_api_key",
                "coord.heartbeat_interval_secs",
                "coord.request_timeout_secs",
                "relay.url",
                "relay.import_cmd",
                "mesh.listen",
                "mesh.registry_url",
                "mesh.heartbeat_secs",
                "mesh.tls_enabled",
                "mesh.seeds",
                "storage.data_dir",
                "storage.extra_contribution_gb",
                "metrics.listen",
                "log.level",
                "log.format",
            ];
            bail!(
                "Unknown config field: {}\nAvailable fields: {}",
                key,
                available.join(", ")
            );
        }
    }

    Ok(())
}

/// Executa o subcomando `config get`.
pub fn cmd_get(args: &GetArgs, target_path: Option<&Path>) -> Result<()> {
    let config_path = target_path
        .map(|p| p.to_path_buf())
        .unwrap_or_else(default_config_path);
    if !config_path.exists() {
        bail!(
            "Config file not found at {}. Run 'goy-node config init' first.",
            config_path.display()
        );
    }

    let content = std::fs::read_to_string(&config_path)?;
    let config: GoyNodeConfig = toml::from_str(&content)?;

    let value = get_field(&config, &args.key)?;

    // Mascarar secrets na saída
    if args.key.contains("api_key") || args.key.contains("secret") {
        println!("{}", mask_secret(&value));
    } else {
        println!("{value}");
    }

    Ok(())
}

/// Lê o valor de um campo específico em notação dotted.
pub fn get_field(config: &GoyNodeConfig, key: &str) -> Result<String> {
    match key {
        "coord.url" => Ok(config.coord.url.clone()),
        "coord.admin_api_key" => Ok(config.coord.admin_api_key.clone()),
        "coord.heartbeat_interval_secs" => Ok(config.coord.heartbeat_interval_secs.to_string()),
        "coord.request_timeout_secs" => Ok(config.coord.request_timeout_secs.to_string()),
        "relay.url" => Ok(config.relay.url.clone()),
        "relay.import_cmd" => Ok(config.relay.import_cmd.clone().unwrap_or_default()),
        "mesh.listen" => Ok(config.mesh.listen.clone()),
        "mesh.registry_url" => Ok(config.mesh.registry_url.clone().unwrap_or_default()),
        "mesh.heartbeat_secs" => Ok(config.mesh.heartbeat_secs.to_string()),
        "mesh.tls_enabled" => Ok(config.mesh.tls_enabled.to_string()),
        "mesh.seeds" => Ok(toml::to_string(&config.mesh.seeds)?.trim().to_string()),
        "storage.data_dir" => Ok(config.storage.data_dir.display().to_string()),
        "storage.extra_contribution_gb" => Ok(config.storage.extra_contribution_gb.to_string()),
        "metrics.listen" => Ok(config.metrics.listen.clone()),
        "log.level" => Ok(config.log.level.clone()),
        "log.format" => Ok(config.log.format.clone()),
        _ => bail!(
            "Unknown config field: {}. Run 'goy-node config show' to see all fields.",
            key
        ),
    }
}

/// Escreve a configuração no disco com permissões 0600 em Unix.
pub fn write_config(path: &Path, config: &GoyNodeConfig) -> Result<()> {
    // Criar diretório pai
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Serializar
    let header = "# Goy Node Configuration\n# Generated by: goy-node config init\n\n";
    let body = toml::to_string_pretty(config)?;
    let content = format!("{header}{body}");

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        file.write_all(content.as_bytes())?;
    }
    #[cfg(not(unix))]
    {
        std::fs::write(path, content)?;
    }

    Ok(())
}

/// Escreve a configuração auto-gerada com cabeçalho explicativo e permissões 0600 em Unix.
pub fn write_config_auto(path: &Path, config: &GoyNodeConfig) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let header = concat!(
        "# Goy Node Configuration\n",
        "# Auto-generated on first run. Edit with: goy-node config set <key> <value>\n",
        "# Or regenerate with: goy-node config init --force\n",
        "\n",
    );
    let body = toml::to_string_pretty(config)?;
    let content = format!("{header}{body}");

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        file.write_all(content.as_bytes())?;
    }
    #[cfg(not(unix))]
    {
        std::fs::write(path, content)?;
    }

    Ok(())
}

fn default_goy_node_config() -> GoyNodeConfig {
    GoyNodeConfig {
        coord: super::schema::CoordConfig {
            url: "http://localhost:8080".to_string(),
            admin_api_key: String::new(),
            heartbeat_interval_secs: super::schema::default_heartbeat_interval(),
            request_timeout_secs: super::schema::default_coord_timeout(),
        },
        relay: super::schema::RelayConfig {
            url: super::schema::default_relay_url(),
            import_cmd: None,
        },
        mesh: super::schema::MeshConfig {
            listen: super::schema::default_mesh_listen(),
            seeds: vec![],
            registry_url: None,
            heartbeat_secs: super::schema::default_mesh_heartbeat(),
            tls_enabled: true,
            trusted_fingerprints: Default::default(),
        },
        storage: super::schema::StorageConfig {
            data_dir: super::schema::default_data_dir(),
            extra_contribution_gb: 0,
        },
        metrics: super::schema::MetricsConfig {
            listen: super::schema::default_metrics_listen(),
        },
        log: super::schema::LogConfig {
            level: super::schema::default_log_level(),
            format: super::schema::default_log_format(),
        },
    }
}
