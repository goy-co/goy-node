use super::schema::GoyNodeConfig;
use anyhow::{bail, Result};
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use tracing::{info, warn};

/// Origem de um valor de configuração.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigSource {
    /// Valor definido via CLI flag (ex: --coord-url)
    CliFlag(String),
    /// Valor lido do config.toml
    ConfigFile(PathBuf),
    /// Valor lido de variável de ambiente (deprecated)
    EnvVar(String),
    /// Valor definido interativamente pelo utilizador
    InteractivePrompt,
    /// Valor default hardcoded
    Default,
}

impl fmt::Display for ConfigSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CliFlag(name) => write!(f, "CLI flag --{name}"),
            Self::ConfigFile(path) => write!(f, "config.toml ({})", path.display()),
            Self::EnvVar(name) => write!(f, "env var {name}"),
            Self::InteractivePrompt => write!(f, "interactive prompt"),
            Self::Default => write!(f, "default"),
        }
    }
}

/// Um valor de configuração com metadata de origem.
#[derive(Debug, Clone)]
pub struct SourcedValue<T> {
    pub value: T,
    pub source: ConfigSource,
}

/// Resultado completo da resolução de configuração.
#[derive(Debug)]
pub struct ResolvedConfig {
    pub config: GoyNodeConfig,
    /// Mapa de campos → origem (para logging/debugging).
    /// Chaves usam notação dotted: "coord.url", "mesh.listen", etc.
    pub sources: HashMap<String, ConfigSource>,
    /// Warnings gerados durante a resolução (ex: env vars deprecated).
    pub warnings: Vec<String>,
}

/// Opções de resolução passadas pelo CLI.
#[derive(Debug, Default, Clone)]
pub struct ResolveOptions {
    /// Path explícito do config.toml (via --config)
    pub config_path: Option<PathBuf>,
    /// Override do coord URL (via --coord-url)
    pub coord_url: Option<String>,
    /// Override da admin API key (via --admin-api-key)
    pub admin_api_key: Option<String>,
    /// Override do data dir (via --data-dir)
    pub data_dir: Option<PathBuf>,
    /// Override do log level (via --log-level)
    pub log_level: Option<String>,
    /// Override do log format (via --log-format)
    pub log_format: Option<String>,
    /// Se true, nunca fazer prompts interativos
    pub no_interactive: bool,
}

/// Resolve a configuração completa aplicando a cascata de prioridade.
pub fn resolve(opts: &ResolveOptions) -> Result<ResolvedConfig> {
    let mut sources = HashMap::new();
    let mut warnings = Vec::new();

    let config_path = opts
        .config_path
        .clone()
        .unwrap_or_else(super::default_config_path);

    // ── Passo 1: Carregar ou auto-gerar config ─────────────────────────
    let (mut config, auto_generated) = if config_path.exists() {
        let content = std::fs::read_to_string(&config_path)?;
        let cfg: GoyNodeConfig = toml::from_str(&content).map_err(|e| {
            anyhow::anyhow!(
                "Failed to parse config at {}: {e}",
                config_path.display()
            )
        })?;
        info!("📄 Loaded config from {}", config_path.display());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Ok(meta) = std::fs::metadata(&config_path) {
                let mode = meta.permissions().mode() & 0o777;
                if mode & 0o044 != 0 {
                    warnings.push(format!(
                        "⚠️  Config file {} has insecure permissions ({:03o}). Contains secrets — should be 0600. Fix: chmod 600 {}",
                        config_path.display(),
                        mode,
                        config_path.display()
                    ));
                }
            }
        }
        let src = ConfigSource::ConfigFile(config_path.clone());
        mark_all_fields_as_source(&mut sources, src);
        (cfg, false)
    } else {
        // AUTO-GENERATE: construir config a partir de CLI flags + defaults
        warn!(
            "🔧 First-time setup: config file not found at {}. Generating...",
            config_path.display()
        );
        let cfg = build_config_from_opts(opts);
        mark_all_fields_as_source(&mut sources, ConfigSource::Default);
        mark_cli_sources(&mut sources, opts);
        (cfg, true)
    };

    // ── Passo 2: Env vars (deprecated) — só se NÃO foi auto-gerado ─────
    let file_source = ConfigSource::ConfigFile(config_path.clone());
    if !auto_generated {
        apply_env_overrides(&mut config, &file_source, &mut sources, &mut warnings);
    }

    // ── Passo 3: CLI flags (sempre aplicadas) ──────────────────────────
    apply_cli_overrides(&mut config, opts, &mut sources);

    // ── Passo 4: Prompts interativos (se faltar algo) ───────────────────
    let prompt_result = super::prompts::prompt_missing_fields(&mut config, opts, &sources)?;
    for (field, _) in &prompt_result.filled_fields {
        sources.insert(field.clone(), ConfigSource::InteractivePrompt);
    }

    // ── Passo 5: Validação final de campos obrigatórios ────────────────
    if config.coord.url.is_empty() {
        bail!("coord.url is required but was not provided.");
    }
    if config.coord.admin_api_key.trim().is_empty() {
        bail!("coord.admin_api_key is required but was not provided.");
    }

    // ── Passo 6: Escrever config se auto-gerado ou preenchido por prompts ─
    let should_write = auto_generated || prompt_result.prompted;
    if should_write {
        super::commands::write_config_auto(&config_path, &config)?;
        info!("💾 Configuration saved to {}", config_path.display());
        warnings.push(format!(
            "Configuration saved to {}. Edit with 'goy-node config set'.",
            config_path.display()
        ));
    }

    // ── Passo 7: Validação completa ────────────────────────────────────
    config.validate()?;

    // ── Passo 8: Logging de transparência ──────────────────────────────
    log_resolved_config(&config, &sources);

    Ok(ResolvedConfig {
        config,
        sources,
        warnings,
    })
}

/// Constrói um GoyNodeConfig mínimo a partir das CLI flags fornecidas.
/// Campos não fornecidos ficam com defaults.
fn build_config_from_opts(opts: &ResolveOptions) -> GoyNodeConfig {
    let mut config = default_goy_node_config();

    if let Some(ref url) = opts.coord_url {
        config.coord.url = url.clone();
    }
    if let Some(ref key) = opts.admin_api_key {
        config.coord.admin_api_key = key.clone();
    }
    if let Some(ref dir) = opts.data_dir {
        config.storage.data_dir = dir.clone();
    }
    if let Some(ref level) = opts.log_level {
        config.log.level = level.clone();
    }
    if let Some(ref format) = opts.log_format {
        config.log.format = format.clone();
    }

    config
}

/// Marca as fontes dos campos que vieram de CLI flags.
fn mark_cli_sources(sources: &mut HashMap<String, ConfigSource>, opts: &ResolveOptions) {
    if opts.coord_url.is_some() {
        sources.insert(
            "coord.url".to_string(),
            ConfigSource::CliFlag("coord-url".to_string()),
        );
    }
    if opts.admin_api_key.is_some() {
        sources.insert(
            "coord.admin_api_key".to_string(),
            ConfigSource::CliFlag("admin-api-key".to_string()),
        );
    }
    if opts.data_dir.is_some() {
        sources.insert(
            "storage.data_dir".to_string(),
            ConfigSource::CliFlag("data-dir".to_string()),
        );
    }
    if opts.log_level.is_some() {
        sources.insert(
            "log.level".to_string(),
            ConfigSource::CliFlag("log-level".to_string()),
        );
    }
    if opts.log_format.is_some() {
        sources.insert(
            "log.format".to_string(),
            ConfigSource::CliFlag("log-format".to_string()),
        );
    }
}

/// Mapeamento de env vars → campos do config.
const ENV_VAR_MAPPINGS: &[(&str, &str)] = &[
    ("GOY_API_URL", "coord.url"),
    ("GOY_ADMIN_API_KEY", "coord.admin_api_key"),
    ("GOY_DATA_DIR", "storage.data_dir"),
    ("GOY_RELAY_URL", "relay.url"),
    ("GOY_MESH_LISTEN", "mesh.listen"),
    ("GOY_METRICS_LISTEN", "metrics.listen"),
    ("RUST_LOG", "log.level"),
];

fn apply_env_overrides(
    config: &mut GoyNodeConfig,
    _file_source: &ConfigSource,
    sources: &mut HashMap<String, ConfigSource>,
    warnings: &mut Vec<String>,
) {
    for (env_var, field) in ENV_VAR_MAPPINGS {
        if let Ok(value) = std::env::var(env_var) {
            if value.trim().is_empty() {
                continue;
            }

            // Warning de deprecation
            let warning = format!(
                "⚠️  {} is deprecated. Use [{}] in config.toml or the corresponding CLI flag.",
                env_var,
                field.split('.').next().unwrap_or(field)
            );
            warn!("{}", warning);
            warnings.push(warning);

            // Só aplicar se o campo ainda tem valor default
            // (config.toml tem prioridade sobre env vars)
            if !sources.contains_key(*field)
                || matches!(sources.get(*field), Some(ConfigSource::Default))
            {
                apply_env_value(config, env_var, &value);
                sources.insert(field.to_string(), ConfigSource::EnvVar(env_var.to_string()));
            }
        }
    }
}

fn apply_env_value(config: &mut GoyNodeConfig, env_var: &str, value: &str) {
    match env_var {
        "GOY_API_URL" => config.coord.url = value.to_string(),
        "GOY_ADMIN_API_KEY" => config.coord.admin_api_key = value.to_string(),
        "GOY_DATA_DIR" => config.storage.data_dir = PathBuf::from(value),
        "GOY_RELAY_URL" => config.relay.url = value.to_string(),
        "GOY_MESH_LISTEN" => config.mesh.listen = value.to_string(),
        "GOY_METRICS_LISTEN" => config.metrics.listen = value.to_string(),
        "RUST_LOG" => config.log.level = value.to_string(),
        _ => {}
    }
}

fn apply_cli_overrides(
    config: &mut GoyNodeConfig,
    opts: &ResolveOptions,
    sources: &mut HashMap<String, ConfigSource>,
) {
    if let Some(ref url) = opts.coord_url {
        config.coord.url = url.clone();
        sources.insert(
            "coord.url".to_string(),
            ConfigSource::CliFlag("coord-url".to_string()),
        );
    }

    if let Some(ref key) = opts.admin_api_key {
        config.coord.admin_api_key = key.clone();
        sources.insert(
            "coord.admin_api_key".to_string(),
            ConfigSource::CliFlag("admin-api-key".to_string()),
        );
    }

    if let Some(ref dir) = opts.data_dir {
        config.storage.data_dir = dir.clone();
        sources.insert(
            "storage.data_dir".to_string(),
            ConfigSource::CliFlag("data-dir".to_string()),
        );
    }

    if let Some(ref level) = opts.log_level {
        config.log.level = level.clone();
        sources.insert(
            "log.level".to_string(),
            ConfigSource::CliFlag("log-level".to_string()),
        );
    }

    if let Some(ref format) = opts.log_format {
        config.log.format = format.clone();
        sources.insert(
            "log.format".to_string(),
            ConfigSource::CliFlag("log-format".to_string()),
        );
    }
}

fn mark_all_fields_as_source(sources: &mut HashMap<String, ConfigSource>, source: ConfigSource) {
    let fields = [
        "coord.url",
        "coord.admin_api_key",
        "coord.heartbeat_interval_secs",
        "coord.request_timeout_secs",
        "relay.url",
        "relay.import_cmd",
        "mesh.listen",
        "mesh.seeds",
        "mesh.registry_url",
        "mesh.heartbeat_secs",
        "mesh.tls_enabled",
        "storage.data_dir",
        "storage.extra_contribution_gb",
        "metrics.listen",
        "log.level",
        "log.format",
    ];
    for f in fields {
        sources.insert(f.to_string(), source.clone());
    }
}

fn log_resolved_config(config: &GoyNodeConfig, sources: &HashMap<String, ConfigSource>) {
    info!("═══ Resolved Configuration ═══");

    let fields: Vec<(&str, String)> = vec![
        ("coord.url", config.coord.url.clone()),
        (
            "coord.admin_api_key",
            mask_secret(&config.coord.admin_api_key),
        ),
        (
            "coord.heartbeat_interval_secs",
            config.coord.heartbeat_interval_secs.to_string(),
        ),
        ("relay.url", config.relay.url.clone()),
        ("mesh.listen", config.mesh.listen.clone()),
        ("mesh.tls_enabled", config.mesh.tls_enabled.to_string()),
        (
            "storage.data_dir",
            config.storage.data_dir.display().to_string(),
        ),
        (
            "storage.extra_contribution_gb",
            config.storage.extra_contribution_gb.to_string(),
        ),
        ("metrics.listen", config.metrics.listen.clone()),
        ("log.level", config.log.level.clone()),
        ("log.format", config.log.format.clone()),
    ];

    for (field, value) in fields {
        let source = sources
            .get(field)
            .map(|s| s.to_string())
            .unwrap_or_else(|| "implicit default".to_string());
        info!("  {field} = \"{value}\" ← {source}");
    }

    info!("═══════════════════════════════");
}

/// Mascara secrets para logging: mostra primeiros 4 chars + ****
pub fn mask_secret(s: &str) -> String {
    if s.len() <= 4 {
        "****".to_string()
    } else {
        format!("{}****", &s[..4])
    }
}

/// Retorna um GoyNodeConfig com todos os defaults preenchidos.
/// Usado quando não existe config.toml.
pub(crate) fn default_goy_node_config() -> GoyNodeConfig {
    GoyNodeConfig {
        coord: super::schema::CoordConfig {
            url: String::new(),           // Obrigatório — deve ser fornecido
            admin_api_key: String::new(), // Obrigatório
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
