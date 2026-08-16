use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Configuração raiz do Goy Node.
/// Serializa para/deserializa de ~/.config/goy-node/config.toml
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoyNodeConfig {
    pub coord: CoordConfig,
    pub relay: RelayConfig,
    pub mesh: MeshConfig,
    pub storage: StorageConfig,
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub log: LogConfig,
}

/// Conexão ao coord-server.
/// NOVA SECÇÃO — não existia anteriormente.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoordConfig {
    /// URL base do coord-server (ex: "http://localhost:8080")
    pub url: String,

    /// Admin API key para autenticação.
    /// Armazenada em plain text no config.toml.
    /// Em produção, considerar integração com secret manager.
    pub admin_api_key: String,

    /// Intervalo entre heartbeats em segundos.
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_secs: u64,

    /// Timeout para requests HTTP ao coord-server em segundos.
    #[serde(default = "default_coord_timeout")]
    pub request_timeout_secs: u64,
}

pub(crate) fn default_heartbeat_interval() -> u64 {
    30
}

pub(crate) fn default_coord_timeout() -> u64 {
    10
}

/// Configuração do relay Nostr local (strfry).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelayConfig {
    /// WebSocket URL do relay local.
    #[serde(default = "default_relay_url")]
    pub url: String,

    /// Comando opcional para importação em massa.
    #[serde(default)]
    pub import_cmd: Option<String>,
}

pub(crate) fn default_relay_url() -> String {
    "ws://127.0.0.1:7777".to_string()
}

/// Configuração do mesh agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeshConfig {
    /// Endereço e porta de escuta para conexões inbound.
    #[serde(default = "default_mesh_listen")]
    pub listen: String,

    /// Seeds conhecidos para bootstrap.
    #[serde(default)]
    pub seeds: Vec<String>,

    /// URL do registry central (opcional, usa discovery via coord-server se ausente).
    #[serde(default)]
    pub registry_url: Option<String>,

    /// Intervalo de keepalive/heartbeat em segundos.
    #[serde(default = "default_mesh_heartbeat")]
    pub heartbeat_secs: u64,

    /// TLS mútuo entre peers.
    #[serde(default = "default_true")]
    pub tls_enabled: bool,

    /// Fingerprints SHA-256 pré-aprovados (TOFU bypass).
    #[serde(default)]
    pub trusted_fingerprints: std::collections::HashMap<String, String>,
}

pub(crate) fn default_mesh_listen() -> String {
    "0.0.0.0:8443".to_string()
}

pub(crate) fn default_mesh_heartbeat() -> u64 {
    30
}

pub(crate) fn default_true() -> bool {
    true
}

/// Configuração de storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    /// Diretório de dados.
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,

    /// Espaço adicional voluntário em GB acima do mínimo (50 GB).
    #[serde(default)]
    pub extra_contribution_gb: u64,
}

pub(crate) fn default_data_dir() -> PathBuf {
    PathBuf::from("/var/lib/goy-node")
}

/// Configuração de métricas/observabilidade.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsConfig {
    /// Endereço e porta do servidor HTTP de métricas.
    /// "" ou "off" para desativar.
    #[serde(default = "default_metrics_listen")]
    pub listen: String,
}

pub(crate) fn default_metrics_listen() -> String {
    "127.0.0.1:9090".to_string()
}

/// Configuração de logging.
/// NOVA SECÇÃO — anteriormente só via RUST_LOG env var.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogConfig {
    /// Nível de log: trace, debug, info, warn, error.
    #[serde(default = "default_log_level")]
    pub level: String,

    /// Formato: pretty (human-readable) ou json (machine-readable).
    #[serde(default = "default_log_format")]
    pub format: String,
}

pub(crate) fn default_log_level() -> String {
    "info".to_string()
}

pub(crate) fn default_log_format() -> String {
    "pretty".to_string()
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
        }
    }
}
