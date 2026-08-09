use std::path::Path;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub relay: RelayConfig,
    pub mesh: MeshConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RelayConfig {
    /// WebSocket URL do relay local (strfry)
    pub url: String,
    /// Comando opcional para importação em massa (ex: "strfry import")
    pub import_cmd: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MeshConfig {
    /// Endereço onde o mesh agent escuta peers
    pub listen: String,
    /// Seeds conhecidos para bootstrap
    #[serde(default)]
    pub seeds: Vec<String>,
    /// URL do registry central (opcional)
    pub registry_url: Option<String>,
    /// Heartbeat interval em segundos
    #[serde(default = "default_heartbeat")]
    pub heartbeat_secs: u64,
}

fn default_heartbeat() -> u64 {
    30
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        if path.exists() {
            let contents = std::fs::read_to_string(path)?;
            let cfg: Config = toml::from_str(&contents)?;
            Ok(cfg)
        } else {
            tracing::warn!(
                "Config file not found at {}, using defaults",
                path.display()
            );
            Ok(Config::default())
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            relay: RelayConfig {
                url: "ws://127.0.0.1:7777".to_string(),
                import_cmd: None,
            },
            mesh: MeshConfig {
                listen: "0.0.0.0:8443".to_string(),
                seeds: vec![],
                registry_url: None,
                heartbeat_secs: default_heartbeat(),
            },
        }
    }
}
