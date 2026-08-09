use std::net::SocketAddr;
use std::path::Path;

use serde::Deserialize;
use tracing::{info, warn};

pub const DEFAULT_CONFIG_TEMPLATE: &str = r#"# =====================================================================
# Goy Node Configuration
# =====================================================================

[relay]
# WebSocket URL do relay Nostr local (ex: strfry)
url = "ws://127.0.0.1:7777"

# Comando opcional para importação em massa (opcional)
# import_cmd = "strfry import"

[mesh]
# Endereço e porta onde o mesh agent escuta conexões inbound de peers
listen = "0.0.0.0:8443"

# Lista de nós seeds conhecidos para bootstrap (ex: ["ws://peer1:8443", "ws://peer2:8443"])
seeds = []

# URL do registry central de nó (opcional)
# registry_url = "https://registry.goy.company"

# Intervalo entre mensagens keepalive/heartbeat em segundos (default: 30s)
heartbeat_secs = 30
"#;

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
    /// Carrega a configuração do caminho especificado.
    /// Se o ficheiro não existir, gera um `config.toml` default com comentários explicativos.
    pub fn load_or_generate(path: &Path) -> anyhow::Result<Self> {
        if !path.exists() {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(path, DEFAULT_CONFIG_TEMPLATE)?;
            info!("📝 Generated default config at {}. Edit to customize.", path.display());
        }

        let contents = std::fs::read_to_string(path)?;
        let mut cfg: Config = toml::from_str(&contents)?;

        cfg.apply_env_overrides();
        cfg.validate()?;

        Ok(cfg)
    }

    /// Carrega configuração a partir de uma string TOML (útil para testes).
    #[allow(dead_code)]
    pub fn load_from_str(contents: &str) -> anyhow::Result<Self> {
        let mut cfg: Config = toml::from_str(contents)?;
        cfg.apply_env_overrides();
        cfg.validate()?;
        Ok(cfg)
    }

    /// Aplica substituições por variáveis de ambiente com prefixo `GOY_NODE_`.
    pub fn apply_env_overrides(&mut self) {
        if let Ok(url) = std::env::var("GOY_NODE_RELAY_URL") {
            info!("🔧 Override from env GOY_NODE_RELAY_URL: {url}");
            self.relay.url = url;
        }

        if let Ok(cmd) = std::env::var("GOY_NODE_RELAY_IMPORT_CMD") {
            info!("🔧 Override from env GOY_NODE_RELAY_IMPORT_CMD: {cmd}");
            self.relay.import_cmd = Some(cmd);
        }

        if let Ok(listen) = std::env::var("GOY_NODE_MESH_LISTEN") {
            info!("🔧 Override from env GOY_NODE_MESH_LISTEN: {listen}");
            self.mesh.listen = listen;
        }

        if let Ok(seeds_raw) = std::env::var("GOY_NODE_MESH_SEEDS") {
            let seeds: Vec<String> = seeds_raw
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            info!("🔧 Override from env GOY_NODE_MESH_SEEDS: {seeds:?}");
            self.mesh.seeds = seeds;
        }

        if let Ok(reg) = std::env::var("GOY_NODE_MESH_REGISTRY_URL") {
            info!("🔧 Override from env GOY_NODE_MESH_REGISTRY_URL: {reg}");
            self.mesh.registry_url = Some(reg);
        }

        if let Ok(secs_raw) = std::env::var("GOY_NODE_MESH_HEARTBEAT_SECS") {
            if let Ok(secs) = secs_raw.parse::<u64>() {
                info!("🔧 Override from env GOY_NODE_MESH_HEARTBEAT_SECS: {secs}");
                self.mesh.heartbeat_secs = secs;
            }
        }
    }

    /// Valida rigorosamente todos os campos da configuração.
    pub fn validate(&self) -> anyhow::Result<()> {
        // 1. Valida relay.url
        if !self.relay.url.starts_with("ws://") && !self.relay.url.starts_with("wss://") {
            anyhow::bail!(
                "Invalid relay.url '{}': must start with 'ws://' or 'wss://'",
                self.relay.url
            );
        }

        // 2. Valida mesh.listen (deve ser um SocketAddr válido ex: 0.0.0.0:8443)
        if self.mesh.listen.parse::<SocketAddr>().is_err() {
            anyhow::bail!(
                "Invalid mesh.listen '{}': must be a valid socket address (e.g. '0.0.0.0:8443')",
                self.mesh.listen
            );
        }

        // 3. Valida mesh.seeds
        for seed in &self.mesh.seeds {
            if !seed.starts_with("ws://") && !seed.starts_with("wss://") {
                anyhow::bail!(
                    "Invalid seed URL '{}': must start with 'ws://' or 'wss://'",
                    seed
                );
            }
        }

        // 4. Valida heartbeat_secs
        if self.mesh.heartbeat_secs == 0 {
            anyhow::bail!("Invalid mesh.heartbeat_secs: must be greater than 0");
        } else if self.mesh.heartbeat_secs < 5 {
            warn!(
                "⚠️  heartbeat_secs ({}) is very low, recommended minimum is 5s",
                self.mesh.heartbeat_secs
            );
        }

        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_or_generate_creates_default_config_file() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let config_path = temp_dir.path().join("sub/config.toml");

        assert!(!config_path.exists());
        let cfg = Config::load_or_generate(&config_path)?;

        assert!(config_path.exists());
        assert_eq!(cfg.relay.url, "ws://127.0.0.1:7777");
        assert_eq!(cfg.mesh.listen, "0.0.0.0:8443");
        assert_eq!(cfg.mesh.heartbeat_secs, 30);
        Ok(())
    }

    #[test]
    fn test_validation_fails_on_invalid_relay_url() {
        let mut cfg = Config::default();
        cfg.relay.url = "http://127.0.0.1:7777".to_string();
        let res = cfg.validate();
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("must start with 'ws://' or 'wss://'"));
    }

    #[test]
    fn test_validation_fails_on_invalid_listen_address() {
        let mut cfg = Config::default();
        cfg.mesh.listen = "invalid_address".to_string();
        let res = cfg.validate();
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("must be a valid socket address"));
    }

    #[test]
    fn test_validation_fails_on_invalid_seed_url() {
        let mut cfg = Config::default();
        cfg.mesh.seeds = vec!["invalid_seed_url".to_string()];
        let res = cfg.validate();
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("must start with 'ws://' or 'wss://'"));
    }

    #[test]
    fn test_validation_fails_on_zero_heartbeat() {
        let mut cfg = Config::default();
        cfg.mesh.heartbeat_secs = 0;
        let res = cfg.validate();
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("must be greater than 0"));
    }

    #[test]
    fn test_env_override_precedence() {
        unsafe {
            std::env::set_var("GOY_NODE_RELAY_URL", "ws://10.0.0.99:9999");
            std::env::set_var("GOY_NODE_MESH_LISTEN", "127.0.0.1:9443");
            std::env::set_var("GOY_NODE_MESH_SEEDS", "ws://seed1:8443, ws://seed2:8443");
            std::env::set_var("GOY_NODE_MESH_HEARTBEAT_SECS", "15");
        }

        let mut cfg = Config::default();
        cfg.apply_env_overrides();

        assert_eq!(cfg.relay.url, "ws://10.0.0.99:9999");
        assert_eq!(cfg.mesh.listen, "127.0.0.1:9443");
        assert_eq!(cfg.mesh.seeds, vec!["ws://seed1:8443", "ws://seed2:8443"]);
        assert_eq!(cfg.mesh.heartbeat_secs, 15);
        assert!(cfg.validate().is_ok());

        unsafe {
            std::env::remove_var("GOY_NODE_RELAY_URL");
            std::env::remove_var("GOY_NODE_MESH_LISTEN");
            std::env::remove_var("GOY_NODE_MESH_SEEDS");
            std::env::remove_var("GOY_NODE_MESH_HEARTBEAT_SECS");
        }
    }
}
