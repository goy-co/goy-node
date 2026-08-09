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
    /// Intervalo de descoberta periódica de peers em segundos (default: 60s)
    #[serde(default = "default_discovery")]
    pub discovery_secs: u64,
    /// Endereço acessível na VPN / rede para publicitar no registry (opcional override)
    pub mesh_url: Option<String>,
    /// ID único do nó no mesh/registry (opcional override)
    pub node_id: Option<String>,
    /// Fator de replicação N-of-M (default: 3). 0 = desativar replicação ativa
    #[serde(default = "default_replication_factor")]
    pub replication_factor: u32,
    /// Limite de eventos por segundo por peer (default: 50)
    #[serde(default = "default_max_events_per_sec")]
    pub max_events_per_second_per_peer: u32,
    /// Limite de bytes por segundo por peer (default: 1MB = 1048576)
    #[serde(default = "default_max_bytes_per_sec")]
    pub max_bytes_per_second_per_peer: u64,
    /// Tamanho máximo de mensagem recebida em bytes (default: 512KB = 524288)
    #[serde(default = "default_max_msg_size")]
    pub max_message_size: usize,
}

fn default_heartbeat() -> u64 {
    30
}

fn default_discovery() -> u64 {
    60
}

fn default_replication_factor() -> u32 {
    3
}

fn default_max_events_per_sec() -> u32 {
    50
}

fn default_max_bytes_per_sec() -> u64 {
    1_048_576
}

fn default_max_msg_size() -> usize {
    524_288
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

        if let Ok(secs_raw) = std::env::var("GOY_NODE_MESH_DISCOVERY_SECS") {
            if let Ok(secs) = secs_raw.parse::<u64>() {
                info!("🔧 Override from env GOY_NODE_MESH_DISCOVERY_SECS: {secs}");
                self.mesh.discovery_secs = secs;
            }
        }

        if let Ok(url) = std::env::var("GOY_NODE_MESH_URL") {
            info!("🔧 Override from env GOY_NODE_MESH_URL: {url}");
            self.mesh.mesh_url = Some(url);
        }

        if let Ok(id) = std::env::var("GOY_NODE_ID") {
            info!("🔧 Override from env GOY_NODE_ID: {id}");
            self.mesh.node_id = Some(id);
        }

        if let Ok(rf_raw) = std::env::var("GOY_NODE_REPLICATION_FACTOR") {
            if let Ok(rf) = rf_raw.parse::<u32>() {
                info!("🔧 Override from env GOY_NODE_REPLICATION_FACTOR: {rf}");
                self.mesh.replication_factor = rf;
            }
        }

        if let Ok(v_raw) = std::env::var("GOY_NODE_MAX_EVENTS_PER_SEC") {
            if let Ok(v) = v_raw.parse::<u32>() {
                info!("🔧 Override from env GOY_NODE_MAX_EVENTS_PER_SEC: {v}");
                self.mesh.max_events_per_second_per_peer = v;
            }
        }

        if let Ok(v_raw) = std::env::var("GOY_NODE_MAX_BYTES_PER_SEC") {
            if let Ok(v) = v_raw.parse::<u64>() {
                info!("🔧 Override from env GOY_NODE_MAX_BYTES_PER_SEC: {v}");
                self.mesh.max_bytes_per_second_per_peer = v;
            }
        }

        if let Ok(v_raw) = std::env::var("GOY_NODE_MAX_MSG_SIZE") {
            if let Ok(v) = v_raw.parse::<usize>() {
                info!("🔧 Override from env GOY_NODE_MAX_MSG_SIZE: {v}");
                self.mesh.max_message_size = v;
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

        // 5. Valida discovery_secs
        if self.mesh.discovery_secs == 0 {
            anyhow::bail!("Invalid mesh.discovery_secs: must be greater than 0");
        }

        Ok(())
    }
}

impl Default for MeshConfig {
    fn default() -> Self {
        Self {
            listen: "0.0.0.0:8443".to_string(),
            seeds: vec![],
            registry_url: None,
            heartbeat_secs: default_heartbeat(),
            discovery_secs: default_discovery(),
            mesh_url: None,
            node_id: None,
            replication_factor: default_replication_factor(),
            max_events_per_second_per_peer: default_max_events_per_sec(),
            max_bytes_per_second_per_peer: default_max_bytes_per_sec(),
            max_message_size: default_max_msg_size(),
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
            mesh: MeshConfig::default(),
        }
    }
}

/// Tenta detectar o endereço do nó na VPN (Tailscale) ou interface local.
/// Se `override_url` for fornecido (config ou env var), este tem sempre prioridade.
pub fn detect_mesh_url(listen_addr: &str, override_url: Option<&str>) -> String {
    if let Some(url) = override_url {
        let trimmed = url.trim();
        if !trimmed.is_empty() {
            info!("🌐 Mesh URL manually set: {trimmed}");
            return trimmed.to_string();
        }
    }

    let port = listen_addr
        .parse::<SocketAddr>()
        .map(|a| a.port())
        .unwrap_or(8443);

    // 1. Tentar Tailscale status --json
    if let Ok(output) = std::process::Command::new("tailscale")
        .args(["status", "--json"])
        .output()
    {
        if output.status.success() {
            if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&output.stdout) {
                if let Some(self_node) = json.get("Self") {
                    if let Some(dns_name) = self_node.get("DNSName").and_then(|v| v.as_str()) {
                        let clean_dns = dns_name.trim_end_matches('.');
                        if !clean_dns.is_empty() {
                            let url = format!("ws://{clean_dns}:{port}");
                            info!("🌐 Mesh URL auto-detected via Tailscale MagicDNS: {url}");
                            return url;
                        }
                    }
                    if let Some(ips) = self_node.get("TailscaleIPs").and_then(|v| v.as_array()) {
                        for ip in ips {
                            if let Some(ip_str) = ip.as_str() {
                                if ip_str.starts_with("100.") {
                                    let url = format!("ws://{ip_str}:{port}");
                                    info!("🌐 Mesh URL auto-detected via Tailscale IP: {url}");
                                    return url;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // 2. Fallback: IP não-loopback da interface de rede (via UDP probe)
    if let Ok(socket) = std::net::UdpSocket::bind("0.0.0.0:0") {
        if socket.connect("1.1.1.1:80").is_ok() {
            if let Ok(addr) = socket.local_addr() {
                let ip = addr.ip();
                if !ip.is_loopback() {
                    let url = format!("ws://{ip}:{port}");
                    info!("🌐 Mesh URL auto-detected via local interface IP: {url}");
                    return url;
                }
            }
        }
    }

    let fallback_url = format!("ws://127.0.0.1:{port}");
    info!("🌐 Mesh URL auto-detected fallback: {fallback_url}");
    fallback_url
}

#[cfg(test)]
mod tests {
    use super::*;

    static ENV_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn test_load_or_generate_creates_default_config_file() -> anyhow::Result<()> {
        let _lock = ENV_MUTEX.lock().unwrap();
        unsafe {
            std::env::remove_var("GOY_NODE_RELAY_URL");
            std::env::remove_var("GOY_NODE_MESH_LISTEN");
            std::env::remove_var("GOY_NODE_MESH_SEEDS");
            std::env::remove_var("GOY_NODE_MESH_HEARTBEAT_SECS");
            std::env::remove_var("GOY_NODE_MESH_DISCOVERY_SECS");
            std::env::remove_var("GOY_NODE_MESH_URL");
            std::env::remove_var("GOY_NODE_ID");
        }

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
    fn test_validation_fails_on_zero_discovery() {
        let mut cfg = Config::default();
        cfg.mesh.discovery_secs = 0;
        let res = cfg.validate();
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("must be greater than 0"));
    }

    #[test]
    fn test_detect_mesh_url_override() {
        let url = detect_mesh_url("0.0.0.0:8443", Some("ws://goy-node-custom:8443"));
        assert_eq!(url, "ws://goy-node-custom:8443");
    }

    #[test]
    fn test_detect_mesh_url_fallback() {
        let url = detect_mesh_url("0.0.0.0:9443", None);
        assert!(url.starts_with("ws://"));
        assert!(url.ends_with(":9443"));
    }

    #[test]
    fn test_env_override_precedence() {
        let _lock = ENV_MUTEX.lock().unwrap();
        unsafe {
            std::env::set_var("GOY_NODE_RELAY_URL", "ws://10.0.0.99:9999");
            std::env::set_var("GOY_NODE_MESH_LISTEN", "127.0.0.1:9443");
            std::env::set_var("GOY_NODE_MESH_SEEDS", "ws://seed1:8443, ws://seed2:8443");
            std::env::set_var("GOY_NODE_MESH_HEARTBEAT_SECS", "15");
            std::env::set_var("GOY_NODE_MESH_DISCOVERY_SECS", "45");
            std::env::set_var("GOY_NODE_MESH_URL", "ws://override.tailnet:8443");
            std::env::set_var("GOY_NODE_ID", "node-override-id");
            std::env::set_var("GOY_NODE_REPLICATION_FACTOR", "5");
        }

        let mut cfg = Config::default();
        cfg.apply_env_overrides();

        assert_eq!(cfg.relay.url, "ws://10.0.0.99:9999");
        assert_eq!(cfg.mesh.listen, "127.0.0.1:9443");
        assert_eq!(cfg.mesh.seeds, vec!["ws://seed1:8443", "ws://seed2:8443"]);
        assert_eq!(cfg.mesh.heartbeat_secs, 15);
        assert_eq!(cfg.mesh.discovery_secs, 45);
        assert_eq!(cfg.mesh.mesh_url, Some("ws://override.tailnet:8443".to_string()));
        assert_eq!(cfg.mesh.node_id, Some("node-override-id".to_string()));
        assert_eq!(cfg.mesh.replication_factor, 5);
        assert!(cfg.validate().is_ok());

        unsafe {
            std::env::remove_var("GOY_NODE_RELAY_URL");
            std::env::remove_var("GOY_NODE_MESH_LISTEN");
            std::env::remove_var("GOY_NODE_MESH_SEEDS");
            std::env::remove_var("GOY_NODE_MESH_HEARTBEAT_SECS");
            std::env::remove_var("GOY_NODE_MESH_DISCOVERY_SECS");
            std::env::remove_var("GOY_NODE_MESH_URL");
            std::env::remove_var("GOY_NODE_ID");
            std::env::remove_var("GOY_NODE_REPLICATION_FACTOR");
        }
    }
}
