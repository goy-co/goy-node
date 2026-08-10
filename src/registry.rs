//! Cliente HTTP para o registry central de nós do Goy Node.
//!
//! Fornece:
//! - Registo de nó (`POST /relays`)
//! - Heartbeat periódico (`PUT /relays/{node_id}`)
//! - Deregisto gracioso (`DELETE /relays/{node_id}`)
//! - Descoberta de peers (`GET /relays`)
//! - Cache em disco para tolerância a falhas temporárias do registry

use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

/// Estrutura de informação de um nó registado no registry central.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RelayInfo {
    pub node_id: String,
    pub relay_url: String,
    pub mesh_url: String,
    pub version: String,
    pub capabilities: Vec<String>,
    /// Fingerprint SHA-256 (hex) do certificado TLS do nó. `None` quando TLS está desativado.
    #[serde(default)]
    pub cert_fingerprint: Option<String>,
    #[serde(default)]
    pub last_seen: Option<u64>,
}

/// Cliente HTTP para comunicar com a REST API do registry.
#[derive(Debug, Clone)]
pub struct RegistryClient {
    registry_url: String,
    client: reqwest::Client,
}

impl RegistryClient {
    /// Cria uma nova instância de `RegistryClient`.
    pub fn new(registry_url: String) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();
        Self {
            registry_url: registry_url.trim_end_matches('/').to_string(),
            client,
        }
    }

    /// Retorna a URL base do registry.
    #[allow(dead_code)]
    pub fn registry_url(&self) -> &str {
        &self.registry_url
    }

    /// Registar o nó no registry: `POST /relays`
    pub async fn register(&self, info: &RelayInfo) -> anyhow::Result<()> {
        let url = format!("{}/relays", self.registry_url);
        let resp = self.client.post(&url).json(info).send().await?;
        if resp.status().is_success() {
            info!(
                "📋 Node successfully registered at registry: {}",
                info.node_id
            );
            Ok(())
        } else {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("Registry POST /relays returned status {status}: {text}");
        }
    }

    /// Heartbeat no registry: `PUT /relays/{node_id}`
    pub async fn heartbeat(&self, node_id: &str) -> anyhow::Result<()> {
        let url = format!("{}/relays/{node_id}", self.registry_url);
        let resp = self.client.put(&url).send().await?;
        if resp.status().is_success() {
            tracing::debug!("💓 Registry heartbeat sent for {node_id}");
            Ok(())
        } else {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("Registry PUT /relays/{node_id} returned status {status}: {text}");
        }
    }

    /// Deregistar no shutdown: `DELETE /relays/{node_id}`
    pub async fn deregister(&self, node_id: &str) -> anyhow::Result<()> {
        let url = format!("{}/relays/{node_id}", self.registry_url);
        let resp = self.client.delete(&url).send().await?;
        if resp.status().is_success() {
            info!("👋 Node successfully deregistered from registry: {node_id}");
            Ok(())
        } else {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("Registry DELETE /relays/{node_id} returned status {status}: {text}");
        }
    }

    /// Descoberta de peers: `GET /relays`
    pub async fn fetch_relays(&self) -> anyhow::Result<Vec<RelayInfo>> {
        let url = format!("{}/relays", self.registry_url);
        let resp = self.client.get(&url).send().await?;
        if resp.status().is_success() {
            let relays: Vec<RelayInfo> = resp.json().await?;
            info!("🔍 Registry returned {} registered peers", relays.len());
            Ok(relays)
        } else {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("Registry GET /relays returned status {status}: {text}");
        }
    }
}

/// Guarda a cache local da última lista de peers do registry em `data_dir/registry_peers.json`.
pub fn save_cached_peers(data_dir: &Path, relays: &[RelayInfo]) {
    if let Err(e) = std::fs::create_dir_all(data_dir) {
        warn!(
            "⚠️  Failed to create data directory {}: {e}",
            data_dir.display()
        );
        return;
    }

    let file_path = data_dir.join("registry_peers.json");
    let tmp_path = data_dir.join("registry_peers.json.tmp");
    if let Ok(bytes) = serde_json::to_vec(relays)
        && std::fs::write(&tmp_path, bytes).is_ok()
    {
        let _ = std::fs::rename(tmp_path, file_path);
    }
}

/// Carrega a cache local da última lista de peers do registry de `data_dir/registry_peers.json`.
pub fn load_cached_peers(data_dir: &Path) -> Vec<RelayInfo> {
    let file_path = data_dir.join("registry_peers.json");
    if file_path.exists()
        && let Ok(bytes) = std::fs::read(&file_path)
        && let Ok(relays) = serde_json::from_slice::<Vec<RelayInfo>>(&bytes)
    {
        info!("💾 Loaded {} cached registry peers from disk", relays.len());
        return relays;
    }
    vec![]
}
