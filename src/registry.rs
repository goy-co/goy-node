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

/// Metadata de capacidade de armazenamento reservada e disponível (em GB) reportada ao registry central.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageMetadata {
    pub reserved_gb: u64,
    pub available_gb: u64,
}

impl StorageMetadata {
    /// Converte `StorageInfo` para `StorageMetadata`.
    pub fn from_info(info: &crate::storage::StorageInfo) -> Self {
        Self {
            reserved_gb: info.total_reserved_gb,
            available_gb: info.available_gb,
        }
    }

    /// Converte bytes para `StorageMetadata` em GB.
    pub fn from_bytes(reserved_bytes: u64, available_bytes: u64) -> Self {
        Self {
            reserved_gb: reserved_bytes / (1024 * 1024 * 1024),
            available_gb: available_bytes / (1024 * 1024 * 1024),
        }
    }
}

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
    /// Capacidade de armazenamento do nó (em GB). Omitido se indisponível.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<StorageMetadata>,
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
    pub async fn heartbeat(
        &self,
        node_id: &str,
        storage: Option<StorageMetadata>,
    ) -> anyhow::Result<()> {
        let url = format!("{}/relays/{node_id}", self.registry_url);
        let req = if let Some(ref st) = storage {
            self.client
                .put(&url)
                .json(&serde_json::json!({ "storage": st }))
        } else {
            self.client.put(&url)
        };
        let resp = req.send().await?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_metadata_conversions() {
        let info = crate::storage::StorageInfo {
            total_reserved_gb: 150,
            available_gb: 234,
            used_gb: 12,
            filesystem_path: std::path::PathBuf::from("/var/lib/goy-node"),
        };
        let meta = StorageMetadata::from_info(&info);
        assert_eq!(meta.reserved_gb, 150);
        assert_eq!(meta.available_gb, 234);

        let meta_bytes = StorageMetadata::from_bytes(161_061_273_600, 251_327_098_880);
        assert_eq!(meta_bytes.reserved_gb, 150);
        assert_eq!(meta_bytes.available_gb, 234);
    }

    #[test]
    fn test_relay_info_serialization_with_and_without_storage() -> anyhow::Result<()> {
        let info_with_storage = RelayInfo {
            node_id: "node-1".to_string(),
            relay_url: "ws://127.0.0.1:7777".to_string(),
            mesh_url: "ws://127.0.0.1:8443".to_string(),
            version: "0.1.0".to_string(),
            capabilities: vec!["mesh".to_string()],
            cert_fingerprint: None,
            last_seen: None,
            storage: Some(StorageMetadata {
                reserved_gb: 150,
                available_gb: 234,
            }),
        };

        let json_str = serde_json::to_string(&info_with_storage)?;
        assert!(json_str.contains(r#""storage":{"reserved_gb":150,"available_gb":234}"#));

        let info_no_storage = RelayInfo {
            node_id: "node-2".to_string(),
            relay_url: "ws://127.0.0.1:7777".to_string(),
            mesh_url: "ws://127.0.0.1:8443".to_string(),
            version: "0.1.0".to_string(),
            capabilities: vec!["mesh".to_string()],
            cert_fingerprint: None,
            last_seen: None,
            storage: None,
        };

        let json_str_no_storage = serde_json::to_string(&info_no_storage)?;
        assert!(!json_str_no_storage.contains("storage"));

        Ok(())
    }

    #[test]
    fn test_legacy_relay_info_deserialization_without_storage_field() -> anyhow::Result<()> {
        let legacy_json = r#"{
            "node_id": "legacy-node",
            "relay_url": "ws://127.0.0.1:7777",
            "mesh_url": "ws://127.0.0.1:8443",
            "version": "0.1.0",
            "capabilities": ["mesh"]
        }"#;

        let parsed: RelayInfo = serde_json::from_str(legacy_json)?;
        assert_eq!(parsed.node_id, "legacy-node");
        assert_eq!(parsed.storage, None);

        Ok(())
    }
}
