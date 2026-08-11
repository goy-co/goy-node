//! Cliente da API da Goy Company para onboarding e registo de nós na plataforma.

use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{info, warn};

/// URL base padrão da API da Goy Company.
pub const DEFAULT_GOY_API_URL: &str = "https://api.goyco.xyz";

/// Valida a chave de autenticação (auth key) fornecida pela Goy Company.
/// Uma chave válida deve começar com `gc_` e ter pelo menos 10 caracteres.
pub fn validate_auth_key(key: &str) -> bool {
    let trimmed = key.trim();
    trimmed.starts_with("gc_") && trimmed.len() >= 10
}

/// Request de registo de um novo nó na plataforma Goy Company.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRegisterRequest {
    pub auth_key: String,
    pub node_id: Option<String>,
    pub os: String,
}

/// Configuração de VPN retornada pela API no registo do nó.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VpnConfig {
    pub auth_key: String,
    #[serde(default)]
    pub control_url: Option<String>,
    #[serde(default)]
    pub provider: Option<String>,
}

/// Response devolvida pela API ao registar o nó.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRegisterResponse {
    pub node_id: String,
    #[serde(default)]
    pub vpn_config: Option<VpnConfig>,
    #[serde(default)]
    pub registry_url: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,

    // Campos de compatibilidade para respostas flat/legacy ou mocks antigos
    #[serde(default)]
    pub vpn_auth_key: Option<String>,
    #[serde(default)]
    pub vpn_control_url: Option<String>,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub bearer_token: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
}

impl NodeRegisterResponse {
    pub fn get_vpn_auth_key(&self) -> Option<String> {
        self.vpn_config
            .as_ref()
            .map(|c| c.auth_key.clone())
            .or_else(|| self.vpn_auth_key.clone())
    }

    pub fn get_vpn_control_url(&self) -> Option<String> {
        self.vpn_config
            .as_ref()
            .and_then(|c| c.control_url.clone())
            .or_else(|| self.vpn_control_url.clone())
    }

    pub fn get_vpn_provider(&self) -> Option<String> {
        self.vpn_config
            .as_ref()
            .and_then(|c| c.provider.clone())
            .or_else(|| self.provider.clone())
    }
}

/// Cliente HTTP da API Goy Company.
pub struct GoyApiClient {
    base_url: String,
    http: Client,
}

impl GoyApiClient {
    pub fn new(base_url: Option<&str>) -> Self {
        let url = base_url
            .unwrap_or(DEFAULT_GOY_API_URL)
            .trim_end_matches('/')
            .to_string();

        let http = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_default();

        Self {
            base_url: url,
            http,
        }
    }

    /// Registar nó na API da Goy Company usando a auth key.
    pub async fn register_node(
        &self,
        auth_key: &str,
        node_id: Option<&str>,
    ) -> anyhow::Result<NodeRegisterResponse> {
        if !validate_auth_key(auth_key) {
            anyhow::bail!(
                "Invalid auth key format. Key must start with 'gc_' and have at least 10 characters."
            );
        }

        // Se modo MOCK estiver ativo (env var ou ambiente de teste sem conectividade)
        if std::env::var("GOY_API_MOCK").is_ok() || self.base_url.contains("mock.local") {
            info!("⚙️ Goy API Mock mode active");
            let mock_id = node_id.unwrap_or("node-mock-12345").to_string();
            return Ok(NodeRegisterResponse {
                node_id: mock_id,
                vpn_config: Some(VpnConfig {
                    auth_key: format!("tskey-auth-{auth_key}-mock"),
                    control_url: Some("https://headscale.goyco.xyz".to_string()),
                    provider: Some("headscale".to_string()),
                }),
                registry_url: Some("https://registry.goyco.xyz".to_string()),
                created_at: Some("2026-08-11T23:57:00Z".to_string()),
                vpn_auth_key: None,
                vpn_control_url: None,
                provider: None,
                bearer_token: Some("goy_bearer_mock_token_999".to_string()),
                message: Some("Mock registration successful".to_string()),
            });
        }

        let bearer_key = std::env::var("GOY_ADMIN_API_KEY")
            .ok()
            .filter(|k| !k.trim().is_empty())
            .unwrap_or_else(|| auth_key.to_string());

        let endpoint = format!("{}/v1/nodes/register", self.base_url);
        let req_body = NodeRegisterRequest {
            auth_key: auth_key.to_string(),
            node_id: node_id.map(|s| s.to_string()),
            os: std::env::consts::OS.to_string(),
        };

        info!("🌐 Connecting to Goy Company API at {endpoint}...");
        let resp = self
            .http
            .post(&endpoint)
            .bearer_auth(bearer_key)
            .json(&req_body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let err_text = resp.text().await.unwrap_or_default();
            anyhow::bail!("Goy API registration failed ({status}): {err_text}");
        }

        let res_payload = resp.json::<NodeRegisterResponse>().await?;
        Ok(res_payload)
    }

    /// Deregistar nó da plataforma Goy Company.
    pub async fn deregister_node(&self, bearer_token: &str, node_id: &str) -> anyhow::Result<()> {
        if std::env::var("GOY_API_MOCK").is_ok() || self.base_url.contains("mock.local") {
            info!("⚙️ Goy API Mock mode active for deregistration");
            return Ok(());
        }

        let endpoint = format!("{}/v1/nodes/{}", self.base_url, node_id);
        info!("🌐 Deregistering node {node_id} from Goy Company API...");

        let resp = self
            .http
            .delete(&endpoint)
            .bearer_auth(bearer_token)
            .send()
            .await?;

        if !resp.status().is_success() {
            warn!(
                "⚠️ Goy API deregistration returned status {}",
                resp.status()
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_auth_key() {
        assert!(validate_auth_key("gc_123456789"));
        assert!(validate_auth_key("gc_abcdefghijklmn"));
        assert!(!validate_auth_key("invalid_key"));
        assert!(!validate_auth_key("gc_short")); // < 10 chars
        assert!(!validate_auth_key(""));
    }

    #[tokio::test]
    async fn test_mock_api_registration() {
        let client = GoyApiClient::new(Some("http://mock.local"));
        let res = client
            .register_node("gc_test_key_12345", Some("node-test-1"))
            .await
            .unwrap();

        assert_eq!(res.node_id, "node-test-1");
        assert!(
            res.get_vpn_auth_key()
                .unwrap()
                .contains("gc_test_key_12345")
        );
        assert_eq!(res.get_vpn_provider(), Some("headscale".to_string()));
        assert_eq!(
            res.registry_url,
            Some("https://registry.goyco.xyz".to_string())
        );
    }

    #[test]
    fn test_node_register_response_deserialization_nested_vpn_config() -> anyhow::Result<()> {
        let json_coord_server = r#"{
            "node_id": "node-cs-1",
            "vpn_config": {
                "auth_key": "tskey-auth-nested-123",
                "control_url": "https://headscale.goyco.xyz",
                "provider": "headscale"
            },
            "registry_url": "https://registry.goyco.xyz",
            "created_at": "2026-08-11T23:57:00Z"
        }"#;

        let res: NodeRegisterResponse = serde_json::from_str(json_coord_server)?;
        assert_eq!(res.node_id, "node-cs-1");
        assert_eq!(
            res.get_vpn_auth_key(),
            Some("tskey-auth-nested-123".to_string())
        );
        assert_eq!(
            res.get_vpn_control_url(),
            Some("https://headscale.goyco.xyz".to_string())
        );
        assert_eq!(res.get_vpn_provider(), Some("headscale".to_string()));
        assert_eq!(
            res.registry_url,
            Some("https://registry.goyco.xyz".to_string())
        );

        Ok(())
    }

    #[test]
    fn test_node_register_response_deserialization_provider() -> anyhow::Result<()> {
        let json_tailscale = r#"{
            "node_id": "node-ts-1",
            "vpn_auth_key": "tskey-auth-123",
            "vpn_control_url": null,
            "provider": "tailscale",
            "bearer_token": "token-123",
            "message": "ok"
        }"#;
        let res1: NodeRegisterResponse = serde_json::from_str(json_tailscale)?;
        assert_eq!(res1.get_vpn_provider(), Some("tailscale".to_string()));

        let json_headscale = r#"{
            "node_id": "node-hs-1",
            "vpn_auth_key": "hskey-auth-123",
            "vpn_control_url": "https://hs.goyco.xyz",
            "provider": "headscale",
            "bearer_token": "token-456",
            "message": "ok"
        }"#;
        let res2: NodeRegisterResponse = serde_json::from_str(json_headscale)?;
        assert_eq!(res2.get_vpn_provider(), Some("headscale".to_string()));

        let json_legacy = r#"{
            "node_id": "node-legacy",
            "vpn_auth_key": "key-789",
            "vpn_control_url": "https://hs.goyco.xyz",
            "bearer_token": "token-789",
            "message": "ok"
        }"#;
        let res3: NodeRegisterResponse = serde_json::from_str(json_legacy)?;
        assert_eq!(res3.get_vpn_provider(), None);

        Ok(())
    }

    #[tokio::test]
    async fn test_goy_admin_api_key_env_var_override() {
        unsafe {
            std::env::set_var("GOY_ADMIN_API_KEY", "admin_secret_key_123");
        }
        let client = GoyApiClient::new(Some("http://mock.local"));
        let res = client
            .register_node("gc_test_key_12345", Some("node-admin-test"))
            .await;
        assert!(res.is_ok());

        unsafe {
            std::env::remove_var("GOY_ADMIN_API_KEY");
        }
    }
}
