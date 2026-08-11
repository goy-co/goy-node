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

/// Response devolvida pela API ao registar o nó.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRegisterResponse {
    pub node_id: String,
    pub vpn_auth_key: Option<String>,
    pub vpn_control_url: Option<String>,
    #[serde(default)]
    pub provider: Option<String>,
    pub bearer_token: String,
    pub message: String,
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
                vpn_auth_key: Some(format!("tskey-auth-{auth_key}-mock")),
                vpn_control_url: Some("https://headscale.goyco.xyz".to_string()),
                provider: Some("headscale".to_string()),
                bearer_token: "goy_bearer_mock_token_999".to_string(),
                message: "Mock registration successful".to_string(),
            });
        }

        let endpoint = format!("{}/v1/nodes/register", self.base_url);
        let req_body = NodeRegisterRequest {
            auth_key: auth_key.to_string(),
            node_id: node_id.map(|s| s.to_string()),
            os: std::env::consts::OS.to_string(),
        };

        info!("🌐 Connecting to Goy Company API at {endpoint}...");
        let resp = self.http.post(&endpoint).json(&req_body).send().await?;

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
        assert!(res.vpn_auth_key.unwrap().contains("gc_test_key_12345"));
        assert_eq!(res.provider, Some("headscale".to_string()));
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
        assert_eq!(res1.provider, Some("tailscale".to_string()));

        let json_headscale = r#"{
            "node_id": "node-hs-1",
            "vpn_auth_key": "hskey-auth-123",
            "vpn_control_url": "https://hs.goyco.xyz",
            "provider": "headscale",
            "bearer_token": "token-456",
            "message": "ok"
        }"#;
        let res2: NodeRegisterResponse = serde_json::from_str(json_headscale)?;
        assert_eq!(res2.provider, Some("headscale".to_string()));

        let json_legacy = r#"{
            "node_id": "node-legacy",
            "vpn_auth_key": "key-789",
            "vpn_control_url": "https://hs.goyco.xyz",
            "bearer_token": "token-789",
            "message": "ok"
        }"#;
        let res3: NodeRegisterResponse = serde_json::from_str(json_legacy)?;
        assert_eq!(res3.provider, None);

        Ok(())
    }
}
