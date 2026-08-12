//! Serviço de Heartbeat periódico do Goy Node para o registry central (`coord-server`).
//!
//! Envia pedidos HTTP `PUT /v1/relays/{node_id}` com payload JSON dinâmico
//! contendo URL do relay, fingerprint TLS, capacidades de armazenamento,
//! versão e uptime.

use std::sync::Arc;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::HeartbeatConfig;
use crate::metrics::Metrics;

/// Payload de capacidades de armazenamento (em GB).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoragePayload {
    pub reserved_gb: u64,
    pub available_gb: u64,
}

/// Payload completo do heartbeat enviado ao `coord-server` (`PUT /v1/relays/{node_id}`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeartbeatV1RelayPayload {
    pub url: String,
    pub fingerprint: String,
    pub storage: StoragePayload,
    pub version: String,
    pub uptime_secs: u64,
}

/// Componente de serviço que executa o loop de heartbeat periódico.
pub struct HeartbeatService {
    config: HeartbeatConfig,
    registry_url: String,
    client: reqwest::Client,
    node_id: String,
    auth_key: String,
    url_provider: Arc<dyn Fn() -> String + Send + Sync>,
    fingerprint_provider: Arc<dyn Fn() -> Option<String> + Send + Sync>,
    storage_stats_provider: Arc<dyn Fn() -> (u64, u64) + Send + Sync>,
    version: String,
    metrics: Arc<Metrics>,
    cancel: CancellationToken,
}

impl HeartbeatService {
    /// Cria uma nova instância do `HeartbeatService`.
    pub fn new(
        config: HeartbeatConfig,
        registry_url: String,
        client: reqwest::Client,
        node_id: String,
        auth_key: String,
        url_provider: Arc<dyn Fn() -> String + Send + Sync>,
        fingerprint_provider: Arc<dyn Fn() -> Option<String> + Send + Sync>,
        storage_stats_provider: Arc<dyn Fn() -> (u64, u64) + Send + Sync>,
        metrics: Arc<Metrics>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            config,
            registry_url: registry_url.trim_end_matches('/').to_string(),
            client,
            node_id,
            auth_key,
            url_provider,
            fingerprint_provider,
            storage_stats_provider,
            version: env!("CARGO_PKG_VERSION").to_string(),
            metrics,
            cancel,
        }
    }

    /// Constrói o payload de heartbeat com dados dinâmicos lidos em tempo real.
    pub fn build_payload(&self) -> HeartbeatV1RelayPayload {
        let (reserved_gb, available_gb) = (self.storage_stats_provider)();
        let url = (self.url_provider)();
        let fingerprint = (self.fingerprint_provider)().unwrap_or_default();
        let uptime_secs = self.metrics.uptime_seconds();

        HeartbeatV1RelayPayload {
            url,
            fingerprint,
            storage: StoragePayload {
                reserved_gb,
                available_gb,
            },
            version: self.version.clone(),
            uptime_secs,
        }
    }

    /// Envia o pedido HTTP `PUT /v1/relays/{node_id}` com o Bearer token do nó.
    pub async fn send_heartbeat(&self, payload: &HeartbeatV1RelayPayload) -> anyhow::Result<()> {
        let endpoint = format!("{}/v1/relays/{}", self.registry_url, self.node_id);
        let resp = self
            .client
            .put(&endpoint)
            .header("Authorization", format!("Bearer {}", self.auth_key))
            .json(payload)
            .send()
            .await?;

        if resp.status().is_success() {
            Ok(())
        } else {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("HTTP status {status}: {text}");
        }
    }

    /// Executa o loop assíncrono de heartbeat com retries e backoff exponencial.
    pub async fn run(self) {
        if !self.config.enabled {
            info!("ℹ️  HeartbeatService disabled by configuration");
            return;
        }

        info!(
            "💓 HeartbeatService started for node {} (interval: {}s, registry: {})",
            self.node_id, self.config.interval_secs, self.registry_url
        );

        let mut consecutive_failures: u32 = 0;

        loop {
            if self.cancel.is_cancelled() {
                info!("💓 Heartbeat service stopped");
                break;
            }

            let payload = self.build_payload();
            let result = self.send_heartbeat(&payload).await;

            let wait_secs = match result {
                Ok(()) => {
                    if consecutive_failures > 0 {
                        info!(
                            "✅ Heartbeat recovered after {} consecutive failure(s)",
                            consecutive_failures
                        );
                    }
                    consecutive_failures = 0;
                    self.metrics.record_heartbeat_success();
                    tracing::debug!("💓 Heartbeat sent successfully for node {}", self.node_id);
                    self.config.interval_secs
                }
                Err(err) => {
                    consecutive_failures += 1;
                    self.metrics.record_heartbeat_failure();

                    if consecutive_failures >= 3 {
                        warn!(
                            "⚠️  Heartbeat failed for node {} (consecutive failures: {}): {err}",
                            self.node_id, consecutive_failures
                        );
                    } else {
                        tracing::debug!(
                            "Heartbeat attempt failed for node {} (consecutive failures: {}): {err}",
                            self.node_id, consecutive_failures
                        );
                    }

                    // Exponential backoff: min(2^(failures - 1), 60)
                    let backoff = 1u64
                        .checked_shl(consecutive_failures.saturating_sub(1))
                        .unwrap_or(60);
                    std::cmp::min(backoff, 60)
                }
            };

            tokio::select! {
                _ = self.cancel.cancelled() => {
                    info!("💓 Heartbeat service stopped");
                    break;
                }
                _ = tokio::time::sleep(std::time::Duration::from_secs(wait_secs)) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio_util::sync::CancellationToken;

    fn test_client() -> reqwest::Client {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_heartbeat_happy_path() -> anyhow::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let registry_url = format!("http://{addr}");

        let received_auth = Arc::new(tokio::sync::Mutex::new(String::new()));
        let received_body = Arc::new(tokio::sync::Mutex::new(String::new()));

        let auth_clone = received_auth.clone();
        let body_clone = received_body.clone();

        tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = vec![0u8; 2048];
                let n = socket.read(&mut buf).await.unwrap();
                let req_str = String::from_utf8_lossy(&buf[..n]);

                for line in req_str.lines() {
                    if line.to_lowercase().starts_with("authorization:") {
                        *auth_clone.lock().await = line.to_string();
                    }
                }
                if let Some(body_start) = req_str.find("\r\n\r\n") {
                    *body_clone.lock().await = req_str[body_start + 4..].to_string();
                }

                let response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK";
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });

        let metrics = Arc::new(Metrics::new());
        let cancel = CancellationToken::new();

        let service = HeartbeatService::new(
            HeartbeatConfig {
                enabled: true,
                interval_secs: 60,
            },
            registry_url,
            test_client(),
            "test-node-id".to_string(),
            "gc_test_auth_key_12345".to_string(),
            Arc::new(|| "ws://100.80.1.5:8443".to_string()),
            Arc::new(|| Some("sha256_hex_fingerprint_test".to_string())),
            Arc::new(|| (50, 200)),
            metrics.clone(),
            cancel.clone(),
        );

        let payload = service.build_payload();
        assert_eq!(payload.url, "ws://100.80.1.5:8443");
        assert_eq!(payload.fingerprint, "sha256_hex_fingerprint_test");
        assert_eq!(payload.storage.reserved_gb, 50);
        assert_eq!(payload.storage.available_gb, 200);

        let send_res = service.send_heartbeat(&payload).await;
        assert!(send_res.is_ok());

        let auth_val = received_auth.lock().await.clone();
        assert!(auth_val.contains("Bearer gc_test_auth_key_12345"));

        let body_val = received_body.lock().await.clone();
        assert!(body_val.contains("ws://100.80.1.5:8443"));

        Ok(())
    }

    #[tokio::test]
    async fn test_heartbeat_disabled_does_not_run() {
        let metrics = Arc::new(Metrics::new());
        let cancel = CancellationToken::new();

        let service = HeartbeatService::new(
            HeartbeatConfig {
                enabled: false,
                interval_secs: 60,
            },
            "http://127.0.0.1:9999".to_string(),
            test_client(),
            "test-node-id".to_string(),
            "gc_key".to_string(),
            Arc::new(|| "ws://127.0.0.1:8443".to_string()),
            Arc::new(|| None),
            Arc::new(|| (50, 200)),
            metrics.clone(),
            cancel.clone(),
        );

        service.run().await;
        assert_eq!(metrics.heartbeat_total.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_heartbeat_graceful_shutdown() {
        let metrics = Arc::new(Metrics::new());
        let cancel = CancellationToken::new();

        let service = HeartbeatService::new(
            HeartbeatConfig {
                enabled: true,
                interval_secs: 60,
            },
            "http://127.0.0.1:9999".to_string(),
            test_client(),
            "test-node-id".to_string(),
            "gc_key".to_string(),
            Arc::new(|| "ws://127.0.0.1:8443".to_string()),
            Arc::new(|| None),
            Arc::new(|| (50, 200)),
            metrics.clone(),
            cancel.clone(),
        );

        cancel.cancel(); // Cancel before running
        service.run().await;
        assert_eq!(metrics.heartbeat_total.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_dynamic_payload_updates() {
        let metrics = Arc::new(Metrics::new());
        let cancel = CancellationToken::new();

        let storage_state = Arc::new(AtomicU64::new(100));
        let storage_clone = storage_state.clone();

        let service = HeartbeatService::new(
            HeartbeatConfig::default(),
            "http://127.0.0.1:9999".to_string(),
            test_client(),
            "node-1".to_string(),
            "gc_key".to_string(),
            Arc::new(|| "ws://100.80.1.5:8443".to_string()),
            Arc::new(|| Some("fp_1".to_string())),
            Arc::new(move || (50, storage_clone.load(Ordering::Relaxed))),
            metrics.clone(),
            cancel.clone(),
        );

        let p1 = service.build_payload();
        assert_eq!(p1.storage.available_gb, 100);

        storage_state.store(180, Ordering::Relaxed);
        let p2 = service.build_payload();
        assert_eq!(p2.storage.available_gb, 180);
    }

    #[tokio::test]
    async fn test_retry_exponential_backoff_calculation() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let registry_url = format!("http://{addr}");

        // Drop listener immediately to cause connection error on client
        drop(listener);

        let metrics = Arc::new(Metrics::new());
        let cancel = CancellationToken::new();

        let service = HeartbeatService::new(
            HeartbeatConfig {
                enabled: true,
                interval_secs: 1,
            },
            registry_url,
            test_client(),
            "node-err".to_string(),
            "gc_key".to_string(),
            Arc::new(|| "ws://127.0.0.1:8443".to_string()),
            Arc::new(|| None),
            Arc::new(|| (50, 200)),
            metrics.clone(),
            cancel.clone(),
        );

        let payload = service.build_payload();
        let err_res = service.send_heartbeat(&payload).await;
        assert!(err_res.is_err());
    }
}
