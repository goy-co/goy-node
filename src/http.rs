//! Servidor HTTP leve para observabilidade do Goy Node.
//!
//! Responde em `127.0.0.1:9090` (ou `metrics.listen`) com três endpoints:
//!
//! | Endpoint | Resposta | Content-Type |
//! |----------|----------|--------------|
//! | `GET /metrics` | Prometheus text format v0.0.4 | `text/plain; version=0.0.4` |
//! | `GET /health`  | `{"status":"ok","peers":N,"uptime":X}` (200) ou `{"status":"degraded",...}` (503) | `application/json` |
//! | `GET /info`    | versão, node_id, fingerprint, config summary | `application/json` |
//!
//! Não expõe nada para a mesh/VPN — liga apenas em localhost e não há routing
//! de peers para o serviço. Shutdown gracioso via `CancellationToken`.
//!
//! Não usamos `hyper`/`axum` para evitar dependências pesadas; basta-nos um
//! parser HTTP/1.1 manual mínimo (sem parsing de body, sem keep-alive), porque
//! os clientes (Prometheus scrape, curl, liveness probe) pedem uma foto e fecham.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::metrics::Metrics;

/// Informação estática do nó exposta em `GET /info`.
#[derive(Clone)]
pub struct NodeInfo {
    pub version: String,
    pub node_id: String,
    pub cert_fingerprint: Option<String>,
    pub relay_url: String,
    pub mesh_listen: String,
    pub replication_factor: u32,
    pub tls_enabled: bool,
}

/// Inicia o servidor HTTP de observabilidade e bloqueia até shutdown.
///
/// Liga **apenas** em `listen_addr` (esperado: `127.0.0.1:9090`), e recusa
/// conexões de outras interfaces — verificar [start_http_server] para o
/// modo robusto que valida isto.
pub async fn run_http_server(
    listen_addr: String,
    metrics: Arc<Metrics>,
    node_info: NodeInfo,
    mesh_state: Option<Arc<crate::mesh::MeshState>>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(&listen_addr).await?;
    info!("📊 Metrics/health HTTP server listening on http://{listen_addr}");

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("📊 HTTP server shutting down");
                break;
            }
            res = listener.accept() => {
                match res {
                    Ok((stream, peer)) => {
                        if !peer.ip().is_loopback() {
                            warn!("⚠️ HTTP server rejected non-loopback connection from {peer}");
                            continue;
                        }
                        let m = metrics.clone();
                        let info = node_info.clone();
                        let st = mesh_state.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(stream, m, info, st).await {
                                debug!("HTTP connection error: {e}");
                            }
                        });
                    }
                    Err(e) => error!("❌ HTTP accept error: {e}"),
                }
            }
        }
    }
    Ok(())
}

/// Timeout de leitura para um request: 5s é farto para um `curl` local.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Handler de uma conexão HTTP/1.1. Lê só a request line + headers (sem body),
/// responde uma vez e fecha — sem keep-alive, sem chunked, sem pipelining.
async fn handle_connection(
    stream: tokio::net::TcpStream,
    metrics: Arc<Metrics>,
    node_info: NodeInfo,
    mesh_state: Option<Arc<crate::mesh::MeshState>>,
) -> anyhow::Result<()> {
    let mut reader = BufReader::new(stream);
    let mut request_line = String::new();
    // Lê a primeira linha; ignora cabeçalhos seguintes.
    match tokio::time::timeout(READ_TIMEOUT, reader.read_line(&mut request_line)).await {
        Ok(Ok(0)) => return Ok(()), // conexão fechada pelo cliente — silencioso
        Ok(Ok(_)) => {}
        Ok(Err(e)) => return Err(e.into()),
        Err(_) => return Ok(()), // timeout — cliente silencioso, fecha
    }
    // Esvazia headers até à linha em branco, com limite de 64 linhas.
    for _ in 0..64 {
        let mut hdr = String::new();
        match reader.read_line(&mut hdr).await {
            Ok(0) | Ok(_) if hdr.trim().is_empty() => break,
            Ok(_) => {}
            Err(_) => break,
        }
    }
    // Pega o stream de volta para escrever a resposta.
    let mut stream = reader.into_inner();

    let route = request_line.split_whitespace().nth(1).unwrap_or("/");
    debug!("HTTP request: {request_line:?} -> route {route:?}");

    let (status, content_type, body) = match route {
        "/metrics" => {
            let body = metrics.render_prometheus();
            (
                200,
                "text/plain; version=0.0.4; charset=utf-8",
                body.into_bytes(),
            )
        }
        "/health" => {
            let peers = metrics.peers_connected();
            let uptime = metrics.uptime_seconds();
            let (status, status_str) = if peers > 0 {
                (200u16, "ok")
            } else {
                (503, "degraded")
            };
            let body = format!(r#"{{"status":"{status_str}","peers":{peers},"uptime":{uptime}}}"#);
            (status, "application/json", body.into_bytes())
        }
        "/peers" => {
            let peers_vec = mesh_state
                .as_ref()
                .map(|s| s.get_peer_sessions())
                .unwrap_or_default();
            let body = serde_json::to_string(&peers_vec).unwrap_or_else(|_| "[]".to_string());
            (200, "application/json", body.into_bytes())
        }
        "/info" => {
            let fp = node_info.cert_fingerprint.as_deref().unwrap_or("null");
            let fp_str = if fp == "null" {
                "null".to_string()
            } else {
                format!("\"{fp}\"")
            };
            let body = format!(
                r#"{{"version":"{}","node_id":"{}","cert_fingerprint":{},"relay_url":"{}","mesh_listen":"{}","replication_factor":{},"tls_enabled":{}}}"#,
                escape_json(&node_info.version),
                escape_json(&node_info.node_id),
                fp_str,
                escape_json(&node_info.relay_url),
                escape_json(&node_info.mesh_listen),
                node_info.replication_factor,
                node_info.tls_enabled
            );
            (200, "application/json", body.into_bytes())
        }
        _ => (404, "text/plain", b"404 Not Found\n".to_vec()),
    };

    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        503 => "Service Unavailable",
        _ => "OK",
    };
    let header = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;
    let _ = stream.shutdown().await;
    Ok(())
}

/// Escapa aspas e barras invertidas em strings JSON.
fn escape_json(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{EventSource, Metrics};

    /// Reserva uma porta efémera em loopback e devolve-a já libertada.
    async fn free_addr() -> anyhow::Result<std::net::SocketAddr> {
        let l = TcpListener::bind("127.0.0.1:0").await?;
        let addr = l.local_addr()?;
        drop(l);
        Ok(addr)
    }

    async fn http_get(listen: &str, path: &str) -> anyhow::Result<(u16, String)> {
        let mut stream = None;
        for _ in 0..30 {
            if let Ok(s) = tokio::net::TcpStream::connect(listen).await {
                stream = Some(s);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let mut stream = stream.ok_or_else(|| anyhow::anyhow!("failed to connect to {listen}"))?;
        let req = format!("GET {path} HTTP/1.1\r\nHost: {listen}\r\nConnection: close\r\n\r\n");
        tokio::io::AsyncWriteExt::write_all(&mut stream, req.as_bytes()).await?;

        let mut resp = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut stream, &mut resp).await?;

        let status_code = resp
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse::<u16>().ok())
            .unwrap_or(0);

        let body = resp
            .split_once("\r\n\r\n")
            .map(|(_, b)| b.to_string())
            .unwrap_or(resp);
        Ok((status_code, body))
    }

    #[tokio::test]
    async fn test_metrics_endpoint_serves_prometheus_text() -> anyhow::Result<()> {
        let addr = free_addr().await?;
        let listen = format!("127.0.0.1:{}", addr.port());
        let metrics = Arc::new(Metrics::new());
        metrics.inc_events_received(EventSource::Relay);
        metrics
            .events_replicated
            .fetch_add(42, std::sync::atomic::Ordering::Relaxed);

        let node_info = NodeInfo {
            version: "test".to_string(),
            node_id: "node-test".to_string(),
            cert_fingerprint: Some("deadbeef".to_string()),
            relay_url: "ws://localhost:7777".to_string(),
            mesh_listen: "0.0.0.0:8443".to_string(),
            replication_factor: 3,
            tls_enabled: true,
        };
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let listen_clone = listen.clone();
        let m = metrics.clone();
        let ni = node_info.clone();
        tokio::spawn(async move {
            let _ = run_http_server(listen_clone, m, ni, None, cancel_clone).await;
        });

        let (status, body) = http_get(&listen, "/metrics").await?;
        assert_eq!(status, 200);

        for expected in [
            "# TYPE goy_events_received_total counter",
            "goy_events_received_total{source=\"relay\"} 1",
            "# TYPE goy_events_replicated_total counter",
            "goy_events_replicated_total 42",
        ] {
            assert!(
                body.contains(expected),
                "metrics output missing {expected:?}, got:\n{body}"
            );
        }

        cancel.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn test_health_endpoint_returns_json_and_status() -> anyhow::Result<()> {
        let addr = free_addr().await?;
        let listen = format!("127.0.0.1:{}", addr.port());
        let metrics = Arc::new(Metrics::new());
        metrics.set_peers_connected(2);

        let node_info = NodeInfo {
            version: "test".to_string(),
            node_id: "node-test".to_string(),
            cert_fingerprint: None,
            relay_url: "ws://localhost:7777".to_string(),
            mesh_listen: "0.0.0.0:8443".to_string(),
            replication_factor: 3,
            tls_enabled: true,
        };
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let listen_clone = listen.clone();
        let m = metrics.clone();
        let ni = node_info.clone();
        tokio::spawn(async move {
            let _ = run_http_server(listen_clone, m, ni, None, cancel_clone).await;
        });

        let (status, body) = http_get(&listen, "/health").await?;
        assert_eq!(status, 200, "expected 200 with peers>0, got: {status}");
        assert!(
            body.contains(r#""status":"ok""#),
            "expected status ok in: {body}"
        );
        assert!(body.contains(r#""peers":2"#), "expected peers=2 in: {body}");

        cancel.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn test_health_endpoint_503_when_no_peers() -> anyhow::Result<()> {
        let addr = free_addr().await?;
        let listen = format!("127.0.0.1:{}", addr.port());
        let metrics = Arc::new(Metrics::new());
        let node_info = NodeInfo {
            version: "test".to_string(),
            node_id: "n".to_string(),
            cert_fingerprint: None,
            relay_url: "ws://localhost:7777".to_string(),
            mesh_listen: "0.0.0.0:8443".to_string(),
            replication_factor: 3,
            tls_enabled: true,
        };
        let cancel = CancellationToken::new();
        let c = cancel.clone();
        let l = listen.clone();
        let m = metrics.clone();
        let ni = node_info.clone();
        tokio::spawn(async move {
            let _ = run_http_server(l, m, ni, None, c).await;
        });

        let (status, body) = http_get(&listen, "/health").await?;
        assert_eq!(status, 503, "expected 503 with 0 peers, got: {status}");
        assert!(body.contains(r#""status":"degraded""#));

        cancel.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn test_info_endpoint_contains_node_metadata() -> anyhow::Result<()> {
        let addr = free_addr().await?;
        let listen = format!("127.0.0.1:{}", addr.port());
        let metrics = Arc::new(Metrics::new());
        let node_info = NodeInfo {
            version: "0.1.0-test".to_string(),
            node_id: "node-xyz".to_string(),
            cert_fingerprint: Some("ABC123".to_string()),
            relay_url: "ws://relay.local:7777".to_string(),
            mesh_listen: "0.0.0.0:8443".to_string(),
            replication_factor: 5,
            tls_enabled: true,
        };
        let cancel = CancellationToken::new();
        let c = cancel.clone();
        let l = listen.clone();
        let m = metrics.clone();
        let ni = node_info.clone();
        tokio::spawn(async move {
            let _ = run_http_server(l, m, ni, None, c).await;
        });

        let (status, body) = http_get(&listen, "/info").await?;
        assert_eq!(status, 200);

        for expected in [
            r#""version":"0.1.0-test""#,
            r#""node_id":"node-xyz""#,
            r#""cert_fingerprint":"ABC123""#,
            r#""relay_url":"ws://relay.local:7777""#,
            r#""replication_factor":5"#,
            r#""tls_enabled":true"#,
        ] {
            assert!(
                body.contains(expected),
                "info endpoint missing {expected:?} in: {body}"
            );
        }

        cancel.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn test_unknown_route_returns_404() -> anyhow::Result<()> {
        let addr = free_addr().await?;
        let listen = format!("127.0.0.1:{}", addr.port());
        let metrics = Arc::new(Metrics::new());
        let node_info = NodeInfo {
            version: "t".to_string(),
            node_id: "t".to_string(),
            cert_fingerprint: None,
            relay_url: "ws://x".to_string(),
            mesh_listen: "0.0.0.0:9".to_string(),
            replication_factor: 1,
            tls_enabled: false,
        };
        let cancel = CancellationToken::new();
        let c = cancel.clone();
        let l = listen.clone();
        let m = metrics.clone();
        let ni = node_info.clone();
        tokio::spawn(async move {
            let _ = run_http_server(l, m, ni, None, c).await;
        });

        let (status, _body) = http_get(&listen, "/admin").await?;
        assert_eq!(status, 404, "expected 404, got: {status}");

        cancel.cancel();
        Ok(())
    }
}
