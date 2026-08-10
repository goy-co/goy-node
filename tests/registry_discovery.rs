use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use goy_node::config::MeshConfig;
use goy_node::registry::{RegistryClient, RelayInfo};
use goy_node::relay::RelayEvent;

/// Server HTTP Mock simples para simular a REST API do Registry central.
pub struct MockRegistry {
    pub url: String,
    pub relays: Arc<Mutex<HashMap<String, RelayInfo>>>,
    pub cancel: CancellationToken,
}

impl MockRegistry {
    pub async fn start() -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let url = format!("http://{addr}");
        let relays: Arc<Mutex<HashMap<String, RelayInfo>>> = Arc::new(Mutex::new(HashMap::new()));
        let cancel = CancellationToken::new();

        let relays_clone = relays.clone();
        let cancel_clone = cancel.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_clone.cancelled() => break,
                    res = listener.accept() => {
                        let Ok((mut socket, _)) = res else { break };
                        let relays = relays_clone.clone();

                        tokio::spawn(async move {
                            let mut buf = [0u8; 4096];
                            let Ok(n) = socket.read(&mut buf).await else { return };
                            let req_str = String::from_utf8_lossy(&buf[..n]);

                            let mut lines = req_str.lines();
                            let first_line = lines.next().unwrap_or_default();
                            let parts: Vec<&str> = first_line.split_whitespace().collect();
                            if parts.len() < 2 {
                                return;
                            }
                            let method = parts[0];
                            let path = parts[1];

                            if method == "POST" && path == "/relays" {
                                if let Some(body_idx) = req_str.find("\r\n\r\n") {
                                    let body = &req_str[body_idx + 4..];
                                    if let Ok(info) = serde_json::from_str::<RelayInfo>(body) {
                                        relays.lock().unwrap().insert(info.node_id.clone(), info);
                                        let response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK";
                                        let _ = socket.write_all(response.as_bytes()).await;
                                        return;
                                    }
                                }
                                let response = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n";
                                let _ = socket.write_all(response.as_bytes()).await;
                            } else if method == "PUT" && path.starts_with("/relays/") {
                                let node_id = path.trim_start_matches("/relays/").to_string();
                                if let Some(relay) = relays.lock().unwrap().get_mut(&node_id) {
                                    relay.last_seen = Some(chrono::Utc::now().timestamp() as u64);
                                }
                                let response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK";
                                let _ = socket.write_all(response.as_bytes()).await;
                            } else if method == "DELETE" && path.starts_with("/relays/") {
                                let node_id = path.trim_start_matches("/relays/").to_string();
                                relays.lock().unwrap().remove(&node_id);
                                let response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK";
                                let _ = socket.write_all(response.as_bytes()).await;
                            } else if method == "GET" && path == "/relays" {
                                let list: Vec<RelayInfo> = relays.lock().unwrap().values().cloned().collect();
                                let json = serde_json::to_string(&list).unwrap_or_default();
                                let response = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                                    json.len(),
                                    json
                                );
                                let _ = socket.write_all(response.as_bytes()).await;
                            } else {
                                let response = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
                                let _ = socket.write_all(response.as_bytes()).await;
                            }
                        });
                    }
                }
            }
        });

        Ok(Self {
            url,
            relays,
            cancel,
        })
    }

    pub fn stop(&self) {
        self.cancel.cancel();
    }
}

// ── 1. Teste Unitário do Cliente Registry HTTP ──────────────────────────────────
#[tokio::test]
async fn test_registry_client_http_ops() -> anyhow::Result<()> {
    let mock = MockRegistry::start().await?;
    let client = RegistryClient::new(mock.url.clone());

    let info = RelayInfo {
        node_id: "node-test-1".to_string(),
        relay_url: "ws://127.0.0.1:7777".to_string(),
        mesh_url: "ws://127.0.0.1:19500".to_string(),
        version: "0.1.0".to_string(),
        capabilities: vec!["nostr".to_string(), "mesh".to_string()],
        cert_fingerprint: None,
        last_seen: None,
    };

    // 1. Register POST /relays
    client.register(&info).await?;
    assert_eq!(mock.relays.lock().unwrap().len(), 1);

    // 2. Fetch GET /relays
    let fetched = client.fetch_relays().await?;
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].node_id, "node-test-1");

    // 3. Heartbeat PUT /relays/{node_id}
    client.heartbeat("node-test-1").await?;
    assert!(
        mock.relays
            .lock()
            .unwrap()
            .get("node-test-1")
            .unwrap()
            .last_seen
            .is_some()
    );

    // 4. Deregister DELETE /relays/{node_id}
    client.deregister("node-test-1").await?;
    assert_eq!(mock.relays.lock().unwrap().len(), 0);

    mock.stop();
    Ok(())
}

// ── 2. Teste de Integração: 3 nós + registry (Arranca C -> descobre A e B -> full mesh) ──
#[tokio::test]
async fn test_three_nodes_dynamic_discovery_full_mesh() -> anyhow::Result<()> {
    let cancel = CancellationToken::new();
    let mock = MockRegistry::start().await?;

    let l_a = TcpListener::bind("127.0.0.1:0").await?;
    let addr_a = l_a.local_addr()?;
    drop(l_a);

    let l_b = TcpListener::bind("127.0.0.1:0").await?;
    let addr_b = l_b.local_addr()?;
    drop(l_b);

    let l_c = TcpListener::bind("127.0.0.1:0").await?;
    let addr_c = l_c.local_addr()?;
    drop(l_c);

    // Node A
    let (_relay_events_tx_a, relay_events_rx_a) = broadcast::channel::<RelayEvent>(16);
    let (relay_publish_tx_a, mut relay_publish_rx_a) = mpsc::channel::<String>(16);
    let cfg_a = MeshConfig {
        listen: addr_a.to_string(),
        seeds: vec![],
        registry_url: Some(mock.url.clone()),
        heartbeat_secs: 10,
        discovery_secs: 1,
        mesh_url: Some(format!("ws://{addr_a}")),
        node_id: Some("node-A".to_string()),
        ..MeshConfig::default()
    };

    let c_a = cancel.clone();
    tokio::spawn(async move {
        let _ = goy_node::mesh::run(
            cfg_a,
            "ws://127.0.0.1:57777".to_string(),
            None,
            relay_events_rx_a,
            relay_publish_tx_a,
            c_a,
        )
        .await;
    });

    // Node B
    let (_relay_events_tx_b, relay_events_rx_b) = broadcast::channel::<RelayEvent>(16);
    let (relay_publish_tx_b, mut relay_publish_rx_b) = mpsc::channel::<String>(16);
    let cfg_b = MeshConfig {
        listen: addr_b.to_string(),
        seeds: vec![],
        registry_url: Some(mock.url.clone()),
        heartbeat_secs: 10,
        discovery_secs: 1,
        mesh_url: Some(format!("ws://{addr_b}")),
        node_id: Some("node-B".to_string()),
        ..MeshConfig::default()
    };

    let c_b = cancel.clone();
    tokio::spawn(async move {
        let _ = goy_node::mesh::run(
            cfg_b,
            "ws://127.0.0.1:57777".to_string(),
            None,
            relay_events_rx_b,
            relay_publish_tx_b,
            c_b,
        )
        .await;
    });

    // Aguarda Node A e Node B registarem no registry
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Node C arranca
    let (relay_events_tx_c, relay_events_rx_c) = broadcast::channel::<RelayEvent>(16);
    let (relay_publish_tx_c, _relay_publish_rx_c) = mpsc::channel::<String>(16);
    let cfg_c = MeshConfig {
        listen: addr_c.to_string(),
        seeds: vec![],
        registry_url: Some(mock.url.clone()),
        heartbeat_secs: 10,
        discovery_secs: 1,
        mesh_url: Some(format!("ws://{addr_c}")),
        node_id: Some("node-C".to_string()),
        ..MeshConfig::default()
    };

    let c_c = cancel.clone();
    tokio::spawn(async move {
        let _ = goy_node::mesh::run(
            cfg_c,
            "ws://127.0.0.1:57777".to_string(),
            None,
            relay_events_rx_c,
            relay_publish_tx_c,
            c_c,
        )
        .await;
    });

    // Aguarda Node C descobrir A e B via registry e conectar automaticamente
    tokio::time::sleep(Duration::from_millis(2500)).await;

    // Transmissão de evento vindo de Node C -> deve chegar a Node A e Node B
    let event_from_c = r#"["EVENT","sub_c",{"id":"evt_from_c_999","content":"Hello from Node C"}]"#;
    relay_events_tx_c.send(RelayEvent {
        raw: event_from_c.to_string(),
    })?;

    let rec_a = tokio::time::timeout(Duration::from_secs(5), relay_publish_rx_a.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("Node A did not receive event from Node C"))?;
    let rec_b = tokio::time::timeout(Duration::from_secs(5), relay_publish_rx_b.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("Node B did not receive event from Node C"))?;

    let expected = r#"["EVENT",{"id":"evt_from_c_999","content":"Hello from Node C"}]"#;
    assert_eq!(rec_a, expected);
    assert_eq!(rec_b, expected);

    cancel.cancel();
    mock.stop();
    Ok(())
}

// ── 3. Teste de Resiliência: Registry cai -> operação continua -> Registry volta ──────────
#[tokio::test]
async fn test_registry_resilience_outage_and_recovery() -> anyhow::Result<()> {
    let cancel = CancellationToken::new();
    let temp_dir = tempfile::tempdir()?;
    let data_dir_a = temp_dir.path().join("node_a");

    let mock = MockRegistry::start().await?;

    let l_a = TcpListener::bind("127.0.0.1:0").await?;
    let addr_a = l_a.local_addr()?;
    drop(l_a);

    let l_b = TcpListener::bind("127.0.0.1:0").await?;
    let addr_b = l_b.local_addr()?;
    drop(l_b);

    // Node A
    let (relay_events_tx_a, relay_events_rx_a) = broadcast::channel::<RelayEvent>(16);
    let (relay_publish_tx_a, _relay_publish_rx_a) = mpsc::channel::<String>(16);
    let cfg_a = MeshConfig {
        listen: addr_a.to_string(),
        seeds: vec![],
        registry_url: Some(mock.url.clone()),
        heartbeat_secs: 10,
        discovery_secs: 1,
        mesh_url: Some(format!("ws://{addr_a}")),
        node_id: Some("node-A-resilient".to_string()),
        ..MeshConfig::default()
    };

    let c_a = cancel.clone();
    let dir_a1 = data_dir_a.clone();
    tokio::spawn(async move {
        let _ = goy_node::mesh::run(
            cfg_a,
            "ws://127.0.0.1:57777".to_string(),
            Some(dir_a1),
            relay_events_rx_a,
            relay_publish_tx_a,
            c_a,
        )
        .await;
    });

    // Node B
    let (_relay_events_tx_b, relay_events_rx_b) = broadcast::channel::<RelayEvent>(16);
    let (relay_publish_tx_b, mut relay_publish_rx_b) = mpsc::channel::<String>(16);
    let cfg_b = MeshConfig {
        listen: addr_b.to_string(),
        seeds: vec![],
        registry_url: Some(mock.url.clone()),
        heartbeat_secs: 10,
        discovery_secs: 1,
        mesh_url: Some(format!("ws://{addr_b}")),
        node_id: Some("node-B-resilient".to_string()),
        ..MeshConfig::default()
    };

    let c_b = cancel.clone();
    let dir_a2 = data_dir_a.clone();
    tokio::spawn(async move {
        let _ = goy_node::mesh::run(
            cfg_b,
            "ws://127.0.0.1:57777".to_string(),
            Some(dir_a2),
            relay_events_rx_b,
            relay_publish_tx_b,
            c_b,
        )
        .await;
    });

    // Aguarda descobrirem-se
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Desliga o registry (simula queda do servidor)
    mock.stop();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Eventos continuam a fluir na mesh normalmente mesmo sem registry
    let live_evt = r#"["EVENT","sub_live",{"id":"evt_resilience_1","content":"mesh operational"}]"#;
    relay_events_tx_a.send(RelayEvent {
        raw: live_evt.to_string(),
    })?;

    let rec_b = tokio::time::timeout(Duration::from_secs(3), relay_publish_rx_b.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("Node B did not receive event during registry outage"))?;
    assert_eq!(
        rec_b,
        r#"["EVENT",{"id":"evt_resilience_1","content":"mesh operational"}]"#
    );

    cancel.cancel();
    Ok(())
}
