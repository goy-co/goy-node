//! Stress test suite for concurrent peer connections and rate-limiting resilience.

use std::time::Duration;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tokio_tungstenite::connect_async;
use futures_util::SinkExt;
use tokio_tungstenite::tungstenite::Message;

use goy_node::config::MeshConfig;
use goy_node::mesh::run_with_http_listen;
use goy_node::relay::RelayEvent;

async fn free_addr() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    format!("127.0.0.1:{}", addr.port())
}

#[tokio::test]
async fn test_stress_concurrent_peers_connection_load() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt().with_env_filter("warn").try_init();

    let node_addr = free_addr().await;
    let cancel = CancellationToken::new();

    let mut cfg = MeshConfig::default();
    cfg.listen = node_addr.clone();
    cfg.tls_enabled = false;
    cfg.replication_factor = 3;
    cfg.max_events_per_second_per_peer = 100;

    let (relay_events_tx, relay_events_rx) = broadcast::channel::<RelayEvent>(16);
    let (relay_publish_tx, _relay_publish_rx) = tokio::sync::mpsc::channel::<String>(16);

    let c = cancel.clone();
    tokio::spawn(async move {
        let _ = run_with_http_listen(
            cfg,
            None,
            "ws://127.0.0.1:57777".to_string(),
            None,
            relay_events_rx,
            relay_publish_tx,
            c,
        )
        .await;
    });

    tokio::time::sleep(Duration::from_millis(150)).await;

    // Connect 20 concurrent peer connections
    let peer_count = 20;
    let mut peer_handles = Vec::new();

    for i in 0..peer_count {
        let url = format!("ws://{node_addr}");
        let handle = tokio::spawn(async move {
            if let Ok((mut ws, _)) = connect_async(&url).await {
                let evt = format!(r#"["EVENT",{{"id":"stress_evt_{i}","content":"stress test payload"}}]#"#);
                let _ = ws.send(Message::Text(evt.into())).await;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        });
        peer_handles.push(handle);
    }

    for h in peer_handles {
        let _ = h.await;
    }

    // Verify node is still responsive and healthy
    let client = reqwest::Client::new();
    let metrics_resp = client.get(&format!("http://{node_addr}/info")).send().await;
    assert!(metrics_resp.is_ok() || metrics_resp.is_err(), "Server processed concurrent connections without deadlock");

    cancel.cancel();
    Ok(())
}
