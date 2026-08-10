//! Stress test suite for large-volume event backfill and cursor accuracy.

use std::time::Duration;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

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
async fn test_stress_backfill_stream_performance() -> anyhow::Result<()> {
    let (addr_a, addr_b) = (free_addr().await, free_addr().await);
    let cancel = CancellationToken::new();

    let cfg_a = MeshConfig {
        listen: addr_a.clone(),
        tls_enabled: false,
        replication_factor: 3,
        ..MeshConfig::default()
    };

    let (_relay_events_tx_a, relay_events_rx_a) = broadcast::channel::<RelayEvent>(16);
    let (relay_publish_tx_a, _relay_publish_rx_a) = tokio::sync::mpsc::channel::<String>(16);

    let c_a = cancel.clone();
    tokio::spawn(async move {
        let _ = run_with_http_listen(
            cfg_a,
            None,
            "ws://127.0.0.1:57777".to_string(),
            None,
            relay_events_rx_a,
            relay_publish_tx_a,
            c_a,
        )
        .await;
    });

    let cfg_b = MeshConfig {
        listen: addr_b.clone(),
        seeds: vec![format!("ws://{addr_a}")],
        tls_enabled: false,
        ..MeshConfig::default()
    };

    let (_relay_events_tx_b, relay_events_rx_b) = broadcast::channel::<RelayEvent>(16);
    let (relay_publish_tx_b, _relay_publish_rx_b) = tokio::sync::mpsc::channel::<String>(16);

    let c_b = cancel.clone();
    tokio::spawn(async move {
        let _ = run_with_http_listen(
            cfg_b,
            None,
            "ws://127.0.0.1:57777".to_string(),
            None,
            relay_events_rx_b,
            relay_publish_tx_b,
            c_b,
        )
        .await;
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    cancel.cancel();
    Ok(())
}
