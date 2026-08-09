use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use goy_node::config::MeshConfig;
use goy_node::relay::RelayEvent;

#[tokio::test]
async fn test_outbound_seed_reconnect_and_sync() -> anyhow::Result<()> {
    let cancel = CancellationToken::new();

    let l_a = TcpListener::bind("127.0.0.1:0").await?;
    let addr_a = l_a.local_addr()?;
    drop(l_a);

    let l_b = TcpListener::bind("127.0.0.1:0").await?;
    let addr_b = l_b.local_addr()?;
    drop(l_b);

    // ── Node A (no seeds) ──────────────────────────────────
    let (relay_events_tx_a, relay_events_rx_a) = broadcast::channel::<RelayEvent>(16);
    let (relay_publish_tx_a, mut relay_publish_rx_a) = mpsc::channel::<String>(16);

    let cfg_a = MeshConfig {
        listen: addr_a.to_string(),
        seeds: vec![],
        ..MeshConfig::default()
    };

    let cancel_a = cancel.clone();
    tokio::spawn(async move {
        let _ = goy_node::mesh::run(cfg_a, "ws://127.0.0.1:57777".to_string(), None, relay_events_rx_a, relay_publish_tx_a, cancel_a).await;
    });

    // ── Node B (seeds = Node A) ──────────
    let (relay_events_tx_b, relay_events_rx_b) = broadcast::channel::<RelayEvent>(16);
    let (relay_publish_tx_b, mut relay_publish_rx_b) = mpsc::channel::<String>(16);

    let cfg_b = MeshConfig {
        listen: addr_b.to_string(),
        seeds: vec![format!("ws://{addr_a}")],
        ..MeshConfig::default()
    };

    let cancel_b = cancel.clone();
    tokio::spawn(async move {
        let _ = goy_node::mesh::run(cfg_b, "ws://127.0.0.1:57777".to_string(), None, relay_events_rx_b, relay_publish_tx_b, cancel_b).await;
    });

    // Aguarda o Node B fazer a conexão outbound para o Node A
    tokio::time::sleep(Duration::from_millis(350)).await;

    // 1. Transmissão do Node A -> Node B
    let event_1 = r#"["EVENT","sub_1",{"id":"e000000000000000000000000000000000000000000000000000000000000001","content":"Test A to B"}]"#;
    relay_events_tx_a.send(RelayEvent {
        raw: event_1.to_string(),
    })?;

    let received_b = tokio::time::timeout(Duration::from_secs(2), relay_publish_rx_b.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("Node B did not receive event"))?;
    assert_eq!(
        received_b,
        r#"["EVENT",{"id":"e000000000000000000000000000000000000000000000000000000000000001","content":"Test A to B"}]"#
    );

    // 2. Transmissão do Node B -> Node A
    let event_2 = r#"["EVENT","sub_2",{"id":"e000000000000000000000000000000000000000000000000000000000000002","content":"Test B to A"}]"#;
    relay_events_tx_b.send(RelayEvent {
        raw: event_2.to_string(),
    })?;

    let received_a = tokio::time::timeout(Duration::from_secs(2), relay_publish_rx_a.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("Node A did not receive event"))?;
    assert_eq!(
        received_a,
        r#"["EVENT",{"id":"e000000000000000000000000000000000000000000000000000000000000002","content":"Test B to A"}]"#
    );

    cancel.cancel();
    Ok(())
}

#[tokio::test]
async fn test_mesh_deduplication_triangle_loop() -> anyhow::Result<()> {
    let cancel = CancellationToken::new();

    let l_a = TcpListener::bind("127.0.0.1:0").await?;
    let addr_a = l_a.local_addr()?;
    drop(l_a);

    let l_b = TcpListener::bind("127.0.0.1:0").await?;
    let addr_b = l_b.local_addr()?;
    drop(l_b);

    let l_c = TcpListener::bind("127.0.0.1:0").await?;
    let addr_c = l_c.local_addr()?;
    drop(l_c);

    // Node A, Node B (seed A), Node C (seed A e seed B)
    let (relay_events_tx_a, relay_events_rx_a) = broadcast::channel::<RelayEvent>(16);
    let (relay_publish_tx_a, _relay_publish_rx_a) = mpsc::channel::<String>(16);

    let (_relay_events_tx_b, relay_events_rx_b) = broadcast::channel::<RelayEvent>(16);
    let (relay_publish_tx_b, mut relay_publish_rx_b) = mpsc::channel::<String>(16);

    let (_relay_events_tx_c, relay_events_rx_c) = broadcast::channel::<RelayEvent>(16);
    let (relay_publish_tx_c, mut relay_publish_rx_c) = mpsc::channel::<String>(16);

    let cfg_a = MeshConfig {
        listen: addr_a.to_string(),
        seeds: vec![],
        ..MeshConfig::default()
    };
    let cfg_b = MeshConfig {
        listen: addr_b.to_string(),
        seeds: vec![format!("ws://{addr_a}")],
        ..MeshConfig::default()
    };
    let cfg_c = MeshConfig {
        listen: addr_c.to_string(),
        seeds: vec![format!("ws://{addr_a}"), format!("ws://{addr_b}")],
        ..MeshConfig::default()
    };

    let c_a = cancel.clone();
    tokio::spawn(async move { let _ = goy_node::mesh::run(cfg_a, "ws://127.0.0.1:57777".to_string(), None, relay_events_rx_a, relay_publish_tx_a, c_a).await; });
    let c_b = cancel.clone();
    tokio::spawn(async move { let _ = goy_node::mesh::run(cfg_b, "ws://127.0.0.1:57777".to_string(), None, relay_events_rx_b, relay_publish_tx_b, c_b).await; });
    let c_c = cancel.clone();
    tokio::spawn(async move { let _ = goy_node::mesh::run(cfg_c, "ws://127.0.0.1:57777".to_string(), None, relay_events_rx_c, relay_publish_tx_c, c_c).await; });

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Transmit um evento a partir do Node A
    let event_loop = r#"["EVENT","sub_loop",{"id":"e000000000000000000000000000000000000000000000000000000000000099","content":"Loop test"}]"#;
    relay_events_tx_a.send(RelayEvent {
        raw: event_loop.to_string(),
    })?;

    // Node B e Node C devem receber exatamente UMA vez cada (dedup)
    let rec_b = tokio::time::timeout(Duration::from_secs(2), relay_publish_rx_b.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("Node B did not receive loop event"))?;
    let rec_c = tokio::time::timeout(Duration::from_secs(2), relay_publish_rx_c.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("Node C did not receive loop event"))?;

    let expected = r#"["EVENT",{"id":"e000000000000000000000000000000000000000000000000000000000000099","content":"Loop test"}]"#;
    assert_eq!(rec_b, expected);
    assert_eq!(rec_c, expected);

    cancel.cancel();
    Ok(())
}
