use std::time::Duration;

use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use goy_node::config::MeshConfig;
use goy_node::relay::RelayEvent;

#[tokio::test]
async fn test_outbound_seed_reconnect_and_sync() -> anyhow::Result<()> {
    let cancel = CancellationToken::new();

    // ── Node A (listening on 19446, no seeds) ──────────────────────────────────
    let (relay_events_tx_a, relay_events_rx_a) = broadcast::channel::<RelayEvent>(16);
    let (relay_publish_tx_a, mut relay_publish_rx_a) = mpsc::channel::<String>(16);

    let cfg_a = MeshConfig {
        listen: "127.0.0.1:19446".to_string(),
        seeds: vec![],
        registry_url: None,
        heartbeat_secs: 30,
    };

    let cancel_a = cancel.clone();
    tokio::spawn(async move {
        let _ = goy_node::mesh::run(cfg_a, "ws://127.0.0.1:57777".to_string(), None, relay_events_rx_a, relay_publish_tx_a, cancel_a).await;
    });

    // ── Node B (seeds = ["ws://127.0.0.1:19446"], listening on 19447) ──────────
    let (relay_events_tx_b, relay_events_rx_b) = broadcast::channel::<RelayEvent>(16);
    let (relay_publish_tx_b, mut relay_publish_rx_b) = mpsc::channel::<String>(16);

    let cfg_b = MeshConfig {
        listen: "127.0.0.1:19447".to_string(),
        seeds: vec!["ws://127.0.0.1:19446".to_string()],
        registry_url: None,
        heartbeat_secs: 30,
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

    // Node A (19451), Node B (19452, seed A), Node C (19453, seed A e seed B)
    let (relay_events_tx_a, relay_events_rx_a) = broadcast::channel::<RelayEvent>(16);
    let (relay_publish_tx_a, _relay_publish_rx_a) = mpsc::channel::<String>(16);

    let (_relay_events_tx_b, relay_events_rx_b) = broadcast::channel::<RelayEvent>(16);
    let (relay_publish_tx_b, mut relay_publish_rx_b) = mpsc::channel::<String>(16);

    let (_relay_events_tx_c, relay_events_rx_c) = broadcast::channel::<RelayEvent>(16);
    let (relay_publish_tx_c, mut relay_publish_rx_c) = mpsc::channel::<String>(16);

    let cfg_a = MeshConfig {
        listen: "127.0.0.1:19451".to_string(),
        seeds: vec![],
        registry_url: None,
        heartbeat_secs: 30,
    };
    let cfg_b = MeshConfig {
        listen: "127.0.0.1:19452".to_string(),
        seeds: vec!["ws://127.0.0.1:19451".to_string()],
        registry_url: None,
        heartbeat_secs: 30,
    };
    let cfg_c = MeshConfig {
        listen: "127.0.0.1:19453".to_string(),
        seeds: vec!["ws://127.0.0.1:19451".to_string(), "ws://127.0.0.1:19452".to_string()],
        registry_url: None,
        heartbeat_secs: 30,
    };

    let c_a = cancel.clone();
    tokio::spawn(async move { let _ = goy_node::mesh::run(cfg_a, "ws://127.0.0.1:57777".to_string(), None, relay_events_rx_a, relay_publish_tx_a, c_a).await; });
    let c_b = cancel.clone();
    tokio::spawn(async move { let _ = goy_node::mesh::run(cfg_b, "ws://127.0.0.1:57777".to_string(), None, relay_events_rx_b, relay_publish_tx_b, c_b).await; });
    let c_c = cancel.clone();
    tokio::spawn(async move { let _ = goy_node::mesh::run(cfg_c, "ws://127.0.0.1:57777".to_string(), None, relay_events_rx_c, relay_publish_tx_c, c_c).await; });

    tokio::time::sleep(Duration::from_millis(400)).await;

    // Publica um evento no Nó A
    let event_loop = r#"["EVENT","sub_loop",{"id":"loop_event_999","content":"loop check"}]"#;
    relay_events_tx_a.send(RelayEvent { raw: event_loop.to_string() })?;

    // Tanto B como C devem receber o evento exatamente uma vez
    let rec_b = tokio::time::timeout(Duration::from_secs(2), relay_publish_rx_b.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("Node B recv error"))?;
    let rec_c = tokio::time::timeout(Duration::from_secs(2), relay_publish_rx_c.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("Node C recv error"))?;

    let expected_norm = r#"["EVENT",{"id":"loop_event_999","content":"loop check"}]"#;
    assert_eq!(rec_b, expected_norm);
    assert_eq!(rec_c, expected_norm);

    // Aguarda um momento para garantir que não há loops infinitos nem mensagens repetidas
    tokio::time::sleep(Duration::from_millis(200)).await;

    cancel.cancel();
    Ok(())
}
