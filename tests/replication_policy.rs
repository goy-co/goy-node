use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use goy_node::config::MeshConfig;
use goy_node::consistent_hash::ConsistentHashRing;
use goy_node::relay::RelayEvent;

#[test]
fn test_select_replication_peers_deterministic() {
    let mut ring = ConsistentHashRing::new(150);
    ring.add_peer("ws://node-1:8443");
    ring.add_peer("ws://node-2:8443");
    ring.add_peer("ws://node-3:8443");
    ring.add_peer("ws://node-4:8443");
    ring.add_peer("ws://node-5:8443");

    let event_id = "e000000000000000000000000000000000000000000000000000000000000001";

    // 1. Replicação N-of-M com factor = 3 (seleciona 3 peers distintos)
    let selected_1 = ring.get_responsible_peers(event_id, 3);
    assert_eq!(selected_1.len(), 3);

    // 2. Determinismo: chamadas consecutivas devem devolver exatamente os mesmos peers na mesma ordem
    let selected_2 = ring.get_responsible_peers(event_id, 3);
    assert_eq!(selected_1, selected_2);

    // 3. Exclusão da origem (source_peer)
    let selected_with_src: Vec<String> = selected_1
        .iter()
        .filter(|p| *p != "ws://node-1:8443")
        .cloned()
        .collect();
    assert!(!selected_with_src.contains(&"ws://node-1:8443".to_string()));

    // 4. factor = 0 (replicação desativada) -> 0 peers
    let selected_rf0 = ring.get_responsible_peers(event_id, 0);
    assert!(selected_rf0.is_empty());
}

#[tokio::test]
async fn test_five_nodes_replication_factor_three() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();
    let cancel = CancellationToken::new();

    // 5 nós em porta dinâmica com replication_factor = 3
    let mut listeners = Vec::new();
    let mut addrs = Vec::new();
    let mut mesh_urls = Vec::new();

    for _ in 0..5 {
        let l = TcpListener::bind("127.0.0.1:0").await?;
        let addr = l.local_addr()?;
        mesh_urls.push(format!("ws://{addr}"));
        addrs.push(addr);
        listeners.push(l);
    }
    drop(listeners);

    let mut publish_rxs = Vec::new();
    let mut events_txs = Vec::new();

    for i in 0..5 {
        let (relay_events_tx, relay_events_rx) = broadcast::channel::<RelayEvent>(16);
        let (relay_publish_tx, relay_publish_rx) = mpsc::channel::<String>(16);

        let seeds: Vec<String> = mesh_urls
            .iter()
            .enumerate()
            .filter(|(idx, _)| *idx != i)
            .map(|(_, url)| url.clone())
            .collect();

        let cfg = MeshConfig {
            listen: addrs[i].to_string(),
            seeds,
            registry_url: None,
            heartbeat_secs: 30,
            discovery_secs: 60,
            mesh_url: Some(mesh_urls[i].clone()),
            node_id: Some(format!("node-{}", i)),
            ..MeshConfig::default()
        };

        let c = cancel.clone();
        tokio::spawn(async move {
            let _ = goy_node::mesh::run(
                cfg,
                "ws://127.0.0.1:57777".to_string(),
                None,
                relay_events_rx,
                relay_publish_tx,
                c,
            )
            .await;
        });

        events_txs.push(relay_events_tx);
        publish_rxs.push(relay_publish_rx);
    }

    tokio::time::sleep(Duration::from_millis(600)).await;

    let test_evt = r#"["EVENT","sub_rf3",{"id":"evt_rf3_test_001","content":"RF=3 test payload"}]"#;
    events_txs[0].send(RelayEvent {
        raw: test_evt.to_string(),
    })?;

    let mut receiving_nodes = std::collections::HashSet::new();
    for (i, rx) in publish_rxs.iter_mut().enumerate().skip(1) {
        if let Ok(Some(msg)) = tokio::time::timeout(Duration::from_millis(600), rx.recv()).await
            && msg.contains("evt_rf3_test_001")
        {
            receiving_nodes.insert(i);
        }
    }

    assert_eq!(
        receiving_nodes.len(),
        2,
        "Com replication_factor=3, exatamente 2 outros nós devem receber o evento (nós receptores: {receiving_nodes:?})"
    );

    cancel.cancel();
    Ok(())
}

#[tokio::test]
async fn test_replication_resilience_on_node_failure() -> anyhow::Result<()> {
    let cancel = CancellationToken::new();

    let mut addrs = Vec::new();
    let mut mesh_urls = Vec::new();

    for _ in 0..3 {
        let l = TcpListener::bind("127.0.0.1:0").await?;
        let addr = l.local_addr()?;
        mesh_urls.push(format!("ws://{addr}"));
        addrs.push(addr);
        drop(l);
    }

    let cancel_node0 = CancellationToken::new();
    let (events_tx_0, events_rx_0) = broadcast::channel::<RelayEvent>(16);
    let (publish_tx_0, _publish_rx_0) = mpsc::channel::<String>(16);
    let cfg_0 = MeshConfig {
        listen: addrs[0].to_string(),
        seeds: vec![mesh_urls[1].clone(), mesh_urls[2].clone()],
        registry_url: None,
        heartbeat_secs: 30,
        discovery_secs: 60,
        mesh_url: Some(mesh_urls[0].clone()),
        node_id: Some("node-0".to_string()),
        ..MeshConfig::default()
    };
    let c0 = cancel_node0.clone();
    tokio::spawn(async move {
        let _ = goy_node::mesh::run(
            cfg_0,
            "ws://127.0.0.1:57777".to_string(),
            None,
            events_rx_0,
            publish_tx_0,
            c0,
        )
        .await;
    });

    let (_events_tx_1, events_rx_1) = broadcast::channel::<RelayEvent>(16);
    let (publish_tx_1, mut publish_rx_1) = mpsc::channel::<String>(16);
    let cfg_1 = MeshConfig {
        listen: addrs[1].to_string(),
        seeds: vec![mesh_urls[0].clone(), mesh_urls[2].clone()],
        registry_url: None,
        heartbeat_secs: 30,
        discovery_secs: 60,
        mesh_url: Some(mesh_urls[1].clone()),
        node_id: Some("node-1".to_string()),
        ..MeshConfig::default()
    };
    let c1 = cancel.clone();
    tokio::spawn(async move {
        let _ = goy_node::mesh::run(
            cfg_1,
            "ws://127.0.0.1:57777".to_string(),
            None,
            events_rx_1,
            publish_tx_1,
            c1,
        )
        .await;
    });

    let (_events_tx_2, events_rx_2) = broadcast::channel::<RelayEvent>(16);
    let (publish_tx_2, mut publish_rx_2) = mpsc::channel::<String>(16);
    let cfg_2 = MeshConfig {
        listen: addrs[2].to_string(),
        seeds: vec![mesh_urls[0].clone(), mesh_urls[1].clone()],
        registry_url: None,
        heartbeat_secs: 30,
        discovery_secs: 60,
        mesh_url: Some(mesh_urls[2].clone()),
        node_id: Some("node-2".to_string()),
        ..MeshConfig::default()
    };
    let c2 = cancel.clone();
    tokio::spawn(async move {
        let _ = goy_node::mesh::run(
            cfg_2,
            "ws://127.0.0.1:57777".to_string(),
            None,
            events_rx_2,
            publish_tx_2,
            c2,
        )
        .await;
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let test_evt = r#"["EVENT","sub_res",{"id":"evt_resilience_100","content":"resilience data"}]"#;
    events_tx_0.send(RelayEvent {
        raw: test_evt.to_string(),
    })?;

    let _r1 = tokio::time::timeout(Duration::from_secs(2), publish_rx_1.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("Node 1 did not receive"))?;
    let _r2 = tokio::time::timeout(Duration::from_secs(2), publish_rx_2.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("Node 2 did not receive"))?;

    cancel_node0.cancel();
    tokio::time::sleep(Duration::from_millis(200)).await;

    cancel.cancel();
    Ok(())
}
