use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use goy_node::config::MeshConfig;
use goy_node::rate_limiter::{PeerRateLimiter, RateLimitReason};
use goy_node::relay::RelayEvent;

#[test]
fn test_peer_rate_limiter_unit() {
    let mut limiter = PeerRateLimiter::new(10, 1000);

    // Consumir 10 eventos válidos
    for i in 0..10 {
        assert_eq!(
            limiter.try_consume(50),
            Ok(()),
            "Event {i} should pass rate limit"
        );
    }

    // 11º evento deve ser rejeitado por exaustão de eventos
    assert_eq!(
        limiter.try_consume(50),
        Err(RateLimitReason::EventsExhausted)
    );

    // Espera 250ms para repor ~2.5 tokens
    std::thread::sleep(Duration::from_millis(250));

    // Próximo evento deve passar
    assert_eq!(limiter.try_consume(50), Ok(()));
}

#[tokio::test]
async fn test_oversized_message_pre_parse_rejection() -> anyhow::Result<()> {
    let cancel = CancellationToken::new();

    let l_a = TcpListener::bind("127.0.0.1:0").await?;
    let addr_a = l_a.local_addr()?;
    drop(l_a);

    let l_b = TcpListener::bind("127.0.0.1:0").await?;
    let addr_b = l_b.local_addr()?;
    drop(l_b);

    let (_relay_events_tx_a, relay_events_rx_a) = broadcast::channel::<RelayEvent>(16);
    let (relay_publish_tx_a, mut relay_publish_rx_a) = mpsc::channel::<String>(16);

    let cfg_a = MeshConfig {
        listen: addr_a.to_string(),
        seeds: vec![],
        max_message_size: 150, // Limite pequeno de 150 bytes
        ..MeshConfig::default()
    };

    let cancel_a = cancel.clone();
    tokio::spawn(async move {
        let _ = goy_node::mesh::run(
            cfg_a,
            "ws://127.0.0.1:57777".to_string(),
            None,
            relay_events_rx_a,
            relay_publish_tx_a,
            cancel_a,
        )
        .await;
    });

    let (relay_events_tx_b, relay_events_rx_b) = broadcast::channel::<RelayEvent>(16);
    let (relay_publish_tx_b, _relay_publish_rx_b) = mpsc::channel::<String>(16);

    let cfg_b = MeshConfig {
        listen: addr_b.to_string(),
        seeds: vec![format!("ws://{addr_a}")],
        ..MeshConfig::default()
    };

    let cancel_b = cancel.clone();
    tokio::spawn(async move {
        let _ = goy_node::mesh::run(
            cfg_b,
            "ws://127.0.0.1:57777".to_string(),
            None,
            relay_events_rx_b,
            relay_publish_tx_b,
            cancel_b,
        )
        .await;
    });

    // Aguarda conexão entre B e A
    tokio::time::sleep(Duration::from_millis(350)).await;

    // Enviar mensagem gigante (> 150 bytes) do Node B
    let huge_content = "X".repeat(300);
    let oversized_event = format!(
        r#"["EVENT","sub_huge",{{"id":"e_huge_00000000000000000000000000000000000000000000000000000001","content":"{huge_content}"}}]"#
    );

    relay_events_tx_b.send(RelayEvent {
        raw: oversized_event,
    })?;

    // Node A deve rejeitar a mensagem antes do parsing e NUNCA publicar no relay local
    let res = tokio::time::timeout(Duration::from_millis(600), relay_publish_rx_a.recv()).await;
    assert!(
        res.is_err(),
        "Node A must reject oversized message and not publish to local relay"
    );

    cancel.cancel();
    Ok(())
}

#[tokio::test]
async fn test_peer_rate_limiting_burst_and_recovery() -> anyhow::Result<()> {
    let cancel = CancellationToken::new();

    let l_a = TcpListener::bind("127.0.0.1:0").await?;
    let addr_a = l_a.local_addr()?;
    drop(l_a);

    let l_b = TcpListener::bind("127.0.0.1:0").await?;
    let addr_b = l_b.local_addr()?;
    drop(l_b);

    let (_relay_events_tx_a, relay_events_rx_a) = broadcast::channel::<RelayEvent>(64);
    let (relay_publish_tx_a, mut relay_publish_rx_a) = mpsc::channel::<String>(64);

    let cfg_a = MeshConfig {
        listen: addr_a.to_string(),
        seeds: vec![],
        max_events_per_second_per_peer: 10, // Limite de 10 msgs/s por peer
        ..MeshConfig::default()
    };

    let cancel_a = cancel.clone();
    tokio::spawn(async move {
        let _ = goy_node::mesh::run(
            cfg_a,
            "ws://127.0.0.1:57777".to_string(),
            None,
            relay_events_rx_a,
            relay_publish_tx_a,
            cancel_a,
        )
        .await;
    });

    let (relay_events_tx_b, relay_events_rx_b) = broadcast::channel::<RelayEvent>(64);
    let (relay_publish_tx_b, _relay_publish_rx_b) = mpsc::channel::<String>(64);

    let cfg_b = MeshConfig {
        listen: addr_b.to_string(),
        seeds: vec![format!("ws://{addr_a}")],
        ..MeshConfig::default()
    };

    let cancel_b = cancel.clone();
    tokio::spawn(async move {
        let _ = goy_node::mesh::run(
            cfg_b,
            "ws://127.0.0.1:57777".to_string(),
            None,
            relay_events_rx_b,
            relay_publish_tx_b,
            cancel_b,
        )
        .await;
    });

    tokio::time::sleep(Duration::from_millis(350)).await;

    // Enviar burst de 20 eventos rapidos do Node B
    for i in 0..20 {
        let evt =
            format!(r#"["EVENT","sub_burst",{{"id":"e_burst_{i:060}","content":"burst {i}"}}]"#);
        relay_events_tx_b.send(RelayEvent { raw: evt })?;
    }

    // Apenas os tokens disponíveis (10 - 1 de REQ inicial = 9) passam
    let mut received_count = 0;
    while tokio::time::timeout(Duration::from_millis(300), relay_publish_rx_a.recv())
        .await
        .is_ok()
    {
        received_count += 1;
    }

    assert!(
        received_count < 20,
        "Burst of 20 events should be rate limited (received {received_count})"
    );
    assert!(
        received_count > 0,
        "Initial allowed events should pass (received {received_count})"
    );

    // Aguarda 1.1s para repor tokens no bucket
    tokio::time::sleep(Duration::from_millis(1100)).await;

    // Enviar mais 1 evento -> deve ser aceite pós-recuperação
    let evt_recovered = r#"["EVENT","sub_burst",{"id":"e_recovered_00000000000000000000000000000000000000000000000001","content":"recovered"}]"#;
    relay_events_tx_b.send(RelayEvent {
        raw: evt_recovered.to_string(),
    })?;

    let rec = tokio::time::timeout(Duration::from_secs(2), relay_publish_rx_a.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("Recovered event should have been received"))?;
    assert!(rec.contains("e_recovered"));

    cancel.cancel();
    Ok(())
}

#[tokio::test]
async fn test_peer_rate_limit_isolation() -> anyhow::Result<()> {
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

    let (_relay_events_tx_a, relay_events_rx_a) = broadcast::channel::<RelayEvent>(64);
    let (relay_publish_tx_a, mut relay_publish_rx_a) = mpsc::channel::<String>(64);

    let cfg_a = MeshConfig {
        listen: addr_a.to_string(),
        seeds: vec![],
        max_events_per_second_per_peer: 10, // Limite de 10 msgs/s por peer
        ..MeshConfig::default()
    };

    let cancel_a = cancel.clone();
    tokio::spawn(async move {
        let _ = goy_node::mesh::run(
            cfg_a,
            "ws://127.0.0.1:57777".to_string(),
            None,
            relay_events_rx_a,
            relay_publish_tx_a,
            cancel_a,
        )
        .await;
    });

    // Node B (seed Node A)
    let (relay_events_tx_b, relay_events_rx_b) = broadcast::channel::<RelayEvent>(64);
    let (relay_publish_tx_b, _relay_publish_rx_b) = mpsc::channel::<String>(64);
    let cfg_b = MeshConfig {
        listen: addr_b.to_string(),
        seeds: vec![format!("ws://{addr_a}")],
        ..MeshConfig::default()
    };

    let cancel_b = cancel.clone();
    tokio::spawn(async move {
        let _ = goy_node::mesh::run(
            cfg_b,
            "ws://127.0.0.1:57777".to_string(),
            None,
            relay_events_rx_b,
            relay_publish_tx_b,
            cancel_b,
        )
        .await;
    });

    // Node C (seed Node A)
    let (relay_events_tx_c, relay_events_rx_c) = broadcast::channel::<RelayEvent>(64);
    let (relay_publish_tx_c, _relay_publish_rx_c) = mpsc::channel::<String>(64);
    let cfg_c = MeshConfig {
        listen: addr_c.to_string(),
        seeds: vec![format!("ws://{addr_a}")],
        ..MeshConfig::default()
    };

    let cancel_c = cancel.clone();
    tokio::spawn(async move {
        let _ = goy_node::mesh::run(
            cfg_c,
            "ws://127.0.0.1:57777".to_string(),
            None,
            relay_events_rx_c,
            relay_publish_tx_c,
            cancel_c,
        )
        .await;
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Node B esgota o seu próprio bucket enviando 25 eventos
    for i in 0..25 {
        let evt_b =
            format!(r#"["EVENT","sub_iso_b",{{"id":"e_from_b_{i:060}","content":"B {i}"}}]"#);
        relay_events_tx_b.send(RelayEvent { raw: evt_b })?;
    }

    // Node B deve ser rate limited (alguns eventos passam, a maioria é rejeitada)
    let mut count_b = 0;
    while tokio::time::timeout(Duration::from_millis(250), relay_publish_rx_a.recv())
        .await
        .is_ok()
    {
        count_b += 1;
    }
    assert!(count_b < 25, "Node B should be rate limited");
    assert!(
        count_b > 0,
        "Node B should have delivered some events before limit"
    );

    // Mesmo com o bucket do Node B esgotado, Node C envia evento e passa IMEDIATAMENTE!
    let evt_c = r#"["EVENT","sub_iso_c",{"id":"e_from_c_000000000000000000000000000000000000000000000000000001","content":"C event"}]"#;
    relay_events_tx_c.send(RelayEvent {
        raw: evt_c.to_string(),
    })?;

    let rec_c = tokio::time::timeout(Duration::from_secs(2), relay_publish_rx_a.recv())
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!("Node C event should pass independently of Node B's limit")
        })?;
    assert!(rec_c.contains("e_from_c"));

    cancel.cancel();
    Ok(())
}
