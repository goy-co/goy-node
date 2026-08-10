//! Integration tests for observability (Prometheus metrics & HTTP health endpoint).

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use goy_node::config::MeshConfig;
use goy_node::mesh::run_with_http_listen;
use goy_node::relay::RelayEvent;

/// Reserves a random ephemeral TCP port on loopback and returns the address string.
async fn free_addr() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    format!("127.0.0.1:{}", addr.port())
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
    stream.write_all(req.as_bytes()).await?;

    let mut resp = String::new();
    stream.read_to_string(&mut resp).await?;

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
async fn test_observability_http_endpoints_end_to_end() -> anyhow::Result<()> {
    let mesh_listen = free_addr().await;
    let metrics_listen = free_addr().await;

    let cfg = MeshConfig {
        listen: mesh_listen.clone(),
        tls_enabled: false,
        ..MeshConfig::default()
    };

    let (relay_events_tx, relay_events_rx) = broadcast::channel::<RelayEvent>(16);
    let (relay_publish_tx, _relay_publish_rx) = tokio::sync::mpsc::channel::<String>(16);
    let cancel = CancellationToken::new();

    let c = cancel.clone();
    let m_listen = metrics_listen.clone();
    tokio::spawn(async move {
        let _ = run_with_http_listen(
            cfg,
            Some(m_listen),
            "ws://127.0.0.1:57777".to_string(),
            None,
            relay_events_rx,
            relay_publish_tx,
            c,
        )
        .await;
    });

    // 1. GET /health with 0 peers -> 503 degraded
    let (status, res) = http_get(&metrics_listen, "/health").await?;
    assert_eq!(
        status, 503,
        "expected 503 Degraded when 0 peers, got: {status}"
    );
    assert!(res.contains(r#""status":"degraded""#));
    assert!(res.contains(r#""peers":0"#));

    // 2. Send a dummy relay event to bump the counter
    relay_events_tx.send(RelayEvent {
        raw: r#"["EVENT",{"id":"evt_obs_001","pubkey":"alice","kind":1,"created_at":1000,"content":"hello","sig":"sig"}]"#.to_string(),
    })?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 3. GET /metrics -> Prometheus text format with goy_events_received_total{source="relay"} 1
    let (status, metrics_text) = http_get(&metrics_listen, "/metrics").await?;
    assert_eq!(status, 200);

    for expected in [
        "# TYPE goy_events_received_total counter",
        r#"goy_events_received_total{source="relay"} 1"#,
        "# TYPE goy_peers_connected gauge",
        "goy_peers_connected 0",
        "# TYPE goy_uptime_seconds gauge",
    ] {
        assert!(
            metrics_text.contains(expected),
            "metrics output missing {expected:?}, got:\n{metrics_text}"
        );
    }

    // 4. GET /info -> Node Metadata
    let (status, info_json) = http_get(&metrics_listen, "/info").await?;
    assert_eq!(status, 200);
    assert!(info_json.contains(r#""version":""#));
    assert!(info_json.contains(r#""tls_enabled":false"#));
    assert!(info_json.contains(&format!(r#""mesh_listen":"{mesh_listen}""#)));

    cancel.cancel();
    Ok(())
}

#[tokio::test]
async fn test_http_metrics_server_disabled_when_listen_is_none() -> anyhow::Result<()> {
    let mesh_listen = free_addr().await;
    let metrics_listen = free_addr().await;

    let cfg = MeshConfig {
        listen: mesh_listen,
        tls_enabled: false,
        ..MeshConfig::default()
    };

    let (_relay_events_tx, relay_events_rx) = broadcast::channel::<RelayEvent>(16);
    let (relay_publish_tx, _relay_publish_rx) = tokio::sync::mpsc::channel::<String>(16);
    let cancel = CancellationToken::new();

    let c = cancel.clone();
    tokio::spawn(async move {
        let _ = run_with_http_listen(
            cfg,
            None, // HTTP server disabled
            "ws://127.0.0.1:57777".to_string(),
            None,
            relay_events_rx,
            relay_publish_tx,
            c,
        )
        .await;
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Connection attempt to disabled metrics address should fail
    let res = tokio::net::TcpStream::connect(&metrics_listen).await;
    assert!(
        res.is_err(),
        "connection to disabled metrics server should fail"
    );

    cancel.cancel();
    Ok(())
}
