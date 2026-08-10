//! Integration tests for the Admin CLI subcommands (`goy-node status`, `peers`, `info`, `metrics`).

use std::time::Duration;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use goy_node::cli::{Cli, Commands, handle_cli};
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

#[tokio::test]
async fn test_admin_cli_endpoints_and_json_formatting() -> anyhow::Result<()> {
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

    // Wait for HTTP server startup
    for _ in 0..20 {
        if tokio::net::TcpStream::connect(&metrics_listen)
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // 1. Status subcommand with --json
    let cli_status = Cli {
        config: None,
        data_dir: None,
        json: true,
        command: Some(Commands::Status),
    };
    let handled = handle_cli(&cli_status, Some(&metrics_listen)).await?;
    assert!(handled);

    // 2. Peers subcommand with --json
    let cli_peers = Cli {
        config: None,
        data_dir: None,
        json: true,
        command: Some(Commands::Peers),
    };
    let handled = handle_cli(&cli_peers, Some(&metrics_listen)).await?;
    assert!(handled);

    // 3. Info subcommand with text output
    let cli_info = Cli {
        config: None,
        data_dir: None,
        json: false,
        command: Some(Commands::Info),
    };
    let handled = handle_cli(&cli_info, Some(&metrics_listen)).await?;
    assert!(handled);

    // 4. Metrics subcommand
    let cli_metrics = Cli {
        config: None,
        data_dir: None,
        json: false,
        command: Some(Commands::Metrics),
    };
    let handled = handle_cli(&cli_metrics, Some(&metrics_listen)).await?;
    assert!(handled);

    cancel.cancel();
    Ok(())
}
