//! Stress test suite for hash ring rebalancing under load.

use std::time::Duration;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use goy_node::config::MeshConfig;
use goy_node::consistent_hash::ConsistentHashRing;
use goy_node::mesh::run_with_http_listen;
use goy_node::relay::RelayEvent;

async fn free_addr() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    format!("127.0.0.1:{}", addr.port())
}

#[tokio::test]
async fn test_stress_rebalance_node_addition() -> anyhow::Result<()> {
    let mut ring = ConsistentHashRing::new(150);

    for i in 0..5 {
        ring.add_peer(&format!("node-{i}"));
    }

    assert_eq!(ring.peer_count(), 5);

    let total_events = 5_000;
    let initial_assignments: Vec<String> = (0..total_events)
        .map(|i| ring.get_primary_peer(&format!("evt_key_{i}")).unwrap())
        .collect();

    ring.add_peer("node-5");
    assert_eq!(ring.peer_count(), 6);

    let new_assignments: Vec<String> = (0..total_events)
        .map(|i| ring.get_primary_peer(&format!("evt_key_{i}")).unwrap())
        .collect();

    let mut moved_keys = 0;
    for i in 0..total_events {
        if initial_assignments[i] != new_assignments[i] {
            moved_keys += 1;
            assert_eq!(new_assignments[i], "node-5");
        }
    }

    let moved_pct = (moved_keys as f64 / total_events as f64) * 100.0;
    println!("✅ Rebalanced {moved_keys}/{total_events} keys ({moved_pct:.2}%) to new node-5");
    assert!(
        moved_pct > 12.0 && moved_pct < 22.0,
        "Moved keys percentage must be around 1/6 (~16.6%), got {moved_pct:.2}%"
    );

    Ok(())
}

#[tokio::test]
async fn test_stress_rebalance_mesh_agent_cluster() -> anyhow::Result<()> {
    let mut addrs = Vec::new();
    for _ in 0..3 {
        addrs.push(free_addr().await);
    }

    let cancel = CancellationToken::new();
    let mut handles = Vec::new();

    for i in 0..3 {
        let seeds = if i > 0 {
            vec![format!("ws://{}", addrs[i - 1])]
        } else {
            vec![]
        };

        let cfg = MeshConfig {
            listen: addrs[i].clone(),
            seeds,
            tls_enabled: false,
            replication_factor: 3,
            ..MeshConfig::default()
        };

        let (_relay_events_tx, relay_events_rx) = broadcast::channel::<RelayEvent>(16);
        let (relay_publish_tx, _relay_publish_rx) = tokio::sync::mpsc::channel::<String>(16);

        let c = cancel.clone();
        let handle = tokio::spawn(async move {
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
        handles.push(handle);
    }

    tokio::time::sleep(Duration::from_millis(300)).await;

    cancel.cancel();
    for h in handles {
        let _ = h.await;
    }

    Ok(())
}
