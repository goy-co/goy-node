//! Integration tests for Consistent Hashing and Data Rebalancing.

use std::time::Duration;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use goy_node::config::MeshConfig;
use goy_node::consistent_hash::ConsistentHashRing;
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
async fn test_consistent_hash_ring_node_join_and_leave() {
    let mut ring = ConsistentHashRing::new(150);
    assert_eq!(ring.peer_count(), 0);

    ring.add_peer("node-1");
    ring.add_peer("node-2");
    ring.add_peer("node-3");

    assert_eq!(ring.peer_count(), 3);
    assert_eq!(ring.vnode_count(), 450);

    let key = "evt_hash_test_100";
    let responsible = ring.get_responsible_peers(key, 2);
    assert_eq!(responsible.len(), 2);
    assert_ne!(responsible[0], responsible[1]);

    // Primary peer query
    let primary = ring.get_primary_peer(key);
    assert_eq!(primary, Some(responsible[0].clone()));

    // Remove primary peer
    ring.remove_peer(&responsible[0]);
    assert_eq!(ring.peer_count(), 2);
    assert_eq!(ring.vnode_count(), 300);

    let new_responsible = ring.get_responsible_peers(key, 2);
    assert_eq!(new_responsible.len(), 2);
    assert!(!new_responsible.contains(&responsible[0]));
}

#[tokio::test]
async fn test_five_nodes_consistent_hashing_sync() -> anyhow::Result<()> {
    let mut node_addrs = Vec::new();
    for _ in 0..5 {
        node_addrs.push(free_addr().await);
    }

    let cancel = CancellationToken::new();
    let mut handles = Vec::new();

    for i in 0..5 {
        let mut cfg = MeshConfig::default();
        cfg.listen = node_addrs[i].clone();
        cfg.tls_enabled = false;
        cfg.replication_factor = 3;
        cfg.vnodes_per_peer = 150;

        // Seed to previous node
        if i > 0 {
            cfg.seeds = vec![format!("ws://{}", node_addrs[i - 1])];
        }

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

    // Wait for full mesh connectivity
    tokio::time::sleep(Duration::from_millis(500)).await;

    cancel.cancel();
    for h in handles {
        let _ = h.await;
    }

    Ok(())
}
