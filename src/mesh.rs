//! Mesh agent: sincroniza eventos entre peers via WebSocket.
//!
//! - Consome eventos do relay local e encaminha para peers
//! - Recebe eventos de peers e publica no relay local
//! - Deduplica por event ID para evitar loops

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use backoff::ExponentialBackoffBuilder;
use dashmap::DashSet;
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::MeshConfig;
use crate::relay::RelayEvent;

/// Estado compartilhado entre todas as tarefas do mesh agent.
struct MeshState {
    /// Event IDs já vistos (dedup global).
    seen_ids: DashSet<String>,
    /// Canal para publicar eventos remotos no relay local.
    relay_tx: mpsc::Sender<String>,
    /// IDs/URLs de peers atualmente conectados (inbound e outbound).
    connected_peers: DashSet<String>,
}

/// Guard RAII para remover o peer de `connected_peers` quando a sessão termina.
struct PeerGuard {
    state: Arc<MeshState>,
    peer_id: String,
}

impl Drop for PeerGuard {
    fn drop(&mut self) {
        self.state.connected_peers.remove(&self.peer_id);
    }
}

/// Inicia o mesh agent: listener para peers + seeds outbound + consumer de eventos locais.
pub async fn run(
    cfg: MeshConfig,
    mut relay_events: broadcast::Receiver<RelayEvent>,
    relay_publish_tx: mpsc::Sender<String>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let state = Arc::new(MeshState {
        seen_ids: DashSet::new(),
        relay_tx: relay_publish_tx,
        connected_peers: DashSet::new(),
    });

    // ── Listener para peers ────────────────────────────────────────────
    let listener = TcpListener::bind(&cfg.listen).await?;
    info!("🌐 Mesh agent listening on {}", cfg.listen);

    // Canal interno para distribuir eventos locais para todos os peers
    let (peer_broadcast_tx, _) = broadcast::channel::<String>(4096);

    // ── Task: consumir eventos do relay local → broadcast para peers ──
    let state_clone = state.clone();
    let peer_tx_clone = peer_broadcast_tx.clone();
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel_clone.cancelled() => break,
                result = relay_events.recv() => {
                    match result {
                        Ok(event) => {
                            if let Some(id) = extract_event_id(&event.raw) {
                                if state_clone.seen_ids.insert(id.clone()) {
                                    info!("📡 Local relay event {id} received, broadcasting to peers");
                                    let _ = peer_tx_clone.send(event.raw);
                                } else {
                                    tracing::debug!("🔁 Relay event {id} already seen (dedup), skipping broadcast");
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!("⚠️  Relay events lagged by {n}, some events may be missed");
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            info!("📡 Relay event channel closed");
                            break;
                        }
                    }
                }
            }
        }
    });

    // ── Tasks: conexões outbound com seeds ─────────────────────────────
    for seed_url in cfg.seeds.clone() {
        start_seed_task(
            seed_url,
            state.clone(),
            peer_broadcast_tx.clone(),
            cancel.clone(),
        );
    }

    // ── Loop principal: aceitar conexões inbound de peers ──────────────
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("🌐 Mesh agent shutting down");
                break;
            }
            result = listener.accept() => {
                match result {
                    Ok((stream, addr)) => {
                        info!("🤝 Peer connected inbound: {addr}");
                        let state = state.clone();
                        let peer_rx = peer_broadcast_tx.subscribe();
                        let cancel = cancel.clone();
                        tokio::spawn(handle_inbound_peer(stream, addr, state, peer_rx, cancel));
                    }
                    Err(e) => {
                        error!("❌ Failed to accept peer connection: {e}");
                    }
                }
            }
        }
    }

    Ok(())
}

/// Spawna a task de reconexão automática para um seed remoto.
fn start_seed_task(
    seed_url: String,
    state: Arc<MeshState>,
    peer_broadcast_tx: broadcast::Sender<String>,
    cancel: CancellationToken,
) {
    use tokio_tungstenite::connect_async;

    tokio::spawn(async move {
        let backoff = ExponentialBackoffBuilder::new()
            .with_initial_interval(Duration::from_secs(1))
            .with_max_interval(Duration::from_secs(60))
            .with_max_elapsed_time(None)
            .build();

        let seed_url_clone = seed_url.clone();
        backoff::future::retry(backoff, || {
            let seed_url = seed_url_clone.clone();
            let state = state.clone();
            let peer_broadcast_tx = peer_broadcast_tx.clone();
            let cancel = cancel.clone();

            async move {
                if cancel.is_cancelled() {
                    return Err::<(), _>(backoff::Error::permanent(anyhow::anyhow!("shutdown")));
                }

                if state.connected_peers.contains(&seed_url) {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    return Err::<(), _>(backoff::Error::transient(anyhow::anyhow!("already connected")));
                }

                info!("🌱 Connecting outbound to seed: {seed_url}");
                match connect_async(&seed_url).await {
                    Ok((ws, _)) => {
                        info!("🟢 Outbound connection established to seed: {seed_url}");
                        let peer_rx = peer_broadcast_tx.subscribe();
                        let (sink, stream) = ws.split();
                        handle_peer_stream(seed_url.clone(), sink, stream, state, peer_rx, cancel).await;
                        Err::<(), _>(backoff::Error::transient(anyhow::anyhow!("seed connection ended")))
                    }
                    Err(e) => {
                        warn!("🔌 Seed connection to {seed_url} failed: {e}. Reconnecting…");
                        Err::<(), _>(backoff::Error::transient(anyhow::Error::new(e)))
                    }
                }
            }
        })
        .await
        .ok();

        info!("🌱 Seed task for {seed_url} stopped");
    });
}

/// Trata conexão inbound de um peer (TcpStream -> WebSocket).
async fn handle_inbound_peer(
    stream: tokio::net::TcpStream,
    addr: SocketAddr,
    state: Arc<MeshState>,
    peer_rx: broadcast::Receiver<String>,
    cancel: CancellationToken,
) {
    use tokio_tungstenite::accept_async;

    let ws = match accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            warn!("❌ WebSocket handshake failed for {addr}: {e}");
            return;
        }
    };

    let peer_id = format!("inbound:{addr}");
    let (sink, stream) = ws.split();
    handle_peer_stream(peer_id, sink, stream, state, peer_rx, cancel).await;
}

/// Handler unificado de sessão peer bidirecional (independente de inbound ou outbound).
async fn handle_peer_stream<Si, St>(
    peer_id: String,
    mut sink: Si,
    mut stream: St,
    state: Arc<MeshState>,
    mut peer_rx: broadcast::Receiver<String>,
    cancel: CancellationToken,
) where
    Si: Sink<Message> + Unpin + Send + 'static,
    St: Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin + Send + 'static,
{
    if !state.connected_peers.insert(peer_id.clone()) {
        warn!("⚠️ Already connected to peer {peer_id}, skipping duplicate session");
        return;
    }
    let _guard = PeerGuard {
        state: state.clone(),
        peer_id: peer_id.clone(),
    };

    info!("🤝 Active peer session: {peer_id}");

    // Canal interno para enviar mensagens de controle (pong, OK, etc.) ao sink
    let (ctrl_tx, mut ctrl_rx) = mpsc::channel::<Message>(32);

    // ── Task: enviar eventos locais + mensagens de controle para este peer ──
    let cancel_clone = cancel.clone();
    let send_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel_clone.cancelled() => break,
                // Mensagens de controle (pong, OK, close, etc.) têm prioridade
                Some(ctrl_msg) = ctrl_rx.recv() => {
                    if sink.send(ctrl_msg).await.is_err() {
                        break;
                    }
                }
                // Eventos do relay local para encaminhar ao peer
                result = peer_rx.recv() => {
                    match result {
                        Ok(raw) => {
                            if sink.send(Message::Text(raw.into())).await.is_err() {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }
    });

    // ── Receber eventos do peer → dedup → publicar no relay local ─────
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            msg = stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Some(id) = extract_event_id(&text) {
                            if state.seen_ids.insert(id.clone()) {
                                info!("📥 Event {id} received from peer {peer_id}, publishing to local relay");
                                if state.relay_tx.send(text.to_string()).await.is_err() {
                                    warn!("⚠️  Relay publish channel closed");
                                    break;
                                }
                                // Resposta OK otimista para o peer (assumindo aceitação pelo strfry)
                                let ok_msg = format!(r#"["OK","{}",true,""]"#, id);
                                let _ = ctrl_tx.send(Message::Text(ok_msg.into())).await;
                            } else {
                                tracing::debug!("🔁 Event {id} from peer {peer_id} already seen (dedup), skipping relay publish");
                            }
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        // Envia pong via canal de controle em vez de usar sink diretamente
                        let _ = ctrl_tx.send(Message::Pong(data)).await;
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        info!("🔌 Peer disconnected: {peer_id}");
                        break;
                    }
                    Some(Err(e)) => {
                        warn!("⚠️  Peer {peer_id} error: {e}");
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    send_task.abort();
    info!("👋 Peer session ended: {peer_id}");
}

/// Extrai o event ID de uma mensagem EVENT JSON.
/// Suporta formatos:
/// - ["EVENT", {"id":"hex",...}]
/// - ["EVENT", "subscription_id", {"id":"hex",...}]
fn extract_event_id(raw: &str) -> Option<String> {
    let parsed: serde_json::Value = serde_json::from_str(raw).ok()?;
    let arr = parsed.as_array()?;
    if arr.is_empty() || arr[0].as_str() != Some("EVENT") {
        return None;
    }
    if arr.len() == 2 {
        arr[1].get("id")?.as_str().map(|s| s.to_string())
    } else if arr.len() >= 3 {
        arr[2].get("id")?.as_str().map(|s| s.to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_extract_event_id_formats() {
        let msg_2_elem = r#"["EVENT",{"id":"abc123id","pubkey":"pub123"}]"#;
        assert_eq!(extract_event_id(msg_2_elem), Some("abc123id".to_string()));

        let msg_3_elem = r#"["EVENT","sub_42",{"id":"def456id","pubkey":"pub123"}]"#;
        assert_eq!(extract_event_id(msg_3_elem), Some("def456id".to_string()));

        let msg_invalid = r#"["REQ","sub_42",{}]"#;
        assert_eq!(extract_event_id(msg_invalid), None);
    }

    #[tokio::test]
    async fn test_bidirectional_relay_and_peer_flow() -> anyhow::Result<()> {
        use tokio_tungstenite::connect_async;

        let (relay_events_tx, relay_events_rx) = broadcast::channel::<RelayEvent>(16);
        let (relay_publish_tx, mut relay_publish_rx) = mpsc::channel::<String>(16);
        let cancel = CancellationToken::new();

        let cfg = MeshConfig {
            listen: "127.0.0.1:18443".to_string(),
            seeds: vec![],
            registry_url: None,
            heartbeat_secs: 30,
        };

        let cancel_mesh = cancel.clone();
        tokio::spawn(async move {
            let _ = run(cfg, relay_events_rx, relay_publish_tx, cancel_mesh).await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Conecta peer via WebSocket
        let ws_url = "ws://127.0.0.1:18443";
        let (mut ws_stream, _) = connect_async(ws_url).await?;

        // 1. Fluxo: Relay local -> Mesh Agent -> Peer
        let event_from_relay = r#"["EVENT","goy-live",{"id":"relay_evt_1","content":"hello from strfry"}]"#;
        relay_events_tx.send(RelayEvent {
            raw: event_from_relay.to_string(),
        })?;

        // Peer deve receber o evento vindo do relay local
        let msg = tokio::time::timeout(Duration::from_secs(2), ws_stream.next())
            .await?
            .ok_or_else(|| anyhow::anyhow!("ws_stream closed unexpectedly"))??;

        assert_eq!(msg.to_text()?, event_from_relay);

        // 2. Fluxo: Peer -> Mesh Agent -> Relay local + OK otimista
        let event_from_peer = r#"["EVENT",{"id":"peer_evt_1","content":"hello from peer"}]"#;
        ws_stream
            .send(Message::Text(event_from_peer.into()))
            .await?;

        // Publisher para o relay deve receber o evento
        let published_to_relay =
            tokio::time::timeout(Duration::from_secs(2), relay_publish_rx.recv())
                .await?
                .ok_or_else(|| anyhow::anyhow!("relay_publish_rx closed unexpectedly"))?;
        assert_eq!(published_to_relay, event_from_peer);

        // Peer deve receber resposta OK otimista
        let ok_msg = tokio::time::timeout(Duration::from_secs(2), ws_stream.next())
            .await?
            .ok_or_else(|| anyhow::anyhow!("ws_stream closed unexpectedly"))??;
        assert_eq!(ok_msg.to_text()?, r#"["OK","peer_evt_1",true,""]"#);

        cancel.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn test_two_nodes_seed_connection_flow() -> anyhow::Result<()> {
        let cancel = CancellationToken::new();

        // ── Node A (sem seeds, escuta em 18446) ───────────────────────────
        let (relay_events_tx_a, relay_events_rx_a) = broadcast::channel::<RelayEvent>(16);
        let (relay_publish_tx_a, mut relay_publish_rx_a) = mpsc::channel::<String>(16);

        let cfg_a = MeshConfig {
            listen: "127.0.0.1:18446".to_string(),
            seeds: vec![],
            registry_url: None,
            heartbeat_secs: 30,
        };

        let cancel_a = cancel.clone();
        tokio::spawn(async move {
            let _ = run(cfg_a, relay_events_rx_a, relay_publish_tx_a, cancel_a).await;
        });

        // ── Node B (com seed = ws://127.0.0.1:18446, escuta em 18447) ───────
        let (relay_events_tx_b, relay_events_rx_b) = broadcast::channel::<RelayEvent>(16);
        let (relay_publish_tx_b, mut relay_publish_rx_b) = mpsc::channel::<String>(16);

        let cfg_b = MeshConfig {
            listen: "127.0.0.1:18447".to_string(),
            seeds: vec!["ws://127.0.0.1:18446".to_string()],
            registry_url: None,
            heartbeat_secs: 30,
        };

        let cancel_b = cancel.clone();
        tokio::spawn(async move {
            let _ = run(cfg_b, relay_events_rx_b, relay_publish_tx_b, cancel_b).await;
        });

        // Aguarda estabelecimento da conexão outbound do Node B -> Node A
        tokio::time::sleep(Duration::from_millis(300)).await;

        // 1. Evento publicado no strfry do Node A -> deve chegar ao strfry do Node B
        let event_a = r#"["EVENT","sub_a",{"id":"evt_from_node_a","content":"hello from Node A"}]"#;
        relay_events_tx_a.send(RelayEvent {
            raw: event_a.to_string(),
        })?;

        let received_at_node_b =
            tokio::time::timeout(Duration::from_secs(2), relay_publish_rx_b.recv())
                .await?
                .ok_or_else(|| anyhow::anyhow!("Node B relay_publish_rx closed"))?;
        assert_eq!(received_at_node_b, event_a);

        // 2. Evento publicado no strfry do Node B -> deve chegar ao strfry do Node A
        let event_b = r#"["EVENT","sub_b",{"id":"evt_from_node_b","content":"hello from Node B"}]"#;
        relay_events_tx_b.send(RelayEvent {
            raw: event_b.to_string(),
        })?;

        let received_at_node_a =
            tokio::time::timeout(Duration::from_secs(2), relay_publish_rx_a.recv())
                .await?
                .ok_or_else(|| anyhow::anyhow!("Node A relay_publish_rx closed"))?;
        assert_eq!(received_at_node_a, event_b);

        cancel.cancel();
        Ok(())
    }
}




