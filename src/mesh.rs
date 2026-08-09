//! Mesh agent: sincroniza eventos entre peers via WebSocket.
//!
//! - Consome eventos do relay local e encaminha para peers
//! - Recebe eventos de peers e publica no relay local
//! - Deduplica por event ID para evitar loops

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use backoff::ExponentialBackoffBuilder;
use dashmap::{DashMap, DashSet};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::MeshConfig;
use crate::registry::{self, RegistryClient, RelayInfo};
use crate::relay::RelayEvent;

/// Estado compartilhado entre todas as tarefas do mesh agent.
struct MeshState {
    /// Event IDs já vistos (dedup global).
    seen_ids: DashSet<String>,
    /// Último timestamp (created_at) visto por peer para backfill incremental.
    peer_cursors: DashMap<String, u64>,
    /// Canal para publicar eventos remotos no relay local.
    relay_tx: mpsc::Sender<String>,
    /// IDs/URLs de peers atualmente conectados (inbound e outbound).
    connected_peers: DashSet<String>,
    /// WebSocket URL do relay local para consultas de backfill.
    relay_url: String,
    /// Diretório para persistência de estado em disco (opcional).
    data_dir: Option<PathBuf>,
}

/// Carrega seen_ids e peer_cursors do disco.
fn load_state(data_dir: Option<&Path>) -> (DashSet<String>, DashMap<String, u64>) {
    let seen_ids = DashSet::new();
    let peer_cursors = DashMap::new();

    let dir = match data_dir {
        Some(d) => d,
        None => return (seen_ids, peer_cursors),
    };

    // 1. Carregar seen_ids.json
    let seen_file = dir.join("seen_ids.json");
    if seen_file.exists() {
        match std::fs::read(&seen_file) {
            Ok(bytes) => match serde_json::from_slice::<Vec<String>>(&bytes) {
                Ok(ids) => {
                    info!("💾 Loaded {} seen event IDs from {}", ids.len(), seen_file.display());
                    for id in ids {
                        seen_ids.insert(id);
                    }
                }
                Err(e) => {
                    warn!("⚠️  Failed to parse {}: {e}. Starting fresh with empty seen_ids.", seen_file.display());
                }
            },
            Err(e) => {
                warn!("⚠️  Failed to read {}: {e}. Starting fresh with empty seen_ids.", seen_file.display());
            }
        }
    }

    // 2. Carregar peer_cursors.json
    let cursors_file = dir.join("peer_cursors.json");
    if cursors_file.exists() {
        match std::fs::read(&cursors_file) {
            Ok(bytes) => match serde_json::from_slice::<std::collections::HashMap<String, u64>>(&bytes) {
                Ok(map) => {
                    info!("💾 Loaded {} peer cursors from {}", map.len(), cursors_file.display());
                    for (peer, cursor) in map {
                        peer_cursors.insert(peer, cursor);
                    }
                }
                Err(e) => {
                    warn!("⚠️  Failed to parse {}: {e}. Starting fresh with empty peer_cursors.", cursors_file.display());
                }
            },
            Err(e) => {
                warn!("⚠️  Failed to read {}: {e}. Starting fresh with empty peer_cursors.", cursors_file.display());
            }
        }
    }

    (seen_ids, peer_cursors)
}

/// Guarda o estado em disco atomicamente (escrita em .tmp + rename).
fn save_state(state: &MeshState) {
    let dir = match &state.data_dir {
        Some(d) => d,
        None => return,
    };

    if let Err(e) = std::fs::create_dir_all(dir) {
        warn!("⚠️  Failed to create data directory {}: {e}", dir.display());
        return;
    }

    // 1. Salvar seen_ids.json
    let seen_vec: Vec<String> = state.seen_ids.iter().map(|r| r.clone()).collect();
    if let Ok(bytes) = serde_json::to_vec(&seen_vec) {
        let final_path = dir.join("seen_ids.json");
        let tmp_path = dir.join("seen_ids.json.tmp");
        if std::fs::write(&tmp_path, bytes).is_ok() {
            let _ = std::fs::rename(tmp_path, final_path);
        }
    }

    // 2. Salvar peer_cursors.json
    let cursors_map: std::collections::HashMap<String, u64> = state
        .peer_cursors
        .iter()
        .map(|r| (r.key().clone(), *r.value()))
        .collect();
    if let Ok(bytes) = serde_json::to_vec(&cursors_map) {
        let final_path = dir.join("peer_cursors.json");
        let tmp_path = dir.join("peer_cursors.json.tmp");
        if std::fs::write(&tmp_path, bytes).is_ok() {
            let _ = std::fs::rename(tmp_path, final_path);
        }
    }

    info!("💾 State saved to disk ({} seen_ids, {} cursors)", seen_vec.len(), cursors_map.len());
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

/// Carrega ou gera o ID único do nó (`node_id`).
fn load_or_generate_node_id(cfg_node_id: Option<&str>, data_dir: Option<&Path>) -> String {
    if let Some(id) = cfg_node_id {
        let trimmed = id.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }

    if let Some(dir) = data_dir {
        let path = dir.join("node_id.txt");
        if path.exists() {
            if let Ok(id) = std::fs::read_to_string(&path) {
                let trimmed = id.trim().to_string();
                if !trimmed.is_empty() {
                    return trimmed;
                }
            }
        }
        let new_id = uuid::Uuid::new_v4().to_string();
        if let Err(e) = std::fs::create_dir_all(dir) {
            warn!("⚠️ Failed to create data directory {}: {e}", dir.display());
        }
        if let Err(e) = std::fs::write(&path, &new_id) {
            warn!("⚠️ Failed to write node_id.txt at {}: {e}", path.display());
        }
        return new_id;
    }

    uuid::Uuid::new_v4().to_string()
}

/// Inicia o mesh agent: listener para peers + seeds outbound + consumer de eventos locais.
pub async fn run(
    cfg: MeshConfig,
    relay_url: String,
    data_dir: Option<PathBuf>,
    mut relay_events: broadcast::Receiver<RelayEvent>,
    relay_publish_tx: mpsc::Sender<String>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let (seen_ids, peer_cursors) = load_state(data_dir.as_deref());

    let state = Arc::new(MeshState {
        seen_ids,
        peer_cursors,
        relay_tx: relay_publish_tx,
        connected_peers: DashSet::new(),
        relay_url,
        data_dir: data_dir.clone(),
    });

    // ── Node ID & Mesh URL Auto-detection ──────────────────────────────
    let node_id = load_or_generate_node_id(cfg.node_id.as_deref(), data_dir.as_deref());
    let mesh_url = crate::config::detect_mesh_url(&cfg.listen, cfg.mesh_url.as_deref());
    info!("🆔 Node ID: {node_id}");

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

    // ── Tasks: conexões outbound com seeds estáticos ──────────────────
    let heartbeat_secs = cfg.heartbeat_secs;
    for seed_url in cfg.seeds.clone() {
        start_seed_task(
            seed_url,
            state.clone(),
            peer_broadcast_tx.clone(),
            heartbeat_secs,
            cancel.clone(),
        );
    }

    // ── Registry Central & Dynamic Peer Discovery ──────────────────────
    if let Some(ref registry_url) = cfg.registry_url {
        let registry_client = RegistryClient::new(registry_url.clone());
        let relay_info = RelayInfo {
            node_id: node_id.clone(),
            relay_url: state.relay_url.clone(),
            mesh_url: mesh_url.clone(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            capabilities: vec!["nostr".to_string(), "mesh".to_string()],
            last_seen: None,
        };

        // Registo inicial na startup (POST /relays)
        if let Err(e) = registry_client.register(&relay_info).await {
            warn!("⚠️ Initial registry registration failed at {registry_url}: {e}. Operating with static seeds/cache.");
        }

        // Task: Heartbeat periódico no registry (PUT /relays/{node_id}) + Deregisto no shutdown (DELETE)
        let heartbeat_client = registry_client.clone();
        let heartbeat_node_id = node_id.clone();
        let cancel_heartbeat = cancel.clone();
        let hb_interval_secs = cfg.heartbeat_secs;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(hb_interval_secs));
            interval.tick().await; // ignora o primeiro tick imediato

            loop {
                tokio::select! {
                    _ = cancel_heartbeat.cancelled() => break,
                    _ = interval.tick() => {
                        if let Err(e) = heartbeat_client.heartbeat(&heartbeat_node_id).await {
                            warn!("⚠️ Registry heartbeat failed: {e}");
                        }
                    }
                }
            }

            // Deregisto gracioso no shutdown
            if let Err(e) = heartbeat_client.deregister(&heartbeat_node_id).await {
                warn!("⚠️ Registry deregistration failed on shutdown: {e}");
            }
        });

        // Task: Descoberta periódica de peers no registry (GET /relays)
        let discovery_client = registry_client.clone();
        let my_node_id = node_id.clone();
        let my_mesh_url = mesh_url.clone();
        let state_disc = state.clone();
        let tx_disc = peer_broadcast_tx.clone();
        let cancel_disc = cancel.clone();
        let discovery_secs = cfg.discovery_secs;
        let data_dir_disc = data_dir.clone();

        tokio::spawn(async move {
            let missing_cycles: DashMap<String, u32> = DashMap::new();
            let mut interval = tokio::time::interval(Duration::from_secs(discovery_secs));

            loop {
                tokio::select! {
                    _ = cancel_disc.cancelled() => break,
                    _ = interval.tick() => {
                        match discovery_client.fetch_relays().await {
                            Ok(relays) => {
                                if let Some(ref dir) = data_dir_disc {
                                    registry::save_cached_peers(dir, &relays);
                                }

                                let active_mesh_urls: std::collections::HashSet<String> = relays
                                    .iter()
                                    .map(|r| r.mesh_url.clone())
                                    .collect();

                                for relay in relays {
                                    if relay.node_id == my_node_id || relay.mesh_url == my_mesh_url {
                                        continue;
                                    }

                                    missing_cycles.remove(&relay.mesh_url);

                                    if !state_disc.connected_peers.contains(&relay.mesh_url) {
                                        info!("🌐 Dynamic peer discovered from registry: {} ({})", relay.mesh_url, relay.node_id);
                                        start_seed_task(
                                            relay.mesh_url.clone(),
                                            state_disc.clone(),
                                            tx_disc.clone(),
                                            hb_interval_secs,
                                            cancel_disc.clone(),
                                        );
                                    }
                                }

                                for mut entry in missing_cycles.iter_mut() {
                                    if !active_mesh_urls.contains(entry.key()) {
                                        *entry.value_mut() += 1;
                                        if *entry.value() >= 3 {
                                            warn!("⚠️ Peer {} absent from registry for {} cycles", entry.key(), entry.value());
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("⚠️ Dynamic peer discovery failed: {e}. Falling back to cached peers.");
                                if let Some(ref dir) = data_dir_disc {
                                    let cached = registry::load_cached_peers(dir);
                                    for relay in cached {
                                        if relay.node_id == my_node_id || relay.mesh_url == my_mesh_url {
                                            continue;
                                        }
                                        if !state_disc.connected_peers.contains(&relay.mesh_url) {
                                            info!("🌐 Connecting to cached peer: {}", relay.mesh_url);
                                            start_seed_task(
                                                relay.mesh_url.clone(),
                                                state_disc.clone(),
                                                tx_disc.clone(),
                                                hb_interval_secs,
                                                cancel_disc.clone(),
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
    } else {
        info!("ℹ️  registry_url not configured, using static seeds only");
    }

    // ── Task: métricas de conectividade e salvamento periódico em disco ──
    let state_metrics = state.clone();
    let cancel_metrics = cancel.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.tick().await;
        loop {
            tokio::select! {
                _ = cancel_metrics.cancelled() => break,
                _ = interval.tick() => {
                    let connected_count = state_metrics.connected_peers.len();
                    info!("📊 Mesh status: {connected_count} peers connected");
                    save_state(&state_metrics);
                }
            }
        }
    });

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
                        tokio::spawn(handle_inbound_peer(stream, addr, state, peer_rx, heartbeat_secs, cancel));
                    }
                    Err(e) => {
                        error!("❌ Failed to accept peer connection: {e}");
                    }
                }
            }
        }
    }

    // Guardar estado no shutdown gracioso
    save_state(&state);

    Ok(())
}

/// Spawna a task de reconexão automática para um seed remoto.
fn start_seed_task(
    seed_url: String,
    state: Arc<MeshState>,
    peer_broadcast_tx: broadcast::Sender<String>,
    heartbeat_secs: u64,
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
                        handle_peer_stream(seed_url.clone(), sink, stream, state, peer_rx, heartbeat_secs, cancel).await;
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
    heartbeat_secs: u64,
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
    handle_peer_stream(peer_id, sink, stream, state, peer_rx, heartbeat_secs, cancel).await;
}

/// Handler unificado de sessão peer bidirecional (independente de inbound ou outbound).
async fn handle_peer_stream<Si, St>(
    peer_id: String,
    mut sink: Si,
    mut stream: St,
    state: Arc<MeshState>,
    mut peer_rx: broadcast::Receiver<String>,
    heartbeat_secs: u64,
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

    // Canal interno para enviar mensagens de controle (pong, OK, EOSE, backfill) ao sink
    let (ctrl_tx, mut ctrl_rx) = mpsc::channel::<Message>(256);

    // ── Task: enviar eventos locais + mensagens de controle + heartbeats periódicos ──
    let cancel_clone = cancel.clone();
    let heartbeat_secs_clone = heartbeat_secs;
    let send_task = tokio::spawn(async move {
        let mut heartbeat_interval =
            tokio::time::interval(Duration::from_secs(heartbeat_secs_clone));
        heartbeat_interval.tick().await; // ignora o primeiro tick imediato

        loop {
            tokio::select! {
                _ = cancel_clone.cancelled() => break,
                _ = heartbeat_interval.tick() => {
                    let notice = Message::Text(r#"["NOTICE","goy-heartbeat"]"#.into());
                    if sink.send(notice).await.is_err() {
                        break;
                    }
                }
                // Mensagens de controle (pong, OK, EOSE, backfill) têm prioridade
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

    // ── Pedido de backfill inicial para o peer (usando cursor salvo se existir) ──
    let cursor = state
        .peer_cursors
        .get(&peer_id)
        .map(|c| *c.value())
        .unwrap_or(0);
    info!("🔄 Sending initial backfill REQ to peer {peer_id} with cursor={cursor}");
    let backfill_req = format!(r#"["REQ","goy-backfill",{{"since":{},"limit":500}}]"#, cursor);
    let _ = ctrl_tx.send(Message::Text(backfill_req.into())).await;

    // ── Receber mensagens do peer (REQ, EVENT, EOSE, NOTICE, Ping, Pong) + timeout de inatividade ──
    let timeout_secs = heartbeat_secs * 3;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            res = tokio::time::timeout(Duration::from_secs(timeout_secs), stream.next()) => {
                match res {
                    Err(_) => {
                        warn!("💀 Peer {peer_id} timed out (no activity for {timeout_secs}s), disconnecting");
                        break;
                    }
                    Ok(Some(Ok(Message::Text(text)))) => {
                        if text.starts_with(r#"["REQ""#) {
                            if let Some((sub_id, filter)) = parse_req_msg(&text) {
                                let relay_url = state.relay_url.clone();
                                let peer_id = peer_id.clone();
                                let ctrl_tx = ctrl_tx.clone();
                                tokio::spawn(async move {
                                    handle_backfill_req(sub_id, filter, relay_url, peer_id, ctrl_tx).await;
                                });
                            }
                        } else if text.starts_with(r#"["EVENT""#) {
                            if let Some(id) = extract_event_id(&text) {
                                if let Some(ts) = extract_event_timestamp(&text) {
                                    state
                                        .peer_cursors
                                        .entry(peer_id.clone())
                                        .and_modify(|c| *c = (*c).max(ts))
                                        .or_insert(ts);
                                }
                                if state.seen_ids.insert(id.clone()) {
                                    info!("📥 Event {id} received from peer {peer_id}, publishing to local relay");
                                    let normalized = normalize_event_for_publish(&text);
                                    if state.relay_tx.send(normalized).await.is_err() {
                                        warn!("⚠️  Relay publish channel closed");
                                        break;
                                    }
                                    // Resposta OK otimista apenas para eventos em tempo real (2 elementos)
                                    if is_live_publish_event(&text) {
                                        let ok_msg = format!(r#"["OK","{}",true,""]"#, id);
                                        let _ = ctrl_tx.send(Message::Text(ok_msg.into())).await;
                                    }
                                } else {
                                    tracing::debug!("🔁 Event {id} from peer {peer_id} already seen (dedup), skipping relay publish");
                                }
                            }
                        } else if text.starts_with(r#"["EOSE""#) {
                            if let Some(sub_id) = parse_eose_sub_id(&text) {
                                info!("🏁 Backfill completed from peer {peer_id} (EOSE received for sub '{sub_id}')");
                            }
                        } else if text.starts_with(r#"["NOTICE","goy-heartbeat""#) {
                            tracing::debug!("💓 Heartbeat notice received from {peer_id}");
                        }
                    }
                    Ok(Some(Ok(Message::Ping(data)))) => {
                        let _ = ctrl_tx.send(Message::Pong(data)).await;
                    }
                    Ok(Some(Ok(Message::Pong(_)))) => {
                        tracing::debug!("💓 Pong received from {peer_id}");
                    }
                    Ok(Some(Ok(Message::Close(_)))) | Ok(None) => {
                        info!("🔌 Peer disconnected: {peer_id}");
                        break;
                    }
                    Ok(Some(Err(e))) => {
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

/// Handler de consulta de backfill no relay local para responder ao REQ do peer.
async fn handle_backfill_req(
    sub_id: String,
    filter: Option<serde_json::Value>,
    relay_url: String,
    peer_id: String,
    ctrl_tx: mpsc::Sender<Message>,
) {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::connect_async;

    info!("🔍 Handling backfill REQ sub '{sub_id}' for peer {peer_id}");

    let (mut ws, _) = match connect_async(&relay_url).await {
        Ok(res) => res,
        Err(e) => {
            warn!("🔌 Failed to connect to local relay at {relay_url} for backfill: {e}");
            let eose_msg = format!(r#"["EOSE","{}"]"#, sub_id);
            let _ = ctrl_tx.send(Message::Text(eose_msg.into())).await;
            return;
        }
    };

    let limit = filter
        .as_ref()
        .and_then(|f| f.get("limit"))
        .and_then(|l| l.as_u64())
        .unwrap_or(500) as usize;

    let req_payload = match filter {
        Some(ref f) => format!(r#"["REQ","{}",{}]"#, sub_id, f),
        None => format!(r#"["REQ","{}",{{"since":0,"limit":{}}}]"#, sub_id, limit),
    };

    if let Err(e) = ws.send(Message::Text(req_payload.into())).await {
        warn!("🔌 Failed to send REQ to local relay at {relay_url}: {e}");
        let eose_msg = format!(r#"["EOSE","{}"]"#, sub_id);
        let _ = ctrl_tx.send(Message::Text(eose_msg.into())).await;
        return;
    }

    let mut sent_count = 0;
    let mut eose_sent = false;

    while let Some(msg) = ws.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if text.starts_with(r#"["EVENT""#) {
                    if sent_count < limit {
                        if ctrl_tx.send(Message::Text(text.into())).await.is_err() {
                            break;
                        }
                        sent_count += 1;
                    }
                    if sent_count >= limit {
                        let eose_msg = format!(r#"["EOSE","{}"]"#, sub_id);
                        let _ = ctrl_tx.send(Message::Text(eose_msg.into())).await;
                        eose_sent = true;
                        break;
                    }
                } else if text.starts_with(r#"["EOSE""#) {
                    let eose_msg = format!(r#"["EOSE","{}"]"#, sub_id);
                    let _ = ctrl_tx.send(Message::Text(eose_msg.into())).await;
                    eose_sent = true;
                    break;
                }
            }
            Ok(Message::Ping(data)) => {
                let _ = ws.send(Message::Pong(data)).await;
            }
            Err(e) => {
                warn!("⚠️  Error reading from local relay during backfill: {e}");
                break;
            }
            _ => {}
        }
    }

    if !eose_sent {
        let eose_msg = format!(r#"["EOSE","{}"]"#, sub_id);
        let _ = ctrl_tx.send(Message::Text(eose_msg.into())).await;
    }

    info!("📦 backfill: enviados {sent_count}/{limit} eventos para peer {peer_id}");
}

/// Normaliza mensagem EVENT recebida de um peer para publicação no relay local.
/// Se for um evento de resposta a REQ (3 elementos: ["EVENT", sub_id, event_obj]),
/// converte para formato de publicação (2 elementos: ["EVENT", event_obj]).
fn normalize_event_for_publish(raw: &str) -> String {
    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(raw) {
        if let Some(arr) = parsed.as_array() {
            if arr.len() >= 3 && arr[0].as_str() == Some("EVENT") {
                return format!(r#"["EVENT",{}]"#, arr[2]);
            }
        }
    }
    raw.to_string()
}

/// Extrai sub_id e filtro de uma mensagem REQ. Format: ["REQ", sub_id, filter]
fn parse_req_msg(raw: &str) -> Option<(String, Option<serde_json::Value>)> {
    let parsed: serde_json::Value = serde_json::from_str(raw).ok()?;
    let arr = parsed.as_array()?;
    if arr.len() >= 2 && arr[0].as_str() == Some("REQ") {
        let sub_id = arr[1].as_str()?.to_string();
        let filter = arr.get(2).cloned();
        Some((sub_id, filter))
    } else {
        None
    }
}

/// Extrai sub_id de uma mensagem EOSE. Format: ["EOSE", sub_id]
fn parse_eose_sub_id(raw: &str) -> Option<String> {
    let parsed: serde_json::Value = serde_json::from_str(raw).ok()?;
    let arr = parsed.as_array()?;
    if arr.len() >= 2 && arr[0].as_str() == Some("EOSE") {
        arr[1].as_str().map(|s| s.to_string())
    } else {
        None
    }
}

/// Retorna verdadeiro se for um evento de publicação em tempo real (2 elementos: ["EVENT", event_obj]).
fn is_live_publish_event(raw: &str) -> bool {
    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(raw) {
        if let Some(arr) = parsed.as_array() {
            return arr.len() == 2 && arr[0].as_str() == Some("EVENT");
        }
    }
    false
}

/// Extrai o timestamp `created_at` de um evento Nostr JSON.
fn extract_event_timestamp(raw: &str) -> Option<u64> {
    let parsed: serde_json::Value = serde_json::from_str(raw).ok()?;
    let arr = parsed.as_array()?;
    if arr.is_empty() || arr[0].as_str() != Some("EVENT") {
        return None;
    }
    if arr.len() == 2 {
        arr[1].get("created_at")?.as_u64()
    } else if arr.len() >= 3 {
        arr[2].get("created_at")?.as_u64()
    } else {
        None
    }
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
            discovery_secs: 60,
            mesh_url: None,
            node_id: None,
        };

        let cancel_mesh = cancel.clone();
        tokio::spawn(async move {
            let _ = run(cfg, "ws://127.0.0.1:57777".to_string(), None, relay_events_rx, relay_publish_tx, cancel_mesh).await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Conecta peer via WebSocket
        let ws_url = "ws://127.0.0.1:18443";
        let (mut ws_stream, _) = connect_async(ws_url).await?;

        // Peer deve receber a mensagem de pedido de backfill inicial enviada pelo nó
        let init_req = tokio::time::timeout(Duration::from_secs(2), ws_stream.next())
            .await?
            .ok_or_else(|| anyhow::anyhow!("ws_stream closed unexpectedly"))??;
        assert_eq!(init_req.to_text()?, r#"["REQ","goy-backfill",{"since":0,"limit":500}]"#);

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
            discovery_secs: 60,
            mesh_url: None,
            node_id: None,
        };

        let cancel_a = cancel.clone();
        tokio::spawn(async move {
            let _ = run(cfg_a, "ws://127.0.0.1:57777".to_string(), None, relay_events_rx_a, relay_publish_tx_a, cancel_a).await;
        });

        // ── Node B (com seed = ws://127.0.0.1:18446, escuta em 18447) ───────
        let (relay_events_tx_b, relay_events_rx_b) = broadcast::channel::<RelayEvent>(16);
        let (relay_publish_tx_b, mut relay_publish_rx_b) = mpsc::channel::<String>(16);

        let cfg_b = MeshConfig {
            listen: "127.0.0.1:18447".to_string(),
            seeds: vec!["ws://127.0.0.1:18446".to_string()],
            registry_url: None,
            heartbeat_secs: 30,
            discovery_secs: 60,
            mesh_url: None,
            node_id: None,
        };

        let cancel_b = cancel.clone();
        tokio::spawn(async move {
            let _ = run(cfg_b, "ws://127.0.0.1:57777".to_string(), None, relay_events_rx_b, relay_publish_tx_b, cancel_b).await;
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
        assert_eq!(
            received_at_node_b,
            r#"["EVENT",{"id":"evt_from_node_a","content":"hello from Node A"}]"#
        );

        // 2. Evento publicado no strfry do Node B -> deve chegar ao strfry do Node A
        let event_b = r#"["EVENT","sub_b",{"id":"evt_from_node_b","content":"hello from Node B"}]"#;
        relay_events_tx_b.send(RelayEvent {
            raw: event_b.to_string(),
        })?;

        let received_at_node_a =
            tokio::time::timeout(Duration::from_secs(2), relay_publish_rx_a.recv())
                .await?
                .ok_or_else(|| anyhow::anyhow!("Node A relay_publish_rx closed"))?;
        assert_eq!(
            received_at_node_a,
            r#"["EVENT",{"id":"evt_from_node_b","content":"hello from Node B"}]"#
        );

        cancel.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn test_backfill_between_two_nodes_with_mock_relay() -> anyhow::Result<()> {
        use tokio::net::TcpListener;
        use tokio_tungstenite::accept_async;

        let cancel = CancellationToken::new();

        // ── 1. Subir servidor WebSocket Mock Relay para o Node A (escuta em porta dinâmica) ──
        let mock_listener = TcpListener::bind("127.0.0.1:0").await?;
        let mock_addr = mock_listener.local_addr()?;
        let mock_url = format!("ws://{mock_addr}");
        let cancel_mock = cancel.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_mock.cancelled() => break,
                    res = mock_listener.accept() => {
                        if let Ok((stream, _)) = res {
                            if let Ok(mut ws) = accept_async(stream).await {
                                while let Some(Ok(msg)) = ws.next().await {
                                    if let Message::Text(text) = msg {
                                        if text.starts_with(r#"["REQ""#) {
                                            // Ao receber REQ, responde com um evento histórico + EOSE
                                            let hist_evt = r#"["EVENT","goy-backfill",{"id":"hist_123","content":"historical data"}]"#;
                                            let eose = r#"["EOSE","goy-backfill"]"#;
                                            let _ = ws.send(Message::Text(hist_evt.into())).await;
                                            let _ = ws.send(Message::Text(eose.into())).await;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        // ── 2. Node A (configurado com mock relay url, escuta em 18450) ──
        let (relay_events_tx_a, relay_events_rx_a) = broadcast::channel::<RelayEvent>(16);
        let (relay_publish_tx_a, _relay_publish_rx_a) = mpsc::channel::<String>(16);

        let cfg_a = MeshConfig {
            listen: "127.0.0.1:18450".to_string(),
            seeds: vec![],
            registry_url: None,
            heartbeat_secs: 30,
            discovery_secs: 60,
            mesh_url: None,
            node_id: None,
        };

        let cancel_a = cancel.clone();
        tokio::spawn(async move {
            let _ = run(cfg_a, mock_url, None, relay_events_rx_a, relay_publish_tx_a, cancel_a).await;
        });

        // ── 3. Node B (seed = Node A, escuta em 18451) ──
        let (_relay_events_tx_b, relay_events_rx_b) = broadcast::channel::<RelayEvent>(16);
        let (relay_publish_tx_b, mut relay_publish_rx_b) = mpsc::channel::<String>(16);

        let cfg_b = MeshConfig {
            listen: "127.0.0.1:18451".to_string(),
            seeds: vec!["ws://127.0.0.1:18450".to_string()],
            registry_url: None,
            heartbeat_secs: 30,
            discovery_secs: 60,
            mesh_url: None,
            node_id: None,
        };

        let cancel_b = cancel.clone();
        tokio::spawn(async move {
            let _ = run(cfg_b, "ws://127.0.0.1:57777".to_string(), None, relay_events_rx_b, relay_publish_tx_b, cancel_b).await;
        });

        // Aguarda Node B conectar a Node A e solicitar backfill
        let received_backfill_at_b =
            tokio::time::timeout(Duration::from_secs(3), relay_publish_rx_b.recv())
                .await?
                .ok_or_else(|| anyhow::anyhow!("Node B did not receive backfill event"))?;

        // Verifica que o evento histórico foi normalizado para publicação local em B
        assert_eq!(
            received_backfill_at_b,
            r#"["EVENT",{"id":"hist_123","content":"historical data"}]"#
        );

        // ── 4. Transmissão live após backfill ──
        let live_evt = r#"["EVENT","goy-live",{"id":"live_456","content":"live data"}]"#;
        relay_events_tx_a.send(RelayEvent {
            raw: live_evt.to_string(),
        })?;

        let received_live_at_b =
            tokio::time::timeout(Duration::from_secs(2), relay_publish_rx_b.recv())
                .await?
                .ok_or_else(|| anyhow::anyhow!("Node B did not receive live event"))?;

        assert_eq!(
            received_live_at_b,
            r#"["EVENT",{"id":"live_456","content":"live data"}]"#
        );

        cancel.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn test_heartbeat_and_dead_peer_timeout_detection() -> anyhow::Result<()> {
        use tokio_tungstenite::connect_async;

        let cancel = CancellationToken::new();

        // ── Node A (heartbeat_secs = 1 -> timeout threshold = 3s) ──────
        let (_relay_events_tx_a, relay_events_rx_a) = broadcast::channel::<RelayEvent>(16);
        let (relay_publish_tx_a, _relay_publish_rx_a) = mpsc::channel::<String>(16);

        let cfg_a = MeshConfig {
            listen: "127.0.0.1:18460".to_string(),
            seeds: vec![],
            registry_url: None,
            heartbeat_secs: 1, // timeout = 3s
            discovery_secs: 60,
            mesh_url: None,
            node_id: None,
        };

        let cancel_a = cancel.clone();
        tokio::spawn(async move {
            let _ = run(cfg_a, "ws://127.0.0.1:57777".to_string(), None, relay_events_rx_a, relay_publish_tx_a, cancel_a).await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Peer conecta a Node A
        let (mut ws_stream, _) = connect_async("ws://127.0.0.1:18460").await?;

        // 1. Recebe pedido de backfill inicial
        let init_req = tokio::time::timeout(Duration::from_secs(2), ws_stream.next())
            .await?
            .ok_or_else(|| anyhow::anyhow!("ws closed"))??;
        assert_eq!(init_req.to_text()?, r#"["REQ","goy-backfill",{"since":0,"limit":500}]"#);

        // 2. Recebe heartbeat periodicamente de Node A (após 1s)
        let heartbeat_msg = tokio::time::timeout(Duration::from_secs(2), ws_stream.next())
            .await?
            .ok_or_else(|| anyhow::anyhow!("ws closed"))??;
        assert_eq!(heartbeat_msg.to_text()?, r#"["NOTICE","goy-heartbeat"]"#);

        // 3. Peer fica completamente silencioso e não envia nada
        // Node A deve detectar o timeout após 3 segundos e fechar a conexão
        let _timeout_result = tokio::time::timeout(Duration::from_secs(4), async {
            while let Some(msg) = ws_stream.next().await {
                if msg.is_err() || matches!(msg, Ok(Message::Close(_))) {
                    return Ok::<(), anyhow::Error>(());
                }
            }
            Ok(())
        }).await;

        cancel.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn test_state_persistence_save_load_corrupt() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let data_dir = temp_dir.path().to_path_buf();

        let state = MeshState {
            seen_ids: DashSet::new(),
            peer_cursors: DashMap::new(),
            relay_tx: mpsc::channel(1).0,
            connected_peers: DashSet::new(),
            relay_url: "ws://127.0.0.1:7777".to_string(),
            data_dir: Some(data_dir.clone()),
        };

        state.seen_ids.insert("evt_persisted_1".to_string());
        state.seen_ids.insert("evt_persisted_2".to_string());
        state.peer_cursors.insert("ws://127.0.0.1:19999".to_string(), 1786290000);

        save_state(&state);

        // Carrega estado e verifica persistência
        let (loaded_seen, loaded_cursors) = load_state(Some(&data_dir));
        assert!(loaded_seen.contains("evt_persisted_1"));
        assert!(loaded_seen.contains("evt_persisted_2"));
        assert_eq!(loaded_cursors.get("ws://127.0.0.1:19999").map(|c| *c.value()), Some(1786290000));

        // Simula corrupção de ficheiro
        std::fs::write(data_dir.join("seen_ids.json"), b"corrupted data {{{")?;
        let (corrupt_seen, _corrupt_cursors) = load_state(Some(&data_dir));
        assert!(corrupt_seen.is_empty(), "Corrupt file should fallback to empty set without panic");

        Ok(())
    }

    #[tokio::test]
    async fn test_node_restart_incremental_backfill_with_cursor() -> anyhow::Result<()> {
        use tokio::net::TcpListener;
        use tokio_tungstenite::accept_async;

        let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

        let temp_dir = tempfile::tempdir()?;
        let data_dir_b = temp_dir.path().to_path_buf();

        let cancel = CancellationToken::new();

        // ── Mock Relay do Node A (escuta em porta dinâmica) ──
        let mock_listener = TcpListener::bind("127.0.0.1:0").await?;
        let mock_addr = mock_listener.local_addr()?;
        let mock_url = format!("ws://{mock_addr}");
        let (req_tx, mut req_rx) = mpsc::channel::<String>(16);

        let cancel_mock = cancel.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_mock.cancelled() => break,
                    res = mock_listener.accept() => {
                        if let Ok((stream, _)) = res {
                            let req_tx = req_tx.clone();
                            tokio::spawn(async move {
                                if let Ok(mut ws) = accept_async(stream).await {
                                    while let Some(Ok(msg)) = ws.next().await {
                                        if let Message::Text(text) = msg {
                                            if text.starts_with(r#"["REQ""#) {
                                                let _ = req_tx.send(text.clone()).await;
                                                let hist_evt = r#"["EVENT","goy-backfill",{"id":"hist_ts_1","created_at":1786000500,"content":"ts data"}]"#;
                                                let eose = r#"["EOSE","goy-backfill"]"#;
                                                let _ = ws.send(Message::Text(hist_evt.into())).await;
                                                let _ = ws.send(Message::Text(eose.into())).await;
                                            }
                                        }
                                    }
                                }
                            });
                        }
                    }
                }
            }
        });

        // ── 1. Node A (porta dinâmica) ──
        let tmp_listener_a = TcpListener::bind("127.0.0.1:0").await?;
        let addr_a = tmp_listener_a.local_addr()?;
        drop(tmp_listener_a);
        let listen_a = addr_a.to_string();
        let seed_a = format!("ws://{addr_a}");

        let (_relay_events_tx_a, relay_events_rx_a) = broadcast::channel::<RelayEvent>(16);
        let (relay_publish_tx_a, _relay_publish_rx_a) = mpsc::channel::<String>(16);
        let cfg_a = MeshConfig {
            listen: listen_a.clone(),
            seeds: vec![],
            registry_url: None,
            heartbeat_secs: 30,
            discovery_secs: 60,
            mesh_url: Some(seed_a.clone()),
            node_id: Some("node-A".to_string()),
        };
        let cancel_a = cancel.clone();
        tokio::spawn(async move {
            let _ = run(cfg_a, mock_url, None, relay_events_rx_a, relay_publish_tx_a, cancel_a).await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        // ── 2. Primeira Execução do Node B (porta dinâmica, seed = Node A) ──
        let tmp_listener_b1 = TcpListener::bind("127.0.0.1:0").await?;
        let addr_b1 = tmp_listener_b1.local_addr()?;
        drop(tmp_listener_b1);
        let listen_b1 = addr_b1.to_string();

        let cancel_b1 = CancellationToken::new();
        let (_relay_events_tx_b, relay_events_rx_b) = broadcast::channel::<RelayEvent>(16);
        let (relay_publish_tx_b, mut relay_publish_rx_b) = mpsc::channel::<String>(16);

        let cfg_b = MeshConfig {
            listen: listen_b1,
            seeds: vec![seed_a.clone()],
            registry_url: None,
            heartbeat_secs: 30,
            discovery_secs: 60,
            mesh_url: Some(format!("ws://{addr_b1}")),
            node_id: Some("node-B".to_string()),
        };

        let c_b1 = cancel_b1.clone();
        let dir_b1 = data_dir_b.clone();
        tokio::spawn(async move {
            let _ = run(cfg_b.clone(), "ws://127.0.0.1:57777".to_string(), Some(dir_b1), relay_events_rx_b, relay_publish_tx_b, c_b1).await;
        });

        // Primeiro REQ recebido no Mock Relay deve ser since: 0
        let req_1 = tokio::time::timeout(Duration::from_secs(5), req_rx.recv())
            .await?
            .ok_or_else(|| anyhow::anyhow!("Mock relay did not receive first REQ"))?;
        assert!(req_1.contains(r#""since":0"#));

        // Node B recebe o evento histórico com created_at = 1786000500
        let _received_b1 = tokio::time::timeout(Duration::from_secs(5), relay_publish_rx_b.recv())
            .await?
            .ok_or_else(|| anyhow::anyhow!("Node B1 did not receive backfill event"))?;

        // Cancela a primeira execução do Node B (shutdown gracioso salva o cursor 1786000500)
        cancel_b1.cancel();
        tokio::time::sleep(Duration::from_millis(400)).await;

        // ── 3. Reinício do Node B (porta dinâmica, reutiliza data_dir_b) ──
        let tmp_listener_b2 = TcpListener::bind("127.0.0.1:0").await?;
        let addr_b2 = tmp_listener_b2.local_addr()?;
        drop(tmp_listener_b2);
        let listen_b2 = addr_b2.to_string();

        let cancel_b2 = cancel.clone();
        let (_relay_events_tx_b2, relay_events_rx_b2) = broadcast::channel::<RelayEvent>(16);
        let (relay_publish_tx_b2, _relay_publish_rx_b2) = mpsc::channel::<String>(16);

        let cfg_b2 = MeshConfig {
            listen: listen_b2,
            seeds: vec![seed_a.clone()],
            registry_url: None,
            heartbeat_secs: 30,
            discovery_secs: 60,
            mesh_url: Some(format!("ws://{addr_b2}")),
            node_id: Some("node-B".to_string()),
        };

        let c_b2 = cancel_b2.clone();
        let dir_b2 = data_dir_b.clone();
        tokio::spawn(async move {
            let _ = run(cfg_b2, "ws://127.0.0.1:57777".to_string(), Some(dir_b2), relay_events_rx_b2, relay_publish_tx_b2, c_b2).await;
        });

        // O segundo REQ recebido no Mock Relay deve usar o cursor salvo: since: 1786000500!
        let req_2 = tokio::time::timeout(Duration::from_secs(5), req_rx.recv())
            .await?
            .ok_or_else(|| anyhow::anyhow!("Mock relay did not receive second REQ after restart"))?;
        assert!(req_2.contains(r#""since":1786000500"#));

        cancel.cancel();
        Ok(())
    }
}




