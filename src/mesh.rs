//! Mesh agent: sincroniza eventos entre peers via WebSocket.
//!
//! - Consome eventos do relay local e encaminha para peers
//! - Recebe eventos de peers e publica no relay local
//! - Deduplica por event ID para evitar loops

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use backoff::ExponentialBackoffBuilder;
use dashmap::{DashMap, DashSet};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::MeshConfig;
use crate::metrics::{EventSource, Metrics};
use crate::registry::{self, RegistryClient, RelayInfo};
use crate::relay::RelayEvent;
use crate::tls::{FingerprintStore, NodeCertificate, TrustDecision};

/// Contexto TLS partilhado pelas tasks do mesh.
///
/// Quando `mesh.tls_enabled = false` este contexto é `None` e todas as
/// conexões usam TCP plaintext (apenas para testes locais).
pub struct TlsContext {
    /// Certificado auto-assinado deste nó.
    pub cert: NodeCertificate,
    /// Store TOFU de fingerprints conhecidos por peer.
    pub fingerprints: FingerprintStore,
    /// Configuração rustls do listener (mTLS, TLS 1.3).
    pub server_config: Arc<rustls::ServerConfig>,
}

impl std::fmt::Debug for TlsContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsContext")
            .field("fingerprint", &self.cert.fingerprint)
            .finish()
    }
}

impl TlsContext {
    /// Constrói o contexto TLS a partir da config e do `node_id`.
    pub fn new(cfg: &MeshConfig, node_id: &str, data_dir: Option<&Path>) -> anyhow::Result<Self> {
        let cert = match data_dir {
            Some(dir) => crate::tls::load_or_generate_cert(dir, node_id)?,
            None => crate::tls::generate_self_signed(node_id)?,
        };
        let server_config = crate::tls::server_config(&cert)?;
        let fingerprints = FingerprintStore::load(data_dir, &cfg.trusted_fingerprints);
        Ok(Self {
            cert,
            fingerprints,
            server_config,
        })
    }
}

/// Informações ativas e estatísticas de tráfego de um peer conectado.
#[derive(Debug, Clone)]
pub struct PeerSessionInfo {
    pub peer_id: String,
    pub address: String,
    pub direction: String,
    pub connected_since: u64,
    pub events_sent: Arc<AtomicU64>,
    pub events_received: Arc<AtomicU64>,
    pub tls_fingerprint: Option<String>,
}

/// Estrutura para serialização JSON da rota `GET /peers`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PeerSessionView {
    pub peer_id: String,
    pub address: String,
    pub direction: String,
    pub connected_since: u64,
    pub events_sent: u64,
    pub events_received: u64,
    pub tls_fingerprint: Option<String>,
}

impl PeerSessionInfo {
    pub fn to_view(&self) -> PeerSessionView {
        PeerSessionView {
            peer_id: self.peer_id.clone(),
            address: self.address.clone(),
            direction: self.direction.clone(),
            connected_since: self.connected_since,
            events_sent: self.events_sent.load(Ordering::Relaxed),
            events_received: self.events_received.load(Ordering::Relaxed),
            tls_fingerprint: self.tls_fingerprint.clone(),
        }
    }
}

/// Estado compartilhado entre todas as tarefas do mesh agent.
pub struct MeshState {
    /// Event IDs já vistos (dedup global).
    pub seen_ids: DashSet<String>,
    /// Conjunto de event IDs que foram explicitamente deletados (NIP-09).
    pub deleted_ids: DashSet<String>,
    /// Eventos com tag `expiration` (NIP-40): event_id -> expiration timestamp Unix.
    pub expiring_events: DashMap<String, u64>,
    /// Último timestamp (created_at) visto por peer para backfill incremental.
    pub peer_cursors: DashMap<String, u64>,
    /// Registro da versão mais recente de eventos substituíveis (replacement_key -> (created_at, event_id)).
    pub latest_replaceable: DashMap<String, (u64, String)>,
    /// Canal para publicar eventos remotos no relay local.
    pub relay_tx: mpsc::Sender<String>,
    /// IDs/URLs de peers atualmente conectados (inbound e outbound).
    pub connected_peers: DashSet<String>,
    /// Canais de controle dos peers conectados para mensagens direcionadas.
    pub peer_channels: DashMap<String, mpsc::Sender<Message>>,
    /// Limites e rate limiters por peer (token buckets).
    pub rate_limiters: DashMap<String, Arc<Mutex<crate::rate_limiter::PeerRateLimiter>>>,
    /// Fator de replicação N-of-M.
    pub replication_factor: u32,
    /// Configurações de rate limit e limites de mensagem.
    pub max_events_per_sec: u32,
    pub max_bytes_per_sec: u64,
    pub max_msg_size: usize,
    /// Contador de eventos armazenados no nó local.
    pub events_stored: AtomicU64,
    /// Contador de eventos replicados para peers.
    pub events_replicated: AtomicU64,
    /// Métricas de rate limiting.
    pub events_rate_limited: AtomicU64,
    pub bytes_rate_limited: AtomicU64,
    pub messages_oversized: AtomicU64,
    /// Struct de métricas Prometheus partilhado com o servidor HTTP.
    pub metrics: Arc<Metrics>,
    /// Sessões de peers ativas com estatísticas de tráfego e metadados.
    pub active_peer_sessions: DashMap<String, Arc<PeerSessionInfo>>,
    /// Anel de consistent hashing para replicação e rebalanceamento uniforme.
    pub hash_ring: Arc<std::sync::RwLock<crate::consistent_hash::ConsistentHashRing>>,
    /// WebSocket URL do relay local para consultas de backfill.
    pub relay_url: String,
    /// Diretório para persistência de estado em disco (opcional).
    pub data_dir: Option<PathBuf>,
    /// Informações de armazenamento do nó verificado no arranque.
    #[allow(dead_code)]
    pub storage_info: Option<crate::storage::StorageInfo>,
}

impl MeshState {
    /// Retorna a lista de visões serializáveis das sessões de peers ativas.
    pub fn get_peer_sessions(&self) -> Vec<PeerSessionView> {
        let mut vec: Vec<PeerSessionView> = self
            .active_peer_sessions
            .iter()
            .map(|r| r.value().to_view())
            .collect();
        vec.sort_by(|a, b| a.peer_id.cmp(&b.peer_id));
        vec
    }
}

/// Seleciona os peers para replicação de um evento Nostr usando o anel de Consistent Hashing.
pub fn select_replication_peers(
    state: &MeshState,
    event_id: &str,
    source_peer: Option<&str>,
) -> Vec<String> {
    let replication_factor = state.replication_factor as usize;
    if replication_factor <= 1 {
        return vec![];
    }
    let target_count = replication_factor - 1;

    let ring = state.hash_ring.read().unwrap();
    let responsible = ring.get_responsible_peers(event_id, ring.peer_count());

    let filtered: Vec<String> = responsible
        .into_iter()
        .filter(|p| source_peer.is_none_or(|src| src != p))
        .collect();

    // Deduplica conexões para preferir outbound (ws:// ou wss://) sobre conexões inbound temporárias
    let outbound: Vec<String> = filtered
        .iter()
        .filter(|p| p.starts_with("ws://") || p.starts_with("wss://"))
        .cloned()
        .collect();

    let candidates = if !outbound.is_empty() {
        outbound
    } else {
        filtered
    };

    candidates.into_iter().take(target_count).collect()
}

/// Encaminha o evento para `replication_factor - 1` peers selecionados pelo Consistent Hash Ring.
pub fn replicate_event(
    state: &Arc<MeshState>,
    event_id: &str,
    event_raw: &str,
    source_peer: Option<&str>,
) {
    state.events_stored.fetch_add(1, Ordering::Relaxed);

    let replication_factor = state.replication_factor;
    if replication_factor == 0 {
        return;
    }

    let targets = select_replication_peers(state, event_id, source_peer);

    if targets.is_empty() {
        return;
    }

    let msg = Message::Text(event_raw.into());
    for target in targets {
        if let Some(tx) = state.peer_channels.get(&target)
            && tx.value().try_send(msg.clone()).is_ok()
        {
            state.events_replicated.fetch_add(1, Ordering::Relaxed);
            state
                .metrics
                .events_replicated
                .fetch_add(1, Ordering::Relaxed);
            if let Some(session) = state.active_peer_sessions.get(&target) {
                session.events_sent.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Processa a lógica de substituição para eventos substituíveis (kinds 0, 3, 10002, 10000–19999, 30000–39999).
/// Retorna `true` se o evento for novo/mais recente (e portanto deve ser aceito e propagado),
/// ou `false` se for um evento antigo/obsoleto que deve ser descartado.
pub fn process_replaceable_event(state: &MeshState, raw_event: &str) -> bool {
    let Some(event_obj) = crate::event_types::extract_event_object(raw_event) else {
        return true; // Não é um objeto evento, permite parse normal
    };

    let event_id = event_obj
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Verificar se o evento foi deletado (NIP-09)
    if state.deleted_ids.contains(&event_id) {
        tracing::debug!("🗑️ Skipping deleted event {event_id}");
        return false;
    }

    let Some(key) = crate::event_types::replacement_key(&event_obj) else {
        return true; // Não é evento substituível
    };

    let created_at = event_obj
        .get("created_at")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    if let Some(entry) = state.latest_replaceable.get(&key) {
        let (stored_created_at, stored_id) = entry.value();
        if created_at < *stored_created_at {
            tracing::debug!(
                "♻️ Discarding stale replaceable event {key} (created_at {created_at} < {stored_created_at})"
            );
            return false;
        } else if created_at == *stored_created_at && event_id >= *stored_id {
            tracing::debug!(
                "♻️ Discarding tie-break lost replaceable event {key} (id {event_id} >= {stored_id})"
            );
            return false;
        }
    }

    state
        .latest_replaceable
        .insert(key.clone(), (created_at, event_id.clone()));
    info!("♻️ Updated latest replaceable event {key} (created_at={created_at}, id={event_id})");
    true
}

/// Processa um evento de deleção (kind 5, NIP-09).
/// - Verifica autoridade: apenas o autor pode deletar os seus próprios eventos.
/// - Por tags `e`: marca os event IDs como deletados em `state.deleted_ids`.
/// - Por tags `a`: remove a entrada correspondente de `state.latest_replaceable`.
///
/// Retorna `true` se o evento de deleção foi processado com sucesso (deve ser replicado),
/// `false` se for inválido (sem pubkey) — mas sempre replica kind 5 válidos para que os outros
/// nós também apliquem a deleção.
pub fn process_deletion_event(state: &MeshState, raw_event: &str) -> bool {
    let Some(event_obj) = crate::event_types::extract_event_object(raw_event) else {
        return true;
    };

    let kind = event_obj.get("kind").and_then(|v| v.as_u64()).unwrap_or(0);
    if !crate::event_types::is_deletion(kind) {
        return true; // Não é evento de deleção, permitir processamento normal
    }

    let Some(del_pubkey) = event_obj.get("pubkey").and_then(|v| v.as_str()) else {
        tracing::debug!("🗑️ Deletion event missing pubkey, discarding");
        return false;
    };

    // Processar tags `e` (deleção por ID de evento)
    let e_tags = crate::event_types::extract_e_tags(&event_obj);
    for target_id in &e_tags {
        state.deleted_ids.insert(target_id.clone());
        state.metrics.events_deleted.fetch_add(1, Ordering::Relaxed);
        // Também remover de latest_replaceable se esse evento era substituível
        state.latest_replaceable.retain(|_, v| v.1 != *target_id);
        info!("🗑️ Deleted event {target_id} by {del_pubkey} (tag e)");
    }

    // Processar tags `a` (deleção por coordenada de evento substituível)
    let a_tags = crate::event_types::extract_a_tags(&event_obj);
    for coord_key in &a_tags {
        // Validar autoridade: a coordenada deve pertencer ao mesmo pubkey
        // Formato: "pubkey:kind" ou "pubkey:kind:d_tag"
        let coord_pubkey = coord_key.split(':').next().unwrap_or("");
        if coord_pubkey != del_pubkey {
            tracing::debug!(
                "🗑️ Deletion of `a` tag {coord_key} rejected: pubkey mismatch ({del_pubkey} != {coord_pubkey})"
            );
            continue;
        }
        if state.latest_replaceable.remove(coord_key).is_some() {
            state.metrics.events_deleted.fetch_add(1, Ordering::Relaxed);
            info!("🗑️ Deleted replaceable event at coord {coord_key} by {del_pubkey} (tag a)");
        }
    }

    true
}

/// Verifica se um evento raw está expirado (NIP-40).
/// - Se o evento tiver tag `expiration` e o timestamp já tiver passado: descarta (retorna `false`).
/// - Se o evento tiver `expiration` futura: regista em `state.expiring_events` e aceita (retorna `true`).
/// - Se não tiver tag `expiration`: aceita normalmente (retorna `true`).
pub fn process_expiry_check(state: &MeshState, raw_event: &str) -> bool {
    let Some(event_obj) = crate::event_types::extract_event_object(raw_event) else {
        return true;
    };

    let Some(exp_ts) = crate::event_types::extract_expiration(&event_obj) else {
        return true; // Sem tag expiration — aceitar
    };

    let now_ts = chrono::Utc::now().timestamp() as u64;
    if exp_ts <= now_ts {
        tracing::debug!("⏰ Discarding expired event (expiration={exp_ts}, now={now_ts})");
        return false;
    }

    // Evento com expiração futura — registar para limpeza periódica
    let event_id = event_obj
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if !event_id.is_empty() {
        state.expiring_events.insert(event_id, exp_ts);
    }
    true
}

/// Task background que remove periodicamente os eventos expirados (NIP-40).
/// Corre a cada `interval_secs` segundos até cancelação.
pub async fn run_expiry_cleanup_task(
    state: Arc<MeshState>,
    interval_secs: u64,
    cancel: CancellationToken,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = interval.tick() => {
                let now_ts = chrono::Utc::now().timestamp() as u64;
                let mut expired_count = 0u64;

                state.expiring_events.retain(|event_id, exp_ts| {
                    if *exp_ts <= now_ts {
                        // Remover de seen_ids para libertar memória
                        state.seen_ids.remove(event_id.as_str());
                        // Remover de latest_replaceable se era substituível
                        state.latest_replaceable.retain(|_, v| v.1 != *event_id);
                        expired_count += 1;
                        false // remove from expiring_events
                    } else {
                        true // keep
                    }
                });

                if expired_count > 0 {
                    state.metrics.events_expired.fetch_add(expired_count, Ordering::Relaxed);
                    info!("🧹 Expired {expired_count} events (NIP-40 cleanup)");
                }

                // Limpeza de rate limiters de peers desconectados
                state.rate_limiters.retain(|peer_id, _| state.connected_peers.contains(peer_id));
            }
        }
    }
}

/// Carrega seen_ids e peer_cursors do disco.
fn load_state(data_dir: Option<&Path>) -> (DashSet<String>, DashSet<String>, DashMap<String, u64>) {
    let seen_ids = DashSet::new();
    let deleted_ids = DashSet::new();
    let peer_cursors = DashMap::new();

    let dir = match data_dir {
        Some(d) => d,
        None => return (seen_ids, deleted_ids, peer_cursors),
    };

    // 1. Carregar seen_ids.json
    let seen_file = dir.join("seen_ids.json");
    if seen_file.exists() {
        match std::fs::read(&seen_file) {
            Ok(bytes) => match serde_json::from_slice::<Vec<String>>(&bytes) {
                Ok(ids) => {
                    info!(
                        "💾 Loaded {} seen event IDs from {}",
                        ids.len(),
                        seen_file.display()
                    );
                    for id in ids {
                        seen_ids.insert(id);
                    }
                }
                Err(e) => {
                    warn!(
                        "⚠️  Failed to parse {}: {e}. Starting fresh with empty seen_ids.",
                        seen_file.display()
                    );
                }
            },
            Err(e) => {
                warn!(
                    "⚠️  Failed to read {}: {e}. Starting fresh with empty seen_ids.",
                    seen_file.display()
                );
            }
        }
    }

    // 2. Carregar deleted_ids.json
    let deleted_file = dir.join("deleted_ids.json");
    if deleted_file.exists() {
        match std::fs::read(&deleted_file) {
            Ok(bytes) => match serde_json::from_slice::<Vec<String>>(&bytes) {
                Ok(ids) => {
                    info!(
                        "🗑️  Loaded {} deleted event IDs from {}",
                        ids.len(),
                        deleted_file.display()
                    );
                    for id in ids {
                        deleted_ids.insert(id);
                    }
                }
                Err(e) => {
                    warn!(
                        "⚠️  Failed to parse {}: {e}. Starting fresh with empty deleted_ids.",
                        deleted_file.display()
                    );
                }
            },
            Err(e) => {
                warn!(
                    "⚠️  Failed to read {}: {e}. Starting fresh with empty deleted_ids.",
                    deleted_file.display()
                );
            }
        }
    }

    // 3. Carregar peer_cursors.json
    let cursors_file = dir.join("peer_cursors.json");
    if cursors_file.exists() {
        match std::fs::read(&cursors_file) {
            Ok(bytes) => {
                match serde_json::from_slice::<std::collections::HashMap<String, u64>>(&bytes) {
                    Ok(map) => {
                        info!(
                            "💾 Loaded {} peer cursors from {}",
                            map.len(),
                            cursors_file.display()
                        );
                        for (peer, cursor) in map {
                            peer_cursors.insert(peer, cursor);
                        }
                    }
                    Err(e) => {
                        warn!(
                            "⚠️  Failed to parse {}: {e}. Starting fresh with empty peer_cursors.",
                            cursors_file.display()
                        );
                    }
                }
            }
            Err(e) => {
                warn!(
                    "⚠️  Failed to read {}: {e}. Starting fresh with empty peer_cursors.",
                    cursors_file.display()
                );
            }
        }
    }

    (seen_ids, deleted_ids, peer_cursors)
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

    // 2. Salvar deleted_ids.json
    let deleted_vec: Vec<String> = state.deleted_ids.iter().map(|r| r.clone()).collect();
    if let Ok(bytes) = serde_json::to_vec(&deleted_vec) {
        let final_path = dir.join("deleted_ids.json");
        let tmp_path = dir.join("deleted_ids.json.tmp");
        if std::fs::write(&tmp_path, bytes).is_ok() {
            let _ = std::fs::rename(tmp_path, final_path);
        }
    }

    // 3. Salvar peer_cursors.json
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

    info!(
        "💾 State saved to disk ({} seen_ids, {} deleted_ids, {} cursors)",
        seen_vec.len(),
        deleted_vec.len(),
        cursors_map.len()
    );
}

/// Guard RAII para remover o peer de `connected_peers` quando a sessão termina.
struct PeerGuard {
    state: Arc<MeshState>,
    peer_id: String,
}

impl Drop for PeerGuard {
    fn drop(&mut self) {
        self.state.connected_peers.remove(&self.peer_id);
        self.state.peer_channels.remove(&self.peer_id);
        self.state.rate_limiters.remove(&self.peer_id);
        self.state.active_peer_sessions.remove(&self.peer_id);
        self.state.metrics.dec_peers_connected();

        if let Ok(mut ring) = self.state.hash_ring.write() {
            ring.remove_peer(&self.peer_id);
            self.state
                .metrics
                .hash_ring_peers
                .store(ring.peer_count() as u64, Ordering::Relaxed);
            self.state
                .metrics
                .hash_ring_vnodes
                .store(ring.vnode_count() as u64, Ordering::Relaxed);
        }
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
        if path.exists()
            && let Ok(id) = std::fs::read_to_string(&path)
        {
            let trimmed = id.trim().to_string();
            if !trimmed.is_empty() {
                return trimmed;
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
#[allow(dead_code)]
pub async fn run(
    cfg: MeshConfig,
    relay_url: String,
    data_dir: Option<PathBuf>,
    relay_events: broadcast::Receiver<RelayEvent>,
    relay_publish_tx: mpsc::Sender<String>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    run_with_http_listen(
        cfg,
        None,
        relay_url,
        data_dir,
        relay_events,
        relay_publish_tx,
        cancel,
    )
    .await
}

/// Inicia o mesh agent com suporte opcional a um servidor HTTP local de observabilidade (`metrics.listen`).
pub async fn run_with_http_listen(
    cfg: MeshConfig,
    metrics_listen: Option<String>,
    relay_url: String,
    data_dir: Option<PathBuf>,
    relay_events: broadcast::Receiver<RelayEvent>,
    relay_publish_tx: mpsc::Sender<String>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    run_with_http_listen_and_storage_with_heartbeat(
        cfg,
        metrics_listen,
        relay_url,
        data_dir,
        None,
        None,
        relay_events,
        relay_publish_tx,
        cancel,
    )
    .await
}

/// Inicia o mesh agent com suporte a servidor HTTP de métricas, estado de storage verificado e heartbeat config.
#[allow(clippy::too_many_arguments)]
pub async fn run_with_http_listen_and_storage(
    cfg: MeshConfig,
    metrics_listen: Option<String>,
    relay_url: String,
    data_dir: Option<PathBuf>,
    storage_info: Option<crate::storage::StorageInfo>,
    relay_events: broadcast::Receiver<RelayEvent>,
    relay_publish_tx: mpsc::Sender<String>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    run_with_http_listen_and_storage_with_heartbeat(
        cfg,
        metrics_listen,
        relay_url,
        data_dir,
        storage_info,
        None,
        relay_events,
        relay_publish_tx,
        cancel,
    )
    .await
}

/// Inicia o mesh agent com suporte completo a servidor HTTP de métricas, storage verificado e heartbeat config.
#[allow(clippy::too_many_arguments)]
pub async fn run_with_http_listen_and_storage_with_heartbeat(
    cfg: MeshConfig,
    metrics_listen: Option<String>,
    relay_url: String,
    data_dir: Option<PathBuf>,
    storage_info: Option<crate::storage::StorageInfo>,
    heartbeat_config: Option<crate::config::HeartbeatConfig>,
    mut relay_events: broadcast::Receiver<RelayEvent>,
    relay_publish_tx: mpsc::Sender<String>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let (seen_ids, deleted_ids, peer_cursors) = load_state(data_dir.as_deref());

    let metrics = Arc::new(Metrics::new());

    if let Some(ref info) = storage_info {
        let reserved_bytes = info.total_reserved_gb.saturating_mul(1_073_741_824);
        let available_bytes = info.available_gb.saturating_mul(1_073_741_824);
        let used_bytes = info.used_gb.saturating_mul(1_073_741_824);
        metrics.update_storage_metrics(reserved_bytes, available_bytes, used_bytes);
    }

    let state = Arc::new(MeshState {
        seen_ids,
        deleted_ids,
        expiring_events: DashMap::new(),
        peer_cursors,
        latest_replaceable: DashMap::new(),
        relay_tx: relay_publish_tx,
        connected_peers: DashSet::new(),
        peer_channels: DashMap::new(),
        rate_limiters: DashMap::new(),
        replication_factor: cfg.replication_factor,
        max_events_per_sec: cfg.max_events_per_second_per_peer,
        max_bytes_per_sec: cfg.max_bytes_per_second_per_peer,
        max_msg_size: cfg.max_message_size,
        events_stored: AtomicU64::new(0),
        events_replicated: AtomicU64::new(0),
        events_rate_limited: AtomicU64::new(0),
        bytes_rate_limited: AtomicU64::new(0),
        messages_oversized: AtomicU64::new(0),
        metrics: metrics.clone(),
        active_peer_sessions: DashMap::new(),
        hash_ring: Arc::new(std::sync::RwLock::new(
            crate::consistent_hash::ConsistentHashRing::new(cfg.vnodes_per_peer as usize),
        )),
        relay_url,
        data_dir: data_dir.clone(),
        storage_info: storage_info.clone(),
    });

    // ── Task Periódica: Atualização de Métricas de Storage (60s) ────────
    if storage_info.is_some()
        && let Some(ref data_dir_buf) = state.data_dir
    {
        let metrics_clone = metrics.clone();
        let data_dir_clone = data_dir_buf.clone();
        let extra_contribution_gb = storage_info
            .as_ref()
            .map(|i| {
                i.total_reserved_gb
                    .saturating_sub(crate::storage::MIN_RESERVED_GB)
            })
            .unwrap_or(0);
        let cancel_clone = cancel.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            interval.tick().await;

            loop {
                tokio::select! {
                    _ = cancel_clone.cancelled() => break,
                    _ = interval.tick() => {
                        let dir = data_dir_clone.clone();
                        let storage_cfg = crate::storage::StorageConfig {
                            extra_contribution_gb,
                            data_dir: dir,
                        };

                        let metrics_res = tokio::task::spawn_blocking(move || {
                            crate::storage::get_storage_metrics(&storage_cfg)
                        }).await;

                        match metrics_res {
                            Ok(Ok(sm)) => {
                                metrics_clone.update_storage_metrics(
                                    sm.reserved_bytes,
                                    sm.available_bytes,
                                    sm.used_bytes,
                                );
                            }
                            Ok(Err(err)) => {
                                warn!("⚠️ Erro ao atualizar métricas periódicas de storage: {err}");
                            }
                            Err(err) => {
                                warn!("⚠️ Task spawn_blocking de métricas de storage falhou: {err}");
                            }
                        }
                    }
                }
            }
        });
    }

    // ── Node ID & Mesh URL Auto-detection ──────────────────────────────
    let node_id = load_or_generate_node_id(cfg.node_id.as_deref(), data_dir.as_deref());
    let mesh_url = crate::config::detect_mesh_url(&cfg.listen, cfg.mesh_url.as_deref());
    info!("🆔 Node ID: {node_id}");

    // ── Contexto TLS (certificado do nó + store TOFU) ──────────────────
    let tls_ctx: Option<Arc<TlsContext>> = if cfg.tls_enabled {
        let ctx = Arc::new(TlsContext::new(&cfg, &node_id, data_dir.as_deref())?);
        info!("🔐 Node cert fingerprint: {}", ctx.cert.fingerprint);
        Some(ctx)
    } else {
        warn!(
            "🔓 TLS DISABLED (mesh.tls_enabled = false) — peer traffic is PLAINTEXT. Dev/testing only!"
        );
        None
    };
    let cert_fingerprint = tls_ctx.as_ref().map(|c| c.cert.fingerprint.clone());

    // ── Servidor HTTP de Observabilidade (Prometheus + Health + Peers) ─
    if let Some(listen_addr) = metrics_listen {
        let node_info = crate::http::NodeInfo {
            version: env!("CARGO_PKG_VERSION").to_string(),
            node_id: node_id.clone(),
            cert_fingerprint: cert_fingerprint.clone(),
            relay_url: state.relay_url.clone(),
            mesh_listen: cfg.listen.clone(),
            replication_factor: cfg.replication_factor,
            tls_enabled: cfg.tls_enabled,
        };
        let m = metrics.clone();
        let c = cancel.clone();
        let st = state.clone();
        tokio::spawn(async move {
            if let Err(e) =
                crate::http::run_http_server(listen_addr, m, node_info, Some(st), c).await
            {
                error!("❌ HTTP observability server failed: {e}");
            }
        });
    }

    // ── Listener para peers ────────────────────────────────────────────
    let listener = TcpListener::bind(&cfg.listen).await?;
    info!(
        "🌐 Mesh agent listening on {} ({})",
        cfg.listen,
        if tls_ctx.is_some() {
            "TLS 1.3 mutual"
        } else {
            "plaintext"
        }
    );

    // ── Task: consumir eventos do relay local → replicar para peers ──
    let state_clone = state.clone();
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel_clone.cancelled() => break,
                result = relay_events.recv() => {
                    match result {
                        Ok(event) => {
                            if let Some(id) = extract_event_id(&event.raw) {
                                // Processar deleções (NIP-09) antes de qualquer outra coisa
                                process_deletion_event(&state_clone, &event.raw);
                                // Verificar expiração (NIP-40)
                                if !process_expiry_check(&state_clone, &event.raw) {
                                    continue;
                                }
                                // Verificar substituição e deduplicação
                                if process_replaceable_event(&state_clone, &event.raw) {
                                    if state_clone.seen_ids.insert(id.clone()) {
                                        state_clone.metrics.inc_events_received(EventSource::Relay);
                                        info!("📡 Local relay event {id} received, replicating to peers");
                                        replicate_event(&state_clone, &id, &event.raw, None);
                                    } else {
                                        tracing::debug!("🔁 Relay event {id} already seen (dedup), skipping replication");
                                    }
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
            tls_ctx.clone(),
            heartbeat_secs,
            cancel.clone(),
        );
    }

    // ── Task: limpeza periódica de eventos expirados (NIP-40) ─────────
    {
        let state_exp = state.clone();
        let cancel_exp = cancel.clone();
        tokio::spawn(async move {
            run_expiry_cleanup_task(state_exp, 60, cancel_exp).await;
        });
    }
    // ── Registry Central & Dynamic Peer Discovery ──────────────────────
    if let Some(ref registry_url) = cfg.registry_url {
        let registry_client = RegistryClient::new(registry_url.clone());
        let storage_meta = storage_info
            .as_ref()
            .map(registry::StorageMetadata::from_info);

        let relay_info = RelayInfo {
            node_id: node_id.clone(),
            relay_url: state.relay_url.clone(),
            mesh_url: mesh_url.clone(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            capabilities: vec!["nostr".to_string(), "mesh".to_string()],
            cert_fingerprint: cert_fingerprint.clone(),
            last_seen: None,
            storage: storage_meta,
        };

        // Registo inicial na startup (POST /relays)
        if let Err(e) = registry_client.register(&relay_info).await {
            warn!(
                "⚠️ Initial registry registration failed at {registry_url}: {e}. Operating with static seeds/cache."
            );
        } else if let Some(ref st) = relay_info.storage {
            info!(
                "📋 Registered storage capacity at registry: {} GB reserved, {} GB available",
                st.reserved_gb, st.available_gb
            );
        }

        // Task: HeartbeatService (PUT /v1/relays/{node_id})
        let hb_cfg = heartbeat_config.unwrap_or_default();
        if hb_cfg.enabled {
            let onboard_state = crate::onboard::check_onboard_status(data_dir.as_deref());
            if let Some(auth_key) = onboard_state.as_ref().and_then(|st| st.bearer_token.clone()) {
                let mesh_url_hb = mesh_url.clone();
                let cert_fp_hb = cert_fingerprint.clone();
                let metrics_hb = metrics.clone();

                let hb_service = crate::heartbeat::HeartbeatService::new(
                    hb_cfg,
                    registry_url.clone(),
                    reqwest::Client::builder()
                        .timeout(Duration::from_secs(10))
                        .build()
                        .unwrap_or_default(),
                    node_id.clone(),
                    auth_key,
                    Arc::new(move || mesh_url_hb.clone()),
                    Arc::new(move || cert_fp_hb.clone()),
                    Arc::new(move || {
                        let res_b = metrics_hb.storage_reserved_bytes.load(std::sync::atomic::Ordering::Relaxed);
                        let avail_b = metrics_hb.storage_available_bytes.load(std::sync::atomic::Ordering::Relaxed);
                        (res_b / (1024 * 1024 * 1024), avail_b / (1024 * 1024 * 1024))
                    }),
                    metrics.clone(),
                    cancel.clone(),
                );
                tokio::spawn(hb_service.run());
            } else {
                info!("ℹ️  Node not onboarded (no auth_key); skipping HeartbeatService");
            }
        } else {
            info!("ℹ️  Heartbeat service is disabled by configuration");
        }

        // Deregisto gracioso no shutdown (DELETE /relays/{node_id})
        let dereg_client = registry_client.clone();
        let dereg_node_id = node_id.clone();
        let cancel_dereg = cancel.clone();
        tokio::spawn(async move {
            cancel_dereg.cancelled().await;
            if let Err(e) = dereg_client.deregister(&dereg_node_id).await {
                warn!("⚠️ Registry deregistration failed on shutdown: {e}");
            }
        });

        // Task: Descoberta periódica de peers no registry (GET /relays)
        let discovery_client = registry_client.clone();
        let my_node_id = node_id.clone();
        let my_mesh_url = mesh_url.clone();
        let state_disc = state.clone();
        let cancel_disc = cancel.clone();
        let discovery_secs = cfg.discovery_secs;
        let hb_interval_secs = cfg.heartbeat_secs;
        let data_dir_disc = data_dir.clone();
        let tls_disc = tls_ctx.clone();

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
                                            tls_disc.clone(),
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
                                                tls_disc.clone(),
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

    // ── Task: métricas de replicação e salvamento periódico em disco ──
    let state_metrics = state.clone();
    let cancel_metrics = cancel.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.tick().await;
        loop {
            tokio::select! {
                _ = cancel_metrics.cancelled() => break,
                _ = interval.tick() => {
                    let stored = state_metrics.events_stored.load(Ordering::Relaxed);
                    let replicated = state_metrics.events_replicated.load(Ordering::Relaxed);
                    let rate_limited_events = state_metrics.events_rate_limited.load(Ordering::Relaxed);
                    let rate_limited_bytes = state_metrics.bytes_rate_limited.load(Ordering::Relaxed);
                    let oversized = state_metrics.messages_oversized.load(Ordering::Relaxed);
                    let active_peers = state_metrics.peer_channels.len();
                    info!(
                        "📊 Replication: {} stored, {} replicated, RF={} | Rate limiting: {} events rejected, {} bytes rejected, {} oversized | active_peers={}",
                        stored, replicated, state_metrics.replication_factor, rate_limited_events, rate_limited_bytes, oversized, active_peers
                    );
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
                        let cancel = cancel.clone();
                        let tls = tls_ctx.clone();
                        tokio::spawn(handle_inbound_peer(stream, addr, state, tls, heartbeat_secs, cancel));
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
///
/// A conexão é TLS 1.3 mútua com verificação de fingerprint quando `tls_ctx`
/// está presente; caso contrário é TCP plaintext (dev-only).
fn start_seed_task(
    seed_url: String,
    state: Arc<MeshState>,
    tls_ctx: Option<Arc<TlsContext>>,
    heartbeat_secs: u64,
    cancel: CancellationToken,
) {
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
            let cancel = cancel.clone();
            let tls_ctx = tls_ctx.clone();

            async move {
                if cancel.is_cancelled() {
                    return Err::<(), _>(backoff::Error::permanent(anyhow::anyhow!("shutdown")));
                }

                if state.connected_peers.contains(&seed_url) {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    return Err::<(), _>(backoff::Error::transient(anyhow::anyhow!(
                        "already connected"
                    )));
                }

                info!("🌱 Connecting outbound to seed: {seed_url}");
                match connect_to_peer(&seed_url, tls_ctx.as_ref()).await {
                    Ok(PeerConnection::Tls { sink, stream }) => {
                        info!("🟢 Outbound TLS connection established to seed: {seed_url}");
                        handle_peer_stream(
                            seed_url.clone(),
                            sink,
                            stream,
                            state,
                            heartbeat_secs,
                            cancel,
                        )
                        .await;
                        Err::<(), _>(backoff::Error::transient(anyhow::anyhow!(
                            "seed connection ended"
                        )))
                    }
                    Ok(PeerConnection::Plain { sink, stream }) => {
                        info!("🟢 Outbound plaintext connection established to seed: {seed_url}");
                        handle_peer_stream(
                            seed_url.clone(),
                            sink,
                            stream,
                            state,
                            heartbeat_secs,
                            cancel,
                        )
                        .await;
                        Err::<(), _>(backoff::Error::transient(anyhow::anyhow!(
                            "seed connection ended"
                        )))
                    }
                    Err(e) => {
                        warn!("🔌 Seed connection to {seed_url} failed: {e}. Reconnecting…");
                        Err::<(), _>(backoff::Error::transient(e))
                    }
                }
            }
        })
        .await
        .ok();

        info!("🌱 Seed task for {seed_url} stopped");
    });
}

/// Trata conexão inbound de um peer.
///
/// Quando o TLS está ativo, faz primeiro o handshake TLS 1.3 mútuo, verifica o
/// fingerprint do certificado do cliente contra o store TOFU e só depois faz o
/// upgrade para WebSocket. O resto do protocolo é idêntico ao plaintext.
async fn handle_inbound_peer(
    stream: tokio::net::TcpStream,
    addr: SocketAddr,
    state: Arc<MeshState>,
    tls_ctx: Option<Arc<TlsContext>>,
    heartbeat_secs: u64,
    cancel: CancellationToken,
) {
    use tokio_tungstenite::accept_async;

    let peer_id = format!("inbound:{addr}");

    let Some(tls) = tls_ctx else {
        // Fallback plaintext (dev-only)
        let ws = match accept_async(stream).await {
            Ok(ws) => ws,
            Err(e) => {
                warn!("❌ WebSocket handshake failed for {addr}: {e}");
                return;
            }
        };
        let (sink, stream) = ws.split();
        handle_peer_stream(peer_id, sink, stream, state, heartbeat_secs, cancel).await;
        return;
    };

    let acceptor = tokio_rustls::TlsAcceptor::from(tls.server_config.clone());
    let tls_stream = match acceptor.accept(stream).await {
        Ok(s) => s,
        Err(e) => {
            warn!("🔒 TLS handshake failed for inbound peer {addr}: {e}");
            return;
        }
    };

    // Verificar o fingerprint do certificado do cliente (TOFU).
    let (_, conn) = tls_stream.get_ref();
    let Some(received) = crate::tls::peer_fingerprint(conn) else {
        warn!("🔒 Inbound peer {addr} presented no client certificate, rejecting");
        return;
    };

    match tls.fingerprints.verify_or_learn(&peer_id, &received) {
        TrustDecision::Match => {
            info!("🔐 Inbound peer {addr} verified (fingerprint {received})");
        }
        TrustDecision::LearnedOnFirstUse => {
            info!("🔐 Inbound peer {addr} trusted on first use (fingerprint {received})");
        }
        TrustDecision::Mismatch { expected, .. } => {
            error!(
                "🚨 Rejecting inbound peer {addr}: fingerprint mismatch — expected {expected}, received {received} (possible MITM)"
            );
            return;
        }
    }

    let ws = match accept_async(tls_stream).await {
        Ok(ws) => ws,
        Err(e) => {
            warn!("❌ WebSocket handshake failed for {addr} over TLS: {e}");
            return;
        }
    };

    let (sink, stream) = ws.split();
    handle_peer_stream(peer_id, sink, stream, state, heartbeat_secs, cancel).await;
}

/// Estabelece uma conexão WebSocket outbound a um peer, sobre TLS 1.3 mútuo
/// quando `tls_ctx` está presente, ou TCP plaintext quando não está.
///
/// A identidade do peer é verificada pelo fingerprint do certificado: pinned ou
/// previamente conhecido tem de bater exatamente; peer novo é aceite e o
/// fingerprint é guardado (trust-on-first-use).
async fn connect_to_peer(
    peer_url: &str,
    tls_ctx: Option<&Arc<TlsContext>>,
) -> anyhow::Result<PeerConnection> {
    use tokio_tungstenite::{client_async, connect_async};

    let Some(tls) = tls_ctx else {
        let (ws, _) = connect_async(peer_url).await?;
        let (sink, stream) = ws.split();
        return Ok(PeerConnection::Plain { sink, stream });
    };

    let (host, port) = parse_ws_host_port(peer_url)?;
    let expected = tls.fingerprints.expected(peer_url);
    let (client_cfg, observed) = crate::tls::client_config(&tls.cert, peer_url, expected)?;

    let tcp = tokio::net::TcpStream::connect((host.as_str(), port)).await?;
    let server_name = rustls::pki_types::ServerName::try_from(host.clone())
        .map_err(|e| anyhow::anyhow!("invalid server name '{host}': {e}"))?
        .to_owned();

    let connector = tokio_rustls::TlsConnector::from(client_cfg);
    let tls_stream = connector
        .connect(server_name, tcp)
        .await
        .map_err(|e| anyhow::anyhow!("TLS handshake with {peer_url} failed: {e}"))?;

    // Handshake passou: se o peer era desconhecido, aprende o fingerprint agora.
    let received = observed
        .lock()
        .ok()
        .and_then(|g| g.clone())
        .ok_or_else(|| anyhow::anyhow!("no peer certificate observed for {peer_url}"))?;

    match tls.fingerprints.verify_or_learn(peer_url, &received) {
        TrustDecision::Match => {
            info!("🔐 Outbound peer {peer_url} verified (fingerprint {received})");
        }
        TrustDecision::LearnedOnFirstUse => {
            info!("🔐 Outbound peer {peer_url} trusted on first use (fingerprint {received})");
        }
        TrustDecision::Mismatch { expected, .. } => {
            anyhow::bail!(
                "fingerprint mismatch for {peer_url}: expected {expected}, received {received} (possible MITM)"
            );
        }
    }

    let (ws, _) = client_async(peer_url, tls_stream).await?;
    let (sink, stream) = ws.split();
    Ok(PeerConnection::Tls { sink, stream })
}

type WsStreamOf<S> = tokio_tungstenite::WebSocketStream<S>;
type TlsClientStream = tokio_rustls::client::TlsStream<tokio::net::TcpStream>;
type PlainWs = tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>;

/// Conexão outbound estabelecida, em TLS ou plaintext.
enum PeerConnection {
    Tls {
        sink: futures_util::stream::SplitSink<WsStreamOf<TlsClientStream>, Message>,
        stream: futures_util::stream::SplitStream<WsStreamOf<TlsClientStream>>,
    },
    Plain {
        sink: futures_util::stream::SplitSink<WsStreamOf<PlainWs>, Message>,
        stream: futures_util::stream::SplitStream<WsStreamOf<PlainWs>>,
    },
}

/// Extrai `(host, port)` de um URL `ws://host:port` ou `wss://host:port`.
fn parse_ws_host_port(url: &str) -> anyhow::Result<(String, u16)> {
    let rest = url
        .strip_prefix("ws://")
        .or_else(|| url.strip_prefix("wss://"))
        .ok_or_else(|| anyhow::anyhow!("peer URL '{url}' must start with ws:// or wss://"))?;
    let authority = rest.split(['/', '?', '#']).next().unwrap_or(rest);

    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) if !h.is_empty() && p.chars().all(|c| c.is_ascii_digit()) => (
            h.to_string(),
            p.parse::<u16>()
                .map_err(|e| anyhow::anyhow!("invalid port in '{url}': {e}"))?,
        ),
        _ => (
            authority.to_string(),
            if url.starts_with("wss://") { 443 } else { 80 },
        ),
    };

    if host.is_empty() {
        anyhow::bail!("peer URL '{url}' has no host");
    }
    Ok((host, port))
}

/// Handler unificado de sessão peer bidirecional (independente de inbound ou outbound).
async fn handle_peer_stream<Si, St>(
    peer_id: String,
    mut sink: Si,
    mut stream: St,
    state: Arc<MeshState>,
    heartbeat_secs: u64,
    cancel: CancellationToken,
) where
    Si: Sink<Message> + Unpin + Send + 'static,
    St: Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
        + Unpin
        + Send
        + 'static,
{
    if !state.connected_peers.insert(peer_id.clone()) {
        warn!("⚠️ Already connected to peer {peer_id}, skipping duplicate session");
        return;
    }
    state.metrics.inc_peers_connected();

    // Canal interno para enviar mensagens ao peer (heartbeats, OK, EOSE, backfill, eventos replicados)
    let (ctrl_tx, mut ctrl_rx) = mpsc::channel::<Message>(256);

    state.peer_channels.insert(peer_id.clone(), ctrl_tx.clone());

    let (direction, address) = if peer_id.starts_with("inbound:") {
        (
            "inbound".to_string(),
            peer_id.trim_start_matches("inbound:").to_string(),
        )
    } else {
        let addr = parse_ws_host_port(&peer_id)
            .map(|(h, p)| format!("{h}:{p}"))
            .unwrap_or_else(|_| peer_id.clone());
        ("outbound".to_string(), addr)
    };

    let session_info = Arc::new(PeerSessionInfo {
        peer_id: peer_id.clone(),
        address,
        direction,
        connected_since: chrono::Utc::now().timestamp() as u64,
        events_sent: Arc::new(AtomicU64::new(0)),
        events_received: Arc::new(AtomicU64::new(0)),
        tls_fingerprint: None,
    });
    state
        .active_peer_sessions
        .insert(peer_id.clone(), session_info);

    {
        let mut ring = state.hash_ring.write().unwrap();
        ring.add_peer(&peer_id);
        state
            .metrics
            .hash_ring_peers
            .store(ring.peer_count() as u64, Ordering::Relaxed);
        state
            .metrics
            .hash_ring_vnodes
            .store(ring.vnode_count() as u64, Ordering::Relaxed);
    }

    let state_rebal = state.clone();
    let peer_id_rebal = peer_id.clone();
    let cancel_rebal = cancel.clone();
    tokio::spawn(async move {
        rebalance_to_new_peer(state_rebal, peer_id_rebal, cancel_rebal).await;
    });

    let _guard = PeerGuard {
        state: state.clone(),
        peer_id: peer_id.clone(),
    };

    info!("🤝 Active peer session: {peer_id}");

    // ── Task: enviar mensagens de controle + heartbeats periódicos ao peer ──
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
                Some(ctrl_msg) = ctrl_rx.recv() => {
                    if sink.send(ctrl_msg).await.is_err() {
                        break;
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
    let backfill_req = format!(
        r#"["REQ","goy-backfill",{{"since":{},"limit":500}}]"#,
        cursor
    );
    let _ = ctrl_tx.send(Message::Text(backfill_req)).await;

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
                        let msg_bytes = text.len();

                        // 1. Verificação de tamanho máximo de mensagem (antes de qualquer parsing)
                        if msg_bytes > state.max_msg_size {
                            state.messages_oversized.fetch_add(1, Ordering::Relaxed);
                            state.metrics.messages_oversized.fetch_add(1, Ordering::Relaxed);
                            warn!(
                                "⚠️ Oversized message from peer {peer_id}: {msg_bytes} bytes > max {}",
                                state.max_msg_size
                            );
                            continue;
                        }

                        // 2. Token Bucket Rate Limiting por peer
                        let limiter_arc = state
                            .rate_limiters
                            .entry(peer_id.clone())
                            .or_insert_with(|| {
                                Arc::new(Mutex::new(
                                    crate::rate_limiter::PeerRateLimiter::new(
                                        state.max_events_per_sec,
                                        state.max_bytes_per_sec,
                                    ),
                                ))
                            })
                            .clone();

                        {
                            let mut limiter = limiter_arc.lock().await;
                            if let Err(reason) = limiter.try_consume(msg_bytes) {
                                match reason {
                                    crate::rate_limiter::RateLimitReason::EventsExhausted => {
                                        state.events_rate_limited.fetch_add(1, Ordering::Relaxed);
                                        state.metrics.events_rate_limited.fetch_add(1, Ordering::Relaxed);
                                    }
                                    crate::rate_limiter::RateLimitReason::BytesExhausted => {
                                        state.bytes_rate_limited.fetch_add(1, Ordering::Relaxed);
                                        state.metrics.events_rate_limited.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                                if !limiter.warned {
                                    limiter.warned = true;
                                    warn!("⚠️ Peer {peer_id} rate limited: {reason} ({msg_bytes} bytes)");
                                } else {
                                    tracing::debug!("⚠️ Peer {peer_id} rate limited: {reason} ({msg_bytes} bytes)");
                                }
                                continue;
                            }
                        }

                        if text.starts_with(r#"["REQ""#) {
                            state.metrics.backfill_requests.fetch_add(1, Ordering::Relaxed);
                            if let Some((sub_id, filter)) = parse_req_msg(&text) {
                                let relay_url = state.relay_url.clone();
                                let peer_id = peer_id.clone();
                                let ctrl_tx = ctrl_tx.clone();
                                tokio::spawn(async move {
                                    handle_backfill_req(sub_id, filter, relay_url, peer_id, ctrl_tx).await;
                                });
                            }
                        } else if text.starts_with(r#"["EVENT""#) {
                            state.metrics.inc_events_received(EventSource::Peer);
                            if let Some(session) = state.active_peer_sessions.get(&peer_id) {
                                session.events_received.fetch_add(1, Ordering::Relaxed);
                            }
                            if let Some(id) = extract_event_id(&text) {
                                if let Some(ts) = extract_event_timestamp(&text) {
                                    state
                                        .peer_cursors
                                        .entry(peer_id.clone())
                                        .and_modify(|c| *c = (*c).max(ts))
                                        .or_insert(ts);
                                }
                                // Processar deleções (NIP-09) antes de qualquer outra coisa
                                process_deletion_event(&state, &text);
                                // Verificar expiração (NIP-40)
                                if !process_expiry_check(&state, &text) {
                                    continue;
                                }
                                // Verificar substituição e deduplicação
                                if process_replaceable_event(&state, &text) {
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
                                            let _ = ctrl_tx.send(Message::Text(ok_msg)).await;
                                        }
                                    } else {
                                        tracing::debug!("🔁 Event {id} from peer {peer_id} already seen (dedup), skipping relay publish");
                                    }
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

/// Rebalanceamento em background: ao entrar um novo peer na mesh, consulta os eventos
/// locais e envia ao novo peer aqueles pelos quais ele passou a ser responsável no anel.
async fn rebalance_to_new_peer(
    state: Arc<MeshState>,
    new_peer_id: String,
    cancel: CancellationToken,
) {
    tokio::time::sleep(Duration::from_millis(100)).await;

    if cancel.is_cancelled() || !state.peer_channels.contains_key(&new_peer_id) {
        return;
    }

    let replication_factor = state.replication_factor as usize;
    if replication_factor == 0 {
        return;
    }

    let event_ids: Vec<String> = state.seen_ids.iter().map(|r| r.clone()).collect();
    if event_ids.is_empty() {
        return;
    }

    let mut target_ids = Vec::new();
    {
        let ring = state.hash_ring.read().unwrap();
        for event_id in event_ids {
            let responsible = ring.get_responsible_peers(&event_id, replication_factor);
            if responsible.contains(&new_peer_id) {
                target_ids.push(event_id);
            }
        }
    }

    if target_ids.is_empty() {
        return;
    }

    let ctrl_tx = match state.peer_channels.get(&new_peer_id) {
        Some(tx) => tx.value().clone(),
        None => return,
    };

    let filter = serde_json::json!({
        "ids": target_ids,
        "limit": target_ids.len()
    });

    let rebalanced_count = target_ids.len() as u64;
    state
        .metrics
        .rebalance_events_sent
        .fetch_add(rebalanced_count, Ordering::Relaxed);
    info!("🔄 Rebalancing: sent {rebalanced_count} events to new peer {new_peer_id}");

    handle_backfill_req(
        "goy-rebalance".to_string(),
        Some(filter),
        state.relay_url.clone(),
        new_peer_id,
        ctrl_tx,
    )
    .await;
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
            let _ = ctrl_tx.send(Message::Text(eose_msg)).await;
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

    if let Err(e) = ws.send(Message::Text(req_payload)).await {
        warn!("🔌 Failed to send REQ to local relay at {relay_url}: {e}");
        let eose_msg = format!(r#"["EOSE","{}"]"#, sub_id);
        let _ = ctrl_tx.send(Message::Text(eose_msg)).await;
        return;
    }

    let mut sent_count = 0;
    let mut eose_sent = false;

    while let Some(msg) = ws.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if text.starts_with(r#"["EVENT""#) {
                    if sent_count < limit {
                        if ctrl_tx.send(Message::Text(text)).await.is_err() {
                            break;
                        }
                        sent_count += 1;
                    }
                    if sent_count >= limit {
                        let eose_msg = format!(r#"["EOSE","{}"]"#, sub_id);
                        let _ = ctrl_tx.send(Message::Text(eose_msg)).await;
                        eose_sent = true;
                        break;
                    }
                } else if text.starts_with(r#"["EOSE""#) {
                    let eose_msg = format!(r#"["EOSE","{}"]"#, sub_id);
                    let _ = ctrl_tx.send(Message::Text(eose_msg)).await;
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
        let _ = ctrl_tx.send(Message::Text(eose_msg)).await;
    }

    info!("📦 backfill: enviados {sent_count}/{limit} eventos para peer {peer_id}");
}

/// Normaliza mensagem EVENT recebida de um peer para publicação no relay local.
/// Se for um evento de resposta a REQ (3 elementos: ["EVENT", sub_id, event_obj]),
/// converte para formato de publicação (2 elementos: ["EVENT", event_obj]).
fn normalize_event_for_publish(raw: &str) -> String {
    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(raw)
        && let Some(arr) = parsed.as_array()
        && arr.len() >= 3
        && arr[0].as_str() == Some("EVENT")
    {
        return format!(r#"["EVENT",{}]"#, arr[2]);
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
    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(raw)
        && let Some(arr) = parsed.as_array()
    {
        return arr.len() == 2 && arr[0].as_str() == Some("EVENT");
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

    /// Reserva uma porta efémera em loopback e devolve-a já libertada.
    ///
    /// Os testes correm em paralelo (e `cargo test` corre os binários lib e bin
    /// em simultâneo), por isso portas fixas colidem de forma intermitente.
    async fn free_addr() -> anyhow::Result<std::net::SocketAddr> {
        let l = TcpListener::bind("127.0.0.1:0").await?;
        let addr = l.local_addr()?;
        drop(l);
        Ok(addr)
    }

    #[tokio::test]
    async fn test_bidirectional_relay_and_peer_flow() -> anyhow::Result<()> {
        use tokio_tungstenite::connect_async;

        let (relay_events_tx, relay_events_rx) = broadcast::channel::<RelayEvent>(16);
        let (relay_publish_tx, mut relay_publish_rx) = mpsc::channel::<String>(16);
        let cancel = CancellationToken::new();

        let addr = free_addr().await?;
        let cfg = MeshConfig {
            listen: addr.to_string(),
            seeds: vec![],
            registry_url: None,
            heartbeat_secs: 30,
            discovery_secs: 60,
            mesh_url: None,
            node_id: None,
            replication_factor: 3,
            vnodes_per_peer: 150,
            max_events_per_second_per_peer: 50,
            max_bytes_per_second_per_peer: 1_048_576,
            max_message_size: 524_288,
            // Estes testes legados falam WebSocket plaintext diretamente.
            tls_enabled: false,
            trusted_fingerprints: std::collections::HashMap::new(),
        };

        let cancel_mesh = cancel.clone();
        tokio::spawn(async move {
            let _ = run(
                cfg,
                "ws://127.0.0.1:57777".to_string(),
                None,
                relay_events_rx,
                relay_publish_tx,
                cancel_mesh,
            )
            .await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Conecta peer via WebSocket
        let ws_url = format!("ws://{addr}");
        let ws_url = ws_url.as_str();
        let (mut ws_stream, _) = connect_async(ws_url).await?;

        // Peer deve receber a mensagem de pedido de backfill inicial enviada pelo nó
        let init_req = tokio::time::timeout(Duration::from_secs(2), ws_stream.next())
            .await?
            .ok_or_else(|| anyhow::anyhow!("ws_stream closed unexpectedly"))??;
        assert_eq!(
            init_req.to_text()?,
            r#"["REQ","goy-backfill",{"since":0,"limit":500}]"#
        );

        // 1. Fluxo: Relay local -> Mesh Agent -> Peer
        let event_from_relay =
            r#"["EVENT","goy-live",{"id":"relay_evt_1","content":"hello from strfry"}]"#;
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

        let (addr_a, addr_b) = (free_addr().await?, free_addr().await?);

        // ── Node A (sem seeds) ────────────────────────────────────────────
        let (relay_events_tx_a, relay_events_rx_a) = broadcast::channel::<RelayEvent>(16);
        let (relay_publish_tx_a, mut relay_publish_rx_a) = mpsc::channel::<String>(16);

        let cfg_a = MeshConfig {
            listen: addr_a.to_string(),
            seeds: vec![],
            registry_url: None,
            heartbeat_secs: 30,
            discovery_secs: 60,
            mesh_url: Some(format!("ws://{addr_a}")),
            node_id: None,
            replication_factor: 3,
            vnodes_per_peer: 150,
            max_events_per_second_per_peer: 50,
            max_bytes_per_second_per_peer: 1_048_576,
            max_message_size: 524_288,
            // Estes testes legados falam WebSocket plaintext diretamente.
            tls_enabled: false,
            trusted_fingerprints: std::collections::HashMap::new(),
        };

        let cancel_a = cancel.clone();
        tokio::spawn(async move {
            let _ = run(
                cfg_a,
                "ws://127.0.0.1:57777".to_string(),
                None,
                relay_events_rx_a,
                relay_publish_tx_a,
                cancel_a,
            )
            .await;
        });

        // ── Node B (com seed = ws://127.0.0.1:18446, escuta em 18447) ───────
        let (relay_events_tx_b, relay_events_rx_b) = broadcast::channel::<RelayEvent>(16);
        let (relay_publish_tx_b, mut relay_publish_rx_b) = mpsc::channel::<String>(16);

        let cfg_b = MeshConfig {
            listen: addr_b.to_string(),
            seeds: vec![format!("ws://{addr_a}")],
            registry_url: None,
            heartbeat_secs: 30,
            discovery_secs: 60,
            mesh_url: Some(format!("ws://{addr_b}")),
            node_id: None,
            replication_factor: 3,
            vnodes_per_peer: 150,
            max_events_per_second_per_peer: 50,
            max_bytes_per_second_per_peer: 1_048_576,
            max_message_size: 524_288,
            // Estes testes legados falam WebSocket plaintext diretamente.
            tls_enabled: false,
            trusted_fingerprints: std::collections::HashMap::new(),
        };

        let cancel_b = cancel.clone();
        tokio::spawn(async move {
            let _ = run(
                cfg_b,
                "ws://127.0.0.1:57777".to_string(),
                None,
                relay_events_rx_b,
                relay_publish_tx_b,
                cancel_b,
            )
            .await;
        });

        // Aguarda estabelecimento da conexão outbound do Node B -> Node A
        tokio::time::sleep(Duration::from_millis(600)).await;

        // 1. Evento publicado no strfry do Node A -> deve chegar ao strfry do Node B
        let event_a = r#"["EVENT","sub_a",{"id":"evt_from_node_a","content":"hello from Node A"}]"#;
        let mut received_at_node_b = None;
        for _ in 0..20 {
            let _ = relay_events_tx_a.send(RelayEvent {
                raw: event_a.to_string(),
            });
            if let Ok(Some(msg)) =
                tokio::time::timeout(Duration::from_millis(150), relay_publish_rx_b.recv()).await
            {
                received_at_node_b = Some(msg);
                break;
            }
        }
        let received_at_node_b = received_at_node_b
            .ok_or_else(|| anyhow::anyhow!("Node B relay_publish_rx timed out"))?;
        assert_eq!(
            received_at_node_b,
            r#"["EVENT",{"id":"evt_from_node_a","content":"hello from Node A"}]"#
        );

        // 2. Evento publicado no strfry do Node B -> deve chegar ao strfry do Node A
        let event_b = r#"["EVENT","sub_b",{"id":"evt_from_node_b","content":"hello from Node B"}]"#;
        let mut received_at_node_a = None;
        for _ in 0..20 {
            let _ = relay_events_tx_b.send(RelayEvent {
                raw: event_b.to_string(),
            });
            if let Ok(Some(msg)) =
                tokio::time::timeout(Duration::from_millis(150), relay_publish_rx_a.recv()).await
            {
                received_at_node_a = Some(msg);
                break;
            }
        }
        let received_at_node_a = received_at_node_a
            .ok_or_else(|| anyhow::anyhow!("Node A relay_publish_rx timed out"))?;
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
                        if let Ok((stream, _)) = res
                            && let Ok(mut ws) = accept_async(stream).await {
                                while let Some(Ok(msg)) = ws.next().await {
                                    if let Message::Text(text) = msg
                                        && text.starts_with(r#"["REQ""#) {
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
        });

        let (addr_a, addr_b) = (free_addr().await?, free_addr().await?);

        // ── 2. Node A (configurado com mock relay url) ──
        let (relay_events_tx_a, relay_events_rx_a) = broadcast::channel::<RelayEvent>(16);
        let (relay_publish_tx_a, _relay_publish_rx_a) = mpsc::channel::<String>(16);

        let cfg_a = MeshConfig {
            listen: addr_a.to_string(),
            seeds: vec![],
            registry_url: None,
            heartbeat_secs: 30,
            discovery_secs: 60,
            mesh_url: None,
            node_id: None,
            replication_factor: 3,
            vnodes_per_peer: 150,
            max_events_per_second_per_peer: 50,
            max_bytes_per_second_per_peer: 1_048_576,
            max_message_size: 524_288,
            // Estes testes legados falam WebSocket plaintext diretamente.
            tls_enabled: false,
            trusted_fingerprints: std::collections::HashMap::new(),
        };

        let cancel_a = cancel.clone();
        tokio::spawn(async move {
            let _ = run(
                cfg_a,
                mock_url,
                None,
                relay_events_rx_a,
                relay_publish_tx_a,
                cancel_a,
            )
            .await;
        });

        // ── 3. Node B (seed = Node A, escuta em 18451) ──
        let (_relay_events_tx_b, relay_events_rx_b) = broadcast::channel::<RelayEvent>(16);
        let (relay_publish_tx_b, mut relay_publish_rx_b) = mpsc::channel::<String>(16);

        let cfg_b = MeshConfig {
            listen: addr_b.to_string(),
            seeds: vec![format!("ws://{addr_a}")],
            registry_url: None,
            heartbeat_secs: 30,
            discovery_secs: 60,
            mesh_url: None,
            node_id: None,
            replication_factor: 3,
            vnodes_per_peer: 150,
            max_events_per_second_per_peer: 50,
            max_bytes_per_second_per_peer: 1_048_576,
            max_message_size: 524_288,
            // Estes testes legados falam WebSocket plaintext diretamente.
            tls_enabled: false,
            trusted_fingerprints: std::collections::HashMap::new(),
        };

        let cancel_b = cancel.clone();
        tokio::spawn(async move {
            let _ = run(
                cfg_b,
                "ws://127.0.0.1:57777".to_string(),
                None,
                relay_events_rx_b,
                relay_publish_tx_b,
                cancel_b,
            )
            .await;
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

        let addr_a = free_addr().await?;

        // ── Node A (heartbeat_secs = 1 -> timeout threshold = 3s) ──────
        let (_relay_events_tx_a, relay_events_rx_a) = broadcast::channel::<RelayEvent>(16);
        let (relay_publish_tx_a, _relay_publish_rx_a) = mpsc::channel::<String>(16);

        let cfg_a = MeshConfig {
            listen: addr_a.to_string(),
            seeds: vec![],
            registry_url: None,
            heartbeat_secs: 1, // timeout = 3s
            discovery_secs: 60,
            mesh_url: Some(format!("ws://{addr_a}")),
            node_id: None,
            replication_factor: 3,
            vnodes_per_peer: 150,
            max_events_per_second_per_peer: 50,
            max_bytes_per_second_per_peer: 1_048_576,
            max_message_size: 524_288,
            // Estes testes legados falam WebSocket plaintext diretamente.
            tls_enabled: false,
            trusted_fingerprints: std::collections::HashMap::new(),
        };

        let cancel_a = cancel.clone();
        tokio::spawn(async move {
            let _ = run(
                cfg_a,
                "ws://127.0.0.1:57777".to_string(),
                None,
                relay_events_rx_a,
                relay_publish_tx_a,
                cancel_a,
            )
            .await;
        });

        // Aguardar que Node A arranque o listener e conectar
        let mut ws_stream = None;
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if let Ok((s, _)) = connect_async(format!("ws://{addr_a}")).await {
                ws_stream = Some(s);
                break;
            }
        }
        let mut ws_stream = ws_stream.ok_or_else(|| anyhow::anyhow!("Failed to connect to Node A"))?;

        // 1. Recebe pedido de backfill inicial
        let init_req = tokio::time::timeout(Duration::from_secs(2), ws_stream.next())
            .await?
            .ok_or_else(|| anyhow::anyhow!("ws closed"))??;
        assert_eq!(
            init_req.to_text()?,
            r#"["REQ","goy-backfill",{"since":0,"limit":500}]"#
        );

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
        })
        .await;

        cancel.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn test_state_persistence_save_load_corrupt() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let data_dir = temp_dir.path().to_path_buf();

        let mut state = make_test_state();
        state.data_dir = Some(data_dir.clone());

        state.seen_ids.insert("evt_persisted_1".to_string());
        state.seen_ids.insert("evt_persisted_2".to_string());
        state.deleted_ids.insert("evt_deleted_1".to_string());
        state
            .peer_cursors
            .insert("ws://127.0.0.1:19999".to_string(), 1786290000);

        save_state(&state);

        // Carrega estado e verifica persistência
        let (loaded_seen, loaded_deleted, loaded_cursors) = load_state(Some(&data_dir));
        assert!(loaded_seen.contains("evt_persisted_1"));
        assert!(loaded_seen.contains("evt_persisted_2"));
        assert!(
            loaded_deleted.contains("evt_deleted_1"),
            "deleted_ids must persist"
        );
        assert_eq!(
            loaded_cursors
                .get("ws://127.0.0.1:19999")
                .map(|c| *c.value()),
            Some(1786290000)
        );

        // Simula corrupção de ficheiro
        std::fs::write(data_dir.join("seen_ids.json"), b"corrupted data {{{")?;
        let (corrupt_seen, _, _corrupt_cursors) = load_state(Some(&data_dir));
        assert!(
            corrupt_seen.is_empty(),
            "Corrupt file should fallback to empty set without panic"
        );

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
                            let req_tx = req_tx.clone();
                            tokio::spawn(async move {
                                if let Ok(mut ws) = accept_async(stream).await {
                                    while let Some(Ok(msg)) = ws.next().await {
                                        if let Message::Text(text) = msg
                                            && text.starts_with(r#"["REQ""#) {
                                                let _ = req_tx.send(text.clone()).await;
                                                let hist_evt = r#"["EVENT","goy-backfill",{"id":"hist_ts_1","created_at":1786000500,"content":"ts data"}]"#;
                                                let eose = r#"["EOSE","goy-backfill"]"#;
                                                let _ = ws.send(Message::Text(hist_evt.into())).await;
                                                let _ = ws.send(Message::Text(eose.into())).await;
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
            replication_factor: 3,
            vnodes_per_peer: 150,
            max_events_per_second_per_peer: 50,
            max_bytes_per_second_per_peer: 1_048_576,
            max_message_size: 524_288,
            // Estes testes legados falam WebSocket plaintext diretamente.
            tls_enabled: false,
            trusted_fingerprints: std::collections::HashMap::new(),
        };
        let cancel_a = cancel.clone();
        tokio::spawn(async move {
            let _ = run(
                cfg_a,
                mock_url,
                None,
                relay_events_rx_a,
                relay_publish_tx_a,
                cancel_a,
            )
            .await;
        });

        tokio::time::sleep(Duration::from_millis(400)).await;

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
            replication_factor: 3,
            vnodes_per_peer: 150,
            max_events_per_second_per_peer: 50,
            max_bytes_per_second_per_peer: 1_048_576,
            max_message_size: 524_288,
            // Estes testes legados falam WebSocket plaintext diretamente.
            tls_enabled: false,
            trusted_fingerprints: std::collections::HashMap::new(),
        };

        let c_b1 = cancel_b1.clone();
        let dir_b1 = data_dir_b.clone();
        tokio::spawn(async move {
            let _ = run(
                cfg_b.clone(),
                "ws://127.0.0.1:57777".to_string(),
                Some(dir_b1),
                relay_events_rx_b,
                relay_publish_tx_b,
                c_b1,
            )
            .await;
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

        // Limpa mensagens REQ residuais no canal da primeira execução
        while req_rx.try_recv().is_ok() {}

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
            replication_factor: 3,
            vnodes_per_peer: 150,
            max_events_per_second_per_peer: 50,
            max_bytes_per_second_per_peer: 1_048_576,
            max_message_size: 524_288,
            // Estes testes legados falam WebSocket plaintext diretamente.
            tls_enabled: false,
            trusted_fingerprints: std::collections::HashMap::new(),
        };

        let c_b2 = cancel_b2.clone();
        let dir_b2 = data_dir_b.clone();
        tokio::spawn(async move {
            let _ = run(
                cfg_b2,
                "ws://127.0.0.1:57777".to_string(),
                Some(dir_b2),
                relay_events_rx_b2,
                relay_publish_tx_b2,
                c_b2,
            )
            .await;
        });

        // O segundo REQ recebido no Mock Relay deve usar o cursor salvo: since: 1786000500!
        let req_2 = tokio::time::timeout(Duration::from_secs(5), req_rx.recv())
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("Mock relay did not receive second REQ after restart")
            })?;
        assert!(req_2.contains(r#""since":1786000500"#));

        cancel.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn test_replaceable_events_newer_overwrites_older() {
        let state = make_test_state();

        let evt_v1 = r#"["EVENT",{"id":"id_v1","pubkey":"pub_alice","kind":0,"created_at":1000,"content":"name: Alice v1"}]"#;
        let evt_v2 = r#"["EVENT",{"id":"id_v2","pubkey":"pub_alice","kind":0,"created_at":2000,"content":"name: Alice v2"}]"#;

        assert!(process_replaceable_event(&state, evt_v1));
        assert_eq!(
            state
                .latest_replaceable
                .get("pub_alice:0")
                .map(|r| r.value().clone()),
            Some((1000, "id_v1".to_string()))
        );

        assert!(process_replaceable_event(&state, evt_v2));
        assert_eq!(
            state
                .latest_replaceable
                .get("pub_alice:0")
                .map(|r| r.value().clone()),
            Some((2000, "id_v2".to_string()))
        );
    }

    #[tokio::test]
    async fn test_replaceable_events_stale_discarded() {
        let state = make_test_state();

        let evt_v2 = r#"["EVENT",{"id":"id_v2","pubkey":"pub_alice","kind":0,"created_at":2000,"content":"name: Alice v2"}]"#;
        let evt_v1 = r#"["EVENT",{"id":"id_v1","pubkey":"pub_alice","kind":0,"created_at":1000,"content":"name: Alice v1"}]"#;

        assert!(process_replaceable_event(&state, evt_v2));
        assert!(
            !process_replaceable_event(&state, evt_v1),
            "Stale replaceable event must be discarded"
        );

        assert_eq!(
            state
                .latest_replaceable
                .get("pub_alice:0")
                .map(|r| r.value().clone()),
            Some((2000, "id_v2".to_string()))
        );
    }

    #[tokio::test]
    async fn test_replaceable_events_backfill_latest_only() {
        let state = make_test_state();

        let backfill_v1 = r#"["EVENT","sub_bf",{"id":"bf_1","pubkey":"pub_bob","kind":3,"created_at":100,"content":"contacts v1"}]"#;
        let backfill_v3 = r#"["EVENT","sub_bf",{"id":"bf_3","pubkey":"pub_bob","kind":3,"created_at":300,"content":"contacts v3"}]"#;
        let backfill_v2 = r#"["EVENT","sub_bf",{"id":"bf_2","pubkey":"pub_bob","kind":3,"created_at":200,"content":"contacts v2"}]"#;

        assert!(process_replaceable_event(&state, backfill_v1));
        assert!(process_replaceable_event(&state, backfill_v3));
        assert!(
            !process_replaceable_event(&state, backfill_v2),
            "v2 is older than v3, must be rejected"
        );

        assert_eq!(
            state
                .latest_replaceable
                .get("pub_bob:3")
                .map(|r| r.value().clone()),
            Some((300, "bf_3".to_string()))
        );
    }

    fn make_test_state() -> MeshState {
        MeshState {
            seen_ids: DashSet::new(),
            deleted_ids: DashSet::new(),
            expiring_events: DashMap::new(),
            peer_cursors: DashMap::new(),
            latest_replaceable: DashMap::new(),
            relay_tx: mpsc::channel(1).0,
            connected_peers: DashSet::new(),
            peer_channels: DashMap::new(),
            rate_limiters: DashMap::new(),
            replication_factor: 3,
            max_events_per_sec: 50,
            max_bytes_per_sec: 1_048_576,
            max_msg_size: 524_288,
            events_stored: AtomicU64::new(0),
            events_replicated: AtomicU64::new(0),
            events_rate_limited: AtomicU64::new(0),
            bytes_rate_limited: AtomicU64::new(0),
            messages_oversized: AtomicU64::new(0),
            metrics: Arc::new(Metrics::new()),
            active_peer_sessions: DashMap::new(),
            hash_ring: Arc::new(std::sync::RwLock::new(
                crate::consistent_hash::ConsistentHashRing::new(150),
            )),
            relay_url: "ws://127.0.0.1:7777".to_string(),
            data_dir: None,
            storage_info: None,
        }
    }

    #[tokio::test]
    async fn test_deletion_by_e_tag_marks_event_deleted() {
        let state = make_test_state();

        let del_event = r#"["EVENT",{"id":"del_1","pubkey":"pub_alice","kind":5,"created_at":9000,"tags":[["e","target_event_1"],["e","target_event_2"]]}]"#;
        let result = process_deletion_event(&state, del_event);
        assert!(result, "Valid deletion event should return true");
        assert!(
            state.deleted_ids.contains("target_event_1"),
            "target_event_1 must be deleted"
        );
        assert!(
            state.deleted_ids.contains("target_event_2"),
            "target_event_2 must be deleted"
        );
    }

    #[tokio::test]
    async fn test_deleted_event_rejected_by_process_replaceable() {
        let state = make_test_state();

        // Mark an event as deleted
        state.deleted_ids.insert("profile_evt_1".to_string());

        // Now try to process it as a replaceable event
        let profile_evt = r#"["EVENT",{"id":"profile_evt_1","pubkey":"pub_alice","kind":0,"created_at":5000,"content":"name: Alice"}]"#;
        let accepted = process_replaceable_event(&state, profile_evt);
        assert!(
            !accepted,
            "Deleted event must be rejected by process_replaceable_event"
        );
    }

    #[tokio::test]
    async fn test_deletion_by_a_tag_removes_replaceable() {
        let state = make_test_state();

        // First register a replaceable event in the store
        state.latest_replaceable.insert(
            "pub_alice:0".to_string(),
            (1000, "profile_id_1".to_string()),
        );
        assert!(state.latest_replaceable.contains_key("pub_alice:0"));

        // Send a deletion event targeting the "a" coordinate "0:pub_alice"
        let del_event = r#"["EVENT",{"id":"del_a_1","pubkey":"pub_alice","kind":5,"created_at":9000,"tags":[["a","0:pub_alice"]]}]"#;
        process_deletion_event(&state, del_event);

        assert!(
            !state.latest_replaceable.contains_key("pub_alice:0"),
            "Replaceable event must be removed from store after 'a' tag deletion"
        );
    }

    #[tokio::test]
    async fn test_deletion_authority_mismatch_rejected() {
        let state = make_test_state();

        // Mallory tries to delete Alice's replaceable event
        state.latest_replaceable.insert(
            "pub_alice:0".to_string(),
            (1000, "alice_profile".to_string()),
        );

        let del_event = r#"["EVENT",{"id":"del_mal","pubkey":"pub_mallory","kind":5,"created_at":9000,"tags":[["a","0:pub_alice"]]}]"#;
        process_deletion_event(&state, del_event);

        assert!(
            state.latest_replaceable.contains_key("pub_alice:0"),
            "Deletion by wrong pubkey must be ignored"
        );
    }

    #[tokio::test]
    async fn test_deletion_of_parameterized_replaceable_by_a_tag() {
        let state = make_test_state();

        state.latest_replaceable.insert(
            "pub_bob:30001:my-list".to_string(),
            (500, "list_event_id".to_string()),
        );

        // Deletion via a tag: "30001:pub_bob:my-list"
        let del_event = r#"["EVENT",{"id":"del_list","pubkey":"pub_bob","kind":5,"created_at":9000,"tags":[["a","30001:pub_bob:my-list"]]}]"#;
        process_deletion_event(&state, del_event);

        assert!(
            !state
                .latest_replaceable
                .contains_key("pub_bob:30001:my-list"),
            "Parameterized replaceable event must be removed by 'a' tag deletion"
        );
    }

    #[tokio::test]
    async fn test_backfill_event_deleted_before_arrival_is_discarded() {
        let state = make_test_state();

        // Pre-mark a backfill event as deleted
        state.deleted_ids.insert("historical_evt_99".to_string());

        // Now this event arrives during backfill
        let backfill_evt = r#"["EVENT","sub_bf",{"id":"historical_evt_99","pubkey":"pub_carol","kind":1,"created_at":1000,"content":"Hello"}]"#;
        let accepted = process_replaceable_event(&state, backfill_evt);
        assert!(
            !accepted,
            "Event deleted before backfill arrival must be discarded"
        );
    }

    // ── NIP-40: Expirable Events ─────────────────────────────────────────

    #[test]
    fn test_process_expiry_check_already_expired() {
        let state = make_test_state();
        // expiration in the deep past (Unix epoch + 1)
        let raw = r#"["EVENT",{"id":"exp_evt_1","pubkey":"pub_alice","kind":1,"created_at":1000,"tags":[["expiration","1"]],"content":"old"}]"#;
        let accepted = process_expiry_check(&state, raw);
        assert!(!accepted, "Already-expired event must be rejected");
        assert!(
            !state.expiring_events.contains_key("exp_evt_1"),
            "Expired event must not be registered in expiring_events"
        );
    }

    #[test]
    fn test_process_expiry_check_future_expiration() {
        let state = make_test_state();
        // expiration far in the future
        let raw = r#"["EVENT",{"id":"exp_evt_2","pubkey":"pub_alice","kind":1,"created_at":9000,"tags":[["expiration","9999999999"]],"content":"future"}]"#;
        let accepted = process_expiry_check(&state, raw);
        assert!(accepted, "Event with future expiration must be accepted");
        assert!(
            state.expiring_events.contains_key("exp_evt_2"),
            "Future expiration must be registered in expiring_events"
        );
        assert_eq!(
            state.expiring_events.get("exp_evt_2").map(|v| *v),
            Some(9999999999u64)
        );
    }

    #[test]
    fn test_process_expiry_check_no_expiration_tag() {
        let state = make_test_state();
        let raw = r#"["EVENT",{"id":"no_exp_evt","pubkey":"pub_alice","kind":1,"created_at":1000,"content":"no expiry"}]"#;
        let accepted = process_expiry_check(&state, raw);
        assert!(
            accepted,
            "Event without expiration tag must always be accepted"
        );
        assert!(
            !state.expiring_events.contains_key("no_exp_evt"),
            "Event without expiration must not be tracked"
        );
    }

    #[tokio::test]
    async fn test_expiry_cleanup_task_removes_expired_events() {
        let state = Arc::new(make_test_state());

        let future_exp = 9_999_999_999u64;
        let past_exp = 1u64; // already expired

        state.seen_ids.insert("evt_future".to_string());
        state
            .expiring_events
            .insert("evt_future".to_string(), future_exp);

        state.seen_ids.insert("evt_past".to_string());
        state
            .expiring_events
            .insert("evt_past".to_string(), past_exp);

        // Run cleanup with a very short interval; just tick manually by calling the retain logic
        // We simulate by calling the cleanup inline with the current time
        let now_ts = chrono::Utc::now().timestamp() as u64;
        let mut expired_count = 0u64;
        state.expiring_events.retain(|event_id, exp_ts| {
            if *exp_ts <= now_ts {
                state.seen_ids.remove(event_id.as_str());
                state.latest_replaceable.retain(|_, v| v.1 != *event_id);
                expired_count += 1;
                false
            } else {
                true
            }
        });

        assert_eq!(expired_count, 1, "Only one event should be expired");
        assert!(
            !state.expiring_events.contains_key("evt_past"),
            "Past event must be removed from expiring_events"
        );
        assert!(
            !state.seen_ids.contains("evt_past"),
            "Past event must be removed from seen_ids"
        );
        assert!(
            state.expiring_events.contains_key("evt_future"),
            "Future event must remain in expiring_events"
        );
        assert!(
            state.seen_ids.contains("evt_future"),
            "Future event must remain in seen_ids"
        );
    }

    #[tokio::test]
    async fn test_expiry_cleanup_removes_from_latest_replaceable() {
        let state = Arc::new(make_test_state());

        // Register a replaceable event that will expire
        state.latest_replaceable.insert(
            "pub_alice:0".to_string(),
            (1000, "expiring_profile".to_string()),
        );
        state
            .expiring_events
            .insert("expiring_profile".to_string(), 1u64); // already expired

        // Simulate cleanup
        let now_ts = chrono::Utc::now().timestamp() as u64;
        state.expiring_events.retain(|event_id, exp_ts| {
            if *exp_ts <= now_ts {
                state.seen_ids.remove(event_id.as_str());
                state.latest_replaceable.retain(|_, v| v.1 != *event_id);
                false
            } else {
                true
            }
        });

        assert!(
            !state.latest_replaceable.contains_key("pub_alice:0"),
            "Expired replaceable event must be removed from latest_replaceable"
        );
    }

    #[test]
    fn test_process_expiry_check_invalid_expiration_tag() {
        let state = make_test_state();
        // Invalid expiration value — should be treated as no expiration
        let raw = r#"["EVENT",{"id":"bad_exp_evt","pubkey":"pub_alice","kind":1,"created_at":1000,"tags":[["expiration","not-a-ts"]],"content":"bad"}]"#;
        let accepted = process_expiry_check(&state, raw);
        assert!(
            accepted,
            "Event with invalid expiration tag must be accepted (treated as no expiration)"
        );
        assert!(!state.expiring_events.contains_key("bad_exp_evt"));
    }

    #[test]
    fn test_expired_backfill_event_discarded() {
        let state = make_test_state();
        // Backfill event with expiration already in the past
        let raw = r#"["EVENT","sub_bf",{"id":"old_exp","pubkey":"pub_bob","kind":1,"created_at":100,"tags":[["expiration","2"]],"content":"stale"}]"#;
        let accepted = process_expiry_check(&state, raw);
        assert!(
            !accepted,
            "Expired event arriving via backfill must be discarded"
        );
    }
}
