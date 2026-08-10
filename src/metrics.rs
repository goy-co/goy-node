//! Métricas Prometheus e estado de observabilidade do Goy Node.
//!
//!	Um `Metrics` agrega counters e gauges atómicos e formata-os em Prometheus
//! text format via [`Metrics::render_prometheus`]. O struct é criado uma vez no
//! arranque e partilhado como `Arc<Metrics>`, tanto pelo `MeshState` como pelo
//! servidor HTTP (`crate::http`).
//!
//! ## Métricas expostas
//!
//! | Nome | Tipo | Labels | Descrição |
//! |------|------|--------|-----------|
//! | `goy_events_received_total` | counter | `source=relay\|peer` | Eventos recebidos (do relay local ou de peers) |
//! | `goy_events_replicated_total` | counter | — | Eventos replicados para peers |
//! | `goy_events_rate_limited_total` | counter | — | Eventos rejeitados por rate limiting |
//! | `goy_events_expired_total` | counter | — | Eventos removidos pelo cleanup NIP-40 |
//! | `goy_events_deleted_total` | counter | — | Eventos marcados como deletados (NIP-09) |
//! | `goy_peers_connected` | gauge | — | Peers atualmente conectados (inbound+outbound) |
//! | `goy_backfill_requests_total` | counter | — | Pedidos de backfill (REQ) recebidos de peers |
//! | `goy_messages_oversized_total` | counter | — | Mensagens rejeitadas por exceder o tamanho máximo |
//! | `goy_uptime_seconds` | gauge | — | Tempo de atividade do nó em segundos |
//!
//! Os valores são lidos com `Ordering::Relaxed` — aceitável para métricas
//! de monotorização onde algum desvio momentâneo não é problemático.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Origem de eventos para o counter `goy_events_received_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventSource {
    /// Evento consumido do relay local (strfry).
    Relay,
    /// Evento recebido de um peer da mesh.
    Peer,
}

impl EventSource {
    /// Rótulo Prometheus usado como valor da label `source` no counter
    /// `goy_events_received_total`. Mantém apenas `relay` e `peer`, conforme
    /// a especificação observável do projeto.
    pub const fn as_str(self) -> &'static str {
        match self {
            EventSource::Relay => "relay",
            EventSource::Peer => "peer",
        }
    }
}

/// Métricas do nó, partilhadas entre o mesh agent e o servidor HTTP.
///
/// Todos os campos são `AtomicU64` para poderem ser incrementados/lidos sem
/// mutex. O `started_at` regista o instante de arranque para calcular o
/// gauge de uptime sob demanda.
pub struct Metrics {
    // Contadores com label `source`.
    pub events_received_relay: AtomicU64,
    pub events_received_peer: AtomicU64,
    // Resto dos contadores e gauges.
    pub events_replicated: AtomicU64,
    pub events_rate_limited: AtomicU64,
    pub events_expired: AtomicU64,
    pub events_deleted: AtomicU64,
    pub backfill_requests: AtomicU64,
    pub messages_oversized: AtomicU64,
    /// peers conectados (gauge). Atualizado pelo MeshState quando um peer
    /// liga/desliga — não é derivado dos counters.
    pub peers_connected: AtomicU64,
    /// Métricas de Consistent Hashing e Rebalanceamento.
    pub hash_ring_peers: AtomicU64,
    pub hash_ring_vnodes: AtomicU64,
    pub rebalance_events_sent: AtomicU64,
    /// Instante de arranque do nó, para cálculo do gauge `goy_uptime_seconds`.
    pub started_at: Instant,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Cria um novo conjunto de métricas zerado.
    pub fn new() -> Self {
        Self {
            events_received_relay: AtomicU64::new(0),
            events_received_peer: AtomicU64::new(0),
            events_replicated: AtomicU64::new(0),
            events_rate_limited: AtomicU64::new(0),
            events_expired: AtomicU64::new(0),
            events_deleted: AtomicU64::new(0),
            backfill_requests: AtomicU64::new(0),
            messages_oversized: AtomicU64::new(0),
            peers_connected: AtomicU64::new(0),
            hash_ring_peers: AtomicU64::new(0),
            hash_ring_vnodes: AtomicU64::new(0),
            rebalance_events_sent: AtomicU64::new(0),
            started_at: Instant::now(),
        }
    }

    /// Incrementa `goy_events_received_total` (label `source=relay|peer`).
    /// Conveniência para evitar repetir `match` em cada ponto.
    pub fn inc_events_received(&self, source: EventSource) {
        let target = match source {
            EventSource::Relay => &self.events_received_relay,
            EventSource::Peer => &self.events_received_peer,
        };
        target.fetch_add(1, Ordering::Relaxed);
    }

    /// Marca a conexão de um peer: ajusta o gauge `peers_connected` como +1 numa operação.
    pub fn inc_peers_connected(&self) {
        self.peers_connected.fetch_add(1, Ordering::Relaxed);
    }

    /// Marca a desconexão de um peer: ajusta o gauge `peers_connected` como -1 numa operação.
    pub fn dec_peers_connected(&self) {
        let prev = self.peers_connected.fetch_sub(1, Ordering::Relaxed);
        // Nunca deixa o gauge abaixo de zero (pode acontecer se um Double::Drop
        // chamar dec sem o inc correspondente).
        if prev == 0 {
            self.peers_connected.store(0, Ordering::Relaxed);
        }
    }

    /// Lê e retorna o número de peers conectados (gauge).
    pub fn peers_connected(&self) -> u64 {
        self.peers_connected.load(Ordering::Relaxed)
    }

    /// Calcula o uptime decorrido desde [`Metrics::new`] em segundos.
    pub fn uptime_seconds(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    /// Sincroniza o gauge `peers_connected` com o estado real do `MeshState`.
    /// Usado para inicialização e periodicamente para self-heal de desvios.
    pub fn set_peers_connected(&self, count: u64) {
        self.peers_connected.store(count, Ordering::Relaxed);
    }

    /// Renderiza todas as métricas no formato texto Prometheus 0.0.4.
    ///
    /// Cada métrica é precedida de comentários `# HELP` e `# TYPE`. O gauge
    /// `goy_peers_connected` reflete o peer_connector count atual.
    /// O counter `goy_events_received_total` tem label (`source`), gerando
    /// duas linhas (relay, peer).
    ///
    /// A ordem dos bytes em cada linha é determinada por Prometheus:
    /// `metric_name{label="value"}value\n`.
    pub fn render_prometheus(&self) -> String {
        let received_relay = self.events_received_relay.load(Ordering::Relaxed);
        let received_peer = self.events_received_peer.load(Ordering::Relaxed);
        let replicated = self.events_replicated.load(Ordering::Relaxed);
        let rate_limited = self.events_rate_limited.load(Ordering::Relaxed);
        let expired = self.events_expired.load(Ordering::Relaxed);
        let deleted = self.events_deleted.load(Ordering::Relaxed);
        let backfill = self.backfill_requests.load(Ordering::Relaxed);
        let oversized = self.messages_oversized.load(Ordering::Relaxed);
        let peers = self.peers_connected.load(Ordering::Relaxed);
        let uptime = self.uptime_seconds();

        let mut s = String::with_capacity(2048);

        // goy_events_received_total{source="relay"}
        s.push_str("# HELP goy_events_received_total Total events received from relay or peers.\n");
        s.push_str("# TYPE goy_events_received_total counter\n");
        s.push_str(&format!("goy_events_received_total{{source=\"relay\"}} {received_relay}\n"));
        s.push_str(&format!("goy_events_received_total{{source=\"peer\"}} {received_peer}\n"));

        // goy_events_replicated_total
        s.push_str("# HELP goy_events_replicated_total Total events replicated to peers (N-of-M).\n");
        s.push_str("# TYPE goy_events_replicated_total counter\n");
        s.push_str(&format!("goy_events_replicated_total {replicated}\n"));

        // goy_events_rate_limited_total
        s.push_str("# HELP goy_events_rate_limited_total Events rejected by per-peer rate limiting.\n");
        s.push_str("# TYPE goy_events_rate_limited_total counter\n");
        s.push_str(&format!("goy_events_rate_limited_total {rate_limited}\n"));

        // goy_events_expired_total
        s.push_str("# HELP goy_events_expired_total Events removed by NIP-40 expiry cleanup.\n");
        s.push_str("# TYPE goy_events_expired_total counter\n");
        s.push_str(&format!("goy_events_expired_total {expired}\n"));

        // goy_events_deleted_total
        s.push_str("# HELP goy_events_deleted_total Events marked as deleted (NIP-09).\n");
        s.push_str("# TYPE goy_events_deleted_total counter\n");
        s.push_str(&format!("goy_events_deleted_total {deleted}\n"));

        // goy_peers_connected
        s.push_str("# HELP goy_peers_connected Number of peer connections currently active (inbound + outbound).\n");
        s.push_str("# TYPE goy_peers_connected gauge\n");
        s.push_str(&format!("goy_peers_connected {peers}\n"));

        // goy_backfill_requests_total
        s.push_str("# HELP goy_backfill_requests_total Backfill REQ messages received from peers.\n");
        s.push_str("# TYPE goy_backfill_requests_total counter\n");
        s.push_str(&format!("goy_backfill_requests_total {backfill}\n"));

        // goy_messages_oversized_total
        s.push_str("# HELP goy_messages_oversized_total Messages rejected for exceeding max_message_size.\n");
        s.push_str("# TYPE goy_messages_oversized_total counter\n");
        s.push_str(&format!("goy_messages_oversized_total {oversized}\n"));

        // goy_hash_ring_peers
        let ring_peers = self.hash_ring_peers.load(Ordering::Relaxed);
        s.push_str("# HELP goy_hash_ring_peers Number of physical peers present in the consistent hash ring.\n");
        s.push_str("# TYPE goy_hash_ring_peers gauge\n");
        s.push_str(&format!("goy_hash_ring_peers {ring_peers}\n"));

        // goy_hash_ring_vnodes
        let ring_vnodes = self.hash_ring_vnodes.load(Ordering::Relaxed);
        s.push_str("# HELP goy_hash_ring_vnodes Total virtual nodes active in the consistent hash ring.\n");
        s.push_str("# TYPE goy_hash_ring_vnodes gauge\n");
        s.push_str(&format!("goy_hash_ring_vnodes {ring_vnodes}\n"));

        // goy_rebalance_events_sent_total
        let rebalanced = self.rebalance_events_sent.load(Ordering::Relaxed);
        s.push_str("# HELP goy_rebalance_events_sent_total Total events transferred during background hash ring rebalancing.\n");
        s.push_str("# TYPE goy_rebalance_events_sent_total counter\n");
        s.push_str(&format!("goy_rebalance_events_sent_total {rebalanced}\n"));

        // goy_uptime_seconds
        s.push_str("# HELP goy_uptime_seconds Seconds since the node process started.\n");
        s.push_str("# TYPE goy_uptime_seconds gauge\n");
        s.push_str(&format!("goy_uptime_seconds {uptime}\n"));

        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_prometheus_includes_all_metrics_and_correct_values() {
        let m = Metrics::new();
        m.inc_events_received(EventSource::Relay);
        m.inc_events_received(EventSource::Relay);
        m.inc_events_received(EventSource::Peer);
        m.events_replicated.fetch_add(5, Ordering::Relaxed);
        m.events_rate_limited.fetch_add(2, Ordering::Relaxed);
        m.events_expired.fetch_add(7, Ordering::Relaxed);
        m.events_deleted.fetch_add(3, Ordering::Relaxed);
        m.backfill_requests.fetch_add(1, Ordering::Relaxed);
        m.messages_oversized.fetch_add(4, Ordering::Relaxed);
        m.set_peers_connected(11);

        let out = m.render_prometheus();

        // HELP / TYPE / valor para cada métrica
        for expected in [
            "# HELP goy_events_received_total Total events received from relay or peers.",
            "# TYPE goy_events_received_total counter",
            "goy_events_received_total{source=\"relay\"} 2",
            "goy_events_received_total{source=\"peer\"} 1",
            "# TYPE goy_events_replicated_total counter",
            "goy_events_replicated_total 5",
            "goy_events_rate_limited_total 2",
            "goy_events_expired_total 7",
            "goy_events_deleted_total 3",
            "# TYPE goy_peers_connected gauge",
            "goy_peers_connected 11",
            "goy_backfill_requests_total 1",
            "goy_messages_oversized_total 4",
            "# TYPE goy_uptime_seconds gauge",
        ] {
            assert!(
                out.contains(expected),
                "expected Prometheus output to contain {expected:?}, got:\n{out}"
            );
        }
    }

    /// O gauge `goy_peers_connected` nunca deve baixar de zero.
    #[test]
    fn test_peers_connected_clamped_at_zero() {
        let m = Metrics::new();
        m.dec_peers_connected(); // tenta ir para -1
        assert_eq!(m.peers_connected(), 0);
        m.inc_peers_connected();
        m.inc_peers_connected();
        assert_eq!(m.peers_connected(), 2);
        m.dec_peers_connected();
        m.dec_peers_connected();
        m.dec_peers_connected(); // extra dec
        assert_eq!(m.peers_connected(), 0);
    }

    #[test]
    fn test_uptime_is_monotonically_nondecreasing() {
        let m = Metrics::new();
        let t0 = m.uptime_seconds();
        std::thread::sleep(std::time::Duration::from_millis(1100));
        let t1 = m.uptime_seconds();
        assert!(t1 >= t0, "uptime must not go backwards: {t0} -> {t1}");
    }

    /// Verifica que o formato produzido é compatível com o regex de uma linha
    /// Prometheus: `metric{name="value"} number`.
    #[test]
    fn test_prometheus_line_format_wellformed() {
        let m = Metrics::new();
        m.inc_events_received(EventSource::Relay);
        let out = m.render_prometheus();
        for line in out.lines() {
            if line.starts_with('#') || line.is_empty() {
                continue;
            }
            // cada linha de métrica deve ter pelo menos um espaço separando nome/labels do valor
            let Some((_, value)) = line.rsplit_once(' ') else {
                panic!("malformed Prometheus line (no value separator): {line:?}");
            };
            assert!(value.parse::<u64>().is_ok(), "value must be a non-negative integer: {value:?} in line {line:?}");
        }
    }
}
