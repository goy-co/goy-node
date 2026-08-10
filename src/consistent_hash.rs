//! Anel de Consistent Hashing com Virtual Nodes (vnodes) para distribuição uniforme de dados.
//!
//! Cada peer físico é mapeado para `vnodes_per_peer` posições virtuais no anel.
//! A busca por réplicas responsáveis percorre o anel no sentido dos ponteiros do relógio (clockwise),
//! selecionando N peers físicos distintos.

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// Anel de consistent hashing determinístico.
#[derive(Debug, Clone)]
pub struct ConsistentHashRing {
    /// Número de nós virtuais por peer físico (default: 150).
    vnodes_per_peer: usize,
    /// Mapeamento: posição hash no anel -> peer_id físico.
    ring: BTreeMap<u64, String>,
    /// Lista de peers físicos presentes no anel.
    peers: Vec<String>,
}

#[allow(dead_code)]
impl ConsistentHashRing {
    /// Cria um novo anel de consistent hashing.
    pub fn new(vnodes_per_peer: usize) -> Self {
        Self {
            vnodes_per_peer: vnodes_per_peer.max(1),
            ring: BTreeMap::new(),
            peers: Vec::new(),
        }
    }

    /// Retorna a quantidade de vnodes por peer físico.
    pub fn vnodes_per_peer(&self) -> usize {
        self.vnodes_per_peer
    }

    /// Adiciona um peer físico ao anel gerando `vnodes_per_peer` posições virtuais.
    pub fn add_peer(&mut self, peer_id: &str) -> bool {
        if self.peers.iter().any(|p| p == peer_id) {
            return false;
        }

        self.peers.push(peer_id.to_string());
        self.peers.sort();

        for i in 0..self.vnodes_per_peer {
            let hash = hash_key(&format!("{peer_id}:vnode:{i}"));
            self.ring.insert(hash, peer_id.to_string());
        }

        true
    }

    /// Remove um peer físico e todos os seus vnodes do anel.
    pub fn remove_peer(&mut self, peer_id: &str) -> bool {
        if !self.peers.iter().any(|p| p == peer_id) {
            return false;
        }

        self.peers.retain(|p| p != peer_id);

        for i in 0..self.vnodes_per_peer {
            let hash = hash_key(&format!("{peer_id}:vnode:{i}"));
            self.ring.remove(&hash);
        }

        true
    }

    /// Retorna a quantidade de peers físicos no anel.
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Retorna a quantidade total de vnodes ativos no anel.
    pub fn vnode_count(&self) -> usize {
        self.ring.len()
    }

    /// Retorna a lista de peers físicos atualmente no anel.
    pub fn get_peers(&self) -> Vec<String> {
        self.peers.clone()
    }

    /// Retorna o primeiro peer primário responsável por uma chave.
    pub fn get_primary_peer(&self, key: &str) -> Option<String> {
        self.get_responsible_peers(key, 1).into_iter().next()
    }

    /// Retorna `replication_factor` peers físicos distintos responsáveis pela chave.
    /// Percorre o anel no sentido dos ponteiros do relógio (clockwise).
    pub fn get_responsible_peers(&self, key: &str, replication_factor: usize) -> Vec<String> {
        if self.ring.is_empty() || replication_factor == 0 {
            return Vec::new();
        }

        let target_count = replication_factor.min(self.peers.len());
        let hash_val = hash_key(key);

        let mut result = Vec::with_capacity(target_count);

        let clockwise = self.ring.range(hash_val..);
        let wrapped = self.ring.range(..hash_val);

        for (_hash, peer) in clockwise.chain(wrapped) {
            if !result.contains(peer) {
                result.push(peer.clone());
                if result.len() == target_count {
                    break;
                }
            }
        }

        result
    }
}

/// Computa o valor hash u64 de uma string usando os primeiros 8 bytes do SHA-256.
pub fn hash_key(key: &str) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    let result = hasher.finalize();
    u64::from_be_bytes(result[0..8].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_ring_add_and_remove_peer() {
        let mut ring = ConsistentHashRing::new(10);
        assert_eq!(ring.peer_count(), 0);
        assert_eq!(ring.vnode_count(), 0);

        assert!(ring.add_peer("peer-1"));
        assert_eq!(ring.peer_count(), 1);
        assert_eq!(ring.vnode_count(), 10);

        assert!(!ring.add_peer("peer-1")); // duplicado recusa

        assert!(ring.add_peer("peer-2"));
        assert_eq!(ring.peer_count(), 2);
        assert_eq!(ring.vnode_count(), 20);

        assert!(ring.remove_peer("peer-1"));
        assert_eq!(ring.peer_count(), 1);
        assert_eq!(ring.vnode_count(), 10);
    }

    #[test]
    fn test_responsible_peers_distinct_and_deterministic() {
        let mut ring = ConsistentHashRing::new(50);
        ring.add_peer("node-1");
        ring.add_peer("node-2");
        ring.add_peer("node-3");
        ring.add_peer("node-4");

        let key = "event_abc123";

        let resp1 = ring.get_responsible_peers(key, 3);
        let resp2 = ring.get_responsible_peers(key, 3);

        assert_eq!(resp1, resp2, "Must be deterministic");
        assert_eq!(resp1.len(), 3);

        let unique_peers: std::collections::HashSet<_> = resp1.iter().collect();
        assert_eq!(unique_peers.len(), 3);
    }

    #[test]
    fn test_uniform_distribution_across_peers() {
        let mut ring = ConsistentHashRing::new(150);
        let peers = vec!["node-a", "node-b", "node-c", "node-d", "node-e"];
        for p in &peers {
            ring.add_peer(p);
        }

        let mut counts: HashMap<String, usize> = HashMap::new();
        let total_keys = 10_000;

        for i in 0..total_keys {
            let key = format!("event_key_{i}");
            if let Some(primary) = ring.get_primary_peer(&key) {
                *counts.entry(primary).or_default() += 1;
            }
        }

        assert_eq!(counts.len(), peers.len());
        let expected_per_peer = total_keys / peers.len();

        for (peer, count) in &counts {
            let deviation = ((*count as f64 - expected_per_peer as f64).abs()
                / expected_per_peer as f64)
                * 100.0;
            assert!(
                deviation < 15.0,
                "Peer {peer} count {count} deviates by {deviation:.2}% (expected ~{expected_per_peer})"
            );
        }
    }

    #[test]
    fn test_peer_addition_minimal_key_movement() {
        let mut ring = ConsistentHashRing::new(150);
        let peers = vec!["node-1", "node-2", "node-3", "node-4"];
        for p in &peers {
            ring.add_peer(p);
        }

        let total_keys = 1_000;
        let initial_mapping: Vec<String> = (0..total_keys)
            .map(|i| ring.get_primary_peer(&format!("key_{i}")).unwrap())
            .collect();

        ring.add_peer("node-5");

        let new_mapping: Vec<String> = (0..total_keys)
            .map(|i| ring.get_primary_peer(&format!("key_{i}")).unwrap())
            .collect();

        let mut moved_keys = 0;
        for i in 0..total_keys {
            if initial_mapping[i] != new_mapping[i] {
                moved_keys += 1;
                assert_eq!(new_mapping[i], "node-5");
            }
        }

        let movement_pct = (moved_keys as f64 / total_keys as f64) * 100.0;
        assert!(
            movement_pct > 10.0 && movement_pct < 30.0,
            "Moved {moved_keys}/{total_keys} keys ({movement_pct:.1}%), expected ~20%"
        );
    }
}
