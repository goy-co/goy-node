# Goy Mesh Protocol Specification (v0.1.0-alpha)

Specification of the peer-to-peer wire protocol, message frames, synchronization mechanisms, security layer, and rate-limiting rules used by Goy Node.

---

## 1. Security & Encryption Layer

- **Transport**: Encrypted mutual TLS 1.3 (mTLS) over TCP on port `8443`.
- **Identity & Certificates**: ECDSA P-256 self-signed node certificates generated automatically at first startup.
- **Trust Model**: Trust-On-First-Use (TOFU) with SHA-256 fingerprint pinning (`mesh.trusted_fingerprints`).
- **Network Boundaries**: Operating inside private Tailscale / Headscale WireGuard VPN tunnels.

---

## 2. Wire Protocol Message Frames

All peer-to-peer communication uses WebSocket text frames containing JSON array messages.

### A. Event Propagation (`EVENT`)
```json
["EVENT", {
  "id": "e000000000000000000000000000000000000000000000000000000000000001",
  "pubkey": "a1b2c3...",
  "created_at": 1770681600,
  "kind": 1,
  "tags": [],
  "content": "Hello Goy Mesh",
  "sig": "f8e7d6..."
}]
```

### B. Historical Sync Request (`REQ`)
```json
["REQ", "goy-backfill", { "since": 1770600000, "limit": 500 }]
```

### C. End of Stored Events (`EOSE`)
```json
["EOSE", "goy-backfill"]
```

### D. Optimistic Acknowledgement (`OK`)
```json
["OK", "e000000000000000000000000000000000000000000000000000000000000001", true, ""]
```

### E. Heartbeat & Notice (`NOTICE`)
```json
["NOTICE", "goy-heartbeat"]
```

---

## 3. Data Replication & Consistent Hashing

1. **Hash Ring Positioning**: Every physical peer is assigned `vnodes_per_peer: 150` virtual node positions on a 64-bit truncated SHA-256 hash ring.
2. **Replica Selection**: `get_responsible_peers(event_id, replication_factor)` traverses the ring clockwise from `hash(event_id)` to select $N$ distinct physical peers.
3. **Rebalancing**: On peer join or leave, background tasks rebalance $\sim 1/N$ of key responsibilities with zero interruption to ongoing streaming.

---

## 4. Rate Limiting Rules

- **Per-Peer Event Limit**: Default 50 events/second (token-bucket algorithm).
- **Per-Peer Bandwidth Limit**: Default 1 MB/second.
- **Max Message Size**: Default 512 KB per WebSocket frame.
