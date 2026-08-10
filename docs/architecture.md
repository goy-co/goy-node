# Goy Node Architecture Specification

Detailed specification of the internal system architecture, data processing pipeline, module boundaries, and design trade-offs of Goy Node.

---

## 1. System Architecture Overview

```
                      +-----------------------------+
                      |    Goy Company Platform     |
                      |   (https://api.goyco.xyz)   |
                      +--------------+--------------+
                                     | Onboarding API
                                     v
+-------------------------------------------------------------------------------+
| GOY NODE                                                                      |
|                                                                               |
|  +---------------------+      +---------------------+     +----------------+  |
|  | Local Relay (strfry)| <--> |     Mesh Agent      | --> |  HTTP Server   |  |
|  |    (ws://7777)      |      |   (tokio runtime)   |     | (127.0.0.1:9090|  |
|  +---------------------+      +----------+----------+     +----------------+  |
|                                          |                                    |
|         +--------------------------------+--------------------------------+   |
|         |                                |                                |   |
|         v                                v                                v   |
|  +--------------+               +------------------+             +---------+  |
|  |  DashSet     |               | Consistent Hash  |             | Rate    |  |
|  | (seen_ids)   |               | Ring (150 vnodes)|             | Limiter |  |
|  +--------------+               +------------------+             +---------+  |
|                                          |                                    |
|                                          v                                    |
|                                 +------------------+                          |
|                                 |   mTLS 1.3 TOFU   |                          |
|                                 | (0.0.0.0:8443)   |                          |
|                                 +--------+---------+                          |
+------------------------------------------|------------------------------------+
                                           | Encrypted Tunnel
                                           v
                             +---------------------------+
                             |    Peers in Mesh Network  |
                             +---------------------------+
```

---

## 2. Event Processing Pipeline

When a Nostr event arrives from either the local relay or a remote peer, it traverses a strict multi-stage processing pipeline:

```
[ Incoming Event Raw JSON ]
           │
           ▼
Stage 1: Size & Rate Limit Verification (max_message_size & token bucket rate limiter)
           │
           ▼
Stage 2: Global Deduplication (Check & Insert in lock-free DashSet)
           │
           ▼
Stage 3: NIP-09 Deletion Verification (Check if event_id is marked deleted)
           │
           ▼
Stage 4: NIP-40 Expiration Cleanup (Check expiration tag vs current Unix timestamp)
           │
           ▼
Stage 5: NIP-16/33 Replacement Evaluation (Check replaceable kind timestamps)
           │
           ▼
Stage 6: Local Relay Publishing (Forward to strfry local WebSocket)
           │
           ▼
Stage 7: Consistent Hash Replication (Select N distinct peers on ring & forward)
```

---

## 3. Module Boundaries & Responsibilities

1. **`src/mesh.rs`**: Core async event loop orchestrating WebSocket streams, backfill queues, peer sessions, and state management.
2. **`src/consistent_hash.rs`**: Virtual-node hash ring (`BTreeMap<u64, String>`) for deterministic $O(\log N)$ replica lookup.
3. **`src/tls.rs`**: Self-signed ECDSA P-256 certificate generation, mTLS 1.3 server/client rustls config, and TOFU fingerprint persistence.
4. **`src/rate_limiter.rs`**: Per-peer token-bucket rate limiter enforcing event rate and bandwidth caps.
5. **`src/onboard.rs` & `src/goy_api.rs`**: Goy Company API client and Tailscale/Headscale VPN CLI integration.
6. **`src/http.rs` & `src/metrics.rs`**: Observability HTTP server exposing Prometheus metrics (`/metrics`), health status (`/health`), connected peers (`/peers`), and JSON node metadata (`/info`).
7. **`src/cli.rs`**: Admin CLI subcommand handler (`run`, `status`, `peers`, `info`, `metrics`, `onboard`, `offboard`).
