# Goy Node — Mesh Agent for Nostr Relays over Tailscale / Headscale VPN

[![CI Status](https://github.com/the-goy-company/goy-node/actions/workflows/ci.yml/badge.svg)](https://github.com/the-goy-company/goy-node/actions/workflows/ci.yml)
[![CodeQL](https://github.com/the-goy-company/goy-node/actions/workflows/codeql.yml/badge.svg)](https://github.com/the-goy-company/goy-node/actions/workflows/codeql.yml)
[![Release](https://img.shields.io/github/v/release/the-goy-company/goy-node?color=blue)](https://github.com/the-goy-company/goy-node/releases)
[![License](https://img.shields.io/badge/license-Proprietary-orange.svg)](LICENSE)

**Goy Node** is an enterprise-grade, zero-configuration mesh agent built in Rust that connects local Nostr relays (such as [strfry](https://github.com/hoytech/strfry)) into a peer-to-peer, encrypted, N-of-M replicated mesh over private VPN networks.

---

## ⚡ Quick Start (3 Steps)

```bash
# 1. Install Goy Node on Linux or macOS
curl -fsSL https://raw.githubusercontent.com/the-goy-company/goy-node/main/deploy/install.sh | bash

# 2. Onboard your node into the Goy VPN Network using your Auth Key
goy-node onboard --auth-key gc_your_company_auth_key_here

# 3. Start the Goy Node mesh agent
goy-node run
```

---

## 🧠 How It Works in 3 Paragraphs

1. **Local Relay Sync & Encrypted Peer Connections**:
   Goy Node runs alongside your local Nostr relay (strfry). It subscribes to new events posted to your local relay via WebSocket, deduplicates them using a lock-free `DashSet`, and securely forwards them across mutual TLS (mTLS 1.3 with Trust-on-First-Use) connections to other nodes in your mesh.

2. **Consistent Hashing & N-of-M Data Replication**:
   Data distribution relies on a virtual-node-based **Consistent Hash Ring** (`vnodes_per_peer: 150`). When an event is ingested, the ring deterministically selects $N$ physical peer nodes for active replication. When new peers join or leave, background tasks rebalance only $\sim 1/N$ of keys, preserving system bandwidth and overall node performance.

3. **Built-in VPN & Observability**:
   The integrated onboarding wizard connects your node to the Goy Company Tailscale/Headscale VPN, auto-detects your internal MagicDNS hostname and IP address, and configures the node automatically. A built-in HTTP server (`http://127.0.0.1:9090`) exposes real-time Prometheus metrics (`/metrics`), health status (`/health`), connected peers (`/peers`), and JSON node info (`/info`), easily managed via the `goy-node` CLI.

---

## 🏗️ Architecture Overview

```
+-------------------------------------------------------------------------+
|                              GOY NODE                                   |
|                                                                         |
|  +------------------+     +-------------------+     +----------------+  |
|  |  Local Relay WS  | <-> |   Mesh Agent      | <-> |  HTTP Server   |  |
|  |   (ws://7777)    |     | (Deduplication,   |     |  (127.0.0.1:   |  |
|  +------------------+     | Consistent Hash,  |     |     9090)      |  |
|                           | Rate Limiting)    |     +----------------+  |
|                           +---------+---------+                         |
|                                     |                                   |
|                                     v                                   |
|                           +-------------------+                         |
|                           |  mTLS 1.3 TOFU    |                         |
|                           | (0.0.0.0:8443)    |                         |
|                           +---------+---------+                         |
+-------------------------------------|-----------------------------------+
                                      | (Encrypted VPN Tunnel)
                                      v
                        +---------------------------+
                        |  PEER NODES IN MESH       |
                        +---------------------------+
```

---

## ✨ Features & Documentation Links

- **Consistent Hashing Data Replication**: Uniform event distribution with minimal key movement ([`src/consistent_hash.rs`](file:///Users/andrecabrita/Developer/goy-co/node/src/consistent_hash.rs)).
- **mTLS 1.3 with Trust-On-First-Use (TOFU)**: End-to-end encrypted peer traffic with pinned fingerprints ([`docs/mesh-protocol.md`](docs/mesh-protocol.md)).
- **Nostr Protocol NIP Support**: Full compliance with NIP-09 (Deletion), NIP-16/33 (Replaceable Events), NIP-40 (Expiration), and NIP-42.
- **Integrated Onboarding**: Plug-and-play setup via `goy-node onboard` ([`docs/deployment-guide.md`](docs/deployment-guide.md)).
- **Prometheus Observability & Admin CLI**: Built-in HTTP endpoints and interactive CLI commands (`status`, `peers`, `info`, `metrics`).
- **Architectural Decision Records**: Deep dives into design trade-offs ([`docs/adr/`](docs/adr/)).

---

## 📊 System Limits & Hardware Specifications

For detailed performance benchmarks, memory consumption profiles, and hardware recommendations, see **[`docs/limits.md`](docs/limits.md)**.

| Profile | Hardware | Throughput | Max Peers |
|---|---|---|---|
| **Minimum (Edge)** | 1 vCPU, 512 MB RAM | $\approx 500$ events/sec | 10 peers |
| **Recommended (Hub)** | 2–4 vCPU, 2–4 GB RAM | $\approx 5,000$ events/sec | 50 peers |

---

## 📜 License

Copyright © 2024–2026 The Goy Company. All rights reserved.  
Licensed under the Goy Source Available License. See [`LICENSE`](LICENSE) for details.
