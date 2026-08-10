# ADR 001: Goy Node Architecture Design

* **Status**: Accepted
* **Date**: 2026-08-10
* **Author**: The Goy Company

## Context
Nostr relay infrastructure relies on individual relays operating in isolation. Scaling Nostr relays across private company networks requires automatic peer-to-peer data synchronization, high availability, and deduplication without introducing a single point of failure or complex database clusters.

## Decision
We decided to build **Goy Node** as a decoupled async Rust agent (`tokio`) that runs alongside local Nostr relays (such as `strfry`). Goy Node connects to the local relay via WebSocket (`ws://127.0.0.1:7777`) and manages peer-to-peer mesh synchronization, deduplication (`DashSet`), rate-limiting, and mTLS security independently of the underlying relay engine.

## Consequences
### Positive
- Decoupled from relay internal database engine; works with strfry or any Nostr NIP-01 compliant relay.
- High performance powered by Rust async runtime (`tokio`, `dashmap`, `rustls`).
- Zero single point of failure.

### Negative
- Requires running a separate lightweight sidecar process alongside strfry.
