# ADR 004: Trust-on-First-Use (TOFU) mTLS vs Centralized Certificate Authority

* **Status**: Accepted
* **Date**: 2026-08-10
* **Author**: The Goy Company

## Context
Securing peer-to-peer WebSocket connections between Goy Nodes requires mutual authentication and TLS 1.3 encryption. Managing a centralized Certificate Authority (CA) introduces operational dependencies and single points of failure.

## Decision
We decided to implement **Trust-On-First-Use (TOFU) with self-signed ECDSA P-256 node certificates** and SHA-256 fingerprint pinning (`mesh.trusted_fingerprints`).

## Consequences
### Positive
- Zero dependency on external CA infrastructure or PKI certificate renewal services.
- Instant node startup with local certificate generation.
- Explicit fingerprint pinning allows pre-approved trust overrides.

### Negative
- Initial connection requires trust-on-first-use unless fingerprints are pre-pinned in configuration.
