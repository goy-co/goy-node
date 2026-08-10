# ADR 003: N-of-M Replication via Consistent Hashing vs Sharding / Erasure Coding

* **Status**: Accepted
* **Date**: 2026-08-10
* **Author**: The Goy Company

## Context
Replicating Nostr events across a dynamic peer mesh requires balancing data durability, low retrieval latency, and minimal network overhead when nodes join or leave the cluster.

## Decision
We decided to adopt **N-of-M active replication with a Virtual-Node-based Consistent Hash Ring** (`vnodes_per_peer: 150`). When an event is ingested, SHA-256 ring positioning selects $N$ distinct physical replica peers.

## Consequences
### Positive
- Minimizes key movement during peer membership changes to $\sim 1/N$ of total keys.
- Low-latency retrieval: full copies exist on $N$ nodes without needing reconstruction chunks (unlike erasure coding).
- Simple deterministic selection with $O(\log N)$ lookup performance (`BTreeMap<u64, String>`).

### Negative
- Higher storage footprint per node compared to erasure coding ($N\times$ full storage vs fractional coding).
