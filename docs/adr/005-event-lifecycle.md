# ADR 005: Strict Event Processing Order & NIP Lifecycle Compliance

* **Status**: Accepted
* **Date**: 2026-08-10
* **Author**: The Goy Company

## Context
Nostr events include special semantics such as deletion (NIP-09), expiration timestamps (NIP-40), and replaceable events (NIP-16/33). Processing events out of order can lead to deleted or expired events being improperly accepted and propagated.

## Decision
We decided to enforce a **strict 7-stage processing pipeline** for every event ingested by Goy Node:
1. Message size & rate limit check
2. Global deduplication check (`seen_ids`)
3. NIP-09 deletion evaluation (`deleted_ids`)
4. NIP-40 expiration evaluation (`expiring_events`)
5. NIP-16/33 replacement timestamp check (`latest_replaceable`)
6. Local relay publishing
7. Consistent Hash Ring replica forwarding

## Consequences
### Positive
- Strict NIP compliance: deleted or expired events are dropped before reaching local relays or peer replication.
- Deterministic behavior across all mesh nodes.

### Negative
- Minor validation overhead per event ($\ll 10\,\mu\text{s}$).
