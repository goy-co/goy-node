# ADR 002: Integrated Tailscale/Headscale VPN vs Provider-Agnostic Networking

* **Status**: Accepted
* **Date**: 2026-08-10
* **Author**: The Goy Company

## Context
Deploying mesh networks across public internet hosts introduces firewall traversal complexities, NAT punching issues, and exposure to public attacks.

## Decision
We decided to integrate **Tailscale / Headscale WireGuard VPN** natively into the Goy Node onboarding workflow (`goy-node onboard`). Goy Node uses Tailscale CLI automation (`tailscale up --authkey=...`) to establish encrypted overlay network tunnels, auto-detecting internal VPN IPs and MagicDNS hostnames.

## Consequences
### Positive
- Zero manual router port forwarding or NAT traversal required.
- Network level security: all peer-to-peer traffic is contained inside the private WireGuard overlay network.
- MagicDNS provides human-readable hostnames for node discovery.

### Negative
- Requires Tailscale / Headscale CLI installed on the host system.
