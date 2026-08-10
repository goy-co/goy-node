# Goy Node Deployment & Operations Guide

Comprehensive guide for deploying, configuring, operating, and troubleshooting Goy Node in production environments.

---

## 1. Installation Methods

### Method A: Automated Shell Installer (Linux & macOS)

```bash
curl -fsSL https://raw.githubusercontent.com/the-goy-company/goy-node/main/deploy/install.sh | bash
```

### Method B: Docker & Docker Compose

```bash
cd deploy
docker-compose up -d
```

### Method C: Systemd Service Unit (Linux)

Copy `deploy/goy-node.service` to `/etc/systemd/system/goy-node.service`:

```ini
[Unit]
Description=Goy Node — Nostr Relay Mesh Agent
After=network-online.target tailscaled.service

[Service]
Type=simple
User=goynode
ExecStart=/usr/local/bin/goy-node run
Restart=always
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl enable --now goy-node
```

---

## 2. Complete Environment Variables Reference

| Variable | Description | Default |
|---|---|---|
| `GOY_NODE_CONFIG` | Path to `config.toml` file | `~/.config/goy-node/config.toml` |
| `GOY_NODE_DATA_DIR` | Path to data directory | `~/.local/share/goy-node/` |
| `GOY_NODE_RELAY_URL` | WebSocket URL of local strfry relay | `ws://127.0.0.1:7777` |
| `GOY_NODE_MESH_LISTEN` | Bind address for mesh TCP listener | `0.0.0.0:8443` |
| `GOY_NODE_MESH_SEEDS` | Comma-separated seed URLs | `""` |
| `GOY_NODE_REPLICATION_FACTOR` | N-of-M replication factor | `3` |
| `GOY_NODE_VNODES_PER_PEER` | Virtual nodes per peer on hash ring | `150` |
| `GOY_NODE_METRICS_LISTEN` | HTTP observability server bind address | `127.0.0.1:9090` |
| `GOY_API_URL` | Goy Company API base URL | `https://api.goyco.xyz` |

---

## 3. Onboarding & Offboarding Step-by-Step

### Onboarding
To join the Goy Company VPN network and register your node:

```bash
goy-node onboard --auth-key gc_your_company_auth_key_here
```

Non-interactive automation mode (for CI/CD or automated scripts):
```bash
goy-node onboard --auth-key gc_your_company_auth_key_here --non-interactive --vpn-only
```

### Offboarding
To deregister your node and disconnect from the VPN:

```bash
goy-node offboard --force
```

---

## 4. Troubleshooting Common Issues

### Issue 1: `❌ Goy Node is not running on 127.0.0.1:9090`
- Check if `goy-node run` is actively running.
- Verify `metrics.listen` in `config.toml` is set to `"127.0.0.1:9090"`.

### Issue 2: `⚠️ Tailscale/Headscale CLI is not installed`
- Install Tailscale via `curl -fsSL https://tailscale.com/install.sh | sh` or your OS package manager (`apt`, `brew`).

### Issue 3: `🔌 Failed to connect to local relay at ws://127.0.0.1:7777`
- Ensure strfry or your local Nostr relay is running and listening on port `7777`.
