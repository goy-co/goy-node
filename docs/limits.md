# Goy Node — System Performance & Resource Limits

Technical specification of performance limits, memory consumption profiles, concurrent connection capacity, scale benchmarks, and hardware recommendations for operating Goy Node in production.

---

## 1. System Performance & Throughput Limits

| Parameter | Measured Limit | Recommended Operational Limit |
|---|---|---|
| **Raw Event Deduplication (single thread)** | $\approx 2,500,000$ ops/sec | 100,000 events/sec |
| **Consistent Hash Ring Lookups ($N=3$)** | $\approx 1,800,000$ lookups/sec | 50,000 lookups/sec |
| **Single-Node Event Throughput** | $\approx 15,000$ events/sec | 2,500 events/sec |
| **Max Concurrent Peer Connections** | 250 connections | 50 active peers per node |
| **Per-Peer Rate Limit Default** | 50 events/sec | 50–200 events/sec |
| **Per-Peer Bandwidth Default** | 1 MB/sec | 1 MB/sec |
| **Max Message Size Default** | 512 KB | 512 KB |

---

## 2. Memory Consumption Breakdown

The memory footprint of `goy-node` is dominated by the in-memory deduplication set (`DashSet<String>`) and active peer sessions.

| Component | 100,000 Events | 500,000 Events | 1,000,000 Events |
|---|---|---|---|
| **`DashSet<String>` (seen_ids)** | $\approx 8.5$ MB | $\approx 42.5$ MB | $\approx 85.0$ MB |
| **`ConsistentHashRing` (150 vnodes/peer, 50 peers)** | $\approx 0.6$ MB | $\approx 0.6$ MB | $\approx 0.6$ MB |
| **Per-Peer Connection Buffers (50 peers)** | $\approx 12.0$ MB | $\approx 12.0$ MB | $\approx 12.0$ MB |
| **Base Process Overhead** | $\approx 15.0$ MB | $\approx 15.0$ MB | $\approx 15.0$ MB |
| **Total Estimated RAM** | **$\approx 36.1$ MB** | **$\approx 70.1$ MB** | **$\approx 112.6$ MB** |

---

## 3. Storage & Persistence Performance

- **Atomic JSON Save/Load (`seen_ids.json`)**:
  - **100K IDs**: Save time $\approx 18$ ms, Load time $\approx 12$ ms (File size: $\approx 7.2$ MB).
  - **500K IDs**: Save time $\approx 95$ ms, Load time $\approx 68$ ms (File size: $\approx 36.0$ MB).
  - **1M IDs**: Save time $\approx 210$ ms, Load time $\approx 145$ ms (File size: $\approx 72.0$ MB).
- **Atomic File Writes**: `seen_ids.json.tmp` is flushed and renamed atomically using `std::fs::rename`, preventing corruption during abrupt power loss or SIGKILL.

---

## 4. Hardware Recommendations

### Minimum Hardware Specification (Small Node / Edge)
- **CPU**: 1 vCPU (x86_64 or ARM64 / Apple Silicon).
- **RAM**: 512 MB.
- **Disk**: 50 GB free space on `data_dir` filesystem (hardcoded minimum).
- **Network**: 10 Mbps VPN connection.
- **Target Workload**: Up to 10 peers, $\approx 500$ events/sec.

### Recommended Hardware Specification (Production Mesh Hub)
- **CPU**: 2–4 vCPU.
- **RAM**: 2 GB – 4 GB.
- **Disk**: 100 GB+ NVMe SSD.
- **Network**: 100 Mbps+ WireGuard / Headscale VPN.
- **Target Workload**: 50+ peers, $\approx 5,000$ events/sec.

---

## 5. Requisitos de Storage

O Goy Node implementa um contrato social obrigatório de armazenamento para garantir a redundância e persistência de dados em toda a rede mesh.

### Espaço Reservado e Mínimo Obrigatório

- **Mínimo Obrigatório (`MIN_RESERVED_GB`)**: **50 GB** (hardcoded no código-fonte, não configurável).
- **Contribuição Voluntária Extra**: Configurável via `storage.extra_contribution_gb` em `config.toml` ou variável de ambiente `GOY_NODE_EXTRA_STORAGE_GB`.
- **Fórmula de Espaço Reservado**:
  $$\text{Total Reservado (GB)} = \text{MIN\_RESERVED\_GB (50)} + \text{extra\_contribution\_gb}$$

### Tabela de Recomendações por Perfil de Operador

| Perfil de Operador | Mínimo Obrigatório | Contribuição Extra | Total Reservado |
|---|---|---|---|
| **Voluntário Individual** | 50 GB | 0 – 50 GB | 50 – 100 GB |
| **Organização Parceira** | 50 GB | 100 – 200 GB | 150 – 250 GB |
| **Seed / Core Infrastructure** | 50 GB | 450+ GB | 500+ GB |

### Comportamento Runtime perante Armazenamento Insuficiente

1. **Verificação no arranque (`cmd_run`)**:
   - Se o espaço disponível no filesystem do `data_dir` for inferior a 50 GB, o nó **recusa iniciar** com registo de erro limpo e **exit code 3** (`EXIT_STORAGE_ERROR`).
2. **Verificação no onboarding (`goy-node onboard`)**:
   - Aborta imediatamente a tentativa de registo e configuração de VPN se o espaço for < 50 GB, retornando **exit code 5** (`EXIT_ONBOARD_STORAGE_ERROR`).
3. **Monitorização de Saúde (`GET /health`)**:
   - Se o espaço disponível descer abaixo do limiar crítico de 10% do mínimo de 50 GB (5 GB = `5_368_709_120` bytes), o endpoint `/health` responde com HTTP 503 e `"status":"degraded"`.

### Métricas Prometheus de Storage

| Métrica | Tipo | Descrição |
|---|---|---|
| `goy_storage_reserved_bytes` | Gauge | Espaço total reservado para a mesh em bytes (mínimo 50 GB + extra). |
| `goy_storage_available_bytes` | Gauge | Espaço livre atualmente disponível no filesystem do `data_dir`. |
| `goy_storage_used_bytes` | Gauge | Espaço efetivamente ocupado pelo Goy Node no `data_dir`. |

*Nota: Mecanismos avançados de evicção de dados e re-balanceamento dinâmico baseado em capacidade de storage estão planeados para a release v0.2.0.*

---

## 6. Known Operational Trade-offs

1. **JSON State Serialization vs Binary Storage**:
   - `seen_ids` is stored as JSON array. For sets larger than 1,000,000 IDs ($>70$ MB), disk serialization pause takes $\approx 200$ ms. Future major release may introduce a binary columnar format if `seen_ids` exceeds 5 million entries.
2. **Rebalancing Overhead**:
   - When a 6th peer joins a 5-node cluster, background rebalancing moves $\approx 16.6\%$ ($\approx 1/N$) of event keys to the new peer. Bandwidth rate limiting applies during transfer to ensure zero impact on real-time message routing.
