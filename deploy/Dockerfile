# ── Stage 1: cargo-chef planner ─────────────────────────────────────
FROM rust:1.97.1-slim-trixie AS chef
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && cargo install cargo-chef --locked
WORKDIR /usr/src/goy-node

# ── Stage 2: Dependency plan ─────────────────────────────────────────
FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches
RUN cargo chef prepare --recipe-path recipe.json

# ── Stage 3: Build (deps cached, source compiled separately) ─────────
FROM chef AS builder
COPY --from=planner /usr/src/goy-node/recipe.json recipe.json
# Esta camada é cacheada enquanto as dependências não mudarem (~95% dos builds)
RUN cargo chef cook --release --recipe-path recipe.json

# Copiar código fonte e compilar apenas o binário final
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches
RUN cargo build --release --bin goy-node

# ── Stage 4: Runtime ─────────────────────────────────────────────────
FROM debian:trixie-slim AS runtime

# Metadata OCI Labels
LABEL org.opencontainers.image.title="goy-node" \
      org.opencontainers.image.description="Mesh agent for Goy Node — automatic Nostr relay synchronization over VPN" \
      org.opencontainers.image.version="0.1.1-alpha" \
      org.opencontainers.image.vendor="The Goy Company" \
      org.opencontainers.image.licenses="Proprietary"

# Instalar pacotes de runtime essenciais
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Criar utilizador e grupo não-root para execução segura
RUN groupadd --gid 10001 goynode \
    && useradd --uid 10001 --gid 10001 --create-home --shell /bin/false goynode

# Criar diretórios para configuração e dados
RUN mkdir -p /etc/goy-node /var/lib/goy-node \
    && chown -R goynode:goynode /etc/goy-node /var/lib/goy-node

# Copiar o binário compilado
COPY --from=builder /usr/src/goy-node/target/release/goy-node /usr/local/bin/goy-node
RUN chmod +x /usr/local/bin/goy-node

# Alternar para o utilizador não-root
USER goynode:goynode
WORKDIR /var/lib/goy-node

# Expor portas:
# - 7777: Local Nostr relay WebSocket
# - 8443: Mesh P2P TLS listener
# - 9090: Prometheus Metrics & Health HTTP server
EXPOSE 7777 8443 9090

# Volumes para persistência de dados e configuração
VOLUME ["/etc/goy-node", "/var/lib/goy-node"]

# Configurar variáveis de ambiente padrão
ENV GOY_NODE_CONFIG=/etc/goy-node/config.toml \
    GOY_NODE_DATA_DIR=/var/lib/goy-node

ENTRYPOINT ["/usr/local/bin/goy-node"]
CMD ["run"]
