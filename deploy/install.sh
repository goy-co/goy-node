#!/usr/bin/env bash
# ── Goy Node Installer & Service Manager ─────────────────────────────
# Copyright © 2024-2026 The Goy Company. All rights reserved.

set -euo pipefail

# Configuração de caminhos padrão
INSTALL_BIN_DIR="/usr/local/bin"
CONFIG_DIR="/etc/goy-node"
DATA_DIR="/var/lib/goy-node"
SERVICE_NAME="goy-node"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
DEFAULT_ENV_FILE="/etc/default/goy-node"
SYSTEM_USER="goy-node"
SYSTEM_GROUP="goy-node"

# Funções de logging
info() { echo -e "\031[34m[INFO]\033[0m $*"; }
success() { echo -e "\033[32m[OK]\033[0m $*"; }
warn() { echo -e "\033[33m[WARN]\033[0m $*"; }
error() { echo -e "\033[31m[ERROR]\033[0m $*" >&2; exit 1; }

usage() {
    cat <<EOF
Uso: $0 [opções]

Opções:
  --install     Instala o binário, configura diretórios e ativa o serviço systemd (padrão)
  --uninstall   Para o serviço, desativa e remove os ficheiros instalados
  --build       Força a recompilação local com cargo em release antes de instalar
  --help        Exibe esta mensagem de ajuda

EOF
    exit 0
}

MODE="install"
FORCE_BUILD=false

for arg in "$@"; do
    case "$arg" in
        --uninstall) MODE="uninstall" ;;
        --build) FORCE_BUILD=true ;;
        --help|-h) usage ;;
        *) warn "Opção desconhecida: $arg" ;;
    esac
done

check_root() {
    if [ "$(id -u)" -ne 0 ]; then
        error "Este script requer privilégios de root (sudo $0)."
    fi
}

do_uninstall() {
    check_root
    info "A desinstalar o Goy Node..."

    if command -v systemctl >/dev/null 2>&1 && [ -f "$SERVICE_FILE" ]; then
        info "A parar e desativar serviço systemd..."
        systemctl stop "$SERVICE_NAME" || true
        systemctl disable "$SERVICE_NAME" || true
        rm -f "$SERVICE_FILE"
        systemctl daemon-reload
        success "Serviço systemd removido."
    fi

    if [ -f "$INSTALL_BIN_DIR/goy-node" ]; then
        rm -f "$INSTALL_BIN_DIR/goy-node"
        success "Binário $INSTALL_BIN_DIR/goy-node removido."
    fi

    if [ -f "$DEFAULT_ENV_FILE" ]; then
        rm -f "$DEFAULT_ENV_FILE"
    fi

    info "Ficheiros de configuração ($CONFIG_DIR) e dados ($DATA_DIR) foram mantidos por segurança."
    info "Para remover completamente os dados, execute:"
    info "  sudo rm -rf $CONFIG_DIR $DATA_DIR"
    success "Desinstalação concluída com sucesso."
    exit 0
}

do_install() {
    check_root
    info "🟢 A iniciar a instalação do Goy Node..."

    # 1. Verificar/Compilar Binário
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

    BINARY_PATH="$PROJECT_ROOT/target/release/goy-node"

    if [ "$FORCE_BUILD" = true ] || [ ! -f "$BINARY_PATH" ]; then
        info "A compilar o binário em modo release (cargo build --release)..."
        if command -v cargo >/dev/null 2>&1; then
            (cd "$PROJECT_ROOT" && cargo build --release)
        else
            error "Cargo não está instalado. Por favor instale Rust/Cargo ou forneça o binário compilado em target/release/goy-node."
        fi
    fi

    if [ ! -f "$BINARY_PATH" ]; then
        error "Binário não encontrado em $BINARY_PATH"
    fi

    # 2. Copiar Binário
    info "A instalar binário em $INSTALL_BIN_DIR/goy-node..."
    mkdir -p "$INSTALL_BIN_DIR"
    cp "$BINARY_PATH" "$INSTALL_BIN_DIR/goy-node"
    chmod 755 "$INSTALL_BIN_DIR/goy-node"
    success "Binário instalado em $INSTALL_BIN_DIR/goy-node."

    # 3. Criar Utilizador e Grupo de Sistema
    if ! getent group "$SYSTEM_GROUP" >/dev/null 2>&1; then
        info "A criar grupo de sistema $SYSTEM_GROUP..."
        groupadd --system "$SYSTEM_GROUP"
    fi

    if ! id -u "$SYSTEM_USER" >/dev/null 2>&1; then
        info "A criar utilizador de sistema $SYSTEM_USER..."
        useradd --system --gid "$SYSTEM_GROUP" --no-create-home --shell /bin/false "$SYSTEM_USER"
    fi

    # 4. Criar Diretórios e Permissões
    info "A configurar diretórios de configuração e dados..."
    mkdir -p "$CONFIG_DIR" "$DATA_DIR"
    chown -R "$SYSTEM_USER:$SYSTEM_GROUP" "$CONFIG_DIR" "$DATA_DIR"
    chmod 755 "$CONFIG_DIR"
    chmod 700 "$DATA_DIR"

    # 5. Criar config.toml padrão se não existir
    if [ ! -f "$CONFIG_DIR/config.toml" ]; then
        info "A gerar ficheiro de configuração inicial em $CONFIG_DIR/config.toml..."
        cat <<EOF > "$CONFIG_DIR/config.toml"
# Configuração Goy Node — Gerado Automaticamente

[relay]
url = "ws://127.0.0.1:7777"
reconnect_interval_secs = 5

[mesh]
listen = "0.0.0.0:8443"
replication_factor = 3
tls_enabled = true
max_events_per_second_per_peer = 50
max_bytes_per_second_per_peer = 1048576
max_message_size = 524288

[metrics]
listen = "127.0.0.1:9090"
EOF
        chown "$SYSTEM_USER:$SYSTEM_GROUP" "$CONFIG_DIR/config.toml"
        chmod 644 "$CONFIG_DIR/config.toml"
        success "Configuração inicial criada."
    fi

    # 6. Instalar Serviço Systemd (se systemd estiver disponível)
    if command -v systemctl >/dev/null 2>&1; then
        info "A instalar o serviço Systemd ($SERVICE_FILE)..."
        cp "$PROJECT_ROOT/deploy/goy-node.service" "$SERVICE_FILE"
        chmod 644 "$SERVICE_FILE"

        if [ ! -f "$DEFAULT_ENV_FILE" ]; then
            touch "$DEFAULT_ENV_FILE"
            chmod 644 "$DEFAULT_ENV_FILE"
        fi

        systemctl daemon-reload
        systemctl enable "$SERVICE_NAME"
        systemctl restart "$SERVICE_NAME"
        success "Serviço systemd $SERVICE_NAME ativado e iniciado."
    else
        warn "Systemctl não encontrado. O serviço não foi registado no systemd."
    fi

    # 7. Instruções Finais
    echo ""
    echo "=========================================================================="
    echo "🎉 Instalação do Goy Node concluída com sucesso!"
    echo "=========================================================================="
    echo "  • Verificar estado do serviço : systemctl status goy-node"
    echo "  • Verificar logs em tempo real: journalctl -u goy-node -f"
    echo "  • Executar Admin CLI         : goy-node status"
    echo "  • Listar peers conectados    : goy-node peers"
    echo "  • Ficheiro de configuração   : /etc/goy-node/config.toml"
    echo "  • Diretório de dados         : /var/lib/goy-node"
    echo "=========================================================================="
}

if [ "$MODE" = "uninstall" ]; then
    do_uninstall
else
    do_install
fi
