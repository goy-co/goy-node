# ── Goy Node Makefile ───────────────────────────────────────────────
.PHONY: all build test check clean docker-build docker-run install uninstall help

IMAGE_NAME ?= goy-node
IMAGE_TAG ?= latest

all: build

build:
	@echo "🟢 Building goy-node binary (release mode)..."
	cargo build --release

test:
	@echo "🧪 Running unit and integration tests..."
	cargo test

check:
	@echo "🔍 Running cargo check..."
	cargo check

clean:
	@echo "🧹 Cleaning build artifacts..."
	cargo clean

docker-build:
	@echo "🐳 Building Docker image $(IMAGE_NAME):$(IMAGE_TAG)..."
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) -f deploy/Dockerfile .

docker-run:
	@echo "🚀 Running Docker container $(IMAGE_NAME):$(IMAGE_TAG)..."
	docker run --rm -it \
		-p 7777:7777 -p 8443:8443 -p 9090:9090 \
		--name goy-node-local \
		$(IMAGE_NAME):$(IMAGE_TAG)

install:
	@echo "📦 Installing goy-node on system..."
	sudo ./deploy/install.sh --install

uninstall:
	@echo "🗑️ Uninstalling goy-node from system..."
	sudo ./deploy/install.sh --uninstall

help:
	@echo "Goy Node Build & Management Commands:"
	@echo "  make build         Compiles the release binary (cargo build --release)"
	@echo "  make test          Runs cargo test"
	@echo "  make check         Runs cargo check"
	@echo "  make docker-build  Builds multi-stage Docker image"
	@echo "  make docker-run    Runs local Docker container with mapped ports"
	@echo "  make install       Installs binary, systemd service, and default config"
	@echo "  make uninstall     Stops and removes installed systemd service and binary"
	@echo "  make clean         Cleans cargo build directory"
