# Flare IM Core - Makefile Helper

CARGO ?= cargo

# 自动检测 protoc 路径
PROTOC ?= $(shell which protoc || echo "/opt/homebrew/bin/protoc" || echo "/usr/local/bin/protoc" || echo "")

.PHONY: help build fmt lint check test clean

help:
	@echo "Flare IM Core Make targets"
	@echo "  help            Show this help"
	@echo "  build           cargo build --all"
	@echo "  fmt             cargo fmt --all"
	@echo "  lint            cargo clippy --all-targets --all-features"
	@echo "  check           cargo check"
	@echo "  test            cargo test"
	@echo "  clean           cargo clean"
	@echo "  run-<service>   Start service (see list below)"
	@echo ""
	@echo "Environment variables:"
	@echo "  PROTOC          Path to protoc binary (auto-detected: $(PROTOC))"

build:
	@if [ -z "$(PROTOC)" ] || [ ! -f "$(PROTOC)" ]; then \
		echo "❌ Error: protoc not found. Please install protobuf:"; \
		echo "   macOS: brew install protobuf"; \
		echo "   Or set PROTOC environment variable to protoc path"; \
		exit 1; \
	fi
	@echo "📦 Using protoc: $(PROTOC)"
	PROTOC=$(PROTOC) $(CARGO) build --all

fmt:
	$(CARGO) fmt --all

lint:
	$(CARGO) clippy --all-targets --all-features -- -D warnings

check:
	$(CARGO) check

test:
	$(CARGO) test --all

clean:
	$(CARGO) clean

# Service launch helpers ----------------------------------------------------

.PHONY: run-access-gateway run-core-gateway run-signaling-online run-signaling-route \
	run-push-server run-push-worker \
	run-message-orchestrator run-storage-writer run-storage-reader \
	run-media run-session

run-access-gateway:
	$(CARGO) run -p flare-signaling-gateway --bin flare-signaling-gateway

run-core-gateway:
	$(CARGO) run -p flare-core-gateway --bin flare-core-gateway

run-signaling-online:
	$(CARGO) run -p flare-signaling-online --bin flare-signaling-online

run-signaling-route:
	$(CARGO) run -p flare-signaling-route --bin flare-signaling-route

run-push-server:
	$(CARGO) run -p flare-push-server --bin flare-push-server

run-push-worker:
	$(CARGO) run -p flare-push-worker --bin flare-push-worker

run-message-orchestrator:
	$(CARGO) run -p flare-orchestrator --bin flare-orchestrator

run-storage-writer:
	$(CARGO) run -p flare-storage-writer --bin flare-storage-writer

run-storage-reader:
	$(CARGO) run -p flare-storage-reader --bin flare-storage-reader

run-media:
	$(CARGO) run -p flare-media --bin flare-media

run-hook-engine:
	$(CARGO) run -p flare-hook-engine --bin flare-hook-engine

run-session:
	$(CARGO) run -p flare-session --bin flare-session
