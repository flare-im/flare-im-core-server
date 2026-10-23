# Flare IM Core - Makefile Helper

CARGO ?= cargo

# 自动检测 protoc 路径
PROTOC ?= $(shell which protoc || echo "/opt/homebrew/bin/protoc" || echo "/usr/local/bin/protoc" || echo "")

.PHONY: help build fmt lint check check-dashboard-metrics check-dockerfile-build-args test clean start-core start-social stop

help:
	@echo "Flare IM Core Make targets"
	@echo "  help            Show this help"
	@echo "  build           cargo build --all"
	@echo "  fmt             cargo fmt --all"
	@echo "  lint            cargo clippy --all-targets --all-features"
	@echo "  check           Dashboard/alert metric gate, then cargo check"
	@echo "  check-dashboard-metrics  Grafana panels and alert rules only use registered metrics"
	@echo "  test            cargo test"
	@echo "  clean           cargo clean"
	@echo "  start-core      Start full stack, single gateway, no business hooks"
	@echo "  start-core-fast Start core stack without cargo build (FLARE_SKIP_BUILD=1)"
	@echo "  start-social    Start full stack, single gateway, flare-social hook"
	@echo "  stop            Stop all services"
	@echo "  run-<service>   Start service (see list below)"
	@echo ""
	@echo "Optional: make start-core ARGS=\"trace\"  (or multi / multi trace)"
	@echo "Environment variables:"
	@echo "  PROTOC          Path to protoc binary (auto-detected: $(PROTOC))"
	@echo "  FLARE_SKIP_BUILD=1  Skip cargo build when binaries already exist"
	@echo "  CARGO_BUILD_JOBS=N  Limit parallel rustc jobs (helps avoid OOM on start)"

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

check: check-dashboard-metrics check-dockerfile-build-args
	$(CARGO) check

# 面板与告警只能引用源码里注册过的指标与标签（依赖同级仓库 flare-server-core、flare-core）。
check-dashboard-metrics:
	bash scripts/check_dashboard_metrics.sh
	PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_check_dashboard_metrics.py'

# 文档教人传的 --build-arg 必须在 Dockerfile 里声明；扫描整个多仓工作区，冻结仓除外。
check-dockerfile-build-args:
	bash scripts/check_dockerfile_build_args.sh
	PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_check_dockerfile_build_args.py'

test: test-opensource-runner
	$(CARGO) test --all

.PHONY: test-opensource-runner
test-opensource-runner:
	PYTHONDONTWRITEBYTECODE=1 python3 -m unittest scripts/test_smoke_opensource_container.py scripts/lib/test_timeout_process.py

# The development launcher gives each key only to the processes release/supervise-core.toml does.
.PHONY: test-launcher-keys
test-launcher-keys:
	PYTHONDONTWRITEBYTECODE=1 python3 -m unittest scripts/test_start_server_keys.py

.PHONY: db-migrate check-schema dev-reset test-migrations
.PHONY: test-credential-projection
test-credential-projection:
	PYTHONDONTWRITEBYTECODE=1 python3 scripts/test_credential_projection.py

db-migrate:
	bash scripts/migrate_db.sh

check-schema:
	bash scripts/check_schema_parity.sh

dev-reset:
	bash scripts/dev_reset_db.sh

test-migrations:
	PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tools/flare-migrate -p 'test_unit.py' -v

clean:
	$(CARGO) clean

# Full stack launch (Hook profile) --------------------------------------------

SCRIPTS := ./scripts
ARGS ?=

.PHONY: start-core start-core-fast start-social stop

start-core:
	bash $(SCRIPTS)/start_server_core.sh single $(ARGS)

start-core-fast:
	FLARE_SKIP_BUILD=1 bash $(SCRIPTS)/start_server_core.sh single $(ARGS)

start-social:
	bash $(SCRIPTS)/start_server_social.sh single $(ARGS)

stop:
	$(SCRIPTS)/stop_server.sh

# Service launch helpers ----------------------------------------------------

.PHONY: run-access-gateway run-api-gateway run-signaling-online run-signaling-route \
	run-push-server run-push-worker \
	run-message-orchestrator run-storage-writer run-storage-reader \
	run-media run-session

run-access-gateway:
	$(CARGO) run -p flare-signaling-gateway --bin flare-signaling-gateway

run-api-gateway:
	$(CARGO) run -p flare-api-gateway --bin flare-api-gateway

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

run-flare-capability:
	$(CARGO) run -p flare-capability --bin flare-capability

run-session:
	$(CARGO) run -p flare-session --bin flare-session
