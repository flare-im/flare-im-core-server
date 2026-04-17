# Flare Hook Engine Makefile

.PHONY: help test test-integration test-hook-service test-unit build run clean check lint fmt

# 默认目标
.DEFAULT_GOAL := help

# Hook服务端口
HOOK_SERVICE_PORT ?= 50110

help: ## 显示帮助信息
	@echo "Flare Hook Engine 可用命令："
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

build: ## 编译项目
	@echo "构建 Hook Engine..."
	cargo build --release

run: ## 运行 Hook Engine 服务
	@echo "启动 Hook Engine 服务 (端口: ${HOOK_SERVICE_PORT})..."
	HOOK_SERVICE_PORT=${HOOK_SERVICE_PORT} cargo run --bin flare-capability

test: ## 运行所有测试
	@echo "运行所有测试..."
	cargo test -p flare-capability --all

test-unit: ## 运行单元测试
	@echo "运行单元测试..."
	cargo test -p flare-capability --lib

test-integration: ## 运行 HookExtension 集成测试
	@echo "运行 HookExtension 集成测试..."
	cargo test --test integration_test -p flare-capability -- --nocapture

test-hook-service: ## 运行 HookService 集成测试
	@echo "运行 HookService 集成测试..."
	cargo test --test hook_service_test -p flare-capability -- --nocapture

test-all: test-unit test-integration test-hook-service ## 运行所有测试（单元+集成）

check: ## 检查代码（不编译）
	@echo "检查代码..."
	cargo check -p flare-capability --all-targets

lint: ## 运行 clippy 检查
	@echo "运行 clippy 检查..."
	cargo clippy -p flare-capability --all-targets -- -D warnings

fmt: ## 格式化代码
	@echo "格式化代码..."
	cargo fmt -p flare-capability

fmt-check: ## 检查代码格式
	@echo "检查代码格式..."
	cargo fmt -p flare-capability -- --check

clean: ## 清理构建产物
	@echo "清理构建产物..."
	cargo clean -p flare-capability

# 运行带服务的测试（需要先启动服务）
test-with-server: ## 启动服务并运行测试（后台运行服务）
	@echo "启动 Hook Engine 服务（后台）..."
	@HOOK_SERVICE_PORT=${HOOK_SERVICE_PORT} cargo run --bin flare-capability &
	@SERVER_PID=$$!; \
	sleep 5; \
	echo "等待服务启动..."; \
	echo "运行测试..."; \
	cargo test -p flare-capability --test integration_test --test hook_service_test || true; \
	echo "停止服务..."; \
	kill $$SERVER_PID 2>/dev/null || true

# 开发环境测试（快速测试）
test-dev: ## 运行快速开发测试
	@echo "运行开发测试..."
	cargo test -p flare-capability --lib -- --nocapture

# 显示测试覆盖率（需要 cargo-tarpaulin）
coverage: ## 生成测试覆盖率报告（需要 cargo install cargo-tarpaulin）
	@echo "生成测试覆盖率报告..."
	cargo tarpaulin -p flare-capability --out Html --output-dir coverage
	@echo "覆盖率报告已生成：coverage/tarpaulin-report.html"

