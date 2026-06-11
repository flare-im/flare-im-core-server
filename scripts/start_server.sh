#!/bin/bash
# 启动 Flare IM Core 所有服务模块
#
# 使用方法:
#   ./scripts/start_server.sh [single|multi] [trace|debug]
#   - single: 启动单网关模式（默认，仅启动一个 access-gateway 实例）
#   - multi:  启动多网关模式（启动多个 access-gateway 实例）
#   - trace|debug: 第二参数，全量跟踪（RUST_LOG=trace；debug 为历史别名，仅排障）
#   - 默认: 未设置环境变量 RUST_LOG 时使用「业务 debug + 第三方降噪」，避免 logs/ 暴涨
#
# Hook 配置档（推荐专用脚本）：
#   ./scripts/start_server_core.sh    — config/hooks.core.toml，不注册业务 Hook
#   ./scripts/start_server_social.sh  — config/hooks.social.toml，需 flare-social-hook
#   FLARE_HOOKS_PROFILE=core|social ./scripts/start_server.sh ...

set -e

# 降低本地 dev Consul 429：共享 discover 缓存 + 拉长刷新间隔（可被环境变量覆盖）
export CONSUL_DISCOVERY_REFRESH_INTERVAL="${CONSUL_DISCOVERY_REFRESH_INTERVAL:-90}"
export CONSUL_DISCOVER_CACHE_TTL_SECS="${CONSUL_DISCOVER_CACHE_TTL_SECS:-15}"
export SERVICE_HEARTBEAT_INTERVAL="${SERVICE_HEARTBEAT_INTERVAL:-30}"

# 本地基础设施必须直连，避免系统 HTTP 代理拦截 localhost。
IM_CORE_NO_PROXY='localhost,127.0.0.1,::1'
export NO_PROXY="${NO_PROXY:+$NO_PROXY,}$IM_CORE_NO_PROXY"
export no_proxy="${no_proxy:+$no_proxy,}$IM_CORE_NO_PROXY"

# 颜色输出

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOGS_DIR="$PROJECT_ROOT/logs"
LOCAL_GATEWAY_TOKEN_SECRET_FILE="$LOGS_DIR/.dev-token-secret"

cleanup_launchctl_dev_labels() {
    if ! command -v launchctl >/dev/null 2>&1; then
        return 0
    fi

    local label suffix
    for suffix in debug release dev; do
        for label in \
            flare-signaling-online \
            flare-signaling-route \
            flare-capability \
            flare-conversation \
            flare-message-ingest \
            flare-message-orchestrator \
            flare-storage-writer \
            flare-storage-reader \
            flare-sync-orchestrator \
            flare-push-server \
            flare-push-worker \
            flare-media \
            flare-api-gateway \
            flare-access-gateway \
            flare-access-gateway-beijing-1 \
            flare-access-gateway-shanghai-1; do
            launchctl remove "$label-$suffix" >/dev/null 2>&1 || true
        done
    done
}

echo -e "${YELLOW}🧹 清理本地 launchctl debug/release/dev 残留任务...${NC}"
cleanup_launchctl_dev_labels
echo -e "${GREEN}   ✓ 清理完成${NC}"
echo ""

# Hook 配置档（可选）：core=无业务 Hook，social=flare-social PreSend
if [ -n "${FLARE_HOOKS_PROFILE:-}" ]; then
    # shellcheck source=lib/hooks_profile.sh
    source "$SCRIPT_DIR/lib/hooks_profile.sh"
    if flare_im_activate_hooks_profile "$FLARE_HOOKS_PROFILE" "$PROJECT_ROOT"; then
        echo -e "${BLUE}🔗 Hook 配置档: ${FLARE_HOOKS_PROFILE} → config/hooks.toml${NC}"
    else
        exit 1
    fi
fi

# 服务目录名（logs/pid 前缀）-> 实际 Cargo 二进制名
service_binary_name() {
    case "$1" in
        message-orchestrator) printf '%s' "flare-orchestrator" ;;
        *) printf '%s' "flare-$1" ;;
    esac
}

# 释放监听端口（避免上次异常退出后残留进程导致 gRPC transport error / AddrInUse）
free_listen_port() {
    local port="$1"
    if ! command -v lsof >/dev/null 2>&1; then
        return 0
    fi
    local pids
    pids=$(lsof -ti "tcp:${port}" -sTCP:LISTEN 2>/dev/null || true)
    if [ -z "$pids" ]; then
        return 0
    fi
    echo -e "${YELLOW}   释放端口 ${port}（残留监听 PID: ${pids}）...${NC}"
    # shellcheck disable=SC2086
    kill $pids 2>/dev/null || true
    sleep 1
    # shellcheck disable=SC2086
    kill -9 $pids 2>/dev/null || true
}

launchctl_label_pid() {
    local label="$1"
    launchctl list 2>/dev/null | awk -v label="$label" '$3 == label { print $1; exit }'
}

start_detached_process() {
    local label="$1"
    local log_file="$2"
    shift 2

    DETACHED_PID=""
    if [ "${FLARE_USE_LAUNCHCTL:-0}" != "0" ] && command -v launchctl >/dev/null 2>&1; then
        launchctl remove "$label" >/dev/null 2>&1 || true
        launchctl submit -l "$label" -o "$log_file" -e "$log_file" -- \
            /bin/sh -c 'cd "$1" && shift && exec "$@"' sh "$PROJECT_ROOT" "$@"

        local waited=0
        while [ "$waited" -lt 10 ]; do
            DETACHED_PID=$(launchctl_label_pid "$label")
            if [ -n "$DETACHED_PID" ] && [ "$DETACHED_PID" != "-" ]; then
                return 0
            fi
            sleep 1
            waited=$((waited + 1))
        done
        return 1
    fi

    nohup /bin/sh -c 'cd "$1" && shift && exec "$@"' sh "$PROJECT_ROOT" "$@" </dev/null > "$log_file" 2>&1 &
    DETACHED_PID=$!
    return 0
}

generate_local_token_secret() {
    if command -v openssl >/dev/null 2>&1; then
        openssl rand -base64 48 | tr -d '\n'
        return 0
    fi

    if command -v uuidgen >/dev/null 2>&1; then
        printf '%s%s\n' "$(uuidgen | tr -d '-')" "$(uuidgen | tr -d '-')"
        return 0
    fi

    LC_ALL=C tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 64
}

ensure_local_gateway_token_secrets() {
    if [ -n "${FLARE_CORE_GATEWAY_TOKEN_SECRET:-}" ] && [ -n "${ACCESS_GATEWAY_TOKEN_SECRET:-}" ]; then
        return 0
    fi

    mkdir -p "$LOGS_DIR"
    if [ ! -s "$LOCAL_GATEWAY_TOKEN_SECRET_FILE" ]; then
        umask 077
        generate_local_token_secret > "$LOCAL_GATEWAY_TOKEN_SECRET_FILE"
    fi

    local secret
    secret="$(cat "$LOCAL_GATEWAY_TOKEN_SECRET_FILE")"
    export FLARE_CORE_GATEWAY_TOKEN_SECRET="${FLARE_CORE_GATEWAY_TOKEN_SECRET:-$secret}"
    export ACCESS_GATEWAY_TOKEN_SECRET="${ACCESS_GATEWAY_TOKEN_SECRET:-$secret}"
    export FLARE_ADMIN_GATEWAY_TOKEN_SECRET="${FLARE_ADMIN_GATEWAY_TOKEN_SECRET:-$secret}"
}

FLARE_BUILD_PROFILE="${FLARE_BUILD_PROFILE:-debug}"
case "$FLARE_BUILD_PROFILE" in
    debug|release)
        ;;
    *)
        echo -e "${RED}错误: FLARE_BUILD_PROFILE 只能是 debug 或 release，当前: $FLARE_BUILD_PROFILE${NC}"
        exit 1
        ;;
esac

# 解析 Cargo 实际产物目录：上层仓库 `.cargo/config.toml` 可能将 `target-dir` 指到仓库根的 `target/`，
# 与 `flare-im-core/target` 不一致；启动与停止脚本必须使用与 `cargo build` 相同的 profile 目录。
resolve_cargo_target_profile() {
    local profile="$1"
    if [ -n "${CARGO_TARGET_DIR:-}" ]; then
        printf '%s/%s\n' "${CARGO_TARGET_DIR%/}" "$profile"
        return 0
    fi
    local meta td
    td=""
    meta=$(cd "$PROJECT_ROOT" && cargo metadata --format-version=1 --no-deps 2>/dev/null) || true
    if [ -n "$meta" ]; then
        if command -v jq >/dev/null 2>&1; then
            td=$(printf '%s' "$meta" | jq -r '.target_directory // empty' 2>/dev/null) || true
        fi
        if [ -z "$td" ] || [ "$td" = "null" ]; then
            td=$(printf '%s' "$meta" | grep -o '"target_directory":"[^"]*"' | head -1 | sed 's/"target_directory":"//;s/"$//')
        fi
        if [ -n "$td" ]; then
            printf '%s/%s\n' "$td" "$profile"
            return 0
        fi
    fi
    printf '%s/target/%s\n' "$PROJECT_ROOT" "$profile"
}
CARGO_TARGET_BIN_DIR="$(resolve_cargo_target_profile "$FLARE_BUILD_PROFILE")"
CARGO_BUILD_ARGS=(build --all)
if [ "$FLARE_BUILD_PROFILE" = "release" ]; then
    CARGO_BUILD_ARGS+=(--release)
fi

# 启动脚本依赖的核心二进制（与 CORE_SERVICES + access-gateway 一致）
REQUIRED_CORE_BINARIES=(
    flare-signaling-online
    flare-signaling-route
    flare-capability
    flare-conversation
    flare-message-ingest
    flare-orchestrator
    flare-storage-writer
    flare-storage-reader
    flare-sync-orchestrator
    flare-push-server
    flare-push-worker
    flare-media
    flare-api-gateway
    flare-signaling-gateway
)

core_binaries_ready() {
    local bin
    for bin in "${REQUIRED_CORE_BINARIES[@]}"; do
        if [ ! -x "$CARGO_TARGET_BIN_DIR/$bin" ]; then
            return 1
        fi
    done
    return 0
}

echo -e "${YELLOW}🧹 清除之前的日志...${NC}"
# 清除之前的日志
rm -rf "$LOGS_DIR"/*.log
echo -e "${GREEN}   ✓ 清除完成${NC}"
echo ""

# 解析参数
GATEWAY_MODE="${1:-single}"  # 默认单网关模式
VERBOSE_LOG_MODE=""           # 第二参数 trace|debug 时启用全量 RUST_LOG=trace（仅排障）

# 与 flare-server-core `default_env_filter` 同类项对齐：默认业务 debug，ORM/MQ/gRPC 栈降噪。
# 勿默认 trace：会覆盖 TOML 降噪并让 sqlx 等把 logs/ 撑到数 GB。
IM_CORE_DEFAULT_RUST_LOG='debug,hyper=warn,reqwest=warn,h2=warn,rdkafka=warn,tower=warn,tokio=warn,sqlx=warn,tantivy=warn,async_nats=warn,tonic=warn,redis=warn'

if [ "$GATEWAY_MODE" != "single" ] && [ "$GATEWAY_MODE" != "multi" ]; then
    echo -e "${RED}错误: 无效的参数 '$GATEWAY_MODE'${NC}"
    echo "使用方法: $0 [single|multi] [trace|debug]"
    echo "  - single: 启动单网关模式（默认）"
    echo "  - multi:  启动多网关模式"
    echo "  - trace|debug: 第二参数，全量跟踪（RUST_LOG=trace；debug 为历史别名）"
    echo "  - 默认: 未设置环境变量 RUST_LOG 时使用业务 debug + 第三方降噪"
    exit 1
fi

if [ "${2:-}" = "trace" ] || [ "${2:-}" = "debug" ]; then
    VERBOSE_LOG_MODE=1
    export RUST_LOG="${RUST_LOG:-trace}"
    echo -e "${GREEN}🔧 全量跟踪日志: RUST_LOG=$RUST_LOG（logs 体积会显著增大）${NC}"
    echo ""
else
    export RUST_LOG="${RUST_LOG:-$IM_CORE_DEFAULT_RUST_LOG}"
fi

# 创建日志目录
mkdir -p "$LOGS_DIR"
ensure_local_gateway_token_secrets
echo -e "${BLUE}🔐 本地网关 token secret 已准备: logs/.dev-token-secret（内容不会打印；本地客户端可将该文件内容作为 TOKEN_SECRET）${NC}"
echo ""

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Flare IM Core 完整服务启动脚本${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "${YELLOW}📁 日志目录: $LOGS_DIR${NC}"
echo -e "${YELLOW}🚪 网关模式: $GATEWAY_MODE${NC}"
echo -e "${YELLOW}📦 构建模式: $FLARE_BUILD_PROFILE${NC}"
if [ -n "$VERBOSE_LOG_MODE" ]; then
    echo -e "${GREEN}🔧 日志: RUST_LOG=$RUST_LOG${NC}"
else
    echo -e "${BLUE}   日志: RUST_LOG=$RUST_LOG${NC}"
fi
echo ""

# 检查基础设施服务
echo -e "${YELLOW}📦 检查基础设施服务状态...${NC}"
MISSING_INFRA=0
check_service() {
    local service=$1
    local port=$2
    
    if nc -z localhost $port 2>/dev/null; then
        echo -e "${GREEN}   ✓ $service 已就绪 (端口 $port)${NC}"
        return 0
    else
        echo -e "${RED}   ✗ $service 未运行 (端口 $port)${NC}"
        return 1
    fi
}

check_required_service() {
    check_service "$1" "$2" || MISSING_INFRA=1
}

resolve_mq_backend() {
    if [ -n "${FLARE_MQ_DEFAULT_BACKEND:-}" ]; then
        printf '%s\n' "$(printf '%s' "$FLARE_MQ_DEFAULT_BACKEND" | tr '[:upper:]' '[:lower:]')"
        return 0
    fi

    awk '
        /^\[mq\]/ { in_mq=1; next }
        /^\[/ && in_mq { exit }
        in_mq && $1 == "default_backend" {
            gsub(/"/, "", $3);
            print tolower($3);
            exit
        }
    ' "$PROJECT_ROOT/config/base.toml"
}

MQ_BACKEND="$(resolve_mq_backend)"
[ -n "$MQ_BACKEND" ] || MQ_BACKEND="nats"

check_required_service "Redis" "26379"
check_required_service "PostgreSQL" "25432"
check_required_service "RustFS/S3" "29000"
case "$MQ_BACKEND" in
    kafka)
        echo -e "${YELLOW}   ↷ Kafka 端口检查已跳过（由服务启动时按配置连接）${NC}"
        ;;
    nats|jetstream)
        check_required_service "NATS JetStream" "24222"
        ;;
    *)
        echo -e "${RED}   ✗ 未支持的 MQ 后端: $MQ_BACKEND (期望 nats|jetstream|kafka)${NC}"
        MISSING_INFRA=1
        ;;
esac
check_required_service "Consul" "28500"

echo ""
echo -e "${YELLOW}💡 提示: 如需启动基础设施服务，请运行:${NC}"
echo "   ${BLUE}cd deploy && docker compose up -d consul redis postgres nats rustfs${NC}"
echo ""

if [ "$MISSING_INFRA" -ne 0 ]; then
    echo -e "${RED}❌ 基础设施未全部就绪，已停止启动，避免客户端连接到半活服务。${NC}"
    exit 1
fi

case "$MQ_BACKEND" in
    kafka)
        echo -e "${YELLOW}📬 当前 MQ: Kafka (bootstrap: 127.0.0.1:29092)，NATS 不参与业务链路${NC}"
        ;;
    nats|jetstream)
        echo -e "${YELLOW}📬 当前 MQ: NATS JetStream，Stream 将由服务启动时按配置自动创建/更新${NC}"
        ;;
esac
echo ""

# 检查并停止旧进程
echo -e "${YELLOW}🔍 检查并停止旧进程...${NC}"

# 定义所有核心服务（包含 Makefile 中所有 run-* 服务）
# 注意：access-gateway (Signaling Gateway 服务，位于 flare-signaling/gateway) 不在核心服务列表中，将通过单/多网关模式启动
CORE_SERVICES=(
    "signaling-online"
    "signaling-route"
    "capability"
    "conversation"
    "message-ingest"
    "message-orchestrator"
    "storage-writer"
    "storage-reader"
    "sync-orchestrator"
    "push-server"
    "push-worker"
    "media"
    "core-gateway"
)

# 停止所有核心服务
for service in "${CORE_SERVICES[@]}"; do
    pid_file="$LOGS_DIR/flare-$service.pid"
    if [ -f "$pid_file" ]; then
        pid=$(cat "$pid_file")
        if ps -p "$pid" > /dev/null 2>&1; then
            echo -e "${YELLOW}   停止旧的 $service 进程 (PID: $pid)...${NC}"
            kill "$pid" 2>/dev/null || true
            sleep 1
            if ps -p "$pid" > /dev/null 2>&1; then
                kill -9 "$pid" 2>/dev/null || true
            fi
            rm -f "$pid_file"
        else
            rm -f "$pid_file"
        fi
    fi
    # 额外检查：按实际二进制名停止（message-orchestrator -> flare-orchestrator）
    bin=$(service_binary_name "$service")
    pkill -f "target/debug/${bin}" 2>/dev/null || true
    pkill -f "/target/debug/${bin}" 2>/dev/null || true
    pkill -f "target/release/${bin}" 2>/dev/null || true
    pkill -f "/target/release/${bin}" 2>/dev/null || true
done

# 清理已知本地服务监听端口，避免 release/debug 切换或异常退出留下旧进程。
# 仅覆盖本启动脚本管理的 core/access gateway 端口，不触碰中间件端口。
STARTUP_LISTEN_PORTS=(
    50050 50061 50062 50090 50110 50181
    19181 19182
    60051 60052 60060 60070 60071 60080 60081 60083 60084
)
for port in "${STARTUP_LISTEN_PORTS[@]}"; do
    free_listen_port "$port"
done

# 停止默认的 access-gateway 实例（如果存在）
pid_file="$LOGS_DIR/flare-access-gateway.pid"
if [ -f "$pid_file" ]; then
    pid=$(cat "$pid_file")
    if ps -p "$pid" > /dev/null 2>&1; then
        echo -e "${YELLOW}   停止默认 access-gateway 进程 (PID: $pid)...${NC}"
        kill "$pid" 2>/dev/null || true
        sleep 1
        if ps -p "$pid" > /dev/null 2>&1; then
            kill -9 "$pid" 2>/dev/null || true
        fi
        rm -f "$pid_file"
    else
        rm -f "$pid_file"
    fi
fi

# 停止多网关实例（使用普通数组，兼容 bash 3.x）
# 格式: gateway_key:region:gateway_id:ws_port:grpc_port
# 端口分配规则：
#   - WebSocket: ws_port (客户端连接)
#   - QUIC: ws_port + 1 (客户端连接)
#   - gRPC: grpc_port (服务间调用，直接地址连接)
# 注意：端口间隔至少 10，避免冲突
GATEWAYS=(
    "beijing-1:beijing:gateway-beijing-1:60051:60060"
    "shanghai-1:shanghai:gateway-shanghai-1:60070:60080"
)

for gateway_config in "${GATEWAYS[@]}"; do
    IFS=':' read -r gateway_key region gateway_id ws_port grpc_port <<< "$gateway_config"
    pid_file="$LOGS_DIR/flare-access-gateway-$gateway_key.pid"
    if [ -f "$pid_file" ]; then
        pid=$(cat "$pid_file")
        if ps -p "$pid" > /dev/null 2>&1; then
            echo -e "${YELLOW}   停止多网关实例 $gateway_key (PID: $pid)...${NC}"
            kill "$pid" 2>/dev/null || true
            sleep 1
            if ps -p "$pid" > /dev/null 2>&1; then
                kill -9 "$pid" 2>/dev/null || true
            fi
            rm -f "$pid_file"
        else
            rm -f "$pid_file"
        fi
    fi
done

sleep 1
echo -e "${GREEN}   ✓ 旧进程清理完成${NC}"
echo ""

cd "$PROJECT_ROOT"

# 先统一编译所有服务（避免并发编译导致文件锁冲突）
echo -e "${YELLOW}📦 编译所有核心服务...${NC}"
# 自动检测并设置 PROTOC 环境变量
if [ -z "$PROTOC" ]; then
    if command -v protoc > /dev/null 2>&1; then
        export PROTOC=$(which protoc)
    elif [ -f "/opt/homebrew/bin/protoc" ]; then
        export PROTOC="/opt/homebrew/bin/protoc"
    elif [ -f "/usr/local/bin/protoc" ]; then
        export PROTOC="/usr/local/bin/protoc"
    fi
fi

if [ -n "$PROTOC" ] && [ -f "$PROTOC" ]; then
    echo -e "${GREEN}   📦 使用 protoc: $PROTOC${NC}"
else
    echo -e "${RED}   ✗ 错误: 未找到 protoc 编译器${NC}"
    echo -e "${YELLOW}   请安装 protobuf: brew install protobuf${NC}"
    echo -e "${YELLOW}   或设置 PROTOC 环境变量指向 protoc 路径${NC}"
    exit 1
fi

BUILD_LOG="$LOGS_DIR/cargo-build.log"
if [ "${FLARE_SKIP_BUILD:-0}" = "1" ] && core_binaries_ready; then
    echo -e "${GREEN}   ↷ 跳过编译 (FLARE_SKIP_BUILD=1，二进制已在 $CARGO_TARGET_BIN_DIR)${NC}"
elif [ "${FLARE_SKIP_BUILD:-0}" = "1" ]; then
    echo -e "${YELLOW}   ⚠ FLARE_SKIP_BUILD=1 但部分二进制缺失，仍将执行 cargo build${NC}"
fi

if [ "${FLARE_SKIP_BUILD:-0}" != "1" ] || ! core_binaries_ready; then
    cargo_build_cmd=(cargo "${CARGO_BUILD_ARGS[@]}")
    if [ -n "${CARGO_BUILD_JOBS:-}" ]; then
        cargo_build_cmd+=(--jobs "$CARGO_BUILD_JOBS")
        echo -e "${BLUE}   并行编译: --jobs $CARGO_BUILD_JOBS${NC}"
    fi
    echo -e "${BLUE}   编译日志: $BUILD_LOG${NC}"
    if ! PROTOC="$PROTOC" "${cargo_build_cmd[@]}" > "$BUILD_LOG" 2>&1; then
        echo -e "${RED}   ✗ 编译失败，末尾输出:${NC}"
        tail -n 40 "$BUILD_LOG" || true
        echo -e "${YELLOW}   完整日志: $BUILD_LOG${NC}"
        echo -e "${YELLOW}   或运行: PROTOC=$PROTOC ${cargo_build_cmd[*]}${NC}"
        exit 1
    fi
fi
echo -e "${GREEN}   ✓ 编译完成${NC}"
echo ""

# 启动核心服务
echo -e "${GREEN}🚀 启动 Flare IM Core 核心服务...${NC}"

# 定义服务启动顺序（按照依赖关系排序）
# 1. 基础服务：signaling-online（在线状态服务）、signaling-route（路由目录服务）
# 2. 能力服务：capability（服务注册名 flare-capability，见 flare_im_contracts::service_names::CAPABILITY；包与二进制名均为 flare-capability）
# 3. 会话服务：conversation（会话管理服务）
# 4. 消息摄入：message-ingest（发送 gRPC、seq、WAL、Pre/PostSend Hook）
# 5. 消息编排：message-orchestrator（主消息流 fanout、消息操作事件）
# 6. 存储服务：storage-writer（消息持久化）、storage-reader（消息查询）
# 7. 同步编排：sync-orchestrator（统一 SyncService 入口）
# 8. 推送服务：push-server（推送服务）、push-worker（推送工作器）
# 9. 媒资服务：media（媒资服务）
# 10. 核心网关：core-gateway（业务系统统一入口）
# 11. 接入网关：signaling-gateway (Signaling Gateway 服务，位于 flare-signaling/gateway，通过单/多网关模式启动，见下方启动部分)

# 启动服务（后台运行）
for service in "${CORE_SERVICES[@]}"; do
    echo -e "${YELLOW}   启动 $service...${NC}"
    
    # 根据服务名称构建包名和二进制名称
    case "$service" in
        "signaling-online")
            PACKAGE="flare-signaling-online"
            BIN_NAME="flare-signaling-online"
            # 优先使用命令行参数，其次使用配置文件，最后使用默认值
            SIGNALING_ONLINE_HOST=${SIGNALING_ONLINE_SERVICE_HOST:-$(grep -E '^\s*address\s*=' "$PROJECT_ROOT/config/services/signaling-online.toml" | sed -E 's/.*=\s*"([^"]*)".*/\1/' | head -1)}
            SIGNALING_ONLINE_HOST=${SIGNALING_ONLINE_HOST:-"0.0.0.0"}
            SIGNALING_ONLINE_PORT=${SIGNALING_ONLINE_SERVICE_PORT:-$(grep -E '^\s*port\s*=' "$PROJECT_ROOT/config/services/signaling-online.toml" | sed -E 's/.*=\s*([0-9]+).*/\1/' | head -1)}
            SIGNALING_ONLINE_PORT=${SIGNALING_ONLINE_PORT:-50061}
            ONLINE_SERVICE_ENDPOINT=${ONLINE_SERVICE_ENDPOINT:-"http://${SIGNALING_ONLINE_HOST}:${SIGNALING_ONLINE_PORT}"}
            # 直接设置环境变量
            export SIGNALING_ONLINE_SERVICE_HOST="$SIGNALING_ONLINE_HOST"
            export SIGNALING_ONLINE_SERVICE_PORT="$SIGNALING_ONLINE_PORT"
            export ONLINE_SERVICE_ENDPOINT="$ONLINE_SERVICE_ENDPOINT"
            ENV_VARS="set"
            ;;
        "signaling-route")
            PACKAGE="flare-signaling-route"
            BIN_NAME="flare-signaling-route"
            # 优先使用命令行参数，其次使用配置文件，最后使用默认值
            SIGNALING_ROUTE_HOST=${SIGNALING_ROUTE_SERVICE_HOST:-$(grep -E '^\s*address\s*=' "$PROJECT_ROOT/config/services/signaling-route.toml" | sed -E 's/.*=\s*"([^"]*)".*/\1/' | head -1)}
            SIGNALING_ROUTE_HOST=${SIGNALING_ROUTE_HOST:-"0.0.0.0"}
            SIGNALING_ROUTE_PORT=${SIGNALING_ROUTE_SERVICE_PORT:-$(grep -E '^\s*port\s*=' "$PROJECT_ROOT/config/services/signaling-route.toml" | sed -E 's/.*=\s*([0-9]+).*/\1/' | head -1)}
            SIGNALING_ROUTE_PORT=${SIGNALING_ROUTE_PORT:-50062}
            ROUTE_SERVICE_ENDPOINT=${ROUTE_SERVICE_ENDPOINT:-"http://${SIGNALING_ROUTE_HOST}:${SIGNALING_ROUTE_PORT}"}
            # 直接设置环境变量
            export SIGNALING_ROUTE_SERVICE_HOST="$SIGNALING_ROUTE_HOST"
            export SIGNALING_ROUTE_SERVICE_PORT="$SIGNALING_ROUTE_PORT"
            export ROUTE_SERVICE_ENDPOINT="$ROUTE_SERVICE_ENDPOINT"
            ENV_VARS="set"
            ;;
        "capability")
            PACKAGE="flare-capability"
            BIN_NAME="flare-capability"
            ENV_VARS=""
            ;;
        "conversation")
            PACKAGE="flare-conversation"
            BIN_NAME="flare-conversation"
            ENV_VARS=""
            ;;
        "message-ingest")
            PACKAGE="flare-message-ingest"
            BIN_NAME="flare-message-ingest"
            ENV_VARS=""
            ;;
        "message-orchestrator")
            PACKAGE="flare-orchestrator"
            BIN_NAME="flare-orchestrator"
            ENV_VARS=""
            ;;
        "storage-writer")
            PACKAGE="flare-storage-writer"
            BIN_NAME="flare-storage-writer"
            ENV_VARS=""
            ;;
        "storage-reader")
            PACKAGE="flare-storage-reader"
            BIN_NAME="flare-storage-reader"
            ENV_VARS=""
            ;;
        "sync-orchestrator")
            PACKAGE="flare-sync-orchestrator"
            BIN_NAME="flare-sync-orchestrator"
            export SYNC_ORCHESTRATOR_HOST="${SYNC_ORCHESTRATOR_HOST:-0.0.0.0}"
            export SYNC_ORCHESTRATOR_PORT="${SYNC_ORCHESTRATOR_PORT:-60084}"
            ENV_VARS=""
            ;;
        "push-server")
            PACKAGE="flare-push-server"
            BIN_NAME="flare-push-server"
            ENV_VARS=""
            ;;
        "push-worker")
            PACKAGE="flare-push-worker"
            BIN_NAME="flare-push-worker"
            if [ "$GATEWAY_MODE" = "single" ]; then
                export ACCESS_GATEWAY_GRPC_ENDPOINT="${ACCESS_GATEWAY_GRPC_ENDPOINT:-http://127.0.0.1:60060}"
            fi
            ENV_VARS=""
            ;;
        "media")
            PACKAGE="flare-media"
            BIN_NAME="flare-media"
            ENV_VARS=""
            ;;
        "core-gateway")
            PACKAGE="flare-api-gateway"
            BIN_NAME="flare-api-gateway"
            MESSAGE_INGEST_PORT=${MESSAGE_INGEST_PORT:-$(grep -E '^\s*port\s*=' "$PROJECT_ROOT/config/services/message_ingest.toml" | sed -E 's/.*=\s*([0-9]+).*/\1/' | head -1)}
            MESSAGE_INGEST_PORT=${MESSAGE_INGEST_PORT:-50182}
            MESSAGE_ORCH_PORT=${MESSAGE_ORCH_PORT:-$(grep -E '^\s*port\s*=' "$PROJECT_ROOT/config/services/message_orchestrator.toml" | sed -E 's/.*=\s*([0-9]+).*/\1/' | head -1)}
            MESSAGE_ORCH_PORT=${MESSAGE_ORCH_PORT:-50181}
            CONVERSATION_PORT=${CONVERSATION_PORT:-$(grep -E '^\s*port\s*=' "$PROJECT_ROOT/config/services/conversation.toml" | sed -E 's/.*=\s*([0-9]+).*/\1/' | head -1)}
            CONVERSATION_PORT=${CONVERSATION_PORT:-50090}
            MEDIA_PORT=${MEDIA_PORT:-$(grep -E '^\s*port\s*=' "$PROJECT_ROOT/config/services/media.toml" | sed -E 's/.*=\s*([0-9]+).*/\1/' | head -1)}
            MEDIA_PORT=${MEDIA_PORT:-60081}
            export FLARE_CORE_GATEWAY_GRPC_MESSAGE_INGEST_STATIC_FALLBACK="${FLARE_CORE_GATEWAY_GRPC_MESSAGE_INGEST_STATIC_FALLBACK:-http://127.0.0.1:${MESSAGE_INGEST_PORT}}"
            export FLARE_CORE_GATEWAY_GRPC_MESSAGE_ORCHESTRATOR_STATIC_FALLBACK="${FLARE_CORE_GATEWAY_GRPC_MESSAGE_ORCHESTRATOR_STATIC_FALLBACK:-http://127.0.0.1:${MESSAGE_ORCH_PORT}}"
            export FLARE_CORE_GATEWAY_GRPC_CONVERSATION_STATIC_FALLBACK="${FLARE_CORE_GATEWAY_GRPC_CONVERSATION_STATIC_FALLBACK:-http://127.0.0.1:${CONVERSATION_PORT}}"
            export FLARE_CORE_GATEWAY_GRPC_MEDIA_STATIC_FALLBACK="${FLARE_CORE_GATEWAY_GRPC_MEDIA_STATIC_FALLBACK:-http://127.0.0.1:${MEDIA_PORT}}"
            ENV_VARS="set"
            ;;
        *)
            echo -e "${RED}   ✗ 未知服务: $service${NC}"
            continue
            ;;
    esac
    
    # 设置 PID 文件路径
    pid_file="$LOGS_DIR/flare-$service.pid"
    
    # message-ingest/message-orchestrator：启动前确保端口未被残留进程占用
    if [ "$service" = "message-ingest" ]; then
        ingest_port=${MESSAGE_INGEST_PORT:-$(grep -E '^\s*port\s*=' "$PROJECT_ROOT/config/services/message_ingest.toml" | sed -E 's/.*=\s*([0-9]+).*/\1/' | head -1)}
        ingest_port=${ingest_port:-50182}
        free_listen_port "$ingest_port"
    elif [ "$service" = "message-orchestrator" ]; then
        orch_port=${MESSAGE_ORCH_PORT:-$(grep -E '^\s*port\s*=' "$PROJECT_ROOT/config/services/message_orchestrator.toml" | sed -E 's/.*=\s*([0-9]+).*/\1/' | head -1)}
        orch_port=${orch_port:-50181}
        free_listen_port "$orch_port"
    fi

    env_args=(
        "PATH=$PATH"
        "RUST_LOG=$RUST_LOG"
        "NO_PROXY=$NO_PROXY"
        "no_proxy=$no_proxy"
        "CONSUL_DISCOVERY_REFRESH_INTERVAL=$CONSUL_DISCOVERY_REFRESH_INTERVAL"
        "CONSUL_DISCOVER_CACHE_TTL_SECS=$CONSUL_DISCOVER_CACHE_TTL_SECS"
        "SERVICE_HEARTBEAT_INTERVAL=$SERVICE_HEARTBEAT_INTERVAL"
        "FLARE_CORE_GATEWAY_TOKEN_SECRET=$FLARE_CORE_GATEWAY_TOKEN_SECRET"
        "ACCESS_GATEWAY_TOKEN_SECRET=$ACCESS_GATEWAY_TOKEN_SECRET"
        "FLARE_ADMIN_GATEWAY_TOKEN_SECRET=$FLARE_ADMIN_GATEWAY_TOKEN_SECRET"
    )
    [ -n "${SIGNALING_ONLINE_SERVICE_HOST:-}" ] && env_args+=("SIGNALING_ONLINE_SERVICE_HOST=$SIGNALING_ONLINE_SERVICE_HOST")
    [ -n "${SIGNALING_ONLINE_SERVICE_PORT:-}" ] && env_args+=("SIGNALING_ONLINE_SERVICE_PORT=$SIGNALING_ONLINE_SERVICE_PORT")
    [ -n "${ONLINE_SERVICE_ENDPOINT:-}" ] && env_args+=("ONLINE_SERVICE_ENDPOINT=$ONLINE_SERVICE_ENDPOINT")
    [ -n "${SIGNALING_ROUTE_SERVICE_HOST:-}" ] && env_args+=("SIGNALING_ROUTE_SERVICE_HOST=$SIGNALING_ROUTE_SERVICE_HOST")
    [ -n "${SIGNALING_ROUTE_SERVICE_PORT:-}" ] && env_args+=("SIGNALING_ROUTE_SERVICE_PORT=$SIGNALING_ROUTE_SERVICE_PORT")
    [ -n "${ROUTE_SERVICE_ENDPOINT:-}" ] && env_args+=("ROUTE_SERVICE_ENDPOINT=$ROUTE_SERVICE_ENDPOINT")
    [ -n "${SYNC_ORCHESTRATOR_HOST:-}" ] && env_args+=("SYNC_ORCHESTRATOR_HOST=$SYNC_ORCHESTRATOR_HOST")
    [ -n "${SYNC_ORCHESTRATOR_PORT:-}" ] && env_args+=("SYNC_ORCHESTRATOR_PORT=$SYNC_ORCHESTRATOR_PORT")
    [ -n "${ACCESS_GATEWAY_GRPC_ENDPOINT:-}" ] && env_args+=("ACCESS_GATEWAY_GRPC_ENDPOINT=$ACCESS_GATEWAY_GRPC_ENDPOINT")
    [ -n "${FLARE_CORE_GATEWAY_GRPC_MESSAGE_INGEST_STATIC_FALLBACK:-}" ] && env_args+=("FLARE_CORE_GATEWAY_GRPC_MESSAGE_INGEST_STATIC_FALLBACK=$FLARE_CORE_GATEWAY_GRPC_MESSAGE_INGEST_STATIC_FALLBACK")
    [ -n "${FLARE_CORE_GATEWAY_GRPC_MESSAGE_ORCHESTRATOR_STATIC_FALLBACK:-}" ] && env_args+=("FLARE_CORE_GATEWAY_GRPC_MESSAGE_ORCHESTRATOR_STATIC_FALLBACK=$FLARE_CORE_GATEWAY_GRPC_MESSAGE_ORCHESTRATOR_STATIC_FALLBACK")
    [ -n "${FLARE_CORE_GATEWAY_GRPC_CONVERSATION_STATIC_FALLBACK:-}" ] && env_args+=("FLARE_CORE_GATEWAY_GRPC_CONVERSATION_STATIC_FALLBACK=$FLARE_CORE_GATEWAY_GRPC_CONVERSATION_STATIC_FALLBACK")
    [ -n "${FLARE_CORE_GATEWAY_GRPC_MEDIA_STATIC_FALLBACK:-}" ] && env_args+=("FLARE_CORE_GATEWAY_GRPC_MEDIA_STATIC_FALLBACK=$FLARE_CORE_GATEWAY_GRPC_MEDIA_STATIC_FALLBACK")

    # 启动服务（使用编译好的二进制，避免并发编译问题）
    start_detached_process "flare-$service-$FLARE_BUILD_PROFILE" "$LOGS_DIR/flare-$service.log" /usr/bin/env "${env_args[@]}" "$CARGO_TARGET_BIN_DIR/$BIN_NAME"
    service_pid="$DETACHED_PID"
    
    # 清理环境变量
    if [ "$service" = "signaling-online" ]; then
        unset SIGNALING_ONLINE_SERVICE_HOST SIGNALING_ONLINE_SERVICE_PORT ONLINE_SERVICE_ENDPOINT
    elif [ "$service" = "signaling-route" ]; then
        unset SIGNALING_ROUTE_SERVICE_HOST SIGNALING_ROUTE_SERVICE_PORT ROUTE_SERVICE_ENDPOINT
    elif [ "$service" = "sync-orchestrator" ]; then
        unset SYNC_ORCHESTRATOR_HOST SYNC_ORCHESTRATOR_PORT
    elif [ "$service" = "push-worker" ]; then
        unset ACCESS_GATEWAY_GRPC_ENDPOINT
    elif [ "$service" = "core-gateway" ]; then
        unset FLARE_CORE_GATEWAY_GRPC_MESSAGE_INGEST_STATIC_FALLBACK FLARE_CORE_GATEWAY_GRPC_MESSAGE_ORCHESTRATOR_STATIC_FALLBACK FLARE_CORE_GATEWAY_GRPC_CONVERSATION_STATIC_FALLBACK FLARE_CORE_GATEWAY_GRPC_MEDIA_STATIC_FALLBACK
    fi
    echo $service_pid > "$pid_file"
    sleep 3
done

# 等待核心服务启动
echo ""
echo -e "${YELLOW}⏳ 等待核心服务启动...${NC}"
sleep 10

# 检查核心服务是否运行
check_process() {
    local service=$1
    local pid_file="$LOGS_DIR/flare-$service.pid"
    local bin
    bin=$(service_binary_name "$service")
    
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if ps -p "$pid" > /dev/null 2>&1; then
            echo -e "${GREEN}   ✓ $service 正在运行 (PID: $pid)${NC}"
            return 0
        elif pgrep -f "$CARGO_TARGET_BIN_DIR/$bin" > /dev/null 2>&1; then
            local actual_pid
            actual_pid=$(pgrep -f "$CARGO_TARGET_BIN_DIR/$bin" | head -1)
            echo "$actual_pid" > "$pid_file"
            echo -e "${GREEN}   ✓ $service 正在运行 (PID: $actual_pid)${NC}"
            return 0
        else
            echo -e "${RED}   ✗ $service 启动失败${NC}"
            echo -e "${YELLOW}   查看日志: tail -f $LOGS_DIR/flare-$service.log${NC}"
            return 1
        fi
    else
        echo -e "${RED}   ✗ $service PID 文件不存在${NC}"
        return 1
    fi
}

echo ""
echo -e "${GREEN}📊 核心服务状态检查:${NC}"
CORE_START_FAILURES=0
for service in "${CORE_SERVICES[@]}"; do
    check_process "$service" || CORE_START_FAILURES=1
done

echo ""
if [ "$CORE_START_FAILURES" -ne 0 ]; then
    echo -e "${RED}❌ 核心服务未全部启动，已中止后续网关启动。${NC}"
    exit 1
fi
echo -e "${GREEN}✅ 核心服务启动完成${NC}"
echo ""

# SFU 能力插件（Consul 注册 flare-strom-sfu；capability 通过 service_name 发现）
STROM_SFU_DIR="$PROJECT_ROOT/../flare-plugin/flare-strom-sfu"
if [ "${START_STROM_SFU_PLUGIN:-1}" != "0" ] && [ -f "$STROM_SFU_DIR/Makefile" ]; then
    echo -e "${GREEN}🚀 启动 Strom SFU 插件（flare-strom-sfu → Consul）...${NC}"
    if (cd "$STROM_SFU_DIR" && make plugin-stop >/dev/null 2>&1 || true) \
        && (cd "$STROM_SFU_DIR" && make plugin-start); then
        echo -e "${GREEN}   ✓ Strom SFU plugin 已启动（gRPC :50060，服务名 flare-strom-sfu）${NC}"
    else
        echo -e "${YELLOW}   ⚠ Strom SFU plugin 启动失败；RTC 通话需手动: cd flare-plugin/flare-strom-sfu && make plugin-start${NC}"
    fi
    echo ""
else
    echo -e "${YELLOW}💡 跳过 Strom SFU plugin（START_STROM_SFU_PLUGIN=0 或目录不存在）${NC}"
    echo -e "${YELLOW}   RTC 通话需: cd flare-plugin/flare-strom-sfu && make plugin-start${NC}"
    echo ""
fi

# 启动 Access Gateway（根据模式选择单网关或多网关）
if [ "$GATEWAY_MODE" == "single" ]; then
    # 单网关模式：启动单个 signaling-gateway 实例 (Signaling Gateway 服务，位于 flare-signaling/gateway)
    echo -e "${GREEN}🚀 启动 Access Gateway（单网关模式）...${NC}"
    echo ""
    
    # 使用默认端口（从配置文件中读取）
    DEFAULT_WS_PORT=60051
    DEFAULT_GRPC_PORT=60060
    
    # 检查并停止可能存在的旧进程
    pid_file="$LOGS_DIR/flare-access-gateway.pid"
    if [ -f "$pid_file" ]; then
        pid=$(cat "$pid_file")
        if ps -p "$pid" > /dev/null 2>&1; then
            echo -e "${YELLOW}   停止旧的 access-gateway 进程 (PID: $pid)...${NC}"
            kill "$pid" 2>/dev/null || true
            sleep 1
            if ps -p "$pid" > /dev/null 2>&1; then
                kill -9 "$pid" 2>/dev/null || true
            fi
            rm -f "$pid_file"
        else
            rm -f "$pid_file"
        fi
    fi
    
    # 仅清理「在本端口上 LISTEN」的旧 access-gateway（flare-signaling-gateway）。
    # 注意：勿使用 `lsof -ti :port` 无状态过滤——它会把「作为客户端连到该端口」的进程也算进来，
    # 曾误杀 flare-signaling-online（gRPC 在 50061），导致 access-gateway 启动后核心服务缺位。
    check_and_kill_stale_access_gateway_listener() {
        local port=$1
        local pid
        pid=$(lsof -nP -iTCP:"$port" -sTCP:LISTEN -t 2>/dev/null | head -1)
        [ -z "$pid" ] && return 0
        local cmd
        cmd=$(ps -p "$pid" -o args= 2>/dev/null || true)
        case "$cmd" in
            *flare-signaling-gateway*)
                echo -e "${YELLOW}   检测到端口 $port 上旧 access-gateway (PID: $pid)，正在停止...${NC}"
                kill "$pid" 2>/dev/null || true
                sleep 1
                if ps -p "$pid" > /dev/null 2>&1; then
                    kill -9 "$pid" 2>/dev/null || true
                fi
                ;;
            *)
                echo -e "${RED}   错误: 端口 $port 已被其他进程监听 (PID: $pid)，无法启动 access-gateway${NC}"
                echo -e "${YELLOW}   命令行: $cmd${NC}"
                echo -e "${YELLOW}   请结束占用进程或修改网关/服务端口配置后重试。${NC}"
                exit 1
                ;;
        esac
    }
    
    # 检查并清理端口占用（仅旧网关监听）
    check_and_kill_stale_access_gateway_listener "$DEFAULT_WS_PORT"
    check_and_kill_stale_access_gateway_listener "$((DEFAULT_WS_PORT + 1))"  # QUIC port
    check_and_kill_stale_access_gateway_listener "$DEFAULT_GRPC_PORT"
    
    echo -e "${YELLOW}   启动 access-gateway (默认端口)...${NC}"
    echo -e "${BLUE}      WebSocket: $DEFAULT_WS_PORT, QUIC: $((DEFAULT_WS_PORT + 1)), gRPC: $DEFAULT_GRPC_PORT${NC}"
    
    gateway_env_args=(
        "PATH=$PATH"
        "RUST_LOG=$RUST_LOG"
        "NO_PROXY=$NO_PROXY"
        "no_proxy=$no_proxy"
        "CONSUL_DISCOVERY_REFRESH_INTERVAL=$CONSUL_DISCOVERY_REFRESH_INTERVAL"
        "CONSUL_DISCOVER_CACHE_TTL_SECS=$CONSUL_DISCOVER_CACHE_TTL_SECS"
        "SERVICE_HEARTBEAT_INTERVAL=$SERVICE_HEARTBEAT_INTERVAL"
        "FLARE_CORE_GATEWAY_TOKEN_SECRET=$FLARE_CORE_GATEWAY_TOKEN_SECRET"
        "ACCESS_GATEWAY_TOKEN_SECRET=$ACCESS_GATEWAY_TOKEN_SECRET"
        "FLARE_ADMIN_GATEWAY_TOKEN_SECRET=$FLARE_ADMIN_GATEWAY_TOKEN_SECRET"
        "PORT=$DEFAULT_WS_PORT"
        "GRPC_PORT=$DEFAULT_GRPC_PORT"
    )
    start_detached_process "flare-access-gateway-$FLARE_BUILD_PROFILE" "$LOGS_DIR/flare-access-gateway.log" /usr/bin/env "${gateway_env_args[@]}" "$CARGO_TARGET_BIN_DIR/flare-signaling-gateway"
    gateway_pid="$DETACHED_PID"
    echo $gateway_pid > "$pid_file"
    sleep 3
    
    # 检查是否启动成功
    if ps -p $(cat "$pid_file") > /dev/null 2>&1; then
        echo -e "${GREEN}      ✓ access-gateway 启动成功 (PID: $(cat $pid_file))${NC}"
    else
        echo -e "${RED}      ✗ access-gateway 启动失败${NC}"
        echo -e "${YELLOW}     查看日志: tail -f $LOGS_DIR/flare-access-gateway.log${NC}"
        exit 1
    fi
    
    echo ""
else
    # 多网关模式：启动多个 signaling-gateway 实例 (Signaling Gateway 服务，位于 flare-signaling/gateway)
    # 注意：access-gateway 已在前面统一编译，这里直接使用编译好的二进制
    echo -e "${GREEN}🚀 启动 Access Gateway（多网关模式）...${NC}"
    echo ""
    
    # 定义多网关配置（使用普通数组，兼容 bash 3.x）
    # 格式: gateway_key:region:gateway_id:ws_port:grpc_port
    # 端口分配规则：
    #   - WebSocket: ws_port (客户端连接)
    #   - QUIC: ws_port + 1 (客户端连接)
    #   - gRPC: grpc_port (服务间调用，直接地址连接)
    # 注意：端口间隔至少 10，避免冲突
    GATEWAYS=(
        "beijing-1:beijing:gateway-beijing-1:60051:60060"
        "shanghai-1:shanghai:gateway-shanghai-1:60070:60080"
    )
    
    for gateway_config in "${GATEWAYS[@]}"; do
        IFS=':' read -r gateway_key region gateway_id ws_port grpc_port <<< "$gateway_config"
        
        echo -e "${YELLOW}   启动 $gateway_key (Region: $region, ID: $gateway_id)...${NC}"
        echo -e "${BLUE}      WebSocket: $ws_port, QUIC: $((ws_port + 1)), gRPC: $grpc_port${NC}"
        
        # 检查并停止可能存在的旧进程
        pid_file="$LOGS_DIR/flare-access-gateway-$gateway_key.pid"
        if [ -f "$pid_file" ]; then
            pid=$(cat "$pid_file")
            if ps -p "$pid" > /dev/null 2>&1; then
                echo -e "${YELLOW}     停止旧的 $gateway_key 进程 (PID: $pid)...${NC}"
                kill "$pid" 2>/dev/null || true
                sleep 1
                if ps -p "$pid" > /dev/null 2>&1; then
                    kill -9 "$pid" 2>/dev/null || true
                fi
                rm -f "$pid_file"
            else
                rm -f "$pid_file"
            fi
        fi
        
        # 启动 Access Gateway（使用编译好的二进制，避免并发编译问题）
        # 同时设置环境变量供 direct_address 模块使用
        # 将 gateway_key 转换为大写并替换连字符为下划线（兼容 bash 3.x，变量名不能包含连字符）
        gateway_key_upper=$(echo "$gateway_key" | tr '[:lower:]' '[:upper:]' | tr '-' '_')
        # 使用 eval 来设置动态环境变量名
        eval "GATEWAY_${gateway_key_upper}_GRPC_PORT=$grpc_port"
        
        eval "GATEWAY_${gateway_key_upper}_GRPC_PORT=$grpc_port"
        gateway_env_args=(
            "PATH=$PATH"
            "RUST_LOG=$RUST_LOG"
            "NO_PROXY=$NO_PROXY"
            "no_proxy=$no_proxy"
            "CONSUL_DISCOVERY_REFRESH_INTERVAL=$CONSUL_DISCOVERY_REFRESH_INTERVAL"
            "CONSUL_DISCOVER_CACHE_TTL_SECS=$CONSUL_DISCOVER_CACHE_TTL_SECS"
            "SERVICE_HEARTBEAT_INTERVAL=$SERVICE_HEARTBEAT_INTERVAL"
            "FLARE_CORE_GATEWAY_TOKEN_SECRET=$FLARE_CORE_GATEWAY_TOKEN_SECRET"
            "ACCESS_GATEWAY_TOKEN_SECRET=$ACCESS_GATEWAY_TOKEN_SECRET"
            "FLARE_ADMIN_GATEWAY_TOKEN_SECRET=$FLARE_ADMIN_GATEWAY_TOKEN_SECRET"
            "GATEWAY_ID=$gateway_id"
            "GATEWAY_REGION=$region"
            "PORT=$ws_port"
            "GRPC_PORT=$grpc_port"
            "GATEWAY_${gateway_key_upper}_GRPC_PORT=$grpc_port"
        )
        start_detached_process "flare-access-gateway-$gateway_key-$FLARE_BUILD_PROFILE" "$LOGS_DIR/flare-access-gateway-$gateway_key.log" /usr/bin/env "${gateway_env_args[@]}" "$CARGO_TARGET_BIN_DIR/flare-signaling-gateway"
        gateway_pid="$DETACHED_PID"
        echo $gateway_pid > "$pid_file"
        sleep 3
        
        # 检查是否启动成功
        if ps -p $(cat "$pid_file") > /dev/null 2>&1; then
            echo -e "${GREEN}      ✓ $gateway_key 启动成功 (PID: $(cat $pid_file))${NC}"
        else
            echo -e "${RED}      ✗ $gateway_key 启动失败${NC}"
            echo -e "${YELLOW}     查看日志: tail -f $LOGS_DIR/flare-access-gateway-$gateway_key.log${NC}"
            exit 1
        fi
        
        echo ""
    done
fi

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}✅ Flare IM Core 所有服务启动完成！${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# 等待服务完全启动（signaling-route 依赖 conversation，需等端口就绪）
echo -e "${YELLOW}⏳ 等待服务完全启动并检查状态...${NC}"

wait_for_listen_port() {
    local port=$1
    local label=$2
    local max_wait=${3:-90}
    local waited=0
    while [ "$waited" -lt "$max_wait" ]; do
        if command -v nc >/dev/null 2>&1 && nc -z localhost "$port" 2>/dev/null; then
            return 0
        elif lsof -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
            return 0
        fi
        sleep 2
        waited=$((waited + 2))
    done
    echo -e "${YELLOW}   ⚠ $label 端口 $port 在 ${max_wait}s 内未就绪${NC}"
    return 1
}

wait_for_listen_port 50090 "conversation" 90 || true
wait_for_listen_port 50062 "signaling-route" 90 || true
sleep 2

# 调用检查脚本
echo ""
"$SCRIPT_DIR/check_services.sh"
CHECK_RESULT=$?

echo ""
echo -e "${YELLOW}📝 使用说明:${NC}"
echo ""

if [ "$GATEWAY_MODE" == "single" ]; then
    echo "连接到网关:"
    echo "   ${GREEN}NEGOTIATION_HOST=localhost:60051 cargo run --example chatroom_client -- user1${NC}"
    echo ""
else
    echo "1. 连接到北京网关:"
    echo "   ${GREEN}NEGOTIATION_HOST=localhost:60051 cargo run --example chatroom_client -- user1${NC}"
    echo ""
    echo "2. 连接到上海网关:"
    echo "   ${GREEN}NEGOTIATION_HOST=localhost:60070 cargo run --example chatroom_client -- user2${NC}"
    echo ""
fi

echo "3. 业务系统推送消息:"
echo "   ${GREEN}cargo run --example business_push_client${NC}"
echo ""
echo -e "${YELLOW}📋 服务日志:${NC}"
if [ "$GATEWAY_MODE" == "single" ]; then
    echo "   - Access Gateway: tail -f $LOGS_DIR/flare-access-gateway.log"
else
    echo "   - Access Gateway (北京): tail -f $LOGS_DIR/flare-access-gateway-beijing-1.log"
    echo "   - Access Gateway (上海): tail -f $LOGS_DIR/flare-access-gateway-shanghai-1.log"
fi
echo "   - Message Orchestrator: tail -f $LOGS_DIR/flare-message-orchestrator.log"
echo "   - Push Server: tail -f $LOGS_DIR/flare-push-server.log"
echo ""
echo -e "${YELLOW}📁 所有日志文件位置:${NC}"
echo "   $LOGS_DIR/"
echo ""
echo -e "${YELLOW}🛑 停止所有服务:${NC}"
echo "   ${RED}./scripts/stop_server.sh${NC}"
echo ""

# 如果检查失败，返回非零退出码
if [ $CHECK_RESULT -ne 0 ]; then
    exit 1
fi
