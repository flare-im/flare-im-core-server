#!/bin/bash
# 启动 Flare IM Core 所有服务模块
#
# 使用方法:
#   ./scripts/start_server.sh [single|multi] [trace|debug]
#   - single: 启动单网关模式（默认，仅启动一个 access-gateway 实例）
#   - multi:  启动多网关模式（启动多个 access-gateway 实例）
#   - trace|debug: 第二参数，全量跟踪（RUST_LOG=trace；debug 为历史别名，仅排障）
#   - 默认: 未设置环境变量 RUST_LOG 时使用「业务 debug + 第三方降噪」，避免 logs/ 暴涨

set -e

# 本地基础设施（Consul/Redis/Postgres 等）必须直连，避免系统 HTTP 代理拦截 localhost。
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

# 解析 Cargo 实际产物目录：上层仓库 `.cargo/config.toml` 可能将 `target-dir` 指到仓库根的 `target/`，
# 与 `flare-im-core/target` 不一致；启动与停止脚本必须使用与 `cargo build` 相同的 debug 目录。
resolve_cargo_target_debug() {
    if [ -n "${CARGO_TARGET_DIR:-}" ]; then
        printf '%s/debug\n' "${CARGO_TARGET_DIR%/}"
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
            printf '%s/debug\n' "$td"
            return 0
        fi
    fi
    printf '%s/target/debug\n' "$PROJECT_ROOT"
}
CARGO_TARGET_DEBUG="$(resolve_cargo_target_debug)"

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

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Flare IM Core 完整服务启动脚本${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "${YELLOW}📁 日志目录: $LOGS_DIR${NC}"
echo -e "${YELLOW}🚪 网关模式: $GATEWAY_MODE${NC}"
if [ -n "$VERBOSE_LOG_MODE" ]; then
    echo -e "${GREEN}🔧 日志: RUST_LOG=$RUST_LOG${NC}"
else
    echo -e "${BLUE}   日志: RUST_LOG=$RUST_LOG${NC}"
fi
echo ""

# 检查基础设施服务
echo -e "${YELLOW}📦 检查基础设施服务状态...${NC}"
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

check_service "Redis" "26379"
check_service "PostgreSQL" "25432"
case "$MQ_BACKEND" in
    kafka)
        echo -e "${YELLOW}   ↷ Kafka 端口检查已跳过（由服务启动时按配置连接）${NC}"
        ;;
    nats|jetstream)
        check_service "NATS JetStream" "24222"
        ;;
    *)
        echo -e "${RED}   ✗ 未支持的 MQ 后端: $MQ_BACKEND (期望 nats|jetstream|kafka)${NC}"
        ;;
esac
check_service "Consul" "28500"

echo ""
echo -e "${YELLOW}💡 提示: 如需启动基础设施服务，请运行:${NC}"
echo "   ${BLUE}cd deploy && docker-compose up -d${NC}"
echo ""

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
    # 额外检查：通过进程名查找并停止
    pkill -f "target/debug/flare-$service" 2>/dev/null || true
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

if ! PROTOC="$PROTOC" cargo build --all > /dev/null 2>&1; then
    echo -e "${RED}   ✗ 编译失败，请检查错误信息${NC}"
    echo -e "${YELLOW}   运行 'PROTOC=$PROTOC cargo build --all' 查看详细错误${NC}"
    exit 1
fi
echo -e "${GREEN}   ✓ 编译完成${NC}"
echo ""

# 启动核心服务
echo -e "${GREEN}🚀 启动 Flare IM Core 核心服务...${NC}"

# 定义服务启动顺序（按照依赖关系排序）
# 1. 基础服务：signaling-online（在线状态服务）、signaling-route（路由目录服务）
# 2. 能力服务：capability（服务注册名 flare-capability，见 flare_im_core::service_names::CAPABILITY；包与二进制名均为 flare-capability）
# 3. 会话服务：conversation（会话管理服务）
# 4. 消息编排：message-orchestrator（消息编排服务）
# 5. 存储服务：storage-writer（消息持久化）、storage-reader（消息查询）
# 6. 同步编排：sync-orchestrator（统一 SyncService 入口）
# 7. 推送服务：push-server（推送服务）、push-worker（推送工作器）
# 8. 媒资服务：media（媒资服务）
# 9. 核心网关：core-gateway（业务系统统一入口）
# 10. 接入网关：signaling-gateway (Signaling Gateway 服务，位于 flare-signaling/gateway，通过单/多网关模式启动，见下方启动部分)

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
        "message-orchestrator")
            PACKAGE="flare-orchestrator"
            BIN_NAME="flare-orchestrator"
            # RTC bridge 懒连接 capability，不阻塞启动；媒体后端需单独启动 strom-sfu。
            export MESSAGE_ORCHESTRATOR_CAPABILITY_RTC_BRIDGE="${MESSAGE_ORCHESTRATOR_CAPABILITY_RTC_BRIDGE:-1}"
            export MESSAGE_ORCHESTRATOR_CAPABILITY_GRPC_URI="${MESSAGE_ORCHESTRATOR_CAPABILITY_GRPC_URI:-http://127.0.0.1:50110}"
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
            ENV_VARS=""
            ;;
        "media")
            PACKAGE="flare-media"
            BIN_NAME="flare-media"
            ENV_VARS=""
            ;;
        "core-gateway")
            PACKAGE="flare-core-gateway"
            BIN_NAME="flare-core-gateway"
            ENV_VARS=""
            ;;
        *)
            echo -e "${RED}   ✗ 未知服务: $service${NC}"
            continue
            ;;
    esac
    
    # 设置 PID 文件路径
    pid_file="$LOGS_DIR/flare-$service.pid"
    
    # 启动服务（使用编译好的二进制，避免并发编译问题）
    # 子进程继承当前 shell 的 RUST_LOG（默认经上方设为 debug+ 降噪，或 trace 排障模式）
    "$CARGO_TARGET_DEBUG/$BIN_NAME" > "$LOGS_DIR/flare-$service.log" 2>&1 &
    
    # 清理环境变量
    if [ "$service" = "signaling-online" ]; then
        unset SIGNALING_ONLINE_SERVICE_HOST SIGNALING_ONLINE_SERVICE_PORT ONLINE_SERVICE_ENDPOINT
    elif [ "$service" = "signaling-route" ]; then
        unset SIGNALING_ROUTE_SERVICE_HOST SIGNALING_ROUTE_SERVICE_PORT ROUTE_SERVICE_ENDPOINT
    elif [ "$service" = "message-orchestrator" ]; then
        unset MESSAGE_ORCHESTRATOR_CAPABILITY_RTC_BRIDGE MESSAGE_ORCHESTRATOR_CAPABILITY_GRPC_URI
    elif [ "$service" = "sync-orchestrator" ]; then
        unset SYNC_ORCHESTRATOR_HOST SYNC_ORCHESTRATOR_PORT
    fi
    service_pid=$!
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
    
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if ps -p "$pid" > /dev/null 2>&1; then
            echo -e "${GREEN}   ✓ $service 正在运行 (PID: $pid)${NC}"
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
for service in "${CORE_SERVICES[@]}"; do
    check_process "$service"
done

echo ""
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
    
    # 启动 Access Gateway（使用编译好的二进制，已在前面统一编译）
    PORT="$DEFAULT_WS_PORT" \
    GRPC_PORT="$DEFAULT_GRPC_PORT" \
    "$CARGO_TARGET_DEBUG/flare-signaling-gateway" > "$LOGS_DIR/flare-access-gateway.log" 2>&1 &
    
    gateway_pid=$!
    echo $gateway_pid > "$pid_file"
    sleep 3
    
    # 检查是否启动成功
    if ps -p $(cat "$pid_file") > /dev/null 2>&1; then
        echo -e "${GREEN}      ✓ access-gateway 启动成功 (PID: $(cat $pid_file))${NC}"
    else
        echo -e "${RED}      ✗ access-gateway 启动失败${NC}"
        echo -e "${YELLOW}     查看日志: tail -f $LOGS_DIR/flare-access-gateway.log${NC}"
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
        
        # 启动服务（使用编译好的二进制，已在前面统一编译）
        eval "GATEWAY_${gateway_key_upper}_GRPC_PORT=$grpc_port"
        GATEWAY_ID="$gateway_id" \
        GATEWAY_REGION="$region" \
        PORT="$ws_port" \
        GRPC_PORT="$grpc_port" \
        "$CARGO_TARGET_DEBUG/flare-signaling-gateway" > "$LOGS_DIR/flare-access-gateway-$gateway_key.log" 2>&1 &
        
        gateway_pid=$!
        echo $gateway_pid > "$pid_file"
        sleep 3
        
        # 检查是否启动成功
        if ps -p $(cat "$pid_file") > /dev/null 2>&1; then
            echo -e "${GREEN}      ✓ $gateway_key 启动成功 (PID: $(cat $pid_file))${NC}"
        else
            echo -e "${RED}      ✗ $gateway_key 启动失败${NC}"
            echo -e "${YELLOW}     查看日志: tail -f $LOGS_DIR/flare-access-gateway-$gateway_key.log${NC}"
        fi
        
        echo ""
    done
fi

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}✅ Flare IM Core 所有服务启动完成！${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# 等待服务完全启动
echo -e "${YELLOW}⏳ 等待服务完全启动并检查状态...${NC}"
sleep 5

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
echo "   ${RED}./scripts/stop_service.sh${NC}"
echo ""

# 如果检查失败，返回非零退出码
if [ $CHECK_RESULT -ne 0 ]; then
    exit 1
fi
