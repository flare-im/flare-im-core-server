#!/bin/bash
# 检查所有服务是否正常运行（进程状态和端口监听）

set +e  # 不因为单个服务检查失败而退出

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOGS_DIR="$PROJECT_ROOT/logs"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  检查 Flare IM Core 服务状态${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# 检查基础设施服务
echo -e "${YELLOW}📦 检查基础设施服务...${NC}"
check_infra_service() {
    local service=$1
    local port=$2
    
    if command -v nc >/dev/null 2>&1 && nc -z localhost "$port" 2>/dev/null; then
        echo -e "${GREEN}   ✓ $service 已就绪 (端口 $port)${NC}"
        return 0
    elif lsof -i :"$port" >/dev/null 2>&1; then
        echo -e "${GREEN}   ✓ $service 已就绪 (端口 $port)${NC}"
        return 0
    else
        echo -e "${RED}   ✗ $service 未运行 (端口 $port)${NC}"
        return 1
    fi
}

check_infra_service "Redis" "26379"
check_infra_service "PostgreSQL" "25432"
check_infra_service "Kafka" "29092"
check_infra_service "Consul" "28500"

echo ""

# 定义所有服务及其端口（格式：服务名:端口号，空表示仅检查进程）
# 注意：没有端口的服务（如 storage-writer, push-server, push-worker）仅检查进程
SERVICES=(
    "signaling-online:50061"
    "signaling-route:50062"
    "capability:"  # gRPC 端口见应用配置；此处仅检查 pid 进程
    "conversation:50090"
    "message-orchestrator:50181"
    "storage-writer:"  # 无端口（Kafka 消费者）
    "storage-reader:60083"
    "sync-orchestrator:60084"
    "push-server:"  # 无端口（Kafka 消费者）
    "push-worker:"  # 无端口（Kafka 消费者）
    "media:60081"
    "core-gateway:50050"
)

# Access Gateway 不在此脚本中检查

# 脚本内服务 key → 对外展示名（与 cargo 包/二进制一致）
service_display_name() {
    case "$1" in
        capability) echo "flare-capability" ;;
        *) echo "$1" ;;
    esac
}

# 检查服务函数
check_service() {
    local service=$1
    local port=$2
    local label
    label="$(service_display_name "$service")"
    local pid_file="$LOGS_DIR/flare-$service.pid"
    
    local process_ok=false
    local port_ok=false
    local pid=""
    
    # 检查进程
    if [ -f "$pid_file" ]; then
        pid=$(cat "$pid_file" 2>/dev/null)
        if [ -n "$pid" ] && ps -p "$pid" > /dev/null 2>&1; then
            process_ok=true
        fi
    fi
    
    # 如果没有 PID 文件或进程不存在，尝试通过二进制名查找（capability 对应 flare-capability）
    if [ "$process_ok" = false ]; then
        if [ "$service" = "capability" ]; then
            if pgrep -f "target/debug/flare-capability" > /dev/null 2>&1 || \
               pgrep -f "target/release/flare-capability" > /dev/null 2>&1 || \
               pgrep -f "cargo.*flare-capability" > /dev/null 2>&1; then
                process_ok=true
            fi
        else
            if pgrep -f "target/debug/flare-$service" > /dev/null 2>&1 || \
               pgrep -f "target/release/flare-$service" > /dev/null 2>&1 || \
               pgrep -f "cargo.*flare-$service" > /dev/null 2>&1; then
                process_ok=true
            fi
        fi
    fi
    
    # 检查端口（仅当配置了具体端口时参与「端口异常」判断）
    if [ -n "$port" ] && [ "$port" != "" ]; then
        if command -v nc >/dev/null 2>&1 && nc -z localhost "$port" 2>/dev/null; then
            port_ok=true
        elif lsof -i :"$port" >/dev/null 2>&1; then
            port_ok=true
        fi
    else
        port_ok=false
    fi
    
    # 输出检查结果
    if [ "$process_ok" = true ] && { [ -z "$port" ] || [ "$port_ok" = true ]; }; then
        if [ -n "$port" ] && [ "$port" != "" ]; then
            echo -e "${GREEN}   ✓ $label (PID: $pid, 端口: $port)${NC}"
        else
            echo -e "${GREEN}   ✓ $label (PID: $pid)${NC}"
        fi
        return 0
    elif [ "$process_ok" = true ] && [ -n "$port" ] && [ "$port_ok" = false ]; then
        echo -e "${YELLOW}   ⚠ $label 进程运行中但端口 $port 未监听${NC}"
        return 1
    elif [ "$process_ok" = false ] && [ -n "$port" ] && [ "$port_ok" = true ]; then
        echo -e "${YELLOW}   ⚠ $label 端口 $port 监听中但进程不存在${NC}"
        return 1
    else
        echo -e "${RED}   ✗ $label 未运行${NC}"
        return 1
    fi
}

# 检查核心服务
echo -e "${YELLOW}🚀 检查 Flare IM Core 核心服务...${NC}"
ALL_RUNNING=true
FAILED_SERVICES=()

for service_port in "${SERVICES[@]}"; do
    IFS=':' read -r service port <<< "$service_port"
    if ! check_service "$service" "$port"; then
        ALL_RUNNING=false
        FAILED_SERVICES+=("$service")
    fi
done

echo ""

# 汇总结果
if [ "$ALL_RUNNING" = true ]; then
    echo -e "${GREEN}✅ 所有服务运行正常${NC}"
    exit 0
else
    echo -e "${YELLOW}⚠️  部分服务未正常运行${NC}"
    if [ ${#FAILED_SERVICES[@]} -gt 0 ]; then
        failed_labels=()
        for s in "${FAILED_SERVICES[@]}"; do
            failed_labels+=("$(service_display_name "$s")")
        done
        echo -e "${YELLOW}   未正常运行的服务: ${failed_labels[*]}${NC}"
        echo -e "${YELLOW}   提示: 查看日志了解详情${NC}"
        for service in "${FAILED_SERVICES[@]}"; do
            echo -e "     ${BLUE}tail -f $LOGS_DIR/flare-$service.log${NC}"
        done
    fi
    exit 1
fi
