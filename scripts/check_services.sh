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
    "hook-engine:"  # 无端口配置（可能是 Kafka 消费者或其他）
    "conversation:50090"
    "message-orchestrator:50081"
    "storage-writer:"  # 无端口（Kafka 消费者）
    "storage-reader:60083"
    "sync-orchestrator:60084"
    "push-server:"  # 无端口（Kafka 消费者）
    "push-worker:"  # 无端口（Kafka 消费者）
    "media:60081"
    "core-gateway:50050"
)

# Access Gateway 不在此脚本中检查

# 检查服务函数
check_service() {
    local service=$1
    local port=$2
    local pid_file="$LOGS_DIR/flare-$service.pid"
    
    local process_ok=false
    local port_ok=false
    
    # 检查进程
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file" 2>/dev/null)
        if [ -n "$pid" ] && ps -p "$pid" > /dev/null 2>&1; then
            process_ok=true
        fi
    fi
    
    # 如果没有 PID 文件或进程不存在，尝试通过进程名查找
    if [ "$process_ok" = false ]; then
        if pgrep -f "target/debug/flare-$service" > /dev/null 2>&1 || \
           pgrep -f "cargo.*flare-$service" > /dev/null 2>&1; then
            process_ok=true
        fi
    fi
    
    # 检查端口（如果配置了端口）
    if [ -n "$port" ] && [ "$port" != "" ]; then
        if command -v nc >/dev/null 2>&1 && nc -z localhost "$port" 2>/dev/null; then
            port_ok=true
        elif lsof -i :"$port" >/dev/null 2>&1; then
            port_ok=true
        fi
    else
        # 没有端口配置的服务，只需要检查进程
        port_ok=true
    fi
    
    # 输出检查结果
    if [ "$process_ok" = true ] && [ "$port_ok" = true ]; then
        if [ -n "$port" ] && [ "$port" != "" ]; then
            echo -e "${GREEN}   ✓ $service (PID: $pid, 端口: $port)${NC}"
        else
            echo -e "${GREEN}   ✓ $service (PID: $pid)${NC}"
        fi
        return 0
    elif [ "$process_ok" = true ] && [ "$port_ok" = false ]; then
        echo -e "${YELLOW}   ⚠ $service 进程运行中但端口 $port 未监听${NC}"
        return 1
    elif [ "$process_ok" = false ] && [ "$port_ok" = true ]; then
        echo -e "${YELLOW}   ⚠ $service 端口 $port 监听中但进程不存在${NC}"
        return 1
    else
        echo -e "${RED}   ✗ $service 未运行${NC}"
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
        echo -e "${YELLOW}   未正常运行的服务: ${FAILED_SERVICES[*]}${NC}"
        echo -e "${YELLOW}   提示: 查看日志了解详情${NC}"
        for service in "${FAILED_SERVICES[@]}"; do
            echo -e "     ${BLUE}tail -f $LOGS_DIR/flare-$service.log${NC}"
        done
    fi
    exit 1
fi
