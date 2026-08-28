#!/usr/bin/env bash
# 运行时指标验证：跑一遍真实流量，确认关键指标**真的有样本**。
#
# 静态门禁（check_dead_metrics.sh）只能保证"有代码写它"，保证不了"那条路径会被走到"。
# 本仓真栽过：把推送埋点加在 push_encoded_payload_to_user 上，而统一读扩散上线后
# 实时投递走的是会话级订阅广播（deliver_to_conversation），埋点代码在、路径不通，
# 指标一直是空的。只有拿真实流量对指标，才能发现这种"代码在但没被执行"。
#
# 用法：在**部署好的服务器上**执行，先自行制造一些消息流量再跑。
#
# ⚠️ 指标名必须与 /metrics 里的实际输出逐字一致。本仓的网关指标命名**不统一**：
# push_latency_seconds 带 access_gateway_ 前缀，push_success_total 不带。
# 我第一版按前缀猜名字，于是门禁对着一个不存在的指标报错——
# 加指标时先 `curl /metrics | grep '^# TYPE'` 看真名，别照着字段名推。
set -uo pipefail

FAILED=0
check() {  # check <容器> <端口> <指标名> <说明>
  local c="$1" port="$2" metric="$3" desc="$4"
  local n
  n=$(docker exec "$c" curl -s "http://127.0.0.1:$port/metrics" 2>/dev/null \
      | grep -c "^${metric}" || true)
  if [ "${n:-0}" -gt 0 ]; then
    echo "  ✓ $desc（$metric）"
  else
    echo "  ✗ $desc（$metric）没有任何样本——埋点代码可能在一条走不到的路径上" >&2
    FAILED=1
  fi
}

# 端口从各服务自己的启动日志里取，别写死
port_of() {
  docker logs "$1" 2>&1 | grep -oE 'metrics endpoint listening address=0\.0\.0\.0:[0-9]+' \
    | tail -1 | grep -oE '[0-9]+$'
}

GW=flare-im-core-docker-signaling-gateway-1
OR=flare-im-core-docker-orchestrator-1

check "$OR" "$(port_of $OR)" "message_orchestrator_fanout_latency_seconds_count" "扇出耗时"
check "$GW" "$(port_of $GW)" "push_success_total" "推送成功数"
check "$GW" "$(port_of $GW)" "access_gateway_push_latency_seconds_count" "推送耗时"

if [ "$FAILED" = 1 ]; then
  echo "" >&2
  echo "指标为空有两种可能：确实没有流量，或**埋点在一条不会被走到的路径上**。" >&2
  echo "先确认刚才真的发过消息，再去核对埋点位置。" >&2
  exit 1
fi
echo "✓ 关键指标都有真实样本"
