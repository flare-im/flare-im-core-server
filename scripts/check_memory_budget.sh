#!/usr/bin/env bash
# 内存预算守卫：部署前跑，不满足就拒绝启动。
#
# 挡两类事故（都真实发生过）：
#  1. 进程自身预算 ≥ 容器上限。NATS 的 NATS_MAX_MEM_STORE 被调到 2G 而容器
#     还是 384m，nats-server 被 cgroup OOM 杀了 34 次。Redis 同理
#     （maxmemory 必须低于 mem_limit，RSS 会高于 maxmemory）。
#  2. 所有容器上限之和 > 物理内存。此时"每个容器都有上限"依然挡不住全局 OOM
#     ——push-server 无界扇出涨到 13.4GB 触发 CONSTRAINT_NONE，把 NATS 和
#     Redis 一起拖下水。
#
# 用法: check_memory_budget.sh [compose 文件...]
set -uo pipefail

fail=0
note() { printf '  %s\n' "$1"; }
bad()  { printf '✗ %s\n' "$1"; fail=1; }
ok()   { printf '✓ %s\n' "$1"; }

# 用 tr 而不是 ${v,,}：后者要 bash 4+，macOS 自带的还是 3.2，
# 在那里会 bad substitution 然后静默返回空值。
to_mb() {
    local v num unit
    v=$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | tr -d ' ')
    num=$(printf '%s' "$v" | sed 's/[^0-9].*$//')
    unit=$(printf '%s' "$v" | sed 's/^[0-9]*//')
    [ -z "$num" ] && { echo 0; return; }
    case "$unit" in
        g|gb) echo $(( num * 1024 )) ;;
        m|mb) echo "$num" ;;
        k|kb) echo $(( num / 1024 )) ;;
        "")   echo $(( num / 1024 / 1024 )) ;;   # 裸数字按字节
        *)    echo 0 ;;
    esac
}

# ── 1. NATS：内存存储配额 vs 容器上限 ──
nats_store="${NATS_MAX_MEM_STORE:-256MB}"
nats_limit="${NATS_MEM_LIMIT:-1g}"
s=$(to_mb "$nats_store"); l=$(to_mb "$nats_limit")
if [ "$s" -le 0 ] || [ "$l" -le 0 ]; then
    bad "NATS 内存配置解析失败（store='${nats_store}' limit='${nats_limit}'）"
else
    if [ "$s" -ge "$l" ]; then
        bad "NATS_MAX_MEM_STORE=${nats_store} ≥ 容器上限 ${nats_limit}：JetStream 会按配额去涨，必然被 cgroup OOM"
    elif [ $(( s * 100 / l )) -gt 60 ]; then
        bad "NATS_MAX_MEM_STORE=${nats_store} 占容器上限 ${nats_limit} 的 $(( s * 100 / l ))%，余量不足 40%"
        note "连接缓冲与 file-store 索引不计入该配额，贴太近仍会 OOM"
    else
        ok "NATS 内存配额 ${nats_store} / 上限 ${nats_limit}（占 $(( s * 100 / l ))%）"
    fi
fi

# ── 2. Redis：maxmemory vs 容器上限 ──
r_max="${REDIS_MAXMEMORY:-1200mb}"
r_lim="${REDIS_MEM_LIMIT:-1536m}"
s=$(to_mb "$r_max"); l=$(to_mb "$r_lim")
if [ "$s" -le 0 ] || [ "$l" -le 0 ]; then
    bad "Redis 内存配置解析失败（maxmemory='${r_max}' limit='${r_lim}'）"
else
    if [ "$s" -ge "$l" ]; then
        bad "REDIS_MAXMEMORY=${r_max} ≥ 容器上限 ${r_lim}：Redis 的 RSS 高于 maxmemory，必被 OOM"
    elif [ $(( s * 100 / l )) -gt 70 ]; then
        # 阈值定在 70% 是有来由的：线上 maxmemory 1200mb / 容器 1536m（78%）
        # 这一档被 cgroup OOM 杀过。实测碎片率 1.12，RSS 显著高于 used_memory，
        # 再加复制缓冲与客户端输出缓冲，78% 的余量不够。
        bad "REDIS_MAXMEMORY=${r_max} 占上限 ${r_lim} 的 $(( s * 100 / l ))%，超过 70%——碎片与输出缓冲会顶穿（78% 那档已被 OOM 杀过）"
    else
        ok "Redis maxmemory ${r_max} / 上限 ${r_lim}（占 $(( s * 100 / l ))%）"
    fi
fi

# ── 3. 运行中容器：上限之和 vs 物理内存 ──
if command -v docker >/dev/null 2>&1 && docker ps -q >/dev/null 2>&1; then
    unlimited=$(docker ps --format '{{.Names}}' | while read -r c; do
        [ "$(docker inspect -f '{{.HostConfig.Memory}}' "$c" 2>/dev/null)" = "0" ] && echo "$c"
    done)
    if [ -n "$unlimited" ]; then
        bad "以下容器没有内存上限，任何一个失控都会触发全局 OOM 拖垮全机："
        echo "$unlimited" | sed 's/^/    /'
    else
        ok "所有运行中容器都设了内存上限"
    fi

    total_mb=$(docker ps --format '{{.Names}}' | while read -r c; do
        docker inspect -f '{{.HostConfig.Memory}}' "$c" 2>/dev/null
    done | awk '{s+=$1} END {print int(s/1024/1024)}')
    phys_mb=$(free -m 2>/dev/null | awk '/^Mem:/{print $2}')
    if [ -n "$phys_mb" ] && [ "$total_mb" -gt 0 ]; then
        if [ "$total_mb" -gt "$phys_mb" ]; then
            bad "容器上限合计 ${total_mb}MB > 物理内存 ${phys_mb}MB：仍可能全局 OOM"
        elif [ $(( total_mb * 100 / phys_mb )) -gt 92 ]; then
            bad "容器上限合计 ${total_mb}MB 占物理内存 ${phys_mb}MB 的 $(( total_mb * 100 / phys_mb ))%，宿主余量不足"
        else
            ok "容器上限合计 ${total_mb}MB / 物理 ${phys_mb}MB（占 $(( total_mb * 100 / phys_mb ))%）"
        fi
    fi
fi

[ "$fail" = 0 ] && echo "内存预算检查通过" || echo "内存预算检查未通过——修正后再启动"
exit "$fail"
