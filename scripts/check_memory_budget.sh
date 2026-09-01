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
# 默认值必须与实际部署一致，否则门禁校验的是一个没人用的数字。
# 900mb / 1536m = 59%，给 BGSAVE / AOF-rewrite 的 COW 留出余量。
r_max="${REDIS_MAXMEMORY:-900mb}"
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

# ── 2b. 运行态核对：配置文件写了什么不算数，运行中的 Redis 是什么才算 ──
#
# 线上事故（2026-09-01）就漏在这里：override 里早就写成 1000mb，但**容器一直没重建**，
# 运行中的 Redis 仍是 1200mb/1536m=78%。上面第 2 节读的是环境变量里的**意图**，
# 因此一路绿灯，而 redis-server 那天被 cgroup OOM 杀掉、重启加载 RDB 7 秒，
# 期间全服务 BusyLoadingError，用户表现是发消息失败。
if command -v docker >/dev/null 2>&1 && docker inspect flare-redis >/dev/null 2>&1; then
    run_max=$(docker exec flare-redis redis-cli CONFIG GET maxmemory 2>/dev/null | tail -1 | tr -d '\r')
    run_lim=$(docker inspect flare-redis --format '{{.HostConfig.Memory}}' 2>/dev/null)
    if [ -n "$run_max" ] && [ -n "$run_lim" ] && [ "$run_max" -gt 0 ] 2>/dev/null && [ "$run_lim" -gt 0 ] 2>/dev/null; then
        run_pct=$(( run_max * 100 / run_lim ))
        if [ "$run_pct" -gt 70 ]; then
            bad "运行中的 Redis maxmemory 占容器上限 ${run_pct}%，超过 70%（配置文件写的是 ${r_max}——容器很可能没重建）"
        else
            ok "运行中的 Redis maxmemory 占容器上限 ${run_pct}%"
        fi
        # 配置漂移：文件与运行态不一致，说明改了配置但没生效
        want_mb=$(to_mb "$r_max"); run_mb=$(( run_max / 1024 / 1024 ))
        if [ "$want_mb" -gt 0 ] && [ "$run_mb" -ne "$want_mb" ]; then
            bad "配置漂移：文件写 ${want_mb}MB，运行中却是 ${run_mb}MB —— 改完配置必须重建容器"
        fi
    else
        note "无法读取运行中的 Redis 配置，跳过运行态核对"
    fi

    # 周期性 RDB 快照 + 未开 AOF：每次 save 都 fork 一个与数据集等量的进程，
    # 写入期间 COW 页累积会把 cgroup 顶爆（线上就是 save 60 10000 每约 75 秒一次）。
    run_save=$(docker exec flare-redis redis-cli CONFIG GET save 2>/dev/null | tail -1 | tr -d '\r')
    run_aof=$(docker exec flare-redis redis-cli CONFIG GET appendonly 2>/dev/null | tail -1 | tr -d '\r')
    if [ -n "$run_save" ] && echo "$run_save" | grep -qE '(^| )60 '; then
        bad "Redis 开着 60 秒级 RDB 快照（save='${run_save}'）：每约 75 秒 fork 一个约等于数据集大小的进程，COW 会顶爆 cgroup"
    elif [ "$run_aof" = "no" ] && [ -n "$run_save" ]; then
        note "Redis 未开 AOF 且有周期快照（save='${run_save}'）：确认 fork 频率与 COW 余量"
    else
        ok "Redis 持久化：appendonly=${run_aof:-?} save='${run_save}'"
    fi
fi

# ── 2c. vm.overcommit_memory ──
# Redis 启动时会自己警告：没开 overcommit，后台保存或复制在内存吃紧时可能失败。
if [ -r /proc/sys/vm/overcommit_memory ]; then
    oc=$(cat /proc/sys/vm/overcommit_memory)
    if [ "$oc" != "1" ]; then
        bad "vm.overcommit_memory=${oc}，应为 1：否则 Redis 的 BGSAVE/AOF-rewrite fork 可能失败或被杀"
    else
        ok "vm.overcommit_memory=1"
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
