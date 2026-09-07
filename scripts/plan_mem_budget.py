#!/usr/bin/env python3
"""按服务器物理内存/核数自动计算最优容器内存预算 + PostgreSQL 内存参数。

与 check_memory_budget.sh 配套:那个**校验**现状(合计≤物理、进程预算<容器上限、
maxmemory<mem_limit),这个**计算**该给每个容器分多少——一台新机器或改配置时直接照它落。

设计原则(全部来自本仓真实事故,见 memory):
  1. 三铁律:∑mem_limit ≤ 物理×0.92;maxmemory < 容器上限(Dragonfly RSS 高于 maxmemory);
     进程内配额(NATS_MAX_MEM_STORE)显著低于容器上限。违反任一都被 cgroup 全局 OOM。
  2. PostgreSQL 是弹性大头:拿走"其余都分完后"的剩余预算,盘缓存越大命中越高。
     shared_buffers=PG 上限×0.25(封顶 8G),effective_cache_size≈(PG 上限+宿主余量)×0.75。
  3. Dragonfly 内存轻(实测闲置 36MB、10 万群扇出峰值 ~1.3GB),按峰值+余量缩放,不要
     像旧配置给 8G 上限只用 36MB——mem_limit 是上限不是预留,但会挤占"合计≤物理"的额度。
  4. push-server 曾无界扇出涨到 13.4GB 触发全局 OOM,必须硬顶;网关/编排类按角色定额。
  5. 每服务预算按角色,不按机器缩放(扇出/连接缓冲驱动,与总内存无关);只有 PG 与
     Dragonfly 两个"数据驻留"角色随机器缩放。

用法:
  plan_mem_budget.py                      # 读本机 free/nproc,打印最优方案
  plan_mem_budget.py --phys-mb 32768 --nproc 16   # 规划一台目标机器
  plan_mem_budget.py --emit-override      # 额外打印 compose override 片段 + PG -c 参数
仅依赖标准库。
"""
import argparse
import os
import subprocess
import sys

# 角色定额(MB):随机器缩放的只有 PG(弹性剩余)与 KV(按峰值缩放);其余固定。
# 值来自实测峰值 + 安全余量(见 flare-oom-cascade-and-memory-budget / push-offline-envelope)。
FIXED_INFRA = {
    "consul": 256,    # 实测 125MB
    "rustfs": 768,    # 实测 235MB,对象操作留头
    "nats": 1024,     # 实测 84MB,JetStream 内存流;NATS_MAX_MEM_STORE=上限×0.4
}
# 业务服务(svc-*):扇出/连接缓冲驱动,定额不随机器缩放。
APP_SERVICES = {
    "admin-gateway": 256,
    "api-gateway": 256,
    "capability": 256,
    "conversation": 512,     # 未读/会话读写
    "media": 256,
    "message-ingest": 512,   # 发送主链 + seq floor 缓存
    "orchestrator": 512,     # 实测峰值 260MB,256m 会 OOM
    "push-server": 1024,     # 曾 13.4GB 无界扇出,硬顶
    "push-worker": 512,
    "signaling-gateway": 512,
    "signaling-online": 256,
    "signaling-route": 256,
    "storage-reader": 384,
    "storage-writer": 384,
    "sync-orchestrator": 384,
}

PG_SHARED_BUFFERS_CAP_MB = 8192   # 超过 8G 收益递减(PG 官方经验)
KV_MIN_MB, KV_MAX_MB = 2048, 4096  # Dragonfly:峰值 1.3GB → 下限 2G 留余量,上限封顶
HOST_RESERVE_FRAC = 0.08
HOST_RESERVE_MIN_MB = 1024
BUDGET_CEIL_FRAC = 0.92            # ∑mem_limit 不得超过物理的 92%


def detect_phys_mb() -> int:
    try:
        out = subprocess.run(["free", "-m"], capture_output=True, text=True, timeout=10).stdout
        for line in out.splitlines():
            if line.lower().startswith("mem:"):
                return int(line.split()[1])
    except Exception:
        pass
    # 退化:/proc/meminfo
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) // 1024
    except Exception:
        pass
    sys.exit("无法探测物理内存,请用 --phys-mb 指定")


def detect_nproc() -> int:
    try:
        return os.cpu_count() or 4
    except Exception:
        return 4


def plan(phys_mb: int, nproc: int, max_connections: int):
    host_reserve = max(HOST_RESERVE_MIN_MB, int(phys_mb * HOST_RESERVE_FRAC))
    usable = phys_mb - host_reserve

    fixed_total = sum(FIXED_INFRA.values()) + sum(APP_SERVICES.values())

    # KV(Dragonfly):按机器缩放但夹在 [2G, 4G];maxmemory = 上限×0.8。
    kv_limit = max(KV_MIN_MB, min(KV_MAX_MB, int(phys_mb * 0.15)))
    kv_maxmemory = int(kv_limit * 0.8)

    # PostgreSQL:拿剩余全部弹性预算。
    pg_limit = usable - fixed_total - kv_limit

    # 若剩余不足,PG 兜底 1G,并提示机器偏小。
    warnings = []
    if pg_limit < 1024:
        warnings.append(
            f"物理内存偏小:PG 仅分到 {pg_limit}MB(<1G)。已兜底 1024MB,"
            f"但 ∑ 可能超 92%;建议加内存或下调 KV/业务定额。"
        )
        pg_limit = 1024

    total = fixed_total + kv_limit + pg_limit
    ceil_mb = int(phys_mb * BUDGET_CEIL_FRAC)
    if total > ceil_mb:
        # 超顶:优先从 PG 收(它最弹性),保证铁律 ∑≤物理×0.92。
        over = total - ceil_mb
        pg_limit = max(1024, pg_limit - over)
        total = fixed_total + kv_limit + pg_limit
        warnings.append(f"∑ 超 92% 上限,已从 PG 回收 {over}MB → PG={pg_limit}MB。")

    # PG 内存参数(由 PG 上限推导)。
    shared_buffers = min(PG_SHARED_BUFFERS_CAP_MB, int(pg_limit * 0.25))
    # effective_cache_size 是规划器提示(不占 RAM):PG 上限 + 宿主页缓存可用量的估计。
    effective_cache = int((pg_limit + host_reserve * 0.5) * 0.75) + shared_buffers
    maintenance_work_mem = min(512, max(64, int(pg_limit * 0.05)))
    # work_mem 会被每连接的每个排序/哈希节点各用一份:上限=(PG上限-shared_buffers)/最大连接/并发节点估计。
    work_mem = max(4, min(32, (pg_limit - shared_buffers) // max_connections // 2))
    wal_buffers = 32

    return {
        "phys_mb": phys_mb, "nproc": nproc, "host_reserve": host_reserve,
        "kv_limit": kv_limit, "kv_maxmemory": kv_maxmemory,
        "pg_limit": pg_limit, "fixed_total": fixed_total, "total": total,
        "ceil_mb": ceil_mb, "pct": round(total * 100 / phys_mb, 1),
        "pg": {
            "shared_buffers": shared_buffers, "effective_cache_size": effective_cache,
            "maintenance_work_mem": maintenance_work_mem, "work_mem": work_mem,
            "wal_buffers": wal_buffers, "max_connections": max_connections,
        },
        "nats_max_mem_store": int(FIXED_INFRA["nats"] * 0.4),
        "warnings": warnings,
    }


def human(mb: int) -> str:
    return f"{mb/1024:.1f}g" if mb >= 1024 else f"{mb}m"


def print_plan(p):
    print(f"=== 最优内存预算(物理 {p['phys_mb']}MB / {p['nproc']} 核)===")
    print(f"宿主预留: {p['host_reserve']}MB  |  ∑mem_limit: {p['total']}MB "
          f"({p['pct']}% 物理, 上限 {p['ceil_mb']}MB)  "
          f"{'✓' if p['total'] <= p['ceil_mb'] else '✗ 超顶'}")
    print(f"\n  PostgreSQL   mem_limit={human(p['pg_limit'])}  (弹性大头)")
    pg = p["pg"]
    print(f"    shared_buffers={pg['shared_buffers']}MB  effective_cache_size={pg['effective_cache_size']}MB")
    print(f"    work_mem={pg['work_mem']}MB  maintenance_work_mem={pg['maintenance_work_mem']}MB  "
          f"wal_buffers={pg['wal_buffers']}MB  max_connections={pg['max_connections']}")
    print(f"  Dragonfly    mem_limit={human(p['kv_limit'])}  maxmemory={human(p['kv_maxmemory'])}  "
          f"({'✓ maxmemory<上限' if p['kv_maxmemory'] < p['kv_limit'] else '✗'})")
    for k, v in FIXED_INFRA.items():
        extra = f"  NATS_MAX_MEM_STORE={p['nats_max_mem_store']}MB" if k == "nats" else ""
        print(f"  {k:<12} mem_limit={human(v)}{extra}")
    print(f"  业务服务合计 {sum(APP_SERVICES.values())}MB / {len(APP_SERVICES)} 个")
    for w in p["warnings"]:
        print(f"  ⚠ {w}")


def emit_override(p):
    pg = p["pg"]
    print("\n=== compose override 片段 ===")
    print("services:")
    print(f"  redis:\n    mem_limit: {human(p['kv_limit'])}")
    print(f"    command: [\"--logtostderr\", \"--maxmemory={human(p['kv_maxmemory'])}\", "
          f"\"--dir=/data\", \"--dbfilename=dump\", \"--snapshot_cron=*/2 * * * *\"]")
    print(f"  postgres:\n    mem_limit: {human(p['pg_limit'])}")
    for k, v in FIXED_INFRA.items():
        print(f"  {k}:\n    mem_limit: {human(v)}")
    for k, v in APP_SERVICES.items():
        print(f"  {k}:\n    mem_limit: {human(v)}")
    print("\n=== PostgreSQL command -c 参数 ===")
    for kv in [f"max_connections={pg['max_connections']}",
               f"shared_buffers={pg['shared_buffers']}MB",
               f"effective_cache_size={pg['effective_cache_size']}MB",
               f"work_mem={pg['work_mem']}MB",
               f"maintenance_work_mem={pg['maintenance_work_mem']}MB",
               f"wal_buffers={pg['wal_buffers']}MB",
               "wal_compression=on",
               "shared_preload_libraries=timescaledb,pg_stat_statements"]:
        print(f"      - -c\n      - {kv}")


def main():
    ap = argparse.ArgumentParser(description="按机器算最优容器内存预算 + PG 参数")
    ap.add_argument("--phys-mb", type=int, help="物理内存 MB(默认探测本机)")
    ap.add_argument("--nproc", type=int, help="核数(默认探测本机)")
    ap.add_argument("--max-connections", type=int, default=300, help="PG max_connections(默认 300)")
    ap.add_argument("--emit-override", action="store_true", help="额外打印 compose override + PG -c 片段")
    args = ap.parse_args()

    phys = args.phys_mb or detect_phys_mb()
    nproc = args.nproc or detect_nproc()
    p = plan(phys, nproc, args.max_connections)
    print_plan(p)
    if args.emit_override:
        emit_override(p)


if __name__ == "__main__":
    main()
