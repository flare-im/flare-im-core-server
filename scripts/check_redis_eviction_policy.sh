#!/usr/bin/env bash
# Redis 淘汰策略门禁。
#
# 这个 Redis 混着两类键：带 TTL 的缓存/幂等键，和**不带 TTL 的会话 seq 高水位**。
# `allkeys-lru` 在内存吃紧时会连后者一起淘汰；seq 键一旦丢失，下一条消息的 INCR
# 从 1 重新开始，会话 seq 大幅回退、新消息被排到时间线中间，而且是**永久错乱**。
# 本仓历史上真出过这个故障，且它零信号——不到内存压力临界点完全看不出来。
#
# 允许 volatile-*（只淘汰有 TTL 的键）与 noeviction；禁止任何 allkeys-*。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bad=0
found=0

while IFS= read -r -d '' f; do
  # 只看真正配置 redis 的地方
  grep -q 'maxmemory-policy' "$f" || continue
  found=1
  while IFS= read -r policy; do
    case "$policy" in
      volatile-*|noeviction)
        echo "  ✓ $(basename "$f"): $policy"
        ;;
      allkeys-*)
        echo "  ✗ $(basename "$f"): $policy —— 会淘汰不带 TTL 的 seq 高水位键" >&2
        bad=1
        ;;
      *)
        echo "  ? $(basename "$f"): $policy —— 未知策略，请确认它不会淘汰无 TTL 的键" >&2
        bad=1
        ;;
    esac
  done < <(grep -o 'maxmemory-policy[ =]\+[a-z-]\+' "$f" | awk '{print $NF}')
done < <(find "$ROOT/release" "$ROOT/deploy" -name 'docker-compose*.yml' -not -path '*/dist/*' -print0 2>/dev/null)

[ "$found" = 1 ] || { echo "没有任何 compose 配置 maxmemory-policy（未设上限则不会淘汰），跳过"; exit 0; }

if [ "$bad" = 1 ]; then
  echo "" >&2
  echo "seq 高水位键（seq:<tenant>:<conversation>）没有 TTL，必须免于淘汰。" >&2
  echo "改用 volatile-lru：只淘汰有 TTL 的缓存键，那些本来就可再生。" >&2
  exit 1
fi
echo "✓ Redis 淘汰策略不会碰到无 TTL 的键"
