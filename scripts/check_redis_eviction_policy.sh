#!/usr/bin/env bash
# KV 淘汰策略门禁(Redis / Dragonfly 通用)。
#
# 这个 KV 混着两类键：带 TTL 的缓存/幂等键，和**不带 TTL 的会话 seq 高水位**。
# 任何会淘汰无 TTL 键的配置都会让 seq 键丢失：下一条消息的 INCR 从 1 重开,会话 seq
# 大幅回退、新消息排进时间线中间,**永久错乱**且零信号。本仓历史上真出过这个故障。
#
# 判据(不淘汰无 TTL 键)：
#   - Redis:   允许 volatile-*(只淘汰有 TTL 的键)与 noeviction;禁止任何 allkeys-*。
#   - Dragonfly: 默认 noeviction(内存满写 OOM、不淘汰)即安全;**禁止 --cache_mode**
#                (开启后按 LRU 淘汰、会连无 TTL 的 seq 键一起淘汰)。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bad=0
found=0

while IFS= read -r -d '' f; do
  is_dragonfly=0
  grep -qiE 'dragonfly' "$f" && is_dragonfly=1

  # Dragonfly:禁止开启 cache_mode(会淘汰无 TTL 键)
  if [ "$is_dragonfly" = 1 ]; then
    if grep -vE '^\s*#' "$f" | grep -qiE -- '--cache_mode|cache_mode[ =]+true'; then
      echo "  ✗ $(basename "$f"): Dragonfly 开了 --cache_mode —— 会 LRU 淘汰无 TTL 的 seq 高水位键" >&2
      bad=1
    else
      echo "  ✓ $(basename "$f"): Dragonfly 默认 noeviction(未开 cache_mode),无 TTL 键免于淘汰"
    fi
    found=1
  fi

  # Redis:检查 maxmemory-policy
  if grep -q 'maxmemory-policy' "$f"; then
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
          echo "  ? $(basename "$f"): $policy —— 未知策略,请确认它不会淘汰无 TTL 的键" >&2
          bad=1
          ;;
      esac
    done < <(grep -o 'maxmemory-policy[ =]\+[a-z-]\+' "$f" | awk '{print $NF}')
  fi
done < <(find "$ROOT/release" "$ROOT/deploy" -name 'docker-compose*.yml' -not -path '*/dist/*' -print0 2>/dev/null)

[ "$found" = 1 ] || { echo "没有任何 compose 配置 KV 淘汰相关项(未设上限则不淘汰),跳过"; exit 0; }

if [ "$bad" = 1 ]; then
  echo "" >&2
  echo "seq 高水位键(seq:<tenant>:<conversation>)没有 TTL,必须免于淘汰。" >&2
  echo "Redis 用 volatile-lru;Dragonfly 不要开 --cache_mode(默认 noeviction 即安全)。" >&2
  exit 1
fi
echo "✓ KV 淘汰策略不会碰到无 TTL 的键"
