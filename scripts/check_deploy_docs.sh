#!/usr/bin/env bash
# 门禁：部署文档里的命令，照抄能不能用。
#
# 部署文档是使用者接触这个项目的第一步，而它腐坏时**没有任何信号**：改了 compose
# 文件名、删了某一层、换了变量名，测试全绿、CI 全绿，只有照着做的人卡在第一条命令。
#
# 这个仓已经吃过同类的亏：docker-compose.stack.yml 默认指向一个**从未发布过**的镜像，
# 里面积了库名写错、中间件地址硬编码等好几处，直到真去跑才暴露。
#
# 判据三条：
#   1. 文档里点名的每个 compose 文件都存在；
#   2. 文档里给出的每种叠加组合，`docker compose config` 都能通过；
#   3. 不设 FLARE_TOKEN_SECRET 时必须**失败**——这是刻意的护栏，
#      默认签名密钥等于默认漏洞，退化成有默认值时没人会发现。
#
# 用法：./scripts/check_deploy_docs.sh
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

RED='\033[0;31m'; GREEN='\033[0;32m'; NC='\033[0m'
fail=0
note_fail() { printf "${RED}✗${NC} %s\n" "$1"; fail=1; }
note_ok() { printf "${GREEN}✓${NC} %s\n" "$1"; }

if ! command -v docker >/dev/null 2>&1; then
  # 判红而不是跳过：「没验」和「验过没问题」必须是两种结果。
  note_fail "没有 docker/podman，无法校验 compose —— 这道门禁需要它"
  exit 1
fi

# ── 1. 文档点名的 compose 文件都在 ──────────────────────────────────────────
# 本仓自己的部署文档是**必需**的；工作区根的两份 README 在单仓 checkout（CI 就是）
# 里不存在，缺席时跳过而不是判红——它们属于另一个仓，不是本仓能保证的东西。
REQUIRED_DOCS=("deploy/README.md")
OPTIONAL_DOCS=("../README.md" "../README.zh-CN.md")
declare -a referenced=()
for d in "${REQUIRED_DOCS[@]}"; do
  [ -f "$d" ] || { note_fail "缺本仓的部署文档：$d"; continue; }
  # 要连 `../flare-social/` 这样的前缀一起抓：只抓 `deploy/...` 会把业务栈的路径
  # 截成本仓下不存在的路径，然后报一个假的「文件缺失」。
  while IFS= read -r f; do referenced+=("$f"); done < <(
    grep -oE '(\.\./[A-Za-z0-9_-]+/)?deploy/docker-compose[a-z.-]*\.yml' "$d" 2>/dev/null | sort -u
  )
done

for d in "${OPTIONAL_DOCS[@]}"; do
  [ -f "$d" ] || continue
  while IFS= read -r f; do referenced+=("$f"); done < <(
    grep -oE '(\.\./[A-Za-z0-9_-]+/)?deploy/docker-compose[a-z.-]*\.yml' "$d" 2>/dev/null | sort -u
  )
done

missing=0
for f in $(printf '%s\n' "${referenced[@]}" | sort -u); do
  [ -f "$f" ] || { note_fail "文档引用了不存在的 compose 文件：$f"; missing=1; }
done
[ $missing -eq 0 ] && note_ok "文档引用的 compose 文件都存在（$(printf '%s\n' "${referenced[@]}" | sort -u | wc -l | tr -d ' ') 个）"

# ── 2. 各种叠加组合都能通过 config ──────────────────────────────────────────
check_combo() {
  local label="$1"; shift
  if FLARE_TOKEN_SECRET=x docker compose "$@" config >/dev/null 2>&1; then
    note_ok "$label"
  else
    note_fail "$label —— docker compose config 未通过"
    FLARE_TOKEN_SECRET=x docker compose "$@" config 2>&1 | tail -4 | sed 's/^/      /'
  fi
}

check_combo "仅基础设施" -f deploy/docker-compose.yml
check_combo "基础设施 + 服务栈（预构建镜像）" \
  -f deploy/docker-compose.yml -f deploy/docker-compose.stack.yml
check_combo "基础设施 + 服务栈 + 本地构建" \
  -f deploy/docker-compose.yml -f deploy/docker-compose.stack.yml -f deploy/docker-compose.build.yml

# 业务栈是同级仓，缺席时不判红（开源使用者可能只拿了 IM 核）
if [ -f ../flare-social/deploy/docker-compose.social.yml ]; then
  check_combo "基础设施 + 服务栈 + 业务服务端" \
    -f deploy/docker-compose.yml -f deploy/docker-compose.stack.yml \
    -f ../flare-social/deploy/docker-compose.social.yml
else
  echo "  · 跳过业务栈组合（同级仓 flare-social 不在）"
fi

# ── 3. 缺签名密钥必须失败 ───────────────────────────────────────────────────
if (unset FLARE_TOKEN_SECRET; docker compose -f deploy/docker-compose.yml \
      -f deploy/docker-compose.stack.yml config >/dev/null 2>&1); then
  note_fail "不设 FLARE_TOKEN_SECRET 竟然通过了 —— 默认签名密钥等于默认漏洞，这个护栏不能丢"
else
  note_ok "缺 FLARE_TOKEN_SECRET 时拒绝启动"
fi

# ── 4. .env.example 覆盖文档里提到的每个变量 ────────────────────────────────
if [ -f deploy/.env.example ]; then
  vmiss=0
  for v in FLARE_TOKEN_SECRET POSTGRES_USER POSTGRES_PASSWORD POSTGRES_DB \
           REDIS_PASSWORD RUSTFS_ACCESS_KEY RUSTFS_SECRET_KEY GRAFANA_ADMIN_PASSWORD; do
    grep -q "^$v=" deploy/.env.example || { note_fail ".env.example 缺变量 $v"; vmiss=1; }
  done
  [ $vmiss -eq 0 ] && note_ok ".env.example 覆盖全部可配项"
  # 签名密钥必须留空：给了默认值就等于把漏洞写进模板
  if grep -qE '^FLARE_TOKEN_SECRET=.+' deploy/.env.example; then
    note_fail ".env.example 里的 FLARE_TOKEN_SECRET 有默认值 —— 必须留空"
  else
    note_ok "FLARE_TOKEN_SECRET 在模板里留空"
  fi
else
  note_fail "缺 deploy/.env.example"
fi

echo ""
[ $fail -eq 0 ] && { echo "部署文档与 compose 一致。"; exit 0; }
echo "部署文档与仓库实际对不上——照着做的人会卡在第一条命令。"
exit 1
