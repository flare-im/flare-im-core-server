#!/usr/bin/env bash
# 启动 flare-im-core（启用 flare-social PreSend Hook）
#
# 用法与 start_server.sh 相同，例如：
#   ./scripts/start_server_social.sh
#   ./scripts/start_server_social.sh single
#
# Hook 配置：config/hooks.social.toml
#
# 前置：需另启 Social 栈（至少 flare-social-hook）：
#   cd ../flare-social && ./scripts/start_social.sh
#
# 可选：本脚本启动前检查 Consul 中 flare-social-hook
#   START_SOCIAL_HOOK_CHECK=0 ./scripts/start_server_social.sh  # 跳过检查

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

if [ "${START_SOCIAL_HOOK_CHECK:-1}" != "0" ]; then
    if command -v curl >/dev/null 2>&1; then
        consul_url="${CONSUL_HTTP_ADDR:-http://127.0.0.1:28500}"
        if ! curl -sf "${consul_url}/v1/status/leader" >/dev/null 2>&1; then
            echo -e "${YELLOW}⚠ Consul 未就绪 (${consul_url})；Social Hook 发现可能失败${NC}" >&2
        else
            svc_json=$(curl -sf "${consul_url}/v1/catalog/service/flare-social-hook" 2>/dev/null || true)
            if [ -z "$svc_json" ] || [ "$svc_json" = "[]" ] || [ "$svc_json" = "null" ]; then
                echo -e "${YELLOW}⚠ 未在 Consul 发现 flare-social-hook${NC}" >&2
                echo -e "${YELLOW}  请先启动: cd ../flare-social && ./scripts/start_social.sh${NC}" >&2
                echo -e "${YELLOW}  或跳过检查: START_SOCIAL_HOOK_CHECK=0 $0 $*${NC}" >&2
            fi
        fi
    fi
fi

export FLARE_HOOKS_PROFILE=social
exec "$SCRIPT_DIR/start_server.sh" "$@"
