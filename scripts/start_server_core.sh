#!/usr/bin/env bash
# 启动 flare-im-core（业务中立：不注册 flare-social 等业务 Hook）
#
# 用法与 start_server.sh 相同，例如：
#   ./scripts/start_server_core.sh
#   ./scripts/start_server_core.sh single
#   ./scripts/start_server_core.sh single trace
#
# Hook 配置：config/hooks.core.toml（空 Hook 列表）

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export FLARE_HOOKS_PROFILE=core
exec "$SCRIPT_DIR/start_server.sh" "$@"
