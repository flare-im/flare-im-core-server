#!/usr/bin/env bash
# Hook 配置档激活：core（无业务 Hook）。
#
# 业务 Hook 不随 Core 发布：业务服务端自己的 Hook 注册文件经 MESSAGE_INGEST_HOOKS_CONFIG /
# STORAGE_HOOKS_CONFIG（或 hooks.d 目录）交给 Core 加载。
#
# 用法（由 start_server*.sh 调用）：
#   flare_im_activate_hooks_profile core /path/to/flare-im-core

flare_im_hooks_profile_usage() {
    cat <<'EOF'
Hook 配置档:
  core    — 业务中立，不注册 PreSend/PostSend 等 Hook

业务 Hook：把业务服务端的 Hook 注册文件路径设给 MESSAGE_INGEST_HOOKS_CONFIG。

示例:
  ./scripts/start_server_core.sh single
  FLARE_HOOKS_PROFILE=core ./scripts/start_server.sh single
EOF
}

flare_im_activate_hooks_profile() {
    local profile="${1:-}"
    local project_root="${2:-}"

    if [ -z "$profile" ] || [ -z "$project_root" ]; then
        echo "flare_im_activate_hooks_profile: 缺少 profile 或 project_root" >&2
        return 1
    fi

    case "$profile" in
        core) ;;
        *)
            echo "无效的 FLARE_HOOKS_PROFILE: $profile（期望 core）" >&2
            flare_im_hooks_profile_usage >&2
            return 1
            ;;
    esac

    local src="$project_root/config/hooks.${profile}.toml"
    local link="$project_root/config/hooks.toml"

    if [ ! -f "$src" ]; then
        echo "Hook 配置文件不存在: $src" >&2
        return 1
    fi

    ln -sfn "hooks.${profile}.toml" "$link"

    export FLARE_HOOKS_PROFILE="$profile"
    export MESSAGE_INGEST_HOOKS_CONFIG="$link"
    export STORAGE_HOOKS_CONFIG="$link"
    export CONFIG_FILE="$link"

    return 0
}
