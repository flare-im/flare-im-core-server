#!/usr/bin/env bash
# Hook 配置档激活：core（无业务 Hook）| social（flare-social PreSend）
#
# 用法（由 start_server*.sh 调用）：
#   flare_im_activate_hooks_profile core  /path/to/flare-im-core
#   flare_im_activate_hooks_profile social /path/to/flare-im-core

flare_im_hooks_profile_usage() {
    cat <<'EOF'
Hook 配置档:
  core    — 业务中立，不注册 PreSend/PostSend 等 Hook
  social  — 启用 flare-social PreSend（需 flare-social-hook 在 Consul 可达）

示例:
  ./scripts/start_server_core.sh single
  ./scripts/start_server_social.sh single
  FLARE_HOOKS_PROFILE=social ./scripts/start_server.sh single
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
        core | social) ;;
        *)
            echo "无效的 FLARE_HOOKS_PROFILE: $profile（期望 core 或 social）" >&2
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
