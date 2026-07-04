#!/usr/bin/env bash

flare_release_script_dir() {
    cd "$(dirname "${BASH_SOURCE[1]}")" && pwd
}

flare_release_app_home() {
    local script_dir
    script_dir="$(flare_release_script_dir)"
    cd "$script_dir/.." && pwd
}

flare_log() {
    printf '[flare-release] %s\n' "$*"
}

flare_warn() {
    printf '[flare-release][warn] %s\n' "$*" >&2
}

flare_die() {
    printf '[flare-release][error] %s\n' "$*" >&2
    exit 1
}

flare_require_command() {
    command -v "$1" >/dev/null 2>&1 || flare_die "missing required command: $1"
}

flare_compose_cmd() {
    if docker compose version >/dev/null 2>&1; then
        printf '%s\n' "docker compose"
    elif command -v docker-compose >/dev/null 2>&1; then
        printf '%s\n' "docker-compose"
    else
        return 1
    fi
}

flare_check_port() {
    local port="$1"
    if command -v nc >/dev/null 2>&1; then
        nc -z 127.0.0.1 "$port" >/dev/null 2>&1 && return 0
    fi
    if command -v lsof >/dev/null 2>&1; then
        lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1 && return 0
    fi
    bash -c ":</dev/tcp/127.0.0.1/$port" >/dev/null 2>&1
}

flare_wait_for_port() {
    local port="$1"
    local label="$2"
    local timeout_seconds="${3:-90}"
    local waited=0

    while [ "$waited" -lt "$timeout_seconds" ]; do
        if flare_check_port "$port"; then
            flare_log "$label ready on 127.0.0.1:$port"
            return 0
        fi
        sleep 2
        waited=$((waited + 2))
    done

    flare_warn "$label did not become ready on 127.0.0.1:$port within ${timeout_seconds}s"
    return 1
}

flare_load_env_file() {
    local app_home="$1"
    local env_file="${FLARE_RELEASE_ENV_FILE:-$app_home/.env}"
    if [ -f "$env_file" ]; then
        # shellcheck disable=SC1090
        set -a
        . "$env_file"
        set +a
    fi
}

flare_ensure_token_secrets() {
    local app_home="$1"
    mkdir -p "$app_home/data" "$app_home/logs" "$app_home/run"

    if [ -n "${FLARE_API_GATEWAY_TOKEN_SECRET:-}" ] && [ -n "${ACCESS_GATEWAY_TOKEN_SECRET:-}" ]; then
        export FLARE_ADMIN_GATEWAY_TOKEN_SECRET="${FLARE_ADMIN_GATEWAY_TOKEN_SECRET:-$FLARE_API_GATEWAY_TOKEN_SECRET}"
        return 0
    fi

    local secret_file="$app_home/data/.dev-token-secret"
    if [ ! -s "$secret_file" ]; then
        umask 077
        if command -v openssl >/dev/null 2>&1; then
            openssl rand -base64 48 | tr -d '\n' > "$secret_file"
        elif command -v uuidgen >/dev/null 2>&1; then
            printf '%s%s\n' "$(uuidgen | tr -d '-')" "$(uuidgen | tr -d '-')" > "$secret_file"
        else
            LC_ALL=C tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 64 > "$secret_file"
        fi
    fi

    local secret
    secret="$(cat "$secret_file")"
    export FLARE_API_GATEWAY_TOKEN_SECRET="${FLARE_API_GATEWAY_TOKEN_SECRET:-$secret}"
    export ACCESS_GATEWAY_TOKEN_SECRET="${ACCESS_GATEWAY_TOKEN_SECRET:-$secret}"
    export FLARE_ADMIN_GATEWAY_TOKEN_SECRET="${FLARE_ADMIN_GATEWAY_TOKEN_SECRET:-$secret}"
}

flare_release_service_specs() {
    cat <<'SPECS'
signaling-online:flare-signaling-online:50061
signaling-route:flare-signaling-route:50062
capability:flare-capability:
conversation:flare-conversation:50090
message-ingest:flare-message-ingest:50182
message-orchestrator:flare-orchestrator:50181
storage-writer:flare-storage-writer:
storage-reader:flare-storage-reader:60083
sync-orchestrator:flare-sync-orchestrator:60084
push-server:flare-push-server:
push-worker:flare-push-worker:
media:flare-media:60081
api-gateway:flare-api-gateway:50050
access-gateway:flare-signaling-gateway:60051
SPECS
}

flare_release_required_bins() {
    flare_release_service_specs | while IFS=: read -r _service bin _port; do
        printf '%s\n' "$bin"
    done | sort -u
}

flare_configure_runtime_env() {
    local app_home="$1"

    export FLARE_CONFIG_PATH="${FLARE_CONFIG_PATH:-$app_home/config}"
    export FLARE_MQ_DEFAULT_BACKEND="${FLARE_MQ_DEFAULT_BACKEND:-nats}"
    export RUST_LOG="${RUST_LOG:-info,hyper=warn,reqwest=warn,h2=warn,tower=warn,tokio=warn,sqlx=warn,tonic=warn,redis=warn,async_nats=warn}"

    local no_proxy_hosts='localhost,127.0.0.1,::1'
    export NO_PROXY="${NO_PROXY:+$NO_PROXY,}$no_proxy_hosts"
    export no_proxy="${no_proxy:+$no_proxy,}$no_proxy_hosts"

    export CONSUL_DISCOVERY_REFRESH_INTERVAL="${CONSUL_DISCOVERY_REFRESH_INTERVAL:-90}"
    export CONSUL_DISCOVER_CACHE_TTL_SECS="${CONSUL_DISCOVER_CACHE_TTL_SECS:-15}"
    export SERVICE_HEARTBEAT_INTERVAL="${SERVICE_HEARTBEAT_INTERVAL:-30}"

    export SIGNALING_ONLINE_SERVICE_HOST="${SIGNALING_ONLINE_SERVICE_HOST:-0.0.0.0}"
    export SIGNALING_ONLINE_SERVICE_PORT="${SIGNALING_ONLINE_SERVICE_PORT:-50061}"
    export ONLINE_SERVICE_ENDPOINT="${ONLINE_SERVICE_ENDPOINT:-http://127.0.0.1:50061}"

    export SIGNALING_ROUTE_SERVICE_HOST="${SIGNALING_ROUTE_SERVICE_HOST:-0.0.0.0}"
    export SIGNALING_ROUTE_SERVICE_PORT="${SIGNALING_ROUTE_SERVICE_PORT:-50062}"
    export ROUTE_SERVICE_ENDPOINT="${ROUTE_SERVICE_ENDPOINT:-http://127.0.0.1:50062}"

    export SYNC_ORCHESTRATOR_HOST="${SYNC_ORCHESTRATOR_HOST:-0.0.0.0}"
    export SYNC_ORCHESTRATOR_PORT="${SYNC_ORCHESTRATOR_PORT:-60084}"

    export ACCESS_GATEWAY_GRPC_ENDPOINT="${ACCESS_GATEWAY_GRPC_ENDPOINT:-http://127.0.0.1:60060}"
    export FLARE_API_GATEWAY_GRPC_MESSAGE_INGEST_STATIC_FALLBACK="${FLARE_API_GATEWAY_GRPC_MESSAGE_INGEST_STATIC_FALLBACK:-http://127.0.0.1:50182}"
    export FLARE_API_GATEWAY_GRPC_MESSAGE_ORCHESTRATOR_STATIC_FALLBACK="${FLARE_API_GATEWAY_GRPC_MESSAGE_ORCHESTRATOR_STATIC_FALLBACK:-http://127.0.0.1:50181}"
    export FLARE_API_GATEWAY_GRPC_CONVERSATION_STATIC_FALLBACK="${FLARE_API_GATEWAY_GRPC_CONVERSATION_STATIC_FALLBACK:-http://127.0.0.1:50090}"
    export FLARE_API_GATEWAY_GRPC_MEDIA_STATIC_FALLBACK="${FLARE_API_GATEWAY_GRPC_MEDIA_STATIC_FALLBACK:-http://127.0.0.1:60081}"
}
