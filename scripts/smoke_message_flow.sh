#!/usr/bin/env bash
# Flare IM Core 消息链路冒烟（DEP-06 / SEC-07 / S1），两种模式：
#
# http（默认）：只走对外入口。经 Edge 调 Social 公共 API 注册并登录两个用户、互加
# 好友（满足 social 档 PreSend 的好友判定）→ 用 Social 登录返回的 IM 令牌经 Edge 调
# Core api-gateway 发送单聊消息 → 经 Core 会话列表回读 → 在数据库容器内以只读事务
# SELECT 核对 messages 与 message_write_ledger 已持久化。
#
# grpc（--mode grpc）：直连单容器 Core 的 Message Ingest gRPC 发送，交给
# tools/flare-smoke-grpc 执行。每次调用都带 x-flare-ctx：用 Cell 内部签名私钥
# （core.env 的 FLARE_INTERNAL_CTX_PRIVATE_KEY，经 600 文件给出）在该进程内签名，
# 不存在未签名路径，也不发旧的身份元数据。Core 的 conversation/storage-reader
# 读接口只认未签名的身份元数据，所以回读（存储行、会话行）与归档落库确认改走只读
# 数据库会话，口令只从 600 文件读取，默认/弱口令一律拒绝。gRPC 端口只在 compose
# 内网可达，该模式按设计在一次性网内探针容器里运行（见 flare-deploy
# tests/stack/grpc_smoke_probe.py）；本脚本在 grpc 模式下只需要 bash、find 与该二进制。
#
# 旧的直连 gRPC 模式（向 message-ingest / storage-reader 直发未签名的身份头、默认
# 租户 0、带默认口令的宿主机数据库 URL）已删除，它的变量一律拒绝。
#
# 口令与令牌只从文件读取（或在私有临时目录生成一次性口令），不进命令行参数、
# 不进 shell 变量、不回显；失败时只打印响应的 code/reason/message。
set -euo pipefail
set +x
umask 077

usage() {
    cat <<'USAGE'
Usage: smoke_message_flow.sh [--mode http|grpc] [--dry-run] [-h|--help]

  --mode http   (default; also SMOKE_MODE) through the public Edge, see below.
  --mode grpc   signed direct gRPC to a single-container Core, see "gRPC mode".
  --dry-run     grpc mode only: validate settings, key and password, print the
                plan, open no connection.

HTTP mode: message-flow smoke through the public Edge. Registers two
fresh users in an isolated e2e- tenant via the Social public API, makes them
friends, sends one single-chat message through Core's /api/v1/messages/send
with the IM token returned by Social login, reads it back through Core's
conversation list, and verifies persistence with a read-only SELECT executed
inside the PostgreSQL container.

Required:
  SMOKE_BASE_URL           Public origin, e.g. https://im.example.test:18443
                           (https only; plain http is accepted for loopback hosts).
  SMOKE_TENANT_ID          Isolated fixture tenant e2e-<20 lowercase hex>, e.g. one
                           created by flare-social/scripts/prepare_e2e_fixture.py.
                           The default tenant "0" is never accepted.
  SMOKE_PG_CONTAINER       PostgreSQL container used for the read-only check,
                           unless SMOKE_SKIP_DB_CHECK=1.

Optional:
  SMOKE_CA_FILE            PEM CA bundle for a private/self-signed Edge certificate.
  SMOKE_SERVER_NAME        TLS SNI and Host name when it differs from the host in
                           SMOKE_BASE_URL (the connection still goes to that host).
  SMOKE_HOST_HEADER        Explicit Host header (default: derived from the URL).
  SMOKE_CORE_API_PREFIX    Edge prefix of the Core API (default: /api).
  SMOKE_SOCIAL_API_PREFIX  Edge prefix of the Social API (default: /social-api).
  SMOKE_PASSWORD_FILE      File holding the password for the two fixture users
                           (mode 600, 8-4096 printable characters). When unset a
                           one-time random password is generated in a private
                           temporary directory.
  SMOKE_CONTAINER_RUNTIME  podman (default) or docker.
  SMOKE_PG_DATABASE        Core database inside the container (default: flare_core).
  SMOKE_PG_USER            Database role (default: the container's POSTGRES_USER).
  SMOKE_SKIP_DB_CHECK      1 skips the persistence check (reported as skipped).
  SMOKE_TIMEOUT_SECONDS    Per-wait timeout, 1-900 (default: 60).

Fixture users stay in the isolated tenant for diagnosis; nothing is deleted.

gRPC mode (--mode grpc): runs flare-smoke-grpc (flare-im-core
tools/flare-smoke-grpc). It sends one text message through Message Ingest's
MessageSendService/SendMessage with x-flare-ctx signed in-process by the Cell
internal private key; no call is made without it and no legacy identity
metadata is sent. Read-back is first over signed gRPC, each call signed for
the reading user: Storage Reader's QueryMessagesBySeq (as the recipient)
returns the message at the sent seq, and the conversation service's
ListConversations (as each user) lists the conversation at that seq or later.
A read-only PostgreSQL session (default_transaction_read_only=on, BEGIN
TRANSACTION READ ONLY) is the second evidence: the messages row,
message_write_ledger at archive_persisted or later, and both users'
conversation seq at least the sent seq, read the way Core's conversation list
reads it (conversation and member not deleted, seq =
GREATEST(conversations.last_message_seq, newest message the member may see)).
The gRPC ports are only reachable inside the compose network; run this mode in
a one-off in-network probe container (flare-deploy tests/stack/grpc_smoke_probe.py).

Required (gRPC mode):
  SMOKE_TENANT_ID              Isolated fixture tenant e2e-<20 lowercase hex>.
  SMOKE_GRPC_INGEST_ENDPOINT   http://HOST:PORT of Message Ingest, e.g. http://core:50182.
  SMOKE_GRPC_SENDER_ID         Existing users of that tenant who are already
  SMOKE_GRPC_RECIPIENT_ID      friends (e.g. the two users an HTTP-mode run reports).
  SMOKE_GRPC_SIGNING_KEY_FILE  Mode-600 file holding the Cell internal private key
                               (the value of FLARE_INTERNAL_CTX_PRIVATE_KEY).
  SMOKE_GRPC_DB_HOST           PostgreSQL host reachable from where this runs.
  SMOKE_GRPC_DB_USER           Database role (e.g. Core's runtime role).
  SMOKE_GRPC_DB_PASSWORD_FILE  Mode-600 file holding that role's password; default
                               or weak passwords are refused.

Optional (gRPC mode):
  SMOKE_GRPC_STORAGE_READER_ENDPOINT
                               http://HOST:PORT of Storage Reader (default: the
                               ingest host on port 60083).
  SMOKE_GRPC_CONVERSATION_ENDPOINT
                               http://HOST:PORT of the conversation service
                               (default: the ingest host on port 50090).
  SMOKE_GRPC_DB_PORT           Default 5432.
  SMOKE_GRPC_DB_NAME           Default flare_core.
  SMOKE_GRPC_CLIENT            flare-smoke-grpc binary (default: found on PATH).
  SMOKE_TIMEOUT_SECONDS        Per-wait timeout, 1-900 (default: 60).

HTTP-mode settings (SMOKE_BASE_URL, SMOKE_PG_CONTAINER, ...) are refused in
gRPC mode, and SMOKE_GRPC_* settings are not read in HTTP mode.
USAGE
}

die() {
    echo "smoke_message_flow: $*" >&2
    exit 1
}

mode="${SMOKE_MODE:-http}"
dry_run=0
while [ "$#" -gt 0 ]; do
    case "$1" in
        -h|--help)
            usage
            exit 0
            ;;
        --mode)
            [ "$#" -ge 2 ] || die "--mode needs a value (http or grpc)"
            mode="$2"
            shift 2
            ;;
        --mode=*)
            mode="${1#--mode=}"
            shift
            ;;
        --dry-run)
            dry_run=1
            shift
            ;;
        *)
            usage >&2
            die "unknown argument"
            ;;
    esac
done

case "$mode" in
    http|grpc) ;;
    *) die "mode must be http or grpc; the unsigned direct gRPC mode was removed, --mode grpc signs every call inside the Cell" ;;
esac
if [ "$mode" = "http" ] && [ "$dry_run" = "1" ]; then
    die "--dry-run is only supported with --mode grpc"
fi

# 旧 gRPC 模式的变量不再生效；静默忽略会让调用方以为还在测旧链路，直接拒绝。
# 只打印变量名，不打印值（旧的数据库 URL 可能带口令）。
for legacy in SMOKE_MESSAGE_INGEST_ENDPOINT SMOKE_STORAGE_READER_ENDPOINT \
    SMOKE_POSTGRES_URL SMOKE_GRPCURL SMOKE_PSQL SMOKE_PROTO_ROOT \
    SMOKE_SENDER_ID SMOKE_RECIPIENT_ID SMOKE_CONVERSATION_ID; do
    if [ -n "${!legacy:-}" ]; then
        die "$legacy belonged to the removed unsigned gRPC mode; unset it and use the HTTP or SMOKE_GRPC_* settings (see --help)"
    fi
done

# 私有文件：普通文件、非符号链接、可读，组和其他人没有任何权限（find 的 -perm 在
# GNU 与 BSD 上都可用，探针容器里没有 python3）。只检查元数据，不读内容。
require_private_file() {
    local name="$1" path="${!1:-}"
    [ -n "$path" ] || die "$name is required in grpc mode (see --help)"
    [ ! -L "$path" ] || die "$name must not be a symlink"
    { [ -f "$path" ] && [ -r "$path" ]; } || die "$name is not a readable regular file"
    if [ -n "$(find "$path" -prune \( -perm -0040 -o -perm -0020 -o -perm -0010 \
        -o -perm -0004 -o -perm -0002 -o -perm -0001 \) -print)" ]; then
        die "$name must not be accessible by group or others (chmod 600)"
    fi
}

# gRPC 模式：校验后 exec flare-smoke-grpc。只给它非秘密值与秘密文件的路径；
# 私钥与口令由它在自己进程内读取、校验（含默认/弱口令拒绝）并使用。
run_grpc_mode() {
    local setting
    for setting in SMOKE_BASE_URL SMOKE_CA_FILE SMOKE_SERVER_NAME SMOKE_HOST_HEADER \
        SMOKE_CORE_API_PREFIX SMOKE_SOCIAL_API_PREFIX SMOKE_PASSWORD_FILE \
        SMOKE_CONTAINER_RUNTIME SMOKE_PG_CONTAINER SMOKE_PG_DATABASE SMOKE_PG_USER \
        SMOKE_SKIP_DB_CHECK; do
        if [ -n "${!setting:-}" ]; then
            die "$setting is an HTTP-mode setting; grpc mode sends through Message Ingest and reads back over a read-only database session (see --help)"
        fi
    done
    local tenant="${SMOKE_TENANT_ID:-}" timeout="${SMOKE_TIMEOUT_SECONDS:-60}"
    if ! [[ "$tenant" =~ ^e2e-[0-9a-f]{20}$ ]]; then
        die "SMOKE_TENANT_ID must be an isolated fixture tenant e2e-<20 lowercase hex>; the default tenant is never used"
    fi
    if ! [[ "$timeout" =~ ^[0-9]{1,3}$ ]] || [ "$timeout" -lt 1 ] || [ "$timeout" -gt 900 ]; then
        die "SMOKE_TIMEOUT_SECONDS must be an integer between 1 and 900"
    fi
    for setting in SMOKE_GRPC_INGEST_ENDPOINT SMOKE_GRPC_SENDER_ID SMOKE_GRPC_RECIPIENT_ID \
        SMOKE_GRPC_DB_HOST SMOKE_GRPC_DB_USER; do
        [ -n "${!setting:-}" ] || die "$setting is required in grpc mode (see --help)"
    done
    require_private_file SMOKE_GRPC_SIGNING_KEY_FILE
    require_private_file SMOKE_GRPC_DB_PASSWORD_FILE

    local client="${SMOKE_GRPC_CLIENT:-flare-smoke-grpc}"
    if [[ "$client" != */* ]]; then
        client="$(command -v "$client" 2>/dev/null)" \
            || die "flare-smoke-grpc not found; build tools/flare-smoke-grpc or set SMOKE_GRPC_CLIENT"
    fi
    { [ -f "$client" ] && [ -x "$client" ]; } || die "SMOKE_GRPC_CLIENT is not an executable file"

    local -a args=(
        --ingest-endpoint "$SMOKE_GRPC_INGEST_ENDPOINT"
        --tenant-id "$tenant"
        --sender-id "$SMOKE_GRPC_SENDER_ID"
        --recipient-id "$SMOKE_GRPC_RECIPIENT_ID"
        --signing-key-file "$SMOKE_GRPC_SIGNING_KEY_FILE"
        --db-host "$SMOKE_GRPC_DB_HOST"
        --db-port "${SMOKE_GRPC_DB_PORT:-5432}"
        --db-name "${SMOKE_GRPC_DB_NAME:-flare_core}"
        --db-user "$SMOKE_GRPC_DB_USER"
        --db-password-file "$SMOKE_GRPC_DB_PASSWORD_FILE"
        --timeout-seconds "$timeout"
    )
    if [ -n "${SMOKE_GRPC_STORAGE_READER_ENDPOINT:-}" ]; then
        args+=(--storage-reader-endpoint "$SMOKE_GRPC_STORAGE_READER_ENDPOINT")
    fi
    if [ -n "${SMOKE_GRPC_CONVERSATION_ENDPOINT:-}" ]; then
        args+=(--conversation-endpoint "$SMOKE_GRPC_CONVERSATION_ENDPOINT")
    fi
    if [ "$dry_run" = "1" ]; then
        args+=(--dry-run)
    fi
    exec "$client" "${args[@]}"
}

if [ "$mode" = "grpc" ]; then
    run_grpc_mode
fi

base_url="${SMOKE_BASE_URL:-}"
tenant_id="${SMOKE_TENANT_ID:-}"
ca_file="${SMOKE_CA_FILE:-}"
server_name="${SMOKE_SERVER_NAME:-}"
host_header_override="${SMOKE_HOST_HEADER:-}"
core_prefix="${SMOKE_CORE_API_PREFIX-/api}"
social_prefix="${SMOKE_SOCIAL_API_PREFIX-/social-api}"
password_file="${SMOKE_PASSWORD_FILE:-}"
container_runtime="${SMOKE_CONTAINER_RUNTIME:-podman}"
pg_container="${SMOKE_PG_CONTAINER:-}"
pg_database="${SMOKE_PG_DATABASE:-flare_core}"
pg_user="${SMOKE_PG_USER:-}"
skip_db_check="${SMOKE_SKIP_DB_CHECK:-0}"
timeout_seconds="${SMOKE_TIMEOUT_SECONDS:-60}"

[ -n "$base_url" ] || die "SMOKE_BASE_URL is required (see --help)"
if ! [[ "$tenant_id" =~ ^e2e-[0-9a-f]{20}$ ]]; then
    die "SMOKE_TENANT_ID must be an isolated fixture tenant e2e-<20 lowercase hex>; the default tenant is never used"
fi
if ! [[ "$timeout_seconds" =~ ^[0-9]{1,3}$ ]] || [ "$timeout_seconds" -lt 1 ] || [ "$timeout_seconds" -gt 900 ]; then
    die "SMOKE_TIMEOUT_SECONDS must be an integer between 1 and 900"
fi
if [ -n "$ca_file" ] && { [ ! -f "$ca_file" ] || [ ! -r "$ca_file" ]; }; then
    die "SMOKE_CA_FILE is not a readable file"
fi
case "$skip_db_check" in
    0|1) ;;
    *) die "SMOKE_SKIP_DB_CHECK must be 0 or 1" ;;
esac
if [ "$skip_db_check" = "0" ]; then
    case "$container_runtime" in
        podman|docker) ;;
        *) die "SMOKE_CONTAINER_RUNTIME must be podman or docker" ;;
    esac
    [[ "$pg_container" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$ ]] \
        || die "SMOKE_PG_CONTAINER is required (a container name), or set SMOKE_SKIP_DB_CHECK=1"
    [[ "$pg_database" =~ ^[A-Za-z_][A-Za-z0-9_]{0,62}$ ]] || die "SMOKE_PG_DATABASE is not a plain identifier"
    if [ -n "$pg_user" ] && ! [[ "$pg_user" =~ ^[A-Za-z_][A-Za-z0-9_]{0,62}$ ]]; then
        die "SMOKE_PG_USER is not a plain identifier"
    fi
fi

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

require_command curl
require_command python3
if [ "$skip_db_check" = "0" ]; then
    require_command "$container_runtime"
fi

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/flare-message-flow.XXXXXX")"
chmod 700 "$work_dir"
cleanup() {
    rm -rf "$work_dir"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

helper="$work_dir/helper.py"
cat > "$helper" <<'PY'
"""JSON/credential helper for smoke_message_flow.sh.

Tokens and passwords only ever move between private files; nothing secret is
printed. Failure summaries carry code/reason/message with token-like strings
redacted, never the response data.
"""
import hashlib
import json
import os
import re
import secrets
import stat
import sys
import urllib.parse

MAX_BODY = 1024 * 1024
SCALAR = re.compile(r"^[A-Za-z0-9_:-]{1,160}$")
TOKEN = re.compile(r"^[A-Za-z0-9._~+/=-]{16,8192}$")
DNS_NAME = re.compile(
    r"^(?=.{1,253}$)[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*$"
)
HOST_HEADER = re.compile(r"^(?:[A-Za-z0-9.-]{1,253}|\[[0-9A-Fa-f:.]+\])(?::[0-9]{1,5})?$")
PREFIX = re.compile(r"^(?:/[A-Za-z0-9_-][A-Za-z0-9._-]*)*$")
LOOPBACK = {"localhost", "127.0.0.1", "::1"}
CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
SECRET_LIKE = (
    re.compile(r"[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}"),
    re.compile(r"[A-Za-z0-9+/=_.~-]{32,}"),
)
LOGIN_TYPE_EMAIL = 1
CONTACT_SOURCE_SEARCH = 1
CONVERSATION_TYPE_SINGLE = 1
# The HTTP gateway wraps the JSON body as CustomContent("http.json").
MESSAGE_TYPE_CUSTOM = 19


class Failure(Exception):
    pass


def write_private(path, text):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(text)


def write_json(path, value):
    write_private(path, json.dumps(value, separators=(",", ":")))


def redact(value, limit=240):
    text = "".join(ch if ch.isprintable() else "?" for ch in str(value))
    for pattern in SECRET_LIKE:
        text = pattern.sub("[redacted]", text)
    return text[:limit]


def envelope(path):
    try:
        with open(path, "rb") as handle:
            raw = handle.read(MAX_BODY + 1)
    except OSError:
        return None
    if len(raw) > MAX_BODY:
        return None
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def success_data(path):
    value = envelope(path)
    if not value or value.get("code") != 0 or not isinstance(value.get("data"), dict):
        raise Failure("unexpected response envelope")
    return value["data"]


def lookup(data, dotted):
    value = data
    for key in dotted.split("."):
        if not isinstance(value, dict) or key not in value:
            raise Failure(f"response is missing {dotted}")
        value = value[key]
    return value


def read_password(path):
    with open(path, encoding="utf-8") as handle:
        value = handle.read(4098)
    value = value.rstrip("\r\n")
    if not 8 <= len(value) <= 4096 or any(not ch.isprintable() for ch in value):
        raise Failure("password file must hold one printable password of 8-4096 characters")
    return value


def cmd_check(step, status, path, quiet=False):
    value = envelope(path)
    if status == "200" and value and value.get("code") == 0 and isinstance(value.get("data"), dict):
        return 0
    if not quiet:
        code = value.get("code") if value else None
        reason = redact(value.get("reason", "")) if value else ""
        message = redact(value.get("message", "")) if value else ""
        print(
            f"smoke_message_flow: step={step} http_status={redact(status, 8)} "
            f"code={redact(code, 16)} reason={reason} message={message}",
            file=sys.stderr,
        )
    return 1


def cmd_scalar(path, dotted):
    value = lookup(success_data(path), dotted)
    if isinstance(value, bool):
        print("true" if value else "false")
    elif isinstance(value, int):
        print(value)
    elif isinstance(value, str) and SCALAR.match(value):
        print(value)
    else:
        raise Failure(f"{dotted} has an unexpected type or characters")
    return 0


def cmd_auth_header(path, dotted, dest):
    token = lookup(success_data(path), dotted)
    if not isinstance(token, str) or not TOKEN.match(token):
        raise Failure(f"{dotted} missing or malformed (value withheld)")
    write_private(dest, f"Authorization: Bearer {token}\n")
    return 0


def cmd_password(candidate, generated):
    if candidate:
        try:
            info = os.stat(candidate)
        except OSError as error:
            raise Failure("SMOKE_PASSWORD_FILE is not readable") from error
        if not stat.S_ISREG(info.st_mode):
            raise Failure("SMOKE_PASSWORD_FILE must be a regular file")
        if info.st_mode & 0o077:
            raise Failure("SMOKE_PASSWORD_FILE must not be accessible by group or others (chmod 600)")
        read_password(candidate)
        print(candidate)
    else:
        write_private(generated, secrets.token_urlsafe(24) + "\n")
        print(generated)
    return 0


def cmd_auth_body(dest, kind, tenant, login_key, password_path, device_id):
    body = {
        "tenant_id": tenant,
        "login_type": LOGIN_TYPE_EMAIL,
        "login_key": login_key,
        "password": read_password(password_path),
        "device_id": device_id,
        "device_name": "flare-message-flow-smoke",
    }
    if kind == "register":
        body["display_name"] = login_key.split("@", 1)[0]
    elif kind != "login":
        raise Failure("unknown auth body kind")
    write_json(dest, body)
    return 0


def cmd_friend_request_body(dest, target):
    write_json(dest, {"target_user_id": target, "message": "flare message-flow smoke",
                      "source": CONTACT_SOURCE_SEARCH})
    return 0


def cmd_respond_body(dest):
    write_json(dest, {"accept": True})
    return 0


def cmd_send_body(dest, conversation_id, recipient, client_msg_id, text):
    write_json(dest, {
        "conversation_id": conversation_id,
        "conversation_type": CONVERSATION_TYPE_SINGLE,
        "channel_id": recipient,
        "client_msg_id": client_msg_id,
        "message_type": MESSAGE_TYPE_CUSTOM,
        "content": {"text": text},
        "sync": False,
    })
    return 0


def cmd_cid(first, second):
    # Mirrors flare-core generate_single_chat_conversation_id:
    # SHA256("DM:v1:{min}:{max}")[:10] -> Crockford Base32 -> "1A" prefix.
    low, high = (first, second) if first <= second else (second, first)
    digest = hashlib.sha256(f"DM:v1:{low}:{high}".encode()).digest()[:10]
    bits = int.from_bytes(digest, "big")
    print("1A" + "".join(CROCKFORD[(bits >> shift) & 31] for shift in range(75, -1, -5)))
    return 0


def cmd_conversation_seq(path, conversation_id):
    conversations = success_data(path).get("conversations")
    if not isinstance(conversations, list):
        raise Failure("conversation list missing")
    for item in conversations:
        if isinstance(item, dict) and item.get("conversation_id") == conversation_id:
            seq = item.get("max_seq")
            if isinstance(seq, int) and not isinstance(seq, bool) and seq >= 0:
                print(seq)
            return 0
    return 0


def cmd_origin(dest, base_url, server_name, host_header, core_prefix, social_prefix):
    parts = urllib.parse.urlsplit(base_url)
    if parts.scheme not in ("https", "http") or not parts.hostname:
        raise Failure("SMOKE_BASE_URL must be an absolute https:// origin")
    if parts.username is not None or parts.password is not None:
        raise Failure("SMOKE_BASE_URL must not embed credentials")
    if parts.query or parts.fragment or parts.path not in ("", "/"):
        raise Failure("SMOKE_BASE_URL must be a bare origin; use the *_API_PREFIX settings for paths")
    try:
        port = parts.port
    except ValueError as error:
        raise Failure("SMOKE_BASE_URL has an invalid port") from error
    default_port = 443 if parts.scheme == "https" else 80
    port = port or default_port
    host = parts.hostname.lower()
    if parts.scheme == "http" and host not in LOOPBACK:
        raise Failure("plain http is only allowed for loopback targets; use https through Edge")
    if server_name and not DNS_NAME.match(server_name):
        raise Failure("SMOKE_SERVER_NAME must be a DNS name")
    if host_header and not HOST_HEADER.match(host_header):
        raise Failure("SMOKE_HOST_HEADER has unexpected characters")
    for name, prefix in (("SMOKE_CORE_API_PREFIX", core_prefix),
                         ("SMOKE_SOCIAL_API_PREFIX", social_prefix)):
        if not PREFIX.match(prefix) or "/../" in prefix + "/" or "/./" in prefix + "/":
            raise Failure(f"{name} must be empty or a plain /path")

    def bracket(value):
        return f"[{value}]" if ":" in value else value

    name = (server_name or host).lower()
    authority = bracket(name) + ("" if port == default_port else f":{port}")
    origin = f"{parts.scheme}://{authority}"
    connect_to = ""
    if name != host:
        connect_to = f"{bracket(name)}:{port}:{bracket(host)}:{port}"
    lines = [
        f"scheme={parts.scheme}",
        f"origin={origin}",
        f"connect_to={connect_to}",
        f"host_header={host_header}",
        f"core_base={origin}{core_prefix}",
        f"social_base={origin}{social_prefix}",
    ]
    write_private(dest, "\n".join(lines) + "\n")
    return 0


COMMANDS = {
    "check": lambda a: cmd_check(*a[:3], quiet=a[3:] == ["--quiet"]),
    "scalar": lambda a: cmd_scalar(*a),
    "auth-header": lambda a: cmd_auth_header(*a),
    "password": lambda a: cmd_password(*a),
    "auth-body": lambda a: cmd_auth_body(*a),
    "friend-request-body": lambda a: cmd_friend_request_body(*a),
    "respond-body": lambda a: cmd_respond_body(*a),
    "send-body": lambda a: cmd_send_body(*a),
    "cid": lambda a: cmd_cid(*a),
    "conversation-seq": lambda a: cmd_conversation_seq(*a),
    "origin": lambda a: cmd_origin(*a),
    "run-id": lambda a: print(secrets.token_hex(6)) or 0,
}


def main(argv):
    if not argv or argv[0] not in COMMANDS:
        print("smoke_message_flow: internal helper misuse", file=sys.stderr)
        return 2
    try:
        return COMMANDS[argv[0]](argv[1:])
    except Failure as error:
        print(f"smoke_message_flow: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
PY

py() {
    python3 "$helper" "$@"
}

py origin "$work_dir/origin.env" "$base_url" "$server_name" "$host_header_override" \
    "$core_prefix" "$social_prefix" || exit 1
scheme=""
origin=""
connect_to=""
host_header=""
core_base=""
social_base=""
while IFS='=' read -r key value; do
    case "$key" in
        scheme) scheme="$value" ;;
        origin) origin="$value" ;;
        connect_to) connect_to="$value" ;;
        host_header) host_header="$value" ;;
        core_base) core_base="$value" ;;
        social_base) social_base="$value" ;;
    esac
done < "$work_dir/origin.env"
[ -n "$scheme" ] && [ -n "$origin" ] || die "could not derive the request origin"

password_path="$(py password "$password_file" "$work_dir/generated-password")" || exit 1

# 所有请求共用：不走本机代理（避免令牌发往无关代理）、不跟随重定向、只允许
# 目标协议；鉴权头从私有文件读入，令牌不出现在 curl 的命令行参数里。
last_status=""
http_status() {
    local out="$1" method="$2" url="$3" auth="$4" body="$5" code
    local -a args
    args=(--silent --show-error --noproxy '*' --max-redirs 0 --proto "=$scheme"
        --connect-timeout 5 --max-time 30 -X "$method"
        -H 'Accept: application/json' -o "$out" -w '%{http_code}')
    if [ -n "$ca_file" ]; then args+=(--cacert "$ca_file"); fi
    if [ -n "$connect_to" ]; then args+=(--connect-to "$connect_to"); fi
    if [ -n "$host_header" ]; then args+=(-H "Host: $host_header"); fi
    if [ -n "$auth" ]; then args+=(-H "@$auth"); fi
    if [ -n "$body" ]; then
        args+=(-H 'Content-Type: application/json' --data-binary "@$body")
    fi
    rm -f "$out"
    : > "$out"
    code="$(curl "${args[@]}" "$url")" || code="000"
    printf '%s' "${code:-000}"
}

# api <step> <method> <url> <auth_file|""> <body_file|""> [--quiet]
# 响应写到 $work_dir/<step>.json；成功（HTTP 200 且 code=0）返回 0。
api() {
    local step="$1" method="$2" url="$3" auth="$4" body="$5" quiet="${6:-}"
    last_status="$(http_status "$work_dir/$step.json" "$method" "$url" "$auth" "$body")"
    if [ -n "$quiet" ]; then
        py check "$step" "$last_status" "$work_dir/$step.json" --quiet
    else
        py check "$step" "$last_status" "$work_dir/$step.json"
    fi
}

now() {
    date +%s
}

run_id="$(py run-id)" || exit 1

# 两个全新用户：Social 公共 API 注册，再显式登录，取登录返回的 Social 访问令牌
# （调 Social）与 IM 令牌（调 Core）。
register_and_login() {
    local who="$1" login_key registered logged_in
    login_key="smoke-${who}-${run_id}@e2e.invalid"
    py auth-body "$work_dir/register-$who.body" register "$tenant_id" "$login_key" \
        "$password_path" "smoke-${run_id}-${who}" || exit 1
    api "register-$who" POST "$social_base/api/v1/auth/register" "" "$work_dir/register-$who.body" \
        || die "Social registration failed (verification-code or invite-only tenants are not supported by this smoke)"
    registered="$(py scalar "$work_dir/register-$who.json" user_id)" || exit 1

    py auth-body "$work_dir/login-$who.body" login "$tenant_id" "$login_key" \
        "$password_path" "smoke-${run_id}-${who}" || exit 1
    api "login-$who" POST "$social_base/api/v1/auth/login" "" "$work_dir/login-$who.body" \
        || die "Social login failed"
    logged_in="$(py scalar "$work_dir/login-$who.json" user_id)" || exit 1
    [ "$logged_in" = "$registered" ] || die "Social login returned a different user than registration"
    py auth-header "$work_dir/login-$who.json" tokens.access_token "$work_dir/social-$who.auth" \
        || die "Social login returned no access token"
    py auth-header "$work_dir/login-$who.json" tokens.im_connect_token "$work_dir/im-$who.auth" \
        || die "Social login returned no IM token; Core requests need it"
    rm -f "$work_dir/register-$who.body" "$work_dir/login-$who.body"
    printf -v "user_$who" '%s' "$registered"
}

user_a=""
user_b=""
register_and_login a
register_and_login b

# 好友关系：A 发起、B 同意（对方开启免验证时服务端直接返回已通过）。
py friend-request-body "$work_dir/friend-request.body" "$user_b" || exit 1
api friend-request POST "$social_base/api/v1/relations/friend-requests" \
    "$work_dir/social-a.auth" "$work_dir/friend-request.body" || die "friend request failed"
request_status="$(py scalar "$work_dir/friend-request.json" status)" || exit 1
if [ "$request_status" != "2" ]; then
    request_id="$(py scalar "$work_dir/friend-request.json" request_id)" || exit 1
    py respond-body "$work_dir/friend-accept.body" || exit 1
    api friend-accept POST "$social_base/api/v1/relations/friend-requests/$request_id/respond" \
        "$work_dir/social-b.auth" "$work_dir/friend-accept.body" || die "friend request acceptance failed"
    accepted_status="$(py scalar "$work_dir/friend-accept.json" status)" || exit 1
    [ "$accepted_status" = "2" ] || die "friend request was not accepted (status=$accepted_status)"
fi

# PreSend 用同一个 can_send_message 判定；等关系读侧收敛后再发。
deadline=$(( $(now) + timeout_seconds ))
while :; do
    if api friendship GET "$social_base/api/v1/relations/friends/check/$user_b" \
        "$work_dir/social-a.auth" "" --quiet; then
        can_send="$(py scalar "$work_dir/friendship.json" can_send_message)" || can_send=""
        [ "$can_send" = "true" ] && break
    fi
    if [ "$(now)" -ge "$deadline" ]; then
        py check friendship "$last_status" "$work_dir/friendship.json" || true
        die "friendship did not allow sending within ${timeout_seconds}s"
    fi
    sleep 1
done

conversation_id="$(py cid "$user_a" "$user_b")" || exit 1
py send-body "$work_dir/send.body" "$conversation_id" "$user_b" "smoke-$run_id" \
    "flare message-flow smoke $run_id" || exit 1
api send POST "$core_base/api/v1/messages/send" "$work_dir/im-a.auth" "$work_dir/send.body" \
    || die "Core send through Edge failed"
send_success="$(py scalar "$work_dir/send.json" success)" || exit 1
[ "$send_success" = "true" ] || die "Core send returned success=false"
server_msg_id="$(py scalar "$work_dir/send.json" server_msg_id)" || exit 1
conversation_seq="$(py scalar "$work_dir/send.json" seq)" || exit 1
[[ "$conversation_seq" =~ ^[1-9][0-9]{0,18}$ ]] || die "Core send returned no positive conversation seq"

# 回读：收发双方的 Core 会话列表都要看到该会话，且 max_seq 不低于发送序号。
readback() {
    local who="$1" deadline seen
    deadline=$(( $(now) + timeout_seconds ))
    while :; do
        seen=""
        if api "conversations-$who" GET "$core_base/api/v1/conversations?limit=100" \
            "$work_dir/im-$who.auth" "" --quiet; then
            seen="$(py conversation-seq "$work_dir/conversations-$who.json" "$conversation_id")" || seen=""
        fi
        if [ -n "$seen" ] && [ "$seen" -ge "$conversation_seq" ]; then
            printf -v "readback_$who" '%s' "$seen"
            return 0
        fi
        if [ "$(now)" -ge "$deadline" ]; then
            py check "conversations-$who" "$last_status" "$work_dir/conversations-$who.json" || true
            die "Core conversation list for user $who did not reach seq $conversation_seq within ${timeout_seconds}s"
        fi
        sleep 1
    done
}

readback_b=""
readback_a=""
readback b
readback a

minimum_ledger_state="archive_persisted"
accepted_ledger_state_pattern="^(archive_persisted|storage_persisted|wal_cleaned|ack_published)$"
messages_row_count="skipped"
ledger_state="skipped"

# 持久化核对：在数据库容器内执行，只读事务 + 会话级只读默认值双重保证；
# 使用容器自身的管理账号环境，口令不经过宿主机。参数经 psql 变量安全引用。
if [ "$skip_db_check" = "0" ]; then
    cat > "$work_dir/durability.sql" <<'SQL'
BEGIN TRANSACTION READ ONLY;
SELECT
    (SELECT COUNT(*) FROM messages
      WHERE tenant_id = :'tenant_id'
        AND server_id = :'server_id'
        AND conversation_id = :'conversation_id'
        AND sender_id = :'sender_id'
        AND seq = CAST(:'seq' AS BIGINT))
    || '|' ||
    COALESCE((SELECT write_state FROM message_write_ledger
               WHERE tenant_id = :'tenant_id'
                 AND server_id = :'server_id'), '');
COMMIT;
SQL

    db_query() {
        # shellcheck disable=SC2016 # expanded by sh inside the container, not here
        "$container_runtime" exec -i \
            -e 'PGOPTIONS=-c default_transaction_read_only=on' \
            "$pg_container" \
            sh -c 'role="$1"; shift
if [ -z "$role" ]; then role="$POSTGRES_USER"; PGPASSWORD="${POSTGRES_PASSWORD:-}"; export PGPASSWORD; fi
exec psql -X -q -A -t -v ON_ERROR_STOP=1 -U "$role" -d "$1" \
    -v tenant_id="$2" -v server_id="$3" -v conversation_id="$4" -v sender_id="$5" -v seq="$6"' \
            sh "$pg_user" "$pg_database" "$tenant_id" "$server_msg_id" "$conversation_id" \
            "$user_a" "$conversation_seq" \
            < "$work_dir/durability.sql"
    }

    deadline=$(( $(now) + timeout_seconds ))
    durable_ready=0
    messages_row_count="unknown"
    ledger_state="unknown"
    while :; do
        if db_output="$(db_query 2> "$work_dir/db.err")"; then
            db_output="$(printf '%s' "$db_output" | tr -d '[:space:]')"
            messages_row_count="${db_output%%|*}"
            ledger_state="${db_output#*|}"
            if [ "${messages_row_count:-0}" -ge 1 ] 2>/dev/null \
                && [[ "$ledger_state" =~ $accepted_ledger_state_pattern ]]; then
                durable_ready=1
                break
            fi
        fi
        [ "$(now)" -lt "$deadline" ] || break
        sleep 1
    done
    if [ "$durable_ready" -ne 1 ]; then
        echo "server_msg_id=$server_msg_id" >&2
        echo "messages_row_count=${messages_row_count:-unknown}" >&2
        echo "ledger_write_state=${ledger_state:-unknown}" >&2
        if [ -s "$work_dir/db.err" ]; then
            echo "last database error:" >&2
            head -n 5 "$work_dir/db.err" >&2
        fi
        die "message did not reach durable storage ($minimum_ledger_state or later) within ${timeout_seconds}s"
    fi
fi

echo "flare_message_flow_smoke_report"
echo "mode=http"
echo "origin=$origin"
if [ -n "$connect_to" ]; then
    echo "connect_to=$connect_to"
fi
echo "tenant_id=$tenant_id"
echo "sender_user_id=$user_a"
echo "recipient_user_id=$user_b"
echo "conversation_id=$conversation_id"
echo "server_msg_id=$server_msg_id"
echo "conversation_seq=$conversation_seq"
echo "readback_recipient_max_seq=$readback_b"
echo "readback_sender_max_seq=$readback_a"
echo "messages_row_count=$messages_row_count"
echo "ledger_write_state=$ledger_state"
echo "minimum_ledger_write_state=$minimum_ledger_state"
echo "status=pass"
