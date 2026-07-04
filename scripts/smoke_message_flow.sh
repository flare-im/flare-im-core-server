#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WORKSPACE_ROOT="$(cd "$PROJECT_ROOT/.." && pwd)"
proto_root="${SMOKE_PROTO_ROOT:-$WORKSPACE_ROOT}"

grpcurl_bin="${SMOKE_GRPCURL:-grpcurl}"
psql_bin="${SMOKE_PSQL:-psql}"
message_ingest_endpoint="${SMOKE_MESSAGE_INGEST_ENDPOINT:-127.0.0.1:50182}"
storage_reader_endpoint="${SMOKE_STORAGE_READER_ENDPOINT:-127.0.0.1:60083}"
postgres_url="${SMOKE_POSTGRES_URL:-postgres://flare:flare123@localhost:25432/flare2}"
tenant_id="${SMOKE_TENANT_ID:-0}"
sender_id="${SMOKE_SENDER_ID:-smoke-user-a}"
recipient_id="${SMOKE_RECIPIENT_ID:-smoke-user-b}"
timeout_seconds="${SMOKE_TIMEOUT_SECONDS:-30}"

expected_durability="SEND_ACK_DURABILITY_BROKER_ACCEPTED"
conversation_id="${SMOKE_CONVERSATION_ID:-single:${sender_id}:${recipient_id}}"

require_command() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required command: $1" >&2
        exit 127
    fi
}

require_command "$grpcurl_bin"
require_command "$psql_bin"

if [ ! -f "$proto_root/flare-grpc-proto/proto/message_service.proto" ]; then
    echo "missing proto file: $proto_root/flare-grpc-proto/proto/message_service.proto" >&2
    exit 1
fi

if [ ! -f "$proto_root/flare-grpc-proto/proto/storage_service.proto" ]; then
    echo "missing proto file: $proto_root/flare-grpc-proto/proto/storage_service.proto" >&2
    exit 1
fi

if command -v uuidgen >/dev/null 2>&1; then
    run_id="$(uuidgen | tr '[:upper:]' '[:lower:]')"
else
    run_id="$(date +%s)-$$"
fi

created_at_ms=$(( $(date +%s) * 1000 ))
request_file="$(mktemp "${TMPDIR:-/tmp}/flare-message-flow-request.XXXXXX")"
response_file="$(mktemp "${TMPDIR:-/tmp}/flare-message-flow-response.XXXXXX")"
error_file="$(mktemp "${TMPDIR:-/tmp}/flare-message-flow-error.XXXXXX")"
read_request_file="$(mktemp "${TMPDIR:-/tmp}/flare-message-flow-read-request.XXXXXX")"
read_response_file="$(mktemp "${TMPDIR:-/tmp}/flare-message-flow-read-response.XXXXXX")"
read_error_file="$(mktemp "${TMPDIR:-/tmp}/flare-message-flow-read-error.XXXXXX")"

cleanup() {
    rm -f \
        "$request_file" \
        "$response_file" \
        "$error_file" \
        "$read_request_file" \
        "$read_response_file" \
        "$read_error_file"
}
trap cleanup EXIT

cat > "$request_file" <<JSON
{
  "conversationId": "$conversation_id",
  "sync": false,
  "svid": "smoke",
  "message": {
    "conversationId": "$conversation_id",
    "clientMsgId": "smoke-client-$run_id",
    "senderId": "$sender_id",
    "source": "MESSAGE_SOURCE_USER",
    "createdAt": "$created_at_ms",
    "conversationType": "CONVERSATION_TYPE_SINGLE",
    "messageType": "MESSAGE_TYPE_TEXT",
    "channelId": "$recipient_id",
    "content": {
      "text": {
        "text": "flare smoke message $run_id"
      }
    },
    "status": "MESSAGE_STATUS_CREATED"
  }
}
JSON

if ! "$grpcurl_bin" \
    -plaintext \
    -import-path "$proto_root/flare-grpc-proto/proto" \
    -import-path "$proto_root/flare-proto/proto" \
    -proto "$proto_root/flare-grpc-proto/proto/message_service.proto" \
    -H "x-request-id: smoke-$run_id" \
    -H "x-tenant-id: $tenant_id" \
    -H "x-user-id: $sender_id" \
    -d @ \
    "$message_ingest_endpoint" \
    flare.message.v1.MessageSendService/SendMessage \
    < "$request_file" \
    > "$response_file" 2> "$error_file"; then
    echo "SendMessage smoke request failed." >&2
    cat "$error_file" >&2
    exit 1
fi

if ! grep -Eq '"success"[[:space:]]*:[[:space:]]*true' "$response_file"; then
    echo "SendMessage returned a non-success response:" >&2
    cat "$response_file" >&2
    exit 1
fi

if ! grep -q "$expected_durability" "$response_file"; then
    echo "SendMessage did not reach $expected_durability:" >&2
    cat "$response_file" >&2
    exit 1
fi

server_id="$(sed -n 's/.*"serverMsgId"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$response_file" | head -1)"
conversation_seq="$(sed -n 's/.*"conversationSeq"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$response_file" | head -1)"
if [ -z "$conversation_seq" ]; then
    conversation_seq="$(sed -n 's/.*"conversationSeq"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' "$response_file" | head -1)"
fi

if [ -z "$server_id" ] || [ -z "$conversation_seq" ]; then
    echo "SendMessage response did not include serverMsgId and conversationSeq:" >&2
    cat "$response_file" >&2
    exit 1
fi

sql_literal() {
    printf "%s" "$1" | sed "s/'/''/g"
}

tenant_sql="$(sql_literal "$tenant_id")"
server_sql="$(sql_literal "$server_id")"

query_message_count() {
    "$psql_bin" "$postgres_url" \
        -v ON_ERROR_STOP=1 \
        -Atc "SELECT COUNT(*) FROM messages WHERE tenant_id = '$tenant_sql' AND server_id = '$server_sql';" \
        | tr -d '[:space:]'
}

query_ledger_state() {
    "$psql_bin" "$postgres_url" \
        -v ON_ERROR_STOP=1 \
        -Atc "SELECT COALESCE((SELECT write_state FROM message_write_ledger WHERE tenant_id = '$tenant_sql' AND server_id = '$server_sql' ORDER BY updated_at DESC LIMIT 1), '');" \
        | tr -d '[:space:]'
}

deadline=$(( $(date +%s) + timeout_seconds ))
message_count="0"
ledger_state=""
durable_ready=0

while [ "$(date +%s)" -le "$deadline" ]; do
    if message_count="$(query_message_count 2>/dev/null)" \
        && ledger_state="$(query_ledger_state 2>/dev/null)"; then
        if [ "${message_count:-0}" -ge 1 ] && [ "$ledger_state" = "ack_published" ]; then
            durable_ready=1
            break
        fi
    fi

    sleep 1
done

if [ "$durable_ready" -ne 1 ]; then
    echo "message flow smoke timed out waiting for durable storage." >&2
    echo "server_msg_id=$server_id" >&2
    echo "messages_row_count=${message_count:-unknown}" >&2
    echo "ledger_write_state=${ledger_state:-unknown}" >&2
    echo "expected_ledger_write_state=ack_published" >&2
    exit 1
fi

cat > "$read_request_file" <<JSON
{
  "conversationId": "$conversation_id",
  "afterSeq": 0,
  "beforeSeq": 0,
  "limit": 100,
  "userId": "$recipient_id"
}
JSON

if ! "$grpcurl_bin" \
    -plaintext \
    -import-path "$proto_root/flare-grpc-proto/proto" \
    -import-path "$proto_root/flare-proto/proto" \
    -proto "$proto_root/flare-grpc-proto/proto/storage_service.proto" \
    -H "x-request-id: smoke-read-$run_id" \
    -H "x-tenant-id: $tenant_id" \
    -H "x-user-id: $recipient_id" \
    -d @ \
    "$storage_reader_endpoint" \
    flare.storage.v1.StorageReaderService/QueryMessagesBySeq \
    < "$read_request_file" \
    > "$read_response_file" 2> "$read_error_file"; then
    echo "StorageReader QueryMessagesBySeq smoke request failed." >&2
    cat "$read_error_file" >&2
    exit 1
fi

if ! grep -q "$server_id" "$read_response_file"; then
    echo "StorageReader did not return the sent server_msg_id:" >&2
    cat "$read_response_file" >&2
    exit 1
fi

storage_reader_messages_count="$(grep -o '"serverId"' "$read_response_file" | wc -l | tr -d '[:space:]')"

echo "flare_message_flow_smoke_report"
echo "message_ingest_endpoint=$message_ingest_endpoint"
echo "storage_reader_endpoint=$storage_reader_endpoint"
echo "tenant_id=$tenant_id"
echo "conversation_id=$conversation_id"
echo "server_msg_id=$server_id"
echo "conversation_seq=$conversation_seq"
echo "durability=$expected_durability"
echo "messages_row_count=$message_count"
echo "ledger_write_state=$ledger_state"
echo "storage_reader_messages_count=$storage_reader_messages_count"
echo "status=pass"
