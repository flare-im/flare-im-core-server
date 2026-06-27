#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-${BOOTSTRAP_SERVERS:-kafka:9092}}"
KAFKA_BIN_DIR="${KAFKA_BIN_DIR:-/opt/kafka/bin}"
WAIT_TIMEOUT_SECONDS="${KAFKA_WAIT_TIMEOUT_SECONDS:-120}"

MESSAGE_PARTITIONS="${FLARE_KAFKA_MESSAGE_PARTITIONS:-12}"
PUSH_PARTITIONS="${FLARE_KAFKA_PUSH_PARTITIONS:-12}"
REPLICATION_FACTOR="${FLARE_KAFKA_REPLICATION_FACTOR:-1}"
MIN_INSYNC_REPLICAS="${FLARE_KAFKA_MIN_INSYNC_REPLICAS:-1}"

MESSAGE_RETENTION_MS="${FLARE_KAFKA_MESSAGE_RETENTION_MS:-604800000}"
PUSH_RETENTION_MS="${FLARE_KAFKA_PUSH_RETENTION_MS:-86400000}"
RETRY_RETENTION_MS="${FLARE_KAFKA_RETRY_RETENTION_MS:-86400000}"
DLQ_RETENTION_MS="${FLARE_KAFKA_DLQ_RETENTION_MS:-1209600000}"

TOPICS_BIN="${KAFKA_BIN_DIR}/kafka-topics.sh"
BROKER_API_BIN="${KAFKA_BIN_DIR}/kafka-broker-api-versions.sh"

wait_for_kafka() {
  echo "Waiting for Kafka at ${BOOTSTRAP_SERVERS}..."
  for _ in $(seq 1 "${WAIT_TIMEOUT_SECONDS}"); do
    if "${BROKER_API_BIN}" --bootstrap-server "${BOOTSTRAP_SERVERS}" >/dev/null 2>&1; then
      echo "Kafka is ready."
      return 0
    fi
    sleep 1
  done
  echo "Kafka is not ready after ${WAIT_TIMEOUT_SECONDS}s." >&2
  return 1
}

ensure_topic() {
  local topic="$1"
  local partitions="$2"
  local retention_ms="$3"

  echo "Ensuring Kafka topic ${topic} partitions=${partitions} rf=${REPLICATION_FACTOR} min_isr=${MIN_INSYNC_REPLICAS} retention_ms=${retention_ms}"
  "${TOPICS_BIN}" \
    --bootstrap-server "${BOOTSTRAP_SERVERS}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor "${REPLICATION_FACTOR}" \
    --config "cleanup.policy=delete" \
    --config "compression.type=producer" \
    --config "min.insync.replicas=${MIN_INSYNC_REPLICAS}" \
    --config "retention.ms=${retention_ms}"
}

wait_for_kafka

# Message write path.
ensure_topic "flare.im.message.main" "${MESSAGE_PARTITIONS}" "${MESSAGE_RETENTION_MS}"
ensure_topic "flare.im.message.main.dlq" "${MESSAGE_PARTITIONS}" "${DLQ_RETENTION_MS}"
ensure_topic "flare.im.message.storage" "${MESSAGE_PARTITIONS}" "${MESSAGE_RETENTION_MS}"
ensure_topic "flare.im.message.events" "${MESSAGE_PARTITIONS}" "${MESSAGE_RETENTION_MS}"
ensure_topic "flare.im.conversation.update" "${MESSAGE_PARTITIONS}" "${MESSAGE_RETENTION_MS}"
ensure_topic "flare.im.conversation.ensure" "${MESSAGE_PARTITIONS}" "${MESSAGE_RETENTION_MS}"

# Future bounded retry topics. They are cheap and make failure handling deterministic
# once Kafka nack/retry is enabled.
ensure_topic "flare.im.message.retry.5s" "${MESSAGE_PARTITIONS}" "${RETRY_RETENTION_MS}"
ensure_topic "flare.im.message.retry.30s" "${MESSAGE_PARTITIONS}" "${RETRY_RETENTION_MS}"
ensure_topic "flare.im.message.retry.5m" "${MESSAGE_PARTITIONS}" "${RETRY_RETENTION_MS}"
ensure_topic "flare.im.message.main.retry.5s" "${MESSAGE_PARTITIONS}" "${RETRY_RETENTION_MS}"
ensure_topic "flare.im.message.storage.retry.5s" "${MESSAGE_PARTITIONS}" "${RETRY_RETENTION_MS}"

# Push path.
ensure_topic "flare.im.push.messages" "${PUSH_PARTITIONS}" "${PUSH_RETENTION_MS}"
ensure_topic "flare.im.push.events" "${PUSH_PARTITIONS}" "${PUSH_RETENTION_MS}"
ensure_topic "flare.im.push.acks" "${PUSH_PARTITIONS}" "${PUSH_RETENTION_MS}"
ensure_topic "flare.im.push.envelope" "${PUSH_PARTITIONS}" "${PUSH_RETENTION_MS}"
ensure_topic "flare.im.push.online" "${PUSH_PARTITIONS}" "${PUSH_RETENTION_MS}"
ensure_topic "flare.im.push.offline" "${PUSH_PARTITIONS}" "${PUSH_RETENTION_MS}"
ensure_topic "flare.im.push.dlq" "${PUSH_PARTITIONS}" "${DLQ_RETENTION_MS}"
ensure_topic "flare.im.push.retry.5s" "${PUSH_PARTITIONS}" "${RETRY_RETENTION_MS}"
ensure_topic "flare.im.push.retry.30s" "${PUSH_PARTITIONS}" "${RETRY_RETENTION_MS}"
ensure_topic "flare.im.push.retry.5m" "${PUSH_PARTITIONS}" "${RETRY_RETENTION_MS}"

echo "Kafka topic provisioning completed."
