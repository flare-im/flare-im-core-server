# Flare DLQ Replay

`flare-dlq-replay` is the first operational replay tool for Flare IM DLQ records.
It replays JSONL-exported DLQ payloads to a target Kafka topic or JetStream
subject through the shared `Producer` abstraction.

## Input

Each non-empty JSONL line is one replay record:

```json
{"source_topic":"flare.im.message.main.dlq","key":"conversation-a","payload_base64":"...","headers":{"x-trace-id":"trace-a"}}
```

`target_topic` can be set per record or supplied globally with `--target-topic`.
The payload is always base64 so exported protobuf bytes are replayed unchanged.

## Usage

```bash
cargo run -p flare-dlq-replay -- \
  --input dlq.jsonl \
  --target-topic flare.im.message.main \
  --dry-run

cargo run -p flare-dlq-replay -- \
  --backend nats \
  --input dlq.jsonl \
  --target-topic flare.im.message.main \
  --nats-url nats://127.0.0.1:24222

cargo run -p flare-dlq-replay -- \
  --backend kafka \
  --input dlq.jsonl \
  --target-topic flare.im.push.messages \
  --kafka-brokers 127.0.0.1:29092
```

The tool adds replay headers:

- `x-flare-dlq-replayed=true`
- `x-flare-dlq-replayed-at-ms=<timestamp>`
- `x-flare-dlq-source-topic=<source>` when present

Use `--limit` for drills and `--max-payload-bytes` to keep replay bounded.

## Replay safety

Consumers treat a replayed record like a first delivery; the replay headers do
not make them skip work already done. Check that the consumer behind a DLQ
subject tolerates a repeat before replaying it:

- `flare.im.dlq.conversation-ensure`: idempotent (an existing conversation only
  gets its missing participants added).
- `flare.im.dlq.conversation-read-receipt`: read receipts are idempotent (the
  read cursor only moves forward), but **message bodies are not**: the
  conversation's `last_message_seq` takes the larger value, while every member
  of a single chat or small group (up to
  `CONVERSATION_LARGE_CONVERSATION_PRECISE_UNREAD_THRESHOLD` members, default
  500) whose read cursor is below the message's seq gets one more persisted
  unread. Replay each such record exactly once; a record that reached the DLQ
  never counted, so one replay restores it, and a second replay counts it twice.
  An extra unread stays until the member reads the conversation to its latest
  message.

No idempotency key guards the unread increment: the only key, (tenant,
conversation, seq), would need a per-message record of what was counted, and
using `last_message_seq` as the guard would drop increments of messages that
were redelivered after later ones. See "重放死信（DLQ）" in
[`docs/06-testing-performance-and-operations.md`](../../docs/06-testing-performance-and-operations.md),
which also lists the consumers that have no DLQ on the Kafka backend.

## Environment

- `FLARE_DLQ_REPLAY_TENANT_ID`
- `FLARE_DLQ_REPLAY_LOG`
- `FLARE_DLQ_REPLAY_NATS_URL`
- `FLARE_DLQ_REPLAY_NATS_TIMEOUT_MS`
- `FLARE_DLQ_REPLAY_NATS_RETRIES`
- `FLARE_DLQ_REPLAY_NATS_RETRY_BACKOFF_MS`
- `FLARE_DLQ_REPLAY_KAFKA_BROKERS`
- `FLARE_DLQ_REPLAY_KAFKA_CLIENT_ID`
