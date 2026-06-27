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

## Environment

- `FLARE_DLQ_REPLAY_TENANT_ID`
- `FLARE_DLQ_REPLAY_LOG`
- `FLARE_DLQ_REPLAY_NATS_URL`
- `FLARE_DLQ_REPLAY_NATS_TIMEOUT_MS`
- `FLARE_DLQ_REPLAY_NATS_RETRIES`
- `FLARE_DLQ_REPLAY_NATS_RETRY_BACKOFF_MS`
- `FLARE_DLQ_REPLAY_KAFKA_BROKERS`
- `FLARE_DLQ_REPLAY_KAFKA_CLIENT_ID`
