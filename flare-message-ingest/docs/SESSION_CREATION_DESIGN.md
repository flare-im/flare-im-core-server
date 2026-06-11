# Message Ingest Conversation Ensure

## Boundary

`flare-message-ingest` owns conversation ensure for the message send path.

The production message path is:

```text
Client -> Gateway -> Route -> Message Ingest -> flare.im.message.main
                                  |
                                  +-> Conversation ensure
```

`flare-orchestrator` does not accept `MessageSendService.SendMessage` traffic and does not create conversations for message sends. It consumes `flare.im.message.main` after ingest and fans accepted messages/events out to storage and push streams.

## Why It Lives Here

Conversation records must exist before accepted messages are fanned out to storage and unread/projection updates. Keeping ensure in Message Ingest gives the send command a single authoritative write boundary:

- validate and normalize the message
- run PreSend/PostSend extensions
- allocate sequence and timeline metadata
- ensure the conversation exists
- write the final-message WAL
- publish the accepted envelope to `flare.im.message.main`
- return broker-accepted send ACK semantics

This keeps Orchestrator as a fanout/operation-event service instead of a second message ingestion owner.

## Modes

### Sync

Default mode. Message Ingest calls Conversation gRPC with an `ensure_conversation` request and blocks the send command on success or failure.

Use this when strong send-path semantics matter more than shaving a few milliseconds from accepted ACK latency.

### Async

Message Ingest publishes a `conversation.ensure` event. Conversation consumes it and creates or repairs the conversation idempotently.

Use this only when Conversation supports idempotent ensure consumption and downstream projections can tolerate the short eventual-consistency window.

## Configuration

- `session_creation_mode = "sync" | "async"` defaults to `sync`.
- `conversation_ensure_cache_capacity` controls the hot single-chat ensure cache.
- `conversation_ensure_cache_ttl_seconds` controls the hot single-chat ensure cache TTL.

## Invariants

- Message sends go to `MessageSendService.SendMessage` in `flare-message-ingest`.
- Operation events/actions go to `flare-orchestrator`.
- Orchestrator may consume `flare.im.message.main`, but it must not own message-send validation, sequence allocation, WAL, or conversation ensure.
- `conversation.ensure` payloads are protobuf `MqEnvelope(EventCustom)` messages, not legacy JSON envelopes.
