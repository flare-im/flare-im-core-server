# Flare Hook / Plugin Extension Boundary

This document is the single authoritative description of where message
Hook/Plugin execution lives across `flare-message-ingest` and
`flare-orchestrator`. It replaces the two per-crate copies that had drifted
apart (each described only its own side of the boundary).

## Send-time hooks live in `flare-message-ingest`

`flare-message-ingest` owns message send-time hooks.

Extension phases:

- `pre_send`: validates or mutates the message draft before the command enters
  the durable pipeline.
- `post_send`: observes the accepted message after it is published to the main
  queue.

Runtime controls:

- tenant allowlist
- message type allowlist
- pre-send timeout and retry
- post-send timeout, retry, and fail-open / fail-closed mode

`flare-capability` may be injected as a HookPlugin target for send-time policy
work. RTC / media signaling must use realtime / capability packet routing and
plugin services instead of durable `Event` enrichment.

## `flare-orchestrator` is deterministic and does not run hooks

`flare-orchestrator` no longer owns message Hook or Plugin execution. Its
current responsibility is:

- consume the durable message main queue
- allocate and fan out durable event sequences
- publish storage and push fanout
- handle message operation events such as recall, edit, delete, pin, and read
  state

It processes already-accepted durable work and does **not** call external
HookPlugin or CapabilityService adapters during fanout. This keeps the
orchestrator deterministic.

Extension responsibility for the send-command path lives in
`flare-message-ingest`. Realtime RTC and media signaling live in the
signaling / capability / plugin services and must not be routed through durable
IM events.
