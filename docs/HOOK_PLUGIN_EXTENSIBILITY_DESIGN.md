# Flare Hook / Plugin Extension Boundary

This document is the single authoritative description of where message
Hook/Plugin execution lives across `flare-message-ingest` and
`flare-orchestrator`. It replaces the two per-crate copies that had drifted
apart (each described only its own side of the boundary).

## Send-time hooks live in `flare-message-ingest`

`flare-message-ingest` owns message send-time hooks. There is one runtime, `flare-im-core-hooks`,
embedded in Ingest: the call chain, adapters, circuit breakers, time limits and execution evidence.

The one hook point is `message.pre_send`: it validates or rewrites the message draft before the
command enters the durable pipeline. Hooks are synchronous interception only; observing what
happened belongs to events.

Two configuration sources feed the chain, which runs application, platform, then deployment hooks:

- the deployment file, required policy, loaded at startup and fatal when broken;
- `hook_configs`, platform and application hooks, hot-loaded from PostgreSQL about once a second.

Runtime controls:

- tenant allowlist
- message type allowlist
- total pre-send budget, shared by the steps of the chain
- per-hook timeout, required flag and selector

`flare-capability` is the control plane only: configuration, validation, statistics, queries and
retention. It does not run hooks, so a Capability outage does not affect sending. RTC / media
signaling must use realtime / capability packet routing and plugin services instead of durable
`Event` enrichment.

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
