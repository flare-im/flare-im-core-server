# Flare Message Ingest Hook Extension Boundary

`flare-message-ingest` owns message send-time hooks.

Current extension phases:

- `pre_send`: validates or mutates the message draft before the command enters the durable pipeline
- `post_send`: observes the accepted message after it is published to the main queue

Runtime controls:

- tenant allowlist
- message type allowlist
- pre-send timeout and retry
- post-send timeout, retry, and fail-open/fail-closed mode

`flare-capability` may be injected as a HookPlugin target for send-time policy work. RTC/media signaling must use realtime/capability packet routing and plugin services instead of durable `Event` enrichment.

`flare-orchestrator` does not execute HookPlugin or CapabilityService calls. It consumes durable work and performs storage/push fanout only.
