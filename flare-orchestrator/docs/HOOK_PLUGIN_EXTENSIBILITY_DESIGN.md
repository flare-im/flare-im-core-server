# Flare Orchestrator Extension Boundary

`flare-orchestrator` no longer owns message Hook or Plugin execution.

Current responsibility:

- consume the durable message main queue
- allocate and fan out durable event sequences
- publish storage and push fanout
- handle message operation events such as recall, edit, delete, pin, and read state

Extension responsibility now lives in `flare-message-ingest` for the send command path. Realtime RTC and media signaling live in signaling/capability/plugin services and must not be routed through durable IM events.

This boundary keeps orchestrator deterministic: it processes already accepted durable work and does not call external HookPlugin or CapabilityService adapters during fanout.
