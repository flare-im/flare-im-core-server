# Service Config Index

Each file owns exactly one `[services.<key>]` table. File names use kebab-case; table keys use snake_case.

| File | Table | Runtime package |
| --- | --- | --- |
| `access-gateway.toml` | `[services.access_gateway]` | `flare-signaling-gateway` |
| `admin-gateway.toml` | `[services.admin_gateway]` | `flare-admin-gateway` |
| `api-gateway.toml` | `[services.api_gateway]` | `flare-api-gateway` |
| `capability.toml` | `[services.capability]` | `flare-capability` |
| `conversation.toml` | `[services.conversation]` | `flare-conversation` |
| `media.toml` | `[services.media]` | `flare-media` |
| `message-ingest.toml` | `[services.message_ingest]` | `flare-message-ingest` |
| `message-orchestrator.toml` | `[services.message_orchestrator]` | `flare-orchestrator` |
| `push-proxy.toml` | `[services.push_proxy]` | `flare-push-proxy` |
| `push-server.toml` | `[services.push_server]` | `flare-push-server` |
| `push-worker.toml` | `[services.push_worker]` | `flare-push-worker` |
| `signaling-online.toml` | `[services.signaling_online]` | `flare-signaling-online` |
| `signaling-route.toml` | `[services.signaling_route]` | `flare-signaling-route` |
| `storage-reader.toml` | `[services.storage_reader]` | `flare-storage-reader` |
| `storage-writer.toml` | `[services.storage_writer]` | `flare-storage-writer` |
| `sync-orchestrator.toml` | `[services.sync_orchestrator]` | `flare-sync-orchestrator` |

Use this directory for service-owned runtime knobs only. Shared Redis, PostgreSQL, MQ, object storage, registry, logging, and hook profiles belong in `base.toml` or `shared/`.
