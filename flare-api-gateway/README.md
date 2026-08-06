# Flare HTTP-gRPC Gateway

English · [中文](README.zh-CN.md)

A high-performance HTTP → gRPC gateway for exposing the IM system's RESTful API.

## Architecture

```
┌─────────────┐
│   HTTP      │  RESTful API
│   Client    │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────┐
│  Flare HTTP-gRPC Gateway            │
│  ┌───────────────────────────────┐  │
│  │ Interface Layer (HTTP)        │  │
│  │ - Handler                     │  │
│  │ - Middleware (Auth/Trace)     │  │
│  │ - Router                      │  │
│  └───────────────┬───────────────┘  │
│                  │                   │
│  ┌───────────────▼───────────────┐  │
│  │ Application Layer             │  │
│  │ - Request Validation          │  │
│  │ - Context Propagation         │  │
│  └───────────────┬───────────────┘  │
│                  │                   │
│  ┌───────────────▼───────────────┐  │
│  │ Infrastructure Layer          │  │
│  │ - gRPC Client Manager         │  │
│  │ - Error Mapping               │  │
│  └───────────────────────────────┘  │
└─────────────┬───────────────────────┘
              │
              ▼
    ┌─────────────────┐
    │  gRPC Services  │
    │  - MediaService │
    │  - MessageService│
    │  - Conversation  │
    └─────────────────┘
```

## Tech Stack

- **Language**: Rust 2024 Edition (1.94.0)
- **HTTP framework**: Axum
- **gRPC client**: Tonic
- **Async runtime**: Tokio
- **Middleware**: Tower, Tower-HTTP
- **Serialization**: Serde, Serde JSON
- **Error handling**: Thiserror, Anyhow
- **Tracing**: Tracing, Tracing-Subscriber
- **OpenAPI**: Utoipa, Utoipa-Swagger-UI

## Features

✅ RESTful HTTP API exposure
✅ JSON request/response
✅ Parameter validation
✅ gRPC service forwarding
✅ Unified error handling
✅ Distributed tracing (Tracing)
✅ Request timeout
✅ Rate limiting support
✅ OpenAPI documentation (Swagger UI)
✅ CORS support
✅ Health check

## Gateway specification

For the production-grade capability boundary, security authentication, error mapping, unified response format, rate limiting, and observability specification, see [`GATEWAY_SPEC.md`](./GATEWAY_SPEC.md).
For the Rust/gRPC proxy layer, typed client, context propagation, and subsequent integration roadmap, see [`GRPC_PROXY_DESIGN.md`](./GRPC_PROXY_DESIGN.md).
The Admin API has been split out into the standalone internal service `flare-admin-gateway`; for the third-party HTTP API, the Admin API, and the authentication offloading design, see [`ADMIN_AND_THIRD_PARTY_API.md`](./ADMIN_AND_THIRD_PARTY_API.md), and for the Admin service usage documentation, see [`../flare-admin-gateway/README.md`](../flare-admin-gateway/README.md).

Core principles:

- The gateway only performs boundary adaptation from HTTP/JSON to internal gRPC; it does not carry domain rules such as message ordering, friend relationships, or unread counts.
- The gateway does not issue, refresh, or store tokens; protected API calls offload to an authentication provider to restore the principal, and propagate `x-trace-id`, `x-request-id`, `x-tenant-id`, and `x-user-id` downstream.
- All JSON APIs uniformly return `flare_server_core::http::ApiResponse<T>`.
- Downstream gRPC errors must be mapped to a stable HTTP status + `ErrorCode`; internal errors must not be exposed to the client.

## Authentication integration points

The API Gateway uses the `core_jwt` mode by default to delegate token validation to a shared auth provider. Its configuration comes from `token_secret`, `token_issuer`, and `token_ttl_seconds` in `config/services/api-gateway.toml`, and it supports `trusted_token_issuers` to trust JWTs issued by business systems. The Gateway does not provide token issuance, refresh, revocation, or storage APIs. When a business system needs a fully customized login state, it can set:

```bash
FLARE_API_GATEWAY_AUTH_MODE=http_hook
FLARE_API_GATEWAY_AUTH_HOOK_URL=http://127.0.0.1:8088/internal/auth/validate
FLARE_API_GATEWAY_AUTH_HOOK_TIMEOUT_MS=800
FLARE_API_GATEWAY_AUTH_HOOK_SECRET=change-me
FLARE_API_GATEWAY_AUTH_HOOK_SECRET_HEADER=x-flare-auth-hook-secret
```

Hook request body:

```json
{
  "token": "bearer-token",
  "trace_id": "trace-id",
  "request_id": "request-id",
  "path": "/api/v1/messages/send",
  "method": "POST"
}
```

Hook success response:

```json
{
  "active": true,
  "user_id": "user-1",
  "tenant_id": "tenant-1",
  "device_id": "device-1",
  "app_id": "business-console",
  "expires_at": 1780000000,
  "scopes": ["message:send", "admin_gateway:admin:*"],
  "metadata": {
    "source": "business-auth"
  }
}
```

When the hook returns `401`/`403` or `{"active": false}`, the Gateway returns `401` to the client. When the hook is unreachable, times out, returns a non-2xx status, or returns an incomplete response contract, the Gateway returns `503`, to avoid mistaking a failure of the business authentication system for an invalid user credential.

The Admin API is no longer carried by the `flare-api-gateway` public process; it is uniformly served by the standalone internal `flare-admin-gateway` process. When the business side needs management capabilities, the business authentication system should return the `admin_gateway:admin` / `admin_gateway:admin:*` scopes via `http_hook` or the shared `flare-server-core::auth` provider. Secret configuration can be used internally within the business authentication system, for example a hook secret, a business JWT issuer secret, or mTLS; configuring `ADMIN_SECRET` on the Gateway to directly grant access is not recommended.

Admin authentication check:

```http
GET /api/v1/admin/auth/check
Authorization: Bearer <admin_or_service_token>
x-tenant-id: tenant-1
x-actor-id: admin-1
x-request-id: request-1
```

Admin API capability discovery:

```http
GET /api/v1/admin/capabilities
Authorization: Bearer <admin_or_service_token>
x-tenant-id: tenant-1
x-request-id: request-1
```

This endpoint returns the scopes, headers, security boundaries, and already-exposed endpoints required by the Admin API, making it easy for a business console or third-party server to perform automatic validation before integrating. The Admin Gateway only provides an API boundary; it does not provide an admin console UI, business administrators, role approvals, or a menu system.

Admin Gateway read-only operational endpoints:

```http
GET /api/v1/admin/gateway/health
GET /api/v1/admin/gateway/upstreams
GET /api/v1/admin/gateway/routes
GET /api/v1/admin/gateway/config
Authorization: Bearer <admin_or_service_token>
x-tenant-id: tenant-1
x-request-id: request-1
```

`/gateway/config` returns only a redacted snapshot; sensitive values such as the hook secret, token secret, database URL, and object-storage key must not be output in plaintext. `/gateway/health` reflects the health of the Gateway's own management plane; it does not actively probe downstream, to avoid management queries impacting the low-latency main path.

Admin multi-dimensional message query:

```http
POST /api/v1/admin/messages/query
Authorization: Bearer <admin_or_service_token>
x-tenant-id: tenant-1
x-request-id: request-1
Content-Type: application/json

{
  "conversation_id": "conv-1",
  "sender_id": "user-1",
  "start_time": 1780000000000,
  "end_time": 1780003600000,
  "limit": 100
}
```

This endpoint is a Gateway typed facade that translates into `StorageReaderService.SearchMessages`. To protect message-storage query performance, at least one index filter condition or a time range is required, and `limit` is capped at 500; it returns message summaries and extension keys, and does not directly expand binary extension content.

Admin message detail and event chain:

```http
GET /api/v1/admin/messages/msg-1
GET /api/v1/admin/messages/msg-1/events?event_types=1,2,8&limit=100
Authorization: Bearer <admin_or_service_token>
x-tenant-id: tenant-1
x-request-id: request-1
```

The detail endpoint calls `StorageReaderService.GetMessage` and returns `404` when the message does not exist. The event-chain endpoint calls `StorageReaderService.QueryMessageEvents`; `limit` is capped at 500, and `event_types` is a comma-separated list of proto event-type integers; the response returns only event index summaries and does not directly expand the full payload.

Admin message export task:

```http
POST /api/v1/admin/messages/export
Authorization: Bearer <admin_or_service_token>
x-tenant-id: tenant-1
x-actor-id: admin-1
x-audit-reason: compliance-export
idempotency-key: export-conv-1-1780000000000
Content-Type: application/json

{
  "conversation_id": "conv-1",
  "start_time": 1780000000000,
  "end_time": 1780003600000,
  "sender_id": "user-1"
}
```

The export endpoint calls `StorageReaderService.ExportMessages` to create an asynchronous task. It must specify a `conversation_id` and a time range, to avoid the management side triggering an unbounded large scan. The Storage Reader writes pending tasks into `message_export_tasks`; the actual file generation, object-storage location, and download authorization are handled by a subsequent storage/export worker.

Admin write operations must also include:

```http
x-audit-reason: user-complaint-investigation
idempotency-key: stable-admin-operation-id
```

## Quick start

### 1. Configure environment variables

```bash
cp .env.example .env
# Edit the .env file to configure the gRPC service addresses
```

### 2. Run the service

```bash
cargo run --release
```

### 3. Access the API documentation

Open in a browser: http://localhost:8080/swagger-ui/

## API endpoints

### MediaService

#### Generate upload URL
```http
POST /api/v1/medias/upload-url
Content-Type: application/json

{
  "bucket": "media-bucket",
  "object_key": "uploads/test.jpg",
  "mime_type": "image/jpeg",
  "expected_size": 102400,
  "expires_in": 3600
}
```

#### Get file URL
```http
POST /api/v1/medias/file-url
Content-Type: application/json

{
  "file_id": "file-123",
  "expires_in": 3600,
  "download": false
}
```

#### Get file info
```http
GET /api/v1/medias/file-info?file_id=file-123
```

#### Delete file
```http
DELETE /api/v1/medias/file
Content-Type: application/json

{
  "file_id": "file-123",
  "hard_delete": false
}
```

### MessageService

#### Send message
```http
POST /api/v1/messages/send
Content-Type: application/json
Authorization: Bearer <token>

{
  "conversation_id": "private:user1:user2",
  "content": {"text": "Hello"},
  "message_type": 1
}
```

#### Recall message
```http
POST /api/v1/messages/recall
Content-Type: application/json
Authorization: Bearer <token>

{
  "conversation_id": "private:user1:user2",
  "message_id": "msg-123"
}
```

### ConversationService

#### Get conversation list
```http
GET /api/v1/conversations?user_id=user-123&page=1&page_size=20
Authorization: Bearer <token>
```

## Project structure

```
src/
├── config/              # Configuration management
│   ├── mod.rs
│   └── settings.rs
├── error/               # Error handling
│   ├── mod.rs
│   └── gateway_error.rs
├── context/             # Context
│   ├── mod.rs
│   └── ctx.rs
├── domain/              # Domain layer (reserved)
│   └── mod.rs
├── application/         # Application layer
│   ├── mod.rs
│   └── handler/
│       ├── mod.rs
│       └── media.rs
├── infrastructure/      # Infrastructure layer
│   ├── mod.rs
│   └── grpc/
│       ├── mod.rs
│       └── media_client.rs
├── interface/           # Interface layer
│   ├── mod.rs
│   ├── grpc/           # gRPC interface (reserved)
│   │   └── mod.rs
│   └── http/           # HTTP interface
│       ├── mod.rs
│       ├── handler.rs
│       ├── response.rs
│       ├── router.rs
│       └── middleware/
│           ├── mod.rs
│           ├── auth.rs
│           ├── tracing.rs
│           ├── rate_limit.rs
│           └── timeout.rs
├── lib.rs
└── main.rs
```

## Development guide

### Adding a new API endpoint

1. Define the request/response models in `interface/http/response.rs`
2. Implement the Handler in `interface/http/handler.rs`
3. Register the route in `interface/http/router.rs`
4. Update the OpenAPI documentation annotations

### Adding a new gRPC service

1. Add a client wrapper under `infrastructure/grpc/`
2. Register the new client in `GrpcClients`
3. Call the new service in the Handler

## Testing

```bash
# Run unit tests
cargo test

# Run integration tests
cargo test --test integration
```

## Monitoring

- **Health check**: `GET /health`
- **Prometheus metrics**: (to be implemented)
- **Distributed tracing**: supported via Tracing

## License

Copyright © 2024 Flare IM
