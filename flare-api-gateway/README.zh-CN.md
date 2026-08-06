# Flare HTTP-gRPC Gateway

[English](README.md) · 中文

高性能 HTTP → gRPC 网关,用于 IM 系统的 RESTful API 暴露。

## 架构

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

## 技术栈

- **语言**: Rust 2024 Edition (1.94.0)
- **HTTP 框架**: Axum
- **gRPC 客户端**: Tonic
- **异步运行时**: Tokio
- **中间件**: Tower, Tower-HTTP
- **序列化**: Serde, Serde JSON
- **错误处理**: Thiserror, Anyhow
- **追踪**: Tracing, Tracing-Subscriber
- **OpenAPI**: Utoipa, Utoipa-Swagger-UI

## 功能特性

✅ RESTful HTTP API 暴露
✅ JSON 请求/响应
✅ 参数校验
✅ gRPC 服务转发
✅ 统一错误处理
✅ 链路追踪 (Tracing)
✅ 请求超时
✅ 限流支持
✅ OpenAPI 文档 (Swagger UI)
✅ CORS 支持
✅ 健康检查

## 网关规范

生产级能力边界、安全认证、错误映射、统一返回格式、限流与观测规范见 [`GATEWAY_SPEC.md`](./GATEWAY_SPEC.md)。
Rust/gRPC 代理层、typed client、上下文透传和后续接入路线见 [`GRPC_PROXY_DESIGN.md`](./GRPC_PROXY_DESIGN.md)。
Admin API 已拆分到独立内网服务 `flare-admin-gateway`；三方 HTTP API、Admin API 和认证下沉设计见 [`ADMIN_AND_THIRD_PARTY_API.md`](./ADMIN_AND_THIRD_PARTY_API.md)，Admin 服务使用文档见 [`../flare-admin-gateway/README.md`](../flare-admin-gateway/README.md)。

核心原则：

- 网关只做 HTTP/JSON 到内部 gRPC 的边界适配，不承载消息顺序、好友关系、未读数等领域规则。
- 网关不签发、不刷新、不存储 token；受保护 API 调用下沉认证 provider 恢复 principal，并向下游透传 `x-trace-id`、`x-request-id`、`x-tenant-id`、`x-user-id`。
- 所有 JSON API 统一返回 `flare_server_core::http::ApiResponse<T>`。
- 下游 gRPC 错误必须映射为稳定的 HTTP status + `ErrorCode`，不能把内部错误裸露给客户端。

## 认证接入点

API Gateway 默认使用 `core_jwt` 模式委托共享 auth provider 校验 token。配置来自 `config/services/api-gateway.toml` 的 `token_secret`、`token_issuer`、`token_ttl_seconds`，并支持 `trusted_token_issuers` 信任业务系统签发的 JWT。Gateway 不提供 token 签发、刷新、撤销或存储 API。业务系统需要完全自定义登录态时，可设置：

```bash
FLARE_API_GATEWAY_AUTH_MODE=http_hook
FLARE_API_GATEWAY_AUTH_HOOK_URL=http://127.0.0.1:8088/internal/auth/validate
FLARE_API_GATEWAY_AUTH_HOOK_TIMEOUT_MS=800
FLARE_API_GATEWAY_AUTH_HOOK_SECRET=change-me
FLARE_API_GATEWAY_AUTH_HOOK_SECRET_HEADER=x-flare-auth-hook-secret
```

Hook 请求体：

```json
{
  "token": "bearer-token",
  "trace_id": "trace-id",
  "request_id": "request-id",
  "path": "/api/v1/messages/send",
  "method": "POST"
}
```

Hook 成功响应：

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

Hook 返回 `401`/`403` 或 `{"active": false}` 时，Gateway 对客户端返回 `401`；Hook 不可达、超时、返回非 2xx 或响应合同不完整时，Gateway 返回 `503`，避免把业务认证系统故障误判为用户凭证错误。

Admin API 不再由 `flare-api-gateway` public 进程承载，统一由 `flare-admin-gateway` 独立内网进程提供。业务端需要管理能力时，应由业务认证系统通过 `http_hook` 或共享 `flare-server-core::auth` provider 返回 `admin_gateway:admin` / `admin_gateway:admin:*` scope。密钥配置可以用于业务认证系统内部，例如 hook secret、业务 JWT issuer secret 或 mTLS，不建议在 Gateway 配置 `ADMIN_SECRET` 直接放行。

Admin 认证检查：

```http
GET /api/v1/admin/auth/check
Authorization: Bearer <admin_or_service_token>
x-tenant-id: tenant-1
x-actor-id: admin-1
x-request-id: request-1
```

Admin API 能力发现：

```http
GET /api/v1/admin/capabilities
Authorization: Bearer <admin_or_service_token>
x-tenant-id: tenant-1
x-request-id: request-1
```

该接口返回 Admin API 所需 scope、header、安全边界和已开放 endpoint，方便业务后台或三方服务端接入前做自动校验。Admin Gateway 只提供 API 边界，不提供管理后台页面、业务管理员、角色审批或菜单系统。

Admin Gateway 只读运维接口：

```http
GET /api/v1/admin/gateway/health
GET /api/v1/admin/gateway/upstreams
GET /api/v1/admin/gateway/routes
GET /api/v1/admin/gateway/config
Authorization: Bearer <admin_or_service_token>
x-tenant-id: tenant-1
x-request-id: request-1
```

`/gateway/config` 只返回脱敏快照；hook secret、token secret、数据库 URL、对象存储 key 等敏感值不得明文输出。`/gateway/health` 是 Gateway 自身管理面健康，不主动向下游发起探测，避免管理查询影响低延迟主链路。

Admin 消息多维查询：

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

该接口是 Gateway typed facade，会转换为 `StorageReaderService.SearchMessages`。为保护消息存储查询性能，至少需要一个索引过滤条件或时间范围，`limit` 最大 500；返回消息摘要和扩展 key，不直接展开二进制扩展内容。

Admin 消息详情和事件链：

```http
GET /api/v1/admin/messages/msg-1
GET /api/v1/admin/messages/msg-1/events?event_types=1,2,8&limit=100
Authorization: Bearer <admin_or_service_token>
x-tenant-id: tenant-1
x-request-id: request-1
```

详情接口调用 `StorageReaderService.GetMessage`，不存在时返回 `404`。事件链接口调用 `StorageReaderService.QueryMessageEvents`，`limit` 最大 500，`event_types` 使用逗号分隔的 proto event type 整数；响应只返回事件索引摘要，不直接展开完整 payload。

Admin 消息导出任务：

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

导出接口调用 `StorageReaderService.ExportMessages` 创建异步任务，必须指定 `conversation_id` 和时间范围，避免管理端触发无界大扫描。Storage Reader 会将 pending 任务写入 `message_export_tasks`；真实文件生成、对象存储落点和下载授权由后续 storage/export worker 负责。

Admin 写操作还必须包含：

```http
x-audit-reason: user-complaint-investigation
idempotency-key: stable-admin-operation-id
```

## 快速开始

### 1. 配置环境变量

```bash
cp .env.example .env
# 编辑 .env 文件,配置 gRPC 服务地址
```

### 2. 运行服务

```bash
cargo run --release
```

### 3. 访问 API 文档

打开浏览器访问: http://localhost:8080/swagger-ui/

## API 接口

### MediaService

#### 生成上传 URL
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

#### 获取文件 URL
```http
POST /api/v1/medias/file-url
Content-Type: application/json

{
  "file_id": "file-123",
  "expires_in": 3600,
  "download": false
}
```

#### 获取文件信息
```http
GET /api/v1/medias/file-info?file_id=file-123
```

#### 删除文件
```http
DELETE /api/v1/medias/file
Content-Type: application/json

{
  "file_id": "file-123",
  "hard_delete": false
}
```

### MessageService

#### 发送消息
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

#### 撤回消息
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

#### 获取会话列表
```http
GET /api/v1/conversations?user_id=user-123&page=1&page_size=20
Authorization: Bearer <token>
```

## 项目结构

```
src/
├── config/              # 配置管理
│   ├── mod.rs
│   └── settings.rs
├── error/               # 错误处理
│   ├── mod.rs
│   └── gateway_error.rs
├── context/             # 上下文
│   ├── mod.rs
│   └── ctx.rs
├── domain/              # 领域层(预留)
│   └── mod.rs
├── application/         # 应用层
│   ├── mod.rs
│   └── handler/
│       ├── mod.rs
│       └── media.rs
├── infrastructure/      # 基础设施层
│   ├── mod.rs
│   └── grpc/
│       ├── mod.rs
│       └── media_client.rs
├── interface/           # 接口层
│   ├── mod.rs
│   ├── grpc/           # gRPC 接口(预留)
│   │   └── mod.rs
│   └── http/           # HTTP 接口
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

## 开发指南

### 添加新的 API 接口

1. 在 `interface/http/response.rs` 中定义请求/响应模型
2. 在 `interface/http/handler.rs` 中实现 Handler
3. 在 `interface/http/router.rs` 中注册路由
4. 更新 OpenAPI 文档注解

### 添加新的 gRPC 服务

1. 在 `infrastructure/grpc/` 中添加客户端封装
2. 在 `GrpcClients` 中注册新客户端
3. 在 Handler 中调用新服务

## 测试

```bash
# 运行单元测试
cargo test

# 运行集成测试
cargo test --test integration
```

## 监控

- **健康检查**: `GET /health`
- **Prometheus 指标**: (待实现)
- **链路追踪**: 通过 Tracing 支持

## 许可证

Copyright © 2024 Flare IM
