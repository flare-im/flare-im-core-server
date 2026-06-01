# Flare HTTP-gRPC Gateway

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

核心原则：

- 网关只做 HTTP/JSON 到内部 gRPC 的边界适配，不承载消息顺序、好友关系、未读数等领域规则。
- 所有受保护 API 必须经过统一认证，并向下游透传 `x-trace-id`、`x-request-id`、`x-tenant-id`、`x-user-id`。
- 所有 JSON API 统一返回 `flare_server_core::http::ApiResponse<T>`。
- 下游 gRPC 错误必须映射为稳定的 HTTP status + `ErrorCode`，不能把内部错误裸露给客户端。

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

#### 标记消息已读
```http
POST /api/v1/messages/read
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
