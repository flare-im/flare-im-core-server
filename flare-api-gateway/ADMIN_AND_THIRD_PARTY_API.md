# Flare Gateway Admin 与三方接入 API 设计

## 1. 架构方向

`flare-api-gateway` 和 `flare-admin-gateway` 都是轻量 API Gateway，不是业务服务，也不是认证中心。它们只负责 HTTP/gRPC 边界适配、上下文透传、稳定错误映射、限流、审计入口和文档暴露。

两类网关分工如下：

| 网关 | 主要职责 | 不应该承担 |
|------|----------|------------|
| `flare-signaling/gateway` | 长连接接入、连接状态、实时上下行、客户端 ACK、连接质量、AccessGateway gRPC 推送 | 管理后台 API、业务 BFF 聚合、HTTP 管理查询 |
| `flare-api-gateway` | 第三方 HTTP API、HTTP 到内部 gRPC typed proxy、OpenAPI 文档 | Admin API、token 签发、token 存储、IM 领域状态、业务系统规则、直接写 storage |
| `flare-admin-gateway` | 内网 Admin API、管理查询 typed facade、导出任务创建、运维只读快照、Admin OpenAPI 文档 | 管理后台页面、业务角色/审批、token 生命周期、IM 领域状态、直接写 storage |

认证和 token 能力必须下沉：

| 能力 | 所属层 |
|------|--------|
| token 签发、刷新、撤销、会话存储 | 业务认证系统或 `flare-server-core` auth 基础设施 |
| JWT/HMAC/OIDC/mTLS/provider hook 实现 | `flare-server-core` auth provider 或业务系统 |
| 入站身份恢复 | gateway 调用下沉的 auth provider，只得到标准 principal |
| 领域权限最终判断 | 下游 IM 服务、Capability、业务系统 Hook 或业务插件 |
| 管理端权限和审计 | Admin 权限服务或 Capability 管理面，下游记录审计事件 |

Gateway 可以做边界级认证，但不能拥有 token 生命周期，不能把业务角色、好友关系、群权限写在 gateway 内。

密钥配置可以作为下沉认证 provider 的实现细节，而不是 Admin API 的直接放行条件：

| 密钥用途 | 推荐位置 | Gateway 行为 |
|----------|----------|--------------|
| Core Gateway 调用业务 auth hook | `FLARE_CORE_GATEWAY_AUTH_HOOK_SECRET` | Core Gateway 发送给 hook，证明调用方是 Core Gateway |
| Admin Gateway 调用业务 auth hook | `FLARE_ADMIN_GATEWAY_AUTH_HOOK_SECRET` | Admin Gateway 发送给 hook，证明调用方是 Admin Gateway |
| 业务系统签发 JWT | 业务认证系统或 `trusted_token_issuers` | Provider 校验后返回 principal |
| 服务间 mTLS / service token | 网格、反向代理、业务 auth provider | Gateway 只消费认证结果 |
| `ADMIN_SECRET` 直接调用 Gateway | 不推荐 | 会让 Gateway 重新承担认证中心职责 |

## 2. API 面向对象

| 接入方 | 推荐入口 | 说明 |
|--------|----------|------|
| 终端 IM SDK | `flare-signaling/gateway` 长连接 | 实时收发、离线同步、ACK、重连恢复 |
| 业务服务端 | typed gRPC 优先，`flare-api-gateway` HTTP API 作为 facade | 高频可信内网调用使用 typed gRPC；外部、后台和低频任务使用 HTTP/OpenAPI |
| 管理后台 | `flare-admin-gateway` Admin API | 查询、审计、运维、强制操作 |
| 内部服务 | 内部 gRPC typed client | 只在可信网络和 mTLS/service token 下使用 |
| 外部三方 ISV | HTTP API 优先 | 不直接开放内部 gRPC，除非专门部署 gRPC facade |

## 3. 认证与上下文合同

### 3.1 入站 HTTP

所有受保护 HTTP API 使用：

```http
Authorization: Bearer <access_token>
x-tenant-id: tenant-1
x-request-id: idem-or-request-id
x-trace-id: trace-id
x-app-id: app-1
```

处理规则：

1. Gateway 调用下沉认证 provider 校验 token；业务无关 principal、scope 和 validator 合同位于 `flare-server-core::auth`。
2. provider 返回标准 principal：`user_id`、`tenant_id`、`device_id`、`app_id`、`scopes`、`expires_at`。
3. Gateway 将 principal 映射为内部上下文 header，不信任请求体里的 `user_id` 作为身份。
4. 下游服务继续做租户、成员、capability、hook 和领域权限校验。

Admin API 需要 provider 返回以下任一 scope：

- `admin_gateway:admin`
- `admin_gateway:admin:*`
- `core_gateway:admin`
- `core_gateway:admin:*`
- `gateway:admin`
- `flare:admin`
- `admin:*`
- `admin`

当前 `core_jwt` claims 不携带 scopes，因此默认不能访问 Admin API。业务端需要管理能力时，应使用 `http_hook` 或共享 `flare-server-core::auth` provider 返回 `admin_gateway:*` 管理 scope；`core_gateway:*` 是开发期迁移别名。

### 3.2 入站 Admin

Admin API 必须使用管理员或服务主体，不复用普通用户权限。

```http
Authorization: Bearer <admin_or_service_token>
x-tenant-id: tenant-1
x-actor-id: admin-1
x-request-id: request-id
x-audit-reason: investigate-message-complaint
```

可用性检查：

```http
GET /api/v1/admin/auth/check
Authorization: Bearer <admin_or_service_token>
x-tenant-id: tenant-1
x-actor-id: admin-1
x-request-id: request-id
```

Admin API 能力发现：

```http
GET /api/v1/admin/capabilities
Authorization: Bearer <admin_or_service_token>
x-tenant-id: tenant-1
x-request-id: request-id
```

`/auth/check` 只验证 Admin 调用边界，`/capabilities` 返回 Admin API 所需 scope、header、安全边界、已开放 endpoint 和业务自管事项。Admin Gateway 不提供管理后台页面，也不内置业务管理员、角色、审批或菜单系统。

Admin Gateway 只读运维接口：

```http
GET /api/v1/admin/gateway/health
GET /api/v1/admin/gateway/upstreams
GET /api/v1/admin/gateway/routes
GET /api/v1/admin/gateway/config
Authorization: Bearer <admin_or_service_token>
x-tenant-id: tenant-1
x-request-id: request-id
```

这些接口只返回 Gateway 本地管理面快照：

- `/gateway/health`：Gateway 管理面状态、route/upstream 数量，不主动探测下游。
- `/gateway/upstreams`：当前 gRPC route、静态 fallback 和超时配置。
- `/gateway/routes`：HTTP route 分组、认证要求、Admin 要求、下游归属。
- `/gateway/config`：脱敏配置快照，不输出 hook secret、token secret、数据库 URL、对象存储 key。

Admin 写操作必须满足：

- 有 `x-actor-id`。
- 有 `x-request-id` 或 `Idempotency-Key`。
- 有明确 reason，例如 `x-audit-reason`。
- 下游服务记录审计事件。
- 高风险操作二次确认应由管理后台或业务权限系统完成，gateway 不内置审批流。

### 3.3 出站 gRPC metadata

Gateway 访问下游 gRPC 时必须透传：

| metadata | 说明 |
|----------|------|
| `x-trace-id` | 链路追踪 |
| `x-request-id` | 幂等和排障 |
| `x-tenant-id` | 租户隔离 |
| `x-user-id` | 用户请求的认证主体 |
| `x-actor-id` | Admin 或代操作主体 |
| `x-app-id` | 开放平台应用 |
| `x-audit-reason` | 管理操作原因 |

## 4. 第三方 HTTP API

统一前缀：`/api/v1`。

| 分组 | API | 用途 | 状态 |
|------|-----|------|------|
| Message | `POST /api/v1/messages/send` | 服务端/机器人发消息 | 已接入 typed gRPC |
| Message | `POST /api/v1/messages/recall` | 撤回消息 | 已接入 typed gRPC |
| Message | `POST /api/v1/messages/read` | 标记单条已读 | 已接入 typed gRPC |
| Conversation | `GET /api/v1/conversations` | 会话列表 | 已接入 typed gRPC |
| Conversation | `GET /api/v1/conversations/participants` | 参与者分页 | 已接入 typed gRPC |
| Conversation | `POST /api/v1/conversations/participants/manage` | 参与者管理 | 已接入 typed gRPC，需 Admin/强权限 |
| Media | `/api/v1/medias/*` | 上传、预签名、引用、处理、对象管理 | 已接入 |
| Presence | `/api/v1/presence/*` | 在线状态查询、登出 | 已接入 |
| Capability | `/api/v1/capabilities/*` | 能力列表、授权、插件管理 | 待接入 |
| Storage Query | `POST /api/v1/admin/messages/query` | 管理端多维消息查询 | 已在 `flare-admin-gateway` 接入 storage reader typed facade |

第三方 HTTP 写请求要求：

- `Idempotency-Key` 或 `x-request-id` 必须稳定。
- 发消息必须有 `client_msg_id`，用于重复请求收敛。
- 不允许客户端传入 `sender_id` 覆盖认证主体。
- 媒体“消息附件”由消息路径创建引用；非消息媒体不由 core 自动归档或删除。

## 5. Admin API 设计

统一前缀：`/api/v1/admin`。Admin API 是管理面，不作为普通开放 API 暴露。

### 5.1 Gateway 运维

| API | 方法 | 用途 | 下游 |
|-----|------|------|------|
| `/auth/check` | GET | 校验业务端 Admin token 是否满足 Gateway Admin 调用条件 | gateway auth middleware |
| `/capabilities` | GET | 返回 Admin API 所需 scope、header、安全边界和已开放 endpoint | gateway contract |
| `/gateway/health` | GET | Gateway 管理面状态和 route/upstream 数量，不主动探测下游 | gateway contract |
| `/gateway/upstreams` | GET | 下游 gRPC route、fallback 和 timeout 快照 | gateway config |
| `/gateway/routes` | GET | 已注册 HTTP route 和保护状态 | gateway router |
| `/gateway/config` | GET | 脱敏配置快照 | gateway config |

配置快照必须脱敏：token secret、hook secret、数据库 URL、对象存储 key 不得返回明文。

### 5.2 消息管理

| API | 方法 | 用途 | 下游 |
|-----|------|------|------|
| `/messages/query` | POST | 多维查询消息，typed facade，拒绝无界扫描 | `StorageReaderService.SearchMessages` |
| `/messages/{message_id}` | GET | 单条消息详情 | `StorageReaderService.GetMessage` |
| `/messages/{message_id}/events` | GET | 消息事件链 | `StorageReaderService.QueryMessageEvents` |
| `/messages/export` | POST | 创建受审计的消息导出任务 | `StorageReaderService.ExportMessages` |
| `/messages/{message_id}/recall` | POST | 管理员撤回 | `MessageActionService.RecallMessage` |
| `/messages/{message_id}/delete` | POST | 管理员删除 | `MessageActionService.DeleteMessage` |

查询维度建议：

- `conversation_id`
- `sender_id`
- `channel_id`
- `message_id`
- `client_msg_id`
- `message_type`
- `conversation_type`
- `source`
- `status`
- `is_recalled`
- `time_range`
- `seq_range`

查询架构：

1. 业务后台只调用 Admin Gateway HTTP Admin API。
2. Admin Gateway 从 Admin token 和 `x-tenant-id` 建立上下文，不接受请求体覆盖 tenant。
3. `application::admin_messages` 将 typed HTTP 字段转换为 storage-reader 支持的过滤表达式。
4. Admin Gateway 调用 `StorageReaderService.SearchMessages`、`GetMessage`、`QueryMessageEvents` 或 `ExportMessages`，storage-reader 基于读模型和索引执行查询。
5. Admin Gateway 返回消息摘要、稳定索引字段、attributes、extension keys 和事件摘要，不直接展开二进制扩展内容。

性能保护：

- 至少需要一个索引过滤条件或时间范围，避免全租户扫描。
- `limit` 最大 500。
- 详情接口不存在时返回 `404`，事件链接口按 message_id 查询并限制分页。
- 导出必须指定 `conversation_id` 和时间范围；HTTP 只创建异步导出任务，Storage Reader 持久化 pending 任务到 `message_export_tasks`，不在请求内同步生成大文件。
- 返回按 storage-reader 当前读模型排序，不在 Gateway 里做二次大内存排序。
- 真实导出文件、对象存储落点和下载授权由后续 storage/export worker 负责。

### 5.3 会话管理

| API | 方法 | 用途 | 下游 |
|-----|------|------|------|
| `/conversations` | GET | 会话查询 | `ConversationReadService.ListConversations/SearchConversations` |
| `/conversations/{conversation_id}` | GET | 会话详情 | `ConversationReadService.GetConversationDetail` |
| `/conversations/{conversation_id}` | PATCH | 更新会话 | `ConversationManageService.UpdateConversation` |
| `/conversations/{conversation_id}/participants` | GET | 成员分页 | `ConversationReadService.ListConversationParticipants` |
| `/conversations/{conversation_id}/participants` | POST | 成员变更 | `ConversationManageService.ManageParticipants` |
| `/conversations/{conversation_id}/sync` | POST | 强制同步 | `ConversationManageService.ForceConversationSync` |

### 5.4 媒体管理

| API | 方法 | 用途 | 下游 |
|-----|------|------|------|
| `/media/files/{file_id}` | GET | 文件详情 | `MediaService.GetFileInfo` |
| `/media/files/{file_id}/references` | GET | 引用列表 | `MediaService.ListReferences` |
| `/media/files/{file_id}/acl` | PATCH | ACL 调整 | `MediaService.SetObjectAcl` |
| `/media/files/{file_id}/delete` | POST | 人工删除 | `MediaService.DeleteFile` |
| `/media/orphans/cleanup` | POST | 消息媒体孤儿清理 | `MediaService.CleanupOrphanedAssets` |

媒体原则：

- 相同文件应由 media service 做 hash/digest 归并和引用计数。
- 消息媒体引用由消息写入路径创建。
- 无引用消息媒体允许自动归档或清理。
- 非消息媒体不由 core 自动处理，必须由第三方业务主动删除。

### 5.5 在线与设备管理

| API | 方法 | 用途 | 下游 |
|-----|------|------|------|
| `/presence/users/{user_id}` | GET | 在线状态 | `OnlineService.GetUserPresence` |
| `/presence/users/{user_id}/devices` | GET | 设备列表 | `OnlineService.ListUserDevices` |
| `/presence/devices/{device_id}` | GET | 设备详情 | `OnlineService.GetDevice` |
| `/presence/devices/{device_id}/kick` | POST | 强制下线 | `OnlineService.KickDevice` |

强制下线必须记录 `x-actor-id` 和 `x-audit-reason`。

### 5.6 Capability 与插件管理

| API | 方法 | 用途 | 下游 |
|-----|------|------|------|
| `/capabilities` | GET | 能力列表 | `CapabilityService.ListCapabilities` |
| `/capabilities/users/{user_id}` | GET | 用户能力 | `CapabilityService.ListUserCapabilities` |
| `/capabilities/users/{user_id}/grant` | POST | 授权能力 | `CapabilityService.GrantUserCapability` |
| `/capabilities/users/{user_id}/revoke` | POST | 撤销能力 | `CapabilityService.RevokeUserCapability` |
| `/capabilities/tenants/{tenant_id}/switch` | PATCH | 租户能力开关 | `CapabilityService.SetTenantCapabilitySwitch` |
| `/plugins` | GET | 插件列表 | `CapabilityService.ListRegisteredPlugins` |
| `/plugins` | POST | 注册插件 endpoint | `CapabilityService.RegisterPluginEndpoint` |
| `/plugins/{plugin_id}` | DELETE | 注销插件 endpoint | `CapabilityService.DeregisterPluginEndpoint` |
| `/hooks` | GET/POST/PATCH/DELETE | Hook 配置管理 | `CapabilityService.Administer` |

Capability 管理不应该写死某个插件类型，SFU、机器人、风控、审核都通过 capability/plugin 合同扩展。

## 6. 三方 gRPC 使用设计

### 6.1 推荐策略

默认不把内部 gRPC 直接暴露给外部三方。原因：

- 内部 proto 变化频率高于 HTTP OpenAPI。
- gRPC metadata、mTLS、服务发现和权限更适合内网。
- 直接暴露内部服务会绕开 HTTP 层的限流、审计、错误稳定化和防误用保护。

如确实需要 gRPC，采用专门的 gRPC facade，而不是 generic proxy：

| 模式 | 适用对象 | 稳定性 |
|------|----------|--------|
| HTTP OpenAPI | 外部三方、业务后台 | 稳定公开合同 |
| gRPC facade | 高性能可信业务方 | 版本化公开 proto |
| 内部 typed gRPC | Flare 内部服务 | 内网合同 |
| Generic gRPC proxy | 运维调试 | 仅内网 allowlist，不公开 |

### 6.2 gRPC facade 建议服务

未来可以在 `flare-api-gateway/src/interface/grpc` 暴露一个轻量 facade：

```proto
service CoreGatewayPublicService {
  rpc SendMessage(SendMessageRequest) returns (SendMessageResponse);
  rpc RecallMessage(RecallMessageRequest) returns (RecallMessageResponse);
  rpc ListConversations(ListConversationsRequest) returns (ListConversationsResponse);
  rpc GetConversationDetail(GetConversationDetailRequest) returns (GetConversationDetailResponse);
  rpc GetFileInfo(GetFileInfoRequest) returns (GetFileInfoResponse);
  rpc GetUserPresence(GetUserPresenceRequest) returns (GetUserPresenceResponse);
}

service CoreGatewayAdminService {
  rpc QueryMessages(QueryMessagesRequest) returns (QueryMessagesResponse);
  rpc ExportMessages(ExportMessagesRequest) returns (ExportMessagesResponse);
  rpc ListUserDevices(ListUserDevicesRequest) returns (ListUserDevicesResponse);
  rpc KickDevice(KickDeviceRequest) returns (KickDeviceResponse);
  rpc ListCapabilities(ListCapabilitiesRequest) returns (ListCapabilitiesResponse);
}
```

Facade 规则：

- 使用公开 proto 包名和明确版本，例如 `flare.core_gateway.v1`。
- 不直接复用所有内部 proto，只暴露可承诺稳定的子集。
- metadata 与 HTTP header 语义保持一致。
- Admin service 必须有独立权限、审计和 allowlist。

### 6.3 内部 gRPC metadata 示例

```bash
grpcurl \
  -H 'authorization: Bearer <service-token>' \
  -H 'x-tenant-id: tenant-1' \
  -H 'x-user-id: user-1' \
  -H 'x-actor-id: admin-1' \
  -H 'x-request-id: request-1' \
  -H 'x-trace-id: trace-1' \
  127.0.0.1:50050 \
  flare.core_gateway.v1.CoreGatewayAdminService/QueryMessages
```

## 7. 错误、限流和审计

所有 HTTP API 返回 `ApiResponse<T>`。HTTP status 和 `ErrorCode` 同时表达错误。

| 场景 | HTTP |
|------|------|
| token 缺失或无效 | 401 |
| 已认证但无权限 | 403 |
| 请求参数错误 | 400 |
| 幂等冲突 | 409 |
| 限流 | 429 |
| 下游不可用 | 503 |
| 下游超时 | 504 |

Admin 审计事件至少包含：

- `tenant_id`
- `actor_id`
- `target_user_id` 或 `target_resource`
- `operation`
- `request_id`
- `trace_id`
- `reason`
- `before/after` 摘要，敏感字段脱敏

## 8. 实施顺序

P0：

1. 共享 `AuthenticatedPrincipal`、`TokenValidationRequest`、`TokenValidator` 和 `AuthError` 已下沉到 `flare-server-core::auth`；gateway 只保留 HTTP hook/JWT 适配。
2. Admin API 已先落地 `/api/v1/admin/auth/check` 和 `/api/v1/admin/capabilities`，用于业务端验证 Admin token、上下文和能力发现。
3. Admin Gateway 只读运维能力已落地：`/gateway/health`、`/gateway/upstreams`、`/gateway/routes`、`/gateway/config`。
4. Admin 消息存储查询已落地：`/messages/query`、`/messages/{message_id}`、`/messages/{message_id}/events`、`/messages/export`。
5. 所有 Admin 写操作增加 `x-actor-id`、`x-audit-reason`、idempotency 校验。
6. 给 OpenAPI 增加 public/admin 分组和 security scheme。

P1：

1. 接入 capability/plugin/hook 管理 API。
2. 接入 presence device list/kick。
3. gRPC facade 只暴露稳定 public/admin 子集。

P2：

1. Generic gRPC proxy 只作为内网运维调试功能，默认关闭。
2. Admin API 操作审计投递到专门审计流。
3. 根据三方 SDK 需求生成公开 HTTP/gRPC client。

## 9. 设计取舍

| 方案 | 优点 | 风险 | 决策 |
|------|------|------|------|
| Gateway 自己实现完整认证 | 接入快 | 网关变重，业务规则泄漏，难复用 | 不采用 |
| Gateway 调用下沉 auth provider | 轻量、可复用、可替换 | 需要清晰 principal 合同 | 采用 |
| 直接开放内部 gRPC | 性能好 | 合同不稳定，安全边界弱 | 默认不采用 |
| 单独 gRPC facade | 稳定、可治理 | 需要维护公开 proto | 三方高性能场景采用 |
| Generic gRPC proxy | 调试灵活 | 极高安全风险 | 仅内网 allowlist |
