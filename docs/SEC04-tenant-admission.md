# SEC-04 租户运行时状态执行

Core 的 `tenants` 投影是数据面判定源，进程内按租户缓存，默认刷新间隔 1 秒。`active` 可接入；`suspended`、`deleting` 一律拒绝。`strict` 模式下无投影或回源失败也拒绝，且没有投影数据库时服务启动失败；`lenient` 仅供 Core-only/开发兼容，已知停用租户仍被拒绝。生产 Compose 对接入网关、API Gateway、MessageIngest 显式配置 `strict`。

| 入口 | 判定位置 | 停用时结果 |
| --- | --- | --- |
| WS/QUIC 新连接 | Access Gateway 验证 token 后 | `TENANT_UNAVAILABLE` |
| HTTP 消息/会话/媒资/在线 API | API Gateway Bearer 验证后、下游调用前 | 403 |
| 本地 token 签发 | App 凭据或开发开关验证后、签发前 | 403 |
| 本地 token 刷新 | 验证签名并从原 token 取租户后、轮换前 | 403 |
| gRPC 消息摄入 | 已验证上下文之后、幂等重放/限流/WAL/Hook 前 | `PermissionDenied` |

API Gateway 使用 `services.api_gateway.postgres` 指向 `[postgres.tenant_runtime]`，可用 `FLARE_API_GATEWAY_TENANT_RUNTIME_POSTGRES_URL` 覆盖；策略用 `FLARE_API_GATEWAY_TENANT_POLICY` 覆盖配置。MessageIngest 使用 `services.message_ingest.postgres` 或 `MESSAGE_INGEST_TENANT_RUNTIME_POSTGRES_URL`；策略由 `MESSAGE_INGEST_TENANT_POLICY` 指定。Access Gateway 使用原有 `ACCESS_GATEWAY_TENANT_*` 配置。投影改变后，单个进程最多再使用约 1 秒的旧状态；验收以 5 秒内全部拒绝为门槛。

Core 自签的普通 HTTP/WS 接入令牌只允许 `token_use` 缺省（历史 access）或显式 `access`。`refresh`/`control`/`admin` 令牌不能当普通接入令牌，也不能经旧 access 刷新宽限路径换新。

待 SEC-07 一起完成：Online Login 的 `max_online_devices_per_user` 必须在 tenant-scoped Redis session 键和 tenant/device kick 事件上原子执行；现有仓储按裸 user_id 查询/删会话，直接在此基础上“踢最旧”会误踢其他租户的同名用户。当前不把设备上限标记为完成。真实双租户 E2E、停用后 5 秒的 HTTP/WS/刷新/发送门禁仍需运行。
