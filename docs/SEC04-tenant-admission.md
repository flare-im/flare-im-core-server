# SEC-04 租户运行时状态执行

Core 的 `tenants` 投影是数据面判定源，进程内按租户缓存，默认刷新间隔 1 秒。`active` 可接入；`suspended`、`deleting` 一律拒绝。`strict` 模式下无投影或回源失败也拒绝，且没有投影数据库时服务启动失败；`lenient` 仅供 Core-only/开发兼容，已知停用租户仍被拒绝。生产 Compose 对接入网关、API Gateway、MessageIngest 显式配置 `strict`。

| 入口 | 判定位置 | 停用时结果 |
| --- | --- | --- |
| WS/QUIC 新连接 | Access Gateway 验证 token 后 | 关连接前下发 ERROR 帧（协商前 JSON），message 为 `TENANT_UNAVAILABLE`，metadata `code=AUTHENTICATION_FAILED` |
| WS/QUIC 新连接（CONNECT_ACK 之后） | Online Login 登记会话时 | 网关缓存还没翻转、Online 已看到停用时，ACK 已经发出：关连接前按协商格式下发 KICKED，message 与 metadata `reason` 为 `TENANT_UNAVAILABLE`，metadata `code=PERMISSION_DENIED` |
| HTTP 消息/会话/媒资/在线 API | API Gateway Bearer 验证后、下游调用前 | 403 `TENANT_UNAVAILABLE`；发送时若 MessageIngest 的缓存先看到停用，同样回 403 `TENANT_UNAVAILABLE` |
| 本地 token 签发 | App 凭据或开发开关验证后、签发前 | 403 `TENANT_UNAVAILABLE` |
| 本地 token 刷新 | 验证签名并从原 token 取租户后、轮换前 | 403 `TENANT_UNAVAILABLE` |
| gRPC 消息摄入 | 已验证上下文之后、幂等重放/限流/WAL/Hook 前 | `PermissionDenied` |

`strict` 下读不到投影（数据库抖动等）时上表各入口仍然拒绝，但这不是租户结论，按可重试拒绝上报，不用 `TENANT_UNAVAILABLE`：WS/QUIC 新连接在 CONNECT 前收到的 ERROR 帧 message 与 metadata `code` 都是 `SERVICE_UNAVAILABLE`（网关以 flare-core `AuthResult::retryable_failure` 拒绝）；MessageIngest 答 `ServiceUnavailable`（gRPC `UNAVAILABLE`），API Gateway 发送回 503 `SERVICE_UNAVAILABLE`；API Gateway 自己的租户门回 503 `AUTH_PROVIDER_UNAVAILABLE`。客户端对这些原因退避重试（Core SDK 的重连循环照常继续；收到 `TENANT_UNAVAILABLE` 才终止会话、不再重连）。`lenient` 下读不到投影照常放行。

API Gateway 使用 `services.api_gateway.postgres` 指向 `[postgres.tenant_runtime]`，可用 `FLARE_API_GATEWAY_TENANT_RUNTIME_POSTGRES_URL` 覆盖；策略用 `FLARE_API_GATEWAY_TENANT_POLICY` 覆盖配置。MessageIngest 使用 `services.message_ingest.postgres` 或 `MESSAGE_INGEST_TENANT_RUNTIME_POSTGRES_URL`；策略由 `MESSAGE_INGEST_TENANT_POLICY` 指定。Access Gateway 使用原有 `ACCESS_GATEWAY_TENANT_*` 配置。投影改变后，单个进程最多再使用约 1 秒的旧状态；验收以 5 秒内全部拒绝为门槛。

网关鉴权和 Online Login 各用一份 1 秒 TTL 的缓存，停用后约 1 秒内两者可能不一致，CONNECT_ACK 之后的这一行就是为此准备的。不把租户检查整体挪到 ACK 之前：会话登记本身就是 Online 的准入判定，挪到 ACK 之前会让投递先于 ACK 到达客户端；而在 ACK 之前再做一次检查也挡不住检查与登记之间发生的停用，ACK 之后的失败无论如何都要带原因。

Online 登记会话时，`TENANT_UNAVAILABLE` 只用于确定的租户结论：已知的 `suspended` / `deleting`，或严格策略下没有投影的租户。读不到投影（如数据库抖动）时不论策略都拒绝登记，但按服务不可用上报，ACK 之后的 KICKED 原因是 `SERVICE_UNAVAILABLE`，客户端照常退避重连；若也报 `TENANT_UNAVAILABLE`，客户端会终止会话、不再重连，一次抖动就会让这段时间里连进来的客户端一直离线。这与 HTTP 租户门对投影不可用回 503 而不是 403 一致。

已接纳的长连接不会被重新检查，由 Control 踢掉：状态写入后 Control 立即投递该租户的投影（在独立任务里执行，请求被取消也会跑完，不会让已领取的 outbox 行空占 30 秒租约），核确认非 active 的快照后调 Online `KickTenant`，当即一次、2 秒后再一次。第二次踢掉的是缓存过期前重连进来的连接；第二次之前 Control 会重读租户状态，这 2 秒内已恢复为 active 就不再踢。网关对每个踢线信号并发下发 KICKED，每条连接最多等 1 秒，慢连接不会拖住其他连接和后续信号。网关关连接前下发 KICKED 帧，message 与 metadata `reason` 都是类型化原因：租户踢线为 `TENANT_UNAVAILABLE`（请求里的原因只进 Online 日志），运维按设备踢为 `DEVICE_KICKED`，同设备新连接接替旧连接为 `SESSION_REPLACED`，令牌撤销为 `TOKEN_REVOKED`，不带原因的旧信号为 `KICKED`。

Core 自签的普通 HTTP/WS 接入令牌只允许 `token_use` 缺省（历史 access）或显式 `access`。`refresh`/`control`/`admin` 令牌不能当普通接入令牌，也不能经旧 access 刷新宽限路径换新。

待 SEC-07 一起完成：Online Login 的 `max_online_devices_per_user` 必须在 tenant-scoped Redis session 键和 tenant/device kick 事件上原子执行；现有仓储按裸 user_id 查询/删会话，直接在此基础上“踢最旧”会误踢其他租户的同名用户。当前不把设备上限标记为完成。真实双租户 E2E、停用后 5 秒的 HTTP/WS/刷新/发送门禁仍需运行。
