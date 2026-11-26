# Flare Capability

[English](README.md) · 中文

Flare IM **能力服务**：**Hook 控制面**（配置、校验、统计和保留期；Hook 在 Message Ingest 里运行）+ **能力扩展子系统**（DDD：`domain/capability` 领域模型与端口；CQRS：`application` 仅 **`commands` / `handler` / `queries`**；基础设施：`infrastructure/capability` 下按 **`registration` / `dispatch` / `routing`** 分包）。进程装配入口为 **`composition`**（`process_config` / `runtime_context` / **`wiring`** / `bootstrap` / `hook_registry`；原 `service`）。

## 核心职责

- **Hook 控制面**：写入并校验 `hook_configs`，提供 `hook_executions` 的统计和查询，按保留期清理。见 [Hook 控制面](#hook-控制面)。
- **能力扩展**：通过 `CapabilityExtensionRegistry` 聚合 Resolver；插件是托管插件：部署在插件注册表里，由租户的插件投影授权。核心不认识任何具体插件：通话、投票等都是建立在业务中立宿主能力上的插件。能力目录、授权与命令分发经 **`flare.capability.v1.CapabilityService` gRPC** 与本进程 Hook 端口同服暴露；外部 HTTP 由接入网关转发至该 gRPC（无旧版 plugin 宿主与 outbox）

### 环境变量（能力扩展）

| 变量 | 说明 |
|------|------|
| `FLARE_PLUGIN_REALTIME_PUSH_URL` | 宿主能力 `im.realtime.deliver` 用的 Push Proxy `PushService`（`http(s)://` 或 `discovery://`）：插件向会话成员在线连接投递的非持久实时包（未配置时投递回不可用） |
| `FLARE_CAPABILITY_NOTICE_REDIS_URL` | 租户通知频道 `{namespace}:notice` 所在 Redis（未配置时用 `token_store` 档，频道名也取它的 namespace）；插件授权投影的新版本提交后，Capability 在此发布 `{"tenantId","kind":"plugin.catalog.changed","version"}`，网关转告该租户的连接 |

### 能力 gRPC（`CapabilityService`）

与 `config/base.toml` 中 `port`（默认 **50051**）一致；客户端使用 `flare-grpc-proto` 生成的 `CapabilityServiceClient`。主要 RPC：`ListCapabilities`、`Dispatch`、`SetTenantCapabilitySwitch`。`Dispatch` 的 `payload_json` 为 JSON 字符串，例如 `example.poll.vote`：

```json
{ "codec": "VP8" }
```

进程配置 **`database_url`** 时，`CapabilityService` 的授权/租户开关走 **`PostgresCapabilityPolicy`**，与 Hook 配置共用连接池；Hook 与 capability 表均在 **`deploy/init.sql`** 的「Hook 引擎 + Capability 策略」节（`hook_configs`、`hook_executions`、`capability_user_grants`、`capability_tenant_switches`、`capability_service_settings`、`capability_audit_log` 等；该节对前述对象均 **`DROP TABLE IF EXISTS ... CASCADE` 后再 `CREATE TABLE`**）。若报错 `relation "capability_service_settings" does not exist`，说明 **flare-capability 当前连接的那套 PostgreSQL** 里没有该表：常见原因是 **脚本跑在别的实例/库**（例如编排器用 `5432`，独立进程 **`flare-capability` 连 `DATABASE_URL`，没有则连所加载配置里 `[services.capability].postgres` 指定的 postgres 档（未指定时 `[postgres.media]`）；两者都没有时拒绝启动**，不再回落到内置的连接串，见 `composition/bootstrap.rs`）。启动时会打 `flare_capability::db` 日志（脱敏后的 `host:port/dbname`），请与执行 `deploy/init.sql` 的目标对齐。**注意**：该节会删除并重建 Hook/Capability 相关表，生产库请用迁移流程评估数据。未配置数据库时使用 **`InMemoryCapabilityGrants`** 并注入开发用示例授权。

### 插件部署

部署由运维登记到 Core 的插件注册表（`/platform/plugin-registry/**`），进程不自己登记。`ListRegisteredPlugins` 读应用的授权快照，只用于诊断。

### 插件服务调用（`plugin.service.dispatch`）

插件部署经资源桥、凭自己的运行时凭据（`x-flare-plugin-runtime`）调用另一个已安装插件提供的服务。Core 按本地投影、租户操作开关和共享请求预算准入；校验服务票据并转发调用的路由器在 `flare-plugin-host` 里，不属于这个开源工作区。所以这里编出的 `flare-capability` 对所有服务调用都按被拒的资源请求拒绝（配置了插件运行时公钥时，启动日志会说明）。

发布镜像、开发脚本和插件实验台改编 `with-plugin-host/`：一个链接路由器的独立 Cargo 工作区，产出同名的 `flare-capability`（`cargo build --manifest-path flare-capability/with-plugin-host/Cargo.toml`）。检出了 `../flare-im-plugin-sdk` 时 `scripts/start_server.sh` 自动这样编；`FLARE_CAPABILITY_PLUGIN_HOST=0` 保持开源版，`=1` 要求必须带路由器。

### `ExtensionPlugin` 的身份

`ExtensionPlugin` 路由器（`flare.capability.v1.ExtensionPlugin/Call`）挂在允许匿名准入的签名上下文层后面。验过的 `x-flare-ctx` 是唯一身份：`GenericRequest.metadata`、`x-tenant-id`、`x-user-id` 等调用方元数据一概不读。

- `flare.capability.v1.health_check` 是唯一不带上下文也回答的 operation；带了无效上下文时它同样回 `UNAUTHENTICATED`。
- 其余 operation 需要签名的 `User`、`Service` 或 `TenantAdmin` 上下文，没有则 `UNAUTHENTICATED`。`Staff`、`System`、`Guest` 上下文，以及租户为空、`*` 或带首尾空白的上下文，回 `PERMISSION_DENIED`。员工只经 Admin Gateway 进 Core。
- handler 拿到的是验过的上下文，租户、用户、actor、请求号与追踪号都来自签名。`flare.media.v1.list_plugin_routes` 只列签名租户的插件；请求体里的 `tenant_id` 必须为空或等于该租户。
- 没有 Cell 公钥集（`FLARE_INTERNAL_CTX_PUBLIC_KEYS` / `FLARE_INTERNAL_CTX_PUBLIC_KEY`）时路由器不挂该层，只回答健康检查，其余调用一律 `UNAUTHENTICATED`。

## Hook 控制面

Hook 在 Message Ingest 里运行：`flare-im-core-hooks` 是唯一的运行时，调用链、适配器、熔断、时限和执行记录
都在那里。本服务是控制面，不执行 Hook，所以 Capability 故障不影响发送。

配置在 `hook_configs` 表（结构见 `deploy/init.sql`）。`tenant_id` 有值的行属于这个应用，为空的行是平台
Hook。部署文件里的 Hook 不在这里，Message Ingest 把只读摘要写到 `hook_deployment_summary`。

`CapabilityService.Administer` 提供 `flare.extension.v1.hook_config.*`：`create`、`get`、`list`、`update`、
`delete`、`set_status`、`get_statistics`、`query_executions`。每次写入在同一事务里递增
`hook_config_versions` 里这个范围的版本，写入在咨询锁下串行，版本号按提交顺序递增；Message Ingest 约
每秒查一次，只重载变了的范围。

### Hook 配置项（`HookConfigItem`）

| 字段 | 含义 |
|---|---|
| `name` | 在应用和类型内唯一；平台和部署 Hook 的名字保留 |
| `description` | 可选说明 |
| `enabled` | 停用后不再调用 |
| `order` | 同一来源内的先后，0 到 1000，小的先执行 |
| `timeout_ms` | 1 到 30000；运行时再按总时限剩余截断 |
| `require_success` | 必需（管理面字段叫 `required`）：目标失败时回可重试的 503；否则跳过这一步 |
| `selector` | `tenants`、`conversation_types`、`message_types`，空表示全部 |
| `transport` | gRPC（`endpoint` 或 `service_name`）或 Webhook（`endpoint`、`secret`、`headers`） |
| `metadata` | 原样传给 Hook |

签名密钥、Webhook 头的取值和 gRPC metadata 的取值只写不读，见
[Hook 插件合同](../docs/HOOK-PLUGIN-CONTRACT.md)。

## 执行证据

Message Ingest 批量写 `hook_executions`。这里负责：

- `hook_config.get_statistics`：按 `sample_weight` 加权的执行次数，真正发出调用的平均与 P99 耗时，熔断
  次数，按错误码的失败数。
- `hook_config.query_executions`：从新到旧，按时间范围、消息号和是否成功过滤。
- 保留期清理：分批删除最旧的行。

| 配置 | 环境变量 | 默认 | 范围 |
|---|---|---|---|
| `hook_executions.retention_enabled` | `FLARE_CAPABILITY_HOOK_EXECUTIONS_RETENTION_ENABLED` | `true` | |
| `hook_executions.retention_days` | `FLARE_CAPABILITY_HOOK_EXECUTIONS_RETENTION_DAYS` | 7 | 1 到 90 |
| `hook_executions.retention_sweep_secs` | `FLARE_CAPABILITY_HOOK_EXECUTIONS_RETENTION_SWEEP_SECS` | 600 | 10 到 86400 |
| `hook_executions.retention_delete_batch` | `FLARE_CAPABILITY_HOOK_EXECUTIONS_RETENTION_DELETE_BATCH` | 5000 | 100 到 100000 |

采样、缓冲和熔断的参数归 Message Ingest：`config/services/message-ingest.toml` 的 `[hook_executions]`、
`[hook_circuit]`，环境变量 `FLARE_HOOK_EXECUTIONS_*`、`FLARE_HOOK_CIRCUIT_*`。

## 参考文档

- [Hook 与插件扩展性设计](../docs/HOOK_PLUGIN_EXTENSIBILITY_DESIGN.md)
