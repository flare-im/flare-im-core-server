# Flare Capability

[English](README.md) · 中文

Flare IM **能力服务**：**Hook 引擎**（配置与 gRPC 调度）+ **能力扩展子系统**（DDD：`domain/capability` 领域模型、端口与 **Dispatch 领域服务**；CQRS：`application` 仅 **`commands` / `handler` / `queries`**；基础设施：`infrastructure/capability` 下按 **`registration` / `dispatch` / `routing` / `internal`** 分包，当前 RTC 通过远端媒体控制 gRPC 适配与自动注册装配）。进程装配入口为 **`composition`**（`process_config` / `runtime_context` / **`wiring`** / `bootstrap` / `hook_registry`；原 `service`）。

## 核心职责

- **配置管理**：支持配置文件、动态API（数据库）、配置中心三种配置方式
- **执行调度**：按优先级执行Hook，支持并发执行和超时控制
- **执行证据与熔断**：把 Hook 执行记录写进 `hook_executions`（失败全量、成功采样）并提供统计，按 (租户, Hook) 熔断，见[执行证据与熔断](#执行证据与熔断)
- **扩展机制**：支持 gRPC、WebHook、Local Plugin（Hook 传输侧）三种扩展方式；Hook 的 gRPC 面（`HookExtension` / `HookService`）与 `CapabilityService` 同文件 **`capability_service.proto`**、同包 **`flare.capability.v1`**，Rust 侧统一 `flare_grpc_proto::capability`
- **能力扩展**：通过 `CapabilityExtensionRegistry` 聚合 Guard / Resolver / `RtcCapability`；RTC 后端通过 `capability_runtime.plugin_discovery_endpoints`（`capability_id = rtc.media.control`）或环境变量 `FLARE_MEDIA_CONTROL_GRPC_ENDPOINT` 装配。能力目录、授权与命令分发经 **`flare.capability.v1.CapabilityService` gRPC** 与本进程 Hook 端口同服暴露；外部 HTTP 由接入网关转发至该 gRPC（无旧版 plugin 宿主与 outbox）

### 环境变量（能力扩展）

| 变量 | 说明 |
|------|------|
| `FLARE_MEDIA_CONTROL_GRPC_ENDPOINT` | 覆盖媒体控制 gRPC 地址（未配置时走 `capability_runtime.plugin_discovery_endpoints` 自动发现） |

### 能力 gRPC（`CapabilityService`）

与 `config/base.toml` 中 `port`（默认 **50051**）一致；客户端使用 `flare-grpc-proto` 生成的 `CapabilityServiceClient`。主要 RPC：`ListCapabilities`、`ListUserCapabilities`、`Dispatch`、`GrantUserCapability`、`RevokeUserCapability`、`SetTenantCapabilitySwitch`。`Dispatch` 的 `payload_json` 为 JSON 字符串，例如 `rtc.call.video`：

```json
{ "codec": "VP8" }
```

进程配置 **`database_url`** 时，`CapabilityService` 的授权/租户开关走 **`PostgresCapabilityPolicy`**，与 Hook 配置共用连接池；Hook 与 capability 表均在 **`deploy/init.sql`** 的「Hook 引擎 + Capability 策略」节（`hook_configs`、`hook_executions`、`capability_user_grants`、`capability_tenant_switches`、`capability_service_settings`、`capability_audit_log` 等；该节对前述对象均 **`DROP TABLE IF EXISTS ... CASCADE` 后再 `CREATE TABLE`**）。若报错 `relation "capability_service_settings" does not exist`，说明 **flare-capability 当前连接的那套 PostgreSQL** 里没有该表：常见原因是 **脚本跑在别的实例/库**（例如编排器用 `5432`，独立进程 **`flare-capability` 连 `DATABASE_URL`，没有则连所加载配置里 `[services.capability].postgres` 指定的 postgres 档（未指定时 `[postgres.media]`）；两者都没有时拒绝启动**，不再回落到内置的连接串，见 `composition/bootstrap.rs`）。启动时会打 `flare_capability::db` 日志（脱敏后的 `host:port/dbname`），请与执行 `deploy/init.sql` 的目标对齐。**注意**：该节会删除并重建 Hook/Capability 相关表，生产库请用迁移流程评估数据。未配置数据库时使用 **`InMemoryCapabilityGrants`** 并注入开发用示例授权。

**`user capability grant missing or expired`**：`Dispatch` 在 `capability_user_grants` 中未找到匹配 **tenant_id + user_id + capability_id**（含 `namespace.*`）的未过期行。编排器默认 **tenant_id = `0`**（见 `ctx.tenant_id().unwrap_or("0")`）。`deploy/init.sql` 会插入 **`('0','*','rtc.*')`**：`user_id = *` 表示该租户下任意用户均可 RTC（开发友好；生产请按用户灌库并删除通配行）。若库是旧版脚本建的，请手动执行：

`INSERT INTO public.capability_user_grants (tenant_id, user_id, capability_id, plan_code, source) VALUES ('0', '*', 'rtc.*', 'dev', 'manual') ON CONFLICT DO NOTHING;`

### 插件登记（`RegisterPluginEndpoint`）

插件端点有两种来源：

- **配置发现**：`capability_runtime.plugin_discovery_endpoints` 与 `FLARE_MEDIA_CONTROL_GRPC_ENDPOINT`。这类端点只在读到配置的进程里，从不写库，也不会被下面的 TTL 清理。
- **登记**：`CapabilityService.RegisterPluginEndpoint` / `DeregisterPluginEndpoint`，由平台员工经 Admin Gateway 调用（`core:plugins:write`），或由插件宿主带 Cell 签发的、租户为自己的 `Service` 上下文调用。有库时 Capability 先把登记写进 `capability_plugin_endpoints`（迁移 `0011`），写成功才进本进程的路由簿；写库失败则调用回 `UNAVAILABLE`，什么也不变。注销先删库里的行。同一 `(tenant, plugin)` 先由配置发现、后被登记时，登记覆盖配置。

每个 Capability 实例启动时整表载入（表不存在则启动失败：先跑迁移），之后按周期重读，多实例由此收敛，重启也不丢登记。比某次重读快照更新的本地写不会被那次快照覆盖。每轮健康检查通过的登记记下 `last_healthy_at`；超过 TTL 既没重新登记、也没通过一次健康检查的登记被删除（每分钟检查一次），其他实例在下次重读时去掉。没有库时登记只在本进程：其他实例看不到，重启即丢（启动日志说明一次）。

| 设置（环境变量，其次 `[capability_runtime]` 键） | 缺省 | 取值范围 |
| --- | --- | --- |
| `FLARE_CAPABILITY_PLUGIN_ROUTE_REFRESH_SECS` / `plugin_route_refresh_secs` | 10 | 1 到 3600 秒 |
| `FLARE_CAPABILITY_PLUGIN_ROUTE_TTL_SECS` / `plugin_route_ttl_secs` | 86400 | 300 秒到 30 天 |

超出范围的值被忽略，使用缺省值。

### `ExtensionPlugin` 的身份

`ExtensionPlugin` 路由器（`flare.capability.v1.ExtensionPlugin/Call`）挂在允许匿名准入的签名上下文层后面。验过的 `x-flare-ctx` 是唯一身份：`GenericRequest.metadata`、`x-tenant-id`、`x-user-id` 等调用方元数据一概不读。

- `flare.capability.v1.health_check` 是唯一不带上下文也回答的 operation；带了无效上下文时它同样回 `UNAUTHENTICATED`。
- 其余 operation 需要签名的 `User`、`Service` 或 `TenantAdmin` 上下文，没有则 `UNAUTHENTICATED`。`Staff`、`System`、`Guest` 上下文，以及租户为空、`*` 或带首尾空白的上下文，回 `PERMISSION_DENIED`。员工只经 Admin Gateway 进 Core。
- handler 拿到的是验过的上下文，租户、用户、actor、请求号与追踪号都来自签名。`flare.media.v1.list_plugin_routes` 只列签名租户的插件；请求体里的 `tenant_id` 必须为空或等于该租户。
- 没有 Cell 公钥集（`FLARE_INTERNAL_CTX_PUBLIC_KEYS` / `FLARE_INTERNAL_CTX_PUBLIC_KEY`）时路由器不挂该层，只回答健康检查，其余调用一律 `UNAUTHENTICATED`。

## Hook配置方式

本服务中 Hook 配置支持三种来源，按优先级从高到低排序：

### 1. 动态API配置（最高优先级）

**存储位置**：PostgreSQL数据库

**特点**：
- 支持多租户隔离
- 支持动态增删改查
- 支持版本管理
- 支持审计日志（记录创建者、创建时间、更新时间）

**表结构**：以 **`deploy/init.sql`** 为准（含 `group_name`、`require_success`、`metadata`、`created_by` 等与 `PostgresHookConfigRepository` 对齐的列）。以下为概念示意：

```sql
CREATE TABLE hook_configs (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT,                    -- 租户ID（可选，用于多租户隔离）
    hook_type TEXT NOT NULL,            -- Hook类型（pre_send, post_send等）
    name TEXT NOT NULL,                 -- Hook名称
    version TEXT,                       -- Hook版本
    description TEXT,                   -- Hook描述
    enabled BOOLEAN NOT NULL DEFAULT true,
    priority INTEGER NOT NULL DEFAULT 100,
    group_name TEXT,                    -- Hook分组（validation/critical/business）
    timeout_ms BIGINT NOT NULL DEFAULT 1000,
    max_retries INTEGER NOT NULL DEFAULT 0,
    error_policy TEXT NOT NULL DEFAULT 'fail_fast',
    require_success BOOLEAN NOT NULL DEFAULT true,
    selector_config JSONB NOT NULL DEFAULT '{}',  -- 选择器配置
    transport_config JSONB NOT NULL,               -- 传输配置
    metadata JSONB,                                -- 元数据
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT,                               -- 创建者
    
    UNIQUE(tenant_id, hook_type, name)  -- 唯一约束
);
```

**使用方式**（启动完整进程，含 gRPC 与可选插件面）：
```rust
use anyhow::Result;
use flare_capability::domain::model::ExecutionMode;
use flare_capability::composition::{ApplicationBootstrap, CapabilityServiceConfig};

#[tokio::main]
async fn main() -> Result<()> {
    let config = CapabilityServiceConfig {
        database_url: Some("postgresql://user:pass@localhost:5432/flare".to_string()),
        tenant_id: Some("tenant-a".to_string()),
        config_file: Some("config/hooks.toml".into()),
        config_center_endpoint: Some("etcd://localhost:2379".to_string()),
        execution_mode: ExecutionMode::Sequential,
        ..Default::default()
    };
    ApplicationBootstrap::run(config).await
}
```

**配置管理API**（通过PostgresHookConfigRepository）：
```rust
// 保存Hook配置
repository.save(
    Some("tenant-a"),
    "pre_send",
    &hook_config_item,
    Some("admin")
).await?;

// 删除Hook配置
repository.delete(
    Some("tenant-a"),
    "pre_send",
    "hook-name"
).await?;
```

### 2. 配置中心配置（中等优先级）

**存储位置**：etcd 或 Consul

**特点**：
- 支持多租户隔离
- 支持配置变更监听（watch）
- 适合分布式环境

**配置键格式**：
- 多租户：`/flare/hooks/{tenant_id}/config`
- 全局：`/flare/hooks/config`

**使用方式**：
```rust
use flare_capability::domain::model::ExecutionMode;
use flare_capability::composition::CapabilityServiceConfig;

let config = CapabilityServiceConfig {
    config_center_endpoint: Some("etcd://localhost:2379".to_string()),
    tenant_id: Some("tenant-a".to_string()),
    execution_mode: ExecutionMode::Sequential,
    ..Default::default()
};
```

**配置格式**（JSON）：
```json
{
  "pre_send": [
    {
      "name": "friendship-check",
      "priority": 5,
      "timeout_ms": 50,
      "enabled": true,
      "require_success": true,
      "selector": {
        "tenants": ["tenant-a"],
        "conversation_types": ["single"]
      },
      "transport": {
        "type": "grpc",
        "endpoint": "https://hooks.relation.svc:7443"
      }
    }
  ]
}
```

### 3. 配置文件配置（最低优先级）

**存储位置**：本地TOML文件

**特点**：
- 适合开发环境，以及对所有租户生效的平台级 Hook
- 每次刷新都重新读取（见「配置刷新」）

**配置文件路径**：`CONFIG_FILE`，默认 `config/hooks.toml`（相对工作目录）

**加载规则**：
- 文件可选。没有文件时 Capability 不带文件 Hook 运行，启动日志在 INFO 级说明；有文件时记录路径与 Hook 条数。
- 文件存在却用不了，Capability 拒绝启动：读不了（无权限、是目录、不是 UTF-8、符号链接指向不存在的文件），不是 Hook 配置（TOML 语法或字段类型错误，或顶层写了 15 类 Hook 以外的表，例如 `[[pre_sned]]`），或其中的 Hook 校验不过（优先级、超时、分组、错误策略、Webhook 空密钥、保留的 Webhook 头）。文件先于配置中心与数据库检查，数据库不可达盖不住这个错误。
- 报错带文件路径、解析错误的行列、校验错误的 Hook 名与头名，不带文件内容：TOML 解析器自己的报错会引用出错的那一行（字段类型错误还会带上取值），可能正是 Webhook 密钥或鉴权头。
- 运行中文件改坏了不会让服务退出：文件里的 Hook 停在上一次加载成功的版本，每次刷新按 ERROR 记下错误，其余配置源照常刷新。文件改好后下一次刷新生效；删掉文件则下一次刷新后没有文件 Hook。
- Hook 条目里不认识的键 Capability 忽略（可能读同一份文件的 Message Ingest 会拒绝）。

**配置示例**：
```toml
[[pre_send]]
name = "friendship-check"
priority = 5
timeout_ms = 50
enabled = true
require_success = true

[pre_send.selector]
tenants = ["tenant-a"]
conversation_types = ["single"]
message_types = ["text", "image"]

[pre_send.transport]
type = "grpc"
endpoint = "https://hooks.relation.svc:7443"

[pre_send.retry]
max_retries = 3
retry_interval_ms = 100
backoff_strategy = "exponential"
```

## 配置优先级和合并规则

1. **优先级顺序**：数据库配置 > 配置中心配置 > 配置文件配置
2. **合并规则**：同名Hook配置，高优先级覆盖低优先级
3. **加载顺序**：按优先级从低到高加载，最后加载的配置会覆盖前面的同名配置

## 多租户支持

Hook 配置支持多租户场景：

- **租户隔离**：每个租户可以有独立的Hook配置
- **全局配置**：`tenant_id`为`NULL`的配置（以及配置文件）对所有租户生效
- **租户配置**：租户自己的 Hook 只对该租户生效，与全局 Hook 一起执行，不能顶替全局 Hook。全局 Hook 的名字在同类型内为平台保留：租户经 Administer 创建或改名成这个名字时返回 `ALREADY_EXISTS`（停用的全局行同样保留名字）；绕过 Administer 写进库的同名租户行不加载（记告警），全局 Hook 照常执行
- **选择器**：`selector.tenants` 非空时，Hook 只作用于所列租户

**加载与执行**（未设 `TENANT_ID` 的进程，即多租户默认）：数据库加载器把全局行作为全局配置，
各租户经 `CapabilityService/Administer` 登记的行按租户分桶。`HookPlugin.Call` 按调用方签名
上下文里的租户选 Hook：全局配置加该租户的桶。某个租户的行无效时只让该租户停在上一版配置；
某个配置源加载失败时沿用它上一次加载的结果。Administer 写入后，处理写入的实例立即重载该租户的
桶，并在 PostgreSQL 频道 `flare_hook_config_changed` 上 `NOTIFY`：共用同一个库的其余实例重载同一
租户（全局行则全量重载），不必等定时刷新。监听重连后全量重载一次，补上断开期间漏掉的通知；定时
刷新仍是兜底。监听用一条独立的 PostgreSQL 连接，不占仓储连接池（每个实例多一条连接），随进程
停机关闭。每次按租户重载在 INFO 级记录租户与 Hook 条数。设了
`TENANT_ID` 的进程保持单租户行为（不保留全局 Hook 的名字），加载下面这些行作为全局配置：
```sql
SELECT * FROM hook_configs
WHERE enabled = true
  AND (tenant_id IS NULL OR tenant_id = 'tenant-a')
ORDER BY hook_type, priority ASC
```

**调用方**：`HookPlugin.Call` 只认 Cell 签名的上下文（`x-flare-ctx`，用
`FLARE_INTERNAL_CTX_PUBLIC_KEY` 验签）。没有或验不过签名回 `UNAUTHENTICATED`；请求体里的租户或
用户与签名不一致回 `PERMISSION_DENIED`。没有配置公钥时所有调用都被拒绝。

## 配置刷新

支持 Hook 配置热刷新：

- **刷新间隔**：默认60秒（可通过`refresh_interval_secs`配置）
- **自动刷新**：定时从所有配置源重新加载配置
- **失败**：某个配置源加载失败时沿用它上一次加载的结果，并记录失败（配置文件为 ERROR，数据库与配置中心为 WARN）；合成的全局配置校验不过时当前 Hook 不变。刷新不会让运行中的服务退出。
- **启动**：三个配置源里只有配置文件必须加载成功（见「配置文件配置」）。启动时从数据库或配置中心加载 Hook 行失败只记日志，下一次刷新重试；连上数据库本身仍是必需的。

## 使用示例

### 基本使用

```rust
use anyhow::Result;
use flare_capability::domain::model::ExecutionMode;
use flare_capability::composition::{ApplicationBootstrap, CapabilityServiceConfig};

#[tokio::main]
async fn main() -> Result<()> {
    let config = CapabilityServiceConfig {
        config_file: Some("config/hooks.toml".into()),
        // 不内置回落：库须显式配置。
        database_url: Some(std::env::var("DATABASE_URL").expect("set DATABASE_URL")),
        config_center_endpoint: Some("etcd://localhost:22379".to_string()),
        tenant_id: Some("tenant-a".to_string()),
        execution_mode: ExecutionMode::Sequential,
        refresh_interval_secs: 60,
        ..Default::default()
    };
    ApplicationBootstrap::run(config).await
}
```

业务进程内执行 PreSend 等 Hook 请使用 `flare_im_hooks::HookDispatcher`（与编排器同进程），本二进制主要负责 Hook 扩展 gRPC 与能力插件运行时。

### 数据库配置管理

```rust
use flare_capability::infrastructure::persistence::PostgresHookConfigRepository;
use flare_capability::domain::models::{HookConfigItem, HookSelectorConfig, HookTransportConfig};

// 创建配置仓储
let repository = PostgresHookConfigRepository::new(
    "postgresql://user:pass@localhost:5432/flare"
).await?;


// 创建Hook配置
let hook_config = HookConfigItem {
    name: "friendship-check".to_string(),
    priority: 5,
    timeout_ms: 50,
    enabled: true,
    require_success: true,
    selector: HookSelectorConfig {
        tenants: vec!["tenant-a".to_string()],
        conversation_types: vec!["single".to_string()],
        ..Default::default()
    },
    transport: HookTransportConfig::Grpc {
        endpoint: "https://hooks.relation.svc:7443".to_string(),
        metadata: HashMap::new(),
    },
    ..Default::default()
};

// 保存配置
let id = repository.save(
    Some("tenant-a"),
    "pre_send",
    &hook_config,
    Some("admin")
).await?;
```

## 架构设计

本服务采用 DDD + CQRS 架构：

```
flare-capability/
├── domain/              # 领域层
│   ├── models.rs        # Hook配置模型、执行计划
│   ├── repositories.rs  # 仓储接口
│   └── service.rs       # 领域服务（编排服务）
├── application/         # 应用层
│   ├── commands.rs      # Hook执行命令
│   ├── queries.rs       # Hook查询服务
│   └── service.rs       # 应用服务门面
├── infrastructure/      # 基础设施层
│   ├── adapters/        # Hook适配器（gRPC、WebHook、Local）
│   ├── config/          # 配置加载器（文件、数据库、配置中心）
│   ├── hook_execution/  # 执行证据：缓冲、批量写库、保留期、指标
│   └── persistence/     # 配置持久化（PostgreSQL）
├── interface/           # 接口层
│   └── grpc/            # gRPC服务接口
└── service/             # 服务层
    ├── bootstrap.rs     # 应用启动器
    └── registry.rs      # 服务注册
```

## 配置项说明

### Hook配置项（HookConfigItem）

| 字段 | 类型 | 说明 | 默认值 |
|------|------|------|--------|
| `name` | String | Hook名称（必填） | - |
| `priority` | i32 | 优先级（0-1000，越小越高） | 100 |
| `group` | String | 分组：`validation`、`critical` 或 `business`（不区分大小写），其他值报配置错误；不写时 priority >= 100 为 `validation`，否则为 `business`。`critical` 组的 Hook 不可达或超时一律拒绝，`business` 组从不中断主流程 | 按 priority |
| `timeout_ms` | u64 | 超时时间（毫秒） | 1000 |
| `enabled` | bool | 是否启用 | true |
| `require_success` | bool | 是否要求成功 | true |
| `error_policy` | String | 错误策略（fail_fast/retry/ignore） | fail_fast |
| `max_retries` | u32 | 最大重试次数 | 0 |
| `selector` | HookSelectorConfig | 选择器配置 | - |
| `transport` | HookTransportConfig | 传输配置 | - |

### Hook选择器（HookSelectorConfig）

用于匹配Hook执行条件：

| 字段 | 类型 | 说明 |
|------|------|------|
| `tenants` | Vec<String> | 租户列表（空表示匹配所有租户） |
| `conversation_types` | Vec<String> | 会话类型列表（空表示匹配所有类型） |
| `message_types` | Vec<String> | 消息类型列表（空表示匹配所有类型） |
| `user_ids` | Vec<String> | 用户ID列表（空表示匹配所有用户） |
| `tags` | HashMap<String, String> | 标签匹配 |

### Hook传输配置（HookTransportConfig）

支持三种传输方式：

1. **gRPC传输**：
   ```toml
   [transport]
   type = "grpc"
   endpoint = "https://hooks.example.com:7443"
   ```

2. **WebHook传输**：
   ```toml
   [transport]
   type = "webhook"
   endpoint = "https://hooks.example.com/webhook"
   secret = "your-secret-key"
   ```
   配了密钥时投递带 `Flare-Signature: t=<unix 秒>,v1=<hex HMAC-SHA256(secret, t "." 原始请求体)>`，
   与 Core 的 Webhook 适配器同一契约（`docs/HOOK-PLUGIN-CONTRACT.md` §7），旧的
   `X-Hook-Signature` 已删除。密钥只写不读：`hook_config.create`、`get`、`list`、`update` 都不返回
   它（`transport.metadata["signing_secret_configured"]` 说明是否已设置），`update` 时密钥留空即
   沿用已存的，日志与审计记录里也没有它；密钥留空并带 `transport.metadata["clear_signing_secret"] = "true"`
   的 `update` 清除密钥、回到不签名。自定义头与 gRPC `metadata` 的取值同样只写不读：读回只有键名、
   值为 `<redacted>`，`update` 时值为空或 `<redacted>` 即沿用已存的。文件或配置中心里
   `secret = ""` 在加载时报错，不签名就不写 `secret`。上下文缺租户时不投递。gRPC 传输原样转发调用方的签名
   上下文（`x-flare-ctx`），没有签名上下文时不发出调用。

3. **Local Plugin传输**：
   ```toml
   [transport]
   type = "local"
   target = "plugin-name"
   ```

## 执行证据与熔断

`HookPlugin` 为 `pre_send`、`post_send`、`delivery`、`recall` 调用的每个 Hook 都先过一个
(租户, Hook) 熔断器，并在 `hook_executions`（源 DDL `0015_20261016_hook_execution_evidence.sql`）留下执行记录。

- **熔断**：按签名里的租户、Hook 类型与名称分开，每个 Capability 实例各自维护。同一 Hook 连续
  `failure_threshold` 次调用失败或超时（都在这一串第一次失败后的 `failure_window_secs` 内）即打开
  `open_secs`，期间不调用该 Hook。必需的 Hook（critical 组或 `require_success`）此时与 Hook 不可用
  相同：PreSend、Recall 回可重试的 `UNAVAILABLE`（Message Ingest 回 503），PostSend、Delivery 以
  `UNAVAILABLE` 失败；可选的 Hook 跳过。窗口到期后放一次探测：成功关闭，失败再打开一个窗口，探测
  未结束时其余调用仍短路。Hook 给出结论（放行或拒绝）都算健康。打开、探测失败（第 1、2、4、8…
  次）与关闭记日志，租户与 Hook 名转义；单次短路只记 DEBUG。
- **记录**：租户、Hook 类型与名称（`hook_id` 为 `hook_type:name`）、Hook 版本、消息号、请求号与
  追踪号、Hook 是否给出结论（`success`）、耗时、错误码、固定说明、时间与 `sample_weight`。从不记
  消息正文或下游错误原文：调用失败只记错误码与 "hook call failed"，拒绝记 `HOOK_REJECTED` 与
  Hook 的拒绝原因码，超时记 `HOOK_TIMEOUT`，熔断短路记 `HOOK_CIRCUIT_OPEN`。拒绝、失败、超时全量
  记录；放行与熔断短路按 1/N 采样，N 为 `1 / success_sample_ratio` 四舍五入（0.01 即每 100 次留
  1 次，0.3 即每 3 次留 1 次），留下的一行 `sample_weight` 为 N。
- **写入**：消息链路上只把记录放进内存里的有界缓冲（一次加锁，从不等待）。后台任务在攒够
  `batch_size` 条时立即写一批，另每 `flush_interval_ms` 写一次；停机时在 5 秒内写完余下的（每批的
  写入截到这 5 秒为止），然后关闭缓冲：没写完的、被截断的那一批以及之后仍在收尾的请求交来的记录都
  按 `shutdown` 计为丢弃。缓冲满时先丢采样的：新来的采样记录直接丢，新来的全量记录挤掉最旧的采样
  记录；缓冲里全是全量记录时丢新来的。写库失败或超过 10 秒的一批丢弃，不重试。每次丢弃都计数。
- **保留期**：每 `retention_sweep_secs` 删除早于 `retention_days` 的行，最旧的先删，每条语句至多
  `retention_delete_batch` 行。
- **读取**：`hook_config.get_statistics` 与 `hook_config.query_executions`（`Administer`）只读调用方
  租户这一条 Hook 的行。计数按权重估计；平均与 p99 耗时按权重计算、不含熔断短路；
  `circuit_break_count` 是熔断短路次数，`error_count_by_code` 按错误码统计失败（拒绝算成功，不在
  其中）。统计不给时间范围时取最近 24 小时；执行记录从新到旧，至多 `limit` 条（上限 1000），带
  `has_more`。
- 没有库时只熔断，不记录。

| 设置（环境变量，其次配置键） | 缺省 | 取值范围 |
| --- | --- | --- |
| `FLARE_CAPABILITY_HOOK_EXECUTIONS_ENABLED` / `hook_executions.enabled` | true | true 或 false |
| `FLARE_CAPABILITY_HOOK_EXECUTIONS_BUFFER_CAPACITY` / `hook_executions.buffer_capacity` | 10000 | 100 到 1,000,000 条 |
| `FLARE_CAPABILITY_HOOK_EXECUTIONS_BATCH_SIZE` / `hook_executions.batch_size` | 500 | 1 到 5000，不超过缓冲上限 |
| `FLARE_CAPABILITY_HOOK_EXECUTIONS_FLUSH_INTERVAL_MS` / `hook_executions.flush_interval_ms` | 1000 | 50 到 60000 毫秒 |
| `FLARE_CAPABILITY_HOOK_EXECUTIONS_SUCCESS_SAMPLE_RATIO` / `hook_executions.success_sample_ratio` | 0.01 | 0（不留）到 1（全留） |
| `FLARE_CAPABILITY_HOOK_EXECUTIONS_RETENTION_DAYS` / `hook_executions.retention_days` | 7 | 1 到 90 天 |
| `FLARE_CAPABILITY_HOOK_EXECUTIONS_RETENTION_SWEEP_SECS` / `hook_executions.retention_sweep_secs` | 600 | 10 到 86400 秒 |
| `FLARE_CAPABILITY_HOOK_EXECUTIONS_RETENTION_DELETE_BATCH` / `hook_executions.retention_delete_batch` | 5000 | 100 到 100000 行 |
| `FLARE_CAPABILITY_HOOK_CIRCUIT_ENABLED` / `hook_circuit.enabled` | true | true 或 false |
| `FLARE_CAPABILITY_HOOK_CIRCUIT_FAILURE_THRESHOLD` / `hook_circuit.failure_threshold` | 5 | 1 到 1000 |
| `FLARE_CAPABILITY_HOOK_CIRCUIT_FAILURE_WINDOW_SECS` / `hook_circuit.failure_window_secs` | 30 | 1 到 3600 秒 |
| `FLARE_CAPABILITY_HOOK_CIRCUIT_OPEN_SECS` / `hook_circuit.open_secs` | 20 | 1 到 3600 秒 |

配置键从运行时配置文件（`config/services/capability.toml`）读；超出范围的值被忽略，使用缺省值。

指标（Hook 名不做标签）：`capability_hook_execution_records_queued_total`、
`capability_hook_execution_records_written_total`、
`capability_hook_execution_records_dropped_total{reason=buffer_full|write_failed|shutdown, class=sampled|always}`、
`capability_hook_execution_records_buffered`、`capability_hook_execution_flushes_total{result}`、
`capability_hook_execution_records_expired_total`、
`capability_hook_circuit_transitions_total{tenant_id, transition=opened|reopened|closed}`、
`capability_hook_circuit_short_circuits_total{tenant_id}` 与 `capability_hook_circuits_open`，另有通用的
gRPC 服务端指标（`GrpcMetricsLayer`）。

## 参考文档

- [Hook 与插件扩展性设计](../docs/HOOK_PLUGIN_EXTENSIBILITY_DESIGN.md)
