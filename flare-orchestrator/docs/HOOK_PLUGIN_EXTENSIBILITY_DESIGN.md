# Flare Orchestrator Hook/Plugin 可扩展架构设计（DDD + CQRS）

## 1. 目标与约束

### 1.1 目标
- 支持 Hook 与 Plugin 的长期扩展（新增能力无需改核心编排主干）。
- 保持写路径低延迟与高可靠（百万级并发、最终一致、可降级）。
- 明确应用层编排、领域规则、基础设施调用边界（避免业务逻辑泄漏到 interface）。
- 支持灰度、租户级策略、可观测、可回滚。

### 1.2 非目标
- 不在本轮重写现有消息发送/事件推送主流程。
- 不在本轮引入复杂脚本引擎（如 Lua/WASM）作为默认执行路径。
- 不打破现有 gRPC 协议与上游调用方式。

### 1.3 当前约束（基于现状）
- 已有 Hook 执行链：`HookExecutionService` + `HookDispatcher`。
- 已有 Plugin 桥接能力：`CallCapabilityBridge`（`EVENT_CALL_SIGNAL` enrich）。
- 编排入口较集中在 `wire` 与 `EventHandler/MessageHandler`。
- 配置项已具备 capability endpoint、bridge 开关与 hooks 自动注入开关。

---

## 2. 现状问题诊断

1. **扩展点语义不统一**  
   Hook 与 Plugin 都在“扩展编排行为”，但生命周期、入参与失败策略分散在各 handler 内。

2. **执行策略分散**  
   超时、重试、降级、熔断等策略没有统一抽象，后续接入更多插件时容易各写一套。

3. **目录分层刚起步**  
   已完成 `handlers/hook` 与 `handlers/plugin` 初步拆分，但运行时注册、路由与策略还未独立成模块。

4. **多租户/灰度能力弱**  
   当前多为全局开关，缺少 tenant/conversation/message_type 维度策略路由。

5. **可观测粒度不足**  
   缺少统一 extension span、阶段耗时、失败原因分类、降级原因统计。

---

## 3. 目标架构（推荐）

## 3.1 分层结构

- `interface`：仅协议转换、`Ctx` 提取与调用 application。
- `application`：用例编排与扩展点调度（不写业务规则）。
- `domain`：规则与策略模型（扩展点语义、失败策略、路由规则）。
- `infrastructure`：gRPC/MQ/Redis/Kafka 等具体适配器。

推荐目录（增量演进）：

```text
application/
  handlers/
    hook/
      message_hook_handler.rs
      event_hook_handler.rs
      mod.rs
    plugin/
      call_capability_bridge.rs
      plugin_router_handler.rs
      mod.rs
  extension/
    orchestrator.rs
    policy.rs
    context.rs
    mod.rs

domain/
  extension/
    model.rs
    policy.rs
    routing.rs
    error.rs
    mod.rs
  repository/
    extension_gateway.rs   # 扩展调用统一端口

infrastructure/
  extension/
    grpc_gateway.rs
    local_gateway.rs
    mod.rs
```

---

## 3.2 扩展模型统一（核心）

定义统一扩展阶段：
- `BeforeValidate`
- `BeforePersist`
- `AfterPersist`
- `BeforePush`
- `AfterPush`
- `EventEnrich`（当前 `EVENT_CALL_SIGNAL` 属于这一类）

定义统一扩展类型：
- `Hook`：偏消息变换、审核、策略增强（可修改 draft/context）。
- `Plugin`：偏外部能力编排（能力服务、RTC/SFU、风控、机器人等）。

统一执行上下文 `ExtensionContext`：
- `ctx`（trace/user/tenant）
- `conversation_id`
- `request_id`
- `payload`（Message/Event/Command）
- `labels`（message_type、business_type、tenant policy tags）

统一执行结果 `ExtensionOutcome`：
- `Continue`
- `Mutated(payload)`
- `ShortCircuit(response)`（极少数场景）
- `Degraded(reason)`
- `Rejected(code, message)`

---

## 3.3 策略中心（Policy Center）

每个扩展项具备显式策略：
- `timeout_ms`
- `retry`（次数/退避）
- `failure_mode`：`FailClosed | FailOpen`
- `idempotency`：是否要求幂等
- `concurrency_limit`
- `tenant_scope`（全局 / 租户白名单）

建议策略默认值：
- Hook（PreSend）默认 `FailClosed`（安全相关）
- Hook（PostSend）默认 `FailOpen`
- Plugin（外部 enrich）默认 `FailOpen + degrade_mark`（当前 Call bridge 模式）

---

## 3.4 路由中心（Routing）

扩展路由键建议：
- `event_type`
- `message_type`
- `conversation_type`
- `tenant_id`
- `business_type`

路由方式：
- 静态配置优先（TOML/YAML）
- 动态覆盖（后续可接配置中心）
- 支持权重与优先级

---

## 3.5 统一网关接口（解耦外部插件）

在 domain 定义统一端口（示意）：

```rust
#[async_trait]
pub trait ExtensionGateway: Send + Sync {
    async fn invoke(
        &self,
        ctx: &Ctx,
        target: &ExtensionTarget,
        request: ExtensionRequest,
    ) -> Result<ExtensionResponse>;
}
```

由 infrastructure 提供：
- `GrpcExtensionGateway`（调用 capability/hook 插件服务）
- `LocalExtensionGateway`（本地内置扩展）

好处：
- handler 不关心 gRPC 细节。
- 统一治理（超时、重试、metrics、trace）。

---

## 3.6 可观测与治理

### Trace
- 统一 span：`extension.execute`
- tags：`extension.type`、`extension.name`、`phase`、`tenant_id`、`failure_mode`

### Metrics（建议）
- `extension_invocations_total{type,name,phase,result}`
- `extension_latency_ms_bucket{type,name,phase}`
- `extension_degrade_total{type,name,reason}`
- `extension_timeout_total{type,name}`

### Log
- 统一结构日志字段：
  - `trace_id` `request_id` `conversation_id`
  - `extension_name` `phase` `result`
  - `degrade_reason` `error_class`

---

## 4. 与现有代码的映射建议

### 4.1 已有模块保留并升级
- `domain/service/hook_execution_service.rs`  
  保留为 Hook 执行核心，新增统一 `policy` 入参。
- `application/handlers/plugin/call_capability_bridge.rs`  
  保留，作为 `Plugin` 的一个实现。

### 4.2 需要新增
1. `application/extension/orchestrator.rs`  
   负责统一调度 Hook + Plugin 执行顺序。
2. `domain/extension/*`  
   承载扩展阶段、策略、路由规则模型。
3. `infrastructure/extension/grpc_gateway.rs`  
   承载外部插件统一调用通道。

### 4.3 需要改造的入口
- `MessageHandler::handle_send_message`  
  改为通过 `ExtensionOrchestrator` 执行 pre/post 阶段。
- `EventHandler::handle_general_event`  
  将 `call_capability_bridge` 调用纳入 `EventEnrich` 扩展阶段。
- `wire::initialize`  
  增加 extension runtime 组装（registry/router/policy/gateway）。

---

## 5. 分阶段迁移路线（低风险）

## Phase 0（已完成/进行中）
- 完成 handlers 目录按 hook/plugin 拆分。
- `CallCapabilityBridge` 迁移至 `handlers/plugin`。

## Phase 1（建议先做）
- 引入 `ExtensionOrchestrator`（仅封装，不改现有业务语义）。
- 将 `MessageHandler` / `EventHandler` 的扩展调用改为走 orchestrator。
- 保持功能等价，新增 trace + metrics。

## Phase 2
- 引入 `ExtensionPolicy` 与统一超时/重试/降级策略。
- 为 `CallCapabilityBridge`、Pre/Post Hook 接入统一失败模式。

## Phase 3
- 引入路由规则（tenant / event_type / message_type）。
- 支持配置热更新（先本地 reload，后配置中心）。

## Phase 4
- 插件沙箱化（可选）：隔离线程池、并发令牌、熔断器。
- 引入插件健康探测与自动摘除。

---

## 6. 配置模型建议（增量）

新增配置示意：

```toml
[extension]
enabled = true

[extension.policy.default]
timeout_ms = 800
retry = 0
failure_mode = "fail_open"

[[extension.plugins]]
name = "rtc_call_enrich"
type = "grpc"
phase = "event_enrich"
enabled = true
timeout_ms = 1200
failure_mode = "fail_open"
target = "flare-capability"
match_event_types = ["EVENT_CALL_SIGNAL"]

[[extension.hooks]]
name = "content_moderation_pre_send"
phase = "before_persist"
enabled = true
timeout_ms = 500
failure_mode = "fail_closed"
tenant_allowlist = ["tenant_a", "tenant_b"]
```

---

## 7. 关键工程规范（必须）

1. 所有扩展执行函数显式透传 `ctx: &Ctx`。  
2. Application 层只编排，不直接拼 gRPC 细节。  
3. Domain 层仅依赖抽象端口（gateway trait）。  
4. 禁止 `unwrap()/panic!`，所有失败可分类可观测。  
5. 所有扩展点必须定义幂等语义与降级行为。  

---

## 8. 验收标准（Definition of Done）

- 新增一个 Hook（例如敏感词）无需改主流程 handler 代码。
- 新增一个 Plugin（例如 AI 摘要）仅需新增实现 + 配置。
- 任意扩展失败可观测且可降级，不阻断非关键链路。
- 写路径 P99 延迟增量可控（建议 < 5ms in-process，< 20ms remote plugin）。
- 支持租户级启停与灰度。

---

## 9. 近期执行优先级（建议）

1. **先落 Phase 1**：实现 `ExtensionOrchestrator` 壳层与调用收口。  
2. **再落 Phase 2**：统一策略中心（超时/降级）。  
3. **最后落 Phase 3**：路由与多租户灰度。  

该顺序能在不破坏现有行为的前提下，快速把扩展能力从“可用”提升到“可持续演进”。
