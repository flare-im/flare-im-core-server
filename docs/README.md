# Flare IM Core 文档索引

本目录是 `flare-im-core` 的主文档入口，面向架构设计、生产接入、消息可靠性、第三方集成和测试运维。旧目录 `../doc/` 暂时保留为历史设计材料和深度参考，本次不修改。

## 阅读顺序

1. [架构与技术栈总览](01-architecture-overview.md)
2. [消息可靠性与低延迟链路](02-message-reliability-and-low-latency.md)
3. [消息类型与通知持久化](03-message-model-and-notification-persistence.md)
4. [第三方接入与使用说明](04-third-party-integration.md)
5. [业务系统接入示例](05-business-system-examples.md)
6. [测试、性能与运维](06-testing-performance-and-operations.md)
7. [Core 架构优化落地方案](07-core-architecture-optimization-roadmap.md)
8. [亿级演进整体改造规划](08-billion-scale-evolution-plan.md)
9. [亿级落地验证标准](09-billion-scale-validation.md)
10. [10 万人大群测试报告](10-100k-large-group-test-report.md)
11. [超大群与可靠性整改方案](11-large-group-and-reliability-remediation.md)

## 文档分类

| 类别 | 文档 | 回答的问题 |
|------|------|------------|
| 架构 | [01-architecture-overview.md](01-architecture-overview.md) | Core 的边界是什么，服务如何分层，技术栈怎么选，topic 和存储怎么串起来。 |
| 可靠性 | [02-message-reliability-and-low-latency.md](02-message-reliability-and-low-latency.md) | 关键消息流程如何降低延迟，如何追求 0 可观测丢失，失败后怎么恢复。 |
| 协议 | [03-message-model-and-notification-persistence.md](03-message-model-and-notification-persistence.md) | 有哪些消息类型，通知是否持久化，系统消息/临时消息/操作事件怎么设计。 |
| 接入 | [04-third-party-integration.md](04-third-party-integration.md) | 业务服务、客户端 SDK、HTTP API、内部 gRPC、Hook、Capability 应该怎么接。 |
| 示例 | [05-business-system-examples.md](05-business-system-examples.md) | 业务系统如何实现用户、好友、群、成员与权限，并与 Core 交互。 |
| 测试运维 | [06-testing-performance-and-operations.md](06-testing-performance-and-operations.md) | 如何启动、测试、压测、看指标、看 ledger、定位丢消息或延迟问题。 |
| 落地方案 | [07-core-architecture-optimization-roadmap.md](07-core-architecture-optimization-roadmap.md) | 网关边界、message-ingest、PostgreSQL/TimescaleDB、统一鉴权如何分阶段落地。 |
| 演进规划 | [08-billion-scale-evolution-plan.md](08-billion-scale-evolution-plan.md) | 亿级容量下低延迟、0 丢失、同步策略的瓶颈在哪，统一读扩散内核如何分四阶段改造。 |
| 验证标准 | [09-billion-scale-validation.md](09-billion-scale-validation.md) | 统一读扩散、大群 notify+pull、DLQ replay、压测和混沌演练如何验收。 |
| 测试报告 | [10-100k-large-group-test-report.md](10-100k-large-group-test-report.md) | 10 万参与者大群 pure-ping 分页、online-only、coalescing 的功能验证结果。 |
| 整改方案 | [11-large-group-and-reliability-remediation.md](11-large-group-and-reliability-remediation.md) | 100k 会话复检发现的伪读扩散四层基底、Redis 关键路径耦合等 15 个问题的逐条解决方案。 |
| 测试报告 | [10-100k-large-group-test-report.md](10-100k-large-group-test-report.md) | 10 万人大群 recipient-less ping、分页、coalescing 和验证结果。 |

## 接入协议推荐

生产环境业务系统接入 Core 时，主链业务 Hook 推荐使用 gRPC Hook，高频服务间调用推荐使用 typed gRPC。HTTP/OpenAPI 主要用于外部三方、管理后台、业务后台低频操作和临时适配。

## 目录边界

- `docs/`：当前主文档，保持结构化、可维护、面向实现和接入。
- `doc/`：旧设计记录、历史方案、临时报告、drawio 和深度讨论材料，后续可以逐步迁移。
- 各 crate 内 README/设计文档：保留模块局部细节，例如 `flare-api-gateway/GATEWAY_SPEC.md`、`flare-orchestrator/docs/*`。

## 写作约定

- 稳定协议语义必须指向 typed fields、enum、proto 或明确的领域模型。
- `attributes` 和 `extensions` 只描述业务扩展，不能承载 Core 稳定语义。
- 所有可靠性承诺要区分持久消息和临时消息。
- 所有三方接入示例要明确身份来源、租户、幂等键、权限校验位置和 Core 边界。
