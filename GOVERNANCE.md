# 项目治理

本文说明 Flare IM 的决策方式。目的不是形式化流程，而是让使用方（尤其是要把本项目
放进生产环境的团队）能判断：这个项目由谁决定方向、改动如何被接受、会不会突然变卦。

## 当前状态：单一维护方

Flare IM 目前由核心团队维护，**尚未形成多方治理结构**。这一点对采纳方很重要，
所以明说而不是含糊带过：

- 技术方向由核心团队决定
- 外部贡献欢迎，但合并与否由核心团队判断
- 尚无独立的技术委员会或投票机制

如果你的组织需要「项目不会被单一方向左右」的保证，请在采纳前把这一点纳入评估。
随着外部贡献者增加，治理结构会相应演进，届时本文会更新。

## 决策流程

**日常改动**（bug 修复、性能优化、文档）：走 PR 评审，维护者批准即可合并。

**架构改动**（跨层影响、公开 API 变更、协议契约调整）：需要先开 issue 或设计文档
讨论，达成一致后再实施。设计文档放在 `docs/design/`。

**破坏性变更**：需要说明迁移路径、影响范围与兼容期安排。协议契约的破坏性变更
（如删字段）要求 `reserved` 保护字段号，并在 CHANGELOG 明确标注。

## 版本与兼容性

- 遵循[语义化版本](https://semver.org/lang/zh-CN/)
- **主版本号变更**才允许破坏性变更
- 已发布版本**不撤回**。发错了就发新版本修正，不删旧版本——已经依赖它的人不该
  因为我们的失误而构建失败

### 版本对齐（目标，尚未达成）

契约层（`flare-proto` / `flare-grpc-proto`）独立演进；实现层
（`flare-core` / `flare-server-core` / `flare-im-core` / `flare-im-core-sdk` /
`flare-im-core-client-sdk` / `flare-im-design`）**同一发布批次使用同一版本号**，
每次发布附兼容矩阵。

> ⚠️ **当前尚未对齐**：实现层实际是 1.0.3 与 1.0.4 混用（契约层已到 2.0.0）。
> 首次公开发布时统一，见
> [`docs/roadmap/product-and-business-design.md`](docs/roadmap/product-and-business-design.md) §1.3。
> 在对齐完成前，请以各仓库自身的 `Cargo.toml` 为准。

## 开源与商业的边界

Flare IM 采用**开放核心（Open Core）**模式：**通信开源，身份与社交业务商业**。

### 开源（Apache-2.0）

通信基础设施的完整链路：

| 仓库 | 内容 |
|---|---|
| `flare-proto` / `flare-grpc-proto` | 协议契约 |
| `flare-core` | 传输层（WebSocket / QUIC） |
| `flare-server-core` | 服务端基座 + 鉴权契约 |
| `flare-im-core` | IM 微服务 |
| `flare-im-core-sdk` | 客户端核心 |
| `flare-im-core-client-sdk` | 七端 SDK |
| `flare-im-design` | 跨端 UI Kit |

### 商业授权

- 账号体系、好友关系、群治理（角色 / 入群审批 / 禁言）、朋友圈
- 业务能力插件

### 开源部分是通信基础设施，不是开箱即用的 IM 产品

说在前面，避免你 clone 完才发现登不上去：

**开源部分不含账号体系。** 但它自带完整且可插拔的鉴权契约，两条路都在开源侧：

- `CoreJwtTokenValidator` —— 本地验 JWT。**手签一个 token 就能跑起来做 demo /
  POC，不需要任何用户体系。**
- `HttpHookTokenValidator` —— 把 token POST 到你自己的接口。**接入自有用户体系
  的入口。**

业务规则同理，`flare-im-core/crates/flare-im-hooks` 提供 9 个扩展点
（PreSend / PostSend / Delivery / Recall / MessageRead / MessageReaction /
ConversationLifecycle / ConversationMember / GetConversationParticipants）。

所以要上生产，你需要自行实现用户体系并按上述契约接入。这与 Sendbird /
Twilio Conversations 的「自带身份」模型一致，区别是 Flare 可自托管、
协议与核心可审计。

商业部分只是这套公开契约的一个实现 —— 架构上并不特殊，你完全可以按同样的契约
写自己的那一套。

### 不变的承诺

- 本组织开源仓库中的代码按 [Apache-2.0](LICENSE) 授权，**已发布的内容不会被
  追溯收回**
- 商业能力是**独立的产品**，不会通过改变现有开源代码的许可来实现
- 鉴权与 hooks 契约属于开源部分，**不会为了逼迫付费而閹割或闭源**

## 安全响应

见 [SECURITY.md](SECURITY.md)。安全问题的处理优先于功能开发。

## 联系

- 技术讨论：GitHub issue
- 安全问题：见 SECURITY.md
- 商业咨询：`flare1522@163.com`
