# 贡献指南

感谢你考虑为 Flare IM 贡献。本文说明如何提交改动。

## 开始之前

**先开 issue 讨论。** 除非是显而易见的小修复（错别字、坏链接），请先开 issue 说明
你想做什么。这能避免你写完一大段代码后才发现方向与项目规划冲突。

架构方向与未完成项见：

- [`docs/roadmap/open-items.md`](https://github.com/flare-im/flare-workspace/blob/main/docs/roadmap/open-items.md) —— 未完成技术项的权威清单
- [`docs/roadmap/competitive-roadmap.md`](https://github.com/flare-im/flare-workspace/blob/main/docs/roadmap/competitive-roadmap.md) —— 能力对标与路线
- [`docs/roadmap/oss-commercial-strategy.md`](https://github.com/flare-im/flare-workspace/blob/main/docs/roadmap/oss-commercial-strategy.md) —— 项目形态与商业边界

## 仓库结构

Flare IM 是多仓库项目，改动前先确认该去哪个仓库：

| 仓库 | 内容 |
|---|---|
| `flare-proto` / `flare-grpc-proto` | 协议契约（改动影响全链路，需谨慎） |
| `flare-core` | 传输层（WebSocket / QUIC） |
| `flare-server-core` | 服务端基座 |
| `flare-im-core` | 服务端微服务 |
| `flare-im-core-sdk` | 客户端核心（Rust） |
| `flare-im-core-client-sdk` | 七端 SDK 与示例应用 |
| `flare-im-design` | 跨端 UI Kit |
| `flare-social` | 社交业务层 |
| `flare-sdk-plugin` | 插件 SDK（能力插件的契约、schema 与模板） |
| `flare-plugin` | 插件宿主指南 |
| `official` | 官网与对外文档 |

**分层纪律**：能力应当下沉到最底层的合适位置。同一逻辑不要在多端各写一遍——
放进 Rust 核心，各端通过 FFI/WASM 消费。

## 提交改动

1. 从 `main` 建分支
2. 写改动，**同时补测试**
3. 本地跑通验证（见下）
4. 提 PR，说明**为什么**这么改，而不只是改了什么

### 提交信息

```
<类型>: <一句话说明>

<为什么需要这个改动；如果修了 bug，说明根因而不只是症状>
```

类型：`feat` / `fix` / `perf` / `refactor` / `docs` / `test` / `chore`

### 本地验证

Rust 仓库：

```bash
cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings && cargo test
```

前端仓库：

```bash
npm run typecheck && npm test
```

**PR 必须是绿的。** 测试失败、clippy 有告警的 PR 不会被合并。

## 代码要求

- **不要留 `unwrap()` / `expect()` 在非测试的主路径上**。错误要么处理，要么向上传播。
- **不要为了通过测试而改测试**。测试红了通常意味着代码有问题。
- **跨端改动要五端对齐**。改了一端的组件契约，其余端要么同步改，要么在 PR 里说明
  为什么不需要。
- **公开 API 的改动要更新对应文档与 CHANGELOG。**

## 协议契约改动

改 `flare-proto` / `flare-grpc-proto` 影响全链路，额外要求：

- **删字段必须 `reserved`**（字段号与名字都要），否则字段号被复用会导致线格式错乱
- 说明前后兼容性影响：老客户端遇到新服务端会怎样，反之亦然
- 契约改动的 PR 需要更长的评审周期

## 许可

提交贡献即表示你同意你的贡献按 [Apache-2.0](LICENSE) 授权，且你有权这么做。

## 安全问题

**不要用 issue 或 PR 报告安全漏洞。** 见 [SECURITY.md](SECURITY.md)。
