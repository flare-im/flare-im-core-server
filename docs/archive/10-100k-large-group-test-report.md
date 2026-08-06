# 10 万人大群测试报告

日期：2026-06-12

## 结论

本次已将 Push Server 的确定性超大群测试扩展为真实生成 100,000 个会话参与者 ID，而不是仅设置 `member_count = 100000`。

测试通过，验证了：

- recipient-less pure ping 首次进入时会按页解析完整 100,000 参与者。
- page size 为 500 时，共解析 200 页。
- 在线任务只发送给在线用户；离线用户在 pure ping online-only 模式下不构造、不编码 `PushTaskEnvelope`。
- 同一 coalesce window 内第二条高水位 ping 不再重复扫描 100,000 参与者，只更新 pending trailing ping 的最高 `max_conversation_seq`。

## 测试场景

| 项目 | 值 |
|------|----|
| 会话类型 | 超大群 recipient-less pure ping |
| 参与者数量 | 100,000 |
| 分页大小 | 500 |
| 预期分页次数 | 200 |
| 在线样本 | `member-000000`、`member-049999`、`member-099999` |
| 第一条 ping 水位 | 41 |
| 第二条 ping 水位 | 45 |
| coalesce window | 60s |

## 验收结果

| 验收项 | 结果 |
|--------|------|
| 第一条 ping 解析完整 100,000 参与者 | 通过 |
| 第一条 ping 分页次数为 200 | 通过 |
| 第一条 ping 只发布 3 个在线任务 | 通过 |
| 第二条 ping 不新增分页扫描 | 通过 |
| 第二条 ping 不新增在线任务 | 通过 |
| trailing pending ping 保留最高水位 45 | 通过 |
| offline pure ping 路径跳过 task 构造和编码 | 通过 |

## 代码覆盖

- `flare-push/server/src/application/handlers/push_router_handler.rs`
  - `publish_targeted_tasks`
  - `ConversationPingCoalescer`
  - `recipientless_conversation_ping_coalesces_high_volume_watermarks_before_member_scan`

## 执行命令

```bash
cargo test -p flare-push-server recipientless_conversation_ping_coalesces_high_volume_watermarks_before_member_scan
cargo fmt --all -- --check
cargo check -p flare-push-server
cargo test -p flare-push-server
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

## 执行结果

| 命令 | 结果 |
|------|------|
| `cargo test -p flare-push-server recipientless_conversation_ping_coalesces_high_volume_watermarks_before_member_scan` | 1 passed, 8 filtered out, 0.09s |
| `cargo fmt --all -- --check` | passed |
| `cargo check -p flare-push-server` | 2 crates compiled |
| `cargo test -p flare-push-server` | 9 passed |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | No issues found |
| `cargo test --workspace --all-features` | 343 passed, 17 ignored |

## 说明

这是本地确定性超大群功能测试，目标是验证 100,000 参与者分页、online-only pure ping、Push Server pre-pagination coalescing 和最高水位保留逻辑。

它不是全在线 100,000 设备的网络压测。全在线压力、真实 Access Gateway 下行、reader 热缓存命中率和端到端 P99 仍应在 staging/压测环境按 `09-billion-scale-validation.md` 执行。
