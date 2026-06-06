# 测试、性能与运维

本文说明 `flare-im-core` 的本地启动、测试矩阵、压测入口、性能读数和生产排障观测点。

## 本地依赖

`deploy/docker-compose.yml` 拉起本地中间件：

| 服务 | 端口 | 用途 |
|------|------|------|
| Consul | `28500` | 服务注册发现。 |
| Redis | `26379` | WAL、ACK、presence、缓存。 |
| PostgreSQL / TimescaleDB | `25432` | 消息、事件、会话、媒体、ledger。 |
| NATS JetStream | `24222` / `28222` | 默认 MQ。 |
| Kafka | `29092` | 可选 MQ 后端。 |
| RustFS | `29000` / `29001` | S3 兼容对象存储。 |
| Prometheus | `29090` | 指标。 |
| Grafana | `23000` | Dashboard。 |
| Loki | `3100` | 日志。 |
| Tempo | `3200` / `4317` / `4318` | Trace。 |

启动：

```bash
cd flare-im-core/deploy
docker compose up -d
```

启动 Core 服务：

```bash
cd flare-im-core
./scripts/start_server_core.sh
```

接入业务系统 Hook：

```bash
# 先启动业务系统 gRPC Hook 服务，并确保它已注册到服务发现或有固定 endpoint。
# 然后将 config/hooks.toml 指向业务系统 Hook 配置，再启动 Core。
./scripts/start_server.sh
```

## 核心服务启动顺序

建议顺序：

1. `flare-signaling-online`
2. `flare-signaling-route`
3. `flare-capability`
4. `flare-conversation`
5. `flare-orchestrator`
6. `flare-storage-writer`
7. `flare-storage-reader`
8. `flare-push-server`
9. `flare-push-worker`
10. `flare-signaling-gateway`
11. `flare-core-gateway`
12. `flare-admin-gateway`

原因：

- online/route 为接入层提供路由和在线状态。
- conversation/orchestrator/storage/push 是消息主链。
- gateway 最后启动，避免代理到尚未注册的下游。

## 测试矩阵

| 测试类别 | 目标 | 示例 |
|----------|------|------|
| 单元测试 | 领域规则和 handler 小边界 | message profile、durability、WAL replay、gateway DTO。 |
| 集成测试 | 跨服务交互 | message flow、storage consistency、push delivery。 |
| 并发测试 | 多租户、多会话、高并发 | `tests/integration/concurrent_scenarios/*`。 |
| 错误场景 | 超时、网络、服务不可用、数据格式错误 | `tests/integration/error_scenarios/*`。 |
| 压力测试 | 高负载、长跑、资源限制 | `tests/integration/stress/*`。 |
| 性能测试 | 批处理、并发推送、消息发送 | `tests/performance/*`、`examples/perf_message_send.rs`。 |

常用命令：

```bash
cargo test --workspace
cargo test -p flare-im-core --lib
cargo test -p flare-orchestrator
cargo test -p flare-storage-writer
```

针对真实中间件的测试需要先启动 Docker Compose，并按测试说明准备环境变量。

## 消息发送压测

压测入口：

```bash
cargo run -p flare-im-core --example perf_message_send
```

环境变量：

| 变量 | 默认值 | 含义 |
|------|--------|------|
| `PERF_ENDPOINT` | `http://127.0.0.1:50181` | `MessageSendService` endpoint。 |
| `PERF_TOTAL` | `1000` | 发送总量。 |
| `PERF_CONCURRENCY` | `32` | 并发数。 |
| `PERF_PAIRS` | `64` | 用户对数量。 |
| `PERF_TENANT_ID` | `0` | 租户。 |
| `PERF_PAYLOAD_BYTES` | `64` | 文本 payload 大小。 |
| `PERF_METRICS_ENABLED` | `true` | 是否读取 Prometheus metrics。 |
| `PERF_STORAGE_WAIT_TIMEOUT_MS` | `10000` | 等待 storage metrics 收敛时间。 |
| `PERF_ORCHESTRATOR_METRICS_ENDPOINT` | `http://127.0.0.1:19181/metrics` | orchestrator metrics。 |
| `PERF_STORAGE_WRITER_METRICS_ENDPOINT` | `http://127.0.0.1:19182/metrics` | storage writer metrics。 |

示例：

```bash
PERF_TOTAL=3000 \
PERF_CONCURRENCY=64 \
PERF_PAIRS=128 \
PERF_PAYLOAD_BYTES=64 \
cargo run -p flare-im-core --example perf_message_send
```

## 当前性能报告

`../doc/message_send_performance_report_2026-06-06.md` 记录了一次本地 dev build 集成压测。该报告覆盖：

- `flare-orchestrator` `MessageSendService`
- Redis WAL
- NATS JetStream fanout
- `flare-storage-writer`
- PostgreSQL/TimescaleDB
- `flare-push-server`
- `flare-push-worker`

结果摘要：

| 场景 | Total | Success | Throughput | Avg | P95 | P99 | Storage loss observed |
|------|------:|--------:|-----------:|----:|----:|----:|----------------------:|
| Smoke 64B | 10 | 10 | 147.42 ACK/s | 21.920 ms | 25.186 ms | 25.186 ms | 0 |
| Single conversation 64B | 1000 | 1000 | 318.46 ACK/s | 98.411 ms | 178.856 ms | 288.940 ms | 0 |
| Multi conversation 64B | 3000 | 3000 | 179.45 ACK/s | 351.117 ms | 570.038 ms | 1348.198 ms | 0 |
| Multi conversation 1KB | 1000 | 1000 | 157.95 ACK/s | 396.724 ms | 1158.081 ms | 1339.489 ms | 0 |

解释：

- 这是本地开发构建，不是生产容量上限。
- 离线推送后端未配置，push worker 出现重投递噪声，影响尾延迟。
- durability 检查显示基线后 5100 条发送对应 5100 条消息和 5100 条 ledger，failed 为 0。

## 推荐压测分层

### 1. 写路径基线

只启动：

- conversation
- orchestrator
- storage-writer
- PostgreSQL
- Redis
- JetStream/Kafka

目标：测量 WAL、MQ、storage writer 和 ledger，不受 push offline backend 影响。

### 2. 推送路径基线

启动：

- signaling gateway
- signaling online
- route
- push-server
- push-worker
- 在线客户端

目标：测量在线/离线投递、push ack、redelivery。

### 3. 端到端体验

启动全栈和 SDK 客户端。

目标：测量用户感知的 send ack、push 到达、sync 补齐、重连恢复。

## 可靠性校验

压测后至少检查：

```sql
SELECT count(*) FROM messages WHERE created_at >= '<baseline>';

SELECT write_state, count(*)
FROM message_write_ledger
WHERE updated_at >= '<baseline>'
GROUP BY write_state;
```

期望：

- 发送成功数和新增消息数一致，或有可解释的 deduplicated。
- `message_write_ledger.write_state = failed` 为 0。
- 没有长期停留的 non-ack ledger rows。
- WAL pending 不持续增长。
- MQ DLQ 没有新增不可恢复消息。

## 观测指标

| 指标 | 说明 |
|------|------|
| `message_orchestrator_send_stage_duration_seconds` | 发送各阶段耗时。 |
| `message_orchestrator_send_total` | 按 durability/outcome 统计发送。 |
| `mq_process_ack_total` | MQ consumer ack/nack/term 结果。 |
| `mq_process_ack_duration_seconds` | MQ consumer ack/nack/term 耗时。 |
| storage writer persist latency | 存储写延迟。 |
| push online/offline backlog | 推送堆积。 |
| WAL pending count | 恢复压力。 |
| `message_write_ledger` | durable write 状态事实表。 |

低基数要求：

- metrics label 不要使用 tenant_id、conversation_id、message_id。
- trace/log 可以带 request_id、trace_id、message_id。

## 排障手册

### 已收到 `BrokerAccepted`，但历史查不到

1. 查 `message_write_ledger` 是否存在该 `server_msg_id`。
2. 没有 ledger：查 `TOPIC_MESSAGE_MAIN` lag、WAL pending、orchestrator main consumer 日志。
3. 有 ledger 但 failed：查 storage writer、DB、event stream、DLQ。
4. storage persisted：查 storage reader 查询条件、tenant、conversation_id、seq。
5. 客户端未看到：查 sync cursor 和 push backlog。

### 消息重复

1. 检查业务方是否重试时换了 `client_msg_id`。
2. 检查 storage writer 幂等 key 是否启用。
3. 检查 WAL replay 是否重复投递但未被幂等收敛。
4. 检查客户端 UI 是否按 `server_msg_id`/`client_msg_id` 去重。

### 尾延迟高

1. 看 send stage metrics：是否卡在 Hook、conversation ensure、WAL、MQ publish。
2. 看 DB 连接池是否耗尽。
3. 看 push-worker 是否无 backend 导致 redelivery storm。
4. 看 MQ lag 和 ack wait/max deliver。
5. 看 Hook endpoint 超时和重试策略。

### 重要通知丢失

1. 检查是否使用 `NotificationContent.persistent = false`。
2. 如果是 false，这是临时通知，不承诺离线恢复。
3. 需要历史可查时改为 `persistent = true`。
4. 业务已有可靠通知中心时，可以保留 false，但客户端要从业务通知中心补齐。

## 生产前检查清单

- Rust release build 可用，未因未使用的 MQ 后端依赖阻断。
- PostgreSQL 总连接池小于 `max_connections`，并为每个服务设置明确 pool size。
- JetStream/Kafka durable、retry、DLQ 配置确认。
- offline push backend 已配置，或 push-worker 对不可投递任务有 park/DLQ 策略。
- 主链业务 Hook 使用 gRPC transport，并有短超时、熔断和明确 error policy。
- 业务系统到 Core 的高频服务间调用使用 typed gRPC，HTTP/OpenAPI 只作为外部、管理或低频 facade。
- message write ledger、MQ lag、DLQ、WAL pending、send stage metrics 接入 Dashboard。
- Admin Gateway 查询接口有分页、limit、时间范围和审计。
- 压测分离写路径和推送路径，端到端压测不替代分层基线。

## 建议后续优化

- 为不同服务设置独立 PostgreSQL pool profile。
- 将 offline push 未配置场景改为 bounded parking 或 DLQ，避免重投递风暴影响主链压测。
- 为 JetStream 和 Kafka 分别建立 CI profile，避免默认编译未使用后端阻塞 release。
- 缓存或批量化高频单聊 conversation ensure。
- 增加 storage persist、ledger transition、push redelivery 的一等 Prometheus 指标。
