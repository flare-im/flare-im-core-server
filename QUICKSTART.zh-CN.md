# 五分钟跑通

[English](./QUICKSTART.md) · **中文**

目标：**不写一行代码、不搭用户体系**，把开源栈跑起来并用一个真实 token 调通接口。

## 先说清楚你会拿到什么

开源部分是**通信基础设施**，不含账号体系（没有注册登录、好友、群治理、朋友圈）。
所以这份快速上手不会让你「注册一个账号然后登录」—— 那条路在商业部分。

这里走的是**自带身份**模式：你手签一个 token，服务端用共享密钥验签。
真要上生产时，把手签换成你自己的用户系统即可（见文末）。

## 1. 起依赖

```bash
docker compose -f deploy/docker-compose.yml up -d \
  consul redis postgres nats rustfs
```

只需要这五个。compose 里还定义了 Kafka、Grafana、Loki、Prometheus、Tempo——
真正跑起来之后它们有用，但只为评估核心就把它们拉下来，白白多花几分钟和几 GB。

## 2. 起服务

```bash
./scripts/start_server.sh
```

服务是否就绪：

```bash
./scripts/check_services.sh
```

## 3. 签一个 token（**这一步就是「无需用户体系」的关键**）

**签发端与服务端必须用同一把密钥。** 服务端没有内置默认密钥 ——
上一步的 `start_server.sh` 会随机生成一把存进 `logs/.dev-token-secret`
并注入给各网关。所以先把它取出来：

```bash
export FLARE_TOKEN_SECRET="$(cat logs/.dev-token-secret)"

cd ../flare-server-core
TOKEN=$(cargo run -q --example mint_token -- alice)
echo "$TOKEN"
```

指定租户或有效期：

```bash
cargo run -q --example mint_token -- alice --tenant 0 --ttl 86400
```

签发者默认是 `flare-im-core`，与网关配置（`config/services/api-gateway.toml`
的 `token_issuer`）一致。**密钥或签发者对不上，第 4 步就会 401** —— 这是这份
快速上手最容易踩的坑，所以工具在没拿到密钥时会直接报错退出，而不是签出一个
注定用不了的 token。

> 注意：上面那把是本机开发密钥。生产环境请通过 `FLARE_API_GATEWAY_TOKEN_SECRET`
> 注入强密钥（至少 32 字节），签发端用同一把 —— 弱密钥等于任何人都能伪造
> 任意用户的身份。

## 4. 调接口

```bash
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:50050/api/v1/conversations
```

跑通到这里，说明**传输、验签、服务链路全通**。

## 5. 一条命令自证（推荐先跑这个）

```bash
./scripts/smoke_opensource.sh
```

它会跑 6 个真实端到端用例（发消息落库、事件总线、全事件面、全量操作面、
未读回归、端到端加密），**全程不碰任何商业组件**，退出码 0 即全通过：

```
✓ 开源栈自足：6/6 通过（全程未用到任何商业组件）
```

> RTC 房间加入不在这 6 项之列：它由 SFU 能力插件提供，而那个插件不在开源仓里。
> 插件没在跑时，示例会明确跳过这一步而不是让整例变红——否则只克隆公开仓的人
> 会以为核心链路坏了。

密钥不用另外配 —— 示例会自己读上一步生成的 `logs/.dev-token-secret`。

## 6. 看一个完整客户端

示例客户端自己签 token，所以同样要拿到服务端那把密钥：

```bash
export TOKEN_SECRET="$(cat logs/.dev-token-secret)"
NEGOTIATION_HOST=localhost:60051 \
  cargo run -p flare-im-core-examples --example chatroom_client -- user1
```

连上后会看到 `✅ 收到 CONNECT_ACK`（SDK 打的）与 `✅ 已连接到 localhost:60051`。

`examples/` 下还有 `integration_client.rs`（业务集成）与
`perf_message_send.rs`（压测），跑法相同。

---

## 从 demo 走向生产

上面手签 token 只是为了让你**不搭用户体系也能评估**。真接入时替换两处即可，
两处的契约都在开源部分：

### 换掉身份来源

网关持有的是 `Arc<dyn TokenValidator>`，验签从一开始就是可插拔的：

| 实现 | 场景 |
|---|---|
| `CoreJwtTokenValidator` | 本地验 JWT。你的用户系统用同一密钥签发 token 即可，改配置不改代码。 |
| `HttpHookTokenValidator` | 把 token POST 到你自己的接口去验。适合已有独立鉴权服务的场景。 |

### 接入你的业务规则

`crates/flare-im-hooks` 提供 9 个扩展点：

`PreSend` / `PostSend` / `Delivery` / `Recall` / `MessageRead` /
`MessageReaction` / `ConversationLifecycle` / `ConversationMember`

发消息前做敏感词校验、发送后写审计、成员变更时同步你的组织架构，都在这一层。

---

## 卡住了

| 现象 | 原因 |
|---|---|
| 接口返回 401 | token 与服务端密钥不一致；或签发者不是 `flare-im-core`；或租户不匹配（默认 `"0"`） |
| 启动脚本报 `✗ Consul 未运行 (端口 28500)` | 见下方「放置一周后再启动」 |
| 服务起不来 | 依赖没起齐，先跑 `check_services.sh` 看缺哪个 |
| token 立刻失效 | 检查机器时钟；验签带 60 秒时钟偏移宽限，漂移过大会失败 |

### 放置一周后再启动：Consul 拒绝启动

容器显示 `running`，但端口 28500 不通，启动脚本判定基础设施未就绪并中止。
`docker compose logs consul` 里会看到：

```
refusing to rejoin cluster because server has been offline for more than
the configured server_rejoin_age_max (168h0m0s) - consider wiping your data dir
```

Consul 的数据目录是 bind mount（`deploy/data/consul`），离线超过 168 小时就会
拒绝重新加入集群。全新环境不会遇到，但**跑过一次、搁置一周再回来必然遇到**。

本地开发的服务发现数据可以安全清空：

```bash
docker compose -f deploy/docker-compose.yml stop consul
rm -rf deploy/data/consul && mkdir -p deploy/data/consul
docker compose -f deploy/docker-compose.yml up -d consul
```

等它选出 leader 再起服务：

```bash
curl -s http://127.0.0.1:28500/v1/status/leader
```

---

## 本文档的验证状态

上述流程**已在 macOS 上实际跑通**（2026-08-03），不是照着代码推断出来的：

- 12 个依赖容器全部 healthy
- 全部微服务启动成功，api-gateway 监听 50050
- 用 `logs/.dev-token-secret` 签出的 token 调 `/api/v1/conversations`
  返回 **HTTP 200** `{"code":0,"data":{"conversations":[],...}}`
- 不带 token 返回 **HTTP 401**，确认鉴权确实在生效
- 示例客户端连上网关，收到 `CONNECT_ACK`

途中真实踩到并已修复/收录的问题：

1. **Consul 陈旧数据导致启动中止** —— 已收录进上面排查表
2. **`cargo run --example chatroom_client` 跑不了** —— `examples/` 既无
   `Cargo.toml` 也不是 workspace 成员，报 `no example target named ...`。
   而 `start_server.sh` 启动成功后打印的正是这条命令。已补 examples 包清单。
3. **示例客户端连不上** —— 它硬编码用 `insecure-secret` 签 token，而服务端用
   随机强密钥，必然被拒（服务端拒绝弱密钥是正确行为）。已改为读 `TOKEN_SECRET`。

边界与商业部分的划分见工作区根 `GOVERNANCE.md`。

---

跑通之后，接进自己的业务看 [INTEGRATION.md](./INTEGRATION.zh-CN.md)：
用户体系怎么接、各端客户端怎么用、生产要改哪三件事、哪些部分需要你自己实现。
