# 接入指南

[English](./INTEGRATION.md) · **中文**

把 Flare IM 接进你自己的业务。读完这一篇你应该能回答：**我的用户体系怎么接、
消息怎么发、我要自己实现哪些东西。**

> 先跑通再读这里：[QUICKSTART.md](./QUICKSTART.zh-CN.md)（五分钟，含一条命令的自证）。

---

## 0. 先划清边界

开源部分是**通信基础设施**：连接、消息、会话、同步、已读、撤回、群会话、
媒体、离线推送、端到端加密。

它**不含**账号体系 —— 没有注册登录、好友关系、群成员治理、朋友圈。
这不是阉割，是分工：每家的用户体系都长得不一样，硬塞一套进来反而是负担。

所以接入的第一件事永远是：**告诉 Flare「这个连接是谁」**。

---

## 1. 接你的用户体系（唯一必做的一步）

网关持有的是 `Arc<dyn TokenValidator>`，验签从一开始就是可插拔的。两条路：

### 路线 A：你的服务签 JWT，Flare 验签（推荐，改配置不改代码）

适合你已经有登录体系、能在登录成功时多签一个 token。

```
你的登录接口 ──签发 JWT──> App ──带 token 连接──> Flare 网关（用同一密钥验签）
```

配置 `config/services/access-gateway.toml`：

```toml
[services.access_gateway]
token_issuer = "your-auth-service"   # 与你签发时的 iss 一致
token_ttl_seconds = 3600

[services.access_gateway.auth_provider]
mode = "core_jwt"                    # 默认值，用内置 JWT 验签
```

密钥不写进配置文件，经环境变量注入（见下）。若你还需要同时信任另一个签发者
（比如迁移期间新旧并行），加 `trusted_token_issuers`：

```toml
[[services.access_gateway.trusted_token_issuers]]
issuer = "legacy-auth"
# secret 同样经环境变量注入
```

```bash
export ACCESS_GATEWAY_TOKEN_SECRET="<至少 32 字节的强密钥>"
export FLARE_API_GATEWAY_TOKEN_SECRET="$ACCESS_GATEWAY_TOKEN_SECRET"
```

你签发的 JWT 需要这几个 claim：

| claim | 含义 | 必填 |
|---|---|---|
| `sub` | 用户 ID（Flare 用它作为消息的发送者） | 是 |
| `iss` | 签发者，须与网关配置一致 | 是 |
| `exp` | 过期时间 | 是 |
| `tenant_id` | 租户，单租户填 `"0"` | 是 |
| `device_id` | 设备标识，多端登录时用 | 否 |

**就这些。** 你不需要把用户资料同步给 Flare —— 昵称头像由你的业务层提供，
见下面第 4 节。

### 路线 B：Flare 回调你的接口验签

适合 token 不是 JWT、或验签逻辑复杂（比如要查封禁状态）的情况。

```toml
[services.access_gateway.auth_provider]
mode = "http_hook"
hook_url = "https://your-service/internal/verify-token"
hook_timeout_ms = 800                          # 默认 800ms
hook_secret_header = "x-flare-auth-hook-secret" # 默认值
# hook_secret 经环境变量注入，Flare 会带在上面这个 header 里
```

Flare 会 POST：

```json
{ "token": "...", "trace_id": "...", "request_id": "...", "path": "...", "method": "..." }
```

你返回（注意字段是 `active`，不是 `valid`）：

```json
{
  "active": true,
  "user_id": "u_123",
  "tenant_id": "0",
  "device_id": "optional",
  "expires_at": 1785999999,
  "scopes": [],
  "metadata": {}
}
```

`active: false`、缺 `user_id` 或非 2xx 一律拒绝连接。这条路每次连接多一跳网络，
超时默认 800ms，但换来的是验签逻辑完全在你手里。

---

## 2. 客户端接入

### Rust

```rust
use flare_im_core_sdk::prelude::*;

let client = IMClient::new();
client.init(Some("my-app".into()), None).await?;

// token 来自你的登录接口，不是 SDK 生成的
let apis = client.login(&user_id, Some(&token), LoginDbKind::Sqlite, |_, _| {}).await?;

// 发一条文本
let conv = apis.conversation_api.get_one(&peer_id, &ConversationType::Single).await?;
let msg = apis.message_build_api.create_text(&conv.conversation_id, "你好", false, &[]).await?;
apis.message_api.send_no_oss(msg).await?;
```

### TypeScript / Web

```bash
npm install @flare-im/sdk
```

```ts
import { WebFlareImClient } from "@flare-im/sdk/web";

// bridge 负责加载 WASM 运行时与本地存储
const client = new WebFlareImClient(wrapWebHostBridge(bridge));
await client.login({ userId, token });   // token 同样来自你的登录接口
```

嫌装配繁琐的话，`@flare-im/vue-ui` 里有现成的 `createProductionAppClient()`
把 bridge / WASM / 存储都装好了，直接拿到可用的 client —— 可以先照着它抄，
再按自己的运行时替换。

其他平台包见 [`flare-im-core-client-sdk`](../flare-im-core-client-sdk)：
iOS(Swift) / Android(Kotlin) / Flutter(Dart) / 鸿蒙(ArkTS)。
契约一致，命名按各语言习惯。

### 现成的 UI

```bash
npm install @flare-im/vue-ui     # 111 个组件，Vue 3
```

原生四端（Flutter / SwiftUI / Compose）同一套组件契约，见
[`flare-im-design`](../flare-im-design)。不想用也行，SDK 不依赖它。

---

## 3. 部署

### 最小可用

```bash
docker compose -f deploy/docker-compose.yml up -d   # Postgres / NATS / Consul
./scripts/start_server.sh
```

### 生产要改的三件事

1. **密钥**：`ACCESS_GATEWAY_TOKEN_SECRET` 等必须经密钥管理注入，
   至少 32 字节。弱密钥 = 任何人都能伪造任意用户身份。
2. **存储**：`config/services/*.toml` 里的 Postgres / 对象存储指向你自己的实例。
3. **推送证书**：要离线推送的话配 APNs（.p8）与 FCM（service account JSON），
   见 `flare-push/` 下各 channel 的实现（APNs 用 .p8，FCM 用 service account JSON）。

服务是无状态的，横向扩容直接加副本；会话路由经 Consul 发现。

---

## 4. 你需要自己实现的部分

这些是**故意**留给你的，因为它们和你的业务强耦合：

| 你要做的 | 为什么在你这边 | 接口 |
|---|---|---|
| 用户注册登录 | 每家都不一样 | 签发 token（第 1 节） |
| 昵称头像 | 数据在你的用户表里 | `ProfileProvider` |
| 好友 / 群成员关系 | 你的业务规则 | 发消息前自行校验，或用 hook |
| 谁能给谁发消息 | 同上 | `PreSend` hook |
| 消息内容审核 | 各地合规要求不同 | `PreSend` hook |

Hook 是**同步拦截**：Flare 在关键节点（如 `pre_send`）回调你的接口，你返回放行
或拒绝，拒绝的消息不会落库也不会投递。

三种注册方式任选：配置文件、动态 API（数据库）、配置中心；传输侧支持 gRPC、
WebHook、本地插件。契约与配置方式见 [`flare-capability/README.md`](./flare-capability/README.md)。

> Hook 有超时控制（默认 5s），你的接口挂了不会把消息链路一起拖死 ——
> 但**超时的默认行为是放行还是拒绝，取决于你怎么配**，上生产前务必确认这一项。

> 不想自己实现这些？身份与社交业务（好友、群治理、朋友圈）有现成的商业版本，
> 与开源部分同一套协议，接口不变。

---

## 5. 端到端加密

开源部分自带 E2EE 管线与一个参考实现：

```bash
# 例子在同级仓：端到端加密的接口面归客户端 SDK。
cd ../flare-im-core-sdk
cargo run --example e2ee_demo --features "lifecycle-sqlite e2ee"
```

```
服务端可读文本  : "[Encrypted message]"
服务端载荷      : 323 字节密文
明文是否泄漏    : 否 ✅
Bob 解出        : 见面地点改到中山路 42 号
第三方解密      : 失败 ✅
```

架构上是**密码学无关**的：`ContentEncryptionInterceptor` 负责把明文换成密文信封，
具体算法由注入的 `ContentCodec` 决定。仓库里的 `X25519AeadCodec`
（X25519 + HKDF + XChaCha20-Poly1305）是可用的参考实现，也是你自己实现时的模板。

**它不做**前向保密（Double Ratchet）、多设备密钥同步、公钥分发与轮换 ——
这些属于密钥管理，接口是 `E2eeKeyManager`，需要绑定你的身份体系。

---

## 6. 遇到问题

| 症状 | 多半是 |
|---|---|
| 连接报「协商超时」 | **token 验签失败**（密钥或 issuer 对不上），不是网络问题 —— 看网关日志的 `Token 验证失败` |
| 401 | 同上，或 token 过期 |
| 消息发出去对方收不到 | 会话未建立，或 `PreSend` hook 拒绝了 |
| 服务起不来 | `./scripts/check_services.sh` 会指出是哪个依赖没就绪 |

排查顺序建议：`check_services.sh` → 网关日志 → `smoke_opensource.sh`
（能跑通说明问题在你的接入侧，不在 Flare）。

---

## 许可

Apache-2.0。可商用、可闭源分发，保留版权声明即可。
