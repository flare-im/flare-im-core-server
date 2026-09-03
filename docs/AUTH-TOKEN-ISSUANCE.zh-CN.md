# 接入 Token 的签发与刷新：从客户端本地签发迁到服务端

## 1. 问题

现状是 core SDK 在客户端本地签发接入 JWT（`generate_core_token`，HS256），签名密钥要么打进安装包，
要么让用户在登录页手填。两种都不成立：

- 密钥进了客户端 = 任何拿到安装包的人都能伪造任意用户身份（`mint_token.py` 与网关文档早已写明）。
- 用户手填密钥只适合联调，生产用户根本不知道密钥是什么（2026-09-03 生产 web 实测就是这个坑）。
- 刷新逻辑散落在各端应用层（web 组合式按 `exp` 自算、原生端基本没刷新），五端行为不一致。

## 2. 三种部署形态，一个客户端契约

| 形态 | 谁签发/刷新 | 谁校验 | 客户端拿 token 的方式 |
|---|---|---|---|
| **只用 core** | `flare-api-gateway` 暴露 `/api/v1/auth/tokens`（签发）与 `/api/v1/auth/tokens/refresh`（刷新），生命周期实现在 `flare-server-core::auth::issuer`，网关只做路由 | 各网关 `core_jwt` 模式 | **SDK 托管**：配置 `auth.tokenEndpoint`，`login(userId)` 不传 token，SDK 自己去网关取并到期前刷新 |
| **flare-social** | `flare-user` 登录返回 `im_connect_token`（`local_generate`），刷新走 `flare-user` 的 refresh；或 `hook` 模式由业务系统签 | 网关信任 social issuer，或 `http_hook` 回调 social | **应用托管**：social SDK 已把 `im_connect_token` 作为显式 token 传给 core `login`，刷新后调 `update_access_token`（现状已如此，本次只验证不改） |
| **自建业务** | 业务后端自己签（持有密钥或调网关签发接口，用 app 凭据） | `core_jwt`（trusted issuer）或 `http_hook` | **应用托管**：同上，业务 App 调自家登录接口拿 token，传给 `login(userId, token)`；监听 `TOKEN_EXPIRED` 后刷新并 `update_access_token` |

客户端只有两条路：**SDK 托管**（core-only）与**应用托管**（social / 自建）。两条路都不接触签名密钥。
`sdk.generate_core_token` 从五端契约中删除；`CoreTokenConfig` 只留给服务端 crate 与测试。

## 3. 网关 API（core-only 形态）

原则遵守 `GATEWAY_SPEC.md`：网关不拥有 token 生命周期，签发/刷新/撤销实现下沉到
`flare-server-core::auth::issuer::TokenIssuer`，网关只暴露路由、做入站鉴权与限流。

### 3.1 签发 `POST /api/v1/auth/tokens`

```json
{ "userId": "hugo", "tenantId": "0", "deviceId": "ios-abc", "ttlSecs": 3600 }
```

谁能调：

1. **业务后端（生产）**：带 `x-app-id` + `x-app-secret`，与网关配置的 app 凭据匹配（`FLARE_API_GATEWAY_APP_CREDENTIALS="app1:secret1,app2:secret2"`）。
   这是服务端到服务端调用，客户端永远不持有 app secret。
2. **开发/示例（联调）**：`FLARE_API_GATEWAY_AUTH_DEV_ISSUE=true` 时允许不带凭据直接按 userId 签发。
   默认关闭；开启时启动日志 WARN 醒目提示，且受网关限流。示例 App 的「只输用户 ID 登录」走这条。

响应：

```json
{ "token": "eyJ…", "expiresAt": 1788452076, "userId": "hugo", "tenantId": "0" }
```

### 3.2 刷新 `POST /api/v1/auth/tokens/refresh`

`Authorization: Bearer <当前 token>`，请求体可空。规则：

- token 签名与 issuer 必须合法；`exp` 允许已过期但不超过 `refresh_grace_secs`（默认 7 天）；
- 若配置了 `TokenStore`（Redis），旧 `jti` 必须未被撤销，签发新 token 后撤销旧 `jti`（轮换）；
- 新 token 继承 `sub / tenant_id / device_id`，`exp = now + ttl`。

### 3.3 `http_hook` 模式

网关鉴权为 `http_hook` 时，签发/刷新同样转发给业务 hook（`FLARE_API_GATEWAY_AUTH_HOOK_ISSUE_URL`），
带同一个 hook secret 头；未配置则 `501`，提示「签发已委托给业务认证系统」。

## 4. SDK 改造

- `SdkConfig.auth`：`{ tokenEndpoint?: string, appId?: string }`（`tokenEndpoint` 缺省 = `httpUrl`）。
- `login(userId, token?)`：`token` 为空且配置了 `tokenEndpoint` → SDK 调 3.1 签发；否则沿用显式 token；
  两者都没有 → 明确报错「未配置 token 来源」。不再回退到环境变量或 `dev-test-token`。
- 刷新归核心：核心解析 `exp`，到期前 5 分钟（可配）调 3.2 刷新并 `update_access_token`；重连前若 token 已过期也先刷新；
  收到 `TOKEN_EXPIRED` 事件时同样刷新后重连。应用托管形态下核心不发起刷新，只抛事件。
- 契约：删除 `sdk.generate_core_token`；`client_config.json` 增加 `auth` 段；七个绑定重新生成。
- kit 与五个示例 App：登录页只留「用户 ID」（高级区保留「粘贴接入 Token」给应用托管形态），
  删除「签名密钥」输入与本地签发；刷新逻辑从 web 组合式移除，交给核心。

## 5. 验证

- server-core：签发/刷新/轮换/宽限期/撤销单测。
- api-gateway：端点集成测试（dev_issue 关→401、app 凭据对/错、刷新宽限内/外、http_hook 转发）。
- core SDK：`login` 无 token 走网关的单测（mock HTTP）；刷新调度单测；契约 codegen-check。
- 生产 118.107.9.221：网关镜像重建部署（`FLARE_API_GATEWAY_AUTH_DEV_ISSUE=true`），web 只输用户 ID 登录成功；
  iOS 模拟器同样只输用户 ID 登录。
- social：flare-social e2e 里登录→IM 连接路径不变（回归）。
