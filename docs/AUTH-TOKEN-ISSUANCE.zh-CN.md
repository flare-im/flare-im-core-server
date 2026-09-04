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

## 6. Token 验证（Validation）：core 独立 / 三方接入 / 二者混合

签发之外，**验证**同样双模式，且可组合。网关（接入网关做长连接鉴权、api-gateway 做 REST 鉴权）
都通过 `build_token_validator(&auth, token_service, trusted_issuers)` 拿到一个 `TokenValidator`。

### 6.1 三种编排

| 形态 | 配置 | 行为 |
|---|---|---|
| **core 独立验证** | `AUTH_MODE=core_jwt`，不配 hook | 本地 HS256 校验 core 自签 token（+ `trusted_token_issuers` 受信 issuer）。零网络开销。 |
| **三方 hook 验证（纯委托）** | `AUTH_MODE=http_hook` + `AUTH_HOOK_URL` | 每枚 token 都 POST 给业务 hook 验证，业务方拥有整套 token 流程。 |
| **混合（core 本地 + 三方兜底）** | `AUTH_MODE=core_jwt` + `AUTH_HOOK_URL` | `ChainedTokenValidator` 分层：先本地（core JWT + 受信 issuer，认得的零网络开销），本地不认的（三方 opaque token）才回落到 hook 询问业务方。**一套部署既能 core 独立验证，又能适配三方 token 验证。** |

`flare-social` 现走「受信 issuer」（`flare-user` 用共享密钥签 JWT，core 本地按 issuer+secret 验），
无网络开销；若业务方改用 opaque token 或要服务端强控（撤销/轮换），换用**验证 hook**（混合形态）即可，客户端契约不变。

### 6.2 验证 hook 契约

- 请求 `POST {AUTH_HOOK_URL}`，头带 `{AUTH_HOOK_SECRET_HEADER}: {AUTH_HOOK_SECRET}`：
  `{ "token", "traceId", "requestId", "path", "method" }`
- 响应（200）：`{ "active": true, "userId", "tenantId?", "deviceId?", "appId?", "expiresAt?", "scopes": [], "metadata": {} }`；
  `active=false` 或 401/403 → 拒绝；5xx/超时 → `ProviderUnavailable`（**可重试**，不把可能合法的三方 token 永久拒掉）。
- 超时 `AUTH_HOOK_TIMEOUT_MS`（默认 800ms）。在长连接鉴权热路径上，本地层零开销，只有三方 opaque token 才触发这一跳。

### 6.3 刷新令牌（access + refresh 双 token）

`core_jwt` 签发时下发一对：短效 access（`token_ttl_seconds`，默认按部署，生产 7 天）+ 长效 refresh
（`refresh_token_ttl_seconds`，默认 30 天，`token_use=refresh`）。刷新端点收到 refresh token 即发新 access
并轮换 refresh；兼容旧客户端拿宽限内的 access 换新。`http_hook` 形态下 refresh 由业务 hook 负责（响应可带 `refreshToken`）。
客户端凭 refresh token 免重登续期，支撑 7x24。
