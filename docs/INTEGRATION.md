# Integration guide

**English** · [中文](./INTEGRATION.zh-CN.md)

Wire Flare IM into your own product. After reading this you should be able to answer:
**how do I connect my identity system, how do I send a message, and what do I have to
build myself.**

> Get it running first: [QUICKSTART.md](./QUICKSTART.md) (five minutes, ends with a
> one-command self-check).

---

## 0. The boundary, up front

The open-source part is **communication infrastructure**: connections, messages,
conversations, sync, read receipts, recall, group conversations, media, offline push,
end-to-end encryption.

It does **not** include an account system — no signup/login, no friend relations, no group
member governance, no moments. That is not a crippled build; it is a division of labour.
Every product's identity system looks different, and bundling one would be a burden.

So the first step of any integration is always: **tell Flare who this connection is.**

---

## 1. Connect your identity system (the only mandatory step)

The gateway holds an `Arc<dyn TokenValidator>` — validation has been pluggable from the
start. Two routes:

### Route A: your service signs a JWT, Flare validates it (recommended — config, not code)

Best when you already have a login flow and can mint one extra token on success.

```
your login API ──signs JWT──> app ──connects with token──> Flare gateway (validates with the same secret)
```

Configure `config/services/access-gateway.toml`:

```toml
[services.access_gateway]
token_issuer = "your-auth-service"   # must match the `iss` you sign with
token_ttl_seconds = 3600

[services.access_gateway.auth_provider]
mode = "core_jwt"                    # the default: validate JWT in-process
```

Secrets never go in the config file; inject them through the environment:

```bash
export ACCESS_GATEWAY_TOKEN_SECRET="<at least 32 bytes>"
export FLARE_API_GATEWAY_TOKEN_SECRET="$ACCESS_GATEWAY_TOKEN_SECRET"
```

If you also need to trust a second issuer (running old and new side by side during a
migration), add `trusted_token_issuers`:

```toml
[[services.access_gateway.trusted_token_issuers]]
issuer = "legacy-auth"
# secret also injected via the environment
```

The JWT you issue needs these claims:

| Claim | Meaning | Required |
|---|---|---|
| `sub` | User ID (Flare uses it as the message sender) | yes |
| `iss` | Issuer; must match the gateway config | yes |
| `exp` | Expiry | yes |
| `tenant_id` | Tenant; use `"0"` if single-tenant | yes |
| `device_id` | Device identifier, for multi-device | no |

**That's all.** You do not need to sync user profiles into Flare — display names and
avatars come from your business layer, see section 4.

### Route B: Flare calls your endpoint to validate

Best when the token isn't a JWT, or validation is non-trivial (checking a ban list, say).

```toml
[services.access_gateway.auth_provider]
mode = "http_hook"
hook_url = "https://your-service/internal/verify-token"
hook_timeout_ms = 800                           # default
hook_secret_header = "x-flare-auth-hook-secret" # default
# hook_secret injected via the environment; Flare sends it in the header above
```

Flare POSTs:

```json
{ "token": "...", "trace_id": "...", "request_id": "...", "path": "...", "method": "..." }
```

You return (note the field is `active`, not `valid`):

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

`active: false`, a missing `user_id`, or a non-2xx response all reject the connection.
This route costs one network hop per connection (800 ms timeout by default) in exchange
for keeping validation entirely on your side.

---

## 2. Client integration

### Rust

```rust
use flare_im_core_sdk::prelude::*;

let client = IMClient::new();
client.init(Some("my-app".into()), None).await?;

// the token comes from your login API, not from the SDK
let apis = client.login(&user_id, Some(&token), LoginDbKind::Sqlite, |_, _| {}).await?;

// send a text message
let conv = apis.conversation_api.get_one(&peer_id, &ConversationType::Single).await?;
let msg = apis.message_build_api.create_text(&conv.conversation_id, "hello", false, &[]).await?;
apis.message_api.send_no_oss(msg).await?;
```

### TypeScript / Web

```bash
npm install @flare-im/sdk
```

```ts
import { WebFlareImClient } from "@flare-im/sdk/web";

// the bridge loads the WASM runtime and local storage
const client = new WebFlareImClient(wrapWebHostBridge(bridge));
await client.login({ userId, token });   // token again comes from your login API
```

If that assembly looks tedious, `@flare-im/vue-ui` ships `createProductionAppClient()`
with the bridge, WASM and storage already wired — copy it first, then swap in your own
runtime.

Other platform packages live in
[`flare-im-core-client-sdk`](../../flare-im-core-client-sdk): Swift (iOS), Kotlin (Android),
Dart (Flutter), ArkTS/Cangjie (HarmonyOS). Same contract, naming idiomatic per language.

### Ready-made UI

```bash
npm install @flare-im/vue-ui     # 111 components, Vue 3
```

The four native platforms (Flutter / SwiftUI / Compose) implement the same component
contract; see [`flare-im-design`](../../flare-im-design). Not using it is fine — the SDK does
not depend on it.

---

## 3. Deployment

### Minimum viable

```bash
docker compose -f deploy/docker-compose.yml up -d   # Postgres / NATS / Consul
./scripts/start_server.sh
```

### Three things to change for production

1. **Secrets.** `ACCESS_GATEWAY_TOKEN_SECRET` and friends must come from a secret manager,
   at least 32 bytes. A weak secret means anyone can forge any user's identity.
2. **Storage.** Point the Postgres and object-storage entries in `config/services/*.toml`
   at your own instances.
3. **Push credentials.** For offline push, configure APNs (.p8) and FCM (service account
   JSON); see the channel implementations under `flare-push/`.

Services are stateless — scale horizontally by adding replicas; session routing goes
through Consul discovery.

---

## 4. What you implement

These are left to you **on purpose**, because they are tightly coupled to your business:

| You build | Why it's on your side | Interface |
|---|---|---|
| Signup / login | Every product differs | Issue a token (section 1) |
| Display name, avatar | The data lives in your user table | `ProfileProvider` |
| Friend / group membership | Your business rules | Validate before send, or use a hook |
| Who may message whom | Same as above | `PreSend` hook |
| Content moderation | Compliance differs by region | `PreSend` hook |

Hooks are **synchronous interceptors**: Flare calls your endpoint at key points (such as
`pre_send`) and you allow or reject. A rejected message is neither stored nor delivered.

Three registration methods are available — config file, dynamic API (database), or config
centre; transports are gRPC, WebHook or a local plugin. See
[`flare-capability/README.md`](../flare-capability/README.md).

Writing a gRPC hook plugin: [`docs/HOOK-PLUGIN-CONTRACT.md`](./HOOK-PLUGIN-CONTRACT.md)
is the wire contract — the four operations that actually reach a remote plugin,
the protobuf payload types, and what `pre_send` must return when it denies.
Runnable references: [`examples/hook_rate_limit.rs`](../examples/hook_rate_limit.rs)
and [`examples/hook_audit_log.rs`](../examples/hook_audit_log.rs).

> Hooks have a timeout (5 s by default), so a dead endpoint of yours won't drag the whole
> message path down with it — but **whether a timeout allows or rejects depends on how you
> configure it**, and that is worth confirming before production.

> Don't want to build these? Identity and social features (friends, group governance,
> moments) exist as a commercial layer speaking the same protocol; the interfaces above do
> not change.

---

## 5. End-to-end encryption

The open-source part ships the E2EE pipeline plus a reference implementation:

```bash
# The example lives in the sibling repo — the client SDK owns the E2EE surface.
cd ../flare-im-core-sdk
cargo run --example e2ee_demo --features "lifecycle-sqlite e2ee"
```

The demo prints its evidence in Chinese; this is what a successful run looks like:

```
服务端可读文本  : "[Encrypted message]"     # server-visible text
服务端载荷      : 323 字节密文              # server payload: ciphertext only
明文是否泄漏    : 否 ✅                     # plaintext leaked: no
Bob 解出        : 见面地点改到中山路 42 号   # recovered by the key holder, byte-identical
第三方解密      : 失败 ✅                   # third-party decrypt: failed
```

The architecture is **cryptography-agnostic**: `ContentEncryptionInterceptor` swaps
plaintext for a ciphertext envelope, and the actual algorithm comes from an injected
`ContentCodec`. The bundled `X25519AeadCodec` (X25519 + HKDF + XChaCha20-Poly1305) is a
usable reference implementation and a template for writing your own.

**It does not do** forward secrecy (Double Ratchet), multi-device key sync, or public-key
distribution and rotation. Those belong to key management — the interface is
`E2eeKeyManager`, and it has to be bound to your identity system.

---

## 6. When something goes wrong

| Symptom | Usually means |
|---|---|
| Connection reports "negotiation timeout" | **Token validation failed** (secret or issuer mismatch), not a network problem — check the gateway log for `Token validation failed` |
| 401 | Same as above, or an expired token |
| Message sent but not received | The conversation wasn't established, or a `PreSend` hook rejected it |
| A service won't start | `./scripts/check_services.sh` points at whichever dependency isn't ready |

Suggested order: `check_services.sh` → gateway log → `smoke_opensource.sh` (if that
passes, the problem is on your integration side, not in Flare).

---

## License

Apache-2.0. Commercial use and closed-source distribution are both fine; keep the
copyright notice.
