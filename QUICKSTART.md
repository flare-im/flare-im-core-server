# Five-minute start

**English** · [中文](./QUICKSTART.zh-CN.md)

Goal: **without writing a line of code and without an identity system**, get the
open-source stack running and call a real API with a real token.

## What you get

The open-source part is **communication infrastructure**. It does not include an account
system — no signup/login, no friends, no group governance, no moments. So this guide will
not have you "register an account and log in"; that path lives in the commercial layer.

What it does have is the **bring-your-own-identity** model: you sign a token yourself and
the server validates it with a shared secret. When you go to production, replace the
hand-signing with your own user system (see the end of this page).

## 1. Start dependencies

```bash
docker compose -f deploy/docker-compose.yml up -d
```

## 2. Start services

```bash
./scripts/start_server.sh
```

Check readiness:

```bash
./scripts/check_services.sh
```

## 3. Sign a token — this is the "no identity system needed" step

**The signer and the server must use the same secret.** The server ships no default
secret. `start_server.sh` generates a random one into `logs/.dev-token-secret` and injects
it into the gateways, so read it from there:

```bash
export FLARE_TOKEN_SECRET="$(cat logs/.dev-token-secret)"

cd ../flare-server-core
TOKEN=$(cargo run -q --example mint_token -- alice)
echo "$TOKEN"
```

With an explicit tenant or TTL:

```bash
cargo run -q --example mint_token -- alice --tenant 0 --ttl 86400
```

The issuer defaults to `flare-im-core`, matching `token_issuer` in
`config/services/api-gateway.toml`. **A mismatched secret or issuer gives you a 401 in
step 4** — the single most common trip-up in this guide, which is why the tool exits with
an error when it can't find a secret instead of minting a token that is guaranteed not to
work.

> ⚠️ That is a local development secret. In production, inject a strong one (at least 32
> bytes) via `FLARE_API_GATEWAY_TOKEN_SECRET` and sign with the same key — a weak secret
> means anyone can forge any user's identity.

## 4. Call an API

```bash
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:50050/api/v1/conversations
```

Getting this far means transport, token validation and the service chain all work.

## 5. Prove it in one command (do this first)

```bash
./scripts/smoke_opensource.sh
```

It runs five real end-to-end cases (send + persist, event bus, full operation surface,
unread regression, RTC room join) plus the E2EE demo, **without touching any commercial
component**. Exit code 0 means all passed:

```
✅ Open-source stack is self-sufficient: 6/6 passed (no commercial components involved)
```

No extra secret configuration needed — the examples read the
`logs/.dev-token-secret` generated in step 2.

## 6. Look at a full client

The example client signs its own token, so it needs the same secret:

```bash
export TOKEN_SECRET="$(cat logs/.dev-token-secret)"
NEGOTIATION_HOST=localhost:60051 \
  cargo run -p flare-im-core-examples --example chatroom_client -- user1
```

Once connected you'll see `CONNECT_ACK received` and `connected to localhost:60051`.

`examples/` also has `integration_client.rs` (business integration) and
`perf_message_send.rs` (load test); same invocation.

---

## From demo to production

Hand-signing a token above exists purely so you can **evaluate without building an
identity system**. For a real integration you replace two things, and both contracts are
in the open-source part:

### Replace the identity source

The gateway holds an `Arc<dyn TokenValidator>` — validation has been pluggable from the
start:

| Implementation | When to use |
|---|---|
| `CoreJwtTokenValidator` | Validate JWT locally. Your user system signs with the same secret; configuration change, no code change. |
| `HttpHookTokenValidator` | POST the token to your own endpoint. Suits an existing standalone auth service. |

### Plug in your business rules

`crates/flare-im-hooks` offers nine extension points:

`PreSend` / `PostSend` / `Delivery` / `Recall` / `MessageRead` / `MessageReaction` /
`ConversationLifecycle` / `ConversationMember` / `GetConversationParticipants`

Content moderation before send, audit records after send, syncing your org chart on
membership change — all belong at this layer.

---

## Stuck?

| Symptom | Cause |
|---|---|
| API returns 401 | Token secret differs from the server's; or the issuer isn't `flare-im-core`; or the tenant doesn't match (default `"0"`) |
| Connection reports "negotiation timeout" | Almost always **token validation failure**, not a network problem — check the gateway log for `Token validation failed` |
| A service won't start | `./scripts/check_services.sh` points at whichever dependency isn't ready |

---

Once it runs, see [INTEGRATION.md](./INTEGRATION.md) to wire it into your own product:
how to connect your identity system, how to use each platform client, the three things to
change for production, and which parts you need to implement yourself.
