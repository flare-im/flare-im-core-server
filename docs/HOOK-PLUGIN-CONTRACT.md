# Hook Plugin Contract

A hook plugin is a gRPC service or webhook Flare calls **before** a message is stored, so it can
allow, reject, or rewrite it. This document is the wire contract.

A runnable reference lives in this repository: [`hook_rate_limit`](../examples/hook_rate_limit.rs).

Hooks are synchronous interception only. To learn what happened rather than stop it, use events,
not a hook.

## 1. One RPC

```protobuf
service HookPlugin {
  rpc Call(GenericRequest) returns (GenericResponse);
}
```

`operation` selects which hook is being invoked. `payload` carries a
**protobuf-encoded** message — this differs from `ExtensionPlugin`, whose payload
is raw JSON. Decode it with prost using the `type_url` below.

## 2. One hook point

| Point | `operation` | Request `type_url` suffix | When |
|---|---|---|---|
| `message.pre_send` | `flare.hook.v1.pre_send` | `flare.capability.v1.PreSendHookRequest` | Before a message is stored or delivered |

`type_url` is the suffix above prefixed with `type.googleapis.com/`. Answer any other `operation`
by allowing it, so a point added later does not stop your traffic.

The point catalog lives in `flare-im-core-hooks`. A new point is added at the command that needs
interception; the runtime and the control plane do not change.

## 3. A hook allows, rejects or rewrites

```protobuf
message PreSendHookResponse {
  bool allow = 1;
  HookMessageDraft draft = 2;              // rewritten draft, honoured when allow=true
  HookRoutingHints routing = 3;
  map<string, string> annotations = 4;     // passed through, read-only downstream
  HookExtensionBag outcome_extensions = 5;
  string deny_reason_code = 6;             // machine-readable, required when allow=false
  string deny_reason_message = 7;          // human-readable, safe to show operators
}
```

A rejected message is **neither stored nor delivered**. Return both reason fields:
the code is what downstream systems branch on, the message is what a human reads
in a support ticket. Rejecting with neither produces a message that vanished with
no explanation anywhere.

Returning a modified `draft` rewrites the message. This is how moderation
(masking) is implemented rather than rejecting outright. A returned `draft` replaces the payload,
headers and metadata, so send all three back. `payload` must decode as a `MessageContent` and must
not empty a message that had content; otherwise the rewrite failed (see §5). To change nothing,
leave `draft` unset.

## 4. One chain

Message Ingest runs one chain per send, serially:

**application hooks, then platform hooks, then deployment hooks**, and by `order` (lower first)
within a source.

Each hook sees the draft the previous one left. The deployment hooks are last on purpose: they see
the final content, so an application rewrite cannot get past them, and their allow is the final
verdict. A hook that rejects ends the chain.

`hook_type` says which point a hook belongs to. There are no priorities, groups, error policies or
retries: a hook is required or optional (`require_success`).

Application hooks cannot be created yet: Administer refuses them until there are outbound
validation, an egress switch and quotas. Platform hooks are not affected.

Applications cannot switch off or replace a platform or deployment hook. Their names are reserved
within the hook type: Administer rejects an application `hook_config.create` or `update` using one
with `ALREADY_EXISTS` (a disabled platform row reserves its name too). A non-empty
`selector.tenants` further limits a hook to the listed tenants.

## 5. Failure semantics

| Case | Required | Optional |
|---|---|---|
| Target rejects explicitly | Rejected | Rejected |
| Target times out, is unreachable or answers something unreadable; circuit open | 503, retryable | Skipped |
| Rewrite does not decode, or empties the message | 503, original kept | This rewrite skipped |
| Total budget exhausted | 503, retryable | Skipped |
| Disabled by an administrator | Not called | Not called |

503 means `SERVICE_UNAVAILABLE`, gRPC `UNAVAILABLE`, HTTP 503. An explicit rejection is HTTP 422
and must not be retried. The error a sender sees names the hook, never the downstream error text.

Every step gets the smaller of the hook's `timeout_ms` and what is left of the total budget,
`MESSAGE_INGEST_EXTENSION_PRE_SEND_TIMEOUT_MS` (1500 ms by default). Keep hooks fast: a slow hook
spends the budget of the hooks after it.

A hook that fails or is skipped leaves the draft as it found it.

Each Ingest instance keeps a circuit breaker per (application, hook): consecutive failures or
timeouts inside a window open it, and while open the hook is not called and the table above applies.
After the open period one probe goes through. An answer, allow or reject, counts as healthy. Budget
exhaustion counts for nothing, since the hook was not at fault. Settings are `[hook_circuit]` in
`config/services/message-ingest.toml` and `FLARE_HOOK_CIRCUIT_*`.

Message Ingest records each call in `hook_executions` through a bounded buffer: rejections,
failures and timeouts in full, allows and short circuits sampled, without message content or
downstream error text. Sending never waits for this. Settings are `[hook_executions]` and
`FLARE_HOOK_EXECUTIONS_*`. Statistics, queries and retention are in the Capability service.

## 6. Configuration

Hooks are **not** self-registering: the operator wires them in, unlike capability
plugins which announce themselves. A hook sits in the critical path of every message
and can reject traffic; letting an arbitrary process insert itself there by
announcing would be a way to take the system down from outside.

There are two sources.

| | Deployment file | Database `hook_configs` |
|---|---|---|
| Holds | Required policy, such as the Social send policy | Platform and application hooks |
| Change | Edit the deployment, restart | Takes effect in about a second |
| At startup | Missing or invalid: the service exits | Unreadable: the service does not become ready |
| Target | Internal | gRPC or webhook |

**Deployment file.** See `config/hooks.business.example.toml` for the shape. An explicitly
configured file or directory is mandatory: a missing path, unreadable file, malformed TOML or
unreadable directory entry aborts startup. Multiple explicit sources are merged in their configured
order (directory files sort by name). No implicit `config/hooks.toml` path can shadow the supplied
Social profile. With no configured source, the embedded empty `hooks.core.toml` profile is used.
Startup logs the source, profile and hook counts; the dispatcher also logs its installed counts.
Container deployments use `MESSAGE_INGEST_HOOKS_CONFIG=/etc/flare/config/hooks.toml`. Message
Ingest writes a summary of what it installed to `hook_deployment_summary`, without secrets, header
or metadata values, for read-only display.

**`hook_configs`.** The Capability service writes rows through `CapabilityService/Administer`
(`hook_config.*`). A row with a tenant belongs to that application; a row without one is a platform
hook. Each write bumps the row's scope in `hook_config_versions` in the same transaction. Message
Ingest polls about once a second, fetches the rows of the scopes that changed and swaps them in
behind an atomic pointer, so a send costs one load and one lookup per application. While the
database is unreachable Message Ingest keeps the last good configuration and warns; a scope with an
invalid row keeps its last good hooks while other scopes reload. The Capability service does not run
hooks, so a Capability outage does not affect sending.

## 7. Context

### HTTP Hook signature

When a webhook transport secret is configured, requests carry
`Flare-Signature: t=<unix-seconds>,v1=<hex(HMAC-SHA256(secret, timestamp + "." + raw-body))>`.
The signing secret is never sent. The old `x-flare-signature` plaintext header and the old
`X-Hook-Signature` (an HMAC of the body alone, without a timestamp) are removed; custom
headers cannot use any of these names or Content-Type (compared case-insensitively). A hook
with such a header is rejected when the deployment file is loaded or written through
Administer (`INVALID_ARGUMENT`), not on every delivery. Unsigned endpoints remain possible
only when no secret was configured. A delivery whose context has no tenant is not sent.

A secret registered through Administer is write-only: `hook_config.create`, `get`,
`list` and `update` return an empty `transport.secret`, and
`transport.metadata["signing_secret_configured"]` says whether one is set. An `update`
with an empty secret keeps the stored one; send a new value to rotate it. To remove the
secret and go back to unsigned delivery, send an `update` with an empty secret and
`transport.metadata["clear_signing_secret"] = "true"`; that key is never in a read-back,
so writing a read-back configuration back never clears the secret. `"false"` or no key
leaves the secret alone, any other value is `INVALID_ARGUMENT`, and so is the key set to
`"true"` together with a non-empty secret.

Custom webhook header values and gRPC `transport.metadata` values (often credentials such
as `Authorization` or `x-api-key`) are write-only as well: reads return every key with
the fixed value `<redacted>`. On `update` the map replaces the stored one key by key: an
empty or `<redacted>` value keeps the stored value of that key (matched exactly, then
case-insensitively), a new value replaces it, and a key left out is removed; an empty or
`<redacted>` value for a key that has no stored value is `INVALID_ARGUMENT`. `create`
rejects `<redacted>` values. Errors name the key, never a value.

Stored values never follow a hook to a new delivery target. When an `update` changes where
the hook delivers — the webhook `endpoint`; for gRPC the `endpoint`, `service_name`,
`registry_type` or `namespace`; or the transport type — an empty or `<redacted>` header or
metadata value is not kept, and neither is a stored signing secret behind an empty
`transport.secret`. Such an `update` is rejected with `INVALID_ARGUMENT` and a message that
starts with `delivery target changed` and names the key (or the signing secret), never a
value; nothing is written. Send the values again for the new target, and for the secret send
a new one or set `clear_signing_secret` to `"true"`. Otherwise anyone allowed to update the
hook could point it at an endpoint of their own and receive the hidden `Authorization`
header, API key or signing secret with the next delivery. Changing only the load balancing
or the timeout keeps the stored values.

`hook_config.create` registers only `pre_send`; any other `hook_type` is `INVALID_ARGUMENT`
as unknown. A target is gRPC or a webhook: `local` is for code linked into the process and is
refused.

A webhook with `secret = ""` in the deployment file is rejected when the file is loaded (the
error names the hook; the service does not start). Omit `secret` for unsigned delivery.

Verify the **raw request bytes**, before JSON parsing, and reject timestamps outside
the allowed clock skew. Example receiver logic (Python standard library):

```python
import hashlib
import hmac
import time

def verify_hook(secret: bytes, raw_body: bytes, signature: str) -> bool:
    try:
        parts = signature.split(",")
        if len(parts) != 2 or not parts[0].startswith("t=") or not parts[1].startswith("v1="):
            return False
        timestamp = parts[0][2:]
        if not timestamp.isascii() or not timestamp.isdecimal():
            return False
        if abs(int(time.time()) - int(timestamp)) > 300:
            return False
        expected = hmac.new(secret, timestamp.encode() + b"." + raw_body, hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, parts[1][3:])
    except (TypeError, ValueError, OverflowError):
        return False
```

Rust receivers can call `flare_im_core_hooks::hooks::adapters::verify_webhook_signature`
(with `WEBHOOK_SIGNATURE_TOLERANCE_SECS`, 300 s), which applies the same checks.

Timestamp validation limits replay lifetime; receivers should additionally deduplicate
side effects by message/event identity. TLS remains required outside local tests.

`HookInvocationContext` carries tenant, user and trace identity. Flare calls you
out of process, so nothing restores this for you — read it from the request.

`HookInvocationContext` carries tenant, user and trace identity. Flare calls you
out of process, so nothing restores this for you — read it from the request.

### gRPC hook caller identity

A gRPC hook call carries the `x-flare-ctx` token Message Ingest received and sets
`GenericRequest.request_id` to the signed request ID, so a plugin can verify the caller with
`FLARE_INTERNAL_CTX_PUBLIC_KEY`. Message Ingest does not call a gRPC hook without a signed context,
and `pre_send` runs only for a signed user: system and platform senders skip it. A plugin should
answer `UNAUTHENTICATED` to a missing or unverifiable token and `PERMISSION_DENIED` when a
`HookInvocationContext` names another tenant or user than the signed one.

Do not trust the draft's author field as an identity assertion; use the context.

## 8. Checklist

- [ ] Implement `flare.hook.v1.pre_send`; allow any other operation
- [ ] Decode `payload` with prost, using the `type_url`, not JSON
- [ ] Always set `deny_reason_code` **and** `deny_reason_message` when denying
- [ ] When rewriting, send the whole draft back with a payload that decodes as `MessageContent`
- [ ] Keep the hook faster than its configured timeout — it is in the send path
- [ ] Tell the operator whether your hook must be required, and why
