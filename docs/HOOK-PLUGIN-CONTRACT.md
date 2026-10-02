# Hook Plugin Contract

A hook plugin is a gRPC service Flare calls **during** message handling, so it can
allow, reject, or rewrite what happens. This document is the wire contract.

Runnable references live in this repository:
[`hook_rate_limit`](../examples/hook_rate_limit.rs),
[`hook_audit_log`](../examples/hook_audit_log.rs).

## 1. One RPC

```protobuf
service HookPlugin {
  rpc Call(GenericRequest) returns (GenericResponse);
}
```

`operation` selects which hook is being invoked. `payload` carries a
**protobuf-encoded** message — this differs from `ExtensionPlugin`, whose payload
is raw JSON. Decode it with prost using the `type_url` below.

## 2. The four operations reachable over gRPC

| `operation` | Request `type_url` suffix | When |
|---|---|---|
| `flare.hook.v1.pre_send` | `flare.capability.v1.PreSendHookRequest` | Before a message is stored or delivered |
| `flare.hook.v1.post_send` | `flare.capability.v1.PostSendHookRequest` | After it is accepted |
| `flare.hook.v1.delivery` | `flare.capability.v1.DeliveryHookRequest` | On delivery to a recipient |
| `flare.hook.v1.recall` | `flare.capability.v1.RecallHookRequest` | On recall |

`type_url` is the suffix above prefixed with `type.googleapis.com/`.

> The internal `HookKind` enum has more variants than these four (push, presence,
> login/logout, reactions, conversation lifecycle). **Those are in-process only.**
> A remote gRPC plugin can implement exactly the four above — the gRPC adapter
> does not dispatch the rest. Writing a remote handler for `message_read` or
> `presence` compiles fine and is never called.

## 3. pre_send is the one that can say no

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
(masking) is implemented rather than rejecting outright.

## 4. Failure policy is configuration, not code

Your hook does not decide what happens when it fails — the operator does, per hook:

| `error_policy` | Behaviour on failure |
|---|---|
| `fail_fast` | Abort the main flow (default) |
| `retry` | Retry, then alarm |
| `ignore` | Log and continue |

Choose `fail_fast` for anything whose absence would be a correctness or
compliance problem (moderation, quota). Choose `ignore` for observational hooks:
an audit-log hook that is down should not stop people from sending messages.

## 5. Priority decides ordering and concurrency

Message Ingest runs the hooks of its own configuration serially in priority
order (lower first); each one's `require_success` decides what a failure does.

The Capability service's hook engine (the `HookPlugin` it serves) also groups
hooks. A hook's `group` (`validation`, `critical` or `business`, case-insensitive)
places it; without one, priority 100 and above is `validation` and below is
`business`. Any other `group` value is a configuration error. The group decides
how the hook runs:

| Group | Execution | Hook rejects | Hook unreachable or timed out |
|---|---|---|---|
| Validation | serial, first | stops the chain | rejects if `require_success`, else skipped |
| Critical | serial, order preserved, after validation | stops the chain | always rejects |
| Business | last, fault tolerant | logged, chain continues | logged, chain continues |

A rejection caused by an unreachable or timed-out hook is answered as a gRPC
`UNAVAILABLE` or `DEADLINE_EXCEEDED`, never as `allow=false`: the caller then
reports a retryable `SERVICE_UNAVAILABLE` (HTTP 503) instead of a policy rejection
(HTTP 422).

A hook that must observe the *final* draft has to sort after every hook that
rewrites it — put rewriting hooks in an earlier group, not merely a lower number
inside the same one.

## 6. Configuration

Hooks are **not** self-registering: the operator wires them in, unlike capability
plugins which announce themselves. See `config/hooks.business.example.toml` in
this repository for the shape (endpoint, priority, timeout, error policy).

An explicitly configured file or directory is mandatory: a missing path, unreadable
file, malformed TOML or unreadable directory entry aborts startup. Multiple explicit
sources are merged in their configured order (directory files sort by name). No
implicit `config/hooks.toml` path can shadow the supplied Social profile. With no
configured source, the embedded empty `hooks.core.toml` profile is used. Startup logs
the source, profile and hook counts; the dispatcher also logs its installed counts.
Container deployments use `MESSAGE_INGEST_HOOKS_CONFIG=/etc/flare/config/hooks.toml`.

That asymmetry is deliberate. A hook sits in the critical path of every message
and can reject traffic; letting an arbitrary process insert itself there by
announcing would be a way to take the system down from outside.

The Capability service's hook engine also runs the hooks tenants register through
`CapabilityService/Administer` (`hook_config.*`). Rows without a tenant (and the
Capability configuration file) apply to every tenant; a tenant's rows apply only to
that tenant's events and run in addition to the global hooks. A tenant cannot replace
or switch off a global hook: the name of a global hook is reserved within its hook
type, Administer rejects a tenant `hook_config.create` or `update` that uses it with
`ALREADY_EXISTS` (a disabled global row reserves its name too), and a same-named
tenant row written some other way is not loaded (a warning is logged) while the global
hook keeps running. A non-empty `selector.tenants` further limits a hook to the listed
tenants. A write through Administer reloads that tenant's hooks at once; other changes
arrive with the periodic refresh (60 s by default). A Capability process started with
`TENANT_ID` keeps the single-tenant behaviour: it loads the global rows and that
tenant's rows only, as one configuration, without the name reservation.

## 7. Context

### HTTP Hook signature

When a webhook transport secret is configured, requests carry
`Flare-Signature: t=<unix-seconds>,v1=<hex(HMAC-SHA256(secret, timestamp + "." + raw-body))>`.
The signing secret is never sent. The old `x-flare-signature` plaintext header is
removed. Custom headers cannot override either signature header or Content-Type.
Unsigned endpoints remain possible only when no secret was configured.

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

Timestamp validation limits replay lifetime; receivers should additionally deduplicate
side effects by message/event identity. TLS remains required outside local tests.

`HookInvocationContext` carries tenant, user and trace identity. Flare calls you
out of process, so nothing restores this for you — read it from the request.

### Calling the Capability service's HookPlugin

The Capability service's `HookPlugin` accepts only Cell-signed callers: each `Call`
must carry the `x-flare-ctx` token the caller received (verified with
`FLARE_INTERNAL_CTX_PUBLIC_KEY`; the Message Ingest hook adapter forwards it
unchanged). A missing or unverifiable token, or a service started without the public
key, gets `UNAUTHENTICATED`. A `HookInvocationContext` whose `tenant_id` differs from
the signed tenant, or whose `operator_user_id` differs from a signed user, gets
`PERMISSION_DENIED`; `pre_send` accepts only a signed user. Hooks are selected by the
signed tenant, and the hooks it runs see the signed identity, not the request body's.

Do not trust the draft's author field as an identity assertion; use the context.

## 8. Checklist

- [ ] Implement only the four operations above; return an error for anything else
- [ ] Decode `payload` with prost, using the `type_url`, not JSON
- [ ] `pre_send`: always set `deny_reason_code` **and** `deny_reason_message` when denying
- [ ] Keep the hook faster than its configured timeout — it is in the send path
- [ ] Tell the operator which `error_policy` your hook needs, and why
