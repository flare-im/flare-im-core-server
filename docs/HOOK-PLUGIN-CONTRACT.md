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

Hooks are grouped by priority, and the group decides how they run:

| Group | Execution |
|---|---|
| Validation | serial, fail fast |
| Critical | serial, order preserved |
| Business | concurrent, fault tolerant |

A hook that must observe the *final* draft has to sort after every hook that
rewrites it — put rewriting hooks in an earlier group, not merely a lower number
inside the same one.

## 6. Configuration

Hooks are **not** self-registering: the operator wires them in, unlike capability
plugins which announce themselves. See `config/hooks.business.example.toml` in
this repository for the shape (endpoint, priority, timeout, error policy).

That asymmetry is deliberate. A hook sits in the critical path of every message
and can reject traffic; letting an arbitrary process insert itself there by
announcing would be a way to take the system down from outside.

## 7. Context

`HookInvocationContext` carries tenant, user and trace identity. Flare calls you
out of process, so nothing restores this for you — read it from the request.

Do not trust the draft's author field as an identity assertion; use the context.

## 8. Checklist

- [ ] Implement only the four operations above; return an error for anything else
- [ ] Decode `payload` with prost, using the `type_url`, not JSON
- [ ] `pre_send`: always set `deny_reason_code` **and** `deny_reason_message` when denying
- [ ] Keep the hook faster than its configured timeout — it is in the send path
- [ ] Tell the operator which `error_policy` your hook needs, and why
