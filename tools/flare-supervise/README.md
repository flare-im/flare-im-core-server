# flare-supervise (DEP-06)

`flare-supervise` is a small, business-neutral multi-process supervisor. The
Core runtime image exposes it as both `/usr/local/bin/flare-supervise` and
`/usr/local/bin/flare-core`:

```sh
flare-core validate --services=all
flare-core supervise --services=all
flare-core supervise --services=api-gateway,signaling-gateway
flare-core health
```

The default manifest is `/etc/flare/supervise-core.toml` (source:
`release/supervise-core.toml`). `--manifest=/absolute/file` permits Social to
reuse the same binary with its own service list. `--socket=/absolute/path` (or
`FLARE_SUPERVISE_HEALTH_SOCKET`) overrides the local health socket.

## Runtime contract

- `all` starts every manifest entry except conditional entries whose
  `enabled_if_env` is absent. Explicitly selecting a disabled entry is an error.
  Unknown and duplicate names are errors.
- Every command is an absolute executable path and is spawned directly, not
  through a shell. The supervisor validates all selected binaries and required
  environment variables **before** spawning any service. The Core manifest
  rejects Control private keys and Control DB URLs in the parent environment.
- `required_env_unless` declares a narrow exact-value exception to a
  `required_env` entry in the **same scope**: top-level requirements use a
  top-level exception; a service-specific requirement uses an exception inside
  that service. The condition variable must be in `shared_env` for a global
  exception or inherited by that service for a service-level exception, and
  cannot be overridden by the child's `environment` literals. Thus the child
  observes the same mode the supervisor checked. An absent, padded, or
  different value does not satisfy the exception; selecting an unrelated
  service does not inherit another service's requirements. This only changes
  presence checking; the child service still validates the mode's supported
  values.
- Child environments start empty. `shared_env`, `shared_env_prefixes`, and each
  service's `inherit_env` / `inherit_env_prefixes` are the only variables copied
  from the parent; `environment` provides service-local literals, applied last
  so they override inherited values. `environment_defaults` entries
  (`when_env`, `equals`, `values`) give a child values only while the inherited
  `when_env` equals `equals` exactly, and only for variables the parent does
  not set at all: a parent value, even an empty one, wins. Validation requires
  the mode variable and every default to be inherited by that child (so the
  parent can still override a default), and rejects a mode pinned by a literal,
  a name that is both a literal and a default, empty or padded values, duplicate
  names and defaults that set a mode variable. Core's Cell
  signing private key reaches only API and signaling gateways. Push credentials
  reach only `push-worker`. Add any new service setting to the proper manifest
  scope; an unlisted setting is intentionally ignored.
- This is **environment routing, not a hard secret sandbox**: all children in
  one container currently run as the same Unix UID, and on ordinary Linux
  `/proc/<pid>/environ` can expose a sibling process's inherited credentials
  to that UID. A high-security deployment must keep signing ingress in
  separate containers or distinct UIDs with proc isolation, or use a dedicated
  signing service/FD-based secret handoff. Do not claim that `env_clear` alone
  enforces private-key isolation.
- On exit or spawn failure, a service restarts independently with bounded
  exponential backoff (500 ms to 30 s in the Core manifest). A process that
  lived at least 60 s resets the backoff. Child stdout and stderr are streamed
  to container stderr as `[service][stdout|stderr]` lines, split at 16 KiB so
  an untrusted line cannot grow supervisor memory without bound.
- SIGTERM/SIGINT stops restarts, forwards SIGTERM to each child process group,
  waits up to 20 s, then sends SIGKILL to any remaining group. This is local
  process termination only; the supervisor does not kill unrelated PIDs/ports.
- `health` returns success only while the supervisor socket responds, every
  selected child is running, and each configured TCP listener accepts a local
  connection. Queue-only workers are checked for process liveness. This is a
  Cell liveness check, **not** a message-flow or dependency-readiness proof.

For DEP-08's new **single Core container only**, use this Compose healthcheck:

```yaml
healthcheck:
  test: ["CMD", "flare-core", "health"]
  interval: 10s
  timeout: 3s
  start_period: 90s
  retries: 3
```

Do not put this healthcheck on the shared base image: the existing
one-process-per-container Compose explicitly runs individual service binaries,
which do not create the supervisor socket. Their commands continue to override
the image's default `CMD` unchanged.

## Core single-container required environment

The Core manifest requires explicit `FLARE_CONFIG_PATH`,
`FLARE_MQ_DEFAULT_BACKEND`, `FLARE_POSTGRES_URL`, `FLARE_REDIS_URL_BASE`,
`FLARE_NATS_URL`, and `SERVICE_ADVERTISE_HOST` so a container cannot silently
use the source config's localhost development defaults. With the default or
explicit `FLARE_REGISTRY_MODE=configured`, `FLARE_CONSUL_ENDPOINT` is also
required. Only exact `FLARE_REGISTRY_MODE=static` lets the supervisor omit it;
Core then disables service registration and discovery, and this does not by
itself provide static targets for every caller or prove DEP-08 viability.
In static mode the manifest's `environment_defaults` give API Gateway and
Admin Gateway the `FLARE_{API,ADMIN}_GATEWAY_GRPC_*_SERVICE_URL` of this
container's listeners (media 60081, conversation 50090, online 50061 and
storage reader 60083 for both; message ingest 50182 and orchestrator 50181 for
API Gateway only; capability 50110 for Admin Gateway only), so they connect
directly instead of trying discovery and logging a fallback WARN per
downstream; values the deployment sets win, and configured mode keeps
discovery.
The Core manifest fixes the signaling gateway child `PORT=60051` and
`GRPC_PORT=60060` regardless of parent values, preserving WS/QUIC 60051/60052
and matching the in-Cell GatewayRouter gRPC target. In static mode, an enabled
Message Ingest gRPC Hook using `discovery://` fails startup; mount a profile
with an explicit private `http(s)://` Social Hook URI.
Use the Core container's network alias (for example `core`) as
`SERVICE_ADVERTISE_HOST` for every registry entry in configured mode. The API and signaling
gateways additionally require their respective end-user token secrets and the
Cell context signing private key. Signed-context consumers require the public
key (including Push Proxy; it never receives the private key) and may also
receive the component key set `FLARE_INTERNAL_CTX_PUBLIC_KEYS`. Media requires
the S3 endpoint and credentials. Message Ingest requires
its Hook configuration path. Admin Gateway joins `all` only when
`FLARE_ADMIN_GATEWAY_ENABLED` is set; it verifies the Staff context the Social
gateway admin partition signs with the Cell key set (Staff pinned to
`kid=gateway`), needs the private key to re-sign that staff for Media, and may
receive `FLARE_AUDIT_NATS_URL` for management effect events. Only Online and
Capability inherit `FLARE_PLATFORM_PUBLIC_KEY`, which verifies the platform's
scheduler and projection worker assertions; no Core service verifies an employee
with it, and the platform signing keys are forbidden in the supervisor environment.

Push Server and Push Worker are also excluded from default `all` until the
deployment enforces broker provenance. The manifest's
`PUSH_SERVER_DELIVERY_READY` and `PUSH_WORKER_DELIVERY_READY` conditions only
select the processes; setting them **does not** bypass each binary's separate
fail-closed startup gate (`<ROLE>_EVENT_PROVENANCE_MODE`, see
`flare_im_service_kit::push_delivery`). The single-host installer sets
`broker-isolated`, which the gate accepts only with an authenticated NATS URL;
`signed-v1` stays reserved for multi-host topologies. `flare-core health` can
be green without Push and must not be interpreted as successful online/offline
delivery.

The supervisor does not generate or distribute keys, migrate schemas, or
provision infrastructure. DEP-08 must provide an explicit Cell environment and
perform migrations before startup. A full `smoke_message_flow.sh` through the
single-container stack remains the DEP-06 acceptance gate; unit tests alone do
not close it.
