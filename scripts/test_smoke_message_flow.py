#!/usr/bin/env python3
"""Self-test for scripts/smoke_message_flow.sh (W1-06, W9-G; DEP-06/SEC-07/S1).

Runs the real script against recording stubs of curl, podman, docker, grpcurl
and psql placed first on PATH; no network, container or database is touched.
The stub curl is a tiny fake of the Edge routes and enforces that Social calls
carry the Social access token and Core calls carry the IM token.

Main guarantees asserted:
  * the HTTP mode never sends identity headers (x-tenant-id, x-user-id,
    x-flare-ctx, x-actor-*) and no gRPC client or host psql is ever invoked;
  * tokens and passwords never appear in any child's argv or in the output;
  * the default tenant and malformed tenants are refused before any request;
  * the persistence check is a read-only SELECT executed inside the container;
  * --mode grpc only execs flare-smoke-grpc, handing it non-secret values and
    the paths of two private (600) files; it refuses missing or open key and
    password files, HTTP-mode settings, the legacy unsigned settings and the
    default tenant before the client starts, and needs neither python3 nor curl.

When a built flare-smoke-grpc is available ($FLARE_SMOKE_GRPC_BIN, or
target/debug/flare-smoke-grpc of this workspace), the gRPC-mode dry run is
also exercised end to end with a generated Ed25519 key: the plan is printed,
and a default database password or a non-private key is refused.

Run: python3 scripts/test_smoke_message_flow.py -v
"""
import base64
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent / "smoke_message_flow.sh"
TENANT = "e2e-" + "0123456789abcdef0123"
PASSWORD = "Smoke-Fixture-Passw0rd"
BASE_URL = "https://edge.smoke.test:18443"
PG_CONTAINER = "flare-p0e2e-postgres-1"
FORBIDDEN_HEADER = re.compile(r"^\s*x-(tenant-id|user-id|flare-ctx|actor-[a-z-]+)\s*:", re.I)
CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

USERS = {"a": "10001", "b": "10002"}
TOKENS = {
    ("access", "10001"): "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMDAwMSJ9.social-access-signature-a",
    ("access", "10002"): "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMDAwMiJ9.social-access-signature-b",
    ("im", "10001"): "eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiIxMDAwMSJ9.im-connect-signature-a",
    ("im", "10002"): "eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiIxMDAwMiJ9.im-connect-signature-b",
}
SECRETS = [PASSWORD, *TOKENS.values(), "refresh-secret-value-a", "refresh-secret-value-b"]

FAKE_CURL = r'''
import json, os, sys, urllib.parse

log_dir = os.environ["FAKE_LOG_DIR"]
tokens = json.loads(os.environ["FAKE_TOKENS"])
users = json.loads(os.environ["FAKE_USERS"])
fail = os.environ.get("FAKE_FAIL", "")
social = os.environ.get("FAKE_SOCIAL_PREFIX", "/social-api") + "/api/v1"
core = os.environ.get("FAKE_CORE_PREFIX", "/api") + "/api/v1"
state_path = os.path.join(log_dir, "state.json")
state = json.load(open(state_path)) if os.path.exists(state_path) else {"passwords": {}, "friends": False}

VALUE = {"-X", "-H", "-o", "-w", "--data-binary", "--cacert", "--connect-to", "--proto",
         "--max-time", "--connect-timeout", "--max-redirs", "--noproxy"}
FLAGS = {"--silent", "--show-error"}
args = sys.argv[1:]
rec = {"argv": args, "headers": [], "method": "GET", "body": None, "url": None, "opts": {}}
i = 0
while i < len(args):
    arg = args[i]
    if arg in VALUE:
        value = args[i + 1]
        i += 2
        if arg == "-H":
            if value.startswith("@"):
                rec["headers"] += [l for l in open(value[1:]).read().splitlines() if l]
            else:
                rec["headers"].append(value)
        elif arg == "-X":
            rec["method"] = value
        elif arg == "--data-binary":
            rec["body"] = open(value[1:]).read() if value.startswith("@") else value
        else:
            rec["opts"][arg] = value
    elif arg in FLAGS:
        i += 1
    elif arg.startswith("-"):
        sys.stderr.write("fake curl: unknown option " + arg + "\n")
        sys.exit(2)
    else:
        rec["url"] = arg
        i += 1

with open(os.path.join(log_dir, "curl.jsonl"), "a") as fh:
    fh.write(json.dumps(rec) + "\n")

url = urllib.parse.urlsplit(rec["url"])
path = url.path
auth = [h.split(":", 1)[1].strip() for h in rec["headers"] if h.lower().startswith("authorization:")]
bearer = auth[0][len("Bearer "):] if auth and auth[0].startswith("Bearer ") else ""
body = json.loads(rec["body"]) if rec["body"] else {}

def who(kind):
    for uid in users.values():
        if bearer == tokens[kind + ":" + uid]:
            return uid
    return None

def ok(data):
    return 200, {"code": 0, "data": data}

def err(status, code, reason, message=""):
    return status, {"code": code, "reason": reason, "message": message}

def session(uid):
    return ok({"user_id": uid, "tokens": {
        "access_token": tokens["access:" + uid], "refresh_token": "refresh-secret-value-" + ("a" if uid == users["a"] else "b"),
        "expires_in": 900, "session_id": "sess-" + uid, "im_connect_token": tokens["im:" + uid]}})

def user_for(login_key):
    return users["a"] if login_key.startswith("smoke-a-") else users["b"]

status, payload = 404, {"code": 404, "reason": "NOT_FOUND"}
if path == social + "/auth/register" and rec["method"] == "POST":
    if body.get("tenant_id") != os.environ["FAKE_TENANT"]:
        status, payload = err(404, 404, "TENANT_NOT_FOUND")
    else:
        state["passwords"][body["login_key"]] = body["password"]
        status, payload = session(user_for(body["login_key"]))
elif path == social + "/auth/login" and rec["method"] == "POST":
    uid = user_for(body["login_key"])
    if fail == "login":
        status, payload = err(401, 401, "INVALID_CREDENTIALS",
                              "rejected " + tokens["access:" + uid])
    elif state["passwords"].get(body["login_key"]) != body.get("password"):
        status, payload = err(401, 401, "INVALID_CREDENTIALS")
    else:
        status, payload = session(uid)
elif path == social + "/relations/friend-requests" and rec["method"] == "POST":
    if who("access") != users["a"] or body.get("target_user_id") != users["b"]:
        status, payload = err(401, 401, "UNAUTHORIZED")
    else:
        status, payload = ok({"request_id": "fr-1", "status": 1})
elif path == social + "/relations/friend-requests/fr-1/respond" and rec["method"] == "POST":
    if who("access") != users["b"] or body != {"accept": True}:
        status, payload = err(401, 401, "UNAUTHORIZED")
    else:
        state["friends"] = True
        status, payload = ok({"request_id": "fr-1", "status": 2})
elif path == social + "/relations/friends/check/" + users["b"] and rec["method"] == "GET":
    if who("access") != users["a"]:
        status, payload = err(401, 401, "UNAUTHORIZED")
    else:
        status, payload = ok({"is_friend": state["friends"], "can_send_message": state["friends"]})
elif path == core + "/messages/send" and rec["method"] == "POST":
    if who("im") != users["a"]:
        status, payload = err(401, 401, "UNAUTHORIZED", "IM token required")
    elif not state["friends"]:
        status, payload = err(200, 1, "SOCIAL_NOT_FRIEND")
    elif fail == "send":
        status, payload = err(500, 500, "MESSAGE_SEND_FAILED")
    else:
        state["conversation_id"] = body["conversation_id"]
        state["sent"] = body
        status, payload = ok({"server_msg_id": "srv-smoke-1", "seq": 7, "success": True})
elif path == core + "/conversations" and rec["method"] == "GET":
    if who("im") is None:
        status, payload = err(401, 401, "UNAUTHORIZED")
    elif fail == "readback" or "conversation_id" not in state:
        status, payload = ok({"conversations": [], "next_cursor": "", "has_more": False})
    else:
        status, payload = ok({"conversations": [{"conversation_id": state["conversation_id"], "max_seq": 7}],
                              "next_cursor": "", "has_more": False})

json.dump(state, open(state_path, "w"))
with open(rec["opts"]["-o"], "w") as fh:
    json.dump(payload, fh)
sys.stdout.write(str(status))
'''

FAKE_CONTAINER = r'''
import json, os, sys
log_dir = os.environ["FAKE_LOG_DIR"]
name = os.path.basename(sys.argv[0])
stdin = sys.stdin.read() if not sys.stdin.isatty() else ""
with open(os.path.join(log_dir, "container.jsonl"), "a") as fh:
    fh.write(json.dumps({"runtime": name, "argv": sys.argv[1:], "stdin": stdin}) + "\n")
sys.stdout.write(os.environ.get("FAKE_DB_OUTPUT", "1|ack_published") + "\n")
'''

FAKE_FORBIDDEN = r'''
import json, os, sys
with open(os.path.join(os.environ["FAKE_LOG_DIR"], "forbidden.jsonl"), "a") as fh:
    fh.write(json.dumps({"tool": os.path.basename(sys.argv[0]), "argv": sys.argv[1:]}) + "\n")
sys.exit(1)
'''


def reference_cid(first, second):
    low, high = sorted((first, second))
    bits = int.from_bytes(hashlib.sha256(f"DM:v1:{low}:{high}".encode()).digest()[:10], "big")
    return "1A" + "".join(CROCKFORD[(bits >> shift) & 31] for shift in range(75, -1, -5))


class Run:
    def __init__(self, proc, log_dir):
        self.proc = proc
        self.log_dir = Path(log_dir)

    def _records(self, name):
        path = self.log_dir / name
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text().splitlines() if line]

    @property
    def curl(self):
        return self._records("curl.jsonl")

    @property
    def containers(self):
        return self._records("container.jsonl")

    @property
    def forbidden(self):
        return self._records("forbidden.jsonl")

    @property
    def output(self):
        return self.proc.stdout + self.proc.stderr

    def report(self):
        return dict(line.split("=", 1) for line in self.proc.stdout.splitlines() if "=" in line)


class SmokeMessageFlowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="smoke-flow-test-"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.bin = self.tmp / "bin"
        self.bin.mkdir()
        for name, source in (("curl", FAKE_CURL), ("podman", FAKE_CONTAINER), ("docker", FAKE_CONTAINER),
                             ("grpcurl", FAKE_FORBIDDEN), ("psql", FAKE_FORBIDDEN)):
            stub = self.bin / name
            stub.write_text(f"#!{sys.executable}\n" + textwrap.dedent(source))
            stub.chmod(0o755)
        (self.bin / "python3").symlink_to(sys.executable)
        self.password_file = self.tmp / "password"
        self.password_file.write_text(PASSWORD + "\n")
        self.password_file.chmod(0o600)
        self.ca_file = self.tmp / "ca.pem"
        self.ca_file.write_text("-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----\n")
        self.runs = 0

    def run_smoke(self, overrides=None, drop=()):
        self.runs += 1
        log_dir = self.tmp / f"log-{self.runs}"
        log_dir.mkdir()
        run_tmp = self.tmp / f"tmp-{self.runs}"
        run_tmp.mkdir()
        env = {
            "PATH": os.pathsep.join([str(self.bin), str(Path(sys.executable).parent), "/usr/bin", "/bin"]),
            "HOME": str(self.tmp),
            "TMPDIR": str(run_tmp),
            "LC_ALL": "C",
            "SMOKE_BASE_URL": BASE_URL,
            "SMOKE_CA_FILE": str(self.ca_file),
            "SMOKE_TENANT_ID": TENANT,
            "SMOKE_PASSWORD_FILE": str(self.password_file),
            "SMOKE_PG_CONTAINER": PG_CONTAINER,
            "SMOKE_TIMEOUT_SECONDS": "2",
            "FAKE_LOG_DIR": str(log_dir),
            "FAKE_TENANT": TENANT,
            "FAKE_USERS": json.dumps(USERS),
            "FAKE_TOKENS": json.dumps({f"{kind}:{uid}": token for (kind, uid), token in TOKENS.items()}),
        }
        env.update(overrides or {})
        for key in drop:
            env.pop(key, None)
        proc = subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True, timeout=180)
        self.assertEqual(list(run_tmp.iterdir()), [], "script must remove its private work directory")
        return Run(proc, log_dir)

    def assert_passed(self, run):
        self.assertEqual(run.proc.returncode, 0, run.output)
        self.assertIn("status=pass", run.proc.stdout)

    def assert_no_unsigned_identity(self, run):
        self.assertEqual(run.forbidden, [], "no gRPC client or host psql may be invoked")
        for rec in run.curl:
            for header in rec["headers"]:
                self.assertIsNone(FORBIDDEN_HEADER.match(header), f"unsigned identity header sent: {header}")
            if rec["body"]:
                self.assertNotRegex(rec["body"], r"(?i)x-(tenant-id|user-id|flare-ctx)")

    def assert_no_secret_leak(self, run):
        exposed = [run.output]
        exposed += [" ".join(rec["argv"]) for rec in run.curl]
        exposed += [" ".join(rec["argv"]) for rec in run.containers]
        for text in exposed:
            for secret in SECRETS:
                self.assertNotIn(secret, text)

    # ---- happy path ---------------------------------------------------------

    def test_http_mode_sends_through_edge_and_reads_back(self):
        run = self.run_smoke()
        self.assert_passed(run)
        report = run.report()
        expected_cid = reference_cid(USERS["a"], USERS["b"])
        self.assertEqual(report["conversation_id"], expected_cid)
        self.assertEqual(report["server_msg_id"], "srv-smoke-1")
        self.assertEqual(report["conversation_seq"], "7")
        self.assertEqual(report["readback_recipient_max_seq"], "7")
        self.assertEqual(report["readback_sender_max_seq"], "7")
        self.assertEqual(report["messages_row_count"], "1")
        self.assertEqual(report["ledger_write_state"], "ack_published")
        self.assertEqual(report["tenant_id"], TENANT)

        urls = [(rec["method"], rec["url"]) for rec in run.curl]
        self.assertIn(("POST", f"{BASE_URL}/api/api/v1/messages/send"), urls)
        self.assertIn(("GET", f"{BASE_URL}/api/api/v1/conversations?limit=100"), urls)
        self.assertIn(("POST", f"{BASE_URL}/social-api/api/v1/auth/register"), urls)
        self.assertIn(("POST", f"{BASE_URL}/social-api/api/v1/auth/login"), urls)
        self.assertIn(("POST", f"{BASE_URL}/social-api/api/v1/relations/friend-requests"), urls)
        for rec in run.curl:
            self.assertEqual(rec["opts"].get("--cacert"), str(self.ca_file))
            self.assertEqual(rec["opts"].get("--proto"), "=https")
            self.assertEqual(rec["opts"].get("--noproxy"), "*")
            self.assertEqual(rec["opts"].get("--max-redirs"), "0")
            self.assertNotIn("--connect-to", rec["opts"])

        send = next(rec for rec in run.curl if rec["url"].endswith("/messages/send"))
        body = json.loads(send["body"])
        self.assertEqual(body["conversation_id"], expected_cid)
        self.assertEqual(body["channel_id"], USERS["b"])
        self.assertEqual(body["conversation_type"], 1)
        self.assertIn(f"Authorization: Bearer {TOKENS[('im', USERS['a'])]}", send["headers"])
        for rec in run.curl:
            if "/social-api/" in rec["url"] and "/auth/" not in rec["url"]:
                self.assertTrue(any(h.startswith("Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.")
                                    for h in rec["headers"]), rec["url"])
            if "/auth/" in rec["url"]:
                self.assertFalse(any(h.lower().startswith("authorization:") for h in rec["headers"]))
                self.assertEqual(json.loads(rec["body"])["tenant_id"], TENANT)
        self.assert_no_unsigned_identity(run)
        self.assert_no_secret_leak(run)

    def test_persistence_check_is_read_only_select_inside_container(self):
        run = self.run_smoke()
        self.assert_passed(run)
        self.assertEqual(len(run.containers), 1)
        call = run.containers[0]
        self.assertEqual(call["runtime"], "podman")
        argv = call["argv"]
        self.assertEqual(argv[:2], ["exec", "-i"])
        self.assertIn("PGOPTIONS=-c default_transaction_read_only=on", argv)
        self.assertIn(PG_CONTAINER, argv)
        self.assertIn("flare_core", argv)
        self.assertIn(TENANT, argv)
        statements = [s.strip() for s in call["stdin"].split(";") if s.strip()]
        self.assertEqual(statements[0], "BEGIN TRANSACTION READ ONLY")
        self.assertEqual(statements[-1], "COMMIT")
        for statement in statements[1:-1]:
            self.assertTrue(statement.upper().startswith("SELECT"), statement)
        self.assertNotRegex(call["stdin"].upper(),
                            r"\b(INSERT|UPDATE|DELETE|MERGE|UPSERT|CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE|COPY|CALL|DO)\b")

    def test_server_name_and_host_are_parameterized(self):
        run = self.run_smoke({"SMOKE_BASE_URL": "https://127.0.0.1:18443",
                              "SMOKE_SERVER_NAME": "p0e2e.example.test",
                              "SMOKE_HOST_HEADER": "p0e2e.example.test:18443",
                              "SMOKE_PG_DATABASE": "core_db", "SMOKE_PG_USER": "auditor"})
        self.assert_passed(run)
        for rec in run.curl:
            self.assertTrue(rec["url"].startswith("https://p0e2e.example.test:18443/"), rec["url"])
            self.assertEqual(rec["opts"]["--connect-to"], "p0e2e.example.test:18443:127.0.0.1:18443")
            self.assertIn("Host: p0e2e.example.test:18443", rec["headers"])
        argv = run.containers[0]["argv"]
        self.assertIn("core_db", argv)
        self.assertIn("auditor", argv)
        self.assert_no_unsigned_identity(run)

    def test_generated_password_when_no_file_given(self):
        run = self.run_smoke(drop=("SMOKE_PASSWORD_FILE",))
        self.assert_passed(run)
        bodies = [json.loads(rec["body"]) for rec in run.curl if "/auth/" in rec["url"]]
        passwords = {body["password"] for body in bodies}
        self.assertEqual(len(passwords), 1)
        generated = passwords.pop()
        self.assertGreaterEqual(len(generated), 24)
        self.assertNotIn(generated, run.output)
        for rec in run.curl + run.containers:
            self.assertNotIn(generated, " ".join(rec["argv"]))

    # ---- every mode: never unsigned identity headers ------------------------

    def test_no_mode_sends_unsigned_grpc_identity_headers(self):
        variants = {
            "default-http": ({}, ()),
            "explicit-http": ({"SMOKE_MODE": "http"}, ()),
            "docker-runtime": ({"SMOKE_CONTAINER_RUNTIME": "docker"}, ()),
            "skip-db": ({"SMOKE_SKIP_DB_CHECK": "1"}, ("SMOKE_PG_CONTAINER",)),
            "loopback-http": ({"SMOKE_BASE_URL": "http://127.0.0.1:18080", "SMOKE_CORE_API_PREFIX": "/core",
                               "SMOKE_SOCIAL_API_PREFIX": "/social", "FAKE_CORE_PREFIX": "/core",
                               "FAKE_SOCIAL_PREFIX": "/social"}, ("SMOKE_CA_FILE",)),
            "grpc-mode": ({"SMOKE_MODE": "grpc"}, ()),
            "grpc-signed-mode": ({"SMOKE_MODE": "grpc-signed"}, ()),
            "legacy-endpoint": ({"SMOKE_MESSAGE_INGEST_ENDPOINT": "127.0.0.1:50182"}, ()),
            "legacy-postgres-url": ({"SMOKE_POSTGRES_URL": "postgres://u:hunter2-legacy@h/db"}, ()),
            "send-rejected": ({"FAKE_FAIL": "send"}, ()),
        }
        for name, (overrides, drop) in variants.items():
            with self.subTest(mode=name):
                run = self.run_smoke(overrides, drop)
                self.assert_no_unsigned_identity(run)
                self.assert_no_secret_leak(run)
                self.assertNotIn("hunter2-legacy", run.output)
                if name.startswith(("grpc", "legacy")):
                    self.assertNotEqual(run.proc.returncode, 0)
                    self.assertEqual(run.curl, [], "rejected modes must not send anything")
                    self.assertEqual(run.containers, [])
                elif name == "send-rejected":
                    self.assertNotEqual(run.proc.returncode, 0)
                    self.assertIn("reason=MESSAGE_SEND_FAILED", run.proc.stderr)
                else:
                    self.assert_passed(run)
        skip = self.run_smoke({"SMOKE_SKIP_DB_CHECK": "1"}, ("SMOKE_PG_CONTAINER",))
        self.assertEqual(skip.containers, [])
        self.assertEqual(skip.report()["messages_row_count"], "skipped")
        docker = self.run_smoke({"SMOKE_CONTAINER_RUNTIME": "docker"})
        self.assertEqual(docker.containers[0]["runtime"], "docker")

    def test_script_text_has_no_grpc_client_or_identity_headers(self):
        text = re.sub(r"(?s)cat <<'USAGE'\n.*?\nUSAGE\n", "", SCRIPT.read_text())
        code = "\n".join(line for line in text.splitlines()
                         if not line.lstrip().startswith("#"))
        self.assertNotRegex(code, r"(?i)x-(tenant-id|user-id|flare-ctx|actor-)")
        self.assertNotRegex(code, r"\bgrpcurl\b")
        self.assertNotIn("-plaintext", code)
        self.assertNotRegex(code, r"(?m)^\s*\"?\$psql_bin")

    # ---- refusals before any request ---------------------------------------

    def assert_refused_before_network(self, run, message):
        self.assertNotEqual(run.proc.returncode, 0)
        self.assertIn(message, run.proc.stderr)
        self.assertEqual(run.curl, [])
        self.assertEqual(run.containers, [])
        self.assertEqual(run.forbidden, [])

    def test_default_or_malformed_tenant_is_refused(self):
        for tenant in ("0", "default", "", "e2e-old", "e2e-0123456789ABCDEF0123", "e2e-';select 1;--",
                       "tenant-0123456789abcdef0123"):
            with self.subTest(tenant=tenant):
                run = self.run_smoke({"SMOKE_TENANT_ID": tenant})
                self.assert_refused_before_network(run, "SMOKE_TENANT_ID must be an isolated fixture tenant")
        run = self.run_smoke(drop=("SMOKE_TENANT_ID",))
        self.assert_refused_before_network(run, "SMOKE_TENANT_ID must be an isolated fixture tenant")

    def test_unsafe_origins_are_refused(self):
        cases = {
            "http://edge.smoke.test": "plain http is only allowed for loopback",
            "https://user:pw@edge.smoke.test": "must not embed credentials",
            "https://edge.smoke.test/api": "must be a bare origin",
            "https://edge.smoke.test/?x=1": "must be a bare origin",
            "ftp://edge.smoke.test": "absolute https:// origin",
        }
        for url, message in cases.items():
            with self.subTest(url=url):
                self.assert_refused_before_network(self.run_smoke({"SMOKE_BASE_URL": url}), message)
        self.assert_refused_before_network(self.run_smoke(drop=("SMOKE_BASE_URL",)), "SMOKE_BASE_URL is required")
        self.assert_refused_before_network(
            self.run_smoke({"SMOKE_HOST_HEADER": "edge.test\r\nX-User-Id: 1"}), "SMOKE_HOST_HEADER")
        self.assert_refused_before_network(
            self.run_smoke({"SMOKE_CORE_API_PREFIX": "/api/../admin"}), "SMOKE_CORE_API_PREFIX")

    def test_password_file_must_be_private(self):
        self.password_file.chmod(0o644)
        run = self.run_smoke()
        self.assert_refused_before_network(run, "chmod 600")
        self.password_file.chmod(0o600)
        self.password_file.write_text("short\n")
        self.assert_refused_before_network(self.run_smoke(), "printable password of 8-4096 characters")

    def test_db_check_needs_container_unless_skipped(self):
        run = self.run_smoke(drop=("SMOKE_PG_CONTAINER",))
        self.assert_refused_before_network(run, "SMOKE_PG_CONTAINER is required")
        run = self.run_smoke({"SMOKE_PG_CONTAINER": "pg; rm -rf /"})
        self.assert_refused_before_network(run, "SMOKE_PG_CONTAINER is required")
        run = self.run_smoke({"SMOKE_CONTAINER_RUNTIME": "sh"})
        self.assert_refused_before_network(run, "podman or docker")

    # ---- failure reporting --------------------------------------------------

    def test_failure_summary_redacts_tokens_and_hides_data(self):
        run = self.run_smoke({"FAKE_FAIL": "login"})
        self.assertNotEqual(run.proc.returncode, 0)
        self.assertIn("step=login-a", run.proc.stderr)
        self.assertIn("reason=INVALID_CREDENTIALS", run.proc.stderr)
        self.assertIn("[redacted]", run.proc.stderr)
        self.assert_no_secret_leak(run)
        self.assertFalse(any("/messages/send" in rec["url"] for rec in run.curl))

    def test_readback_and_durability_timeouts_fail(self):
        run = self.run_smoke({"FAKE_FAIL": "readback", "SMOKE_TIMEOUT_SECONDS": "1"})
        self.assertNotEqual(run.proc.returncode, 0)
        self.assertIn("did not reach seq 7", run.proc.stderr)
        self.assertEqual(run.containers, [])
        run = self.run_smoke({"FAKE_DB_OUTPUT": "1|broker_accepted", "SMOKE_TIMEOUT_SECONDS": "1"})
        self.assertNotEqual(run.proc.returncode, 0)
        self.assertIn("did not reach durable storage", run.proc.stderr)
        self.assertIn("ledger_write_state=broker_accepted", run.proc.stderr)
        run = self.run_smoke({"FAKE_DB_OUTPUT": "0|ack_published", "SMOKE_TIMEOUT_SECONDS": "1"})
        self.assertNotEqual(run.proc.returncode, 0)
        self.assertNotIn("status=pass", run.proc.stdout)

    def test_script_is_executable_and_shellcheck_clean(self):
        self.assertTrue(SCRIPT.stat().st_mode & stat.S_IXUSR)
        shellcheck = shutil.which("shellcheck")
        if shellcheck is None:
            self.skipTest("shellcheck not installed")
        result = subprocess.run([shellcheck, str(SCRIPT)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


# ---- gRPC mode -----------------------------------------------------------------

DB_PASSWORD = "Qx7mV2pL9sKd4RtN8wYb3HjF"
GRPC_SENDER = "7312345678901234567"
GRPC_RECIPIENT = "7312345678901234568"

FAKE_GRPC_CLIENT = r'''
import json, os, stat, sys
log_dir = os.environ["FAKE_LOG_DIR"]
args = sys.argv[1:]
files = {}
for flag in ("--signing-key-file", "--db-password-file"):
    if flag in args:
        path = args[args.index(flag) + 1]
        info = os.lstat(path)
        files[flag] = {"mode": stat.S_IMODE(info.st_mode), "regular": stat.S_ISREG(info.st_mode)}
env = {key: value for key, value in os.environ.items() if key.startswith(("SMOKE_", "PG", "DATABASE"))}
with open(os.path.join(log_dir, "grpc-client.jsonl"), "a") as fh:
    fh.write(json.dumps({"argv": args, "files": files, "env": env}) + "\n")
if "--dry-run" in args:
    print("flare_message_flow_smoke_plan")
    print("mode=grpc")
    print("status=dry-run")
else:
    print("flare_message_flow_smoke_report")
    print("mode=grpc")
    print("status=pass")
sys.exit(int(os.environ.get("FAKE_GRPC_EXIT", "0")))
'''


def ed25519_pem(seed: int) -> str:
    """PKCS#8 PEM of a fixed Ed25519 seed (RFC 8410 layout), stdlib only."""
    der = bytes.fromhex("302e020100300506032b657004220420") + bytes([seed]) * 32
    body = base64.b64encode(der).decode()
    return f"-----BEGIN PRIVATE KEY-----\n{body}\n-----END PRIVATE KEY-----\n"


def built_client():
    candidate = os.environ.get("FLARE_SMOKE_GRPC_BIN")
    if candidate:
        return Path(candidate)
    target = Path(os.environ.get("CARGO_TARGET_DIR") or SCRIPT.resolve().parents[2] / "target")
    path = target / "debug" / "flare-smoke-grpc"
    return path if path.is_file() and os.access(path, os.X_OK) else None


class GrpcHarness(unittest.TestCase):
    """Stubs and helpers shared by the gRPC-mode tests; holds no test itself."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="smoke-grpc-test-"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.bin = self.tmp / "bin"
        self.bin.mkdir()
        # No python3, curl or container runtime is usable in gRPC mode: every one of
        # them is a forbidden stub, and PATH holds nothing else besides find.
        for name in ("curl", "podman", "docker", "grpcurl", "psql", "python3"):
            stub = self.bin / name
            stub.write_text(f"#!{sys.executable}\n" + textwrap.dedent(FAKE_FORBIDDEN))
            stub.chmod(0o755)
        (self.bin / "find").symlink_to(shutil.which("find"))
        self.client = self.bin / "flare-smoke-grpc"
        self.client.write_text(f"#!{sys.executable}\n" + textwrap.dedent(FAKE_GRPC_CLIENT))
        self.client.chmod(0o755)
        self.key_text = ed25519_pem(5)
        self.key_file = self.private("signing.key", self.key_text)
        self.password_file = self.private("db.password", DB_PASSWORD + "\n")
        self.runs = 0

    def private(self, name, text, mode=0o600):
        path = self.tmp / name
        path.write_text(text)
        path.chmod(mode)
        return path

    def run_grpc(self, overrides=None, drop=(), args=("--mode", "grpc"), client=None):
        self.runs += 1
        log_dir = self.tmp / f"log-{self.runs}"
        log_dir.mkdir()
        env = {
            "PATH": str(self.bin),
            "HOME": str(self.tmp),
            "TMPDIR": str(self.tmp),
            "LC_ALL": "C",
            "SMOKE_TENANT_ID": TENANT,
            "SMOKE_GRPC_INGEST_ENDPOINT": "http://core:50182",
            "SMOKE_GRPC_SENDER_ID": GRPC_SENDER,
            "SMOKE_GRPC_RECIPIENT_ID": GRPC_RECIPIENT,
            "SMOKE_GRPC_SIGNING_KEY_FILE": str(self.key_file),
            "SMOKE_GRPC_DB_HOST": "postgres",
            "SMOKE_GRPC_DB_USER": "flare_core_app",
            "SMOKE_GRPC_DB_PASSWORD_FILE": str(self.password_file),
            "SMOKE_GRPC_CLIENT": str(client or self.client),
            "SMOKE_TIMEOUT_SECONDS": "5",
            "FAKE_LOG_DIR": str(log_dir),
        }
        env.update(overrides or {})
        for key in drop:
            env.pop(key, None)
        proc = subprocess.run([shutil.which("bash"), str(SCRIPT), *args], env=env,
                              capture_output=True, text=True, timeout=60)
        return Run(proc, log_dir)

    def client_calls(self, run):
        return run._records("grpc-client.jsonl")

    def assert_no_secret(self, run):
        exposed = [run.output] + [json.dumps(call) for call in self.client_calls(run)]
        for text in exposed:
            self.assertNotIn(DB_PASSWORD, text)
            self.assertNotIn(self.key_text.splitlines()[1], text)

    def assert_refused(self, run, message):
        self.assertNotEqual(run.proc.returncode, 0)
        self.assertIn(message, run.proc.stderr)
        self.assertEqual(self.client_calls(run), [], "client must not start")
        self.assertEqual(run.forbidden, [])
        self.assert_no_secret(run)


class GrpcModeTests(GrpcHarness):
    def test_grpc_mode_execs_the_signed_client_with_file_paths_only(self):
        run = self.run_grpc()
        self.assertEqual(run.proc.returncode, 0, run.output)
        self.assertIn("status=pass", run.proc.stdout)
        self.assertEqual(run.forbidden, [], "no python3, curl, container runtime or gRPC CLI")
        calls = self.client_calls(run)
        self.assertEqual(len(calls), 1)
        argv = calls[0]["argv"]
        options = dict(zip(argv[::2], argv[1::2]))
        self.assertEqual(options, {
            "--ingest-endpoint": "http://core:50182",
            "--tenant-id": TENANT,
            "--sender-id": GRPC_SENDER,
            "--recipient-id": GRPC_RECIPIENT,
            "--signing-key-file": str(self.key_file),
            "--db-host": "postgres",
            "--db-port": "5432",
            "--db-name": "flare_core",
            "--db-user": "flare_core_app",
            "--db-password-file": str(self.password_file),
            "--timeout-seconds": "5",
        })
        self.assertNotIn("--dry-run", argv)
        for flag in ("--signing-key-file", "--db-password-file"):
            self.assertEqual(calls[0]["files"][flag], {"mode": 0o600, "regular": True})
        self.assert_no_secret(run)

        custom = self.run_grpc({"SMOKE_GRPC_DB_PORT": "15432", "SMOKE_GRPC_DB_NAME": "core_db"},
                               args=("--mode=grpc",))
        argv = self.client_calls(custom)[0]["argv"]
        self.assertEqual(argv[argv.index("--db-port") + 1], "15432")
        self.assertEqual(argv[argv.index("--db-name") + 1], "core_db")
        readers = self.run_grpc({"SMOKE_GRPC_STORAGE_READER_ENDPOINT": "http://core:60083",
                                 "SMOKE_GRPC_CONVERSATION_ENDPOINT": "http://core:50090"})
        argv = self.client_calls(readers)[0]["argv"]
        self.assertEqual(argv[argv.index("--storage-reader-endpoint") + 1], "http://core:60083")
        self.assertEqual(argv[argv.index("--conversation-endpoint") + 1], "http://core:50090")
        via_env = self.run_grpc({"SMOKE_MODE": "grpc"}, args=())
        self.assertEqual(via_env.proc.returncode, 0, via_env.output)
        failing = self.run_grpc({"FAKE_GRPC_EXIT": "1"})
        self.assertEqual(failing.proc.returncode, 1)

    def test_dry_run_is_passed_through_and_is_grpc_only(self):
        run = self.run_grpc(args=("--mode", "grpc", "--dry-run"))
        self.assertEqual(run.proc.returncode, 0, run.output)
        self.assertIn("status=dry-run", run.proc.stdout)
        self.assertEqual(self.client_calls(run)[0]["argv"][-1], "--dry-run")
        http = self.run_grpc(args=("--dry-run",))
        self.assert_refused(http, "--dry-run is only supported with --mode grpc")

    def test_missing_or_open_secret_files_are_refused_before_the_client(self):
        self.assert_refused(self.run_grpc(drop=("SMOKE_GRPC_SIGNING_KEY_FILE",)),
                            "SMOKE_GRPC_SIGNING_KEY_FILE is required")
        self.assert_refused(self.run_grpc({"SMOKE_GRPC_SIGNING_KEY_FILE": str(self.tmp / "absent")}),
                            "SMOKE_GRPC_SIGNING_KEY_FILE is not a readable regular file")
        self.assert_refused(self.run_grpc(drop=("SMOKE_GRPC_DB_PASSWORD_FILE",)),
                            "SMOKE_GRPC_DB_PASSWORD_FILE is required")
        for mode in (0o640, 0o604, 0o644, 0o610, 0o601):
            with self.subTest(mode=oct(mode)):
                open_key = self.private(f"key-{mode:o}", self.key_text, mode)
                self.assert_refused(self.run_grpc({"SMOKE_GRPC_SIGNING_KEY_FILE": str(open_key)}),
                                    "chmod 600")
                open_password = self.private(f"pw-{mode:o}", DB_PASSWORD, mode)
                self.assert_refused(self.run_grpc({"SMOKE_GRPC_DB_PASSWORD_FILE": str(open_password)}),
                                    "chmod 600")
        link = self.tmp / "key-link"
        link.symlink_to(self.key_file)
        self.assert_refused(self.run_grpc({"SMOKE_GRPC_SIGNING_KEY_FILE": str(link)}),
                            "must not be a symlink")
        self.assert_refused(self.run_grpc({"SMOKE_GRPC_SIGNING_KEY_FILE": str(self.tmp)}),
                            "not a readable regular file")

    def test_required_settings_and_tenant_are_enforced(self):
        for name in ("SMOKE_GRPC_INGEST_ENDPOINT", "SMOKE_GRPC_SENDER_ID", "SMOKE_GRPC_RECIPIENT_ID",
                     "SMOKE_GRPC_DB_HOST", "SMOKE_GRPC_DB_USER"):
            with self.subTest(name=name):
                self.assert_refused(self.run_grpc(drop=(name,)), f"{name} is required")
        for tenant in ("0", "default", "", "e2e-0123456789ABCDEF0123"):
            with self.subTest(tenant=tenant):
                self.assert_refused(self.run_grpc({"SMOKE_TENANT_ID": tenant}),
                                    "isolated fixture tenant")
        self.assert_refused(self.run_grpc({"SMOKE_TIMEOUT_SECONDS": "901"}), "between 1 and 900")
        self.assert_refused(self.run_grpc({"SMOKE_GRPC_CLIENT": str(self.tmp / "absent")}),
                            "not an executable file")
        self.assert_refused(self.run_grpc({"SMOKE_GRPC_CLIENT": "no-such-client"}),
                            "flare-smoke-grpc not found")

    def test_http_and_legacy_unsigned_settings_are_refused(self):
        for name, value in (("SMOKE_BASE_URL", BASE_URL), ("SMOKE_PG_CONTAINER", PG_CONTAINER),
                            ("SMOKE_SKIP_DB_CHECK", "1"), ("SMOKE_PASSWORD_FILE", str(self.password_file))):
            with self.subTest(name=name):
                self.assert_refused(self.run_grpc({name: value}), f"{name} is an HTTP-mode setting")
        for name, value in (("SMOKE_POSTGRES_URL", "postgres://flare:flare123@localhost:25432/flare2"),
                            ("SMOKE_MESSAGE_INGEST_ENDPOINT", "127.0.0.1:50182"),
                            ("SMOKE_STORAGE_READER_ENDPOINT", "127.0.0.1:60083"),
                            ("SMOKE_SENDER_ID", GRPC_SENDER), ("SMOKE_GRPCURL", "grpcurl")):
            with self.subTest(name=name):
                run = self.run_grpc({name: value})
                self.assert_refused(run, f"{name} belonged to the removed unsigned gRPC mode")
                self.assertNotIn("flare123", run.output)
        for mode in ("grpc-signed", "GRPC", "unsigned"):
            with self.subTest(mode=mode):
                self.assert_refused(self.run_grpc(args=("--mode", mode)), "mode must be http or grpc")

    def test_script_is_shellcheck_clean_in_both_modes(self):
        shellcheck = shutil.which("shellcheck")
        if shellcheck is None:
            self.skipTest("shellcheck not installed")
        result = subprocess.run([shellcheck, str(SCRIPT)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


@unittest.skipIf(built_client() is None, "flare-smoke-grpc is not built (cargo build -p flare-smoke-grpc)")
class GrpcModeRealClientTests(GrpcHarness):
    """The real binary behind the script, dry run only: no connection is opened."""

    def run_real(self, overrides=None):
        return self.run_grpc(overrides, args=("--mode", "grpc", "--dry-run"), client=built_client())

    def test_real_dry_run_loads_key_and_password_and_prints_the_plan(self):
        run = self.run_real()
        self.assertEqual(run.proc.returncode, 0, run.output)
        plan = run.report()
        self.assertEqual(plan["mode"], "grpc")
        self.assertEqual(plan["status"], "dry-run")
        self.assertEqual(plan["signing_key"], "loaded;test_signature=ok")
        self.assertEqual(plan["db_password"], "loaded;policy=ok")
        self.assertEqual(plan["connections_opened"], "0")
        self.assertEqual(plan["conversation_id"], reference_cid(GRPC_SENDER, GRPC_RECIPIENT))
        self.assertEqual(run.forbidden, [])
        self.assert_no_secret(run)

    def test_real_client_refuses_default_password_and_bad_keys(self):
        default = self.private("default.password", "flare123\n")
        run = self.run_real({"SMOKE_GRPC_DB_PASSWORD_FILE": str(default)})
        self.assertEqual(run.proc.returncode, 2, run.output)
        self.assertIn("known default", run.proc.stderr)
        self.assertNotIn("flare123", run.output)
        weak = self.private("weak.password", "short-pw\n")
        self.assertEqual(self.run_real({"SMOKE_GRPC_DB_PASSWORD_FILE": str(weak)}).proc.returncode, 2)
        garbage = self.private("garbage.key", "not a key\n")
        run = self.run_real({"SMOKE_GRPC_SIGNING_KEY_FILE": str(garbage)})
        self.assertEqual(run.proc.returncode, 2, run.output)
        self.assertIn("Ed25519 private key", run.proc.stderr)
        run = self.run_real({"PGPASSWORD": DB_PASSWORD})
        self.assertEqual(run.proc.returncode, 2, run.output)
        self.assertIn("PGPASSWORD is set", run.proc.stderr)
        self.assert_no_secret(run)


if __name__ == "__main__":
    unittest.main()
