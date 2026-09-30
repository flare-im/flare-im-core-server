import base64
import http.server
import io
import json
import os
import shutil
import ssl
import subprocess
import tempfile
import threading
import unittest
import urllib.parse
from email.message import Message
from types import SimpleNamespace
from unittest.mock import patch

import test_media_isolation_http as gate


class HttpGateSafetyTests(unittest.TestCase):
    def test_origins_reject_credentials_and_remote_plaintext(self):
        for url in ["file:///etc/passwd", "https://user:password@example.test", "http://example.test", "https://example.test/#fragment"]:
            with self.subTest(url=url), self.assertRaises(gate.GateFailure):
                gate.origin(url)
        self.assertEqual(gate.origin("http://127.0.0.1:9000/x?signature=secret"), ("http", "127.0.0.1", 9000))

    def test_service_base_accepts_an_origin_or_a_clean_path_prefix(self):
        cases = {
            "http://127.0.0.1:8080": "http://127.0.0.1:8080",
            "http://127.0.0.1:8080/": "http://127.0.0.1:8080",
            "https://edge.test:18443/api": "https://edge.test:18443/api",
            "https://edge.test:18443/api/": "https://edge.test:18443/api",
            "https://edge.test/core/v2_x": "https://edge.test/core/v2_x",
        }
        for url, base in cases.items():
            with self.subTest(url=url):
                self.assertEqual(gate.service_base(url), base)

    def test_service_base_rejects_queries_and_ambiguous_prefixes(self):
        for url in ["https://edge.test/api?x=1", "https://edge.test/api?", "https://edge.test/a/../api",
                    "https://edge.test/./api", "https://edge.test/a//api", "https://edge.test/%2e%2e/api",
                    "https://edge.test/a%2Fb", "https://edge.test/a\\b", "https://edge.test/a b",
                    "https://edge.test/api/..", "http://edge.test/api"]:
            with self.subTest(url=url), self.assertRaises(gate.GateFailure):
                gate.service_base(url)

    def test_storage_redirect_must_stay_under_the_allowed_prefix(self):
        allowance = gate.storage_allowance("https://edge.test:18443/flare-media/")
        self.assertEqual(allowance, (("https", "edge.test", 18443), "/flare-media/"))
        allowed = "https://edge.test:18443/flare-media/t%2541/media/x.txt?X-Amz-Signature=secret"
        self.assertEqual(gate.authorize_storage_target(allowed, allowance), allowed)
        for target in ["https://edge.test:18443/api/api/v1/medias/files/x",
                       "https://edge.test:18443/flare-mediaX/t/x",
                       "https://edge.test:18443/flare-media/../api/x",
                       "https://edge.test:18443/flare-media/%2e%2e/api/x",
                       "https://edge.test:18443/flare-media/t/%2Fetc",
                       "https://edge.test:18443/flare-media/t/a%5Cb",
                       "https://other.test:18443/flare-media/t/x",
                       "https://edge.test:443/flare-media/t/x",
                       "http://edge.test:18443/flare-media/t/x",
                       "/flare-media/t/x", ""]:
            with self.subTest(target=target), self.assertRaises(gate.GateFailure):
                gate.authorize_storage_target(target, allowance)
        # Seen on p0e2e: a bare object key as Location, which a client resolves
        # against the API path instead of the storage origin.
        for target in ["1200000000000000333/media/documents/x.txt", "//edge.test:18443/flare-media/t/x"]:
            with self.subTest(target=target), self.assertRaisesRegex(gate.GateFailure, "not an absolute"):
                gate.authorize_storage_target(target, allowance)
        # An origin-only allowance keeps the previous whole-origin behavior.
        whole = gate.storage_allowance("http://127.0.0.1:9000")
        self.assertEqual(gate.authorize_storage_target("http://127.0.0.1:9000/bucket/k?s=1", whole),
                         "http://127.0.0.1:9000/bucket/k?s=1")

    def test_connect_address_is_numeric_and_plaintext_stays_on_loopback(self):
        self.assertEqual(gate.connect_pins(None, "https://edge.test/api"), {})
        self.assertEqual(gate.connect_pins("127.0.0.1", "https://Edge.Test:18443/api"), {"edge.test": "127.0.0.1"})
        self.assertEqual(gate.connect_pins("192.0.2.10", "https://edge.test/api"), {"edge.test": "192.0.2.10"})
        self.assertEqual(gate.connect_pins("::1", "http://localhost:8080"), {"localhost": "::1"})
        for address, url in [("edge.test", "https://edge.test"), ("127.0.0.1:18443", "https://edge.test"),
                             ("192.0.2.10", "http://localhost:8080")]:
            with self.subTest(address=address), self.assertRaises(gate.GateFailure):
                gate.connect_pins(address, url)
        transport = gate.Transport(pins={"localhost": "192.0.2.10"})
        with self.assertRaises(gate.GateFailure):
            transport.request("http://localhost:9000/object")

    def test_denials_must_not_contain_success_data_or_redirect(self):
        gate.denied((403, {}, b'{"code":403,"data":null}'), anonymous=False)
        gate.denied((404, {}, b'{"code":404,"reason":"NOT_FOUND"}'), anonymous=False)
        gate.denied((401, {}, b'{"code":401,"reason":"UNAUTHORIZED"}'), anonymous=True)
        for response in [(200, {}, b'{}'), (403, {"Location": "https://private.test"}, b'{}'),
                         (404, {}, b'{"code":404,"data":{"url":"private"}}'), (403, {}, b'{"code":0}'),
                         (403, {}, b'{"code":200}'), (403, {}, b'{"data":null}'), (403, {}, b'not-json'),
                         (307, {}, b'{"code":307}')]:
            with self.subTest(response=response), self.assertRaises(gate.GateFailure):
                gate.denied(response, anonymous=False)

    def test_denials_are_typed_by_caller(self):
        # The pre-fix gateway mapped every Media refusal to 500; a foreign bearer
        # must now get 403/404 and an anonymous caller 401.
        for status in [500, 401, 400, 502]:
            with self.subTest(status=status), self.assertRaises(gate.GateFailure):
                gate.denied((status, {}, b'{"code":%d,"reason":"X"}' % status), anonymous=False)
        for status in [403, 404, 500]:
            with self.subTest(status=status), self.assertRaises(gate.GateFailure):
                gate.denied((status, {}, b'{"code":%d,"reason":"X"}' % status), anonymous=True)

    def test_denials_must_not_echo_internal_identifiers(self):
        identifiers = ["8dffe6e2-336e-4ab9-a668-cba3806937a8", "1200000000000000333/media/documents/x.txt",
                       "1200000000000000333", "1200000000000000349"]
        clean = b'{"code":404,"reason":"NOT_FOUND","message":"Resource not found","data":null,"ts":1759262164000}'
        gate.denied((404, {}, clean), anonymous=False, identifiers=identifiers)
        for body in [b'{"code":404,"reason":"8dffe6e2-336e-4ab9-a668-cba3806937a8"}',
                     b'{"code":403,"reason":"FORBIDDEN","message":"key 1200000000000000333/media/documents/x.txt"}',
                     b'{"code":404,"reason":"NOT_FOUND","message":"tenant=1200000000000000349"}',
                     b'{"code":404,"reason":"NOT_FOUND","error":{"params":{"file":"8dffe6e2-336e-4ab9-a668-cba3806937a8"}}}']:
            with self.subTest(body=body), self.assertRaisesRegex(gate.GateFailure, "echoes an internal identifier"):
                gate.denied((int(json.loads(body)["code"]), {}, body), anonymous=False, identifiers=identifiers)
        with self.assertRaisesRegex(gate.GateFailure, "echoes"):
            gate.denied((401, {}, b'{"code":401,"message":"no token for 1200000000000000333"}'), anonymous=True,
                        identifiers=identifiers)

    def test_identifier_echo_matches_whole_tokens_only(self):
        self.assertTrue(gate.echoes("x/1200000000000000333/media", "1200000000000000333"))
        self.assertTrue(gate.echoes("tenant 0 is gone", "0"))
        self.assertFalse(gate.echoes('{"code":404,"ts":1759262164000}', "0"))
        self.assertFalse(gate.echoes("11200000000000000333", "1200000000000000333"))
        self.assertFalse(gate.echoes("anything", ""))

    def test_success_requires_the_core_envelope(self):
        # flare-server-core ApiResponse::success serializes code 0.
        self.assertEqual(gate.successful_data((200, {}, b'{"code":0,"data":{"file_id":"fixture"}}')), {"file_id": "fixture"})
        for response in [(200, {}, b'{"code":500,"data":null}'), (200, {}, b'{"code":0}'),
                         (200, {}, b'{"code":0,"data":null}'), (201, {}, b'{"code":0,"data":{}}'),
                         (200, {}, b'{"code":403,"data":{}}')]:
            with self.subTest(response=response), self.assertRaises(gate.GateFailure):
                gate.successful_data(response)

    def test_bucket_is_tied_to_the_admin_url(self):
        args = gate.parse_args(["--api-url", "https://edge.test/api"])
        self.assertIsNone(args.admin_url)
        gate.parse_args(["--api-url", "https://edge.test/api", "--admin-url", "https://admin.test", "--bucket", "b"])
        for argv in [["--api-url", "https://edge.test/api", "--admin-url", "https://admin.test"],
                     ["--api-url", "https://edge.test/api", "--bucket", "b"]]:
            with self.subTest(argv=argv), patch("sys.stderr", io.StringIO()), self.assertRaises(SystemExit):
                gate.parse_args(argv)

    def test_unreadable_ca_file_fails_closed(self):
        with self.assertRaises(gate.GateFailure):
            gate.Transport(ca_file="/nonexistent/flare-gate-ca.pem")


@unittest.skipUnless(shutil.which("openssl"), "openssl CLI required to mint a throwaway CA")
class PinnedTlsTransportTests(unittest.TestCase):
    """A real loopback TLS server with a certificate only for media.test."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        root = cls.tmp.name

        def openssl(*argv):
            subprocess.run(["openssl", *argv], cwd=root, check=True, capture_output=True)

        with open(os.path.join(root, "leaf.ext"), "w") as ext:
            ext.write("subjectAltName=DNS:media.test\nbasicConstraints=critical,CA:FALSE\n"
                      "keyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\n"
                      "subjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid:always\n")
        openssl("req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes",
                "-keyout", "ca.key", "-out", "ca.pem", "-days", "1", "-subj", "/CN=flare media gate test CA",
                "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign")
        openssl("req", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes",
                "-keyout", "leaf.key", "-out", "leaf.csr", "-subj", "/CN=media.test")
        openssl("x509", "-req", "-in", "leaf.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial",
                "-out", "leaf.pem", "-days", "1", "-extfile", "leaf.ext")
        cls.ca_file = os.path.join(root, "ca.pem")
        cls.seen = []
        cls.sni = []
        seen = cls.seen

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                seen.append({"path": self.path, "host": self.headers.get("Host"),
                             "authorization": self.headers.get("Authorization")})
                if self.path.startswith("/api/redirect"):
                    self.send_response(307)
                    self.send_header("Location", "https://media.test/flare-media/object")
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                body = json.dumps({"code": 0, "data": {"path": self.path}}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(os.path.join(root, "leaf.pem"), os.path.join(root, "leaf.key"))
        server_context.sni_callback = lambda sock, name, ctx: cls.sni.append(name)
        cls.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.server.socket = server_context.wrap_socket(cls.server.socket, server_side=True)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(5)
        cls.tmp.cleanup()

    def setUp(self):
        self.seen.clear()
        self.sni.clear()

    def test_dials_the_pinned_address_with_sni_certificate_and_host_of_the_name(self):
        transport = gate.Transport(ca_file=self.ca_file, pins={"media.test": "127.0.0.1"})
        status, _, payload = transport.request(f"https://media.test:{self.port}/api/api/v1/medias/file-info?file_id=f",
                                               token="fixture-bearer")
        self.assertEqual((status, json.loads(payload)["data"]["path"]), (200, "/api/api/v1/medias/file-info?file_id=f"))
        self.assertEqual(self.sni, ["media.test"])
        self.assertEqual(self.seen, [{"path": "/api/api/v1/medias/file-info?file_id=f",
                                      "host": f"media.test:{self.port}", "authorization": "Bearer fixture-bearer"}])

    def test_only_the_given_ca_is_trusted_and_the_name_must_match(self):
        with self.assertRaises(gate.GateFailure):
            gate.Transport(pins={"media.test": "127.0.0.1"}).request(f"https://media.test:{self.port}/api/x")
        with self.assertRaises(gate.GateFailure):
            gate.Transport(ca_file=self.ca_file, pins={"other.test": "127.0.0.1"}).request(f"https://other.test:{self.port}/api/x")
        self.assertEqual(self.seen, [])

    def test_redirects_are_returned_not_followed_and_proxies_are_ignored(self):
        transport = gate.Transport(ca_file=self.ca_file, pins={"media.test": "127.0.0.1"})
        with patch.dict(os.environ, {"HTTPS_PROXY": "http://127.0.0.1:9", "ALL_PROXY": "http://127.0.0.1:9"}):
            status, headers, _ = transport.request(f"https://media.test:{self.port}/api/redirect", token="fixture-bearer")
        self.assertEqual((status, headers.get("Location")), (307, "https://media.test/flare-media/object"))
        self.assertEqual(len(self.seen), 1)


class FakeMediaService:
    """Scripted Core media API behind an Edge-like prefix plus a storage prefix."""

    API = "https://edge.test:18443/api/api/v1/medias"
    STORAGE = "https://edge.test:18443/flare-media/"
    ADMIN = "https://admin.test/api/v1/admin/medias"

    def __init__(self, env, **faults):
        self.env, self.faults = env, faults
        self.calls = []
        self.files = {}

    def envelope(self, status, data=None, code=None, headers=None, reason="DENIED", message=""):
        body = {"code": 0, "data": data} if code is None else {"code": code, "reason": reason, "message": message}
        return status, headers or {}, json.dumps(body).encode()

    def object_key(self, file_id):
        # Shares no token with the file id or a tenant id, so only the object
        # key itself (from file-info) can reveal an echo of it.
        return "t-enc/media/documents/obj-7f3a9c.txt"

    def refuse(self, token, file_id):
        """Typed refusals of the fixed gateway: 404 across tenants or when gone, 403 across owners."""
        if token == self.env["FLARE_MEDIA_E2E_USER_B"] or file_id not in self.files:
            status, reason = 404, "NOT_FOUND"
        else:
            status, reason = 403, "FORBIDDEN"
        status = self.faults.get("denial_status", status)
        echo = {"file_id": file_id, "object_key": self.object_key(file_id),
                "tenant": self.env["FLARE_MEDIA_E2E_TENANT_A"]}.get(self.faults.get("echo"), "")
        return self.envelope(status, code=status, reason=reason, message=echo)

    def request(self, url, method="GET", *, token=None, tenant=None, body=None):
        self.calls.append(SimpleNamespace(url=url, method=method, token=token, tenant=tenant, body=body))
        if url.startswith(self.STORAGE):
            key = urllib.parse.urlsplit(url).path[len("/flare-media/"):]
            file_id = key.rsplit("/", 1)[-1]
            content = self.faults.get("storage_bytes", self.files.get(file_id))
            return (200, {}, content) if content is not None else (404, {}, b"")
        if url.startswith(self.ADMIN):
            if token != self.env["FLARE_MEDIA_E2E_ADMIN_A"]:
                return self.envelope(403, code=403)
            if tenant != self.env["FLARE_MEDIA_E2E_TENANT_A"]:
                return self.envelope(403, code=403)
            return self.envelope(200, {"bucket": "b"})
        self.require_api(url)
        path = urllib.parse.urlsplit(url).path[len(urllib.parse.urlsplit(self.API).path):]
        query = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
        if path in {"/cleanup-orphaned-assets", "/object-acl", "/objects", "/bucket"}:
            return (200, {}, b"{}") if path == self.faults.get("exposed") else (404, {}, b"")
        if not token:
            status = self.faults.get("anonymous_status", 401)
            return self.envelope(status, code=status, reason="UNAUTHORIZED")
        owner = token == self.env["FLARE_MEDIA_E2E_USER_A"] or token == self.faults.get("leak_to")
        if path == "/file" and method == "DELETE" and set(body or {}) != {"file_id", "hard_delete"}:
            # axum Json<DeleteFileRequest>: plain-serde proto, every field required.
            return 422, {}, b"Failed to deserialize the JSON body into the target type"
        if path == "/upload-file":
            file_id = "f-1"
            self.files[file_id] = base64.b64decode(body["payload"])
            return self.envelope(200, {"file_id": file_id, "success": True, "url": "https://signed.invalid/?sig=x"})
        file_id = query.get("file_id") or (body or {}).get("file_id") or path.rsplit("/", 1)[-1]
        if file_id not in self.files or not owner:
            return self.refuse(token, file_id)
        if path == "/file-info":
            return self.envelope(200, {"file_id": file_id, "size": len(self.files[file_id]),
                                       "object_key": self.object_key(file_id)})
        if path == "/file-url":
            target = self.faults.get("presigned", f"{self.STORAGE}1200000000000000349/media/{file_id}?X-Amz-Signature=secret")
            return self.envelope(200, {"url": target, "cdn_url": "1200000000000000349/media/" + file_id})
        if path == "/file" and method == "DELETE":
            del self.files[file_id]
            return self.envelope(200, {"success": True})
        if path.startswith("/files/") and self.faults.get("stream_files"):
            return 200, {}, self.files[file_id]
        if path.startswith("/files/"):
            target = self.faults.get("redirect", f"{self.STORAGE}1200000000000000349/media/{file_id}?X-Amz-Signature=secret")
            return 307, {"Location": target}, b""
        raise AssertionError(f"unexpected fake route {method} {path}")

    def require_api(self, url):
        if not url.startswith(self.API):
            raise AssertionError(f"request outside the configured bases: {url}")


class GateFlowTests(unittest.TestCase):
    ENV = {
        "FLARE_MEDIA_E2E_DISPOSABLE": "1",
        "FLARE_MEDIA_E2E_USER_A": "bearer-user-a",
        "FLARE_MEDIA_E2E_OTHER_A": "bearer-other-a",
        "FLARE_MEDIA_E2E_USER_B": "bearer-user-b",
        "FLARE_MEDIA_E2E_TENANT_A": "1200000000000000333",
        "FLARE_MEDIA_E2E_TENANT_B": "1200000000000000349",
    }
    EDGE_ARGV = ["--api-url", "https://edge.test:18443/api", "--storage-origin", "https://edge.test:18443/flare-media/"]

    def run_gate(self, argv, env=None, **faults):
        env = dict(self.ENV if env is None else env)
        service = FakeMediaService(env, **faults)
        out = io.StringIO()
        with patch("sys.stderr", io.StringIO()):
            gate.run(gate.parse_args(argv), env, service, out)
        return service, out.getvalue()

    def test_edge_prefix_run_without_admin_reports_admin_not_evaluated(self):
        service, out = self.run_gate(self.EDGE_ARGV)
        self.assertIn("admin: not evaluated", out)
        self.assertIn("public: 4 management routes return 404", out)
        self.assertIn("isolation: 12 requests denied without data or internal identifiers", out)
        self.assertIn("presigned: owner file-url under the storage prefix returns identical bytes", out)
        self.assertIn("download: owner bytes identical", out)
        self.assertIn("PASS: public management boundary (admin not evaluated)", out)
        self.assertFalse([c for c in service.calls if c.url.startswith(FakeMediaService.ADMIN)])
        storage = [c for c in service.calls if c.url.startswith(FakeMediaService.STORAGE)]
        self.assertEqual(len(storage), 2, "presigned file-url and the /files redirect")
        for call in storage:
            self.assertIsNone(call.token)
            self.assertIsNone(call.tenant)
        self.assertEqual(service.files, {}, "the owned fixture must be deleted")
        for secret in ["bearer-", "X-Amz-Signature", "signed.invalid"]:
            self.assertNotIn(secret, out)

    def test_admin_half_runs_when_admin_url_is_given(self):
        env = dict(self.ENV, FLARE_MEDIA_E2E_ADMIN_A="bearer-admin-a")
        service, out = self.run_gate(self.EDGE_ARGV + ["--admin-url", "https://admin.test", "--bucket", "b"], env=env)
        self.assertIn("admin: user token refused on 4 routes", out)
        self.assertNotIn("not evaluated", out)
        self.assertTrue([c for c in service.calls if c.url.startswith(FakeMediaService.ADMIN)])

    def test_admin_token_is_only_required_with_admin_url(self):
        with self.assertRaises(gate.GateFailure):
            self.run_gate(self.EDGE_ARGV + ["--admin-url", "https://admin.test", "--bucket", "b"])
        missing = {k: v for k, v in self.ENV.items() if k != "FLARE_MEDIA_E2E_OTHER_A"}
        with self.assertRaises(gate.GateFailure):
            self.run_gate(self.EDGE_ARGV, env=missing)

    def test_gate_fails_closed_on_each_broken_boundary(self):
        faults = {
            "exposed management route": {"exposed": "/objects"},
            "foreign reader": {"leak_to": "bearer-other-a"},
            "cross-tenant reader": {"leak_to": "bearer-user-b"},
            "storage bytes differ": {"storage_bytes": b"tampered"},
            "redirect outside storage prefix": {"redirect": "https://edge.test:18443/api/api/v1/medias/objects"},
            "redirect to another host": {"redirect": "https://evil.test:18443/flare-media/t/f-1"},
            "relative redirect (bare object key)": {"redirect": "1200000000000000349/media/f-1"},
            "presigned url outside storage prefix": {"presigned": "https://edge.test:18443/api/x?sig=1"},
            "untyped 500 denial (pre-fix mapping)": {"denial_status": 500},
            "anonymous not refused by authentication": {"anonymous_status": 404},
            "denial echoes the file id": {"echo": "file_id"},
            "denial echoes the object key": {"echo": "object_key"},
            "denial echoes the tenant id": {"echo": "tenant"},
        }
        for name, fault in faults.items():
            with self.subTest(name), self.assertRaises(gate.GateFailure):
                self.run_gate(self.EDGE_ARGV, **fault)

    def test_redirect_needs_an_explicit_storage_origin(self):
        env = dict(self.ENV)
        service = FakeMediaService(env)
        with patch("sys.stderr", io.StringIO()), self.assertRaises(gate.GateFailure):
            gate.run(gate.parse_args(["--api-url", "https://edge.test:18443/api"]), env, service, io.StringIO())
        self.assertFalse([c for c in service.calls if c.url.startswith(FakeMediaService.STORAGE)])
        self.assertEqual(service.files, {})

    def test_without_storage_origin_presigned_half_is_not_evaluated(self):
        env = dict(self.ENV)
        service = FakeMediaService(env, stream_files=True)
        out = io.StringIO()
        with patch("sys.stderr", io.StringIO()):
            gate.run(gate.parse_args(["--api-url", "https://edge.test:18443/api"]), env, service, out)
        self.assertIn("presigned: not evaluated (no --storage-origin)", out.getvalue())
        self.assertIn("download: owner bytes identical (", out.getvalue())
        self.assertIn("streamed by the gateway", out.getvalue())
        self.assertFalse([c for c in service.calls if c.url.startswith(FakeMediaService.STORAGE)])

    def test_fixture_is_deleted_even_when_a_check_fails(self):
        env = dict(self.ENV)
        service = FakeMediaService(env, storage_bytes=b"tampered")
        with patch("sys.stderr", io.StringIO()), self.assertRaises(gate.GateFailure):
            gate.run(gate.parse_args(self.EDGE_ARGV), env, service, io.StringIO())
        self.assertEqual(service.files, {})
        self.assertEqual(service.calls[-1].method, "DELETE")
        self.assertEqual(service.calls[-1].token, "bearer-user-a")

    def test_requires_disposable_opt_in(self):
        env = {k: v for k, v in self.ENV.items() if k != "FLARE_MEDIA_E2E_DISPOSABLE"}
        with self.assertRaises(gate.GateFailure):
            self.run_gate(self.EDGE_ARGV, env=env)


class LegacyRedirectTests(unittest.TestCase):
    def test_application_request_never_follows_storage_redirect(self):
        response = unittest.mock.MagicMock()
        response.status = 307
        response.headers = Message()
        response.headers["Location"] = "https://storage.test/object?sensitive=secret"
        response.read.return_value = b""
        with patch("http.client.HTTPSConnection.request") as send, \
                patch("http.client.HTTPSConnection.getresponse", return_value=response):
            result = gate.Transport().request("https://api.test/files/fixture", token="fixture-bearer")
        self.assertEqual(result[0], 307)
        self.assertEqual(send.call_count, 1)


if __name__ == "__main__":
    unittest.main()
