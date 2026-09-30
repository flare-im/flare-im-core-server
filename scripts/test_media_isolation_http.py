#!/usr/bin/env python3
"""Opt-in SEC-05 HTTP/storage regression against disposable running services.

Tokens are environment-only; responses and signed storage URLs are never logged.
Does not call orphan cleanup: the PostgreSQL test covers its scoped selection.

Service URLs are an origin plus an optional path prefix, so the gate also runs
through an ingress that mounts Core under a prefix (for example Edge's /api).
--ca-file trusts only that PEM bundle. --connect-address dials a numeric
address for the --api-url host while TLS SNI, certificate verification and the
Host header keep the name. Without --admin-url the administrative half is
skipped and reported as "admin: not evaluated". --storage-origin (origin plus
optional path prefix such as Edge's /flare-media/) authorizes the owner's
presigned file-url and the /files 307 target; both are fetched without the
application bearer and must return the uploaded bytes. Foreign callers must be
refused with a typed status (401 without a bearer, 403/404 with one) whose body
carries neither data nor the fixture's file id, object key or a tenant id.
"""
import argparse
import base64
import http.client
import ipaddress
import json
import os
import re
import socket
import ssl
import sys
import urllib.parse
import uuid


RESPONSE_LIMIT = 1024 * 1024
LOOPBACK_NAMES = {"127.0.0.1", "localhost", "::1"}
MANAGEMENT_ROUTES = [
    ("POST", "/cleanup-orphaned-assets"),
    ("POST", "/object-acl"),
    ("GET", "/objects"),
    ("GET", "/bucket"),
]
DENIED_PRINCIPALS = ["USER_B", "OTHER_A", None]
# Conservative path-prefix segment: no encoding, dot segments or separators an
# ingress or object store could normalize differently from this script.
PREFIX_SEGMENT = re.compile(r"[A-Za-z0-9_~-][A-Za-z0-9._~-]*")


class GateFailure(Exception):
    pass


def origin(url):
    value = urllib.parse.urlsplit(url)
    if value.scheme not in {"http", "https"} or not value.hostname or value.username or value.password:
        raise GateFailure("invalid service/storage URL")
    if value.fragment:
        raise GateFailure("URL fragments are not supported")
    if value.scheme == "http" and value.hostname not in LOOPBACK_NAMES:
        raise GateFailure("plaintext transport is restricted to loopback test services")
    return value.scheme, value.hostname, value.port or (443 if value.scheme == "https" else 80)


def service_base(url, what="service"):
    """Origin plus an optional clean path prefix, without a trailing slash."""
    origin(url)
    value = urllib.parse.urlsplit(url)
    if value.query or "?" in url:
        raise GateFailure(f"{what} URL must not carry a query")
    prefix = value.path.rstrip("/")
    if prefix and not all(PREFIX_SEGMENT.fullmatch(segment) for segment in prefix.split("/")[1:]):
        raise GateFailure(f"{what} URL path prefix must be plain segments without dot segments or encoding")
    return f"{value.scheme}://{value.netloc}{prefix}"


def storage_allowance(url):
    """(origin, path prefix ending in '/') that storage URLs must stay under."""
    base = service_base(url, "storage")
    return origin(base), urllib.parse.urlsplit(base).path + "/"


def authorize_storage_target(target, allowance):
    """Accept a storage URL (presigned or redirect) only under the allowed origin and path."""
    allowed_origin, prefix = allowance
    value = urllib.parse.urlsplit(target)
    if value.scheme not in {"http", "https"} or not value.netloc:
        raise GateFailure("download redirect Location is not an absolute http(s) URL")
    if origin(target) != allowed_origin:
        raise GateFailure("set --storage-origin to explicitly authorize the download origin")
    path = value.path
    if not path.startswith(prefix):
        raise GateFailure("download redirect left the authorized storage path prefix")
    for segment in path.split("/"):
        decoded = urllib.parse.unquote(segment)
        if decoded in {".", ".."} or "/" in decoded or "\\" in decoded:
            raise GateFailure("download redirect path is ambiguous")
    return target


def connect_pins(address, api_url):
    """Map the API host to a numeric address; plaintext only to loopback."""
    if not address:
        return {}
    try:
        ip = ipaddress.ip_address(address)
    except ValueError as error:
        raise GateFailure("--connect-address must be a numeric IP address") from error
    scheme, host, _ = origin(api_url)
    if scheme == "http" and not ip.is_loopback:
        raise GateFailure("plaintext transport is restricted to loopback test services")
    return {host: str(ip)}


class _PinnedHTTPConnection(http.client.HTTPConnection):
    def __init__(self, host, port, address, timeout):
        super().__init__(host, port, timeout=timeout)
        self._address = address

    def connect(self):
        self.sock = socket.create_connection((self._address, self.port), self.timeout)


class _PinnedHTTPSConnection(http.client.HTTPSConnection):
    """TCP to `address`; `host` alone names SNI, the certificate and Host."""

    def __init__(self, host, port, address, context, timeout):
        super().__init__(host, port, timeout=timeout, context=context)
        self._address = address
        self._pinned_context = context

    def connect(self):
        raw = socket.create_connection((self._address, self.port), self.timeout)
        try:
            self.sock = self._pinned_context.wrap_socket(raw, server_hostname=self.host)
        except BaseException:
            raw.close()
            raise


class Transport:
    """One request per call: no proxy, no redirect following, no cookies.

    ca_file trusts only that bundle for HTTPS (otherwise the system store).
    pins maps a hostname to the numeric address to dial for it.
    """

    def __init__(self, ca_file=None, pins=None, timeout=15.0):
        try:
            self.context = ssl.create_default_context(cafile=ca_file)
        except (OSError, ValueError) as error:
            raise GateFailure("CA file is unreadable or not a PEM bundle") from error
        self.pins = dict(pins or {})
        self.timeout = timeout

    def request(self, url, method="GET", *, token=None, tenant=None, body=None):
        scheme, host, port = origin(url)
        value = urllib.parse.urlsplit(url)
        target = (value.path or "/") + (f"?{value.query}" if value.query else "")
        address = self.pins.get(host, host)
        if scheme == "http" and host != address and not ipaddress.ip_address(address).is_loopback:
            raise GateFailure("plaintext transport is restricted to loopback test services")
        headers = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if tenant is not None:
            headers["X-Tenant-ID"] = tenant
        data = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(body).encode()
        if scheme == "https":
            connection = _PinnedHTTPSConnection(host, port, address, self.context, self.timeout)
        else:
            connection = _PinnedHTTPConnection(host, port, address, self.timeout)
        try:
            connection.request(method, target, body=data, headers=headers)
            response = connection.getresponse()
            payload = response.read(RESPONSE_LIMIT + 1)
        except (OSError, http.client.HTTPException) as error:
            raise GateFailure("HTTP transport failed (details withheld)") from error
        finally:
            connection.close()
        if len(payload) > RESPONSE_LIMIT:
            raise GateFailure("unexpectedly large response")
        return response.status, response.headers, payload


def successful_data(response):
    status, _, payload = response
    if status != 200:
        raise GateFailure(f"expected HTTP 200, received {status}")
    try:
        envelope = json.loads(payload)
    except ValueError as error:
        raise GateFailure("expected JSON envelope") from error
    # Core ApiResponse::success writes code 0; any other code is an error.
    if not isinstance(envelope, dict) or envelope.get("code") != 0 or not isinstance(envelope.get("data"), dict):
        raise GateFailure("expected successful JSON data envelope")
    return envelope["data"]


def echoes(text, value):
    """True when `value` appears in `text` as a whole token (not inside a longer word or number)."""
    return bool(value) and re.search(rf"(?<![A-Za-z0-9]){re.escape(value)}(?![A-Za-z0-9])", text) is not None


def denied(response, *, anonymous, identifiers=()):
    """A typed refusal: 401 without a bearer, 403/404 for a foreign bearer.

    Media's own text is the caller's file id (cross-tenant not-found) or the
    authorization rule (cross-owner), so a denial must not carry the fixture's
    file id, object key or either tenant id anywhere in its body.
    """
    status, headers, payload = response
    expected = {401} if anonymous else {403, 404}
    if status not in expected or headers.get("Location"):
        allowed = "/".join(str(value) for value in sorted(expected))
        raise GateFailure(f"expected a {allowed} denial without redirect, received {status}")
    try:
        envelope = json.loads(payload)
    except ValueError as error:
        raise GateFailure("expected denial envelope") from error
    code = envelope.get("code") if isinstance(envelope, dict) else None
    # A denial carries a positive error code; 0 (and the legacy 200) mean success.
    if not isinstance(code, int) or isinstance(code, bool) or code <= 0 or code == 200 or envelope.get("data") is not None:
        raise GateFailure("denied request exposed data")
    text = payload.decode("utf-8", errors="replace")
    if any(echoes(text, value) for value in identifiers):
        raise GateFailure("denial echoes an internal identifier (file id, object key or tenant id)")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--api-url", required=True,
                        help="Core public API base: origin plus optional path prefix, e.g. https://edge.example:8443/api")
    parser.add_argument("--admin-url",
                        help="Admin Gateway base (origin plus optional prefix); omit to skip the admin half")
    parser.add_argument("--bucket", help="configured bucket for the admin bucket probe (requires --admin-url)")
    parser.add_argument("--storage-origin",
                        help="allowed storage for presigned file-url and /files redirects: origin plus "
                             "optional path prefix, e.g. https://edge.example:8443/flare-media/")
    parser.add_argument("--ca-file", help="trust only this PEM CA bundle for HTTPS")
    parser.add_argument("--connect-address",
                        help="numeric IP to dial for the --api-url host; SNI, certificate and Host keep the name")
    args = parser.parse_args(argv)
    if args.admin_url and not args.bucket:
        parser.error("--bucket is required with --admin-url")
    if args.bucket and not args.admin_url:
        parser.error("--bucket only applies to the admin checks; pass --admin-url or drop --bucket")
    return args


def delete_body(file_id):
    # DELETE /file deserializes the proto DeleteFileRequest with plain serde:
    # every field is required, so an omitted hard_delete is a 422, not a delete.
    return {"file_id": file_id, "hard_delete": True}


def fixture_env(environ, with_admin):
    names = ["USER_A", "USER_B", "OTHER_A", "TENANT_A", "TENANT_B"] + (["ADMIN_A"] if with_admin else [])
    env = {name: environ.get(f"FLARE_MEDIA_E2E_{name}", "") for name in names}
    if not all(env.values()) or env["TENANT_A"] == env["TENANT_B"]:
        raise GateFailure("requires distinct tenant fixtures and all documented token variables")
    return env


def check_admin(transport, admin, env, bucket, out):
    bucket_path = "/bucket?" + urllib.parse.urlencode({"bucket": bucket})
    for method, path in MANAGEMENT_ROUTES:
        status = transport.request(admin + path, method, token=env["USER_A"], tenant=env["TENANT_A"],
                                   body={} if method == "POST" else None)[0]
        if status not in {401, 403}:
            raise GateFailure("user token reached an administrative handler")
    for tenant in [None, "*", env["TENANT_B"]]:
        status = transport.request(admin + bucket_path, token=env["ADMIN_A"], tenant=tenant)[0]
        if status != 403:
            raise GateFailure("admin tenant scope was not rejected")
    successful_data(transport.request(admin + bucket_path, token=env["ADMIN_A"], tenant=env["TENANT_A"]))
    print(f"admin: user token refused on {len(MANAGEMENT_ROUTES)} routes; "
          "missing/wildcard/foreign tenant scope refused; own-tenant bucket readable", file=out)


def verify_fixture(transport, api, env, file_id, content, allowance, out):
    info_path = "/file-info?" + urllib.parse.urlencode({"file_id": file_id})
    download_path = "/files/" + urllib.parse.quote(file_id, safe="")
    info = successful_data(transport.request(api + info_path, token=env["USER_A"]))
    if info.get("size") != len(content):
        raise GateFailure("owner file info does not describe the uploaded fixture")
    identifiers = [value for value in (file_id, info.get("object_key"), env["TENANT_A"], env["TENANT_B"])
                   if isinstance(value, str) and value]
    denials = 0
    for name in DENIED_PRINCIPALS:
        token = env[name] if name else None
        check = {"anonymous": token is None, "identifiers": identifiers}
        denied(transport.request(api + info_path, token=token), **check)
        denied(transport.request(api + download_path, token=token), **check)
        denied(transport.request(api + "/file-url", "POST", token=token, body={"file_id": file_id, "expires_in": 60}),
               **check)
        denied(transport.request(api + "/file", "DELETE", token=token, body=delete_body(file_id)), **check)
        denials += 4
    print(f"isolation: {denials} requests denied without data or internal identifiers "
          "(tenant B user, other tenant A user: 403/404; anonymous: 401; x info/download/url/delete)", file=out)
    if allowance is None:
        print("presigned: not evaluated (no --storage-origin)", file=out)
    else:
        issued = successful_data(transport.request(api + "/file-url", "POST", token=env["USER_A"],
                                                   body={"file_id": file_id, "expires_in": 60}))
        target = authorize_storage_target(issued.get("url") or "", allowance)
        # A new request, without the application bearer or identity headers.
        result = transport.request(target)
        if result[0] != 200 or result[2] != content:
            raise GateFailure("presigned file-url did not round-trip uploaded bytes")
        print(f"presigned: owner file-url under the storage prefix returns identical bytes ({len(content)} B)",
              file=out)
    result = transport.request(api + download_path, token=env["USER_A"])
    via = "streamed by the gateway"
    if result[0] == 307:
        if allowance is None:
            raise GateFailure("set --storage-origin to explicitly authorize the download origin")
        target = authorize_storage_target(result[1].get("Location", ""), allowance)
        # A new request, without the application bearer or identity headers.
        result = transport.request(target)
        via = "307 to the authorized storage prefix, fetched without the bearer"
    if result[0] != 200 or result[2] != content:
        raise GateFailure("authenticated download did not round-trip uploaded bytes")
    print(f"download: owner bytes identical ({len(content)} B, {via})", file=out)
    # The foreign DELETE attempts above must not have removed the fixture.
    successful_data(transport.request(api + info_path, token=env["USER_A"]))


def run(args, environ, transport, out=None):
    out = out or sys.stdout
    if environ.get("FLARE_MEDIA_E2E_DISPOSABLE") != "1":
        raise GateFailure("requires FLARE_MEDIA_E2E_DISPOSABLE=1 and disposable tenants/services")
    api_base = service_base(args.api_url, "API")
    admin_base = service_base(args.admin_url, "admin") if args.admin_url else None
    allowance = storage_allowance(args.storage_origin) if args.storage_origin else None
    env = fixture_env(environ, admin_base is not None)
    api = api_base + "/api/v1/medias"
    for method, path in MANAGEMENT_ROUTES:
        status = transport.request(api + path, method, token=env["USER_A"], body={} if method == "POST" else None)[0]
        if status != 404:
            raise GateFailure("ordinary gateway still exposes a management route")
    print(f"public: {len(MANAGEMENT_ROUTES)} management routes return 404 for a user token", file=out)
    if admin_base:
        check_admin(transport, admin_base + "/api/v1/admin/medias", env, args.bucket, out)
    else:
        print("admin: not evaluated (no --admin-url)", file=out)
    content = f"SEC05 disposable fixture {uuid.uuid4()}".encode()
    uploaded = successful_data(transport.request(api + "/upload-file", "POST", token=env["USER_A"], body={
        "metadata": {"file_name": "sec05.txt", "mime_type": "text/plain", "file_size": len(content), "file_type": 0, "upload_id": "", "user_id": "spoofed-owner", "metadata": {"tenant_id": env["TENANT_B"]}},
        "payload": base64.b64encode(content).decode(),
    }))
    file_id = uploaded.get("file_id")
    if not uploaded.get("success") or not isinstance(file_id, str) or not file_id:
        raise GateFailure("upload failed or returned no file id")

    def delete_fixture():
        # Delete only the exact file created by this invocation; never orphan-clean a tenant.
        successful_data(transport.request(api + "/file", "DELETE", token=env["USER_A"], body=delete_body(file_id)))

    try:
        verify_fixture(transport, api, env, file_id, content, allowance, out)
    except BaseException:
        try:
            delete_fixture()
        except GateFailure:
            print("cleanup: deleting the owned fixture also failed", file=sys.stderr)
        raise
    delete_fixture()
    scope = "management boundary" if admin_base else "public management boundary (admin not evaluated)"
    print(f"PASS: {scope}, tenant/owner denials, authenticated upload/download; owned fixture deleted", file=out)


def main(argv=None, environ=None):
    args = parse_args(argv)
    environ = os.environ if environ is None else environ
    pins = connect_pins(args.connect_address, args.api_url)
    run(args, environ, Transport(ca_file=args.ca_file, pins=pins))


if __name__ == "__main__":
    try:
        main()
    except GateFailure as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)
