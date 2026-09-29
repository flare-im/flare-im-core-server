#!/usr/bin/env python3
"""Opt-in SEC-05 HTTP/storage regression against disposable running services.

Tokens are environment-only; responses and signed storage URLs are never logged.
Does not call orphan cleanup: the PostgreSQL test covers its scoped selection.
"""
import argparse
import base64
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid


class GateFailure(Exception):
    pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def origin(url):
    value = urllib.parse.urlsplit(url)
    if value.scheme not in {"http", "https"} or not value.hostname or value.username or value.password:
        raise GateFailure("invalid service/storage URL")
    if value.fragment:
        raise GateFailure("URL fragments are not supported")
    if value.scheme == "http" and value.hostname not in {"127.0.0.1", "localhost", "::1"}:
        raise GateFailure("plaintext transport is restricted to loopback test services")
    return value.scheme, value.hostname, value.port or (443 if value.scheme == "https" else 80)


def request(url, method="GET", *, token=None, tenant=None, body=None):
    origin(url)
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if tenant is not None:
        headers["X-Tenant-ID"] = tenant
    if body is not None:
        headers["Content-Type"] = "application/json"
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    # Ignore machine proxy settings so credentials cannot be sent to an unrelated proxy.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    try:
        response = opener.open(req, timeout=15)
    except urllib.error.HTTPError as error:
        response = error
    except (urllib.error.URLError, TimeoutError) as error:
        raise GateFailure("HTTP transport failed (details withheld)") from error
    with response:
        payload = response.read(1024 * 1024 + 1)
        if len(payload) > 1024 * 1024:
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
    if not isinstance(envelope, dict) or envelope.get("code") != 200 or not isinstance(envelope.get("data"), dict):
        raise GateFailure("expected successful JSON data envelope")
    return envelope["data"]


def denied(response):
    status, headers, payload = response
    # Domain RPC errors currently lose their specific HTTP code. This asserts
    # fail-closed data behavior, not that the legacy error mapping is correct.
    if status not in {401, 403, 404, 500} or headers.get("Location"):
        raise GateFailure(f"expected denial without redirect, received {status}")
    try:
        envelope = json.loads(payload)
    except ValueError as error:
        raise GateFailure("expected denial envelope") from error
    if not isinstance(envelope, dict) or envelope.get("data") is not None or envelope.get("code") == 200:
        raise GateFailure("denied request exposed data")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--admin-url", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--storage-origin", help="Explicit allowed S3 origin when /files redirects")
    args = parser.parse_args()
    if os.environ.get("FLARE_MEDIA_E2E_DISPOSABLE") != "1":
        raise GateFailure("requires FLARE_MEDIA_E2E_DISPOSABLE=1 and disposable tenants/services")
    for url in [args.api_url, args.admin_url]:
        origin(url)
        parsed = urllib.parse.urlsplit(url)
        if parsed.query or parsed.path not in {"", "/"}:
            raise GateFailure("service URL must be an origin only")
    names = ["USER_A", "USER_B", "OTHER_A", "ADMIN_A", "TENANT_A", "TENANT_B"]
    env = {name: os.environ.get(f"FLARE_MEDIA_E2E_{name}", "") for name in names}
    if not all(env.values()) or env["TENANT_A"] == env["TENANT_B"]:
        raise GateFailure("requires distinct tenant fixtures and all documented token variables")
    api = args.api_url.rstrip("/") + "/api/v1/medias"
    admin = args.admin_url.rstrip("/") + "/api/v1/admin/medias"
    bucket_path = "/bucket?" + urllib.parse.urlencode({"bucket": args.bucket})
    for method, path in [("POST", "/cleanup-orphaned-assets"), ("POST", "/object-acl"), ("GET", "/objects"), ("GET", "/bucket")]:
        status, _, _ = request(api + path, method, token=env["USER_A"], body={} if method == "POST" else None)
        if status != 404:
            raise GateFailure("ordinary gateway still exposes a management route")
        status, _, _ = request(admin + path, method, token=env["USER_A"], tenant=env["TENANT_A"], body={} if method == "POST" else None)
        if status not in {401, 403}:
            raise GateFailure("user token reached an administrative handler")
    for tenant in [None, "*", env["TENANT_B"]]:
        status, _, _ = request(admin + bucket_path, token=env["ADMIN_A"], tenant=tenant)
        if status != 403:
            raise GateFailure("admin tenant scope was not rejected")
    successful_data(request(admin + bucket_path, token=env["ADMIN_A"], tenant=env["TENANT_A"]))
    content = f"SEC05 disposable fixture {uuid.uuid4()}".encode()
    uploaded = successful_data(request(api + "/upload-file", "POST", token=env["USER_A"], body={
        "metadata": {"file_name": "sec05.txt", "mime_type": "text/plain", "file_size": len(content), "file_type": 0, "upload_id": "", "user_id": "spoofed-owner", "metadata": {"tenant_id": env["TENANT_B"]}},
        "payload": base64.b64encode(content).decode(),
    }))
    file_id = uploaded.get("file_id")
    if not uploaded.get("success") or not isinstance(file_id, str) or not file_id:
        raise GateFailure("upload failed or returned no file id")
    try:
        info_path = "/file-info?" + urllib.parse.urlencode({"file_id": file_id})
        download_path = "/files/" + urllib.parse.quote(file_id, safe="")
        successful_data(request(api + info_path, token=env["USER_A"]))
        for token in [env["USER_B"], env["OTHER_A"], None]:
            denied(request(api + info_path, token=token))
            denied(request(api + download_path, token=token))
            denied(request(api + "/file-url", "POST", token=token, body={"file_id": file_id, "expires_in": 60}))
        result = request(api + download_path, token=env["USER_A"])
        if result[0] == 307:
            target = result[1].get("Location", "")
            if not args.storage_origin or origin(target) != origin(args.storage_origin):
                raise GateFailure("set --storage-origin to explicitly authorize the download origin")
            # A new request, without the application bearer or identity headers.
            result = request(target)
        if result[0] != 200 or result[2] != content:
            raise GateFailure("authenticated download did not round-trip uploaded bytes")
        successful_data(request(api + info_path, token=env["USER_A"]))
    finally:
        # Delete only the exact file created by this invocation; never orphan-clean a tenant.
        successful_data(request(api + "/file", "DELETE", token=env["USER_A"], body={"file_id": file_id}))
    print("PASS: management boundary, tenant/owner denials, authenticated upload/download; owned fixture deleted")


if __name__ == "__main__":
    try:
        main()
    except GateFailure as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)
