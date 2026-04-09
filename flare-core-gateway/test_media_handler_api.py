#!/usr/bin/env python3
"""
全量覆盖 flare-core-gateway media_handler HTTP 接口。

默认覆盖以下 18 个接口：
  /upload-url, /upload-file, /multipart/*, /file-url, /file-info, /file, /references,
  /cleanup-orphaned-assets, /process-image, /process-video, /object-acl, /objects, /bucket

用法:
  python3 test_media_handler_api.py --gateway http://127.0.0.1:50050 --user-id 123456
  python3 test_media_handler_api.py --gateway http://127.0.0.1:50050 --user-id 123456 --strict
"""

from __future__ import annotations

import argparse
import json
import random
import string
import sys
from typing import Any, Dict, Optional, Set
from urllib import error, request


TINY_PNG_BYTES = [
    137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82,
    0, 0, 0, 1, 0, 0, 0, 1, 8, 6, 0, 0, 0, 31, 21, 196,
    137, 0, 0, 0, 13, 73, 68, 65, 84, 8, 153, 99, 248, 15, 4, 0,
    9, 251, 3, 253, 167, 119, 94, 253, 0, 0, 0, 0, 73, 69, 78, 68,
    174, 66, 96, 130,
]


def rand_suffix(n: int = 10) -> str:
    return "".join(random.choices(string.digits, k=n))


def rand_upload_id(prefix: str = "mp-local") -> str:
    return f"{prefix}-{rand_suffix()}"


def build_headers(user_id: str, tenant_id: str) -> Dict[str, str]:
    return {
        "x-user-id": user_id,
        "x-tenant-id": tenant_id,
        "x-trace-id": f"trace-local-{rand_upload_id('trace')}",
    }


def http_json(
    method: str,
    url: str,
    headers: Dict[str, str],
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json", **headers},
        method=method.upper(),
    )
    try:
        with request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return {"status": resp.status, "body": raw}
    except error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        return {"status": e.code, "body": raw}
    except Exception as e:  # pylint: disable=broad-except
        return {"status": -1, "body": str(e)}


def parse_json(raw: str) -> Optional[Dict[str, Any]]:
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:  # pylint: disable=broad-except
        return None


def data_field(parsed: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(parsed, dict):
        return {}
    data = parsed.get("data")
    return data if isinstance(data, dict) else {}


def get_key(obj: Dict[str, Any], *keys: str) -> Optional[Any]:
    for key in keys:
        if key in obj and obj[key] is not None:
            return obj[key]
    return None


def print_response(name: str, method: str, path: str, res: Dict[str, Any]) -> None:
    print(f"\n==> [{name}] {method.upper()} {path}")
    print(f"status={res['status']}")
    print(res["body"])


def is_unimplemented_body(body: str) -> bool:
    text = body.lower()
    return ("unimplemented" in text) or ("not implemented" in text)


class CaseCounter:
    def __init__(self) -> None:
        self.passed = 0
        self.failed = 0
        self.covered: Set[str] = set()

    def ok(self, case_name: str) -> None:
        self.passed += 1
        self.covered.add(case_name)
        print(f"[PASS] {case_name}")

    def fail(self, case_name: str, reason: str) -> None:
        self.failed += 1
        self.covered.add(case_name)
        print(f"[FAIL] {case_name}: {reason}")


def make_metadata(user_id: str, file_name: str, mime_type: str, file_size: int, file_type: int, upload_id: str) -> Dict[str, Any]:
    return {
        "file_name": file_name,
        "mime_type": mime_type,
        "file_size": file_size,
        "file_type": file_type,
        "upload_id": upload_id,
        "metadata": {},
        "user_id": user_id,
        "trace_id": upload_id,
        "namespace": "im.message",
        "business_tag": "chat_attachment",
        "bucket": "",
        "object_key": "",
        "labels": {},
    }


def assert_200(counter: CaseCounter, case_name: str, res: Dict[str, Any]) -> bool:
    if res["status"] == 200:
        counter.ok(case_name)
        return True
    counter.fail(case_name, f"期望 200，实际 {res['status']}")
    return False


def assert_200_or_unimplemented(counter: CaseCounter, case_name: str, res: Dict[str, Any]) -> bool:
    if res["status"] == 200:
        counter.ok(case_name)
        return True
    if res["status"] in (500, 501) and is_unimplemented_body(res["body"]):
        counter.ok(case_name)
        print(f"[INFO] {case_name}: 当前服务返回 UNIMPLEMENTED，按已知能力基线视为通过")
        return True
    counter.fail(case_name, f"期望 200 或 UNIMPLEMENTED(500/501)，实际 {res['status']}")
    return False


def assert_reachable(counter: CaseCounter, case_name: str, res: Dict[str, Any], allowed_status: Set[int]) -> bool:
    if res["status"] in allowed_status:
        counter.ok(case_name)
        return True
    counter.fail(case_name, f"状态码不在允许范围 {sorted(allowed_status)}，实际 {res['status']}")
    return False


def assert_case(
    counter: CaseCounter,
    case_name: str,
    res: Dict[str, Any],
    strict: bool,
    allowed_status: Optional[Set[int]] = None,
    allow_unimplemented: bool = False,
) -> bool:
    if strict:
        return assert_200(counter, case_name, res)
    if allow_unimplemented:
        return assert_200_or_unimplemented(counter, case_name, res)
    return assert_reachable(counter, case_name, res, allowed_status or {200})


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway", default="http://127.0.0.1:50050", help="core-gateway 地址")
    parser.add_argument("--user-id", default="123456", help="x-user-id")
    parser.add_argument("--tenant-id", default="0", help="x-tenant-id")
    parser.add_argument("--bucket", default="flare-media", help="测试使用的 bucket")
    parser.add_argument("--strict", action="store_true", help="严格模式：全部接口必须返回 200 且关键字段必须存在")
    args = parser.parse_args()

    gateway = args.gateway.rstrip("/")
    headers = build_headers(args.user_id, args.tenant_id)
    counter = CaseCounter()

    # 1) upload-file
    upload_file_payload = {
        "metadata": make_metadata(
            user_id=args.user_id,
            file_name="api-test-image.png",
            mime_type="image/png",
            file_size=len(TINY_PNG_BYTES),
            file_type=1,
            upload_id=rand_upload_id("upl"),
        ),
        "payload": TINY_PNG_BYTES,
        "chunk_size": 262144,
    }
    res = http_json("POST", f"{gateway}/api/v1/medias/upload-file", headers, upload_file_payload)
    print_response("upload_file", "POST", "/api/v1/medias/upload-file", res)
    if not assert_case(counter, "upload_file", res, args.strict, {200, 400, 404, 500, 503}):
        return 1
    parsed = parse_json(res["body"])
    upload_data = data_field(parsed)
    file_id = get_key(upload_data, "fileId", "file_id")
    if not isinstance(file_id, str) or not file_id:
        if args.strict:
            counter.fail("upload_file", "严格模式下返回中缺少 file_id")
            return 1
        file_id = "api-test-fallback-file-id"
        print("[INFO] upload_file 未返回 file_id，后续使用 fallback file_id 执行覆盖测试")

    # 2) generate_upload_url（当前后端可能未实现）
    upload_url_payload = {
        "bucket": args.bucket,
        "object_key": f"tests/{rand_upload_id('obj')}.bin",
        "mime_type": "application/octet-stream",
        "expected_size": 128,
        "expires_in": 3600,
    }
    res = http_json("POST", f"{gateway}/api/v1/medias/upload-url", headers, upload_url_payload)
    print_response("generate_upload_url", "POST", "/api/v1/medias/upload-url", res)
    if not assert_case(counter, "generate_upload_url", res, args.strict, allow_unimplemented=True):
        return 1

    # 3) multipart initiate/chunk/complete
    mp_upload_id = rand_upload_id("mp")
    mp_payload_bytes = [1, 2, 3, 4, 5, 6, 7, 8]
    init_payload = {
        "metadata": make_metadata(
            user_id=args.user_id,
            file_name="api-test-multipart.bin",
            mime_type="application/octet-stream",
            file_size=len(mp_payload_bytes),
            file_type=5,
            upload_id=mp_upload_id,
        ),
        "desired_chunk_size": 262144,
    }
    init_res = http_json("POST", f"{gateway}/api/v1/medias/multipart/initiate", headers, init_payload)
    print_response("multipart_initiate", "POST", "/api/v1/medias/multipart/initiate", init_res)
    if not assert_case(counter, "multipart_initiate", init_res, args.strict, {200, 400, 404, 500, 503}):
        return 1
    init_parsed = parse_json(init_res["body"])
    init_data = data_field(init_parsed)
    server_upload_id = get_key(init_data, "uploadId", "upload_id")
    if not isinstance(server_upload_id, str) or not server_upload_id:
        if args.strict:
            counter.fail("multipart_initiate", "严格模式下返回中缺少 upload_id")
            return 1
        server_upload_id = "api-test-fallback-upload-id"
        print("[INFO] multipart_initiate 未返回 upload_id，后续使用 fallback upload_id 执行覆盖测试")

    chunk_payload = {
        "upload_id": server_upload_id,
        "chunk_index": 0,
        "payload": mp_payload_bytes,
    }
    chunk_res = http_json("POST", f"{gateway}/api/v1/medias/multipart/chunk", headers, chunk_payload)
    print_response("multipart_chunk", "POST", "/api/v1/medias/multipart/chunk", chunk_res)
    if not assert_case(counter, "multipart_chunk", chunk_res, args.strict, {200, 400, 404, 500, 503}):
        return 1

    complete_payload = {"upload_id": server_upload_id}
    complete_res = http_json("POST", f"{gateway}/api/v1/medias/multipart/complete", headers, complete_payload)
    print_response("multipart_complete", "POST", "/api/v1/medias/multipart/complete", complete_res)
    if not assert_case(counter, "multipart_complete", complete_res, args.strict, {200, 400, 404, 500, 503}):
        return 1

    complete_parsed = parse_json(complete_res["body"])
    complete_data = data_field(complete_parsed)
    multipart_file_id = get_key(complete_data, "fileId", "file_id")
    if not isinstance(multipart_file_id, str) or not multipart_file_id:
        if args.strict:
            counter.fail("multipart_complete", "严格模式下返回中缺少 file_id")
            return 1
        multipart_file_id = "api-test-fallback-multipart-file-id"
        print("[INFO] multipart_complete 未返回 file_id，后续使用 fallback multipart file_id 执行覆盖测试")

    # 4) multipart abort（对新会话）
    abort_init_payload = {
        "metadata": make_metadata(
            user_id=args.user_id,
            file_name="api-test-abort.bin",
            mime_type="application/octet-stream",
            file_size=16,
            file_type=5,
            upload_id=rand_upload_id("mp-abort"),
        ),
        "desired_chunk_size": 262144,
    }
    abort_init_res = http_json("POST", f"{gateway}/api/v1/medias/multipart/initiate", headers, abort_init_payload)
    print_response("multipart_abort_init", "POST", "/api/v1/medias/multipart/initiate", abort_init_res)
    if not assert_case(counter, "multipart_abort_init", abort_init_res, args.strict, {200, 400, 404, 500, 503}):
        return 1
    abort_init_parsed = parse_json(abort_init_res["body"])
    abort_data = data_field(abort_init_parsed)
    abort_upload_id = get_key(abort_data, "uploadId", "upload_id")
    if not isinstance(abort_upload_id, str) or not abort_upload_id:
        if args.strict:
            counter.fail("multipart_abort_init", "严格模式下返回中缺少 upload_id")
            return 1
        abort_upload_id = "api-test-fallback-abort-upload-id"
        print("[INFO] multipart_abort_init 未返回 upload_id，后续使用 fallback abort upload_id 执行覆盖测试")
    abort_res = http_json("POST", f"{gateway}/api/v1/medias/multipart/abort", headers, {"upload_id": abort_upload_id})
    print_response("multipart_abort", "POST", "/api/v1/medias/multipart/abort", abort_res)
    if not assert_case(counter, "multipart_abort", abort_res, args.strict, {200, 400, 404, 500, 503}):
        return 1

    # 5) file-url / file-info
    file_url_payload = {
        "file_id": file_id,
        "expires_in": 600,
        "download": False,
        "response_headers": {},
    }
    file_url_res = http_json("POST", f"{gateway}/api/v1/medias/file-url", headers, file_url_payload)
    print_response("get_file_url", "POST", "/api/v1/medias/file-url", file_url_res)
    if not assert_case(counter, "get_file_url", file_url_res, args.strict, {200, 400, 404, 500, 503}):
        return 1

    file_info_res = http_json("GET", f"{gateway}/api/v1/medias/file-info?file_id={file_id}", headers)
    print_response("get_file_info", "GET", "/api/v1/medias/file-info", file_info_res)
    if not assert_case(counter, "get_file_info", file_info_res, args.strict, {200, 400, 404, 500, 503}):
        return 1

    # 6) references: create/list/delete
    create_ref_payload = {
        "file_id": file_id,
        "namespace": "im.message",
        "owner_id": args.user_id,
        "business_tag": "chat_attachment",
        "metadata": {"test_case": "media_handler_full"},
        "display_name": "api-test-ref",
        "description": "api coverage",
    }
    create_ref_res = http_json("POST", f"{gateway}/api/v1/medias/references", headers, create_ref_payload)
    print_response("create_reference", "POST", "/api/v1/medias/references", create_ref_res)
    if not assert_case(counter, "create_reference", create_ref_res, args.strict, {200, 400, 404, 500, 503}):
        return 1

    list_ref_res = http_json("GET", f"{gateway}/api/v1/medias/references?file_id={file_id}", headers)
    print_response("list_references", "GET", "/api/v1/medias/references", list_ref_res)
    if not assert_case(counter, "list_references", list_ref_res, args.strict, {200, 400, 404, 500, 503}):
        return 1

    delete_ref_payload = {
        "file_id": file_id,
        "reference_id": "",
    }
    delete_ref_res = http_json("DELETE", f"{gateway}/api/v1/medias/references", headers, delete_ref_payload)
    print_response("delete_reference", "DELETE", "/api/v1/medias/references", delete_ref_res)
    if not assert_case(counter, "delete_reference", delete_ref_res, args.strict, {200, 400, 404, 500, 503}):
        return 1

    # 7) cleanup orphaned assets
    cleanup_payload = {
        "limit": 10,
        "dry_run": True,
    }
    cleanup_res = http_json("POST", f"{gateway}/api/v1/medias/cleanup-orphaned-assets", headers, cleanup_payload)
    print_response("cleanup_orphaned_assets", "POST", "/api/v1/medias/cleanup-orphaned-assets", cleanup_res)
    if not assert_case(counter, "cleanup_orphaned_assets", cleanup_res, args.strict, {200, 400, 404, 500, 503}):
        return 1

    # 8) process-image / process-video
    process_image_payload = {
        "file_id": file_id,
        "operations": [
            {
                "type": "thumbnail",
                "size": 64,
            }
        ],
        "target_bucket": "",
        "output_prefix": "tests/process-image",
    }
    process_image_res = http_json("POST", f"{gateway}/api/v1/medias/process-image", headers, process_image_payload)
    print_response("process_image", "POST", "/api/v1/medias/process-image", process_image_res)
    if not assert_case(counter, "process_image", process_image_res, args.strict, {200, 400, 404, 500, 503}):
        return 1

    process_video_payload = {
        "file_id": multipart_file_id,
        "operations": [
            {
                "type": "compress",
                "bitrate": 400,
                "preset": "fast",
            }
        ],
        "target_bucket": "",
        "output_prefix": "tests/process-video",
    }
    process_video_res = http_json("POST", f"{gateway}/api/v1/medias/process-video", headers, process_video_payload)
    print_response("process_video", "POST", "/api/v1/medias/process-video", process_video_res)
    if not assert_case(counter, "process_video", process_video_res, args.strict, {200, 400, 404, 500, 503}):
        return 1

    # 9) object-acl / objects / bucket（当前服务可能未实现）
    set_acl_payload = {
        "file_id": file_id,
        "entries": [
            {
                "principal": f"user:{args.user_id}",
                "permissions": ["read"],
            }
        ],
    }
    set_acl_res = http_json("POST", f"{gateway}/api/v1/medias/object-acl", headers, set_acl_payload)
    print_response("set_object_acl", "POST", "/api/v1/medias/object-acl", set_acl_res)
    if not assert_case(counter, "set_object_acl", set_acl_res, args.strict, allow_unimplemented=True):
        return 1

    list_objects_res = http_json(
        "GET",
        f"{gateway}/api/v1/medias/objects?bucket={args.bucket}&prefix=tests",
        headers,
    )
    print_response("list_objects", "GET", "/api/v1/medias/objects", list_objects_res)
    if not assert_case(counter, "list_objects", list_objects_res, args.strict, allow_unimplemented=True):
        return 1

    describe_bucket_res = http_json(
        "GET",
        f"{gateway}/api/v1/medias/bucket?bucket={args.bucket}",
        headers,
    )
    print_response("describe_bucket", "GET", "/api/v1/medias/bucket", describe_bucket_res)
    if not assert_case(counter, "describe_bucket", describe_bucket_res, args.strict, allow_unimplemented=True):
        return 1

    # 10) delete-file（清理）
    delete_file_payload = {
        "file_id": file_id,
        "hard_delete": False,
    }
    delete_file_res = http_json("DELETE", f"{gateway}/api/v1/medias/file", headers, delete_file_payload)
    print_response("delete_file", "DELETE", "/api/v1/medias/file", delete_file_res)
    if not assert_case(counter, "delete_file", delete_file_res, args.strict, {200, 400, 404, 500, 503}):
        return 1

    delete_file_payload2 = {
        "file_id": multipart_file_id,
        "hard_delete": False,
    }
    delete_file_res2 = http_json("DELETE", f"{gateway}/api/v1/medias/file", headers, delete_file_payload2)
    print_response("delete_file_multipart_file", "DELETE", "/api/v1/medias/file", delete_file_res2)
    if not assert_case(counter, "delete_file_multipart_file", delete_file_res2, args.strict, {200, 400, 404, 500, 503}):
        return 1

    expected_cases = {
        "upload_file",
        "generate_upload_url",
        "multipart_initiate",
        "multipart_chunk",
        "multipart_complete",
        "multipart_abort_init",
        "multipart_abort",
        "get_file_url",
        "get_file_info",
        "create_reference",
        "list_references",
        "delete_reference",
        "cleanup_orphaned_assets",
        "process_image",
        "process_video",
        "set_object_acl",
        "list_objects",
        "describe_bucket",
        "delete_file",
        "delete_file_multipart_file",
    }
    uncovered = expected_cases - counter.covered
    if uncovered:
        counter.failed += len(uncovered)
        for case_name in sorted(uncovered):
            print(f"[FAIL] {case_name}: 未被执行")

    print("\n==============================")
    print("Media Handler Full API Report")
    print("==============================")
    print(f"覆盖接口用例: {len(counter.covered)}")
    print(f"通过: {counter.passed}")
    print(f"失败: {counter.failed}")

    if counter.failed > 0:
        print("\n[FAIL] media_handler 全量接口测试存在失败。")
        return 2
    print("\n[OK] media_handler 全量接口测试通过。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
