"""check_dockerfile_build_args.sh 的回归测试：冻结仓整目录排除，其余照常拦截。

每个用例在临时目录里搭一个最小的多仓布局，经 FLARE_DOCKERFILE_SCAN_ROOT 指给门禁，
不读也不改真实工作区里的文件。
"""

import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/check_dockerfile_build_args.sh"

GOOD = """FROM rust:1 AS build
ARG APT_MIRROR
ARG CARGO_REGISTRY_MIRROR
ARG CARGO_BUILD_JOBS
ENV RUST_MIN_STACK=16777216
RUN cargo build --release
"""

# 与冻结仓 flare-im-console/deploy/Dockerfile.api 同形：从源码编译，却一个参数都没声明。
BAD = """FROM rust:1 AS build
RUN cargo build --release
"""

RUNTIME_ONLY = """FROM debian:bookworm-slim
COPY app /usr/local/bin/app
"""


class DockerfileBuildArgsGateTest(unittest.TestCase):
    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp(prefix="dockerfile-gate-"))
        self.addCleanup(shutil.rmtree, self.tmp)

    def write(self, rel, text):
        path = self.tmp / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def run_gate(self):
        env = dict(os.environ)
        env["FLARE_DOCKERFILE_SCAN_ROOT"] = str(self.tmp)
        return subprocess.run(
            ["bash", str(SCRIPT)], env=env, capture_output=True, text=True, timeout=60
        )

    def test_frozen_console_repo_is_not_scanned(self):
        self.write("flare-im-core/deploy/Dockerfile", GOOD)
        self.write("flare-im-console/deploy/Dockerfile.api", BAD)
        result = self.run_gate()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("1 个源码构建 Dockerfile", result.stdout)
        self.assertIn("1 个 Dockerfile 属已冻结仓", result.stdout)

    def test_other_repo_missing_args_still_fails(self):
        self.write("flare-im-console/deploy/Dockerfile.api", BAD)
        self.write("flare-social/deploy/Dockerfile", BAD)
        result = self.run_gate()
        self.assertEqual(result.returncode, 1)
        self.assertIn("flare-social/deploy/Dockerfile", result.stderr)
        self.assertNotIn("flare-im-console", result.stderr)

    def test_exclusion_is_exact_top_level_name(self):
        # 同名前缀的新仓、更深层的同名目录都不属于冻结仓，必须照常拦截。
        self.write("flare-im-console-next/Dockerfile", BAD)
        self.write("flare-social/vendor/flare-im-console/Dockerfile", BAD)
        result = self.run_gate()
        self.assertEqual(result.returncode, 1)
        self.assertIn("flare-im-console-next/Dockerfile", result.stderr)
        self.assertIn("flare-social/vendor/flare-im-console/Dockerfile", result.stderr)

    def test_each_missing_item_is_named(self):
        self.write(
            "flare-deploy/Dockerfile.bundle",
            GOOD.replace("ARG CARGO_BUILD_JOBS\n", "").replace(
                "ENV RUST_MIN_STACK=16777216\n", ""
            ),
        )
        result = self.run_gate()
        self.assertEqual(result.returncode, 1)
        self.assertIn("缺少 CARGO_BUILD_JOBS, RUST_MIN_STACK", result.stderr)

    def test_runtime_only_and_build_dirs_are_skipped(self):
        self.write("flare-social/deploy/Dockerfile.runtime", RUNTIME_ONLY)
        self.write("flare-im-core/target/debug/Dockerfile", BAD)
        self.write("flare-im-core/deploy/Dockerfile", GOOD)
        result = self.run_gate()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("1 个源码构建 Dockerfile", result.stdout)

    def test_real_workspace_layout_passes(self):
        result = subprocess.run(
            ["bash", str(SCRIPT)],
            env={k: v for k, v in os.environ.items() if k != "FLARE_DOCKERFILE_SCAN_ROOT"},
            capture_output=True,
            text=True,
            timeout=120,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
