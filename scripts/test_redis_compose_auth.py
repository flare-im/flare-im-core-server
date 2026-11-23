"""Exercise the rendered Compose Dragonfly command against a disposable container."""
import json
import os
from pathlib import Path
import subprocess
import time
import unittest
import uuid
import tempfile
import shutil
import select
import importlib.util
import re
import shlex
from unittest.mock import patch
from urllib.parse import quote, unquote, urlsplit

COMPOSE = Path(__file__).resolve().parents[1] / "deploy/docker-compose.yml"


def deployment_environment():
    path = COMPOSE.parent / "compose.py"
    spec = importlib.util.spec_from_file_location("compose_entry", path)
    entry = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(entry)
    return entry.compose_environment(["--env-file", "/dev/null"])


class RedisComposeUrlTest(unittest.TestCase):
    @unittest.skipUnless(shutil.which("redis-server") and shutil.which("redis-cli"), "requires local Redis binaries")
    def test_encoded_uri_authenticates_with_local_redis(self):
        for password in ("", "pa/ss@%#", "中文 :password"):
            with self.subTest(password=password), tempfile.TemporaryDirectory(prefix="flare-redis-url-") as directory:
                endpoint = str(Path(directory) / "redis.sock")
                process = subprocess.Popen(["redis-server", "--port", "0", "--unixsocket", endpoint,
                    "--save", "", "--appendonly", "no", "--requirepass", password], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                try:
                    output = b""
                    deadline = time.monotonic() + 5
                    while b"Ready to accept connections" not in output:
                        remaining = deadline - time.monotonic()
                        self.assertGreater(remaining, 0, output.decode())
                        self.assertTrue(select.select([process.stdout], [], [], remaining)[0], output.decode())
                        block = os.read(process.stdout.fileno(), 4096)
                        self.assertTrue(block, output.decode())
                        output += block
                    with patch.dict(os.environ, REDIS_PASSWORD=password, REDIS_URL=""):
                        uri = deployment_environment()["REDIS_URL"]
                    reply = subprocess.check_output(["redis-cli", "-u", uri, "-s", endpoint, "ping"],
                        env=dict(os.environ, REDISCLI_AUTH=""), stderr=subprocess.STDOUT, text=True)
                    self.assertIn("PONG", reply)
                finally:
                    process.terminate()
                    process.wait(timeout=5)
                    process.stdout.close()

    def test_reserved_passwords_and_explicit_urls(self):
        stack = COMPOSE.parent / "docker-compose.stack.yml"
        required = set(re.findall(r"\$\{([A-Z0-9_]+):\?", stack.read_text()))
        for password in ("", "ordinary", "pa/ss", "a@b%# :中文"):
            with self.subTest(password=password):
                values = dict.fromkeys(required, "test-value")
                values.update(REDIS_PASSWORD=password, REDIS_URL="")
                with patch.dict(os.environ, values):
                    env = deployment_environment()
                    rendered = json.loads(subprocess.check_output([
                        "docker", "compose", "--env-file", "/dev/null", "-f", str(COMPOSE),
                        "-f", str(COMPOSE.parent / "docker-compose.nats.yml"), "-f", str(stack), "config", "--format", "json",
                    ], env=env, text=True, stderr=subprocess.PIPE))
                uri = rendered["services"]["api-gateway"]["environment"]["REDIS_URL"]
                self.assertEqual(unquote(urlsplit(uri).password or ""), password)
                self.assertEqual(urlsplit(uri).hostname, "redis")
                self.assertEqual(rendered["services"]["push-worker"]["environment"]["PUSH_WORKER_DEVICE_TOKEN_REDIS_URL"], uri + "/5")
                self.assertEqual(rendered["services"]["redis"]["environment"]["REDISCLI_AUTH"], password)
        with patch.dict(os.environ, REDIS_PASSWORD="ignored", REDIS_URL="rediss://:encoded%2Fpass@external:6380"):
            self.assertEqual(deployment_environment()["REDIS_URL"], "rediss://:encoded%2Fpass@external:6380")


class ComposeEntryTest(unittest.TestCase):
    def config_environment(self, directory):
        configs = [COMPOSE.parent / "docker-compose.stack.yml", COMPOSE.parents[2] / "flare-social-server/deploy/docker-compose.social.yml"]
        required = set(re.findall(r"\$\{([A-Z0-9_]+):\?", "\n".join(path.read_text() for path in configs if path.is_file())))
        base = Path(directory) / "base.env"
        base.write_text("".join(f"{key}=test-value\n" for key in sorted(required - {"REDIS_URL"})) + "REDIS_PASSWORD=base-password\n")
        env = {key: value for key, value in os.environ.items() if key not in required | {"REDIS_PASSWORD", "REDIS_URL"}}
        return base, env

    def test_layered_env_files_preserve_credentials_and_precedence(self):
        with tempfile.TemporaryDirectory(prefix="flare-compose-env-") as directory:
            base, env = self.config_environment(directory)
            override = Path(directory) / "override.env"
            override.write_text("REDIS_PASSWORD=pa/ss\n")
            command = ["python3", str(COMPOSE.parent / "compose.py"), "--env-file", str(base), "--env-file", str(override),
                       "-f", str(COMPOSE), "-f", str(COMPOSE.parent / "docker-compose.nats.yml"),
                       "-f", str(COMPOSE.parent / "docker-compose.stack.yml"), "config", "--format", "json"]
            for values, expected in [({}, "redis://:pa%2Fss@redis:6379"),
                                     ({"REDIS_PASSWORD": "shell/pass"}, "redis://:shell%2Fpass@redis:6379"),
                                     ({"REDIS_URL": "rediss://:explicit@external:6380"}, "rediss://:explicit@external:6380")]:
                with self.subTest(environment=list(values)):
                    rendered = json.loads(subprocess.check_output(command, env=dict(env, **values), text=True, stderr=subprocess.PIPE))
                    gateway = rendered["services"]["api-gateway"]["environment"]
                    self.assertEqual(gateway["REDIS_URL"], expected)
                    self.assertEqual(rendered["services"]["signaling-gateway"]["environment"]["ACCESS_GATEWAY_TOKEN_SECRET"], "test-value")
                    self.assertEqual(rendered["services"]["redis"]["environment"]["REDISCLI_AUTH"], values.get("REDIS_PASSWORD", "pa/ss"))
            override.write_text("REDIS_PASSWORD=pa/ss\nREDIS_URL=rediss://:file@external:6380\n")
            rendered = json.loads(subprocess.check_output(command, env=env, text=True, stderr=subprocess.PIPE))
            self.assertEqual(rendered["services"]["api-gateway"]["environment"]["REDIS_URL"], "rediss://:file@external:6380")

    def test_documented_startup_commands_render(self):
        server = COMPOSE.parents[1]
        docs = [COMPOSE.parent / "README.md", server.parent / "README.md", server.parent / "README.zh-CN.md"]
        with tempfile.TemporaryDirectory(prefix="flare-compose-docs-") as directory:
            base, env = self.config_environment(directory)
            checked = 0
            for doc in docs:
                if not doc.is_file():
                    continue
                for block in re.findall(r"```bash\n(.*?)```", doc.read_text(), re.S):
                    for line in block.replace("\\\n", " ").splitlines():
                        if "docker-compose.stack.yml" not in line or " up " not in line:
                            continue
                        with self.subTest(document=doc.name, command=checked):
                            command = shlex.split(line)
                            command = command[:command.index("up")]
                            command[command.index("--env-file") + 1] = str(base)
                            result = subprocess.run([*command, "config", "--format", "json"], cwd=server, env=env,
                                                    stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
                            self.assertEqual(result.returncode, 0, result.stderr)
                            checked += 1
            self.assertGreaterEqual(checked, 1, "no documented full-stack command was exercised")


class RedisComposeAuthTest(unittest.TestCase):
    def test_password_and_empty_modes(self):
        for password in ("ponytail-test-pass", "", "pa/ss@%#"):
            with self.subTest(authenticated=bool(password)):
                env = dict(os.environ, REDIS_PASSWORD=password)
                rendered = subprocess.check_output(
                    ["docker", "compose", "--env-file", "/dev/null", "-f", str(COMPOSE), "config", "--format", "json"],
                    env=env, stderr=subprocess.PIPE, text=True,
                )
                redis = json.loads(rendered)["services"]["redis"]
                name = "ponytail-redis-" + uuid.uuid4().hex
                # Keep the production flags, with test-only memory/thread limits appended.
                command = redis["command"] + ["--maxmemory=256mb", "--proactor_threads=1"]
                try:
                    subprocess.check_output(["docker", "run", "--rm", "-d", "--name", name,
                                             "-e", "REDISCLI_AUTH=" + password, redis["image"], *command])
                    for attempt in range(100):
                        ready = subprocess.run(["docker", "exec", name, "redis-cli", "ping"], capture_output=True, text=True)
                        if "PONG" in ready.stdout:
                            break
                        time.sleep(0.05)
                    self.assertIn("PONG", ready.stdout)
                    encoded = quote(password, safe="")
                    uri = f"redis://:{encoded}@127.0.0.1:6379" if password else "redis://127.0.0.1:6379"
                    client = subprocess.check_output(["docker", "exec", "-e", "REDISCLI_AUTH=", name, "redis-cli", "-u", uri, "ping"], stderr=subprocess.STDOUT, text=True)
                    self.assertIn("PONG", client)
                    anonymous = subprocess.check_output(["docker", "exec", "-e", "REDISCLI_AUTH=", name, "redis-cli", "ping"], text=True)
                    self.assertIn("NOAUTH" if password else "PONG", anonymous)
                    if password:
                        wrong = subprocess.check_output(["docker", "exec", "-e", "REDISCLI_AUTH=incorrect", name, "redis-cli", "ping"], stderr=subprocess.STDOUT, text=True)
                        self.assertNotIn("PONG", wrong)
                        logs = subprocess.check_output(["docker", "logs", name], stderr=subprocess.STDOUT, text=True)
                        self.assertNotIn(password, logs)
                    self.assertEqual(redis["environment"]["REDISCLI_AUTH"], password)
                finally:
                    subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)


if __name__ == "__main__":
    unittest.main()
