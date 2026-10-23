"""The development launcher hands each key only to the processes that
release/supervise-core.toml gives it. Synthetic values only; nothing starts."""
import os
from pathlib import Path
import re
import subprocess
import tomllib
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = (ROOT / "scripts" / "start_server.sh").read_text()
MANIFEST = tomllib.loads((ROOT / "release" / "supervise-core.toml").read_text())
# Launcher service names that the manifest spells differently.
MANIFEST_NAME = {"message-orchestrator": "orchestrator"}
CELL_PRIVATE = "FLARE_INTERNAL_CTX_PRIVATE_KEY"


def between(start, end):
    at = SCRIPT.index(start)
    return SCRIPT[at:SCRIPT.index(end, at)]


def array(name):
    body = between(f"{name}=(", ")")
    return re.findall(r'"?([A-Za-z0-9_-]+)"?\s*$', body[len(name) + 2:], re.M)


def manifest_service(name):
    name = MANIFEST_NAME.get(name, name)
    return next(s for s in MANIFEST["services"] if s["name"] == name)


def allowed(service, variable):
    """flare-supervise's `env_allowed`."""
    if variable in MANIFEST.get("forbidden_env", []):
        return False
    return (variable in MANIFEST.get("shared_env", [])
            or any(variable.startswith(p) for p in MANIFEST.get("shared_env_prefixes", []))
            or variable in service.get("inherit_env", [])
            or any(variable.startswith(p) for p in service.get("inherit_env_prefixes", [])))


CAPABILITY_ONLY = array("CAPABILITY_ONLY_ENV")
SERVICES = array("CORE_SERVICES") + ["admin-gateway"]


def launch(env):
    """The launcher's capability loader and per-service key cases, run as the launcher
    runs them: returns the variables the shell still exports and each service's env_args."""
    program = "\n".join([
        "set -e",
        between("CAPABILITY_ONLY_ENV=(", 'FLARE_BUILD_PROFILE="'),
        "CORE_INTERNAL_PRIVATE_KEY=cell-private",
        "CORE_INTERNAL_PUBLIC_KEY=cell-public",
        "load_capability_env",
        'env | grep -E "^(FLARE_CAPABILITY_CTX|FLARE_PLUGIN)_" | sed "s/^/inherited /" || true',
        'for service in "$@"; do',
        "    env_args=()",
        between('    case "$service" in\n        signaling-online|signaling-route|',
                '    case "$service" in\n        signaling-online|capability)\n'),
        '    printf "%s\\n" "service $service" "${env_args[@]}"',
        "done",
    ])
    base = {k: v for k, v in os.environ.items()
            if not k.startswith(("FLARE_PLUGIN_", "FLARE_CAPABILITY_", "FLARE_INTERNAL_CTX_"))}
    out = subprocess.check_output(["bash", "-c", program, "launcher", *SERVICES],
                                  env={**base, **env}, text=True)
    inherited, services, current = [], {}, None
    for line in out.splitlines():
        if line.startswith("inherited "):
            inherited.append(line.split(" ", 1)[1].split("=", 1)[0])
        elif line.startswith("service "):
            current = services.setdefault(line.split(" ", 1)[1], {})
        elif line:
            name, value = line.split("=", 1)
            current[name] = value
    return inherited, services


class LauncherKeysTest(unittest.TestCase):
    def test_capability_only_variables_are_the_manifests(self):
        capability = manifest_service("capability")
        expected = {v for v in capability["inherit_env"] if v.startswith("FLARE_PLUGIN_")}
        expected.add("FLARE_CAPABILITY_CTX_PRIVATE_KEY")
        self.assertEqual(set(CAPABILITY_ONLY), expected)
        for variable in CAPABILITY_ONLY:
            holders = [s["name"] for s in MANIFEST["services"] if allowed(s, variable)]
            self.assertEqual(holders, ["capability"], variable)

    def test_capability_keys_reach_capability_alone(self):
        env = {name: f"synthetic-{i}" for i, name in enumerate(CAPABILITY_ONLY)}
        env["FLARE_CAPABILITY_GRPC_URI"] = "http://127.0.0.1:50110"
        inherited, services = launch(env)
        self.assertEqual(inherited, [], "left in the inherited environment")
        for service, args in services.items():
            for name in CAPABILITY_ONLY:
                self.assertEqual(name in args, service == "capability", f"{service} {name}")
        for name in CAPABILITY_ONLY:
            self.assertEqual(services["capability"][name], env[name])

    def test_the_cell_private_key_goes_where_the_manifest_gives_it(self):
        _, services = launch({name: "synthetic" for name in CAPABILITY_ONLY})
        for service, args in services.items():
            self.assertEqual(CELL_PRIVATE in args, allowed(manifest_service(service), CELL_PRIVATE),
                             service)
        self.assertEqual(services["message-ingest"][CELL_PRIVATE], "cell-private")
        self.assertEqual(services["message-ingest"]["FLARE_INTERNAL_CTX_PUBLIC_KEY"], "cell-public")
        # Without the plugin resource bridge or the card path Capability signs nothing as the Cell.
        _, services = launch({})
        self.assertNotIn(CELL_PRIVATE, services["capability"])


if __name__ == "__main__":
    unittest.main()
