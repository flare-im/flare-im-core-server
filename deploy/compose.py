#!/usr/bin/env python3
"""Run Compose with a correctly encoded Redis URL; keep the server password raw."""
import argparse
import json
import os
from pathlib import Path
import subprocess
from urllib.parse import quote

DEPLOY = Path(__file__).resolve().parent


def compose_environment(env_arguments):
    command = ["docker", "compose", *env_arguments, "-f", str(DEPLOY / "docker-compose.yml")]
    rendered = json.loads(subprocess.check_output(command + ["config", "--format", "json"], text=True))
    variables = subprocess.check_output(command + ["config", "--environment"], text=True)
    explicit_url = next((line.partition("=")[2] for line in variables.splitlines() if line.startswith("REDIS_URL=")), "")
    password = rendered["services"]["redis"]["environment"]["REDISCLI_AUTH"]
    auth = ":" + quote(password, safe="") + "@" if password else ""
    return dict(os.environ, REDIS_URL=explicit_url or f"redis://{auth}redis:6379")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", action="append")
    options, arguments = parser.parse_known_args()
    if not arguments:
        parser.error("pass the usual Docker Compose files and command")
    env_arguments = [argument for path in options.env_file or [DEPLOY / ".env"]
                     for argument in ("--env-file", str(path))]
    return subprocess.call(
        ["docker", "compose", *env_arguments, *arguments],
        env=compose_environment(env_arguments),
    )


if __name__ == "__main__":
    raise SystemExit(main())
