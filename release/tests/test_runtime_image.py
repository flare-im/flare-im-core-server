"""Static gate for the Core runtime image healthcheck; no Docker and no build."""

import json
from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = ROOT / "release/Dockerfile"
STACK = ROOT / "deploy/docker-compose.stack.yml"
SUPERVISOR_PROBE = ["/usr/local/bin/flare-supervise", "health"]


def instructions(source):
    """Dockerfile instructions, comments dropped, continuations joined, spaces collapsed."""
    joined, current = [], ""
    for line in source.splitlines():
        if line.lstrip().startswith("#"):
            continue
        if line.rstrip().endswith("\\"):
            current += line.rstrip()[:-1] + " "
            continue
        current += line
        if current.strip():
            joined.append(" ".join(current.split()))
        current = ""
    if current.strip():
        joined.append(" ".join(current.split()))
    return joined


def final_stage(source):
    steps = instructions(source)
    last_from = max(index for index, step in enumerate(steps) if step.startswith("FROM "))
    return steps[last_from:]


def healthcheck(steps):
    found = [step for step in steps if step.startswith("HEALTHCHECK ")]
    if len(found) != 1:
        raise AssertionError(f"expected exactly one HEALTHCHECK, found {len(found)}")
    match = re.fullmatch(r"HEALTHCHECK((?: --[a-z-]+=\S+)*) CMD (\[.*\])", found[0])
    if not match:
        raise AssertionError(f"HEALTHCHECK must use options and exec-form CMD: {found[0]}")
    options = dict(option[2:].split("=", 1) for option in match.group(1).split())
    return options, json.loads(match.group(2))


def top_level_block(source, header):
    lines = source.splitlines()
    start = lines.index(header)
    block = []
    for line in lines[start + 1:]:
        if line and not line[0].isspace() and not line.startswith("#"):
            break
        block.append(line)
    return "\n".join(block)


class RuntimeImageHealthcheckTests(unittest.TestCase):
    def setUp(self):
        self.source = DOCKERFILE.read_text()
        self.runtime = final_stage(self.source)

    def test_default_supervisor_command_carries_its_own_healthcheck(self):
        options, command = healthcheck(self.runtime)
        self.assertEqual(command, SUPERVISOR_PROBE)
        # Same timings as the DEP-08 single-host Compose `core` healthcheck.
        self.assertEqual(options, {"interval": "10s", "timeout": "3s",
                                   "start-period": "90s", "retries": "3"})
        self.assertEqual(self.runtime[-1], 'CMD ["flare-core", "supervise", "--services=all"]')
        self.assertIn("RUN ln -s flare-supervise /usr/local/bin/flare-core", self.runtime)

    def test_probe_binary_is_built_and_copied_into_the_runtime_stage(self):
        builder = instructions(self.source)
        self.assertTrue(any("cargo build --locked --release" in step
                            and "-p flare-supervise" in step for step in builder))
        self.assertIn("COPY --from=builder /out/ /usr/local/bin/", self.runtime)

    def test_single_process_stack_disables_the_supervisor_healthcheck(self):
        stack = STACK.read_text()
        anchor = top_level_block(stack, "x-flare-service: &flare-service")
        self.assertRegex(anchor, r"\n  healthcheck:\n    disable: true\n")
        services = top_level_block(stack, "services:")
        blocks = re.split(r"\n  (?=[a-z][a-z0-9-]*:\n)", "\n" + services)
        commanded = [block for block in blocks if "\n    command:" in block]
        self.assertGreaterEqual(len(commanded), 15)
        for block in commanded:
            name = block.split(":", 1)[0].strip()
            with self.subTest(service=name):
                # Every one-binary container inherits the disabled check and no
                # service turns the supervisor socket probe back on.
                self.assertIn("<<: *flare-service", block)
                self.assertNotIn("healthcheck:", block)


if __name__ == "__main__":
    unittest.main()
