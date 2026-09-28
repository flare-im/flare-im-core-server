"""Local launcher regression tests; uses a synthetic credential only."""
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time
import unittest


LIB = Path(__file__).resolve().parent


class DevProcessTest(unittest.TestCase):
    def test_credential_is_inherited_but_absent_from_wrapper_argv(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            child = root / "child"
            child.write_text(
                '#!/bin/bash\n'
                'test "$TEST_SECRET" = "fixture-secret" || exit 2\n'
                'test "$TEST_ENDPOINT" = "http://127.0.0.1:50062" || exit 3\n'
                'echo ready\nexec sleep 30\n'
            )
            child.chmod(0o700)
            env = dict(os.environ, PROJECT_ROOT=directory,
                       LOG_PYTHON=os.sys.executable,
                       ROTATING_SERVICE=str(LIB / "rotating_service.py"))
            pid = int(subprocess.check_output([
                'bash', '-c',
                'source "$1"; start_detached_process test "$2" "$3" '
                '"TEST_SECRET=fixture-secret" "TEST_ENDPOINT=http://127.0.0.1:50062"; '
                'echo "$DETACHED_PID"',
                'test', str(LIB / "dev_process.sh"), str(root / "out.log"), str(child),
            ], env=env, text=True))
            try:
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    log = root / "out.log"
                    if log.exists() and 'ready' in log.read_text():
                        break
                    time.sleep(0.02)
                else:
                    self.fail('child did not receive environment')
                argv = subprocess.check_output(['ps', '-p', str(pid), '-o', 'args='], text=True)
                self.assertIn('rotating_service.py', argv)
                self.assertNotIn('fixture-secret', argv)
                self.assertNotIn('TEST_SECRET', argv)
                self.assertNotIn('/usr/bin/env', argv)
            finally:
                os.kill(pid, signal.SIGTERM)


if __name__ == '__main__':
    unittest.main()
