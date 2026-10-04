import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import signal
import unittest

HERE = Path(__file__).with_name('timeout_process.py')
spec = importlib.util.spec_from_file_location('smoke_timeout', HERE)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class TimeoutTests(unittest.TestCase):
    def test_invalid_or_unbounded_deadlines_are_rejected(self):
        for invalid in ('0', '-1', 'inf', 'nan', '3601'):
            with self.assertRaises(Exception):
                module.seconds(invalid)
        self.assertEqual(module.seconds('0.1'), 0.1)

    def test_command_exit_status_and_output_are_preserved(self):
        result = subprocess.run([sys.executable, str(HERE), '5', sys.executable,
                                 '-c', 'print("real-child"); raise SystemExit(7)'],
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 7)
        self.assertEqual(result.stdout.strip(), 'real-child')

    def test_deadline_terminates_the_child_and_returns_124(self):
        result = subprocess.run([sys.executable, str(HERE), '0.1', sys.executable,
                                 '-c', 'import time; time.sleep(20)'], timeout=10)
        self.assertEqual(result.returncode, 124)

    def test_missing_command_is_rejected(self):
        result = subprocess.run([sys.executable, str(HERE), '1'], capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 2)

    def test_deadline_kills_descendants_even_after_parent_exits(self):
        # Descendant ignores TERM and inherits stdout. communicate can finish
        # only once the watchdog KILLs the entire owned process group.
        command = ('import subprocess,sys,time; '
                   'subprocess.Popen([sys.executable,"-c",'
                   '"import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(20)"]); '
                   'time.sleep(20)')
        result = subprocess.run([sys.executable, str(HERE), '0.5', sys.executable,
                                 '-c', command], capture_output=True, timeout=8)
        self.assertEqual(result.returncode, 124)

    def test_interrupt_is_forwarded_and_status_is_nonzero(self):
        with subprocess.Popen([sys.executable, str(HERE), '20', sys.executable,
                               '-u', '-c', 'import time; print("ready"); time.sleep(20)'],
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as process:
            try:
                self.assertEqual(process.stdout.readline().strip(), 'ready')
                os.kill(process.pid, signal.SIGTERM)
                process.communicate(timeout=8)
                self.assertEqual(process.returncode, 128 + signal.SIGTERM)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=8)


if __name__ == '__main__':
    unittest.main()
