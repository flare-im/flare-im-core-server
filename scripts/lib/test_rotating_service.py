import importlib.util
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

SCRIPT = Path(__file__).with_name("rotating_service.py")
spec = importlib.util.spec_from_file_location("rotating_service", SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class RotationTests(unittest.TestCase):
    def test_rotation_retains_latest_bytes_and_bounds_each_file(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "service.log"
            log = module.RotatingLog(path, 100, 2)
            payload = bytes(range(256)) * 4
            log.write(payload)
            log.close()
            parts = [Path(f"{path}.2").read_bytes(), Path(f"{path}.1").read_bytes(), path.read_bytes()]
            self.assertTrue(all(len(part) <= 100 for part in parts))
            self.assertEqual(b"".join(parts), payload[-224:])
            self.assertFalse(Path(f"{path}.3").exists())

    def test_existing_oversized_file_is_bounded_and_single_writer(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "service.log"
            path.write_bytes(b"old" * 1000)
            log = module.RotatingLog(path, 100, 1)
            self.assertEqual(path.stat().st_size, 100)
            with self.assertRaises(BlockingIOError):
                module.RotatingLog(path, 100, 1)
            log.write(b"new")
            log.close()
            self.assertEqual(Path(f"{path}.1").stat().st_size, 100)
            self.assertEqual(path.read_bytes(), b"new")

    def test_stdout_stderr_and_exit_code(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "service.log"
            result = subprocess.run([sys.executable, str(SCRIPT), "--log", str(path), "--", sys.executable, "-c", "import sys; print('out'); print('err',file=sys.stderr); sys.exit(7)"])
            self.assertEqual(result.returncode, 7)
            self.assertIn(b"out", path.read_bytes())
            self.assertIn(b"err", path.read_bytes())

    def test_termination_reaches_child(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "service.log"
            code = "import signal,time,sys; signal.signal(signal.SIGTERM,lambda *_:sys.exit(0)); print('ready',flush=True); time.sleep(60)"
            proc = subprocess.Popen([sys.executable, str(SCRIPT), "--log", str(path), "--", sys.executable, "-c", code])
            try:
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    if path.exists() and b"ready" in path.read_bytes():
                        break
                    time.sleep(0.05)
                self.assertIn(b"ready", path.read_bytes())
                proc.send_signal(signal.SIGTERM)
                self.assertEqual(proc.wait(timeout=5), 0)
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait()


if __name__ == "__main__":
    unittest.main()
