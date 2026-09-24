#!/usr/bin/env python3
"""Run a service with bounded stdout/stderr logs and forwarded shutdown signals."""
import argparse
import fcntl
import os
from pathlib import Path
import selectors
import signal
import subprocess
import time


class RotatingLog:
    def __init__(self, path, limit, backups):
        self.path = Path(path)
        self.limit = limit
        self.backups = backups
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = open(str(self.path) + ".lock", "a")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.lock.close()
            raise
        for p in [self.path] + [Path(f"{self.path}.{i}") for i in range(1, backups + 1)]:
            if p.is_symlink():
                raise ValueError(f"Refusing symlink log: {p}")
            if p.exists() and p.stat().st_size > limit:
                with p.open("r+b") as stream:
                    stream.seek(-limit, os.SEEK_END)
                    tail = stream.read(limit)
                    stream.seek(0)
                    stream.write(tail)
                    stream.truncate(limit)
        self.stream = self.path.open("ab", buffering=0)
        os.chmod(self.path, 0o600)
        self.size = self.path.stat().st_size

    def write(self, data):
        while data:
            if self.size >= self.limit:
                self.stream.close()
                if self.backups:
                    for i in range(self.backups, 1, -1):
                        src = Path(f"{self.path}.{i - 1}")
                        if src.exists():
                            src.replace(f"{self.path}.{i}")
                    self.path.replace(f"{self.path}.1")
                else:
                    self.path.unlink()
                self.stream = self.path.open("ab", buffering=0)
                os.chmod(self.path, 0o600)
                self.size = 0
            chunk = data[: self.limit - self.size]
            self.stream.write(chunk)
            self.size += len(chunk)
            data = data[len(chunk):]

    def close(self):
        self.stream.close()
        self.lock.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", required=True)
    parser.add_argument("--max-bytes", type=int, default=int(os.environ.get("FLARE_LOG_MAX_BYTES", 20 * 1024 * 1024)))
    parser.add_argument("--backups", type=int, default=int(os.environ.get("FLARE_LOG_BACKUPS", 3)))
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command or args.max_bytes <= 0 or args.backups < 0:
        parser.error("A command, positive max-bytes, and nonnegative backups are required")
    log = RotatingLog(args.log, args.max_bytes, args.backups)
    child = None
    deadline = None

    def stop(signum, _frame):
        nonlocal deadline
        if deadline is None:
            deadline = time.monotonic() + 15
        if child is not None:
            try:
                os.killpg(child.pid, signum)
            except ProcessLookupError:
                pass

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGHUP, stop)
    try:
        child = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True)
        if deadline is not None:
            stop(signal.SIGTERM, None)
        with selectors.DefaultSelector() as selector:
            selector.register(child.stdout, selectors.EVENT_READ)
            while selector.get_map():
                if deadline is not None and time.monotonic() >= deadline:
                    try:
                        os.killpg(child.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                for key, _ in selector.select(timeout=0.5):
                    chunk = os.read(key.fd, 65536)
                    if chunk:
                        log.write(chunk)
                    else:
                        selector.unregister(key.fileobj)
        result = child.wait()
        return result if result >= 0 else 128 - result
    finally:
        if child is not None and child.poll() is None:
            os.killpg(child.pid, signal.SIGKILL)
            child.wait()
        log.close()


if __name__ == "__main__":
    raise SystemExit(main())
