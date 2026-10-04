#!/usr/bin/env python3
"""Portable smoke timeout. Kill the command's whole group, never a named user process."""
import argparse
import math
import os
import signal
import subprocess
import sys


def seconds(value):
    value = float(value)
    if not math.isfinite(value) or not 0 < value <= 3600:
        raise argparse.ArgumentTypeError('timeout must be finite and between 0 and 3600 seconds')
    return value


def terminate_group(process):
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        pass
    # The parent may exit before a descendant. Always finish the same group.
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('seconds', type=seconds)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not args.command:
        parser.error('command required')
    process = subprocess.Popen(args.command, start_new_session=True)

    def interrupted(number, frame):
        terminate_group(process)
        raise SystemExit(128 + number)

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        code = process.wait(timeout=args.seconds)
        return code if code >= 0 else 128 - code
    except subprocess.TimeoutExpired:
        terminate_group(process)
        return 124


if __name__ == '__main__':
    sys.exit(main())
