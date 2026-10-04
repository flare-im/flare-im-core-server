#!/usr/bin/env python3
"""Run real credential projection repositories on an owned local PostgreSQL DB.

Component regression, NOT containerized full-stack or Platform HTTP acceptance.
"""
import os
from pathlib import Path
import secrets
import signal
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def main():
    host = os.environ.get('PGHOST', '/tmp')
    if (host not in ('/tmp', 'localhost', '127.0.0.1', '::1')
            or any(os.environ.get(k) for k in ('PGSERVICE', 'PGSERVICEFILE', 'PGHOSTADDR'))):
        raise SystemExit('requires explicit local PostgreSQL, no remote/service connection')
    port = int(os.environ.get('PGPORT', '5432'))
    if not 0 < port <= 65535:
        raise SystemExit('invalid local PostgreSQL port')
    name = 'flare_credential_test_' + secrets.token_hex(6)
    address = ['-h', host, '-p', str(port)]
    created = False
    process = None
    try:
        subprocess.run(['createdb', *address, name], check=True, timeout=15)
        created = True
        for path in [ROOT / 'migrations/0010_20261004_credential_projection.sql']:
            subprocess.run(['psql', *address, '-X', '-d', name, '-v', 'ON_ERROR_STOP=1',
                            '-1', '-f', str(path)], check=True, stdout=subprocess.DEVNULL,
                           stderr=subprocess.PIPE, timeout=20)
        subprocess.run(['psql', *address, '-X', '-d', name, '-v', 'ON_ERROR_STOP=1', '-c',
                        f"CREATE TABLE flare_credential_test_fixture(token TEXT); INSERT INTO flare_credential_test_fixture VALUES('{name}')"],
                       check=True, stdout=subprocess.DEVNULL, timeout=10)
        user = subprocess.check_output(['psql', *address, '-X', '-qAt', '-d', name,
                                        '-c', 'SELECT current_user'], text=True, timeout=10).strip()
        env = dict(os.environ, FLARE_CREDENTIAL_TEST_DB=name, FLARE_CREDENTIAL_TEST_HOST=host,
                   FLARE_CREDENTIAL_TEST_PORT=str(port), FLARE_CREDENTIAL_TEST_USER=user,
                   CARGO_NET_OFFLINE='true')
        process = subprocess.Popen(['cargo', 'test', '--offline', '--locked', '-q', '-p', 'flare-capability',
                                    '--lib', 'credential_postgres_atomic_regression', '--', '--ignored'],
                                   cwd=ROOT, env=env, start_new_session=True)
        code = process.wait(timeout=600)
        if code:
            raise SystemExit(code)
        print('PASS flare-capability real PostgreSQL: atomic rollback, empty revocation watermark, 64 concurrent snapshots, app isolation')
    finally:
        if process is not None and process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5)
        if created:
            subprocess.run(['dropdb', *address, '--if-exists', name], check=True, timeout=15)


def interrupted(signum, frame):
    raise KeyboardInterrupt


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, interrupted)
    main()
