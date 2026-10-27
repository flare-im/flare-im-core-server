"""Opt-in private Redis and Dragonfly script contracts; no installed registry is read.

FLARE_PUSH_BINDING_SERVICES_TEST=1 starts an ephemeral Redis unix socket and a
network-isolated Dragonfly container from the existing local flare-redis image.
No volume, published port, production key, provider token or external image pull.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import shlex
import subprocess
import tempfile
import time
import unittest
import uuid

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / 'flare-push/proxy/src/infrastructure/device_token_registry.rs').read_text()
SCRIPTS = {name: re.search(r'const ' + name + r': &str = r#"(.*?)"#;', SOURCE, re.S)[1]
           for name in ['REGISTER', 'REVOKE', 'INSPECT']}


def command(args, payload=None):
    if args[:3] == ['podman', 'machine', 'ssh']:
        args = args[:4] + [shlex.join(args[4:])]
    result = subprocess.run(args, input=payload, capture_output=True, text=True, timeout=25)
    lines = [x for x in result.stdout.splitlines() if not x.startswith('/etc/profile.d/default-env.sh:')]
    value = '\n'.join(lines).strip()
    if result.returncode or value.startswith(('ERR ', 'error:')):
        # The scripts use synthetic inputs only. Keep output bounded anyway.
        raise RuntimeError(value[:240] or result.stderr.strip()[-240:] or 'private backend command failed')
    if ' logs ' in ' '.join(args):
        return value + '\n' + result.stderr
    return value


@unittest.skipUnless(os.environ.get('FLARE_PUSH_BINDING_SERVICES_TEST') == '1',
                     'explicit private service fixture only')
class PushBindingScripts(unittest.TestCase):
    def exercise(self, cli):
        def call(*args, payload=None):
            return command(cli + list(args), payload)
        shas = {name: call('-x', 'SCRIPT', 'LOAD', payload=script) for name, script in SCRIPTS.items()}
        prefix = 'qa503-' + uuid.uuid4().hex
        slot, index_a, index_b = [prefix + suffix for suffix in [':slot', ':alice', ':bob']]
        field = 'getui:owned-device'
        deadline = str(int(time.time() * 1000) + 120000)
        def ident(secret):
            return hashlib.sha256(secret.encode()).hexdigest()
        def cmdkey(secret):
            return prefix + ':command:' + ident(secret)
        def register(owner, secret, expected, token='synthetic'):
            value = json.dumps(dict(tenant_id='owned-app', user_id=owner, device_id='owned-device',
                                   platform='android', provider='getui', token=token,
                                   registration_id=ident(secret), registered_at_ms=0))
            digest = hashlib.sha256((value + '|' + expected + '|' + deadline).encode()).hexdigest()
            return call('EVALSHA', shas['REGISTER'], '3', slot,
                        index_a if owner == 'alice' else index_b, cmdkey(secret),
                        expected, ident(secret), digest, deadline, field, value)
        def revoke(secret, sha=None):
            return call('EVALSHA', sha or shas['REVOKE'], '2', cmdkey(secret), prefix + ':unknown', ident(secret))
        def inspect(owner, secret):
            return call('EVALSHA', shas['INSPECT'], '2', slot, cmdkey(secret), owner, ident(secret)).splitlines()
        a, b, c, d = [x * 64 for x in 'abcd']
        self.assertEqual(register('alice', a, ''), '1')
        self.assertEqual(register('alice', a, ''), '1', 'same immutable request is idempotent')
        self.assertEqual(register('alice', a, '', token='changed'), '-1')
        self.assertEqual(inspect('alice', a)[-1], 'active')
        self.assertEqual(register('bob', b, ''), '-1', 'stale version must not switch accounts')
        self.assertEqual(register('bob', b, ident(a)), '1')
        self.assertEqual(call('HLEN', index_a), '0', 'old account index removed atomically')
        self.assertEqual(revoke(a), '1')
        self.assertEqual(inspect('bob', b)[-1], 'active', 'old revoke cannot delete new account binding')
        self.assertEqual(revoke(b), '1')
        self.assertEqual(inspect('bob', b)[-1], 'revoked')
        self.assertEqual(register('bob', c, ident(b)), '1', 'rotation from tombstone version is supported')
        self.assertEqual(revoke(c), '1')
        self.assertEqual(revoke(c), '1', 'repeated revoke is idempotent')
        self.assertEqual(revoke(d), '1')
        self.assertEqual(register('bob', d, ident(c)), '-2', 'revoke-before-late-PUT wins')
        self.assertEqual(call('HLEN', index_b), '0', 'revoked providers leave no active vendor token')
        return prefix

    def test_private_redis_preserves_atomic_lifecycle(self):
        binary = shutil.which('redis-server')
        self.assertIsNotNone(binary)
        with tempfile.TemporaryDirectory(prefix='flare-push-private-') as directory:
            os.chmod(directory, 0o700)
            socket = str(Path(directory) / 'redis.sock')
            child = subprocess.Popen([binary, '--port', '0', '--save', '', '--appendonly', 'no',
                                      '--unixsocket', socket, '--unixsocketperm', '700'],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                end = time.monotonic() + 10
                while not Path(socket).exists() and time.monotonic() < end:
                    self.assertIsNone(child.poll())
                    time.sleep(.02)
                self.exercise(['redis-cli', '--raw', '-s', socket])
            finally:
                child.terminate()
                child.wait(timeout=5)

    def test_private_dragonfly_dynamic_keys_remain_atomic(self):
        ssh = ['podman', 'machine', 'ssh', 'podman-machine-default']
        image = command(ssh + ['podman', 'inspect', 'flare-redis', '--format', '{{.Image}}'])
        self.assertRegex(image, r'^(sha256:)?[0-9a-f]{64}$')
        name = 'flare-qa503-' + uuid.uuid4().hex[:12]
        command(ssh + ['podman', 'run', '-d', '--name', name, '--pull=never', '--network=none',
                       image, '--logtostderr', '--proactor_threads=1', '--maxmemory=384mb'])
        try:
            cli = ssh + ['podman', 'exec', '-i', name, 'redis-cli', '--raw']
            end = time.monotonic() + 10
            while time.monotonic() < end:
                try:
                    if command(cli + ['PING']) == 'PONG':
                        break
                except RuntimeError:
                    time.sleep(.05)
            self.assertEqual(command(cli + ['PING']), 'PONG')
            # A counterfactual removes only the pragma: the exact legacy script
            # must still reproduce the undeclared-key failure on a known binding.
            prefix = self.exercise(cli)
            legacy = SCRIPTS['REVOKE'].replace('--!df flags=allow-undeclared-keys', '', 1)
            sha = command(cli + ['-x', 'SCRIPT', 'LOAD'], legacy)
            secret = 'c' * 64
            key = prefix + ':command:' + hashlib.sha256(secret.encode()).hexdigest()
            # c is already a tombstone; exercise's account switch/known revoke
            # above proves the fixed path. Use an independent minimal stored
            # command to reproduce the old cross-key access without user data.
            command(cli + ['SET', key, json.dumps({'slot': prefix + ':absent-slot',
                                                   'registry': prefix + ':bob', 'field': 'getui:owned-device'})])
            with self.assertRaisesRegex(RuntimeError, 'undeclared key'):
                command(cli + ['EVALSHA', sha, '2', key, prefix + ':unknown',
                               hashlib.sha256(secret.encode()).hexdigest()])
        except Exception:
            print(command(ssh + ['podman', 'logs', '--tail', '10', name])[-1800:])
            raise
        finally:
            command(ssh + ['podman', 'rm', '-f', name])


if __name__ == '__main__':
    unittest.main()
