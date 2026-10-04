"""Runner safety tests only; fake engines do not count as service acceptance."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('oss_smoke', Path(__file__).with_name('smoke_opensource_container.py'))
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)


class RunnerTests(unittest.TestCase):
    def stack(self):
        return smoke.Stack('podman', Path('/tmp/fixture'), dict(smoke.IMAGES))

    def result(self, output='', code=0):
        return subprocess.CompletedProcess([], code, output, '')

    def test_dry_run_has_no_engine_or_filesystem_mutation(self):
        with patch('sys.argv', ['smoke', '--dry-run']), patch.object(smoke, 'assert_ports_free') as ports, \
                patch.object(smoke.subprocess, 'run') as run, patch.object(smoke.tempfile, 'mkdtemp') as directory:
            self.assertEqual(smoke.main(), 0)
        ports.assert_not_called()
        run.assert_not_called()
        directory.assert_not_called()

    def test_ports_are_loopback_and_existing_listener_is_not_stopped(self):
        with patch.object(smoke.socket, 'socket') as make_socket:
            make_socket.return_value.bind.side_effect = OSError('occupied')
            with self.assertRaises(smoke.Failure):
                smoke.assert_ports_free()
            make_socket.return_value.bind.assert_called_once_with(('127.0.0.1', 8080))
            make_socket.return_value.close.assert_called_once()

    def test_private_secret_validation_does_not_follow_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'dev-secret'
            smoke.private_write(path, 'a' * 64 + '\n')
            secret, digest = smoke.read_dev_secret(path)
            self.assertEqual(secret, 'a' * 64)
            self.assertEqual(len(digest), 64)
            path.chmod(0o644)
            with self.assertRaises(smoke.Failure):
                smoke.read_dev_secret(path)
            path.chmod(0o600)
            link = Path(directory) / 'link'
            link.symlink_to(path)
            with self.assertRaises(OSError):
                smoke.read_dev_secret(link)
            with self.assertRaises(FileExistsError):
                smoke.private_write(path, 'do not overwrite')
            self.assertEqual(path.read_text(), 'a' * 64 + '\n')

    def test_environment_does_not_redirect_home_or_inherit_tokens_and_proxies(self):
        original = dict(HOME='/actual-home', CODEX_HOME='/actual-codex', TOKEN='sensitive',
                        TOKEN_SECRET='sensitive', FLARE_IM_SERVER_URL='wss://remote',
                        HTTP_PROXY='remote', FLARE_CONSOLE_SIGNING_KEY='sensitive', PATH='/bin')
        with patch.dict(os.environ, original, clear=True):
            env = smoke.client_env(Path('/tmp/fixture'), Path('/sdk'))
        self.assertEqual(env['HOME'], original['HOME'])
        self.assertEqual(env['CODEX_HOME'], original['CODEX_HOME'])
        self.assertEqual(env['FLARE_SMOKE_DATA_ROOT'], '/tmp/fixture/sdk-data')
        self.assertEqual(env['FLARE_SMOKE_LOG_DIR'], '/tmp/fixture/case-logs')
        self.assertEqual(env['CARGO_NET_OFFLINE'], 'true')
        for key in ('TOKEN', 'TOKEN_SECRET', 'FLARE_CONSOLE_SIGNING_KEY', 'FLARE_IM_SERVER_URL', 'HTTP_PROXY'):
            self.assertNotIn(key, env)

    def test_start_is_bounded_owned_no_pull_and_no_secret_in_argv(self):
        stack = self.stack()
        with patch.object(stack, 'call', return_value=self.result()) as call:
            name = stack.start('core', ['owned-private', 'owned-ingress'], ['--env-file', '/fixture/core.env'])
        args = call.call_args.args
        self.assertIn('--pull=never', args)
        self.assertIn(f'{smoke.LABEL}={stack.owner}', args)
        self.assertIn('--memory-swap', args)
        self.assertIn('--pids-limit', args)
        self.assertIn(name, stack.containers)
        self.assertNotIn('--privileged', args)
        self.assertNotIn('--network=host', args)

    def test_cleanup_refuses_foreign_label_and_engine_error(self):
        stack = self.stack()
        stack.containers = ['foreign', 'engine-failed', 'already-absent', 'owned']
        calls = []

        def call(*args, **kwargs):
            calls.append(args)
            if args[1] == 'inspect':
                return {'owned': self.result(stack.owner), 'foreign': self.result('someone-else'),
                        'engine-failed': self.result(code=125), 'already-absent': self.result(code=125)}[args[-1]]
            if args[1] == 'exists':
                return self.result(code=1 if args[-1] == 'already-absent' else 125)
            return self.result()

        with patch.object(stack, 'call', side_effect=call):
            self.assertEqual(stack.cleanup(), ['engine-failed', 'foreign'])
        self.assertEqual([args for args in calls if args[1] == 'rm'], [('container', 'rm', '-f', '-v', 'owned')])

    def test_failed_create_is_still_considered_for_owned_cleanup(self):
        stack = self.stack()
        with patch.object(stack, 'call', side_effect=smoke.Failure('unknown outcome')):
            with self.assertRaises(smoke.Failure):
                stack.network('owned-private', True)
        self.assertEqual(stack.networks, ['owned-private'])

    def test_redaction_removes_encoded_keys_credentials_and_tokens(self):
        stack = self.stack()
        stack.sensitive = ['long-private-key', 'pg-password']
        result = stack.redact('long-private-key postgres://u:pg-password@db/x redis://:pw@r/0 nats://u:pw@n '
                              'eyJheader.payload.signature')
        for forbidden in ('long-private-key', 'pg-password', '://', 'eyJheader'):
            self.assertNotIn(forbidden, result)

    def test_only_private_files_are_created(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'report.json'
            smoke.private_write(path, json.dumps({'accepted': False}))
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_cleanup_only_removes_explicit_fixture_secrets(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ('core.env', 'cell-private.pem', 'report.json', 'unrelated.txt'):
                smoke.private_write(root / name, 'fixture')
            smoke.remove_secret_files(root)
            self.assertFalse((root / 'core.env').exists())
            self.assertFalse((root / 'cell-private.pem').exists())
            self.assertTrue((root / 'report.json').exists())
            self.assertTrue((root / 'unrelated.txt').exists())


if __name__ == '__main__':
    unittest.main()
