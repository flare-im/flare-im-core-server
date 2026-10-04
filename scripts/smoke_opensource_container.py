#!/usr/bin/env python3
"""SEC-03 isolated, real Core + PostgreSQL/Redis/NATS/RustFS smoke (no Social/Control).

Requires cached Core/migrator images and the sibling SDK. Never pulls images,
restarts a VM, overwrites dev secrets or joins the installed p0e2e networks.
Only owned containers/networks are removed, including on failed/timeout runs.
SDK clients execute on the host; all server services and infrastructure are real
containers. This is NOT the nine-container SaaS stack or a complete SDK assertion suite.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import socket
import stat
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
LABEL = 'io.flare.opensource-smoke'
IMAGES = {
    'core': 'localhost/flare/core:w14',
    'migrate': 'localhost/flare/core-migrate:w14',
    'postgres': 'docker.io/timescale/timescaledb:latest-pg16',
    'redis': 'docker.io/library/redis:7.2-alpine',
    'nats': 'docker.io/library/nats:2.10-alpine',
    'rustfs': 'docker.io/rustfs/rustfs:latest',
}
CASES = ['e2e_message_ops', 'e2e_event_observer', 'e2e_full_event_observer',
         'e2e_full_event_ops', 'e2e_unread_regression', 'e2ee_demo']


class Failure(Exception):
    pass


def private_write(path, content):
    with os.fdopen(os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), 'w') as stream:
        stream.write(content)


def remove_secret_files(fixture):
    for name in ('cell-private.pem', 'cell-public.pem', 'core.env', 'postgres.env',
                 'migrate.env', 'nats.env', 'rustfs.env', 'redis.conf', 'nats.conf'):
        (fixture / name).unlink(missing_ok=True)


def read_dev_secret(path):
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), 'rb') as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or not 32 <= info.st_size <= 4096:
            raise Failure('dev token secret must be a private regular file (32..4096 bytes)')
        raw = stream.read(4097)
    secret = raw.decode().strip()
    if not re.fullmatch(r'[A-Za-z0-9_+/=-]{32,4096}', secret):
        raise Failure('invalid dev secret encoding')
    return secret, hashlib.sha256(raw).hexdigest()


def assert_ports_free():
    sockets = []
    try:
        for port in (8080, 50050):
            item = socket.socket()
            sockets.append(item)
            item.bind(('127.0.0.1', port))
    except OSError as error:
        raise Failure('SDK loopback port 8080/50050 is occupied; no process was stopped') from error
    finally:
        for item in sockets:
            item.close()


def client_env(fixture, sdk):
    # No inherited gateway URLs, user tokens, custom signing credentials or proxy
    # settings; retain only toolchain options. Never repurpose HOME/CODEX_HOME.
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(('FLARE_', 'ACCESS_GATEWAY_', 'PUSH_', 'SIGNALING_', 'CORE_'))
           and key not in ('TOKEN', 'TOKEN_SECRET', 'HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY',
                           'http_proxy', 'https_proxy', 'all_proxy')}
    env.update(FLARE_SDK_ROOT=str(sdk), FLARE_SMOKE_DATA_ROOT=str(fixture / 'sdk-data'),
               FLARE_SMOKE_LOG_DIR=str(fixture / 'case-logs'), CARGO_NET_OFFLINE='true',
               FLARE_SMOKE_WS_URL='ws://127.0.0.1:8080', FLARE_SMOKE_HTTP_URL='http://127.0.0.1:50050',
               FLARE_SMOKE_SFU_ABSENT='true',
               RUST_LOG='warn', NO_PROXY='localhost,127.0.0.1,::1', no_proxy='localhost,127.0.0.1,::1')
    return env


class Stack:
    def __init__(self, runtime, fixture, images):
        self.runtime, self.fixture, self.images = runtime, fixture, images
        self.owner = 'flare-oss-' + secrets.token_hex(6)
        self.containers, self.networks = [], []
        self.sensitive = []

    def redact(self, content):
        for secret in sorted(self.sensitive, key=len, reverse=True):
            content = content.replace(secret, '[REDACTED]')
        content = re.sub(r'(?:postgres(?:ql)?|redis|nats)://[^\s"\x1b]+', '[REDACTED_URI]', content)
        return re.sub(r'eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+', '[REDACTED_TOKEN]', content)

    def call(self, *args, timeout=30, input=None, check=True):
        try:
            result = subprocess.run([self.runtime, *args], text=True, capture_output=True,
                                    input=input, timeout=timeout)
        except (OSError, subprocess.TimeoutExpired) as error:
            raise Failure('container command failed or timed out') from error
        if check and result.returncode:
            # Never echo argv/env or engine errors which may contain secrets.
            private_write(self.fixture / ('engine-error-' + secrets.token_hex(6) + '.log'),
                          self.redact(result.stdout + result.stderr))
            raise Failure('container command returned nonzero: ' + ' '.join(args[:2]))
        return result

    def network(self, name, internal):
        self.networks.append(name)  # also handle an unknown create outcome
        args = ['network', 'create', '--label', f'{LABEL}={self.owner}']
        if internal:
            args.append('--internal')
        self.call(*args, name)

    def start(self, role, networks, extra=(), command=()):
        name = self.owner + '-' + role
        self.containers.append(name)
        args = ['run', '-d', '--pull=never', '--name', name, '--label', f'{LABEL}={self.owner}',
                '--memory', '1536m' if role == 'core' else '384m', '--memory-swap',
                '1536m' if role == 'core' else '384m', '--pids-limit', '512', '--cpus', '2']
        for net in networks:
            args += ['--network', net]
        self.call(*args, *extra, self.images[role], *command, timeout=90)
        return name

    def wait(self, name, command, timeout=90):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.call('exec', name, *command, check=False).returncode == 0:
                return
            if self.call('inspect', '--format', '{{.State.Running}}', name).stdout.strip() != 'true':
                raise Failure('fixture service exited: ' + name)
            time.sleep(1)
        raise Failure('fixture readiness deadline: ' + name)

    def cleanup(self):
        errors = []
        for kind, names in [('container', self.containers), ('network', self.networks)]:
            for name in reversed(names):
                label = self.call(kind, 'inspect', '--format', '{{index .Labels "' + LABEL + '"}}'
                                  if kind == 'network' else '{{index .Config.Labels "' + LABEL + '"}}',
                                  name, check=False)
                if label.returncode:
                    # A failed inspect is not proof of absence (engine down, permissions).
                    exists = self.call(kind, 'exists', name, check=False)
                    if exists.returncode != 1:
                        errors.append(name)
                    continue
                if label.stdout.strip() != self.owner:
                    errors.append(name)
                    continue
                args = [kind, 'rm'] + (['-f', '-v'] if kind == 'container' else []) + [name]
                if self.call(*args, timeout=60, check=False).returncode:
                    errors.append(name)
        return errors

    def evidence(self):
        for name in self.containers:
            result = self.call('logs', name, check=False)
            path = self.fixture / (name + '.log')
            private_write(path, self.redact(result.stdout + result.stderr))


def run_smoke(stack, sdk):
    output = stack.fixture / 'smoke.out'
    env = client_env(stack.fixture, sdk)
    # Overall watchdog is portable even when macOS has no timeout/gtimeout.
    with os.fdopen(os.open(output, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), 'w') as stream:
        process = subprocess.Popen(['bash', str(ROOT / 'scripts/smoke_opensource.sh')],
                                   cwd=ROOT, env=env, stdout=stream, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        try:
            code = process.wait(timeout=2100)
        except BaseException:
            # Allow the portable per-case timer to remove its command group.
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5)
            raise
    for path in [output, *(stack.fixture / 'case-logs').glob('*.log')]:
        text = stack.redact(path.read_text())
        path.write_text(text)
        path.chmod(0o600)
    if code != 0 or '6/6 passed' not in output.read_text():
        raise Failure('open-source smoke failed; inspect protected per-case logs')


def execute(stack, sdk, token):
    private = stack.fixture / 'cell-private.pem'
    public = stack.fixture / 'cell-public.pem'
    subprocess.run(['openssl', 'genpkey', '-algorithm', 'Ed25519', '-out', str(private)],
                   check=True, capture_output=True)
    subprocess.run(['openssl', 'pkey', '-in', str(private), '-pubout', '-out', str(public)],
                   check=True, capture_output=True)
    private.chmod(0o600)
    public.chmod(0o600)
    pk, pub = [base64.b64encode(p.read_bytes()).decode() for p in (private, public)]
    pg_pw, run_pw, redis_pw, nats_pw, s3_pw = [secrets.token_hex(32) for _ in range(5)]
    stack.sensitive = [token, pk, pg_pw, run_pw, redis_pw, nats_pw, s3_pw]
    private_net, public_net, broker_net = [stack.owner + '-' + n for n in ('private', 'ingress', 'broker')]
    for name, internal in [(private_net, True), (public_net, False), (broker_net, True)]:
        stack.network(name, internal)
    files = {}

    def env_file(role, entries):
        path = stack.fixture / (role + '.env')
        private_write(path, ''.join(f'{key}={value}\n' for key, value in entries.items()))
        files[role] = path
        return ['--env-file', str(path)]

    pg = stack.start('postgres', [private_net], env_file('postgres', {
        'POSTGRES_USER': 'fixture', 'POSTGRES_DB': 'flare_core', 'POSTGRES_PASSWORD': pg_pw}))
    # The entrypoint's temporary Unix-only init server is NOT network readiness.
    stack.wait(pg, ['pg_isready', '-q', '-h', '127.0.0.1', '-U', 'fixture', '-d', 'flare_core'])
    migration_name = stack.owner + '-migrate'
    stack.containers.append(migration_name)
    stack.call('run', '--pull=never', '--name', migration_name, '--label', f'{LABEL}={stack.owner}',
               '--network', private_net, *env_file('migrate', {
                   'DATABASE_URL': f'postgres://fixture:{pg_pw}@{pg}:5432/flare_core'}),
               stack.images['migrate'], 'run', timeout=120)
    # A dedicated DML role, never the migrator's owner role; the ledger stays read-only.
    grants = (f"CREATE ROLE core_runtime LOGIN PASSWORD '{run_pw}'; "
              "GRANT CONNECT ON DATABASE flare_core TO core_runtime; "
              "GRANT USAGE ON SCHEMA public TO core_runtime; "
              "GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO core_runtime; "
              "GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA public TO core_runtime; "
              "REVOKE INSERT,UPDATE,DELETE ON flare_schema_migrations FROM core_runtime;")
    stack.call('exec', '-i', pg, 'psql', '-X', '-q', '-v', 'ON_ERROR_STOP=1', '-U', 'fixture',
               '-d', 'flare_core', input=grants)
    redis_config = stack.fixture / 'redis.conf'
    private_write(redis_config, f'bind 0.0.0.0\nrequirepass {redis_pw}\nappendonly no\n')
    redis = stack.start('redis', [private_net], ['--entrypoint', 'redis-server',
                        '-v', f'{redis_config}:/etc/redis.conf:ro'], ['/etc/redis.conf'])
    nats_config = stack.fixture / 'nats.conf'
    private_write(nats_config, 'port: 4222\nhttp: 8222\njetstream {store_dir: "/data"}\n'
                  'authorization {users: [{user: "core", password: $NATS_CORE_PASSWORD}]}\n')
    nats = stack.start('nats', [broker_net], [*env_file('nats', {'NATS_CORE_PASSWORD': nats_pw}),
                       '-v', f'{nats_config}:/etc/nats/nats.conf:ro'], ['-c', '/etc/nats/nats.conf'])
    stack.wait(nats, ['wget', '-q', '-O', '/dev/null', 'http://127.0.0.1:8222/healthz'])
    rustfs = stack.start('rustfs', [private_net], env_file('rustfs', {
        'RUSTFS_ACCESS_KEY': 'smokeaccess', 'RUSTFS_SECRET_KEY': s3_pw,
        'RUSTFS_ADDRESS': ':9000', 'RUSTFS_CONSOLE_ENABLE': 'false',
        'RUSTFS_HEALTH_ENDPOINT_ENABLE': 'true'}), ['rustfs', '/data'])
    stack.wait(rustfs, ['curl', '-fsS', '--max-time', '3', 'http://127.0.0.1:9000/health/ready'])
    core = stack.start('core', [private_net, public_net, broker_net], [
        *env_file('core', {
            'FLARE_CONFIG_PATH': '/etc/flare/config', 'FLARE_REGISTRY_MODE': 'static',
            'FLARE_MQ_DEFAULT_BACKEND': 'nats', 'FLARE_POSTGRES_URL': f'postgres://core_runtime:{run_pw}@{pg}:5432/flare_core',
            'FLARE_REDIS_URL_BASE': f'redis://:{redis_pw}@{redis}:6379',
            'FLARE_NATS_URL': f'nats://core:{nats_pw}@{nats}:4222', 'SERVICE_ADVERTISE_HOST': '127.0.0.1',
            'FLARE_INTERNAL_CTX_PRIVATE_KEY': pk, 'FLARE_INTERNAL_CTX_PUBLIC_KEY': pub,
            'FLARE_API_GATEWAY_TOKEN_SECRET': token, 'ACCESS_GATEWAY_TOKEN_SECRET': token,
            'MESSAGE_INGEST_HOOKS_CONFIG': '/etc/flare/config/hooks.core.toml',
            'MESSAGE_INGEST_CAPABILITY_HOOKS_AUTO': '0',
            'FLARE_S3_ENDPOINT': f'http://{rustfs}:9000', 'FLARE_S3_ACCESS_KEY': 'smokeaccess',
            'FLARE_S3_SECRET_KEY': s3_pw,
            'PUSH_SERVER_DELIVERY_READY': 'true', 'PUSH_WORKER_DELIVERY_READY': 'true',
            'PUSH_SERVER_EVENT_PROVENANCE_MODE': 'broker-isolated',
            'PUSH_PROXY_EVENT_PROVENANCE_MODE': 'broker-isolated',
            'PUSH_WORKER_EVENT_PROVENANCE_MODE': 'broker-isolated',
            'PUSH_WORKER_OFFLINE_DELIVERY_BACKEND': 'disabled',
            'STORAGE_POSTGRES_MAX_CONNECTIONS': '4', 'CONVERSATION_POSTGRES_MAX_CONNECTIONS': '4',
            'CAPABILITY_POSTGRES_MAX_CONNECTIONS': '4', 'RUST_LOG': 'info',
        }), '-p', '127.0.0.1:8080:60051', '-p', '127.0.0.1:50050:50050',
        # Podman's CLI splits comma-delimited sysctl arguments. A contiguous
        # range avoids that ambiguity, in THIS container's namespace only.
        '--sysctl', 'net.ipv4.ip_local_reserved_ports=50050-60084'])
    stack.wait(core, ['flare-supervise', 'health'], timeout=120)
    logged = stack.call('logs', core)
    startup = logged.stdout + logged.stderr
    if 'discovered_plugin_count=0' not in startup or 'hooks loaded: 0' not in startup:
        raise Failure('Core-only fixture has not proven empty hooks and zero discovered plugins')
    print('Real isolated Core stack ready; running smoke_opensource.sh', flush=True)
    run_smoke(stack, sdk)
    # The demo also exercises client cache; independently prove real durable writes.
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        result = stack.call('exec', pg, 'psql', '-X', '-qAt', '-U', 'fixture', '-d', 'flare_core', '-c',
                            "SELECT json_build_object('e2ee_rows',count(*),'ciphertext_only',"
                            "COALESCE(bool_and(message_type=20 AND octet_length(content)>0 AND "
                            "position(convert_to('见面地点改到中山路 42 号','UTF8') IN content)=0 AND "
                            "position('见面地点改到中山路 42 号' IN COALESCE(extra::text,'')||COALESCE(extensions::text,''))=0),false),"
                            "'sender_count',(SELECT count(DISTINCT sender_id) FROM messages WHERE tenant_id='0' AND "
                            "sender_id IN ('message_ops','event_observer','full_event_ops','e2ee_alice'))) "
                            "FROM messages WHERE tenant_id='0' AND sender_id='e2ee_alice';")
        stored = json.loads(result.stdout)
        stack.db_countercheck = stored
        if stored['e2ee_rows'] > 0 and stored['ciphertext_only'] and stored['sender_count'] == 4:
            stack.wait(core, ['flare-supervise', 'health'], timeout=10)
            return {'sdk_cases': CASES, 'sdk_smoke_exit': 0, 'server_e2ee': stored,
                    'optional_sfu': 'not installed; not a Core acceptance requirement'}
        time.sleep(0.5)
    raise Failure('smoke exited successfully but durable message/ciphertext countercheck did not pass')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime', choices=['podman'], default='podman')
    parser.add_argument('--core-image', default=IMAGES['core'])
    parser.add_argument('--migrate-image', default=IMAGES['migrate'])
    parser.add_argument('--sdk-root', type=Path, default=ROOT.parent / 'flare-im-core-sdk')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    images = dict(IMAGES, core=args.core_image, migrate=args.migrate_image)
    if args.dry_run:
        print(json.dumps({'cached_images_only': images, 'server_roles': list(images),
                          'publish': ['127.0.0.1:8080:60051', '127.0.0.1:50050:50050'],
                          'shared_stack_mutations': False, 'SDK_clients': 'host',
                          'cleanup': 'exact names + ownership label, including failures'}))
        return 0
    assert_ports_free()
    sdk = args.sdk_root.resolve()
    if not (sdk / 'Cargo.toml').is_file() or not (sdk / 'examples/common/smoke_config.rs').is_file():
        raise Failure('SDK smoke storage override is required before isolated acceptance')
    secret_path = ROOT / 'logs/.dev-token-secret'
    token, before = read_dev_secret(secret_path)
    # /private/tmp is shared with the local Podman VM; macOS TMPDIR often is not.
    fixture = Path(tempfile.mkdtemp(prefix='flare-oss-smoke-', dir='/private/tmp'))
    for name in ('sdk-data', 'case-logs'):
        (fixture / name).mkdir(mode=0o700)
    stack = Stack(args.runtime, fixture, images)
    report = {'fixture': str(fixture), 'server_containerized': True, 'sdk_host_processes': True,
              'commercial_services': [], 'accepted': False}
    source_files = [ROOT / 'scripts/smoke_opensource.sh',
                    ROOT / 'scripts/smoke_opensource_container.py',
                    ROOT / 'scripts/lib/timeout_process.py',
                    sdk / 'examples/common/smoke_config.rs',
                    sdk / 'examples/common/send_confirmation.rs',
                    *[sdk / ('examples/' + case + '.rs') for case in CASES]]
    report['source_sha256'] = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in source_files}
    try:
        # Resolve tags once to immutable IDs before starting; never pull.
        for role, reference in images.items():
            image_id = stack.call('image', 'inspect', '--format', '{{.Id}}', reference).stdout.strip()
            if not re.fullmatch(r'(sha256:)?[0-9a-f]{64}', image_id):
                raise Failure('invalid cached image ID')
            stack.images[role] = image_id
        report['image_ids'] = dict(stack.images)
        report.update(execute(stack, sdk, token))
        report['accepted'] = True
    except (Failure, OSError, ValueError, subprocess.SubprocessError, KeyboardInterrupt) as error:
        report['failure'] = str(error) if isinstance(error, Failure) else type(error).__name__
    finally:
        if hasattr(stack, 'db_countercheck'):
            report['db_countercheck'] = stack.db_countercheck
        try:
            stack.evidence()
        except Failure:
            report['evidence_error'] = True
        try:
            cleanup_errors = stack.cleanup()
        except Failure:
            cleanup_errors = ['container runtime unavailable; retain exact owner names for manual cleanup']
        report['owner'] = stack.owner
        report['cleanup_errors'] = cleanup_errors
        try:
            report['dev_secret_unchanged'] = read_dev_secret(secret_path)[1] == before
        except (Failure, OSError):
            report['dev_secret_unchanged'] = False
        report['secret_files_removed'] = False
        if not cleanup_errors:
            try:
                remove_secret_files(fixture)
                report['secret_files_removed'] = True
            except OSError:
                report['cleanup_errors'].append('private fixture material could not be removed')
        report['accepted'] = (report['accepted'] and not cleanup_errors and report['dev_secret_unchanged']
                              and not report.get('evidence_error'))
        private_write(fixture / 'report.json', json.dumps(report, indent=2) + '\n')
        # Persist ONLY sanitized evidence, never env/PEM material or SDK databases.
        persistent = ROOT / 'logs/opensource-smoke' / stack.owner
        persistent.mkdir(parents=True, mode=0o700)
        for path in [fixture / 'report.json', *fixture.glob('*.log'), *fixture.glob('*.out'),
                     *(fixture / 'case-logs').glob('*.log')]:
            destination = persistent / path.name
            shutil.copyfile(path, destination)
            destination.chmod(0o600)
        report['persistent_evidence'] = str(persistent)
    print(json.dumps(report), flush=True)
    return 0 if report['accepted'] else 1


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Failure as error:
        print('FAIL: ' + str(error))
        raise SystemExit(1)
