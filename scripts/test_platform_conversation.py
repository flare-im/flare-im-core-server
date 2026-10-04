#!/usr/bin/env python3
"""Owned native API Gateway -> production Conversation -> PostgreSQL regression.

Uses the verbatim three relevant baseline DDL statements + credential migration.
Projection/business rows are explicit SQL fixtures, NOT Control delivery, a migration
acceptance, container full-stack, global revocation SLO or MIG-05 acceptance.
The consolidated public contract is owned by Social in this workspace.
"""
import argparse
import base64
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import secrets
import signal
import socket
import subprocess
import tempfile
import time
from urllib.parse import quote

from jsonschema import Draft4Validator
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT4

ROOT = Path(__file__).resolve().parents[1]
BIN = ROOT.parent / 'target/debug'


def checked(args, **kwargs):
    result = subprocess.run(args, capture_output=True, timeout=30, **kwargs)
    if result.returncode:
        raise RuntimeError(f'fixture command failed: {Path(args[0]).name}')
    return result.stdout


def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def b64(raw):
    return base64.urlsafe_b64encode(raw).decode().rstrip('=')


def wait_for(predicate, label, processes=(), seconds=10):
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        if any(process.poll() is not None for process in processes):
            raise RuntimeError(f'{label}: owned service exited')
        if predicate():
            return
        time.sleep(.05)
    raise RuntimeError(f'{label}: timed out')


def listening(number):
    try:
        with socket.create_connection(('127.0.0.1', number), timeout=.2):
            return True
    except OSError:
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--contract', type=Path, default=ROOT.parent / 'flare-social/contracts/platform/platform-v1.openapi.json')
    parser.add_argument('--via-social', action='store_true', help='real Social Gateway -> real Core Gateway -> real Conversation, separate owned PG databases')
    args = parser.parse_args()
    spec = json.loads(args.contract.read_text())
    operation = '/platform/v1/apps/{app_id}/conversations/{conversation_id}/participants'
    if spec['paths'][operation]['get']['x-flare-scope'] != 'platform.conversations.read':
        raise RuntimeError('wrong authoritative contract')
    registry = Registry().with_resource('', Resource.from_contents(spec, default_specification=DRAFT4))
    validators = {name: Draft4Validator(schema, registry=registry) for name, schema in spec['components']['schemas'].items()}
    host = os.environ.get('PGHOST', '/tmp')
    if (host not in ('/tmp', 'localhost', '127.0.0.1', '::1')
            or any(os.environ.get(k) for k in ('PGSERVICE', 'PGSERVICEFILE', 'PGHOSTADDR'))):
        raise RuntimeError('requires explicit local PostgreSQL, no remote/service connection')
    pg_port = int(os.environ.get('PGPORT', '5432'))
    if not 0 < pg_port <= 65535:
        raise RuntimeError('invalid local PostgreSQL port')
    for name in ('flare-api-gateway', 'flare-conversation'):
        if not (BIN / name).is_file():
            raise RuntimeError(f'build production binary first: {name}')
    if args.via_social and not (BIN / 'flare-social-gateway').is_file():
        raise RuntimeError('build production Social Gateway first')
    db = 'flare_platform_core_test_' + secrets.token_hex(6)
    address = ['-h', host, '-p', str(pg_port)]
    created = False
    social_db = 'flare_platform_social_bridge_test_' + secrets.token_hex(6)
    social_created = False
    processes, handles = [], []
    original_umask = os.umask(0o077)
    try:
        checked(['createdb', *address, db])
        created = True
        def sql(statement):
            return checked(['psql', *address, '-X', '-qAt', '-d', db, '-v', 'ON_ERROR_STOP=1', '-f', '-'], input=statement.encode()).decode().strip()
        baseline = (ROOT / 'migrations/0001_baseline.sql').read_text()
        for table in ('tenants', 'conversations', 'conversation_participants'):
            statements = re.findall(rf'^CREATE TABLE {table} \(\n.*?^\);', baseline, flags=re.M | re.S)
            if len(statements) != 1:
                raise RuntimeError('baseline fixture shape changed')
            sql(statements[0])
        sql('CREATE INDEX ON conversation_participants(tenant_id,conversation_id,joined_at,user_id);')
        checked(['psql', *address, '-X', '-d', db, '-v', 'ON_ERROR_STOP=1', '-1', '-f', str(ROOT / 'migrations/0010_20261004_credential_projection.sql')])
        user = sql('SELECT current_user;')
        password = os.environ.get('PGPASSWORD')
        credential = quote(user, safe='') + (':' + quote(password, safe='') if password else '')
        url = f'postgresql://{credential}@localhost:{pg_port}/{db}?host={quote(host, safe="")}'
        social_url = f'postgresql://{credential}@localhost:{pg_port}/{social_db}?host={quote(host, safe="")}'
        def social_sql(statement):
            return checked(['psql', *address, '-X', '-qAt', '-d', social_db, '-v', 'ON_ERROR_STOP=1', '-f', '-'], input=statement.encode()).decode().strip()
        if args.via_social:
            checked(['createdb', *address, social_db])
            social_created = True
            for migration in sorted((ROOT.parent / 'flare-social/migrations').glob('*.sql')):
                checked(['psql', *address, '-X', '-d', social_db, '-v', 'ON_ERROR_STOP=1', '-1', '-f', str(migration)])
            social_sql("INSERT INTO flare_tenants(tenant_id,name,status,version) VALUES('app-a','native-a',1,1),('app-b','native-b',1,1); INSERT INTO flare_credential_projection_versions(app_id,version) VALUES('app-a',1),('app-b',1);")
        sql("""
          INSERT INTO tenants(tenant_id,name,status,version) VALUES('app-a','native-a','active',1),('app-b','native-b','active',1);
          INSERT INTO conversations(tenant_id,conversation_id) VALUES('app-a','same-conversation'),('app-b','same-conversation');
          INSERT INTO conversation_participants(tenant_id,conversation_id,user_id,roles,joined_at,attributes,muted,pinned)
            VALUES('app-a','same-conversation','a1',ARRAY['owner'],'2026-01-01','{"private":"hidden"}',true,true),
                  ('app-a','same-conversation','a2',ARRAY['member'],'2026-01-01','{}',false,false),
                  ('app-a','same-conversation','a3',ARRAY['admin'],'2026-01-02','{}',false,false),
                  ('app-b','same-conversation','b1',ARRAY['owner'],'2026-01-01','{}',false,false),
                  ('app-a','same-conversation','removed',ARRAY['member'],'2026-01-01','{}',false,false),
                  ('app-a','same-conversation','quit',ARRAY['member'],'2026-01-01','{}',false,false);
          UPDATE conversation_participants SET is_deleted=true WHERE user_id='removed';
          UPDATE conversation_participants SET quit_at=now() WHERE user_id='quit';
          INSERT INTO credential_projection_versions(app_id,version) VALUES('app-a',1),('app-b',1);
        """)
        pepper = secrets.token_bytes(32)
        materials = [b64(pepper)] + ([password] if password else [])
        def key(app='app-a', **changes):
            selector = base64.b32encode(secrets.token_bytes(8)).decode()[:12]
            secret = secrets.token_bytes(32)
            token = f'flr_live_{selector}_{b64(secret)}'
            record = dict(key_id=selector, service_account_id=f'native-sa-{selector}', mode=1,
                          hash=list(hashlib.sha256(pepper + secret).digest()), pepper_fingerprint=list(hashlib.sha256(pepper).digest()),
                          scopes=['platform.conversations.read'], expires_at_ms=0, ip_allow=['127.0.0.1/32'], revoked=False, service_account_enabled=True)
            record.update(changes)
            payload = json.dumps(record, separators=(',', ':'))
            sql(f"INSERT INTO credential_keys VALUES('{selector}','{app}','{payload}'::jsonb);")
            if args.via_social:
                social_sql(f"INSERT INTO flare_credential_keys VALUES('{selector}','{app}','{payload}'::jsonb);")
            materials.extend([token, b64(secret), payload, hashlib.sha256(pepper + secret).hexdigest(), json.dumps(record['hash']), json.dumps(record['hash'], separators=(',', ':'))])
            return token
        token, other = key(), key('app-b')
        wrong_scope, wrong_ip, expired, disabled_sa = key(scopes=['platform.users.read']), key(ip_allow=['192.0.2.1/32']), key(expires_at_ms=1), key(service_account_enabled=False)
        with tempfile.TemporaryDirectory(prefix='flare-platform-core-') as temp:
            cwd = Path(temp)
            private = checked(['openssl', 'genpkey', '-algorithm', 'ED25519']).decode()
            public = checked(['openssl', 'pkey', '-pubout'], input=private.encode()).decode()
            materials.extend([private, url])
            conversation_port, gateway_port = port(), port()
            core_gateway_port = gateway_port
            social_gateway_port = port() if args.via_social else None
            if conversation_port == gateway_port:
                raise RuntimeError('owned port collision: retry fixture')
            if args.via_social and social_gateway_port in (conversation_port, gateway_port):
                raise RuntimeError('owned Social port collision: retry fixture')
            config = cwd / 'config'
            config.mkdir()
            # Runtime-only, 0600 fixture, no secrets in config or inherited defaults.
            (config / 'base.toml').write_text(f'''[service]
name = "platform-fixture"
version = "0.1.0"
[server]
address = "127.0.0.1"
port = {gateway_port}
[registry]
registry_type = "consul"
endpoints = []
namespace = "platform-fixture"
ttl = 30
[logging]
level = "info"
[services.api_gateway.server]
address = "127.0.0.1"
port = {gateway_port}
[services.conversation.server]
address = "127.0.0.1"
port = {conversation_port}
''')
            env = dict(os.environ)
            for name in list(env):
                if name.startswith(('SOCIAL_', 'FLARE_', 'IM_CONNECT_', 'CONTROL_', 'PG', 'CONVERSATION_', 'STORAGE_', 'MQ_', 'OTEL_')):
                    env.pop(name)
            env.update(FLARE_CONFIG_PATH=str(config), FLARE_REGISTRY_MODE='static', CONVERSATION_POSTGRES_URL=url,
                       CONVERSATION_REDIS_URL='redis://127.0.0.1:1/0', CONVERSATION_STORAGE_READER_SERVICE_URL='http://127.0.0.1:1',
                       CONVERSATION_LISTEN=f'127.0.0.1:{conversation_port}',
                       FLARE_API_GATEWAY_TOKEN_SECRET=secrets.token_urlsafe(48), FLARE_INTERNAL_CTX_PRIVATE_KEY=private,
                       FLARE_INTERNAL_CTX_PUBLIC_KEY=public, FLARE_REQUIRE_SIGNED_READ_CTX='true',
                       FLARE_API_GATEWAY_TENANT_RUNTIME_POSTGRES_URL=url, FLARE_API_GATEWAY_TENANT_POLICY='strict',
                       FLARE_API_GATEWAY_GRPC_CONVERSATION_SERVICE_URL=f'http://127.0.0.1:{conversation_port}',
                       FLARE_PLATFORM_API_ENABLED='true', FLARE_API_KEY_PEPPER=b64(pepper),
                       FLARE_SNOWFLAKE_WORKER_ID='29', FLARE_SNOWFLAKE_DATACENTER_ID='29', RUST_LOG='info')
            materials.append(env['FLARE_API_GATEWAY_TOKEN_SECRET'])
            if args.via_social:
                env.update(SOCIAL_POSTGRES_URL=social_url, SOCIAL_REGISTRY_MODE='static',
                           SOCIAL_JWT_SECRET=secrets.token_urlsafe(48), FLARE_INTERNAL_CTX_SIGNING_KEY=private,
                           FLARE_SOCIAL_GATEWAY_ADDRESS=f'127.0.0.1:{social_gateway_port}',
                           FLARE_PLATFORM_CORE_API_URL=f'http://127.0.0.1:{core_gateway_port}')
                materials.extend([social_url, env['SOCIAL_JWT_SECRET']])
            def start(name):
                log = (cwd / f'{name}.log').open('ab')
                handles.append(log)
                child_env = dict(env)
                if args.via_social and name == 'flare-api-gateway':
                    child_env['FLARE_PLATFORM_TRUSTED_PROXY_CIDRS'] = '127.0.0.1/32'
                process = subprocess.Popen([str(BIN / name)], cwd=cwd, env=child_env, stdout=log, stderr=log, start_new_session=True)
                processes.append(process)
                return process
            def stop(process):
                os.killpg(process.pid, signal.SIGTERM)
                process.wait(timeout=10)
            conversation_process = start('flare-conversation')
            wait_for(lambda: listening(conversation_port), 'Conversation ready', processes)
            entry_binary = 'flare-social-gateway' if args.via_social else 'flare-api-gateway'
            core_gateway = None
            if args.via_social:
                core_gateway = start('flare-api-gateway')
                wait_for(lambda: listening(core_gateway_port), 'Core Gateway ready', processes)
                gateway_port = social_gateway_port
            gateway = start(entry_binary)
            try:
                wait_for(lambda: listening(gateway_port), 'Gateway ready', processes)
            except RuntimeError:
                raw = (cwd / f'{entry_binary}.log').read_text(errors='replace')
                markers = ('fallback', 'Connecting to gRPC', 'gRPC clients initialized', 'API tenant admission initialized', 'Gateway HTTP service is listening')
                print('Owned Gateway startup diagnostics: ' + ', '.join(f'{marker}={marker in raw}' for marker in markers))
                listen_lines = [line for line in raw.splitlines() if 'HTTP listen address resolved' in line or 'Gateway HTTP service is listening' in line]
                print('Owned Gateway listen addresses: ' + ','.join(re.findall(r'(?:127\.0\.0\.1|0\.0\.0\.0):[0-9]+', '\n'.join(listen_lines))))
                raise
            count = 0
            def call(path, value=None, status=200, code=None, method='GET', headers=None, body=None):
                nonlocal count
                request_headers = dict(headers or {})
                if value:
                    request_headers['Authorization'] = f'Bearer {value}'
                connection = http.client.HTTPConnection('127.0.0.1', gateway_port, timeout=7)
                try:
                    connection.request(method, path, body=body, headers=request_headers)
                    response = connection.getresponse()
                    raw = response.read(1024 * 1024 + 1)
                    if response.status != status:
                        raise RuntimeError(f'Platform check {count+1}: status {response.status}, expected {status}')
                    if response.getheader('Cache-Control') != 'no-store' or response.getheader('Pragma') != 'no-cache':
                        raise RuntimeError('Platform cache boundary failed')
                    data = json.loads(raw)
                    if any(material.encode() in raw for material in materials):
                        raise RuntimeError('secret material reached Platform response')
                    if list(validators['ParticipantsResponse' if status == 200 else 'Error'].iter_errors(data)):
                        raise RuntimeError('Platform response contract mismatch')
                    if code and data != {'error': {'code': code}}:
                        raise RuntimeError('Platform redacted error boundary failed')
                    count += 1
                    return data['data'] if status == 200 else data
                finally:
                    connection.close()
            def client_probe(value=None):
                connection = http.client.HTTPConnection('127.0.0.1', core_gateway_port, timeout=5)
                try:
                    connection.request('GET', '/api/v1/conversations' if value else '/health',
                                       headers={'Authorization':f'Bearer {value}'} if value else {})
                    response = connection.getresponse()
                    response.read()
                    if response.status != (401 if value else 200):
                        raise RuntimeError('actual client/Platform credential separation failed')
                finally:
                    connection.close()
            path = '/platform/v1/apps/app-a/conversations/same-conversation/participants'
            client_probe(token)
            call(path, status=401, code='API_KEY_UNAUTHENTICATED')
            for jwt in ('user.jwt.token', 'staff.jwt.token'):
                call(path, jwt, 401, 'API_KEY_UNAUTHENTICATED', headers={'x-user-id':'a1','x-tenant-id':'app-a'})
            first = call(path + '?limit=1', token)
            second = call(path + '?limit=1&cursor=' + quote(first['next_cursor'], safe=''), token)
            third = call(path + '?limit=1&cursor=' + quote(second['next_cursor'], safe=''), token)
            if ([page['participants'][0]['user_id'] for page in (first, second, third)] != ['a1','a2','a3']
                    or not first['has_more'] or not second['has_more'] or third['has_more']):
                raise RuntimeError('real keyset ordering/tie/filter failed')
            call(path.replace('app-a','app-b'), token, 403, 'API_KEY_FORBIDDEN')
            if call(path.replace('app-a','app-b'), other)['participants'][0]['user_id'] != 'b1':
                raise RuntimeError('same-ID tenant isolation failed')
            for value in (wrong_scope, wrong_ip):
                call(path, value, 403, 'API_KEY_FORBIDDEN')
            for value in (expired, disabled_sa):
                call(path, value, 401, 'API_KEY_UNAUTHENTICATED')
            unknown = call(path.replace('same-conversation','unknown'), token)
            if unknown['participants'] or sql('SELECT count(*) FROM conversations;') != '2':
                raise RuntimeError('unknown read mutated conversation')
            for query in ('limit=0','limit=101','limit=1&limit=2','cursor=1000','cursor=k%3A1%3A-1%3A0%3Au','api_key=private'):
                call(path+'?'+query, token, 400, 'INVALID_REQUEST')
            call(path, token, 400, 'INVALID_REQUEST', body='private')
            call(path, token, 405, 'METHOD_NOT_ALLOWED', method='POST')
            sql("UPDATE tenants SET status='suspended' WHERE tenant_id='app-a';")
            time.sleep(1.1)
            call(path, token, 403, 'TENANT_INACTIVE')
            sql("UPDATE tenants SET status='active' WHERE tenant_id='app-a';")
            time.sleep(1.1)
            if args.via_social:
                social_sql("UPDATE flare_tenants SET status=2,version=version+1 WHERE tenant_id='app-a';")
                time.sleep(1.1)
                call(path, token, 403, 'TENANT_INACTIVE')
                social_sql("UPDATE flare_tenants SET status=1,version=version+1 WHERE tenant_id='app-a';")
                time.sleep(1.1)
                call(path, token)
            sql("BEGIN; DELETE FROM credential_keys WHERE app_id='app-a'; UPDATE credential_projection_versions SET version=2 WHERE app_id='app-a'; COMMIT;")
            time.sleep(1.1)
            call(path, token, 401, 'API_KEY_UNAUTHENTICATED')
            fresh = key()
            call(path, fresh)
            stop(conversation_process)
            call(path, fresh, 503, 'UPSTREAM_UNAVAILABLE')
            client_probe()
            conversation_process = start('flare-conversation')
            wait_for(lambda: listening(conversation_port), 'Conversation restart', (conversation_process,))
            time.sleep(2.2)
            call(path, fresh)
            stop(gateway)
            gateway = start(entry_binary)
            wait_for(lambda: listening(gateway_port), 'Gateway restart', (gateway, conversation_process))
            call(path, token, 401, 'API_KEY_UNAUTHENTICATED')
            call(path, fresh)
            if core_gateway is not None:
                stop(core_gateway)
                call(path, fresh, 503, 'UPSTREAM_UNAVAILABLE')
                connection = http.client.HTTPConnection('127.0.0.1', gateway_port, timeout=5)
                try:
                    connection.request('GET', '/health')
                    response = connection.getresponse()
                    response.read()
                    if response.status != 200:
                        raise RuntimeError('Social health failed during actual Core Gateway outage')
                finally:
                    connection.close()
                core_gateway = start('flare-api-gateway')
                wait_for(lambda: listening(core_gateway_port), 'Core Gateway restart', (core_gateway, conversation_process))
                time.sleep(2.2)
                call(path, fresh)
            stop(gateway)
            env['FLARE_PLATFORM_TRUSTED_PROXY_CIDRS'] = '127.0.0.1/32'
            gateway = start(entry_binary)
            wait_for(lambda: listening(gateway_port), 'Trusted Gateway', (gateway, conversation_process))
            call(path, fresh, 400, 'INVALID_REQUEST')
            call(path, fresh, 400, 'INVALID_REQUEST', headers={'x-forwarded-for':'invalid'})
            call(path, fresh, headers={'x-forwarded-for':'127.0.0.1'})
            call(path, fresh, 403, 'API_KEY_FORBIDDEN', headers={'x-forwarded-for':'127.0.0.1,192.0.2.1'})
            if args.via_social:
                source_key = key(ip_allow=['192.0.2.0/24'])
                call(path, source_key, headers={'x-forwarded-for':'127.0.0.1,192.0.2.1'})
                # Explicit corrupt/lagging local-policy fixture: Social still grants this
                # exact credential, Core independently denies the endpoint scope.
                selector = source_key.split('_')[2]
                sql(f"UPDATE credential_keys SET payload=jsonb_set(payload,'{{scopes}}','[\"platform.users.read\"]'::jsonb) WHERE key_id='{selector}';")
                time.sleep(1.1)
                call(path, source_key, 403, 'API_KEY_FORBIDDEN', headers={'x-forwarded-for':'192.0.2.1'})
            stop(gateway)
            env['FLARE_PLATFORM_API_ENABLED'] = 'false'
            env.pop('FLARE_API_KEY_PEPPER')
            gateway = start(entry_binary)
            wait_for(lambda: listening(gateway_port), 'Disabled bridge', (gateway, conversation_process))
            connection = http.client.HTTPConnection('127.0.0.1', gateway_port, timeout=5)
            try:
                connection.request('GET', path, headers={'Authorization':f'Bearer {fresh}'})
                response = connection.getresponse()
                response.read()
                if response.status != 404:
                    raise RuntimeError('disabled bridge unexpectedly registered Platform')
            finally:
                connection.close()
            stop(gateway)
            if args.via_social:
                social_sql('ALTER TABLE flare_credential_keys RENAME TO fixture_credentials;')
            else:
                sql('ALTER TABLE credential_keys RENAME TO fixture_credentials;')
            env.update(FLARE_PLATFORM_API_ENABLED='true', FLARE_API_KEY_PEPPER=b64(pepper))
            broken = start(entry_binary)
            if broken.wait(timeout=10) == 0:
                raise RuntimeError('missing credential schema silently accepted')
            stop(conversation_process)
            if core_gateway is not None:
                stop(core_gateway)
            for handle in handles:
                handle.flush()
            for log in cwd.glob('*.log'):
                raw = log.read_bytes()
                if any(material.encode() in raw for material in materials):
                    raise RuntimeError('secret material reached owned service logs')
            chain = 'Social Gateway -> Core Gateway -> Conversation -> separate owned PGs' if args.via_social else 'Core Gateway -> Conversation -> owned PG'
            print(f'PASS native production {chain}: {count} JSON contract responses; real Service read, keyset/filter/isolation, local revocation/restart, outage/recovery, trusted source chain, disabled bridge, missing-schema refusal, redacted logs; NOT container/Control delivery acceptance')
    finally:
        for process in processes:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=5)
        for handle in handles:
            handle.close()
        if created:
            checked(['dropdb', *address, '--if-exists', db])
        if social_created:
            checked(['dropdb', *address, '--if-exists', social_db])
        os.umask(original_umask)


def interrupted(signum, frame):
    raise KeyboardInterrupt


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, interrupted)
    main()
