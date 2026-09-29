#!/usr/bin/env python3
"""Core-only transactional PostgreSQL migration ledger. Requires Python 3 + psql."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
import uuid
from urllib.parse import parse_qsl, unquote, urlsplit

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
LOCK_ID = 71631987120404
LEDGER = "public.flare_schema_migrations"
SAFE_SEARCH_PATH = "pg_catalog,public,pg_temp"
LEDGER_DDL = f"""CREATE TABLE {LEDGER} (
    version BIGINT PRIMARY KEY,
    checksum TEXT NOT NULL CHECK (checksum ~ '^[0-9a-f]{{64}}$'),
    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    baseline_admission JSONB,
    admission_sha256 TEXT,
    CONSTRAINT flare_schema_migrations_admission_check CHECK (
      (baseline_admission IS NULL AND admission_sha256 IS NULL) OR
      (version = 1 AND baseline_admission IS NOT NULL AND admission_sha256 IS NOT NULL
       AND jsonb_typeof(baseline_admission) = 'object'
       AND admission_sha256 ~ '^[0-9a-f]{{64}}$')
    )
);"""


class MigrationError(Exception):
    pass


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sql: str
    checksum: str


def statements(sql):
    """Split SQL while respecting comments, quoted strings and dollar bodies."""
    start = 0
    cursor = 0
    token = None
    depth = 0
    while cursor < len(sql):
        if token == "--":
            if sql[cursor] == "\n":
                token = None
            cursor += 1
        elif depth:
            if sql.startswith("/*", cursor):
                depth += 1
                cursor += 2
            elif sql.startswith("*/", cursor):
                depth -= 1
                cursor += 2
            else:
                cursor += 1
        elif token:
            if sql.startswith(token, cursor):
                if token in {"'", '"'} and sql.startswith(token * 2, cursor):
                    cursor += 2
                else:
                    cursor += len(token)
                    token = None
            else:
                cursor += 1
        elif sql.startswith("--", cursor):
            token = "--"
            cursor += 2
        elif sql.startswith("/*", cursor):
            depth = 1
            cursor += 2
        elif sql[cursor] in {"'", '"'}:
            token = sql[cursor]
            cursor += 1
        elif sql[cursor] == "$" and (match := re.match(r"\$(?:[A-Za-z_][A-Za-z_0-9]*)?\$", sql[cursor:])):
            token = match.group()
            cursor += len(token)
        elif sql[cursor] == ";":
            yield sql[start:cursor]
            cursor += 1
            start = cursor
        else:
            cursor += 1
    if depth or token not in {None, "--"}:
        raise MigrationError("unterminated SQL literal/comment")
    yield sql[start:]


def executable_head(statement):
    """Skip only leading whitespace/comments, including nested block comments."""
    cursor = 0
    while cursor < len(statement):
        if statement[cursor].isspace():
            cursor += 1
        elif statement.startswith("--", cursor):
            end = statement.find("\n", cursor + 2)
            cursor = len(statement) if end < 0 else end + 1
        elif statement.startswith("/*", cursor):
            depth = 1
            cursor += 2
            while cursor < len(statement) and depth:
                if statement.startswith("/*", cursor):
                    depth += 1
                    cursor += 2
                elif statement.startswith("*/", cursor):
                    depth -= 1
                    cursor += 2
                else:
                    cursor += 1
            if depth:
                raise MigrationError("unterminated SQL comment")
        else:
            break
    return statement[cursor:]


def validate_sql(sql):
    if "\\" in sql:
        # Avoid all psql client meta-commands, including hidden commands in dollar bodies.
        raise MigrationError("migration must not contain psql commands/backslashes")
    for statement in statements(sql):
        # Strip leading SQL comments only; keywords inside DO/function bodies are data here.
        head = executable_head(statement)
        first = re.match(r"[A-Za-z_][A-Za-z_0-9]*", head)
        keyword = first.group().upper() if first else ""
        if keyword in {"BEGIN", "COMMIT", "END", "ROLLBACK", "ABORT", "SAVEPOINT", "RELEASE", "VACUUM"}:
            raise MigrationError("migration transaction control is owned by the ledger")
        if keyword in {"START", "PREPARE"} and re.match(
            r"TRANSACTION\b", executable_head(head[first.end():]), re.I
        ):
            raise MigrationError("migration transaction control is owned by the ledger")
        if re.search(r"\bCONCURRENTLY\b", head.split("--", 1)[0], re.I):
            raise MigrationError("nontransactional migration is not supported")


def load_migrations(directory=ROOT / "migrations"):
    paths = sorted(directory.glob("*.sql"))
    migrations = []
    for expected, path in enumerate(paths, 1):
        match = re.fullmatch(r"([0-9]{4})_[a-z0-9_]+\.sql", path.name)
        if not match or int(match[1]) != expected or path.is_symlink() or not path.is_file():
            raise MigrationError("migration names must be regular, contiguous 0001..NNNN SQL files")
        raw = path.read_bytes()
        sql = raw.decode("utf-8")
        validate_sql(sql)
        migrations.append(Migration(expected, path.name, sql, hashlib.sha256(raw).hexdigest()))
    if not migrations:
        raise MigrationError("no migrations found")
    return migrations


def connection_env():
    """A libpq PGDATABASE environment value is a name, not a parsed URI."""
    env = os.environ.copy()
    dsn = env.pop("DATABASE_URL", None)
    if dsn:
        try:
            parsed = urlsplit(dsn)
            name = unquote(parsed.path[1:]) if parsed.path.startswith("/") else ""
            if parsed.scheme not in {"postgres", "postgresql"} or not parsed.hostname or not name or "/" in name or parsed.fragment:
                raise ValueError()
            # An explicit URL completely owns connection settings; inherited PG*
            # must not redirect its host/database/user or inject startup options.
            env = {key: value for key, value in env.items() if not key.startswith("PG")}
            env.update(PGHOST=parsed.hostname, PGPORT=str(parsed.port or 5432), PGDATABASE=name)
            if parsed.username is not None:
                env["PGUSER"] = unquote(parsed.username)
            if parsed.password is not None:
                env["PGPASSWORD"] = unquote(parsed.password)
            allowed = {"sslmode": "PGSSLMODE", "sslrootcert": "PGSSLROOTCERT", "sslcert": "PGSSLCERT", "sslkey": "PGSSLKEY", "connect_timeout": "PGCONNECT_TIMEOUT"}
            seen = set()
            for key, value in parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True):
                if key not in allowed or key in seen:
                    raise ValueError()
                seen.add(key)
                env[allowed[key]] = value
            if any("\x00" in value for value in env.values()):
                raise ValueError()
        except (TypeError, ValueError) as error:
            raise MigrationError("invalid PostgreSQL URI or unsupported connection option") from error
    if not env.get("PGDATABASE"):
        raise MigrationError("DATABASE_URL or explicit PGDATABASE/PGHOST settings required")
    env["PGAPPNAME"] = "flare-migrate"
    env.setdefault("PGCONNECT_TIMEOUT", "10")
    return env


class Database:
    def __init__(self):
        # DSN/password is environment-only; never place it in argv, output or exception text.
        env = connection_env()
        self.errors = tempfile.TemporaryFile(mode="w+t")
        try:
            self.process = subprocess.Popen(
                [env.get("PSQL", "psql"), "-X", "-qAt", "-v", "ON_ERROR_STOP=1"],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.errors,
                env=env, text=True, bufsize=1,
            )
        except OSError as error:
            self.errors.close()
            raise MigrationError("cannot start psql") from error
        try:
            self.query(f"SET search_path={SAFE_SEARCH_PATH}; SET TIME ZONE 'UTC'; SET client_min_messages=warning; SET lock_timeout='5s'; SET statement_timeout='10min';")
            assert_schema_writers(self)
        except MigrationError:
            self.close()
            raise

    def query(self, sql):
        marker = "flare_end_" + uuid.uuid4().hex
        try:
            self.process.stdin.write(sql + "\n\\echo " + marker + "\n")
            self.process.stdin.flush()
            lines = []
            for line in self.process.stdout:
                line = line.rstrip("\n")
                if line == marker:
                    return "\n".join(lines)
                lines.append(line)
        except (BrokenPipeError, OSError) as error:
            raise MigrationError("PostgreSQL request failed; transaction was not committed") from error
        # Do not echo stderr: arbitrary SQL errors can include row values or credentials.
        raise MigrationError("PostgreSQL request failed; inspect protected server logs; transaction was not committed")

    def json(self, sql):
        return json.loads(self.query(sql))

    def close(self):
        if self.process.poll() is None:
            try:
                self.process.stdin.close()  # EOF rolls back an uncommitted transaction.
                self.process.wait(timeout=5)
            except (subprocess.TimeoutExpired, BrokenPipeError):
                self.process.terminate()
                self.process.wait(timeout=5)
        self.process.stdout.close()
        if not self.process.stdin.closed:
            try:
                self.process.stdin.close()
            except BrokenPipeError:
                pass
        self.errors.close()


@contextmanager
def database():
    db = Database()
    try:
        yield db
    finally:
        db.close()


def snapshot(db):
    objects = db.json((HERE / "catalog.sql").read_text())
    environment = db.json("SELECT jsonb_build_object('postgres_major',current_setting('server_version_num')::int/10000,'timescaledb',(SELECT extversion FROM pg_extension WHERE extname='timescaledb'));")
    timescale = {}
    if environment["timescaledb"]:
        timescale = db.json("""SELECT jsonb_build_object(
          'hypertables',(SELECT COALESCE(jsonb_agg(jsonb_build_object('name',hypertable_name,'dimensions',num_dimensions,'compression',compression_enabled) ORDER BY hypertable_name),'[]'::jsonb) FROM timescaledb_information.hypertables WHERE hypertable_schema='public'),
          'dimensions',(SELECT COALESCE(jsonb_agg(to_jsonb(d)-'hypertable_schema' ORDER BY hypertable_name,dimension_number),'[]'::jsonb) FROM timescaledb_information.dimensions d WHERE hypertable_schema='public'),
          'compression',(SELECT COALESCE(jsonb_agg(to_jsonb(c)-'hypertable_schema' ORDER BY hypertable_name,attname),'[]'::jsonb) FROM timescaledb_information.compression_settings c WHERE hypertable_schema='public'),
          'jobs',(SELECT COALESCE(jsonb_agg(jsonb_build_object('table',hypertable_name,'function',proc_name,'config',config-'hypertable_id','scheduled',scheduled) ORDER BY hypertable_name,proc_name),'[]'::jsonb) FROM timescaledb_information.jobs WHERE hypertable_schema='public')
        );""")
    return {"environment": environment, "objects": objects, "timescale": timescale}


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def assert_schema_writers(db):
    # Schema/database owners and the explicit migrator are trusted DDL actors.
    # PUBLIC or another grantee must not plant function overloads during migration.
    unsafe = db.query("""SELECT EXISTS (
      SELECT 1 FROM pg_catalog.pg_namespace n
      CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(n.nspacl,pg_catalog.acldefault('n',n.nspowner))) a
      WHERE n.nspname='public' AND a.privilege_type='CREATE'
        AND a.grantee NOT IN (n.nspowner,
          (SELECT oid FROM pg_catalog.pg_roles WHERE rolname=current_user),
          (SELECT datdba FROM pg_catalog.pg_database WHERE datname=current_database()))
    );""")
    if unsafe != "f":
        raise MigrationError("public schema CREATE is granted to an untrusted role; revoke it before migration")


def execute_migration(db, sql):
    # pg_catalog is implicitly searched FIRST when omitted. This keeps CREATE's
    # destination public, without explicitly putting public ahead of pg_catalog.
    # Explicit pg_temp last also prevents temporary relations shadowing Core tables.
    db.query("SET LOCAL search_path=public,pg_temp;")
    db.query(sql)
    db.query(f"SET LOCAL search_path={SAFE_SEARCH_PATH};")


def assert_reviewed_baseline(actual):
    if canonical(actual) not in {canonical(read_profile(name, actual["environment"])) for name in ("baseline", "latest")}:
        raise MigrationError("schema mismatch: not a reviewed Core baseline; audited import required")


def seed_state(db):
    return db.json("""SELECT jsonb_build_object(
      'tenant_0',(SELECT jsonb_build_object('tenant_id',tenant_id,'status',status,'version',version)
        FROM public.tenants WHERE tenant_id='0'),
      'capability_service_settings_1',(SELECT jsonb_build_object('id',id,'global_enabled',global_enabled)
        FROM public.capability_service_settings WHERE id=1),
      'rtc_tenant_0_wildcard',(SELECT jsonb_build_object('tenant_id',tenant_id,'user_id',user_id,
        'capability_id',capability_id,'expires_at',expires_at,'plan_code',plan_code,'source',source)
        FROM public.capability_user_grants WHERE tenant_id='0' AND user_id='*' AND capability_id='rtc.*')
    );""")


def baseline_evidence(db, migrations, actual=None):
    actual = snapshot(db) if actual is None else actual
    assert_reviewed_baseline(actual)
    identity = db.json("""SELECT jsonb_build_object('database',current_database(),
      'database_oid',(SELECT oid FROM pg_database WHERE datname=current_database()),
      'server_address',inet_server_addr(),'server_port',inet_server_port());""")
    return {"format": "flare-core-baseline-evidence-v1", "database": identity,
            "environment": actual["environment"], "schema_sha256": digest(actual),
            "migration_checksums": [{"version": item.version, "sha256": item.checksum} for item in migrations],
            "seeds": seed_state(db)}


def validate_admission(document, evidence=None):
    if not isinstance(document, dict) or set(document) != {"evidence", "approval"}:
        raise MigrationError("explicit human-reviewed baseline admission is required")
    proof, approval = document["evidence"], document["approval"]
    if not isinstance(proof, dict) or set(proof) != {"format", "database", "environment", "schema_sha256", "migration_checksums", "seeds"} or proof.get("format") != "flare-core-baseline-evidence-v1":
        raise MigrationError("baseline admission evidence format is invalid")
    if not isinstance(proof["schema_sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", proof["schema_sha256"]):
        raise MigrationError("baseline admission schema digest is invalid")
    if not isinstance(approval, dict) or set(approval) != {"reviewer", "reference", "reviewed_at", "seed_intents"}:
        raise MigrationError("baseline admission must contain a human approval, not raw evidence")
    if any(not isinstance(approval[field], str) or not approval[field].strip() for field in ("reviewer", "reference", "reviewed_at")):
        raise MigrationError("baseline admission reviewer/reference/date is missing")
    try:
        reviewed_at = datetime.fromisoformat(approval["reviewed_at"].replace("Z", "+00:00"))
        if reviewed_at.tzinfo is None:
            raise ValueError("timezone required")
    except ValueError as error:
        raise MigrationError("baseline admission date must include a timezone") from error
    seeds = proof.get("seeds")
    required = {"tenant_0", "capability_service_settings_1", "rtc_tenant_0_wildcard"}
    if not isinstance(seeds, dict) or set(seeds) != required or not isinstance(approval["seed_intents"], dict) or set(approval["seed_intents"]) != required:
        raise MigrationError("baseline admission must review all three seed groups")
    for name, state in seeds.items():
        if state is not None and not isinstance(state, dict):
            raise MigrationError("baseline admission seed state is invalid")
        intent = "preserve_missing" if state is None else "preserve_present"
        if approval["seed_intents"][name] != intent:
            raise MigrationError("baseline admission seed intent does not match observed presence")
    if evidence is not None and canonical(proof) != canonical(evidence):
        raise MigrationError("baseline admission is stale or belongs to another database/schema/migration/seed state")


def read_admission(path):
    if path is None:
        return None
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size > 1_048_576:
                raise MigrationError("baseline admission must be a regular JSON file under 1 MiB")
            raw = stream.read(1_048_577)
            if len(raw) > 1_048_576:
                raise MigrationError("baseline admission must be under 1 MiB")
            document = json.loads(raw.decode("utf-8"))
    except (OSError, ValueError) as error:
        raise MigrationError("cannot read baseline admission JSON") from error
    validate_admission(document)
    return document


def read_profile(name, environment):
    for path in sorted((HERE / "schema").glob(name + "*.json")):
        profile = json.loads(path.read_text())
        if profile["environment"] == environment:
            return profile
    raise MigrationError("no reviewed schema profile for this PostgreSQL/TimescaleDB version")


def assert_schema(actual, expected):
    if canonical(actual) != canonical(expected):
        # Only catalog object keys are public; do not print application data or DSNs.
        old = {canonical(value) for value in expected["objects"]}
        new = {canonical(value) for value in actual["objects"]}
        raise MigrationError(f"schema mismatch: {len(old-new)} missing/changed definitions, {len(new-old)} extra/changed definitions; environment/Timescale configuration must also match")


def ledger(db, migrations):
    exists = db.query(f"SELECT to_regclass('{LEDGER}') IS NOT NULL;") == "t"
    if not exists:
        return None
    structure = db.json((HERE / "ledger_catalog.sql").read_text())
    if canonical(structure) != canonical(json.loads((HERE / "ledger-schema.json").read_text())):
        raise MigrationError("migration ledger structure is invalid")
    entries = db.json(f"SELECT COALESCE(jsonb_agg(jsonb_build_object('version',version,'checksum',checksum,'applied_at',applied_at,'baseline_admission',baseline_admission,'admission_sha256',admission_sha256) ORDER BY version),'[]'::jsonb) FROM {LEDGER};")
    for index, entry in enumerate(entries):
        if index >= len(migrations) or entry["version"] != index + 1:
            raise MigrationError("unknown or noncontiguous migration ledger version")
        if entry["checksum"] != migrations[index].checksum:
            raise MigrationError(f"checksum drift at migration {entry['version']:04}")
        admission = entry["baseline_admission"]
        if admission is not None:
            validate_admission(admission)
            if entry["version"] != 1 or entry["admission_sha256"] != digest(admission):
                raise MigrationError("baseline admission audit digest mismatch")
        elif entry["admission_sha256"] is not None:
            raise MigrationError("baseline admission audit is incomplete")
    return entries


def insert_ledger(db, migration, admission=None):
    audit = "NULL" if admission is None else f"convert_from(decode('{canonical(admission).encode('utf-8').hex()}','hex'),'UTF8')::jsonb"
    sha = "NULL" if admission is None else f"'{digest(admission)}'"
    db.query(f"INSERT INTO {LEDGER} (version,checksum,baseline_admission,admission_sha256) VALUES ({migration.version},'{migration.checksum}',{audit},{sha});")


def run(db, migrations, admission=None):
    db.query("BEGIN;")
    assert_schema_writers(db)
    if db.query(f"SELECT pg_try_advisory_xact_lock({LOCK_ID});") != "t":
        raise MigrationError("another Core migration is running")
    entries = ledger(db, migrations)
    applied = len(entries or [])
    adopted = False
    preserved_seeds = None
    if not applied:
        actual = snapshot(db)
        if actual["objects"]:
            # Both are reviewed, complete schemas, not table-existence heuristics.
            assert_reviewed_baseline(actual)
            if db.query("SELECT EXISTS(SELECT 1 FROM messages WHERE status IN (5,6,7,8));") == "t":
                raise MigrationError("historical message-status provenance is ambiguous; audited import required")
            db.query("LOCK TABLE public.tenants,public.capability_service_settings,public.capability_user_grants IN SHARE MODE;")
            validate_admission(admission, baseline_evidence(db, migrations))
            preserved_seeds = admission["evidence"]["seeds"]
            adopted = True
        elif admission is not None:
            raise MigrationError("baseline admission is only valid for an existing reviewed database")
        if entries is None:
            db.query(LEDGER_DDL)
        if adopted:
            insert_ledger(db, migrations[0], admission)
            applied = 1
    else:
        if admission is not None:
            raise MigrationError("baseline admission cannot replace an existing ledger audit")
        # Validate BEFORE executing pending SQL, not just afterward; an extra public
        # function/trigger must not be allowed to run under the migrator's authority.
        assert_reviewed_baseline(snapshot(db))
        db.query("LOCK TABLE public.tenants,public.capability_service_settings,public.capability_user_grants IN SHARE MODE;")
        preserved_seeds = seed_state(db)
    for migration in migrations[applied:]:
        execute_migration(db, migration.sql)
        insert_ledger(db, migration)
    if preserved_seeds is not None and canonical(seed_state(db)) != canonical(preserved_seeds):
        raise MigrationError("pending migrations would change approved/current seed intent; audited corrective migration required")
    actual = snapshot(db)
    assert_schema(actual, read_profile("latest", actual["environment"]))
    db.query("COMMIT;")
    print(f"PASS: {len(migrations)} checksum-verified migrations; {len(migrations)-applied} executed; baseline adopted={str(adopted).lower()}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["run", "status", "verify", "snapshot", "admission-evidence"])
    parser.add_argument("--baseline-admission", type=Path)
    args = parser.parse_args()
    migrations = load_migrations()
    if args.baseline_admission is not None and args.command != "run":
        parser.error("--baseline-admission is only valid with run")
    admission = read_admission(args.baseline_admission)
    with database() as db:
        if args.command == "run":
            run(db, migrations, admission)
        else:
            db.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;")
            if args.command == "snapshot":
                print(json.dumps(snapshot(db), ensure_ascii=False, indent=2, sort_keys=True))
            elif args.command == "admission-evidence":
                print(json.dumps({"evidence": baseline_evidence(db, migrations), "approval": None}, ensure_ascii=False, indent=2, sort_keys=True))
            else:
                entries = ledger(db, migrations)
                if args.command == "verify":
                    if entries is None or len(entries) != len(migrations):
                        raise MigrationError("migration ledger is absent or pending")
                    actual = snapshot(db)
                    assert_schema(actual, read_profile("latest", actual["environment"]))
                    print(f"PASS: latest Core migration schema and {len(entries)} checksums match")
                else:
                    print(json.dumps({"applied": len(entries or []), "pending": len(migrations)-len(entries or []), "ledger_present": entries is not None}))
            db.query("COMMIT;")


if __name__ == "__main__":
    try:
        main()
    except (MigrationError, ValueError, UnicodeError) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)
