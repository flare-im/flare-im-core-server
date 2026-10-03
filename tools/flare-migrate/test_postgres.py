"""Explicit real-Postgres suite; creates/drops only randomly named test databases.

Requires FLARE_MIGRATE_TEST_DISPOSABLE=1, PGHOST/PGPORT/PGUSER/PGDATABASE.
Use a separate TimescaleDB instance. Missing infrastructure fails, never skips.
PG_DUMP/PG_RESTORE (default: on PATH) must match the server's PostgreSQL major version.
"""
from contextlib import contextmanager, redirect_stdout
import dataclasses
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
import uuid
from unittest.mock import patch

import flare_migrate as migrate

# Legacy Social tables that shared Core's public schema before the Social database split.
# Mirrors flare-social/deploy/db/social-tables.json; checked below when that sibling exists.
SOCIAL_TABLE_LIST = migrate.ROOT.parent / "flare-social/deploy/db/social-tables.json"
SOCIAL_TABLES = (
    "flare_blocks", "flare_blocks_changes", "flare_blocks_sync", "flare_contact_meta",
    "flare_contact_tags", "flare_contacts_removed", "flare_contacts_sync", "flare_control_idempotency",
    "flare_feature_flags", "flare_feature_projection_versions", "flare_feature_user_overrides",
    "flare_friend_requests", "flare_friendship_epochs", "flare_friendships",
    "flare_group_announcement_reads", "flare_group_bans", "flare_group_directory_member_events",
    "flare_group_directory_removed", "flare_group_directory_sync", "flare_group_invite_links",
    "flare_group_join_requests", "flare_group_members", "flare_groups", "flare_invite_codes",
    "flare_moment_comments", "flare_moment_feed", "flare_moment_likes", "flare_moments",
    "flare_moments_audience", "flare_moments_settings", "flare_moments_visibility_rules",
    "flare_my_group_settings", "flare_referral_stats", "flare_refresh_tokens", "flare_reports",
    "flare_sensitive_hits", "flare_sensitive_words", "flare_social_outbox",
    "flare_social_outbox_aggregate_seq", "flare_tenant_counters", "flare_tenants",
    "flare_user_credentials", "flare_user_groups", "flare_user_login_keys", "flare_user_privacy",
    "flare_user_profiles", "flare_user_qr_tokens", "flare_user_referrals", "flare_users",
    "flare_verification_codes",
)
# The Social schema's only BIGSERIAL keys; their sequences are separate pg_dump objects.
SOCIAL_SERIAL_KEYS = {"flare_group_directory_member_events": "id", "flare_social_outbox": "outbox_id"}


def social_legacy_sql():
    """Representative legacy Social shapes: plain tables, serial sequences, pg_trgm index, rows."""
    parts = ["CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;"]
    for table in SOCIAL_TABLES:
        key = SOCIAL_SERIAL_KEYS.get(table)
        if key:
            parts.append(f"CREATE TABLE public.{table} ({key} BIGSERIAL PRIMARY KEY, tenant_id TEXT NOT NULL);")
            parts.append(f"INSERT INTO public.{table}(tenant_id) VALUES ('0');")
        else:
            parts.append(f"CREATE TABLE public.{table} (tenant_id TEXT NOT NULL, id TEXT NOT NULL, PRIMARY KEY (tenant_id, id));")
            parts.append(f"INSERT INTO public.{table}(tenant_id,id) VALUES ('0','social-row');")
    parts.append("CREATE INDEX idx_fup_trgm_id ON public.flare_user_profiles USING gin (id gin_trgm_ops);")
    return "\n".join(parts)


class PostgresMigrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.environ.get("FLARE_MIGRATE_TEST_DISPOSABLE") != "1" or os.environ.get("DATABASE_URL"):
            raise RuntimeError("use explicit PG* settings and FLARE_MIGRATE_TEST_DISPOSABLE=1 on a separate test instance")
        cls.admin = migrate.Database()
        cls.migrations = migrate.load_migrations()

    @classmethod
    def tearDownClass(cls):
        cls.admin.close()

    @contextmanager
    def fixture(self):
        name = "flare_migrate_test_" + uuid.uuid4().hex
        self.admin.query(f'CREATE DATABASE "{name}" TEMPLATE template0;')
        try:
            yield name
        finally:
            # No FORCE, wildcards or pre-existing names; all connections must be closed.
            self.admin.query(f'DROP DATABASE "{name}";')

    @contextmanager
    def connect(self, name):
        with patch.dict(os.environ, {"PGDATABASE": name}):
            with migrate.database() as db:
                yield db

    def run_tool(self, db, migrations=None, admission=None):
        with redirect_stdout(io.StringIO()):
            migrate.run(db, self.migrations if migrations is None else migrations, admission)

    def approved_fixture(self, db):
        # Test-only stand-in for external human review; the CLI never creates approval.
        evidence = migrate.baseline_evidence(db, self.migrations)
        return {"evidence": evidence, "approval": {
            "reviewer": "integration-test-reviewer", "reference": "test-only-audit",
            "reviewed_at": "2026-09-29T00:00:00Z",
            "seed_intents": {name: "preserve_missing" if state is None else "preserve_present"
                             for name, state in evidence["seeds"].items()}}}

    def baseline(self, db, version=None):
        if version:
            db.query(f"CREATE EXTENSION timescaledb WITH SCHEMA public VERSION '{version}';")
        db.query("BEGIN;")
        migrate.execute_migration(db, self.migrations[0].sql)
        db.query("COMMIT;")

    def pg_tool(self, name, arguments, stdin=None, stdout=subprocess.PIPE):
        # PG_DUMP / PG_RESTORE may select executables (e.g. wrappers running the
        # server's own binaries); credentials stay in the environment/passfile.
        result = subprocess.run([os.environ.get(name.upper(), name)] + arguments, cwd="/",
                                stdin=stdin, stdout=stdout, stderr=subprocess.PIPE, timeout=120)
        self.assertEqual(result.returncode, 0, result.stderr.decode("utf-8", "replace"))
        return result.stdout

    def restore_without(self, source, target, excluded):
        """pg_dump the source minus `excluded` relations; restore as documented by TimescaleDB."""
        with self.connect(source) as db:
            server_major = db.query("SELECT current_setting('server_version_num')::int/10000;")
            version = db.query("SELECT extversion FROM pg_extension WHERE extname='timescaledb';")
        # A newer pg_restore emits settings (e.g. transaction_timeout) an older server rejects.
        tool = re.search(r"\(PostgreSQL\) ([0-9]+)", self.pg_tool("pg_restore", ["--version"]).decode())
        self.assertEqual(tool and tool[1], server_major, "PG_RESTORE must match the server's PostgreSQL major version")
        with tempfile.TemporaryFile() as archive:
            self.pg_tool("pg_dump", ["--format=custom", "--dbname=" + source]
                         + ["--exclude-table=public." + name for name in excluded], stdout=archive)
            archive.seek(0)
            with self.connect(target) as db:
                db.query(f"CREATE EXTENSION timescaledb WITH SCHEMA public VERSION '{version}';")
                db.query("SELECT timescaledb_pre_restore();")
            self.pg_tool("pg_restore", ["--exit-on-error", "--dbname=" + target], stdin=archive)
            with self.connect(target) as db:
                db.query("SELECT timescaledb_post_restore();")

    def test_empty_database_repeat_and_verified_schema(self):
        with self.fixture() as name, self.connect(name) as db:
            self.run_tool(db)
            before = migrate.ledger(db, self.migrations)
            self.run_tool(db)
            self.assertEqual(migrate.ledger(db, self.migrations), before)
            self.assertEqual(len(before), 9)
            actual = migrate.snapshot(db)
            migrate.assert_schema(actual, migrate.read_profile("latest", actual["environment"]))

    def test_reviewed_old_baseline_preserves_data_and_unknown_media_owner(self):
        with self.fixture() as name, self.connect(name) as db:
            self.baseline(db)
            db.query("INSERT INTO tenants(tenant_id,name) VALUES ('retained','retained'); INSERT INTO media_assets(tenant_id,file_id,file_name,mime_type,file_size,url,metadata) VALUES ('retained','legacy','fixture','text/plain',1,'','{\"owner_id\":\"not-trusted\"}');")
            db.query("INSERT INTO messages(tenant_id,server_id,conversation_id,sender_id,seq,status) VALUES ('retained','safe-status','fixture','user',1,1);")
            self.run_tool(db, admission=self.approved_fixture(db))
            self.assertEqual(db.query("SELECT name FROM tenants WHERE tenant_id='retained';"), "retained")
            self.assertEqual(db.query("SELECT owner_id IS NULL FROM media_assets WHERE tenant_id='retained' AND file_id='legacy';"), "t")
            self.assertEqual(db.query("SELECT status FROM messages WHERE server_id='safe-status';"), "1")
            self.assertEqual(len(migrate.ledger(db, self.migrations)), 9)

    def test_existing_core_timescale_2291_version_is_supported(self):
        with self.fixture() as name, self.connect(name) as db:
            self.baseline(db, "2.29.1")
            self.run_tool(db, admission=self.approved_fixture(db))
            actual = migrate.snapshot(db)
            self.assertEqual(actual["environment"]["timescaledb"], "2.29.1")
            migrate.assert_schema(actual, migrate.read_profile("latest", actual["environment"]))

    def ledgered_0008(self, db):
        """A database migrated to 0008 by the previous release: ledger rows 1..8, previous latest shape."""
        db.query("BEGIN;")
        db.query(migrate.LEDGER_DDL)
        for migration in self.migrations[:8]:
            migrate.execute_migration(db, migration.sql)
            migrate.insert_ledger(db, migration)
        db.query("COMMIT;")
        actual = migrate.snapshot(db)
        migrate.assert_schema(actual, migrate.read_profile("v0008", actual["environment"]))

    def test_0009_upgrades_a_ledgered_0008_database_and_keeps_duplicate_history_rows(self):
        # Rows written before 0009 have no identity; duplicates from redelivery stay and do not block the index.
        history = "INSERT INTO message_operation_history(tenant_id,message_id,operation_type,operator_id,operation_data,timestamp) VALUES ('retained','m1','EVENT_REACTION','u1','{\"event_base64\":\"CAg=\"}','2026-10-01T00:00:00Z');"
        identified = "INSERT INTO message_operation_history(tenant_id,message_id,operation_type,conversation_id,seq) VALUES ('retained','m1','EVENT_REACTION','c1',7)"
        with self.fixture() as name:
            with self.connect(name) as db:
                self.ledgered_0008(db)
                db.query(history + history)
                self.run_tool(db)
                self.assertEqual(len(migrate.ledger(db, self.migrations)), 9)
                self.assertEqual(db.query("SELECT count(*) FROM message_operation_history WHERE tenant_id='retained' AND seq IS NULL;"), "2")
                actual = migrate.snapshot(db)
                migrate.assert_schema(actual, migrate.read_profile("latest", actual["environment"]))
                # New rows carry the identity: the Writer's ON CONFLICT skips a redelivery.
                for _ in range(2):
                    db.query(identified + " ON CONFLICT (tenant_id,conversation_id,seq,message_id) WHERE seq IS NOT NULL DO NOTHING;")
                self.assertEqual(db.query("SELECT count(*) FROM message_operation_history WHERE seq=7;"), "1")
                self.run_tool(db)
                self.assertEqual(len(migrate.ledger(db, self.migrations)), 9)
            with self.connect(name) as db:
                # A plain insert of the same identity is refused by the unique index (psql stops on the error).
                with self.assertRaises(migrate.MigrationError):
                    db.query(identified + ";")
            with self.connect(name) as db:
                self.assertEqual(db.query("SELECT count(*) FROM message_operation_history WHERE seq=7;"), "1")

    def test_0009_after_an_online_concurrent_index_prebuild(self):
        # The large-table procedure in docs/06-testing-performance-and-operations.md: add the two
        # nullable columns and build the unique index CONCURRENTLY before the window, check that it is
        # valid, then run 0009, whose IF NOT EXISTS steps find both and leave the ledger checksum alone.
        columns = ("ALTER TABLE message_operation_history ADD COLUMN IF NOT EXISTS conversation_id TEXT, "
                   "ADD COLUMN IF NOT EXISTS seq BIGINT;")
        index = ("CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_message_operation_history_event_identity "
                 "ON message_operation_history(tenant_id, conversation_id, seq, message_id) WHERE seq IS NOT NULL;")
        valid = ("SELECT indisvalid FROM pg_index "
                 "WHERE indexrelid='public.idx_message_operation_history_event_identity'::regclass;")
        clash = ("INSERT INTO message_operation_history(tenant_id,message_id,operation_type,conversation_id,seq) "
                 "VALUES ('retained','m1','EVENT_REACTION','c1',7);")
        with self.fixture() as name:
            with self.connect(name) as db:
                self.ledgered_0008(db)
                db.query(columns)
                # Stand-in for an interrupted build: two rows with one identity make it fail midway.
                db.query(clash + clash)
            with self.connect(name) as db:
                with self.assertRaises(migrate.MigrationError):
                    db.query(index)
            with self.connect(name) as db:
                # A failed concurrent build leaves an INVALID index that IF NOT EXISTS would skip; the
                # pre-check refuses that shape before executing anything.
                self.assertEqual(db.query(valid), "f")
                with self.assertRaisesRegex(migrate.MigrationError, "schema mismatch"):
                    self.run_tool(db)
            with self.connect(name) as db:
                self.assertEqual(len(migrate.ledger(db, self.migrations)), 8)
                db.query("DELETE FROM message_operation_history WHERE seq IS NOT NULL;")
                db.query("DROP INDEX CONCURRENTLY public.idx_message_operation_history_event_identity;")
                db.query(index)
                self.assertEqual(db.query(valid), "t")
                self.run_tool(db)
                self.assertEqual(len(migrate.ledger(db, self.migrations)), 9)
                actual = migrate.snapshot(db)
                migrate.assert_schema(actual, migrate.read_profile("latest", actual["environment"]))

    def test_unledgered_0008_shape_is_admitted_and_upgraded(self):
        with self.fixture() as name, self.connect(name) as db:
            db.query("BEGIN;")
            for migration in self.migrations[:8]:
                migrate.execute_migration(db, migration.sql)
            db.query("COMMIT;")
            self.run_tool(db, admission=self.approved_fixture(db))
            ledger = migrate.ledger(db, self.migrations)
            self.assertEqual(len(ledger), 9)
            self.assertIsNotNone(ledger[0]["baseline_admission"])
            actual = migrate.snapshot(db)
            migrate.assert_schema(actual, migrate.read_profile("latest", actual["environment"]))

    def test_v48_label_and_table_existence_do_not_prove_a_baseline(self):
        with self.fixture() as name:
            with self.connect(name) as db:
                db.query("CREATE TABLE public.schema_version(version integer); INSERT INTO public.schema_version VALUES (48); CREATE TABLE public.messages(id text);")
                with self.assertRaises(migrate.MigrationError):
                    self.run_tool(db)
            with self.connect(name) as db:
                self.assertEqual(db.query("SELECT to_regclass('public.flare_schema_migrations') IS NULL;"), "t")
                self.assertEqual(db.query("SELECT version FROM schema_version;"), "48")

    def test_mixed_social_tables_refuse_admission_until_core_only_dump(self):
        if SOCIAL_TABLE_LIST.exists():
            self.assertEqual(json.loads(SOCIAL_TABLE_LIST.read_text()), list(SOCIAL_TABLES))
        absent = "SELECT to_regclass('public.flare_schema_migrations') IS NULL;"
        with self.fixture() as mixed, self.fixture() as naive, self.fixture() as core_only:
            with self.connect(mixed) as db:
                self.baseline(db)
                db.query("INSERT INTO tenants(tenant_id,name) VALUES ('retained','retained');")
                db.query("INSERT INTO messages(tenant_id,server_id,conversation_id,sender_id,seq,status) VALUES ('retained','kept','fixture','user',1,1);")
                source_review = self.approved_fixture(db)
                db.query(social_legacy_sql())
                # Neither fresh evidence nor an earlier Core-only review admits the mixed schema.
                with self.assertRaisesRegex(migrate.MigrationError, "schema mismatch"):
                    migrate.baseline_evidence(db, self.migrations)
                with self.assertRaisesRegex(migrate.MigrationError, "schema mismatch"):
                    self.run_tool(db, admission=source_review)
            with self.connect(mixed) as db:
                self.assertEqual(db.query(absent), "t")
                self.assertEqual(db.query("SELECT count(*) FROM flare_social_outbox;"), "1")
                listed = ",".join(f"'{name}'" for name in SOCIAL_TABLES)
                owned = db.query(f"""SELECT s.relname FROM pg_depend d
                  JOIN pg_class s ON s.oid=d.objid AND s.relkind='S'
                  JOIN pg_class t ON t.oid=d.refobjid
                  WHERE d.classid='pg_class'::regclass AND d.refclassid='pg_class'::regclass
                    AND d.deptype IN ('a','i') AND t.relnamespace='public'::regnamespace
                    AND t.relname IN ({listed}) ORDER BY 1;""").split()
            self.assertEqual(owned, ["flare_group_directory_member_events_id_seq", "flare_social_outbox_outbox_id_seq"])
            # Excluding only the listed tables still dumps their serial sequences: refused.
            self.restore_without(mixed, naive, SOCIAL_TABLES)
            with self.connect(naive) as db:
                self.assertEqual(db.query("SELECT count(*) FROM pg_class WHERE relnamespace='public'::regnamespace AND relkind='S' AND left(relname,6)='flare_';"), "2")
                with self.assertRaisesRegex(migrate.MigrationError, "schema mismatch"):
                    migrate.baseline_evidence(db, self.migrations)
            self.restore_without(mixed, core_only, SOCIAL_TABLES + tuple(owned))
            with self.connect(core_only) as db:
                self.assertEqual(db.query("SELECT count(*) FROM pg_class WHERE relnamespace='public'::regnamespace AND left(relname,6)='flare_';"), "0")
                # The source review is bound to the source database identity, not the copy.
                with self.assertRaisesRegex(migrate.MigrationError, "stale"):
                    self.run_tool(db, admission=source_review)
            with self.connect(core_only) as db:
                self.assertEqual(db.query(absent), "t")
                self.run_tool(db, admission=self.approved_fixture(db))
                self.assertEqual(len(migrate.ledger(db, self.migrations)), 9)
                self.assertEqual(db.query("SELECT name FROM tenants WHERE tenant_id='retained';"), "retained")
                self.assertEqual(db.query("SELECT status FROM messages WHERE server_id='kept';"), "1")
            with self.connect(mixed) as db:
                self.assertEqual(db.query(absent), "t")

    def test_missing_constraint_is_not_accepted_as_a_baseline(self):
        with self.fixture() as name:
            with self.connect(name) as db:
                self.baseline(db)
                db.query("ALTER TABLE tenants DROP CONSTRAINT tenants_status_check;")
                with self.assertRaises(migrate.MigrationError):
                    self.run_tool(db)
            with self.connect(name) as db:
                self.assertEqual(db.query("SELECT to_regclass('public.flare_schema_migrations') IS NULL;"), "t")

    def test_ambiguous_historical_data_is_not_silently_rewritten(self):
        with self.fixture() as name:
            with self.connect(name) as db:
                self.baseline(db)
                db.query("INSERT INTO messages(tenant_id,server_id,conversation_id,sender_id,seq,status) VALUES ('0','legacy','fixture','user',1,6);")
                with self.assertRaisesRegex(migrate.MigrationError, "provenance"):
                    self.run_tool(db)
            with self.connect(name) as db:
                self.assertEqual(db.query("SELECT status FROM messages WHERE server_id='legacy';"), "6")
                self.assertEqual(db.query("SELECT to_regclass('public.flare_schema_migrations') IS NULL;"), "t")

    def test_checksum_drift_refuses_without_rewriting_ledger(self):
        with self.fixture() as name:
            with self.connect(name) as db:
                self.run_tool(db)
                before = migrate.ledger(db, self.migrations)
                changed = list(self.migrations)
                changed[0] = dataclasses.replace(changed[0], checksum=hashlib.sha256(b"changed").hexdigest())
                with self.assertRaisesRegex(migrate.MigrationError, "checksum drift"):
                    self.run_tool(db, changed)
            with self.connect(name) as db:
                self.assertEqual(migrate.ledger(db, self.migrations), before)

    def test_holes_in_ledger_refuse(self):
        with self.fixture() as name, self.connect(name) as db:
            self.run_tool(db)
            db.query("DELETE FROM public.flare_schema_migrations WHERE version=4;")
            with self.assertRaisesRegex(migrate.MigrationError, "noncontiguous"):
                self.run_tool(db)

    def test_sql_failure_rolls_back_ddl_and_ledger(self):
        with self.fixture() as name:
            with self.connect(name) as db:
                version = len(self.migrations) + 1
                broken = migrate.Migration(version, f"{version:04}_failure.sql", "CREATE TABLE rollback_marker(id bigint); SELECT 1/0;", hashlib.sha256(b"broken").hexdigest())
                with self.assertRaises(migrate.MigrationError):
                    self.run_tool(db, self.migrations + [broken])
            with self.connect(name) as db:
                self.assertEqual(db.query("SELECT to_regclass('public.flare_schema_migrations') IS NULL AND to_regclass('public.rollback_marker') IS NULL AND to_regclass('public.messages') IS NULL;"), "t")

    def test_concurrent_migrator_fails_closed(self):
        with self.fixture() as name, self.connect(name) as first:
            first.query(f"BEGIN; SELECT pg_advisory_xact_lock({migrate.LOCK_ID});")
            with self.connect(name) as second:
                with self.assertRaisesRegex(migrate.MigrationError, "another Core migration"):
                    self.run_tool(second)
            first.query("ROLLBACK;")

    def test_post_ledger_schema_drift_is_rejected(self):
        with self.fixture() as name, self.connect(name) as db:
            self.run_tool(db)
            db.query("ALTER TABLE tenants ALTER COLUMN name DROP NOT NULL;")
            with self.assertRaisesRegex(migrate.MigrationError, "schema mismatch"):
                self.run_tool(db)

    def test_dev_reset_snapshot_matches_migrations_and_parity_is_read_only(self):
        with self.fixture() as name, self.connect(name) as db:
            db.query("SET search_path=public,pg_temp;")
            db.query((migrate.ROOT / "deploy/init.sql").read_text())
            db.query(f"SET search_path={migrate.SAFE_SEARCH_PATH};")
            self.run_tool(db, admission=self.approved_fixture(db))
            before = migrate.ledger(db, self.migrations)
            env = dict(os.environ, PGDATABASE=name)
            result = subprocess.run(["bash", str(migrate.ROOT / "scripts/check_schema_parity.sh")], env=env, cwd="/", text=True, capture_output=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(migrate.ledger(db, self.migrations), before)

    def test_schema_alone_without_human_seed_review_never_stamps_baseline(self):
        with self.fixture() as name:
            with self.connect(name) as db:
                self.baseline(db)
                with self.assertRaisesRegex(migrate.MigrationError, "human-reviewed"):
                    self.run_tool(db)
            with self.connect(name) as db:
                self.assertEqual(db.query("SELECT to_regclass('public.flare_schema_migrations') IS NULL;"), "t")

    def test_cli_evidence_is_read_only_and_requires_external_approval(self):
        with self.fixture() as name, self.connect(name) as db:
            self.baseline(db)
            env = dict(os.environ, PGDATABASE=name)
            command = [sys.executable, str(migrate.HERE / "flare_migrate.py")]
            evidence = subprocess.run(command + ["admission-evidence"], env=env,
                                      text=True, capture_output=True, timeout=30)
            self.assertEqual(evidence.returncode, 0, evidence.stderr)
            document = json.loads(evidence.stdout)
            self.assertIsNone(document["approval"])
            self.assertEqual(db.query("SELECT to_regclass('public.flare_schema_migrations') IS NULL;"), "t")
            with tempfile.TemporaryDirectory(prefix="flare-migrate-cli-test-") as directory:
                admission_path = migrate.Path(directory) / "admission.json"
                admission_path.write_text(json.dumps(document))
                rejected = subprocess.run(command + ["run", "--baseline-admission", str(admission_path)],
                                          env=env, text=True, capture_output=True, timeout=30)
                self.assertNotEqual(rejected.returncode, 0)
                self.assertIn("human approval", rejected.stderr)
                self.assertEqual(db.query("SELECT to_regclass('public.flare_schema_migrations') IS NULL;"), "t")
                # Deliberate test-only external approval; the production CLI has no approve command.
                document["approval"] = self.approved_fixture(db)["approval"]
                admission_path.write_text(json.dumps(document))
                accepted = subprocess.run(command + ["run", "--baseline-admission", str(admission_path)],
                                          env=env, text=True, capture_output=True, timeout=30)
                self.assertEqual(accepted.returncode, 0, accepted.stderr)
                self.assertEqual(migrate.ledger(db, self.migrations)[0]["baseline_admission"], document)

    def test_absent_seeds_require_explicit_waiver_and_are_not_reinserted(self):
        with self.fixture() as name:
            with self.connect(name) as db:
                self.baseline(db)
                db.query("DELETE FROM capability_user_grants; DELETE FROM capability_service_settings;")
                approved = self.approved_fixture(db)
                self.assertEqual(approved["approval"]["seed_intents"]["rtc_tenant_0_wildcard"], "preserve_missing")
                self.assertEqual(approved["approval"]["seed_intents"]["capability_service_settings_1"], "preserve_missing")
                missing_waiver = {**approved, "approval": {**approved["approval"], "seed_intents": {key: "preserve_present" for key in approved["approval"]["seed_intents"]}}}
                with self.assertRaisesRegex(migrate.MigrationError, "seed intent"):
                    self.run_tool(db, admission=missing_waiver)
            with self.connect(name) as db:
                self.run_tool(db, admission=approved)
                self.assertEqual(db.query("SELECT (SELECT count(*) FROM capability_service_settings)+(SELECT count(*) FROM capability_user_grants);"), "0")
                audit = migrate.ledger(db, self.migrations)[0]
                self.assertEqual(audit["baseline_admission"], approved)
                self.assertEqual(audit["admission_sha256"], migrate.digest(approved))

    def test_tenant_seed_reactivation_by_0007_rolls_back_even_with_waiver(self):
        with self.fixture() as name:
            with self.connect(name) as db:
                self.baseline(db)
                db.query("DELETE FROM tenants WHERE tenant_id='0';")
                approved = self.approved_fixture(db)
                self.assertEqual(approved["approval"]["seed_intents"]["tenant_0"], "preserve_missing")
                with self.assertRaisesRegex(migrate.MigrationError, "change approved/current seed intent"):
                    self.run_tool(db, admission=approved)
            with self.connect(name) as db:
                self.assertEqual(db.query("SELECT count(*) FROM tenants WHERE tenant_id='0';"), "0")
                self.assertEqual(db.query("SELECT to_regclass('public.flare_schema_migrations') IS NULL;"), "t")

    def test_disabled_seed_policy_is_preserved_not_reactivated(self):
        with self.fixture() as name, self.connect(name) as db:
            self.baseline(db)
            db.query("UPDATE tenants SET status='suspended' WHERE tenant_id='0'; UPDATE capability_service_settings SET global_enabled=false; DELETE FROM capability_user_grants;")
            self.run_tool(db, admission=self.approved_fixture(db))
            self.assertEqual(db.query("SELECT status FROM tenants WHERE tenant_id='0';"), "suspended")
            self.assertEqual(db.query("SELECT global_enabled FROM capability_service_settings WHERE id=1;"), "f")
            self.assertEqual(db.query("SELECT count(*) FROM capability_user_grants;"), "0")

    def test_stale_seed_review_is_rejected(self):
        with self.fixture() as name:
            with self.connect(name) as db:
                self.baseline(db)
                approved = self.approved_fixture(db)
                db.query("UPDATE capability_service_settings SET global_enabled=false;")
                with self.assertRaisesRegex(migrate.MigrationError, "stale"):
                    self.run_tool(db, admission=approved)
            with self.connect(name) as db:
                self.assertEqual(db.query("SELECT to_regclass('public.flare_schema_migrations') IS NULL;"), "t")

    def test_ledger_pk_check_and_default_are_not_optional(self):
        changes = [
            "ALTER TABLE public.flare_schema_migrations DROP CONSTRAINT flare_schema_migrations_pkey;",
            "ALTER TABLE public.flare_schema_migrations DROP CONSTRAINT flare_schema_migrations_checksum_check;",
            "ALTER TABLE public.flare_schema_migrations ALTER COLUMN applied_at DROP DEFAULT;",
            "ALTER TABLE public.flare_schema_migrations DROP CONSTRAINT flare_schema_migrations_admission_check;",
        ]
        for sql in changes:
            with self.subTest(sql=sql), self.fixture() as name, self.connect(name) as db:
                self.run_tool(db)
                db.query(sql)
                with self.assertRaisesRegex(migrate.MigrationError, "ledger structure"):
                    migrate.ledger(db, self.migrations)

    def test_changed_admission_digest_is_rejected(self):
        with self.fixture() as name, self.connect(name) as db:
            self.baseline(db)
            self.run_tool(db, admission=self.approved_fixture(db))
            db.query("UPDATE public.flare_schema_migrations SET baseline_admission=jsonb_set(baseline_admission,'{approval,reference}','\"tampered\"') WHERE version=1;")
            with self.assertRaisesRegex(migrate.MigrationError, "digest mismatch"):
                migrate.ledger(db, self.migrations)

    def test_public_create_grants_are_rejected_before_migration(self):
        with self.fixture() as name:
            with self.connect(name) as db:
                db.query("GRANT CREATE ON SCHEMA public TO PUBLIC;")
                with self.assertRaisesRegex(migrate.MigrationError, "untrusted role"):
                    self.run_tool(db)
            with self.assertRaisesRegex(migrate.MigrationError, "untrusted role"):
                with self.connect(name):
                    pass

    def test_public_function_cannot_shadow_catalog_lock(self):
        with self.fixture() as name, self.connect(name) as db:
            db.query("CREATE FUNCTION public.pg_try_advisory_xact_lock(bigint) RETURNS boolean LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'SHADOW EXECUTED'; END $$;")
            db.query("BEGIN;")
            self.assertEqual(db.query("SELECT pg_try_advisory_xact_lock(1);"), "t")
            db.query("ROLLBACK;")
            with self.assertRaisesRegex(migrate.MigrationError, "schema mismatch|reviewed schema"):
                self.run_tool(db)


if __name__ == "__main__":
    unittest.main()
