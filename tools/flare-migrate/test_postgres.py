"""Explicit real-Postgres suite; creates/drops only randomly named test databases.

Requires FLARE_MIGRATE_TEST_DISPOSABLE=1, PGHOST/PGPORT/PGUSER/PGDATABASE.
Use a separate TimescaleDB instance. Missing infrastructure fails, never skips.
"""
from contextlib import contextmanager, redirect_stdout
import dataclasses
import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
import uuid
from unittest.mock import patch

import flare_migrate as migrate


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

    def test_empty_database_repeat_and_verified_schema(self):
        with self.fixture() as name, self.connect(name) as db:
            self.run_tool(db)
            before = migrate.ledger(db, self.migrations)
            self.run_tool(db)
            self.assertEqual(migrate.ledger(db, self.migrations), before)
            self.assertEqual(len(before), 8)
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
            self.assertEqual(len(migrate.ledger(db, self.migrations)), 8)

    def test_existing_core_timescale_2291_version_is_supported(self):
        with self.fixture() as name, self.connect(name) as db:
            self.baseline(db, "2.29.1")
            self.run_tool(db, admission=self.approved_fixture(db))
            actual = migrate.snapshot(db)
            self.assertEqual(actual["environment"]["timescaledb"], "2.29.1")
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
                broken = migrate.Migration(9, "0009_failure.sql", "CREATE TABLE rollback_marker(id bigint); SELECT 1/0;", hashlib.sha256(b"broken").hexdigest())
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
