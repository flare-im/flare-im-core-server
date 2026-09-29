import hashlib
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import flare_migrate as migrate


class MigrationUnitTests(unittest.TestCase):
    def test_database_url_maps_to_discrete_libpq_environment(self):
        url = "postgresql://operator:p%40ss@[::1]:32109/fixture%5Fdb?sslmode=require"
        with patch.dict(os.environ, {"DATABASE_URL": url, "PGDATABASE": "wrong", "PGHOST": "wrong", "PGOPTIONS": "unsafe"}, clear=True):
            env = migrate.connection_env()
        self.assertEqual(env["PGDATABASE"], "fixture_db")
        self.assertEqual(env["PGHOST"], "::1")
        self.assertEqual(env["PGPORT"], "32109")
        self.assertEqual(env["PGUSER"], "operator")
        self.assertEqual(env["PGPASSWORD"], "p@ss")
        self.assertEqual(env["PGSSLMODE"], "require")
        self.assertNotIn("DATABASE_URL", env)
        self.assertNotIn("PGOPTIONS", env)

    def test_explicit_libpq_settings_remain_supported(self):
        with patch.dict(os.environ, {"PGDATABASE": "fixture", "PGHOST": "/private/tmp/socket", "PGPASSFILE": "/secure/pgpass"}, clear=True):
            env = migrate.connection_env()
        self.assertEqual(env["PGDATABASE"], "fixture")
        self.assertEqual(env["PGHOST"], "/private/tmp/socket")
        self.assertEqual(env["PGPASSFILE"], "/secure/pgpass")

    def test_unsafe_url_options_and_secret_diagnostics_are_rejected(self):
        for url in ("postgresql://a:secret@host/db?options=bad", "postgresql://a:secret@host/db?sslmode=require&sslmode=disable", "postgresql://a:secret@host/db%00name", "mysql://a:secret@host/db"):
            with self.subTest(url_kind=url.split(":", 1)[0]), patch.dict(os.environ, {"DATABASE_URL": url}, clear=True):
                with self.assertRaises(migrate.MigrationError) as caught:
                    migrate.connection_env()
                self.assertNotIn("secret", str(caught.exception))

    def test_psql_argv_never_contains_uri_or_password(self):
        with patch.dict(os.environ, {"DATABASE_URL": "postgresql://operator:secret@host/db"}, clear=True), patch.object(migrate.subprocess, "Popen", side_effect=OSError()) as popen:
            with self.assertRaises(migrate.MigrationError):
                migrate.Database()
        args, kwargs = popen.call_args
        self.assertNotIn("secret", repr(args))
        self.assertNotIn("postgresql://", repr(args))
        self.assertEqual(kwargs["env"]["PGPASSWORD"], "secret")

    def reviewed_fixture(self):
        seeds = {name: None for name in ("tenant_0", "capability_service_settings_1", "rtc_tenant_0_wildcard")}
        return {"evidence": {
            "format": "flare-core-baseline-evidence-v1", "database": {"database": "test", "database_oid": 42},
            "environment": {"postgres_major": 16, "timescaledb": "2.30.0"},
            "schema_sha256": "a" * 64, "migration_checksums": [{"version": 1, "sha256": "b" * 64}], "seeds": seeds,
        }, "approval": {"reviewer": "unit-test-only", "reference": "test-review", "reviewed_at": "2026-09-29T00:00:00Z",
                        "seed_intents": {name: "preserve_missing" for name in seeds}}}

    def test_missing_connection_is_failure_not_skip(self):
        env = {key: value for key, value in os.environ.items() if key not in {"DATABASE_URL", "PGDATABASE"}}
        result = subprocess.run([sys.executable, str(migrate.HERE / "flare_migrate.py"), "verify"], env=env, capture_output=True, text=True, timeout=10)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("explicit PGDATABASE", result.stderr)
        self.assertNotIn("SKIP", result.stdout)

    def test_all_patches_are_accounted_for_and_baseline_has_no_drop(self):
        migrations = migrate.load_migrations()
        self.assertEqual(len(migrations), 8)
        self.assertNotIn("\nDROP ", migrations[0].sql)
        for patch in (migrate.ROOT / "deploy/patches").glob("*.sql"):
            self.assertTrue(any(patch.name in migration.name for migration in migrations[1:]), patch.name)

    def test_contiguous_regular_migration_names_are_required(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            # Fixture creation only; product SQL is always edited via source-controlled patches.
            (folder / "0002_missing_baseline.sql").write_text("SELECT 1;")
            with self.assertRaises(migrate.MigrationError):
                migrate.load_migrations(folder)

    def test_symlinked_migration_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            (folder / "source.txt").write_text("SELECT 1;")
            (folder / "0001_baseline.sql").symlink_to(folder / "source.txt")
            with self.assertRaises(migrate.MigrationError):
                migrate.load_migrations(folder)

    def test_transactions_and_psql_commands_cannot_escape_the_ledger(self):
        for sql in ["COMMIT;", "SELECT 1; ROLLBACK;", "/* leading */ BEGIN;",
                    "/* outer /* nested */ still outer */ COMMIT;",
                    "-- first\n/* outer /* nested */ still outer */ ROLLBACK;",
                    "\\! echo bad", "CREATE INDEX CONCURRENTLY idx ON t(id);",
                    "/* outer /* nested */ still outer */ CREATE INDEX CONCURRENTLY idx ON t(id);",
                    "START TRANSACTION;", "START /* comment */ TRANSACTION;",
                    "PREPARE TRANSACTION 'x';",
                    "PREPARE /* outer /* nested */ still outer */ TRANSACTION 'x';"]:
            with self.subTest(sql=sql), self.assertRaises(migrate.MigrationError):
                migrate.validate_sql(sql)
        migrate.validate_sql("DO $body$ BEGIN PERFORM 1; END $body$; SELECT 'COMMIT;';")

    def test_unterminated_sql_is_rejected(self):
        for sql in ["SELECT 'bad", "DO $$ BEGIN;", "SELECT 1; /* bad"]:
            with self.subTest(sql=sql), self.assertRaises(migrate.MigrationError):
                migrate.validate_sql(sql)

    def test_sha256_is_over_exact_file_bytes(self):
        for migration in migrate.load_migrations():
            raw = (migrate.ROOT / "migrations" / migration.name).read_bytes()
            self.assertEqual(migration.checksum, hashlib.sha256(raw).hexdigest())
            self.assertNotEqual(migration.checksum, hashlib.sha256(raw + b"\n").hexdigest())

    def test_schema_profiles_preserve_bigint_precision(self):
        profiles = list((migrate.HERE / "schema").glob("*.json"))
        self.assertGreaterEqual(len(profiles), 2)
        for path in profiles:
            value = json.loads(path.read_text())
            sequences = [entry for entry in value["objects"] if entry["kind"] == "sequence"]
            self.assertTrue(sequences)
            self.assertTrue(all(entry["max"] == 9223372036854775807 for entry in sequences))

    def test_version_and_definition_drift_are_not_ignored(self):
        old = {"objects": [{"kind": "column", "not_null": True}], "environment": {"timescaledb": "2.29.1"}}
        for new in [{"objects": [{"kind": "column", "not_null": False}], "environment": old["environment"]}, {"objects": old["objects"], "environment": {"timescaledb": "2.30.0"}}]:
            with self.assertRaises(migrate.MigrationError):
                migrate.assert_schema(new, old)

    def test_raw_evidence_is_not_self_approving(self):
        doc = self.reviewed_fixture()
        doc["approval"] = None
        with self.assertRaisesRegex(migrate.MigrationError, "human approval"):
            migrate.validate_admission(doc)

    def test_missing_seed_requires_explicit_preserve_missing(self):
        doc = self.reviewed_fixture()
        migrate.validate_admission(doc, doc["evidence"])
        doc["approval"]["seed_intents"]["rtc_tenant_0_wildcard"] = "preserve_present"
        with self.assertRaisesRegex(migrate.MigrationError, "seed intent"):
            migrate.validate_admission(doc)

    def test_admission_is_bound_to_database_schema_files_and_seed_state(self):
        doc = self.reviewed_fixture()
        for field, changed in [("database", {"database": "other"}), ("schema_sha256", "c" * 64),
                               ("migration_checksums", [{"version": 1, "sha256": "d" * 64}]),
                               ("seeds", {**doc["evidence"]["seeds"], "tenant_0": {"status": "active"}})]:
            current = copy.deepcopy(doc["evidence"])
            current[field] = changed
            with self.subTest(field=field), self.assertRaisesRegex(migrate.MigrationError, "stale"):
                migrate.validate_admission(doc, current)

    def test_admission_digest_is_canonical_and_covers_approval(self):
        doc = self.reviewed_fixture()
        self.assertEqual(migrate.digest(doc), migrate.digest(json.loads(json.dumps(doc, sort_keys=True))))
        changed = copy.deepcopy(doc)
        changed["approval"]["reference"] = "other"
        self.assertNotEqual(migrate.digest(doc), migrate.digest(changed))

    def test_admission_rejects_symlink_fifo_and_unapproved_json(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            source = folder / "approval.json"
            source.write_text(json.dumps(self.reviewed_fixture()))
            self.assertIsNotNone(migrate.read_admission(source))
            link = folder / "link.json"
            link.symlink_to(source)
            fifo = folder / "fifo"
            os.mkfifo(fifo)
            for path in (link, fifo):
                with self.assertRaises(migrate.MigrationError):
                    migrate.read_admission(path)
            source.write_text('{"approval":null}')
            with self.assertRaises(migrate.MigrationError):
                migrate.read_admission(source)

    def test_ledger_profile_includes_primary_check_and_timestamp_default(self):
        profile = json.loads((migrate.HERE / "ledger-schema.json").read_text())
        self.assertEqual({item["name"] for item in profile["constraints"]}, {
            "flare_schema_migrations_pkey", "flare_schema_migrations_checksum_check", "flare_schema_migrations_admission_check"})
        columns = {item["name"]: item for item in profile["columns"]}
        self.assertEqual(columns["applied_at"]["default"], "CURRENT_TIMESTAMP")
        self.assertTrue(all(item["validated"] for item in profile["constraints"]))


if __name__ == "__main__":
    unittest.main()
