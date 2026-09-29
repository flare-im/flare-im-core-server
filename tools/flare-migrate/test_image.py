"""Static gate for the opt-in Core one-shot migration image; no DB or Docker."""

from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = ROOT / "release/Dockerfile.migrate"
DOCKERIGNORE = ROOT / "release/Dockerfile.migrate.dockerignore"


class MigratorImageTests(unittest.TestCase):
    def test_image_has_explicit_base_and_only_ledger_inputs(self):
        source = DOCKERFILE.read_text()
        self.assertIn("ARG MIGRATOR_BASE_IMAGE\nFROM ${MIGRATOR_BASE_IMAGE}", source)
        self.assertIn("grep -Eq '^[^[:space:]@]+@sha256:[a-f0-9]{64}$'", source)
        self.assertIn("sys.version_info >= (3, 10)", source)
        self.assertIn("postgresql-client", source)
        self.assertIn("COPY --chown=10001:10001 migrations/ ./migrations/", source)
        self.assertIn("COPY --chown=10001:10001 tools/flare-migrate/ ./tools/flare-migrate/", source)
        self.assertIn('USER 10001:10001', source)
        self.assertIn('ENTRYPOINT ["python3", "/opt/flare-im-core/tools/flare-migrate/flare_migrate.py"]', source)
        self.assertIn('CMD ["run"]', source)
        self.assertNotIn("COPY deploy/init.sql", source)
        self.assertNotIn("flare-api-gateway", source)

    def test_docker_context_excludes_source_and_dev_sql(self):
        active = [line.strip() for line in DOCKERIGNORE.read_text().splitlines()
                  if line.strip() and not line.lstrip().startswith("#")]
        self.assertEqual(active, ["*", "!migrations/", "!migrations/**",
                                  "!tools/", "!tools/flare-migrate/",
                                  "!tools/flare-migrate/flare_migrate.py",
                                  "!tools/flare-migrate/catalog.sql",
                                  "!tools/flare-migrate/ledger_catalog.sql",
                                  "!tools/flare-migrate/ledger-schema.json",
                                  "!tools/flare-migrate/schema/",
                                  "!tools/flare-migrate/schema/*.json"])


if __name__ == "__main__":
    unittest.main()
