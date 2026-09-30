# Core migration ledger

`flare-migrate` is a Python 3 + `psql` tool with no additional Python/Rust dependency. The authoritative production history is `migrations/*.sql`. It maintains `public.flare_schema_migrations(version BIGINT PRIMARY KEY, checksum TEXT, applied_at TIMESTAMPTZ, baseline_admission JSONB, admission_sha256 TEXT)`; checksum is SHA-256 of the exact immutable file bytes. The last two columns are paired and non-null only on version 0001 when a reviewed existing database is adopted. Fresh execution of 0001 leaves both null.

## Commands

Set `DATABASE_URL` securely in the environment, or set explicit libpq `PGHOST`, `PGPORT`, `PGUSER`, `PGDATABASE` and optionally `PGPASSFILE`. No implicit database/container default is selected. Credentials are not passed in argv or emitted in diagnostics. `PSQL` may select a psql executable path, not a shell command.

`DATABASE_URL` accepts an explicit `postgres://` / `postgresql://` host/database URI;
it is decoded into discrete libpq environment settings, never assigned as a literal
`PGDATABASE` name. It overrides inherited PG connection settings. URL options are
limited to `sslmode`, `sslrootcert`, `sslcert`, `sslkey` and `connect_timeout`, without
duplicates; use explicit PG settings for separately reviewed Unix-socket/passfile setups.

```sh
bash scripts/migrate_db.sh            # run
bash scripts/migrate_db.sh status     # read-only applied/pending count
bash scripts/check_schema_parity.sh   # read-only checksums + latest logical schema
python3 tools/flare-migrate/flare_migrate.py snapshot  # catalog only; no application rows
python3 tools/flare-migrate/flare_migrate.py admission-evidence  # read-only, UNAPPROVED seed review input
bash scripts/migrate_db.sh run --baseline-admission /secure/operator-reviewed-admission.json
```

Run in a maintenance window with writers stopped and a verified backup. The entire pending batch, including ledger entries, commits in one PostgreSQL transaction. A transaction-scoped advisory lock rejects a second migrator. DDL lock timeout is 5 seconds; each statement timeout is 10 minutes. Failure rolls back the complete batch; a failing or inaccessible database is never a successful SKIP. Never edit an applied file to resolve drift: recover the authentic migration source or investigate the database history.

Ordinary `CREATE INDEX` replaces the old standalone patch's `CONCURRENTLY` so index creation and its ledger commit are atomic. This is deliberately a maintenance-window path, not an online/index-backfill orchestration product. The tool rejects client meta-commands, explicit transaction control and concurrent DDL inside migration files, including transaction keywords hidden behind nested comments or comments between `PREPARE` and `TRANSACTION`.

## Frozen history and patch mapping

| Ledger | Source / purpose |
|---|---|
| 0001 | Frozen `deploy/init.sql` shape at the pre-SEC05 snapshot: all DROP statements removed; `media_assets.owner_id` reserved for 0008 |
| 0002 | `20260701_add_messages_thread_id.sql` |
| 0003 | `20260810_add_participant_visible_from_seq.sql` |
| 0004 | `20260812_add_participant_mention_only.sql` |
| 0005 | `20260831_fix_message_status_enum_drift.sql`, with mandatory provenance gate |
| 0006 | `20260901_participants_joined_at_index.sql`, transactional index creation |
| 0007 | `20260924_add_tenant_projection_version.sql` |
| 0008 | `20260929_media_authenticated_owner.sql`, transaction controlled by ledger |

The initial snapshot already contains several historical additive DDL changes. Their archived patches remain explicit numbered steps; `IF NOT EXISTS` makes those specific changes no-ops after the frozen snapshot. They are recorded through execution, not assumed applied because a table exists. Historical `deploy/patches` are retained for traceability, not an alternative automatic upgrade path.

The baseline inherits the existing development tenant `0` and RTC bootstrap grant. This task does not reinterpret those seed policies; production provisioning/security configuration must review or remove development grants before admitting users.

## Existing-database admission is strict

An unledgered, nonempty Core public schema is eligible only if it exactly matches one of the reviewed `schema/baseline*.json` or `schema/latest*.json` profiles. Only baseline version 0001 is registered without replay; all remaining migration steps execute and are recorded normally. A matching `latest` profile handles a manually applied SEC05 patch without rewriting legacy ownership.

Schema equality is **necessary, not sufficient**. Adoption also requires `--baseline-admission` naming an externally human-reviewed JSON document. `admission-evidence` outputs only `evidence` plus `approval: null`; there is no CLI approval or force command. It captures database name/OID/server endpoint, PostgreSQL/Timescale environment, complete logical schema SHA-256, exact migration file hashes, and these policy-sensitive seed states:

- `tenant_0`: presence, tenant ID, status and projection version;
- `capability_service_settings_1`: presence and current `global_enabled`;
- `rtc_tenant_0_wildcard`: presence, expiry and source/plan of grant `('0','*','rtc.*')`.

The trusted operator must review authoritative history/backup and desired seed policy, not merely copy the generated evidence. An independent review/change record supplies `approval.reviewer`, `approval.reference`, timezone-qualified ISO `approval.reviewed_at`, and `approval.seed_intents` with exactly those three keys. Every present row requires `preserve_present`; every absent row requires explicit `preserve_missing`. Keep the reviewed document in a protected operator-controlled location. Review identities/references are attestations verified by the organization's change-control process, **not cryptographic authentication**; a JSON hash provides integrity, not proof that a person really approved it. The tool never fabricates reviewer identity, signs its own approval, fills a missing seed, or assumes a deleted RTC wildcard should be restored.

During adoption, the three seed tables are locked against writers, then current schema/file hashes, database identity and seed values are compared again with the review. Stale evidence refuses adoption. The approval and its canonical JSON SHA-256 commit atomically with version 0001. Every subsequent ledger read validates audit structure/digest, the ledger's PK/CHECK constraints, column defaults/nullability, index validity, and absence of custom triggers/rules/RLS. It does not require live seed values to stay forever unchanged after adoption; authorized operational changes remain valid.

Pending migrations must also preserve the seed state they started with (or the human-approved adoption state). Important limitation: frozen **0007** inserts tenant `0` as active when absent. A `preserve_missing` tenant-0 waiver therefore **cannot safely pass this migration set**: the end-of-transaction guard detects the reinsert and rolls the whole batch back. A missing settings row or deleted RTC grant can be explicitly reviewed and remain absent; suspended tenants and `global_enabled=false` remain unchanged. Fixing a tenant-0-absent installation needs a separately reviewed corrective migration/import design, not a rewritten checksum, silent skipped step, or temporary automatic reactivation.

The earlier unshipped three-column prototype ledger is no longer accepted as equivalent to this audited format. Do not drop its history to bypass validation; an operator must verify its provenance and design an explicit schema/audit upgrade. No existing user database was automatically upgraded by this change.

The profile covers public tables/columns, types/defaults/nullability, PK/FK/check/unique constraints and validation state, indexes including predicates/validity, sequences (not their current position), views, functions, triggers, RLS policies, and Timescale hypertable/dimension/compression/policy configuration. Owners, object ACLs, comments, physical chunk count/OIDs, application rows and sequence positions are not structural equivalence. The seed admission and schema CREATE-ACL guard are separate checks; full business-row authorization and backup validation remain deployment requirements. Unrelated Social tables in the same public schema make baseline adoption fail; the tool must not silently adopt a mixed database.

To split a mixed database, admit a restored Core-only copy, never the shared original. Use `pg_dump`/`pg_restore` of the **server's** PostgreSQL major version (a newer `pg_restore` emits settings such as `transaction_timeout` that PostgreSQL 16 rejects). Dump the source excluding every Social table (`flare-social/deploy/db/social-tables.json`) **and the sequences those tables own**: excluding a table does not exclude its serial sequence, and a leftover sequence is refused as extra schema. Restore into a new database the TimescaleDB way (`CREATE EXTENSION timescaledb VERSION '<source version>'`, `timescaledb_pre_restore()`, `pg_restore`, `timescaledb_post_restore()`), then produce `admission-evidence` from the copy. Evidence or a review of the source is bound to its database identity and is rejected as stale for the copy. Extension-owned objects such as Social's `pg_trgm` functions are outside the fingerprint; any other leftover object is refused.

Control/catalog queries use `pg_catalog,public,pg_temp`. While executing a trusted migration file, the path is `public,pg_temp`: PostgreSQL implicitly searches `pg_catalog` first, while unqualified CREATE statements still create objects in `public`, not the system catalog. The safe explicit path is restored immediately afterward. Explicit `pg_temp` last prevents temporary relation shadowing. Before any admission or SQL execution, PUBLIC/other-role CREATE grants on `public` are refused. Only the configured migrator and schema/database owners are trusted DDL actors; those administrative principals must themselves be protected. The tool does not revoke grants silently. Known schema shape is verified **before** running pending migrations, so extra public functions/triggers cannot execute first and only be discovered afterward. Stop writers and coordinate privileged DDL administrators throughout the maintenance window.

Reviewed environments currently are PostgreSQL major **16**, TimescaleDB **2.29.1** and **2.30.0**. Profiles were produced in an isolated real Timescale instance, not by regex parsing DDL or stubbing extension functions. Other versions fail closed until their real migration result is reviewed and added. Do not normalize away a schema difference merely to make a new profile pass.

### Historical message status ambiguity

The old standalone status patch mapped old recalled `6` to modern recalled `5` using recall history, but a modern deleted `6` may also have earlier recall history. Schema equality cannot establish whether that data patch was applied. Automatic admission and migration 0005 therefore reject any message status in `5,6,7,8`. They do not guess, relabel rows, or falsely stamp the patch complete. Databases with these values need an audited import/corrective migration backed by reliable historical evidence; no `--force-baseline` bypass is provided. Other rows are preserved, including trusted existing tenant data and unknown media owners.

### v48 is not yet a proven historical baseline

Read-only inspection on 2026-09-29 found the existing `flare2` on PostgreSQL **16.15** / TimescaleDB **2.29.1**, Core and Social tables mixed in public, no migration/version ledger, and no current `tenants.status` CHECK constraint. No authoritative v48 schema was found in this repository/history or that database's catalogs. Thus this tool does **not** claim “v48 to latest” acceptance. The tests prove that an arbitrary version-48 marker plus table existence is rejected and leaves the database untouched. Importing actual v48 remains blocked on an authoritative schema/history export and its data-provenance review.

2026-09-30 source rebuild: the v48 deployment was built from commit `c46ee33`. Its `deploy/init.sql` plus its six `deploy/patches`, replayed in an isolated PostgreSQL 16.15 instance with TimescaleDB 2.29.1 and 2.30.0, fingerprint exactly to the reviewed `baseline` profiles; `admission-evidence`, a fixture approval, `run` (7 executed), `verify` and a repeated `run` (0 executed) passed with unchanged row counts. The same layout with the v48-era Social tables in `public` was refused until split as described above. A read-only catalog export of one deployed v48 database matched the baseline objects and the Timescale hypertable/compression/job configuration exactly, but that database runs TimescaleDB **2.30.1**, which has no reviewed profile yet, so the tool fails closed there. This establishes the v48 *schema* shape only; data provenance (message statuses, seed intent) and the release gates below remain open.

### Remaining formal DEP-04 release gates

The local tool and isolated fixtures do **not** complete DEP-04. Formal acceptance remains blocked on all of the following:

- An authoritative real-v48 schema/history export and reviewed data provenance, including ambiguous message statuses and intentional seed removals. A version label or a newly constructed current-schema fixture is not historical evidence.
- A representative restored backup undergoing the approved data migration with before/after row reconciliation, interruption recovery and a rehearsed restore/rollback procedure. Transaction rollback tests do not prove recovery after a committed migration or a production-scale backup restore.
- Explicit production role/owner/object-ACL parity and least-privilege review. The current `public` CREATE guard prevents untrusted DDL, but the structural fingerprint intentionally does not certify all object ACLs or role memberships.
- Timescale job parity against the actual restored source, including job ownership, enabled state, schedules, configuration, retention/compression effects and behavior after restart. The current fixture fingerprint covers selected hypertable/policy settings, not every operational job property or historical execution effect.

These gates require an isolated copy plus an approved operator runbook; do not point tests at the existing `flare2` database or mark it migrated based on the fixture results.

## Developer reset is separate

`deploy/init.sql` is only the current destructive development snapshot. It clears the old Core ledger along with Core tables. It is never used by the migration command or read-only parity check. `make dev-reset` requires explicit connection configuration **and** `FLARE_ALLOW_DEV_RESET=1`; never set this for a production or shared business database. Fresh provisioning must use `make db-migrate`, not init.sql.

## Tests and acceptance

```sh
make test-migrations
```

The real PostgreSQL suite requires a **separate disposable TimescaleDB instance** and an account allowed to create test databases. It creates UUID-named databases, closes their connections, then deletes only those exact databases. It never uses FORCE or deletes an existing target database. Missing infrastructure fails rather than skips.

```sh
# Explicit PGHOST/PGPORT/PGUSER/PGDATABASE point to that isolated instance.
# PG_DUMP/PG_RESTORE (default: pg_dump/pg_restore on PATH) must match its major version.
FLARE_MIGRATE_TEST_DISPOSABLE=1 PYTHONDONTWRITEBYTECODE=1 \
  python3 -m unittest discover -s tools/flare-migrate -p test_postgres.py -v
```

Coverage: empty creation, idempotence with unchanged timestamps, human-approved older baseline preserving data, additive media ownership, Timescale 2.29.1/2.30.0, checksum drift, ledger holes, transaction rollback, concurrent migrator exclusion, missing constraints, post-ledger shape drift, ambiguous historical status refusal, development-snapshot parity and read-only verification. Added admission cases cover missing human approval, explicit missing-seed waivers, disabled-policy preservation, stale review, 0007 reactivation rollback, audit digest tampering, ledger PK/CHECK/default tampering, PUBLIC CREATE refusal and catalog-function shadowing. A mixed Core/Social case proves that neither fresh evidence nor an earlier Core-only review admits the shared schema, that a table-only `pg_dump` exclusion is still refused because of leftover serial sequences, and that the Core-only restored copy is admitted with its data intact while the source stays unledgered. Source-level tests also cover file ordering, symlink/FIFO refusal, immutable checksums, transaction escape rejection, admission binding and BIGINT fingerprint precision.

Initial pre-review local acceptance was 9 source tests and 12 real PostgreSQL cases; the audit-hardening suite now has 19 source tests and 23 PostgreSQL cases, including an actual CLI evidence/admission round trip. These isolated checks are not v48 provenance acceptance or a production backup/restore rehearsal.
