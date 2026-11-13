# Core initial communication schema

Generic messages, events, conversations, media ACLs, transport credentials, Hook
execution and durable evidence live here. Social directories and product policy
remain outside Core. No tenant zero or wildcard RTC grant is created.

The cluster administrator installs the reviewed TimescaleDB extension in the
new database and assigns database ownership to the isolated `core_initialize`
login from `roles.sql`. `database.sql` makes the installation boundary explicit:
only the owner and `core_runtime` can connect, and PUBLIC has no CONNECT or
TEMPORARY privilege. This matches the canonical lab launcher
`ensure_database("core", "core_initialize", ["core_runtime"])`. Services use `core_runtime`, which has no DDL/owner
ability. The initializer currently reviews PostgreSQL 16 with TimescaleDB
2.29.1 or 2.30.0; other environments fail closed until source profiles are
reviewed. Neither initializer command uses or creates a SQLx migration ledger.

Set `CORE_INITIAL_DATABASE_URL` privately and set `CORE_INITIAL_DATABASE` to the
same explicit database name. Passwords and connection URLs must stay out of
logs. Then run the normal `flare-core-initialize` binary with no argument for a
fresh, empty v3 database. Schema, system seeds, the v3 `core_initial_schema`
marker and a `core_initial_history` fresh transition commit in one transaction.
Replay verifies the full current source shape, authority and history without
resetting configuration or data. Unrelated nonempty databases are rejected.

## Accepted v1 and v2 forward upgrades

Stop or quiesce writers, capture the normal reviewed backup, and run the same
operator binary with the explicit `upgrade` argument and the exact database
owner connection. This accepts only the released initial models archived under
`versions/v1` and `versions/v2`, whose schema-plus-NUL-plus-seeds SHA-256 are
`0ef150419f08a05d966226025d426e7cb2cf6013d976565f0002e9b733275093` (v1) and
`3e882365f055b204d38c8667bed194ee8393d7cf25dfe670fc7401fdba808f4f` (v2).
The actual catalog must also exactly match a source-generated descriptor in
`upgrades/v1-pg16-ts*.json` or `upgrades/v2-pg16-ts*.json`: relations, columns, indexes, constraints, triggers,
functions, schema/role/database ownership and ACLs, and Timescale settings.
The installation-specific superuser that installs Timescale and owns its
actual extension-member namespaces is normalized as a typed bootstrap principal;
its superuser status, namespace membership and every ACL permission remain
checked. Application owners and ACL grantees are exact.
An altered marker alone cannot authorize a different database. SQLx-ledgered
databases, runtime/superuser identities, extra objects or changed authority are
rejected. This command never imports or adopts another schema model.

The bounded transaction uses the initializer advisory lock and locks existing
application tables. From v1 it first adds typed media ownership and purpose with
the three checks in `upgrades/0002_application_media.sql`. Existing assets stay
User-owned with General purpose; client metadata cannot promote them. The
database must then match the released v2 descriptor and carry exactly its one v2
history row, whether it came from that step or from a fresh v2 initialization.
`upgrades/0003_plugin_web_bundles.sql` then creates the empty `plugin_web_bundles`
table of the fresh v3 schema and widens the history checks to the v3 transition.
Each step advances the genuine initial marker and records the accepted old
source, target source and exact forward DDL hashes in `core_initial_history`, so
a v1 database ends with two rows and a v2 database gains one. Runtime can read
history and cannot insert, update or delete it. The target descriptor must match
the fresh v3 descriptor in `upgrades/v3-pg16-ts*.json` before commit. Failure rolls back DDL, marker
and history together; an identical completed repeat verifies provenance and
returns `changed:false` without rewriting its timestamp.

Fresh initialization and forward upgrade preserve the system capability
configuration after completion. No message status, historical event, tenant
policy or existing media payload is reclassified. SQL fixtures validate schema,
transactions and ACLs; they do not prove API message/ACK or delivery behavior.

## Adding a version

The released sources, their upgrade DDL and their descriptors are never edited:
an installation that used them must still verify. A new version archives the
released `schema.sql` and `seeds.sql` under `versions/v<N>`, adds the forward DDL
as `upgrades/<NNNN>_<name>.sql` and the fresh `v<N+1>-pg16-ts*.json` descriptors,
and widens the marker and history checks. The descriptors are generated on a
disposable PostgreSQL 16 container with each reviewed TimescaleDB version, after
the same generator reproduces the committed descriptors byte for byte; every
upgrade path must produce the same catalog as the fresh model. The v3
descriptors were made this way on PostgreSQL 16.15 with TimescaleDB 2.29.1 and
2.30.0: the fresh model, v1 through v2 to v3 and fresh v2 to v3 all give the
same 1462 objects.
