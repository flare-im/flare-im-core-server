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
fresh, empty v2 database. Schema, system seeds, the v2 `core_initial_schema`
marker and a `core_initial_history` fresh transition commit in one transaction.
Replay verifies the full current source shape, authority and history without
resetting configuration or data. Unrelated nonempty databases are rejected.

## Accepted v1 forward upgrade

Stop or quiesce writers, capture the normal reviewed backup, and run the same
operator binary with the explicit `upgrade` argument and the exact database
owner connection. This accepts only the original initial model archived under
`versions/v1`, whose schema-plus-NUL-plus-seeds SHA-256 is
`0ef150419f08a05d966226025d426e7cb2cf6013d976565f0002e9b733275093`.
The actual catalog must also exactly match a source-generated descriptor in
`upgrades/v1-pg16-ts*.json`: relations, columns, indexes, constraints, triggers,
functions, schema/role/database ownership and ACLs, and Timescale settings.
The installation-specific superuser that installs Timescale and owns its
actual extension-member namespaces is normalized as a typed bootstrap principal;
its superuser status, namespace membership and every ACL permission remain
checked. Application owners and ACL grantees are exact.
An altered marker alone cannot authorize a different database. SQLx-ledgered
databases, runtime/superuser identities, extra objects or changed authority are
rejected. This command never imports or adopts another schema model.

The bounded transaction uses the initializer advisory lock and locks existing
application tables. It adds typed media ownership and purpose with the three
checks in `upgrades/0002_application_media.sql`. Existing assets stay User-owned
with General purpose; client metadata cannot promote them. The same file adds
the user-level notification settings table and the participant `notify_all`
flag with its partial index; existing participants start with it off. The same transaction
advances the genuine initial marker to v2 and records the accepted old source,
target source and exact forward DDL hashes in `core_initial_history`. Runtime
can read history and cannot insert, update or delete it. The target descriptor
must match the fresh v2 descriptor before commit. Failure rolls back DDL, marker
and history together; an identical completed repeat verifies provenance and
returns `changed:false` without rewriting its timestamp.

Fresh initialization and forward upgrade preserve the system capability
configuration after completion. No message status, historical event, tenant
policy or existing media payload is reclassified. SQL fixtures validate schema,
transactions and ACLs; they do not prove API message/ACK or delivery behavior.
