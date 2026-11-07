# Core single initial communication schema

Fresh database only. Generic messages/events/conversations, media ACLs, transport
credentials, Hook execution and durable evidence live here. No Social users,
friendships, group directory, organization, plan, subscription or product
notification rules. There is no pre-active tenant zero or wildcard RTC grant.

The cluster administrator installs the TimescaleDB extension in the **new** DB
and gives ownership to the isolated `core_initialize` login. Normal services use
`core_runtime` with no DDL/owner ability. The single target SQL/seeds will execute
as one initialization transaction, not a history/backfill/adoption chain.

`flare-core-initialize` requires `CORE_INITIAL_DATABASE_URL` and the same explicit
`CORE_INITIAL_DATABASE`; source schema/system seed/version commit together.
Nonempty unrelated databases and runtime/superuser identities are rejected.
Exact completed replay does not reset data. There is no migration ledger,
historical message reclassification, old import or data backfill.

The focused actual TimescaleDB method passed on PostgreSQL16.15/TimescaleDB2.30.0
in an isolated temporary local container, including DDL rollback, no default
tenant/grants, true hypertable creation, runtime chunk INSERT and DDL/version
write rejection. The normal binary also created and replayed a separate empty
DB. Direct SQL INSERT is only a schema/ACL test, **not** a real message, ACK,
tenant authorization, Writer durability or full-stack result. The fixture and
its private network were removed; no existing volume/database was touched.

Final eight-service assembly, runtime secrets and Web/Flutter/two-client results
remain pending. Source image wiring includes the initializer but no image was
built or installed in this check. Existing databases are never reset.
