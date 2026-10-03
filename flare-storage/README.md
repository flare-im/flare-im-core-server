# Flare Storage Service

English · [中文](README.zh-CN.md)

> **Status**: The core path uses PostgreSQL / TimescaleDB as the sole persistent database; Redis only carries the hot cache, WAL, short-lived state, and idempotency assistance.

`flare-storage` contains two core services:

- `flare-storage/writer`: consumes MQ storage events and completes message archiving, the event stream, the ledger, the hot cache, and ACKs.
- `flare-storage/reader`: provides queries for historical messages, the event stream, audit, and export.

## Design boundaries

- Does not use MongoDB to store message bodies or real-time data.
- Does not perform cross-database synchronous dual-writing of the same message data.
- PostgreSQL / TimescaleDB is the sole persistent database family for messages, events, the ledger, and audit queries.
- Redis is only used for the hot cache, the send/write WAL, the idempotency window, presence/short-lived state, and read-path acceleration.
- MQ is the asynchronous boundary of the write path; when the writer fails to consume, it goes into retry / DLQ, rather than making the access layer synchronously wait for database success.

## Writer

Responsibilities of `flare-storage/writer`:

- Consume storage topics such as `flare.im.message.storage` and `flare.im.message.events`.
- Use the Redis idempotency window and the PostgreSQL `message_write_ledger` to ensure re-entrancy on duplicate delivery.
- Write messages into `messages`, and write operation/sync events into `events`.
- Maintain the hot cache and write-stage state.
- Publish ACK or failure status, for client ack/sync convergence and operational troubleshooting.

Durable message write flow:

```text
flare-message-ingest
  -> MQ main
  -> flare-orchestrator fanout
  -> storage topic
  -> flare-storage/writer
  -> PostgreSQL / TimescaleDB
  -> ACK / retry / DLQ
```

Failure handling:

- Retryable errors go into `STORAGE_MESSAGE_RETRY_TOPIC`.
- Errors that exceed the retry budget or are unrecoverable go into `STORAGE_MESSAGE_DLQ_TOPIC`.
- A redelivered event is checked against the existing `events` row at the same `(tenant_id, conversation_id, seq)`: the same event (identical, re-encoded with a different map entry order, stored with the type number of the pre-fix writer, or the same message differing only in metadata) is accepted as a duplicate and the existing row is kept; a different event is a conflict and goes through retry to the DLQ.
- `message_write_ledger` records stages such as archive/event/ack, to facilitate troubleshooting by `tenant_id + server_id`.

## Reader

Responsibilities of `flare-storage/reader`:

- `QueryMessages`: query messages by conversation, seq, time range, and pagination cursor.
- `GetMessage`: query details by message ID.
- `QueryMessageEvents`: query the event stream related to a message.
- `QueryMessageWriteLedger`: query the write stage and failure reason.
- `ExportMessages`: register an export task; the file generation is performed by a subsequent worker.
- Message-operation queries and audit reads uniformly go back to the source PostgreSQL / TimescaleDB, with Redis serving only as a cache.

## Configuration

Common Writer environment variables:

- `STORAGE_JETSTREAM_URL`
- `STORAGE_JETSTREAM_GROUP`
- `STORAGE_JETSTREAM_ACK_SUBJECT`
- `STORAGE_MESSAGE_RETRY_TOPIC`
- `STORAGE_MESSAGE_DLQ_TOPIC`
- `STORAGE_REDIS_URL`
- `STORAGE_REDIS_HOT_TTL_SECONDS`
- `STORAGE_REDIS_IDEMPOTENCY_TTL_SECONDS`
- `STORAGE_POSTGRES_URL`
- `STORAGE_POSTGRES_MAX_CONNECTIONS`
- `STORAGE_POSTGRES_MIN_CONNECTIONS`
- `STORAGE_POSTGRES_ACQUIRE_TIMEOUT_SECONDS`
- `STORAGE_WAL_HASH_KEY`

Common Reader environment variables:

- `STORAGE_REDIS_URL`
- `STORAGE_POSTGRES_URL` (falls back to `POSTGRES_URL`)
- `STORAGE_READER_DEFAULT_RANGE_SECONDS`
- `STORAGE_READER_MAX_PAGE_SIZE`

Both processes share one Redis connection per process, bounded by:

- `STORAGE_REDIS_CONNECT_TIMEOUT_MS`: each connection attempt, TCP plus handshake (default 2000). After a failed connect the process does not try again for this long and fails fast instead.
- `STORAGE_REDIS_RESPONSE_TIMEOUT_MS`: each command or pipeline (default 3000).
- `STORAGE_REDIS_CONNECT_RETRIES`: retries after a failed connection attempt, 0 to 5 (default 1, about 1 to 2 s apart).

With the defaults an unreachable Redis fails a call within about 6 s. The Writer only logs a failed hot-cache write (WARN, `storage_writer_persist_total{path="hot_cache",result="error"}`) and still acknowledges the durable write; the Reader falls back to PostgreSQL. A value outside its range stops the process at startup.

The actual configuration comes primarily from `config/services/*.toml` and `FlareAppConfig`; environment variables are used for deployment overrides.

## PostgreSQL / TimescaleDB

The authoritative schema history is `migrations/*.sql`, applied and checksum-verified by `flare-migrate` (`make db-migrate`; see `tools/flare-migrate/README.md`). `deploy/init.sql` is only the destructive development snapshot of the same schema (`make dev-reset`), never an upgrade path for an existing database. The DDL is not duplicated in this README, to avoid schema drift.

Current core conventions:

- `messages` is the message aggregate root, built as a TimescaleDB hypertable by `created_at`.
- `timestamp` is the business message time, used for the timeline, filtering, and display; it is not used as the partition key.
- `events` is a durable event stream, with event idempotency guaranteed by `tenant_id + conversation_id + seq`.
- `message_write_ledger` is a regular table, responsible for the final idempotency of `tenant_id + server_id` and diagnosing write-path status.
- Sync queries preferentially use `(tenant_id, conversation_id, seq)`.
- Management-side retrieval preferentially uses a composite index of tenant + dimension + `timestamp DESC`.

For the complete fields, indexes, compression policies, and triggers, see `migrations/` (or the snapshot in `deploy/init.sql`) and `deploy/TIMESCALEDB_GUIDE.md`.

## Verification

```bash
cargo test --package flare-storage-writer
cargo test --package flare-storage-reader
```
