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

- `STORAGE_READER_REDIS_URL`
- `STORAGE_READER_POSTGRES_URL`
- `STORAGE_READER_DEFAULT_RANGE_SECONDS`
- `STORAGE_READER_MAX_PAGE_SIZE`

The actual configuration comes primarily from `config/services/*.toml` and `FlareAppConfig`; environment variables are used for deployment overrides.

## PostgreSQL / TimescaleDB

Database initialization uses `deploy/init.sql` as the sole entry point; the DDL is not duplicated in the storage module README, to avoid schema drift.

Current core conventions:

- `messages` is the message aggregate root, built as a TimescaleDB hypertable by `created_at`.
- `timestamp` is the business message time, used for the timeline, filtering, and display; it is not used as the partition key.
- `events` is a durable event stream, with event idempotency guaranteed by `tenant_id + conversation_id + seq`.
- `message_write_ledger` is a regular table, responsible for the final idempotency of `tenant_id + server_id` and diagnosing write-path status.
- Sync queries preferentially use `(tenant_id, conversation_id, seq)`.
- Management-side retrieval preferentially uses a composite index of tenant + dimension + `timestamp DESC`.

For the complete fields, indexes, compression policies, and triggers, see `deploy/init.sql` and `deploy/TIMESCALEDB_GUIDE.md`.

## Verification

```bash
cargo test --package flare-storage-writer
cargo test --package flare-storage-reader
```
