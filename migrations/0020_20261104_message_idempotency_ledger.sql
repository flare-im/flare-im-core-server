-- Durable send idempotency ledger. Additive only: one new table with its hash partitions.
--
-- Ingest deduplicates resends by client_msg_id in Redis for a bounded window; a client that resends
-- after that window (offline longer than it, or Redis lost the key) must still get the first result
-- instead of a second message. Storage Writer claims (tenant_id, sender_id, client_msg_id) here in
-- the same transaction that stores the message (INSERT ... ON CONFLICT DO NOTHING); a send whose key
-- is already held by another message of the same conversation is not stored again. Ingest reads the
-- row only when a send's client creation time is older than the Redis window.
--
-- Not a Timescale hypertable: a hypertable's unique constraints must contain its time column, which
-- cannot express this uniqueness. Hash partitions on (tenant_id, sender_id) keep the key globally
-- unique (the partition key is inside the primary key); time partitions would let one client_msg_id
-- fall into two partitions on different days. Rows are kept 7 days and deleted by Storage Writer in
-- bounded batches by created_at.
CREATE TABLE message_idempotency_ledger (
    tenant_id TEXT NOT NULL,
    sender_id TEXT NOT NULL,
    client_msg_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    server_id TEXT NOT NULL,
    conversation_seq BIGINT NOT NULL,
    durability SMALLINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, sender_id, client_msg_id)
) PARTITION BY HASH (tenant_id, sender_id);
COMMENT ON TABLE message_idempotency_ledger IS '发送幂等账本：(tenant_id, sender_id, client_msg_id) 唯一，覆盖 Redis 去重窗口之外的重发；保留 7 天';
COMMENT ON COLUMN message_idempotency_ledger.server_id IS '首个占用该 client_msg_id 的消息';
COMMENT ON COLUMN message_idempotency_ledger.durability IS 'SendAckDurability 枚举值；写入时消息已落库，记 PERSISTED';
COMMENT ON COLUMN message_idempotency_ledger.created_at IS '入账时间，按它清理过期行';

CREATE TABLE message_idempotency_ledger_p00 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 0);
CREATE TABLE message_idempotency_ledger_p01 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 1);
CREATE TABLE message_idempotency_ledger_p02 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 2);
CREATE TABLE message_idempotency_ledger_p03 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 3);
CREATE TABLE message_idempotency_ledger_p04 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 4);
CREATE TABLE message_idempotency_ledger_p05 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 5);
CREATE TABLE message_idempotency_ledger_p06 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 6);
CREATE TABLE message_idempotency_ledger_p07 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 7);
CREATE TABLE message_idempotency_ledger_p08 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 8);
CREATE TABLE message_idempotency_ledger_p09 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 9);
CREATE TABLE message_idempotency_ledger_p10 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 10);
CREATE TABLE message_idempotency_ledger_p11 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 11);
CREATE TABLE message_idempotency_ledger_p12 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 12);
CREATE TABLE message_idempotency_ledger_p13 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 13);
CREATE TABLE message_idempotency_ledger_p14 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 14);
CREATE TABLE message_idempotency_ledger_p15 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 15);
CREATE TABLE message_idempotency_ledger_p16 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 16);
CREATE TABLE message_idempotency_ledger_p17 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 17);
CREATE TABLE message_idempotency_ledger_p18 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 18);
CREATE TABLE message_idempotency_ledger_p19 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 19);
CREATE TABLE message_idempotency_ledger_p20 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 20);
CREATE TABLE message_idempotency_ledger_p21 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 21);
CREATE TABLE message_idempotency_ledger_p22 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 22);
CREATE TABLE message_idempotency_ledger_p23 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 23);
CREATE TABLE message_idempotency_ledger_p24 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 24);
CREATE TABLE message_idempotency_ledger_p25 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 25);
CREATE TABLE message_idempotency_ledger_p26 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 26);
CREATE TABLE message_idempotency_ledger_p27 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 27);
CREATE TABLE message_idempotency_ledger_p28 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 28);
CREATE TABLE message_idempotency_ledger_p29 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 29);
CREATE TABLE message_idempotency_ledger_p30 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 30);
CREATE TABLE message_idempotency_ledger_p31 PARTITION OF message_idempotency_ledger FOR VALUES WITH (MODULUS 32, REMAINDER 31);

CREATE INDEX idx_message_idempotency_ledger_created_at ON message_idempotency_ledger (created_at);
