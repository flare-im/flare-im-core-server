-- 操作历史按操作事件的自然身份判重。
-- Storage Writer 先记操作历史、再写事件流（events 表是该事件已应用完的标记）；写事件流失败后的重投
-- 会再记一次操作历史。一行的身份是（租户，会话，seq，消息）：操作事件在事件流里以（租户，会话，seq）
-- 为键，已读回执一个事件对应多条消息。新 Writer 写入这两列并以 ON CONFLICT DO NOTHING 跳过重复。
--
-- 先于新的 Storage Writer 执行（新 Writer 写这两列，缺列时操作事件写入失败）；旧 Writer 不写这两列，
-- 照常可用。可重复执行。
--
-- 存量行：本迁移之前写入的行两列为 NULL，不回填，也不进下面的部分唯一索引；其中由重投多记的重复行
-- 原样保留，建索引不会因此失败。查与清理见 docs/06-testing-performance-and-operations.md。

ALTER TABLE message_operation_history
    ADD COLUMN IF NOT EXISTS conversation_id TEXT,
    ADD COLUMN IF NOT EXISTS seq BIGINT;

COMMENT ON COLUMN message_operation_history.conversation_id IS
    '操作事件所在会话；与 seq 一起是事件流 events 的键。迁移 0009 之前的行为 NULL';

COMMENT ON COLUMN message_operation_history.seq IS
    '操作事件的会话 seq；迁移 0009 之前的行为 NULL';

CREATE UNIQUE INDEX IF NOT EXISTS idx_message_operation_history_event_identity
    ON message_operation_history(tenant_id, conversation_id, seq, message_id)
    WHERE seq IS NOT NULL;
