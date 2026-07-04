-- Add thread_id to existing messages tables created before thread support.
-- Safe to run repeatedly against local/dev databases.

ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS thread_id TEXT;

COMMENT ON COLUMN messages.thread_id IS
    '话题/线程根消息 ID；普通消息为空';

CREATE INDEX IF NOT EXISTS idx_messages_tenant_thread_seq
    ON messages(tenant_id, thread_id, seq)
    WHERE thread_id IS NOT NULL;
