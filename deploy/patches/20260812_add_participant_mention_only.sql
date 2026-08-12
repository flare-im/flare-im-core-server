-- 给已有库的 conversation_participants 补 mention_only（只接收提到我的消息）。
-- 可重复执行。默认 FALSE 表示照旧全部接收，语义与升级前一致。

ALTER TABLE conversation_participants
    ADD COLUMN IF NOT EXISTS mention_only BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN conversation_participants.mention_only IS
    '只接收提到我的消息：其余消息照常投递，但不产生离线推送。与 muted 正交';
