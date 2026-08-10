-- 给已有库的 conversation_participants 补 visible_from_seq。
-- 可重复执行。默认 0 表示不限，语义与旧行为一致。

ALTER TABLE conversation_participants
    ADD COLUMN IF NOT EXISTS visible_from_seq BIGINT NOT NULL DEFAULT 0;

COMMENT ON COLUMN conversation_participants.visible_from_seq IS
    '该成员可见的历史下限：只能读到 seq > 此值的消息；0=不限。业务层（如群历史可见性策略）决定是否设值';
