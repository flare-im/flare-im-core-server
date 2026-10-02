-- 会话 last_message_seq 存量回填（W9-S3 F1 的存量数据）。
--
-- 背景：会话服务的事件消费者曾把租户写死为 "0"，非 0 租户的 conversations.last_message_seq
-- 从未被推进（一直为 NULL）。修复后新事件按事件自己的租户写入，这一列在会话的下一条消息时
-- 自愈；本脚本把已有行一次补齐，不必等新消息。
--
-- 读路径对这一列的依赖：客户端会话列表（load_bootstrap）取 GREATEST(列, 消息尾 seq)，不靠它；
-- 直接读这一列的是 SearchConversations 返回的 last_message_seq，以及删除会话时推进成员
-- last_read_seq / last_sync_seq 与 user_sync_cursor 的 tail。回填前，非 0 租户在这两处读到 0。
--
-- 语义：按 (tenant_id, conversation_id) 对齐，把列抬到该会话 messages 的最大 seq；只升不降，
-- 已经更大的值保留（清除历史后，列可以合法地大于剩余消息的 seq）；不改 updated_at，不改变
-- 会话列表排序。可重复执行，第二次为 UPDATE 0。逐会话按 seq 倒序取一行，与会话列表的消息尾
-- 查询同一走法。
--
-- 不在本脚本内：
--   * 成员 unread_count 与 last_read_seq。已读回执同样曾按租户 "0" 处理（生产代码里成员
--     last_read_seq 只由这个消费者推进），非 0 租户成员的 last_read_seq 也没有推进，按它重算
--     未读会把已读消息算成未读，所以不回填；成员下一次已读回执会重算这两列（读到会话尾时
--     未读归 0）。
--   * 曾被他租户事件改写过的租户 0 同 ID 会话。只升不降的回填无法判定哪部分是串写的，
--     下面的只读查询列出候选，人工核对：
--       SELECT z.conversation_id, z.last_message_seq,
--              (SELECT MAX(m.seq) FROM messages m
--                WHERE m.tenant_id = '0' AND m.conversation_id = z.conversation_id) AS max_seq
--       FROM conversations z
--       WHERE z.tenant_id = '0'
--         AND EXISTS (SELECT 1 FROM conversations o
--                     WHERE o.conversation_id = z.conversation_id AND o.tenant_id <> '0');
--
-- 用法（连接参数用 PG* 环境变量，口令走 PGPASSFILE 或 ~/.pgpass，不进命令行）：
--   预览（执行后回滚，只看影响行数）：
--     psql -v ON_ERROR_STOP=1 -c 'BEGIN' -f scripts/backfill_conversation_last_message_seq.sql -c 'ROLLBACK'
--   执行：
--     psql -v ON_ERROR_STOP=1 --single-transaction -f scripts/backfill_conversation_last_message_seq.sql
-- 用例：flare-conversation/tests/conversation_event_tenant_pg.rs。

WITH tail AS (
    SELECT c.tenant_id, c.conversation_id, last_message.seq AS max_seq
    FROM conversations c
    CROSS JOIN LATERAL (
        SELECT m.seq
        FROM messages m
        WHERE m.tenant_id = c.tenant_id
          AND m.conversation_id = c.conversation_id
        ORDER BY m.seq DESC
        LIMIT 1
    ) last_message
)
UPDATE conversations c
SET last_message_seq = tail.max_seq
FROM tail
WHERE c.tenant_id = tail.tenant_id
  AND c.conversation_id = tail.conversation_id
  AND COALESCE(c.last_message_seq, 0) < tail.max_seq;
