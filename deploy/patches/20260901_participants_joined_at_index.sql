-- conversation_participants 按 (joined_at, user_id) 排序的覆盖索引。
--
-- 现有索引只到 (tenant_id, conversation_id)，而两条热路径都按
-- `ORDER BY joined_at ASC, user_id ASC` 取数：
--
--   1. 会话摘要的成员预览 LATERAL（LIMIT 10）——线上 EXPLAIN 显示对十万人群
--      做 top-N heapsort 扫 100,003 行，单会话 106ms；它是
--      ConversationBootstrap 剩余耗时的大头（bootstrap 是 sync_snapshot 的主体，
--      直接影响首屏）。
--   2. 成员分页的 keyset 游标——十万人群的事件扇出（已读回执等）要遍历全部成员。
--
-- 加上这个索引后两者都变成有序索引扫描 + LIMIT，不再排序。
--
-- 用 CONCURRENTLY：普通 CREATE INDEX 会持写锁，成员表在大规模部署下可能很大。
-- 注意 CONCURRENTLY 不能在事务块里执行——本文件由 db-migrate 用 psql -f 直接跑，
-- 每条语句自动提交，满足要求。
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_conversation_participants_conv_joined
    ON conversation_participants (tenant_id, conversation_id, joined_at, user_id);
