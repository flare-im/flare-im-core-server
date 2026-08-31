-- 把 messages.status 里的旧编码矫正回 proto MessageStatus。
--
-- 旧的 fsm_state_to_status_int 自造了一套值：RECALLED→6、DELETED_HARD→7、
-- DELETED_SOFT→8。而 proto 里 RECALLED=5、DELETED=6，7/8 根本不存在。
-- 读写两侧用的是同一套错值，服务端内部自洽，所以线上看不出来；暴露它的是
-- 客户端——SDK 按 status == RECALLED(5) 判 is_recalled，从服务端全量同步时
-- 读到 6 判定为 false，撤回过的消息会带着原文重新显示。
--
-- 不能只看 status 判断该迁哪个值：迁移前的 6 是撤回，迁移后的 6 是删除，
-- 光凭这一列区分不了，二次运行会把已经迁好的删除又降成撤回。所以撤回一律
-- 以 message_operation_history 里的撤回事件为准，这样重复执行是空操作。

-- 撤回：旧值 6 且确有撤回事件 → proto RECALLED(5)。
-- 写侧按 server_id 或 client_msg_id 认消息，两边都比对。
UPDATE messages m
SET status = 5
WHERE m.status = 6
  AND EXISTS (
      SELECT 1 FROM message_operation_history h
      WHERE h.tenant_id = m.tenant_id
        AND h.operation_type = 'EVENT_MESSAGE_RECALL'
        AND h.message_id IN (m.server_id, m.client_msg_id)
  );

-- 删除：越界的 7/8 收敛到 proto DELETED(6)。
-- 硬删软删的区别本就不该编码进 status，它由 MessageDeleteEvent 承载。
UPDATE messages SET status = 6 WHERE status IN (7, 8);
