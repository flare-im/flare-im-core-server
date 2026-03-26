//! 同步批大小与分页上限（保护读侧与网关，保证「可渐进拉齐」而非单次巨包）。

/// 单会话快照内消息条数上限（与存储读侧 clamp 对齐思路）。
pub const MAX_MESSAGES_PER_CONVERSATION: i32 = 100;

/// 单次事件查询条数上限。
pub const MAX_QUERY_EVENTS_LIMIT: i32 = 500;

/// 快照分页：每页最少会话数（用于 `has_more` 计算）。
pub const MIN_SNAPSHOT_PAGE_SIZE: i32 = 1;

#[inline]
pub fn clamp_messages_per_conversation(n: i32) -> i32 {
    if n <= 0 {
        0
    } else {
        n.min(MAX_MESSAGES_PER_CONVERSATION)
    }
}

#[inline]
pub fn clamp_query_events_limit(n: i32) -> i32 {
    if n <= 0 {
        MAX_QUERY_EVENTS_LIMIT.min(100)
    } else {
        n.min(MAX_QUERY_EVENTS_LIMIT)
    }
}
