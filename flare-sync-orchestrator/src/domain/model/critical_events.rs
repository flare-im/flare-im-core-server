use flare_proto::common::EventType;

/// 默认必须参与离线/增量回放的事件类型（与 `im_sync_best_practice` 一致：影响最终状态、需参与 seq 补齐）。
pub const DEFAULT_CRITICAL_EVENT_TYPES: [i32; 9] = [
    EventType::EventMessageRecall as i32,
    EventType::EventMessageEdit as i32,
    EventType::EventMessageDelete as i32,
    EventType::EventConversationUpdate as i32,
    EventType::EventConversationDelete as i32,
    EventType::EventPin as i32,
    EventType::EventUnpin as i32,
    EventType::EventMark as i32,
    EventType::EventUnmark as i32,
];

/// 若请求未指定 `event_types`，则补全为关键事件集合，避免客户端漏传导致状态不一致。
pub fn normalize_query_event_types(event_types: &mut Vec<i32>) {
    if event_types.is_empty() {
        *event_types = DEFAULT_CRITICAL_EVENT_TYPES.to_vec();
    }
}
