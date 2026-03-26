/// 客户端同步意图（对应工业界「初始化 / 离线追赶 / 增量」三阶段，由调用方在元数据中声明或隐含在参数里）。
///
/// - **初始化**：`SYNC_KIND_SYNC_SNAPSHOT`（或会话列表类 kind）+ 可选分页 `snapshot_cursor`。
/// - **离线追赶**：`SYNC_KIND_QUERY_EVENTS` 在本地 `after_seq` 与服务器 `max_seq` 之间补齐关键事件。
/// - **增量**：长连接实时 + 周期性 `QUERY_EVENTS` / `CONVERSATION_MAX_SEQ` 校验水位。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncIntent {
    /// 冷启动或换设备：快照为主，事件为辅。
    InitialBootstrap,
    /// 断线重连、App 从后台恢复：事件回放补齐状态机。
    OfflineCatchUp,
    /// 已在线：小窗口校验或补偿。
    Incremental,
}

impl SyncIntent {
    /// 根据是否携带会话内事件游标推断意图（启发式，供日志与限流策略使用）。
    pub fn from_event_anchor(after_seq: i64, max_seq_hint: Option<i64>) -> Self {
        match (after_seq, max_seq_hint) {
            (0, _) => Self::InitialBootstrap,
            (n, Some(m)) if m > n + 1 => Self::OfflineCatchUp,
            _ => Self::Incremental,
        }
    }
}
