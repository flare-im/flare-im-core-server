/// 快照分页游标：`millis|conversation_id`，按 `(last_timestamp_ms, conversation_id)` 字典序翻页。
/// 兼容纯 `millis` 形式（客户端持久化的是 u64 毫秒水位）→ 视为 `(ms, "")`。
/// 旧实现对纯 ms 游标返回 None（当冷启全量），导致增量列表同步从未生效。
pub fn parse_snapshot_cursor(cursor: &str) -> Option<(i64, String)> {
    if cursor.is_empty() {
        return None;
    }
    match cursor.split_once('|') {
        Some((ms, cid)) => {
            let ms = ms.parse::<i64>().ok()?;
            Some((ms, cid.to_string()))
        }
        None => cursor.parse::<i64>().ok().map(|ms| (ms, String::new())),
    }
}

pub fn build_snapshot_cursor(ms: i64, conversation_id: &str) -> String {
    format!("{ms}|{conversation_id}")
}

pub fn ts_millis(ts: Option<&prost_types::Timestamp>) -> i64 {
    let Some(ts) = ts else {
        return 0;
    };
    (ts.seconds * 1000) + (ts.nanos as i64 / 1_000_000)
}
