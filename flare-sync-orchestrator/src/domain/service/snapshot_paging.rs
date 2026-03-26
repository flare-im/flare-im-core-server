/// 快照分页游标：`millis|conversation_id`，按 `(last_timestamp_ms, conversation_id)` 字典序翻页。
pub fn parse_snapshot_cursor(cursor: &str) -> Option<(i64, String)> {
    if cursor.is_empty() {
        return None;
    }
    let (ms, cid) = cursor.split_once('|')?;
    let ms = ms.parse::<i64>().ok()?;
    Some((ms, cid.to_string()))
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
