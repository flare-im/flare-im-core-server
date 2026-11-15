//! 推送决策链的读取（管理员排障）：一条消息的全部记录，或一个用户最近的几条消息。
//!
//! 记录由 push-server、push-worker 与本服务按 `flare_im_contracts::push_trace` 的格式写在推送 Redis
//! （与设备绑定同一个实例）；这里只读。读不懂的记录跳过，过期的消息不出现。

use std::sync::Arc;
use std::time::Duration;

use flare_im_contracts::push_trace::{PushTraceEntry, trace_index_key, trace_list_key};
use flare_im_service_kit::redis_connection::SharedRedisConnection;

/// 一次读取（含建连）的时限。管理员在等，慢了就报不可用。
const READ_BUDGET: Duration = Duration::from_secs(3);

/// 读出来的一条消息的决策链。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushTraceRead {
    pub trace_key: String,
    /// 第一条记录的时刻：列最近的消息时取索引里的分数（记录被裁剪过也不变），
    /// 查一条消息时取最早的那条记录。
    pub first_at_ms: i64,
    /// 按时刻排好（几个服务各自批量写，写入顺序不一定是时间顺序）。
    pub entries: Vec<PushTraceEntry>,
}

impl PushTraceRead {
    /// 整条链的会话：第一条带会话的记录的 `conv`。
    pub fn conversation_id(&self) -> &str {
        self.entries
            .iter()
            .find_map(|entry| entry.conversation_id().filter(|id| !id.is_empty()))
            .unwrap_or_default()
    }
}

/// 读失败：推送 Redis 连不上、超时或回了错。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceReadUnavailable;

impl std::fmt::Display for TraceReadUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("push trace store unavailable")
    }
}

impl std::error::Error for TraceReadUnavailable {}

/// 解码一条消息的记录并按时刻排序；一条都读不懂时为 `None`。
fn decoded(trace_key: String, raw: &[String], indexed_at_ms: Option<i64>) -> Option<PushTraceRead> {
    let mut entries: Vec<PushTraceEntry> = raw
        .iter()
        .filter_map(|raw| PushTraceEntry::decode(raw))
        .collect();
    if entries.is_empty() {
        return None;
    }
    entries.sort_by_key(|entry| entry.at_ms);
    let first_at_ms = indexed_at_ms.unwrap_or(entries[0].at_ms);
    Some(PushTraceRead {
        trace_key,
        first_at_ms,
        entries,
    })
}

pub struct PushTraceStore {
    redis: Arc<SharedRedisConnection>,
}

impl PushTraceStore {
    pub fn new(redis: Arc<SharedRedisConnection>) -> Self {
        Self { redis }
    }

    /// 一条消息的决策链；没有（从没记过或已过期）时为 `None`。
    pub async fn trace(
        &self,
        tenant_id: &str,
        user_id: &str,
        trace_key: &str,
    ) -> Result<Option<PushTraceRead>, TraceReadUnavailable> {
        let key = trace_list_key(tenant_id, user_id, trace_key);
        let raw: Vec<String> = tokio::time::timeout(READ_BUDGET, async {
            let mut connection = self.redis.get().await?;
            redis::cmd("LRANGE")
                .arg(&key)
                .arg(0)
                .arg(-1)
                .query_async::<Vec<String>>(&mut connection)
                .await
        })
        .await
        .map_err(|_| TraceReadUnavailable)?
        .map_err(|_| TraceReadUnavailable)?;
        Ok(decoded(trace_key.to_string(), &raw, None))
    }

    /// 一个用户最近的 `limit` 条消息的决策链，按开始时间从新到旧；索引里还在、记录已过期的跳过。
    pub async fn recent(
        &self,
        tenant_id: &str,
        user_id: &str,
        limit: usize,
    ) -> Result<Vec<PushTraceRead>, TraceReadUnavailable> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let index = trace_index_key(tenant_id, user_id);
        let stop = i64::try_from(limit).unwrap_or(i64::MAX).saturating_sub(1);
        let (members, lists) = tokio::time::timeout(READ_BUDGET, async {
            let mut connection = self.redis.get().await?;
            let members: Vec<(String, i64)> = redis::cmd("ZREVRANGE")
                .arg(&index)
                .arg(0)
                .arg(stop)
                .arg("WITHSCORES")
                .query_async(&mut connection)
                .await?;
            if members.is_empty() {
                return Ok((members, Vec::new()));
            }
            let mut pipeline = redis::pipe();
            for (trace_key, _) in &members {
                pipeline
                    .cmd("LRANGE")
                    .arg(trace_list_key(tenant_id, user_id, trace_key))
                    .arg(0)
                    .arg(-1);
            }
            let lists: Vec<Vec<String>> = pipeline.query_async(&mut connection).await?;
            Ok::<_, redis::RedisError>((members, lists))
        })
        .await
        .map_err(|_| TraceReadUnavailable)?
        .map_err(|_| TraceReadUnavailable)?;
        Ok(members
            .into_iter()
            .zip(lists)
            .filter_map(|((trace_key, first_at_ms), raw)| {
                decoded(trace_key, &raw, Some(first_at_ms))
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_im_contracts::push_trace::TraceStage;

    #[test]
    fn entries_are_sorted_by_time_and_unreadable_ones_skipped() {
        let receipt = PushTraceEntry::new(30, TraceStage::Receipt, "d-1").with_conversation("c-1");
        let route = PushTraceEntry::new(10, TraceStage::Route, "d-1").with_conversation("c-1");
        let raw = vec![receipt.encode(), "not json".to_string(), route.encode()];
        let read = decoded("m-1".to_string(), &raw, None).expect("two readable entries");
        assert_eq!(read.entries, vec![route, receipt]);
        assert_eq!(read.first_at_ms, 10);
        assert_eq!(read.conversation_id(), "c-1");
        let indexed = decoded("m-1".to_string(), &raw, Some(5)).unwrap();
        assert_eq!(indexed.first_at_ms, 5, "索引里的分数优先");
    }

    #[test]
    fn a_trace_without_readable_entries_is_absent() {
        assert!(decoded("m-1".to_string(), &["{}".to_string()], None).is_none());
        assert!(decoded("m-1".to_string(), &[], Some(1)).is_none());
    }

    #[test]
    fn the_conversation_skips_entries_without_one() {
        let read = decoded(
            "m-1".to_string(),
            &[
                PushTraceEntry::new(1, TraceStage::Vendor, "d-1").encode(),
                PushTraceEntry::new(2, TraceStage::Receipt, "d-1")
                    .with_conversation("c-9")
                    .encode(),
            ],
            None,
        )
        .unwrap();
        assert_eq!(read.conversation_id(), "c-9");
    }
}
