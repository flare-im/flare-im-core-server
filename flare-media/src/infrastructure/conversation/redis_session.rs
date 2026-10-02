use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use flare_server_core::error::{ErrorCode, Result, map_infra_error};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use tokio::sync::Mutex;

use crate::domain::model::{UploadSession, UploadSessionStatus};
use crate::domain::repository::{SessionUpdate, UploadSessionStore};

/// 会话过期后记录在 Redis 里再留这么久，供回收任务读出对象 key、分片上传 id 释放资源。
/// 回收失败按租期重试，这段时间覆盖存储短时故障；超过后记录自然过期，残留交给存储侧
/// 生命周期规则。
const RECLAIM_RETENTION_SECONDS: i64 = 24 * 3600;

/// 领取一条过期会话：分值（过期时刻或上一次租期）早于截止时刻才领，领到后分值改成本次租期，
/// 租期内其他副本领不到。记录已不在（过期后又过了保留期）时顺手清掉索引项。
/// 两个键都经 KEYS 声明，Dragonfly 的脚本键检查与 Redis 一致。
const CLAIM_SCRIPT: &str = r#"
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if not score or tonumber(score) >= tonumber(ARGV[2]) then
  return false
end
local payload = redis.call('GET', KEYS[2])
if not payload then
  redis.call('ZREM', KEYS[1], ARGV[1])
  return false
end
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
return payload
"#;

/// 比较后写入：记录仍是读出时的原文才写入新记录并更新回收索引，否则不动、返回 0。
/// 两个键都经 KEYS 声明。
const COMPARE_AND_SET_SCRIPT: &str = r#"
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return 0
end
redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
redis.call('ZADD', KEYS[2], ARGV[4], ARGV[5])
return 1
"#;

/// [`UploadSessionStore::update_session`] 遇到其他副本并发写入时的重读次数上限（含第一次）。
const UPDATE_ATTEMPTS: u32 = 10;

/// 比较失败后的退避：随次数增长，并加随机抖动，错开各副本的下一次读改写。
fn update_backoff(attempt: u32) -> std::time::Duration {
    let jitter = (uuid::Uuid::new_v4().as_u128() % 5) as u64;
    std::time::Duration::from_millis(u64::from(attempt) * 2 + jitter)
}

#[derive(Clone)]
pub struct RedisUploadSessionStore {
    namespace: String,
    ttl: Duration,
    connection: Arc<Mutex<ConnectionManager>>,
}

impl RedisUploadSessionStore {
    pub async fn new(
        redis_url: &str,
        namespace: impl Into<String>,
        ttl_seconds: i64,
    ) -> Result<Self> {
        let client = redis::Client::open(redis_url)
            .map_err(|e| map_infra_error(e, ErrorCode::NetworkError, "redis client open"))?;
        let connection = client
            .get_connection_manager()
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::NetworkError, "redis connection manager"))?;
        Ok(Self {
            namespace: namespace.into(),
            ttl: Duration::seconds(ttl_seconds.max(60)),
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    fn key(&self, upload_id: &str) -> String {
        format!("{}:{}", self.namespace, upload_id)
    }

    /// 待回收索引：成员是 upload_id，分值是会话过期时刻（毫秒），领取后改为租期。
    /// upload_id 都是 UUID，不会与这个键重名。
    fn reclaim_index_key(&self) -> String {
        format!("{}:reclaim", self.namespace)
    }

    fn ensure_session_defaults(session: &mut UploadSession) {
        let now = Utc::now();
        if session.created_at.timestamp() == 0 {
            session.created_at = now;
        }
        session.updated_at = now;
        if session.status == UploadSessionStatus::Completed
            || session.status == UploadSessionStatus::Aborted
        {
            session.expires_at = now + Duration::hours(1);
        }
    }

    fn ttl_for_session(&self, session: &UploadSession) -> u64 {
        let now = Utc::now();
        let diff = (session.expires_at - now).num_seconds();
        let clamped = diff.max(60).min(self.ttl.num_seconds());
        (clamped + RECLAIM_RETENTION_SECONDS) as u64
    }

    /// 写入前的记录：补默认字段后序列化。
    fn prepared(&self, session: &UploadSession) -> Result<(UploadSession, String)> {
        let mut session = session.clone();
        Self::ensure_session_defaults(&mut session);
        let payload = serde_json::to_string(&session).map_err(|e| {
            map_infra_error(e, ErrorCode::SerializationError, "serialize upload session")
        })?;
        Ok((session, payload))
    }

    async fn write_session(&self, session: &UploadSession, action: &'static str) -> Result<()> {
        let (session, payload) = self.prepared(session)?;
        let mut conn = self.connection.lock().await;
        // 记录与索引同一事务写入：有记录就一定在索引里，回收任务不会漏掉它。
        let _: () = redis::pipe()
            .atomic()
            .set_ex(
                self.key(&session.upload_id),
                payload,
                self.ttl_for_session(&session),
            )
            .ignore()
            .zadd(
                self.reclaim_index_key(),
                &session.upload_id,
                session.expires_at.timestamp_millis(),
            )
            .ignore()
            .query_async(&mut *conn)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::NetworkError, action))?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl UploadSessionStore for RedisUploadSessionStore {
    async fn create_session(&self, session: &UploadSession) -> Result<()> {
        self.write_session(session, "failed to create upload session in redis")
            .await
    }

    async fn get_session(&self, upload_id: &str) -> Result<Option<UploadSession>> {
        // 过期会话不再可用；记录留给回收任务释放资源，这里不删。
        Ok(self
            .get_session_record(upload_id)
            .await?
            .filter(|session| session.expires_at >= Utc::now()))
    }

    async fn get_session_record(&self, upload_id: &str) -> Result<Option<UploadSession>> {
        let mut conn = self.connection.lock().await;
        let payload: Option<String> = conn.get(self.key(upload_id)).await.map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::NetworkError,
                "failed to fetch upload session from redis",
            )
        })?;
        let Some(payload) = payload else {
            return Ok(None);
        };
        let session: UploadSession = serde_json::from_str(&payload).map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::DeserializationError,
                "failed to deserialize upload session",
            )
        })?;
        Ok(Some(session))
    }

    async fn upsert_session(&self, session: &UploadSession) -> Result<()> {
        self.write_session(session, "failed to upsert upload session in redis")
            .await
    }

    /// 乐观并发：读出原文、改写，再用脚本比较原文后写入；其间被别人写过就重读重来。
    /// 读、改、写期间一直持有本副本的连接锁，本副本内的改写因此依次进行、互不冲突；
    /// 只有其他副本的写入会让比较失败，失败后短暂随机退避再重来。
    async fn update_session(
        &self,
        upload_id: &str,
        apply: &SessionUpdate<'_>,
    ) -> Result<Option<UploadSession>> {
        let key = self.key(upload_id);
        let script = redis::Script::new(COMPARE_AND_SET_SCRIPT);
        for attempt in 1..=UPDATE_ATTEMPTS {
            let mut conn = self.connection.lock().await;
            let current: Option<String> = conn.get(&key).await.map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::NetworkError,
                    "failed to fetch upload session from redis",
                )
            })?;
            let Some(current) = current else {
                return Ok(None);
            };
            let mut session: UploadSession = serde_json::from_str(&current).map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::DeserializationError,
                    "failed to deserialize upload session",
                )
            })?;
            if !apply(&mut session)? {
                return Ok(Some(session));
            }
            let (session, payload) = self.prepared(&session)?;
            let written: i64 = script
                .key(&key)
                .key(self.reclaim_index_key())
                .arg(&current)
                .arg(&payload)
                .arg(self.ttl_for_session(&session))
                .arg(session.expires_at.timestamp_millis())
                .arg(upload_id)
                .invoke_async(&mut *conn)
                .await
                .map_err(|e| {
                    map_infra_error(
                        e,
                        ErrorCode::NetworkError,
                        "failed to update upload session in redis",
                    )
                })?;
            drop(conn);
            if written == 1 {
                return Ok(Some(session));
            }
            if attempt < UPDATE_ATTEMPTS {
                tokio::time::sleep(update_backoff(attempt)).await;
            }
        }
        Err(flare_server_core::flare_err!(
            ErrorCode::ServiceUnavailable,
            "upload session is being updated concurrently; retry"
        ))
    }

    async fn delete_session(&self, upload_id: &str) -> Result<()> {
        let mut conn = self.connection.lock().await;
        let _: () = redis::pipe()
            .atomic()
            .del(self.key(upload_id))
            .ignore()
            .zrem(self.reclaim_index_key(), upload_id)
            .ignore()
            .query_async(&mut *conn)
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::NetworkError,
                    "failed to delete upload session from redis",
                )
            })?;
        Ok(())
    }

    async fn claim_expired_sessions(
        &self,
        expired_before: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<UploadSession>> {
        let index = self.reclaim_index_key();
        let cutoff = expired_before.timestamp_millis();
        let mut conn = self.connection.lock().await;
        let candidates: Vec<String> = conn
            .zrangebyscore_limit(&index, "-inf", format!("({cutoff}"), 0, limit as isize)
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::NetworkError,
                    "failed to list expired upload sessions",
                )
            })?;
        let script = redis::Script::new(CLAIM_SCRIPT);
        let mut claimed = Vec::with_capacity(candidates.len());
        for upload_id in candidates {
            // 每个候选单独原子领取：两次调用之间别的副本可能已领走或会话已续期。
            let payload: Option<String> = script
                .key(&index)
                .key(self.key(&upload_id))
                .arg(&upload_id)
                .arg(cutoff)
                .arg(lease_until.timestamp_millis())
                .invoke_async(&mut *conn)
                .await
                .map_err(|e| {
                    map_infra_error(
                        e,
                        ErrorCode::NetworkError,
                        "failed to claim expired upload session",
                    )
                })?;
            let Some(payload) = payload else {
                continue;
            };
            match serde_json::from_str::<UploadSession>(&payload) {
                Ok(session) => claimed.push(session),
                Err(err) => {
                    // 读不出的记录永远回收不了，删掉以免每轮都领到它。
                    tracing::warn!(
                        error = %err,
                        upload_id = %upload_id,
                        "dropping undecodable expired upload session"
                    );
                    let _: () = redis::pipe()
                        .atomic()
                        .del(self.key(&upload_id))
                        .ignore()
                        .zrem(&index, &upload_id)
                        .ignore()
                        .query_async(&mut *conn)
                        .await
                        .map_err(|e| {
                            map_infra_error(
                                e,
                                ErrorCode::NetworkError,
                                "failed to drop undecodable upload session",
                            )
                        })?;
                }
            }
        }
        Ok(claimed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::model::UploadSession;
    use std::collections::HashMap;

    fn session(upload_id: &str, expires_at: DateTime<Utc>) -> UploadSession {
        UploadSession {
            upload_id: upload_id.to_string(),
            tenant_id: "tenant-a".to_string(),
            file_id: Some(format!("file-{upload_id}")),
            file_name: "a.txt".to_string(),
            mime_type: "text/plain".to_string(),
            file_type: "FILE_TYPE_FILE".to_string(),
            chunk_size: 1,
            total_size: Some(1),
            uploaded_size: 0,
            uploaded_chunks: Vec::new(),
            uploaded_parts: Vec::new(),
            transport_kind: None,
            storage_upload_id: None,
            bucket: None,
            object_key: None,
            single_part_upload_url: None,
            file_fingerprint: None,
            head_tail_sha256: None,
            full_sha256: None,
            total_parts: None,
            user_id: "owner".to_string(),
            namespace: None,
            business_tag: None,
            trace_id: None,
            metadata: HashMap::new(),
            status: UploadSessionStatus::Pending,
            expires_at,
            created_at: expires_at - Duration::hours(1),
            updated_at: expires_at - Duration::hours(1),
            completion: None,
        }
    }

    /// 在一次性 Redis 上跑：`FLARE_TEST_REDIS_URL` 指向只供本测试使用的实例
    /// （测试在随机命名空间下读写，并在结束时删掉它写过的键）。
    #[tokio::test]
    #[ignore = "requires FLARE_TEST_REDIS_URL"]
    async fn expired_sessions_stay_readable_for_reclaim_and_are_claimed_once_per_lease() {
        let url = std::env::var("FLARE_TEST_REDIS_URL").expect("FLARE_TEST_REDIS_URL");
        let namespace = format!("media-upload-reclaim-test-{}", uuid::Uuid::new_v4());
        let store = RedisUploadSessionStore::new(&url, namespace.clone(), 3600)
            .await
            .unwrap();
        let other_replica = RedisUploadSessionStore::new(&url, namespace.clone(), 3600)
            .await
            .unwrap();
        let now = Utc::now();
        let expired = session(
            &uuid::Uuid::new_v4().to_string(),
            now - Duration::minutes(10),
        );
        let live = session(
            &uuid::Uuid::new_v4().to_string(),
            now + Duration::minutes(10),
        );
        store.create_session(&expired).await.unwrap();
        store.create_session(&live).await.unwrap();

        // 过期会话对上传路径不可见，但记录还在，供回收。
        assert!(
            store
                .get_session(&expired.upload_id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(store.get_session(&live.upload_id).await.unwrap().is_some());
        // 回收任务复查用的原始记录：过期了也读得到。
        let record = store
            .get_session_record(&expired.upload_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            record.expires_at.timestamp_millis(),
            expired.expires_at.timestamp_millis()
        );
        assert!(
            store
                .get_session_record(&uuid::Uuid::new_v4().to_string())
                .await
                .unwrap()
                .is_none()
        );

        let lease = now + Duration::minutes(5);
        let claimed = store.claim_expired_sessions(now, lease, 10).await.unwrap();
        assert_eq!(
            claimed
                .iter()
                .map(|s| s.upload_id.as_str())
                .collect::<Vec<_>>(),
            vec![expired.upload_id.as_str()]
        );
        assert_eq!(claimed[0].file_id, expired.file_id);
        // 租期内另一个副本领不到同一会话。
        assert!(
            other_replica
                .claim_expired_sessions(now, lease, 10)
                .await
                .unwrap()
                .is_empty()
        );
        // 释放失败、没有删除：租期过后可以重新领取。
        let reclaimed = other_replica
            .claim_expired_sessions(
                lease + Duration::seconds(1),
                lease + Duration::minutes(5),
                10,
            )
            .await
            .unwrap();
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].upload_id, expired.upload_id);
        // 收尾删除后记录与索引都没了；之后到期的只剩 live。
        store.delete_session(&expired.upload_id).await.unwrap();
        let later = store
            .claim_expired_sessions(now + Duration::days(2), now + Duration::days(3), 10)
            .await
            .unwrap();
        assert_eq!(
            later
                .iter()
                .map(|s| s.upload_id.as_str())
                .collect::<Vec<_>>(),
            vec![live.upload_id.as_str()]
        );

        // 记录过了保留期自然消失、索引还在：再次领取时只清掉索引项。
        let mut conn = store.connection.lock().await;
        let _: () = conn.del(store.key(&live.upload_id)).await.unwrap();
        drop(conn);
        assert!(
            store
                .claim_expired_sessions(now + Duration::days(4), now + Duration::days(5), 10)
                .await
                .unwrap()
                .is_empty()
        );
        let mut conn = store.connection.lock().await;
        let remaining: usize = conn.zcard(store.reclaim_index_key()).await.unwrap();
        let records: usize = conn
            .exists(vec![
                store.key(&expired.upload_id),
                store.key(&live.upload_id),
            ])
            .await
            .unwrap();
        assert_eq!((remaining, records), (0, 0));
    }
    /// 领取之后被续期（完成直传开始合并）：记录与索引都换成新的过期时刻，原租期过后也领
    /// 不到；回收任务复查时读到的是续期后的记录。
    #[tokio::test]
    #[ignore = "requires FLARE_TEST_REDIS_URL"]
    async fn a_session_renewed_after_its_claim_is_not_claimed_again() {
        let url = std::env::var("FLARE_TEST_REDIS_URL").expect("FLARE_TEST_REDIS_URL");
        let namespace = format!("media-upload-renew-test-{}", uuid::Uuid::new_v4());
        let store = RedisUploadSessionStore::new(&url, namespace, 3600)
            .await
            .unwrap();
        let now = Utc::now();
        let mut renewed = session(
            &uuid::Uuid::new_v4().to_string(),
            now - Duration::minutes(10),
        );
        store.create_session(&renewed).await.unwrap();
        let lease = now + Duration::minutes(5);
        assert_eq!(
            store
                .claim_expired_sessions(now, lease, 10)
                .await
                .unwrap()
                .len(),
            1
        );

        renewed.expires_at = now + Duration::hours(1);
        store.upsert_session(&renewed).await.unwrap();
        let record = store
            .get_session_record(&renewed.upload_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            record.expires_at.timestamp_millis(),
            renewed.expires_at.timestamp_millis()
        );
        assert!(
            store
                .get_session(&renewed.upload_id)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .claim_expired_sessions(
                    lease + Duration::seconds(1),
                    lease + Duration::minutes(10),
                    10
                )
                .await
                .unwrap()
                .is_empty()
        );

        store.delete_session(&renewed.upload_id).await.unwrap();
        let mut conn = store.connection.lock().await;
        let remaining: usize = conn.zcard(store.reclaim_index_key()).await.unwrap();
        assert_eq!(remaining, 0);
    }

    /// 多个副本同时在同一会话上改字段（并发提交分片）：每次改写都落在最新记录上，谁也
    /// 盖不掉谁；改写函数拒绝时不写入；记录不存在时答 `None`。修复前提交分片是读出整份
    /// 记录后写回，并发提交会互相覆盖。
    #[tokio::test]
    #[ignore = "requires FLARE_TEST_REDIS_URL"]
    async fn concurrent_field_updates_do_not_overwrite_each_other() {
        use crate::domain::model::UploadedPartRecord;

        let url = std::env::var("FLARE_TEST_REDIS_URL").expect("FLARE_TEST_REDIS_URL");
        let namespace = format!("media-upload-update-test-{}", uuid::Uuid::new_v4());
        let mut replicas = Vec::new();
        for _ in 0..4 {
            replicas.push(Arc::new(
                RedisUploadSessionStore::new(&url, namespace.clone(), 3600)
                    .await
                    .unwrap(),
            ));
        }
        let base = session(
            &uuid::Uuid::new_v4().to_string(),
            Utc::now() + Duration::minutes(10),
        );
        replicas[0].create_session(&base).await.unwrap();

        const PARTS: u32 = 16;
        let mut tasks = Vec::new();
        for part_number in 1..=PARTS {
            let store = replicas[part_number as usize % replicas.len()].clone();
            let upload_id = base.upload_id.clone();
            tasks.push(tokio::spawn(async move {
                store
                    .update_session(&upload_id, &|current| {
                        current.merge_uploaded_part(UploadedPartRecord {
                            part_number,
                            etag: format!("etag-{part_number}"),
                            size: 1,
                            sha256: None,
                        });
                        Ok(true)
                    })
                    .await
            }));
        }
        for task in tasks {
            assert!(task.await.unwrap().unwrap().is_some());
        }
        let stored = replicas[0]
            .get_session_record(&base.upload_id)
            .await
            .unwrap()
            .unwrap();
        let mut parts: Vec<u32> = stored
            .uploaded_parts
            .iter()
            .map(|part| part.part_number)
            .collect();
        parts.sort_unstable();
        assert_eq!(parts, (1..=PARTS).collect::<Vec<_>>());

        // 改写函数拒绝：原样返回它的错误，记录不变。
        let err = replicas[1]
            .update_session(&base.upload_id, &|current| {
                current.uploaded_parts.clear();
                Err(flare_server_core::flare_err!(
                    ErrorCode::InvalidParameter,
                    "rejected"
                ))
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Some(ErrorCode::InvalidParameter));
        let kept = replicas[1]
            .get_session_record(&base.upload_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(kept.uploaded_parts.len(), PARTS as usize);
        assert!(
            replicas[1]
                .update_session(&uuid::Uuid::new_v4().to_string(), &|_| Ok(true))
                .await
                .unwrap()
                .is_none()
        );

        replicas[0].delete_session(&base.upload_id).await.unwrap();
        let mut conn = replicas[0].connection.lock().await;
        let remaining: usize = conn.zcard(replicas[0].reclaim_index_key()).await.unwrap();
        assert_eq!(remaining, 0);
    }
}
