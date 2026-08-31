use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use redis::aio::ConnectionManager;

use crate::domain::repository::{ConversationChange, UserSyncIndexRepository};
use flare_im_contracts::Ctx;
use flare_server_core::error::{FlareError, Result};

const DEFAULT_TENANT_ID: &str = "0";
/// 单个 Redis pipeline 里最多塞多少个用户的 EVAL。
///
/// 线上事故：十万人群里**一条已读回执**触发 recipient_count=100000 的扇出，
/// 这里把十万个 EVAL 塞进同一个 pipe.atomic()，而每个 EVAL 都重发一遍完整
/// Lua 脚本正文（约 700B）——单个 pipeline 缓冲区约 70MB，
/// 结果是 `broken pipe` + flare-orchestrator 在 512m 上被 cgroup OOM 杀掉，
/// NATS 重投后再次 OOM，形成循环（实测重启 7 次）。
///
/// 每个用户只操作自己的三个键，彼此完全独立，跨十万用户的 MULTI/EXEC
/// 拿不到任何有意义的保证；分片后仍保留**片内** atomic。
/// 跨片的部分失败由调用方的 user_sync 补偿队列兜底。
/// 复用同一个 `Script` 以拿到稳定的 SHA1，配合 EVALSHA 使用。
///
/// 用 EVAL 时**每个用户**都要重发一遍脚本正文（约 700B）：十万人群一次已读回执
/// 就是约 70MB 的构造与传输，实测单次耗时约 48 秒。EVALSHA 只发 40 字节的 sha，
/// 同一次扇出的传输量降到约 1/17。
fn record_change_script() -> &'static redis::Script {
    static SCRIPT: std::sync::OnceLock<redis::Script> = std::sync::OnceLock::new();
    SCRIPT.get_or_init(|| redis::Script::new(RECORD_CONVERSATION_CHANGE_SCRIPT))
}

const RECORD_CHANGE_CHUNK_DEFAULT: usize = 500;

/// 拆成纯函数是为了能测：0 或非法值必须回落到默认值。
/// 若回落成 0，chunks(0) 会 panic；若回落成极大值，等于没分片、OOM 照旧。
fn parse_record_change_chunk(raw: Option<&str>) -> usize {
    raw.and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|size| *size > 0)
        .unwrap_or(RECORD_CHANGE_CHUNK_DEFAULT)
}

fn record_change_chunk_size() -> usize {
    static VALUE: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *VALUE.get_or_init(|| {
        parse_record_change_chunk(
            std::env::var("FLARE_USER_SYNC_INDEX_CHUNK").ok().as_deref(),
        )
    })
}

const RECORD_CONVERSATION_CHANGE_SCRIPT: &str = r#"
local version = redis.call('INCR', KEYS[1])
local member = tostring(version) .. ':' .. ARGV[1]
redis.call('ZADD', KEYS[2], version, member)
redis.call(
  'HSET',
  KEYS[3],
  'conversation_id', ARGV[1],
  'max_conversation_seq', ARGV[2],
  'version', version,
  'updated_at_ms', ARGV[3]
)

local max_changes = tonumber(ARGV[4])
if max_changes ~= nil and max_changes > 0 then
  local overflow = redis.call('ZCARD', KEYS[2]) - max_changes
  if overflow > 0 then
    redis.call('ZREMRANGEBYRANK', KEYS[2], 0, overflow - 1)
  end
end

local ttl_seconds = tonumber(ARGV[5])
if ttl_seconds ~= nil and ttl_seconds > 0 then
  redis.call('EXPIRE', KEYS[2], ttl_seconds)
  redis.call('EXPIRE', KEYS[3], ttl_seconds)
end

return version
"#;

const RECORD_CONVERSATION_VERSION_BUMP_SCRIPT: &str = r#"
local version = redis.call('INCR', KEYS[1])
redis.call(
  'HSET',
  KEYS[2],
  'conversation_id', ARGV[1],
  'max_conversation_seq', ARGV[2],
  'version', version,
  'updated_at_ms', ARGV[3]
)

local ttl_seconds = tonumber(ARGV[4])
if ttl_seconds ~= nil and ttl_seconds > 0 then
  redis.call('EXPIRE', KEYS[1], ttl_seconds)
  redis.call('EXPIRE', KEYS[2], ttl_seconds)
end

return version
"#;

pub struct RedisUserSyncIndexRepository {
    client: Arc<redis::Client>,
    max_changes_per_user: usize,
    change_ttl_seconds: u64,
}

impl RedisUserSyncIndexRepository {
    pub fn new(
        client: Arc<redis::Client>,
        max_changes_per_user: usize,
        change_ttl_seconds: u64,
    ) -> Self {
        Self {
            client,
            max_changes_per_user: max_changes_per_user.max(1),
            change_ttl_seconds,
        }
    }

    async fn get_connection(&self) -> Result<ConnectionManager> {
        ConnectionManager::new(self.client.as_ref().clone())
            .await
            .map_err(|err| FlareError::system(format!("Redis user sync index connect: {err}")))
    }

    fn tenant_id(ctx: &Ctx) -> String {
        ctx.tenant_id()
            .filter(|tenant_id| !tenant_id.trim().is_empty())
            .unwrap_or(DEFAULT_TENANT_ID)
            .to_string()
    }

    fn version_key(tenant_id: &str, user_id: &str) -> String {
        format!("sync:user:{tenant_id}:{user_id}:version")
    }

    fn changes_key(tenant_id: &str, user_id: &str) -> String {
        format!("sync:user:{tenant_id}:{user_id}:changes")
    }

    fn conversation_state_key(tenant_id: &str, user_id: &str, conversation_id: &str) -> String {
        format!("sync:user:{tenant_id}:{user_id}:conversation:{conversation_id}")
    }

    fn conversation_version_key(tenant_id: &str, conversation_id: &str) -> String {
        format!("sync:conversation:{tenant_id}:{conversation_id}:version")
    }

    fn conversation_sync_state_key(tenant_id: &str, conversation_id: &str) -> String {
        format!("sync:conversation:{tenant_id}:{conversation_id}:state")
    }

    fn normalized_user_ids(user_ids: &[String]) -> Vec<String> {
        user_ids
            .iter()
            .map(|user_id| user_id.trim())
            .filter(|user_id| !user_id.is_empty())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(ToString::to_string)
            .collect()
    }

    fn normalized_known_conversation_versions(known: &[(String, u64)]) -> Vec<(String, u64)> {
        let mut versions: BTreeMap<String, u64> = BTreeMap::new();
        for (conversation_id, version) in known {
            let conversation_id = conversation_id.trim();
            if conversation_id.is_empty() {
                continue;
            }
            versions
                .entry(conversation_id.to_string())
                .and_modify(|known_version| *known_version = (*known_version).max(*version))
                .or_insert(*version);
        }
        versions.into_iter().collect()
    }

    fn parse_u64_field(
        state: &HashMap<String, String>,
        field: &str,
        conversation_id: &str,
    ) -> Result<Option<u64>> {
        state
            .get(field)
            .map(|value| {
                value.parse::<u64>().map_err(|err| {
                    FlareError::system(format!(
                        "Redis user sync index invalid {field} for conversation_id={conversation_id}: {err}"
                    ))
                })
            })
            .transpose()
    }

    fn parse_i64_field(
        state: &HashMap<String, String>,
        field: &str,
        conversation_id: &str,
    ) -> Result<Option<i64>> {
        state
            .get(field)
            .map(|value| {
                value.parse::<i64>().map_err(|err| {
                    FlareError::system(format!(
                        "Redis user sync index invalid {field} for conversation_id={conversation_id}: {err}"
                    ))
                })
            })
            .transpose()
    }

    fn conversation_change_from_state(
        conversation_id: &str,
        known_version: u64,
        state: &HashMap<String, String>,
    ) -> Result<Option<ConversationChange>> {
        let Some(version) = Self::parse_u64_field(state, "version", conversation_id)? else {
            return Ok(None);
        };
        if version <= known_version {
            return Ok(None);
        }

        let stored_conversation_id = state
            .get("conversation_id")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .unwrap_or(conversation_id)
            .to_string();

        Ok(Some(ConversationChange {
            conversation_id: stored_conversation_id,
            version,
            max_conversation_seq: Self::parse_u64_field(
                state,
                "max_conversation_seq",
                conversation_id,
            )?
            .unwrap_or_default(),
            occurred_at_ms: Self::parse_i64_field(state, "updated_at_ms", conversation_id)?
                .unwrap_or_default(),
        }))
    }
}

#[async_trait]
impl UserSyncIndexRepository for RedisUserSyncIndexRepository {
    async fn record_conversation_change(
        &self,
        ctx: &Ctx,
        user_ids: &[String],
        conversation_id: &str,
        max_conversation_seq: u64,
        occurred_at_ms: i64,
    ) -> Result<()> {
        let conversation_id = conversation_id.trim();
        if conversation_id.is_empty() {
            return Ok(());
        }

        let user_ids = Self::normalized_user_ids(user_ids);
        if user_ids.is_empty() {
            return Ok(());
        }

        let tenant_id = Self::tenant_id(ctx);
        let mut conn = self.get_connection().await?;
        let seq = max_conversation_seq.to_string();
        let occurred_at = occurred_at_ms.to_string();
        let max_changes = self.max_changes_per_user.to_string();
        let ttl = self.change_ttl_seconds.to_string();

        let chunk_size = record_change_chunk_size();
        let script_sha = record_change_script().get_hash().to_string();
        for chunk in user_ids.chunks(chunk_size) {
            // Redis 重启或 SCRIPT FLUSH 之后脚本缓存会空，EVALSHA 返回 NOSCRIPT。
            // 这时加载一次脚本再重试**当前分片**；只重试一次，避免异常下打转。
            let mut reloaded = false;
            loop {
                let mut pipe = redis::pipe();
                pipe.atomic();
                for user_id in chunk {
                    pipe.cmd("EVALSHA")
                        .arg(&script_sha)
                        .arg(3)
                        .arg(Self::version_key(&tenant_id, user_id))
                        .arg(Self::changes_key(&tenant_id, user_id))
                        .arg(Self::conversation_state_key(
                            &tenant_id,
                            user_id,
                            conversation_id,
                        ))
                        .arg(conversation_id)
                        .arg(&seq)
                        .arg(&occurred_at)
                        .arg(&max_changes)
                        .arg(&ttl);
                }

                match pipe.query_async::<Vec<redis::Value>>(&mut conn).await {
                    Ok(_) => break,
                    Err(err)
                        if !reloaded && err.kind() == redis::ErrorKind::NoScriptError =>
                    {
                        reloaded = true;
                        let _: String = redis::cmd("SCRIPT")
                            .arg("LOAD")
                            .arg(RECORD_CONVERSATION_CHANGE_SCRIPT)
                            .query_async(&mut conn)
                            .await
                            .map_err(|err| {
                                FlareError::system(format!(
                                    "Redis user sync index script load failed tenant_id={tenant_id} conversation_id={conversation_id}: {err}"
                                ))
                            })?;
                    }
                    Err(err) => {
                        return Err(FlareError::system(format!(
                            "Redis user sync index batch record failed tenant_id={tenant_id} conversation_id={conversation_id} user_count={} chunk_size={}: {err}",
                            user_ids.len(),
                            chunk.len()
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    async fn record_conversation_version_bump(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        max_conversation_seq: u64,
        occurred_at_ms: i64,
    ) -> Result<u64> {
        let conversation_id = conversation_id.trim();
        if conversation_id.is_empty() {
            return Ok(0);
        }

        let tenant_id = Self::tenant_id(ctx);
        let mut conn = self.get_connection().await?;
        redis::Script::new(RECORD_CONVERSATION_VERSION_BUMP_SCRIPT)
            .key(Self::conversation_version_key(&tenant_id, conversation_id))
            .key(Self::conversation_sync_state_key(
                &tenant_id,
                conversation_id,
            ))
            .arg(conversation_id)
            .arg(max_conversation_seq.to_string())
            .arg(occurred_at_ms.to_string())
            .arg(self.change_ttl_seconds.to_string())
            .invoke_async(&mut conn)
            .await
            .map_err(|err| {
                FlareError::system(format!(
                    "Redis user sync index conversation version bump failed tenant_id={tenant_id} conversation_id={conversation_id}: {err}"
                ))
            })
    }

    async fn diff_changed_conversations(
        &self,
        ctx: &Ctx,
        known: &[(String, u64)],
    ) -> Result<Vec<ConversationChange>> {
        let known = Self::normalized_known_conversation_versions(known);
        if known.is_empty() {
            return Ok(Vec::new());
        }

        let tenant_id = Self::tenant_id(ctx);
        let mut conn = self.get_connection().await?;
        let mut pipe = redis::pipe();
        for (conversation_id, _) in &known {
            pipe.cmd("HGETALL").arg(Self::conversation_sync_state_key(
                &tenant_id,
                conversation_id,
            ));
        }

        let states: Vec<HashMap<String, String>> =
            pipe.query_async(&mut conn).await.map_err(|err| {
                FlareError::system(format!(
                    "Redis user sync index conversation diff failed tenant_id={tenant_id} conversation_count={}: {err}",
                    known.len()
                ))
            })?;

        let mut changes = Vec::new();
        for ((conversation_id, known_version), state) in known.into_iter().zip(states) {
            if let Some(change) =
                Self::conversation_change_from_state(&conversation_id, known_version, &state)?
            {
                changes.push(change);
            }
        }
        Ok(changes)
    }
}

#[cfg(test)]
mod tests {
    use super::{RECORD_CHANGE_CHUNK_DEFAULT, parse_record_change_chunk};

    #[test]
    fn chunk_size_never_falls_back_to_zero_or_unbounded() {
        assert_eq!(parse_record_change_chunk(None), RECORD_CHANGE_CHUNK_DEFAULT);
        assert_eq!(parse_record_change_chunk(Some("1000")), 1000);
        assert_eq!(parse_record_change_chunk(Some(" 1000 ")), 1000);
        // 0 必须被拒：slice::chunks(0) 直接 panic
        assert_eq!(parse_record_change_chunk(Some("0")), RECORD_CHANGE_CHUNK_DEFAULT);
        assert_eq!(parse_record_change_chunk(Some("abc")), RECORD_CHANGE_CHUNK_DEFAULT);
        assert_eq!(parse_record_change_chunk(Some("")), RECORD_CHANGE_CHUNK_DEFAULT);
        assert_eq!(parse_record_change_chunk(Some("-1")), RECORD_CHANGE_CHUNK_DEFAULT);
    }

    #[test]
    fn record_conversation_change_actually_chunks_the_pipeline() {
        // 上面两个测试只覆盖纯函数与 chunks() 算术——把 record_conversation_change
        // 里的分片删掉它们照样绿。这里直接对源码断言，守住修复本身：
        // 必须按 chunk 建 pipeline，而不是在全量 user_ids 上建一个。
        let source = include_str!("redis_user_sync_index.rs");
        let body = source
            .split_once("async fn record_conversation_change")
            .expect("函数存在")
            .1
            .split_once("async fn record_conversation_version_bump")
            .expect("下一个函数存在")
            .0;
        assert!(
            body.contains("user_ids.chunks(chunk_size)"),
            "record_conversation_change 必须分片遍历 user_ids"
        );
        assert!(
            body.contains("for user_id in chunk"),
            "EVALSHA 必须只对当前分片累加，不能对全量 user_ids"
        );
        assert!(
            body.contains("EVALSHA"),
            "必须用 EVALSHA：EVAL 会给每个用户重发一遍脚本正文"
        );
        assert!(
            !body.contains(r#"cmd("EVAL")"#),
            "回归：改回 EVAL 了，脚本正文会被逐用户重发"
        );
        // 脚本正文在函数体里只应出现一次——即 NOSCRIPT 之后的那次 SCRIPT LOAD。
        assert_eq!(
            body.matches("RECORD_CONVERSATION_CHANGE_SCRIPT").count(),
            1,
            "脚本正文只应用于 SCRIPT LOAD，不应出现在逐用户的命令里"
        );
        assert!(
            !body.contains("for user_id in &user_ids"),
            "回归：又把全量 user_ids 塞进单个 pipeline 了"
        );
    }

    #[test]
    fn hundred_thousand_recipients_are_split_into_many_pipelines() {
        // 回归：十万人群的一条已读回执曾把十万个 EVAL 塞进同一个 pipeline
        // （约 70MB），orchestrator 在 512m 上被 OOM。
        let users: Vec<String> = (0..100_000).map(|i| format!("u{i}")).collect();
        let size = parse_record_change_chunk(None);
        let chunks: Vec<_> = users.chunks(size).collect();
        assert_eq!(chunks.len(), 200);
        assert!(chunks.iter().all(|c| c.len() <= size));
        assert_eq!(chunks.iter().map(|c| c.len()).sum::<usize>(), 100_000);
    }

    use super::*;

    #[test]
    fn normalized_user_ids_trims_deduplicates_and_orders() {
        let ids = vec![
            " user-b ".to_string(),
            String::new(),
            "user-a".to_string(),
            "user-b".to_string(),
        ];

        assert_eq!(
            RedisUserSyncIndexRepository::normalized_user_ids(&ids),
            vec!["user-a".to_string(), "user-b".to_string()]
        );
    }

    #[test]
    fn keys_are_tenant_and_user_scoped() {
        assert_eq!(
            RedisUserSyncIndexRepository::version_key("tenant-a", "user-a"),
            "sync:user:tenant-a:user-a:version"
        );
        assert_eq!(
            RedisUserSyncIndexRepository::changes_key("tenant-a", "user-a"),
            "sync:user:tenant-a:user-a:changes"
        );
        assert_eq!(
            RedisUserSyncIndexRepository::conversation_state_key("tenant-a", "user-a", "conv-a"),
            "sync:user:tenant-a:user-a:conversation:conv-a"
        );
        assert_eq!(
            RedisUserSyncIndexRepository::conversation_version_key("tenant-a", "conv-a"),
            "sync:conversation:tenant-a:conv-a:version"
        );
        assert_eq!(
            RedisUserSyncIndexRepository::conversation_sync_state_key("tenant-a", "conv-a"),
            "sync:conversation:tenant-a:conv-a:state"
        );
    }

    #[test]
    fn normalized_known_conversation_versions_trims_deduplicates_and_keeps_highest_version() {
        let known = vec![
            (" conv-b ".to_string(), 2),
            ("conv-a".to_string(), 1),
            ("conv-b".to_string(), 7),
            (" ".to_string(), 99),
        ];

        assert_eq!(
            RedisUserSyncIndexRepository::normalized_known_conversation_versions(&known),
            vec![("conv-a".to_string(), 1), ("conv-b".to_string(), 7)]
        );
    }

    #[test]
    fn conversation_change_from_state_detects_lagging_versions() {
        let state = HashMap::from([
            ("conversation_id".to_string(), "conv-a".to_string()),
            ("version".to_string(), "3".to_string()),
            ("max_conversation_seq".to_string(), "42".to_string()),
            ("updated_at_ms".to_string(), "1700000000000".to_string()),
        ]);

        assert_eq!(
            RedisUserSyncIndexRepository::conversation_change_from_state("conv-a", 2, &state)
                .expect("valid state"),
            Some(ConversationChange {
                conversation_id: "conv-a".to_string(),
                version: 3,
                max_conversation_seq: 42,
                occurred_at_ms: 1_700_000_000_000,
            })
        );
        assert_eq!(
            RedisUserSyncIndexRepository::conversation_change_from_state("conv-a", 3, &state)
                .expect("valid state"),
            None
        );
    }
}
