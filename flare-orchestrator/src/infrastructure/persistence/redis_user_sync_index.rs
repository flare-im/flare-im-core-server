use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use redis::aio::ConnectionManager;

use crate::domain::repository::{ConversationChange, UserSyncIndexRepository};
use flare_im_contracts::Ctx;
use flare_server_core::error::{FlareError, Result};

const DEFAULT_TENANT_ID: &str = "0";
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

        let mut pipe = redis::pipe();
        pipe.atomic();
        for user_id in &user_ids {
            pipe.cmd("EVAL")
                .arg(RECORD_CONVERSATION_CHANGE_SCRIPT)
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

        let _: Vec<redis::Value> = pipe.query_async(&mut conn).await.map_err(|err| {
            FlareError::system(format!(
                "Redis user sync index batch record failed tenant_id={tenant_id} conversation_id={conversation_id} user_count={}: {err}",
                user_ids.len()
            ))
        })?;

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
