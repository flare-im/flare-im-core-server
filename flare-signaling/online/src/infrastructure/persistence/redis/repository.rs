use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use flare_grpc_proto::signaling::online::DeviceConflictStrategy;
use flare_server_core::auth::KickSignal;
use flare_server_core::error::Result;
use redis::{AsyncCommands, aio::ConnectionManager};
use serde::{Deserialize, Serialize};

use crate::config::OnlineConfig;
use crate::domain::aggregate::Connection;
use crate::domain::model::OnlineStatusRecord;
use crate::domain::repository::{
    ConversationRepository, EvictedConnection, tenant_session_member, tenant_session_member_prefix,
};
use crate::domain::value_object::{ConnectionId, DeviceId, DevicePriority, TokenVersion, UserId};
use flare_server_core::context::Context as SrvContext;

const CONNECTION_KEY_PREFIX: &str = "session";
/// 租户在线索引：`tenant:sessions:{tenant}` = SET of v2 长度编码成员，TTL 与 `session:{tenant}:{user}` 同步续期。
const TENANT_SESSIONS_KEY_PREFIX: &str = "tenant:sessions";

// Both the user hash and the tenant device index are changed by one Redis
// script. A competing login cannot observe a partial eviction/admission or
// let two devices exceed the cap. The script returns only old physical
// connection IDs, each with why it was evicted (`replaced`: same device ID,
// `device_conflict`: another device removed by the Exclusive /
// PlatformExclusive rule, `device_cap`: over the device cap), which selects
// the kick reason; publishing the resulting kick is deliberately outside the
// session Redis transaction because the kick store may be a separate Redis.
const ADMIT_CONNECTION_SCRIPT: &str = r#"
local new = cjson.decode(ARGV[2])
local current = redis.call('HGETALL', KEYS[1])
local records = {}
local devices = {}
for i = 1, #current, 2 do
  local ok, rec = pcall(cjson.decode, current[i + 1])
  if not ok or type(rec) ~= 'table' or rec.tenant_id ~= new.tenant_id
      or type(rec.connection_id) ~= 'string' or rec.connection_id == ''
      or type(rec.device_id) ~= 'string' or rec.device_id == '' then
    return redis.error_reply('invalid existing tenant session')
  end
  if rec.connection_id == new.connection_id then
    return redis.error_reply('physical connection already admitted')
  end
  table.insert(records, { field = current[i], value = rec })
  local entry = devices[rec.device_id]
  if not entry then
    entry = { created_at = tonumber(rec.created_at) or tonumber(rec.last_seen) or 0 }
    devices[rec.device_id] = entry
  elseif (tonumber(rec.created_at) or 0) < entry.created_at then
    entry.created_at = tonumber(rec.created_at) or 0
  end
end

local strategy = tonumber(ARGV[4])
local removed_devices = {}
for _, item in ipairs(records) do
  local rec = item.value
  if rec.device_id == new.device_id then
    removed_devices[rec.device_id] = 'replaced'
  elseif strategy == 1
      or (strategy == 2 and rec.device_platform == new.device_platform) then
    removed_devices[rec.device_id] = 'device_conflict'
  end
end

local cap = tonumber(ARGV[5]) or 0
if cap > 0 then
  local remaining = 0
  for device, _ in pairs(devices) do
    if not removed_devices[device] then remaining = remaining + 1 end
  end
  while remaining >= cap do
    local oldest_device = nil
    local oldest_at = nil
    for device, entry in pairs(devices) do
      if not removed_devices[device] and
          (oldest_at == nil or entry.created_at < oldest_at or
          (entry.created_at == oldest_at and device < oldest_device)) then
        oldest_device = device
        oldest_at = entry.created_at
      end
    end
    if not oldest_device then break end
    removed_devices[oldest_device] = 'device_cap'
    remaining = remaining - 1
  end
end

local evicted = {}
for _, item in ipairs(records) do
  local rec = item.value
  if removed_devices[rec.device_id] then
    redis.call('HDEL', KEYS[1], item.field)
    table.insert(evicted, { conversation_id = rec.conversation_id, device_id = rec.device_id, connection_id = rec.connection_id, cause = removed_devices[rec.device_id] })
  end
end
for device, _ in pairs(removed_devices) do
  redis.call('SREM', KEYS[2], ARGV[6] .. device)
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[3])
redis.call('SADD', KEYS[2], ARGV[6] .. new.device_id)
redis.call('EXPIRE', KEYS[2], ARGV[3])
if #evicted == 0 then return '[]' end
return cjson.encode(evicted)
"#;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RedisConnectionRecord {
    conversation_id: String,
    gateway_id: String,
    server_id: String,
    device_id: String,
    device_platform: String,
    connection_id: String,
    created_at: i64,
    last_seen: i64,
    device_priority: i32,
    token_version: i64,
    tenant_id: String,
}

#[derive(Clone)]
pub struct RedisConversationRepository {
    commands: Option<Arc<sqlx::PgPool>>,
    client: Arc<redis::Client>,
    /// kick 频道所在实例（token_store profile），与 signaling-gateway 的订阅端一致。
    kick_client: Arc<redis::Client>,
    config: Arc<OnlineConfig>,
}

// No split HGET/HDEL/unindex window: a concurrent login may replace the same device.
// Return the removed record's PHYSICAL connection, not a user/device-wide broadcast.
const REMOVE_FOR_KICK: &str = r#"
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return false end
local rec = cjson.decode(raw)
if rec.tenant_id ~= ARGV[2] or type(rec.connection_id) ~= 'string' or rec.connection_id == '' then
  return redis.error_reply('invalid kick session')
end
if ARGV[4] and rec.connection_id ~= ARGV[4] then return false end
local keep = false
for _, value in ipairs(redis.call('HVALS', KEYS[1])) do
  local other = cjson.decode(value)
  if other.conversation_id ~= rec.conversation_id and other.device_id == rec.device_id then keep = true end
end
redis.call('HDEL', KEYS[1], ARGV[1])
if not keep then redis.call('SREM', KEYS[2], ARGV[3] .. rec.device_id) end
return rec.connection_id
"#;

const UNINDEX_IF_ABSENT: &str = r#"
for _, raw in ipairs(redis.call('HVALS', KEYS[1])) do
  local rec = cjson.decode(raw)
  if rec.tenant_id == ARGV[1] and rec.device_id == ARGV[2] then return 0 end
end
return redis.call('SREM', KEYS[2], ARGV[3])
"#;

impl RedisConversationRepository {
    pub fn new(client: Arc<redis::Client>, config: Arc<OnlineConfig>) -> Self {
        let kick_client = if config.kick_redis_url == config.redis_url {
            client.clone()
        } else {
            match redis::Client::open(config.kick_redis_url.as_str()) {
                Ok(kick_client) => Arc::new(kick_client),
                Err(error) => {
                    tracing::warn!(
                        %error,
                        "invalid kick redis url; KickTenant will publish on the session redis instead"
                    );
                    client.clone()
                }
            }
        };
        Self {
            commands: None,
            client,
            kick_client,
            config,
        }
    }

    fn connection_key(tenant_id: &str, user_id: &str) -> String {
        format!("{}:{}:{}", CONNECTION_KEY_PREFIX, tenant_id, user_id)
    }

    fn tenant_sessions_key(tenant_id: &str) -> String {
        format!("{}:{}", TENANT_SESSIONS_KEY_PREFIX, tenant_id)
    }

    fn ttl(&self) -> i64 {
        self.config.redis_ttl_seconds as i64
    }

    /// 会话删除后同步索引：同一 (user, device) 若还有其他会话则保留成员。
    fn unindex_if_last(
        pipe: &mut redis::Pipeline,
        user_id: &str,
        remaining: &[RedisConnectionRecord],
        removed: &RedisConnectionRecord,
    ) {
        let still_online = remaining.iter().any(|r| {
            r.device_id == removed.device_id && r.conversation_id != removed.conversation_id
        });
        if !still_online {
            pipe.srem(
                Self::tenant_sessions_key(&removed.tenant_id),
                tenant_session_member(user_id, &removed.device_id),
            )
            .ignore();
        }
    }

    async fn connection(&self) -> Result<ConnectionManager> {
        ConnectionManager::new(self.client.as_ref().clone())
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("redis connection: {}", e))
            })
    }

    fn to_timestamp(seconds: i64) -> Option<DateTime<Utc>> {
        Utc.timestamp_opt(seconds, 0).single()
    }

    fn record_from_connection(session: &Connection) -> RedisConnectionRecord {
        RedisConnectionRecord {
            tenant_id: session.tenant_id().to_string(),
            conversation_id: session.id().as_str().to_string(),
            gateway_id: session.gateway_id().to_string(),
            server_id: session.server_id().to_string(),
            device_id: session.device_id().as_str().to_string(),
            device_platform: session.device_platform().to_string(),
            connection_id: session.id().as_str().to_string(),
            created_at: session.created_at().timestamp_millis(),
            last_seen: session.last_heartbeat_at().timestamp(),
            device_priority: session.device_priority().as_i32(),
            token_version: session.token_version().value(),
        }
    }

    fn parse_record(payload: &str) -> Result<RedisConnectionRecord> {
        serde_json::from_str(payload).map_err(|e| {
            flare_server_core::error::FlareError::system(format!("invalid session payload: {}", e))
        })
    }

    fn records_from_values(values: HashMap<String, String>) -> Result<Vec<RedisConnectionRecord>> {
        values
            .into_values()
            .map(|payload| Self::parse_record(&payload))
            .collect()
    }

    fn connection_from_record(
        user_id: &UserId,
        record: RedisConnectionRecord,
    ) -> Result<Connection> {
        let conversation_id = ConnectionId::from_string(record.conversation_id)
            .map_err(|e| flare_server_core::error::FlareError::system((e).to_string()))?;
        let device_id = DeviceId::new(record.device_id)
            .map_err(|e| flare_server_core::error::FlareError::system((e).to_string()))?;
        let last_seen = Self::to_timestamp(record.last_seen).ok_or_else(|| {
            flare_server_core::error::FlareError::system("invalid last_seen timestamp".to_string())
        })?;

        Ok(Connection::reconstitute(
            conversation_id,
            user_id.clone(),
            device_id,
            record.tenant_id,
            record.device_platform,
            record.server_id,
            record.gateway_id,
            DevicePriority::from_i32(record.device_priority),
            TokenVersion::from(record.token_version),
            None,
            Utc.timestamp_millis_opt(record.created_at)
                .single()
                .unwrap_or(last_seen),
            last_seen,
        ))
    }

    async fn load_user_records(
        &self,
        conn: &mut ConnectionManager,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<Vec<RedisConnectionRecord>> {
        let key = Self::connection_key(tenant_id, user_id);
        let values: HashMap<String, String> = conn.hgetall(&key).await.map_err(|e| {
            flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
        })?;
        let records = Self::records_from_values(values)?;
        if records.iter().any(|record| record.tenant_id != tenant_id) {
            return Err(flare_server_core::error::FlareError::system(
                "session tenant mismatch",
            ));
        }
        Ok(records)
    }
}

impl ConversationRepository for RedisConversationRepository {
    async fn save_connection(&self, session: &Connection) -> Result<()> {
        let mut conn = self.connection().await?;
        let key = Self::connection_key(session.tenant_id(), session.user_id().as_str());
        let record = Self::record_from_connection(session);
        let value = serde_json::to_string(&record).map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "serialize session payload: {}",
                e
            ))
        })?;
        let tenant_key = Self::tenant_sessions_key(&record.tenant_id);
        let ttl = self.ttl();
        // 会话哈希与租户索引同一管道写入、同一 TTL：索引永远不比会话活得久。
        let _: () = redis::pipe()
            .atomic()
            .hset(&key, session.id().as_str(), value)
            .ignore()
            .expire(&key, ttl)
            .ignore()
            .sadd(
                &tenant_key,
                tenant_session_member(session.user_id().as_str(), &record.device_id),
            )
            .ignore()
            .expire(&tenant_key, ttl)
            .ignore()
            .query_async(&mut conn)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
            })?;
        Ok(())
    }

    async fn admit_connection(
        &self,
        session: &Connection,
        gateway_connection_id: &str,
        strategy: DeviceConflictStrategy,
        max_devices: u32,
    ) -> Result<Vec<EvictedConnection>> {
        if gateway_connection_id.is_empty() {
            return Err(flare_server_core::error::FlareError::system(
                "authenticated gateway connection ID is required",
            ));
        }
        let mut record = Self::record_from_connection(session);
        record.connection_id = gateway_connection_id.to_owned();
        let payload = serde_json::to_string(&record).map_err(|e| {
            flare_server_core::error::FlareError::system(format!("serialize session payload: {e}"))
        })?;
        let mut conn = self.connection().await?;
        let encoded: String = redis::Script::new(ADMIT_CONNECTION_SCRIPT)
            .key(Self::connection_key(
                session.tenant_id(),
                session.user_id().as_str(),
            ))
            .key(Self::tenant_sessions_key(session.tenant_id()))
            .arg(session.id().as_str())
            .arg(payload)
            .arg(self.ttl())
            .arg(strategy as i32)
            .arg(max_devices)
            .arg(tenant_session_member_prefix(session.user_id().as_str()))
            .invoke_async(&mut conn)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!(
                    "atomic session admission failed: {e}"
                ))
            })?;
        serde_json::from_str(&encoded).map_err(|e| {
            flare_server_core::error::FlareError::system(format!("invalid admission result: {e}"))
        })
    }

    async fn remove_connection(
        &self,
        tenant_id: &str,
        conversation_id: &ConnectionId,
        user_id: &UserId,
    ) -> Result<()> {
        let mut conn = self.connection().await?;
        let key = Self::connection_key(tenant_id, user_id.as_str());
        let records = self
            .load_user_records(&mut conn, tenant_id, user_id.as_str())
            .await?;
        let mut pipe = redis::pipe();
        pipe.atomic().hdel(&key, conversation_id.as_str()).ignore();
        if let Some(removed) = records
            .iter()
            .find(|r| r.conversation_id == conversation_id.as_str())
        {
            Self::unindex_if_last(&mut pipe, user_id.as_str(), &records, removed);
        }
        let _: () = pipe.query_async(&mut conn).await.map_err(|e| {
            flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
        })?;
        tracing::info!(conversation_id = %conversation_id.as_ref(), user_id = %user_id.as_ref(), "session removed from redis");
        Ok(())
    }

    async fn remove_connection_for_kick(
        &self,
        tenant: &str,
        session: &ConnectionId,
        user: &UserId,
        reason: &str,
    ) -> Result<Option<String>> {
        let mut conn = self.connection().await?;
        let raw: Option<String> = conn
            .hget(
                Self::connection_key(tenant, user.as_str()),
                session.as_str(),
            )
            .await
            .map_err(|_| {
                flare_server_core::flare_err!(
                    flare_server_core::error::ErrorCode::DatabaseError,
                    "kick session lookup failed"
                )
            })?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let record = Self::parse_record(&raw)?;
        if record.tenant_id != tenant || record.connection_id.is_empty() {
            return Err(flare_server_core::flare_err!(
                flare_server_core::error::ErrorCode::PermissionDenied,
                "kick session mismatch"
            ));
        }
        if let Some(evidence) = flare_im_service_kit::audit_effects::durable::pending_evidence(
            tenant,
            "online_connection",
            &record.connection_id,
            "retire",
        )
        .map_err(|_| flare_server_core::error::FlareError::system("kick attribution rejected"))?
        {
            let id = self
                .prepare_command(tenant, user.as_str(), &record, reason, &evidence)
                .await?;
            if self.advance_command(&id).await? {
                return Ok(Some(record.connection_id));
            }
            return Err(flare_server_core::flare_err!(
                flare_server_core::error::ErrorCode::ServiceUnavailable,
                "kick command pending receiver confirmation"
            ));
        }
        // Failed publication leaves the exact session available for retry. The
        // signal only names this physical socket; a later same-device login is safe.
        self.broadcast_user_kick(
            tenant,
            user.as_str(),
            Some(&record.device_id),
            Some(&record.connection_id),
            reason,
        )
        .await?;
        redis::Script::new(REMOVE_FOR_KICK)
            .key(Self::connection_key(tenant, user.as_str()))
            .key(Self::tenant_sessions_key(tenant))
            .arg(session.as_str())
            .arg(tenant)
            .arg(tenant_session_member_prefix(user.as_str()))
            .arg(&record.connection_id)
            .invoke_async(&mut conn)
            .await
            .map_err(|_| {
                flare_server_core::flare_err!(
                    flare_server_core::error::ErrorCode::DatabaseError,
                    "conditional session removal failed"
                )
            })
    }

    async fn touch_connection(
        &self,
        tenant_id: &str,
        conversation_id: &ConnectionId,
        user_id: &UserId,
    ) -> Result<()> {
        let mut conn = self.connection().await?;
        let key = Self::connection_key(tenant_id, user_id.as_str());
        let payload = conn
            .hget::<_, _, Option<String>>(&key, conversation_id.as_str())
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
            })?
            .ok_or_else(|| {
                flare_server_core::error::FlareError::system("connection not found".to_string())
            })?;
        let mut record = Self::parse_record(&payload)?;
        if record.tenant_id != tenant_id {
            return Err(flare_server_core::error::FlareError::system(
                "session tenant mismatch",
            ));
        }
        record.last_seen = Utc::now().timestamp();
        let value = serde_json::to_string(&record).map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "serialize session payload: {}",
                e
            ))
        })?;
        let ttl = self.ttl();
        // 心跳同时给租户索引续期，否则索引会先于活跃会话过期。
        let _: () = redis::pipe()
            .atomic()
            .hset(&key, conversation_id.as_str(), value)
            .ignore()
            .expire(&key, ttl)
            .ignore()
            .expire(Self::tenant_sessions_key(&record.tenant_id), ttl)
            .ignore()
            .query_async(&mut conn)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
            })?;
        Ok(())
    }

    async fn fetch_statuses(
        &self,
        tenant_id: &str,
        user_ids: &[String],
    ) -> Result<HashMap<String, OnlineStatusRecord>> {
        if user_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let mut conn = self.connection().await?;
        let mut pipe = redis::pipe();
        for user_id in user_ids {
            pipe.cmd("HGETALL")
                .arg(Self::connection_key(tenant_id, user_id));
        }

        let records_by_user: Vec<HashMap<String, String>> =
            pipe.query_async(&mut conn).await.map_err(|e| {
                flare_server_core::error::FlareError::system(format!(
                    "fetch statuses pipeline failed: {}",
                    e
                ))
            })?;

        let mut result = HashMap::new();
        for (user_id, values) in user_ids.iter().zip(records_by_user) {
            let records = Self::records_from_values(values)?;
            if records.iter().any(|record| record.tenant_id != tenant_id) {
                return Err(flare_server_core::error::FlareError::system(
                    "session tenant mismatch",
                ));
            }
            if let Some(latest) = records.into_iter().max_by_key(|record| record.last_seen) {
                let last_seen = Self::to_timestamp(latest.last_seen);
                result.insert(
                    user_id.clone(),
                    OnlineStatusRecord {
                        online: true,
                        server_id: latest.server_id,
                        gateway_id: Some(latest.gateway_id),
                        cluster_id: None,
                        last_seen,
                        device_id: Some(latest.device_id),
                        device_platform: Some(latest.device_platform),
                    },
                );
            }
        }

        Ok(result)
    }

    async fn get_user_connections(
        &self,
        tenant_id: &str,
        user_id: &UserId,
    ) -> Result<Vec<Connection>> {
        let mut conn = self.connection().await?;
        self.load_user_records(&mut conn, tenant_id, user_id.as_str())
            .await?
            .into_iter()
            .filter(|record| record.tenant_id == tenant_id)
            .map(|record| Self::connection_from_record(user_id, record))
            .collect()
    }

    async fn remove_user_connections(
        &self,
        tenant_id: &str,
        user_id: &UserId,
        device_ids: Option<&[DeviceId]>,
    ) -> Result<()> {
        let mut conn = self.connection().await?;
        let key = Self::connection_key(tenant_id, user_id.as_str());
        let records = self
            .load_user_records(&mut conn, tenant_id, user_id.as_str())
            .await?;
        let mut pipe = redis::pipe();
        pipe.atomic();

        if let Some(device_ids) = device_ids {
            for record in records
                .iter()
                .filter(|r| device_ids.iter().any(|d| d.as_str() == r.device_id))
            {
                pipe.hdel(&key, &record.conversation_id).ignore();
                pipe.srem(
                    Self::tenant_sessions_key(&record.tenant_id),
                    tenant_session_member(user_id.as_str(), &record.device_id),
                )
                .ignore();
            }
        } else {
            pipe.del(&key).ignore();
            for record in &records {
                pipe.srem(
                    Self::tenant_sessions_key(&record.tenant_id),
                    tenant_session_member(user_id.as_str(), &record.device_id),
                )
                .ignore();
            }
        }

        let _: () = pipe.query_async(&mut conn).await.map_err(|e| {
            flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
        })?;

        Ok(())
    }

    async fn get_connection_by_device(
        &self,
        tenant_id: &str,
        user_id: &UserId,
        device_id: &DeviceId,
    ) -> Result<Option<Connection>> {
        let sessions = self.get_user_connections(tenant_id, user_id).await?;
        Ok(sessions
            .into_iter()
            .find(|s| s.device_id().as_str() == device_id.as_str()))
    }

    async fn list_user_connections(&self, ctx: &SrvContext) -> Result<Vec<Connection>> {
        let tenant_id = ctx.tenant_id().ok_or_else(|| {
            flare_server_core::error::FlareError::system("tenant_id is required in context")
        })?;
        let user_id = ctx.user_id().ok_or_else(|| {
            flare_server_core::error::FlareError::system(
                "user_id is required in context".to_string(),
            )
        })?;
        let user_id_vo = UserId::new(user_id.to_string())
            .map_err(|e| flare_server_core::error::FlareError::system((e).to_string()))?;
        self.get_user_connections(tenant_id, &user_id_vo).await
    }

    async fn scan_tenant_sessions(
        &self,
        tenant_id: &str,
        cursor: u64,
        count: usize,
    ) -> Result<(u64, Vec<String>)> {
        let mut conn = self.connection().await?;
        let (next, members): (u64, Vec<String>) = redis::cmd("SSCAN")
            .arg(Self::tenant_sessions_key(tenant_id))
            .arg(cursor)
            .arg("COUNT")
            .arg(count.max(1))
            .query_async(&mut conn)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
            })?;
        Ok((next, members))
    }

    async fn unindex_tenant_session(&self, tenant_id: &str, member: &str) -> Result<()> {
        let mut conn = self.connection().await?;
        if let Some((user, device)) = crate::domain::repository::parse_tenant_session_member(member)
        {
            let _: usize = redis::Script::new(UNINDEX_IF_ABSENT)
                .key(Self::connection_key(tenant_id, user))
                .key(Self::tenant_sessions_key(tenant_id))
                .arg(tenant_id)
                .arg(device)
                .arg(member)
                .invoke_async(&mut conn)
                .await
                .map_err(|_| {
                    flare_server_core::flare_err!(
                        flare_server_core::error::ErrorCode::DatabaseError,
                        "conditional index cleanup failed"
                    )
                })?;
            return Ok(());
        }
        let _: usize = conn
            .srem(Self::tenant_sessions_key(tenant_id), member)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
            })?;
        Ok(())
    }

    async fn broadcast_user_kick(
        &self,
        tenant_id: &str,
        user_id: &str,
        device_id: Option<&str>,
        connection_id: Option<&str>,
        reason: &str,
    ) -> Result<()> {
        let mut conn = ConnectionManager::new(self.kick_client.as_ref().clone())
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("redis connection: {}", e))
            })?;
        let signal = KickSignal {
            tenant_id: tenant_id.to_owned(),
            user_id: user_id.to_owned(),
            device_id: device_id.map(str::to_owned),
            connection_id: connection_id.map(str::to_owned),
            reason: Some(reason.to_owned()),
        };
        let payload = serde_json::to_string(&signal).map_err(|e| {
            flare_server_core::error::FlareError::system(format!("serialize kick signal: {e}"))
        })?;
        let _: () = conn
            .publish(&self.config.kick_channel, payload)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
            })?;
        Ok(())
    }
}

#[path = "kick_commands.rs"]
mod kick_commands;

#[cfg(test)]
mod admission_tests {
    use super::*;
    use crate::domain::repository::EvictionCause;

    #[tokio::test]
    #[ignore = "requires isolated FLARE_TEST_REDIS_URL"]
    async fn a_delayed_kick_never_removes_or_targets_the_replacement_connection() {
        let client = redis::Client::open(std::env::var("FLARE_TEST_REDIS_URL").unwrap()).unwrap();
        let mut conn = ConnectionManager::new(client).await.unwrap();
        let tenant = format!("kick-{}", uuid::Uuid::new_v4().simple());
        let key = RedisConversationRepository::connection_key(&tenant, "same");
        let index = RedisConversationRepository::tenant_sessions_key(&tenant);
        let old = record(&tenant, "device", "old", 1);
        let new = record(&tenant, "device", "new", 2);
        let _: () = redis::pipe()
            .hset(
                &key,
                &old.conversation_id,
                serde_json::to_string(&old).unwrap(),
            )
            .ignore()
            .hset(
                &key,
                &new.conversation_id,
                serde_json::to_string(&new).unwrap(),
            )
            .ignore()
            .sadd(
                &index,
                crate::domain::repository::tenant_session_member("same", "device"),
            )
            .ignore()
            .query_async(&mut conn)
            .await
            .unwrap();
        let physical: Option<String> = redis::Script::new(REMOVE_FOR_KICK)
            .key(&key)
            .key(&index)
            .arg(&old.conversation_id)
            .arg(&tenant)
            .arg(tenant_session_member_prefix("same"))
            .invoke_async(&mut conn)
            .await
            .unwrap();
        assert_eq!(physical.as_deref(), Some("old"));
        let surviving: bool = conn.hexists(&key, &new.conversation_id).await.unwrap();
        assert!(surviving);
        let indexed: bool = conn
            .sismember(
                &index,
                crate::domain::repository::tenant_session_member("same", "device"),
            )
            .await
            .unwrap();
        assert!(indexed);
        let repeated: Option<String> = redis::Script::new(REMOVE_FOR_KICK)
            .key(&key)
            .key(&index)
            .arg(&old.conversation_id)
            .arg(&tenant)
            .arg(tenant_session_member_prefix("same"))
            .invoke_async(&mut conn)
            .await
            .unwrap();
        assert!(repeated.is_none());
        let removed: usize = redis::Script::new(UNINDEX_IF_ABSENT)
            .key(&key)
            .key(&index)
            .arg(&tenant)
            .arg("device")
            .arg(crate::domain::repository::tenant_session_member(
                "same", "device",
            ))
            .invoke_async(&mut conn)
            .await
            .unwrap();
        assert_eq!(
            removed, 0,
            "stale cleanup must preserve the newly admitted index"
        );
        let wrong: redis::RedisResult<Option<String>> = redis::Script::new(REMOVE_FOR_KICK)
            .key(&key)
            .key(&index)
            .arg(&new.conversation_id)
            .arg("other")
            .arg(tenant_session_member_prefix("same"))
            .invoke_async(&mut conn)
            .await;
        assert!(wrong.is_err());
        let _: () = redis::pipe()
            .del(&key)
            .ignore()
            .del(&index)
            .ignore()
            .query_async(&mut conn)
            .await
            .unwrap();
    }

    fn record(
        tenant: &str,
        device: &str,
        physical: &str,
        created_at: i64,
    ) -> RedisConnectionRecord {
        record_on(tenant, device, "ios", physical, created_at)
    }

    fn record_on(
        tenant: &str,
        device: &str,
        platform: &str,
        physical: &str,
        created_at: i64,
    ) -> RedisConnectionRecord {
        RedisConnectionRecord {
            conversation_id: format!("session-{physical}"),
            gateway_id: "test-gateway".into(),
            server_id: "test-server".into(),
            device_id: device.into(),
            device_platform: platform.into(),
            connection_id: physical.into(),
            created_at,
            last_seen: created_at,
            device_priority: 2,
            token_version: 0,
            tenant_id: tenant.into(),
        }
    }

    async fn admit(
        conn: &mut ConnectionManager,
        key: &str,
        index: &str,
        item: &RedisConnectionRecord,
    ) -> Vec<EvictedConnection> {
        admit_with(conn, key, index, item, DeviceConflictStrategy::Coexist, 1).await
    }

    async fn admit_with(
        conn: &mut ConnectionManager,
        key: &str,
        index: &str,
        item: &RedisConnectionRecord,
        strategy: DeviceConflictStrategy,
        max_devices: u32,
    ) -> Vec<EvictedConnection> {
        let encoded: String = redis::Script::new(ADMIT_CONNECTION_SCRIPT)
            .key(key)
            .key(index)
            .arg(&item.conversation_id)
            .arg(serde_json::to_string(item).unwrap())
            .arg(60)
            .arg(strategy as i32)
            .arg(max_devices)
            .arg(tenant_session_member_prefix("shared-user"))
            .invoke_async(conn)
            .await
            .unwrap();
        serde_json::from_str(&encoded).unwrap()
    }

    /// `(device_id, connection_id, cause)`，按连接 ID 排序（脚本遍历 HGETALL，顺序不定）。
    fn causes(evicted: Vec<EvictedConnection>) -> Vec<(String, String, EvictionCause)> {
        let mut causes: Vec<_> = evicted
            .into_iter()
            .map(|e| (e.device_id, e.connection_id, e.cause))
            .collect();
        causes.sort_by(|a, b| a.1.cmp(&b.1));
        causes
    }

    fn cause(
        device: &str,
        physical: &str,
        cause: EvictionCause,
    ) -> (String, String, EvictionCause) {
        (device.to_string(), physical.to_string(), cause)
    }

    /// 准入脚本按记录报出挤掉的缘由：冲突策略挤掉的其他设备是 `device_conflict`（踢线原因
    /// DEVICE_CONFLICT，客户端结束会话），同设备接替是 `replaced`，超出设备数上限是 `device_cap`。
    /// Run with `FLARE_TEST_REDIS_URL` pointing at an isolated Redis instance.
    #[tokio::test]
    #[ignore = "requires FLARE_TEST_REDIS_URL"]
    async fn admission_reports_why_each_connection_was_evicted() {
        use DeviceConflictStrategy::{Coexist, Exclusive, PlatformExclusive};
        use EvictionCause::{DeviceCap, DeviceConflict, Replaced};

        let url = std::env::var("FLARE_TEST_REDIS_URL").expect("FLARE_TEST_REDIS_URL");
        let client = redis::Client::open(url).unwrap();
        let mut conn = ConnectionManager::new(client).await.unwrap();
        let tenant = format!("w9k1-{}", &uuid::Uuid::new_v4().simple().to_string()[..20]);
        let key = RedisConversationRepository::connection_key(&tenant, "shared-user");
        let index = RedisConversationRepository::tenant_sessions_key(&tenant);
        let mut admit =
            async |item: RedisConnectionRecord, strategy: DeviceConflictStrategy, cap: u32| {
                causes(admit_with(&mut conn, &key, &index, &item, strategy, cap).await)
            };

        assert!(
            admit(
                record_on(&tenant, "pc-1", "pc", "p1", 1),
                PlatformExclusive,
                0
            )
            .await
            .is_empty()
        );
        assert!(
            admit(
                record_on(&tenant, "ios-1", "ios", "i1", 2),
                PlatformExclusive,
                0
            )
            .await
            .is_empty()
        );
        // 同平台的另一台设备：旧 PC 是设备冲突，iOS 不受影响。
        assert_eq!(
            admit(
                record_on(&tenant, "pc-2", "pc", "p2", 3),
                PlatformExclusive,
                0
            )
            .await,
            vec![cause("pc-1", "p1", DeviceConflict)]
        );
        // 同一设备重连：接替，不是设备冲突。
        assert_eq!(
            admit(
                record_on(&tenant, "pc-2", "pc", "p2b", 4),
                PlatformExclusive,
                0
            )
            .await,
            vec![cause("pc-2", "p2", Replaced)]
        );
        // 互斥：其他设备一律是设备冲突，同设备仍是接替。
        assert_eq!(
            admit(record_on(&tenant, "pc-2", "pc", "p2c", 5), Exclusive, 0).await,
            vec![
                cause("ios-1", "i1", DeviceConflict),
                cause("pc-2", "p2b", Replaced),
            ]
        );
        // 共存 + 上限 2：第三台设备挤掉最早的一台，是超出上限，不是设备冲突。
        assert!(
            admit(record_on(&tenant, "web-1", "web", "w1", 6), Coexist, 2)
                .await
                .is_empty()
        );
        assert_eq!(
            admit(record_on(&tenant, "ios-2", "ios", "i2", 7), Coexist, 2).await,
            vec![cause("pc-2", "p2c", DeviceCap)]
        );
        // 平台互斥 + 上限：同平台的旧设备是设备冲突，腾出名额后不再按上限挤别的设备。
        assert_eq!(
            admit(
                record_on(&tenant, "web-2", "web", "w2", 8),
                PlatformExclusive,
                2
            )
            .await,
            vec![cause("web-1", "w1", DeviceConflict)]
        );

        let _: () = redis::pipe()
            .del(&key)
            .ignore()
            .del(&index)
            .ignore()
            .query_async(&mut conn)
            .await
            .unwrap();
    }

    /// Run with `FLARE_TEST_REDIS_URL` pointing at an isolated Redis instance.
    #[tokio::test]
    #[ignore = "requires FLARE_TEST_REDIS_URL"]
    async fn admission_is_atomic_tenant_scoped_and_kicks_only_old_physical_connection() {
        let url = std::env::var("FLARE_TEST_REDIS_URL").expect("FLARE_TEST_REDIS_URL");
        let client = redis::Client::open(url).unwrap();
        let mut first = ConnectionManager::new(client.clone()).await.unwrap();
        let mut second = ConnectionManager::new(client).await.unwrap();
        let tenant = format!("sec07-{}", &uuid::Uuid::new_v4().simple().to_string()[..20]);
        let other = format!("sec07-{}", &uuid::Uuid::new_v4().simple().to_string()[..20]);
        let key = RedisConversationRepository::connection_key(&tenant, "shared-user");
        let index = RedisConversationRepository::tenant_sessions_key(&tenant);
        let other_key = RedisConversationRepository::connection_key(&other, "shared-user");
        let other_index = RedisConversationRepository::tenant_sessions_key(&other);

        assert!(
            admit(&mut first, &key, &index, &record(&tenant, "d1", "old", 1))
                .await
                .is_empty()
        );
        let evicted = admit(&mut first, &key, &index, &record(&tenant, "d1", "new", 2)).await;
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].connection_id, "old");
        let records: HashMap<String, String> = first.hgetall(&key).await.unwrap();
        assert_eq!(records.len(), 1);
        assert!(
            records
                .values()
                .next()
                .unwrap()
                .contains("\"connection_id\":\"new\"")
        );

        let left_record = record(&tenant, "d2", "left", 3);
        let right_record = record(&tenant, "d3", "right", 3);
        let (left, right) = tokio::join!(
            admit(&mut first, &key, &index, &left_record),
            admit(&mut second, &key, &index, &right_record)
        );
        assert_eq!(left.len(), 1);
        assert_eq!(right.len(), 1);
        let records: HashMap<String, String> = first.hgetall(&key).await.unwrap();
        assert_eq!(
            records.len(),
            1,
            "concurrent logins must respect one-device cap"
        );

        assert!(
            admit(
                &mut second,
                &other_key,
                &other_index,
                &record(&other, "d1", "other", 4)
            )
            .await
            .is_empty()
        );
        let other_records: HashMap<String, String> = second.hgetall(&other_key).await.unwrap();
        assert_eq!(
            other_records.len(),
            1,
            "same user ID in another tenant is independent"
        );

        let _: () = redis::pipe()
            .del(&key)
            .ignore()
            .del(&index)
            .ignore()
            .del(&other_key)
            .ignore()
            .del(&other_index)
            .ignore()
            .query_async(&mut first)
            .await
            .unwrap();
    }
}

/// 在线状态发布者实现（基于 Redis Pub/Sub）
pub struct RedisPresencePublisher {
    client: Arc<redis::Client>,
}

impl RedisPresencePublisher {
    pub fn new(client: Arc<redis::Client>) -> Self {
        Self { client }
    }

    async fn connection(&self) -> Result<ConnectionManager> {
        ConnectionManager::new(self.client.as_ref().clone())
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("redis connection: {}", e))
            })
    }

    fn presence_channel(tenant_id: &str, user_id: &str) -> String {
        format!("presence:{}:{}", tenant_id, user_id)
    }
}

impl crate::domain::repository::PresencePublisher for RedisPresencePublisher {
    async fn publish_presence_event(
        &self,
        tenant_id: &str,
        event: flare_grpc_proto::signaling::online::PresenceEvent,
    ) -> Result<()> {
        let mut conn = self.connection().await?;
        let payload = prost::Message::encode_to_vec(&event);
        let _: () = conn
            .publish(format!("presence_events:{tenant_id}"), payload)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
            })?;
        if !event.user_id.trim().is_empty() {
            let status = event.status.as_ref();
            let last_seen = status
                .and_then(|s| s.last_seen.as_ref())
                .map(|ts| ts.seconds)
                .unwrap_or_else(|| chrono::Utc::now().timestamp());
            let occurred_at = event
                .occurred_at
                .as_ref()
                .map(|ts| ts.seconds)
                .unwrap_or_else(|| chrono::Utc::now().timestamp());
            let value = serde_json::json!({
                "online": status.map(|s| s.online).unwrap_or(false),
                "server_id": status.map(|s| s.server_id.as_str()).unwrap_or_default(),
                "cluster_id": status.map(|s| s.cluster_id.as_str()).unwrap_or_default(),
                "last_seen": last_seen,
                "device_id": status.map(|s| s.device_id.as_str()).unwrap_or_default(),
                "device_platform": status.map(|s| s.device_platform.as_str()).unwrap_or_default(),
                "gateway_id": status.map(|s| s.gateway_id.as_str()).unwrap_or_default(),
                "occurred_at": occurred_at,
                "conflict_action": event.conflict_action,
                "reason": event.reason.clone(),
            });
            let _: () = conn
                .publish(
                    Self::presence_channel(tenant_id, &event.user_id),
                    value.to_string(),
                )
                .await
                .map_err(|e| {
                    flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
                })?;
        }
        Ok(())
    }

    async fn publish_user_presence_event(
        &self,
        tenant_id: &str,
        event: flare_grpc_proto::signaling::online::UserPresenceEvent,
    ) -> Result<()> {
        let mut conn = self.connection().await?;
        let payload = prost::Message::encode_to_vec(&event);
        let _: () = conn
            .publish(format!("user_presence_events:{tenant_id}"), payload)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::repository::parse_tenant_session_member;

    #[test]
    fn legacy_record_without_tenant_or_physical_connection_is_not_accepted() {
        let payload = r#"{"conversation_id":"c1","gateway_id":"g","server_id":"s","device_id":"d1","device_platform":"ios","last_seen":1,"device_priority":2,"token_version":0}"#;
        assert!(RedisConversationRepository::parse_record(payload).is_err());

        let with_tenant = r#"{"conversation_id":"c1","gateway_id":"g","server_id":"s","device_id":"d1","device_platform":"ios","last_seen":1,"device_priority":2,"token_version":0,"tenant_id":"acme"}"#;
        assert!(RedisConversationRepository::parse_record(with_tenant).is_err());
        let with_tenant = r#"{"conversation_id":"c1","gateway_id":"g","server_id":"s","device_id":"d1","device_platform":"ios","connection_id":"physical-1","created_at":1,"last_seen":1,"device_priority":2,"token_version":0,"tenant_id":"acme"}"#;
        assert_eq!(
            RedisConversationRepository::parse_record(with_tenant)
                .unwrap()
                .tenant_id,
            "acme"
        );
    }

    #[test]
    fn tenant_index_key_and_member_shape() {
        assert_eq!(
            RedisConversationRepository::connection_key("a", "same"),
            "session:a:same"
        );
        assert_eq!(
            RedisConversationRepository::connection_key("b", "same"),
            "session:b:same"
        );
        assert_ne!(
            RedisConversationRepository::connection_key("a", "same"),
            RedisConversationRepository::connection_key("b", "same")
        );
        assert_eq!(
            RedisConversationRepository::tenant_sessions_key("acme"),
            "tenant:sessions:acme"
        );
        assert_eq!(tenant_session_member("u1", "d1"), "v2:2:u1:d1");
        assert_eq!(
            parse_tenant_session_member("v2:4:u:a::d:1"),
            Some(("u:a:", "d:1"))
        );
        assert_eq!(parse_tenant_session_member("u1:d1"), None);
    }
}
