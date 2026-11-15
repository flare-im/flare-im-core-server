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
    ConversationRepository, EvictedConnection, KICK_PENDING_CONFIRMATION, tenant_session_member,
    tenant_session_member_prefix,
};
use crate::domain::value_object::{
    ConnectionId, DeviceAppState, DeviceAttention, DeviceId, DevicePriority, DeviceStateUpdate,
    TokenVersion, UserId,
};
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
//
// 同一个脚本里维护网关连接索引 `gw:conns:{gateway_id}`（新连接 SADD、被挤掉的 SREM），网关宕机后
// 清扫器据此找到它的全部会话。Dragonfly 默认不许脚本访问未声明的键：KEYS[3] 是新连接所在网关的
// 集合，KEYS[4..] 是调用方预先声明的其他网关集合；要挤掉的记录落在未声明的网关上时，脚本在任何
// 写入之前返回 `{"retry_with":[...]}`，调用方带上这些键重试。
//
// KEYS: 1 会话哈希，2 租户索引，3.. 网关连接集合
// ARGV: 1 会话字段，2 记录，3 TTL，4 冲突策略，5 设备上限，6 租户索引成员前缀，7 用户 ID，
//       8 网关连接集合键前缀
const ADMIT_CONNECTION_SCRIPT: &str = r#"
local function gateway_member(tenant, field)
  return 'v1:' .. string.len(tenant) .. ':' .. tenant .. ':' .. string.len(ARGV[7]) .. ':' .. ARGV[7] .. ':' .. field
end
local new = cjson.decode(ARGV[2])
if type(new.identity_session_id) == 'string' and new.identity_session_id ~= ''
    and redis.call('SISMEMBER', KEYS[4], new.identity_session_id) == 1 then
  return redis.error_reply('TOKEN_REVOKED')
end
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

local declared = {}
for i = 3, #KEYS do declared[KEYS[i]] = true end
local undeclared = {}
for _, item in ipairs(records) do
  local rec = item.value
  local gateway = tostring(rec.gateway_id or '')
  if removed_devices[rec.device_id] and not declared[ARGV[8] .. gateway] then
    table.insert(undeclared, gateway)
  end
end
if #undeclared > 0 then return cjson.encode({ retry_with = undeclared }) end

local evicted = {}
for _, item in ipairs(records) do
  local rec = item.value
  if removed_devices[rec.device_id] then
    redis.call('HDEL', KEYS[1], item.field)
    redis.call('SREM', ARGV[8] .. tostring(rec.gateway_id or ''), gateway_member(rec.tenant_id, item.field))
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
redis.call('SADD', KEYS[3], gateway_member(new.tenant_id, ARGV[1]))
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
    #[serde(default)]
    identity_session_id: Option<String>,
    /// 设备应用状态：`active` / `standby` / `parking` / `deferred`，缺失即 `active`。推送路由直接
    /// 读这几个字段判断设备的提示归链路上的进程还是推送通道，字段名与取值是两边共用的格式。
    #[serde(default)]
    app_state: DeviceAppState,
    /// 用户注意力：`unspecified` / `attentive` / `idle`，缺失即 `unspecified`。
    #[serde(default)]
    attention: DeviceAttention,
    /// 最近一次应用状态变化的时刻（Unix 毫秒），缺失即 0。
    #[serde(default)]
    state_since_ms: i64,
    /// 最近一次上报的未读总数，缺失即 0。
    #[serde(default)]
    badge: u32,
}

// 设备状态上报：对会话哈希里的那个字段做一次读改写。与心跳、踢线同在脚本里串行，上报不会把
// 已经移除的会话写回来，也不会盖掉并发心跳写入的 `last_seen`。只改设备状态这四个字段：不续 TTL、
// 不动 `last_seen`，上报不算心跳。`state_since_ms` 只在应用状态真的变了才前移，取 Redis 自己的
// 时钟：多个 Online 实例与网关的本地时钟不参与，同一条记录上的先后只有一个基准。
//
// 记下时返回这条会话的设备标识（调用方据此写推送闸门的设备信号），会话已不存在时返回空。
//
// KEYS: 1 会话哈希
// ARGV: 1 会话字段，2 租户，3 应用状态，4 注意力，5 角标
const REPORT_DEVICE_STATE_SCRIPT: &str = r#"
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return false end
local rec = cjson.decode(raw)
if rec.tenant_id ~= ARGV[2] then return redis.error_reply('session tenant mismatch') end
if (rec.app_state or 'active') ~= ARGV[3] then
  local time = redis.call('TIME')
  rec.state_since_ms = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
end
rec.app_state = ARGV[3]
rec.attention = ARGV[4]
rec.badge = tonumber(ARGV[5])
redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(rec))
return tostring(rec.device_id)
"#;

// 推送闸门读的两种设备信号，写在会话记录所在的 Redis 里，Push Server 按同样的键读（见
// `flare-push/server` 的 `RedisDeviceSignals`），键名与取值格式是两边共用的存储格式：
//
// - `push:dev:active:{tenant}:{user}:{device}`：设备最近一次以前台状态登录或上报的时刻（毫秒）。
//   闸门在下一次碰到这台设备时发现它比自己记下的新，就把这台设备的通知槽位整体作废、退出摘要模式。
// - `push:dev:badge:{tenant}:{user}:{device}`：`{未读总数}:{时刻}`，设备最近一次上报非前台状态时
//   带的角标基线。闸门发现它比自己采用的新，就以它为基线重新累加。
//
// 时刻取 Redis 的时钟，与会话记录的 `state_since_ms` 同一个基准。
const PUSH_DEVICE_ACTIVE_KEY_PREFIX: &str = "push:dev:active";
const PUSH_DEVICE_BADGE_KEY_PREFIX: &str = "push:dev:badge";
/// 设备信号保留多久。比闸门里任何状态都活得长：闸门的设备状态一天不写就过期。
const PUSH_DEVICE_SIGNAL_TTL_MS: u64 = 2 * 24 * 60 * 60 * 1000;

// KEYS: 1 信号键
// ARGV: 1 取值前缀（角标基线为 `{未读总数}:`，前台标记为空），2 保留毫秒
const MARK_PUSH_DEVICE_SIGNAL_SCRIPT: &str = r#"
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
redis.call('SET', KEYS[1], ARGV[1] .. string.format('%d', now), 'PX', ARGV[2])
return 1
"#;

/// 推送闸门要知道的一次设备状态变化。
enum PushDeviceSignal {
    /// 设备以前台状态登录或上报。
    Active,
    /// 设备上报了非前台状态，带当时的未读总数。
    BadgeBase(u32),
}

fn push_device_signal_key(prefix: &str, tenant_id: &str, user_id: &str, device_id: &str) -> String {
    format!("{prefix}:{tenant_id}:{user_id}:{device_id}")
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
// KEYS[3]/ARGV[5]（可选）：该记录所在网关的连接集合与成员，随会话一起移除。调用方按 ARGV[4]
// 的物理连接读出的记录给出网关；ARGV[4] 不符时脚本不删，集合也不动，两边仍一致。
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
if KEYS[3] and ARGV[5] then redis.call('SREM', KEYS[3], ARGV[5]) end
return rec.connection_id
"#;

/// 准入脚本要挤掉的记录落在未声明的网关上时，带上这些网关重试的次数上限。一次重试就能补齐
/// 读到的全部网关；只有并发登录不断把记录换到新网关上时才会用尽。
const ADMIT_ATTEMPTS: usize = 3;

/// 准入脚本的应答：挤掉的连接，或要求带上更多网关连接集合重试。
#[derive(Deserialize)]
#[serde(untagged)]
enum AdmissionReply {
    RetryWith { retry_with: Vec<String> },
    Evicted(Vec<EvictedConnection>),
}

/// 网关连接集合 `gw:conns:{gateway_id}`（约定见 `flare_im_service_kit::gateway::lease`）。
fn gateway_conns_key(gateway_id: &str) -> String {
    flare_im_service_kit::gateway::lease::conns_key(gateway_id)
}

/// 会话在网关连接集合里的成员：能定位回 `session:{tenant}:{user}` 里的那个字段。
fn gateway_member(tenant_id: &str, user_id: &str, session_field: &str) -> String {
    flare_im_service_kit::gateway::lease::connection_member(tenant_id, user_id, session_field)
}

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

    pub(crate) fn connection_key(tenant_id: &str, user_id: &str) -> String {
        format!("{}:{}:{}", CONNECTION_KEY_PREFIX, tenant_id, user_id)
    }

    fn identity_revocations_key(tenant: &str, user: &str) -> String {
        // Length framing avoids tenant/user delimiter aliasing.
        format!(
            "identity:revoked:{}:{}:{}:{}",
            tenant.len(),
            tenant,
            user.len(),
            user
        )
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
            identity_session_id: session.identity_session_id().map(str::to_owned),
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
            app_state: session.app_state(),
            attention: session.attention(),
            state_since_ms: session.state_since().timestamp_millis(),
            badge: session.badge(),
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

        let identity_session_id = record.identity_session_id;
        let created_at = Utc
            .timestamp_millis_opt(record.created_at)
            .single()
            .unwrap_or(last_seen);
        let state_since = Utc
            .timestamp_millis_opt(record.state_since_ms)
            .single()
            .unwrap_or(created_at);
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
            created_at,
            last_seen,
        )
        .with_identity_session_id(identity_session_id)
        .with_device_state(
            record.app_state,
            record.attention,
            state_since,
            record.badge,
        ))
    }

    /// 写一条推送闸门的设备信号。只是提示：写不进去不影响登录与上报本身，闸门最多晚一点才知道
    /// 设备回过前台、或者角标基线变了，所以只记日志。
    async fn mark_push_device_signal(
        conn: &mut ConnectionManager,
        tenant_id: &str,
        user_id: &str,
        device_id: &str,
        signal: PushDeviceSignal,
    ) {
        let (prefix, value_prefix) = match signal {
            PushDeviceSignal::Active => (PUSH_DEVICE_ACTIVE_KEY_PREFIX, String::new()),
            PushDeviceSignal::BadgeBase(badge) => {
                (PUSH_DEVICE_BADGE_KEY_PREFIX, format!("{badge}:"))
            }
        };
        let marked: redis::RedisResult<i32> = redis::Script::new(MARK_PUSH_DEVICE_SIGNAL_SCRIPT)
            .key(push_device_signal_key(
                prefix, tenant_id, user_id, device_id,
            ))
            .arg(value_prefix)
            .arg(PUSH_DEVICE_SIGNAL_TTL_MS)
            .invoke_async(conn)
            .await;
        if let Err(error) = marked {
            tracing::warn!(
                %error,
                tenant_id,
                user_id,
                device_id,
                "push device signal not recorded"
            );
        }
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
            .sadd(
                gateway_conns_key(&record.gateway_id),
                gateway_member(
                    &record.tenant_id,
                    session.user_id().as_str(),
                    session.id().as_str(),
                ),
            )
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
        let script = redis::Script::new(ADMIT_CONNECTION_SCRIPT);
        // 被挤掉的记录可能在别的网关上：第一次只声明新连接所在网关的集合（重连回同一网关是常态），
        // 脚本报出缺的网关后带上重试。
        let mut other_gateways: Vec<String> = Vec::new();
        for _ in 0..ADMIT_ATTEMPTS {
            let mut invocation = script.prepare_invoke();
            invocation
                .key(Self::connection_key(
                    session.tenant_id(),
                    session.user_id().as_str(),
                ))
                .key(Self::tenant_sessions_key(session.tenant_id()))
                .key(gateway_conns_key(&record.gateway_id))
                .key(Self::identity_revocations_key(
                    session.tenant_id(),
                    session.user_id().as_str(),
                ));
            for gateway in &other_gateways {
                invocation.key(gateway_conns_key(gateway));
            }
            invocation
                .arg(session.id().as_str())
                .arg(&payload)
                .arg(self.ttl())
                .arg(strategy as i32)
                .arg(max_devices)
                .arg(tenant_session_member_prefix(session.user_id().as_str()))
                .arg(session.user_id().as_str())
                .arg(flare_im_service_kit::gateway::lease::CONNS_KEY_PREFIX);
            let encoded: String = invocation.invoke_async(&mut conn).await.map_err(|e| {
                flare_server_core::error::FlareError::system(format!(
                    "atomic session admission failed: {e}"
                ))
            })?;
            let reply: AdmissionReply = serde_json::from_str(&encoded).map_err(|e| {
                flare_server_core::error::FlareError::system(format!(
                    "invalid admission result: {e}"
                ))
            })?;
            match reply {
                AdmissionReply::Evicted(evicted) => {
                    if session.app_state() == DeviceAppState::Active {
                        Self::mark_push_device_signal(
                            &mut conn,
                            session.tenant_id(),
                            session.user_id().as_str(),
                            session.device_id().as_str(),
                            PushDeviceSignal::Active,
                        )
                        .await;
                    }
                    return Ok(evicted);
                }
                AdmissionReply::RetryWith { retry_with } => {
                    for gateway in retry_with {
                        if gateway != record.gateway_id && !other_gateways.contains(&gateway) {
                            other_gateways.push(gateway);
                        }
                    }
                }
            }
        }
        Err(flare_server_core::error::FlareError::system(
            "atomic session admission kept racing with logins on other gateways",
        ))
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
            // 与会话哈希同一事务移出网关连接集合
            pipe.srem(
                gateway_conns_key(&removed.gateway_id),
                gateway_member(tenant_id, user_id.as_str(), conversation_id.as_str()),
            )
            .ignore();
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
                KICK_PENDING_CONFIRMATION
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
            .key(gateway_conns_key(&record.gateway_id))
            .arg(session.as_str())
            .arg(tenant)
            .arg(tenant_session_member_prefix(user.as_str()))
            .arg(&record.connection_id)
            .arg(gateway_member(tenant, user.as_str(), session.as_str()))
            .invoke_async(&mut conn)
            .await
            .map_err(|_| {
                flare_server_core::flare_err!(
                    flare_server_core::error::ErrorCode::DatabaseError,
                    "conditional session removal failed"
                )
            })
    }

    async fn revoke_identity_sessions(
        &self,
        tenant: &str,
        user: &UserId,
        session_ids: &[String],
        cutoff_ms: i64,
        request_id: &str,
        signing_key_id: &str,
    ) -> Result<u32> {
        let mut conn = self.connection().await?;
        // Session IDs are immutable UUIDs. Extending this set's retention never
        // affects later sessions, unlike a moving user/time revocation boundary.
        if !session_ids.is_empty() {
            let _: () = redis::pipe()
                .atomic()
                .sadd(
                    Self::identity_revocations_key(tenant, user.as_str()),
                    session_ids,
                )
                .ignore()
                .expire(
                    Self::identity_revocations_key(tenant, user.as_str()),
                    2_592_000,
                )
                .ignore()
                .query_async(&mut conn)
                .await
                .map_err(|error| {
                    flare_server_core::error::FlareError::system(format!(
                        "identity revocation persistence failed: {error}"
                    ))
                })?;
        }
        let records = self
            .load_user_records(&mut conn, tenant, user.as_str())
            .await?;
        let mut kicked = 0u32;
        for record in records {
            // A connection without a sid predates authoritative sid propagation and is matched
            // by the cutoff. Do not refresh the cutoff on a retry or infer a sid from device
            // metadata. The rule lives on the aggregate: the domain service applies the same one
            // to decide whose push bindings go with these sessions.
            let revoked = Connection::identity_revocation_covers(
                record.identity_session_id.as_deref(),
                record.created_at,
                session_ids,
                cutoff_ms,
            );
            if revoked {
                // Security delivery requires the exact gateway receiver ACK, even
                // without a Staff task scope. A PUBLISH count is never completion.
                let evidence = serde_json::json!({
                    "event":flare_im_service_kit::audit_effects::EFFECT_EVENT,
                    "request_id":request_id,"at_ms":chrono::Utc::now().timestamp_millis(),
                    "scope":tenant,"source":"signaling-online","actor_id":"flare-social-bridge",
                    "actor_type":"service","credential_id":format!("internal-key:{signing_key_id}"),"permission":null,"aggregate":"online_connection",
                    "aggregate_id":record.connection_id,"action":"retire","outcome":"committed",
                })
                .to_string();
                let command = self
                    .prepare_command(tenant, user.as_str(), &record, "TOKEN_REVOKED", &evidence)
                    .await?;
                if !self.advance_command(&command).await? {
                    return Err(flare_server_core::flare_err!(
                        flare_server_core::error::ErrorCode::ServiceUnavailable,
                        "identity connection retirement pending receiver confirmation"
                    ));
                }
                kicked += 1;
            }
        }
        Ok(kicked)
    }

    async fn touch_connection(
        &self,
        tenant_id: &str,
        conversation_id: &ConnectionId,
        user_id: &UserId,
    ) -> Result<()> {
        let mut conn = self.connection().await?;
        // Read/modify/write atomically: an in-flight heartbeat cannot reinsert a
        // retired record after its physical gateway has acknowledged the kick.
        let touched: i32 = redis::Script::new(
            r#"
local raw = redis.call('HGET',KEYS[1],ARGV[1])
if not raw then return 0 end
local rec = cjson.decode(raw)
if rec.tenant_id ~= ARGV[2] then return redis.error_reply('session tenant mismatch') end
if type(rec.identity_session_id) == 'string' and rec.identity_session_id ~= ''
    and redis.call('SISMEMBER',KEYS[3],rec.identity_session_id) == 1 then
  return redis.error_reply('TOKEN_REVOKED')
end
rec.last_seen = tonumber(ARGV[3])
redis.call('HSET',KEYS[1],ARGV[1],cjson.encode(rec))
redis.call('EXPIRE',KEYS[1],ARGV[4])
redis.call('EXPIRE',KEYS[2],ARGV[4])
return 1
"#,
        )
        .key(Self::connection_key(tenant_id, user_id.as_str()))
        .key(Self::tenant_sessions_key(tenant_id))
        .key(Self::identity_revocations_key(tenant_id, user_id.as_str()))
        .arg(conversation_id.as_str())
        .arg(tenant_id)
        .arg(Utc::now().timestamp())
        .arg(self.ttl())
        .invoke_async(&mut conn)
        .await
        .map_err(|_| {
            flare_server_core::error::FlareError::system(
                "connection heartbeat unavailable or revoked",
            )
        })?;
        if touched != 1 {
            return Err(flare_server_core::error::FlareError::system(
                "connection not found",
            ));
        }
        Ok(())
    }

    async fn report_device_state(
        &self,
        tenant_id: &str,
        conversation_id: &ConnectionId,
        user_id: &UserId,
        update: DeviceStateUpdate,
    ) -> Result<bool> {
        let mut conn = self.connection().await?;
        let device_id: Option<String> = redis::Script::new(REPORT_DEVICE_STATE_SCRIPT)
            .key(Self::connection_key(tenant_id, user_id.as_str()))
            .arg(conversation_id.as_str())
            .arg(tenant_id)
            .arg(update.app_state.as_str())
            .arg(update.attention.as_str())
            .arg(update.badge)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!(
                    "device state report failed: {e}"
                ))
            })?;
        let Some(device_id) = device_id else {
            return Ok(false);
        };
        let signal = if update.app_state == DeviceAppState::Active {
            PushDeviceSignal::Active
        } else {
            PushDeviceSignal::BadgeBase(update.badge)
        };
        Self::mark_push_device_signal(&mut conn, tenant_id, user_id.as_str(), &device_id, signal)
            .await;
        Ok(true)
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
                pipe.srem(
                    gateway_conns_key(&record.gateway_id),
                    gateway_member(tenant_id, user_id.as_str(), &record.conversation_id),
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
                pipe.srem(
                    gateway_conns_key(&record.gateway_id),
                    gateway_member(tenant_id, user_id.as_str(), &record.conversation_id),
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
            .del(gateway_conns_key(&test_gateway(&tenant)))
            .ignore()
            .query_async(&mut conn)
            .await
            .unwrap();
    }

    pub(super) fn record(
        tenant: &str,
        device: &str,
        physical: &str,
        created_at: i64,
    ) -> RedisConnectionRecord {
        record_on(tenant, device, "ios", physical, created_at)
    }

    fn test_gateway(tenant: &str) -> String {
        format!("{tenant}-gw")
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
            // 每个租户一台网关：用例并发跑也不会共用同一个 gw:conns 集合。
            gateway_id: test_gateway(tenant),
            server_id: "test-server".into(),
            device_id: device.into(),
            device_platform: platform.into(),
            connection_id: physical.into(),
            created_at,
            last_seen: created_at,
            device_priority: 2,
            token_version: 0,
            tenant_id: tenant.into(),
            identity_session_id: None,
            app_state: DeviceAppState::Active,
            attention: DeviceAttention::Unspecified,
            state_since_ms: created_at,
            badge: 0,
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

    pub(super) async fn admit_with(
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
            .key(gateway_conns_key(&item.gateway_id))
            .key(RedisConversationRepository::identity_revocations_key(
                &item.tenant_id,
                "shared-user",
            ))
            .arg(&item.conversation_id)
            .arg(serde_json::to_string(item).unwrap())
            .arg(60)
            .arg(strategy as i32)
            .arg(max_devices)
            .arg(tenant_session_member_prefix("shared-user"))
            .arg("shared-user")
            .arg(flare_im_service_kit::gateway::lease::CONNS_KEY_PREFIX)
            .invoke_async(conn)
            .await
            .unwrap();
        serde_json::from_str(&encoded).unwrap()
    }

    fn test_repository(url: &str) -> RedisConversationRepository {
        RedisConversationRepository::new(
            Arc::new(redis::Client::open(url).unwrap()),
            Arc::new(OnlineConfig {
                redis_url: url.to_string(),
                redis_ttl_seconds: 60,
                presence_prefix: "presence:user".into(),
                kick_redis_url: url.to_string(),
                kick_channel: format!("flare-test-{}:kick", uuid::Uuid::new_v4().simple()),
                tenant_runtime_postgres_url: None,
                tenant_policy: Default::default(),
                max_online_devices_per_user: 0,
                push_proxy_url: None,
            }),
        )
    }

    fn connection_on(tenant: &str, user: &str, device: &str, gateway: &str) -> Connection {
        Connection::create(crate::domain::aggregate::ConnectionCreateParams {
            user_id: UserId::new(user.to_string()).unwrap(),
            device_id: DeviceId::new(device.to_string()).unwrap(),
            tenant_id: tenant.to_string(),
            device_platform: "ios".to_string(),
            server_id: gateway.to_string(),
            gateway_id: gateway.to_string(),
            device_priority: DevicePriority::from_i32(2),
            token_version: TokenVersion::from(0),
            initial_quality: None,
            app_state: DeviceAppState::Active,
            attention: DeviceAttention::Unspecified,
        })
    }

    async fn stored(conn: &mut ConnectionManager, key: &str, field: &str) -> RedisConnectionRecord {
        let raw: String = conn.hget(key, field).await.unwrap();
        RedisConversationRepository::parse_record(&raw).unwrap()
    }

    async fn redis_now_ms(conn: &mut ConnectionManager) -> i64 {
        let (seconds, micros): (i64, i64) = redis::cmd("TIME").query_async(conn).await.unwrap();
        seconds * 1000 + micros / 1000
    }

    /// 设备状态上报只重写那条会话的应用状态、注意力与角标：`state_since_ms` 在应用状态变化时
    /// 按 Redis 时钟前移，同一状态下不动；`last_seen`、TTL 与其余字段不变。心跳重写记录后这四个
    /// 字段还在；会话不存在返回 `false`；同设备重连接替后新记录带自己的初始状态。
    /// Run with `FLARE_TEST_REDIS_URL` pointing at an isolated Redis instance.
    #[tokio::test]
    #[ignore = "requires FLARE_TEST_REDIS_URL"]
    async fn a_device_state_report_rewrites_only_the_device_state_of_that_session() {
        use DeviceAppState::{Active, Deferred, Parking};
        use DeviceAttention::{Attentive, Idle, Unspecified};

        let url = std::env::var("FLARE_TEST_REDIS_URL").expect("FLARE_TEST_REDIS_URL");
        let repository = test_repository(&url);
        let mut conn = ConnectionManager::new(redis::Client::open(url.as_str()).unwrap())
            .await
            .unwrap();
        let tenant = format!(
            "dstate-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        );
        let gateway = format!("{tenant}-gw");
        let key = RedisConversationRepository::connection_key(&tenant, "u:1");
        let update = |app_state, attention, badge| DeviceStateUpdate {
            app_state,
            attention,
            badge,
        };

        let session = connection_on(&tenant, "u:1", "phone", &gateway);
        let field = session.id().as_str();
        repository
            .admit_connection(&session, "phys-1", DeviceConflictStrategy::Coexist, 0)
            .await
            .unwrap();
        let admitted = stored(&mut conn, &key, field).await;
        assert_eq!(admitted.app_state, Active);
        assert_eq!(admitted.attention, Unspecified);
        assert_eq!(
            admitted.state_since_ms,
            session.state_since().timestamp_millis()
        );
        assert_eq!(admitted.badge, 0);
        let _: bool = conn.expire(&key, 30).await.unwrap();

        // 同一应用状态：只改注意力与角标，不重新计时。
        assert!(
            repository
                .report_device_state(
                    &tenant,
                    session.id(),
                    session.user_id(),
                    update(Active, Attentive, 4)
                )
                .await
                .unwrap()
        );
        let same = stored(&mut conn, &key, field).await;
        assert_eq!(
            (same.app_state, same.attention, same.badge),
            (Active, Attentive, 4)
        );
        assert_eq!(same.state_since_ms, admitted.state_since_ms);

        // 应用状态变化：按 Redis 时钟前移。
        let before = redis_now_ms(&mut conn).await;
        assert!(
            repository
                .report_device_state(
                    &tenant,
                    session.id(),
                    session.user_id(),
                    update(Parking, Idle, 6)
                )
                .await
                .unwrap()
        );
        let after = redis_now_ms(&mut conn).await;
        let parked = stored(&mut conn, &key, field).await;
        assert_eq!(
            (parked.app_state, parked.attention, parked.badge),
            (Parking, Idle, 6)
        );
        assert!(
            (before..=after).contains(&parked.state_since_ms),
            "state_since_ms {} must come from the Redis clock ({before}..={after})",
            parked.state_since_ms
        );

        // 其余字段与 TTL 不动：上报不算心跳。
        assert_eq!(parked.connection_id, "phys-1");
        assert_eq!(parked.device_id, "phone");
        assert_eq!(parked.device_platform, "ios");
        assert_eq!(parked.gateway_id, gateway);
        assert_eq!(parked.created_at, admitted.created_at);
        assert_eq!(parked.last_seen, admitted.last_seen);
        let ttl: i64 = conn.ttl(&key).await.unwrap();
        assert!(ttl <= 30, "a state report must not refresh the session TTL");

        // 心跳重写整条记录，设备状态还在；聚合读回的也是它。
        repository
            .touch_connection(&tenant, session.id(), session.user_id())
            .await
            .unwrap();
        let touched = stored(&mut conn, &key, field).await;
        assert_eq!(
            (
                touched.app_state,
                touched.attention,
                touched.badge,
                touched.state_since_ms
            ),
            (Parking, Idle, 6, parked.state_since_ms)
        );
        let restored = repository
            .get_user_connections(&tenant, session.user_id())
            .await
            .unwrap();
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].app_state(), Parking);
        assert_eq!(restored[0].attention(), Idle);
        assert_eq!(restored[0].badge(), 6);
        assert_eq!(
            restored[0].state_since().timestamp_millis(),
            parked.state_since_ms
        );

        // 后台、不能自己提示：脚本写进记录的取值是 `deferred`，又是一次应用状态变化；心跳重写
        // 之后还在。
        let before = redis_now_ms(&mut conn).await;
        assert!(
            repository
                .report_device_state(
                    &tenant,
                    session.id(),
                    session.user_id(),
                    update(Deferred, Unspecified, 7)
                )
                .await
                .unwrap()
        );
        let after = redis_now_ms(&mut conn).await;
        let raw: String = conn.hget(&key, field).await.unwrap();
        let json: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(json["app_state"], "deferred");
        let deferred = stored(&mut conn, &key, field).await;
        assert_eq!(
            (deferred.app_state, deferred.attention, deferred.badge),
            (Deferred, Unspecified, 7)
        );
        assert!((before..=after).contains(&deferred.state_since_ms));
        repository
            .touch_connection(&tenant, session.id(), session.user_id())
            .await
            .unwrap();
        let touched = stored(&mut conn, &key, field).await;
        assert_eq!(
            (touched.app_state, touched.state_since_ms),
            (Deferred, deferred.state_since_ms)
        );

        // 会话不存在：不写任何东西。
        let missing = ConnectionId::new();
        assert!(
            !repository
                .report_device_state(
                    &tenant,
                    &missing,
                    session.user_id(),
                    update(Active, Unspecified, 0)
                )
                .await
                .unwrap()
        );
        assert!(
            !conn
                .hexists::<_, _, bool>(&key, missing.as_str())
                .await
                .unwrap()
        );

        // 同设备重连接替：旧会话已不在，新记录是新连接自己的初始状态。
        let replacement = connection_on(&tenant, "u:1", "phone", &gateway);
        let evicted = repository
            .admit_connection(&replacement, "phys-2", DeviceConflictStrategy::Coexist, 0)
            .await
            .unwrap();
        assert_eq!(evicted.len(), 1);
        assert!(
            !repository
                .report_device_state(
                    &tenant,
                    session.id(),
                    session.user_id(),
                    update(Active, Attentive, 1)
                )
                .await
                .unwrap()
        );
        let fresh = stored(&mut conn, &key, replacement.id().as_str()).await;
        assert_eq!((fresh.app_state, fresh.badge), (Active, 0));

        // 记录的租户与签名租户不符：拒绝，不改记录。
        let foreign_field = ConnectionId::new();
        let foreign = record("other-tenant", "tablet", "phys-x", 1);
        let _: () = conn
            .hset(
                &key,
                foreign_field.as_str(),
                serde_json::to_string(&foreign).unwrap(),
            )
            .await
            .unwrap();
        assert!(
            repository
                .report_device_state(
                    &tenant,
                    &foreign_field,
                    session.user_id(),
                    update(Parking, Idle, 2)
                )
                .await
                .is_err()
        );
        assert_eq!(
            stored(&mut conn, &key, foreign_field.as_str())
                .await
                .app_state,
            Active
        );

        let _: () = redis::pipe()
            .del(&key)
            .ignore()
            .del(RedisConversationRepository::tenant_sessions_key(&tenant))
            .ignore()
            .del(gateway_conns_key(&gateway))
            .ignore()
            .query_async(&mut conn)
            .await
            .unwrap();
    }

    /// 准入与断开在会话哈希的同一事务里维护 `gw:conns:{gateway_id}`：新连接进所在网关的集合，
    /// 被挤掉（哪怕在另一台网关上，脚本会要求带上那台网关的键重试）与正常断开的连接移出集合。
    /// Run with `FLARE_TEST_REDIS_URL` pointing at an isolated Redis instance.
    #[tokio::test]
    #[ignore = "requires FLARE_TEST_REDIS_URL"]
    async fn admission_and_removal_maintain_the_gateway_connection_index() {
        let url = std::env::var("FLARE_TEST_REDIS_URL").expect("FLARE_TEST_REDIS_URL");
        let repository = test_repository(&url);
        let mut conn = ConnectionManager::new(redis::Client::open(url.as_str()).unwrap())
            .await
            .unwrap();
        let tenant = format!("gwidx-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
        let gw_a = format!("{tenant}-gw-a");
        let gw_b = format!("{tenant}-gw-b");

        let first = connection_on(&tenant, "u:1", "phone", &gw_a);
        assert!(
            repository
                .admit_connection(&first, "phys-1", DeviceConflictStrategy::Coexist, 0)
                .await
                .unwrap()
                .is_empty()
        );
        let first_member = gateway_member(&tenant, "u:1", first.id().as_str());
        assert!(
            conn.sismember::<_, _, bool>(gateway_conns_key(&gw_a), &first_member)
                .await
                .unwrap()
        );

        // 同一设备从另一台网关重连：挤掉 gw-a 上的旧会话，旧成员移出 gw-a，新成员进 gw-b。
        let second = connection_on(&tenant, "u:1", "phone", &gw_b);
        let evicted = repository
            .admit_connection(&second, "phys-2", DeviceConflictStrategy::Coexist, 0)
            .await
            .unwrap();
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].connection_id, "phys-1");
        assert!(
            !conn
                .sismember::<_, _, bool>(gateway_conns_key(&gw_a), &first_member)
                .await
                .unwrap()
        );
        let second_member = gateway_member(&tenant, "u:1", second.id().as_str());
        assert!(
            conn.sismember::<_, _, bool>(gateway_conns_key(&gw_b), &second_member)
                .await
                .unwrap()
        );

        // 正常断开：成员随会话一起移出。
        repository
            .remove_connection(&tenant, second.id(), second.user_id())
            .await
            .unwrap();
        assert!(
            !conn
                .exists::<_, bool>(gateway_conns_key(&gw_b))
                .await
                .unwrap()
        );

        let _: () = redis::pipe()
            .del(RedisConversationRepository::connection_key(&tenant, "u:1"))
            .ignore()
            .del(RedisConversationRepository::tenant_sessions_key(&tenant))
            .ignore()
            .del(gateway_conns_key(&gw_a))
            .ignore()
            .del(gateway_conns_key(&gw_b))
            .ignore()
            .query_async(&mut conn)
            .await
            .unwrap();
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
            .del(gateway_conns_key(&test_gateway(&tenant)))
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
            .del(gateway_conns_key(&test_gateway(&tenant)))
            .ignore()
            .del(gateway_conns_key(&test_gateway(&other)))
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
    use super::admission_tests::{admit_with, record};
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

    /// 设备状态在会话记录里的格式是与推送路由共用的：字段名、小写取值、毫秒时间戳；原有的
    /// `device_id`、`device_platform` 不变。写出的记录读回来是同一个设备状态。
    #[test]
    fn the_session_record_carries_the_device_state_in_the_shared_format() {
        let mut connection = Connection::create(crate::domain::aggregate::ConnectionCreateParams {
            user_id: UserId::new("u1".to_string()).unwrap(),
            device_id: DeviceId::new("phone".to_string()).unwrap(),
            tenant_id: "acme".to_string(),
            device_platform: "ios".to_string(),
            server_id: "s".to_string(),
            gateway_id: "g".to_string(),
            device_priority: DevicePriority::from_i32(2),
            token_version: TokenVersion::from(0),
            initial_quality: None,
            app_state: DeviceAppState::Standby,
            attention: DeviceAttention::Attentive,
        });
        let connected = RedisConversationRepository::record_from_connection(&connection);
        let json = serde_json::to_value(&connected).unwrap();
        assert_eq!(json["app_state"], "standby");
        assert_eq!(json["attention"], "attentive");
        assert_eq!(
            json["state_since_ms"],
            connection.created_at().timestamp_millis()
        );
        assert_eq!(json["badge"], 0);

        let parked_at = connection.created_at() + chrono::Duration::seconds(5);
        connection.report_device_state(
            DeviceStateUpdate {
                app_state: DeviceAppState::Parking,
                attention: DeviceAttention::Idle,
                badge: 3,
            },
            parked_at,
        );
        let parked = RedisConversationRepository::record_from_connection(&connection);
        let json = serde_json::to_value(&parked).unwrap();
        assert_eq!(json["app_state"], "parking");
        assert_eq!(json["attention"], "idle");
        assert_eq!(json["state_since_ms"], parked_at.timestamp_millis());
        assert_eq!(json["badge"], 3);
        assert_eq!(json["device_id"], "phone");
        assert_eq!(json["device_platform"], "ios");

        let restored = RedisConversationRepository::connection_from_record(
            connection.user_id(),
            RedisConversationRepository::parse_record(&json.to_string()).unwrap(),
        )
        .unwrap();
        assert_eq!(restored.app_state(), DeviceAppState::Parking);
        assert_eq!(restored.attention(), DeviceAttention::Idle);
        assert_eq!(
            restored.state_since().timestamp_millis(),
            parked_at.timestamp_millis()
        );
        assert_eq!(restored.badge(), 3);

        // 后台、不能自己提示：记录里是 `deferred`，读回来是同一个状态。
        let deferred_at = parked_at + chrono::Duration::seconds(5);
        connection.report_device_state(
            DeviceStateUpdate {
                app_state: DeviceAppState::Deferred,
                attention: DeviceAttention::Unspecified,
                badge: 4,
            },
            deferred_at,
        );
        let deferred = RedisConversationRepository::record_from_connection(&connection);
        let json = serde_json::to_value(&deferred).unwrap();
        assert_eq!(json["app_state"], "deferred");
        assert_eq!(json["state_since_ms"], deferred_at.timestamp_millis());
        let restored = RedisConversationRepository::connection_from_record(
            connection.user_id(),
            RedisConversationRepository::parse_record(&json.to_string()).unwrap(),
        )
        .unwrap();
        assert_eq!(restored.app_state(), DeviceAppState::Deferred);
        assert!(restored.app_state().alerts_owned_by_push());
        assert_eq!(
            restored.state_since().timestamp_millis(),
            deferred_at.timestamp_millis()
        );
        assert_eq!(restored.badge(), 4);
    }

    /// 没有设备状态字段的记录：前台、注意力未指定、`state_since_ms` 与角标为 0。
    #[test]
    fn a_record_without_device_state_fields_reads_as_active() {
        let payload = r#"{"conversation_id":"c1","gateway_id":"g","server_id":"s","device_id":"d1","device_platform":"ios","connection_id":"physical-1","created_at":1,"last_seen":1,"device_priority":2,"token_version":0,"tenant_id":"acme"}"#;
        let record = RedisConversationRepository::parse_record(payload).unwrap();
        assert_eq!(record.app_state, DeviceAppState::Active);
        assert_eq!(record.attention, DeviceAttention::Unspecified);
        assert_eq!(record.state_since_ms, 0);
        assert_eq!(record.badge, 0);
        // 取值是固定的四个名字，别的写法不接受。
        let deferred = payload.replacen('{', r#"{"app_state":"deferred","#, 1);
        assert_eq!(
            RedisConversationRepository::parse_record(&deferred)
                .unwrap()
                .app_state,
            DeviceAppState::Deferred
        );
        let unknown = payload.replacen('{', r#"{"app_state":"background","#, 1);
        assert!(RedisConversationRepository::parse_record(&unknown).is_err());
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
    #[tokio::test]
    #[ignore = "requires isolated FLARE_TEST_REDIS_URL"]
    async fn identity_tombstone_fences_pending_admission_without_rejecting_new_sid_or_other_tenant()
    {
        let client = redis::Client::open(std::env::var("FLARE_TEST_REDIS_URL").unwrap()).unwrap();
        let mut conn = ConnectionManager::new(client).await.unwrap();
        let tenant = format!("identity-{}", uuid::Uuid::new_v4().simple());
        let other = format!("identity-{}", uuid::Uuid::new_v4().simple());
        let tombstone =
            RedisConversationRepository::identity_revocations_key(&tenant, "shared-user");
        let old_sid = uuid::Uuid::new_v4().to_string();
        let _: () = conn.sadd(&tombstone, &old_sid).await.unwrap();
        let key = RedisConversationRepository::connection_key(&tenant, "shared-user");
        let index = RedisConversationRepository::tenant_sessions_key(&tenant);
        let mut old = record(&tenant, "old-device", "pending-old", 1);
        old.identity_session_id = Some(old_sid.clone());
        let denied: redis::RedisResult<String> = redis::Script::new(ADMIT_CONNECTION_SCRIPT)
            .key(&key)
            .key(&index)
            .key(gateway_conns_key(&old.gateway_id))
            .key(&tombstone)
            .arg(&old.conversation_id)
            .arg(serde_json::to_string(&old).unwrap())
            .arg(60)
            .arg(DeviceConflictStrategy::Coexist as i32)
            .arg(10)
            .arg(tenant_session_member_prefix("shared-user"))
            .arg("shared-user")
            .arg(flare_im_service_kit::gateway::lease::CONNS_KEY_PREFIX)
            .invoke_async(&mut conn)
            .await;
        assert!(denied.unwrap_err().to_string().contains("TOKEN_REVOKED"));
        let count: usize = conn.hlen(&key).await.unwrap();
        assert_eq!(
            count, 0,
            "revocation must precede all admission/index side effects"
        );
        let mut new = record(&tenant, "new-device", "new-connection", 2);
        new.identity_session_id = Some(uuid::Uuid::new_v4().to_string());
        assert!(
            admit_with(
                &mut conn,
                &key,
                &index,
                &new,
                DeviceConflictStrategy::Coexist,
                10
            )
            .await
            .is_empty()
        );
        let other_key = RedisConversationRepository::connection_key(&other, "shared-user");
        let other_index = RedisConversationRepository::tenant_sessions_key(&other);
        let mut independent = record(&other, "old-device", "other-connection", 1);
        independent.identity_session_id = Some(old_sid);
        assert!(
            admit_with(
                &mut conn,
                &other_key,
                &other_index,
                &independent,
                DeviceConflictStrategy::Coexist,
                10
            )
            .await
            .is_empty()
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
            .del(&tombstone)
            .ignore()
            .del(gateway_conns_key(&new.gateway_id))
            .ignore()
            .del(gateway_conns_key(&independent.gateway_id))
            .ignore()
            .query_async(&mut conn)
            .await
            .unwrap();
    }
}
