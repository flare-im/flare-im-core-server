use std::sync::Arc;

use chrono::{TimeZone, Utc};
use flare_server_core::error::{ErrorCode, Result, map_infra_error};
use redis::{AsyncCommands, aio::ConnectionManager};

use crate::config::ConversationConfig;
use crate::domain::model::{DevicePresence, DeviceState};
use crate::domain::repository::{PresenceRepository, PresenceUpdate};

pub struct RedisPresenceRepository {
    client: Arc<redis::Client>,
    config: Arc<ConversationConfig>,
}

impl RedisPresenceRepository {
    pub fn new(client: Arc<redis::Client>, config: Arc<ConversationConfig>) -> Self {
        Self { client, config }
    }

    async fn connection(&self) -> Result<ConnectionManager> {
        ConnectionManager::new(self.client.as_ref().clone())
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "redis connection"))
    }

    fn device_key(&self, user_id: &str, device_id: &str) -> String {
        device_key(&self.config.presence_prefix, user_id, device_id)
    }

    fn devices_index_key(&self, user_id: &str) -> String {
        devices_index_key(&self.config.presence_prefix, user_id)
    }
}

fn device_key(prefix: &str, user_id: &str, device_id: &str) -> String {
    format!("{prefix}:{user_id}:{device_id}")
}

/// 该用户的设备索引集合（SET，成员是 device_id）。
///
/// 用 `{prefix}-index:` 而不是 `{prefix}:`：设备键恒以 `{prefix}:` 开头，
/// 索引键恒以 `{prefix}-index:` 开头，紧跟前缀的那个字符一个是 `:` 一个是 `-`，
/// 所以**无论 user_id / device_id 取什么值**都不可能撞名。
///
/// 最初写成 `{prefix}:{user_id}`（"两段 vs 三段所以撞不上"），
/// 被本文件的测试直接证伪：user_id 含冒号时
/// `index("u1:d1")` 与 `device("u1","d1")` 完全相同。
fn devices_index_key(prefix: &str, user_id: &str) -> String {
    format!("{prefix}-index:{user_id}")
}



impl PresenceRepository for RedisPresenceRepository {
    async fn list_devices(
        &self,
        _ctx: &flare_server_core::context::Context,
        user_id: &str,
    ) -> Result<Vec<DevicePresence>> {
        let mut conn = self.connection().await?;
        let mut devices = Vec::new();

        // 这里原本是 `KEYS {prefix}:{user}:*`。KEYS 会扫描**整个键空间**，而且是
        // 单线程独占执行——线上 slowlog 实测单次 460–477ms，期间所有服务的 Redis
        // 请求全部排队。更要命的是这条路径在**会话 bootstrap** 里（登录必经），
        // 即每次登录都让整个 Redis 停顿约 0.5 秒。
        //
        // 改为按用户维护设备索引集合，读取变成 O(该用户设备数)。
        let device_ids: Vec<String> = conn
            .smembers(self.devices_index_key(user_id))
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "redis smembers"))?;
        if device_ids.is_empty() {
            return Ok(devices);
        }

        // 原本是逐键 HGETALL（N+1 次往返），改为一次 pipeline 取回。
        let mut pipe = redis::pipe();
        for device_id in &device_ids {
            pipe.cmd("HGETALL").arg(self.device_key(user_id, device_id));
        }
        let maps: Vec<std::collections::HashMap<String, String>> = pipe
            .query_async(&mut conn)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "redis hgetall pipeline"))?;

        let mut stale: Vec<String> = Vec::new();
        for (device_id, map) in device_ids.iter().zip(maps.into_iter()) {
            // 索引里有、哈希已不存在：设备记录被删或过期了。顺手把索引清干净，
            // 否则索引只增不减，读取代价会随历史设备累积。
            if map.is_empty() {
                stale.push(device_id.clone());
                continue;
            }
            {
                let device_id = device_id.as_str();
                let state = map
                    .get("state")
                    .map(|v| DeviceState::from_persistence_value(v.as_str()))
                    .unwrap_or(DeviceState::Unspecified);
                let last_seen_at = map
                    .get("last_seen_ts")
                    .and_then(|v| v.parse::<i64>().ok())
                    .and_then(|ts| Utc.timestamp_millis_opt(ts).single());
                let presence = DevicePresence {
                    device_id: device_id.to_string(),
                    device_platform: map.get("platform").cloned().filter(|v| !v.is_empty()),
                    state,
                    last_seen_at,
                };
                devices.push(presence);
            }
        }

        if !stale.is_empty() {
            let _: std::result::Result<(), _> = conn
                .srem::<_, _, ()>(self.devices_index_key(user_id), stale)
                .await;
        }

        Ok(devices)
    }

    async fn update_presence(
        &self,
        _ctx: &flare_server_core::context::Context,
        update: PresenceUpdate,
    ) -> Result<()> {
        let mut conn = self.connection().await?;
        let key = self.device_key(&update.user_id, &update.device_id);
        let now = Utc::now().timestamp_millis();
        let state = match update.state {
            DeviceState::Online => "online",
            DeviceState::Offline => "offline",
            DeviceState::Conflict => "conflict",
            DeviceState::Unspecified => "unknown",
        };

        let conflict_resolution = update
            .conflict_resolution
            .map(|c| c.as_str().to_string())
            .unwrap_or_default();
        let conflict_reason = update.conflict_reason.unwrap_or_default();

        let notify_conflict = if update.notify_conflict { "1" } else { "0" };

        let fields = [
            (
                "platform".to_string(),
                update.device_platform.clone().unwrap_or_default(),
            ),
            ("state".to_string(), state.to_string()),
            ("last_seen_ts".to_string(), now.to_string()),
            ("conflict_resolution".to_string(), conflict_resolution),
            ("notify_conflict".to_string(), notify_conflict.to_string()),
            ("conflict_reason".to_string(), conflict_reason),
        ];

        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        // 索引与哈希必须一起写，否则 list_devices 看不到这台设备。
        let mut pipe = redis::pipe();
        pipe.atomic();
        pipe.cmd("HSET").arg(&key).arg(&field_refs);
        pipe.cmd("SADD")
            .arg(self.devices_index_key(&update.user_id))
            .arg(&update.device_id);
        let _: Vec<redis::Value> = pipe
            .query_async(&mut conn)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "redis presence write"))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{device_key, devices_index_key};

    #[test]
    fn index_key_can_never_collide_with_a_device_key() {
        let prefix = "presence:user";
        assert_eq!(devices_index_key(prefix, "u1"), "presence:user-index:u1");
        assert_eq!(device_key(prefix, "u1", "d1"), "presence:user:u1:d1");

        // 穷举容易撞名的取值：user_id / device_id 含冒号是最危险的一类。
        // 第一版设计（索引键写成 `{prefix}:{user_id}`）正是被下面
        // ("u1:d1", "u1", "d1") 这组证伪的。
        let evil = ["", "u1", "d1", "u1:d1", ":", "::", "index", "-index"];
        for iu in evil {
            for du in evil {
                for dd in evil {
                    assert_ne!(
                        devices_index_key(prefix, iu),
                        device_key(prefix, du, dd),
                        "索引键(user={iu:?}) 与设备键(user={du:?}, device={dd:?}) 撞名"
                    );
                }
            }
        }
    }

    #[test]
    fn presence_read_path_must_not_use_keys_command() {
        // 回归门禁：`KEYS` 会扫描整个键空间且**独占阻塞** Redis，
        // 线上 slowlog 实测单次 460–477ms。这条路径在会话 bootstrap 里（登录必经），
        // 等于每次登录让整个 Redis 停顿半秒。
        // 只扫非测试部分：断言里写的那些字面量本身就含 KEYS，会自我误报
        let source = include_str!("redis_presence.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("源码非空");
        assert!(
            !source.contains(".keys("),
            "presence 读取不能用 KEYS：它阻塞整个 Redis"
        );
        assert!(
            !source.contains(r#"cmd("KEYS")"#),
            "presence 读取不能用 KEYS：它阻塞整个 Redis"
        );
        assert!(
            source.contains("smembers"),
            "应通过每用户设备索引集合读取"
        );
    }
}
