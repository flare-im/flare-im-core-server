//! Online 写下的设备信号，与会话记录在同一个 Redis 里（Push Server 的 `PUSH_SERVER_REDIS_URL`）。
//!
//! 键名与取值是两边共用的存储格式（Online 一侧见 `flare-signaling/online` 的会话仓储）：
//!
//! - `push:dev:active:{tenant}:{user}:{device}`：设备最近一次以前台状态登录或上报的时刻（毫秒）。
//! - `push:dev:badge:{tenant}:{user}:{device}`：`{未读总数}:{时刻}`，设备最近一次上报非前台
//!   状态时带的角标基线。
//!
//! 一次路由只读一回：这一批候选设备的两个键放进一条 MGET。

use std::collections::HashMap;
use std::time::Duration;

use flare_im_contracts::Ctx;
use flare_server_core::error::{ErrorCode, FlareError};
use redis::aio::ConnectionManager;

use crate::domain::model::{BadgeBase, DeviceSignals};
use crate::domain::repository::DeviceSignalRepository;

const ACTIVE_KEY_PREFIX: &str = "push:dev:active";
const BADGE_KEY_PREFIX: &str = "push:dev:badge";

/// 单次往返的时限。读不到就当没有信号，路由不等它。
const IO_BUDGET: Duration = Duration::from_millis(500);

/// 一条 MGET 至多带多少台设备（每台两个键）。
const SIGNAL_READ_BATCH: usize = 250;

fn unavailable(reason: impl Into<String>) -> FlareError {
    FlareError::localized(
        ErrorCode::ServiceUnavailable,
        format!("device signals unavailable: {}", reason.into()),
    )
}

fn signal_key(prefix: &str, tenant_id: &str, user_id: &str, device_id: &str) -> String {
    format!("{prefix}:{tenant_id}:{user_id}:{device_id}")
}

fn parse_active(value: Option<&str>) -> i64 {
    value
        .and_then(|value| value.trim().parse::<i64>().ok())
        .unwrap_or(0)
}

fn parse_badge_base(value: Option<&str>) -> Option<BadgeBase> {
    let (count, at_ms) = value?.trim().split_once(':')?;
    Some(BadgeBase {
        count: count.parse().ok()?,
        at_ms: at_ms.parse().ok()?,
    })
}

pub struct RedisDeviceSignals {
    conn: ConnectionManager,
}

impl RedisDeviceSignals {
    /// 连接会话记录所在的 Redis。错误里不带地址：地址里可能有口令。
    pub async fn connect(redis_url: &str) -> Result<Self, FlareError> {
        let client = redis::Client::open(redis_url)
            .map_err(|_| FlareError::system("invalid push server redis uri"))?;
        let conn = tokio::time::timeout(Duration::from_secs(3), ConnectionManager::new(client))
            .await
            .map_err(|_| unavailable("connect timed out"))?
            .map_err(|err| FlareError::system(format!("connect push server redis: {err}")))?;
        Ok(Self { conn })
    }
}

#[async_trait::async_trait]
impl DeviceSignalRepository for RedisDeviceSignals {
    async fn device_signals(
        &self,
        _ctx: &Ctx,
        tenant_id: &str,
        devices: &[(String, String)],
    ) -> Result<HashMap<(String, String), DeviceSignals>, FlareError> {
        let mut out = HashMap::with_capacity(devices.len());
        let mut conn = self.conn.clone();
        for batch in devices.chunks(SIGNAL_READ_BATCH) {
            let mut cmd = redis::cmd("MGET");
            for (user_id, device_id) in batch {
                cmd.arg(signal_key(ACTIVE_KEY_PREFIX, tenant_id, user_id, device_id))
                    .arg(signal_key(BADGE_KEY_PREFIX, tenant_id, user_id, device_id));
            }
            let values: Vec<Option<String>> =
                tokio::time::timeout(IO_BUDGET, cmd.query_async(&mut conn))
                    .await
                    .map_err(|_| unavailable("read timed out"))?
                    .map_err(|err| unavailable(err.to_string()))?;
            if values.len() != batch.len() * 2 {
                return Err(unavailable("reply count mismatch"));
            }
            for (device, pair) in batch.iter().zip(values.chunks(2)) {
                let signals = DeviceSignals {
                    active_at_ms: parse_active(pair[0].as_deref()),
                    badge_base: parse_badge_base(pair[1].as_deref()),
                };
                if signals != DeviceSignals::default() {
                    out.insert(device.clone(), signals);
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signals_parse_or_are_absent() {
        assert_eq!(parse_active(Some("1731300000123")), 1_731_300_000_123);
        assert_eq!(parse_active(Some("junk")), 0);
        assert_eq!(parse_active(None), 0);
        assert_eq!(
            parse_badge_base(Some("12:1731300000123")),
            Some(BadgeBase {
                count: 12,
                at_ms: 1_731_300_000_123
            })
        );
        for broken in [None, Some(""), Some("12"), Some("x:1"), Some("1:x")] {
            assert_eq!(parse_badge_base(broken), None, "{broken:?}");
        }
    }

    /// 键名与 Online 写的一致；Online 一侧的常量在会话仓储里，改一边必须改另一边。
    #[test]
    fn keys_match_what_online_writes() {
        assert_eq!(
            signal_key(ACTIVE_KEY_PREFIX, "t1", "u1", "phone"),
            "push:dev:active:t1:u1:phone"
        );
        assert_eq!(
            signal_key(BADGE_KEY_PREFIX, "t1", "u1", "phone"),
            "push:dev:badge:t1:u1:phone"
        );
    }
}
