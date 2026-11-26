//! 接入网关租约与网关连接索引：access-gateway、signaling-online、推送路由三方共用的约定。
//!
//! 网关进程被 `kill -9`、所在主机宕机时，它持有的连接不会走正常断开：Online 里这些会话一直
//! 挂到 TTL，推送也继续往一台已经不存在的网关上投。为此：
//!
//! - 网关每 [`LEASE_RENEW_INTERVAL`] 写一次 `gw:lease:{gateway_id}`（TTL [`LEASE_TTL_SECS`]），
//!   同时 `SADD gw:known {gateway_id}`；
//! - Online 的准入 / 断开脚本在增删会话哈希的同一个事务里维护 `gw:conns:{gateway_id}`
//!   （成员见 [`connection_member`]，能定位回 `session:{tenant}:{user}` 里的那条记录）；
//! - Online 的单例清扫器发现 `gw:known` 中某个网关租约缺失，就按正常断开清掉它的连接；
//! - 推送路由投递前查租约（本地缓存 [`LEASE_PROBE_CACHE_TTL`]），租约缺失的网关上的设备按离线处理。
//!
//! 这些键都放在 Online 会话所在的 Redis（[`session_redis_url`]）：`gw:conns` 必须与会话哈希同实例
//! 才能原子维护，租约与已知网关集合放在一起，清扫器与推送路由一次连接就能读全。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::config::FlareAppConfig;
use crate::redis_connection::{RedisConnectionLimits, SharedRedisConnection};

/// 租约键前缀：`gw:lease:{gateway_id}`。
pub const LEASE_KEY_PREFIX: &str = "gw:lease:";
/// 写过租约的网关集合。清扫器只看这里面的网关，从没写过租约的网关（旧版本、没配会话 Redis）
/// 不会被当成宕机。
pub const KNOWN_GATEWAYS_KEY: &str = "gw:known";
/// 网关连接索引键前缀：`gw:conns:{gateway_id}`。
pub const CONNS_KEY_PREFIX: &str = "gw:conns:";
/// 租约 TTL。续期间隔的 3 倍：丢一两次续期（Redis 抖动、GC 停顿）不会被误判宕机。
pub const LEASE_TTL_SECS: u64 = 15;
/// 续期间隔。
pub const LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(5);
/// 推送路由对租约结论的本地缓存时长：避免每条消息一次 Redis 往返。
pub const LEASE_PROBE_CACHE_TTL: Duration = Duration::from_secs(2);
/// 网关重启后把上一个进程遗留的连接集合改名成孤儿网关，交给清扫器按宕机处理；孤儿 ID 由
/// 原网关 ID、这个标记与新进程的启动随机串拼成。
pub const ORPHAN_MARKER: &str = "#orphan-";

/// 读 Online 会话 Redis 时与 signaling-online 用同一个环境变量覆盖，两边不会各指一个实例。
const SESSION_REDIS_URL_ENV: &str = "SIGNALING_ONLINE_REDIS_URL";
/// 本地缓存的网关数上限，超过就先清掉过期项。
const PROBE_CACHE_MAX_ENTRIES: usize = 1024;

pub fn lease_key(gateway_id: &str) -> String {
    format!("{LEASE_KEY_PREFIX}{gateway_id}")
}

pub fn conns_key(gateway_id: &str) -> String {
    format!("{CONNS_KEY_PREFIX}{gateway_id}")
}

/// 上一个进程遗留连接集合的改名目标（见 [`ORPHAN_MARKER`]）。
pub fn orphan_gateway_id(gateway_id: &str, boot: &str) -> String {
    format!("{gateway_id}{ORPHAN_MARKER}{boot}")
}

/// `gw:known` 成员对应的真实网关 ID：孤儿 ID 去掉标记与启动串，其余原样。
pub fn base_gateway_id(known_id: &str) -> &str {
    known_id
        .rfind(ORPHAN_MARKER)
        .map(|at| &known_id[..at])
        .unwrap_or(known_id)
}

/// `gw:conns:{gateway_id}` 的成员：`v1:{租户字节长}:{租户}:{用户字节长}:{用户}:{会话字段}`。
/// 用户 ID 可以带 `:`，所以按长度切分；Online 的 Lua 脚本按同一格式拼接（`string.len` 也是字节长）。
pub fn connection_member(tenant_id: &str, user_id: &str, session_field: &str) -> String {
    format!(
        "v1:{}:{tenant_id}:{}:{user_id}:{session_field}",
        tenant_id.len(),
        user_id.len()
    )
}

/// 反解 [`connection_member`]：`(tenant, user, session_field)`。
pub fn parse_connection_member(member: &str) -> Option<(&str, &str, &str)> {
    let rest = member.strip_prefix("v1:")?;
    let (tenant_len, rest) = rest.split_once(':')?;
    let tenant_len = tenant_len.parse::<usize>().ok()?;
    let tenant = rest.get(..tenant_len)?;
    let rest = rest.get(tenant_len..)?.strip_prefix(':')?;
    let (user_len, rest) = rest.split_once(':')?;
    let user_len = user_len.parse::<usize>().ok()?;
    let user = rest.get(..user_len)?;
    let field = rest.get(user_len..)?.strip_prefix(':')?;
    (!tenant.is_empty() && !user.is_empty() && !field.is_empty()).then_some((tenant, user, field))
}

/// 租约的值。`boot` 是网关进程的启动随机串：同一 ID 的租约换了 `boot`，说明前一个进程已经不在
/// （或两台进程配了同一个 `GATEWAY_ID`）。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayLease {
    #[serde(default)]
    pub region: String,
    #[serde(default)]
    pub zone: String,
    #[serde(default)]
    pub conns: u64,
    #[serde(default)]
    pub boot: String,
}

/// Online 会话 Redis：`SIGNALING_ONLINE_REDIS_URL`，否则 `services.signaling_online.redis` 指向的
/// profile。两者都没有时返回 `None`，调用方不启用租约（网关不写、路由不查），行为与以前一致。
pub fn session_redis_url(app: &FlareAppConfig) -> Option<String> {
    std::env::var(SESSION_REDIS_URL_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| {
            app.signaling_online_service()
                .redis
                .as_deref()
                .and_then(|name| app.redis_profile(name))
                .map(|profile| profile.url.clone())
        })
}

/// 网关是否还能投递：租约在，或者它根本不在租约体系里（不在 `gw:known`）。后者让滚动升级期间
/// 还没写租约的旧网关照常收推送，而不是被当成宕机、设备全部转离线。
pub fn gateway_routable(known: bool, lease_present: bool) -> bool {
    lease_present || !known
}

/// 租约事实的来源。生产实现是 [`RedisGatewayLeaseSource`]；测试可以注入替身。
#[async_trait::async_trait]
pub trait GatewayLeaseSource: Send + Sync {
    /// `(gateway_id 在 gw:known 里, 租约存在)`。
    async fn lookup(&self, gateway_id: &str) -> redis::RedisResult<(bool, bool)>;
}

/// 从 Online 会话 Redis 读租约：一次流水线 `EXISTS gw:lease:{id}` + `SISMEMBER gw:known {id}`。
pub struct RedisGatewayLeaseSource {
    connection: SharedRedisConnection,
}

impl RedisGatewayLeaseSource {
    pub fn open(redis_url: &str) -> redis::RedisResult<Self> {
        Ok(Self {
            connection: SharedRedisConnection::new(
                redis::Client::open(redis_url)?,
                RedisConnectionLimits::default(),
            ),
        })
    }
}

#[async_trait::async_trait]
impl GatewayLeaseSource for RedisGatewayLeaseSource {
    async fn lookup(&self, gateway_id: &str) -> redis::RedisResult<(bool, bool)> {
        let mut connection = self.connection.get().await?;
        let (lease_present, known): (bool, bool) = redis::pipe()
            .exists(lease_key(gateway_id))
            .sismember(KNOWN_GATEWAYS_KEY, gateway_id)
            .query_async(&mut connection)
            .await?;
        Ok((known, lease_present))
    }
}

/// 推送路由用的租约探针：结论按网关缓存 [`LEASE_PROBE_CACHE_TTL`]。
///
/// Redis 读不到时按「可投递」处理（fail open）：租约只是让投递更早放弃一台已宕机的网关，
/// Redis 抖动不能把所有在线设备一起判成离线。
pub struct GatewayLeaseProbe {
    source: Arc<dyn GatewayLeaseSource>,
    cache_ttl: Duration,
    cache: std::sync::Mutex<HashMap<String, (bool, Instant)>>,
}

impl GatewayLeaseProbe {
    pub fn new(source: Arc<dyn GatewayLeaseSource>, cache_ttl: Duration) -> Self {
        Self {
            source,
            cache_ttl,
            cache: std::sync::Mutex::new(HashMap::new()),
        }
    }

    pub fn from_redis_url(redis_url: &str) -> redis::RedisResult<Self> {
        Ok(Self::new(
            Arc::new(RedisGatewayLeaseSource::open(redis_url)?),
            LEASE_PROBE_CACHE_TTL,
        ))
    }

    /// 能否把推送投到这台网关（见 [`gateway_routable`]）。
    pub async fn is_routable(&self, gateway_id: &str) -> bool {
        if gateway_id.is_empty() {
            return true;
        }
        let now = Instant::now();
        if let Some((routable, expires_at)) = self
            .cache
            .lock()
            .ok()
            .and_then(|cache| cache.get(gateway_id).copied())
            && expires_at > now
        {
            return routable;
        }
        let routable = match self.source.lookup(gateway_id).await {
            Ok((known, lease_present)) => gateway_routable(known, lease_present),
            Err(error) => {
                tracing::warn!(
                    gateway_id,
                    %error,
                    "gateway lease lookup failed; routing as if the gateway were alive"
                );
                true
            }
        };
        if let Ok(mut cache) = self.cache.lock() {
            if cache.len() >= PROBE_CACHE_MAX_ENTRIES {
                cache.retain(|_, (_, expires_at)| *expires_at > now);
            }
            cache.insert(gateway_id.to_string(), (routable, now + self.cache_ttl));
        }
        routable
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FixedSource {
        known: bool,
        lease_present: bool,
        fail: bool,
        lookups: AtomicUsize,
    }

    impl FixedSource {
        fn new(known: bool, lease_present: bool) -> Arc<Self> {
            Arc::new(Self {
                known,
                lease_present,
                fail: false,
                lookups: AtomicUsize::new(0),
            })
        }
    }

    #[async_trait::async_trait]
    impl GatewayLeaseSource for FixedSource {
        async fn lookup(&self, _gateway_id: &str) -> redis::RedisResult<(bool, bool)> {
            self.lookups.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(redis::RedisError::from(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "redis down",
                )));
            }
            Ok((self.known, self.lease_present))
        }
    }

    #[test]
    fn member_round_trips_ids_that_contain_the_delimiter() {
        let member = connection_member("acme", "u:1:", "6f1c-session");
        assert_eq!(member, "v1:4:acme:4:u:1::6f1c-session");
        assert_eq!(
            parse_connection_member(&member),
            Some(("acme", "u:1:", "6f1c-session"))
        );
        assert_eq!(parse_connection_member("v1:4:acme:2:u1:"), None);
        assert_eq!(parse_connection_member("v2:4:acme:2:u1:s"), None);
        assert_eq!(parse_connection_member("v1:9:acme:2:u1:s"), None);
    }

    #[test]
    fn orphan_ids_map_back_to_the_real_gateway() {
        let orphan = orphan_gateway_id("gateway-host-a-60051", "b00t");
        assert_eq!(orphan, "gateway-host-a-60051#orphan-b00t");
        assert_eq!(base_gateway_id(&orphan), "gateway-host-a-60051");
        assert_eq!(
            base_gateway_id("gateway-host-a-60051"),
            "gateway-host-a-60051"
        );
        assert_eq!(lease_key("g1"), "gw:lease:g1");
        assert_eq!(conns_key("g1"), "gw:conns:g1");
    }

    /// 只有「在租约体系里、租约却没了」才算宕机；从没写过租约的旧网关照常投递。
    #[test]
    fn only_a_known_gateway_without_lease_is_unroutable() {
        assert!(gateway_routable(true, true));
        assert!(!gateway_routable(true, false));
        assert!(gateway_routable(false, false));
        assert!(gateway_routable(false, true));
    }

    #[tokio::test]
    async fn the_verdict_is_cached_for_the_probe_ttl() {
        let source = FixedSource::new(true, false);
        let probe = GatewayLeaseProbe::new(source.clone(), Duration::from_millis(80));
        assert!(!probe.is_routable("gw-dead").await);
        assert!(!probe.is_routable("gw-dead").await);
        assert_eq!(
            source.lookups.load(Ordering::SeqCst),
            1,
            "second call is cached"
        );
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(!probe.is_routable("gw-dead").await);
        assert_eq!(
            source.lookups.load(Ordering::SeqCst),
            2,
            "expired entry is re-read"
        );
    }

    #[tokio::test]
    async fn a_redis_failure_fails_open() {
        let source = Arc::new(FixedSource {
            known: true,
            lease_present: false,
            fail: true,
            lookups: AtomicUsize::new(0),
        });
        let probe = GatewayLeaseProbe::new(source, LEASE_PROBE_CACHE_TTL);
        assert!(probe.is_routable("gw-1").await);
    }

    #[test]
    fn lease_value_tolerates_missing_fields() {
        let lease: GatewayLease = serde_json::from_str(r#"{"conns":3}"#).unwrap();
        assert_eq!(lease.conns, 3);
        assert!(lease.boot.is_empty());
    }
}
