//! 网关租约：本网关每 5s 在 Online 会话 Redis 里续一次 `gw:lease:{gateway_id}`（TTL 15s）并登记进
//! `gw:known`。进程被 `kill -9` 或主机宕机时租约随 TTL 消失，Online 的清扫器据此按正常断开清掉
//! 这台网关的连接，推送路由也不再往它投递（约定见 `flare_im_service_kit::gateway::lease`）。
//!
//! 网关 ID 是稳定的（重启不变），所以启动时要先把上一个进程遗留的连接集合改名成孤儿网关交给
//! 清扫器：崩溃后几秒内就重启的话，旧租约还没过期就被新进程续上，清扫器永远看不到它缺失，
//! 旧进程那些连接就只能等会话 TTL。

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use flare_im_service_kit::gateway::lease::{
    GatewayLease, KNOWN_GATEWAYS_KEY, LEASE_RENEW_INTERVAL, LEASE_TTL_SECS, conns_key, lease_key,
    orphan_gateway_id,
};
use flare_im_service_kit::redis_connection::{RedisConnectionLimits, SharedRedisConnection};
use tokio::sync::oneshot;
use tracing::{error, info, warn};

/// 续租：返回旧值，用来发现「同一 ID 的租约被别的进程写过」。
const RENEW_SCRIPT: &str = r#"
local previous = redis.call('GET', KEYS[1])
redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[2]))
redis.call('SADD', KEYS[2], ARGV[3])
return previous
"#;

/// 只删自己（同一 boot）写的租约：同 ID 的新进程已经续上的租约不能被旧进程的停机流程删掉。
const RELEASE_SCRIPT: &str = r#"
local current = redis.call('GET', KEYS[1])
if not current then return 0 end
local ok, lease = pcall(cjson.decode, current)
if ok and type(lease) == 'table' and lease.boot == ARGV[1] then
  redis.call('DEL', KEYS[1])
  return 1
end
return 0
"#;

/// 上一个进程遗留的连接集合改名为孤儿网关并登记进 `gw:known`；孤儿网关永远没有租约，清扫器
/// 下一轮就按宕机处理它。
const ADOPT_SCRIPT: &str = r#"
if redis.call('EXISTS', KEYS[1]) == 1 then
  redis.call('RENAME', KEYS[1], KEYS[2])
  redis.call('SADD', KEYS[3], ARGV[1])
  return 1
end
return 0
"#;

/// 当前连接数的来源（租约值里带上，便于运维看各节点负载）。
pub type ConnectionCounter = Arc<dyn Fn() -> u64 + Send + Sync>;

pub struct GatewayLeaseWriter {
    connection: SharedRedisConnection,
    gateway_id: String,
    region: String,
    zone: String,
    /// 本进程的启动随机串，区分同一网关 ID 的前后两个进程。
    boot: String,
    connections: ConnectionCounter,
    renewed_once: AtomicBool,
}

impl GatewayLeaseWriter {
    pub fn open(
        redis_url: &str,
        gateway_id: String,
        region: Option<String>,
        zone: Option<String>,
        connections: ConnectionCounter,
    ) -> redis::RedisResult<Self> {
        Ok(Self {
            connection: SharedRedisConnection::new(
                redis::Client::open(redis_url)?,
                RedisConnectionLimits::default(),
            ),
            gateway_id,
            region: region.unwrap_or_default(),
            zone: zone.unwrap_or_default(),
            boot: uuid::Uuid::new_v4().simple().to_string()[..12].to_string(),
            connections,
            renewed_once: AtomicBool::new(false),
        })
    }

    pub fn gateway_id(&self) -> &str {
        &self.gateway_id
    }

    /// 启动时、开始接受连接之前调用：把上一个进程留下的 `gw:conns:{id}` 交给清扫器。
    /// 返回是否真的有遗留集合。
    pub async fn adopt_previous_incarnation(&self) -> redis::RedisResult<bool> {
        let orphan = orphan_gateway_id(&self.gateway_id, &self.boot);
        let mut connection = self.connection.get().await?;
        let adopted: i64 = redis::Script::new(ADOPT_SCRIPT)
            .key(conns_key(&self.gateway_id))
            .key(conns_key(&orphan))
            .key(KNOWN_GATEWAYS_KEY)
            .arg(&orphan)
            .invoke_async(&mut connection)
            .await?;
        if adopted == 1 {
            info!(
                gateway_id = %self.gateway_id,
                orphan = %orphan,
                "connections left by the previous gateway process handed to the Online sweeper"
            );
        }
        Ok(adopted == 1)
    }

    fn lease_value(&self) -> String {
        let lease = GatewayLease {
            region: self.region.clone(),
            zone: self.zone.clone(),
            conns: (self.connections)(),
            boot: self.boot.clone(),
        };
        serde_json::to_string(&lease).unwrap_or_default()
    }

    /// 续一次租约。
    pub async fn renew(&self) -> redis::RedisResult<()> {
        let mut connection = self.connection.get().await?;
        let previous: Option<String> = redis::Script::new(RENEW_SCRIPT)
            .key(lease_key(&self.gateway_id))
            .key(KNOWN_GATEWAYS_KEY)
            .arg(self.lease_value())
            .arg(LEASE_TTL_SECS)
            .arg(&self.gateway_id)
            .invoke_async(&mut connection)
            .await?;
        let first = !self.renewed_once.swap(true, Ordering::SeqCst);
        let previous_boot = previous
            .as_deref()
            .and_then(|raw| serde_json::from_str::<GatewayLease>(raw).ok())
            .map(|lease| lease.boot)
            .filter(|boot| !boot.is_empty() && *boot != self.boot);
        if let Some(previous_boot) = previous_boot {
            if first {
                warn!(
                    gateway_id = %self.gateway_id,
                    %previous_boot,
                    "lease of a previous process was still alive (fast restart after a crash, or another process uses the same GATEWAY_ID)"
                );
            } else {
                error!(
                    gateway_id = %self.gateway_id,
                    %previous_boot,
                    "gateway lease overwritten by another process: two gateways share this GATEWAY_ID"
                );
            }
        }
        Ok(())
    }

    /// 删掉自己写的租约（停机最后一步）。
    pub async fn release(&self) -> redis::RedisResult<bool> {
        let mut connection = self.connection.get().await?;
        let released: i64 = redis::Script::new(RELEASE_SCRIPT)
            .key(lease_key(&self.gateway_id))
            .arg(&self.boot)
            .invoke_async(&mut connection)
            .await?;
        Ok(released == 1)
    }

    /// 续租循环。停机时先等 `drained`（长连接服务器按现有断开逻辑关完连接、各连接已向 Online 注销），
    /// 再删租约：先删租约的话清扫器会和正常断开抢同一批连接。
    pub async fn run(
        self: Arc<Self>,
        shutdown_rx: oneshot::Receiver<()>,
        drained: impl Future<Output = ()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut ticker = tokio::time::interval_at(
            tokio::time::Instant::now() + LEASE_RENEW_INTERVAL,
            LEASE_RENEW_INTERVAL,
        );
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tokio::pin!(shutdown_rx);
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                _ = ticker.tick() => {
                    if let Err(error) = self.renew().await {
                        warn!(gateway_id = %self.gateway_id, %error, "gateway lease renewal failed");
                    }
                }
            }
        }
        drained.await;
        match self.release().await {
            Ok(true) => info!(gateway_id = %self.gateway_id, "gateway lease released"),
            Ok(false) => {}
            Err(error) => warn!(
                gateway_id = %self.gateway_id,
                %error,
                "gateway lease release failed; it expires on its own"
            ),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::AsyncCommands;

    fn writer(url: &str, gateway_id: &str, conns: u64) -> GatewayLeaseWriter {
        GatewayLeaseWriter::open(
            url,
            gateway_id.to_string(),
            Some("cn-north".into()),
            Some("az-1".into()),
            Arc::new(move || conns),
        )
        .unwrap()
    }

    /// Run with `FLARE_TEST_REDIS_URL` pointing at an isolated Redis instance.
    #[tokio::test]
    #[ignore = "requires FLARE_TEST_REDIS_URL"]
    async fn renew_adopt_and_release_follow_the_lease_contract() {
        let url = std::env::var("FLARE_TEST_REDIS_URL").expect("FLARE_TEST_REDIS_URL");
        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let gateway_id = format!("gw-test-{}", uuid::Uuid::new_v4().simple());

        // 上一个进程崩溃前留下的连接集合。
        let _: () = conn
            .sadd(conns_key(&gateway_id), "v1:1:t:1:u:old-session")
            .await
            .unwrap();
        let first = writer(&url, &gateway_id, 7);
        assert!(first.adopt_previous_incarnation().await.unwrap());
        let orphan = orphan_gateway_id(&gateway_id, &first.boot);
        assert!(
            !conn
                .exists::<_, bool>(conns_key(&gateway_id))
                .await
                .unwrap()
        );
        assert!(
            conn.sismember::<_, _, bool>(conns_key(&orphan), "v1:1:t:1:u:old-session")
                .await
                .unwrap()
        );
        assert!(
            conn.sismember::<_, _, bool>(KNOWN_GATEWAYS_KEY, &orphan)
                .await
                .unwrap(),
            "the orphan has no lease, so the sweeper treats it as dead"
        );
        assert!(!first.adopt_previous_incarnation().await.unwrap());

        first.renew().await.unwrap();
        let raw: String = conn.get(lease_key(&gateway_id)).await.unwrap();
        let lease: GatewayLease = serde_json::from_str(&raw).unwrap();
        assert_eq!(
            (lease.region.as_str(), lease.zone.as_str(), lease.conns),
            ("cn-north", "az-1", 7)
        );
        let ttl: i64 = conn.ttl(lease_key(&gateway_id)).await.unwrap();
        assert!(ttl > 0 && ttl <= LEASE_TTL_SECS as i64, "ttl={ttl}");
        assert!(
            conn.sismember::<_, _, bool>(KNOWN_GATEWAYS_KEY, &gateway_id)
                .await
                .unwrap()
        );

        // 同 ID 的新进程续上之后，旧进程的停机流程不能删掉新租约。
        let second = writer(&url, &gateway_id, 1);
        second.renew().await.unwrap();
        assert!(!first.release().await.unwrap());
        assert!(
            conn.exists::<_, bool>(lease_key(&gateway_id))
                .await
                .unwrap()
        );
        assert!(second.release().await.unwrap());
        assert!(
            !conn
                .exists::<_, bool>(lease_key(&gateway_id))
                .await
                .unwrap()
        );

        let _: () = redis::pipe()
            .del(conns_key(&orphan))
            .ignore()
            .srem(KNOWN_GATEWAYS_KEY, &gateway_id)
            .ignore()
            .srem(KNOWN_GATEWAYS_KEY, &orphan)
            .ignore()
            .query_async(&mut conn)
            .await
            .unwrap();
    }
}
