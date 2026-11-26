//! 构建分布上报：每 30s 把本网关按（租户，平台，构建）的在线连接数整份写进 Redis，供 capability 的
//! `ClientBuildStats.GetConnectionBuilds` 汇总（约定见 `flare_im_core_service_kit::gateway::connection_builds`）。
//!
//! 这一轮没有连接的租户删掉上一轮写的哈希并退出网关集合；停机时删掉全部，不等 TTL。
//!
//! 一轮的全部租户排进一条流水线一次发出（每个租户自成一个 MULTI 块），租户再多也只是一次往返。
//! 连接是单节点的 `redis::Client`。

use std::collections::BTreeSet;
use std::sync::Arc;

use flare_im_core_service_kit::gateway::connection_builds::{
    REPORT_INTERVAL, hash_fields, queue_gateway_builds,
};
use flare_im_core_service_kit::metrics::AccessGatewayMetrics;
use flare_im_core_service_kit::redis_connection::{RedisConnectionLimits, SharedRedisConnection};
use tokio::sync::{Mutex, oneshot};
use tracing::{debug, warn};

pub struct ConnectionBuildReporter {
    connection: SharedRedisConnection,
    gateway_id: String,
    metrics: Arc<AccessGatewayMetrics>,
    /// 上一轮写过哈希的租户
    written: Mutex<BTreeSet<String>>,
}

impl ConnectionBuildReporter {
    pub fn open(
        redis_url: &str,
        gateway_id: String,
        metrics: Arc<AccessGatewayMetrics>,
    ) -> redis::RedisResult<Self> {
        Ok(Self {
            connection: SharedRedisConnection::new(
                redis::Client::open(redis_url)?,
                RedisConnectionLimits::default(),
            ),
            gateway_id,
            metrics,
            written: Mutex::new(BTreeSet::new()),
        })
    }

    /// 写一轮：有连接的租户整份替换，上一轮有、这一轮没有的租户删掉。
    pub async fn report(&self) -> redis::RedisResult<()> {
        let snapshot = self.metrics.connection_build_snapshot();
        let mut written = self.written.lock().await;
        let (pipe, current) = round_pipeline(&snapshot, &written, &self.gateway_id, now_ms());
        if !current.is_empty() || !written.is_empty() {
            let mut connection = self.connection.get().await?;
            pipe.query_async::<()>(&mut connection).await?;
        }
        debug!(
            gateway_id = %self.gateway_id,
            tenants = current.len(),
            "connection build distribution reported"
        );
        *written = current;
        Ok(())
    }

    /// 删掉本网关写过的全部哈希（停机最后一步）。
    pub async fn clear(&self) -> redis::RedisResult<()> {
        let mut written = self.written.lock().await;
        if written.is_empty() {
            return Ok(());
        }
        let now_ms = now_ms();
        let mut pipe = redis::pipe();
        for tenant_id in written.iter() {
            queue_gateway_builds(&mut pipe, tenant_id, &self.gateway_id, &[], now_ms);
        }
        let mut connection = self.connection.get().await?;
        pipe.query_async::<()>(&mut connection).await?;
        written.clear();
        Ok(())
    }

    /// 上报循环；停机时清掉自己写的数据。
    pub async fn run(
        self: Arc<Self>,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut ticker = tokio::time::interval(REPORT_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tokio::pin!(shutdown_rx);
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                _ = ticker.tick() => {
                    if let Err(error) = self.report().await {
                        warn!(gateway_id = %self.gateway_id, %error, "connection build report failed");
                    }
                }
            }
        }
        if let Err(error) = self.clear().await {
            warn!(
                gateway_id = %self.gateway_id,
                %error,
                "connection build report not cleared; it expires on its own"
            );
        }
        Ok(())
    }
}

/// 一轮上报的流水线：这一轮有连接的租户整份替换，上一轮写过、这一轮没有的租户删掉。返回流水线和
/// 这一轮写了哈希的租户。
fn round_pipeline(
    snapshot: &[(
        String,
        Vec<flare_im_core_service_kit::metrics::PlatformBuildDistribution>,
    )],
    written: &BTreeSet<String>,
    gateway_id: &str,
    now_ms: i64,
) -> (redis::Pipeline, BTreeSet<String>) {
    let mut pipe = redis::pipe();
    let mut current = BTreeSet::new();
    for (tenant_id, platforms) in snapshot {
        let fields = hash_fields(platforms);
        if fields.is_empty() {
            continue;
        }
        queue_gateway_builds(&mut pipe, tenant_id, gateway_id, &fields, now_ms);
        current.insert(tenant_id.clone());
    }
    for tenant_id in written.difference(&current) {
        queue_gateway_builds(&mut pipe, tenant_id, gateway_id, &[], now_ms);
    }
    (pipe, current)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_im_core_service_kit::metrics::{BuildDistribution, PlatformBuildDistribution};

    fn count(packed: &[u8], word: &str) -> usize {
        // RESP bulk string of the command name: `$<len>\r\n<word>\r\n`.
        let needle = format!("${}\r\n{word}\r\n", word.len());
        packed
            .windows(needle.len())
            .filter(|window| *window == needle.as_bytes())
            .count()
    }

    #[test]
    fn one_round_is_one_pipeline_with_a_transaction_per_tenant() {
        let android = |connections| {
            vec![PlatformBuildDistribution {
                platform: "android".to_string(),
                distribution: BuildDistribution {
                    builds: vec![(201, connections)],
                    other: 0,
                },
            }]
        };
        let snapshot = vec![
            ("t1".to_string(), android(3)),
            ("t2".to_string(), android(1)),
            // No connections left: written nothing, so it counts as gone.
            ("t3".to_string(), Vec::new()),
        ];
        let written = BTreeSet::from(["t2".to_string(), "t3".to_string(), "t4".to_string()]);
        let (pipe, current) = round_pipeline(&snapshot, &written, "gw-a", 1_000_000);
        assert_eq!(
            current,
            BTreeSet::from(["t1".to_string(), "t2".to_string()])
        );
        let packed = pipe.get_packed_pipeline();
        // t1 and t2 replaced, t3 and t4 removed: four transactions in one pipeline.
        assert_eq!(count(&packed, "MULTI"), 4);
        assert_eq!(count(&packed, "EXEC"), 4);
        assert_eq!(count(&packed, "HSET"), 2);
        assert_eq!(count(&packed, "ZREM"), 2);
    }
}
