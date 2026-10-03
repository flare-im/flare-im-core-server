use std::sync::Arc;

use flare_im_contracts::Ctx;
use flare_im_contracts::wal_pending_index_key;
use flare_im_service_kit::redis_connection::SharedRedisConnection;
use flare_server_core::error::Result;
use tracing::instrument;

use crate::domain::repository::WalCleanupRepository;

pub struct RedisWalCleanupRepository {
    /// 进程共享的连接（有界建连与应答）：清理在持久写之后、确认之前，不能被 Redis 故障卡住。
    redis: Arc<SharedRedisConnection>,
    wal_key: String,
    /// Redis 更新耗时指标。以前 redis_update_duration_seconds 只有声明没有写入路径，
    /// 注册进 Prometheus 后永远显示 0，看的人会以为这条路径从没跑过。
    metrics: Option<Arc<flare_im_service_kit::metrics::StorageWriterMetrics>>,
}

impl RedisWalCleanupRepository {
    pub fn new(redis: Arc<SharedRedisConnection>, wal_key: String) -> Self {
        Self {
            redis,
            wal_key,
            metrics: None,
        }
    }

    pub fn with_metrics(
        mut self,
        metrics: Option<Arc<flare_im_service_kit::metrics::StorageWriterMetrics>>,
    ) -> Self {
        self.metrics = metrics;
        self
    }
}

impl WalCleanupRepository for RedisWalCleanupRepository {
    #[instrument(skip(self), fields(message_id))]
    async fn remove(&self, ctx: &Ctx, message_id: &str) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        let mut conn = self.redis.get().await?;
        let pending_key = wal_pending_index_key(&self.wal_key);
        let mut pipe = redis::pipe();
        pipe.atomic()
            .cmd("HDEL")
            .arg(&self.wal_key)
            .arg(message_id)
            .cmd("ZREM")
            .arg(&pending_key)
            .arg(message_id);
        let started = std::time::Instant::now();
        let _: Vec<redis::Value> = pipe.query_async(&mut conn).await?;
        if let Some(m) = &self.metrics {
            m.observe_redis_update(started.elapsed().as_secs_f64());
        }
        Ok(())
    }
}
