//! # Prometheus 指标收集模块
//!
//! 为各个服务模块提供统一的 Prometheus 指标收集能力。

use once_cell::sync::Lazy;
use prometheus::{
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
};

/// 全局指标注册表
pub static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

/// 存储写入服务指标
pub struct StorageWriterMetrics {
    /// 消息持久化总数
    pub messages_persisted_total: IntCounterVec,
    /// 消息持久化耗时（秒）
    pub messages_persisted_duration_seconds: Histogram,
    /// 数据库写入耗时（秒）
    pub db_write_duration_seconds: Histogram,
    /// Redis 更新耗时（秒）
    pub redis_update_duration_seconds: Histogram,
    /// 消息重复处理次数
    pub messages_duplicate_total: IntCounter,
    /// 批量处理大小
    pub batch_size: Histogram,
}

impl StorageWriterMetrics {
    pub fn new() -> Self {
        let messages_persisted_total = IntCounterVec::new(
            Opts::new(
                "messages_persisted_total",
                "Total number of messages persisted",
            ),
            &["tenant_id"],
        )
        .expect("Failed to create messages_persisted_total metric");

        let messages_persisted_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "messages_persisted_duration_seconds",
                "Message persistence duration in seconds",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]),
        )
        .expect("Failed to create messages_persisted_duration_seconds metric");

        let db_write_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "db_write_duration_seconds",
                "Database write duration in seconds",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5]),
        )
        .expect("Failed to create db_write_duration_seconds metric");

        let redis_update_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "redis_update_duration_seconds",
                "Redis update duration in seconds",
            )
            .buckets(vec![0.0001, 0.0005, 0.001, 0.005, 0.01]),
        )
        .expect("Failed to create redis_update_duration_seconds metric");

        let messages_duplicate_total = IntCounter::new(
            "messages_duplicate_total",
            "Total number of duplicate messages",
        )
        .expect("Failed to create messages_duplicate_total metric");

        let batch_size = Histogram::with_opts(
            HistogramOpts::new("storage_writer_batch_size", "Batch size for storage writer")
                .buckets(vec![1.0, 10.0, 50.0, 100.0, 500.0, 1000.0]),
        )
        .expect("Failed to create batch_size metric");

        // 注册指标，忽略重复注册错误（在基准测试中可能会重复创建）
        let _ = REGISTRY.register(Box::new(messages_persisted_total.clone()));
        let _ = REGISTRY.register(Box::new(messages_persisted_duration_seconds.clone()));
        let _ = REGISTRY.register(Box::new(db_write_duration_seconds.clone()));
        let _ = REGISTRY.register(Box::new(redis_update_duration_seconds.clone()));
        let _ = REGISTRY.register(Box::new(messages_duplicate_total.clone()));
        let _ = REGISTRY.register(Box::new(batch_size.clone()));

        Self {
            messages_persisted_total,
            messages_persisted_duration_seconds,
            db_write_duration_seconds,
            redis_update_duration_seconds,
            messages_duplicate_total,
            batch_size,
        }
    }
}

impl Default for StorageWriterMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Access Gateway 指标
pub struct AccessGatewayMetrics {
    /// 活跃连接数
    pub connections_active: IntGauge,
    /// 消息推送总数
    pub messages_pushed_total: IntCounterVec,
    /// 推送成功次数
    pub push_success_total: IntCounterVec,
    /// 推送失败次数
    pub push_failure_total: IntCounterVec,
    /// 连接断开次数
    pub connection_disconnected_total: IntCounter,
    /// 客户端ACK接收次数
    pub client_ack_received_total: IntCounterVec,
    /// 推送延迟（秒）
    pub push_latency_seconds: HistogramVec,
    /// 在线状态缓存命中率
    pub online_cache_hit_total: IntCounter,
    pub online_cache_miss_total: IntCounter,
}

impl AccessGatewayMetrics {
    pub fn new() -> Self {
        let connections_active =
            IntGauge::new("connections_active", "Number of active connections")
                .expect("Failed to create connections_active metric");

        let messages_pushed_total = IntCounterVec::new(
            Opts::new("messages_pushed_total", "Total number of messages pushed"),
            &["tenant_id"],
        )
        .expect("Failed to create messages_pushed_total metric");

        let push_success_total = IntCounterVec::new(
            Opts::new("push_success_total", "Total number of successful pushes"),
            &["tenant_id"],
        )
        .expect("Failed to create push_success_total metric");

        let push_failure_total = IntCounterVec::new(
            Opts::new("push_failure_total", "Total number of failed pushes"),
            &["failure_reason", "tenant_id"],
        )
        .expect("Failed to create push_failure_total metric");

        let connection_disconnected_total = IntCounter::new(
            "connection_disconnected_total",
            "Total number of disconnected connections",
        )
        .expect("Failed to create connection_disconnected_total metric");

        let client_ack_received_total = IntCounterVec::new(
            Opts::new(
                "client_ack_received_total",
                "Total number of client ACKs received",
            ),
            &["tenant_id"],
        )
        .expect("Failed to create client_ack_received_total metric");

        let push_latency_seconds = HistogramVec::new(
            HistogramOpts::new(
                "access_gateway_push_latency_seconds",
                "Push latency in seconds",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5]),
            &["tenant_id"],
        )
        .expect("Failed to create push_latency_seconds metric");

        let online_cache_hit_total = IntCounter::new(
            "online_cache_hit_total",
            "Total number of online cache hits",
        )
        .expect("Failed to create online_cache_hit_total metric");

        let online_cache_miss_total = IntCounter::new(
            "online_cache_miss_total",
            "Total number of online cache misses",
        )
        .expect("Failed to create online_cache_miss_total metric");

        let _ = REGISTRY.register(Box::new(connections_active.clone()));
        let _ = REGISTRY.register(Box::new(messages_pushed_total.clone()));
        let _ = REGISTRY.register(Box::new(push_success_total.clone()));
        let _ = REGISTRY.register(Box::new(push_failure_total.clone()));
        let _ = REGISTRY.register(Box::new(connection_disconnected_total.clone()));
        let _ = REGISTRY.register(Box::new(client_ack_received_total.clone()));
        let _ = REGISTRY.register(Box::new(push_latency_seconds.clone()));
        let _ = REGISTRY.register(Box::new(online_cache_hit_total.clone()));
        let _ = REGISTRY.register(Box::new(online_cache_miss_total.clone()));

        Self {
            connections_active,
            messages_pushed_total,
            push_success_total,
            push_failure_total,
            connection_disconnected_total,
            client_ack_received_total,
            push_latency_seconds,
            online_cache_hit_total,
            online_cache_miss_total,
        }
    }
}

impl Default for AccessGatewayMetrics {
    fn default() -> Self {
        Self::new()
    }
}
