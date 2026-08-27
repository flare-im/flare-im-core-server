//! # Prometheus 指标收集模块
//!
//! 为各个服务模块提供统一的 Prometheus 指标收集与导出能力。

use std::{net::SocketAddr, time::Duration};

use axum::{
    Router,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use once_cell::sync::Lazy;
use prometheus::{
    Encoder, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts,
    Registry, TextEncoder,
};
use tokio::{net::TcpListener, sync::oneshot};

/// 全局指标注册表
pub static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

/// Prometheus 指标 HTTP 出口配置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsEndpointConfig {
    pub enabled: bool,
    pub address: String,
    pub port: u16,
    pub path: String,
}

impl MetricsEndpointConfig {
    pub fn new(address: impl Into<String>, port: u16) -> Self {
        Self {
            enabled: true,
            address: address.into(),
            port,
            path: "/metrics".to_string(),
        }
    }

    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        self.path = normalize_metrics_path(path.into());
        self
    }

    pub fn disabled() -> Self {
        Self {
            enabled: false,
            address: "127.0.0.1".to_string(),
            port: 0,
            path: "/metrics".to_string(),
        }
    }

    pub fn socket_addr(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        format!("{}:{}", self.address, self.port).parse()
    }
}

impl Default for MetricsEndpointConfig {
    fn default() -> Self {
        Self::disabled()
    }
}

fn normalize_metrics_path(path: String) -> String {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        "/metrics".to_string()
    } else if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{trimmed}")
    }
}

/// 将全局注册表编码为 Prometheus text format。
pub fn encode_prometheus_text() -> Result<String, prometheus::Error> {
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer)?;
    Ok(String::from_utf8_lossy(&buffer).into_owned())
}

async fn metrics_handler() -> Response {
    match encode_prometheus_text() {
        Ok(body) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, prometheus::TEXT_FORMAT)],
            body,
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("encode prometheus metrics failed: {error}"),
        )
            .into_response(),
    }
}

/// 启动轻量 Prometheus `/metrics` HTTP 出口。
pub async fn serve_prometheus_metrics(
    config: MetricsEndpointConfig,
    shutdown_rx: oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !config.enabled {
        let _ = shutdown_rx.await;
        return Ok(());
    }

    let address = config.socket_addr()?;
    let path = normalize_metrics_path(config.path);
    let app = Router::new().route(&path, get(metrics_handler));
    let listener = TcpListener::bind(address).await?;

    tracing::info!(
        %address,
        path = %path,
        "Prometheus metrics endpoint listening"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = shutdown_rx.await;
        })
        .await?;

    Ok(())
}

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
    /// 存储持久化一等结果指标。
    pub storage_persist_total: IntCounterVec,
    /// 写入账本状态迁移指标。
    pub ledger_transition_total: IntCounterVec,
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

        let storage_persist_total = IntCounterVec::new(
            Opts::new(
                "storage_writer_persist_total",
                "Total number of storage persist attempts by path and result",
            ),
            &["path", "result"],
        )
        .expect("Failed to create storage_writer_persist_total metric");

        let ledger_transition_total = IntCounterVec::new(
            Opts::new(
                "storage_writer_ledger_transition_total",
                "Total number of message write ledger transitions by stage and result",
            ),
            &["stage", "result"],
        )
        .expect("Failed to create storage_writer_ledger_transition_total metric");

        // 注册指标，忽略重复注册错误（在基准测试中可能会重复创建）
        let _ = REGISTRY.register(Box::new(messages_persisted_total.clone()));
        let _ = REGISTRY.register(Box::new(messages_persisted_duration_seconds.clone()));
        let _ = REGISTRY.register(Box::new(db_write_duration_seconds.clone()));
        let _ = REGISTRY.register(Box::new(redis_update_duration_seconds.clone()));
        let _ = REGISTRY.register(Box::new(messages_duplicate_total.clone()));
        let _ = REGISTRY.register(Box::new(batch_size.clone()));
        let _ = REGISTRY.register(Box::new(storage_persist_total.clone()));
        let _ = REGISTRY.register(Box::new(ledger_transition_total.clone()));

        Self {
            messages_persisted_total,
            messages_persisted_duration_seconds,
            db_write_duration_seconds,
            redis_update_duration_seconds,
            messages_duplicate_total,
            batch_size,
            storage_persist_total,
            ledger_transition_total,
        }
    }

    pub fn record_storage_persist(&self, path: &str, result: &str) {
        self.storage_persist_total
            .with_label_values(&[path, result])
            .inc();
    }

    pub fn record_ledger_transition(&self, stage: &str, result: &str) {
        self.ledger_transition_total
            .with_label_values(&[stage, result])
            .inc();
    }
}

impl Default for StorageWriterMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Push Worker 指标。
pub struct PushWorkerMetrics {
    /// 离线推送重投递/降级处置计数。
    pub offline_redelivery_total: IntCounterVec,
}

impl PushWorkerMetrics {
    pub fn new() -> Self {
        let offline_redelivery_total = IntCounterVec::new(
            Opts::new(
                "push_worker_offline_redelivery_total",
                "Total number of offline push redelivery prevention actions",
            ),
            &["reason", "action"],
        )
        .expect("Failed to create push_worker_offline_redelivery_total metric");

        let _ = REGISTRY.register(Box::new(offline_redelivery_total.clone()));

        Self {
            offline_redelivery_total,
        }
    }

    pub fn record_offline_redelivery(&self, reason: &str, action: &str) {
        self.offline_redelivery_total
            .with_label_values(&[reason, action])
            .inc();
    }
}

impl Default for PushWorkerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Message Orchestrator send-path 指标。
///
/// Label 保持低基数：只使用固定 stage/outcome/durability，避免按租户、会话或消息 ID 打标签。
pub struct MessageOrchestratorMetrics {
    /// Send path 分段耗时。
    pub send_stage_duration_seconds: HistogramVec,
    /// Send 请求结果总数。
    pub send_total: IntCounterVec,
    /// Batch send 请求大小。
    pub batch_send_size: Histogram,
    /// 扇出耗时：从摄入时刻到把消息投出去。
    ///
    /// **两端都取服务端时钟**，因此不受客户端时钟偏差与跨网 RTT 影响——
    /// 这正是以前答不上「消息从落库到送达之间那几百毫秒去哪了」的原因：
    /// `messages.created_at` 存的是客户端时钟，客户端观测又混着网络抖动，
    /// 两个都不能用来给服务端定责。
    pub fanout_latency_seconds: HistogramVec,
}

impl MessageOrchestratorMetrics {
    pub fn new() -> Self {
        let send_stage_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "message_orchestrator_send_stage_duration_seconds",
                "Message orchestrator send path stage duration in seconds",
            )
            .buckets(vec![
                0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
            ]),
            &["stage", "outcome"],
        )
        .expect("Failed to create message_orchestrator_send_stage_duration_seconds metric");

        let send_total = IntCounterVec::new(
            Opts::new(
                "message_orchestrator_send_total",
                "Total number of message send attempts handled by orchestrator",
            ),
            &["durability", "outcome"],
        )
        .expect("Failed to create message_orchestrator_send_total metric");

        let fanout_latency_seconds = HistogramVec::new(
            HistogramOpts::new(
                "message_orchestrator_fanout_latency_seconds",
                "Latency from message ingestion to fanout dispatch, measured entirely on server clock",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
            ]),
            // 低基数：只区分投递方式与规模档，不按租户/会话打标签
            &["mode", "size_bucket"],
        )
        .expect("Failed to create message_orchestrator_fanout_latency_seconds metric");

        let batch_send_size = Histogram::with_opts(
            HistogramOpts::new(
                "message_orchestrator_batch_send_size",
                "Batch size for orchestrator batch send requests",
            )
            .buckets(vec![1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0]),
        )
        .expect("Failed to create message_orchestrator_batch_send_size metric");

        let _ = REGISTRY.register(Box::new(send_stage_duration_seconds.clone()));
        let _ = REGISTRY.register(Box::new(send_total.clone()));
        let _ = REGISTRY.register(Box::new(batch_send_size.clone()));
        let _ = REGISTRY.register(Box::new(fanout_latency_seconds.clone()));

        Self {
            send_stage_duration_seconds,
            send_total,
            batch_send_size,
            fanout_latency_seconds,
        }
    }

    pub fn observe_send_stage(&self, stage: &str, outcome: &str, duration: Duration) {
        self.send_stage_duration_seconds
            .with_label_values(&[stage, outcome])
            .observe(duration.as_secs_f64());
    }

    pub fn record_send_total(&self, durability: &str, outcome: &str) {
        self.send_total
            .with_label_values(&[durability, outcome])
            .inc();
    }

    pub fn observe_batch_size(&self, size: usize) {
        self.batch_send_size.observe(size as f64);
    }

    /// 记录扇出耗时。`ingestion_ts_ms` 是**服务端**摄入时刻（毫秒）。
    ///
    /// 传 0 或未来时间会被忽略：客户端可能塞进来一个偏了几十秒的时间戳，
    /// 把它算进直方图会把分位数彻底污染（实测有客户端时钟慢 34 秒）。
    pub fn observe_fanout_latency(&self, mode: &str, recipient_count: usize, ingestion_ts_ms: i64) {
        if ingestion_ts_ms <= 0 {
            return;
        }
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        if now_ms == 0 {
            return;
        }
        let elapsed_ms = now_ms - ingestion_ts_ms;
        if elapsed_ms < 0 {
            return;
        }
        let size_bucket = match recipient_count {
            0..=1 => "1",
            2..=10 => "10",
            11..=100 => "100",
            101..=500 => "500",
            _ => "500+",
        };
        self.fanout_latency_seconds
            .with_label_values(&[mode, size_bucket])
            .observe(elapsed_ms as f64 / 1000.0);
    }
}

#[cfg(test)]
mod fanout_latency_tests {
    use super::MessageOrchestratorMetrics;

    /// 荒谬的摄入时刻必须被丢弃，不能污染分位数。
    ///
    /// 这条防线是有来由的：`messages.created_at` 存的是**客户端**时钟，
    /// 实测有客户端慢 34 秒。一旦这种值混进直方图，p50/p90 就全废了，
    /// 而这个指标的全部意义就是给服务端耗时定责。
    #[test]
    fn bogus_ingestion_timestamps_are_ignored() {
        let m = MessageOrchestratorMetrics::new();
        let before = m
            .fanout_latency_seconds
            .with_label_values(&["inline", "1"])
            .get_sample_count();

        m.observe_fanout_latency("inline", 1, 0); // 取不到
        m.observe_fanout_latency("inline", 1, -1); // 非法
        let future = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 60_000;
        m.observe_fanout_latency("inline", 1, future); // 未来时间（客户端时钟快）

        let after = m
            .fanout_latency_seconds
            .with_label_values(&["inline", "1"])
            .get_sample_count();
        assert_eq!(after, before, "非法/未来的摄入时刻不能被计入");
    }

    /// 正常值要被计入，且按收件人规模分桶（低基数）。
    #[test]
    fn valid_samples_are_recorded_and_bucketed_by_size() {
        let m = MessageOrchestratorMetrics::new();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        m.observe_fanout_latency("inline", 1, now - 10);
        m.observe_fanout_latency("inline", 300, now - 10);

        assert_eq!(
            m.fanout_latency_seconds
                .with_label_values(&["inline", "1"])
                .get_sample_count(),
            1
        );
        assert_eq!(
            m.fanout_latency_seconds
                .with_label_values(&["inline", "500"])
                .get_sample_count(),
            1,
            "300 个收件人应落在 500 档"
        );
    }
}

impl Default for MessageOrchestratorMetrics {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_orchestrator_metrics_records_send_result() {
        let metrics = MessageOrchestratorMetrics::new();

        metrics.observe_send_stage("mq_publish", "success", Duration::from_millis(7));
        metrics.record_send_total("broker_accepted", "success");
        metrics.observe_batch_size(3);

        assert_eq!(
            metrics
                .send_total
                .with_label_values(&["broker_accepted", "success"])
                .get(),
            1
        );
    }

    #[test]
    fn metrics_endpoint_config_normalizes_path() {
        let config = MetricsEndpointConfig::new("127.0.0.1", 9090).with_path("custom_metrics");

        assert_eq!(config.path, "/custom_metrics");
        assert_eq!(
            config.socket_addr().expect("valid socket address"),
            "127.0.0.1:9090"
                .parse::<SocketAddr>()
                .expect("valid literal")
        );
    }

    #[test]
    fn encode_prometheus_text_exports_registered_metrics() {
        let metrics = MessageOrchestratorMetrics::new();
        metrics.observe_send_stage("mq_publish", "success", Duration::from_millis(3));

        let encoded = encode_prometheus_text().expect("metrics encode succeeds");

        assert!(encoded.contains("message_orchestrator_send_stage_duration_seconds"));
        assert!(encoded.contains("stage=\"mq_publish\""));
    }
}
