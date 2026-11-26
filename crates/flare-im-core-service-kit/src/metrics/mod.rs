//! # Prometheus 指标收集模块
//!
//! 为各个服务模块提供统一的 Prometheus 指标收集与导出能力。

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use flare_core_runtime::ServiceRuntime;
use flare_server_core::error::{FlareError, Result};
use once_cell::sync::Lazy;
use prometheus::{
    Encoder, Gauge, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry, TextEncoder,
};
use tokio::{net::TcpListener, sync::oneshot};

use crate::config::ServiceMetricsSection;

pub mod app_version;
pub mod connection_builds;
mod grpc;
mod http;
pub use app_version::{
    APP_VERSION_MAX_LEN, AppVersionLabels, MAX_APP_VERSION_LABELS, OTHER_APP_VERSION_LABEL,
    app_version_label, sanitized_app_version,
};
pub use connection_builds::{
    BuildDistribution, ConnectionBuildCounts, MAX_REPORTED_BUILDS, OTHER_BUILDS_LABEL,
    PlatformBuildDistribution,
};
pub use grpc::{GrpcMetricsLayer, GrpcMetricsService, TRANSPORT_ERROR_CODE, UNKNOWN_METHOD};
pub use http::{HttpMetricsLayer, HttpMetricsService, UNMATCHED_ROUTE};

/// 全局指标注册表
pub static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

/// 关闭租户标签时 `tenant_id` 标签的统一取值。
pub const AGGREGATED_TENANT_LABEL: &str = "_all";

static TENANT_LABELS: AtomicBool = AtomicBool::new(true);

/// 设置 `[metrics] tenant_labels`。配置加载时调用一次；关闭后按租户的数据改走用量流与
/// trace/log，指标只保留聚合序列，避免云上租户数把时序基数放大（设计 §13.2）。
pub fn set_tenant_labels(enabled: bool) {
    TENANT_LABELS.store(enabled, Ordering::Relaxed);
}

pub fn tenant_labels_enabled() -> bool {
    TENANT_LABELS.load(Ordering::Relaxed)
}

/// 指标 `tenant_id` 标签取值：开关打开时是真实租户，关闭时统一为 [`AGGREGATED_TENANT_LABEL`]。
/// 所有带 `tenant_id` 标签的写入都必须经过这里。
pub fn tenant_label(tenant_id: &str) -> &str {
    if tenant_labels_enabled() {
        tenant_id
    } else {
        AGGREGATED_TENANT_LABEL
    }
}

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

    pub fn socket_addr(&self) -> std::result::Result<SocketAddr, std::net::AddrParseError> {
        format!("{}:{}", self.address, self.port).parse()
    }

    /// 由 `[services.<name>.metrics]` 段解析。段缺失或 `enabled = false` 时关闭；
    /// 启用却没给端口、或地址非法，属于显式配置错误，拒绝启动。
    pub fn from_section(
        service_name: &str,
        section: Option<&ServiceMetricsSection>,
    ) -> Result<Self> {
        let Some(section) = section else {
            return Ok(Self::disabled());
        };
        if section.enabled == Some(false) {
            return Ok(Self::disabled());
        }
        let port = section.port.filter(|port| *port != 0).ok_or_else(|| {
            FlareError::system(format!(
                "[services.{service_name}.metrics] is enabled but has no port"
            ))
        })?;
        let address = section
            .address
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("0.0.0.0");
        let mut config = Self::new(address, port);
        if let Some(path) = section.path.as_deref() {
            config = config.with_path(path);
        }
        config.socket_addr().map_err(|error| {
            FlareError::system(format!(
                "[services.{service_name}.metrics] invalid address {address}:{port}: {error}"
            ))
        })?;
        Ok(config)
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

/// 收集 [`REGISTRY`] 与 prometheus 默认注册表的全部指标族，按名称排序。
///
/// 默认注册表以前没有任何出口，注册在那里的指标等于不存在：server-core 的
/// `mq_process_ack_total`/`mq_process_ack_duration_seconds` 就在其中，发布门禁面板与
/// nack/term、死信告警全靠它。默认注册表在 Linux 上还自带进程指标
/// （`process_cpu_seconds_total`、`process_resident_memory_bytes`、`process_open_fds` 等，
/// 依赖本 crate 打开的 `process` 特性），不在 [`REGISTRY`] 再注册一份，免得每次抓取读两遍 /proc。
///
/// 同名指标族只保留 [`REGISTRY`] 的那份：同一名字输出两次会让 Prometheus 整次抓取失败。
fn gather_all_registries() -> Vec<prometheus::proto::MetricFamily> {
    let mut families = REGISTRY.gather();
    let own: std::collections::HashSet<String> = families
        .iter()
        .map(|family| family.name().to_string())
        .collect();
    families.extend(
        prometheus::default_registry()
            .gather()
            .into_iter()
            .filter(|family| !own.contains(family.name())),
    );
    families.sort_by(|a, b| a.name().cmp(b.name()));
    families
}

/// 将全部注册表编码为 Prometheus text format（见 [`gather_all_registries`]）。
pub fn encode_prometheus_text() -> std::result::Result<String, prometheus::Error> {
    let encoder = TextEncoder::new();
    let metric_families = gather_all_registries();
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
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

/// 进程身份指标：每个挂了 `/metrics` 的进程至少导出这两条，
/// 抓取目标即使没有业务指标也能区分服务、版本与重启。
struct ServiceIdentityMetrics {
    info: IntGaugeVec,
    start_time_seconds: Gauge,
}

static SERVICE_IDENTITY: Lazy<ServiceIdentityMetrics> = Lazy::new(|| {
    let info = IntGaugeVec::new(
        Opts::new(
            "flare_service_info",
            "Flare service identity; value is always 1",
        ),
        &["service", "version"],
    )
    .expect("flare_service_info metric definition is static");
    let start_time_seconds = Gauge::new(
        "flare_service_start_time_seconds",
        "Unix time the service process started serving metrics",
    )
    .expect("flare_service_start_time_seconds metric definition is static");
    let _ = REGISTRY.register(Box::new(info.clone()));
    let _ = REGISTRY.register(Box::new(start_time_seconds.clone()));
    ServiceIdentityMetrics {
        info,
        start_time_seconds,
    }
});

fn record_service_identity(service_name: &str) {
    let identity = &*SERVICE_IDENTITY;
    identity
        .info
        .with_label_values(&[service_name, env!("CARGO_PKG_VERSION")])
        .set(1);
    if identity.start_time_seconds.get() == 0.0 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs_f64())
            .unwrap_or_default();
        identity.start_time_seconds.set(now);
    }
}

/// 给运行时挂上 `/metrics` 出口（关闭时原样返回）。
///
/// 共享运行时（`build_service_runtime_plan` / `build_background_service_runtime`）与
/// 自带 HTTP 装配的网关都经这里，保证监听、优雅停机与身份指标一致。
pub fn attach_metrics_endpoint(
    runtime: ServiceRuntime,
    service_name: &str,
    config: MetricsEndpointConfig,
) -> ServiceRuntime {
    if !config.enabled {
        return runtime;
    }
    record_service_identity(service_name);
    runtime.add_spawn_with_shutdown(format!("{service_name}-metrics"), move |shutdown_rx| {
        serve_prometheus_metrics(config, shutdown_rx)
    })
}

/// 同一个监听上再挂 `GET /readyz`（见 [`crate::health::Readiness`]）。
///
/// 收到停机信号后先把就绪状态转为排空（`/readyz` 回 503），等 `drained` 完成（进程关完连接）
/// 再停监听：排空期间负载均衡的探测拿到的是 503，而不是连接被拒。没有独立 HTTP 端口的进程
/// （如信令网关）借指标端口暴露就绪探针。
pub fn attach_metrics_endpoint_with_readiness<D>(
    runtime: ServiceRuntime,
    service_name: &str,
    config: MetricsEndpointConfig,
    readiness: std::sync::Arc<crate::health::Readiness>,
    drained: D,
) -> ServiceRuntime
where
    D: std::future::Future<Output = ()> + Send + 'static,
{
    if !config.enabled {
        return runtime;
    }
    record_service_identity(service_name);
    runtime.add_spawn_with_shutdown(format!("{service_name}-metrics"), move |shutdown_rx| {
        serve_metrics_and_readiness(config, readiness, shutdown_rx, drained)
    })
}

async fn serve_metrics_and_readiness<D>(
    config: MetricsEndpointConfig,
    readiness: std::sync::Arc<crate::health::Readiness>,
    shutdown_rx: oneshot::Receiver<()>,
    drained: D,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    D: std::future::Future<Output = ()> + Send + 'static,
{
    let address = config.socket_addr()?;
    let path = normalize_metrics_path(config.path);
    let app = Router::new()
        .route(&path, get(metrics_handler))
        .merge(crate::health::readyz_router(readiness.clone()));
    let listener = TcpListener::bind(address).await?;

    tracing::info!(
        %address,
        path = %path,
        "Prometheus metrics and /readyz endpoint listening"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx.await;
            readiness.mark_draining();
            drained.await;
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

    /// 记录一次批量落库的批大小。
    pub fn observe_batch_size(&self, size: usize) {
        self.batch_size.observe(size as f64);
    }

    /// 记录一次 Redis 更新耗时（秒）。
    pub fn observe_redis_update(&self, seconds: f64) {
        if seconds >= 0.0 {
            self.redis_update_duration_seconds.observe(seconds);
        }
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

/// Push Proxy 指标。
///
/// 到达回执按通道、应用版本与来路计数。应用版本按 [`app_version_label`] 记：至多 32 个
/// `[0-9A-Za-z._+-]` 字符，每个进程最多 64 个不同取值，其余记为 `other`。到达率是它除以
/// Push Worker 的 `flare_push_vendor_sent_total{receipt_capable="true"}`，两边用同一份规则。
pub struct PushProxyMetrics {
    /// 收到的到达回执，按通道（`apns`、`fcm`、`getui`）、应用版本与来路（`extension`、`app`）。
    pub receipts_total: IntCounterVec,
}

impl PushProxyMetrics {
    pub fn new() -> Self {
        let receipts_total = IntCounterVec::new(
            Opts::new(
                "flare_push_receipts_total",
                "Push arrival receipts reported by devices, by vendor, app version and path",
            ),
            &["vendor", "app_version", "path"],
        )
        .expect("Failed to create flare_push_receipts_total metric");

        let _ = REGISTRY.register(Box::new(receipts_total.clone()));

        Self { receipts_total }
    }

    /// `app_version` 是客户端报的原值，这里收成有上限的标签。
    pub fn record_receipt(&self, vendor: &str, app_version: &str, path: &str) {
        self.receipts_total
            .with_label_values(&[vendor, app_version_label(app_version), path])
            .inc();
    }
}

impl Default for PushProxyMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Push Server 路由指标。
///
/// 标签都是固定的小集合（处置、交接原因），不按租户、用户、设备或会话打标签。
pub struct PushServerMetrics {
    /// 发出的离线任务数，按处置（`loud`、`quiet` 等）。一台设备一条任务。
    pub offline_tasks_total: IntCounterVec,
    /// 消费的设备交接数，按原因（`parked`、`ack_timeout`、`link_lost`、`gateway_lost`）。
    /// 被 broker 重投的交接每处理一遍计一次。
    pub device_handoffs_total: IntCounterVec,
    /// 设备交接补发的离线任务数。它们同时计入 `offline_tasks_total`。
    pub catch_up_alerts_total: IntCounter,
    /// 读设备推送绑定失败的次数。失败时这条记录不发离线任务，在线任务不受影响。
    pub binding_lookup_failures_total: IntCounter,
    /// 提示决策的结果，每台候选设备一次，按决策（`drop`、`quiet`、`candidate`、`priority`、
    /// `delay`、`badge_only`）。
    pub notify_decisions_total: IntCounterVec,
    /// 闸门给出的处置，按结果（`loud`、`quiet`、`defer`、`count_only`、`digest`、`tail`、
    /// `due_defer`、`due_nothing`、`fail_open`）。
    pub notify_gate_outcomes_total: IntCounterVec,
    /// 闸门状态比较后写入的冲突次数。冲突后重读重算，重试用完才放行。
    pub notify_gate_cas_conflicts_total: IntCounter,
    /// 闸门放行（按会话第一条处理）的次数，按原因（`conflict`：冲突重试用完，`store`：状态存储
    /// 与进程内兜底都不可用）。
    pub notify_gate_fail_open_total: IntCounterVec,
    /// 推送 Redis 不可用、改用进程内闸门的次数。
    pub notify_gate_fallbacks_total: IntCounter,
    /// 到期队列里领取的条目，按种类（`tail`、`delay`）与结果（`emitted`、`cancelled`、
    /// `rescheduled`、`nothing`）。
    pub notify_due_entries_total: IntCounterVec,
    /// 对已发出通知的修正，按种类（`clear`、`recall_replace`、`badge`）。
    pub notify_corrections_total: IntCounterVec,
    /// 群会话的波次，按事件（`immediate`：空闲会话的消息当场枚举，`started`：间隔里开始累加，
    /// `held`：累加进间隔的消息，`fired`：间隔结束枚举一次，`fail_open`：状态用不上、当场枚举）。
    pub notify_waves_total: IntCounterVec,
    /// 波次枚举出的接收者人数，按范围（`members`、`subscribers`、`everyone`）。
    pub wave_recipients_enumerated_total: IntCounterVec,
    /// 被点名、不等波次直接推送的成员人数。
    pub wave_direct_mentions_total: IntCounter,
    /// @所有人 的处理，按结果（`members`：成员不超过上限、人人按点名提示，`wave`：超过上限的全员
    /// 波次，`throttled`：超出配额、按普通消息处理）。
    pub all_mentions_total: IntCounterVec,
    /// 发出的离线任务按优先级通道（`direct`、`group`、`bulk`）。它们同时计入 `offline_tasks_total`。
    pub offline_lane_tasks_total: IntCounterVec,
}

impl PushServerMetrics {
    pub fn new() -> Self {
        let offline_tasks_total = IntCounterVec::new(
            Opts::new(
                "push_server_offline_tasks_total",
                "Offline push tasks emitted by the push router, one per target device",
            ),
            &["disposition"],
        )
        .expect("Failed to create push_server_offline_tasks_total metric");
        let device_handoffs_total = IntCounterVec::new(
            Opts::new(
                "push_server_device_handoffs_total",
                "Device handoffs consumed by the push router",
            ),
            &["cause"],
        )
        .expect("Failed to create push_server_device_handoffs_total metric");
        let catch_up_alerts_total = IntCounter::new(
            "push_server_catch_up_alerts_total",
            "Offline push tasks emitted to catch up a handed-off device",
        )
        .expect("Failed to create push_server_catch_up_alerts_total metric");
        let binding_lookup_failures_total = IntCounter::new(
            "push_server_binding_lookup_failures_total",
            "Failed reads of the device push binding registry",
        )
        .expect("Failed to create push_server_binding_lookup_failures_total metric");
        let notify_decisions_total = IntCounterVec::new(
            Opts::new(
                "push_server_notify_decisions_total",
                "Alert decisions made for candidate devices, by decision",
            ),
            &["decision"],
        )
        .expect("Failed to create push_server_notify_decisions_total metric");
        let notify_gate_outcomes_total = IntCounterVec::new(
            Opts::new(
                "push_server_notify_gate_outcomes_total",
                "Anti-disturbance gate outcomes, by outcome",
            ),
            &["outcome"],
        )
        .expect("Failed to create push_server_notify_gate_outcomes_total metric");
        let notify_gate_cas_conflicts_total = IntCounter::new(
            "push_server_notify_gate_cas_conflicts_total",
            "Compare-and-set conflicts while writing gate state",
        )
        .expect("Failed to create push_server_notify_gate_cas_conflicts_total metric");
        let notify_gate_fail_open_total = IntCounterVec::new(
            Opts::new(
                "push_server_notify_gate_fail_open_total",
                "Messages let through as a conversation's first because gate state could not be used",
            ),
            &["reason"],
        )
        .expect("Failed to create push_server_notify_gate_fail_open_total metric");
        let notify_gate_fallbacks_total = IntCounter::new(
            "push_server_notify_gate_fallbacks_total",
            "Gate operations served by the in-process gate because push Redis was unavailable",
        )
        .expect("Failed to create push_server_notify_gate_fallbacks_total metric");
        let notify_due_entries_total = IntCounterVec::new(
            Opts::new(
                "push_server_notify_due_entries_total",
                "Due queue entries claimed by the push server, by kind and result",
            ),
            &["kind", "result"],
        )
        .expect("Failed to create push_server_notify_due_entries_total metric");
        let notify_corrections_total = IntCounterVec::new(
            Opts::new(
                "push_server_notify_corrections_total",
                "Corrections of notifications already sent, by kind",
            ),
            &["kind"],
        )
        .expect("Failed to create push_server_notify_corrections_total metric");
        let notify_waves_total = IntCounterVec::new(
            Opts::new(
                "push_server_notify_waves_total",
                "Conversation wave events of group offline push, by event",
            ),
            &["event"],
        )
        .expect("Failed to create push_server_notify_waves_total metric");
        let wave_recipients_enumerated_total = IntCounterVec::new(
            Opts::new(
                "push_server_wave_recipients_enumerated_total",
                "Recipients enumerated by conversation waves, by audience",
            ),
            &["audience"],
        )
        .expect("Failed to create push_server_wave_recipients_enumerated_total metric");
        let wave_direct_mentions_total = IntCounter::new(
            "push_server_wave_direct_mentions_total",
            "Mentioned members pushed directly instead of waiting for the conversation wave",
        )
        .expect("Failed to create push_server_wave_direct_mentions_total metric");
        let all_mentions_total = IntCounterVec::new(
            Opts::new(
                "push_server_all_mentions_total",
                "Messages mentioning everyone in a group, by how they were pushed",
            ),
            &["result"],
        )
        .expect("Failed to create push_server_all_mentions_total metric");
        let offline_lane_tasks_total = IntCounterVec::new(
            Opts::new(
                "push_server_offline_lane_tasks_total",
                "Offline push tasks emitted by the push router, by priority lane",
            ),
            &["lane"],
        )
        .expect("Failed to create push_server_offline_lane_tasks_total metric");

        let _ = REGISTRY.register(Box::new(offline_tasks_total.clone()));
        let _ = REGISTRY.register(Box::new(device_handoffs_total.clone()));
        let _ = REGISTRY.register(Box::new(catch_up_alerts_total.clone()));
        let _ = REGISTRY.register(Box::new(binding_lookup_failures_total.clone()));
        let _ = REGISTRY.register(Box::new(notify_decisions_total.clone()));
        let _ = REGISTRY.register(Box::new(notify_gate_outcomes_total.clone()));
        let _ = REGISTRY.register(Box::new(notify_gate_cas_conflicts_total.clone()));
        let _ = REGISTRY.register(Box::new(notify_gate_fail_open_total.clone()));
        let _ = REGISTRY.register(Box::new(notify_gate_fallbacks_total.clone()));
        let _ = REGISTRY.register(Box::new(notify_due_entries_total.clone()));
        let _ = REGISTRY.register(Box::new(notify_corrections_total.clone()));
        let _ = REGISTRY.register(Box::new(notify_waves_total.clone()));
        let _ = REGISTRY.register(Box::new(wave_recipients_enumerated_total.clone()));
        let _ = REGISTRY.register(Box::new(wave_direct_mentions_total.clone()));
        let _ = REGISTRY.register(Box::new(all_mentions_total.clone()));
        let _ = REGISTRY.register(Box::new(offline_lane_tasks_total.clone()));

        Self {
            offline_tasks_total,
            device_handoffs_total,
            catch_up_alerts_total,
            binding_lookup_failures_total,
            notify_decisions_total,
            notify_gate_outcomes_total,
            notify_gate_cas_conflicts_total,
            notify_gate_fail_open_total,
            notify_gate_fallbacks_total,
            notify_due_entries_total,
            notify_corrections_total,
            notify_waves_total,
            wave_recipients_enumerated_total,
            wave_direct_mentions_total,
            all_mentions_total,
            offline_lane_tasks_total,
        }
    }

    pub fn record_wave(&self, event: &str) {
        self.notify_waves_total.with_label_values(&[event]).inc();
    }

    pub fn record_wave_recipients(&self, audience: &str, count: u64) {
        self.wave_recipients_enumerated_total
            .with_label_values(&[audience])
            .inc_by(count);
    }

    pub fn record_wave_direct_mentions(&self, count: u64) {
        self.wave_direct_mentions_total.inc_by(count);
    }

    pub fn record_all_mention(&self, result: &str) {
        self.all_mentions_total.with_label_values(&[result]).inc();
    }

    pub fn record_offline_lane(&self, lane: &str) {
        self.offline_lane_tasks_total
            .with_label_values(&[lane])
            .inc();
    }

    pub fn record_offline_task(&self, disposition: &str) {
        self.offline_tasks_total
            .with_label_values(&[disposition])
            .inc();
    }

    pub fn record_device_handoff(&self, cause: &str) {
        self.device_handoffs_total.with_label_values(&[cause]).inc();
    }

    pub fn record_catch_up_alert(&self) {
        self.catch_up_alerts_total.inc();
    }

    pub fn record_binding_lookup_failure(&self) {
        self.binding_lookup_failures_total.inc();
    }

    pub fn record_notify_decision(&self, decision: &str) {
        self.notify_decisions_total
            .with_label_values(&[decision])
            .inc();
    }

    pub fn record_gate_outcome(&self, outcome: &str) {
        self.notify_gate_outcomes_total
            .with_label_values(&[outcome])
            .inc();
    }

    pub fn record_gate_cas_conflict(&self) {
        self.notify_gate_cas_conflicts_total.inc();
    }

    pub fn record_gate_fail_open(&self, reason: &str) {
        self.notify_gate_fail_open_total
            .with_label_values(&[reason])
            .inc();
    }

    pub fn record_gate_fallback(&self) {
        self.notify_gate_fallbacks_total.inc();
    }

    pub fn record_due_entry(&self, kind: &str, result: &str) {
        self.notify_due_entries_total
            .with_label_values(&[kind, result])
            .inc();
    }

    pub fn record_correction(&self, kind: &str) {
        self.notify_corrections_total
            .with_label_values(&[kind])
            .inc();
    }
}

impl Default for PushServerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// 扇出耗时的收件人规模分档。独立成函数是为了能直接测，
/// 不必构造整个 metrics 结构——`new()` 会往全局 REGISTRY 注册，
/// 而只有进程内第一个实例注册得上，在测试里构造实例会把注册"抢"走、
/// 打破依赖全局导出的其它测试。
pub fn fanout_size_bucket(recipient_count: usize) -> &'static str {
    match recipient_count {
        0..=1 => "1",
        2..=10 => "10",
        11..=100 => "100",
        101..=500 => "500",
        _ => "500+",
    }
}

/// 由摄入时刻算出扇出耗时（秒）；时间戳非法或落在未来时返回 `None`。
///
/// 客户端时钟可能偏几十秒（实测有慢 34 秒的），这种值一旦进直方图
/// 就会把分位数带偏，而这个指标的全部意义就是给服务端耗时定责。
pub fn fanout_elapsed_secs(now_ms: i64, ingestion_ts_ms: i64) -> Option<f64> {
    if ingestion_ts_ms <= 0 || now_ms <= 0 {
        return None;
    }
    let elapsed = now_ms - ingestion_ts_ms;
    (elapsed >= 0).then(|| elapsed as f64 / 1000.0)
}

/// Signaling Online 网关清扫器指标。
///
/// `gateway_id` 标签是真实网关 ID（重启遗留的孤儿集合也记在原网关名下），基数等于网关节点数。
pub struct OnlineSweeperMetrics {
    /// 因所在网关租约缺失而被清掉的连接数。
    pub removed_total: IntCounterVec,
}

impl OnlineSweeperMetrics {
    pub fn new() -> Self {
        let removed_total = IntCounterVec::new(
            Opts::new(
                "flare_online_sweeper_removed_total",
                "Online connections removed because their access gateway lost its lease",
            ),
            &["gateway_id"],
        )
        .expect("Failed to create flare_online_sweeper_removed_total metric");
        let _ = REGISTRY.register(Box::new(removed_total.clone()));
        Self { removed_total }
    }

    pub fn record_removed(&self, gateway_id: &str, count: u64) {
        if count > 0 {
            self.removed_total
                .with_label_values(&[gateway_id])
                .inc_by(count);
        }
    }
}

impl Default for OnlineSweeperMetrics {
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
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let Some(secs) = fanout_elapsed_secs(now_ms, ingestion_ts_ms) else {
            return;
        };
        self.fanout_latency_seconds
            .with_label_values(&[mode, fanout_size_bucket(recipient_count)])
            .observe(secs);
    }
}

#[cfg(test)]
mod fanout_latency_tests {
    use super::{fanout_elapsed_secs, fanout_size_bucket};

    /// 荒谬的摄入时刻必须被丢弃，不能污染分位数。
    ///
    /// 这条防线是有来由的：`messages.created_at` 曾原样存**客户端**时钟，
    /// 实测有客户端慢 34 秒。这种值进了直方图，p50/p90 就全废了。
    #[test]
    fn bogus_ingestion_timestamps_yield_no_sample() {
        assert_eq!(fanout_elapsed_secs(1_000_000, 0), None, "取不到时丢弃");
        assert_eq!(fanout_elapsed_secs(1_000_000, -1), None, "非法值丢弃");
        assert_eq!(
            fanout_elapsed_secs(1_000_000, 1_060_000),
            None,
            "未来时刻丢弃"
        );
        assert_eq!(fanout_elapsed_secs(0, 1_000), None, "拿不到当前时间时丢弃");
    }

    #[test]
    fn normal_elapsed_is_converted_to_seconds() {
        assert_eq!(fanout_elapsed_secs(1_000_250, 1_000_000), Some(0.25));
        assert_eq!(fanout_elapsed_secs(1_000_000, 1_000_000), Some(0.0));
    }

    /// 标签必须低基数：按规模分档，不按具体人数打标签。
    #[test]
    fn recipient_count_is_bucketed_not_labeled_verbatim() {
        assert_eq!(fanout_size_bucket(0), "1");
        assert_eq!(fanout_size_bucket(1), "1");
        assert_eq!(fanout_size_bucket(7), "10");
        assert_eq!(fanout_size_bucket(99), "100");
        assert_eq!(fanout_size_bucket(300), "500");
        assert_eq!(fanout_size_bucket(9999), "500+");
    }
}

impl Default for MessageOrchestratorMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Message Ingest 首触 seq floor 自愈被 Storage Reader 以「元数据与签名冲突」拒绝的次数：floor 查询
/// 按会话租户派生子上下文、原样转发入站签名，签名租户与会话租户不同就会这样，存储恢复也不会好。
/// 不带标签，租户与会话只进 WARN 日志。
static INGEST_SEQ_FLOOR_TENANT_CONFLICTS: Lazy<IntCounter> = Lazy::new(|| {
    let counter = IntCounter::new(
        "message_ingest_seq_floor_tenant_conflict_total",
        "Seq floor self-heal lookups refused because the conversation tenant differs from the signed tenant",
    )
    .expect("message_ingest_seq_floor_tenant_conflict_total metric definition is static");
    let _ = REGISTRY.register(Box::new(counter.clone()));
    counter
});

/// 记一次 seq floor 自愈的租户冲突（指标 `message_ingest_seq_floor_tenant_conflict_total`）。
pub fn record_ingest_seq_floor_tenant_conflict() {
    INGEST_SEQ_FLOOR_TENANT_CONFLICTS.inc();
}

/// 本进程累计的 seq floor 自愈租户冲突次数；首次调用即注册指标，摄入服务启动时调一次，
/// 没有冲突时 `/metrics` 里也有这条 0。
pub fn ingest_seq_floor_tenant_conflicts() -> u64 {
    INGEST_SEQ_FLOOR_TENANT_CONFLICTS.get()
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
    /// 通过认证的 CONNECT，按客户端上报的接入清单 serial 与服务端已知值的关系分桶
    pub connect_access_serial_total: IntCounterVec,
    /// 被拒的 CONNECT，按原因分桶（含版本门禁）
    pub handshake_rejected_total: IntCounterVec,
    /// 在线连接数，按（租户，平台，构建）分桶；每个（租户，平台）只列最新的
    /// [`MAX_REPORTED_BUILDS`] 个构建，其余记在 `app_build="other"` 下
    pub connections_by_build: IntGaugeVec,
    /// 已在线、之后被抬高的下限（最低构建号或封禁区间）判为必须升级而断开的连接，按原因分桶
    /// （取值同 `handshake_rejected_total` 的版本门禁原因）
    pub connections_evicted_total: IntCounterVec,
    /// 上报给推送通道的设备交接，按原因分桶（`parked`、`ack_timeout`、`link_lost`）
    pub device_handoffs_total: IntCounterVec,
    /// 没能交到推送侧的设备交接，按原因分桶（`queue_full`、`retries_exhausted`、`refused`）
    pub device_handoffs_dropped_total: IntCounterVec,
    /// 送达确认超时次数：可提示的下行帧在期限内没有等到客户端确认
    pub delivery_ack_timeouts_total: IntCounter,
    /// 送达确认超时后探活失败（没有 PONG 也没有确认）而被关闭的连接数
    pub connections_probe_closed_total: IntCounter,
    /// 处于「立即交接」状态的连接数：帧照常写出，可提示的内容同时交给推送通道
    pub connections_immediate_handoff: IntGauge,
    builds: Mutex<BuildState>,
}

/// 构建分布的计数与已导出的序列。
#[derive(Default)]
struct BuildState {
    /// 按真实租户计，供查询接口读
    by_tenant: ConnectionBuildCounts,
    /// 关闭租户标签时按聚合标签计，供导出
    by_label: ConnectionBuildCounts,
    /// 每个（租户标签，平台）上次导出的 `app_build` 标签值，下次导出时删掉不再列出的
    exported: HashMap<(String, String), Vec<String>>,
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

        let connect_access_serial_total = IntCounterVec::new(
            Opts::new(
                "connect_access_serial_total",
                "Authenticated CONNECTs by the client access serial against the projected one",
            ),
            &["tenant_id", "state"],
        )
        .expect("Failed to create connect_access_serial_total metric");

        let _ = REGISTRY.register(Box::new(connections_active.clone()));
        let _ = REGISTRY.register(Box::new(messages_pushed_total.clone()));
        let _ = REGISTRY.register(Box::new(push_success_total.clone()));
        let _ = REGISTRY.register(Box::new(push_failure_total.clone()));
        let _ = REGISTRY.register(Box::new(connection_disconnected_total.clone()));
        let _ = REGISTRY.register(Box::new(client_ack_received_total.clone()));
        let _ = REGISTRY.register(Box::new(push_latency_seconds.clone()));
        let _ = REGISTRY.register(Box::new(online_cache_hit_total.clone()));
        let _ = REGISTRY.register(Box::new(online_cache_miss_total.clone()));
        let handshake_rejected_total = IntCounterVec::new(
            Opts::new(
                "handshake_rejected_total",
                "Rejected CONNECTs by reason, including the client version gate",
            ),
            &["reason"],
        )
        .expect("Failed to create handshake_rejected_total metric");

        let connections_by_build = IntGaugeVec::new(
            Opts::new(
                "connections_by_build",
                "Online connections by tenant, release platform and app build (newest builds only)",
            ),
            &["tenant_id", "platform", "app_build"],
        )
        .expect("Failed to create connections_by_build metric");

        let connections_evicted_total = IntCounterVec::new(
            Opts::new(
                "connections_evicted_total",
                "Online connections closed because a raised client build floor now requires an upgrade, by reason",
            ),
            &["reason"],
        )
        .expect("Failed to create connections_evicted_total metric");

        let device_handoffs_total = IntCounterVec::new(
            Opts::new(
                "device_handoffs_total",
                "Device handoffs reported to the push channel, by cause",
            ),
            &["cause"],
        )
        .expect("Failed to create device_handoffs_total metric");

        let device_handoffs_dropped_total = IntCounterVec::new(
            Opts::new(
                "device_handoffs_dropped_total",
                "Device handoffs that never reached the push channel, by reason",
            ),
            &["reason"],
        )
        .expect("Failed to create device_handoffs_dropped_total metric");

        let delivery_ack_timeouts_total = IntCounter::new(
            "delivery_ack_timeouts_total",
            "Connections whose alert frames were not confirmed within the delivery ack deadline",
        )
        .expect("Failed to create delivery_ack_timeouts_total metric");

        let connections_probe_closed_total = IntCounter::new(
            "connections_probe_closed_total",
            "Connections closed because the probe after a delivery ack timeout got no answer",
        )
        .expect("Failed to create connections_probe_closed_total metric");

        let connections_immediate_handoff = IntGauge::new(
            "connections_immediate_handoff",
            "Connections whose alert frames are handed to the push channel as they are written",
        )
        .expect("Failed to create connections_immediate_handoff metric");

        let _ = REGISTRY.register(Box::new(connect_access_serial_total.clone()));
        let _ = REGISTRY.register(Box::new(handshake_rejected_total.clone()));
        let _ = REGISTRY.register(Box::new(connections_by_build.clone()));
        let _ = REGISTRY.register(Box::new(connections_evicted_total.clone()));
        let _ = REGISTRY.register(Box::new(device_handoffs_total.clone()));
        let _ = REGISTRY.register(Box::new(device_handoffs_dropped_total.clone()));
        let _ = REGISTRY.register(Box::new(delivery_ack_timeouts_total.clone()));
        let _ = REGISTRY.register(Box::new(connections_probe_closed_total.clone()));
        let _ = REGISTRY.register(Box::new(connections_immediate_handoff.clone()));

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
            connect_access_serial_total,
            handshake_rejected_total,
            connections_by_build,
            connections_evicted_total,
            device_handoffs_total,
            device_handoffs_dropped_total,
            delivery_ack_timeouts_total,
            connections_probe_closed_total,
            connections_immediate_handoff,
            builds: Mutex::new(BuildState::default()),
        }
    }

    /// 记录一次被拒的 CONNECT。`reason` 必须取自固定集合（如 `token_invalid`、`below_min_build`），
    /// 不能带客户端自报的内容。
    pub fn record_handshake_rejected(&self, reason: &str) {
        self.handshake_rejected_total
            .with_label_values(&[reason])
            .inc();
    }

    /// 记录一条因下限抬高被要求升级并断开的在线连接。`reason` 取版本门禁的固定集合
    /// （`below_min_build`、`blocked_build`），与 CONNECT 被拒时同一套取值。
    pub fn record_connection_evicted(&self, reason: &str) {
        self.connections_evicted_total
            .with_label_values(&[reason])
            .inc();
    }

    /// 记录一次上报给推送通道的设备交接。`cause` 取固定集合：`parked`（客户端托管）、
    /// `ack_timeout`（送达确认超时，或连接处于立即交接状态）、`link_lost`（断开时仍有未确认的帧）。
    pub fn record_device_handoff(&self, cause: &str) {
        self.device_handoffs_total.with_label_values(&[cause]).inc();
    }

    /// 记录一次没能交到推送侧的设备交接。`reason` 取固定集合：`queue_full`（上报队列满）、
    /// `retries_exhausted`（重试后仍失败）、`refused`（对端明确拒绝，或本端没有签名私钥）。
    pub fn record_device_handoff_dropped(&self, reason: &str) {
        self.device_handoffs_dropped_total
            .with_label_values(&[reason])
            .inc();
    }

    /// 记录一次送达确认超时。
    pub fn record_delivery_ack_timeout(&self) {
        self.delivery_ack_timeouts_total.inc();
    }

    /// 记录一条探活失败被关闭的连接。
    pub fn record_connection_probe_closed(&self) {
        self.connections_probe_closed_total.inc();
    }

    /// 一条连接进入立即交接状态；与 [`Self::immediate_handoff_left`] 成对。
    pub fn immediate_handoff_entered(&self) {
        self.connections_immediate_handoff.inc();
    }

    /// 一条连接离开立即交接状态（回了送达确认、设备回到前台，或连接结束）。
    pub fn immediate_handoff_left(&self) {
        self.connections_immediate_handoff.dec();
    }

    /// 一条连接上线：计入（租户，平台，构建）。`platform` 是发布清单平台段，`build` 没报时为 0。
    pub fn connection_build_opened(&self, tenant_id: &str, platform: &str, build: u64) {
        self.update_builds(tenant_id, platform, build, true);
    }

    /// 一条连接下线：与 [`Self::connection_build_opened`] 成对，参数相同。
    pub fn connection_build_closed(&self, tenant_id: &str, platform: &str, build: u64) {
        self.update_builds(tenant_id, platform, build, false);
    }

    /// 本网关上一个租户各平台的在线构建分布（只含本实例的连接）。
    pub fn connection_build_distribution(&self, tenant_id: &str) -> Vec<PlatformBuildDistribution> {
        self.builds
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .by_tenant
            .tenant(tenant_id)
    }

    /// 本网关全部租户的在线构建分布，供周期上报 Redis（见 `gateway::connection_builds`）。
    pub fn connection_build_snapshot(&self) -> Vec<(String, Vec<PlatformBuildDistribution>)> {
        self.builds
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .by_tenant
            .snapshot()
    }

    fn update_builds(&self, tenant_id: &str, platform: &str, build: u64, opened: bool) {
        let mut guard = self.builds.lock().unwrap_or_else(PoisonError::into_inner);
        let state = &mut *guard;
        let apply = |counts: &mut ConnectionBuildCounts, tenant: &str| {
            if opened {
                counts.opened(tenant, platform, build)
            } else {
                counts.closed(tenant, platform, build)
            }
        };
        let by_tenant = apply(&mut state.by_tenant, tenant_id);
        let label = tenant_label(tenant_id);
        let exported = if label == tenant_id {
            by_tenant
        } else {
            apply(&mut state.by_label, label)
        };
        export_builds(
            &self.connections_by_build,
            &mut state.exported,
            label,
            platform,
            &exported,
        );
    }

    /// 记录一次通过认证的 CONNECT 上报的接入清单 serial：`current` 与投影一致，`behind` 落后，
    /// `ahead` 领先（投影或缓存还没跟上），`unreported` 没带。
    /// 采用率 = `current` / (`current` + `behind` + `ahead`)。
    pub fn record_connect_access_serial(
        &self,
        tenant_id: &str,
        reported: Option<u64>,
        projected: u64,
    ) {
        let state = match reported {
            None => "unreported",
            Some(serial) if serial == projected => "current",
            Some(serial) if serial < projected => "behind",
            Some(_) => "ahead",
        };
        self.connect_access_serial_total
            .with_label_values(&[tenant_label(tenant_id), state])
            .inc();
    }

    /// 记录一次向某用户的推送投递结果。
    ///
    /// 这几个指标以前**只有声明、没有任何写入路径**：注册进了 Prometheus，
    /// 于是 /metrics 里永远是 0。运维在故障时看到 push_success_total 0
    /// 会直接断定「推送全挂」，而实际推送好好的——比没有这个指标更糟。
    pub fn record_push_result(&self, tenant_id: &str, ok: i32, fail: i32) {
        let tenant = tenant_label(tenant_id);
        if ok > 0 {
            self.push_success_total
                .with_label_values(&[tenant])
                .inc_by(ok as u64);
            self.messages_pushed_total
                .with_label_values(&[tenant])
                .inc_by(ok as u64);
        }
        if fail > 0 {
            self.push_failure_total
                .with_label_values(&["delivery_failed", tenant])
                .inc_by(fail as u64);
        }
    }

    /// 记录一次推送投递耗时（秒）。
    pub fn observe_push_latency(&self, tenant_id: &str, seconds: f64) {
        if seconds < 0.0 {
            return;
        }
        self.push_latency_seconds
            .with_label_values(&[tenant_label(tenant_id)])
            .observe(seconds);
    }

    /// 记录在线状态缓存命中/未命中。
    pub fn record_online_cache(&self, hit: bool) {
        if hit {
            self.online_cache_hit_total.inc();
        } else {
            self.online_cache_miss_total.inc();
        }
    }

    /// 记录收到的客户端 ACK。
    pub fn record_client_ack(&self, tenant_id: &str) {
        self.client_ack_received_total
            .with_label_values(&[tenant_label(tenant_id)])
            .inc();
    }
}

/// 把一个（租户标签，平台）的分布写进 `connections_by_build`，并删掉上次导出、这次不再列出的序列。
fn export_builds(
    gauge: &IntGaugeVec,
    exported: &mut HashMap<(String, String), Vec<String>>,
    tenant: &str,
    platform: &str,
    distribution: &BuildDistribution,
) {
    let mut labels: Vec<String> = distribution
        .builds
        .iter()
        .map(|(build, _)| build.to_string())
        .collect();
    if distribution.other > 0 {
        labels.push(OTHER_BUILDS_LABEL.to_string());
    }
    let key = (tenant.to_string(), platform.to_string());
    if let Some(previous) = exported.get(&key) {
        for stale in previous.iter().filter(|label| !labels.contains(label)) {
            let _ = gauge.remove_label_values(&[tenant, platform, stale.as_str()]);
        }
    }
    for (build, connections) in &distribution.builds {
        gauge
            .with_label_values(&[tenant, platform, build.to_string().as_str()])
            .set(i64::try_from(*connections).unwrap_or(i64::MAX));
    }
    if distribution.other > 0 {
        gauge
            .with_label_values(&[tenant, platform, OTHER_BUILDS_LABEL])
            .set(i64::try_from(distribution.other).unwrap_or(i64::MAX));
    }
    if labels.is_empty() {
        exported.remove(&key);
    } else {
        exported.insert(key, labels);
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
    fn push_server_metrics_count_tasks_handoffs_and_registry_failures() {
        let metrics = PushServerMetrics::new();

        metrics.record_offline_task("loud");
        metrics.record_offline_task("loud");
        metrics.record_offline_task("quiet");
        metrics.record_device_handoff("ack_timeout");
        metrics.record_catch_up_alert();
        metrics.record_binding_lookup_failure();

        let tasks = |disposition: &str| {
            metrics
                .offline_tasks_total
                .with_label_values(&[disposition])
                .get()
        };
        assert_eq!(tasks("loud"), 2);
        assert_eq!(tasks("quiet"), 1);
        assert_eq!(
            metrics
                .device_handoffs_total
                .with_label_values(&["ack_timeout"])
                .get(),
            1
        );
        assert_eq!(metrics.catch_up_alerts_total.get(), 1);
        assert_eq!(metrics.binding_lookup_failures_total.get(), 1);
    }

    #[test]
    fn push_server_metrics_count_gate_decisions_and_corrections() {
        let metrics = PushServerMetrics::new();

        metrics.record_notify_decision("delay");
        metrics.record_gate_outcome("count_only");
        metrics.record_gate_outcome("count_only");
        metrics.record_gate_cas_conflict();
        metrics.record_gate_fail_open("conflict");
        metrics.record_gate_fallback();
        metrics.record_due_entry("tail", "emitted");
        metrics.record_correction("clear");

        assert_eq!(
            metrics
                .notify_decisions_total
                .with_label_values(&["delay"])
                .get(),
            1
        );
        assert_eq!(
            metrics
                .notify_gate_outcomes_total
                .with_label_values(&["count_only"])
                .get(),
            2
        );
        assert_eq!(metrics.notify_gate_cas_conflicts_total.get(), 1);
        assert_eq!(
            metrics
                .notify_gate_fail_open_total
                .with_label_values(&["conflict"])
                .get(),
            1
        );
        assert_eq!(metrics.notify_gate_fallbacks_total.get(), 1);
        assert_eq!(
            metrics
                .notify_due_entries_total
                .with_label_values(&["tail", "emitted"])
                .get(),
            1
        );
        assert_eq!(
            metrics
                .notify_corrections_total
                .with_label_values(&["clear"])
                .get(),
            1
        );
    }

    #[test]
    fn push_server_metrics_count_waves_mentions_and_lanes() {
        let metrics = PushServerMetrics::new();

        metrics.record_wave("immediate");
        metrics.record_wave("fired");
        metrics.record_wave_recipients("subscribers", 40);
        metrics.record_wave_recipients("subscribers", 2);
        metrics.record_wave_direct_mentions(3);
        metrics.record_all_mention("throttled");
        metrics.record_offline_lane("bulk");
        metrics.record_offline_lane("bulk");

        assert_eq!(
            metrics
                .notify_waves_total
                .with_label_values(&["fired"])
                .get(),
            1
        );
        assert_eq!(
            metrics
                .wave_recipients_enumerated_total
                .with_label_values(&["subscribers"])
                .get(),
            42
        );
        assert_eq!(metrics.wave_direct_mentions_total.get(), 3);
        assert_eq!(
            metrics
                .all_mentions_total
                .with_label_values(&["throttled"])
                .get(),
            1
        );
        assert_eq!(
            metrics
                .offline_lane_tasks_total
                .with_label_values(&["bulk"])
                .get(),
            2
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
    fn metrics_section_absent_or_disabled_means_no_endpoint() {
        assert!(
            !MetricsEndpointConfig::from_section("svc", None)
                .unwrap()
                .enabled
        );
        let disabled = ServiceMetricsSection {
            enabled: Some(false),
            port: Some(19190),
            ..ServiceMetricsSection::default()
        };
        assert!(
            !MetricsEndpointConfig::from_section("svc", Some(&disabled))
                .unwrap()
                .enabled
        );
    }

    #[test]
    fn metrics_section_resolves_address_port_and_path() {
        let section = ServiceMetricsSection {
            port: Some(19190),
            path: Some("prom".to_string()),
            ..ServiceMetricsSection::default()
        };
        let config = MetricsEndpointConfig::from_section("conversation", Some(&section)).unwrap();
        assert!(config.enabled);
        assert_eq!(config.address, "0.0.0.0");
        assert_eq!(config.port, 19190);
        assert_eq!(config.path, "/prom");
    }

    #[test]
    fn enabled_metrics_section_without_port_or_with_bad_address_fails() {
        let no_port = ServiceMetricsSection::default();
        let error = MetricsEndpointConfig::from_section("media", Some(&no_port))
            .expect_err("port is required");
        assert!(error.to_string().contains("services.media.metrics"));

        let bad_address = ServiceMetricsSection {
            address: Some("not an ip".to_string()),
            port: Some(19191),
            ..ServiceMetricsSection::default()
        };
        assert!(MetricsEndpointConfig::from_section("media", Some(&bad_address)).is_err());
    }

    #[test]
    fn exported_build_series_drop_builds_that_fall_out_of_the_list() {
        let gauge = IntGaugeVec::new(
            Opts::new("test_connections_by_build", "test"),
            &["tenant_id", "platform", "app_build"],
        )
        .unwrap();
        let mut exported = HashMap::new();
        let mut counts = ConnectionBuildCounts::default();
        for build in 1..=MAX_REPORTED_BUILDS as u64 {
            let snapshot = counts.opened("t1", "android", build);
            export_builds(&gauge, &mut exported, "t1", "android", &snapshot);
        }
        assert_eq!(gauge.with_label_values(&["t1", "android", "1"]).get(), 1);

        // 第 21 个构建上线：最旧的 1 从列表里掉出，计入 other
        let snapshot = counts.opened("t1", "android", 21);
        export_builds(&gauge, &mut exported, "t1", "android", &snapshot);
        assert_eq!(gauge.with_label_values(&["t1", "android", "21"]).get(), 1);
        assert_eq!(
            gauge
                .with_label_values(&["t1", "android", OTHER_BUILDS_LABEL])
                .get(),
            1
        );
        let families = prometheus::core::Collector::collect(&gauge);
        let series = families[0].get_metric();
        assert_eq!(series.len(), MAX_REPORTED_BUILDS + 1);
        assert!(series.iter().all(|metric| {
            metric
                .get_label()
                .iter()
                .all(|label| label.name() != "app_build" || label.value() != "1")
        }));

        // 全部下线：序列全部删除
        for build in 1..=21u64 {
            let snapshot = counts.closed("t1", "android", build);
            export_builds(&gauge, &mut exported, "t1", "android", &snapshot);
        }
        assert!(
            prometheus::core::Collector::collect(&gauge)[0]
                .get_metric()
                .is_empty()
        );
        assert!(exported.is_empty());
    }

    #[test]
    fn connection_builds_are_queryable_by_tenant() {
        let metrics = AccessGatewayMetrics::new();
        metrics.connection_build_opened("tenant-q", "ios", 300);
        metrics.connection_build_opened("tenant-q", "ios", 300);
        metrics.connection_build_opened("tenant-q", "tauri", 12);
        metrics.connection_build_closed("tenant-q", "ios", 300);
        let distribution = metrics.connection_build_distribution("tenant-q");
        assert_eq!(distribution.len(), 2);
        assert_eq!(distribution[0].platform, "ios");
        assert_eq!(distribution[0].distribution.builds, vec![(300, 1)]);
        assert_eq!(distribution[1].platform, "tauri");
        assert!(metrics.connection_build_distribution("other").is_empty());

        metrics.record_handshake_rejected("below_min_build");
        assert_eq!(
            metrics
                .handshake_rejected_total
                .with_label_values(&["below_min_build"])
                .get(),
            1
        );

        metrics.record_connection_evicted("blocked_build");
        metrics.record_connection_evicted("blocked_build");
        assert_eq!(
            metrics
                .connections_evicted_total
                .with_label_values(&["blocked_build"])
                .get(),
            2
        );
        assert_eq!(
            metrics
                .connections_evicted_total
                .with_label_values(&["below_min_build"])
                .get(),
            0
        );
    }

    #[test]
    fn device_handoff_metrics_count_by_cause_and_track_the_immediate_handoff_gauge() {
        let metrics = AccessGatewayMetrics::new();
        metrics.record_device_handoff("ack_timeout");
        metrics.record_device_handoff("ack_timeout");
        metrics.record_device_handoff("parked");
        metrics.record_device_handoff_dropped("queue_full");
        metrics.record_delivery_ack_timeout();
        metrics.record_connection_probe_closed();
        assert_eq!(
            metrics
                .device_handoffs_total
                .with_label_values(&["ack_timeout"])
                .get(),
            2
        );
        assert_eq!(
            metrics
                .device_handoffs_total
                .with_label_values(&["link_lost"])
                .get(),
            0
        );
        assert_eq!(
            metrics
                .device_handoffs_dropped_total
                .with_label_values(&["queue_full"])
                .get(),
            1
        );
        assert_eq!(metrics.delivery_ack_timeouts_total.get(), 1);
        assert_eq!(metrics.connections_probe_closed_total.get(), 1);

        metrics.immediate_handoff_entered();
        metrics.immediate_handoff_entered();
        metrics.immediate_handoff_left();
        assert_eq!(metrics.connections_immediate_handoff.get(), 1);
    }

    #[test]
    fn tenant_label_switch_aggregates_tenants() {
        set_tenant_labels(false);
        assert_eq!(tenant_label("tenant-a"), AGGREGATED_TENANT_LABEL);
        set_tenant_labels(true);
        assert_eq!(tenant_label("tenant-a"), "tenant-a");
    }

    #[tokio::test]
    async fn attached_endpoint_serves_identity_metrics_and_stops_on_shutdown() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind probe");
        let port = listener.local_addr().expect("probe addr").port();
        drop(listener);

        let config = MetricsEndpointConfig::new("127.0.0.1", port);
        record_service_identity("identity-probe");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(serve_prometheus_metrics(config, shutdown_rx));

        let mut body = String::new();
        for _ in 0..50 {
            if let Ok(mut stream) = tokio::net::TcpStream::connect(("127.0.0.1", port)).await {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                stream
                    .write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                    .await
                    .expect("write request");
                stream
                    .read_to_string(&mut body)
                    .await
                    .expect("read response");
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            body.starts_with("HTTP/1.1 200"),
            "unexpected response: {body}"
        );
        assert!(body.contains("flare_service_info{service=\"identity-probe\""));
        assert!(body.contains("flare_service_start_time_seconds"));

        shutdown_tx.send(()).expect("server still running");
        server
            .await
            .expect("server task joins")
            .expect("server exits cleanly");
    }

    #[test]
    fn encode_prometheus_text_exports_registered_metrics() {
        let metrics = MessageOrchestratorMetrics::new();
        metrics.observe_send_stage("mq_publish", "success", Duration::from_millis(3));

        let encoded = encode_prometheus_text().expect("metrics encode succeeds");

        assert!(encoded.contains("message_orchestrator_send_stage_duration_seconds"));
        assert!(encoded.contains("stage=\"mq_publish\""));
    }

    /// server-core 的 MQ ack 指标注册在默认注册表；/metrics 必须把它带出来，
    /// 否则发布门禁的 nack/term 与死信告警永远无数。
    #[test]
    fn encode_prometheus_text_exports_default_registry_metrics() {
        let probe = IntCounter::new(
            "service_kit_default_registry_probe_total",
            "probe registered in the prometheus default registry",
        )
        .expect("probe metric definition is static");
        prometheus::default_registry()
            .register(Box::new(probe.clone()))
            .expect("probe registers once");
        probe.inc();

        let encoded = encode_prometheus_text().expect("metrics encode succeeds");

        assert!(
            encoded.contains("service_kit_default_registry_probe_total 1"),
            "default registry metric missing from /metrics output"
        );
    }

    /// 两个注册表里同名的指标族只能输出一次：重复的 `# TYPE` 会让整次抓取失败。
    #[test]
    fn encode_prometheus_text_keeps_one_family_per_name() {
        let name = "service_kit_duplicate_probe_total";
        let own = IntCounter::new(name, "probe in the service kit registry")
            .expect("probe metric definition is static");
        let default = IntCounter::new(name, "probe in the default registry")
            .expect("probe metric definition is static");
        REGISTRY
            .register(Box::new(own.clone()))
            .expect("probe registers once");
        prometheus::default_registry()
            .register(Box::new(default.clone()))
            .expect("probe registers once");
        own.inc_by(2);
        default.inc_by(5);

        let encoded = encode_prometheus_text().expect("metrics encode succeeds");

        assert_eq!(encoded.matches(&format!("# TYPE {name} ")).count(), 1);
        assert!(encoded.contains(&format!("{name} 2")));
    }

    /// 面板的 CPU/内存/fd 图依赖进程指标；它们只在 Linux 上由默认注册表提供。
    #[cfg(target_os = "linux")]
    #[test]
    fn encode_prometheus_text_exports_process_metrics_on_linux() {
        let encoded = encode_prometheus_text().expect("metrics encode succeeds");
        for name in [
            "process_cpu_seconds_total",
            "process_resident_memory_bytes",
            "process_open_fds",
            "process_max_fds",
            "process_threads",
        ] {
            assert!(
                encoded.contains(&format!("# TYPE {name} ")),
                "{name} missing from /metrics output"
            );
        }
    }
}
