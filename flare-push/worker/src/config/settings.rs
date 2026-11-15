//! Push Worker 配置（以设计文档为准）

use std::{collections::HashMap, env, time::Duration};

use flare_im_contracts::constants::groups::PUSH_WORKER_GROUP_DEFAULT;
use flare_im_contracts::constants::topics::{
    TOPIC_PUSH_DLQ, TOPIC_PUSH_OFFLINE, TOPIC_PUSH_OFFLINE_BULK, TOPIC_PUSH_OFFLINE_GROUP,
    TOPIC_PUSH_ONLINE,
};
use flare_im_service_kit::config::{
    FlareAppConfig, PushVendorGovernanceConfig, PushWorkerVendorsConfig,
};
use flare_im_service_kit::metrics::MetricsEndpointConfig;
use flare_server_core::error::{FlareError, Result};
use flare_server_core::mq::kafka::{KafkaConsumerConfig, KafkaProducerConfig};
use flare_server_core::mq::nats::{
    NatsConsumerConfig, NatsProducerConfig, NatsStreamSpec, default_stream_specs,
};

use crate::domain::vendor_delivery::{Vendor, resolve_binding_order};
use crate::domain::vendor_policy::{BreakerConfig, VendorLimits};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OfflineDeliveryBackend {
    Outbox,
    Getui,
    Fcm,
    Apns,
    Mobile,
    Disabled,
}

impl OfflineDeliveryBackend {
    fn parse(value: &str) -> Result<Self> {
        Ok(match value.trim().to_ascii_lowercase().as_str() {
            "outbox" => Self::Outbox,
            "getui" => Self::Getui,
            "fcm" | "firebase" => Self::Fcm,
            "apns" | "apple" => Self::Apns,
            "mobile" => Self::Mobile,
            "disabled" | "off" | "none" => Self::Disabled,
            _ => {
                return Err(FlareError::system(
                    "unsupported PUSH_WORKER_OFFLINE_DELIVERY_BACKEND",
                ));
            }
        })
    }

    /// 后端的主名（`parse` 接受的第一个写法），用在启动日志里。
    pub fn name(self) -> &'static str {
        match self {
            Self::Outbox => "outbox",
            Self::Getui => "getui",
            Self::Fcm => "fcm",
            Self::Apns => "apns",
            Self::Mobile => "mobile",
            Self::Disabled => "disabled",
        }
    }

    /// 投递后端启动时打的那一行。部署冒烟（flarectl smoke）按这行认出 worker 在用哪个后端，
    /// 改措辞要同步那边；关闭时另有一行说明离线推送是关着的。
    pub fn enabled_line(self) -> String {
        format!("offline push {} backend enabled", self.name())
    }
}

/// 每个消费者同时处理多少条。四个主题各一个消费者：批量通道积压时，单聊与在线投递照常。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerConcurrency {
    pub online: usize,
    /// 单聊、紧急、点名与修正。
    pub offline: usize,
    /// 群里的普通提示与尾部刷新。
    pub offline_group: usize,
    /// 大群波次、@所有人 波次、摘要与补提示。
    pub offline_bulk: usize,
}

impl Default for ConsumerConcurrency {
    /// 单聊通道拿得最多：批量通道再忙，也只占它自己那一份。
    fn default() -> Self {
        Self {
            online: 128,
            offline: 64,
            offline_group: 32,
            offline_bulk: 16,
        }
    }
}

impl ConsumerConcurrency {
    fn from_env() -> Self {
        let defaults = Self::default();
        let read = |name: &str, default: usize| {
            env::var(name)
                .ok()
                .and_then(|value| value.trim().parse::<usize>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(default)
        };
        Self {
            online: read("PUSH_WORKER_ONLINE_CONCURRENCY", defaults.online),
            offline: read("PUSH_WORKER_OFFLINE_CONCURRENCY", defaults.offline),
            offline_group: read(
                "PUSH_WORKER_OFFLINE_GROUP_CONCURRENCY",
                defaults.offline_group,
            ),
            offline_bulk: read(
                "PUSH_WORKER_OFFLINE_BULK_CONCURRENCY",
                defaults.offline_bulk,
            ),
        }
    }
}

#[derive(Clone)]
pub struct PushWorkerConfig {
    pub mq_backend: String,
    pub jetstream_url: String,
    pub jetstream_timeout_ms: u64,
    pub jetstream_retries: u32,
    pub jetstream_retry_backoff_ms: u64,
    pub jetstream_stream_specs: Vec<NatsStreamSpec>,
    pub kafka_brokers: Vec<String>,
    pub kafka_client_id: String,
    pub kafka_options: HashMap<String, String>,
    /// 在线投递消费者的消费组；三条离线通道的消费组在它后面加 `-offline`、`-offline-group`、
    /// `-offline-bulk`。
    pub consumer_group: String,

    pub push_online_topic: String,
    /// 离线任务的单聊通道（单聊、紧急、点名、修正）。
    pub push_offline_topic: String,
    /// 离线任务的群通道（群里的普通提示、尾部刷新）。
    pub push_offline_group_topic: String,
    /// 离线任务的批量通道（大群波次、@所有人 波次、摘要、补提示）。
    pub push_offline_bulk_topic: String,
    pub push_dlq_topic: String,
    pub consumer_concurrency: ConsumerConcurrency,

    /// flare-signaling/online（ListUserDevices）gRPC 地址（与 `config/services/signaling-online.toml` 中 server.port 一致）
    pub online_service_endpoint: String,
    /// 无注册中心时 Access Gateway gRPC 直连地址（与 `GatewayRouterConfig.static_fallback_endpoint` 一致）
    pub access_gateway_static_endpoint: Option<String>,
    /// 未配置离线推送提供者时的有界本地 parking 容量。
    pub offline_parking_capacity: usize,
    /// 离线推送后端：outbox/getui/fcm/apns/mobile/disabled。生产环境必须显式配置，且不接受 outbox。
    pub offline_delivery_backend: OfflineDeliveryBackend,
    pub offline_mobile_providers: Vec<OfflineDeliveryBackend>,
    /// 离线推送 outbox Redis 地址；None 表示禁用（PUSH_WORKER_OFFLINE_REDIS_URL=off）
    pub offline_outbox_redis_url: Option<String>,
    /// 离线推送 outbox Stream key
    pub offline_outbox_stream: String,
    /// 离线推送 outbox Stream MAXLEN（~ 近似裁剪）
    pub offline_outbox_maxlen: usize,
    /// 设备厂商 token registry Redis 地址。
    pub device_token_redis_url: String,
    /// 设备厂商 token registry Redis key 前缀。
    pub device_token_key_prefix: String,
    /// 多少天没活跃的绑定视为不存在：投递时跳过并删除。应与 Push Server 的同名设置一致。
    pub binding_inactive_days: u32,
    /// Private per-application credentials mount, consumed only by push-worker.
    pub mobile_applications_file: Option<String>,
    /// Neutral leased configuration authority; no Platform API dependency.
    pub mobile_snapshot_file: Option<String>,
    /// 个推离线消息默认 TTL。
    pub getui_default_ttl_ms: u64,
    /// 个推 HTTP 请求超时。
    pub getui_request_timeout_ms: u64,
    pub metrics: MetricsEndpointConfig,
    /// 设备有多条推送绑定时依次尝试的厂商（`[services.push_worker] binding_order`），总是含全部厂商。
    pub binding_order: Vec<Vendor>,
    /// 各厂商的并发、速率与熔断（`[services.push_worker.vendors.<vendor>]` 叠在缺省值上）。
    pub vendor_limits: HashMap<Vendor, VendorLimits>,
}

impl PushWorkerConfig {
    /// 一个厂商的治理参数；没解析到的厂商用缺省值。
    pub fn vendor_limits(&self, vendor: Vendor) -> VendorLimits {
        self.vendor_limits
            .get(&vendor)
            .copied()
            .unwrap_or_else(|| VendorLimits::defaults(vendor))
    }
}

/// 与 `signaling-online` 监听地址对齐：优先读 app 中 `[services.signaling_online.server]`，否则本地默认 50061。
fn default_signaling_online_grpc_endpoint(app: &FlareAppConfig) -> String {
    let so = app.signaling_online_service();
    let Some(server) = so.runtime.server.as_ref() else {
        return "http://127.0.0.1:50061".to_string();
    };
    let port = server.port.unwrap_or(50061);
    let host = server
        .address
        .as_deref()
        .filter(|a| !a.is_empty())
        .map(|a| if a == "0.0.0.0" { "127.0.0.1" } else { a })
        .unwrap_or("127.0.0.1");
    format!("http://{}:{}", host, port)
}

impl PushWorkerConfig {
    pub fn from_app_config(app: &FlareAppConfig) -> Result<Self> {
        let service = app.push_worker_service();
        let jetstream_name = service.jetstream.as_deref().unwrap_or("push");
        let jetstream_profile = app.jetstream_profile(jetstream_name);
        let mq_backend = app.mq_default_backend().to_string();
        let kafka_profile = app.kafka_profile("push");
        let jetstream_stream_specs = {
            let specs = app
                .jetstream_topology()
                .into_iter()
                .map(|spec| {
                    NatsStreamSpec::new(spec.stream_name, spec.subjects)
                        .with_max_age(spec.max_age)
                        .with_duplicate_window(spec.duplicate_window)
                        .with_num_replicas(spec.num_replicas)
                        .with_max_bytes(spec.max_bytes)
                })
                .collect::<Vec<_>>();
            if specs.is_empty() {
                default_stream_specs()
            } else {
                specs
            }
        };

        let jetstream_url = env::var("PUSH_WORKER_JETSTREAM_URL")
            .ok()
            .or_else(|| jetstream_profile.map(|cfg| cfg.url.clone()))
            .unwrap_or_else(|| "nats://127.0.0.1:24222".to_string());
        let jetstream_timeout_ms = env::var("PUSH_WORKER_JETSTREAM_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .or_else(|| jetstream_profile.and_then(|cfg| cfg.timeout_ms))
            .unwrap_or(5_000);
        let jetstream_retries = env::var("PUSH_WORKER_JETSTREAM_RETRIES")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .or_else(|| jetstream_profile.and_then(|cfg| cfg.retries))
            .unwrap_or(8);
        let jetstream_retry_backoff_ms = env::var("PUSH_WORKER_JETSTREAM_RETRY_BACKOFF_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .or_else(|| jetstream_profile.and_then(|cfg| cfg.retry_backoff_ms))
            .unwrap_or(25);

        let consumer_group = env::var("PUSH_WORKER_CONSUMER_GROUP")
            .unwrap_or_else(|_| PUSH_WORKER_GROUP_DEFAULT.to_string());
        let kafka_brokers = env::var("PUSH_WORKER_KAFKA_BROKERS")
            .ok()
            .map(|v| {
                v.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            })
            .filter(|v| !v.is_empty())
            .or_else(|| kafka_profile.map(|p| p.brokers.clone()))
            .unwrap_or_else(|| vec!["127.0.0.1:29092".to_string()]);
        let kafka_client_id = env::var("PUSH_WORKER_KAFKA_CLIENT_ID")
            .ok()
            .or_else(|| kafka_profile.and_then(|p| p.client_id.clone()))
            .unwrap_or_else(|| "flare-im-push-worker".to_string());
        let kafka_options = kafka_profile.map(|p| p.options.clone()).unwrap_or_default();

        let push_online_topic = env::var("PUSH_WORKER_PUSH_ONLINE_TOPIC")
            .ok()
            .or_else(|| service.push_online_topic.clone())
            .unwrap_or_else(|| TOPIC_PUSH_ONLINE.to_string());
        let push_offline_topic = env::var("PUSH_WORKER_PUSH_OFFLINE_TOPIC")
            .ok()
            .or_else(|| service.push_offline_topic.clone())
            .unwrap_or_else(|| TOPIC_PUSH_OFFLINE.to_string());
        // 另外两条通道只走环境变量，缺省与 Push Server 的同名设置一致。
        let push_offline_group_topic = env::var("PUSH_WORKER_PUSH_OFFLINE_GROUP_TOPIC")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| TOPIC_PUSH_OFFLINE_GROUP.to_string());
        let push_offline_bulk_topic = env::var("PUSH_WORKER_PUSH_OFFLINE_BULK_TOPIC")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| TOPIC_PUSH_OFFLINE_BULK.to_string());
        let push_dlq_topic = env::var("PUSH_WORKER_PUSH_DLQ_TOPIC")
            .ok()
            .or_else(|| service.push_dlq_topic.clone())
            .unwrap_or_else(|| TOPIC_PUSH_DLQ.to_string());

        let online_service_endpoint = env::var("PUSH_WORKER_ONLINE_SERVICE_ENDPOINT")
            .ok()
            .unwrap_or_else(|| default_signaling_online_grpc_endpoint(app));

        let access_gateway_static_endpoint = env::var("ACCESS_GATEWAY_GRPC_ENDPOINT").ok();
        let offline_parking_capacity = env::var("PUSH_WORKER_OFFLINE_PARKING_CAPACITY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .or(service.offline_parking_capacity)
            .unwrap_or(4096);
        let offline_delivery_backend = resolve_offline_delivery_backend(
            env::var("PUSH_WORKER_OFFLINE_DELIVERY_BACKEND")
                .ok()
                .as_deref(),
            &env::var("FLARE_ENV").unwrap_or_default(),
        )?;
        let offline_mobile_providers = mobile_providers(
            offline_delivery_backend,
            non_empty_env("PUSH_WORKER_OFFLINE_PROVIDERS").as_deref(),
        )?;
        let offline_outbox_redis_url = resolve_offline_outbox_redis_url(
            env::var("PUSH_WORKER_OFFLINE_REDIS_URL").ok(),
            app.redis_profile("push").map(|profile| profile.url.clone()),
        );
        let offline_outbox_stream = env::var("PUSH_WORKER_OFFLINE_OUTBOX_STREAM")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "flare:im:push:offline:outbox".to_string());
        let offline_outbox_maxlen = env::var("PUSH_WORKER_OFFLINE_OUTBOX_MAXLEN")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(100_000);
        let push_redis_profile_url = app.redis_profile("push").map(|profile| profile.url.clone());
        let device_token_redis_url = env::var("PUSH_WORKER_DEVICE_TOKEN_REDIS_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or(push_redis_profile_url)
            .unwrap_or_else(|| "redis://127.0.0.1:6379".to_string());
        let device_token_key_prefix = env::var("PUSH_WORKER_DEVICE_TOKEN_KEY_PREFIX")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "flare:im:push:device_tokens".to_string());
        let binding_inactive_days = env::var("PUSH_WORKER_BINDING_INACTIVE_DAYS")
            .ok()
            .and_then(|value| value.trim().parse::<u32>().ok())
            .filter(|days| *days > 0)
            .unwrap_or(
                flare_im_contracts::domain::push_binding::PUSH_BINDING_INACTIVE_DAYS_DEFAULT,
            );
        reject_global_vendor_credentials()?;
        let binding_order =
            resolve_binding_order(service.binding_order.as_deref().unwrap_or_default())?;
        let vendor_limits = resolve_vendor_limits(service.vendors.as_ref())?;
        let mobile_applications_file = non_empty_env("PUSH_WORKER_MOBILE_APPLICATIONS_FILE");
        let mobile_snapshot_file = non_empty_env("PUSH_WORKER_MOBILE_SNAPSHOT_FILE");
        if mobile_applications_file.is_some() && mobile_snapshot_file.is_some() {
            return Err(flare_server_core::FlareError::localized(
                flare_server_core::ErrorCode::InvalidParameter,
                "choose exactly one mobile credential source",
            ));
        }
        let getui_default_ttl_ms = env::var("PUSH_WORKER_GETUI_DEFAULT_TTL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(7_200_000);
        let getui_request_timeout_ms = env::var("PUSH_WORKER_GETUI_REQUEST_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(5_000);
        let metrics_enabled = parse_bool_env("PUSH_WORKER_METRICS_ENABLED")
            .or(service.metrics_enabled)
            .unwrap_or(true);
        let metrics_address = env::var("PUSH_WORKER_METRICS_ADDRESS")
            .ok()
            .or_else(|| service.metrics_address.clone())
            .unwrap_or_else(|| "0.0.0.0".to_string());
        let metrics_port = env::var("PUSH_WORKER_METRICS_PORT")
            .ok()
            .and_then(|value| value.parse::<u16>().ok())
            .or(service.metrics_port)
            .unwrap_or(19186);
        let metrics_path = env::var("PUSH_WORKER_METRICS_PATH")
            .ok()
            .or_else(|| service.metrics_path.clone())
            .unwrap_or_else(|| "/metrics".to_string());
        let mut metrics =
            MetricsEndpointConfig::new(metrics_address, metrics_port).with_path(metrics_path);
        metrics.enabled = metrics_enabled;

        Ok(Self {
            mq_backend,
            jetstream_url,
            jetstream_timeout_ms,
            jetstream_retries,
            jetstream_retry_backoff_ms,
            jetstream_stream_specs,
            kafka_brokers,
            kafka_client_id,
            kafka_options,
            consumer_group,
            push_online_topic,
            push_offline_topic,
            push_offline_group_topic,
            push_offline_bulk_topic,
            push_dlq_topic,
            consumer_concurrency: ConsumerConcurrency::from_env(),
            online_service_endpoint,
            access_gateway_static_endpoint,
            offline_parking_capacity,
            offline_delivery_backend,
            offline_mobile_providers,
            offline_outbox_redis_url,
            offline_outbox_stream,
            offline_outbox_maxlen,
            device_token_redis_url,
            device_token_key_prefix,
            binding_inactive_days,
            mobile_applications_file,
            mobile_snapshot_file,
            getui_default_ttl_ms,
            getui_request_timeout_ms,
            metrics,
            binding_order,
            vendor_limits,
        })
    }
}

/// `[services.push_worker.vendors.<apns|fcm|getui>]` 叠到各厂商的缺省值上。取值越界是配置错误，
/// 启动即失败，不悄悄改成别的值。
fn resolve_vendor_limits(
    section: Option<&PushWorkerVendorsConfig>,
) -> Result<HashMap<Vendor, VendorLimits>> {
    Vendor::ALL
        .into_iter()
        .map(|vendor| {
            let configured = section.and_then(|vendors| match vendor {
                Vendor::Apns => vendors.apns.as_ref(),
                Vendor::Fcm => vendors.fcm.as_ref(),
                Vendor::Getui => vendors.getui.as_ref(),
            });
            Ok((vendor, configured_vendor_limits(vendor, configured)?))
        })
        .collect()
}

fn configured_vendor_limits(
    vendor: Vendor,
    configured: Option<&PushVendorGovernanceConfig>,
) -> Result<VendorLimits> {
    let defaults = VendorLimits::defaults(vendor);
    let Some(configured) = configured else {
        return Ok(defaults);
    };
    let rate_per_sec = configured.rate_per_sec.unwrap_or(defaults.rate_per_sec);
    let window = configured.window.unwrap_or(defaults.breaker.window);
    let limits = VendorLimits {
        max_concurrency: configured
            .max_concurrency
            .unwrap_or(defaults.max_concurrency),
        rate_per_sec,
        // 没写容量时与速率相同：一秒的量可以一次用掉。
        burst: configured.burst.unwrap_or(rate_per_sec),
        breaker: BreakerConfig {
            failure_ratio: configured
                .failure_ratio
                .unwrap_or(defaults.breaker.failure_ratio),
            window,
            // 窗口改小时，缺省的最少调用数随窗口收小。
            min_calls: configured
                .min_calls
                .unwrap_or(defaults.breaker.min_calls.min(window)),
            open_for: configured
                .open_ms
                .map_or(defaults.breaker.open_for, Duration::from_millis),
            half_open_probes: configured
                .half_open_probes
                .unwrap_or(defaults.breaker.half_open_probes),
        },
    };
    let breaker = &limits.breaker;
    let valid = limits.max_concurrency >= 1
        && limits.rate_per_sec >= 1
        && limits.burst >= 1
        && breaker.failure_ratio.is_finite()
        && breaker.failure_ratio > 0.0
        && breaker.failure_ratio <= 1.0
        && breaker.window >= 1
        && (1..=breaker.window).contains(&breaker.min_calls)
        && !breaker.open_for.is_zero()
        && breaker.half_open_probes >= 1;
    if !valid {
        return Err(FlareError::system(format!(
            "[services.push_worker.vendors.{}] has a value out of range: max_concurrency, rate_per_sec, burst, window, open_ms and half_open_probes must be at least 1, failure_ratio in (0, 1], min_calls between 1 and window",
            vendor.as_str()
        )));
    }
    Ok(limits)
}

/// 解析离线投递后端。没配置时缺省为 `disabled`：不选后端就不投递，也不冒充投递。
///
/// 生产环境另有规则，都在连 MQ 之前生效：
/// - 没配置就拒绝启动。缺省的 `disabled` 会把每条离线任务送进死信，进程照常运行、没有任何报错，
///   漏配一个变量不该变成「用户离线就收不到提示」。
/// - 显式写 `disabled` 可以启动：没有移动端的部署仍然需要 worker 做在线推送，这是有意的选择。
/// - `outbox` 只落库不投递，生产上没有人消费它，照旧拒绝。
fn resolve_offline_delivery_backend(
    raw: Option<&str>,
    environment: &str,
) -> Result<OfflineDeliveryBackend> {
    let production = environment.trim().eq_ignore_ascii_case("production");
    let Some(raw) = raw else {
        if production {
            return Err(FlareError::system(
                "FLARE_ENV=production requires PUSH_WORKER_OFFLINE_DELIVERY_BACKEND to be set: choose fcm, apns, getui or mobile, or set it to disabled to run without offline push",
            ));
        }
        return Ok(OfflineDeliveryBackend::Disabled);
    };
    let backend = OfflineDeliveryBackend::parse(raw)?;
    if production && backend == OfflineDeliveryBackend::Outbox {
        return Err(FlareError::system(
            "FLARE_ENV=production forbids the offline outbox backend: it stores tasks and delivers nothing; set PUSH_WORKER_OFFLINE_DELIVERY_BACKEND to fcm, apns, getui or mobile, or to disabled to run without offline push",
        ));
    }
    Ok(backend)
}

fn mobile_providers(
    backend: OfflineDeliveryBackend,
    raw: Option<&str>,
) -> Result<Vec<OfflineDeliveryBackend>> {
    if backend != OfflineDeliveryBackend::Mobile {
        if raw.is_some() {
            return Err(FlareError::system(
                "OFFLINE_PROVIDERS requires mobile backend",
            ));
        }
        return Ok(Vec::new());
    }
    let raw = raw
        .ok_or_else(|| FlareError::system("mobile backend requires explicit OFFLINE_PROVIDERS"))?;
    let mut providers = Vec::new();
    for name in raw.split(',') {
        let provider = OfflineDeliveryBackend::parse(name)?;
        if !matches!(
            provider,
            OfflineDeliveryBackend::Fcm
                | OfflineDeliveryBackend::Apns
                | OfflineDeliveryBackend::Getui
        ) || providers.contains(&provider)
        {
            return Err(FlareError::system(
                "OFFLINE_PROVIDERS must be unique fcm/apns/getui entries",
            ));
        }
        providers.push(provider);
    }
    Ok(providers)
}

fn reject_global_vendor_credentials() -> Result<()> {
    for name in [
        "PUSH_WORKER_FCM_SERVICE_ACCOUNT_JSON",
        "PUSH_WORKER_APNS_TEAM_ID",
        "PUSH_WORKER_APNS_KEY_ID",
        "PUSH_WORKER_APNS_PRIVATE_KEY",
        "PUSH_WORKER_APNS_TOPIC",
        "PUSH_WORKER_APNS_SANDBOX",
        "PUSH_WORKER_GETUI_APP_ID",
        "PUSH_WORKER_GETUI_APP_KEY",
        "PUSH_WORKER_GETUI_MASTER_SECRET",
        "PUSH_WORKER_GETUI_BASE_URL",
    ] {
        if env::var_os(name).is_some() {
            return Err(FlareError::system(
                "global mobile credentials unsupported; configure per-application private file",
            ));
        }
    }
    Ok(())
}

fn non_empty_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn parse_bool_env(name: &str) -> Option<bool> {
    env::var(name)
        .ok()
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "on" | "yes" => Some(true),
            "0" | "false" | "off" | "no" => Some(false),
            _ => None,
        })
}

fn resolve_offline_outbox_redis_url(
    env_value: Option<String>,
    redis_profile_url: Option<String>,
) -> Option<String> {
    match env_value.as_deref().map(str::trim) {
        Some(value) if value.eq_ignore_ascii_case("off") => None,
        Some(value) if value.eq_ignore_ascii_case("disabled") => None,
        Some(value) if !value.is_empty() => Some(value.to_string()),
        _ => Some(redis_profile_url.unwrap_or_else(|| "redis://127.0.0.1:6379".to_string())),
    }
}

impl NatsConsumerConfig for PushWorkerConfig {
    fn nats_url(&self) -> &str {
        &self.jetstream_url
    }
    fn consumer_group(&self) -> &str {
        &self.consumer_group
    }
    fn enable_manual_ack(&self) -> bool {
        true
    }
    fn batch_size(&self) -> usize {
        64
    }
    fn batch_timeout_ms(&self) -> u64 {
        50
    }
    fn enable_durable(&self) -> bool {
        true
    }

    fn stream_specs(&self) -> Vec<NatsStreamSpec> {
        self.jetstream_stream_specs.clone()
    }
}

impl NatsProducerConfig for PushWorkerConfig {
    fn nats_url(&self) -> &str {
        &self.jetstream_url
    }
    fn timeout_ms(&self) -> u64 {
        self.jetstream_timeout_ms
    }
    fn retries(&self) -> u32 {
        self.jetstream_retries
    }
    fn retry_backoff_ms(&self) -> u64 {
        self.jetstream_retry_backoff_ms
    }

    fn stream_specs(&self) -> Vec<NatsStreamSpec> {
        self.jetstream_stream_specs.clone()
    }
}

impl KafkaProducerConfig for PushWorkerConfig {
    fn kafka_brokers(&self) -> Vec<String> {
        self.kafka_brokers.clone()
    }

    fn kafka_client_id(&self) -> &str {
        &self.kafka_client_id
    }

    fn kafka_options(&self) -> HashMap<String, String> {
        self.kafka_options.clone()
    }
}

impl KafkaConsumerConfig for PushWorkerConfig {
    fn kafka_consumer_group(&self) -> &str {
        &self.consumer_group
    }
}

#[cfg(test)]
impl PushWorkerConfig {
    /// 测试用配置：不指向任何真实依赖，用例用结构体更新语法只覆盖自己关心的字段。
    pub(crate) fn for_tests() -> Self {
        Self {
            mq_backend: "nats".into(),
            jetstream_url: "nats://127.0.0.1:1".into(),
            jetstream_timeout_ms: 1_000,
            jetstream_retries: 0,
            jetstream_retry_backoff_ms: 0,
            jetstream_stream_specs: default_stream_specs(),
            kafka_brokers: Vec::new(),
            kafka_client_id: String::new(),
            kafka_options: HashMap::new(),
            consumer_group: PUSH_WORKER_GROUP_DEFAULT.into(),
            push_online_topic: TOPIC_PUSH_ONLINE.into(),
            push_offline_topic: TOPIC_PUSH_OFFLINE.into(),
            push_offline_group_topic: TOPIC_PUSH_OFFLINE_GROUP.into(),
            push_offline_bulk_topic: TOPIC_PUSH_OFFLINE_BULK.into(),
            push_dlq_topic: TOPIC_PUSH_DLQ.into(),
            consumer_concurrency: ConsumerConcurrency::default(),
            online_service_endpoint: String::new(),
            access_gateway_static_endpoint: None,
            offline_parking_capacity: 16,
            offline_delivery_backend: OfflineDeliveryBackend::Disabled,
            offline_mobile_providers: Vec::new(),
            offline_outbox_redis_url: None,
            offline_outbox_stream: String::new(),
            offline_outbox_maxlen: 0,
            device_token_redis_url: String::new(),
            device_token_key_prefix: String::new(),
            binding_inactive_days: 60,
            mobile_applications_file: None,
            mobile_snapshot_file: None,
            getui_default_ttl_ms: 0,
            getui_request_timeout_ms: 0,
            metrics: MetricsEndpointConfig::new("127.0.0.1", 0),
            binding_order: crate::domain::vendor_delivery::DEFAULT_BINDING_ORDER.to_vec(),
            vendor_limits: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        OfflineDeliveryBackend, mobile_providers, resolve_offline_delivery_backend,
        resolve_offline_outbox_redis_url, resolve_vendor_limits,
    };
    use crate::domain::vendor_delivery::Vendor;
    use crate::domain::vendor_policy::VendorLimits;
    use flare_im_service_kit::config::{PushVendorGovernanceConfig, PushWorkerVendorsConfig};

    /// 没写的厂商与字段用缺省值；容量缺省跟速率走，最少调用数随窗口收小；越界的值拒绝启动。
    #[test]
    fn vendor_governance_overrides_layer_on_the_defaults() {
        let defaults = resolve_vendor_limits(None).unwrap();
        for vendor in Vendor::ALL {
            assert_eq!(defaults[&vendor], VendorLimits::defaults(vendor));
        }
        let section = PushWorkerVendorsConfig {
            getui: Some(PushVendorGovernanceConfig {
                rate_per_sec: Some(50),
                window: Some(4),
                open_ms: Some(5_000),
                ..Default::default()
            }),
            ..Default::default()
        };
        let limits = resolve_vendor_limits(Some(&section)).unwrap();
        let getui = limits[&Vendor::Getui];
        assert_eq!((getui.rate_per_sec, getui.burst), (50, 50));
        assert_eq!(getui.max_concurrency, 64);
        assert_eq!((getui.breaker.window, getui.breaker.min_calls), (4, 4));
        assert_eq!(getui.breaker.open_for, std::time::Duration::from_secs(5));
        assert_eq!(limits[&Vendor::Apns], VendorLimits::defaults(Vendor::Apns));

        for bad in [
            PushVendorGovernanceConfig {
                failure_ratio: Some(1.5),
                ..Default::default()
            },
            PushVendorGovernanceConfig {
                max_concurrency: Some(0),
                ..Default::default()
            },
            PushVendorGovernanceConfig {
                window: Some(5),
                min_calls: Some(6),
                ..Default::default()
            },
        ] {
            let section = PushWorkerVendorsConfig {
                fcm: Some(bad),
                ..Default::default()
            };
            let error = resolve_vendor_limits(Some(&section)).unwrap_err();
            assert!(error.to_string().contains("vendors.fcm"), "{error}");
        }
    }

    #[test]
    fn offline_delivery_backend_is_explicit() {
        assert_eq!(
            mobile_providers(OfflineDeliveryBackend::Mobile, Some("fcm,apns")).unwrap(),
            vec![OfflineDeliveryBackend::Fcm, OfflineDeliveryBackend::Apns]
        );
        for raw in [
            None,
            Some(""),
            Some("fcm,fcm"),
            Some("outbox"),
            Some("mobile"),
            Some("getui,"),
        ] {
            assert!(mobile_providers(OfflineDeliveryBackend::Mobile, raw).is_err());
        }
        assert!(mobile_providers(OfflineDeliveryBackend::Fcm, Some("apns")).is_err());
        assert_eq!(
            OfflineDeliveryBackend::parse("getui").unwrap(),
            OfflineDeliveryBackend::Getui
        );
        assert_eq!(
            OfflineDeliveryBackend::parse("off").unwrap(),
            OfflineDeliveryBackend::Disabled
        );
    }

    /// 生产上漏配后端不能悄悄落回缺省的 `disabled`：那样进程照常运行，
    /// 每条离线任务却都进了死信。`outbox` 只落库不投递，同样拒绝。
    #[test]
    fn production_never_silently_parks_offline_notifications() {
        for environment in ["production", " Production ", "PRODUCTION"] {
            for raw in [None, Some("outbox")] {
                let error = resolve_offline_delivery_backend(raw, environment)
                    .expect_err("production needs an explicit, delivering backend");
                // 报错要让运维知道改哪个变量。
                assert!(
                    error
                        .to_string()
                        .contains("PUSH_WORKER_OFFLINE_DELIVERY_BACKEND")
                );
            }
        }
        for (raw, backend) in [
            ("fcm", OfflineDeliveryBackend::Fcm),
            ("apns", OfflineDeliveryBackend::Apns),
            ("getui", OfflineDeliveryBackend::Getui),
            ("mobile", OfflineDeliveryBackend::Mobile),
        ] {
            assert_eq!(
                resolve_offline_delivery_backend(Some(raw), "production").unwrap(),
                backend
            );
        }
        assert!(OfflineDeliveryBackend::parse("typo").is_err());
    }

    /// 显式写 `disabled` 是有意的选择：没有移动端的部署仍然要靠 worker 做在线推送，
    /// 生产上必须能启动。与「没配置」的区别只在于有人确实做了这个决定。
    #[test]
    fn an_explicit_disabled_backend_starts_in_production() {
        for raw in ["disabled", "off", "none", " Disabled "] {
            assert_eq!(
                resolve_offline_delivery_backend(Some(raw), "production").unwrap(),
                OfflineDeliveryBackend::Disabled
            );
        }
        assert!(resolve_offline_delivery_backend(None, "production").is_err());
    }

    /// 其他环境保持原有行为：没配置就是 `disabled`，开发栈也可以用 `outbox`。
    #[test]
    fn outside_production_an_unset_backend_defaults_to_disabled() {
        for environment in ["", "development", "staging", "test"] {
            assert_eq!(
                resolve_offline_delivery_backend(None, environment).unwrap(),
                OfflineDeliveryBackend::Disabled
            );
            assert_eq!(
                resolve_offline_delivery_backend(Some("disabled"), environment).unwrap(),
                OfflineDeliveryBackend::Disabled
            );
            assert_eq!(
                resolve_offline_delivery_backend(Some("outbox"), environment).unwrap(),
                OfflineDeliveryBackend::Outbox
            );
        }
        // 写错的后端名在任何环境都是配置错误，不会被当成「没配置」而落回缺省。
        for environment in ["development", "production"] {
            assert!(resolve_offline_delivery_backend(Some("typo"), environment).is_err());
        }
    }

    /// 每个后端的主名都能被 `parse` 认回来，启动行按固定格式带上它。
    #[test]
    fn every_backend_names_itself_in_its_startup_line() {
        for (backend, line) in [
            (
                OfflineDeliveryBackend::Outbox,
                "offline push outbox backend enabled",
            ),
            (
                OfflineDeliveryBackend::Getui,
                "offline push getui backend enabled",
            ),
            (
                OfflineDeliveryBackend::Fcm,
                "offline push fcm backend enabled",
            ),
            (
                OfflineDeliveryBackend::Apns,
                "offline push apns backend enabled",
            ),
            (
                OfflineDeliveryBackend::Mobile,
                "offline push mobile backend enabled",
            ),
        ] {
            assert_eq!(
                OfflineDeliveryBackend::parse(backend.name()).unwrap(),
                backend
            );
            assert_eq!(backend.enabled_line(), line);
        }
        assert_eq!(
            OfflineDeliveryBackend::parse(OfflineDeliveryBackend::Disabled.name()).unwrap(),
            OfflineDeliveryBackend::Disabled
        );
    }

    #[test]
    fn offline_outbox_defaults_to_push_redis_profile() {
        let url = resolve_offline_outbox_redis_url(
            None,
            Some("redis://push-redis.internal:6379".to_string()),
        );

        assert_eq!(url.as_deref(), Some("redis://push-redis.internal:6379"));
    }

    #[test]
    fn offline_outbox_uses_local_redis_when_profile_is_absent() {
        let url = resolve_offline_outbox_redis_url(None, None);

        assert_eq!(url.as_deref(), Some("redis://127.0.0.1:6379"));
    }

    #[test]
    fn offline_outbox_can_be_disabled_explicitly() {
        assert_eq!(
            resolve_offline_outbox_redis_url(Some("off".to_string()), None),
            None
        );
        assert_eq!(
            resolve_offline_outbox_redis_url(Some(" disabled ".to_string()), None),
            None
        );
    }

    #[test]
    fn offline_outbox_env_url_overrides_profile() {
        let url = resolve_offline_outbox_redis_url(
            Some("redis://override:6379".to_string()),
            Some("redis://profile:6379".to_string()),
        );

        assert_eq!(url.as_deref(), Some("redis://override:6379"));
    }
}
