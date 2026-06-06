//! 流控器值对象
//!
//! 负责会话QPS、群聊fanout、系统反压检查

use flare_server_core::error::Result;
use redis::{AsyncCommands, aio::ConnectionManager};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::domain::service::RouteContext;

/// 监控客户端trait，用于查询系统反压信号
pub trait MonitoringClient: Send + Sync {
    /// 获取JetStream Lag值
    async fn get_jetstream_lag(&self) -> flare_server_core::error::Result<u64>;

    /// 获取Storage写入延迟（P99）
    async fn get_storage_latency(&self) -> flare_server_core::error::Result<f64>;
}

/// Noop 监控客户端实现（用于不需要监控的场景）
pub struct NoopMonitoringClient;

impl MonitoringClient for NoopMonitoringClient {
    async fn get_jetstream_lag(&self) -> flare_server_core::error::Result<u64> {
        Ok(0)
    }

    async fn get_storage_latency(&self) -> flare_server_core::error::Result<f64> {
        Ok(0.0)
    }
}

/// 默认的流控器类型（使用 NoopMonitoringClient）
pub type DefaultFlowController = FlowController<NoopMonitoringClient>;

/// 热点会话信息
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct HotSessionInfo {
    last_detected: std::time::SystemTime,
    current_qps: u32,
    degraded: bool,
}

/// 流控器
///
/// 职责：
/// - 会话级QPS限制（滑动窗口）
/// - 群聊fanout限制
/// - 系统反压检测（JetStream Lag、Storage延迟）
/// - 热点会话降级
///
/// 使用泛型参数以支持 Rust 2024 原生 async fn in traits
pub struct FlowController<MC> {
    /// 会话级QPS限制（默认50 QPS）
    session_qps_limit: u32,
    /// 热点会话阈值（超过此值触发降级）
    hot_session_threshold: u32,
    /// 是否启用反压检测
    backpressure_enabled: bool,
    /// Redis客户端（用于QPS计数）
    redis_client: Option<Arc<redis::Client>>,
    /// 监控客户端（用于反压信号）
    monitoring_client: Option<Arc<MC>>,
    /// 热点会话缓存
    hot_sessions: Arc<RwLock<HashMap<String, HotSessionInfo>>>,
}

impl<MC: MonitoringClient> Default for FlowController<MC> {
    fn default() -> Self {
        Self::new()
    }
}

impl<MC: MonitoringClient> FlowController<MC> {
    pub fn new() -> Self {
        Self {
            session_qps_limit: 50,
            hot_session_threshold: 100,
            backpressure_enabled: false,
            redis_client: None,
            monitoring_client: None,
            hot_sessions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn with_redis_client(mut self, redis_client: Arc<redis::Client>) -> Self {
        self.redis_client = Some(redis_client);
        self
    }

    pub fn with_monitoring_client(mut self, monitoring_client: Arc<MC>) -> Self {
        self.monitoring_client = Some(monitoring_client);
        self.backpressure_enabled = true;
        self
    }

    /// 获取Redis连接
    async fn get_redis_connection(&self) -> Result<ConnectionManager> {
        if let Some(client) = &self.redis_client {
            Ok(ConnectionManager::new(client.as_ref().clone()).await?)
        } else {
            Err(flare_server_core::error::FlareError::system(
                "Redis client not configured".to_string(),
            ))
        }
    }

    /// 查询会话QPS（使用Redis滑动窗口算法）
    async fn get_session_qps(&self, conversation_id: &str) -> Result<u32> {
        if self.redis_client.is_none() {
            return Ok(0);
        }

        let mut conn = self.get_redis_connection().await?;
        let key = format!("rate_limit:session_qps:{}", conversation_id);

        // 增加计数器并设置过期时间（1秒窗口）
        let count: u32 = conn.incr(&key, 1).await?;

        // 如果是第一次设置，则设置过期时间
        if count == 1 {
            let _: () = conn.expire(&key, 1).await?;
        }

        Ok(count)
    }

    /// 查询JetStream Lag（反压信号之一）
    async fn get_jetstream_lag(&self) -> Result<u64> {
        if let Some(client) = &self.monitoring_client {
            client.get_jetstream_lag().await
        } else {
            Ok(0)
        }
    }

    /// 查询Storage写入延迟（反压信号之一）
    async fn get_storage_latency(&self) -> Result<f64> {
        if let Some(client) = &self.monitoring_client {
            client.get_storage_latency().await
        } else {
            Ok(0.0)
        }
    }

    /// 检查是否为热点会话
    fn is_hot_session(&self, _conversation_id: &str, current_qps: u32) -> bool {
        current_qps > self.hot_session_threshold
    }

    /// 更新热点会话信息
    fn update_hot_session(&self, conversation_id: &str, current_qps: u32) {
        let mut hot_sessions = self.hot_sessions.write().unwrap();
        let info = HotSessionInfo {
            last_detected: std::time::SystemTime::now(),
            current_qps,
            degraded: current_qps > self.hot_session_threshold,
        };
        hot_sessions.insert(conversation_id.to_string(), info);
    }

    /// 检查会话是否已被降级
    fn is_session_degraded(&self, conversation_id: &str) -> bool {
        let hot_sessions = self.hot_sessions.read().unwrap();
        if let Some(info) = hot_sessions.get(conversation_id) {
            info.degraded
        } else {
            false
        }
    }

    /// 流控检查
    ///
    /// # 检查项
    /// 1. 会话级QPS限制（Redis滑动窗口）
    /// 2. 群聊fanout限制（大群消息批次限制）
    /// 3. 系统反压信号（JetStream Lag、Storage延迟）
    /// 4. 热点会话降级
    pub async fn check(&self, ctx: &RouteContext) -> Result<()> {
        // 1. 检查会话QPS（Redis INCR + EXPIRE）
        let conversation_id_str = ctx.conversation_id.as_deref();
        let session_qps = if let Some(conversation_id) = conversation_id_str {
            let qps = self.get_session_qps(conversation_id).await?;
            // 检查是否为热点会话
            if self.is_hot_session(conversation_id, qps) {
                self.update_hot_session(conversation_id, qps);
                // 如果会话已被降级，则延迟推送
                if self.is_session_degraded(conversation_id) {
                    // 延迟推送逻辑（可以根据需要调整延迟时间）
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
            qps
        } else {
            0
        };

        if ctx.conversation_id.is_some() && session_qps > self.session_qps_limit {
            return Err(flare_server_core::error::FlareError::system(format!(
                "Session QPS limit exceeded: {} > {}",
                session_qps, self.session_qps_limit
            )));
        }

        // 2. 检查群聊fanout（大群消息批次限制）
        // 对于群聊消息，检查接收者数量是否超过限制
        // 注意：这里的实现需要从ctx中获取消息类型和接收者信息

        // 3. 检查系统反压信号（JetStream Lag > 10000 或 Storage P99 > 500ms）
        if self.backpressure_enabled {
            // 检查JetStream Lag
            let jetstream_lag = self.get_jetstream_lag().await?;
            if jetstream_lag > 10000 {
                return Err(flare_server_core::error::FlareError::system(format!(
                    "JetStream lag too high: {}",
                    jetstream_lag
                )));
            }

            // 检查Storage写入延迟
            let storage_latency = self.get_storage_latency().await?;
            if storage_latency > 500.0 {
                return Err(flare_server_core::error::FlareError::system(format!(
                    "Storage latency too high: {}ms",
                    storage_latency
                )));
            }
        }

        tracing::trace!(
            conversation_id = ?ctx.conversation_id,
            svid = %ctx.svid,
            session_qps,
            "Flow control check passed"
        );
        Ok(())
    }
}
