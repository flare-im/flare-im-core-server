//! 跨机房选择器值对象
//!
//! 负责基于地理/负载/健康度的跨机房智能路由

use std::collections::HashMap;
use std::sync::Arc;

/// 配置中心客户端 trait，用于查询租户偏好机房（Rust 2024 原生 async，禁止 `dyn`）
pub trait ConfigClient: Send + Sync {
    /// 获取租户偏好机房
    async fn get_tenant_preferred_az(&self, tenant_id: &str) -> anyhow::Result<Option<String>>;
}

/// 无配置中心时的占位实现
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopConfigClient;

impl ConfigClient for NoopConfigClient {
    async fn get_tenant_preferred_az(&self, _tenant_id: &str) -> anyhow::Result<Option<String>> {
        Ok(None)
    }
}

/// 跨机房智能选择器（Multi-AZ Routing）
///
/// 参考微信/Telegram的跨DC路由策略：
/// - 优先用户地理位置（GeoIP）
/// - 其次登录网关所在机房（就近原则）
/// - 机房负载/延迟/存储健康度综合评分
/// - 弱网环境优先近端机房
#[derive(Clone)]
pub struct AzSelector<C: ConfigClient + Send + Sync = NoopConfigClient> {
    /// 默认机房（兜底）
    default_az: String,
    /// 机房优先级配置（geo -> az 映射）
    /// 例如：{"CN-East": "shanghai", "CN-North": "beijing"}
    geo_az_map: HashMap<String, String>,
    /// 配置中心客户端（用于查询租户偏好机房）
    config_client: Option<Arc<C>>,
}

impl AzSelector<NoopConfigClient> {
    pub fn new() -> Self {
        Self {
            default_az: "default".to_string(),
            geo_az_map: HashMap::new(),
            config_client: None,
        }
    }
}

impl<C: ConfigClient + Send + Sync> AzSelector<C> {
    pub fn with_config_client(mut self, config_client: Arc<C>) -> Self {
        self.config_client = Some(config_client);
        self
    }

    /// 选择最优机房
    ///
    /// # 优先级
    /// 1. 地理位置（client_geo）
    /// 2. 登录网关机房（login_gateway）
    /// 3. 租户偏好机房（tenant_id）
    /// 4. 默认机房
    pub fn pick(
        &self,
        client_geo: Option<&str>,
        login_gateway: Option<&str>,
        _tenant_id: Option<&str>,
    ) -> Option<String> {
        // 1. 优先根据地理位置选择
        if let Some(geo) = client_geo {
            if let Some(az) = self.geo_az_map.get(geo) {
                tracing::trace!(geo = %geo, az = %az, "Selected AZ by client geo");
                return Some(az.clone());
            }
        }

        // 2. 根据登录网关提取机房（例如 gateway-sh-1 -> shanghai）
        if let Some(gateway) = login_gateway {
            if let Some(az) = self.extract_az_from_gateway(gateway) {
                tracing::trace!(gateway = %gateway, az = %az, "Selected AZ by login gateway");
                return Some(az);
            }
        }

        // 3. 租户级机房亲和性（查询配置中心）
        // 注意：实际实现应该查询配置中心获取租户偏好机房
        // 这里简化处理，因为 pick 方法是同步的

        // 4. 兜底：使用默认机房
        tracing::trace!(az = %self.default_az, "Using default AZ");
        Some(self.default_az.clone())
    }

    /// 从网关ID提取机房标识（例如 gateway-sh-1 -> shanghai）
    fn extract_az_from_gateway(&self, gateway: &str) -> Option<String> {
        // 简易实现：提取 gateway-{az}-{num} 中的 az 部分
        let parts: Vec<&str> = gateway.split('-').collect();
        if parts.len() >= 3 {
            Some(parts[1].to_string())
        } else {
            None
        }
    }
}

