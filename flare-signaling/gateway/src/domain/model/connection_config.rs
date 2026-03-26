//! 连接领域服务配置

/// 连接管理领域服务配置
#[derive(Debug, Clone)]
pub struct ConnectionDomainServiceConfig {
    pub gateway_id: String,
}

impl Default for ConnectionDomainServiceConfig {
    fn default() -> Self {
        Self {
            gateway_id: "gateway-1".to_string(),
        }
    }
}
