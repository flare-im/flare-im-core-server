//! 路由端点值对象
//!
//! RoutedEndpoint 表示实际路由到的服务端点

use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

/// 路由端点值对象
///
/// 表示实际路由到的服务端点信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutedEndpoint {
    /// 服务名称
    service_name: String,
    /// 实例 ID
    instance_id: String,
    /// 地址
    address: String,
    /// 端口
    port: u16,
}

impl RoutedEndpoint {
    /// 创建新的路由端点
    pub fn new(service_name: String, instance_id: String, address: String, port: u16) -> Self {
        Self {
            service_name,
            instance_id,
            address,
            port,
        }
    }

    /// 转换为端点字符串（address:port）
    pub fn to_endpoint_string(&self) -> String {
        format!("{}:{}", self.address, self.port)
    }

    /// 转换为完整 URL（http://address:port）
    pub fn to_url(&self) -> String {
        format!("http://{}:{}", self.address, self.port)
    }

    /// 获取服务名称
    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    /// 获取实例 ID
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// 获取地址
    pub fn address(&self) -> &str {
        &self.address
    }

    /// 获取端口
    pub fn port(&self) -> u16 {
        self.port
    }
}

impl PartialEq for RoutedEndpoint {
    fn eq(&self, other: &Self) -> bool {
        self.service_name == other.service_name
            && self.instance_id == other.instance_id
            && self.address == other.address
            && self.port == other.port
    }
}

impl Eq for RoutedEndpoint {}

impl Hash for RoutedEndpoint {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.service_name.hash(state);
        self.instance_id.hash(state);
        self.address.hash(state);
        self.port.hash(state);
    }
}

impl std::fmt::Display for RoutedEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}[{}]@{}:{}",
            self.service_name, self.instance_id, self.address, self.port
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_routed_endpoint_creation() {
        let endpoint = RoutedEndpoint::new(
            "signaling-service".to_string(),
            "instance-001".to_string(),
            "192.168.1.100".to_string(),
            8080,
        );

        assert_eq!(endpoint.service_name(), "signaling-service");
        assert_eq!(endpoint.instance_id(), "instance-001");
        assert_eq!(endpoint.address(), "192.168.1.100");
        assert_eq!(endpoint.port(), 8080);
    }

    #[test]
    fn test_to_endpoint_string() {
        let endpoint = RoutedEndpoint::new(
            "signaling-service".to_string(),
            "instance-001".to_string(),
            "192.168.1.100".to_string(),
            8080,
        );

        assert_eq!(endpoint.to_endpoint_string(), "192.168.1.100:8080");
    }

    #[test]
    fn test_to_url() {
        let endpoint = RoutedEndpoint::new(
            "signaling-service".to_string(),
            "instance-001".to_string(),
            "192.168.1.100".to_string(),
            8080,
        );

        assert_eq!(endpoint.to_url(), "http://192.168.1.100:8080");
    }

    #[test]
    fn test_equality() {
        let endpoint1 = RoutedEndpoint::new(
            "service".to_string(),
            "instance-1".to_string(),
            "192.168.1.100".to_string(),
            8080,
        );

        let endpoint2 = RoutedEndpoint::new(
            "service".to_string(),
            "instance-1".to_string(),
            "192.168.1.100".to_string(),
            8080,
        );

        let endpoint3 = RoutedEndpoint::new(
            "service".to_string(),
            "instance-2".to_string(),
            "192.168.1.100".to_string(),
            8080,
        );

        assert_eq!(endpoint1, endpoint2);
        assert_ne!(endpoint1, endpoint3);
    }

    #[test]
    fn test_display() {
        let endpoint = RoutedEndpoint::new(
            "signaling-service".to_string(),
            "instance-001".to_string(),
            "192.168.1.100".to_string(),
            8080,
        );

        assert_eq!(
            format!("{}", endpoint),
            "signaling-service[instance-001]@192.168.1.100:8080"
        );
    }
}
