//! gRPC 连接池
//!
//! 管理到下游服务的 gRPC 连接

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};

use crate::error::{map_infra_error, ErrorCode, Result};

/// gRPC 连接池配置
#[derive(Debug, Clone)]
pub struct GrpcConnectionPoolConfig {
    /// 连接超时时间（秒）
    pub connect_timeout: u64,
    /// 请求超时时间（秒）
    pub request_timeout: u64,
    /// Keep-Alive 间隔（秒）
    pub keep_alive_interval: u64,
    /// Keep-Alive 超时时间（秒）
    pub keep_alive_timeout: u64,
}

impl Default for GrpcConnectionPoolConfig {
    fn default() -> Self {
        Self {
            connect_timeout: 5,
            request_timeout: 30,
            keep_alive_interval: 10,
            keep_alive_timeout: 5,
        }
    }
}

/// gRPC 连接池
///
/// 管理到下游服务的 gRPC 连接，支持动态添加和获取连接
pub struct GrpcConnectionPool {
    /// 连接映射：service_name -> Channel
    connections: Arc<RwLock<HashMap<String, Channel>>>,
    /// 配置
    config: GrpcConnectionPoolConfig,
}

impl GrpcConnectionPool {
    /// 创建新的连接池
    pub fn new(config: GrpcConnectionPoolConfig) -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// 添加服务连接
    ///
    /// # 参数
    /// - `service_name`: 服务名称
    /// - `address`: 服务地址（如 "http://127.0.0.1:8080"）
    ///
    /// # 返回
    /// - `Ok(())`: 添加成功
    /// - `Err`: 连接失败
    pub async fn add_connection(&self, service_name: &str, address: &str) -> Result<()> {
        let endpoint = Endpoint::from_shared(address.to_string())
            .map_err(|e| map_infra_error(e, ErrorCode::NetworkError, &format!("Invalid gRPC endpoint: {}", address)))?
            .connect_timeout(Duration::from_secs(self.config.connect_timeout))
            .timeout(Duration::from_secs(self.config.request_timeout))
            .keep_alive_timeout(Duration::from_secs(self.config.keep_alive_timeout))
            .http2_keep_alive_interval(Duration::from_secs(self.config.keep_alive_interval))
            .connect()
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::NetworkError, &format!("Failed to connect to service {} at {}", service_name, address)))?;

        let mut connections = self.connections.write().await;
        connections.insert(service_name.to_string(), endpoint);

        tracing::info!(
            service_name = %service_name,
            address = %address,
            "Added gRPC connection to pool"
        );

        Ok(())
    }

    /// 获取服务连接
    ///
    /// # 参数
    /// - `service_name`: 服务名称
    ///
    /// # 返回
    /// - `Some(Channel)`: 连接存在
    /// - `None`: 连接不存在
    pub async fn get_connection(&self, service_name: &str) -> Option<Channel> {
        let connections = self.connections.read().await;
        connections.get(service_name).cloned()
    }

    /// 移除服务连接
    ///
    /// # 参数
    /// - `service_name`: 服务名称
    pub async fn remove_connection(&self, service_name: &str) {
        let mut connections = self.connections.write().await;
        if connections.remove(service_name).is_some() {
            tracing::info!(service_name = %service_name, "Removed gRPC connection from pool");
        }
    }

    /// 获取所有服务名称
    ///
    /// # 返回
    /// 服务名称列表
    pub async fn list_services(&self) -> Vec<String> {
        let connections = self.connections.read().await;
        connections.keys().cloned().collect()
    }

    /// 检查服务连接是否存在
    ///
    /// # 参数
    /// - `service_name`: 服务名称
    ///
    /// # 返回
    /// - `true`: 连接存在
    /// - `false`: 连接不存在
    pub async fn has_connection(&self, service_name: &str) -> bool {
        let connections = self.connections.read().await;
        connections.contains_key(service_name)
    }
}

impl Clone for GrpcConnectionPool {
    fn clone(&self) -> Self {
        Self {
            connections: Arc::clone(&self.connections),
            config: self.config.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_connection_pool_new() {
        let config = GrpcConnectionPoolConfig::default();
        let pool = GrpcConnectionPool::new(config);
        assert_eq!(pool.list_services().await.len(), 0);
    }

    #[tokio::test]
    async fn test_has_connection() {
        let pool = GrpcConnectionPool::new(GrpcConnectionPoolConfig::default());
        assert!(!pool.has_connection("test-service").await);
    }
}
