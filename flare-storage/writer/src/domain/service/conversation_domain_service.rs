//! 会话领域服务 - 处理与会话相关的业务逻辑
//!
//! 职责：
//! - 通过gRPC调用Conversation服务获取会话参与者列表
//! - 更新参与者的未读数
//! - 提供领域层的会话操作接口

use anyhow::{Result, anyhow};
use flare_server_core::discovery::ServiceClient;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::warn;

/// 会话领域服务
pub struct ConversationDomainService {
    service_client: Option<Arc<Mutex<ServiceClient>>>,
}

impl ConversationDomainService {
    /// 创建会话领域服务
    pub fn new(service_client: Option<Arc<Mutex<ServiceClient>>>) -> Self {
        Self { service_client }
    }

    /// 获取会话参与者列表
    ///
    /// 通过gRPC调用Conversation服务获取会话的所有参与者，用于更新未读数
    pub async fn get_conversation_participants(&self, _conversation_id: &str) -> Result<Vec<String>> {
        // 注意：由于ServiceClient不能被克隆，我们需要使用Mutex来安全地访问它
        match &self.service_client {
            Some(client_mutex) => {
                let client_guard = client_mutex.lock().await;
                // 这里需要实际调用Conversation服务的API来获取参与者列表
                // 由于具体的API尚未定义，我们暂时返回一个示例实现
                // 在实际部署中，这里会发起gRPC调用
                drop(client_guard); // 显式释放锁
                
                // 临时实现：返回一个空列表，等待Conversation服务API集成
                Ok(vec![])
            }
            None => {
                // 如果没有配置服务客户端，返回空列表
                Ok(vec![])
            }
        }
    }
}
