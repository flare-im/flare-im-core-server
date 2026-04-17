//! # Hook服务注册
//!
//! 提供Hook服务的注册和管理

use crate::domain::model::HookConfigItem;
use crate::infrastructure::config::ConfigWatcher;
use anyhow::Result;
use std::sync::Arc;

/// Hook服务注册表
pub struct CoreHookRegistry {
    config_watcher: Arc<ConfigWatcher>,
}

impl CoreHookRegistry {
    pub fn new(config_watcher: Arc<ConfigWatcher>) -> Self {
        Self { config_watcher }
    }

    /// 获取PreSend Hook列表
    pub async fn get_pre_send_hooks(&self) -> Result<Vec<HookConfigItem>> {
        let config = self.config_watcher.get_config().await;
        Ok(config.pre_send)
    }

    /// 获取PostSend Hook列表
    pub async fn get_post_send_hooks(&self) -> Result<Vec<HookConfigItem>> {
        let config = self.config_watcher.get_config().await;
        Ok(config.post_send)
    }

    /// 获取Delivery Hook列表
    pub async fn get_delivery_hooks(&self) -> Result<Vec<HookConfigItem>> {
        let config = self.config_watcher.get_config().await;
        Ok(config.delivery)
    }

    /// 获取Recall Hook列表
    pub async fn get_recall_hooks(&self) -> Result<Vec<HookConfigItem>> {
        let config = self.config_watcher.get_config().await;
        Ok(config.recall)
    }

    /// 获取SessionCreate Hook列表
    pub async fn get_session_create_hooks(&self) -> Result<Vec<HookConfigItem>> {
        let config = self.config_watcher.get_config().await;
        Ok(config.session_create)
    }

    /// 获取SessionUpdate Hook列表
    pub async fn get_session_update_hooks(&self) -> Result<Vec<HookConfigItem>> {
        let config = self.config_watcher.get_config().await;
        Ok(config.session_update)
    }

    /// 获取SessionDelete Hook列表
    pub async fn get_session_delete_hooks(&self) -> Result<Vec<HookConfigItem>> {
        let config = self.config_watcher.get_config().await;
        Ok(config.session_delete)
    }

    /// 获取所有ConversationLifecycle Hook列表（合并create/update/delete）
    pub async fn get_conversation_lifecycle_hooks(&self) -> Result<Vec<HookConfigItem>> {
        let config = self.config_watcher.get_config().await;
        let mut hooks = Vec::new();
        hooks.extend(config.session_create);
        hooks.extend(config.session_update);
        hooks.extend(config.session_delete);
        Ok(hooks)
    }

    /// 获取UserLogin Hook列表
    pub async fn get_user_login_hooks(&self) -> Result<Vec<HookConfigItem>> {
        let config = self.config_watcher.get_config().await;
        Ok(config.user_login)
    }

    /// 获取UserLogout Hook列表
    pub async fn get_user_logout_hooks(&self) -> Result<Vec<HookConfigItem>> {
        let config = self.config_watcher.get_config().await;
        Ok(config.user_logout)
    }

    /// 获取UserOnline Hook列表
    pub async fn get_user_online_hooks(&self) -> Result<Vec<HookConfigItem>> {
        let config = self.config_watcher.get_config().await;
        Ok(config.user_online)
    }

    /// 获取UserOffline Hook列表
    pub async fn get_user_offline_hooks(&self) -> Result<Vec<HookConfigItem>> {
        let config = self.config_watcher.get_config().await;
        Ok(config.user_offline)
    }

    /// 获取PushPreSend Hook列表
    pub async fn get_push_pre_send_hooks(&self) -> Result<Vec<HookConfigItem>> {
        let config = self.config_watcher.get_config().await;
        Ok(config.push_pre_send)
    }

    /// 获取PushPostSend Hook列表
    pub async fn get_push_post_send_hooks(&self) -> Result<Vec<HookConfigItem>> {
        let config = self.config_watcher.get_config().await;
        Ok(config.push_post_send)
    }

    /// 获取PushDelivery Hook列表
    pub async fn get_push_delivery_hooks(&self) -> Result<Vec<HookConfigItem>> {
        let config = self.config_watcher.get_config().await;
        Ok(config.push_delivery)
    }

    /// 获取GetConversationParticipants Hook列表
    pub async fn get_conversation_participants_hooks(&self) -> Result<Vec<HookConfigItem>> {
        let config = self.config_watcher.get_config().await;
        Ok(config.get_conversation_participants)
    }

    /// 重新加载配置
    pub async fn reload_config(&self) -> Result<()> {
        self.config_watcher.reload().await
    }
}
