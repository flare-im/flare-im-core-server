//! Hook 配置 **读侧** 适配：从 [`ConfigWatcher`](crate::infrastructure::config::ConfigWatcher) 拉取当前快照（CQRS Query 基础设施面）。
//!
//! 每个查询都带租户：返回全局配置叠加该租户自己登记的配置，再去掉选择器限定了别的租户的 Hook
//! （见 [`HookConfigSnapshot::for_tenant`](crate::infrastructure::config::HookConfigSnapshot::for_tenant)）。
//! 租户取调用方签名上下文里的租户，不取请求体。

use std::sync::Arc;

use flare_server_core::error::Result;

use crate::domain::model::{HookConfig, HookConfigItem};
use crate::infrastructure::config::ConfigWatcher;

/// Hook 服务注册表（配置物化前的 **只读** 视图）。
pub struct CoreHookRegistry {
    config_watcher: Arc<ConfigWatcher>,
}

impl CoreHookRegistry {
    pub fn new(config_watcher: Arc<ConfigWatcher>) -> Self {
        Self { config_watcher }
    }

    async fn snapshot(&self, tenant_id: &str) -> HookConfig {
        self.config_watcher.config_for_tenant(tenant_id).await
    }

    /// 获取 PreSend Hook 列表
    pub async fn get_pre_send_hooks(&self, tenant_id: &str) -> Result<Vec<HookConfigItem>> {
        Ok(self.snapshot(tenant_id).await.pre_send)
    }

    /// 获取 PostSend Hook 列表
    pub async fn get_post_send_hooks(&self, tenant_id: &str) -> Result<Vec<HookConfigItem>> {
        Ok(self.snapshot(tenant_id).await.post_send)
    }

    /// 获取 Delivery Hook 列表
    pub async fn get_delivery_hooks(&self, tenant_id: &str) -> Result<Vec<HookConfigItem>> {
        Ok(self.snapshot(tenant_id).await.delivery)
    }

    /// 获取 Recall Hook 列表
    pub async fn get_recall_hooks(&self, tenant_id: &str) -> Result<Vec<HookConfigItem>> {
        Ok(self.snapshot(tenant_id).await.recall)
    }

    /// 获取 SessionCreate Hook 列表
    pub async fn get_session_create_hooks(&self, tenant_id: &str) -> Result<Vec<HookConfigItem>> {
        Ok(self.snapshot(tenant_id).await.session_create)
    }

    /// 获取 SessionUpdate Hook 列表
    pub async fn get_session_update_hooks(&self, tenant_id: &str) -> Result<Vec<HookConfigItem>> {
        Ok(self.snapshot(tenant_id).await.session_update)
    }

    /// 获取 SessionDelete Hook 列表
    pub async fn get_session_delete_hooks(&self, tenant_id: &str) -> Result<Vec<HookConfigItem>> {
        Ok(self.snapshot(tenant_id).await.session_delete)
    }

    /// 获取所有 ConversationLifecycle Hook 列表（合并 create/update/delete）
    pub async fn get_conversation_lifecycle_hooks(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<HookConfigItem>> {
        let c = self.snapshot(tenant_id).await;
        let mut hooks = Vec::new();
        hooks.extend(c.session_create);
        hooks.extend(c.session_update);
        hooks.extend(c.session_delete);
        Ok(hooks)
    }

    /// 获取 UserLogin Hook 列表
    pub async fn get_user_login_hooks(&self, tenant_id: &str) -> Result<Vec<HookConfigItem>> {
        Ok(self.snapshot(tenant_id).await.user_login)
    }

    /// 获取 UserLogout Hook 列表
    pub async fn get_user_logout_hooks(&self, tenant_id: &str) -> Result<Vec<HookConfigItem>> {
        Ok(self.snapshot(tenant_id).await.user_logout)
    }

    /// 获取 UserOnline Hook 列表
    pub async fn get_user_online_hooks(&self, tenant_id: &str) -> Result<Vec<HookConfigItem>> {
        Ok(self.snapshot(tenant_id).await.user_online)
    }

    /// 获取 UserOffline Hook 列表
    pub async fn get_user_offline_hooks(&self, tenant_id: &str) -> Result<Vec<HookConfigItem>> {
        Ok(self.snapshot(tenant_id).await.user_offline)
    }

    /// 获取 PushPreSend Hook 列表
    pub async fn get_push_pre_send_hooks(&self, tenant_id: &str) -> Result<Vec<HookConfigItem>> {
        Ok(self.snapshot(tenant_id).await.push_pre_send)
    }

    /// 获取 PushPostSend Hook 列表
    pub async fn get_push_post_send_hooks(&self, tenant_id: &str) -> Result<Vec<HookConfigItem>> {
        Ok(self.snapshot(tenant_id).await.push_post_send)
    }

    /// 获取 PushDelivery Hook 列表
    pub async fn get_push_delivery_hooks(&self, tenant_id: &str) -> Result<Vec<HookConfigItem>> {
        Ok(self.snapshot(tenant_id).await.push_delivery)
    }

    /// 获取 GetConversationParticipants Hook 列表
    pub async fn get_conversation_participants_hooks(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<HookConfigItem>> {
        Ok(self.snapshot(tenant_id).await.get_conversation_participants)
    }

    /// 重新加载所有配置源
    pub async fn reload_config(&self) -> Result<()> {
        self.config_watcher.reload().await
    }

    /// 只重新加载一个租户自己登记的配置（Administer 写入后调用，见
    /// [`ConfigWatcher::reload_tenant`]）。
    pub async fn reload_tenant(&self, tenant_id: &str) -> Result<()> {
        self.config_watcher.reload_tenant(tenant_id).await
    }
}
