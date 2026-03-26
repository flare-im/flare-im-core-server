//! 订阅领域服务 - 包含所有业务逻辑实现

use std::sync::Arc;

use anyhow::Result;
use flare_proto::signaling::online::{PresenceEvent, UserPresenceEvent, WatchPresenceRequest};
use flare_server_core::error::{ErrorBuilder, ErrorCode};
use tracing::info;

use crate::domain::repository::{PresencePublisher, SubscriptionRepository};

/// 订阅领域服务 - 包含所有业务逻辑（泛型依赖，避免 `dyn` 异步 trait）
pub struct SubscriptionService<SR: SubscriptionRepository + Send + Sync, PP: PresencePublisher + Send + Sync> {
    subscription_repo: Arc<SR>,
    presence_publisher: Arc<PP>,
}

impl<SR: SubscriptionRepository + Send + Sync, PP: PresencePublisher + Send + Sync>
    SubscriptionService<SR, PP>
{
    pub fn new(subscription_repo: Arc<SR>, presence_publisher: Arc<PP>) -> Self {
        Self {
            subscription_repo,
            presence_publisher,
        }
    }

    /// 订阅用户在线状态变化
    pub async fn subscribe_user_presence(&self, user_id: String) -> Result<Vec<UserPresenceEvent>> {
        // 检查用户是否存在
        if user_id.is_empty() {
            return Err(ErrorBuilder::new(ErrorCode::InvalidParameter, "user_id cannot be empty")
                .build_error()
                .into());
        }

        // 记录订阅
        self.subscription_repo
            .add_subscription(user_id.clone(), "presence".to_string())
            .await?;

        info!(user_id = %user_id, "Subscribed to user presence events");

        // 返回当前状态（如果有）
        Ok(Vec::new())
    }

    /// 订阅在线状态流
    pub async fn watch_presence(&self, request: WatchPresenceRequest) -> Result<Vec<PresenceEvent>> {
        let user_ids = &request.user_ids;

        if user_ids.is_empty() {
            return Err(ErrorBuilder::new(ErrorCode::InvalidParameter, "user_ids cannot be empty")
                .build_error()
                .into());
        }

        // 为每个用户添加订阅
        for user_id in user_ids {
            self.subscription_repo
                .add_subscription(user_id.clone(), "presence".to_string())
                .await?;
        }

        info!(user_ids = ?user_ids, "Subscribed to presence events");

        // 返回当前状态（如果有）
        Ok(Vec::new())
    }

    /// 发布在线状态事件
    pub async fn publish_presence_event(&self, event: PresenceEvent) -> Result<()> {
        self.presence_publisher
            .publish_presence_event(event)
            .await?;
        Ok(())
    }

    /// 发布用户状态事件
    pub async fn publish_user_presence_event(&self, event: UserPresenceEvent) -> Result<()> {
        self.presence_publisher
            .publish_user_presence_event(event)
            .await?;
        Ok(())
    }
}