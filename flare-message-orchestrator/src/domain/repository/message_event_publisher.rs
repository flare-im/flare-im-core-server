use async_trait::async_trait;
use crate::error::Result;
use crate::domain::event::{
    MessageDeletedEvent, MessageEditedEvent, MessageFavoritedEvent, MessagePinnedEvent,
    MessageReadEvent, MessageRecalledEvent, MessageReactionAddedEvent,
    MessageReactionRemovedEvent, MessageUnfavoritedEvent, MessageUnpinnedEvent,
};

/// 消息事件发布器接口
#[async_trait]
pub trait EventPublisher: Send + Sync {
    /// 发布消息撤回事件
    async fn publish_recalled(&self, event: &crate::domain::event::MessageRecalledEvent) -> Result<()>;

    /// 发布消息编辑事件
    async fn publish_edited(&self, event: &crate::domain::event::MessageEditedEvent) -> Result<()>;

    /// 发布消息删除事件
    async fn publish_deleted(&self, event: &crate::domain::event::MessageDeletedEvent) -> Result<()>;

    /// 发布消息已读事件
    async fn publish_read(&self, event: &crate::domain::event::MessageReadEvent) -> Result<()>;

    /// 发布消息反应添加事件
    async fn publish_reaction_added(&self, event: &crate::domain::event::MessageReactionAddedEvent) -> Result<()>;

    /// 发布消息反应移除事件
    async fn publish_reaction_removed(&self, event: &crate::domain::event::MessageReactionRemovedEvent) -> Result<()>;

    /// 发布消息置顶事件
    async fn publish_pinned(&self, event: &crate::domain::event::MessagePinnedEvent) -> Result<()>;

    /// 发布消息取消置顶事件
    async fn publish_unpinned(&self, event: &crate::domain::event::MessageUnpinnedEvent) -> Result<()>;

    /// 发布消息收藏事件
    async fn publish_favorited(&self, event: &crate::domain::event::MessageFavoritedEvent) -> Result<()>;

    /// 发布消息取消收藏事件
    async fn publish_unfavorited(&self, event: &crate::domain::event::MessageUnfavoritedEvent) -> Result<()>;
}

/// Kafka消息事件发布器实现
pub struct KafkaEventPublisher {
    kafka_publisher: std::sync::Arc<dyn crate::domain::repository::message_publisher::MessageEventPublisher>,
}

impl KafkaEventPublisher {
    pub fn new(kafka_publisher: std::sync::Arc<dyn crate::domain::repository::message_publisher::MessageEventPublisher>) -> Self {
        Self { kafka_publisher }
    }
}

#[async_trait]
impl EventPublisher for KafkaEventPublisher {
    async fn publish_recalled(&self, event: &crate::domain::event::MessageRecalledEvent) -> Result<()> {
        // 构建推送消息通知相关客户端
        use flare_proto::push::{PushMessageRequest, PushOptions};
        use flare_proto::common::Message as ProtoMessage;
        
        let mut proto_msg = ProtoMessage::default();
        proto_msg.server_id = event.base.message_id.clone();
        proto_msg.conversation_id = event.base.conversation_id.clone();
        proto_msg.sender_id = event.base.operator_id.clone(); // 撤回操作的发起者
        proto_msg.is_recalled = true;
        proto_msg.status = flare_proto::common::MessageStatus::Recalled as i32;
        
        // 获取会话参与者以通知相关客户端
        let push_req = PushMessageRequest {
            user_ids: vec![], // 会由 Push Worker 查询会话成员
            message: Some(proto_msg),
            options: Some(PushOptions {
                persist_if_offline: true, // 撤回消息需要持久化通知
                priority: 1, // 高优先级
                ..Default::default()
            }),
            template_id: String::new(),
            template_data: Default::default(),
        };

        self.kafka_publisher
            .publish_push(push_req)
            .await
            .map_err(|e| crate::error::FlareError::system(format!("Failed to publish recall push message: {}", e)))?;
        Ok(())
    }

    async fn publish_edited(&self, event: &crate::domain::event::MessageEditedEvent) -> Result<()> {
        // 构建推送消息通知相关相关客户端
        use flare_proto::push::{PushMessageRequest, PushOptions};
        use flare_proto::common::{Message as ProtoMessage, MessageStatus};
        use flare_proto::common::EditHistory as ProtoEditHistory;
        
        // 转换 edit_history
        let proto_edit_history: Vec<ProtoEditHistory> = event.edit_history.iter().map(|e| {
            // Decode content bytes to MessageContent for Proto
            let content = flare_proto::decode_message_content(&e.content_encoded).unwrap_or_default();
            ProtoEditHistory {
                edit_version: e.edit_version,
                content: Some(content),
                edited_at: Some(prost_types::Timestamp::from(std::time::SystemTime::from(e.edited_at))),
                editor_id: e.editor_id.clone(),
                reason: e.reason.clone().unwrap_or_default(),
                show_edited_mark: true,
            }
        }).collect();

        let mut proto_msg = ProtoMessage::default();
        proto_msg.server_id = event.base.message_id.clone();
        proto_msg.conversation_id = event.base.conversation_id.clone();
        proto_msg.sender_id = event.base.operator_id.clone(); // 编辑操作的发起者
        proto_msg.status = MessageStatus::Sent as i32;
        proto_msg.current_edit_version = event.edit_version;
        proto_msg.edit_history = proto_edit_history;
        proto_msg.last_edited_at = Some(prost_types::Timestamp::from(std::time::SystemTime::now()));

        // 获取会话参与者以通知相关客户端
        let push_req = PushMessageRequest {
            user_ids: vec![], // 会由 Push Worker 查询会话成员
            message: Some(proto_msg),
            options: Some(PushOptions {
                persist_if_offline: true, // 编辑消息需要持久化通知
                priority: 1, // 高优先级
                ..Default::default()
            }),
            template_id: String::new(),
            template_data: Default::default(),
        };

        self.kafka_publisher
            .publish_push(push_req)
            .await
            .map_err(|e| crate::error::FlareError::system(format!("Failed to publish edit push message: {}", e)))?;
        Ok(())
    }

    async fn publish_deleted(&self, event: &crate::domain::event::MessageDeletedEvent) -> Result<()> {
        // 构建推送消息通知相关客户端
        use flare_proto::push::{PushMessageRequest, PushOptions};
        use flare_proto::common::Message as ProtoMessage;
        
        let mut proto_msg = ProtoMessage::default();
        proto_msg.server_id = event.base.message_id.clone();
        proto_msg.conversation_id = event.base.conversation_id.clone();
        proto_msg.sender_id = event.base.operator_id.clone(); // 删除操作的发起者
        // 对于删除事件，使用 SENT 状态但标记为已删除
        proto_msg.status = flare_proto::common::MessageStatus::Sent as i32;
        // 添加删除信息到扩展字段
        proto_msg.extra.insert("deleted_at".to_string(), chrono::Utc::now().to_rfc3339());
        proto_msg.extra.insert("delete_type".to_string(), event.delete_type.clone());
        proto_msg.extra.insert("is_deleted".to_string(), "true".to_string());

        // 获取会话参与者以通知相关客户端
        let push_req = PushMessageRequest {
            user_ids: vec![], // 会由 Push Worker 查询会话成员
            message: Some(proto_msg),
            options: Some(PushOptions {
                persist_if_offline: true, // 删除消息需要持久化通知
                priority: 1, // 高优先级
                ..Default::default()
            }),
            template_id: String::new(),
            template_data: Default::default(),
        };

        self.kafka_publisher
            .publish_push(push_req)
            .await
            .map_err(|e| crate::error::FlareError::system(format!("Failed to publish delete push message: {}", e)))?;
        Ok(())
    }

    async fn publish_read(&self, _event: &crate::domain::event::MessageReadEvent) -> Result<()> {
        // 对于已读状态，我们通常不推送通知，而是通过 ACK 机制更新状态
        // 但在某些场景下可能需要通知发送方消息已被阅读
        
        // 暂时返回 OK，实际实现取决于业务需求
        Ok(())
    }

    async fn publish_reaction_added(&self, event: &crate::domain::event::MessageReactionAddedEvent) -> Result<()> {
        // 构建推送消息通知相关客户端
        use flare_proto::push::{PushMessageRequest, PushOptions};
        use flare_proto::common::Message as ProtoMessage;
        
        let mut proto_msg = ProtoMessage::default();
        proto_msg.server_id = event.base.message_id.clone();
        proto_msg.conversation_id = event.base.conversation_id.clone();
        proto_msg.sender_id = event.base.operator_id.clone(); // 反应添加者
        proto_msg.status = flare_proto::common::MessageStatus::Sent as i32;
        
        // 在 extra 中添加反应信息
        proto_msg.extra.insert("reaction_emoji".to_string(), event.emoji.clone());
        proto_msg.extra.insert("reaction_action".to_string(), "added".to_string());
        proto_msg.extra.insert("reaction_operator".to_string(), event.base.operator_id.clone());

        // 获取会话参与者以通知相关客户端
        let push_req = PushMessageRequest {
            user_ids: vec![], // 会由 Push Worker 查询会话成员
            message: Some(proto_msg),
            options: Some(PushOptions {
                persist_if_offline: false, // 反应通常是实时的，不需要持久化
                priority: 0, // 普通优先级
                ..Default::default()
            }),
            template_id: String::new(),
            template_data: Default::default(),
        };

        self.kafka_publisher
            .publish_push(push_req)
            .await
            .map_err(|e| crate::error::FlareError::system(format!("Failed to publish reaction added push message: {}", e)))?;
        Ok(())
    }

    async fn publish_reaction_removed(&self, event: &crate::domain::event::MessageReactionRemovedEvent) -> Result<()> {
        // 构建推送消息通知相关客户端
        use flare_proto::push::{PushMessageRequest, PushOptions};
        use flare_proto::common::Message as ProtoMessage;
        
        let mut proto_msg = ProtoMessage::default();
        proto_msg.server_id = event.base.message_id.clone();
        proto_msg.conversation_id = event.base.conversation_id.clone();
        proto_msg.sender_id = event.base.operator_id.clone(); // 反应移除者
        proto_msg.status = flare_proto::common::MessageStatus::Sent as i32;
        
        // 在 extra 中添加反应信息
        proto_msg.extra.insert("reaction_emoji".to_string(), event.emoji.clone());
        proto_msg.extra.insert("reaction_action".to_string(), "removed".to_string());
        proto_msg.extra.insert("reaction_operator".to_string(), event.base.operator_id.clone());

        // 获取会话参与者以通知相关客户端
        let push_req = PushMessageRequest {
            user_ids: vec![], // 会由 Push Worker 查询会话成员
            message: Some(proto_msg),
            options: Some(PushOptions {
                persist_if_offline: false, // 反应通常是实时的，不需要持久化
                priority: 0, // 普通优先级
                ..Default::default()
            }),
            template_id: String::new(),
            template_data: Default::default(),
        };

        self.kafka_publisher
            .publish_push(push_req)
            .await
            .map_err(|e| crate::error::FlareError::system(format!("Failed to publish reaction removed push message: {}", e)))?;
        Ok(())
    }

    async fn publish_pinned(&self, event: &crate::domain::event::MessagePinnedEvent) -> Result<()> {
        // 构建推送消息通知相关客户端
        use flare_proto::push::{PushMessageRequest, PushOptions};
        use flare_proto::common::Message as ProtoMessage;
        
        let mut proto_msg = ProtoMessage::default();
        proto_msg.server_id = event.base.message_id.clone();
        proto_msg.conversation_id = event.base.conversation_id.clone();
        proto_msg.sender_id = event.base.operator_id.clone(); // 置顶操作者
        proto_msg.status = flare_proto::common::MessageStatus::Sent as i32;
        
        // 在 extra 中添加置顶信息
        proto_msg.extra.insert("operation".to_string(), "pinned".to_string());
        proto_msg.extra.insert("pinned_by".to_string(), event.base.operator_id.clone());

        // 获取会话参与者以通知相关客户端
        let push_req = PushMessageRequest {
            user_ids: vec![], // 会由 Push Worker 查询会话成员
            message: Some(proto_msg),
            options: Some(PushOptions {
                persist_if_offline: false, // 置顶通常是实时的，不需要持久化
                priority: 1, // 较高优先级
                ..Default::default()
            }),
            template_id: String::new(),
            template_data: Default::default(),
        };

        self.kafka_publisher
            .publish_push(push_req)
            .await
            .map_err(|e| crate::error::FlareError::system(format!("Failed to publish pinned push message: {}", e)))?;
        Ok(())
    }

    async fn publish_unpinned(&self, event: &crate::domain::event::MessageUnpinnedEvent) -> Result<()> {
        // 构建推送消息通知相关客户端
        use flare_proto::push::{PushMessageRequest, PushOptions};
        use flare_proto::common::Message as ProtoMessage;
        
        let mut proto_msg = ProtoMessage::default();
        proto_msg.server_id = event.base.message_id.clone();
        proto_msg.conversation_id = event.base.conversation_id.clone();
        proto_msg.sender_id = event.base.operator_id.clone(); // 取消置顶操作者
        proto_msg.status = flare_proto::common::MessageStatus::Sent as i32;
        
        // 在 extra 中添加取消置顶信息
        proto_msg.extra.insert("operation".to_string(), "unpinned".to_string());
        proto_msg.extra.insert("unpinned_by".to_string(), event.base.operator_id.clone());

        // 获取会话参与者以通知相关客户端
        let push_req = PushMessageRequest {
            user_ids: vec![], // 会由 Push Worker 查询会话成员
            message: Some(proto_msg),
            options: Some(PushOptions {
                persist_if_offline: false, // 取消置顶通常是实时的，不需要持久化
                priority: 1, // 较高优先级
                ..Default::default()
            }),
            template_id: String::new(),
            template_data: Default::default(),
        };

        self.kafka_publisher
            .publish_push(push_req)
            .await
            .map_err(|e| crate::error::FlareError::system(format!("Failed to publish unpinned push message: {}", e)))?;
        Ok(())
    }

    async fn publish_favorited(&self, _event: &crate::domain::event::MessageFavoritedEvent) -> Result<()> {
        // 收藏操作的推送通知实现
        // 暂时返回 OK，实际实现取决于业务需求
        Ok(())
    }

    async fn publish_unfavorited(&self, _event: &crate::domain::event::MessageUnfavoritedEvent) -> Result<()> {
        // 取消收藏操作的推送通知实现
        // 暂时返回 OK，实际实现取决于业务需求
        Ok(())
    }
}

// 为 KafkaEventPublisher 实现来自 message_operation_service 的 EventPublisher trait
#[async_trait]
impl crate::domain::service::message_operation_service::EventPublisher for KafkaEventPublisher {
    async fn publish_recalled(&self, event: &crate::domain::event::MessageRecalledEvent) -> crate::error::Result<()> {
        // 直接转发到当前实现
        <Self as crate::domain::repository::message_event_publisher::EventPublisher>::publish_recalled(self, event).await
    }

    async fn publish_edited(&self, event: &crate::domain::event::MessageEditedEvent) -> crate::error::Result<()> {
        <Self as crate::domain::repository::message_event_publisher::EventPublisher>::publish_edited(self, event).await
    }

    async fn publish_deleted(&self, event: &crate::domain::event::MessageDeletedEvent) -> crate::error::Result<()> {
        <Self as crate::domain::repository::message_event_publisher::EventPublisher>::publish_deleted(self, event).await
    }

    async fn publish_read(&self, event: &crate::domain::event::MessageReadEvent) -> crate::error::Result<()> {
        <Self as crate::domain::repository::message_event_publisher::EventPublisher>::publish_read(self, event).await
    }

    async fn publish_reaction_added(&self, event: &crate::domain::event::MessageReactionAddedEvent) -> crate::error::Result<()> {
        <Self as crate::domain::repository::message_event_publisher::EventPublisher>::publish_reaction_added(self, event).await
    }

    async fn publish_reaction_removed(&self, event: &crate::domain::event::MessageReactionRemovedEvent) -> crate::error::Result<()> {
        <Self as crate::domain::repository::message_event_publisher::EventPublisher>::publish_reaction_removed(self, event).await
    }

    async fn publish_pinned(&self, event: &crate::domain::event::MessagePinnedEvent) -> crate::error::Result<()> {
        <Self as crate::domain::repository::message_event_publisher::EventPublisher>::publish_pinned(self, event).await
    }

    async fn publish_unpinned(&self, event: &crate::domain::event::MessageUnpinnedEvent) -> crate::error::Result<()> {
        <Self as crate::domain::repository::message_event_publisher::EventPublisher>::publish_unpinned(self, event).await
    }

    async fn publish_favorited(&self, event: &crate::domain::event::MessageFavoritedEvent) -> crate::error::Result<()> {
        <Self as crate::domain::repository::message_event_publisher::EventPublisher>::publish_favorited(self, event).await
    }

    async fn publish_unfavorited(&self, event: &crate::domain::event::MessageUnfavoritedEvent) -> crate::error::Result<()> {
        <Self as crate::domain::repository::message_event_publisher::EventPublisher>::publish_unfavorited(self, event).await
    }
}
