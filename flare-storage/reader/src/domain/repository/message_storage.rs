//! 消息存储仓储接口定义（Port）
//!
//! 负责消息的基本 CRUD 操作以及相关的查询功能

use crate::domain::model::MessageUpdate;
use anyhow::Result;
use chrono::{DateTime, Utc};
use flare_proto::common::{Message, VisibilityStatus};
use std::collections::HashMap;

// Rust 2024: trait 中直接使用 async fn
// 注意：对于 trait 对象（dyn Trait），仍需要使用 async_trait
#[async_trait::async_trait]
pub trait MessageStorage: Send + Sync {
    async fn store_message(&self, message: &Message, conversation_id: &str) -> Result<()>;

    async fn query_messages(
        &self,
        conversation_id: &str,
        user_id: Option<&str>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        limit: i32,
    ) -> Result<Vec<Message>>;

    /// 基于 seq 查询消息（推荐，性能更好）
    ///
    /// # 参数
    /// * `conversation_id` - 会话ID
    /// * `user_id` - 用户ID（可选，用于过滤已删除消息）
    /// * `after_seq` - 查询 seq > after_seq 的消息（用于增量同步）
    /// * `before_seq` - 查询 seq < before_seq 的消息（可选，用于分页）
    /// * `limit` - 返回消息数量限制
    ///
    /// # 返回
    /// * `Ok(Vec<Message>)` - 消息列表（按 seq 升序排序）
    async fn query_messages_by_seq(
        &self,
        conversation_id: &str,
        user_id: Option<&str>,
        after_seq: i64,
        before_seq: Option<i64>,
        limit: i32,
    ) -> Result<Vec<Message>>;

    async fn count_messages(
        &self,
        conversation_id: &str,
        user_id: Option<&str>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<i64>;

    async fn get_message(&self, message_id: &str) -> Result<Option<Message>>;

    /// 获取消息的时间戳
    ///
    /// 用于清除会话时根据消息ID确定清除时间点
    ///
    /// # 参数
    /// * `message_id` - 消息ID
    ///
    /// # 返回
    /// * `Ok(Option<DateTime<Utc>>)` - 消息的时间戳，如果消息不存在则返回None
    async fn get_message_timestamp(&self, message_id: &str) -> Result<Option<DateTime<Utc>>>;

    async fn update_message(&self, message_id: &str, updates: MessageUpdate) -> Result<()>;

    async fn batch_update_visibility(
        &self,
        message_ids: &[String],
        user_id: &str,
        visibility: VisibilityStatus,
    ) -> Result<usize>;

    async fn search_messages(
        &self,
        filters: &[flare_proto::common::FilterExpression],
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        limit: i32,
    ) -> Result<Vec<Message>>;

    async fn update_message_attributes(
        &self,
        message_id: &str,
        attributes: HashMap<String, String>,
        tags: Vec<String>,
    ) -> Result<()>;

    async fn list_all_tags(&self) -> Result<Vec<String>>;

    /// 查询消息操作历史
    async fn query_message_operations(
        &self,
        message_id: &str,
    ) -> Result<Vec<flare_proto::common::MessageOperation>>;

    /// 查询消息编辑历史
    async fn query_message_edit_history(
        &self,
        message_id: &str,
    ) -> Result<Vec<flare_proto::common::EditHistory>>;

    /// 查询消息已读记录
    async fn query_message_read_records(
        &self,
        message_id: &str,
    ) -> Result<Vec<flare_proto::common::MessageReadRecord>>;

    /// 查询消息可见性状态
    async fn query_message_visibility(
        &self,
        message_id: &str,
        user_id: &str,
    ) -> Result<Option<flare_proto::common::VisibilityStatus>>;

    /// 查询消息反应
    async fn query_message_reactions(
        &self,
        message_id: &str,
    ) -> Result<Vec<flare_proto::common::Reaction>>;

    /// 查询置顶消息
    async fn query_pinned_messages(
        &self,
        conversation_id: &str,
    ) -> Result<Vec<flare_proto::common::PinnedMessageInfo>>;
}