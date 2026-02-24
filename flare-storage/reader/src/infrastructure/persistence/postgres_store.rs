//! PostgreSQL 读侧存储实现
//!
//! 基于 TimescaleDB/PostgreSQL 实现消息的查询、更新、搜索等功能
//! 与 Storage Writer 共享相同的数据库表结构
//!
//! 此模块整合了所有 PostgreSQL 存储相关的实现，包括：
//! - 消息存储 (MessageStorage)
//! - 可见性存储 (VisibilityStorage)
//! - 基础 PostgreSQL 功能 (PostgresBaseStorage)

use anyhow::Result;
use chrono;

use crate::config::StorageReaderConfig;
use crate::domain::repository::{MessageStorage, VisibilityStorage};
use crate::infrastructure::persistence::postgres_base::PostgresBaseStorage;
use crate::infrastructure::persistence::message_storage_impl::{PostgresMessageStorageImpl};
use crate::infrastructure::persistence::visibility_storage_impl::{PostgresVisibilityStorageImpl};

/// PostgreSQL 消息存储组合实现
/// 
/// 这是一个组合结构，包含所有必要的存储实现
pub struct PostgresMessageStorage {
    pub message_storage: PostgresMessageStorageImpl,
    pub visibility_storage: PostgresVisibilityStorageImpl,
}

impl PostgresMessageStorage {
    /// 创建新的 PostgreSQL 存储实例
    /// 
    /// # Arguments
    /// * `config` - 存储读取器配置
    /// 
    /// # Returns
    /// * `Some(PostgresMessageStorage)` - 成功创建的存储实例
    /// * `None` - 未配置 PostgreSQL 连接
    pub async fn new(config: &StorageReaderConfig) -> Result<Option<Self>> {
        let base_storage = match PostgresBaseStorage::new(config).await? {
            Some(base) => base,
            None => return Ok(None),
        };

        // 为消息存储创建 base 副本
        let base_for_message = PostgresBaseStorage::new(config).await?.unwrap();
        let message_storage = PostgresMessageStorageImpl::new(base_for_message);
        
        // 为可见性存储创建 base 副本
        let base_for_visibility = PostgresBaseStorage::new(config).await?.unwrap();
        let visibility_storage = PostgresVisibilityStorageImpl::new(base_for_visibility);

        Ok(Some(Self {
            message_storage,
            visibility_storage,
        }))
    }

    /// 健康检查：验证数据库连接和基本查询
    pub async fn health_check(&self) -> Result<()> {
        self.message_storage.base.health_check().await
    }
}

// 实现 MessageStorage trait
#[async_trait::async_trait]
impl MessageStorage for PostgresMessageStorage {
    async fn store_message(&self, message: &flare_proto::common::Message, conversation_id: &str) -> Result<()> {
        self.message_storage.store_message(message, conversation_id).await
    }

    async fn query_messages(
        &self,
        conversation_id: &str,
        user_id: Option<&str>,
        start_time: Option<chrono::DateTime<chrono::Utc>>,
        end_time: Option<chrono::DateTime<chrono::Utc>>,
        limit: i32,
    ) -> Result<Vec<flare_proto::common::Message>> {
        self.message_storage.query_messages(conversation_id, user_id, start_time, end_time, limit).await
    }

    async fn query_messages_by_seq(
        &self,
        conversation_id: &str,
        user_id: Option<&str>,
        after_seq: i64,
        before_seq: Option<i64>,
        limit: i32,
    ) -> Result<Vec<flare_proto::common::Message>> {
        self.message_storage.query_messages_by_seq(conversation_id, user_id, after_seq, before_seq, limit).await
    }

    async fn get_message(&self, message_id: &str) -> Result<Option<flare_proto::common::Message>> {
        self.message_storage.get_message(message_id).await
    }

    async fn get_message_timestamp(&self, message_id: &str) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
        self.message_storage.get_message_timestamp(message_id).await
    }

    async fn update_message(&self, message_id: &str, updates: crate::domain::model::MessageUpdate) -> Result<()> {
        self.message_storage.update_message(message_id, updates).await
    }

    async fn batch_update_visibility(
        &self,
        message_ids: &[String],
        user_id: &str,
        visibility: flare_proto::common::VisibilityStatus,
    ) -> Result<usize> {
        self.message_storage.batch_update_visibility(message_ids, user_id, visibility).await
    }

    async fn count_messages(
        &self,
        conversation_id: &str,
        user_id: Option<&str>,
        start_time: Option<chrono::DateTime<chrono::Utc>>,
        end_time: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<i64> {
        self.message_storage.count_messages(conversation_id, user_id, start_time, end_time).await
    }

    async fn search_messages(
        &self,
        filters: &[flare_proto::common::FilterExpression],
        start_time: Option<chrono::DateTime<chrono::Utc>>,
        end_time: Option<chrono::DateTime<chrono::Utc>>,
        limit: i32,
    ) -> Result<Vec<flare_proto::common::Message>> {
        self.message_storage.search_messages(filters, start_time, end_time, limit).await
    }

    async fn update_message_attributes(
        &self,
        message_id: &str,
        attributes: std::collections::HashMap<String, String>,
        tags: Vec<String>,
    ) -> Result<()> {
        self.message_storage.update_message_attributes(message_id, attributes, tags).await
    }

    async fn list_all_tags(&self) -> Result<Vec<String>> {
        self.message_storage.list_all_tags().await
    }

    async fn query_message_operations(
        &self,
        message_id: &str,
    ) -> Result<Vec<flare_proto::common::MessageOperation>> {
        self.message_storage.query_message_operations(message_id).await
    }

    async fn query_message_edit_history(
        &self,
        message_id: &str,
    ) -> Result<Vec<flare_proto::common::EditHistory>> {
        self.message_storage.query_message_edit_history(message_id).await
    }

    async fn query_message_read_records(
        &self,
        message_id: &str,
    ) -> Result<Vec<flare_proto::common::MessageReadRecord>> {
        self.message_storage.query_message_read_records(message_id).await
    }

    async fn query_message_visibility(
        &self,
        message_id: &str,
        user_id: &str,
    ) -> Result<Option<flare_proto::common::VisibilityStatus>> {
        self.message_storage.query_message_visibility(message_id, user_id).await
    }

    async fn query_message_reactions(
        &self,
        message_id: &str,
    ) -> Result<Vec<flare_proto::common::Reaction>> {
        self.message_storage.query_message_reactions(message_id).await
    }

    async fn query_pinned_messages(
        &self,
        conversation_id: &str,
    ) -> Result<Vec<flare_proto::common::PinnedMessageInfo>> {
        self.message_storage.query_pinned_messages(conversation_id).await
    }
}

// 实现 VisibilityStorage trait
#[async_trait::async_trait]
impl VisibilityStorage for PostgresMessageStorage {
    async fn set_visibility(
        &self,
        message_id: &str,
        user_id: &str,
        conversation_id: &str,
        visibility: flare_proto::common::VisibilityStatus,
    ) -> Result<()> {
        self.visibility_storage.set_visibility(message_id, user_id, conversation_id, visibility).await
    }

    async fn get_visibility(
        &self,
        message_id: &str,
        user_id: &str,
    ) -> Result<Option<flare_proto::common::VisibilityStatus>> {
        self.visibility_storage.get_visibility(message_id, user_id).await
    }

    async fn batch_set_visibility(
        &self,
        message_ids: &[String],
        user_id: &str,
        conversation_id: &str,
        visibility: flare_proto::common::VisibilityStatus,
    ) -> Result<usize> {
        self.visibility_storage.batch_set_visibility(message_ids, user_id, conversation_id, visibility).await
    }

    async fn query_visible_message_ids(
        &self,
        user_id: &str,
        conversation_id: &str,
        visibility_status: flare_proto::common::VisibilityStatus,
    ) -> Result<Vec<String>> {
        self.visibility_storage.query_visible_message_ids(user_id, conversation_id, visibility_status).await
    }
}