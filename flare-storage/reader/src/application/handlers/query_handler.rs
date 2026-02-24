//! 查询处理器（查询侧）- 直接调用基础设施层，不经过领域服务
//!
//! 在 CQRS 架构中，查询侧通常直接调用基础设施层（仓储实现），
//! 因为查询是只读操作，不涉及业务逻辑，不需要经过领域层。

use anyhow::Result;
use chrono::{DateTime, Utc};
use flare_im_core::utils::extract_seq_from_message;
use flare_proto::common::Message;
use std::sync::Arc;
use tracing::instrument;

use crate::application::queries::{
    GetMessageQuery, ListMessageTagsQuery, QueryMessagesBySeqQuery, QueryMessagesQuery,
    SearchMessagesQuery, QueryMessageOperationsQuery,
};
use crate::domain::repository::MessageStorage;
use crate::domain::service::{MessageStorageDomainService, QueryMessagesResult};

/// 消息存储查询处理器（查询侧）
///
/// 对于基于 seq 的查询，需要使用领域服务（因为涉及业务逻辑）
pub struct MessageStorageQueryHandler {
    storage: Arc<dyn MessageStorage + Send + Sync>,
    domain_service: Option<Arc<MessageStorageDomainService>>,
}

impl MessageStorageQueryHandler {
    pub fn new(storage: Arc<dyn MessageStorage + Send + Sync>) -> Self {
        Self {
            storage,
            domain_service: None,
        }
    }

    pub fn with_domain_service(
        storage: Arc<dyn MessageStorage + Send + Sync>,
        domain_service: Arc<MessageStorageDomainService>,
    ) -> Self {
        Self {
            storage,
            domain_service: Some(domain_service),
        }
    }

    /// 查询消息列表
    #[instrument(skip(self), fields(conversation_id = %query.conversation_id))]
    pub async fn handle_query_messages(&self, query: QueryMessagesQuery) -> Result<Vec<Message>> {
        let start_time = if query.start_time == 0 {
            None
        } else {
            DateTime::from_timestamp(query.start_time, 0)
        };

        let end_time = if query.end_time == 0 {
            None
        } else {
            DateTime::from_timestamp(query.end_time, 0)
        };

        self.storage
            .query_messages(
                &query.conversation_id,
                None, // user_id
                start_time,
                end_time,
                query.limit,
            )
            .await
    }

    /// 查询消息列表（带分页结果）
    #[instrument(skip(self), fields(conversation_id = %query.conversation_id))]
    pub async fn handle_query_messages_with_pagination(
        &self,
        query: QueryMessagesQuery,
    ) -> Result<QueryMessagesResult> {
        // 如果有领域服务，使用领域服务处理分页逻辑
        if let Some(domain_service) = &self.domain_service {
            let start_time = if query.start_time == 0 {
                0
            } else {
                query.start_time
            };

            let end_time = if query.end_time == 0 {
                Utc::now().timestamp()
            } else {
                query.end_time
            };

            // 使用领域服务处理分页查询
            domain_service
                .query_messages(
                    &query.conversation_id,
                    start_time,
                    end_time,
                    query.limit,
                    query.cursor.as_deref(),
                )
                .await
        } else {
            // 如果没有领域服务，直接使用存储层（简化实现）
            let start_time = if query.start_time == 0 {
                None
            } else {
                DateTime::from_timestamp(query.start_time, 0)
            };

            let end_time = if query.end_time == 0 {
                None
            } else {
                DateTime::from_timestamp(query.end_time, 0)
            };

            // 直接查询消息
            let messages = self
                .storage
                .query_messages(
                    &query.conversation_id,
                    None, // user_id
                    start_time,
                    end_time,
                    query.limit,
                )
                .await?;

            // 构建简化的 QueryMessagesResult
            let message_count = messages.len() as i32;
            let next_cursor = messages
                .last()
                .and_then(|msg| {
                    msg.timestamp
                        .as_ref()
                        .map(|ts| format!("{}:{}", ts.seconds, msg.server_id.clone()))
                })
                .unwrap_or_default();
            let has_more = message_count >= query.limit;

            Ok(QueryMessagesResult {
                messages,
                next_cursor,
                has_more,
                total_size: message_count as i64,
            })
        }
    }

    /// 获取单条消息
    #[instrument(skip(self), fields(message_id = %query.message_id))]
    pub async fn handle_get_message(&self, query: GetMessageQuery) -> Result<Option<Message>> {
        self.storage.get_message(&query.message_id).await
    }

    /// 获取消息的时间戳
    #[instrument(skip(self), fields(message_id = %message_id))]
    pub async fn handle_get_message_timestamp(
        &self,
        message_id: &str,
    ) -> Result<Option<DateTime<Utc>>> {
        self.storage.get_message_timestamp(message_id).await
    }

    /// 搜索消息
    #[instrument(skip(self))]
    pub async fn handle_search_messages(&self, query: SearchMessagesQuery) -> Result<Vec<Message>> {
        let start_time = if query.start_time == 0 {
            None
        } else {
            DateTime::from_timestamp(query.start_time, 0)
        };

        let end_time = if query.end_time == 0 {
            None
        } else {
            DateTime::from_timestamp(query.end_time, 0)
        };

        self.storage
            .search_messages(&query.filters, start_time, end_time, query.limit)
            .await
    }

    /// 列出所有标签
    #[instrument(skip(self))]
    pub async fn handle_list_message_tags(
        &self,
        _query: ListMessageTagsQuery,
    ) -> Result<Vec<String>> {
        self.storage.list_all_tags().await
    }

    /// 基于 seq 查询消息列表
    #[instrument(skip(self), fields(conversation_id = %query.conversation_id, after_seq = query.after_seq, before_seq = ?query.before_seq))]
    pub async fn handle_query_messages_by_seq(
        &self,
        query: QueryMessagesBySeqQuery,
    ) -> Result<(Vec<Message>, Option<i64>)> {
        let messages = if let Some(domain_service) = &self.domain_service {
            // 使用领域服务（包含业务逻辑）
            domain_service
                .query_messages_by_seq(
                    &query.conversation_id,
                    query.user_id.as_deref(),
                    query.after_seq,
                    query.before_seq,
                    query.limit,
                )
                .await?
        } else {
            // 直接使用存储层（简化实现）
            let messages = self
                .storage
                .query_messages_by_seq(
                    &query.conversation_id,
                    query.user_id.as_deref(),
                    query.after_seq,
                    query.before_seq,
                    query.limit,
                )
                .await?;

            // 构建简化的 QueryMessagesResult
            QueryMessagesResult {
                messages,
                next_cursor: String::new(),
                has_more: false,
                total_size: 0,
            }
        };

        // 提取最后一条消息的 seq（使用工具函数）
        let last_seq = messages
            .messages
            .last()
            .and_then(|msg| extract_seq_from_message(msg));

        Ok((messages.messages, last_seq))
    }

    /// 查询消息操作历史
    #[instrument(skip(self), fields(message_id = %query.message_id))]
    pub async fn handle_query_message_operations(&self, query: QueryMessageOperationsQuery) -> Result<Vec<flare_proto::common::MessageOperation>> {
        self.storage
            .query_message_operations(&query.message_id)
            .await
    }
}
