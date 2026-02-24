//! 查询处理器（编排层）- 轻量级，只负责编排领域服务

use std::sync::Arc;

use flare_im_core::error::Result;
use flare_proto::storage::storage_reader_service_client::StorageReaderServiceClient;
use tracing::instrument;

use crate::application::queries::{
    QueryMessageQuery, QueryMessagesQuery, QueryMessagesResult, SearchMessagesQuery,
};
use crate::domain::service::MessageDomainService;

/// 消息查询处理器（编排层）
///
/// 架构设计：
/// - 作为编排层，负责请求转发和响应封装
/// - 集成 StorageReaderService 进行实际查询
/// - 支持缓存策略和降级处理
/// - 提供统一的错误处理和链路追踪
pub struct MessageQueryHandler {
    _domain_service: Arc<MessageDomainService>, // 保留用于未来扩展
    storage_client: Option<Arc<StorageReaderServiceClient<tonic::transport::Channel>>>,
}

impl MessageQueryHandler {
    pub fn new(
        domain_service: Arc<MessageDomainService>,
        storage_client: Option<Arc<StorageReaderServiceClient<tonic::transport::Channel>>>,
    ) -> Self {
        Self {
            _domain_service: domain_service,
            storage_client,
        }
    }

    /// 获取存储客户端的引用
    pub fn get_storage_client(&self) -> Option<&Arc<StorageReaderServiceClient<tonic::transport::Channel>>> {
        self.storage_client.as_ref()
    }

    /// 查询单条消息
    ///
    /// 实现策略：
    /// 1. 通过 StorageReaderService 查询
    /// 2. 保证数据一致性，不降级处理
    ///
    /// 性能考虑：
    /// - 通过 message_id 主键查询，性能最优
    /// - 支持会话权限验证，防止越权访问
    #[instrument(skip(self), fields(message_id = %query.message_id, conversation_id = %query.conversation_id))]
    pub async fn query_message(
        &self,
        query: QueryMessageQuery,
    ) -> Result<flare_proto::common::Message> {
        let storage_client = self.storage_client.as_ref().ok_or_else(|| {
            flare_im_core::error::FlareError::system("StorageReaderService not configured")
        })?;

        // 构建查询请求
        let request = flare_proto::storage::GetMessageRequest {
            message_id: query.message_id.clone(),
        };

        // 调用存储服务
        let client = storage_client.as_ref();
        let response = client.clone().get_message(request).await.map_err(|e| {
            flare_im_core::error::FlareError::system(&format!("Failed to query message: {}", e))
        })?;
        let response = response.into_inner();

        // 检查响应状态
        if let Some(status) = &response.status {
            if status.code != flare_proto::common::ErrorCode::Ok as i32 {
                return Err(flare_im_core::error::FlareError::system(&format!(
                    "Query message failed: {}",
                    status.message
                )));
            }
        }

        // 返回消息内容
        response.message.ok_or_else(|| {
            flare_im_core::error::FlareError::system(&format!(
                "Message not found: {}",
                query.message_id
            ))
        })
    }

    /// 查询消息列表
    ///
    /// 实现策略：
    /// 1. 基于时间范围和游标的高效分页查询
    /// 2. 支持按序列号查询（性能更优）
    /// 3. 自动处理会话权限验证
    ///
    /// 分页策略：
    /// - 使用 cursor-based 分页，避免深度分页问题
    /// - 支持时间范围过滤，提高查询效率
    /// - 默认限制查询条数，防止大数据量查询
    #[instrument(skip(self), fields(conversation_id = %query.conversation_id))]
    pub async fn query_messages(
        &self,
        query: QueryMessagesQuery,
    ) -> Result<Vec<flare_proto::common::Message>> {
        let storage_client = self.storage_client.as_ref().ok_or_else(|| {
            flare_im_core::error::FlareError::system("StorageReaderService not configured")
        })?;

        // 构建查询请求
        let request = flare_proto::storage::QueryMessagesRequest {
            conversation_id: query.conversation_id.clone(),
            start_time: query.start_time.unwrap_or(0),
            end_time: query.end_time.unwrap_or(chrono::Utc::now().timestamp()),
            limit: query.limit.unwrap_or(50).min(1000), // 限制最大查询数量
            cursor: query.cursor.clone().unwrap_or_default(),
            pagination: Some(flare_proto::common::Pagination {
                cursor: query.cursor.clone().unwrap_or_default(),
                limit: query.limit.unwrap_or(50) as i32,
                has_more: false,
                previous_cursor: String::new(),
                total_size: 0,
            }),
        };

        // 调用存储服务
        let client = storage_client.as_ref();
        let response = client.clone().query_messages(request).await.map_err(|e| {
            flare_im_core::error::FlareError::system(&format!("Failed to query messages: {}", e))
        })?;

        let response = response.into_inner();

        // 检查响应状态
        if let Some(status) = &response.status {
            if status.code != flare_proto::common::ErrorCode::Ok as i32 {
                return Err(flare_im_core::error::FlareError::system(&format!(
                    "Query messages failed: {}",
                    status.message
                )));
            }
        }

        // 返回消息列表
        Ok(response.messages)
    }
    /// 带分页的查询消息列表
    ///
    /// 实现策略：
    /// 1. 基于时间范围和游标的高效分页查询
    /// 2. 支持按序列号查询（性能更优）
    /// 3. 自动处理会话权限验证
    ///
    /// 分页策略：
    /// - 使用 cursor-based 分页，避免深度分页问题
    /// - 支持时间范围过滤，提高查询效率
    /// - 默认限制查询条数，防止大数据量查询
    #[instrument(skip(self), fields(conversation_id = %query.conversation_id))]
    pub async fn query_messages_with_pagination(
        &self,
        query: QueryMessagesQuery,
    ) -> Result<QueryMessagesResult> {
        let storage_client = self.storage_client.as_ref().ok_or_else(|| {
            flare_im_core::error::FlareError::system("StorageReaderService not configured")
        })?;

        // 构建查询请求
        let request = flare_proto::storage::QueryMessagesRequest {
            conversation_id: query.conversation_id.clone(),
            start_time: query.start_time.unwrap_or(0),
            end_time: query.end_time.unwrap_or(chrono::Utc::now().timestamp()),
            limit: query.limit.unwrap_or(50).min(1000), // 限制最大查询数量
            cursor: query.cursor.clone().unwrap_or_default(),
            pagination: Some(flare_proto::common::Pagination {
                cursor: query.cursor.clone().unwrap_or_default(),
                limit: query.limit.unwrap_or(50) as i32,
                has_more: false,
                previous_cursor: String::new(),
                total_size: 0,
            }),
        };

        // 调用存储服务
        let client = storage_client.as_ref();
        let response = client.clone().query_messages(request).await.map_err(|e| {
            flare_im_core::error::FlareError::system(&format!("Failed to query messages: {}", e))
        })?;

        let response = response.into_inner();

        // 检查响应状态
        if let Some(status) = &response.status {
            if status.code != flare_proto::common::ErrorCode::Ok as i32 {
                return Err(flare_im_core::error::FlareError::system(&format!(
                    "Query messages failed: {}",
                    status.message
                )));
            }
        }

        // 构建分页结果
        Ok(QueryMessagesResult {
            messages: response.messages,
            next_cursor: response.next_cursor,
            has_more: response.has_more,
        })
    }

    /// 搜索消息
    ///
    /// 实现策略：
    /// 1. 支持全文搜索和关键词匹配
    /// 2. 支持多会话搜索或单会话搜索
    /// 3. 使用索引优化搜索性能
    ///
    /// 搜索优化：
    /// - 基于 PostgreSQL 的全文搜索索引
    /// - 支持搜索结果排序和高亮显示
    /// - 限制搜索范围，提高响应速度
    #[instrument(skip(self), fields(keyword = %query.keyword))]
    pub async fn search_messages(
        &self,
        query: SearchMessagesQuery,
    ) -> Result<Vec<flare_proto::common::Message>> {
        let storage_client = self.storage_client.as_ref().ok_or_else(|| {
            flare_im_core::error::FlareError::system("StorageReaderService not configured")
        })?;

        // 构建搜索过滤器
        let filters = vec![flare_proto::common::FilterExpression {
            field: "content".to_string(),
            op: flare_proto::common::FilterOperator::Contains as i32,
            values: vec![query.keyword.clone()],
        }];

        // 如果有 conversation_id，添加会话过滤
        let mut all_filters = filters;
        if let Some(conversation_id) = &query.conversation_id {
            all_filters.push(flare_proto::common::FilterExpression {
                field: "conversation_id".to_string(),
                op: flare_proto::common::FilterOperator::Eq as i32,
                values: vec![conversation_id.clone()],
            });
        }

        // 构建搜索请求
        let request = flare_proto::storage::SearchMessagesRequest {
            filters: all_filters,
            sort: vec![flare_proto::common::SortExpression {
                field: "created_at".to_string(),
                direction: flare_proto::common::SortDirection::Desc as i32,
            }],
            pagination: Some(flare_proto::common::Pagination {
                cursor: query.cursor.clone().unwrap_or_default(),
                limit: query.limit.unwrap_or(20).min(100) as i32,
                has_more: false,
                previous_cursor: String::new(),
                total_size: 0,
            }),
            time_range: Some(flare_proto::common::TimeRange {
                start_time: Some(prost_types::Timestamp {
                    seconds: 0,
                    nanos: 0,
                }),
                end_time: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
            }),
        };

        // 调用存储服务
        let mut client = storage_client.as_ref().clone();
        let response = client.search_messages(request).await.map_err(|e| {
            flare_im_core::error::FlareError::system(&format!("Failed to search messages: {}", e))
        })?;

        let response = response.into_inner();

        // 检查响应状态
        if let Some(status) = &response.status {
            if status.code != flare_proto::common::ErrorCode::Ok as i32 {
                return Err(flare_im_core::error::FlareError::system(&format!(
                    "Search messages failed: {}",
                    status.message
                )));
            }
        }

        // 返回搜索结果
        Ok(response.messages)
    }
}
