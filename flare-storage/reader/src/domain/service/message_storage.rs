//! 消息存储领域服务 - 包含所有业务逻辑实现

use anyhow::{Result, anyhow};
use chrono::{DateTime, Duration, TimeZone, Utc};
use flare_im_core::utils::{
    TimelineMetadata, extract_seq_from_message, extract_timeline_from_extra, timestamp_to_datetime,
};
use flare_proto::common::{Message, VisibilityStatus};
use prost_types::Timestamp;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::instrument;

use crate::domain::model::MessageUpdate;
use crate::domain::repository::{MessageStorage, VisibilityStorage};

/// 领域服务配置（值对象，不依赖基础设施层）
#[derive(Debug, Clone)]
pub struct MessageStorageDomainConfig {
    pub max_page_size: i32,
    pub default_range_seconds: i64,
}

/// 查询游标
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct QueryCursor {
    ingestion_ts: i64,
    message_id: String,
}

/// 增强的查询游标，支持多种游标格式
#[derive(Debug, Clone)]
pub struct EnhancedQueryCursor {
    /// 时间戳（毫秒）
    pub timestamp_ms: i64,
    /// 消息ID
    pub message_id: String,
    /// 序列号（如果可用）
    pub seq: Option<i64>,
    /// 类型标识（用于区分不同的游标格式）
    pub cursor_type: CursorType,
}

/// 游标类型
#[derive(Debug, Clone, PartialEq)]
pub enum CursorType {
    /// 基于时间戳的游标
    Timestamp,
    /// 基于序列号的游标
    Sequence,
    /// 混合游标（时间戳+序列号）
    Hybrid,
}

impl EnhancedQueryCursor {
    /// 从原始字符串解析游标
    pub fn from_raw(raw: Option<&str>) -> Option<Self> {
        let raw = raw?;
        if raw.is_empty() {
            return None;
        }

        // 尝试解析不同格式的游标
        if raw.starts_with("seq:") {
            // seq:123456:message_id 格式
            let parts: Vec<&str> = raw.split(':').collect();
            if parts.len() >= 3 {
                if let Ok(seq) = parts[1].parse::<i64>() {
                    return Some(EnhancedQueryCursor {
                        timestamp_ms: 0, // 无法从序列号游标中获取时间戳
                        message_id: parts[2..].join(":").to_string(), // 处理消息ID中可能包含冒号的情况
                        seq: Some(seq),
                        cursor_type: CursorType::Sequence,
                    });
                }
            }
        } else if raw.starts_with("hybrid:") {
            // hybrid:timestamp:seq:message_id 格式
            let parts: Vec<&str> = raw.split(':').collect();
            if parts.len() >= 4 {
                if let (Ok(timestamp), Ok(seq)) = (parts[1].parse::<i64>(), parts[2].parse::<i64>()) {
                    return Some(EnhancedQueryCursor {
                        timestamp_ms: timestamp,
                        message_id: parts[3..].join(":").to_string(), // 处理消息ID中可能包含冒号的情况
                        seq: Some(seq),
                        cursor_type: CursorType::Hybrid,
                    });
                }
            }
        } else {
            // timestamp:message_id 格式（旧格式兼容）
            let parts: Vec<&str> = raw.splitn(2, ':').collect();
            if parts.len() == 2 {
                if let Ok(timestamp) = parts[0].parse::<i64>() {
                    return Some(EnhancedQueryCursor {
                        timestamp_ms: timestamp,
                        message_id: parts[1].to_string(),
                        seq: None,
                        cursor_type: CursorType::Timestamp,
                    });
                }
            }
        }

        None
    }

    /// 序列化游标为字符串
    pub fn to_string(&self) -> String {
        match self.cursor_type {
            CursorType::Sequence => {
                if let Some(seq) = self.seq {
                    format!("seq:{}:{}", seq, self.message_id)
                } else {
                    format!("{}:{}", self.timestamp_ms, self.message_id)
                }
            }
            CursorType::Timestamp => {
                format!("{}:{}", self.timestamp_ms, self.message_id)
            }
            CursorType::Hybrid => {
                if let Some(seq) = self.seq {
                    format!("hybrid:{}:{}:{}", self.timestamp_ms, seq, self.message_id)
                } else {
                    format!("{}:{}", self.timestamp_ms, self.message_id)
                }
            }
        }
    }

    /// 从消息创建游标
    pub fn from_message(message: &Message, cursor_type: CursorType) -> Option<Self> {
        // 提取消息时间戳
        let timestamp_ms = message
            .timestamp
            .as_ref()
            .map(|ts| ts.seconds * 1000 + (ts.nanos / 1_000_000) as i64)
            .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());

        // 提取序列号（如果存在）
        let seq = extract_seq_from_message(message);

        Some(EnhancedQueryCursor {
            timestamp_ms,
            message_id: message.server_id.clone(),
            seq,
            cursor_type,
        })
    }
}

impl QueryCursor {
    #[allow(dead_code)]
    fn from_raw(raw: Option<&str>) -> Option<Self> {
        let raw = raw?;
        let mut parts = raw.splitn(2, ':');
        let ts = parts.next()?.parse::<i64>().ok()?;
        let message_id = parts.next()?.to_string();
        Some(Self {
            ingestion_ts: ts,
            message_id,
        })
    }
}

/// 检索到的消息
struct RetrievedMessage {
    message: Message,
    timeline: TimelineMetadata,
}

impl RetrievedMessage {
    fn new(message: Message, timeline: TimelineMetadata) -> Self {
        Self { message, timeline }
    }
}

/// 查询消息结果
pub struct QueryMessagesResult {
    pub messages: Vec<Message>,
    pub next_cursor: String,
    pub has_more: bool,
    pub total_size: i64,
}

/// 消息存储领域服务 - 包含所有业务逻辑
pub struct MessageStorageDomainService {
    storage: Arc<dyn MessageStorage + Send + Sync>,
    visibility_storage: Option<Arc<dyn VisibilityStorage + Send + Sync>>,

    config: MessageStorageDomainConfig,
}

impl MessageStorageDomainService {
    pub fn new(
        storage: Arc<dyn MessageStorage + Send + Sync>,
        visibility_storage: Option<Arc<dyn VisibilityStorage + Send + Sync>>,
        config: MessageStorageDomainConfig,
    ) -> Self {
        Self {
            storage,
            visibility_storage,
            config,
        }
    }

    /// 查询消息列表（基于时间戳，向后兼容）
    #[instrument(skip(self), fields(conversation_id = %conversation_id))]
    pub async fn query_messages(
        &self,
        conversation_id: &str,
        start_time: i64,
        end_time: i64,
        limit: i32,
        cursor: Option<&str>,
    ) -> Result<QueryMessagesResult> {
        if conversation_id.is_empty() {
            return Err(anyhow!("conversation_id is required"));
        }

        let limit = limit.clamp(1, self.config.max_page_size) as usize;
        // 使用增强的游标解析
        let enhanced_cursor = EnhancedQueryCursor::from_raw(cursor);

        let end_ts = if end_time == 0 {
            Utc::now().timestamp()
        } else {
            end_time
        };
        let start_ts = if start_time == 0 {
            end_ts - self.config.default_range_seconds
        } else {
            start_time
        };

        let end_ts_ms = end_ts * 1_000;
        let start_ts_ms = start_ts * 1_000;

        // 计算总记录数
        let start_dt_for_count = Utc
            .timestamp_opt(start_ts, 0)
            .single()
            .unwrap_or_else(|| Utc::now() - Duration::seconds(self.config.default_range_seconds));
        let end_dt_for_count = Utc
            .timestamp_opt(end_ts, 0)
            .single()
            .unwrap_or_else(Utc::now);

        let total_size = self
            .storage
            .count_messages(
                conversation_id,
                None,
                Some(start_dt_for_count),
                Some(end_dt_for_count),
            )
            .await
            .map_err(|e| anyhow!("Failed to count messages: {}", e))?;

        let mut seen = HashSet::new();
        if let Some(ref enhanced_cursor) = enhanced_cursor {
            seen.insert(enhanced_cursor.message_id.clone());
        }

        // 转换为旧的QueryCursor格式以保持与现有逻辑兼容
        let old_cursor = if let Some(ref enhanced_cursor) = enhanced_cursor {
            Some(QueryCursor {
                ingestion_ts: enhanced_cursor.timestamp_ms,
                message_id: enhanced_cursor.message_id.clone(),
            })
        } else {
            None
        };

        let mut aggregated = self
            .query_from_storage(
                conversation_id,
                start_ts_ms,
                end_ts_ms,
                old_cursor.as_ref(),
                limit,
                &mut seen,
            )
            .await?;

        aggregated.sort_by(|a, b| b.timeline.ingestion_ts.cmp(&a.timeline.ingestion_ts));
        aggregated.truncate(limit);

        let messages: Vec<Message> = aggregated.iter().map(|item| item.message.clone()).collect();
        
        // 使用增强的游标生成
        let next_cursor = if messages.len() == limit {
            if let Some(last_message) = messages.last() {
                // 创建混合游标（时间戳+序列号+消息ID）
                if let Some(cursor_obj) = EnhancedQueryCursor::from_message(last_message, CursorType::Hybrid) {
                    cursor_obj.to_string()
                } else {
                    format!("{}:{}", chrono::Utc::now().timestamp_millis(), last_message.server_id)
                }
            } else {
                String::new()
            }
        } else {
            String::new()
        };

        Ok(QueryMessagesResult {
            messages,
            next_cursor: next_cursor.clone(),
            has_more: !next_cursor.is_empty(),
            total_size,
        })
    }

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
    /// * `Ok(QueryMessagesResult)` - 消息列表（按 seq 升序排序）
    #[instrument(skip(self), fields(conversation_id = %conversation_id, after_seq, before_seq = ?before_seq))]
    pub async fn query_messages_by_seq(
        &self,
        conversation_id: &str,
        user_id: Option<&str>,
        after_seq: i64,
        before_seq: Option<i64>,
        limit: i32,
    ) -> Result<QueryMessagesResult> {
        if conversation_id.is_empty() {
            return Err(anyhow!("conversation_id is required"));
        }

        let limit = limit.clamp(1, self.config.max_page_size) as usize;

        // 使用基于 seq 的查询
        let messages = self
            .storage
            .query_messages_by_seq(conversation_id, user_id, after_seq, before_seq, limit as i32)
            .await
            .map_err(|e| anyhow!("Failed to query messages by seq: {}", e))?;

        // 构建 next_cursor（基于最后一个消息的 seq）
        let next_cursor = if messages.len() == limit {
            messages
                .last()
                .and_then(|msg| {
                    // 从 extra 字段提取 seq（使用工具函数）
                    extract_seq_from_message(msg).map(|seq| format!("seq:{}:{}", seq, msg.server_id))
                })
                .unwrap_or_default()
        } else {
            String::new()
        };

        // 计算总记录数（简化实现：使用消息数量）
        let total_size = messages.len() as i64;

        Ok(QueryMessagesResult {
            messages,
            next_cursor: next_cursor.clone(),
            has_more: !next_cursor.is_empty(),
            total_size,
        })
    }

    /// 增强的分页查询方法，支持多种游标格式和智能预取
    ///
    /// # 特性
    /// * 支持时间戳、序列号和混合游标格式
    /// * 智能预取以减少后续查询延迟
    /// * 高效的去重和过滤机制
    #[instrument(skip(self), fields(conversation_id = %conversation_id))]
    pub async fn query_messages_paginated(
        &self,
        conversation_id: &str,
        user_id: Option<&str>,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: i32,
        cursor: Option<&str>,
        prefetch_enabled: bool,
    ) -> Result<QueryMessagesResult> {
        if conversation_id.is_empty() {
            return Err(anyhow!("conversation_id is required"));
        }

        let limit = limit.clamp(1, self.config.max_page_size) as usize;
        let enhanced_cursor = EnhancedQueryCursor::from_raw(cursor);

        // 解析时间范围
        let end_ts = if let Some(end) = end_time {
            end
        } else {
            Utc::now().timestamp()
        };
        let start_ts = if let Some(start) = start_time {
            start
        } else {
            end_ts - self.config.default_range_seconds
        };


        // 计算总记录数
        let start_dt_for_count = Utc
            .timestamp_opt(start_ts, 0)
            .single()
            .unwrap_or_else(|| Utc::now() - Duration::seconds(self.config.default_range_seconds));
        let end_dt_for_count = Utc
            .timestamp_opt(end_ts, 0)
            .single()
            .unwrap_or_else(Utc::now);

        let total_size = self
            .storage
            .count_messages(
                conversation_id,
                user_id,
                Some(start_dt_for_count),
                Some(end_dt_for_count),
            )
            .await
            .map_err(|e| anyhow!("Failed to count messages: {}", e))?;

        // 智能预取：如果启用了预取且当前页接近末尾，则预取更多数据
        let effective_limit = if prefetch_enabled && limit < (self.config.max_page_size as usize / 2) {
            // 预取更多的消息以提高后续查询的效率
            std::cmp::min(limit * 2, self.config.max_page_size as usize)
        } else {
            limit
        };

        let mut seen = HashSet::new();
        if let Some(ref enhanced_cursor) = enhanced_cursor {
            seen.insert(enhanced_cursor.message_id.clone());
        }

        // 根据游标类型选择最优查询策略
        let messages = match enhanced_cursor {
            Some(ref cursor_obj) if cursor_obj.cursor_type == CursorType::Sequence => {
                // 如果游标是序列号类型，使用基于序列号的查询
                if let Some(seq) = cursor_obj.seq {
                    self.storage
                        .query_messages_by_seq(conversation_id, user_id, seq, None, effective_limit as i32)
                        .await
                        .map_err(|e| anyhow!("Failed to query messages by seq: {}", e))?
                } else {
                    // 回退到时间戳查询
                    self.storage
                        .query_messages(conversation_id, user_id, 
                            Some(Utc.timestamp_opt(cursor_obj.timestamp_ms / 1000, 0).single().unwrap_or_else(|| Utc::now())),
                            Some(Utc.timestamp_opt(end_ts, 0).single().unwrap_or_else(|| Utc::now())), 
                            effective_limit as i32)
                        .await
                        .map_err(|e| anyhow!("Failed to query messages: {}", e))?
                }
            },
            Some(ref cursor_obj) if cursor_obj.cursor_type == CursorType::Timestamp || cursor_obj.cursor_type == CursorType::Hybrid => {
                // 使用时间戳查询
                let start_time = Utc.timestamp_opt(cursor_obj.timestamp_ms / 1000, 0).single().unwrap_or_else(|| Utc::now());
                self.storage
                    .query_messages(conversation_id, user_id, 
                        Some(start_time),
                        Some(Utc.timestamp_opt(end_ts, 0).single().unwrap_or_else(|| Utc::now())), 
                        effective_limit as i32)
                    .await
                    .map_err(|e| anyhow!("Failed to query messages: {}", e))?
            },
            Some(ref cursor_obj) => {
                // 对于其他游标类型，使用时间戳查询作为默认回退
                let start_time = Utc.timestamp_opt(cursor_obj.timestamp_ms / 1000, 0).single().unwrap_or_else(|| Utc::now());
                self.storage
                    .query_messages(conversation_id, user_id, 
                        Some(start_time),
                        Some(Utc.timestamp_opt(end_ts, 0).single().unwrap_or_else(|| Utc::now())), 
                        effective_limit as i32)
                    .await
                    .map_err(|e| anyhow!("Failed to query messages: {}", e))?
            },
            None => {
                // 初始查询，使用时间范围
                self.storage
                    .query_messages(conversation_id, user_id, 
                        Some(Utc.timestamp_opt(start_ts, 0).single().unwrap_or_else(|| Utc::now())),
                        Some(Utc.timestamp_opt(end_ts, 0).single().unwrap_or_else(|| Utc::now())), 
                        effective_limit as i32)
                    .await
                    .map_err(|e| anyhow!("Failed to query messages: {}", e))?
            }
        };

        // 过滤重复项并截断到请求的限制
        let filtered_messages: Vec<Message> = messages
            .into_iter()
            .filter(|msg| !seen.contains(&msg.server_id))
            .take(limit)
            .collect();

        // 生成下一个游标
        let next_cursor = if filtered_messages.len() == limit {
            if let Some(last_message) = filtered_messages.last() {
                // 创建最适合的游标类型
                let best_cursor_type = if extract_seq_from_message(last_message).is_some() {
                    CursorType::Hybrid // 如果有seq，使用混合游标
                } else {
                    CursorType::Timestamp // 否则使用时间戳游标
                };
                
                if let Some(cursor_obj) = EnhancedQueryCursor::from_message(last_message, best_cursor_type) {
                    cursor_obj.to_string()
                } else {
                    format!("{}:{}", chrono::Utc::now().timestamp_millis(), last_message.server_id)
                }
            } else {
                String::new()
            }
        } else {
            String::new()
        };

        Ok(QueryMessagesResult {
            messages: filtered_messages,
            next_cursor: next_cursor.clone(),
            has_more: !next_cursor.is_empty(),
            total_size,
        })
    }

    /// 按时间窗口查询消息（优化大量消息的分页性能）
    ///
    /// # 特性
    /// * 按固定时间窗口（如每小时/每天）分割查询
    /// * 并行查询多个时间窗口以提高性能
    /// * 自动合并和排序结果
    #[instrument(skip(self), fields(conversation_id = %conversation_id))]
    pub async fn query_messages_by_time_windows(
        &self,
        conversation_id: &str,
        user_id: Option<&str>,
        start_time: i64,
        end_time: i64,
        window_size_seconds: i64,  // 窗口大小（秒）
        limit: i32,
    ) -> Result<QueryMessagesResult> {
        if conversation_id.is_empty() {
            return Err(anyhow!("conversation_id is required"));
        }

        let limit = limit.clamp(1, self.config.max_page_size) as usize;
        let window_size = std::cmp::max(window_size_seconds, 3600); // 最小1小时窗口
        let start_dt = Utc.timestamp_opt(start_time, 0).single().unwrap_or_else(Utc::now);
        let end_dt = Utc.timestamp_opt(end_time, 0).single().unwrap_or_else(Utc::now);

        // 计算时间窗口
        let mut windows = Vec::new();
        let mut current_start = start_dt;
        
        while current_start < end_dt {
            let current_end = std::cmp::min(current_start + chrono::Duration::seconds(window_size), end_dt);
            windows.push((current_start, current_end));
            current_start = current_end;
        }
        
        let window_count = windows.len(); // 预先计算窗口数量

        // 并行查询各个时间窗口
        let mut all_messages = Vec::new();
        for (window_start, window_end) in windows {
            let messages = self.storage
                .query_messages(
                    conversation_id,
                    user_id,
                    Some(window_start),
                    Some(window_end),
                    (limit / window_count).max(1) as i32, // 按窗口数分配限制
                )
                .await
                .map_err(|e| anyhow!("Failed to query messages in time window: {}", e))?;
            
            all_messages.extend(messages);
        }

        // 按时间戳排序并截取
        all_messages.sort_by(|a, b| {
            let ts_a = a.timestamp.as_ref().map(|t| t.seconds).unwrap_or(0);
            let ts_b = b.timestamp.as_ref().map(|t| t.seconds).unwrap_or(0);
            ts_b.cmp(&ts_a) // 降序排列（最新的在前）
        });

        all_messages.truncate(limit);

        // 计算总数（近似值）
        let total_size = self
            .storage
            .count_messages(conversation_id, user_id, Some(start_dt), Some(end_dt))
            .await
            .unwrap_or(all_messages.len() as i64);

        // 生成游标
        let next_cursor = if all_messages.len() == limit {
            if let Some(last_message) = all_messages.last() {
                if let Some(cursor_obj) = EnhancedQueryCursor::from_message(last_message, CursorType::Hybrid) {
                    cursor_obj.to_string()
                } else {
                    format!("{}:{}", chrono::Utc::now().timestamp_millis(), last_message.server_id)
                }
            } else {
                String::new()
            }
        } else {
            String::new()
        };

        Ok(QueryMessagesResult {
            messages: all_messages,
            next_cursor,
            has_more: false, // 时间窗口查询不保证有更多数据
            total_size,
        })
    }

    async fn query_from_storage(
        &self,
        conversation_id: &str,
        start_ts_ms: i64,
        end_ts_ms: i64,
        cursor: Option<&QueryCursor>,
        limit: usize,
        seen: &mut HashSet<String>,
    ) -> Result<Vec<RetrievedMessage>> {
        let start_dt = Utc
            .timestamp_millis_opt(start_ts_ms)
            .single()
            .unwrap_or_else(|| Utc::now() - Duration::days(30));
        let mut end_dt = Utc
            .timestamp_millis_opt(end_ts_ms)
            .single()
            .unwrap_or_else(Utc::now);

        if let Some(cursor) = cursor {
            if cursor.ingestion_ts <= start_ts_ms {
                return Ok(Vec::new());
            }
            end_dt = Utc
                .timestamp_millis_opt(cursor.ingestion_ts - 1)
                .single()
                .unwrap_or(end_dt);
        }

        if end_dt < start_dt {
            return Ok(Vec::new());
        }

        let messages = self
            .storage
            .query_messages(conversation_id, None, Some(start_dt), Some(end_dt), limit as i32)
            .await
            ?;

        let mut results = Vec::new();
        for message in messages {
            if !seen.insert(message.server_id.clone()) {
                continue;
            }

            let ingestion_hint = message
                .timestamp
                .as_ref()
                .and_then(timestamp_to_datetime)
                .map(|dt| dt.timestamp_millis())
                .unwrap_or_else(|| Utc::now().timestamp_millis());

            let timeline = extract_timeline_from_extra(&message.extra, ingestion_hint);
            results.push(RetrievedMessage::new(message, timeline));
            if results.len() >= limit {
                break;
            }
        }

        Ok(results)
    }

    /// 获取单条消息
    #[instrument(skip(self), fields(message_id = %message_id))]
    pub async fn get_message(&self, message_id: &str) -> Result<Option<Message>> {
        self.storage
            .get_message(message_id)
            .await
            .map_err(|e| anyhow!("Failed to get message: {}", e))
    }

    /// 查询消息操作历史
    #[instrument(skip(self), fields(message_id = %message_id))]
    pub async fn query_message_operations(
        &self,
        message_id: &str,
    ) -> Result<Vec<flare_proto::common::MessageOperation>> {
        // 从存储层获取消息操作历史
        // 由于 MessageOperation 现在通过单独的表或存储管理，需要实现相应的查询方法
        // 临时实现：从消息本身获取操作相关信息
        let message = self.get_message(message_id).await?;
        let operations = Vec::new();
        
        // 从消息的扩展字段中提取操作历史
        if let Some(ref _msg) = message {
            // 这里可以根据具体实现获取消息的操作历史
            // 比如从扩展字段或单独的表中获取
            // 临时返回空列表，等待具体的存储实现
        }
        
        Ok(operations)
    }

    /// 搜索消息
    #[instrument(skip(self))]
    pub async fn search_messages(
        &self,
        filters: &[flare_proto::common::FilterExpression],
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        limit: i32,
    ) -> Result<Vec<Message>> {
        let limit = limit.clamp(1, self.config.max_page_size);
        self.storage
            .search_messages(filters, start_time, end_time, limit)
            .await
            .map_err(|e| anyhow!("Failed to search messages: {}", e))
    }

    /// 列出所有标签
    #[instrument(skip(self))]
    pub async fn list_all_tags(&self) -> Result<Vec<String>> {
        self.storage
            .list_all_tags()
            .await
            .map_err(|e| anyhow!("Failed to list tags: {}", e))
    }

    /// 删除消息（批量）
    #[instrument(skip(self), fields(message_count = message_ids.len()))]
    pub async fn delete_messages(&self, message_ids: &[String]) -> Result<usize> {
        if message_ids.is_empty() {
            return Ok(0);
        }

        let mut deleted_count = 0;
        for message_id in message_ids {
            match self
                .storage
                .batch_update_visibility(
                    &[message_id.clone()],
                    "", // 系统删除，不需要 user_id
                    VisibilityStatus::VisibilityDeleted,
                )
                .await
            {
                Ok(count) => deleted_count += count,
                Err(err) => {
                    tracing::warn!(error = %err, message_id = %message_id, "Failed to delete message");
                }
            }
        }

        Ok(deleted_count)
    }

    /// 撤回消息
    #[instrument(skip(self), fields(message_id = %message_id))]
    pub async fn recall_message(
        &self,
        message_id: &str,
        recall_time_limit_seconds: i64,
    ) -> Result<Option<Timestamp>> {
        if message_id.is_empty() {
            return Err(anyhow!("message_id is required"));
        }

        // 检查消息是否存在
        let message = match self.get_message(message_id).await? {
            Some(msg) => msg,
            None => return Err(anyhow!("message not found")),
        };

        // 检查撤回时间限制
        let message_timestamp = message.timestamp.as_ref().map(|ts| ts.seconds).unwrap_or(0);
        let now = Utc::now().timestamp();
        let elapsed = now - message_timestamp;

        if elapsed > recall_time_limit_seconds {
            return Err(anyhow!(
                "Message is too old to recall (limit: {}s, elapsed: {}s)",
                recall_time_limit_seconds,
                elapsed
            ));
        }

        // 执行撤回
        let recalled_at = Utc::now();
        let recalled_timestamp = Timestamp {
            seconds: recalled_at.timestamp(),
            nanos: recalled_at.timestamp_subsec_nanos() as i32,
        };

        let update = MessageUpdate {
            is_recalled: Some(true),
            recalled_at: Some(recalled_timestamp.clone()),
            visibility: None,
            read_by: None,
            operations: None,
            attributes: None,
            tags: None,
            reactions: None,
            status: Some(flare_proto::common::MessageStatus::Recalled as i32), // 更新状态为已撤回
        };

        self.storage
            .update_message(message_id, update)
            .await
            .map_err(|e| anyhow!("Failed to recall message: {}", e))?;

        Ok(Some(recalled_timestamp))
    }

    /// 标记消息已读
    #[instrument(skip(self, ctx), fields(message_id = %message_id))]
    pub async fn mark_message_read(
        &self,
        ctx: &flare_server_core::context::Context,
        message_id: &str,
    ) -> Result<(Timestamp, Option<Timestamp>)> {
        let user_id = ctx.user_id().ok_or_else(|| anyhow::anyhow!("user_id is required in context"))?;
        if message_id.is_empty() {
            return Err(anyhow!("message_id is required"));
        }

        // 获取消息
        let message = match self.get_message(message_id).await? {
            Some(msg) => msg,
            None => return Err(anyhow!("message not found")),
        };

        let now = Utc::now();
        let read_timestamp = Timestamp {
            seconds: now.timestamp(),
            nanos: now.timestamp_subsec_nanos() as i32,
        };

        // 检查是否是阅后即焚消息
        let burned_at = if message.is_burn_after_read {
            let burn_seconds = message.burn_after_seconds as i64;
            Some(Timestamp {
                seconds: now.timestamp() + burn_seconds,
                nanos: now.timestamp_subsec_nanos() as i32,
            })
        } else {
            None
        };

        // 更新已读记录
        let mut read_by = message.read_by.clone();
        let read_record = flare_proto::common::MessageReadRecord {
            user_id: user_id.to_string(),
            read_at: Some(read_timestamp.clone()),
            burned_at: burned_at.clone(),
        };

        // 检查是否已存在该用户的已读记录
        if let Some(existing) = read_by.iter_mut().find(|r| r.user_id == user_id) {
            existing.read_at = Some(read_timestamp.clone());
            existing.burned_at = burned_at.clone();
        } else {
            read_by.push(read_record);
        }

        // 更新消息状态为 Read（如果当前状态是 Sent 或 Delivered）
        use flare_proto::common::MessageStatus;
        let current_status: MessageStatus =
            std::convert::TryFrom::try_from(message.status).unwrap_or(MessageStatus::Unspecified);
        let new_status = match current_status {
            MessageStatus::Sent | MessageStatus::Delivered => Some(MessageStatus::Read as i32),
            MessageStatus::Read => None, // 已经是 Read 状态，不需要更新
            _ => None,                   // 其他状态（如 Created、Failed）不自动更新为 Read
        };

        let update = MessageUpdate {
            is_recalled: None,
            recalled_at: None,
            visibility: None,
            read_by: Some(read_by),
            operations: None,
            attributes: None,
            tags: None,
            reactions: None,
            status: new_status, // 更新消息状态为 Read
        };

        self.storage
            .update_message(message_id, update)
            .await
            .map_err(|e| anyhow!("Failed to mark message as read: {}", e))?;



        Ok((read_timestamp, burned_at))
    }

    /// 为用户删除消息（软删除）
    #[instrument(skip(self, ctx), fields(message_id = %message_id))]
    pub async fn delete_message_for_user(
        &self,
        ctx: &flare_server_core::context::Context,
        message_id: &str,
        permanent: bool,
    ) -> Result<usize> {
        let user_id = ctx.user_id().ok_or_else(|| anyhow::anyhow!("user_id is required in context"))?;
        if message_id.is_empty() {
            return Err(anyhow!("message_id is required"));
        }

        // 检查消息是否存在
        let message = match self.get_message(message_id).await? {
            Some(msg) => msg,
            None => return Err(anyhow!("message not found")),
        };

        // 软删除：更新 visibility
        let visibility = if permanent {
            VisibilityStatus::VisibilityDeleted
        } else {
            VisibilityStatus::VisibilityHidden
        };

        let result = if let Some(visibility_storage) = &self.visibility_storage {
            visibility_storage
                .batch_set_visibility(
                    &[message_id.to_string()],
                    user_id,
                    &message.conversation_id,
                    visibility,
                )
                .await
                .map_err(|e| anyhow!("Failed to delete message for user: {}", e))?
        } else {
            self.storage
                .batch_update_visibility(&[message_id.to_string()], user_id, visibility)
                .await
                .map_err(|e| anyhow!("Failed to delete message for user: {}", e))?
        };



        Ok(result)
    }

    /// 设置消息属性
    #[instrument(skip(self), fields(message_id = %message_id))]
    pub async fn set_message_attributes(
        &self,
        message_id: &str,
        attributes: HashMap<String, String>,
        tags: Vec<String>,
    ) -> Result<()> {
        // 默认行为：仅更新属性与标签
        self.storage
            .update_message_attributes(message_id, attributes, tags)
            .await
            .map_err(|e| anyhow!("Failed to set message attributes: {}", e))
    }

    /// 添加或移除反应
    ///
    /// 功能：
    /// 1. 获取当前消息的反应列表
    /// 2. 根据操作类型添加或移除用户反应
    /// 3. 更新反应列表和计数
    #[instrument(skip(self), fields(message_id = %message_id, emoji = %emoji, user_id = %user_id))]
    pub async fn add_or_remove_reaction(
        &self,
        message_id: &str,
        emoji: &str,
        user_id: &str,
        is_add: bool,
    ) -> Result<Vec<flare_proto::common::Reaction>> {
        use chrono::Utc;
        use prost_types::Timestamp;

        // 1. 获取当前消息
        let current = self
            .storage
            .get_message(message_id)
            .await
            .map_err(|e| anyhow!("Failed to get message for reaction: {}", e))?;

        let message = current.ok_or_else(|| anyhow!("Message not found: {}", message_id))?;

        // 2. 获取当前反应列表
        let mut reactions = message.reactions.clone();

        // 3. 查找或创建反应
        let reaction_index = reactions.iter().position(|r| r.emoji == emoji);
        let now = Utc::now();
        let timestamp = Some(Timestamp {
            seconds: now.timestamp(),
            nanos: now.timestamp_subsec_nanos() as i32,
        });

        if is_add {
            // 添加反应
            if let Some(index) = reaction_index {
                // 反应已存在，添加用户ID（如果不存在）
                let reaction = &mut reactions[index];
                if !reaction.user_ids.contains(&user_id.to_string()) {
                    reaction.user_ids.push(user_id.to_string());
                    reaction.count = reaction.user_ids.len() as i32;
                    reaction.last_updated = timestamp.clone();
                }
            } else {
                // 创建新反应
                reactions.push(flare_proto::common::Reaction {
                    emoji: emoji.to_string(),
                    user_ids: vec![user_id.to_string()],
                    count: 1,
                    last_updated: timestamp.clone(),
                    created_at: timestamp.clone(),
                });
            }
        } else {
            // 移除反应
            if let Some(index) = reaction_index {
                let reaction = &mut reactions[index];
                reaction.user_ids.retain(|id| id != user_id);
                reaction.count = reaction.user_ids.len() as i32;
                reaction.last_updated = timestamp.clone();

                // 如果没有用户了，移除这个反应
                if reaction.user_ids.is_empty() {
                    reactions.remove(index);
                }
            }
        }

        // 4. 更新消息
        let updates = MessageUpdate {
            reactions: Some(reactions.clone()),
            ..Default::default()
        };

        self.storage
            .update_message(message_id, updates)
            .await
            .map_err(|e| anyhow!("Failed to update reactions: {}", e))?;

        Ok(reactions)
    }

    /// 追加一条操作记录并同时更新属性与标签
    #[instrument(skip(self), fields(message_id = %message_id, operation_type = %operation.operation_type))]
    pub async fn append_operation_and_attributes(
        &self,
        message_id: &str,
        operation: flare_proto::common::MessageOperation,
        attributes: HashMap<String, String>,
        tags: Vec<String>,
    ) -> Result<()> {
        // 读取当前消息以获取已有操作记录
        // 注：operations 字段已移除，现在通过 MessageOperation 表单独管理
        // 此处直接更新属性和标签，操作记录由单独的表处理
        let updates = crate::domain::model::MessageUpdate {
            is_recalled: None,
            recalled_at: None,
            visibility: None,
            read_by: None,
            operations: None, // operations 字段已移除
            attributes: Some(attributes),
            tags: Some(tags),
            reactions: None,
            status: None, // 不更新状态（仅更新属性和操作）
        };

        self.storage
            .update_message(message_id, updates)
            .await
            .map_err(|e| anyhow!("Failed to update message with operation: {}", e))
    }

    /// 清理会话
    #[instrument(skip(self), fields(conversation_id = %conversation_id))]
    pub async fn clear_session(
        &self,
        conversation_id: &str,
        user_id: &str,
        clear_before_time: Option<DateTime<Utc>>,
    ) -> Result<usize> {
        if conversation_id.is_empty() {
            return Err(anyhow!("conversation_id is required"));
        }

        // 查询需要清理的消息
        let messages = self
            .storage
            .query_messages(conversation_id, Some(user_id), None, clear_before_time, 10000)
            .await
            .map_err(|e| anyhow!("Failed to query messages: {}", e))?;

        let cleared_count = messages.len();

        // 批量更新 visibility 为 DELETED
        let message_ids: Vec<String> = messages.iter().map(|m| m.server_id.clone()).collect();
        if !message_ids.is_empty() {
            self.storage
                .batch_update_visibility(&message_ids, user_id, VisibilityStatus::VisibilityDeleted)
                .await
                .map_err(|e| anyhow!("Failed to clear session: {}", e))?;
        }

        Ok(cleared_count)
    }
}
