//! 消息读取相关领域服务
//!
//! 负责处理标记已读、批量标记已读、会话已读等业务逻辑

use anyhow::Result;
use chrono::Utc;
use tracing::instrument;

use crate::application::commands::{
    BatchMarkMessageReadCommand, MarkAllConversationsReadCommand, MarkConversationReadCommand,
};

/// 消息读取领域服务结果
#[derive(Debug, Clone)]
pub struct MarkReadResult {
    pub read_count: i32,
    pub read_at: chrono::DateTime<Utc>,
    pub last_read_message_id: Option<String>,
}

/// 消息读取领域服务（批量已读/会话已读等；事件由 MessageOperationService 派发）
pub struct MessageReadService;

impl MessageReadService {
    pub fn new() -> Self {
        Self
    }

    /// 批量标记已读（业务逻辑）
    ///
    /// 如果 message_ids 为空，则标记会话中所有未读消息为已读
    #[instrument(skip(self), fields(conversation_id = %cmd.conversation_id, user_id = %cmd.user_id, message_count = cmd.message_ids.len()))]
    pub async fn batch_mark_read(
        &self,
        cmd: BatchMarkMessageReadCommand,
    ) -> Result<MarkReadResult> {
        let read_at = cmd.read_at.unwrap_or_else(|| Utc::now());

        // 如果 message_ids 为空，需要查询会话的未读消息列表
        // 这里简化实现，实际应该通过仓储接口查询未读消息
        let message_ids = if cmd.message_ids.is_empty() {
            // TODO: 通过仓储接口查询会话的未读消息列表
            vec![]
        } else {
            cmd.message_ids
        };

        let read_count = message_ids.len() as i32;

        // 发布已读事件
        // TODO: 批量发布已读事件

        Ok(MarkReadResult {
            read_count,
            read_at,
            last_read_message_id: message_ids.last().cloned(),
        })
    }

    /// 标记会话已读（业务逻辑）
    #[instrument(skip(self), fields(conversation_id = %cmd.conversation_id, user_id = %cmd.user_id))]
    pub async fn mark_conversation_read(
        &self,
        cmd: MarkConversationReadCommand,
    ) -> Result<MarkReadResult> {
        // 转换为批量标记已读命令（message_ids 为空表示标记所有未读消息）
        let batch_cmd = BatchMarkMessageReadCommand {
            conversation_id: cmd.conversation_id.clone(),
            user_id: cmd.user_id.clone(),
            message_ids: vec![],
            read_at: cmd.read_at,
            tenant_id: cmd.tenant_id,
        };

        let result = self.batch_mark_read(batch_cmd).await?;

        // 获取会话的最后一条消息ID
        // TODO: 通过仓储接口查询会话的最后一条消息
        let last_read_message_id = result.last_read_message_id;

        Ok(MarkReadResult {
            read_count: result.read_count,
            read_at: result.read_at,
            last_read_message_id,
        })
    }

    /// 标记全部会话已读（业务逻辑）
    #[instrument(skip(self), fields(user_id = %cmd.user_id))]
    pub async fn mark_all_conversations_read(
        &self,
        cmd: MarkAllConversationsReadCommand,
    ) -> Result<(i32, i32, Vec<(String, i32)>)> {
        // TODO: 通过会话服务查询用户的所有会话
        // 然后对每个会话调用 mark_conversation_read
        // 返回：会话数量、总消息数量、每个会话的统计信息

        Ok((0, 0, vec![]))
    }
}

