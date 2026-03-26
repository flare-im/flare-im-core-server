//! 归档存储仓储（Port）- 使用领域类型，不依赖 proto

use crate::domain::model::{Event, Message};
use anyhow::Result;
use flare_server_core::context::Ctx;

pub trait ArchiveStoreRepository: Send + Sync {
    async fn store_archive(&self, ctx: &Ctx, message: &Message) -> Result<()>;

    /// 批量存储消息；默认逐条 store_archive。
    async fn store_archive_batch(&self, ctx: &Ctx, messages: &[Message]) -> Result<()> {
        for message in messages {
            self.store_archive(ctx, message).await?;
        }
        Ok(())
    }

    /// 更新消息 FSM 状态（用于撤回、编辑、删除等操作）
    async fn update_message_fsm_state(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        fsm_state: &str,
        recall_reason: Option<&str>,
    ) -> Result<()> {
        let _ = (ctx, tenant_id, message_id, fsm_state, recall_reason);
        Ok(())
    }

    /// 更新消息内容（用于编辑操作）；content_text_for_extra 用于同步更新 extra.content_text
    async fn update_message_content(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        new_content: &[u8],
        edit_version: i32,
        editor_id: &str,
        reason: Option<&str>,
        content_text_for_extra: Option<&str>,
    ) -> Result<()> {
        let _ = (
            ctx,
            tenant_id,
            message_id,
            new_content,
            edit_version,
            editor_id,
            reason,
            content_text_for_extra,
        );
        Ok(())
    }

    /// 更新消息可见性（用于软删除操作）
    async fn update_message_visibility(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        user_id: &str,
        visibility_status: &str,
    ) -> Result<()> {
        let _ = (ctx, tenant_id, message_id, user_id, visibility_status);
        Ok(())
    }

    /// 记录消息已读
    async fn record_message_read(&self, ctx: &Ctx, tenant_id: &str, message_id: &str, user_id: &str) -> Result<()> {
        let _ = (ctx, tenant_id, message_id, user_id);
        Ok(())
    }

    /// 添加或更新消息反应
    async fn upsert_message_reaction(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        emoji: &str,
        user_id: &str,
        add: bool,
    ) -> Result<()> {
        let _ = (ctx, tenant_id, message_id, emoji, user_id, add);
        Ok(())
    }

    /// 置顶或取消置顶消息
    async fn pin_message(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        conversation_id: &str,
        user_id: &str,
        pin: bool,
        expire_at: Option<chrono::DateTime<chrono::Utc>>,
        reason: Option<&str>,
    ) -> Result<()> {
        let _ = (
            ctx,
            tenant_id,
            message_id,
            conversation_id,
            user_id,
            pin,
            expire_at,
            reason,
        );
        Ok(())
    }

    /// 标记或取消标记消息
    async fn mark_message(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        conversation_id: &str,
        user_id: &str,
        mark_type: &str,
        color: Option<&str>,
        add: bool,
    ) -> Result<()> {
        let _ = (
            ctx,
            tenant_id,
            message_id,
            conversation_id,
            user_id,
            mark_type,
            color,
            add,
        );
        Ok(())
    }

    /// 追加领域事件到操作历史（读侧 QueryMessageEvents 等）
    async fn append_event(
        &self,
        ctx: &Ctx,
        _tenant_id: &str,
        _message_id: &str,
        _event: &Event,
    ) -> Result<()> {
        let _ = ctx;
        Ok(())
    }

    /// 获取消息（用于权限验证与 tenant_id 解析，写模型内部查询）
    async fn get_message(&self, ctx: &Ctx, message_id: &str) -> Result<Option<Message>> {
        let _ = (ctx, message_id);
        Ok(None)
    }
}
