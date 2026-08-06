//! 归档存储仓储（Port）- 使用领域类型，不依赖 proto

use crate::domain::model::{Event, Message};
use flare_im_contracts::Ctx;
use flare_server_core::error::{FlareError, Result};

/// 未实现守卫：把“忘记 override 的写方法静默成功”变成显式失败。
///
/// 这些方法的默认实现历史上是 `Ok(())` / `Ok(false)` / `Ok(None)` 空实现，
/// 一旦某个具体存储实现漏掉 override，消息编辑/撤回/软删/已读/阅后即焚等操作
/// 会“静默成功却不落库”，且无任何告警——线上不可见的数据丢失。
///
/// 生产实现 `PostgresMessageStore` 已 override 全部方法；默认体只应被测试
/// mock/半成品命中。让默认体返回明确错误后，任何漏实现都会在运行期立即暴露，
/// 测试 double 也必须显式声明“我把该写当作 no-op”而不是静默继承。
#[inline]
fn unimplemented_archive_method(method: &str) -> FlareError {
    FlareError::system(format!(
        "ArchiveStoreRepository::{method} has no durable implementation; the concrete store must override it to avoid silent data loss"
    ))
}

#[allow(clippy::too_many_arguments)]
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
        Err(unimplemented_archive_method("update_message_fsm_state"))
    }

    /// 更新消息内容（用于编辑操作）；`content_text_for_extra` 写入 `extra.contentText`
    async fn update_message_content(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        new_content: &[u8],
        editor_id: &str,
        reason: Option<&str>,
        content_text_for_extra: Option<&str>,
    ) -> Result<()> {
        let _ = (
            ctx,
            tenant_id,
            message_id,
            new_content,
            editor_id,
            reason,
            content_text_for_extra,
        );
        Err(unimplemented_archive_method("update_message_content"))
    }

    /// 更新消息可见性（用于软删除操作）
    async fn update_message_visibility(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        user_id: &str,
        scope: i32,
        visibility_status: &str,
    ) -> Result<()> {
        let _ = (
            ctx,
            tenant_id,
            message_id,
            user_id,
            scope,
            visibility_status,
        );
        Err(unimplemented_archive_method("update_message_visibility"))
    }

    /// 记录消息已读
    async fn record_message_read(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        user_id: &str,
    ) -> Result<()> {
        let _ = (ctx, tenant_id, message_id, user_id);
        Err(unimplemented_archive_method("record_message_read"))
    }

    /// 阅后即焚：首次阅读后安排服务端权威倒计时。
    async fn schedule_message_burn(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        reader_id: Option<&str>,
        first_read_at: i64,
        burn_at: i64,
    ) -> Result<bool> {
        let _ = (
            ctx,
            tenant_id,
            message_id,
            reader_id,
            first_read_at,
            burn_at,
        );
        Err(unimplemented_archive_method("schedule_message_burn"))
    }

    /// 阅后即焚：根据消息自身 `burn_after_read_seconds` 在写模型内原子安排倒计时。
    async fn schedule_message_burn_after_read(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        reader_id: Option<&str>,
        first_read_at: i64,
    ) -> Result<Option<i64>> {
        let _ = (ctx, tenant_id, message_id, reader_id, first_read_at);
        Err(unimplemented_archive_method(
            "schedule_message_burn_after_read",
        ))
    }

    /// 阅后即焚：标记消息已焚毁并清除可见内容。
    async fn mark_message_burned(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        burned_at: i64,
    ) -> Result<bool> {
        let _ = (ctx, tenant_id, message_id, burned_at);
        Err(unimplemented_archive_method("mark_message_burned"))
    }

    /// 阅后即焚：硬删除/清理完成。
    async fn mark_message_hard_deleted(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        hard_deleted_at: i64,
    ) -> Result<bool> {
        let _ = (ctx, tenant_id, message_id, hard_deleted_at);
        Err(unimplemented_archive_method("mark_message_hard_deleted"))
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
        Err(unimplemented_archive_method("upsert_message_reaction"))
    }

    /// 置顶或取消置顶消息
    async fn pin_message(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        conversation_id: &str,
        user_id: &str,
        scope: i32,
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
            scope,
            pin,
            expire_at,
            reason,
        );
        Err(unimplemented_archive_method("pin_message"))
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
        Err(unimplemented_archive_method("mark_message"))
    }

    /// 追加领域事件到操作历史（读侧 QueryMessageEvents 等）
    async fn append_event(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        event: &Event,
    ) -> Result<()> {
        let _ = (ctx, tenant_id, message_id, event);
        Err(unimplemented_archive_method("append_event"))
    }

    /// 获取消息（用于权限验证与 tenant_id 解析，写模型内部查询）
    async fn get_message(&self, ctx: &Ctx, message_id: &str) -> Result<Option<Message>> {
        let _ = (ctx, message_id);
        Err(unimplemented_archive_method("get_message"))
    }
}
