use crate::domain::model::{Thread, ThreadSortOrder};
use crate::error::{ErrorBuilder, ErrorCode, Result};
use flare_server_core::context::Context;
use std::sync::Arc;
use tracing::instrument;

use crate::domain::repository::ThreadRepository;

/// 话题领域服务 - 包含所有业务逻辑
///
/// 使用泛型参数以获得更好的性能（静态分发）
/// 符合 Rust 2024 原生 async fn in traits 规范
pub struct ThreadDomainService<TR> {
    thread_repo: Arc<TR>,
}

impl<TR: ThreadRepository> ThreadDomainService<TR> {
    pub fn new(thread_repo: Arc<TR>) -> Self {
        Self { thread_repo }
    }

    /// 创建话题
    #[instrument(skip(self, ctx), fields(conversation_id = %conversation_id, root_message_id = %root_message_id))]
    pub async fn create_thread(
        &self,
        ctx: &Context,
        conversation_id: &str,
        root_message_id: &str,
        title: Option<&str>,
        creator_id: &str,
    ) -> Result<Thread> {
        let thread_id = self
            .thread_repo
            .create_thread(ctx, conversation_id, root_message_id, title, creator_id)
            .await?;

        self.thread_repo
            .get_thread(ctx, &thread_id)
            .await?
            .ok_or_else(|| {
                ErrorBuilder::new(ErrorCode::InternalError, "thread not found after creation")
                    .build_error()
            })
    }

    /// 获取话题列表
    #[instrument(skip(self, ctx), fields(conversation_id = %conversation_id))]
    pub async fn list_threads(
        &self,
        ctx: &Context,
        conversation_id: &str,
        limit: i32,
        offset: i32,
        include_archived: bool,
        sort_order: ThreadSortOrder,
    ) -> Result<(Vec<Thread>, i32)> {
        self.thread_repo
            .list_threads(
                ctx,
                conversation_id,
                limit,
                offset,
                include_archived,
                sort_order,
            )
            .await
    }

    /// 获取话题详情
    #[instrument(skip(self, ctx), fields(thread_id = %thread_id))]
    pub async fn get_thread(&self, ctx: &Context, thread_id: &str) -> Result<Option<Thread>> {
        self.thread_repo.get_thread(ctx, thread_id).await
    }

    /// 更新话题
    #[instrument(skip(self, ctx), fields(thread_id = %thread_id))]
    pub async fn update_thread(
        &self,
        ctx: &Context,
        thread_id: &str,
        title: Option<&str>,
        is_pinned: Option<bool>,
        is_locked: Option<bool>,
        is_archived: Option<bool>,
    ) -> Result<Thread> {
        self.thread_repo
            .update_thread(ctx, thread_id, title, is_pinned, is_locked, is_archived)
            .await?;

        self.thread_repo
            .get_thread(ctx, thread_id)
            .await?
            .ok_or_else(|| {
                ErrorBuilder::new(ErrorCode::InternalError, "thread not found after update")
                    .build_error()
            })
    }

    /// 删除话题
    #[instrument(skip(self, ctx), fields(thread_id = %thread_id))]
    pub async fn delete_thread(&self, ctx: &Context, thread_id: &str) -> Result<()> {
        self.thread_repo.delete_thread(ctx, thread_id).await
    }

    /// 增加话题回复计数
    #[instrument(skip(self, ctx), fields(thread_id = %thread_id))]
    pub async fn increment_reply_count(
        &self,
        ctx: &Context,
        thread_id: &str,
        reply_message_id: &str,
        reply_user_id: &str,
    ) -> Result<()> {
        self.thread_repo
            .increment_reply_count(ctx, thread_id, reply_message_id, reply_user_id)
            .await
    }

    /// 添加话题参与者
    #[instrument(skip(self, ctx), fields(thread_id = %thread_id, user_id = %user_id))]
    pub async fn add_participant(
        &self,
        ctx: &Context,
        thread_id: &str,
        user_id: &str,
    ) -> Result<()> {
        self.thread_repo
            .add_participant(ctx, thread_id, user_id)
            .await
    }
}
