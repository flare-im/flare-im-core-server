//! 事件处理器（编排层）- 负责编排领域服务
//!
//! ## 核心职责
//! 1. 事件校验（调用 EventDomainService）
//! 2. 序列号分配（调用 EventDomainService）
//! 3. 事件推送（调用 EventDomainService）
//!
//! ## 设计原则
//! - 编排层：不包含业务逻辑，只负责流程编排
//! - 依赖注入：通过构造函数注入所有依赖
//! - CQRS：Command Handler 负责写操作

use std::sync::Arc;

use flare_im_contracts::Ctx;
use flare_proto::common::Event;
use tracing::instrument;

use crate::domain::{PersistenceMode, service::EventDomainService};
use flare_server_core::error::Result;

/// 事件处理器（编排层）
#[derive(Clone)]
pub struct EventHandler {
    /// 事件领域服务
    event_domain_service: Arc<EventDomainService>,
}

impl EventHandler {
    pub fn new(event_domain_service: Arc<EventDomainService>) -> Self {
        Self {
            event_domain_service,
        }
    }

    /// 处理事件
    ///
    /// # 编排流程
    /// 1. 校验事件
    /// 2. 分配序列号
    /// 3. 推送事件
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `event`: 事件
    ///
    /// # 返回
    /// - `Ok(())`: 成功
    /// - `Err`: 错误
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        event_id = %event.event_id,
        conversation_id = %event.conversation_id,
    ))]
    pub async fn handle_event(&self, ctx: &Ctx, event: Event) -> Result<()> {
        let tenant_id = ctx.tenant_id().unwrap_or("0").to_string();
        // 所有事件统一走通用流程（校验 → 分配序列号 → 推送）。
        // 会话标题/头像/删除等读模型变更走直接 gRPC（flare-conversation 同步写读模型），
        // 不经事件路径——避免与直接 API 形成双路径。
        self.handle_general_event(ctx, &tenant_id, event).await
    }

    /// 处理通用事件
    ///
    /// # 编排流程
    /// 1. 校验事件
    /// 2. 分配序列号
    /// 3. 推送事件
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `tenant_id`: 租户 ID
    /// - `event`: 事件
    ///
    /// # 返回
    /// - `Ok(())`: 成功
    /// - `Err`: 错误
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        event_id = %event.event_id,
        conversation_id = %event.conversation_id,
    ))]
    async fn handle_general_event(&self, ctx: &Ctx, tenant_id: &str, event: Event) -> Result<()> {
        // 1. 校验事件
        self.event_domain_service
            .validate_event(ctx, tenant_id, &event)
            .await?;

        // 2. 分配序列号
        let event_with_seq = self
            .event_domain_service
            .allocate_seq(ctx, tenant_id, event)
            .await?;

        // 3. 推送事件
        self.event_domain_service
            .push_event(ctx, event_with_seq.clone(), PersistenceMode::Auto)
            .await?;

        Ok(())
    }
}
