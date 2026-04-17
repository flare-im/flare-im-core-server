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

use flare_im_core::Ctx;
use flare_proto::common::{Event, EventType};
use tracing::instrument;

use crate::application::CallCapabilityBridge;
use crate::domain::{service::EventDomainService, PersistenceMode};
use crate::error::Result;

/// 事件处理器（编排层）
#[derive(Clone)]
pub struct EventHandler {
    /// 事件领域服务
    event_domain_service: Arc<EventDomainService>,
    /// 可选：`EVENT_CALL_SIGNAL` → `flare-capability` `Dispatch`（RTC）
    call_capability_bridge: Option<Arc<CallCapabilityBridge>>,
}

impl EventHandler {
    pub fn new(
        event_domain_service: Arc<EventDomainService>,
        call_capability_bridge: Option<Arc<CallCapabilityBridge>>,
    ) -> Self {
        Self {
            event_domain_service,
            call_capability_bridge,
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

        // 根据事件类型路由到不同的处理方法
        let event_type = EventType::try_from(event.r#type);
        match event_type {
            Ok(EventType::EventConversationUpdate) | Ok(EventType::EventConversationDelete) => {
                // 会话相关事件使用专门的处理器
                self.handle_conversation_event(ctx, event).await
            }
            _ => {
                // 其他事件使用通用处理流程
                self.handle_general_event(ctx, &tenant_id, event).await
            }
        }
    }

    /// 处理会话相关事件 (EVENT_CONVERSATION_UPDATE / EVENT_CONVERSATION_DELETE)
    ///
    /// # 编排流程
    /// 1. 校验事件
    /// 2. 分配序列号
    /// 3. TODO: 更新会话读模型
    /// 4. 推送事件
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `event`: 会话事件
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
    async fn handle_conversation_event(&self, ctx: &Ctx, event: Event) -> Result<()> {
        let tenant_id = ctx.tenant_id().unwrap_or("0").to_string();
        
        // 1. 校验事件
        self.event_domain_service
            .validate_event(ctx, &tenant_id, &event)
            .await?;
        
        // 2. 分配序列号
        let event_with_seq = self.event_domain_service
            .allocate_seq(ctx, &tenant_id, event)
            .await?;
        
        // 3. 推送事件
        self.event_domain_service
            .push_event(ctx, event_with_seq, PersistenceMode::Auto)
            .await?;
        
        // TODO: 4. 更新会话读模型
        // - EVENT_CONVERSATION_UPDATE: 更新会话的标题、头像等信息
        // - EVENT_CONVERSATION_DELETE: 标记会话为已删除或移除会话数据

        
        Ok(())
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
    async fn handle_general_event(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        mut event: Event,
    ) -> Result<()> {
        if let Some(ref bridge) = self.call_capability_bridge {
            if let Err(e) = bridge
                .enrich_call_signal_event(ctx, tenant_id, &mut event)
                .await
            {
                tracing::warn!(
                    error = %e,
                    event_id = %event.event_id,
                    conversation_id = %event.conversation_id,
                    "call_capability_bridge: CapabilityService.Dispatch failed, degrade — push event without SFU enrichment"
                );
            }
        }

        // 1. 校验事件
        self.event_domain_service
            .validate_event(ctx, tenant_id, &event)
            .await?;
        
        // 2. 分配序列号
        let event_with_seq = self.event_domain_service
            .allocate_seq(ctx, tenant_id, event)
            .await?;
        
        // 3. 推送事件
        self.event_domain_service
            .push_event(ctx, event_with_seq, PersistenceMode::Auto)
            .await?;
        
        Ok(())
    }
}
