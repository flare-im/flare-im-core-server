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

/// 慢事件扇出日志阈值（毫秒），env `FLARE_EVENT_SLOW_LOG_MS`，默认 200。
fn event_slow_log_threshold_ms() -> u64 {
    static VALUE: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *VALUE.get_or_init(|| {
        parse_event_slow_log_ms(std::env::var("FLARE_EVENT_SLOW_LOG_MS").ok().as_deref())
    })
}

/// 纯函数便于测试：非法值回落默认，不能回落成 0（那会给每条事件写一行日志）。
fn parse_event_slow_log_ms(raw: Option<&str>) -> u64 {
    const DEFAULT_MS: u64 = 200;
    match raw {
        Some(v) => v.trim().parse::<u64>().unwrap_or(DEFAULT_MS),
        None => DEFAULT_MS,
    }
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
        // 分段计时：十万人群的一条已读回执实测端到端 52 秒，而 orchestrator CPU
        // 只有 0.6%、Redis 累计 1.27s、Postgres 全程空闲——即"在等"，但等在哪
        // 这条链路上没有任何数字。两人单聊同样的事件只要 15ms，所以耗时随接收者
        // 规模增长。先量出是三步里的哪一步，别再靠猜。
        let started = std::time::Instant::now();

        // 1. 校验事件
        self.event_domain_service
            .validate_event(ctx, tenant_id, &event)
            .await?;
        let validate_ms = started.elapsed().as_millis() as u64;

        // 2. 分配序列号
        let allocate_started = std::time::Instant::now();
        let event_with_seq = self
            .event_domain_service
            .allocate_seq(ctx, tenant_id, event)
            .await?;
        let allocate_ms = allocate_started.elapsed().as_millis() as u64;

        // 3. 推送事件
        let push_started = std::time::Instant::now();
        self.event_domain_service
            .push_event(ctx, event_with_seq.clone(), PersistenceMode::Auto)
            .await?;
        let push_ms = push_started.elapsed().as_millis() as u64;

        let total_ms = started.elapsed().as_millis() as u64;
        if total_ms >= event_slow_log_threshold_ms() {
            tracing::info!(
                conversation_id = %event_with_seq.conversation_id,
                event_type = event_with_seq.r#type,
                validate_ms,
                allocate_ms,
                push_ms,
                total_ms,
                "slow event fanout"
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::parse_event_slow_log_ms;

    #[test]
    fn slow_log_threshold_rejects_garbage_without_falling_back_to_zero() {
        assert_eq!(parse_event_slow_log_ms(None), 200);
        assert_eq!(parse_event_slow_log_ms(Some("500")), 500);
        assert_eq!(parse_event_slow_log_ms(Some(" 500 ")), 500);
        assert_eq!(parse_event_slow_log_ms(Some("0")), 0);
        assert_eq!(parse_event_slow_log_ms(Some("abc")), 200);
        assert_eq!(parse_event_slow_log_ms(Some("")), 200);
        assert_eq!(parse_event_slow_log_ms(Some("-1")), 200);
    }
}
