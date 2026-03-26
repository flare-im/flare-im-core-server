//! 消息发布策略（Strategy 模式）
//!
//! 按消息类别与处理类型可插拔选择「存储+推送」或「仅推送」，扩展新消息类型无需改核心分支。
//!
//! **架构落地**: 见 `../doc/ARCHITECTURE_REFACTOR.md` §2（Strategy）。
//! - 接口: `MessagePublishStrategy::publish(PublishContext) -> Future<Result<()>>`
//! - 策略: `PublishBothStrategy`（存储+推送）、`PushOnlyStrategy`（仅推送）
//! - 注册表: `MessagePublishStrategyRegistry::get(category, processing_type)`，未命中时回退为「存储+推送」

use std::future::Future;
use std::pin::Pin;

use anyhow::Result;
use flare_im_core::abstractions::storage_payload::StorageMessagePayload;
use flare_proto::push::PushMessageRequest;
use flare_server_core::context::Ctx;

use crate::domain::model::message_kind::{MessageCategory, MessageProcessingType};
use crate::domain::repository::MessageEventPublisher;

/// 单次发布上下文（供策略使用）
/// 使用领域类型 StorageMessagePayload，Ctx 由调用链从 gRPC 透传并在写入 Kafka 时注入。
pub struct PublishContext<'a> {
    pub request_ctx: &'a Ctx,
    pub publisher: &'a dyn MessageEventPublisher,
    pub storage_payload: StorageMessagePayload,
    pub push_request: PushMessageRequest,
}

/// 消息发布策略：根据类别决定发往存储队列、推送队列或两者。
pub trait MessagePublishStrategy: Send + Sync {
    /// 执行发布（存储/推送或仅推送）
    fn publish<'a>(
        &self,
        ctx: PublishContext<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

/// 存储 + 推送（普通消息、操作类消息）
pub struct PublishBothStrategy;

impl MessagePublishStrategy for PublishBothStrategy {
    fn publish<'a>(
        &self,
        ctx: PublishContext<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            ctx.publisher
                .publish_both(ctx.request_ctx, ctx.storage_payload, ctx.push_request)
                .await
                .map_err(Into::into)
        })
    }
}

/// 仅推送（通知类消息、临时消息等）
pub struct PushOnlyStrategy;

impl MessagePublishStrategy for PushOnlyStrategy {
    fn publish<'a>(
        &self,
        ctx: PublishContext<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            ctx.publisher
                .publish_push(ctx.request_ctx, ctx.push_request)
                .await
                .map_err(Into::into)
        })
    }
}

/// 策略注册表：按 (MessageCategory, MessageProcessingType) 返回策略，支持可插拔扩展。
#[derive(Default)]
pub struct MessagePublishStrategyRegistry {
    strategies: Vec<Box<dyn MessagePublishStrategy>>,
    /// (category, processing_type) -> index
    map: Vec<(MessageCategory, Option<MessageProcessingType>, usize)>,
}

impl MessagePublishStrategyRegistry {
    pub fn new() -> Self {
        let both = Box::new(PublishBothStrategy) as Box<dyn MessagePublishStrategy>;
        let push_only = Box::new(PushOnlyStrategy) as Box<dyn MessagePublishStrategy>;
        let strategies = vec![both, push_only];
        let both_idx = 0;
        let push_only_idx = 1;
        let map = vec![
            (MessageCategory::Operation, None, both_idx),
            (MessageCategory::Normal, Some(MessageProcessingType::Normal), both_idx),
            (MessageCategory::Notification, Some(MessageProcessingType::Notification), push_only_idx),
            (MessageCategory::Temporary, Some(MessageProcessingType::Notification), push_only_idx),
        ];
        Self {
            strategies,
            map,
        }
    }

    /// 根据类别与处理类型获取策略；未命中时默认使用「存储+推送」以保证兼容。
    pub fn get(
        &self,
        category: MessageCategory,
        processing_type: MessageProcessingType,
    ) -> &dyn MessagePublishStrategy {
        let idx = self
            .map
            .iter()
            .find(|(c, pt, _)| *c == category && pt.as_ref() == Some(&processing_type))
            .map(|(_, _, i)| *i)
            .or_else(|| {
                self.map
                    .iter()
                    .find(|(c, pt, _)| *c == category && pt.is_none())
                    .map(|(_, _, i)| *i)
            })
            .unwrap_or(0);
        self.strategies[idx].as_ref()
    }
}
