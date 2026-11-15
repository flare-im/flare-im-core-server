//! 设备交接消费者 - 处理 TOPIC_PUSH_HANDOFF 中的 DeviceHandoff 记录
//!
//! ## 核心职责
//! 1. 消费 TOPIC_PUSH_HANDOFF 中的记录（Push Proxy 校验调用方身份后写入）
//! 2. 反序列化 `DeviceHandoff`
//! 3. 调用 PushRouterHandler 对交接的设备补一次提示
//!
//! ## 设计原则
//! - Interface 层：负责 MQ 消息的接收和反序列化
//! - 可重入：补提示产出的离线任务带幂等键，同一条交接处理几遍结果相同
//! - 错误处理：下游暂时不可用时交给 broker 重投，其余失败发送到 DLQ

use std::sync::Arc;

use async_trait::async_trait;
use flare_proto::common::DeviceHandoff;
use flare_server_core::error::FlareError;
use flare_server_core::mq::consumer::{ConsumerError, Message, MessageHandler, MessageResult};
use prost::Message as _;
use tracing::instrument;

use super::acknowledge_after_dlq;
use crate::application::PushRouterHandler;
use crate::infrastructure::mq::publisher::PushServerMqPublisher;

/// 按交接 Topic 的线格式解码一条记录：字节就是 protobuf `DeviceHandoff`，不套信封。
pub fn decode_device_handoff_record(payload: &[u8]) -> Result<DeviceHandoff, prost::DecodeError> {
    DeviceHandoff::decode(payload)
}

/// 处理失败后这条交接的去向。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailureAction {
    /// 交给 broker 按退避重投。
    Redeliver,
    /// 写入 DLQ 后确认。
    DeadLetter,
}

/// 与事件消费者不同，交接可以重投：事件重投会让在线成员收到重复消息，所以那边一律进 DLQ；
/// 交接只产出带幂等键的离线任务，重投几遍都不会多响一声。下游（绑定、会话服务、broker）
/// 暂时不可用时重投，补提示只是晚几秒；进 DLQ 的话要等人工重放，那时提示早已没有意义。
fn failure_action(error: &FlareError) -> FailureAction {
    if error.is_retryable() {
        FailureAction::Redeliver
    } else {
        FailureAction::DeadLetter
    }
}

/// 设备交接消费者处理器
///
/// 处理 `TOPIC_PUSH_HANDOFF` 中的 `DeviceHandoff` 记录，负责补提示
pub struct PushHandoffHandler {
    /// 推送路由处理器（应用层）
    route_handler: Arc<PushRouterHandler>,
    /// MQ 发布器（用于发送 DLQ）
    publisher: Arc<PushServerMqPublisher>,
}

impl PushHandoffHandler {
    /// 创建新的设备交接消费者处理器
    pub fn new(
        route_handler: Arc<PushRouterHandler>,
        publisher: Arc<PushServerMqPublisher>,
    ) -> Self {
        Self {
            route_handler,
            publisher,
        }
    }
}

#[async_trait]
impl MessageHandler for PushHandoffHandler {
    /// 处理 MQ 消息
    ///
    /// # 处理流程
    /// 1. 反序列化 DeviceHandoff
    /// 2. 用记录头重建（并已由上层重新背书）的上下文调用 PushRouterHandler.handle_device_handoff()
    /// 3. 可重试的失败 NACK，其余失败发送到 DLQ
    #[instrument(skip(self, message), fields(
        topic = %message.context.topic,
        partition = message.context.partition,
        offset = message.context.offset,
    ))]
    async fn handle(&self, message: Message) -> Result<MessageResult, ConsumerError> {
        let ctx = &message.context.ctx;
        let handoff = match decode_device_handoff_record(&message.payload) {
            Ok(handoff) => handoff,
            Err(error) => {
                tracing::error!(
                    error = %error,
                    topic = %message.context.topic,
                    "Failed to deserialize DeviceHandoff, sending raw payload to DLQ"
                );
                return acknowledge_after_dlq(
                    self.publisher
                        .publish_dlq(ctx, message.context.key.as_deref(), message.payload.clone())
                        .await,
                );
            }
        };

        tracing::trace!(
            user_id = %handoff.user_id,
            device_id = %handoff.device_id,
            cause = handoff.cause,
            pending = handoff.pending.len(),
            "Processing DeviceHandoff from TOPIC_PUSH_HANDOFF"
        );

        match self.route_handler.handle_device_handoff(ctx, handoff).await {
            Ok(()) => Ok(MessageResult::Ack),
            Err(error) => match failure_action(&error) {
                FailureAction::Redeliver => {
                    tracing::warn!(
                        error = %error,
                        topic = %message.context.topic,
                        retry_count = message.context.retry_count,
                        "Retryable DeviceHandoff failure, NACKing for redelivery"
                    );
                    Ok(MessageResult::Nack)
                }
                FailureAction::DeadLetter => {
                    tracing::error!(
                        error = %error,
                        topic = %message.context.topic,
                        "Failed to process DeviceHandoff, sending to DLQ"
                    );
                    acknowledge_after_dlq(
                        self.publisher
                            .publish_dlq(
                                ctx,
                                message.context.key.as_deref(),
                                message.payload.clone(),
                            )
                            .await,
                    )
                }
            },
        }
    }

    /// 获取处理器名称
    fn name(&self) -> &str {
        "push-handoff-handler"
    }
}

#[cfg(test)]
mod tests {
    use flare_server_core::error::{ErrorBuilder, ErrorCode};

    use super::*;

    /// 下游暂时不可用时重投：产出的任务带幂等键，重投不会多响一声。
    #[test]
    fn a_downstream_outage_redelivers_the_handoff() {
        let error = ErrorBuilder::new(ErrorCode::ServiceUnavailable, "registry down").build_error();
        assert_eq!(failure_action(&error), FailureAction::Redeliver);
    }

    /// 重投也好不了的失败（租户不符、交接体不合法）进 DLQ，不占着重试预算。
    #[test]
    fn a_rejected_handoff_goes_to_the_dlq() {
        for code in [ErrorCode::PermissionDenied, ErrorCode::InvalidParameter] {
            let error = ErrorBuilder::new(code, "rejected").build_error();
            assert_eq!(failure_action(&error), FailureAction::DeadLetter);
        }
        assert_eq!(
            failure_action(&FlareError::system("not classified")),
            FailureAction::DeadLetter
        );
    }

    #[test]
    fn records_decode_as_a_bare_device_handoff() {
        let handoff = DeviceHandoff {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            device_id: "phone".into(),
            ..Default::default()
        };
        assert_eq!(
            decode_device_handoff_record(&handoff.encode_to_vec()).unwrap(),
            handoff
        );
        assert!(decode_device_handoff_record(&[0xff, 0xff, 0xff]).is_err());
    }
}
