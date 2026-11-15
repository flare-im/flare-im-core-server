//! 离线推送消费者 - 处理 TOPIC_PUSH_OFFLINE 中的 PushTaskEnvelope 消息
//!
//! ## 核心职责
//! 1. 消费 TOPIC_PUSH_OFFLINE 中的 PushTaskEnvelope 消息
//! 2. 经显式配置的 `OfflinePushExecutor` 投递（默认关闭，不冒充设备送达）；
//!    未配置 executor 时回退 DLQ/parking
//!
//! ## 任务形状
//! 一条离线任务只对应一台设备，并带着路由定好的处置。已过期的任务直接确认；
//! 没有目标设备或没有处置的任务进死信，绝不退回「发给该用户的全部设备」。
//!
//! ## 设计原则
//! - Interface 层：负责 MQ 消息的接收和反序列化
//! - 上下文重建：从 MQ headers 中提取追踪信息

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use flare_im_contracts::Ctx;
use flare_im_service_kit::metrics::PushWorkerMetrics;
use flare_im_usage::{PushOutcome, UsageRecorder};
use flare_proto::PushTaskEnvelope;
use flare_server_core::FlareError;
use flare_server_core::mq::consumer::{ConsumerError, Message, MessageHandler, MessageResult};
use prost::Message as _;
use tracing::instrument;

use crate::domain::offline_delivery::{OfflinePushExecutor, validate_offline_task};
use crate::infrastructure::mq::dlq_publisher::DlqPublisher;
use crate::infrastructure::vendor_trace::{VendorEvent, VendorOutcome, VendorTrace};

#[cfg(test)]
const DEFAULT_OFFLINE_PARKING_CAPACITY: usize = 4096;

#[allow(dead_code)]
#[derive(Clone)]
struct ParkedOfflinePush {
    tenant_id: String,
    user_id: String,
    device_id: String,
    message_id: String,
    conversation_id: String,
    payload_len: usize,
    parked_at_ms: i64,
}

struct OfflineParkingLot {
    capacity: usize,
    queue: Mutex<VecDeque<ParkedOfflinePush>>,
}

impl OfflineParkingLot {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            queue: Mutex::new(VecDeque::with_capacity(capacity.min(1024))),
        }
    }

    fn park(&self, envelope: &PushTaskEnvelope, payload_len: usize) -> bool {
        let Ok(mut queue) = self.queue.lock() else {
            return false;
        };
        if queue.len() >= self.capacity {
            return false;
        }
        queue.push_back(ParkedOfflinePush {
            tenant_id: envelope.tenant_id.clone(),
            user_id: envelope.user_id.clone(),
            device_id: envelope.device_id.clone(),
            message_id: envelope.message_id.clone(),
            conversation_id: envelope.conversation_id.clone(),
            payload_len,
            parked_at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_millis() as i64)
                .unwrap_or_default(),
        });
        true
    }

    fn len(&self) -> usize {
        self.queue
            .lock()
            .map(|queue| queue.len())
            .unwrap_or_default()
    }
}

/// 用量计数点（`push.attempts`）：包住不经厂商的离线推送后端（开发用的 outbox），每次投递按租户、
/// 通道与结果计一次。结果的划分与 [`OfflinePushHandler`] 一致：可重试的失败会重投，其余失败进死信。
/// 厂商通道不经这里：绑定选择对每次厂商尝试按厂商计一次（`binding_delivery`）。
pub struct UsageCountedExecutor {
    inner: Arc<dyn OfflinePushExecutor>,
    provider: &'static str,
    usage: UsageRecorder,
}

impl UsageCountedExecutor {
    pub fn wrap(
        inner: Arc<dyn OfflinePushExecutor>,
        provider: &'static str,
        usage: UsageRecorder,
    ) -> Arc<dyn OfflinePushExecutor> {
        Arc::new(Self {
            inner,
            provider,
            usage,
        })
    }
}

#[async_trait::async_trait]
impl OfflinePushExecutor for UsageCountedExecutor {
    async fn deliver(&self, ctx: &Ctx, envelope: &PushTaskEnvelope) -> Result<(), FlareError> {
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            self.inner.deliver(ctx, envelope),
        )
        .await
        .unwrap_or_else(|_| {
            Err(crate::infrastructure::vendor_http::failure(
                "offline delivery deadline; acceptance unknown",
            ))
        });
        let outcome = match &result {
            Ok(()) => PushOutcome::Completed,
            Err(error) if error.is_retryable() => PushOutcome::Retryable,
            Err(_) => PushOutcome::Failed,
        };
        self.usage
            .push_attempt(&envelope.tenant_id, self.provider, outcome);
        result
    }
}

/// 离线推送消费者处理器
pub struct OfflinePushHandler {
    #[allow(dead_code)]
    dlq: Option<Arc<DlqPublisher>>,
    delivery: Option<Arc<dyn OfflinePushExecutor>>,
    parking_lot: Arc<OfflineParkingLot>,
    metrics: Arc<PushWorkerMetrics>,
    /// 决策链里厂商这一环的写入端：这里只记过期丢弃的任务，其余由投递后端记。
    trace: VendorTrace,
}

impl OfflinePushHandler {
    pub fn new(
        dlq: Arc<DlqPublisher>,
        parking_capacity: usize,
        metrics: Arc<PushWorkerMetrics>,
    ) -> Self {
        Self {
            dlq: Some(dlq),
            delivery: None,
            parking_lot: Arc::new(OfflineParkingLot::new(parking_capacity)),
            metrics,
            trace: VendorTrace::disabled(),
        }
    }

    pub fn with_delivery(
        dlq: Arc<DlqPublisher>,
        delivery: Arc<dyn OfflinePushExecutor>,
        parking_capacity: usize,
        metrics: Arc<PushWorkerMetrics>,
    ) -> Self {
        Self {
            dlq: Some(dlq),
            delivery: Some(delivery),
            parking_lot: Arc::new(OfflineParkingLot::new(parking_capacity)),
            metrics,
            trace: VendorTrace::disabled(),
        }
    }

    #[cfg(test)]
    fn without_dlq_for_test() -> Self {
        Self {
            dlq: None,
            delivery: None,
            parking_lot: Arc::new(OfflineParkingLot::new(DEFAULT_OFFLINE_PARKING_CAPACITY)),
            metrics: Arc::new(PushWorkerMetrics::new()),
            trace: VendorTrace::disabled(),
        }
    }

    /// 过期丢弃的任务也记进决策链。
    pub fn with_trace(mut self, trace: VendorTrace) -> Self {
        self.trace = trace;
        self
    }

    fn decode_task_envelope(message: &Message) -> Result<PushTaskEnvelope, ConsumerError> {
        PushTaskEnvelope::decode(message.payload.as_slice())
            .map_err(|e| ConsumerError::Deserialization(format!("PushTaskEnvelope: {}", e)))
    }

    fn is_expired(envelope: &PushTaskEnvelope) -> bool {
        let Some(expire_at) = envelope.expire_at else {
            return false;
        };
        if expire_at <= 0 {
            return false;
        }
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or_default();
        now_ms >= expire_at
    }

    /// 终态任务写进死信后确认。死信写不进去时返回错误，由消费框架按自己的重试预算处理，
    /// 而不是把一条没人看得到的任务悄悄确认掉。
    async fn dead_letter(
        &self,
        ctx: &Ctx,
        envelope: &PushTaskEnvelope,
        payload: Vec<u8>,
    ) -> Result<MessageResult, ConsumerError> {
        let Some(dlq) = &self.dlq else {
            return Err(ConsumerError::DeadLetter(
                "offline push DLQ publisher is not configured".to_string(),
            ));
        };
        dlq.publish(ctx, Some(&envelope.conversation_id), payload)
            .await
            .map_err(|e| ConsumerError::DeadLetter(e.to_string()))?;
        Ok(MessageResult::Ack)
    }
}

#[async_trait::async_trait]
impl MessageHandler for OfflinePushHandler {
    #[instrument(skip(self), fields(
        topic = %message.context.topic,
        partition = message.context.partition,
        offset = message.context.offset,
    ))]
    async fn handle(&self, message: Message) -> Result<MessageResult, ConsumerError> {
        let envelope = Self::decode_task_envelope(&message)?;

        let ctx = &message.context.ctx;

        tracing::trace!(
            trace_id = %ctx.trace_id(),
            user_id = %envelope.user_id,
            device_id = %envelope.device_id,
            tenant_id = %envelope.tenant_id,
            message_id = %envelope.message_id,
            conversation_id = %envelope.conversation_id,
            disposition = envelope.disposition,
            "offline push task received"
        );

        // 过期判定在最前面：一条过了时效的提示无论是什么处置、形状对不对，都不该再打扰用户，
        // 也不值得占一条死信。
        if Self::is_expired(&envelope) {
            tracing::debug!(
                user_id = %envelope.user_id,
                device_id = %envelope.device_id,
                message_id = %envelope.message_id,
                expire_at = ?envelope.expire_at,
                "offline push task expired; acking terminal task"
            );
            self.trace
                .record(&envelope, VendorEvent::new(None, VendorOutcome::Expired));
            return Ok(MessageResult::Ack);
        }

        if let Err(error) = validate_offline_task(&envelope) {
            tracing::error!(
                error = %error,
                user_id = %envelope.user_id,
                tenant_id = %envelope.tenant_id,
                message_id = %envelope.message_id,
                conversation_id = %envelope.conversation_id,
                "offline push task is malformed; sending to DLQ"
            );
            let result = self
                .dead_letter(ctx, &envelope, message.payload.clone())
                .await?;
            self.metrics
                .record_offline_redelivery("malformed_task", "dlq_ack");
            return Ok(result);
        }

        let Some(delivery) = &self.delivery else {
            tracing::warn!(
                user_id = %envelope.user_id,
                tenant_id = %envelope.tenant_id,
                message_id = %envelope.message_id,
                conversation_id = %envelope.conversation_id,
                "offline push delivery backend is not configured; parking or DLQ then acking"
            );
            if let Some(dlq) = &self.dlq {
                dlq.publish(
                    ctx,
                    Some(&envelope.conversation_id),
                    message.payload.clone(),
                )
                .await
                .map_err(|e| ConsumerError::DeadLetter(e.to_string()))?;
                self.metrics
                    .record_offline_redelivery("backend_unconfigured", "dlq_ack");
                return Ok(MessageResult::Ack);
            }

            if self.parking_lot.park(&envelope, message.payload.len()) {
                self.metrics
                    .record_offline_redelivery("backend_unconfigured", "park_ack");
                tracing::debug!(
                    parked_len = self.parking_lot.len(),
                    message_id = %envelope.message_id,
                    "offline push task parked because delivery backend is not configured"
                );
            } else {
                self.metrics
                    .record_offline_redelivery("backend_unconfigured", "parking_full_ack");
                tracing::error!(
                    capacity = self.parking_lot.capacity,
                    message_id = %envelope.message_id,
                    "offline push parking lot is full; acking to prevent redelivery storm"
                );
            }
            return Ok(MessageResult::Ack);
        };

        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            delivery.deliver(ctx, &envelope),
        )
        .await
        .unwrap_or_else(|_| {
            Err(crate::infrastructure::vendor_http::failure(
                "offline delivery deadline; acceptance unknown",
            ))
        });
        match outcome {
            Ok(()) => Ok(MessageResult::Ack),
            Err(error) if error.is_retryable() => {
                tracing::warn!(
                    error = %error,
                    user_id = %envelope.user_id,
                    device_id = %envelope.device_id,
                    message_id = %envelope.message_id,
                    "offline push delivery failed with retryable error; nacking"
                );
                Ok(MessageResult::Nack)
            }
            Err(error) => {
                tracing::error!(
                    error = %error,
                    user_id = %envelope.user_id,
                    device_id = %envelope.device_id,
                    message_id = %envelope.message_id,
                    "offline push delivery failed with terminal error; sending to DLQ"
                );
                self.dead_letter(ctx, &envelope, message.payload.clone())
                    .await
            }
        }
    }

    fn name(&self) -> &str {
        "push-offline-handler"
    }
}

/// 离线推送消费者工厂
pub struct OfflinePushConsumerFactory;

impl OfflinePushConsumerFactory {
    pub fn create_handler(
        dlq: Arc<DlqPublisher>,
        parking_capacity: usize,
        metrics: Arc<PushWorkerMetrics>,
        trace: VendorTrace,
    ) -> Arc<dyn MessageHandler> {
        Arc::new(OfflinePushHandler::new(dlq, parking_capacity, metrics).with_trace(trace))
    }

    pub fn create_handler_with_delivery(
        dlq: Arc<DlqPublisher>,
        delivery: Arc<dyn OfflinePushExecutor>,
        parking_capacity: usize,
        metrics: Arc<PushWorkerMetrics>,
        trace: VendorTrace,
    ) -> Arc<dyn MessageHandler> {
        Arc::new(
            OfflinePushHandler::with_delivery(dlq, delivery, parking_capacity, metrics)
                .with_trace(trace),
        )
    }

    pub fn topic() -> &'static str {
        flare_im_contracts::constants::topics::TOPIC_PUSH_OFFLINE
    }

    pub fn consumer_group() -> &'static str {
        flare_im_contracts::constants::groups::PUSH_WORKER_GROUP_DEFAULT
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_proto::common::PushDisposition;
    use flare_server_core::context::Context;
    use flare_server_core::mq::consumer::{ContentType, MessageContext};

    /// 路由发出的任务形状：一台目标设备、一个处置。
    fn task_envelope() -> PushTaskEnvelope {
        PushTaskEnvelope {
            user_id: "user-1".to_string(),
            message_id: "msg-1".to_string(),
            conversation_id: "conv-1".to_string(),
            tenant_id: "tenant-a".to_string(),
            priority: 5,
            payload_kind: flare_proto::common::PushTaskPayloadKind::Message as i32,
            device_id: "device-1".to_string(),
            disposition: PushDisposition::Loud as i32,
            ..Default::default()
        }
    }

    fn message_of(envelope: &PushTaskEnvelope) -> Message {
        let ctx = Arc::new(
            Context::root()
                .with_tenant_id("tenant-a")
                .with_user_id("user-1")
                .with_trace_id("trace-offline-test"),
        );
        Message::new(
            envelope.encode_to_vec(),
            ContentType::Protobuf,
            MessageContext::new(ctx, "push.offline".to_string()),
        )
    }

    fn task_message() -> Message {
        message_of(&task_envelope())
    }

    #[tokio::test]
    async fn valid_offline_task_is_parked_without_delivery_backend() {
        let handler = OfflinePushHandler::without_dlq_for_test();

        let result = handler.handle(task_message()).await.expect("handle");

        assert_eq!(result, MessageResult::Ack);
        assert_eq!(handler.parking_lot.len(), 1);
    }

    struct RecordingExecutor {
        delivered: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl OfflinePushExecutor for RecordingExecutor {
        async fn deliver(
            &self,
            _ctx: &Ctx,
            _envelope: &PushTaskEnvelope,
        ) -> Result<(), FlareError> {
            self.delivered
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    struct FailingExecutor(flare_server_core::error::ErrorCode);

    #[async_trait::async_trait]
    impl OfflinePushExecutor for FailingExecutor {
        async fn deliver(
            &self,
            _ctx: &Ctx,
            _envelope: &PushTaskEnvelope,
        ) -> Result<(), FlareError> {
            Err(
                flare_server_core::error::ErrorBuilder::new(self.0, "provider failed")
                    .build_error(),
            )
        }
    }

    /// 每次投递按租户、通道与结果计一次 `push.attempts`；结果与 handler 的重投/死信划分一致。
    #[tokio::test]
    async fn every_delivery_attempt_is_counted_by_provider_and_result() {
        use flare_server_core::error::ErrorCode;
        let usage = UsageRecorder::unpublished("push-worker-test");
        let delivered = UsageCountedExecutor::wrap(
            Arc::new(RecordingExecutor {
                delivered: std::sync::atomic::AtomicUsize::new(0),
            }),
            "apns",
            usage.clone(),
        );
        let retryable = UsageCountedExecutor::wrap(
            Arc::new(FailingExecutor(ErrorCode::ServiceUnavailable)),
            "apns",
            usage.clone(),
        );
        let failed = UsageCountedExecutor::wrap(
            Arc::new(FailingExecutor(ErrorCode::InvalidParameter)),
            "fcm",
            usage.clone(),
        );
        let message = task_message();
        let envelope = OfflinePushHandler::decode_task_envelope(&message).unwrap();
        let ctx = &message.context.ctx;
        delivered.deliver(ctx, &envelope).await.unwrap();
        delivered.deliver(ctx, &envelope).await.unwrap();
        assert!(
            retryable
                .deliver(ctx, &envelope)
                .await
                .unwrap_err()
                .is_retryable()
        );
        assert!(failed.deliver(ctx, &envelope).await.is_err());

        let attempts: Vec<_> = usage
            .live()
            .into_iter()
            .map(|reading| {
                assert_eq!(
                    (reading.app.as_str(), reading.meter),
                    ("tenant-a", "push.attempts")
                );
                (reading.dims[0].1, reading.dims[1].1, reading.value)
            })
            .collect();
        assert_eq!(
            attempts,
            [
                ("apns", "completed", 2),
                ("apns", "retryable", 1),
                ("fcm", "failed", 1)
            ]
        );
    }

    /// 接了投递后端、没接死信发布器的 handler：走到死信那一步会以 `DeadLetter` 错误返回。
    fn handler_with_delivery() -> (OfflinePushHandler, Arc<RecordingExecutor>) {
        let delivery = Arc::new(RecordingExecutor {
            delivered: std::sync::atomic::AtomicUsize::new(0),
        });
        let handler = OfflinePushHandler {
            dlq: None,
            delivery: Some(delivery.clone()),
            parking_lot: Arc::new(OfflineParkingLot::new(DEFAULT_OFFLINE_PARKING_CAPACITY)),
            metrics: Arc::new(PushWorkerMetrics::new()),
            trace: VendorTrace::disabled(),
        };
        (handler, delivery)
    }

    #[tokio::test]
    async fn valid_offline_task_goes_through_delivery_backend() {
        let (handler, delivery) = handler_with_delivery();

        let result = handler.handle(task_message()).await.expect("handle");

        assert_eq!(result, MessageResult::Ack);
        assert_eq!(
            delivery.delivered.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(handler.parking_lot.len(), 0);
    }

    /// 没有目标设备或没有处置的任务不可执行：进死信，不交给投递后端——
    /// 交出去就只能退回「发给该用户的全部设备」，那正是按设备规划要消除的行为。
    #[tokio::test]
    async fn task_without_device_or_disposition_goes_to_dlq_not_to_delivery() {
        for malformed in [
            PushTaskEnvelope {
                device_id: String::new(),
                ..task_envelope()
            },
            PushTaskEnvelope {
                device_id: "  ".to_string(),
                ..task_envelope()
            },
            PushTaskEnvelope {
                disposition: PushDisposition::Unspecified as i32,
                ..task_envelope()
            },
        ] {
            let (handler, delivery) = handler_with_delivery();

            let error = handler
                .handle(message_of(&malformed))
                .await
                .expect_err("malformed task takes the DLQ path");

            assert!(matches!(error, ConsumerError::DeadLetter(_)));
            assert_eq!(
                delivery.delivered.load(std::sync::atomic::Ordering::SeqCst),
                0
            );
            assert_eq!(handler.parking_lot.len(), 0);
        }

        // 没配投递后端时同样先看形状：不可执行的任务不会被当成正常任务停泊。
        let handler = OfflinePushHandler::without_dlq_for_test();
        let malformed = PushTaskEnvelope {
            device_id: String::new(),
            ..task_envelope()
        };
        assert!(handler.handle(message_of(&malformed)).await.is_err());
        assert_eq!(handler.parking_lot.len(), 0);
    }

    /// 过期任务不论处置一律确认丢弃：过了时效的提示再发出去只是打扰，重投也不会变得有用。
    #[tokio::test]
    async fn expired_task_of_any_disposition_is_acked_without_delivery() {
        for disposition in [
            PushDisposition::Loud,
            PushDisposition::Quiet,
            PushDisposition::Clear,
            PushDisposition::BadgeOnly,
            PushDisposition::RecallReplace,
            PushDisposition::Digest,
        ] {
            let (handler, delivery) = handler_with_delivery();
            let expired = PushTaskEnvelope {
                disposition: disposition as i32,
                badge: Some(3),
                expire_at: Some(1),
                ..task_envelope()
            };

            let result = handler.handle(message_of(&expired)).await.expect("handle");

            assert_eq!(result, MessageResult::Ack, "{disposition:?}");
            assert_eq!(
                delivery.delivered.load(std::sync::atomic::Ordering::SeqCst),
                0,
                "{disposition:?}"
            );

            // 同一条任务没过期时照常交给投递后端。
            let live = PushTaskEnvelope {
                expire_at: Some(i64::MAX),
                ..expired
            };
            let result = handler.handle(message_of(&live)).await.expect("handle");
            assert_eq!(result, MessageResult::Ack, "{disposition:?}");
            assert_eq!(
                delivery.delivered.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "{disposition:?}"
            );
        }

        // 已过期又不可执行的任务只确认，不占死信。
        let (handler, delivery) = handler_with_delivery();
        let expired_malformed = PushTaskEnvelope {
            device_id: String::new(),
            expire_at: Some(1),
            ..task_envelope()
        };
        let result = handler
            .handle(message_of(&expired_malformed))
            .await
            .expect("handle");
        assert_eq!(result, MessageResult::Ack);
        assert_eq!(
            delivery.delivered.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }
}
