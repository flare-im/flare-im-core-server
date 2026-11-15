use std::collections::HashMap;
use std::sync::Arc;

use flare_im_contracts::Ctx;
use flare_proto::PushTaskEnvelope;
use flare_server_core::error::Result;
use flare_server_core::mq::kafka::KafkaProducerBuilder;
use flare_server_core::mq::nats::NatsProducerBuilder;
use flare_server_core::mq::producer::Producer;
use prost::Message as _;

use crate::config::PushServerConfig;
use crate::domain::model::OfflineLane;

/// Broker 按它去重的记录头：NATS 生产者把这个头的值作为 JetStream 的消息标识，
/// 流在去重窗口内丢弃同一标识的重复发布。
const HEADER_MESSAGE_ID: &str = "x-message-id";

/// 离线任务的记录头。带幂等键的任务以幂等键作为 broker 的消息标识：同一台设备上同一条消息的
/// 提示无论被路由几次（源记录重投、正常路由与设备交接各产出一次）都只落盘一条。
/// 没有幂等键的任务不带这个头，由生产者照旧生成随机标识。
fn offline_task_headers(task: &PushTaskEnvelope) -> Option<HashMap<String, String>> {
    let key = task.idempotency_key.trim();
    (!key.is_empty()).then(|| HashMap::from([(HEADER_MESSAGE_ID.to_string(), key.to_string())]))
}

pub struct PushServerMqPublisher {
    producer: Arc<dyn Producer>,
    config: Arc<PushServerConfig>,
}

impl PushServerMqPublisher {
    pub async fn new(config: Arc<PushServerConfig>) -> Result<Self> {
        let producer: Arc<dyn Producer> = match config.mq_backend.as_str() {
            "kafka" => Arc::new(KafkaProducerBuilder::new().build(config.as_ref()).map_err(
                |e| {
                    flare_server_core::error::FlareError::system(format!(
                        "failed to build kafka producer: {}",
                        e
                    ))
                },
            )?),
            "nats" => Arc::new(
                NatsProducerBuilder::new()
                    .build(config.as_ref())
                    .await
                    .map_err(|e| {
                        flare_server_core::error::FlareError::system(format!(
                            "failed to build jetstream producer: {}",
                            e
                        ))
                    })?,
            ),
            other => {
                return Err(flare_server_core::error::FlareError::system(format!(
                    "unsupported mq backend: {other}"
                )));
            }
        };

        Ok(Self::with_producer(config, producer))
    }

    /// 使用已构建的 Producer 创建发布器（Broker 选择由调用方决定）。
    pub fn with_producer(config: Arc<PushServerConfig>, producer: Arc<dyn Producer>) -> Self {
        Self { producer, config }
    }

    pub async fn publish_online_task(
        &self,
        ctx: &Ctx,
        key: Option<&str>,
        payload: Vec<u8>,
    ) -> Result<()> {
        // 验证 payload 为有效的 PushTaskEnvelope
        let _task = PushTaskEnvelope::decode(payload.as_slice()).map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "decode PushTaskEnvelope failed: {}",
                e
            ))
        })?;

        self.producer
            .send(ctx, &self.config.push_online_topic, key, payload, None)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("mq publish failed: {}", e))
            })
    }

    /// 离线任务这条优先级通道的主题。
    fn offline_topic(&self, lane: OfflineLane) -> &str {
        match lane {
            OfflineLane::Direct => &self.config.push_offline_topic,
            OfflineLane::Group => &self.config.push_offline_group_topic,
            OfflineLane::Bulk => &self.config.push_offline_bulk_topic,
        }
    }

    pub async fn publish_offline_task(
        &self,
        ctx: &Ctx,
        lane: OfflineLane,
        key: Option<&str>,
        payload: Vec<u8>,
    ) -> Result<()> {
        // 验证 payload 为有效的 PushTaskEnvelope，并取出它的幂等键
        let task = PushTaskEnvelope::decode(payload.as_slice()).map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "decode PushTaskEnvelope failed: {}",
                e
            ))
        })?;
        let headers = offline_task_headers(&task);

        self.producer
            .send(ctx, self.offline_topic(lane), key, payload, headers)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("mq publish failed: {}", e))
            })
    }

    pub async fn publish_dlq(&self, ctx: &Ctx, key: Option<&str>, payload: Vec<u8>) -> Result<()> {
        self.producer
            .send(ctx, &self.config.push_dlq_topic, key, payload, None)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("mq publish failed: {}", e))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_server_core::mq::producer::{ProducerError, ProducerMessage};
    use std::sync::Mutex;

    /// 交给 Broker 的一条：(topic, key, 记录头)。
    type SentRecord = (String, Option<String>, Option<HashMap<String, String>>);

    /// 记录交给 Broker 的每一条。
    #[derive(Default)]
    struct RecordingProducer {
        sent: Mutex<Vec<SentRecord>>,
    }

    #[async_trait::async_trait]
    impl Producer for RecordingProducer {
        async fn send(
            &self,
            _ctx: &Ctx,
            topic: &str,
            key: Option<&str>,
            _payload: Vec<u8>,
            headers: Option<HashMap<String, String>>,
        ) -> std::result::Result<(), ProducerError> {
            self.sent
                .lock()
                .unwrap()
                .push((topic.to_string(), key.map(str::to_string), headers));
            Ok(())
        }

        async fn send_batch(
            &self,
            _ctx: &Ctx,
            _messages: Vec<ProducerMessage>,
        ) -> std::result::Result<(), ProducerError> {
            Ok(())
        }

        fn name(&self) -> &str {
            "recording-producer"
        }
    }

    fn publisher(producer: Arc<RecordingProducer>) -> PushServerMqPublisher {
        PushServerMqPublisher::with_producer(
            Arc::new(PushServerConfig {
                push_online_topic: "online".into(),
                push_offline_topic: "offline".into(),
                push_offline_group_topic: "offline_group".into(),
                push_offline_bulk_topic: "offline_bulk".into(),
                ..PushServerConfig::for_tests()
            }),
            producer,
        )
    }

    fn ctx() -> Ctx {
        Arc::new(flare_server_core::context::Context::with_request_id(
            "req-1",
        ))
    }

    /// 离线任务的幂等键必须作为 broker 的消息标识发出去：JetStream 靠它去重，
    /// 少了这个头每次重投都是一条新记录、设备上就是又响一声。
    #[tokio::test]
    async fn offline_tasks_carry_their_idempotency_key_as_the_broker_message_id() {
        let producer = Arc::new(RecordingProducer::default());
        let publisher = publisher(producer.clone());
        let task = PushTaskEnvelope {
            user_id: "u1".into(),
            device_id: "phone".into(),
            idempotency_key: "abc123".into(),
            ..Default::default()
        };

        publisher
            .publish_offline_task(
                &ctx(),
                OfflineLane::Direct,
                Some("u1"),
                task.encode_to_vec(),
            )
            .await
            .unwrap();

        let sent = producer.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        let (topic, key, headers) = &sent[0];
        assert_eq!(topic, "offline");
        assert_eq!(key.as_deref(), Some("u1"));
        assert_eq!(
            headers
                .as_ref()
                .and_then(|headers| headers.get("x-message-id"))
                .map(String::as_str),
            Some("abc123")
        );
    }

    /// 在线任务不带幂等键也不带消息标识头：它们按会话或按用户广播，没有「同一台设备同一条
    /// 提示」这回事。没有幂等键的离线任务同样不带，不能拿空串去和别的任务撞。
    #[tokio::test]
    async fn tasks_without_an_idempotency_key_get_no_message_id_header() {
        let producer = Arc::new(RecordingProducer::default());
        let publisher = publisher(producer.clone());
        let task = PushTaskEnvelope {
            idempotency_key: "  ".into(),
            ..Default::default()
        };

        publisher
            .publish_offline_task(&ctx(), OfflineLane::Direct, None, task.encode_to_vec())
            .await
            .unwrap();
        let keyed = PushTaskEnvelope {
            idempotency_key: "abc123".into(),
            ..Default::default()
        };
        publisher
            .publish_online_task(&ctx(), None, keyed.encode_to_vec())
            .await
            .unwrap();

        let sent = producer.sent.lock().unwrap();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0].0, "offline");
        assert_eq!(sent[1].0, "online");
        assert!(sent.iter().all(|(_, _, headers)| headers.is_none()));
    }

    /// 每条优先级通道落到自己的主题上，幂等键头照带。
    #[tokio::test]
    async fn offline_tasks_go_to_the_subject_of_their_lane() {
        let producer = Arc::new(RecordingProducer::default());
        let publisher = publisher(producer.clone());
        let task = PushTaskEnvelope {
            idempotency_key: "k".into(),
            ..Default::default()
        };
        for lane in [OfflineLane::Bulk, OfflineLane::Group, OfflineLane::Direct] {
            publisher
                .publish_offline_task(&ctx(), lane, Some("u1"), task.encode_to_vec())
                .await
                .unwrap();
        }
        let sent = producer.sent.lock().unwrap();
        let topics: Vec<&str> = sent.iter().map(|(topic, _, _)| topic.as_str()).collect();
        assert_eq!(topics, vec!["offline_bulk", "offline_group", "offline"]);
        assert!(sent.iter().all(|(_, _, headers)| headers.is_some()));
    }
}
