//! 推送任务的四个消费者：在线投递，与离线任务的三条优先级通道（单聊、群、批量）。
//!
//! 一个主题一个消费者，各有自己的消费组与并发：批量通道（大群波次、@所有人、摘要、补提示）
//! 积压时，单聊通道与在线投递照常推进。消费组不同，在 JetStream 上就是不同的 durable；启动时
//! 清理陈旧 durable 只清同一个消费组的，四个消费者互不误删。

use std::collections::HashMap;
use std::sync::Arc;

use flare_server_core::mq::kafka::{KafkaConsumerConfig, KafkaProducerConfig};
use flare_server_core::mq::nats::{NatsConsumerConfig, NatsProducerConfig, NatsStreamSpec};

use crate::config::PushWorkerConfig;

/// 消费的是在线任务还是离线任务。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaneKind {
    Online,
    Offline,
}

/// 一个消费者的形状。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaneSpec {
    /// 任务名前缀，也用在日志里。
    pub name: &'static str,
    pub kind: LaneKind,
    pub topic: String,
    pub group: String,
    pub concurrency: usize,
    /// 新建的 durable 从此刻之后开始投递，不重放流里的历史。
    pub deliver_from_new: bool,
}

/// 四个消费者。在线投递沿用配置的消费组；离线三条通道在它后面加后缀。
///
/// 在线投递只管此刻：它的订阅从「在线加离线」收窄成只有在线之后，durable 是新建的，从此刻开始
/// 投递，不把流里已经过时的在线任务再推一遍（客户端连上后自己同步）。离线通道的新 durable 从头
/// 投递：没送到的提示要送，已经送过的有回执去重，过期的直接确认。
pub fn lane_specs(config: &PushWorkerConfig) -> Vec<LaneSpec> {
    let base = config.consumer_group.trim();
    let concurrency = config.consumer_concurrency;
    vec![
        LaneSpec {
            name: "push-online",
            kind: LaneKind::Online,
            topic: config.push_online_topic.clone(),
            group: base.to_string(),
            concurrency: concurrency.online,
            deliver_from_new: true,
        },
        LaneSpec {
            name: "push-offline",
            kind: LaneKind::Offline,
            topic: config.push_offline_topic.clone(),
            group: format!("{base}-offline"),
            concurrency: concurrency.offline,
            deliver_from_new: false,
        },
        LaneSpec {
            name: "push-offline-group",
            kind: LaneKind::Offline,
            topic: config.push_offline_group_topic.clone(),
            group: format!("{base}-offline-group"),
            concurrency: concurrency.offline_group,
            deliver_from_new: false,
        },
        LaneSpec {
            name: "push-offline-bulk",
            kind: LaneKind::Offline,
            topic: config.push_offline_bulk_topic.clone(),
            group: format!("{base}-offline-bulk"),
            concurrency: concurrency.offline_bulk,
            deliver_from_new: false,
        },
    ]
}

/// 一个消费者的 broker 设置：服务配置原样带过来，只换消费组与新建 durable 的起点。
pub struct LaneBrokerSettings {
    inner: Arc<PushWorkerConfig>,
    group: String,
    deliver_from_new: bool,
}

impl LaneBrokerSettings {
    pub fn new(inner: Arc<PushWorkerConfig>, spec: &LaneSpec) -> Self {
        Self {
            inner,
            group: spec.group.clone(),
            deliver_from_new: spec.deliver_from_new,
        }
    }
}

impl NatsConsumerConfig for LaneBrokerSettings {
    fn nats_url(&self) -> &str {
        NatsConsumerConfig::nats_url(self.inner.as_ref())
    }
    fn consumer_group(&self) -> &str {
        &self.group
    }
    fn enable_manual_ack(&self) -> bool {
        self.inner.enable_manual_ack()
    }
    fn batch_size(&self) -> usize {
        self.inner.batch_size()
    }
    fn batch_timeout_ms(&self) -> u64 {
        self.inner.batch_timeout_ms()
    }
    fn ack_wait_secs(&self) -> u64 {
        self.inner.ack_wait_secs()
    }
    fn max_deliver(&self) -> i64 {
        self.inner.max_deliver()
    }
    fn max_ack_pending(&self) -> i64 {
        self.inner.max_ack_pending()
    }
    fn enable_durable(&self) -> bool {
        self.inner.enable_durable()
    }
    fn deliver_from_new(&self) -> bool {
        self.deliver_from_new
    }
    fn stream_specs(&self) -> Vec<NatsStreamSpec> {
        NatsConsumerConfig::stream_specs(self.inner.as_ref())
    }
}

impl NatsProducerConfig for LaneBrokerSettings {
    fn nats_url(&self) -> &str {
        NatsProducerConfig::nats_url(self.inner.as_ref())
    }
    fn timeout_ms(&self) -> u64 {
        self.inner.timeout_ms()
    }
    fn retries(&self) -> u32 {
        self.inner.retries()
    }
    fn retry_backoff_ms(&self) -> u64 {
        self.inner.retry_backoff_ms()
    }
    fn stream_specs(&self) -> Vec<NatsStreamSpec> {
        NatsProducerConfig::stream_specs(self.inner.as_ref())
    }
}

impl KafkaProducerConfig for LaneBrokerSettings {
    fn kafka_brokers(&self) -> Vec<String> {
        self.inner.kafka_brokers()
    }
    fn kafka_client_id(&self) -> &str {
        self.inner.kafka_client_id()
    }
    fn kafka_acks(&self) -> &str {
        self.inner.kafka_acks()
    }
    fn kafka_compression(&self) -> &str {
        self.inner.kafka_compression()
    }
    fn kafka_linger_ms(&self) -> u64 {
        self.inner.kafka_linger_ms()
    }
    fn kafka_batch_size_bytes(&self) -> usize {
        self.inner.kafka_batch_size_bytes()
    }
    fn kafka_message_timeout_ms(&self) -> u64 {
        self.inner.kafka_message_timeout_ms()
    }
    fn kafka_request_timeout_ms(&self) -> u64 {
        self.inner.kafka_request_timeout_ms()
    }
    fn kafka_enable_idempotence(&self) -> bool {
        self.inner.kafka_enable_idempotence()
    }
    fn kafka_max_in_flight_requests_per_connection(&self) -> u32 {
        self.inner.kafka_max_in_flight_requests_per_connection()
    }
    fn kafka_options(&self) -> HashMap<String, String> {
        self.inner.kafka_options()
    }
}

impl KafkaConsumerConfig for LaneBrokerSettings {
    fn kafka_consumer_group(&self) -> &str {
        &self.group
    }
    fn kafka_enable_auto_commit(&self) -> bool {
        self.inner.kafka_enable_auto_commit()
    }
    fn kafka_auto_offset_reset(&self) -> &str {
        self.inner.kafka_auto_offset_reset()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConsumerConcurrency;
    use flare_im_contracts::constants::topics::{
        TOPIC_PUSH_OFFLINE, TOPIC_PUSH_OFFLINE_BULK, TOPIC_PUSH_OFFLINE_GROUP, TOPIC_PUSH_ONLINE,
    };

    /// 与 JetStream 清理陈旧 durable 的前缀同样的写法：`consumer_{组}_{流}_`，非字母数字换成 `-`。
    fn durable_prefix(group: &str, stream: &str) -> String {
        let sanitize = |value: &str| {
            value
                .chars()
                .map(|ch| {
                    if ch.is_ascii_alphanumeric() {
                        ch.to_ascii_lowercase()
                    } else {
                        '-'
                    }
                })
                .collect::<String>()
        };
        format!("consumer_{}_{}_", sanitize(group), sanitize(stream))
    }

    fn specs(group: &str) -> Vec<LaneSpec> {
        lane_specs(&PushWorkerConfig {
            consumer_group: group.to_string(),
            ..PushWorkerConfig::for_tests()
        })
    }

    /// 一个主题一个消费者；任何一个消费组的清理前缀都不会碰到别的消费组的 durable。
    #[test]
    fn every_subject_has_its_own_group_and_no_group_prunes_another() {
        let lanes = specs("push-worker");
        let topics: Vec<&str> = lanes.iter().map(|lane| lane.topic.as_str()).collect();
        assert_eq!(
            topics,
            vec![
                TOPIC_PUSH_ONLINE,
                TOPIC_PUSH_OFFLINE,
                TOPIC_PUSH_OFFLINE_GROUP,
                TOPIC_PUSH_OFFLINE_BULK
            ]
        );
        for lane in &lanes {
            for other in &lanes {
                if lane.group == other.group {
                    continue;
                }
                let durable = format!(
                    "{}0123456789abcdef",
                    durable_prefix(&other.group, "FLARE_PUSH")
                );
                assert!(
                    !durable.starts_with(&durable_prefix(&lane.group, "FLARE_PUSH")),
                    "{} would prune {}",
                    lane.group,
                    other.group
                );
            }
        }
        assert_eq!(lanes[0].group, "push-worker", "在线投递沿用原来的消费组");
        assert!(lanes[0].deliver_from_new, "在线投递不重放过时的在线任务");
        assert!(lanes[1..].iter().all(|lane| !lane.deliver_from_new));
        assert!(lanes[1..].iter().all(|lane| lane.kind == LaneKind::Offline));
    }

    /// 缺省并发让单聊通道拿得最多：批量通道积压时它照常推进。
    #[test]
    fn the_direct_lane_is_never_the_smallest() {
        let concurrency = ConsumerConcurrency::default();
        assert!(concurrency.offline >= concurrency.offline_group);
        assert!(concurrency.offline_group >= concurrency.offline_bulk);
        assert!(concurrency.offline_bulk > 0);
    }
}
