use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use crate::error::Result;
use flare_proto::push::PushMessageRequest as PushPushMessageRequest;
use flare_proto::storage::StoreMessage as StorageStoreMessageRequest;

/// MessageEventPublisher 的枚举封装，用于在 Rust 2024 下避免 `dyn` + async trait 带来的
/// `E0038: trait is not dyn compatible` 问题。
pub enum MessageEventPublisherItem {
    Kafka(Arc<crate::infrastructure::messaging::kafka_publisher::KafkaMessagePublisher>),
}

impl std::fmt::Debug for MessageEventPublisherItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MessageEventPublisherItem::Kafka(_) => f.debug_tuple("Kafka").finish(),
        }
    }
}

impl crate::domain::repository::message_publisher::MessageEventPublisher for MessageEventPublisherItem {
    fn publish_storage(
        &self,
        payload: StorageStoreMessageRequest,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            match self {
                MessageEventPublisherItem::Kafka(publisher) => {
                    publisher.publish_storage(payload).await
                }
            }
        })
    }

    fn publish_operation(
        &self,
        payload: StorageStoreMessageRequest,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            match self {
                MessageEventPublisherItem::Kafka(publisher) => {
                    publisher.publish_operation(payload).await
                }
            }
        })
    }

    fn publish_push(
        &self,
        payload: PushPushMessageRequest,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            match self {
                MessageEventPublisherItem::Kafka(publisher) => {
                    publisher.publish_push(payload).await
                }
            }
        })
    }

    fn publish_both(
        &self,
        storage_payload: StorageStoreMessageRequest,
        push_payload: PushPushMessageRequest,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            match self {
                MessageEventPublisherItem::Kafka(publisher) => {
                    publisher.publish_both(storage_payload, push_payload).await
                }
            }
        })
    }
}