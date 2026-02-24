use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::error::{FlareError, Result};
use anyhow::{anyhow, Result as AnyhowResult};
use flare_proto::push::PushMessageRequest as PushPushMessageRequest;
use flare_proto::storage::StoreMessage as StorageStoreMessageRequest;
use futures::FutureExt;
use futures::future::try_join_all;
use prost::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::sync::Mutex;

use crate::config::MessageOrchestratorConfig;
use crate::domain::repository::MessageEventPublisher;

/// Kafka 消息发布器（支持批量发送）
pub struct KafkaMessagePublisher {
    producer: Arc<FutureProducer>,
    config: Arc<MessageOrchestratorConfig>,
    // 批量发送缓冲区
    storage_buffer: Arc<Mutex<Vec<StorageStoreMessageRequest>>>,
    operation_buffer: Arc<Mutex<Vec<StorageStoreMessageRequest>>>,
    push_buffer: Arc<Mutex<Vec<PushPushMessageRequest>>>,
    // 最后刷新时间
    last_flush_time: Arc<Mutex<std::time::Instant>>,
}

impl KafkaMessagePublisher {
    pub fn new(producer: Arc<FutureProducer>, config: Arc<MessageOrchestratorConfig>) -> Arc<Self> {
        let publisher = Arc::new(Self {
            producer,
            config: config.clone(),
            storage_buffer: Arc::new(Mutex::new(Vec::new())),
            operation_buffer: Arc::new(Mutex::new(Vec::new())),
            push_buffer: Arc::new(Mutex::new(Vec::new())),
            last_flush_time: Arc::new(Mutex::new(std::time::Instant::now())),
        });

        // 启动自动刷新任务
        let publisher_clone = Arc::clone(&publisher);
        let flush_interval = Duration::from_millis(config.kafka_flush_interval_ms);
        tokio::spawn(async move {
            publisher_clone.auto_flush_loop(flush_interval).await;
        });

        publisher
    }

    /// 自动刷新循环
    async fn auto_flush_loop(self: Arc<Self>, flush_interval: Duration) {
        let mut interval = tokio::time::interval(flush_interval);
        loop {
            interval.tick().await;

            // 刷新存储消息缓冲区
            let storage_messages = {
                let mut buffer = self.storage_buffer.lock().await;
                let last_flush = self.last_flush_time.lock().await;
                let should_flush = buffer.len() >= self.config.kafka_batch_size
                    || last_flush.elapsed() >= flush_interval;

                if should_flush && !buffer.is_empty() {
                    let messages = buffer.drain(..).collect();
                    drop(buffer);
                    Some(messages)
                } else {
                    None
                }
            };

            if let Some(messages) = storage_messages {
                if let Err(e) = self.publish_storage_batch(messages).await {
                    tracing::error!(error = %e, "Failed to flush storage messages");
                }
                *self.last_flush_time.lock().await = std::time::Instant::now();
            }

            // 刷新操作消息缓冲区
            let operation_messages = {
                let mut buffer = self.operation_buffer.lock().await;
                let last_flush = self.last_flush_time.lock().await;
                let should_flush = buffer.len() >= self.config.kafka_batch_size
                    || last_flush.elapsed() >= flush_interval;

                if should_flush && !buffer.is_empty() {
                    let messages = buffer.drain(..).collect();
                    drop(buffer);
                    Some(messages)
                } else {
                    None
                }
            };

            if let Some(messages) = operation_messages {
                if let Err(e) = self.publish_operation_batch(messages).await {
                    tracing::error!(error = %e, "Failed to flush operation messages");
                }
            }

            // 刷新推送消息缓冲区
            let push_messages = {
                let mut buffer = self.push_buffer.lock().await;
                let last_flush = self.last_flush_time.lock().await;
                let should_flush = buffer.len() >= self.config.kafka_batch_size
                    || last_flush.elapsed() >= flush_interval;

                if should_flush && !buffer.is_empty() {
                    let messages = buffer.drain(..).collect();
                    drop(buffer);
                    Some(messages)
                } else {
                    None
                }
            };

            if let Some(messages) = push_messages {
                if let Err(e) = self.publish_push_batch(messages).await {
                    tracing::error!(error = %e, "Failed to flush push messages");
                }
            }
        }
    }

    /// 批量发布存储消息
    async fn publish_storage_batch(&self, payloads: Vec<StorageStoreMessageRequest>) -> Result<()> {
        if payloads.is_empty() {
            return Ok(());
        }

        // 批量编码和构建记录
        // 先编码所有 payload，保存到 Vec 中以保持生命周期
        let mut encoded_payloads = Vec::with_capacity(payloads.len());
        let mut valid_indices = Vec::new();

        for (idx, payload) in payloads.iter().enumerate() {
            let encoded = payload.encode_to_vec();

            // 验证消息大小
            const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024; // 10MB
            if encoded.len() > MAX_MESSAGE_SIZE {
                tracing::error!(
                    payload_size = encoded.len(),
                    max_size = MAX_MESSAGE_SIZE,
                    conversation_id = %payload.conversation_id,
                    message_id = payload.message.as_ref().map(|m| m.server_id.as_str()).unwrap_or("unknown"),
                    "Storage message size exceeds maximum allowed size"
                );
                continue; // 跳过过大的消息
            }

            encoded_payloads.push(encoded);
            valid_indices.push(idx);
        }

        if encoded_payloads.is_empty() {
            return Ok(());
        }

        // 构建记录（借用 encoded_payloads）
        let records: Vec<_> = valid_indices
            .iter()
            .enumerate()
            .map(|(encoded_idx, &payload_idx)| {
                FutureRecord::to(&self.config.kafka_storage_topic)
                    .payload(&encoded_payloads[encoded_idx])
                    .key(&payloads[payload_idx].conversation_id)
            })
            .collect();

        // 批量发送（并发执行）
        let futures: Vec<_> = records
            .into_iter()
            .map(|record| {
                self.producer
                    .send(record, Duration::from_millis(self.config.kafka_timeout_ms))
                    .map(|result| result.map_err(|(err, _)| anyhow!("Kafka send error: {}", err)))
            })
            .collect();

        // 等待所有发送完成（并发执行）
        if let Err(e) = try_join_all(futures).await {
            return Err(FlareError::system(&format!("Kafka batch send error: {}", e)));
        }

        tracing::info!(
            topic = %self.config.kafka_storage_topic,
            batch_size = payloads.len(),
            "Successfully published batch of storage messages to Kafka"
        );

        Ok(())
    }

    /// 批量发布操作消息
    async fn publish_operation_batch(&self, payloads: Vec<StorageStoreMessageRequest>) -> Result<()> {
        if payloads.is_empty() {
            return Ok(());
        }

        let mut encoded_payloads = Vec::with_capacity(payloads.len());
        let mut valid_indices = Vec::new();

        for (idx, payload) in payloads.iter().enumerate() {
            // **关键日志**：记录发布到 Kafka 的消息类型
            if let Some(msg) = &payload.message {
                tracing::info!(
                    message_id = %msg.server_id,
                    message_type = msg.message_type,
                    message_type_label = ?flare_proto::common::MessageType::try_from(msg.message_type).ok(),
                    conversation_id = %msg.conversation_id,
                    has_content = msg.content.is_some(),
                    content_variant = msg.content.as_ref()
                        .and_then(|c| c.content.as_ref())
                        .map(|c| match c {
                            flare_proto::common::message_content::Content::Operation(_) => "Operation",
                            flare_proto::common::message_content::Content::Notification(_) => "Notification",
                            _ => "Other",
                        })
                        .unwrap_or("None"),
                    "📤 发布操作消息到 Kafka (storage-message-operations)"
                );
            }
            
            let encoded = payload.encode_to_vec();

            const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;
            if encoded.len() > MAX_MESSAGE_SIZE {
                tracing::error!(
                    payload_size = encoded.len(),
                    max_size = MAX_MESSAGE_SIZE,
                    conversation_id = %payload.conversation_id,
                    message_id = payload.message.as_ref().map(|m| m.server_id.as_str()).unwrap_or("unknown"),
                    "Operation message size exceeds maximum allowed size"
                );
                continue;
            }

            encoded_payloads.push(encoded);
            valid_indices.push(idx);
        }

        if encoded_payloads.is_empty() {
            return Ok(());
        }

        let records: Vec<_> = valid_indices
            .iter()
            .enumerate()
            .map(|(encoded_idx, &payload_idx)| {
                FutureRecord::to(&self.config.kafka_operation_topic)
                    .payload(&encoded_payloads[encoded_idx])
                    .key(&payloads[payload_idx].conversation_id)
            })
            .collect();

        let futures: Vec<_> = records
            .into_iter()
            .map(|record| {
                self.producer
                    .send(record, Duration::from_millis(self.config.kafka_timeout_ms))
                    .map(|result| result.map_err(|(err, _)| anyhow!("Kafka send error: {}", err)))
            })
            .collect();

        if let Err(e) = try_join_all(futures).await {
            return Err(FlareError::system(&format!("Kafka batch send error: {}", e)));
        }

        tracing::info!(
            topic = %self.config.kafka_operation_topic,
            batch_size = payloads.len(),
            "Successfully published batch of operation messages to Kafka"
        );

        Ok(())
    }

    /// 批量发布推送消息
    async fn publish_push_batch(&self, payloads: Vec<PushPushMessageRequest>) -> Result<()> {
        if payloads.is_empty() {
            return Ok(());
        }

        // 批量编码和构建记录
        // 先编码所有 payload，保存到 Vec 中以保持生命周期
        let mut encoded_payloads = Vec::with_capacity(payloads.len());
        let mut valid_indices = Vec::new();

        for (idx, payload) in payloads.iter().enumerate() {
            let encoded = payload.encode_to_vec();

            // 验证消息大小
            const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024; // 10MB
            if encoded.len() > MAX_MESSAGE_SIZE {
                tracing::error!(
                    payload_size = encoded.len(),
                    max_size = MAX_MESSAGE_SIZE,
                    "Push message size exceeds maximum allowed size"
                );
                continue; // 跳过过大的消息
            }

            encoded_payloads.push(encoded);
            valid_indices.push(idx);
        }

        if encoded_payloads.is_empty() {
            return Ok(());
        }

        // 构建记录（借用 encoded_payloads）
        let records: Vec<_> = valid_indices
            .iter()
            .enumerate()
            .map(|(encoded_idx, &payload_idx)| {
                let key = payloads[payload_idx]
                    .user_ids
                    .first()
                    .map(|s| s.as_str())
                    .unwrap_or("");
                FutureRecord::to(&self.config.kafka_push_topic)
                    .payload(&encoded_payloads[encoded_idx])
                    .key(key)
            })
            .collect();

        // 批量发送（并发执行）
        let futures: Vec<_> = records
            .into_iter()
            .map(|record| {
                self.producer
                    .send(record, Duration::from_millis(self.config.kafka_timeout_ms))
                    .map(|result| result.map_err(|(err, _)| anyhow!("Kafka send error: {}", err)))
            })
            .collect();

        // 等待所有发送完成（并发执行）
        if let Err(e) = try_join_all(futures).await {
            return Err(FlareError::system(&format!("Kafka batch send error: {}", e)));
        }

        tracing::info!(
            topic = %self.config.kafka_push_topic,
            batch_size = payloads.len(),
            "Successfully published batch of push messages to Kafka"
        );

        Ok(())
    }

    /// 立即刷新缓冲区（用于关键消息）
    pub async fn flush(&self) -> Result<()> {
        // 刷新存储消息
        let storage_messages = {
            let mut buffer = self.storage_buffer.lock().await;
            if !buffer.is_empty() {
                let messages = buffer.drain(..).collect();
                drop(buffer);
                Some(messages)
            } else {
                None
            }
        };

        if let Some(messages) = storage_messages {
            self.publish_storage_batch(messages).await?;
        }

        // 刷新操作消息
        let operation_messages = {
            let mut buffer = self.operation_buffer.lock().await;
            if !buffer.is_empty() {
                let messages = buffer.drain(..).collect();
                drop(buffer);
                Some(messages)
            } else {
                None
            }
        };

        if let Some(messages) = operation_messages {
            self.publish_operation_batch(messages).await?;
        }

        // 刷新推送消息
        let push_messages = {
            let mut buffer = self.push_buffer.lock().await;
            if !buffer.is_empty() {
                let messages = buffer.drain(..).collect();
                drop(buffer);
                Some(messages)
            } else {
                None
            }
        };

        if let Some(messages) = push_messages {
            self.publish_push_batch(messages).await?;
        }

        *self.last_flush_time.lock().await = std::time::Instant::now();

        Ok(())
    }
}

impl MessageEventPublisher for KafkaMessagePublisher {
    fn publish_storage(
        &self,
        payload: StorageStoreMessageRequest,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            // 添加到缓冲区
            let should_flush = {
                let mut buffer = self.storage_buffer.lock().await;
                buffer.push(payload);
                buffer.len() >= self.config.kafka_batch_size
            };

            // 如果缓冲区已满，立即刷新
            if should_flush {
                let messages: Vec<StorageStoreMessageRequest> = {
                    let mut buffer = self.storage_buffer.lock().await;
                    buffer.drain(..).collect()
                };
                self.publish_storage_batch(messages).await?;
                *self.last_flush_time.lock().await = std::time::Instant::now();
            }

            Ok(())
        })
    }

    fn publish_operation(
        &self,
        payload: StorageStoreMessageRequest,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            // 添加到缓冲区
            let should_flush = {
                let mut buffer = self.operation_buffer.lock().await;
                buffer.push(payload);
                buffer.len() >= self.config.kafka_batch_size
            };

            // 如果缓冲区已满，立即刷新
            if should_flush {
                let messages: Vec<StorageStoreMessageRequest> = {
                    let mut buffer = self.operation_buffer.lock().await;
                    buffer.drain(..).collect()
                };
                self.publish_operation_batch(messages).await?;
                *self.last_flush_time.lock().await = std::time::Instant::now();
            }

            Ok(())
        })
    }

    fn publish_push(
        &self,
        payload: PushPushMessageRequest,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            // 添加到缓冲区
            let should_flush = {
                let mut buffer = self.push_buffer.lock().await;
                buffer.push(payload);
                buffer.len() >= self.config.kafka_batch_size
            };

            // 如果缓冲区已满，立即刷新
            if should_flush {
                let messages: Vec<PushPushMessageRequest> = {
                    let mut buffer = self.push_buffer.lock().await;
                    buffer.drain(..).collect()
                };
                self.publish_push_batch(messages).await?;
            }

            Ok(())
        })
    }

    fn publish_both(
        &self,
        storage_payload: StorageStoreMessageRequest,
        push_payload: PushPushMessageRequest,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            // 并行添加到缓冲区
            let (storage_should_flush, push_should_flush) = {
                let mut storage_buffer = self.storage_buffer.lock().await;
                let mut push_buffer = self.push_buffer.lock().await;

                storage_buffer.push(storage_payload);
                push_buffer.push(push_payload);

                (
                    storage_buffer.len() >= self.config.kafka_batch_size,
                    push_buffer.len() >= self.config.kafka_batch_size,
                )
            };

            // 如果任一缓冲区已满，刷新所有缓冲区
            if storage_should_flush || push_should_flush {
                self.flush().await?;
            }

            Ok(())
        })
    }
}
