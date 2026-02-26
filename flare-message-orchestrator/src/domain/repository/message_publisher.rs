use flare_proto::push::PushMessageRequest as PushPushMessageRequest;
use flare_proto::storage::StoreMessage as StorageStoreMessageRequest;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use crate::error::Result;

use crate::domain::model::MessageSubmission;

/// 消息事件发布器（Rust 2024: 原生异步 trait）
pub trait MessageEventPublisher: Send + Sync {
    /// 发布消息到存储队列 (flare.im.message.created)
    fn publish_storage(
        &self,
        payload: StorageStoreMessageRequest,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;

    /// 发布操作消息到操作队列 (storage-message-operations)
    fn publish_operation(
        &self,
        payload: StorageStoreMessageRequest,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;

    /// 发布推送任务到推送队列 (flare.im.push.tasks)
    fn publish_push(
        &self,
        payload: PushPushMessageRequest,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;

    /// 并行发布到存储队列和推送队列（仅普通消息）
    fn publish_both(
        &self,
        storage_payload: StorageStoreMessageRequest,
        push_payload: PushPushMessageRequest,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
}