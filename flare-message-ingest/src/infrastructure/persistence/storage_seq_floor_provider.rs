//! 基于 storage-reader 的 [`SeqFloorProvider`] 实现。
//!
//! 通过 `StorageReaderService.GetConversationMessageHead` 读取会话在**消息存储中的权威
//! `max_seq`**，作为 seq 分配的 floor 下限（自愈 Redis 高水位丢失导致的 seq 回退）。
//! 选用消息存储的权威 MAX(seq) 而非会话服务的异步投影，避免投影滞后导致 floor 偏低撞号。

use std::future::Future;
use std::pin::Pin;

use flare_grpc_proto::storage::GetConversationMessageHeadRequest;
use flare_im_contracts::Ctx;
use flare_im_service_kit::clients::StorageReaderServiceClientWrapper;
use flare_server_core::error::Result;
use tonic::transport::Channel;

use crate::domain::repository::SeqFloorProvider;

/// storage-reader 支撑的会话持久化 max_seq 提供者。
#[derive(Clone)]
pub struct StorageSeqFloorProvider {
    client: StorageReaderServiceClientWrapper,
}

impl StorageSeqFloorProvider {
    /// 用已连接（可懒连接）的 storage-reader gRPC channel 构造。
    pub fn from_channel(channel: Channel) -> Self {
        Self {
            client: StorageReaderServiceClientWrapper::from_channel(channel),
        }
    }
}

impl SeqFloorProvider for StorageSeqFloorProvider {
    fn persisted_max_seq<'a>(
        &'a self,
        ctx: &'a Ctx,
        tenant_id: &'a str,
        conversation_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<u64>> + Send + 'a>> {
        let mut client = self.client.clone();
        let conversation_id = conversation_id.to_string();
        Box::pin(async move {
            // reader 侧从 ctx metadata 还原租户：显式参数与 ctx 不一致时以参数为准
            // 派生子 ctx，避免双通道分歧导致 floor 查错租户静默失效。
            let ctx_for_rpc: Ctx = if ctx.tenant_id() == Some(tenant_id) {
                ctx.clone()
            } else {
                std::sync::Arc::new(ctx.with_tenant_id(tenant_id))
            };
            let response = client
                .get_conversation_message_head_with_ctx(
                    &ctx_for_rpc,
                    GetConversationMessageHeadRequest { conversation_id },
                )
                .await?;
            // max_seq 为 i64（无历史时为 0 或负值兜底），转成非负 u64 高水位。
            Ok(response.max_seq.max(0) as u64)
        })
    }
}
