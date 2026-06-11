//! 离线推送 Redis Stream outbox
//!
//! 厂商通道（APNs/FCM 等）接入前的持久化暂存：任务以 XADD 追加到 Stream，
//! 由后续 vendor adapter / 运营工具以消费组方式取走。写入失败返回可重试
//! 错误（ServiceUnavailable），由 JetStream 重投保证任务不丢。

use flare_im_contracts::Ctx;
use flare_proto::PushTaskEnvelope;
use flare_server_core::error::{ErrorCode, FlareError};
use flare_server_core::flare_err;
use prost::Message as _;
use redis::aio::ConnectionManager;

use crate::interface::messaging::offline_consumer::OfflinePushExecutor;

pub struct RedisOfflineOutbox {
    conn: ConnectionManager,
    stream_key: String,
    maxlen: usize,
}

impl RedisOfflineOutbox {
    /// 建立带自动重连的 Redis 连接。失败返回可重试错误，由 wire 决定回退策略。
    pub async fn connect(
        redis_url: &str,
        stream_key: String,
        maxlen: usize,
    ) -> Result<Self, FlareError> {
        let client = redis::Client::open(redis_url)
            .map_err(|e| FlareError::system(format!("offline outbox redis url invalid: {e}")))?;
        let conn = client.get_connection_manager().await.map_err(|e| {
            flare_err!(
                ErrorCode::ServiceUnavailable,
                &format!("offline outbox redis connect: {e}")
            )
        })?;
        Ok(Self {
            conn,
            stream_key,
            maxlen,
        })
    }
}

#[async_trait::async_trait]
impl OfflinePushExecutor for RedisOfflineOutbox {
    async fn deliver(&self, ctx: &Ctx, envelope: &PushTaskEnvelope) -> Result<(), FlareError> {
        let mut conn = self.conn.clone();
        let mut cmd = redis::cmd("XADD");
        cmd.arg(&self.stream_key)
            .arg("MAXLEN")
            .arg("~")
            .arg(self.maxlen)
            .arg("*")
            .arg("tenant_id")
            .arg(&envelope.tenant_id)
            .arg("user_id")
            .arg(&envelope.user_id)
            .arg("message_id")
            .arg(&envelope.message_id)
            .arg("conversation_id")
            .arg(&envelope.conversation_id)
            .arg("payload_kind")
            .arg(envelope.payload_kind)
            .arg("priority")
            .arg(envelope.priority)
            .arg("trace_id")
            .arg(ctx.trace_id());
        if let Some(expire_at) = envelope.expire_at {
            cmd.arg("expire_at").arg(expire_at);
        }
        // 完整信封字节，vendor adapter 解码后可拿到 push_payload/headers 等全部信息
        cmd.arg("envelope").arg(envelope.encode_to_vec());

        let entry_id: String = cmd.query_async(&mut conn).await.map_err(|e| {
            flare_err!(
                ErrorCode::ServiceUnavailable,
                &format!("offline outbox XADD: {e}")
            )
        })?;

        tracing::debug!(
            stream = %self.stream_key,
            entry_id = %entry_id,
            user_id = %envelope.user_id,
            message_id = %envelope.message_id,
            "offline push task appended to outbox"
        );
        Ok(())
    }
}
