//! 同步领域服务（DATA `DataPacket` / `sync_request`）
//!
//! 网关 **零解析**：补全 `device_id` 后透传 [`ISyncPort::forward_sync`]；语义校验与 `SyncRes` 组装全部由 sync-orchestrator 负责。

use std::sync::Arc;

use flare_core::common::ErrorCode;
use flare_core::common::error::{FlareError, Result};
use flare_im_contracts::Ctx;
use flare_proto::common::sync::Payload as SyncPayload;
use flare_proto::common::{Sync, SyncRes};

use crate::domain::ports::ISyncPort;
use crate::domain::service::SyncPullLimiter;

pub struct SyncService {
    port: Arc<dyn ISyncPort>,
    pull_limiter: Option<Arc<SyncPullLimiter>>,
}

impl SyncService {
    pub fn new(port: Arc<dyn ISyncPort>) -> Self {
        Self {
            port,
            pull_limiter: None,
        }
    }

    pub fn with_pull_limiter(mut self, limiter: Arc<SyncPullLimiter>) -> Self {
        self.pull_limiter = Some(limiter);
        self
    }

    /// 处理同步：仅连接态补全 → 下游返回 `SyncRes`；传输/RPC 失败返回领域错误。
    pub async fn execute(&self, tx: &Ctx, connection_id: &str, mut sync: Sync) -> Result<SyncRes> {
        if sync.device_id.is_empty() {
            sync.device_id = tx.device_id().map(str::to_string).unwrap_or_default();
        }
        if is_pull_sync(&sync)
            && let Some(limiter) = &self.pull_limiter
        {
            let tenant_id = tx.tenant_id().unwrap_or("0");
            let user_id = tx.user_id().unwrap_or(connection_id);
            if !limiter.try_acquire(tenant_id, user_id).await {
                return Err(FlareError::localized(
                    ErrorCode::MessageRateLimitExceeded,
                    "sync pull rate limit exceeded",
                ));
            }
        }

        self.port
            .forward_sync(tx, sync)
            .await
            .map_err(|e| FlareError::system(format!("sync forward: {e}")))
    }
}

fn is_pull_sync(sync: &Sync) -> bool {
    matches!(
        sync.payload.as_ref(),
        Some(SyncPayload::SingleConversation(_))
            | Some(SyncPayload::MultiConversation(_))
            | Some(SyncPayload::ConversationsIncremental(_))
            | Some(SyncPayload::ConversationsAll(_))
            | Some(SyncPayload::ConversationDetail(_))
            | Some(SyncPayload::QueryEvents(_))
            | Some(SyncPayload::SyncSnapshot(_))
            | Some(SyncPayload::ConversationMaxSeq(_))
            | Some(SyncPayload::Conversations(_))
            | Some(SyncPayload::ConversationParticipants(_))
            | Some(SyncPayload::ConversationUserSettings(_))
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::domain::service::SyncPullRateLimitConfig;
    use async_trait::async_trait;
    use flare_proto::common::{EventStreamAckSync, SingleConversationSync};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct CountingSyncPort {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl ISyncPort for CountingSyncPort {
        async fn forward_sync(
            &self,
            _tx: &Ctx,
            _sync: Sync,
        ) -> flare_server_core::error::Result<SyncRes> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(SyncRes::default())
        }
    }

    fn context() -> Ctx {
        Arc::new(
            flare_server_core::Context::root()
                .with_tenant_id("t1")
                .with_user_id("u1")
                .with_device_id("d1"),
        )
    }

    #[tokio::test]
    async fn pull_sync_is_rate_limited_before_forwarding() {
        let port = Arc::new(CountingSyncPort::default());
        let sync_port: Arc<dyn ISyncPort> = port.clone();
        let service = SyncService::new(sync_port).with_pull_limiter(Arc::new(
            SyncPullLimiter::new(SyncPullRateLimitConfig {
                enabled: true,
                user_requests_per_second: 1,
                user_burst: 1,
                tenant_requests_per_second: 100,
                tenant_burst: 100,
            }),
        ));
        let sync = Sync {
            device_id: String::new(),
            payload: Some(SyncPayload::SingleConversation(
                SingleConversationSync::default(),
            )),
        };

        service
            .execute(&context(), "conn-1", sync.clone())
            .await
            .expect("first pull should pass");
        let error = service
            .execute(&context(), "conn-1", sync)
            .await
            .expect_err("second pull should be rate limited");

        assert!(error.to_string().contains("sync pull rate limit"));
        assert_eq!(port.calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn event_stream_ack_bypasses_pull_rate_limit() {
        let port = Arc::new(CountingSyncPort::default());
        let sync_port: Arc<dyn ISyncPort> = port.clone();
        let service = SyncService::new(sync_port).with_pull_limiter(Arc::new(
            SyncPullLimiter::new(SyncPullRateLimitConfig {
                enabled: true,
                user_requests_per_second: 1,
                user_burst: 1,
                tenant_requests_per_second: 1,
                tenant_burst: 1,
            }),
        ));
        let sync = Sync {
            device_id: String::new(),
            payload: Some(SyncPayload::EventStreamAck(EventStreamAckSync::default())),
        };

        service
            .execute(&context(), "conn-1", sync.clone())
            .await
            .expect("first ack should pass");
        service
            .execute(&context(), "conn-1", sync)
            .await
            .expect("ack should not consume pull quota");

        assert_eq!(port.calls.load(Ordering::Relaxed), 2);
    }
}
