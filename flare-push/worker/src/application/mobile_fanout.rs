//! Explicit configured vendor fanout. Each backend owns device-level acceptance receipts.
use crate::domain::offline_delivery::OfflinePushExecutor;
use flare_im_contracts::Ctx;
use flare_proto::PushTaskEnvelope;
use flare_server_core::FlareError;
use std::sync::Arc;

pub struct MobileFanout(pub Vec<Arc<dyn OfflinePushExecutor>>);
#[async_trait::async_trait]
impl OfflinePushExecutor for MobileFanout {
    async fn deliver(&self, ctx: &Ctx, envelope: &PushTaskEnvelope) -> Result<(), FlareError> {
        let mut failure = None;
        // At most three configured vendors. One unreachable vendor cannot starve another.
        for result in
            futures::future::join_all(self.0.iter().map(|backend| backend.deliver(ctx, envelope)))
                .await
        {
            if let Err(error) = result
                && failure
                    .as_ref()
                    .is_none_or(|previous: &FlareError| !previous.is_retryable())
            {
                failure = Some(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Backend {
        calls: Arc<AtomicUsize>,
        fail: bool,
    }
    #[async_trait::async_trait]
    impl OfflinePushExecutor for Backend {
        async fn deliver(&self, _: &Ctx, _: &PushTaskEnvelope) -> Result<(), FlareError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                Err(FlareError::localized(
                    flare_server_core::ErrorCode::ServiceUnavailable,
                    "controlled outage",
                ))
            } else {
                Ok(())
            }
        }
    }
    #[tokio::test]
    async fn a_failed_vendor_does_not_skip_other_vendor_and_cannot_ack_task() {
        let calls = Arc::new(AtomicUsize::new(0));
        let fanout = MobileFanout(vec![
            Arc::new(Backend {
                calls: calls.clone(),
                fail: true,
            }),
            Arc::new(Backend {
                calls: calls.clone(),
                fail: false,
            }),
        ]);
        assert!(
            fanout
                .deliver(&Ctx::default(), &PushTaskEnvelope::default())
                .await
                .unwrap_err()
                .is_retryable()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
