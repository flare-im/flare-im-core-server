//! Publishes the rows of Core's tenant notice outbox (see
//! [`crate::domain::repository::tenant_notice_outbox`]). Delivery is at least once and the notice
//! channel drops a version it already saw, so any replica may run this and a retry says nothing
//! twice. Without a notice channel this worker does not run and rows wait.
use std::sync::Arc;
use std::time::Duration;

use crate::domain::repository::tenant_notice::TenantNotices;
use crate::domain::repository::tenant_notice_outbox::TenantNoticeOutbox;

/// Rows claimed per round.
const BATCH: u32 = 64;
/// How long a claimed row stays invisible to other workers.
const LEASE: Duration = Duration::from_secs(30);
/// Pause between rounds that found nothing to do; bounds the delay of a notice after its commit.
const IDLE: Duration = Duration::from_secs(1);
/// First retry delay; doubles per attempt up to [`MAX_BACKOFF`].
const FIRST_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(60);

pub fn backoff(attempts: i32) -> Duration {
    let exponent = attempts.clamp(1, 20) as u32 - 1;
    FIRST_BACKOFF
        .saturating_mul(2u32.saturating_pow(exponent))
        .min(MAX_BACKOFF)
}

pub struct TenantNoticeRelay {
    outbox: Arc<dyn TenantNoticeOutbox>,
    notices: Arc<dyn TenantNotices>,
}

impl TenantNoticeRelay {
    pub fn new(outbox: Arc<dyn TenantNoticeOutbox>, notices: Arc<dyn TenantNotices>) -> Self {
        Self { outbox, notices }
    }

    /// Publish until `shutdown` resolves.
    pub async fn run(self, shutdown: impl std::future::Future<Output = ()>) {
        tokio::pin!(shutdown);
        loop {
            let claimed = self.publish_due().await;
            let pause = if claimed < BATCH as usize {
                IDLE
            } else {
                Duration::ZERO
            };
            tokio::select! {
                _ = &mut shutdown => return,
                _ = tokio::time::sleep(pause) => {}
            }
        }
    }

    /// One round: claim due rows, publish each, end or reschedule it. Returns the rows claimed.
    pub async fn publish_due(&self) -> usize {
        let pending = match self.outbox.claim_due(BATCH, LEASE).await {
            Ok(pending) => pending,
            Err(error) => {
                tracing::warn!(%error, "tenant notice outbox unavailable");
                return 0;
            }
        };
        let claimed = pending.len();
        for notice in pending {
            let stored = match self
                .notices
                .publish(&notice.tenant_id, &notice.topic, notice.version)
                .await
            {
                // A version the channel already saw needs nothing more either.
                Ok(outcome) => {
                    tracing::info!(
                        tenant_id = %notice.tenant_id,
                        topic = %notice.topic,
                        version = notice.version,
                        accepted = outcome.accepted,
                        "tenant notice published"
                    );
                    self.outbox.complete(&notice).await
                }
                Err(error) => {
                    tracing::debug!(tenant_id = %notice.tenant_id, topic = %notice.topic, attempts = notice.attempts, %error, "tenant notice retried later");
                    self.outbox
                        .retry_after(&notice, backoff(notice.attempts), &error.to_string())
                        .await
                }
            };
            if let Err(error) = stored {
                tracing::warn!(%error, tenant_id = %notice.tenant_id, topic = %notice.topic, "tenant notice row not updated; its lease hands it back");
            }
        }
        claimed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::model::NoticeOutcome;
    use crate::domain::repository::tenant_notice_outbox::PendingTenantNotice;
    use flare_server_core::error::{FlareError, Result};
    use std::sync::Mutex;

    #[derive(Default)]
    struct Rows {
        pending: Mutex<Vec<PendingTenantNotice>>,
        completed: Mutex<Vec<(String, u64)>>,
        retried: Mutex<Vec<(String, Duration)>>,
    }

    #[async_trait::async_trait]
    impl TenantNoticeOutbox for Rows {
        async fn claim_due(&self, _: u32, _: Duration) -> Result<Vec<PendingTenantNotice>> {
            Ok(std::mem::take(&mut *self.pending.lock().unwrap()))
        }
        async fn complete(&self, notice: &PendingTenantNotice) -> Result<()> {
            self.completed
                .lock()
                .unwrap()
                .push((notice.topic.clone(), notice.version));
            Ok(())
        }
        async fn retry_after(
            &self,
            notice: &PendingTenantNotice,
            delay: Duration,
            _: &str,
        ) -> Result<()> {
            self.retried
                .lock()
                .unwrap()
                .push((notice.topic.clone(), delay));
            Ok(())
        }
    }

    /// `flare.release` fails; anything else is published, or dropped as already seen when its
    /// version is 1.
    struct Channel;

    #[async_trait::async_trait]
    impl TenantNotices for Channel {
        async fn publish(&self, _: &str, topic: &str, version: u64) -> Result<NoticeOutcome> {
            if topic == "flare.release" {
                return Err(FlareError::system("channel down"));
            }
            Ok(NoticeOutcome {
                accepted: version > 1,
                current_version: version,
            })
        }
    }

    fn pending(topic: &str, version: u64, attempts: i32) -> PendingTenantNotice {
        PendingTenantNotice {
            tenant_id: "app-a".into(),
            topic: topic.into(),
            version,
            attempts,
        }
    }

    #[tokio::test]
    async fn published_rows_end_and_failed_ones_come_back_later() {
        let rows = Arc::new(Rows::default());
        *rows.pending.lock().unwrap() = vec![
            pending("flare.access", 7, 1),
            pending("flare.access", 1, 1),
            pending("flare.release", 3, 3),
        ];
        let relay = TenantNoticeRelay::new(rows.clone(), Arc::new(Channel));
        assert_eq!(relay.publish_due().await, 3);
        assert_eq!(
            *rows.completed.lock().unwrap(),
            vec![
                ("flare.access".to_string(), 7),
                ("flare.access".to_string(), 1)
            ]
        );
        assert_eq!(
            *rows.retried.lock().unwrap(),
            vec![("flare.release".to_string(), Duration::from_secs(4))]
        );
    }

    #[test]
    fn backoff_doubles_up_to_the_cap() {
        assert_eq!(backoff(1), Duration::from_secs(1));
        assert_eq!(backoff(3), Duration::from_secs(4));
        assert_eq!(backoff(20), MAX_BACKOFF);
    }
}
