//! The attested application's identity selects its own provider credentials.
use crate::domain::offline_delivery::{DeviceTokenRepository, OfflinePushExecutor};
use flare_im_contracts::Ctx;
use flare_proto::PushTaskEnvelope;
use flare_server_core::{ErrorCode, FlareError};
use std::{collections::HashMap, sync::Arc};

pub struct ApplicationDelivery {
    pub applications: HashMap<String, ApplicationChannels>,
    pub tokens: Arc<dyn DeviceTokenRepository>,
}
pub struct ApplicationChannels {
    pub providers: Vec<&'static str>,
    pub executor: Arc<dyn OfflinePushExecutor>,
}
#[async_trait::async_trait]
impl OfflinePushExecutor for ApplicationDelivery {
    async fn deliver(&self, ctx: &Ctx, envelope: &PushTaskEnvelope) -> Result<(), FlareError> {
        if ctx.tenant_id() != Some(envelope.tenant_id.as_str()) {
            return Err(FlareError::localized(
                ErrorCode::PermissionDenied,
                "push application context mismatch",
            ));
        }
        let registered = self
            .tokens
            .list_user_tokens(ctx, &envelope.tenant_id, &envelope.user_id)
            .await?;
        let Some(channels) = self.applications.get(&envelope.tenant_id) else {
            // Web-only users with no OS registration need no vendor at all.
            // A missing credential for an existing registration is an explicit failure.
            if registered.is_empty() {
                return Ok(());
            }
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "push application credentials not configured",
            ));
        };
        // Deliver configured channels, but never ACK a missing registered channel
        // as if every device had been handled. Existing per-registration receipts
        // protect already accepted devices if the operator later replays the DLQ.
        let missing = registered
            .iter()
            .any(|token| !channels.providers.contains(&token.provider.as_str()));
        channels.executor.deliver(ctx, envelope).await?;
        if missing {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "push application provider credentials not configured",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Backend(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl OfflinePushExecutor for Backend {
        async fn deliver(&self, _: &Ctx, _: &PushTaskEnvelope) -> Result<(), FlareError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    #[tokio::test]
    async fn no_fallback_or_cross_application_dispatch() {
        let a = Arc::new(AtomicUsize::new(0));
        let b = Arc::new(AtomicUsize::new(0));
        let tokens = Arc::new(crate::infrastructure::push_test_support::Tokens::default());
        let deliveries = ApplicationDelivery {
            applications: HashMap::from([
                (
                    "app-a".into(),
                    ApplicationChannels {
                        providers: vec!["fcm"],
                        executor: Arc::new(Backend(a.clone())),
                    },
                ),
                (
                    "app-b".into(),
                    ApplicationChannels {
                        providers: vec!["apns"],
                        executor: Arc::new(Backend(b.clone())),
                    },
                ),
            ]),
            tokens: tokens.clone(),
        };
        let mut envelope = PushTaskEnvelope {
            tenant_id: "app-a".into(),
            ..Default::default()
        };
        let ctx = Arc::new(Ctx::default().with_tenant_id("app-a"));
        deliveries.deliver(&ctx, &envelope).await.unwrap();
        envelope.tenant_id = "app-b".into();
        assert!(deliveries.deliver(&ctx, &envelope).await.is_err());
        envelope.tenant_id = "missing".into();
        let missing = Arc::new(Ctx::default().with_tenant_id("missing"));
        assert!(deliveries.deliver(&missing, &envelope).await.is_ok());
        tokens
            .rows
            .lock()
            .unwrap()
            .push(crate::infrastructure::push_test_support::token(
                "registered-device",
            ));
        assert!(deliveries.deliver(&missing, &envelope).await.is_err());
        assert_eq!(a.load(Ordering::SeqCst), 1);
        assert_eq!(b.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn missing_registered_provider_is_not_a_silent_success() {
        let calls = Arc::new(AtomicUsize::new(0));
        let tokens = Arc::new(crate::infrastructure::push_test_support::Tokens::default());
        tokens
            .rows
            .lock()
            .unwrap()
            .push(crate::infrastructure::push_test_support::token(
                "fcm-device",
            ));
        let deliveries = ApplicationDelivery {
            applications: HashMap::from([(
                "t1".into(),
                ApplicationChannels {
                    providers: vec!["apns"],
                    executor: Arc::new(Backend(calls.clone())),
                },
            )]),
            tokens,
        };
        let ctx = Arc::new(Ctx::default().with_tenant_id("t1"));
        let envelope = PushTaskEnvelope {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            ..Default::default()
        };
        assert!(deliveries.deliver(&ctx, &envelope).await.is_err());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "configured channels still run"
        );
    }
}
