//! The attested application's identity selects its own provider credentials.
use crate::application::binding_delivery::DeviceBindingsDelivery;
use crate::domain::offline_delivery::{DeviceTokenRepository, OfflinePushExecutor};
use flare_im_contracts::{Ctx, DevicePushToken};
use flare_proto::PushTaskEnvelope;
use flare_server_core::{ErrorCode, FlareError};
use std::{collections::HashMap, sync::Arc};

pub struct ApplicationDelivery {
    /// Per application: its vendor adapters behind the governed binding selection.
    pub applications: HashMap<String, Arc<dyn DeviceBindingsDelivery>>,
    pub tokens: Arc<dyn DeviceTokenRepository>,
}

impl ApplicationDelivery {
    /// Deliver to the bindings the caller already listed for the task's device (listed once
    /// per task).
    pub async fn deliver_listed(
        &self,
        ctx: &Ctx,
        envelope: &PushTaskEnvelope,
        registered: Vec<DevicePushToken>,
    ) -> Result<(), FlareError> {
        let Some(device) = self.applications.get(&envelope.tenant_id) else {
            // A device whose binding is gone by now needs no vendor at all.
            // A missing credential for an existing registration is an explicit failure.
            if registered.is_empty() {
                return Ok(());
            }
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "push application credentials not configured",
            ));
        };
        if registered.is_empty() {
            return Ok(());
        }
        // One binding takes the task; a registered provider without credentials is never
        // acknowledged as if the device had been handled (see `binding_delivery`). Per-binding
        // receipts protect an already accepted binding if the operator later replays the DLQ.
        device.deliver_bindings(ctx, envelope, registered).await
    }
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
        // Only the target device's bindings matter: the task was planned for that device, and
        // the user's other devices each get a task of their own.
        let registered = self
            .tokens
            .list_device_tokens(
                ctx,
                &envelope.tenant_id,
                &envelope.user_id,
                &envelope.device_id,
            )
            .await?;
        self.deliver_listed(ctx, envelope, registered).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Backend(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl DeviceBindingsDelivery for Backend {
        async fn deliver_bindings(
            &self,
            _: &Ctx,
            _: &PushTaskEnvelope,
            bindings: Vec<DevicePushToken>,
        ) -> Result<(), FlareError> {
            assert!(
                !bindings.is_empty(),
                "a device without bindings needs no vendor"
            );
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    #[tokio::test]
    async fn no_fallback_or_cross_application_dispatch() {
        let a = Arc::new(AtomicUsize::new(0));
        let b = Arc::new(AtomicUsize::new(0));
        let tokens = Arc::new(crate::infrastructure::push_test_support::Tokens::default());
        tokens
            .rows
            .lock()
            .unwrap()
            .push(crate::infrastructure::push_test_support::token(
                "registered-device",
            ));
        let deliveries = ApplicationDelivery {
            applications: HashMap::from([
                (
                    "app-a".to_string(),
                    Arc::new(Backend(a.clone())) as Arc<dyn DeviceBindingsDelivery>,
                ),
                (
                    "app-b".to_string(),
                    Arc::new(Backend(b.clone())) as Arc<dyn DeviceBindingsDelivery>,
                ),
            ]),
            tokens: tokens.clone(),
        };
        let mut envelope = PushTaskEnvelope {
            tenant_id: "app-a".into(),
            device_id: "registered-device".into(),
            ..Default::default()
        };
        let ctx = Arc::new(Ctx::default().with_tenant_id("app-a"));
        deliveries.deliver(&ctx, &envelope).await.unwrap();
        envelope.tenant_id = "app-b".into();
        assert!(deliveries.deliver(&ctx, &envelope).await.is_err());
        envelope.tenant_id = "missing".into();
        let missing = Arc::new(Ctx::default().with_tenant_id("missing"));
        // A registered device of an application without credentials is not "handled".
        assert!(deliveries.deliver(&missing, &envelope).await.is_err());
        // Another device's registration is not this task's concern.
        envelope.device_id = "other-device".into();
        assert!(deliveries.deliver(&missing, &envelope).await.is_ok());
        assert_eq!(a.load(Ordering::SeqCst), 1);
        assert_eq!(b.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_device_without_bindings_reaches_no_vendor() {
        let calls = Arc::new(AtomicUsize::new(0));
        let deliveries = ApplicationDelivery {
            applications: HashMap::from([(
                "t1".to_string(),
                Arc::new(Backend(calls.clone())) as Arc<dyn DeviceBindingsDelivery>,
            )]),
            tokens: Arc::new(crate::infrastructure::push_test_support::Tokens::default()),
        };
        let ctx = Arc::new(Ctx::default().with_tenant_id("t1"));
        let envelope = PushTaskEnvelope {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            device_id: "unbound-device".into(),
            ..Default::default()
        };
        deliveries.deliver(&ctx, &envelope).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
