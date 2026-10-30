//! The linked router answers through Core's admission: a verified service request reaches
//! Core's projection, and Core's answer decides between a refusal and an unknown outcome.
use std::sync::Arc;

use flare_capability::application::handler::service_directory::ServiceDirectory;
use flare_capability::capability_core::plugin::{
    invocation::{
        PluginCallSigner, PluginCallVerifier, PluginRuntimeClaims, PluginRuntimeSigner,
        PluginRuntimeVerifier,
    },
    resource::HostCapability,
    runtime::{PluginRuntimeSnapshot, digest_bytes},
};
use flare_capability::composition::{ServiceDispatchFactory, ServiceDispatchParts};
use flare_capability::domain::capability::Result as CapabilityResult;
use flare_capability::domain::repository::PluginAuthorizationReader;
use flare_capability::domain::repository::resource_access::ResourceCallBudget;
use flare_capability::domain::repository::service_dispatch::{
    PluginServiceDispatch, ServiceDispatchError,
};
use flare_capability::infrastructure::capability::InMemoryCapabilityGrants;
use flare_capability_with_plugin_host::PluginHostServiceRouter;
use flare_server_core::error::{FlareError, Result};

/// `None`: nothing installed for the tenant. `Some(())`: the projection store is down.
struct Projection(Option<()>);
#[async_trait::async_trait]
impl PluginAuthorizationReader for Projection {
    async fn plugin_snapshot(&self, _: &str) -> Result<Option<PluginRuntimeSnapshot>> {
        match self.0 {
            None => Ok(None),
            Some(()) => Err(FlareError::system("projection store down")),
        }
    }
}
struct Budget;
#[async_trait::async_trait]
impl ResourceCallBudget for Budget {
    async fn reserve(&self, _: &str, _: &str, _: &str, _: &str, _: i64) -> CapabilityResult<bool> {
        Ok(true)
    }
}

const REQUEST: &[u8] = br#"{"capability":"plugin.service.dispatch","targetPluginId":"flare.wallet","operation":"wallet.balance","businessRef":"ref-1","mode":"recovery","input":{}}"#;

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn router(projection: Projection) -> (Arc<dyn PluginServiceDispatch>, String) {
    let runtime = PluginRuntimeSigner::new("rt".into(), [9; 32]).unwrap();
    let mut runtimes = PluginRuntimeVerifier::default();
    runtimes
        .insert("rt".into(), "dep-1".into(), runtime.public_key())
        .unwrap();
    let token = runtime
        .sign(
            &PluginRuntimeClaims {
                tenant_id: "app-a".into(),
                plugin_id: "flare.redpacket".into(),
                plugin_version: "0.7.5".into(),
                manifest_digest: digest_bytes(b"red packet manifest"),
                deployment_id: "dep-1".into(),
                grant_id: "grant-1".into(),
                grant_revision: 1,
                capability: HostCapability::ServiceDispatch,
                request_id: "req-1".into(),
                request_digest: digest_bytes(REQUEST),
                issued_at_ms: now(),
                expires_at_ms: now() + 30_000,
            },
            now(),
        )
        .unwrap();
    let router = PluginHostServiceRouter.build(ServiceDispatchParts {
        directory: Arc::new(ServiceDirectory::new(
            Arc::new(projection),
            Arc::new(InMemoryCapabilityGrants::new()),
            Arc::new(Budget),
        )),
        calls: PluginCallVerifier::default(),
        runtimes,
        signer: Arc::new(PluginCallSigner::new("call".into(), [8; 32]).unwrap()),
    });
    (router, token)
}

#[tokio::test]
async fn a_tenant_without_a_projection_is_refused() {
    let (router, token) = router(Projection(None));
    assert_eq!(
        router.dispatch(&token, "req-1", REQUEST).await.unwrap_err(),
        ServiceDispatchError::Refused
    );
}

#[tokio::test]
async fn an_unreachable_projection_leaves_the_outcome_unknown() {
    let (router, token) = router(Projection(Some(())));
    assert_eq!(
        router.dispatch(&token, "req-1", REQUEST).await.unwrap_err(),
        ServiceDispatchError::OutcomeUnknown
    );
}

#[tokio::test]
async fn a_forged_or_mismatched_credential_is_refused_before_core_is_asked() {
    let (router, token) = router(Projection(Some(())));
    // A store outage would be an unknown outcome; these never get that far.
    for (token, request_id) in [("not-a-token", "req-1"), (token.as_str(), "req-2")] {
        assert_eq!(
            router
                .dispatch(token, request_id, REQUEST)
                .await
                .unwrap_err(),
            ServiceDispatchError::Refused
        );
    }
}
