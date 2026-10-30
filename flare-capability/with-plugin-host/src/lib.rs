//! Links the plugin service router of `flare-plugin-host` into the Capability service.
//!
//! Core keeps admission (projection, operation switches, shared budget) and the keys; this crate
//! only adapts them to the router's directory and maps the router's answers back to the bridge.
use flare_capability::application::handler::service_directory::ServiceDirectory;
use flare_capability::capability_core::plugin::{
    runtime::PluginRuntimeSnapshot,
    service::{PluginServiceClaims, PluginServiceResponse},
};
use flare_capability::composition::{
    CapabilityExtensions, ServiceDispatchFactory, ServiceDispatchParts,
};
use flare_capability::domain::repository::service_dispatch::{
    PluginServiceDispatch, ServiceDispatchError,
};
use flare_plugin_host::service::PluginServiceRouter;
use std::sync::Arc;
use tonic::{Code, Status};

/// The extensions of this binary: the plugin service router.
pub fn extensions() -> CapabilityExtensions {
    CapabilityExtensions {
        service_dispatch: Some(Arc::new(PluginHostServiceRouter)),
    }
}

/// Builds [`PluginServiceRouter`] from Core's admission and keys.
pub struct PluginHostServiceRouter;

impl ServiceDispatchFactory for PluginHostServiceRouter {
    fn build(&self, parts: ServiceDispatchParts) -> Arc<dyn PluginServiceDispatch> {
        Arc::new(RouterDispatch(PluginServiceRouter::new(
            Arc::new(Directory(parts.directory)),
            parts.calls,
            parts.runtimes,
            parts.signer,
        )))
    }
}

struct RouterDispatch(PluginServiceRouter);

#[async_trait::async_trait]
impl PluginServiceDispatch for RouterDispatch {
    async fn dispatch(
        &self,
        runtime_token: &str,
        request_id: &str,
        bytes: &[u8],
    ) -> Result<PluginServiceResponse, ServiceDispatchError> {
        self.0
            .dispatch(runtime_token, request_id, bytes)
            .await
            .map_err(|status| dispatch_error(&status))
    }
}

/// Unavailable and deadline answers leave the provider's outcome unknown; anything else is a
/// refusal.
fn dispatch_error(status: &Status) -> ServiceDispatchError {
    match status.code() {
        Code::Unavailable | Code::DeadlineExceeded => ServiceDispatchError::OutcomeUnknown,
        _ => ServiceDispatchError::Refused,
    }
}

struct Directory(Arc<ServiceDirectory>);

#[tonic::async_trait]
impl flare_plugin_host::service::ServiceDirectory for Directory {
    async fn snapshot(&self, tenant: &str) -> Result<PluginRuntimeSnapshot, Status> {
        self.0.snapshot(tenant).await
    }
    async fn check_policy(&self, tenant: &str, operation: &str) -> Result<(), Status> {
        self.0.check_policy(tenant, operation).await
    }
    async fn reserve_service(
        &self,
        claims: &PluginServiceClaims,
        origin_request: &str,
    ) -> Result<(), Status> {
        self.0.reserve_service(claims, origin_request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_unavailable_and_deadline_leave_the_outcome_unknown() {
        for code in [Code::Unavailable, Code::DeadlineExceeded] {
            assert_eq!(
                dispatch_error(&Status::new(code, "x")),
                ServiceDispatchError::OutcomeUnknown
            );
        }
        for code in [
            Code::PermissionDenied,
            Code::ResourceExhausted,
            Code::InvalidArgument,
            Code::FailedPrecondition,
            Code::Internal,
        ] {
            assert_eq!(
                dispatch_error(&Status::new(code, "x")),
                ServiceDispatchError::Refused
            );
        }
    }

    #[test]
    fn this_binary_links_the_service_router() {
        assert!(extensions().service_dispatch.is_some());
        assert!(CapabilityExtensions::default().service_dispatch.is_none());
    }
}
