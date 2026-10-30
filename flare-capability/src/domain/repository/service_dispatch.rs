//! Port of the host capability `plugin.service.dispatch`: a plugin deployment's call to a
//! service that another installed plugin provides.
//!
//! The bridge checks only the credential kind and the request size; the router behind this
//! port verifies the service ticket against Core's local projection, asks Core's admission
//! ([`crate::application::handler::service_directory::ServiceDirectory`]) and forwards the call
//! to the providing plugin. A build without a router refuses every service call exactly like any
//! other denied resource request.
use flare_im_capability_core::plugin::service::PluginServiceResponse;

/// Why a routed service call has no answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceDispatchError {
    /// Refused before or by the provider: credential, grant, policy, budget or target.
    Refused,
    /// The provider may or may not have run the call (unavailable or timed out); the caller
    /// must reconcile instead of assuming it did not happen.
    OutcomeUnknown,
}

#[async_trait::async_trait]
pub trait PluginServiceDispatch: Send + Sync {
    /// Route one service request. `runtime_token` is the caller's `x-flare-plugin-runtime`
    /// credential, `bytes` the exact request body it is bound to.
    async fn dispatch(
        &self,
        runtime_token: &str,
        request_id: &str,
        bytes: &[u8],
    ) -> Result<PluginServiceResponse, ServiceDispatchError>;
}
