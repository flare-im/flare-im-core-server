//! What a binary adds to the open Capability service at startup.
//!
//! The open build adds nothing, and routed plugin service calls (`plugin.service.dispatch`) are
//! then refused. A deployment that hosts plugin services links a router and passes its factory
//! here; Core still owns admission and keys, the router only receives them.
use crate::application::handler::service_directory::ServiceDirectory;
use crate::domain::repository::service_dispatch::PluginServiceDispatch;
use flare_im_capability_core::plugin::invocation::{
    PluginCallSigner, PluginCallVerifier, PluginRuntimeVerifier,
};
use std::sync::Arc;

/// Core state a service router is built from, handed over once at startup.
pub struct ServiceDispatchParts {
    /// Admission against the local projection, operation switches and the shared budget.
    pub directory: Arc<ServiceDirectory>,
    /// Verifies the call tickets a service request carries on behalf of a user or staff call.
    pub calls: PluginCallVerifier,
    /// Verifies the calling deployment's runtime credential.
    pub runtimes: PluginRuntimeVerifier,
    /// Signs the delegated call to the providing plugin.
    pub signer: Arc<PluginCallSigner>,
}

/// Builds the router behind [`PluginServiceDispatch`] once the bridge's keys are known.
pub trait ServiceDispatchFactory: Send + Sync {
    fn build(&self, parts: ServiceDispatchParts) -> Arc<dyn PluginServiceDispatch>;
}

#[derive(Clone, Default)]
pub struct CapabilityExtensions {
    /// `None`: `plugin.service.dispatch` is refused (logged once at startup when the bridge is
    /// configured for plugin runtime credentials).
    pub service_dispatch: Option<Arc<dyn ServiceDispatchFactory>>,
}
