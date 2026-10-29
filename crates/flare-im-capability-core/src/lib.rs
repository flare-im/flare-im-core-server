//! Business-neutral capability contracts.
//!
//! This crate owns the stable types and ports shared by capability services,
//! IM services, and optional plugin implementations. Runtime composition,
//! persistence, route books, gRPC servers, and concrete adapters stay in
//! `flare-capability`.

pub mod context;
pub mod descriptor;
pub mod dispatch;
pub mod error;
pub mod extension_operation;
pub mod grant;
pub mod ports;
pub mod recipient;

pub use context::{CapabilityInvokeMeta, ConversationKind, PreSendEvaluateInput, ResolveTrigger};
pub use descriptor::CapabilityDescriptor;
pub use dispatch::{CapabilityDispatchCommand, CapabilityDispatchResult};
pub use error::{CapabilityError, GuardDecision, GuardRejection, Result};
pub use extension_operation::{DynExtensionOperationHandler, ExtensionOperationHandler};
pub use flare_plugin_protocol as plugin;
pub use grant::{TenantCapabilitySwitch, UserCapabilityGrant};
pub use ports::{
    CapabilityDispatchRoute, CapabilityPolicyBackend, PluginHealthProbe, PreSendGuard,
    PreSendGuardPipeline, RecipientResolver,
};
pub use recipient::{RecipientResolveRequest, RecipientResolveResult};
