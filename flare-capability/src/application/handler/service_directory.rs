//! Core's admission for routed plugin service calls (`plugin.service.dispatch`).
//!
//! A service router asks three questions before it forwards a call to the providing plugin: what
//! the tenant's local projection says is installed and granted, whether the operation is switched
//! off for the tenant, and whether the shared per-request budget still has room. The answers come
//! only from Core's own state, never from the caller.
use crate::domain::capability::CapabilityPolicyBackend;
use crate::domain::repository::PluginAuthorizationReader;
use crate::domain::repository::resource_access::ResourceCallBudget;
use flare_im_capability_core::plugin::{
    runtime::PluginRuntimeSnapshot, service::PluginServiceClaims,
};
use std::sync::Arc;
use tonic::Status;

pub struct ServiceDirectory {
    authorization: Arc<dyn PluginAuthorizationReader>,
    policy: Arc<dyn CapabilityPolicyBackend>,
    budget: Arc<dyn ResourceCallBudget>,
}

impl ServiceDirectory {
    pub fn new(
        authorization: Arc<dyn PluginAuthorizationReader>,
        policy: Arc<dyn CapabilityPolicyBackend>,
        budget: Arc<dyn ResourceCallBudget>,
    ) -> Self {
        Self {
            authorization,
            policy,
            budget,
        }
    }

    /// The tenant's current local projection; a tenant without one has nothing installed.
    pub async fn snapshot(&self, tenant: &str) -> Result<PluginRuntimeSnapshot, Status> {
        self.authorization
            .plugin_snapshot(tenant)
            .await
            .map_err(|_| Status::unavailable("service authorization unavailable"))?
            .ok_or_else(|| Status::permission_denied("service application not installed"))
    }

    /// Refuses an operation the tenant has switched off.
    pub async fn check_policy(&self, tenant: &str, operation: &str) -> Result<(), Status> {
        self.policy
            .ensure_not_suspended(tenant, operation)
            .await
            .map_err(|_| Status::permission_denied("service stopped"))
    }

    /// Takes one slot of the shared admission ledger, keyed by the original Core request for
    /// member calls and by the signed workload request for recovery.
    pub async fn reserve_service(
        &self,
        claims: &PluginServiceClaims,
        origin_request: &str,
    ) -> Result<(), Status> {
        let actor = claims
            .actor_id
            .as_deref()
            .unwrap_or(&claims.source.plugin_id);
        if self
            .budget
            .reserve(
                &claims.tenant_id,
                &claims.source.plugin_id,
                actor,
                origin_request,
                claims.expires_at_ms,
            )
            .await
            .map_err(|_| Status::unavailable("service budget unavailable"))?
        {
            Ok(())
        } else {
            Err(Status::resource_exhausted("service invocation budget"))
        }
    }
}
