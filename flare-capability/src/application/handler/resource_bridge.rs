//! Revalidate the current local grant and original user's operation before
//! every resource callback. Resource membership is checked by the owner next.
use crate::domain::capability::{CapabilityError, CapabilityPolicyBackend, Result};
use crate::domain::repository::PluginAuthorizationReader;
use crate::domain::repository::resource_access::{
    ResourceAccessBackend, ResourceActor, ResourceCallBudget,
};
use flare_im_capability_core::plugin::{
    authorization::PluginInvocation,
    invocation::PluginCallVerifier,
    resource::{MAX_RESOURCE_BYTES, ResourceRequest},
};
use serde_json::Value;
use std::{sync::Arc, time::Duration};

pub struct ResourceBridge {
    authorization: Arc<dyn PluginAuthorizationReader>,
    policy: Arc<dyn CapabilityPolicyBackend>,
    verifier: PluginCallVerifier,
    backend: Arc<dyn ResourceAccessBackend>,
    budget: Arc<dyn ResourceCallBudget>,
    slots: tokio::sync::Semaphore,
}
fn denied() -> CapabilityError {
    CapabilityError::PolicyDenied("plugin resource access denied".into())
}
impl ResourceBridge {
    pub fn new(
        authorization: Arc<dyn PluginAuthorizationReader>,
        policy: Arc<dyn CapabilityPolicyBackend>,
        verifier: PluginCallVerifier,
        backend: Arc<dyn ResourceAccessBackend>,
        budget: Arc<dyn ResourceCallBudget>,
    ) -> Self {
        Self {
            authorization,
            policy,
            verifier,
            backend,
            budget,
            slots: tokio::sync::Semaphore::new(16),
        }
    }
    pub async fn call(
        &self,
        token: &str,
        operation: &str,
        request_id: &str,
        bytes: &[u8],
    ) -> Result<Value> {
        let _slot = self
            .slots
            .try_acquire()
            .map_err(|_| CapabilityError::System("resource bridge capacity exhausted".into()))?;
        tokio::time::timeout(
            Duration::from_secs(2),
            self.authorized(token, operation, request_id, bytes),
        )
        .await
        .map_err(|_| CapabilityError::Timeout("resource bridge deadline exceeded".into()))?
    }
    async fn authorized(
        &self,
        token: &str,
        operation: &str,
        request_id: &str,
        bytes: &[u8],
    ) -> Result<Value> {
        if bytes.len() > 4096 {
            return Err(denied());
        }
        let now = chrono::Utc::now().timestamp_millis();
        let claims = self.verifier.verify(token, now).map_err(|_| denied())?;
        if request_id != claims.request_id {
            return Err(denied());
        }
        let request: ResourceRequest = serde_json::from_slice(bytes).map_err(|_| denied())?;
        request.validate().map_err(|_| denied())?;
        if request.capability().id() != operation {
            return Err(denied());
        }
        let conversation = claims.conversation_id.as_ref().ok_or_else(denied)?;
        self.policy
            .ensure_dispatch_allowed(&claims.tenant_id, &claims.actor_id, &claims.operation)
            .await?;
        let snapshot = self
            .authorization
            .plugin_snapshot(&claims.tenant_id)
            .await
            .map_err(|_| denied())?
            .ok_or_else(denied)?;
        snapshot.validate(now).map_err(|_| denied())?;
        let projected = snapshot
            .plugins
            .iter()
            .find(|p| p.grant.grant_id == claims.grant_id)
            .ok_or_else(denied)?;
        if claims.tenant_id != snapshot.tenant_id
            || claims.plugin_id != projected.grant.plugin_id
            || claims.plugin_version != projected.grant.plugin_version
            || claims.manifest_digest != projected.grant.manifest_digest
            || claims.deployment_id != projected.deployment.deployment_id
            || claims.grant_revision != projected.grant.revision
            || claims.configuration_digest != projected.configuration_digest
            || claims.expires_at_ms > projected.grant.valid_until_ms
        {
            return Err(denied());
        }
        let manifest = projected.manifest().map_err(|_| denied())?;
        let op = manifest.operation(&claims.operation).ok_or_else(denied)?;
        if !op.host_capabilities.contains(&request.capability()) {
            return Err(denied());
        }
        projected
            .authorize(&PluginInvocation {
                tenant_id: &claims.tenant_id,
                actor_id: &claims.actor_id,
                operation: &claims.operation,
                actor_permissions: &op.permission_ids.iter().cloned().collect(),
                platform_enabled: true,
                now_ms: now,
            })
            .map_err(|_| denied())?;
        if !self
            .budget
            .reserve(
                &claims.tenant_id,
                &claims.plugin_id,
                &claims.actor_id,
                &claims.request_id,
                claims.expires_at_ms,
            )
            .await?
        {
            return Err(denied());
        }
        let result = self
            .backend
            .read(
                &ResourceActor {
                    tenant_id: claims.tenant_id.clone(),
                    user_id: claims.actor_id.clone(),
                    conversation_id: conversation.clone(),
                    request_id: claims.request_id.clone(),
                },
                &request,
            )
            .await?;
        if chrono::Utc::now().timestamp_millis() >= claims.expires_at_ms
            || serde_json::to_vec(&result).map_err(|_| denied())?.len() > MAX_RESOURCE_BYTES
        {
            return Err(denied());
        }
        self.policy
            .ensure_dispatch_allowed(&claims.tenant_id, &claims.actor_id, &claims.operation)
            .await?;
        let fresh = self
            .authorization
            .plugin_snapshot(&claims.tenant_id)
            .await
            .map_err(|_| denied())?
            .ok_or_else(denied)?;
        fresh
            .validate(chrono::Utc::now().timestamp_millis())
            .map_err(|_| denied())?;
        let current = fresh
            .plugins
            .iter()
            .find(|p| p.grant.grant_id == claims.grant_id)
            .ok_or_else(denied)?;
        if current.deployment != projected.deployment
            || current.configuration_digest != claims.configuration_digest
            || current.grant.revision != claims.grant_revision
        {
            return Err(denied());
        }
        current
            .authorize(&PluginInvocation {
                tenant_id: &claims.tenant_id,
                actor_id: &claims.actor_id,
                operation: &claims.operation,
                actor_permissions: &op.permission_ids.iter().cloned().collect(),
                platform_enabled: true,
                now_ms: chrono::Utc::now().timestamp_millis(),
            })
            .map_err(|_| denied())?;
        Ok(result)
    }
}
