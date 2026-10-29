//! Revalidate the current local grant and original user's operation before
//! every resource callback. Resource membership is checked by the owner next.
//!
//! Writes accept the credentials their capability allows, each revalidated against the current
//! grant and required to name an operation that declares the capability:
//!
//! - `im.plugin.entity_state.publish`: the delegation ticket of a user call or of a staff call
//!   (`x-flare-plugin-call`), or the plugin runtime credential (`x-flare-plugin-runtime`) bound to
//!   the exact request bytes. The plugin writes only its own entities (see [`super::plugin_state`]).
//! - `im.realtime.deliver`: a user call ticket, for the conversation it is bound to, or the
//!   runtime credential, for any conversation of its tenant. Only members of the conversation
//!   receive the packet; the others are listed as skipped.
use crate::domain::capability::{CapabilityError, CapabilityPolicyBackend, Result};
use crate::domain::repository::PluginAuthorizationReader;
use crate::domain::repository::realtime_delivery::{RealtimePacket, RealtimePacketDelivery};
use crate::domain::repository::resource_access::{
    ResourceAccessBackend, ResourceActor, ResourceCallBudget,
};
use flare_im_capability_core::plugin::{
    authorization::PluginInvocation,
    invocation::{PluginCallVerifier, PluginRuntimeVerifier},
    resource::{
        EntityStatePublishView, HostCapability, MAX_RESOURCE_BYTES, MAX_RESOURCE_REQUEST_BYTES,
        MembersCheckView, RealtimeDelivery, RealtimeDeliveryView, ResourceRequest,
        ResourceWriteRequest,
    },
    runtime::{PluginRuntimeSnapshot, ProjectedPlugin},
    state::SharedState,
};
use serde_json::Value;
use std::{sync::Arc, time::Duration};

use super::plugin_state::{PluginStatePublisher, PublishScope, PublishSource};

/// The credential a bridge request carries.
#[derive(Clone, Copy)]
pub enum BridgeCredential<'a> {
    /// `x-flare-plugin-call`: the ticket of the user or staff call in flight.
    Call(&'a str),
    /// `x-flare-plugin-runtime`: the plugin deployment's own credential (writes only).
    Runtime(&'a str),
}

pub struct ResourceBridge {
    authorization: Arc<dyn PluginAuthorizationReader>,
    policy: Arc<dyn CapabilityPolicyBackend>,
    verifier: PluginCallVerifier,
    backend: Arc<dyn ResourceAccessBackend>,
    budget: Arc<dyn ResourceCallBudget>,
    slots: tokio::sync::Semaphore,
    publisher: Option<Arc<PluginStatePublisher>>,
    runtime_verifier: Option<PluginRuntimeVerifier>,
    realtime: Option<Arc<dyn RealtimePacketDelivery>>,
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
            publisher: None,
            runtime_verifier: None,
            realtime: None,
        }
    }
    /// Enables the write capability: `publisher` stores and fans out shared state,
    /// `runtime_verifier` admits plugin runtime credentials (none: runtime writes are refused).
    pub fn with_writes(
        mut self,
        publisher: Option<Arc<PluginStatePublisher>>,
        runtime_verifier: Option<PluginRuntimeVerifier>,
    ) -> Self {
        self.publisher = publisher;
        self.runtime_verifier = runtime_verifier;
        self
    }
    /// Enables `im.realtime.deliver` (none: deliveries answer unavailable).
    pub fn with_realtime(mut self, realtime: Option<Arc<dyn RealtimePacketDelivery>>) -> Self {
        self.realtime = realtime;
        self
    }
    /// A request carrying the ticket of the call in flight.
    pub async fn call(
        &self,
        token: &str,
        operation: &str,
        request_id: &str,
        bytes: &[u8],
    ) -> Result<Value> {
        self.call_with(BridgeCredential::Call(token), operation, request_id, bytes)
            .await
    }
    pub async fn call_with(
        &self,
        credential: BridgeCredential<'_>,
        operation: &str,
        request_id: &str,
        bytes: &[u8],
    ) -> Result<Value> {
        let _slot = self
            .slots
            .try_acquire()
            .map_err(|_| CapabilityError::System("resource bridge capacity exhausted".into()))?;
        tokio::time::timeout(Duration::from_secs(2), async {
            if bytes.len() > MAX_RESOURCE_REQUEST_BYTES {
                return Err(denied());
            }
            if write_capability(operation).is_some() {
                return self.write(credential, operation, request_id, bytes).await;
            }
            match credential {
                BridgeCredential::Call(token) => {
                    self.authorized(token, operation, request_id, bytes).await
                }
                BridgeCredential::Runtime(_) => Err(denied()),
            }
        })
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
            .ensure_not_suspended(&claims.tenant_id, &claims.operation)
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
        if !op.host_capabilities.contains(&request.capability())
            || super::remote_dispatch::restricted_operations(&manifest).contains(&claims.operation)
        {
            return Err(denied());
        }
        projected
            .authorize(&PluginInvocation {
                tenant_id: &claims.tenant_id,
                actor_id: &claims.actor_id,
                operation: &claims.operation,
                actor_permissions: &projected.grant.approved_permissions,
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
            .ensure_not_suspended(&claims.tenant_id, &claims.operation)
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
                actor_permissions: &current.grant.approved_permissions,
                platform_enabled: true,
                now_ms: chrono::Utc::now().timestamp_millis(),
            })
            .map_err(|_| denied())?;
        Ok(result)
    }

    async fn snapshot(&self, tenant: &str, now: i64) -> Result<PluginRuntimeSnapshot> {
        let snapshot = self
            .authorization
            .plugin_snapshot(tenant)
            .await
            .map_err(|_| denied())?
            .ok_or_else(denied)?;
        snapshot.validate(now).map_err(|_| denied())?;
        Ok(snapshot)
    }

    /// A write: who may write, then the capability's own checks.
    async fn write(
        &self,
        credential: BridgeCredential<'_>,
        operation: &str,
        request_id: &str,
        bytes: &[u8],
    ) -> Result<Value> {
        let request: ResourceWriteRequest = serde_json::from_slice(bytes).map_err(|_| denied())?;
        request.validate().map_err(|_| denied())?;
        if request.capability().id() != operation {
            return Err(denied());
        }
        let capability = request.capability();
        let now = chrono::Utc::now().timestamp_millis();
        let writer = match credential {
            BridgeCredential::Call(token) => {
                if let Ok(claims) = self.verifier.verify(token, now) {
                    self.user_writer(claims, capability, request_id, now)
                        .await?
                } else if capability == HostCapability::EntityStatePublish {
                    let claims = self
                        .verifier
                        .verify_staff(token, now)
                        .map_err(|_| denied())?;
                    self.staff_writer(claims, capability, request_id, now)
                        .await?
                } else {
                    // A staff call has no conversation to deliver into.
                    return Err(denied());
                }
            }
            BridgeCredential::Runtime(token) => {
                let verifier = self.runtime_verifier.as_ref().ok_or_else(denied)?;
                let claims = verifier.verify(token, bytes, now).map_err(|_| denied())?;
                self.runtime_writer(claims, operation, request_id, now)
                    .await?
            }
        };
        match request {
            ResourceWriteRequest::EntityStatePublish(state) => {
                self.publish_state(writer, request_id, state).await
            }
            ResourceWriteRequest::RealtimeDeliver(delivery) => {
                self.deliver(writer, request_id, delivery, now).await
            }
        }
    }

    /// `im.plugin.entity_state.publish`: the publisher's own checks (bound entity of this plugin,
    /// version order, coalescing).
    async fn publish_state(
        &self,
        writer: Writer,
        request_id: &str,
        state: SharedState,
    ) -> Result<Value> {
        let publisher = self.publisher.as_ref().ok_or_else(|| {
            CapabilityError::System("plugin shared state publishing unavailable".into())
        })?;
        self.reserve(&writer, request_id).await?;
        let scope = match writer.conversation_id.as_deref() {
            Some(conversation) => PublishScope::Conversation(conversation),
            None => PublishScope::Tenant,
        };
        let (entity, version) = (state.entity.clone(), state.version);
        let outcome = publisher
            .submit(
                &writer.tenant_id,
                &writer.plugin_id,
                scope,
                state,
                PublishSource::Bridge,
            )
            .await?;
        serde_json::to_value(EntityStatePublishView {
            entity,
            version,
            outcome,
        })
        .map_err(|_| denied())
    }

    /// `im.realtime.deliver`: a user ticket delivers into its own conversation only, the runtime
    /// credential into any conversation of its tenant; members receive it, the rest are skipped.
    async fn deliver(
        &self,
        writer: Writer,
        request_id: &str,
        delivery: RealtimeDelivery,
        now: i64,
    ) -> Result<Value> {
        let realtime = self.realtime.as_ref().ok_or_else(|| {
            CapabilityError::System("plugin realtime delivery unavailable".into())
        })?;
        if writer
            .conversation_id
            .as_ref()
            .is_some_and(|bound| *bound != delivery.conversation_id)
        {
            return Err(denied());
        }
        self.reserve(&writer, request_id).await?;
        let members: std::collections::BTreeSet<String> = match &writer.user_id {
            // As the ticket's user: the user must be a member too.
            Some(user_id) => {
                let answer = self
                    .backend
                    .read(
                        &ResourceActor {
                            tenant_id: writer.tenant_id.clone(),
                            user_id: user_id.clone(),
                            conversation_id: delivery.conversation_id.clone(),
                            request_id: request_id.to_owned(),
                        },
                        &ResourceRequest::ConversationMembersCheck {
                            user_ids: delivery.user_ids.clone(),
                        },
                    )
                    .await?;
                let view: MembersCheckView =
                    serde_json::from_value(answer).map_err(|_| denied())?;
                view.validate_for(&delivery.user_ids)
                    .map_err(|_| denied())?;
                view.members.into_iter().collect()
            }
            None => {
                self.backend
                    .tenant_members(
                        &writer.tenant_id,
                        &delivery.conversation_id,
                        request_id,
                        &delivery.user_ids,
                    )
                    .await?
            }
        };
        let (recipients, skipped): (Vec<String>, Vec<String>) = delivery
            .user_ids
            .iter()
            .cloned()
            .partition(|id| members.contains(id));
        let view = RealtimeDeliveryView {
            delivery_id: uuid::Uuid::new_v4().to_string(),
            delivered: recipients.len() as u32,
            skipped,
        };
        if !recipients.is_empty() {
            realtime
                .deliver(&RealtimePacket {
                    tenant_id: writer.tenant_id,
                    request_id: request_id.to_owned(),
                    plugin_id: writer.plugin_id,
                    conversation_id: delivery.conversation_id,
                    user_ids: recipients,
                    packet_type: delivery.packet_type,
                    payload_json: serde_json::to_vec(&delivery.payload).map_err(|_| denied())?,
                    delivery_id: view.delivery_id.clone(),
                    expires_at_ms: now + i64::from(delivery.ttl_ms),
                })
                .await?;
        }
        serde_json::to_value(view).map_err(|_| denied())
    }

    async fn reserve(&self, writer: &Writer, request_id: &str) -> Result<()> {
        if !self
            .budget
            .reserve(
                &writer.tenant_id,
                &writer.plugin_id,
                &writer.actor,
                request_id,
                writer.expires_at_ms,
            )
            .await?
        {
            return Err(denied());
        }
        Ok(())
    }

    async fn user_writer(
        &self,
        claims: flare_im_capability_core::plugin::invocation::PluginCallClaims,
        capability: HostCapability,
        request_id: &str,
        now: i64,
    ) -> Result<Writer> {
        if request_id != claims.request_id {
            return Err(denied());
        }
        let conversation = claims.conversation_id.clone().ok_or_else(denied)?;
        self.policy
            .ensure_not_suspended(&claims.tenant_id, &claims.operation)
            .await?;
        let snapshot = self.snapshot(&claims.tenant_id, now).await?;
        let projected = grant_of(&snapshot, &claims.grant_id)?;
        if super::remote_dispatch::restricted_operations(
            &projected.manifest().map_err(|_| denied())?,
        )
        .contains(&claims.operation)
        {
            return Err(denied());
        }
        require_identity(
            projected,
            &claims.plugin_id,
            &claims.plugin_version,
            &claims.manifest_digest,
            &claims.deployment_id,
            claims.grant_revision,
            claims.expires_at_ms,
        )?;
        if claims.configuration_digest != projected.configuration_digest {
            return Err(denied());
        }
        authorize_writing_operation(
            projected,
            capability,
            &claims.tenant_id,
            &claims.actor_id,
            &claims.operation,
            now,
        )?;
        Ok(Writer {
            tenant_id: claims.tenant_id,
            plugin_id: claims.plugin_id,
            actor: claims.actor_id.clone(),
            user_id: Some(claims.actor_id),
            conversation_id: Some(conversation),
            expires_at_ms: claims.expires_at_ms,
        })
    }

    async fn staff_writer(
        &self,
        claims: flare_im_capability_core::plugin::invocation::PluginStaffCallClaims,
        capability: HostCapability,
        request_id: &str,
        now: i64,
    ) -> Result<Writer> {
        if request_id != claims.request_id {
            return Err(denied());
        }
        let snapshot = self.snapshot(&claims.tenant_id, now).await?;
        let projected = grant_of(&snapshot, &claims.grant_id)?;
        require_identity(
            projected,
            &claims.plugin_id,
            &claims.plugin_version,
            &claims.manifest_digest,
            &claims.deployment_id,
            claims.grant_revision,
            claims.expires_at_ms,
        )?;
        if claims.configuration_digest != projected.configuration_digest {
            return Err(denied());
        }
        let manifest = projected.manifest().map_err(|_| denied())?;
        if !manifest.admin_operations().contains(&claims.operation) {
            return Err(denied());
        }
        authorize_writing_operation(
            projected,
            capability,
            &claims.tenant_id,
            &claims.staff_id,
            &claims.operation,
            now,
        )?;
        Ok(Writer {
            tenant_id: claims.tenant_id,
            plugin_id: claims.plugin_id,
            actor: format!("staff:{}", claims.staff_id),
            user_id: None,
            conversation_id: None,
            expires_at_ms: claims.expires_at_ms,
        })
    }

    async fn runtime_writer(
        &self,
        claims: flare_im_capability_core::plugin::invocation::PluginRuntimeClaims,
        operation: &str,
        request_id: &str,
        now: i64,
    ) -> Result<Writer> {
        if request_id != claims.request_id || claims.capability.id() != operation {
            return Err(denied());
        }
        let snapshot = self.snapshot(&claims.tenant_id, now).await?;
        let projected = grant_of(&snapshot, &claims.grant_id)?;
        require_identity(
            projected,
            &claims.plugin_id,
            &claims.plugin_version,
            &claims.manifest_digest,
            &claims.deployment_id,
            claims.grant_revision,
            claims.expires_at_ms,
        )?;
        // The deployment holds the capability only through an approved operation that declares it.
        let manifest = projected.manifest().map_err(|_| denied())?;
        let declared = manifest
            .capabilities
            .iter()
            .flat_map(|capability| {
                capability
                    .operations
                    .iter()
                    .map(move |op| (format!("{}.{}", capability.id, op.id), op))
            })
            .any(|(wire, op)| {
                op.host_capabilities.contains(&claims.capability)
                    && authorize_writing_operation(
                        projected,
                        claims.capability,
                        &claims.tenant_id,
                        &claims.deployment_id,
                        &wire,
                        now,
                    )
                    .is_ok()
            });
        if !declared {
            return Err(denied());
        }
        Ok(Writer {
            tenant_id: claims.tenant_id,
            plugin_id: claims.plugin_id,
            actor: format!("runtime:{}", claims.deployment_id),
            user_id: None,
            conversation_id: None,
            expires_at_ms: claims.expires_at_ms,
        })
    }
}

/// Who a write runs for, after its credential and grant were checked.
struct Writer {
    tenant_id: String,
    plugin_id: String,
    /// Budget key of the writer: the user, `staff:<id>` or `runtime:<deployment>`.
    actor: String,
    /// User calls only: the ticket's user.
    user_id: Option<String>,
    /// User calls only: the ticket's conversation, the only one the write may touch.
    conversation_id: Option<String>,
    expires_at_ms: i64,
}

fn grant_of<'s>(
    snapshot: &'s PluginRuntimeSnapshot,
    grant_id: &str,
) -> Result<&'s ProjectedPlugin> {
    snapshot
        .plugins
        .iter()
        .find(|p| p.grant.grant_id == grant_id)
        .ok_or_else(denied)
}

/// The credential names exactly the current grant, deployment and manifest.
fn require_identity(
    projected: &ProjectedPlugin,
    plugin_id: &str,
    plugin_version: &str,
    manifest_digest: &str,
    deployment_id: &str,
    grant_revision: u64,
    expires_at_ms: i64,
) -> Result<()> {
    if plugin_id != projected.grant.plugin_id
        || plugin_version != projected.grant.plugin_version
        || manifest_digest != projected.grant.manifest_digest
        || deployment_id != projected.deployment.deployment_id
        || grant_revision != projected.grant.revision
        || expires_at_ms > projected.grant.valid_until_ms
    {
        return Err(denied());
    }
    Ok(())
}

/// The write capability an operation id names, if it names one.
fn write_capability(operation: &str) -> Option<HostCapability> {
    serde_json::from_value::<HostCapability>(Value::String(operation.to_owned()))
        .ok()
        .filter(|capability| capability.is_write())
}

/// `operation` declares the write `capability` and the enabled grant approves it (with the
/// permission whose data scope is the capability id). The writer holds the permissions the
/// installation approved (plugin platform v2 §2): the projection authorizes, no per-user grant.
fn authorize_writing_operation(
    projected: &ProjectedPlugin,
    capability: HostCapability,
    tenant_id: &str,
    actor_id: &str,
    operation: &str,
    now: i64,
) -> Result<()> {
    let manifest = projected.manifest().map_err(|_| denied())?;
    let op = manifest.operation(operation).ok_or_else(denied)?;
    if !op.host_capabilities.contains(&capability) {
        return Err(denied());
    }
    projected
        .authorize(&PluginInvocation {
            tenant_id,
            actor_id,
            operation,
            actor_permissions: &projected.grant.approved_permissions,
            platform_enabled: true,
            now_ms: now,
        })
        .map_err(|_| denied())?;
    Ok(())
}
