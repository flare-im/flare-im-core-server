//! 远程能力动态分发：按 `capability_id` 选择已注册插件并执行 `ExtensionPlugin.Call`。

use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use flare_core_base::context::Ctx;
use flare_grpc_proto::capability::GenericRequest;
use flare_grpc_proto::capability::RegisteredPluginInstance;
use flare_grpc_proto::capability::extension_plugin_client::ExtensionPluginClient;
use flare_server_core::context::{ActorContext, ActorType, Context};
use flare_server_core::{SignedActorType, SignedContextClaims, SignedContextVerifier};
use serde_json::Value;
use tonic::metadata::MetadataMap;
use tonic::{Request, Status};

use crate::domain::capability::CapabilityPolicyBackend;
use crate::domain::capability::{
    CapabilityDispatchCommand, CapabilityDispatchResult, CapabilityError, Result,
    require_dispatch_tenant,
};
use crate::infrastructure::capability::PluginRouteBook;
use crate::infrastructure::capability::plugin_channel::resolve_plugin_channel;
use flare_im_capability_core::plugin::{
    authorization::PluginInvocation, invocation::PluginCallClaims, resource::HostCapability,
};

/// Only the verifier can construct this marker; an untrusted request body or
/// legacy Context metadata cannot create a dispatch identity.
pub(crate) struct VerifiedDispatchOrigin {
    claims: SignedContextClaims,
}

/// The local control projection is the sole remote routing/authorization source.
/// No endpoint self-registration or diagnostic route can grant execution. A
/// business command is attempted once: uncertain delivery must be resolved by
/// its stable request id, especially for non-idempotent financial operations.
pub(crate) async fn dispatch_remote(
    ctx: &Ctx,
    req: &CapabilityDispatchCommand,
    routes: &Arc<PluginRouteBook>,
    policy: &Arc<dyn CapabilityPolicyBackend>,
    timeout: Duration,
) -> Result<CapabilityDispatchResult> {
    tokio::time::timeout(
        timeout,
        dispatch_remote_authorized(ctx, req, routes, policy, timeout),
    )
    .await
    .map_err(|_| {
        CapabilityError::Timeout("plugin authorization or invocation deadline exceeded".into())
    })?
}

async fn dispatch_remote_authorized(
    ctx: &Ctx,
    req: &CapabilityDispatchCommand,
    routes: &Arc<PluginRouteBook>,
    policy: &Arc<dyn CapabilityPolicyBackend>,
    timeout: Duration,
) -> Result<CapabilityDispatchResult> {
    let tenant = require_dispatch_tenant(req)?;
    let user = req
        .user_id
        .as_deref()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| CapabilityError::PolicyDenied("user identity required".into()))?;
    let request_id = req
        .request_id
        .as_deref()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| CapabilityError::PolicyDenied("request identity required".into()))?;
    let denied =
        |_| CapabilityError::PolicyDenied("plugin authorization is unavailable or denied".into());
    ctx.get_data::<VerifiedDispatchOrigin>()
        .ok_or_else(|| denied(()))?
        .require_user_binding(&tenant, user, request_id)
        .map_err(|_| denied(()))?;
    if req.capability_id == flare_im_capability_core::plugin::ui::UI_CATALOG_OPERATION {
        return client_ui_catalog(&tenant, user, request_id, routes, policy).await;
    }
    policy
        .ensure_dispatch_allowed(&tenant, user, &req.capability_id)
        .await?;
    let snapshot = routes
        .authorization
        .as_ref()
        .ok_or_else(|| denied(()))?
        .plugin_snapshot(&tenant)
        .await
        .map_err(|_| denied(()))?
        .ok_or_else(|| denied(()))?;
    let now = chrono::Utc::now().timestamp_millis();
    snapshot.validate(now).map_err(|_| denied(()))?;
    let projected = snapshot
        .plugins
        .iter()
        .find(|p| {
            p.grant.enabled
                && !p.grant.revoked
                && p.manifest()
                    .is_ok_and(|m| m.operation(&req.capability_id).is_some())
        })
        .ok_or_else(|| denied(()))?;
    let manifest = projected.manifest().map_err(|_| denied(()))?;
    let operation = manifest
        .operation(&req.capability_id)
        .ok_or_else(|| denied(()))?;
    // The existing per-operation user policy has just authorized exactly this
    // operation. Translate only its required IDs; payload metadata grants none.
    let actor_permissions = operation.permission_ids.iter().cloned().collect();
    projected
        .authorize(&PluginInvocation {
            tenant_id: &tenant,
            actor_id: user,
            operation: &req.capability_id,
            actor_permissions: &actor_permissions,
            platform_enabled: true,
            now_ms: now,
        })
        .map_err(|_| denied(()))?;
    let timeout = timeout.min(Duration::from_millis(u64::from(operation.timeout_ms)));
    let input = req.payload.as_ref().unwrap_or(&Value::Null);
    if let Some(rejection) = recalled_card_guard(routes, &tenant, &manifest, req, input).await {
        return Ok(rejection);
    }
    let claims = PluginCallClaims {
        tenant_id: tenant.clone(),
        plugin_id: projected.grant.plugin_id.clone(),
        plugin_version: projected.grant.plugin_version.clone(),
        manifest_digest: projected.grant.manifest_digest.clone(),
        deployment_id: projected.deployment.deployment_id.clone(),
        grant_id: projected.grant.grant_id.clone(),
        grant_revision: projected.grant.revision,
        configuration_digest: projected.configuration_digest.clone(),
        actor_id: user.into(),
        operation: req.capability_id.clone(),
        request_id: request_id.into(),
        conversation_id: req.conversation_id.clone(),
        issued_at_ms: now,
        expires_at_ms: now
            .saturating_add(timeout.as_millis().min(120000) as i64)
            .min(projected.grant.valid_until_ms),
    };
    let ticket = routes
        .invocation_signer
        .as_ref()
        .ok_or_else(|| denied(()))?
        .sign(&claims, now)
        .map_err(|_| denied(()))?;
    let endpoint = RegisteredPluginInstance {
        plugin_id: manifest.plugin_id.clone(),
        plugin_version: manifest.version.clone(),
        api_version: manifest.api_version.clone(),
        manifest_sha256: projected
            .grant
            .manifest_digest
            .trim_start_matches("sha256:")
            .into(),
        declared_operations: manifest.operation_ids().into_iter().collect(),
        capability_id: req.capability_id.clone(),
        grpc_authority: projected.deployment.grpc_authority.clone(),
        unverified: false,
        labels: Default::default(),
    };
    let result = tokio::time::timeout(timeout, invoke_once(ctx, req, &endpoint, timeout, &ticket))
        .await
        .map_err(|_| CapabilityError::Timeout("plugin invocation deadline exceeded".into()))??;
    if result.success {
        let declares_publish = operation
            .host_capabilities
            .contains(&HostCapability::EntityStatePublish);
        publish_call_result(
            routes,
            &tenant,
            &projected.grant.plugin_id,
            req.conversation_id.as_deref(),
            declares_publish,
            &result.data,
        );
    }
    Ok(result)
}

/// `PLUGIN_MESSAGE_RECALLED`: the call names the entity of a card that was recalled.
pub const PLUGIN_MESSAGE_RECALLED: &str = "PLUGIN_MESSAGE_RECALLED";

/// Core refuses a call that names the entity of a recalled card (every card bound to it was
/// recalled). The entity is the value a card action of this operation binds from the
/// contribution's `entityPath`; calls naming none (creating a card) pass. Without the bindings
/// (no publisher configured, or the store unavailable) the call passes and the plugin still
/// authorizes it.
async fn recalled_card_guard(
    routes: &PluginRouteBook,
    tenant: &str,
    manifest: &flare_im_capability_core::plugin::PluginManifest,
    req: &CapabilityDispatchCommand,
    input: &Value,
) -> Option<CapabilityDispatchResult> {
    let publisher = routes.state_publisher.as_ref()?;
    let entity = super::plugin_state::invocation_entity(manifest, &req.capability_id, input)?;
    match publisher
        .entity_recalled(tenant, &manifest.plugin_id, &entity)
        .await
    {
        Ok(false) => None,
        Ok(true) => Some(CapabilityDispatchResult {
            request_id: req.request_id.clone().unwrap_or_default(),
            plugin_id: manifest.plugin_id.clone(),
            capability_id: req.capability_id.clone(),
            success: false,
            data: serde_json::json!({ "errorCode": PLUGIN_MESSAGE_RECALLED }),
            error: Some("plugin card was recalled".into()),
        }),
        Err(error) => {
            tracing::warn!(%error, tenant, plugin_id = %manifest.plugin_id, %entity, "recall check unavailable; call passed to the plugin");
            None
        }
    }
}

/// Publish `data.sharedState` of a successful user call in the background: only for an operation
/// that declares `im.plugin.entity_state.publish`, bound to the ticket's conversation. Never
/// changes the call result; problems are logged and counted.
fn publish_call_result(
    routes: &PluginRouteBook,
    tenant: &str,
    plugin_id: &str,
    conversation_id: Option<&str>,
    declares_publish: bool,
    data: &Value,
) {
    use super::plugin_state::{PublishScope, PublishSource};
    use flare_im_capability_core::plugin::state::SharedState;
    let state = match SharedState::from_result(data) {
        Ok(None) => return,
        Ok(Some(state)) => state,
        Err(_) => {
            if let Some(publisher) = &routes.state_publisher {
                publisher
                    .metrics()
                    .record(PublishSource::Invocation, "invalid");
            }
            tracing::warn!(
                tenant,
                plugin_id,
                "plugin returned a malformed sharedState; not published"
            );
            return;
        }
    };
    let Some(publisher) = routes.state_publisher.clone() else {
        tracing::debug!(tenant, plugin_id, entity = %state.entity, "plugin shared state publishing is not configured");
        return;
    };
    if !declares_publish {
        publisher
            .metrics()
            .record(PublishSource::Invocation, "undeclared");
        tracing::warn!(tenant, plugin_id, entity = %state.entity, "operation does not declare im.plugin.entity_state.publish; sharedState not published");
        return;
    }
    let Some(conversation_id) = conversation_id.map(str::to_owned) else {
        publisher
            .metrics()
            .record(PublishSource::Invocation, "unbound");
        tracing::debug!(tenant, plugin_id, entity = %state.entity, "call without a conversation; sharedState not published");
        return;
    };
    let (tenant, plugin_id) = (tenant.to_owned(), plugin_id.to_owned());
    tokio::spawn(async move {
        let entity = state.entity.clone();
        let version = state.version;
        if let Err(error) = publisher
            .submit(
                &tenant,
                &plugin_id,
                PublishScope::Conversation(&conversation_id),
                state,
                PublishSource::Invocation,
            )
            .await
        {
            tracing::warn!(%error, %tenant, %plugin_id, %conversation_id, %entity, version, "call result sharedState not published");
        }
    });
}

#[cfg(test)]
#[path = "remote_dispatch_tests.rs"]
mod tests;

impl VerifiedDispatchOrigin {
    pub(crate) fn verify(
        metadata: &MetadataMap,
        verifier: &SignedContextVerifier,
    ) -> std::result::Result<Self, Status> {
        let mut headers = metadata.get_all("x-flare-ctx").iter();
        let token = headers
            .next()
            .ok_or_else(|| Status::unauthenticated("signed dispatch context required"))?
            .to_str()
            .map_err(|_| Status::unauthenticated("invalid signed dispatch context"))?;
        if headers.next().is_some() || token.len() > 8192 {
            return Err(Status::unauthenticated("invalid signed dispatch context"));
        }
        let claims = verifier
            .verify(token)
            .map_err(|_| Status::unauthenticated("invalid signed dispatch context"))?;
        Ok(Self { claims })
    }

    pub(crate) fn require_user_binding(
        &self,
        tenant_id: &str,
        user_id: &str,
        request_id: &str,
    ) -> std::result::Result<(), Status> {
        if self.claims.actor_type != SignedActorType::User
            || tenant_id != self.claims.tenant_id
            || user_id != self.claims.actor_id
            || request_id != self.claims.request_id
        {
            return Err(Status::permission_denied(
                "dispatch identity does not match signed context",
            ));
        }
        Ok(())
    }

    pub(crate) fn into_context(self) -> Ctx {
        let mut actor = ActorContext::new(&self.claims.actor_id).with_type(ActorType::User);
        for role in &self.claims.roles {
            actor = actor.with_role(role);
        }
        Arc::new(
            Context::with_request_id(&self.claims.request_id)
                .with_tenant_id(&self.claims.tenant_id)
                .with_user_id(&self.claims.actor_id)
                .with_actor(actor)
                .insert_data(self),
        )
    }
}

fn build_request_payload(payload: &Value) -> flare_server_core::error::Result<prost_types::Any> {
    let payload_json = serde_json::to_string(payload).map_err(|e| {
        flare_server_core::error::FlareError::system(format!("serialize request payload_json: {e}"))
    })?;
    Ok(prost_types::Any {
        type_url: "type.googleapis.com/flare.capability.v1.PayloadJson".to_string(),
        value: payload_json.into_bytes(),
    })
}

fn decode_response_payload(payload: Option<prost_types::Any>) -> Value {
    let Some(any) = payload else {
        return Value::Null;
    };
    match String::from_utf8(any.value.clone()) {
        Ok(text) => serde_json::from_str::<Value>(&text).unwrap_or(Value::String(text)),
        Err(_) => Value::String(STANDARD.encode(any.value)),
    }
}

async fn invoke_once(
    ctx: &Ctx,
    req: &CapabilityDispatchCommand,
    endpoint: &RegisteredPluginInstance,
    timeout: Duration,
    ticket: &str,
) -> Result<CapabilityDispatchResult> {
    let tenant_id = require_dispatch_tenant(req)?;
    let user_id = req.user_id.clone().unwrap_or_default();
    let request_id = req
        .request_id
        .clone()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let origin = ctx
        .get_data::<VerifiedDispatchOrigin>()
        .ok_or_else(|| CapabilityError::PolicyDenied("verified dispatch origin required".into()))?;
    origin
        .require_user_binding(&tenant_id, &user_id, &request_id)
        .map_err(|_| CapabilityError::PolicyDenied("dispatch identity mismatch".into()))?;
    call_plugin(
        PluginCall {
            grpc_authority: &endpoint.grpc_authority,
            plugin_id: &endpoint.plugin_id,
            operation: &req.capability_id,
            tenant_id: &tenant_id,
            user_id: (!user_id.is_empty()).then_some(user_id.as_str()),
            request_id: &request_id,
            trace_id: ctx.trace_id(),
            payload: req.payload.as_ref().unwrap_or(&Value::Null),
            ticket,
        },
        timeout,
    )
    .await
}

/// One `ExtensionPlugin.Call` with a signed ticket (user or staff) in `x-flare-plugin-call`.
pub(crate) struct PluginCall<'a> {
    pub grpc_authority: &'a str,
    pub plugin_id: &'a str,
    pub operation: &'a str,
    pub tenant_id: &'a str,
    /// The invoking user (metadata only; the ticket is the identity). `None` for staff calls.
    pub user_id: Option<&'a str>,
    pub request_id: &'a str,
    pub trace_id: &'a str,
    pub payload: &'a Value,
    pub ticket: &'a str,
}

/// Sends `call` once. A plugin rejection is `success: false` with its `errorCode`.
pub(crate) async fn call_plugin(
    call: PluginCall<'_>,
    timeout: Duration,
) -> Result<CapabilityDispatchResult> {
    let payload =
        build_request_payload(call.payload).map_err(|e| CapabilityError::System(e.to_string()))?;
    let channel = resolve_plugin_channel(call.grpc_authority)
        .await
        .map_err(CapabilityError::System)?;
    let mut client = ExtensionPluginClient::new(channel);

    let mut metadata = std::collections::HashMap::new();
    metadata.insert("tenant_id".to_string(), call.tenant_id.to_string());
    if let Some(user_id) = call.user_id {
        metadata.insert("user_id".to_string(), user_id.to_string());
    }
    if !call.trace_id.is_empty() {
        metadata.insert("trace_id".to_string(), call.trace_id.to_string());
    }
    let mut grpc_req = Request::new(GenericRequest {
        operation: call.operation.to_string(),
        metadata,
        payload: Some(payload),
        request_id: call.request_id.to_string(),
    });
    let mut credential: tonic::metadata::MetadataValue<tonic::metadata::Ascii> =
        call.ticket.parse().map_err(|_| {
            CapabilityError::PolicyDenied("invalid plugin invocation credential".into())
        })?;
    credential.set_sensitive(true);
    grpc_req.metadata_mut().insert(
        flare_im_capability_core::plugin::invocation::PLUGIN_CALL_HEADER,
        credential,
    );

    let response = tokio::time::timeout(timeout, client.call(grpc_req))
        .await
        .map_err(|_| CapabilityError::Timeout(format!("plugin call timeout: {}", call.plugin_id)))?
        .map_err(|s| {
            CapabilityError::System(format!("plugin {} call failed: {s}", call.plugin_id))
        })?
        .into_inner();

    if response.request_id != call.request_id {
        return Err(CapabilityError::System(
            "plugin response request identity mismatch".into(),
        ));
    }
    if !response.ok {
        let code = if !response.error_code.is_empty()
            && response.error_code.len() <= 64
            && response
                .error_code
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
        {
            response.error_code
        } else {
            "PLUGIN_REJECTED".into()
        };
        return Ok(CapabilityDispatchResult {
            request_id: call.request_id.to_string(),
            plugin_id: call.plugin_id.to_string(),
            capability_id: call.operation.to_string(),
            success: false,
            data: serde_json::json!({"errorCode":code}),
            error: Some("plugin command rejected".into()),
        });
    }

    Ok(CapabilityDispatchResult::ok(
        call.request_id,
        call.plugin_id,
        call.operation,
        decode_response_payload(response.payload),
    ))
}

/// Leased data from the local control projection; never from the marketplace.
async fn client_ui_catalog(
    tenant: &str,
    user: &str,
    request_id: &str,
    routes: &PluginRouteBook,
    policy: &Arc<dyn CapabilityPolicyBackend>,
) -> Result<CapabilityDispatchResult> {
    use flare_im_capability_core::plugin::ui::{
        ClientPlugin, UI_CATALOG_OPERATION, UiActionTarget, UiCatalog,
    };
    let now = chrono::Utc::now().timestamp_millis();
    let snapshot = match &routes.authorization {
        Some(reader) => reader
            .plugin_snapshot(tenant)
            .await
            .map_err(|_| CapabilityError::PolicyDenied("plugin catalog unavailable".into()))?,
        None => None,
    };
    let mut result = UiCatalog {
        tenant_id: tenant.into(),
        version: 0,
        valid_until_ms: now + 30000,
        plugins: vec![],
    };
    if let Some(snapshot) = snapshot {
        snapshot
            .validate(now)
            .map_err(|_| CapabilityError::PolicyDenied("plugin catalog expired".into()))?;
        result.version = snapshot.version;
        result.valid_until_ms = snapshot.valid_until_ms.min(now.saturating_add(30000));
        for projected in &snapshot.plugins {
            if !projected.grant.enabled
                || projected.grant.revoked
                || now < projected.grant.valid_from_ms
                || now >= projected.grant.valid_until_ms
            {
                continue;
            }
            let manifest = projected
                .manifest()
                .map_err(|_| CapabilityError::PolicyDenied("plugin catalog invalid".into()))?;
            let mut operations = std::collections::BTreeSet::new();
            let mut permissions = std::collections::BTreeSet::new();
            for operation in manifest.operation_ids() {
                if projected.grant.approved_operations.contains(&operation)
                    && manifest.operation(&operation).is_some_and(|op| {
                        op.permission_ids
                            .iter()
                            .all(|p| projected.grant.approved_permissions.contains(p))
                    })
                    && policy
                        .ensure_dispatch_allowed(tenant, user, &operation)
                        .await
                        .is_ok()
                {
                    if let Some(op) = manifest.operation(&operation) {
                        permissions.extend(op.permission_ids.iter().cloned());
                    }
                    operations.insert(operation);
                }
            }
            let configuration = client_configuration(&manifest, projected);
            let mut extensions = manifest.client_extensions;
            // Console-only surfaces (admin-page) never reach clients; then every reference to
            // an action the viewer may not invoke goes with it (choices, rows, translations, a
            // detail whose operation is not offered).
            extensions.contributions.retain(|c| {
                c.kind.is_client_surface()
                    && c.required_permissions
                        .iter()
                        .all(|p| permissions.contains(p))
            });
            let pages = extensions
                .contributions
                .iter()
                .filter(|c| {
                    c.kind == flare_im_capability_core::plugin::manifest::UiContributionKind::Page
                })
                .map(|c| c.id.clone())
                .collect::<std::collections::BTreeSet<_>>();
            for contribution in &mut extensions.contributions {
                contribution.retain_targets(|target| match target {
                    UiActionTarget::Operation { operation } => operations.contains(operation),
                    UiActionTarget::Page { page } => pages.contains(page),
                    UiActionTarget::Native { .. } => true,
                });
            }
            if !extensions.contributions.is_empty() {
                result.valid_until_ms = result.valid_until_ms.min(projected.grant.valid_until_ms);
                result.plugins.push(ClientPlugin {
                    plugin_id: manifest.plugin_id,
                    version: manifest.version,
                    manifest_digest: projected.grant.manifest_digest.clone(),
                    grant_revision: projected.grant.revision,
                    extensions,
                    operations,
                    configuration,
                });
            }
        }
    }
    let value = serde_json::to_value(result)
        .map_err(|_| CapabilityError::System("plugin UI encoding failed".into()))?;
    Ok(CapabilityDispatchResult::ok(
        request_id,
        "flare.plugin",
        UI_CATALOG_OPERATION,
        value,
    ))
}

/// The client-visible configuration of a projected plugin: the projection's subset, kept to the
/// top-level keys the manifest marks `x-flare-client-visible` and to the protocol size limit. A
/// subset outside those bounds is dropped (logged), never sent partially.
fn client_configuration(
    manifest: &flare_im_capability_core::plugin::PluginManifest,
    projected: &flare_im_capability_core::plugin::runtime::ProjectedPlugin,
) -> serde_json::Map<String, Value> {
    if projected.client_configuration.is_empty() {
        return Default::default();
    }
    manifest
        .client_visible_configuration(&Value::Object(projected.client_configuration.clone()))
        .unwrap_or_else(|_| {
            tracing::warn!(
                plugin_id = %manifest.plugin_id,
                "client-visible plugin configuration exceeds its limit; not sent to clients"
            );
            Default::default()
        })
}
