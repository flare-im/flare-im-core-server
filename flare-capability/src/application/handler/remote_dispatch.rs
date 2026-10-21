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
    authorization::PluginInvocation, invocation::PluginCallClaims,
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
    tokio::time::timeout(timeout, invoke_once(ctx, req, &endpoint, timeout, &ticket))
        .await
        .map_err(|_| CapabilityError::Timeout("plugin invocation deadline exceeded".into()))?
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

fn build_request_payload(
    req: &CapabilityDispatchCommand,
) -> flare_server_core::error::Result<prost_types::Any> {
    let payload_json = serde_json::to_string(req.payload.as_ref().unwrap_or(&Value::Null))
        .map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "serialize request payload_json: {e}"
            ))
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

    let payload = build_request_payload(req).map_err(|e| CapabilityError::System(e.to_string()))?;
    let channel = resolve_plugin_channel(endpoint.grpc_authority.as_str())
        .await
        .map_err(CapabilityError::System)?;
    let mut client = ExtensionPluginClient::new(channel);

    let mut metadata = std::collections::HashMap::new();
    metadata.insert("tenant_id".to_string(), tenant_id);
    if !user_id.is_empty() {
        metadata.insert("user_id".to_string(), user_id);
    }
    let trace_id = ctx.trace_id();
    if !trace_id.is_empty() {
        metadata.insert("trace_id".to_string(), trace_id.to_string());
    }
    let mut grpc_req = Request::new(GenericRequest {
        operation: req.capability_id.clone(),
        metadata,
        payload: Some(payload),
        request_id: request_id.clone(),
    });
    let mut credential: tonic::metadata::MetadataValue<tonic::metadata::Ascii> =
        ticket.parse().map_err(|_| {
            CapabilityError::PolicyDenied("invalid plugin invocation credential".into())
        })?;
    credential.set_sensitive(true);
    grpc_req.metadata_mut().insert(
        flare_im_capability_core::plugin::invocation::PLUGIN_CALL_HEADER,
        credential,
    );

    let response = tokio::time::timeout(timeout, client.call(grpc_req))
        .await
        .map_err(|_| {
            CapabilityError::Timeout(format!("plugin call timeout: {}", endpoint.plugin_id))
        })?
        .map_err(|s| {
            CapabilityError::System(format!("plugin {} call failed: {s}", endpoint.plugin_id))
        })?
        .into_inner();

    if response.request_id != request_id {
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
            request_id,
            plugin_id: endpoint.plugin_id.clone(),
            capability_id: req.capability_id.clone(),
            success: false,
            data: serde_json::json!({"errorCode":code}),
            error: Some("plugin command rejected".into()),
        });
    }

    Ok(CapabilityDispatchResult::ok(
        request_id,
        endpoint.plugin_id.clone(),
        req.capability_id.clone(),
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
            let mut extensions = manifest.client_extensions;
            extensions.contributions.retain(|c| {
                c.required_permissions
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
                contribution
                    .schema
                    .actions
                    .retain(|action| match &action.target {
                        UiActionTarget::Operation { operation } => operations.contains(operation),
                        UiActionTarget::Page { page } => pages.contains(page),
                        UiActionTarget::Native { .. } => true,
                    });
                let action_ids = contribution
                    .schema
                    .actions
                    .iter()
                    .map(|action| action.id.as_str())
                    .collect::<std::collections::BTreeSet<_>>();
                for translation in contribution.schema.translations.values_mut() {
                    translation
                        .actions
                        .retain(|id, _| action_ids.contains(id.as_str()));
                }
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
