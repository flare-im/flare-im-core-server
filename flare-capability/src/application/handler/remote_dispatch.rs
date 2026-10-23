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
///
/// Authorization and the plugin call share `timeout`; posting the card a result carries
/// ([`super::plugin_card::PluginCardPoster`]) runs after it with its own bounded attempts.
pub(crate) async fn dispatch_remote(
    ctx: &Ctx,
    req: &CapabilityDispatchCommand,
    routes: &Arc<PluginRouteBook>,
    policy: &Arc<dyn CapabilityPolicyBackend>,
    timeout: Duration,
) -> Result<CapabilityDispatchResult> {
    let invoked = tokio::time::timeout(
        timeout,
        invoke_authorized(ctx, req, routes, policy, timeout),
    )
    .await
    .map_err(|_| {
        CapabilityError::Timeout("plugin authorization or invocation deadline exceeded".into())
    })??;
    match invoked {
        Invoked::Answered(result) => Ok(result),
        Invoked::Plugin(call) => complete_call(routes, req, *call).await,
    }
}

/// What the authorized part of a dispatch produced.
enum Invoked {
    /// Answered by Core itself (the client catalog).
    Answered(CapabilityDispatchResult),
    /// A plugin answered; its result may still post a card and publish shared state.
    Plugin(Box<InvokedCall>),
}

struct InvokedCall {
    result: CapabilityDispatchResult,
    tenant: String,
    manifest: flare_im_capability_core::plugin::PluginManifest,
    declares_publish: bool,
}

fn plugin_denied(code: &str, message: &str) -> CapabilityError {
    CapabilityError::plugin(code, message)
}

/// Operations a user ticket never invokes: the console's admin operations and the platform's
/// lifecycle callbacks (`entityLifecycle.unbound`).
pub(crate) fn restricted_operations(
    manifest: &flare_im_capability_core::plugin::PluginManifest,
) -> std::collections::BTreeSet<String> {
    let mut restricted = manifest.admin_operations();
    restricted.extend(
        manifest
            .client_extensions
            .contributions
            .iter()
            .filter_map(|c| c.entity_lifecycle.as_ref().map(|l| l.unbound.clone())),
    );
    restricted
}

/// The projected plugin a user may invoke `operation` of, and its manifest (plugin platform v2
/// §2): the plugin is enabled for the tenant (in the projection, not revoked, inside its lease),
/// the operation is one of its user operations (neither an admin nor a lifecycle operation) and
/// approved by the installation, and no emergency switch stops it. The caller is the tenant's
/// authenticated user (the verified dispatch origin). Every user of the tenant holds the
/// permissions the installation approved: who may do what inside the plugin (members, roles) is
/// the plugin's own rule, checked through the resource bridge. No per-user grant is read.
async fn authorize_user_operation<'s>(
    snapshot: &'s flare_im_capability_core::plugin::runtime::PluginRuntimeSnapshot,
    tenant: &str,
    user: &str,
    operation: &str,
    policy: &Arc<dyn CapabilityPolicyBackend>,
    now: i64,
) -> Result<(
    &'s flare_im_capability_core::plugin::runtime::ProjectedPlugin,
    flare_im_capability_core::plugin::PluginManifest,
)> {
    use flare_im_service_kit::plugin_platform::{
        PLUGIN_DISABLED, PLUGIN_NOT_INSTALLED, PLUGIN_PERMISSION_DENIED,
    };
    let mut denial = (
        PLUGIN_NOT_INSTALLED,
        "plugin is not installed for this application",
    );
    let mut found = None;
    for projected in &snapshot.plugins {
        let Ok(manifest) = projected.manifest() else {
            continue;
        };
        if manifest.operation(operation).is_none() {
            continue;
        }
        let grant = &projected.grant;
        if grant.revoked {
            continue;
        }
        if !grant.enabled || now < grant.valid_from_ms || now >= grant.valid_until_ms {
            denial = (PLUGIN_DISABLED, "plugin is disabled for this application");
            continue;
        }
        found = Some((projected, manifest));
        break;
    }
    let (projected, manifest) = found.ok_or_else(|| plugin_denied(denial.0, denial.1))?;
    if restricted_operations(&manifest).contains(operation) {
        return Err(plugin_denied(
            PLUGIN_PERMISSION_DENIED,
            "operation is not a user operation of this plugin",
        ));
    }
    policy
        .ensure_not_suspended(tenant, operation)
        .await
        .map_err(|error| match error {
            CapabilityError::PolicyDenied(_) => {
                plugin_denied(PLUGIN_DISABLED, "plugin operation is stopped")
            }
            other => other,
        })?;
    projected
        .authorize(&PluginInvocation {
            tenant_id: tenant,
            actor_id: user,
            operation,
            actor_permissions: &projected.grant.approved_permissions,
            platform_enabled: true,
            now_ms: now,
        })
        .map_err(|_| {
            plugin_denied(
                PLUGIN_PERMISSION_DENIED,
                "operation is not approved for this application",
            )
        })?;
    Ok((projected, manifest))
}

async fn invoke_authorized(
    ctx: &Ctx,
    req: &CapabilityDispatchCommand,
    routes: &Arc<PluginRouteBook>,
    policy: &Arc<dyn CapabilityPolicyBackend>,
    timeout: Duration,
) -> Result<Invoked> {
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
        return client_ui_catalog(&tenant, request_id, routes, policy)
            .await
            .map(Invoked::Answered);
    }
    let snapshot = routes
        .authorization
        .as_ref()
        .ok_or_else(|| denied(()))?
        .plugin_snapshot(&tenant)
        .await
        .map_err(|_| denied(()))?
        .ok_or_else(|| {
            plugin_denied(
                flare_im_service_kit::plugin_platform::PLUGIN_NOT_INSTALLED,
                "no plugin is installed for this application",
            )
        })?;
    let now = chrono::Utc::now().timestamp_millis();
    snapshot.validate(now).map_err(|_| {
        plugin_denied(
            flare_im_service_kit::plugin_platform::PLUGIN_DISABLED,
            "the plugin projection of this application lapsed",
        )
    })?;
    let (projected, manifest) =
        authorize_user_operation(&snapshot, &tenant, user, &req.capability_id, policy, now).await?;
    let operation = manifest
        .operation(&req.capability_id)
        .ok_or_else(|| denied(()))?;
    let timeout = timeout.min(Duration::from_millis(u64::from(operation.timeout_ms)));
    let input = req.payload.as_ref().unwrap_or(&Value::Null);
    card_binding_guard(routes, &tenant, &manifest, req, input).await?;
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
    let declares_publish = operation
        .host_capabilities
        .contains(&HostCapability::EntityStatePublish);
    Ok(Invoked::Plugin(Box::new(InvokedCall {
        result,
        tenant,
        manifest,
        declares_publish,
    })))
}

/// After a successful plugin call: post the card the result carries (`data.message`, replaced by
/// `data.posted`), then publish its `data.sharedState`.
async fn complete_call(
    routes: &PluginRouteBook,
    req: &CapabilityDispatchCommand,
    call: InvokedCall,
) -> Result<CapabilityDispatchResult> {
    let InvokedCall {
        mut result,
        tenant,
        manifest,
        declares_publish,
    } = call;
    if !result.success {
        return Ok(result);
    }
    if result.data.get("message").is_some() {
        let unconfigured;
        let poster = match &routes.card_poster {
            Some(poster) => poster.as_ref(),
            None => {
                unconfigured = super::plugin_card::PluginCardPoster::new(
                    None,
                    routes.entity_bindings.clone(),
                    routes.lifecycle.clone(),
                );
                &unconfigured
            }
        };
        let data = std::mem::take(&mut result.data);
        result.data = poster
            .post_result(
                super::plugin_card::CardPostCall {
                    tenant_id: &tenant,
                    user_id: req.user_id.as_deref().unwrap_or_default(),
                    conversation_id: req.conversation_id.as_deref(),
                    request_id: req.request_id.as_deref().unwrap_or_default(),
                    input: req.payload.as_ref().unwrap_or(&Value::Null),
                    manifest: &manifest,
                },
                data,
            )
            .await?;
    }
    publish_call_result(
        routes,
        &tenant,
        &manifest.plugin_id,
        req.conversation_id.as_deref(),
        declares_publish,
        &result.data,
    );
    Ok(result)
}

/// A user call of an operation a card passes its entity to (some message-content contribution
/// with an `entityPath` binds it into an input of this operation: a card action, the detail
/// operation, or a detail action; see [`super::plugin_state::entity_inputs`]) is accepted only
/// from the card bound to that entity, in the call's conversation (plugin platform v2 §1):
///
/// 1. the call names its message (`message_id`), and that message is bound — looked up by the
///    message, not by the entity the input claims;
/// 2. the binding is this plugin's and in the call's conversation; a recalled card is
///    `PLUGIN_MESSAGE_RECALLED`;
/// 3. the input carries, at an input name the binding's own contribution uses, a valid entity id
///    equal to the bound entity, and every other entity input it fills names the same entity.
///
/// Anything else — no message, a forged or forwarded copy, another plugin's card, the entity
/// missing, malformed or naming another entity — is `PLUGIN_CARD_UNBOUND` before the plugin. An
/// operation no card passes an entity to (creating one) is not affected. The check fails closed:
/// without bindings it refuses.
async fn card_binding_guard(
    routes: &PluginRouteBook,
    tenant: &str,
    manifest: &flare_im_capability_core::plugin::PluginManifest,
    req: &CapabilityDispatchCommand,
    input: &Value,
) -> Result<()> {
    use flare_im_service_kit::plugin_platform::{PLUGIN_CARD_UNBOUND, PLUGIN_MESSAGE_RECALLED};
    let inputs = super::plugin_state::entity_inputs(manifest, &req.capability_id);
    if inputs.is_empty() {
        return Ok(());
    }
    let unbound = || {
        plugin_denied(
            PLUGIN_CARD_UNBOUND,
            "the call does not come from the card bound to this entity",
        )
    };
    let store = routes.entity_bindings.as_ref().ok_or_else(|| {
        CapabilityError::System("plugin entity bindings are not configured".into())
    })?;
    let Some(message_id) = req.message_id.as_deref().filter(|id| !id.is_empty()) else {
        return Err(unbound());
    };
    let binding = store
        .binding_by_message(tenant, message_id)
        .await
        .map_err(|_| CapabilityError::System("plugin entity bindings unavailable".into()))?
        .filter(|binding| binding.plugin_id == manifest.plugin_id)
        .ok_or_else(unbound)?;
    if binding.recalled {
        return Err(plugin_denied(
            PLUGIN_MESSAGE_RECALLED,
            "the plugin card was recalled",
        ));
    }
    if req.conversation_id.as_deref() != Some(binding.conversation_id.as_str()) {
        return Err(unbound());
    }
    // Every entity input the call fills must name the bound entity (a plugin may read any of
    // them), and one of the binding contribution's own inputs must carry it.
    let mut carried = false;
    for (contribution, name) in &inputs {
        if input.get(name.as_str()).is_none() {
            continue;
        }
        match super::plugin_state::entity_at(input, name) {
            Some(entity) if entity == binding.entity => {
                carried |= *contribution == binding.contribution_id;
            }
            _ => return Err(unbound()),
        }
    }
    if carried { Ok(()) } else { Err(unbound()) }
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

/// One `ExtensionPlugin.Call` with a signed ticket (user, staff or platform system) in
/// `x-flare-plugin-call`.
pub(crate) struct PluginCall<'a> {
    pub grpc_authority: &'a str,
    pub plugin_id: &'a str,
    pub operation: &'a str,
    pub tenant_id: &'a str,
    /// The invoking user (metadata only; the ticket is the identity). `None` for staff and
    /// system calls.
    pub user_id: Option<&'a str>,
    pub request_id: &'a str,
    pub trace_id: &'a str,
    pub payload: &'a Value,
    pub ticket: &'a str,
}

/// Sends `call` once. A plugin rejection is `success: false` with its `errorCode`, whether the
/// plugin answered `ok: false` or refused with a definite gRPC status (see [`PluginAnswer`]).
pub(crate) async fn call_plugin(
    call: PluginCall<'_>,
    timeout: Duration,
) -> Result<CapabilityDispatchResult> {
    let request_id = call.request_id.to_owned();
    let plugin_id = call.plugin_id.to_owned();
    let operation = call.operation.to_owned();
    Ok(match call_plugin_answer(call, timeout).await? {
        PluginAnswer::Answered(result) => result,
        PluginAnswer::Refused { reason, .. } => {
            rejection(&request_id, &plugin_id, &operation, &reason)
        }
    })
}

/// How a plugin endpoint answered one call.
#[derive(Debug)]
pub(crate) enum PluginAnswer {
    /// The plugin ran the command: a result, or its own refusal (`ok: false`, `success: false`).
    Answered(CapabilityDispatchResult),
    /// The plugin, or its host before running the command, refused with a definite gRPC status
    /// (invalid input, failed precondition, not found, conflict, refused credential,
    /// unimplemented). `reason` is the status's `ErrorDetail.reason` (may be empty). A host
    /// refuses the ticket itself with `Unauthenticated` (it does not verify under the deployment's
    /// keys, or the header is missing or repeated) or `PermissionDenied` (it does not match the
    /// call): the command never ran. A host whose projection is not current answers
    /// `Unavailable` instead, which is not a refusal (the outcome stays open and is retried).
    Refused { code: tonic::Code, reason: String },
}

impl PluginAnswer {
    /// A refusal of the host's ticket checks rather than of the plugin's business rules: the
    /// command never ran.
    pub(crate) fn is_credential_refusal(&self) -> bool {
        matches!(
            self,
            Self::Refused {
                code: tonic::Code::Unauthenticated | tonic::Code::PermissionDenied,
                ..
            }
        )
    }
}

/// [`call_plugin`] keeping a definite gRPC refusal apart from a plugin answer.
pub(crate) async fn call_plugin_answer(
    call: PluginCall<'_>,
    timeout: Duration,
) -> Result<PluginAnswer> {
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

    let response = match tokio::time::timeout(timeout, client.call(grpc_req))
        .await
        .map_err(|_| CapabilityError::Timeout(format!("plugin call timeout: {}", call.plugin_id)))?
    {
        Ok(response) => response.into_inner(),
        Err(status) => return plugin_status_outcome(&call, status),
    };

    if response.request_id != call.request_id {
        return Err(CapabilityError::System(
            "plugin response request identity mismatch".into(),
        ));
    }
    if !response.ok {
        return Ok(PluginAnswer::Answered(rejection(
            call.request_id,
            call.plugin_id,
            call.operation,
            &response.error_code,
        )));
    }

    Ok(PluginAnswer::Answered(CapabilityDispatchResult::ok(
        call.request_id,
        call.plugin_id,
        call.operation,
        decode_response_payload(response.payload),
    )))
}

/// The code a plugin refusal is answered with: the plugin's own code when well formed and not in
/// the platform's namespaces, otherwise `PLUGIN_REJECTED`. `PLUGIN_` and `CAPABILITY_` codes are
/// the platform's (`PLUGIN_CARD_NOT_POSTED`, `PLUGIN_DISABLED`, `CAPABILITY_OUTCOME_UNKNOWN`):
/// clients act on them (roll back a pending card, drop the catalog, retry), so a plugin must not
/// be able to answer one.
pub(crate) fn plugin_error_code(error_code: &str) -> &str {
    use flare_im_service_kit::plugin_platform::{PLUGIN_REJECTED, is_error_code};
    if is_error_code(error_code)
        && !error_code.starts_with("PLUGIN_")
        && !error_code.starts_with("CAPABILITY_")
    {
        error_code
    } else {
        PLUGIN_REJECTED
    }
}

/// A rejection the plugin answered: `success: false` and its `errorCode` as
/// [`plugin_error_code`] keeps it. The API gateway answers it 422 with that code.
fn rejection(
    request_id: &str,
    plugin_id: &str,
    operation: &str,
    error_code: &str,
) -> CapabilityDispatchResult {
    CapabilityDispatchResult {
        request_id: request_id.to_string(),
        plugin_id: plugin_id.to_string(),
        capability_id: operation.to_string(),
        success: false,
        data: serde_json::json!({ "errorCode": plugin_error_code(error_code) }),
        error: Some("plugin command rejected".into()),
    }
}

/// A gRPC error from the plugin. A status the plugin (or its host, before running the command)
/// answers definitely — invalid input, failed precondition, not found, conflict, refused
/// credential, unimplemented — is [`PluginAnswer::Refused`] with the code its `ErrorDetail`
/// carries: the command did not run. Anything else (unavailable, deadline, internal, unknown, a
/// response over the size limit) leaves the outcome unknown.
fn plugin_status_outcome(call: &PluginCall<'_>, status: Status) -> Result<PluginAnswer> {
    use tonic::Code;
    match status.code() {
        code @ (Code::InvalidArgument
        | Code::FailedPrecondition
        | Code::NotFound
        | Code::AlreadyExists
        | Code::PermissionDenied
        | Code::Unauthenticated
        | Code::Unimplemented) => {
            let reason = flare_server_core::error::grpc::decode_error_detail(&status)
                .map(|detail| detail.reason)
                .unwrap_or_default();
            tracing::debug!(
                plugin_id = call.plugin_id,
                operation = call.operation,
                ?code,
                "plugin refused the call"
            );
            Ok(PluginAnswer::Refused { code, reason })
        }
        _ => Err(CapabilityError::System(format!(
            "plugin {} call failed: {status}",
            call.plugin_id
        ))),
    }
}

/// Leased data from the local control projection; never from the marketplace.
///
/// Every authenticated user of the tenant sees the same catalog: each enabled plugin with the user
/// operations its installation approved (admin and lifecycle operations never reach clients) that
/// no emergency switch stops, and the client contributions those operations keep. Plugins the
/// tenant has but the viewer cannot use are listed in `unavailable` with the reason (disabled,
/// not installed after a revocation, an unreadable manifest, nothing offered), so a card of one
/// says why it is inert. One plugin's problem never fails the catalog.
async fn client_ui_catalog(
    tenant: &str,
    request_id: &str,
    routes: &PluginRouteBook,
    policy: &Arc<dyn CapabilityPolicyBackend>,
) -> Result<CapabilityDispatchResult> {
    use flare_im_capability_core::plugin::ui::{
        ClientPlugin, UI_CATALOG_OPERATION, UiActionTarget, UiCatalog, UiUnavailablePlugin,
        UiUnavailableReason,
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
        unavailable: vec![],
    };
    let mut unavailable: std::collections::BTreeMap<String, UiUnavailableReason> =
        Default::default();
    let mut note = |plugin_id: &str, reason: UiUnavailableReason| {
        // A disabled version says more than a revoked one.
        let entry = unavailable.entry(plugin_id.to_owned()).or_insert(reason);
        if reason == UiUnavailableReason::Disabled {
            *entry = reason;
        }
    };
    if let Some(snapshot) = snapshot {
        snapshot
            .validate(now)
            .map_err(|_| CapabilityError::PolicyDenied("plugin catalog expired".into()))?;
        result.version = snapshot.version;
        result.valid_until_ms = snapshot.valid_until_ms.min(now.saturating_add(30000));
        for projected in &snapshot.plugins {
            let grant = &projected.grant;
            if grant.revoked {
                note(&grant.plugin_id, UiUnavailableReason::NotInstalled);
                continue;
            }
            if !grant.enabled || now < grant.valid_from_ms || now >= grant.valid_until_ms {
                note(&grant.plugin_id, UiUnavailableReason::Disabled);
                continue;
            }
            let Ok(manifest) = projected.manifest() else {
                tracing::warn!(tenant, plugin_id = %grant.plugin_id, "projected plugin manifest unreadable; left out of the catalog");
                note(&grant.plugin_id, UiUnavailableReason::IncompatibleVersion);
                continue;
            };
            let restricted = restricted_operations(&manifest);
            let mut operations = std::collections::BTreeSet::new();
            let mut permissions = std::collections::BTreeSet::new();
            let mut stopped = false;
            for operation in manifest.operation_ids() {
                if restricted.contains(&operation)
                    || !grant.approved_operations.contains(&operation)
                    || !manifest.operation(&operation).is_some_and(|op| {
                        op.permission_ids
                            .iter()
                            .all(|p| grant.approved_permissions.contains(p))
                    })
                {
                    continue;
                }
                if policy
                    .ensure_not_suspended(tenant, &operation)
                    .await
                    .is_err()
                {
                    stopped = true;
                    continue;
                }
                if let Some(op) = manifest.operation(&operation) {
                    permissions.extend(op.permission_ids.iter().cloned());
                }
                operations.insert(operation);
            }
            let configuration = client_configuration(&manifest, projected);
            let mut extensions = manifest.client_extensions;
            let declared_client_surfaces = extensions
                .contributions
                .iter()
                .any(|c| c.kind.is_client_surface());
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
            if extensions.contributions.is_empty() {
                if declared_client_surfaces {
                    note(
                        &grant.plugin_id,
                        if stopped {
                            UiUnavailableReason::Disabled
                        } else {
                            UiUnavailableReason::NoPermission
                        },
                    );
                }
                continue;
            }
            result.valid_until_ms = result.valid_until_ms.min(grant.valid_until_ms);
            result.plugins.push(ClientPlugin {
                plugin_id: manifest.plugin_id,
                version: manifest.version,
                manifest_digest: grant.manifest_digest.clone(),
                grant_revision: grant.revision,
                extensions,
                operations,
                configuration,
            });
        }
    }
    let available: std::collections::BTreeSet<&str> = result
        .plugins
        .iter()
        .map(|p| p.plugin_id.as_str())
        .collect();
    result.unavailable = unavailable
        .into_iter()
        .filter(|(plugin_id, _)| !available.contains(plugin_id.as_str()))
        .map(|(plugin_id, reason)| UiUnavailablePlugin { plugin_id, reason })
        .collect();
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
