//! `CapabilityService.Administer` for the plugin registry (`flare.extension.v1.plugin_version.*`,
//! `plugin_deployment.*`, `plugin_config.*`, `plugin_app_status.*`, `plugin_data.*`, `plugin_dev.*`).
//!
//! Two callers. admin-gateway forwards a Staff context signed for `core:plugins:*`, in the same two
//! scopes as hooks. The platform worker presents its assertion (`authorization: Bearer`), the
//! credential of the tenant projection: the platform's own routes are signed with platform codes,
//! and its background work has no staff member. A platform-scope operation needs the token for `*`;
//! an application one, that application or `*`. Operations marked machine take only the assertion.
//!
//! Runtime facts have no write here: Core's reconciler is the only one who learns them
//! ([`crate::application::handler::plugin_reconcile`]), and `plugin_app_status.get` reads them.
use std::sync::Arc;

use base64::{Engine, engine::general_purpose::STANDARD};
use flare_grpc_proto::capability::{GenericRequest, GenericResponse};
use flare_im_capability_core::plugin::data::DataRequest;
use flare_im_capability_core::plugin::signature::PublisherTrustStore;
use flare_im_service_kit::control_auth::{
    PlatformActorKind, PlatformAssertionVerifier, require_platform_assertion,
};
use flare_im_service_kit::staff_context::{VerifiedStaff, require_staff_any};
use flare_server_core::auth::core_admin::PLATFORM_SCOPE_TENANT;
use flare_server_core::auth::core_admin::permission::{
    PLUGINS_PLATFORM_WRITE, PLUGINS_READ, PLUGINS_WRITE,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tonic::{Request, Response, Status};

use super::service::CapabilityGrpcServer;
use crate::application::commands::PluginRegistryCommands;
use crate::application::handler::plugin_reconcile::PluginReconciler;
use crate::application::queries::PluginRegistryQueries;
use crate::domain::plugin::{
    DeploymentFilter, DeploymentRegistration, DesiredAppState, DevAppFlag, DevRegistration,
    PluginRegistryError, RuntimePatch, VersionFilter, VersionKey, VersionStateChange,
};
use crate::domain::repository::plugin_registry::PluginRegistryRepository;

const VERSION_VERIFY: &str = "flare.extension.v1.plugin_version.verify";
const VERSION_REGISTER: &str = "flare.extension.v1.plugin_version.register";
const VERSION_SET_STATE: &str = "flare.extension.v1.plugin_version.set_state";
const VERSION_LIST: &str = "flare.extension.v1.plugin_version.list";
const VERSION_GET: &str = "flare.extension.v1.plugin_version.get";
const VERSION_ASSET: &str = "flare.extension.v1.plugin_version.asset";
const DEPLOYMENT_REGISTER: &str = "flare.extension.v1.plugin_deployment.register";
const DEPLOYMENT_LIST: &str = "flare.extension.v1.plugin_deployment.list";
const CONFIG_PUT: &str = "flare.extension.v1.plugin_config.put";
const CONFIG_GET: &str = "flare.extension.v1.plugin_config.get";
const STATUS_GET: &str = "flare.extension.v1.plugin_app_status.get";
/// A tenant migration's disabled state for one deployment; Core signs and delivers it.
const RUNTIME_APPLY: &str = "flare.extension.v1.plugin_runtime.apply";
/// One step of a platform data job (export, tenant migration); Core signs and forwards it.
const DATA_CALL: &str = "flare.extension.v1.plugin_data.call";
const DEV_REGISTER: &str = "flare.extension.v1.plugin_dev.register";
const DEV_FLAG_APP: &str = "flare.extension.v1.plugin_dev.flag_app";

/// Token tenant of a platform-scope assertion.
const ANY_TENANT: &str = "*";
const DEFAULT_VERSION_PAGE: i64 = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Access {
    Read,
    Write,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scope {
    Platform,
    Application,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct OperationSpec {
    access: Access,
    scope: Scope,
    machine_only: bool,
    /// Refused unless Capability was started in development mode.
    development: bool,
}

pub(super) fn spec_of(operation: &str) -> Option<OperationSpec> {
    let (access, scope, machine_only) = match operation {
        VERSION_VERIFY | VERSION_REGISTER | VERSION_SET_STATE | DEPLOYMENT_REGISTER
        | DEV_FLAG_APP => (Access::Write, Scope::Platform, false),
        VERSION_LIST | VERSION_GET | VERSION_ASSET | DEPLOYMENT_LIST => {
            (Access::Read, Scope::Platform, false)
        }
        CONFIG_PUT | DEV_REGISTER => (Access::Write, Scope::Application, false),
        CONFIG_GET | STATUS_GET => (Access::Read, Scope::Application, false),
        RUNTIME_APPLY | DATA_CALL => (Access::Write, Scope::Application, true),
        _ => return None,
    };
    Some(OperationSpec {
        access,
        scope,
        machine_only,
        development: matches!(operation, DEV_REGISTER | DEV_FLAG_APP),
    })
}

/// What `Administer` serves the registry with.
pub(crate) struct PluginRegistryApi {
    commands: PluginRegistryCommands,
    queries: PluginRegistryQueries,
    development: bool,
    /// Absent without a plugin call signing key: nothing converges.
    reconciler: Option<Arc<PluginReconciler>>,
}

enum Caller {
    Staff(VerifiedStaff),
    Worker { subject: String, tenant: String },
}

impl Caller {
    fn actor(&self) -> &str {
        match self {
            Self::Staff(staff) => &staff.actor_id,
            Self::Worker { subject, .. } => subject,
        }
    }

    fn require_platform(&self) -> Result<(), Status> {
        let platform = match self {
            Self::Staff(staff) => {
                staff.tenant_id == PLATFORM_SCOPE_TENANT && staff.is_platform_staff()
            }
            Self::Worker { tenant, .. } => tenant == ANY_TENANT,
        };
        platform
            .then_some(())
            .ok_or_else(|| Status::permission_denied("the plugin registry is for the platform"))
    }

    fn require_application(&self, app_id: &str) -> Result<(), Status> {
        match self {
            Self::Staff(staff) => staff.require_tenant(app_id),
            Self::Worker { tenant, .. } if tenant == ANY_TENANT || tenant == app_id => Ok(()),
            Self::Worker { .. } => Err(Status::permission_denied("control tenant mismatch")),
        }
    }
}

fn into_status(error: PluginRegistryError) -> Status {
    match &error {
        PluginRegistryError::Invalid(reason) => Status::invalid_argument(reason.clone()),
        PluginRegistryError::NotFound => Status::not_found(error.to_string()),
        PluginRegistryError::Conflict(reason) => Status::failed_precondition(reason.clone()),
        PluginRegistryError::Denied(reason) => Status::permission_denied(reason.clone()),
        PluginRegistryError::Unavailable => Status::unavailable(error.to_string()),
    }
}

fn parse<T: DeserializeOwned>(payload: &[u8]) -> Result<T, Status> {
    serde_json::from_slice(payload)
        .map_err(|_| Status::invalid_argument("invalid plugin registry request"))
}

fn answer(request_id: String, value: &impl Serialize) -> Result<Response<GenericResponse>, Status> {
    let bytes =
        serde_json::to_vec(value).map_err(|_| Status::internal("plugin registry answer"))?;
    Ok(Response::new(GenericResponse {
        ok: true,
        request_id,
        payload: Some(prost_types::Any {
            type_url: "PayloadJson".to_owned(),
            value: bytes,
        }),
        error_code: String::new(),
        error_message: String::new(),
    }))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SetState {
    plugin_id: String,
    plugin_version: String,
    #[serde(flatten)]
    change: VersionStateChange,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ListVersions {
    plugin_id: Option<String>,
    query: Option<String>,
    updated_since: Option<i64>,
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GetVersion {
    plugin_id: Option<String>,
    plugin_version: Option<String>,
    manifest_digest: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GetAsset {
    plugin_id: String,
    plugin_version: String,
    path: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PutConfig {
    #[serde(flatten)]
    desired: DesiredAppState,
    /// A tenant moved to this deployment: its facts start over there.
    #[serde(default)]
    deployment_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GetConfig {
    app_id: String,
    binding_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GetStatus {
    app_id: Option<String>,
    app_ids: Option<Vec<String>>,
    binding_ids: Option<Vec<String>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ApplyRuntime {
    app_id: String,
    binding_id: String,
    deployment_id: String,
    revision: i64,
}

impl CapabilityGrpcServer {
    pub fn with_plugin_registry(
        mut self,
        repository: Arc<dyn PluginRegistryRepository>,
        trust: PublisherTrustStore,
        platform_verifier: Option<PlatformAssertionVerifier>,
        development: bool,
        reconciler: Option<Arc<PluginReconciler>>,
    ) -> Self {
        self.plugin_registry = Some(Arc::new(PluginRegistryApi {
            commands: PluginRegistryCommands::new(Arc::clone(&repository), trust),
            queries: PluginRegistryQueries::new(repository),
            development,
            reconciler,
        }));
        self.platform_verifier = platform_verifier;
        self
    }

    /// The convergence task, for the process to run.
    pub(crate) fn plugin_reconciler(&self) -> Option<Arc<PluginReconciler>> {
        self.plugin_registry
            .as_ref()
            .and_then(|registry| registry.reconciler.clone())
    }

    fn registry_caller(
        &self,
        request: &Request<GenericRequest>,
        spec: OperationSpec,
    ) -> Result<Caller, Status> {
        if request.metadata().contains_key("authorization") {
            let actor = require_platform_assertion(request, self.platform_verifier.as_ref())?;
            actor.require_kind(PlatformActorKind::ProjectionWorker)?;
            return Ok(Caller::Worker {
                subject: actor.subject,
                tenant: actor.tenant_id,
            });
        }
        if spec.machine_only {
            return Err(Status::permission_denied(
                "this operation is the platform worker's",
            ));
        }
        let verifier = self
            .staff_verifier
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("staff context verifier not configured"))?;
        let code = match (spec.access, spec.scope) {
            (Access::Read, _) => PLUGINS_READ,
            (Access::Write, Scope::Application) => PLUGINS_WRITE,
            (Access::Write, Scope::Platform) => PLUGINS_PLATFORM_WRITE,
        };
        require_staff_any(request, verifier, &[code]).map(Caller::Staff)
    }

    /// `spec` was found for this request's operation. A write leaves an execution record: the
    /// actor, and the reason the staff context signed or the worker stated; the platform keeps the
    /// intent.
    pub(super) async fn administer_registry(
        &self,
        request: Request<GenericRequest>,
        spec: OperationSpec,
    ) -> Result<Response<GenericResponse>, Status> {
        let caller = self.registry_caller(&request, spec)?;
        let registry = self
            .plugin_registry
            .clone()
            .ok_or_else(|| Status::failed_precondition("plugin registry needs a database"))?;
        if spec.development && !registry.development {
            return Err(Status::permission_denied(
                "plugin development mode is not enabled",
            ));
        }
        if spec.scope == Scope::Platform {
            caller.require_platform()?;
        }
        let body = request.into_inner();
        let operation = body.operation.clone();
        let request_id = body.request_id.clone();
        let stated_actor = body.metadata.get("actor").cloned();
        let stated_reason = body.metadata.get("reason").cloned();
        let payload = body
            .payload
            .ok_or_else(|| Status::invalid_argument("payload required"))?
            .value;
        let (tenant, value) = run(&registry, &caller, &operation, payload).await?;
        if spec.access == Access::Write
            && !spec.machine_only
            && let Some(audit) = &self.audit
        {
            let reason = match &caller {
                Caller::Staff(staff) => staff.reason.clone(),
                Caller::Worker { .. } => stated_reason,
            };
            audit
                .record_policy_event(
                    "plugin_registry",
                    &tenant,
                    Some(stated_actor.as_deref().unwrap_or(caller.actor())),
                    None,
                    Some(operation.as_str()),
                    json!({"requestId": request_id, "reason": reason}),
                    None,
                )
                .await;
        }
        answer(request_id, &value)
    }
}

async fn run(
    registry: &PluginRegistryApi,
    caller: &Caller,
    operation: &str,
    payload: Vec<u8>,
) -> Result<(String, Value), Status> {
    let platform = PLATFORM_SCOPE_TENANT.to_owned();
    let value = |serialized: serde_json::Result<Value>| {
        serialized.map_err(|_| Status::internal("plugin registry answer"))
    };
    match operation {
        VERSION_VERIFY => {
            let detail = registry
                .commands
                .verify(payload)
                .await
                .map_err(into_status)?;
            Ok((platform, value(serde_json::to_value(detail))?))
        }
        VERSION_REGISTER => {
            let detail = registry
                .commands
                .register(payload)
                .await
                .map_err(into_status)?;
            Ok((platform, value(serde_json::to_value(detail))?))
        }
        VERSION_SET_STATE => {
            let input: SetState = parse(&payload)?;
            let row = registry
                .commands
                .set_state(&input.plugin_id, &input.plugin_version, &input.change)
                .await
                .map_err(into_status)?;
            Ok((platform, value(serde_json::to_value(row))?))
        }
        VERSION_LIST => {
            let input: ListVersions = parse(&payload)?;
            let items = registry
                .queries
                .versions(VersionFilter {
                    plugin_id: input.plugin_id,
                    query: input.query,
                    updated_since: input.updated_since,
                    limit: input.limit.unwrap_or(DEFAULT_VERSION_PAGE),
                    offset: input.offset.unwrap_or(0),
                })
                .await
                .map_err(into_status)?;
            Ok((platform, json!({"items": items})))
        }
        VERSION_GET => {
            let input: GetVersion = parse(&payload)?;
            let key = match (input.manifest_digest, input.plugin_id, input.plugin_version) {
                (Some(digest), None, None) => VersionKey::Digest(digest),
                (None, Some(plugin_id), Some(plugin_version)) => VersionKey::Release {
                    plugin_id,
                    plugin_version,
                },
                _ => return Err(Status::invalid_argument("name a release or a digest")),
            };
            let detail = registry.queries.version(&key).await.map_err(into_status)?;
            Ok((platform, value(serde_json::to_value(detail))?))
        }
        VERSION_ASSET => {
            let input: GetAsset = parse(&payload)?;
            let asset = registry
                .queries
                .asset(&input.plugin_id, &input.plugin_version, &input.path)
                .await
                .map_err(into_status)?;
            Ok((
                platform,
                json!({
                    "digest": asset.digest,
                    "mediaType": asset.media_type,
                    "bytesBase64": STANDARD.encode(&asset.bytes),
                }),
            ))
        }
        DEPLOYMENT_REGISTER => {
            let input: DeploymentRegistration = parse(&payload)?;
            let row = registry
                .commands
                .register_deployment(&input)
                .await
                .map_err(into_status)?;
            Ok((platform, value(serde_json::to_value(row))?))
        }
        DEPLOYMENT_LIST => {
            let input: DeploymentFilter = parse(&payload)?;
            let items = registry
                .queries
                .deployments(&input)
                .await
                .map_err(into_status)?;
            Ok((platform, json!({"items": items})))
        }
        CONFIG_PUT => {
            let input: PutConfig = parse(&payload)?;
            caller.require_application(&input.desired.app_id)?;
            let tenant = input.desired.app_id.clone();
            let row = registry
                .commands
                .put_config(
                    &input.desired,
                    input.deployment_id.map(RuntimePatch::rebound),
                )
                .await
                .map_err(into_status)?;
            let row = registry.queries.present(row).await.map_err(into_status)?;
            Ok((tenant, value(serde_json::to_value(row))?))
        }
        CONFIG_GET => {
            let input: GetConfig = parse(&payload)?;
            caller.require_application(&input.app_id)?;
            let row = registry
                .queries
                .state(&input.app_id, &input.binding_id)
                .await
                .map_err(into_status)?;
            Ok((input.app_id, value(serde_json::to_value(row))?))
        }
        STATUS_GET => {
            let input: GetStatus = parse(&payload)?;
            let apps = match (input.app_id, input.app_ids) {
                (Some(app), None) => {
                    caller.require_application(&app)?;
                    vec![app]
                }
                (None, Some(apps)) if matches!(caller, Caller::Worker { .. }) => {
                    caller.require_platform()?;
                    apps
                }
                _ => return Err(Status::invalid_argument("name one application")),
            };
            let items = registry
                .queries
                .states(&apps, input.binding_ids.as_deref())
                .await
                .map_err(into_status)?;
            Ok((platform, json!({"items": items})))
        }
        RUNTIME_APPLY => {
            let input: ApplyRuntime = parse(&payload)?;
            caller.require_application(&input.app_id)?;
            registry
                .reconciler
                .as_ref()
                .ok_or_else(|| Status::unavailable("plugin runtime delivery is not configured"))?
                .deliver_disabled(
                    &input.app_id,
                    &input.binding_id,
                    &input.deployment_id,
                    input.revision,
                )
                .await
                .map_err(into_status)?;
            Ok((input.app_id, json!({})))
        }
        DATA_CALL => {
            let input: DataRequest = parse(&payload)?;
            caller.require_application(&input.app_id)?;
            let tenant = input.app_id.clone();
            let answer = registry
                .reconciler
                .as_ref()
                .ok_or_else(|| Status::unavailable("plugin runtime delivery is not configured"))?
                .forward_data(&input)
                .await
                .map_err(into_status)?;
            Ok((tenant, answer))
        }
        DEV_REGISTER => {
            let input: DevRegistration = parse(&payload)?;
            caller.require_application(&input.app_id)?;
            let tenant = input.app_id.clone();
            let registered = registry
                .commands
                .register_development(input)
                .await
                .map_err(into_status)?;
            Ok((tenant, value(serde_json::to_value(registered))?))
        }
        DEV_FLAG_APP => {
            let input: DevAppFlag = parse(&payload)?;
            registry
                .commands
                .flag_development_app(&input)
                .await
                .map_err(into_status)?;
            Ok((platform, json!({})))
        }
        _ => Err(Status::unimplemented("unknown plugin registry operation")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_operation_has_one_scope_and_runtime_delivery_is_machine_only() {
        for operation in [
            VERSION_VERIFY,
            VERSION_REGISTER,
            VERSION_SET_STATE,
            VERSION_LIST,
            VERSION_GET,
            VERSION_ASSET,
            DEPLOYMENT_REGISTER,
            DEPLOYMENT_LIST,
            DEV_FLAG_APP,
        ] {
            assert_eq!(spec_of(operation).unwrap().scope, Scope::Platform);
        }
        for operation in [
            CONFIG_PUT,
            CONFIG_GET,
            STATUS_GET,
            RUNTIME_APPLY,
            DATA_CALL,
            DEV_REGISTER,
        ] {
            assert_eq!(spec_of(operation).unwrap().scope, Scope::Application);
        }
        assert!(spec_of(RUNTIME_APPLY).unwrap().machine_only);
        assert!(spec_of(DATA_CALL).unwrap().machine_only);
        assert!(!spec_of(CONFIG_PUT).unwrap().machine_only);
        assert!(spec_of("flare.extension.v1.hook_config.get").is_none());
    }

    #[test]
    fn only_the_dev_operations_need_development_mode() {
        assert!(spec_of(DEV_REGISTER).unwrap().development);
        assert!(spec_of(DEV_FLAG_APP).unwrap().development);
        assert!(!spec_of(CONFIG_PUT).unwrap().development);
        assert!(!spec_of(VERSION_REGISTER).unwrap().development);
    }
}
