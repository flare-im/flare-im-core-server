//! 本机开发用的控制面：让 `capability_link_preview` 在没有控制面的开源栈上走完整个生命周期。
//!
//! 核心的远程插件调用只认控制面下发、平台签名、带租约的插件授权投影
//! （`TenantProjection.UpsertPlugins`），插件登记要平台签发、绑定请求体的注册凭据。
//! 商业部署里这两件事由平台做；开源栈没有平台，本样例在本机扮演它：
//!
//! - `broker`：注册凭据交换端点（与 flare-plugin-host、`capability_link_preview` 同一协议）；
//! - `project`：签发并推送一份插件授权投影（可撤销、可设短租约）。
//!
//! 另外两条命令扮演已认证的调用方，用 Cell 私钥签内部上下文，身份与网关、插件宿主签的相同：
//!
//! - `dispatch`：以终端用户身份调 `CapabilityService.Dispatch`；
//! - `list`：以插件服务身份查它自己的登记。
//!
//! 核心照常验签：它只信任部署时配置给它的平台公钥（`FLARE_PLATFORM_PUBLIC_KEY`）与 Cell
//! 公钥，本样例不打开任何开关、不跳过任何校验。私钥只从文件读（PKCS#8 PEM），不进命令行、
//! 不回显。只用于开发与冒烟，见 `scripts/smoke_plugin_lifecycle.sh`。
//!
//! ```text
//! capability_dev_control broker   --listen ADDR --platform-key FILE --workload-token-file FILE
//!                                 --deployment-id ID --plugin-id ID --tenant T
//! capability_dev_control project  --capability URL --platform-key FILE --tenant T --version N
//!                                 --manifest FILE --deployment-id ID --authority URL
//!                                 [--lease-secs S] [--revoked]
//! capability_dev_control dispatch --capability URL --cell-key FILE --tenant T --user U
//!                                 --operation OP --payload JSON
//! capability_dev_control list     --capability URL --cell-key FILE --tenant T --plugin-id ID
//! ```
//!
//! 输出是一行 `key=value`，供脚本断言；被拒时打印 gRPC 状态码与平台错误码（`reason=`）。

use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use flare_grpc_proto::capability::capability_service_client::CapabilityServiceClient;
use flare_grpc_proto::capability::{DispatchCapabilityRequest, ListRegisteredPluginsRequest};
use flare_grpc_proto::control::PluginsProjectionUpsert;
use flare_grpc_proto::control::tenant_projection_client::TenantProjectionClient;
use flare_im_capability_core::plugin::PluginManifest;
use flare_im_capability_core::plugin::authorization::TenantPluginGrant;
use flare_im_capability_core::plugin::runtime::{
    MAX_PROJECTION_LEASE_MS, PluginDeployment, PluginRuntimeSnapshot, ProjectedPlugin,
    configuration_digest, digest_bytes,
};
use flare_im_capability_core::plugin::workload::{
    BROKER_PATH, REGISTRATION_TOKEN_USE, RegistrationCredential, RegistrationExchange,
};
use flare_server_core::error::grpc::decode_error_detail;
use flare_server_core::grpc::client::set_signed_context_token;
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, TokenService,
};
use tonic::Request;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// 平台发给核心的控制断言（见 flare-im-service-kit `control_auth`）：投影 worker 的身份、
/// 投影受众、`control` 用途，有效期不超过 60 秒。
const PLATFORM_ISSUER: &str = "flare-platform";
const PROJECTION_WORKER: &str = "flare-platform-worker";
const PROJECTION_AUDIENCE: &str = "flare-core-projection";
const CONTROL_USE: &str = "control";
const ASSERTION_TTL_SECS: u64 = 30;

const USAGE: &str = "usage: capability_dev_control <broker|project|dispatch|list> [--key value]... \
                     (see the header of examples/capability_dev_control.rs)";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let command = args.next().ok_or(USAGE)?;
    let options = Options::parse(args)?;
    match command.as_str() {
        "broker" => broker(&options).await,
        "project" => project(&options).await,
        "dispatch" => dispatch(&options).await,
        "list" => list(&options).await,
        _ => Err(USAGE.into()),
    }
}

/// `--key value` 与不带值的 `--flag`。
struct Options(HashMap<String, Option<String>>);

impl Options {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self> {
        let mut options = HashMap::new();
        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            let key = arg
                .strip_prefix("--")
                .ok_or_else(|| format!("unexpected argument {arg}"))?;
            let value = args.next_if(|next| !next.starts_with("--"));
            options.insert(key.to_owned(), value);
        }
        Ok(Self(options))
    }

    fn get(&self, key: &str) -> Result<&str> {
        self.0
            .get(key)
            .and_then(Option::as_deref)
            .ok_or_else(|| format!("--{key} is required").into())
    }

    fn flag(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }
}

/// 读一个私钥文件；内容不打印。
fn read_key(path: &str) -> Result<String> {
    let material = std::fs::read_to_string(path).map_err(|e| format!("reading {path}: {e}"))?;
    Ok(material.trim().to_owned())
}

/// 平台签名器：注册凭据与控制断言都由平台的 Ed25519 私钥签，签发者 `flare-platform`。
fn platform_signer(path: &str) -> Result<TokenService> {
    TokenService::from_ed25519_private_key(&read_key(path)?, PLATFORM_ISSUER, ASSERTION_TTL_SECS)
        .map_err(|_| format!("{path} is not an Ed25519 PKCS#8 private key").into())
}

fn cell_signer(path: &str) -> Result<SignedContextSigner> {
    SignedContextSigner::from_private_key(&read_key(path)?)
        .map_err(|_| format!("{path} is not the Cell Ed25519 private key").into())
}

fn request_id() -> String {
    format!("dev-{}", uuid::Uuid::new_v4().simple())
}

/// 被拒的调用：gRPC 状态码，以及平台错误码（`ErrorDetail.reason`，没有时为空）。
fn print_refusal(status: &tonic::Status) {
    let reason = decode_error_detail(status)
        .map(|detail| detail.reason)
        .unwrap_or_default();
    println!(
        "refused code={:?} reason={reason} message={}",
        status.code(),
        status.message().replace('\n', " ")
    );
}

// ---- broker ------------------------------------------------------------------

struct Broker {
    platform: TokenService,
    workload_token: String,
    deployment_id: String,
    plugin_id: String,
    tenant: String,
}

/// 注册凭据交换：只为预先约定的那一个部署、插件和租户签，凭据绑定请求体（`audience`）。
async fn broker(options: &Options) -> Result<()> {
    let listen: SocketAddr = options.get("listen")?.parse()?;
    let state = Arc::new(Broker {
        platform: platform_signer(options.get("platform-key")?)?,
        workload_token: read_key(options.get("workload-token-file")?)?,
        deployment_id: options.get("deployment-id")?.to_owned(),
        plugin_id: options.get("plugin-id")?.to_owned(),
        tenant: options.get("tenant")?.to_owned(),
    });
    let app = axum::Router::new()
        .route(BROKER_PATH, axum::routing::post(exchange))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    println!("broker listening={listen}{BROKER_PATH}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn exchange(
    State(broker): State<Arc<Broker>>,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, String) {
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if bearer != Some(broker.workload_token.as_str()) {
        return (StatusCode::UNAUTHORIZED, String::new());
    }
    let Ok(exchange) = serde_json::from_slice::<RegistrationExchange>(&body) else {
        return (StatusCode::BAD_REQUEST, String::new());
    };
    let request = &exchange.request;
    if exchange.deployment_id != broker.deployment_id
        || request.plugin_id != broker.plugin_id
        || request.tenant_id != broker.tenant
    {
        return (StatusCode::FORBIDDEN, String::new());
    }
    // audience() 先按协议校验整份声明，再取请求体摘要。
    let Ok(audience) = request.audience() else {
        return (StatusCode::BAD_REQUEST, String::new());
    };
    match broker.platform.generate_with_ed25519(
        &request.plugin_id,
        Some(&request.tenant_id),
        REGISTRATION_TOKEN_USE,
        &audience,
    ) {
        Ok(token) => match serde_json::to_string(&RegistrationCredential { token }) {
            Ok(json) => (StatusCode::OK, json),
            Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, String::new()),
        },
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, String::new()),
    }
}

// ---- project -----------------------------------------------------------------

/// 投影里这一个插件的部署与授权条件。
struct Projection<'a> {
    tenant: &'a str,
    version: u64,
    deployment_id: &'a str,
    authority: &'a str,
    lease_ms: i64,
    revoked: bool,
}

/// 一份只含这一个插件的授权投影：批准清单里的全部操作、权限与数据范围。
fn snapshot(
    projection: &Projection<'_>,
    manifest_json: String,
    now: i64,
) -> Result<PluginRuntimeSnapshot> {
    let Projection {
        tenant,
        version,
        deployment_id,
        authority,
        lease_ms,
        revoked,
    } = *projection;
    let manifest = PluginManifest::from_json(manifest_json.as_bytes())?;
    let digest = digest_bytes(manifest_json.as_bytes());
    let artifact_digest = manifest
        .artifact
        .as_ref()
        .map(|artifact| artifact.digest.clone())
        .ok_or("the manifest declares no artifact")?;
    let snapshot = PluginRuntimeSnapshot {
        tenant_id: tenant.to_owned(),
        version,
        issued_at_ms: now,
        valid_until_ms: now + lease_ms,
        plugins: vec![ProjectedPlugin {
            grant: TenantPluginGrant {
                grant_id: format!("dev-{}", manifest.plugin_id),
                revision: version,
                tenant_id: tenant.to_owned(),
                plugin_id: manifest.plugin_id.clone(),
                plugin_version: manifest.version.clone(),
                manifest_digest: digest.clone(),
                approved_operations: manifest.operation_ids(),
                approved_permissions: manifest.permissions.iter().map(|p| p.id.clone()).collect(),
                approved_data_scopes: manifest
                    .permissions
                    .iter()
                    .flat_map(|p| p.data_scopes.iter().cloned())
                    .collect::<BTreeSet<_>>(),
                enabled: true,
                revoked,
                valid_from_ms: now,
                valid_until_ms: now + lease_ms,
            },
            deployment: PluginDeployment {
                deployment_id: deployment_id.to_owned(),
                plugin_id: manifest.plugin_id.clone(),
                plugin_version: manifest.version.clone(),
                manifest_digest: digest,
                artifact_digest,
                grpc_authority: authority.to_owned(),
            },
            manifest_json,
            configuration_digest: configuration_digest(&serde_json::json!({}))?,
            client_configuration: Default::default(),
        }],
    };
    // 核心收到后按同一份协议校验；这里先校验，错在本地就不必发出去。
    snapshot.validate(now)?;
    Ok(snapshot)
}

async fn project(options: &Options) -> Result<()> {
    let tenant = options.get("tenant")?;
    let version: u64 = options.get("version")?.parse()?;
    let lease_ms = match options.0.get("lease-secs") {
        Some(_) => options.get("lease-secs")?.parse::<i64>()? * 1000,
        None => MAX_PROJECTION_LEASE_MS / 2,
    };
    let manifest_path = options.get("manifest")?;
    let manifest_json = std::fs::read_to_string(manifest_path)
        .map_err(|e| format!("reading {manifest_path}: {e}"))?;
    let snapshot = snapshot(
        &Projection {
            tenant,
            version,
            deployment_id: options.get("deployment-id")?,
            authority: options.get("authority")?,
            lease_ms,
            revoked: options.flag("revoked"),
        },
        manifest_json,
        chrono::Utc::now().timestamp_millis(),
    )?;
    let assertion = platform_signer(options.get("platform-key")?)?.generate_with_ed25519(
        PROJECTION_WORKER,
        Some(tenant),
        CONTROL_USE,
        PROJECTION_AUDIENCE,
    )?;
    let mut request = Request::new(PluginsProjectionUpsert {
        app_id: tenant.to_owned(),
        version,
        snapshot_json: serde_json::to_vec(&snapshot)?,
    });
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {assertion}").parse()?);
    let mut client = TenantProjectionClient::connect(options.get("capability")?.to_owned()).await?;
    match client.upsert_plugins(request).await {
        Ok(ack) => {
            let ack = ack.into_inner();
            println!(
                "projected accepted={} current_version={}",
                ack.accepted, ack.current_version
            );
        }
        Err(status) => print_refusal(&status),
    }
    Ok(())
}

// ---- dispatch / list -----------------------------------------------------------

/// 以终端用户身份调用：与网关验过用户令牌后签的 `x-flare-ctx` 相同（actor_type=user），
/// 上下文的租户、用户、请求号与请求体一致。
async fn dispatch(options: &Options) -> Result<()> {
    let tenant = options.get("tenant")?;
    let user = options.get("user")?;
    let request_id = request_id();
    let context = cell_signer(options.get("cell-key")?)?.sign(&SignedContextIdentity::new(
        SignedActorType::User,
        user,
        tenant,
        &request_id,
    ))?;
    let mut request = Request::new(DispatchCapabilityRequest {
        capability_id: options.get("operation")?.to_owned(),
        tenant_id: tenant.to_owned(),
        user_id: user.to_owned(),
        payload_json: options.get("payload")?.to_owned(),
        request_id,
        ..Default::default()
    });
    set_signed_context_token(&mut request, &context)?;
    let mut client =
        CapabilityServiceClient::connect(options.get("capability")?.to_owned()).await?;
    match client.dispatch(request).await {
        Ok(response) => {
            let result = response.into_inner().result.unwrap_or_default();
            println!(
                "dispatched success={} plugin_id={} error={} result={}",
                result.success,
                result.plugin_id,
                result.error_message.replace('\n', " "),
                result.result_json
            );
        }
        Err(status) => print_refusal(&status),
    }
    Ok(())
}

/// 以插件服务身份查登记：与插件宿主签的身份相同（actor_type=service，actor_id 为插件 id），
/// 核心只回这个插件自己的登记。
async fn list(options: &Options) -> Result<()> {
    let tenant = options.get("tenant")?;
    let plugin_id = options.get("plugin-id")?;
    let context = cell_signer(options.get("cell-key")?)?.sign(&SignedContextIdentity::new(
        SignedActorType::Service,
        plugin_id,
        tenant,
        request_id(),
    ))?;
    let mut request = Request::new(ListRegisteredPluginsRequest {
        tenant_id: tenant.to_owned(),
        capability_id: String::new(),
    });
    set_signed_context_token(&mut request, &context)?;
    let mut client =
        CapabilityServiceClient::connect(options.get("capability")?.to_owned()).await?;
    match client.list_registered_plugins(request).await {
        Ok(response) => {
            let registered = response
                .into_inner()
                .instances
                .iter()
                .any(|instance| instance.plugin_id == plugin_id);
            println!("listed registered={registered}");
        }
        Err(status) => print_refusal(&status),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const MANIFEST: &str = include_str!("capability_link_preview.manifest.json");

    fn projection(version: u64, lease_ms: i64, revoked: bool) -> Projection<'static> {
        Projection {
            tenant: "app-a",
            version,
            deployment_id: "dep-1",
            authority: "http://127.0.0.1:7803",
            lease_ms,
            revoked,
        }
    }

    #[test]
    fn the_projection_approves_the_manifest_and_carries_its_lease() {
        let now = chrono::Utc::now().timestamp_millis();
        let s = snapshot(&projection(3, 5_000, false), MANIFEST.into(), now).unwrap();
        let plugin = &s.plugins[0];
        assert!(plugin.manifest().is_ok());
        assert!(plugin.grant.approved_operations.contains("link.preview.v1"));
        assert_eq!(
            plugin.grant.manifest_digest,
            digest_bytes(MANIFEST.as_bytes())
        );
        assert_eq!((s.version, plugin.grant.revision), (3, 3));
        assert_eq!(s.valid_until_ms, now + 5_000);
        // 租约在投影本身里：到期以后同一份投影不再成立。
        assert!(s.validate(now + 5_000).is_err());
        let revoked = snapshot(&projection(4, 5_000, true), MANIFEST.into(), now).unwrap();
        assert!(revoked.plugins[0].grant.revoked);
    }

    #[test]
    fn a_lease_beyond_the_protocol_limit_is_refused_locally() {
        let now = chrono::Utc::now().timestamp_millis();
        let too_long = snapshot(
            &projection(1, MAX_PROJECTION_LEASE_MS + 1, false),
            MANIFEST.into(),
            now,
        );
        assert!(too_long.is_err());
    }

    #[test]
    fn options_take_values_and_flags() {
        let options = Options::parse(
            ["--tenant", "app-a", "--revoked", "--version", "2"]
                .into_iter()
                .map(String::from),
        )
        .unwrap();
        assert_eq!(options.get("tenant").unwrap(), "app-a");
        assert_eq!(options.get("version").unwrap(), "2");
        assert!(options.flag("revoked"));
        assert!(options.get("revoked").is_err());
        assert!(Options::parse(["app-a"].into_iter().map(String::from)).is_err());
    }
}
