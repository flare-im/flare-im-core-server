use crate::{
    application::handler::{resource_bridge::ResourceBridge, service_directory::ServiceDirectory},
    composition::extensions::{ServiceDispatchFactory, ServiceDispatchParts},
    domain::capability::CapabilityPolicyBackend,
    domain::repository::realtime_delivery::RealtimePacketDelivery,
    infrastructure::{
        adapters::{realtime_push::GrpcRealtimePush, resource_access::GrpcResourceAccess},
        capability::PluginRouteBook,
        persistence::postgres_resource_budget::PostgresResourceBudget,
    },
    interface::grpc::resource_bridge::PluginHostBridgeGrpc,
};
use flare_im_capability_core::plugin::invocation::PluginRuntimeVerifier;
use flare_server_core::error::{FlareError, Result};
use std::{path::Path, sync::Arc, time::Duration};
use tokio::io::AsyncReadExt;

const RUNTIME_KEYS_LIMIT: u64 = 64 * 1024;

async fn read_runtime_keys(path: &Path) -> std::io::Result<String> {
    let mut contents = String::new();
    tokio::fs::File::open(path)
        .await?
        .take(RUNTIME_KEYS_LIMIT + 1)
        .read_to_string(&mut contents)
        .await?;
    if contents.len() as u64 > RUNTIME_KEYS_LIMIT {
        return Err(std::io::Error::other("runtime key list exceeds size limit"));
    }
    Ok(contents)
}

async fn runtime_verifier() -> Result<Option<PluginRuntimeVerifier>> {
    use std::env::{VarError, var};

    let invalid = || FlareError::system("invalid plugin runtime public keys configuration");
    match (
        var("FLARE_PLUGIN_RUNTIME_PUBLIC_KEYS"),
        var("FLARE_PLUGIN_RUNTIME_PUBLIC_KEYS_FILE"),
    ) {
        (Err(VarError::NotPresent), Err(VarError::NotPresent)) => Ok(None),
        (Ok(raw), Err(VarError::NotPresent)) => PluginRuntimeVerifier::from_public_keys_json(&raw)
            .map(Some)
            .map_err(|_| invalid()),
        (Err(VarError::NotPresent), Ok(file)) => {
            let path = std::path::PathBuf::from(file);
            let mut current = read_runtime_keys(&path).await.map_err(|_| invalid())?;
            let verifier =
                PluginRuntimeVerifier::from_public_keys_json(&current).map_err(|_| invalid())?;
            let live = verifier.clone();
            tokio::spawn(async move {
                let mut refused = false;
                loop {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    match read_runtime_keys(&path).await {
                        Ok(raw) if raw == current => refused = false,
                        Ok(raw) if live.reload_public_keys(&raw).is_ok() => {
                            current = raw;
                            refused = false;
                        }
                        _ if !refused => {
                            tracing::warn!("plugin runtime public keys reload rejected");
                            refused = true;
                        }
                        _ => {}
                    }
                }
            });
            Ok(Some(verifier))
        }
        _ => Err(invalid()),
    }
}

pub async fn compose(
    routes: &Arc<PluginRouteBook>,
    policy: &Arc<dyn CapabilityPolicyBackend>,
    pool: Option<&Arc<sqlx::PgPool>>,
    service_dispatch: Option<&Arc<dyn ServiceDispatchFactory>>,
) -> Result<Option<PluginHostBridgeGrpc>> {
    let invalid =
        || FlareError::system("invalid or incomplete plugin resource bridge configuration");
    let conversation = match std::env::var("FLARE_PLUGIN_RESOURCE_CONVERSATION_URL") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(None),
        Err(_) => return Err(invalid()),
    };
    let social = match std::env::var("FLARE_PLUGIN_RESOURCE_SOCIAL_GROUP_URL") {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(_) => return Err(invalid()),
    };
    if !flare_im_capability_core::plugin::runtime::valid_grpc_authority(&conversation)
        || social.as_ref().is_some_and(|url| {
            !flare_im_capability_core::plugin::runtime::valid_grpc_authority(url)
        })
    {
        return Err(invalid());
    }
    let authorization = routes.authorization.clone().ok_or_else(invalid)?;
    let signer = routes.invocation_signer.as_ref().ok_or_else(invalid)?;
    let mut verifier = match std::env::var("FLARE_PLUGIN_CALL_PUBLIC_KEYS") {
        Ok(raw) => {
            flare_im_capability_core::plugin::invocation::PluginCallVerifier::from_public_keys_json(
                &raw,
            )
            .map_err(|_| invalid())?
        }
        Err(std::env::VarError::NotPresent) => Default::default(),
        Err(_) => return Err(invalid()),
    };
    verifier
        .insert(signer.key_id().into(), signer.public_key())
        .map_err(|_| invalid())?;
    let service_signer = Arc::new(
        flare_im_service_kit::signed_read::service_signer_from_env("plugin resource bridge")?
            .ok_or_else(invalid)?,
    );
    let realtime = realtime_push(&service_signer)?;
    // Plugin deployments' runtime credentials (`x-flare-plugin-runtime`) for the write
    // capability: `[{"key_id","deployment_id","public_key_base64"}]`. Absent: refused.
    let runtime_verifier = runtime_verifier().await?;
    let budget = Arc::new(PostgresResourceBudget(pool.cloned().ok_or_else(invalid)?));
    budget.check_schema().await.map_err(|_| invalid())?;
    // Service calls need both a router linked into this binary and the runtime credentials
    // they are signed with; without either, the bridge refuses them.
    let service_dispatch = match (service_dispatch, runtime_verifier.clone()) {
        (Some(factory), Some(runtimes)) => Some(factory.build(ServiceDispatchParts {
            directory: Arc::new(ServiceDirectory::new(
                authorization.clone(),
                policy.clone(),
                budget.clone(),
            )),
            calls: verifier.clone(),
            runtimes,
            signer: signer.clone(),
        })),
        (None, Some(_)) => {
            tracing::warn!(
                "this capability build has no plugin service router; plugin.service.dispatch is refused"
            );
            None
        }
        (_, None) => None,
    };

    Ok(Some(PluginHostBridgeGrpc(Arc::new(
        ResourceBridge::new(
            authorization,
            policy.clone(),
            verifier,
            Arc::new(GrpcResourceAccess {
                conversation_authority: conversation,
                social_group_authority: social,
                signer: service_signer,
            }),
            budget,
        )
        .with_writes(routes.state_publisher.clone(), runtime_verifier)
        .with_service_dispatch(service_dispatch)
        .with_realtime(realtime),
    ))))
}

/// `im.realtime.deliver` goes to Push Proxy `PushService` at `FLARE_PLUGIN_REALTIME_PUSH_URL`
/// (`http(s)://` or `discovery://`), signed with the Cell key. Absent: deliveries answer
/// unavailable.
fn realtime_push(
    signer: &Arc<flare_server_core::SignedContextSigner>,
) -> Result<Option<Arc<dyn RealtimePacketDelivery>>> {
    let authority = match std::env::var("FLARE_PLUGIN_REALTIME_PUSH_URL") {
        Ok(value) if !value.trim().is_empty() => value.trim().to_owned(),
        Ok(_) | Err(std::env::VarError::NotPresent) => {
            tracing::warn!(
                "FLARE_PLUGIN_REALTIME_PUSH_URL absent; im.realtime.deliver answers unavailable"
            );
            return Ok(None);
        }
        Err(_) => return Err(FlareError::system("invalid FLARE_PLUGIN_REALTIME_PUSH_URL")),
    };
    if !(authority.starts_with("discovery://") && authority.len() > "discovery://".len()
        || flare_im_capability_core::plugin::runtime::valid_grpc_authority(&authority))
    {
        return Err(FlareError::system("invalid FLARE_PLUGIN_REALTIME_PUSH_URL"));
    }
    Ok(Some(Arc::new(GrpcRealtimePush {
        authority,
        signer: signer.clone(),
    })))
}
