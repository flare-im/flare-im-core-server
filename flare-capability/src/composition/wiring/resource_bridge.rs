use crate::{
    application::handler::resource_bridge::ResourceBridge,
    domain::capability::CapabilityPolicyBackend,
    infrastructure::{
        adapters::resource_access::GrpcResourceAccess, capability::PluginRouteBook,
        persistence::postgres_resource_budget::PostgresResourceBudget,
    },
    interface::grpc::resource_bridge::PluginHostBridgeGrpc,
};
use flare_server_core::error::{FlareError, Result};
use std::sync::Arc;

pub async fn compose(
    routes: &Arc<PluginRouteBook>,
    policy: &Arc<dyn CapabilityPolicyBackend>,
    pool: Option<&Arc<sqlx::PgPool>>,
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
    let service_signer =
        flare_im_service_kit::signed_read::service_signer_from_env("plugin resource bridge")?
            .ok_or_else(invalid)?;
    let budget = Arc::new(PostgresResourceBudget(pool.cloned().ok_or_else(invalid)?));
    budget.check_schema().await.map_err(|_| invalid())?;
    Ok(Some(PluginHostBridgeGrpc(Arc::new(ResourceBridge::new(
        authorization,
        policy.clone(),
        verifier,
        Arc::new(GrpcResourceAccess {
            conversation_authority: conversation,
            social_group_authority: social,
            signer: Arc::new(service_signer),
        }),
        budget,
    )))))
}
