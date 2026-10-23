//! 插件登记的读模型装配：有库时接 `capability_plugin_endpoints`，启动载入、周期重读、健康落库、
//! TTL 清理；没有库时只在本进程。

use std::sync::Arc;
use std::time::Duration;

use flare_server_core::error::{FlareError, Result};
use sqlx::PgPool;

use crate::infrastructure::capability::PluginRouteBook;
use crate::infrastructure::config::CapabilityRuntimeConfig;
use crate::infrastructure::persistence::PostgresPluginEndpointRepository;

/// TTL 清理的周期：清理只删超过 TTL（至少 5 分钟）的行，每分钟一次足够。
const CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

pub(super) fn plugin_route_book(db_pool: Option<&Arc<PgPool>>) -> PluginRouteBook {
    match db_pool {
        Some(pool) => PluginRouteBook::with_store(Arc::new(PostgresPluginEndpointRepository::new(
            Arc::clone(pool),
        ))),
        None => {
            tracing::warn!(
                "no database: plugin registrations (RegisterPluginEndpoint) are process-local; \
                 other instances do not see them and a restart loses them"
            );
            PluginRouteBook::new()
        }
    }
}

pub(super) fn runtime_authorization(
    routes: PluginRouteBook,
    pool: Option<&Arc<PgPool>>,
) -> Result<PluginRouteBook> {
    use base64::Engine;
    use flare_im_capability_core::plugin::invocation::PluginCallSigner;
    let signer = match std::env::var("FLARE_PLUGIN_CALL_SIGNING_KEY") {
        Ok(raw) => {
            let seed: [u8; 32] = base64::engine::general_purpose::STANDARD
                .decode(raw.trim())
                .ok()
                .and_then(|v| v.try_into().ok())
                .ok_or_else(|| FlareError::system("invalid plugin invocation signing key"))?;
            let id = std::env::var("FLARE_PLUGIN_CALL_KEY_ID")
                .map_err(|_| FlareError::system("plugin invocation key id required"))?;
            Some(Arc::new(PluginCallSigner::new(id, seed).map_err(|_| {
                FlareError::system("invalid plugin invocation key id")
            })?))
        }
        Err(std::env::VarError::NotPresent) => {
            tracing::warn!("plugin invocation signer absent; remote plugin calls fail closed");
            None
        }
        Err(_) => return Err(FlareError::system("invalid plugin invocation key encoding")),
    };
    let publisher = plugin_state_publisher(pool)?;
    Ok(match pool{Some(pool)=>routes.with_runtime_authorization(Arc::new(crate::infrastructure::persistence::postgres_plugin_authorization::PostgresPluginAuthorization{pool:pool.clone()}),signer),None=>routes}.with_state_publisher(publisher))
}

/// The platform card path and the entity lifecycle outbox (plugin platform v2 §1, §3), on a
/// database. The bindings check card actions; the outbox (migration 0018, checked at startup)
/// carries `recalled` and `post_failed` notifications, delivered by [`lifecycle_worker`]. Posting
/// needs `FLARE_PLUGIN_CARD_INGEST_URL` (Message Ingest), `FLARE_CAPABILITY_CTX_PRIVATE_KEY`
/// (Capability's own context key, `kid=capability`), `FLARE_PLUGIN_RESOURCE_CONVERSATION_URL` and
/// `FLARE_INTERNAL_CTX_PRIVATE_KEY` (the route read); without them every card a plugin returns is
/// given up and the plugin told `post_failed`.
pub(super) async fn card_posting(
    routes: PluginRouteBook,
    pool: Option<&Arc<PgPool>>,
) -> Result<PluginRouteBook> {
    use crate::application::handler::plugin_card::PluginCardPoster;
    use crate::domain::repository::entity_lifecycle::EntityLifecycleOutbox;
    use crate::domain::repository::plugin_state::PluginEntityBindingStore;
    use crate::infrastructure::persistence::postgres_entity_lifecycle::PostgresEntityLifecycleOutbox;
    use crate::infrastructure::persistence::postgres_plugin_entity_bindings::PostgresPluginEntityBindings;
    let Some(pool) = pool else {
        return Ok(routes);
    };
    let outbox = PostgresEntityLifecycleOutbox { pool: pool.clone() };
    outbox.check_schema().await.map_err(|_| {
        FlareError::system("plugin_entity_lifecycle_outbox missing (migration 0018 applied?)")
    })?;
    let outbox: Arc<dyn EntityLifecycleOutbox> = Arc::new(outbox);
    let bindings: Arc<dyn PluginEntityBindingStore> =
        Arc::new(PostgresPluginEntityBindings { pool: pool.clone() });
    let delivery = card_delivery()?;
    if delivery.is_none() {
        tracing::warn!(
            "plugin card posting not configured; cards plugins return are given up (post_failed)"
        );
    }
    let poster = PluginCardPoster::new(delivery, Some(bindings.clone()), Some(outbox.clone()));
    Ok(routes
        .with_entity_bindings(Some(bindings))
        .with_card_posting(Some(Arc::new(poster)), Some(outbox)))
}

/// The worker delivering the lifecycle outbox: needs the outbox, the projection and the plugin
/// invocation signer (the system ticket). Without the signer the rows wait.
pub(super) fn lifecycle_worker(
    routes: &PluginRouteBook,
    plugin_call_timeout: Duration,
) -> Option<crate::application::handler::entity_lifecycle::EntityLifecycleWorker> {
    match (
        &routes.lifecycle,
        &routes.authorization,
        &routes.invocation_signer,
    ) {
        (Some(outbox), Some(authorization), Some(signer)) => Some(
            crate::application::handler::entity_lifecycle::EntityLifecycleWorker::new(
                outbox.clone(),
                authorization.clone(),
                signer.clone(),
                plugin_call_timeout,
            )
            .with_bindings(routes.entity_bindings.clone()),
        ),
        (Some(_), _, _) => {
            tracing::warn!(
                "plugin invocation signer absent; entity lifecycle notifications wait undelivered"
            );
            None
        }
        _ => None,
    }
}

fn optional_env(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(Some(value.trim().to_owned())),
        Ok(_) | Err(std::env::VarError::NotPresent) => Ok(None),
        Err(_) => Err(FlareError::system(format!("invalid {name}"))),
    }
}

/// Capability's own context signer (`FLARE_CAPABILITY_CTX_PRIVATE_KEY`, `kid=capability`): the
/// key of its platform card posts and of its plugin shared state publishes. `None` when unset.
fn capability_signer() -> Result<Option<flare_server_core::SignedContextSigner>> {
    optional_env("FLARE_CAPABILITY_CTX_PRIVATE_KEY")?
        .map(|key| capability_signer_from(&key))
        .transpose()
}

fn capability_signer_from(key: &str) -> Result<flare_server_core::SignedContextSigner> {
    // The key is Capability's own: a Cell key every component holds would let any of them post
    // plugin cards or publish shared state.
    if optional_env(flare_im_service_kit::push_delivery::INTERNAL_CTX_PRIVATE_KEY_ENV)?
        .is_some_and(|cell| cell == key)
    {
        return Err(FlareError::system(
            "FLARE_CAPABILITY_CTX_PRIVATE_KEY must not be the Cell key (FLARE_INTERNAL_CTX_PRIVATE_KEY)",
        ));
    }
    flare_server_core::SignedContextSigner::from_private_key(key)
        .and_then(|signer| signer.with_kid(flare_im_service_kit::plugin_platform::CARD_POST_KID))
        .map_err(|_| FlareError::system("invalid FLARE_CAPABILITY_CTX_PRIVATE_KEY"))
}

fn card_delivery()
-> Result<Option<Arc<dyn crate::domain::repository::plugin_card::PluginCardDelivery>>> {
    use crate::infrastructure::adapters::plugin_card_ingest::GrpcPluginCardDelivery;
    let ingest = optional_env("FLARE_PLUGIN_CARD_INGEST_URL")?;
    let key = optional_env("FLARE_CAPABILITY_CTX_PRIVATE_KEY")?;
    let (Some(ingest), Some(key)) = (ingest, key) else {
        return Ok(None);
    };
    let authority_ok = |value: &str| {
        value.starts_with("discovery://") && value.len() > "discovery://".len()
            || flare_im_capability_core::plugin::runtime::valid_grpc_authority(value)
    };
    if !authority_ok(&ingest) {
        return Err(FlareError::system("invalid FLARE_PLUGIN_CARD_INGEST_URL"));
    }
    let conversation = optional_env("FLARE_PLUGIN_RESOURCE_CONVERSATION_URL")?
        .filter(|value| authority_ok(value))
        .ok_or_else(|| {
            FlareError::system("plugin card posting needs FLARE_PLUGIN_RESOURCE_CONVERSATION_URL")
        })?;
    let post_signer = capability_signer_from(&key)?;
    let read_signer =
        flare_im_service_kit::signed_read::service_signer_from_env("plugin card routing")?
            .ok_or_else(|| {
                FlareError::system("plugin card posting needs FLARE_INTERNAL_CTX_PRIVATE_KEY")
            })?;
    Ok(Some(Arc::new(GrpcPluginCardDelivery {
        ingest_authority: ingest,
        conversation_authority: conversation,
        post_signer: Arc::new(post_signer),
        read_signer: Arc::new(read_signer),
    })))
}

/// Shared state publishing needs the bindings (the database) and the internal event entry
/// (`FLARE_PLUGIN_STATE_EVENT_URL`: Orchestrator `MessageEventService`, `http(s)://` or
/// `discovery://`). Without either, call results' `data.sharedState` is dropped with a debug log
/// and the bridge write answers unavailable.
fn plugin_state_publisher(
    pool: Option<&Arc<PgPool>>,
) -> Result<Option<Arc<crate::application::handler::plugin_state::PluginStatePublisher>>> {
    use crate::application::handler::plugin_state::PluginStatePublisher;
    use crate::infrastructure::adapters::plugin_state_events::GrpcPluginStateEvents;
    use crate::infrastructure::persistence::postgres_plugin_entity_bindings::PostgresPluginEntityBindings;
    let authority = match std::env::var("FLARE_PLUGIN_STATE_EVENT_URL") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => {
            tracing::warn!(
                "FLARE_PLUGIN_STATE_EVENT_URL absent; plugin shared state is not published"
            );
            return Ok(None);
        }
        Err(_) => return Err(FlareError::system("invalid FLARE_PLUGIN_STATE_EVENT_URL")),
    };
    if !(authority.starts_with("discovery://") && authority.len() > "discovery://".len()
        || flare_im_capability_core::plugin::runtime::valid_grpc_authority(&authority))
    {
        return Err(FlareError::system("invalid FLARE_PLUGIN_STATE_EVENT_URL"));
    }
    let Some(pool) = pool else {
        tracing::warn!(
            "no database: plugin entity bindings unavailable; shared state is not published"
        );
        return Ok(None);
    };
    // Orchestrator accepts shared state only under Capability's own key, never from unsigned
    // System metadata.
    let Some(signer) = capability_signer()? else {
        tracing::warn!(
            "FLARE_CAPABILITY_CTX_PRIVATE_KEY absent; plugin shared state is not published"
        );
        return Ok(None);
    };
    Ok(Some(Arc::new(PluginStatePublisher::new(
        Arc::new(PostgresPluginEntityBindings { pool: pool.clone() }),
        Arc::new(GrpcPluginStateEvents {
            authority,
            signer: Arc::new(signer),
        }),
    ))))
}

/// 载入库里的登记（失败则启动失败：表不在说明迁移没跑），再起后台任务按周期重读、按分钟清理。
pub(super) async fn load_and_maintain(
    routes: &Arc<PluginRouteBook>,
    runtime: &CapabilityRuntimeConfig,
) -> Result<()> {
    if !routes.is_persistent() {
        return Ok(());
    }
    let loaded = routes.reload().await.map_err(|error| {
        FlareError::system(format!(
            "cannot load plugin registrations from capability_plugin_endpoints (migration 0011 applied?): {error}"
        ))
    })?;
    tracing::info!(
        loaded,
        refresh_secs = runtime.plugin_route_refresh_interval.as_secs(),
        ttl_secs = runtime.plugin_route_ttl.as_secs(),
        "plugin registrations loaded from capability_plugin_endpoints"
    );
    let routes = Arc::clone(routes);
    let refresh_interval = runtime.plugin_route_refresh_interval;
    let ttl = runtime.plugin_route_ttl;
    tokio::spawn(async move {
        let mut refresh = tokio::time::interval(refresh_interval);
        let mut cleanup = tokio::time::interval(CLEANUP_INTERVAL);
        refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        cleanup.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first tick of each fires at once; the startup load just ran.
        refresh.tick().await;
        cleanup.tick().await;
        loop {
            tokio::select! {
                _ = refresh.tick() => {
                    if let Err(error) = routes.reload().await {
                        tracing::warn!(%error, "plugin registration refresh failed; keeping the last view");
                    }
                }
                _ = cleanup.tick() => match routes.cleanup_stale(ttl).await {
                    Ok(0) => {}
                    Ok(removed) => tracing::info!(removed, "removed plugin registrations not healthy within the TTL"),
                    Err(error) => tracing::warn!(%error, "plugin registration cleanup failed"),
                },
            }
        }
    });
    Ok(())
}
