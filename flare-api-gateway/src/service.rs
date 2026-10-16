use axum::extract::DefaultBodyLimit;
use flare_core_runtime::ServiceRuntime;
use flare_im_service_kit::{
    clients::GrpcClients,
    gateway::{GatewayEnvScope, GatewaySettings, require_secure_token_secret},
    gateway_auth::TenantRuntimeGuard,
    service_names::API_GATEWAY,
    tenant_runtime::{TenantAccessPolicy, TenantRuntimeCache},
};
use flare_server_core::error::{AnyhowContext, Result};
use flare_server_core::{
    TokenService,
    auth::{
        AuthProviderMode, RedisTokenStore, SignedContextSigner, TokenIssuer,
        build_http_hook_token_validator, build_token_issuer, build_token_validator,
    },
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;
use tracing::{info, warn};

use crate::interface::http::MessageContextSigner;
use crate::interface::http::{PushTokenClient, push_proxy_url_from_env, with_request_rate_limit};
use crate::interface::http::{create_public_router, public_cors_layer};

pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    pub async fn run() -> Result<()> {
        let app_config = flare_im_service_kit::load_config(Some("config"));
        let _tracing =
            flare_im_service_kit::tracing::init_tracing_from_config(Some(app_config.logging()))?;

        let settings = GatewaySettings::from_env_for(GatewayEnvScope::Api)?;
        let platform =
            crate::interface::http::platform::PlatformConfig::from_env().map_err(|_| {
                flare_server_core::error::FlareError::system("invalid Platform configuration")
            })?;
        ensure_dev_issue_allowed(
            std::env::var("FLARE_ENV").ok().as_deref(),
            settings.auth.dev_issue,
        )?;
        let gateway_config = app_config.api_gateway_service();
        let address: SocketAddr = flare_im_service_kit::ServiceHelper::parse_server_addr(
            app_config,
            &gateway_config.runtime,
            API_GATEWAY,
        )
        .context("invalid api-gateway listen address (check config/services/api-gateway.toml)")?;
        info!(address = %address, "HTTP listen address resolved");

        info!("Connecting to gRPC services...");
        info!(
            media_service_url = %settings.grpc.media_service_url,
            message_ingest_service_url = %settings.grpc.message_ingest_service_url,
            message_orchestrator_service_url = %settings.grpc.message_orchestrator_service_url,
            conversation_service_url = %settings.grpc.conversation_service_url,
            storage_reader_service_url = %settings.grpc.storage_reader_service_url,
            "Using downstream grpc endpoints"
        );
        // 每次下游调用（含排队等客户端锁）都按 GRPC_REQUEST_TIMEOUT_SECS 限时，超时回可重试的
        // 503/504；以前只有设备推送令牌接口限时，其余下游卡住时请求一直挂着。
        let request_timeout = Duration::from_secs(settings.grpc.request_timeout_secs);
        let clients = Arc::new(
            GrpcClients::new(Arc::new(app_config.clone()), &settings.grpc)
                .await?
                .with_request_timeout(request_timeout),
        );
        info!(
            request_timeout_secs = settings.grpc.request_timeout_secs,
            "gRPC clients initialized"
        );

        // 设备推送令牌接口直连 Push Proxy 的静态地址（惰性连接，不阻塞启动）。
        let push_proxy_url = push_proxy_url_from_env();
        let push_tokens = PushTokenClient::connect_lazy(&push_proxy_url, request_timeout)
            .map_err(flare_server_core::error::FlareError::system)?;
        info!(push_proxy_url = %push_proxy_url, "device push token endpoint uses static Push Proxy address");

        // Hook-owned identity must not require or construct an unused local HMAC authority.
        let token_service = if settings.auth.mode == AuthProviderMode::CoreJwt {
            Some({
                let base = TokenService::new(
                    require_secure_token_secret(
                        "FLARE_API_GATEWAY_TOKEN_SECRET",
                        gateway_config.token_secret.as_deref(),
                        "services.api_gateway.token_secret",
                    )?,
                    gateway_config
                        .token_issuer
                        .clone()
                        .unwrap_or_else(|| "flare-im-core".to_string()),
                    gateway_config.token_ttl_seconds.unwrap_or(3600),
                );
                // 刷新令牌有效期（长效，支撑 7x24 免重登）。缺省 30 天。
                let refresh_ttl = gateway_config
                    .refresh_token_ttl_seconds
                    .unwrap_or(flare_server_core::auth::DEFAULT_REFRESH_TTL_SECS);
                let mut base = base.with_refresh_ttl(refresh_ttl);
                // 接上 token 撤销/轮换存储：刷新令牌轮换后旧的立即作废、校验查撤销位。
                // 约定用 `[redis.token_store]`（可经 services.api_gateway.token_store 改名）；
                // 没有该 profile 则退化为无状态（撤销为空操作，与历史行为一致）。
                let store_profile = gateway_config
                    .token_store
                    .as_deref()
                    .unwrap_or("token_store");
                match app_config.redis_profile(store_profile) {
                    Some(redis) => {
                        let namespace = redis
                            .namespace
                            .clone()
                            .unwrap_or_else(|| "flare".to_string());
                        match RedisTokenStore::with_namespace(&redis.url, namespace) {
                            Ok(store) => {
                                base = base.with_store(Arc::new(store));
                                info!(
                                    profile = store_profile,
                                    "token store attached: refresh-token rotation-revoke enabled"
                                );
                            }
                            Err(err) => {
                                warn!(%err, profile = store_profile, "failed to build token store; token revoke/rotation disabled (stateless)");
                            }
                        }
                    }
                    None => {
                        info!(
                            profile = store_profile,
                            "no token_store redis profile; token revoke/rotation disabled (stateless)"
                        );
                    }
                }
                Arc::new(base)
            })
        } else {
            None
        };
        let auth_validator = match token_service.as_ref() {
            Some(service) => build_token_validator(
                &settings.auth,
                service.clone(),
                &gateway_config.trusted_token_issuers,
            ),
            None => build_http_hook_token_validator(&settings.auth),
        }
        .context("failed to initialize gateway auth validator")?;
        info!(auth_mode = ?settings.auth.mode, "Gateway auth validator initialized");
        let tenant_policy = std::env::var("FLARE_API_GATEWAY_TENANT_POLICY")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| gateway_config.tenant_policy.clone())
            .map(|value| {
                TenantAccessPolicy::parse(&value).ok_or_else(|| {
                    flare_server_core::error::FlareError::system(
                        "FLARE_API_GATEWAY_TENANT_POLICY must be lenient or strict",
                    )
                })
            })
            .transpose()?
            .unwrap_or_default();
        let tenant_url = std::env::var("FLARE_API_GATEWAY_TENANT_RUNTIME_POSTGRES_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                gateway_config
                    .postgres
                    .as_deref()
                    .and_then(|name| app_config.postgres_profile(name))
                    .map(|profile| profile.url.clone())
            });
        let tenant_runtime = match tenant_url.as_deref() {
            Some(url) => TenantRuntimeCache::connect(url, 2)
                .await
                .map_err(flare_server_core::error::FlareError::system)?,
            None if tenant_policy == TenantAccessPolicy::Strict => {
                return Err(flare_server_core::error::FlareError::system(
                    "strict API tenant policy requires a tenant runtime projection database",
                ));
            }
            None => TenantRuntimeCache::disabled(),
        };
        info!(
            policy = tenant_policy.as_str(),
            enabled = tenant_runtime.is_enabled(),
            "API tenant admission initialized"
        );
        let tenant_guard = TenantRuntimeGuard::new(tenant_runtime, tenant_policy);
        let token_issuer = crate::interface::http::auth_handler::TokenIssuerHandle(
            match token_service.as_ref() {
                Some(service) => build_token_issuer(&settings.auth, service.clone()),
                None if settings
                    .auth
                    .hook_issue_url
                    .as_deref()
                    .is_some_and(|url| !url.trim().is_empty()) =>
                {
                    flare_server_core::auth::issuer::HttpHookTokenIssuer::new(&settings.auth)
                        .map(|issuer| Some(Arc::new(issuer) as Arc<dyn TokenIssuer>))
                }
                None => Ok(None),
            }
            .context("failed to initialize gateway token issuer")?,
        );
        if settings.auth.dev_issue {
            tracing::warn!(
                "AUTH_DEV_ISSUE is enabled: POST /api/v1/auth/tokens issues a token for ANY userId \
                 without credentials. Local/dev only — anyone reaching this gateway can impersonate any user."
            );
        }
        info!(
            app_credentials = settings.auth.app_credentials.len(),
            dev_issue = settings.auth.dev_issue,
            issuer_ready = token_issuer.0.is_some(),
            "Gateway token issuer initialized"
        );

        let message_signer = match std::env::var("FLARE_INTERNAL_CTX_PRIVATE_KEY") {
            Ok(material) => Some(SignedContextSigner::from_private_key(&material).map_err(
                |e| {
                    flare_server_core::error::FlareError::system(format!(
                        "invalid internal context private key: {e}"
                    ))
                },
            )?),
            Err(_) => {
                warn!("FLARE_INTERNAL_CTX_PRIVATE_KEY missing; HTTP message send fails closed");
                None
            }
        };

        let platform_router = match platform {
            Some(config) => Some(
                config
                    .router(
                        tenant_url.as_deref().ok_or_else(|| {
                            flare_server_core::error::FlareError::system(
                                "Platform requires a local primary database",
                            )
                        })?,
                        &settings.grpc.conversation_service_url,
                        &settings.grpc.message_ingest_service_url,
                    )
                    .await
                    .map_err(|_| {
                        flare_server_core::error::FlareError::system(
                            "Platform initialization failed",
                        )
                    })?,
            ),
            None => None,
        };

        // 用量预聚合（`api.calls`）：记录器以 Extension 交给鉴权中间件，flush 循环挂进运行时。
        let usage = flare_im_usage::UsageReporter::from_env("flare-api-gateway").await?;

        // 限流在指标层之内：被拒的请求也计入 HTTP 指标。
        let app = with_request_rate_limit(create_public_router(clients), &settings.rate_limit)
            .layer(flare_im_service_kit::metrics::HttpMetricsLayer::new(
                API_GATEWAY,
            ))
            // FLARE_API_GATEWAY_SERVER_MAX_BODY_SIZE，缺省 16 MiB（与以前写死的值相同）。
            .layer(DefaultBodyLimit::max(settings.server.max_body_size))
            .layer(axum::Extension(settings.clone()))
            .layer(axum::Extension(tenant_guard))
            .layer(axum::Extension(auth_validator))
            .layer(axum::Extension(token_issuer))
            .layer(axum::Extension(token_service))
            .layer(axum::Extension(MessageContextSigner(message_signer)))
            .layer(axum::Extension(push_tokens))
            .layer(axum::Extension(usage.recorder()))
            .layer(
                ServiceBuilder::new()
                    .layer(TraceLayer::new_for_http())
                    .layer(public_cors_layer()),
            );

        // Merge only after all client layers: Platform never accepts client JWT/tenant policy.
        let app = match platform_router {
            Some(platform) => app.merge(platform),
            None => app,
        };

        let runtime = flare_im_service_kit::health::attach_runtime_health_checks(
            ServiceRuntime::new(API_GATEWAY)
                .with_address(address)
                .with_health_failure_action(
                    flare_core_runtime::HealthFailureAction::GracefulShutdown,
                )
                .add_spawn_with_shutdown("gateway-http", move |shutdown_rx| async move {
                    let listener = TcpListener::bind(address).await?;
                    info!(address = %address, "✅ Gateway HTTP service is listening");

                    axum::serve(
                        listener,
                        app.into_make_service_with_connect_info::<SocketAddr>(),
                    )
                    .with_graceful_shutdown(async {
                        let _ = shutdown_rx.await;
                    })
                    .await
                    .map_err(|e| e.into())
                })
                .add_spawn_with_shutdown("api-gateway-usage", move |shutdown_rx| {
                    usage.run(shutdown_rx)
                }),
            API_GATEWAY,
        );
        let metrics = flare_im_service_kit::metrics::MetricsEndpointConfig::from_section(
            API_GATEWAY,
            gateway_config.runtime.metrics.as_ref(),
        )?;
        let runtime =
            flare_im_service_kit::metrics::attach_metrics_endpoint(runtime, API_GATEWAY, metrics);

        Ok(runtime
            .run_with_registration(|addr| {
                Box::pin(async move {
                    flare_im_service_kit::discovery::register_runtime_service_only(
                        API_GATEWAY,
                        addr,
                        None,
                    )
                    .await
                })
            })
            .await?)
    }
}

/// 生产环境拒绝联调签发：开着它，任何能访问网关的人都可以不带凭据、按任意 userId 和
/// tenantId 拿到令牌。只打一条告警不够，必须在监听之前拒绝启动。
fn ensure_dev_issue_allowed(environment: Option<&str>, dev_issue: bool) -> Result<()> {
    let production =
        environment.is_some_and(|value| value.trim().eq_ignore_ascii_case("production"));
    if dev_issue && production {
        return Err(flare_server_core::error::FlareError::system(
            "FLARE_ENV=production forbids FLARE_API_GATEWAY_AUTH_DEV_ISSUE=true: \
             dev issuance mints tokens for any userId without app credentials",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::ensure_dev_issue_allowed;

    #[test]
    fn production_refuses_dev_issue() {
        for environment in ["production", "Production", " PRODUCTION "] {
            let err = ensure_dev_issue_allowed(Some(environment), true)
                .expect_err("production must not start with dev issuance");
            assert!(
                err.to_string().contains("FLARE_API_GATEWAY_AUTH_DEV_ISSUE"),
                "{err}"
            );
        }
    }

    #[test]
    fn dev_issue_stays_available_outside_production() {
        for environment in [None, Some(""), Some("development"), Some("staging")] {
            assert!(
                ensure_dev_issue_allowed(environment, true).is_ok(),
                "{environment:?}"
            );
        }
    }

    #[test]
    fn production_without_dev_issue_starts() {
        assert!(ensure_dev_issue_allowed(Some("production"), false).is_ok());
    }
}
