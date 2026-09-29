//! 应用启动：gRPC PushService 监听

use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use flare_server_core::{SignedContextLayer, SignedContextVerifier};
use tracing::info;

use crate::service::wire::{self, ApplicationContext};
use flare_im_contracts::service_names::PUSH_PROXY;

/// 应用启动器
pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    /// 运行应用的主入口点
    pub async fn run() -> Result<()> {
        let verifier = push_proxy_identity_verifier()?;
        let app_config = flare_im_service_kit::load_app_config_from_env();
        let service_config = app_config.push_proxy_service();
        let runtime_plan = flare_im_service_kit::build_service_runtime_plan(
            app_config,
            &service_config.runtime,
            PUSH_PROXY,
            "PUSH_PROXY",
            50091,
        )?;
        info!(address = %runtime_plan.address, "Push Proxy listen address");

        let context = wire::initialize(app_config).await?;
        Self::run_with_context(context, runtime_plan, verifier).await
    }

    async fn run_with_context(
        context: ApplicationContext,
        runtime_plan: flare_im_service_kit::ImServiceRuntimePlan,
        verifier: SignedContextVerifier,
    ) -> Result<()> {
        use flare_grpc_proto::push::push_service_server::PushServiceServer;
        use tonic::transport::Server;

        let address = runtime_plan.address;
        let service_name = runtime_plan.service_name.clone();
        let grpc_metrics =
            flare_im_service_kit::metrics::GrpcMetricsLayer::new(service_name.clone());
        let handler = context.handler.clone();
        let address_clone = address;

        let runtime = runtime_plan.service_runtime().add_spawn_with_shutdown(
            "push-proxy-grpc",
            move |shutdown_rx| async move {
                let svc = SignedContextLayer::new(verifier).layer(PushServiceServer::new(handler));

                Server::builder()
                    .layer(grpc_metrics)
                    .add_service(svc)
                    .serve_with_shutdown(address_clone, async {
                        let _ = shutdown_rx.await;
                    })
                    .await
                    .map_err(|e| format!("gRPC server error: {}", e).into())
            },
        );

        runtime
            .run_with_registration(|addr| {
                Box::pin(async move {
                    flare_im_service_kit::discovery::register_runtime_service_only(
                        &service_name,
                        addr,
                        None,
                    )
                    .await
                })
            })
            .await
            .map_err(flare_server_core::error::FlareError::from)
    }
}

fn push_proxy_identity_verifier() -> Result<SignedContextVerifier> {
    let key = std::env::var("FLARE_INTERNAL_CTX_PUBLIC_KEY").ok();
    verifier_from_public_key(key.as_deref())
}

fn verifier_from_public_key(key: Option<&str>) -> Result<SignedContextVerifier> {
    let key = key.ok_or_else(|| {
        ErrorBuilder::new(
            ErrorCode::ServiceUnavailable,
            "FLARE_INTERNAL_CTX_PUBLIC_KEY is required for Push Proxy",
        )
        .build_error()
    })?;
    SignedContextVerifier::from_public_key(key).map_err(|_| {
        ErrorBuilder::new(
            ErrorCode::ServiceUnavailable,
            "FLARE_INTERNAL_CTX_PUBLIC_KEY is invalid for Push Proxy",
        )
        .build_error()
    })
}

#[cfg(test)]
mod identity_startup_tests {
    use super::*;

    #[test]
    fn missing_or_invalid_public_key_prevents_push_proxy_start() {
        assert!(verifier_from_public_key(None).is_err());
        assert!(verifier_from_public_key(Some("")).is_err());
        assert!(verifier_from_public_key(Some("not a public key")).is_err());
    }
}
