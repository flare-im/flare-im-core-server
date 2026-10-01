//! 应用启动器 - 负责依赖注入和服务启动

use flare_server_core::SignedContextVerifier;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use tracing::info;

use flare_im_contracts::service_names::MEDIA;

use super::wire;
use crate::domain::service::{MediaService, UPLOAD_SESSION_RECLAIM_INTERVAL, UploadReclaimReport};

/// 应用启动器
pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    /// 运行应用的主入口点
    pub async fn run() -> Result<()> {
        let verifier = media_identity_verifier()?;
        let app_config = flare_im_service_kit::load_app_config_from_env();
        let service_config = app_config.media_service();
        let runtime_plan = flare_im_service_kit::build_service_runtime_plan(
            app_config,
            &service_config.runtime,
            MEDIA,
            "MEDIA",
            60081,
        )?;
        info!(address = %runtime_plan.address, "Server address parsed successfully");

        // 使用 Wire 风格的依赖注入构建应用上下文
        let context = wire::initialize(app_config).await?;

        info!("ApplicationBootstrap created successfully");

        // 运行服务
        Self::run_with_context(context, runtime_plan, verifier).await
    }

    /// 运行服务（带应用上下文）
    async fn run_with_context(
        context: wire::ApplicationContext,
        runtime_plan: flare_im_service_kit::ImServiceRuntimePlan,
        verifier: SignedContextVerifier,
    ) -> Result<()> {
        use tonic::transport::Server;

        let address = runtime_plan.address;
        let service_name = runtime_plan.service_name.clone();
        let grpc_metrics =
            flare_im_service_kit::metrics::GrpcMetricsLayer::new(service_name.clone());
        let handler = context.handler.clone();

        info!(
            address = %address,
            port = %address.port(),
            "Starting Media gRPC service..."
        );

        let address_clone = address;
        let runtime = runtime_plan.service_runtime().add_spawn_with_shutdown(
            "media-grpc",
            move |shutdown_rx| async move {
                // 只接受 Cell 签名的内部上下文；未签名的网内调用方无法冒用任意租户。
                let media_service =
                    crate::interface::grpc::authenticated_media_service(handler, verifier);

                Server::builder()
                    .layer(grpc_metrics)
                    .add_service(media_service)
                    .serve_with_shutdown(address_clone, async {
                        let _ = shutdown_rx.await;
                    })
                    .await
                    .map_err(|e| format!("gRPC server error: {}", e).into())
            },
        );

        // 过期上传会话回收：释放存储侧资源与本机分块目录。多副本各跑一份，靠会话存储的
        // 领取租约分工。
        let media_service = context.media_service.clone();
        let runtime = runtime.add_spawn_with_shutdown(
            "media-upload-session-reaper",
            move |mut shutdown_rx| async move {
                let mut interval = tokio::time::interval(UPLOAD_SESSION_RECLAIM_INTERVAL);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        _ = &mut shutdown_rx => break,
                        _ = interval.tick() => reclaim_round(&media_service).await,
                    }
                }
                Ok(())
            },
        );

        // 运行服务（带服务注册）
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

/// 回收一轮过期上传会话；失败只记日志，下一轮重试。
async fn reclaim_round(media_service: &MediaService) {
    match media_service
        .reclaim_expired_upload_sessions(chrono::Utc::now())
        .await
    {
        Ok(report) if report == UploadReclaimReport::default() => {}
        Ok(report) => info!(
            reclaimed = report.reclaimed,
            failed = report.failed,
            stale_chunk_dirs = report.stale_chunk_dirs,
            "expired upload sessions reclaimed"
        ),
        Err(error) => tracing::warn!(error = %error, "upload session reclaim round failed"),
    }
}

/// Media 缺少 Cell 公钥时拒绝启动，而不是退回无认证的 gRPC 入口。
fn media_identity_verifier() -> Result<SignedContextVerifier> {
    let key = std::env::var("FLARE_INTERNAL_CTX_PUBLIC_KEY").ok();
    verifier_from_public_key(key.as_deref())
}

fn verifier_from_public_key(key: Option<&str>) -> Result<SignedContextVerifier> {
    let key = key.filter(|key| !key.trim().is_empty()).ok_or_else(|| {
        ErrorBuilder::new(
            ErrorCode::ServiceUnavailable,
            "FLARE_INTERNAL_CTX_PUBLIC_KEY is required for Media",
        )
        .build_error()
    })?;
    SignedContextVerifier::from_public_key(key).map_err(|_| {
        ErrorBuilder::new(
            ErrorCode::ServiceUnavailable,
            "FLARE_INTERNAL_CTX_PUBLIC_KEY is invalid for Media",
        )
        .build_error()
    })
}

#[cfg(test)]
mod identity_startup_tests {
    use super::*;

    #[test]
    fn missing_or_invalid_public_key_prevents_media_start() {
        assert!(verifier_from_public_key(None).is_err());
        assert!(verifier_from_public_key(Some("")).is_err());
        assert!(verifier_from_public_key(Some("not a public key")).is_err());
    }
}
