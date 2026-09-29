use flare_im_contracts::service_names::PUSH_SERVER;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use tracing::info;

use crate::service::wire::{self, ApplicationContext};

pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    pub async fn run() -> Result<()> {
        require_verified_event_provenance(
            std::env::var("PUSH_SERVER_EVENT_PROVENANCE_MODE")
                .ok()
                .as_deref(),
        )?;
        let app_config = flare_im_service_kit::load_app_config_from_env();
        let service_config = app_config.push_server_service();
        let runtime = flare_im_service_kit::build_background_service_runtime(
            app_config,
            &service_config.runtime,
            PUSH_SERVER,
        );
        let ctx = wire::initialize(app_config).await?;
        Self::run_with_runtime(ctx, runtime).await
    }

    pub async fn run_with_context(context: ApplicationContext) -> Result<()> {
        require_verified_event_provenance(
            std::env::var("PUSH_SERVER_EVENT_PROVENANCE_MODE")
                .ok()
                .as_deref(),
        )?;
        Self::run_with_runtime(
            context,
            flare_im_service_kit::background_service_runtime(PUSH_SERVER),
        )
        .await
    }

    async fn run_with_runtime(
        context: ApplicationContext,
        mut runtime: flare_core_runtime::ServiceRuntime,
    ) -> Result<()> {
        info!(
            "Starting Push Server (push-request -> push-online/push-offline) via ServiceRuntime..."
        );

        let tasks = match context.config.mq_backend.as_str() {
            "kafka" => flare_server_core::mq::kafka::build_kafka_consumer_tasks(
                context.config.as_ref(),
                context.consumer_config,
                context.dispatcher.clone(),
                "push-server-consumer",
            )
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!(
                    "create push-server kafka consumers: {}",
                    e
                ))
            })?,
            // 接通死信:处理失败/毒消息投到独立死信流 flare.im.dlq.push-server,不再无限重投。
            "nats" => flare_server_core::mq::nats::build_nats_consumer_tasks_with_dlq(
                context.config.as_ref(),
                context.consumer_config,
                context.dispatcher.clone(),
                "push-server-consumer",
                "flare.im.dlq.push-server",
            )
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!(
                    "create push-server nats consumers: {}",
                    e
                ))
            })?,
            other => {
                return Err(flare_server_core::error::FlareError::system(format!(
                    "unsupported mq backend: {other}"
                )));
            }
        };

        for task in tasks {
            runtime = runtime.add_task(Box::new(task));
        }

        runtime
            .run()
            .await
            .map_err(flare_server_core::error::FlareError::from)
    }
}

/// Broker headers and payload tenant fields do not authenticate their producer.
/// Pause before constructing consumers or DLQ publishers so unsigned ingress
/// events cannot be rejected by routing and then irreversibly ACKed.
fn require_verified_event_provenance(mode: Option<&str>) -> Result<()> {
    match mode {
        None | Some("paused") => Err(ErrorBuilder::new(
            ErrorCode::ServiceUnavailable,
            "Push Server is paused: signed broker event provenance is not implemented; broker backlog remains subject to configured retention",
        )
        .build_error()),
        Some("signed-v1") => Err(ErrorBuilder::new(
            ErrorCode::ServiceUnavailable,
            "PUSH_SERVER_EVENT_PROVENANCE_MODE=signed-v1 is reserved until producer signature, broker ACL and consumer verification are implemented",
        )
        .build_error()),
        Some(_) => Err(ErrorBuilder::new(
            ErrorCode::InvalidParameter,
            "PUSH_SERVER_EVENT_PROVENANCE_MODE must be paused; signed-v1 is not available",
        )
        .build_error()),
    }
}

#[cfg(test)]
mod provenance_gate_tests {
    use super::*;

    #[tokio::test]
    async fn bootstrap_rejects_before_initializing_broker_consumers() {
        let err = ApplicationBootstrap::run().await.unwrap_err();
        assert!(matches!(
            err.code(),
            Some(ErrorCode::ServiceUnavailable | ErrorCode::InvalidParameter)
        ));
        assert!(
            err.to_string().contains("Push Server is paused")
                || err
                    .to_string()
                    .contains("PUSH_SERVER_EVENT_PROVENANCE_MODE")
        );
    }

    #[test]
    fn default_and_explicit_pause_fail_before_consumer_start() {
        for mode in [None, Some("paused")] {
            let err = require_verified_event_provenance(mode).unwrap_err();
            assert_eq!(err.code(), Some(ErrorCode::ServiceUnavailable));
            assert!(err.to_string().contains("backlog"));
        }
    }

    #[test]
    fn reserved_mode_and_unknown_values_cannot_bypass_gate() {
        let reserved = require_verified_event_provenance(Some("signed-v1")).unwrap_err();
        assert_eq!(reserved.code(), Some(ErrorCode::ServiceUnavailable));
        for mode in ["", "active", "true", "unsafe", "SIGNED-V1"] {
            let err = require_verified_event_provenance(Some(mode)).unwrap_err();
            assert_eq!(err.code(), Some(ErrorCode::InvalidParameter));
        }
    }
}
