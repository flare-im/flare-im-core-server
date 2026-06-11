use flare_im_contracts::service_names::STORAGE_READER;
use flare_server_core::error::Result;
use tracing::info;

mod wire;

pub use wire::{ApplicationContext, MessageStorageType};

/// 应用启动器
pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    /// 运行应用的主入口点
    pub async fn run() -> Result<()> {
        let app_config = flare_im_service_kit::load_app_config_from_env();
        let service_config = app_config.storage_reader_service();
        let runtime_plan = flare_im_service_kit::build_service_runtime_plan(
            app_config,
            &service_config.runtime,
            STORAGE_READER,
            "STORAGE_READER",
            60083,
        )?;
        info!(address = %runtime_plan.address, "Server address parsed successfully");

        // 使用 Wire 风格的依赖注入构建应用上下文
        let context = self::wire::initialize(app_config).await?;

        info!("ApplicationBootstrap created successfully");

        // 运行服务
        Self::run_with_context(context, runtime_plan).await
    }

    /// 运行服务（带应用上下文）
    async fn run_with_context(
        context: ApplicationContext<MessageStorageType>,
        runtime_plan: flare_im_service_kit::ImServiceRuntimePlan,
    ) -> Result<()> {
        use flare_grpc_proto::storage::storage_reader_service_server::StorageReaderServiceServer;
        use tonic::transport::Server;

        let address = runtime_plan.address;
        let service_name = runtime_plan.service_name.clone();
        let handler = context.handler.clone();

        info!(
            address = %address,
            port = %address.port(),
            "Starting Storage Reader gRPC service..."
        );

        let address_clone = address;
        let runtime = runtime_plan.service_runtime().add_spawn_with_shutdown(
            "storage-reader-grpc",
            move |shutdown_rx| async move {
                // 使用 ContextLayer 包裹 Service
                use flare_server_core::middleware::ContextLayer;

                let storage_reader_service = ContextLayer::new()
                    .allow_missing()
                    .layer(StorageReaderServiceServer::new(handler));

                Server::builder()
                    .add_service(storage_reader_service)
                    .serve_with_shutdown(address_clone, async {
                        let _ = shutdown_rx.await;
                    })
                    .await
                    .map_err(|e| format!("gRPC server error: {}", e).into())
            },
        );

        // 运行服务（带服务注册）
        Ok(runtime
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
            .await?)
    }
}
