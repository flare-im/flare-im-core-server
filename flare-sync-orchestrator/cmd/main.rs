use flare_sync_orchestrator::service::ApplicationBootstrap;

#[tokio::main]
async fn main() -> flare_server_core::error::Result<()> {
    ApplicationBootstrap::run().await
}
