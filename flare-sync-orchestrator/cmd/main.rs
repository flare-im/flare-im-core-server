use flare_sync_orchestrator::service::ApplicationBootstrap;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    ApplicationBootstrap::run().await
}
