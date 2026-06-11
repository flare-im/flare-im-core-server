use flare_server_core::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    flare_api_gateway::ApplicationBootstrap::run().await
}
