use flare_capability::composition::ApplicationBootstrap;
use flare_server_core::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    ApplicationBootstrap::run_from_env().await
}
