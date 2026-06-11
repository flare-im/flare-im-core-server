use flare_im_service_kit::tracing::init_tracing_from_config;
use flare_server_core::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing_from_config(None);
    flare_push_proxy::ApplicationBootstrap::run().await
}
