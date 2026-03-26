use anyhow::Result;
use flare_im_core::tracing::init_tracing_from_config;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing_from_config(None);
    flare_push_proxy::ApplicationBootstrap::run().await
}
