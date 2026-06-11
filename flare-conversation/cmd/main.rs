use flare_im_service_kit::tracing::init_tracing_from_config;
use flare_server_core::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // 先加载配置再初始化日志，便于使用 logging.with_ansi、level 等，且默认会抑制第三方库噪音
    let app_config = flare_im_service_kit::load_app_config_from_env();
    init_tracing_from_config(Some(app_config.logging()));

    // 创建应用并启动
    flare_conversation::ApplicationBootstrap::run().await
}
