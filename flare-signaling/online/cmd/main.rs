use flare_im_core::error::Result;
use flare_im_core::tracing::init_tracing_from_config;

#[tokio::main]
async fn main() -> Result<()> {
    // 从配置初始化日志系统（默认 debug 级别）
    init_tracing_from_config(None);

    // 创建应用并启动服务器
    flare_signaling_online::ApplicationBootstrap::run().await
}
