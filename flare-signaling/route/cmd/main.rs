use flare_im_core::tracing::init_tracing_from_config;
use flare_server_core::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // 从配置初始化日志系统（默认 debug 级别）
    init_tracing_from_config(None);

    // 创建应用并启动服务器
    flare_signaling_route::ApplicationBootstrap::run().await
}
