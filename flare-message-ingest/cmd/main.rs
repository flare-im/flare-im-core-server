use flare_im_core_service_kit::tracing::init_tracing_from_config;
use flare_server_core::error::{FlareError, Result};

/// Worker stack size. An unoptimized build keeps every awaited stage future of the send path on
/// the worker stack: SendSystemMessage needs a little over tokio's default 2 MiB there.
const WORKER_STACK_BYTES: usize = 8 * 1024 * 1024;

fn main() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(WORKER_STACK_BYTES)
        .build()
        .map_err(|e| FlareError::system(format!("message ingest runtime: {e}")))?
        .block_on(async {
            // 从配置初始化日志系统（默认 debug 级别）
            let _tracing = init_tracing_from_config(None)?;

            // 创建应用并启动
            flare_message_ingest::ApplicationBootstrap::run().await?;
            Ok(())
        })
}
