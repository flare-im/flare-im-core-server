//! 应用层（CQRS）：查询编排初始化/离线/增量同步；命令更新持久化游标。
//!
//! - **统一入口**：`SyncOrchestrationHandler::execute_sync`（`flare.common.v1.Sync` / `SyncRes`）

mod commands;
pub mod error;
pub mod handlers;
pub mod ports;
pub mod queries;

pub use error::{discovery_unavailable, flare_from_tonic_status, require_nonempty_conversation_id};
