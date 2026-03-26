//! # Gateway 处理器模块（CQRS 编排）
//!
//! - **上行四条线**：SendMessageCommandHandler / SendEventCommandHandler / ReportAckCommandHandler / SendDataCommandHandler（均 execute(cmd)）
//! - **推送**：PushMessageCommandHandler / BatchPushMessageCommandHandler
//! - **连接/游标**：ConnectionHandler、CursorUpdater（trait）
//! - **门面**：MessageHandler（委托四条上行 CommandHandler）
//! - **推送**：PushMessageCommandHandler / BatchPushMessageCommandHandler（上层 gRPC 直接使用）

pub mod connection_handler;
mod auth_handler;
mod send_handler;
mod push_handler;
mod connection_query_handler;

pub use send_handler::SendHandler;
pub use push_handler::PushHandler;
pub use auth_handler::AuthHandler;

pub use connection_handler::ConnectionHandler;
pub use connection_query_handler::ConnectionQueryHandler;