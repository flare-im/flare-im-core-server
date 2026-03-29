//! 连接处理器模块（入口层）
//!
//! **职责**：解析 PayloadCommand → 按类型组 Command 委托 application CommandHandler → 用 protocol 拼装 Frame。
//!
//! ## 四条上行线（DDD+CQRS）
//!
//! | 类型    | 入口方法         | Command                | CommandHandler             |
//! |---------|------------------|------------------------|----------------------------|
//! | MESSAGE | handle_message   | SendMessageCommand     | SendMessageCommandHandler  |
//! | EVENT   | handle_event     | SendEventCommand       | SendEventCommandHandler    |
//! | ACK     | handle_ack       | ReportAckCommand       | ReportAckCommandHandler    |
//! | DATA    | handle_data      | SendDataCommand        | SendDataCommandHandler     |
//!
//! 连接生命周期（on_connect / on_disconnect）在 connection 中实现，委托 application ConnectionHandler；协议编解码在 protocol。

mod connection;
mod message;

pub use connection::LongConnectionHandler;
