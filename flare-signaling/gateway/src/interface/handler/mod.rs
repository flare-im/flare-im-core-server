//! 连接处理器模块
//!
//! 处理客户端长连接的消息接收和推送
//!
//! ## 架构设计
//!
//! 本模块实现了 Flare 模式所需的两层接口：
//!
//! 1. **ServerEventHandler**（核心接口，必需）
//!    - 由 `DefaultServerMessageObserver` 自动路由调用
//!    - 处理消息命令（Message）、通知命令（Notification）、自定义命令（Custom Command）和系统事件
//!    - 处理连接生命周期事件（on_connect、on_disconnect、on_error）
//!    - 利用 `flare-core` 的超时保护、连接管理等高级功能
//!
//! 2. **MessageListener**（必需接口）
//!    - `FlareServerBuilder` 要求必须实现
//!    - 主要用于处理自定义命令（Custom Command）的 fallback
//!
//! ## 消息处理流程
//!
//! ### 系统命令（由 DefaultServerMessageObserver 自动处理）
//! ```
//! PING → 自动回复 PONG + 更新连接活跃时间 → ServerEventHandler.handle_ping（刷新会话心跳）
//! PONG → 更新连接活跃时间 → ServerEventHandler.handle_pong（刷新会话心跳）
//! CONNECT → ServerCore 处理协商 → ServerEventHandler.on_connect（连接建立后处理）
//! ```
//!
//! ### 消息命令（由 DefaultServerMessageObserver 自动路由）
//! ```
//! 客户端消息
//!   ↓
//! DefaultServerMessageObserver（flare-core）
//!   ↓ (自动路由，带超时保护)
//! ServerEventHandler.handle_message_command_by_type
//!   ↓
//! 应用层处理器（MessageHandler）
//! ```
//!
//! ### 自定义命令流程
//! ```
//! 客户端自定义命令
//!   ↓
//! DefaultServerMessageObserver（flare-core）
//!   ↓ (自动路由)
//! ServerEventHandler.handle_custom_command
//!   ↓ (如果返回 None，fallback 到 MessageListener.on_message)
//! handle_frame_impl（处理 Custom Command）
//! ```
//!
//! ### 连接事件（由 DefaultServerMessageObserver 自动处理）
//! ```
//! 连接断开 → ServerEventHandler.on_disconnect（自动调用）
//! 连接错误 → ServerEventHandler.on_error（自动调用）
//! ```

mod connection;
mod lifecycle;
mod message_handler;
mod push;

pub use connection::LongConnectionHandler;
