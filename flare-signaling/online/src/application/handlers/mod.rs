//! CQRS Handler（编排层）

pub mod command_handler;
pub mod gateway_sweeper;
pub mod presence_watcher_handler;
pub mod query_handler;
pub mod user_handler;

pub use command_handler::OnlineCommandHandler;
pub use gateway_sweeper::{GatewaySweeper, GatewaySweeperConfig, SweepReport};
pub use presence_watcher_handler::OnlinePresenceWatcherHandler;
pub use query_handler::OnlineQueryHandler;
pub use user_handler::OnlineUserHandler;
