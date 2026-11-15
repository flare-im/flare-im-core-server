//! CQRS Handler（编排层）

pub mod binding_revocation_dispatcher;
pub mod command_handler;
pub mod gateway_sweeper;
pub mod handoff_dispatcher;
pub mod presence_watcher_handler;
pub mod query_handler;
pub mod user_handler;

pub use binding_revocation_dispatcher::{
    RevocationDispatchConfig, RevocationDispatcher, RevocationQueue, RevocationStats,
    revocation_channel,
};
pub use command_handler::OnlineCommandHandler;
pub use gateway_sweeper::{GatewaySweeper, GatewaySweeperConfig, SweepReport};
pub use handoff_dispatcher::{
    HandoffDispatchConfig, HandoffDispatcher, HandoffQueue, HandoffStats, handoff_channel,
};
pub use presence_watcher_handler::OnlinePresenceWatcherHandler;
pub use query_handler::OnlineQueryHandler;
pub use user_handler::OnlineUserHandler;
