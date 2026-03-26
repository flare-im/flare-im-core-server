//! 领域模型（实体、值对象、配置）

mod connection_config;
mod connection_info;
mod connection;
mod event_uplink_outcome;
mod message;
mod push_result;
mod quality;

pub use connection_config::ConnectionDomainServiceConfig;
pub use connection_info::ConnectionInfo;
pub use connection::{Connection, ConnectionState, ConnectionQuality, DomainError};
pub use event_uplink_outcome::EventUplinkOutcome;
/// 连接上下文（ConnectionInfo 的别名，供 Port/Resolver 使用）
pub use connection_info::ConnectionInfo as ConnectionContext;
pub use message::MessageWrapper;
pub use push_result::DomainPushResult;
pub use quality::{ConnectionQualityMetrics, QualityLevel};
