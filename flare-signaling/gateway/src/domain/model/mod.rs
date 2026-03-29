//! 领域模型（实体、值对象、配置）

mod connection;
mod connection_config;
mod connection_info;
mod event_uplink_outcome;
mod message;
mod push_result;
mod quality;

pub use connection::{Connection, ConnectionQuality, ConnectionState, DomainError};
pub use connection_config::ConnectionDomainServiceConfig;
pub use connection_info::ConnectionInfo;
/// 连接上下文（ConnectionInfo 的别名，供 Port/Resolver 使用）
pub use connection_info::ConnectionInfo as ConnectionContext;
pub use event_uplink_outcome::EventUplinkOutcome;
pub use message::MessageWrapper;
pub use push_result::DomainPushResult;
pub use quality::{ConnectionQualityMetrics, QualityLevel};
