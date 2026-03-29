//! 领域事件（Domain Events）
//!
//! 用于在聚合根状态发生变化时，向外部模块发布事件，实现解耦与最终一致性。

use chrono::{DateTime, Utc};

pub trait DomainEvent: Send + Sync {
    fn event_type(&self) -> &'static str;
    fn occurred_at(&self) -> DateTime<Utc>;
}

pub mod connection_created_event;
pub mod connection_kicked_event;
pub mod priority_changed_event;
pub mod quality_changed_event;

pub use connection_created_event::ConnectionCreatedEvent;
pub use connection_kicked_event::ConnectionKickedEvent;
pub use priority_changed_event::PriorityChangedEvent;
pub use quality_changed_event::QualityChangedEvent;
