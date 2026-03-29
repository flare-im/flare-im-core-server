use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::DomainEvent;
use crate::domain::value_object::{ConnectionId, DeviceId, DevicePriority, TokenVersion, UserId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionCreatedEvent {
    pub conversation_id: ConnectionId,
    pub user_id: UserId,
    pub device_id: DeviceId,
    pub device_priority: DevicePriority,
    pub token_version: TokenVersion,
    pub occurred_at: DateTime<Utc>,
}

impl DomainEvent for ConnectionCreatedEvent {
    fn event_type(&self) -> &'static str {
        "ConnectionCreated"
    }
    fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
    }
}
