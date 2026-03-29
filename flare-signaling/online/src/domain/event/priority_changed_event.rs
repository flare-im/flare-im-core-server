use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::DomainEvent;
use crate::domain::value_object::{ConnectionId, DeviceId, DevicePriority, UserId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityChangedEvent {
    pub conversation_id: ConnectionId,
    pub user_id: UserId,
    pub device_id: DeviceId,
    pub old_priority: DevicePriority,
    pub new_priority: DevicePriority,
    pub occurred_at: DateTime<Utc>,
}

impl DomainEvent for PriorityChangedEvent {
    fn event_type(&self) -> &'static str {
        "PriorityChanged"
    }
    fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
    }
}
