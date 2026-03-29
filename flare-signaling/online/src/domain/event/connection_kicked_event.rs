use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::DomainEvent;
use crate::domain::value_object::{ConnectionId, DeviceId, UserId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionKickedEvent {
    pub conversation_id: ConnectionId,
    pub user_id: UserId,
    pub device_id: DeviceId,
    pub reason: String,
    pub occurred_at: DateTime<Utc>,
}

impl DomainEvent for ConnectionKickedEvent {
    fn event_type(&self) -> &'static str {
        "ConnectionKicked"
    }
    fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
    }
}
