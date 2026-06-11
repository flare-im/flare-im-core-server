//! Call lifecycle domain events.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CallSessionEvent {
    Started {
        id: Uuid,
        conversation_id: String,
        tenant_id: String,
        at: DateTime<Utc>,
    },
    RoomBound {
        id: Uuid,
        sfu_room_id: String,
        capability_instance_id: String,
        at: DateTime<Utc>,
    },
    Accepted {
        id: Uuid,
        user_id: String,
        at: DateTime<Utc>,
    },
    Rejected {
        id: Uuid,
        user_id: String,
        reason: Option<String>,
        at: DateTime<Utc>,
    },
    Cancelled {
        id: Uuid,
        by_user_id: String,
        at: DateTime<Utc>,
    },
    Hangup {
        id: Uuid,
        by_user_id: String,
        at: DateTime<Utc>,
    },
    Ended {
        id: Uuid,
        at: DateTime<Utc>,
    },
    Failed {
        id: Uuid,
        reason: String,
        at: DateTime<Utc>,
    },
}
