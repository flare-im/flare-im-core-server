//! 通话会话领域事件（追加型，供投影与 outbox；不直接调 SFU）。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// 通话侧领域事件（CQRS：由聚合根产生，基础设施落 JetStream/DB）。
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
