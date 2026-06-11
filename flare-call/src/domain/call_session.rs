//! Call session aggregate and business FSM.
//!
//! This state machine tracks IM-level call lifecycle only. WebRTC transport
//! state and SFU/media control stay in capability/plugin implementations.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::event::CallSessionEvent;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallSessionState {
    Initiating,
    Ringing,
    Active,
    Ended,
    Failed,
}

#[derive(Debug, Clone)]
pub struct CallSession {
    pub id: Uuid,
    pub tenant_id: String,
    pub conversation_id: String,
    pub call_id: Option<String>,
    pub sfu_room_id: Option<String>,
    pub capability_instance_id: Option<String>,
    pub state: CallSessionState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl CallSession {
    pub fn start(conversation_id: String, tenant_id: String) -> (Self, CallSessionEvent) {
        let id = Uuid::new_v4();
        let now = Utc::now();
        let session = Self {
            id,
            tenant_id: tenant_id.clone(),
            conversation_id: conversation_id.clone(),
            call_id: None,
            sfu_room_id: None,
            capability_instance_id: None,
            state: CallSessionState::Initiating,
            created_at: now,
            updated_at: now,
        };
        let event = CallSessionEvent::Started {
            id,
            conversation_id,
            tenant_id,
            at: now,
        };
        (session, event)
    }

    pub fn bind_room(
        &mut self,
        sfu_room_id: String,
        capability_instance_id: String,
    ) -> CallSessionEvent {
        self.sfu_room_id = Some(sfu_room_id.clone());
        self.capability_instance_id = Some(capability_instance_id.clone());
        self.updated_at = Utc::now();
        CallSessionEvent::RoomBound {
            id: self.id,
            sfu_room_id,
            capability_instance_id,
            at: self.updated_at,
        }
    }

    pub fn accept(
        &mut self,
        user_id: String,
    ) -> flare_server_core::error::Result<CallSessionEvent> {
        self.state = CallSessionState::Active;
        self.updated_at = Utc::now();
        Ok(CallSessionEvent::Accepted {
            id: self.id,
            user_id,
            at: self.updated_at,
        })
    }

    pub fn reject(
        &mut self,
        user_id: String,
        reason: Option<String>,
    ) -> flare_server_core::error::Result<CallSessionEvent> {
        self.state = CallSessionState::Ended;
        self.updated_at = Utc::now();
        Ok(CallSessionEvent::Rejected {
            id: self.id,
            user_id,
            reason,
            at: self.updated_at,
        })
    }

    pub fn cancel(
        &mut self,
        by_user_id: String,
    ) -> flare_server_core::error::Result<CallSessionEvent> {
        self.state = CallSessionState::Ended;
        self.updated_at = Utc::now();
        Ok(CallSessionEvent::Cancelled {
            id: self.id,
            by_user_id,
            at: self.updated_at,
        })
    }

    pub fn hangup(
        &mut self,
        by_user_id: String,
    ) -> flare_server_core::error::Result<CallSessionEvent> {
        self.state = CallSessionState::Ended;
        self.updated_at = Utc::now();
        Ok(CallSessionEvent::Hangup {
            id: self.id,
            by_user_id,
            at: self.updated_at,
        })
    }

    pub fn end(&mut self) -> CallSessionEvent {
        self.state = CallSessionState::Ended;
        self.updated_at = Utc::now();
        CallSessionEvent::Ended {
            id: self.id,
            at: self.updated_at,
        }
    }

    pub fn fail(&mut self, reason: String) -> CallSessionEvent {
        self.state = CallSessionState::Failed;
        self.updated_at = Utc::now();
        CallSessionEvent::Failed {
            id: self.id,
            reason,
            at: self.updated_at,
        }
    }
}
