//! Gateway-side call session repository adapter.
//!
//! This keeps the long-connection runtime wired to `flare-call` without making
//! gateway depend on storage-writer's internal row model. Production deployments
//! can replace it with a PostgreSQL/Redis backed implementation of the same
//! `flare_call::domain::CallSessionRepository` port.

use std::collections::HashMap;

use async_trait::async_trait;
use flare_call::domain::{CallSession, CallSessionRepository};
use flare_server_core::error::Result;
use tokio::sync::RwLock;
use uuid::Uuid;

use super::router::{CallBindingLookup, CapabilityRouteHint};

#[derive(Default)]
pub struct InMemoryCallSessionRepository {
    sessions: RwLock<HashMap<Uuid, CallSession>>,
}

#[async_trait]
impl CallSessionRepository for InMemoryCallSessionRepository {
    async fn save(&self, session: &CallSession) -> Result<()> {
        self.sessions
            .write()
            .await
            .insert(session.id, session.clone());
        Ok(())
    }

    async fn find_by_id(&self, id: &Uuid) -> Result<Option<CallSession>> {
        Ok(self.sessions.read().await.get(id).cloned())
    }

    async fn find_by_room_id(&self, sfu_room_id: &str) -> Result<Option<CallSession>> {
        Ok(self
            .sessions
            .read()
            .await
            .values()
            .find(|session| session.sfu_room_id.as_deref() == Some(sfu_room_id))
            .cloned())
    }
}

#[async_trait]
impl CallBindingLookup for InMemoryCallSessionRepository {
    async fn resolve_by_call_id(
        &self,
        tenant_id: &str,
        call_id: &str,
    ) -> Result<Option<CapabilityRouteHint>> {
        let sessions = self.sessions.read().await;
        Ok(sessions
            .values()
            .find(|session| {
                session.tenant_id == tenant_id && session.call_id.as_deref() == Some(call_id)
            })
            .and_then(route_hint_from_session))
    }

    async fn resolve_by_room_id(
        &self,
        tenant_id: &str,
        sfu_room_id: &str,
    ) -> Result<Option<CapabilityRouteHint>> {
        let sessions = self.sessions.read().await;
        Ok(sessions
            .values()
            .find(|session| {
                session.tenant_id == tenant_id
                    && session.sfu_room_id.as_deref() == Some(sfu_room_id)
            })
            .and_then(route_hint_from_session))
    }
}

fn route_hint_from_session(session: &CallSession) -> Option<CapabilityRouteHint> {
    let capability_instance_id = session.capability_instance_id.clone()?;
    Some(CapabilityRouteHint {
        capability_instance_id,
        sfu_room_id: session.sfu_room_id.clone(),
        call_id: session.call_id.clone(),
    })
}
