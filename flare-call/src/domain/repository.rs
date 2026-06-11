//! Call session repository port.

use async_trait::async_trait;
use uuid::Uuid;

use super::call_session::CallSession;

#[async_trait]
pub trait CallSessionRepository: Send + Sync {
    async fn save(&self, session: &CallSession) -> flare_server_core::error::Result<()>;

    async fn find_by_id(&self, id: &Uuid) -> flare_server_core::error::Result<Option<CallSession>>;

    async fn find_by_room_id(
        &self,
        sfu_room_id: &str,
    ) -> flare_server_core::error::Result<Option<CallSession>>;
}
