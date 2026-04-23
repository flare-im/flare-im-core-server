//! 通话会话仓储端口（领域层，由 infrastructure 实现 PostgreSQL / 内存等）。

use async_trait::async_trait;

use super::call_session::CallSession;

#[async_trait]
pub trait CallSessionRepository: Send + Sync {
    async fn save(&self, session: &CallSession) -> anyhow::Result<()>;

    async fn find_by_id(&self, id: &uuid::Uuid) -> anyhow::Result<Option<CallSession>>;

    async fn find_by_room_id(&self, sfu_room_id: &str) -> anyhow::Result<Option<CallSession>>;
}
