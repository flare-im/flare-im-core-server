//! 发起通话命令处理（骨架）：持久化聚合 + 发布领域事件；与 orchestrator enrich 顺序后续衔接。

use std::sync::Arc;

use async_trait::async_trait;

use crate::domain::call::{CallSession, CallSessionRepository};

/// CQRS Command：发起通话（对应信令 invite 前的会话侧准备，可与 `EVENT_CALL_SIGNAL` 对齐）。
#[derive(Debug, Clone)]
pub struct StartCallCommand {
    pub tenant_id: String,
    pub conversation_id: String,
}

#[async_trait]
pub trait StartCallHandlerPort: Send + Sync {
    async fn handle(&self, cmd: StartCallCommand) -> anyhow::Result<CallSession>;
}

pub struct StartCallHandler {
    repo: Arc<dyn CallSessionRepository>,
}

impl StartCallHandler {
    pub fn new(repo: Arc<dyn CallSessionRepository>) -> Self {
        Self { repo }
    }
}

#[async_trait]
impl StartCallHandlerPort for StartCallHandler {
    async fn handle(&self, cmd: StartCallCommand) -> anyhow::Result<CallSession> {
        let (session, _ev) = CallSession::start(cmd.conversation_id, cmd.tenant_id);
        self.repo.save(&session).await?;
        Ok(session)
    }
}
