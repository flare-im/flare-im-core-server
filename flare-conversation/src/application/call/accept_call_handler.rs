//! 接听命令处理（骨架）。

use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::domain::call::{CallSession, CallSessionRepository};

#[derive(Debug, Clone)]
pub struct AcceptCallCommand {
    pub session_id: Uuid,
    pub user_id: String,
}

#[async_trait]
pub trait AcceptCallHandlerPort: Send + Sync {
    async fn handle(&self, cmd: AcceptCallCommand) -> anyhow::Result<CallSession>;
}

pub struct AcceptCallHandler {
    repo: Arc<dyn CallSessionRepository>,
}

impl AcceptCallHandler {
    pub fn new(repo: Arc<dyn CallSessionRepository>) -> Self {
        Self { repo }
    }
}

#[async_trait]
impl AcceptCallHandlerPort for AcceptCallHandler {
    async fn handle(&self, cmd: AcceptCallCommand) -> anyhow::Result<CallSession> {
        let mut s = self
            .repo
            .find_by_id(&cmd.session_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("call session not found"))?;
        let _ev = s.accept(cmd.user_id)?;
        self.repo.save(&s).await?;
        Ok(s)
    }
}
