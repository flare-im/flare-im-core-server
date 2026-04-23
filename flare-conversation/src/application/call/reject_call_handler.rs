//! 拒接命令处理（骨架）。

use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::domain::call::{CallSession, CallSessionRepository};

#[derive(Debug, Clone)]
pub struct RejectCallCommand {
    pub session_id: Uuid,
    pub user_id: String,
    pub reason: Option<String>,
}

#[async_trait]
pub trait RejectCallHandlerPort: Send + Sync {
    async fn handle(&self, cmd: RejectCallCommand) -> anyhow::Result<CallSession>;
}

pub struct RejectCallHandler {
    repo: Arc<dyn CallSessionRepository>,
}

impl RejectCallHandler {
    pub fn new(repo: Arc<dyn CallSessionRepository>) -> Self {
        Self { repo }
    }
}

#[async_trait]
impl RejectCallHandlerPort for RejectCallHandler {
    async fn handle(&self, cmd: RejectCallCommand) -> anyhow::Result<CallSession> {
        let mut s = self
            .repo
            .find_by_id(&cmd.session_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("call session not found"))?;
        let _ev = s.reject(cmd.user_id, cmd.reason)?;
        self.repo.save(&s).await?;
        Ok(s)
    }
}
