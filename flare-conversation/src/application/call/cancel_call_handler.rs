//! 取消通话命令处理（骨架）。

use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::domain::call::{CallSession, CallSessionRepository};

#[derive(Debug, Clone)]
pub struct CancelCallCommand {
    pub session_id: Uuid,
    pub by_user_id: String,
}

#[async_trait]
pub trait CancelCallHandlerPort: Send + Sync {
    async fn handle(&self, cmd: CancelCallCommand)
    -> flare_server_core::error::Result<CallSession>;
}

pub struct CancelCallHandler {
    repo: Arc<dyn CallSessionRepository>,
}

impl CancelCallHandler {
    pub fn new(repo: Arc<dyn CallSessionRepository>) -> Self {
        Self { repo }
    }
}

#[async_trait]
impl CancelCallHandlerPort for CancelCallHandler {
    async fn handle(
        &self,
        cmd: CancelCallCommand,
    ) -> flare_server_core::error::Result<CallSession> {
        let mut s = self
            .repo
            .find_by_id(&cmd.session_id)
            .await?
            .ok_or_else(|| {
                flare_server_core::error::FlareError::system("call session not found".to_string())
            })?;
        let _ev = s.cancel(cmd.by_user_id)?;
        self.repo.save(&s).await?;
        Ok(s)
    }
}
