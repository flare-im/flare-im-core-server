//! 事件发送领域服务
//!
//! 负责上行 EVENT 的业务编排（仅业务事件）：
//! - 同步已迁移到 DATA 通道（`DataPacket` + `sync_request`）；
//! - EVENT 仅走领域事件链路（经 Router → Orchestrator，回 `OperationResponse`）。

use std::sync::Arc;

use flare_core::common::error::{FlareError, Result};
use flare_im_core::Ctx;
use tracing::instrument;

use crate::application::commands::SendEventCommand;
use crate::domain::model::EventUplinkOutcome;
use crate::domain::ports::IEventCommandPort;

/// 事件发送领域服务
///
/// 依赖 `ConnectionContextResolver`、`EventCommandPort`（同步控制面已迁至 DATA → [`SyncService`]）。
pub struct SendEventDomainService {
    event_port: Arc<dyn IEventCommandPort>,
}

impl SendEventDomainService {
    pub fn new(event_port: Arc<dyn IEventCommandPort>) -> Self {
        Self { event_port }
    }

    /// 处理上行 Event：按 `EventType` 分发至同步或普通事件链路
    #[instrument(skip(self, tx, cmd), fields(connection_id = %cmd.connection_id, event_type = %cmd.event.r#type))]
    pub async fn execute(&self, tx: &Ctx, cmd: &SendEventCommand) -> Result<EventUplinkOutcome> {
        let operation = self
            .event_port
            .send_event(tx, cmd.event.clone())
            .await
            .map_err(|e| FlareError::system(format!("send event failed: {}", e)))?;

        Ok(EventUplinkOutcome::Operation {
            event_id: cmd.event.event_id.clone(),
            operation,
        })
    }
}
