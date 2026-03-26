//! 同步领域服务（DATA `DataPacket` / `sync_request`）
//!
//! 网关 **零解析**：补全 `device_id` 后透传 [`ISyncPort::forward_sync`]；语义校验与 `SyncRes` 组装全部由 sync-orchestrator 负责。

use std::sync::Arc;

use flare_core::common::error::Result;
use flare_im_core::Ctx;
use flare_proto::common::{ErrorCode, RpcStatus, Sync, SyncRes};

use crate::domain::ports::ISyncPort;

pub struct SyncService {
    port: Arc<dyn ISyncPort>,
}

impl SyncService {
    pub fn new(port: Arc<dyn ISyncPort>) -> Self {
        Self { port }
    }

    /// 处理同步：仅连接态补全 → 下游原样返回（含 `RpcStatus` 业务错误）。
    pub async fn execute(
        &self,
        tx: &Ctx,
        _connection_id: &str,
        mut sync: Sync,
    ) -> Result<SyncRes> {
        if sync.device_id.is_empty() {
            sync.device_id = tx
                .device_id()
                .map(str::to_string)
                .unwrap_or_default();
        }

        match self.port.forward_sync(tx, sync).await {
            Ok(res) => Ok(res),
            Err(e) => Ok(sync_transport_err(e)),
        }
    }
}

/// 传输层 / 网关侧失败（非下游 `SyncRes.status`）。
fn sync_transport_err(e: anyhow::Error) -> SyncRes {
    SyncRes {
        status: Some(RpcStatus {
            code: ErrorCode::Internal as i32,
            message: format!("sync forward: {e}"),
            details: Vec::new(),
            context: None,
            localization_key: String::new(),
            localization_params: Default::default(),
        }),
        payload: None,
    }
}
