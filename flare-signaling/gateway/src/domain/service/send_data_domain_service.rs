//! 数据发送领域服务
//!
//! DATA 通道载荷为 [`flare_proto::common::DataPacket`]（`common/data.proto`）：`SYNC_REQUEST` / `SYNC_RESPONSE` / `USER_CUSTOM`。

use std::collections::HashMap;
use std::sync::Arc;

use flare_core::common::error::{FlareError, Result};
use flare_core::common::ErrorCode;
use flare_im_core::Ctx;
use flare_proto::common::data_packet::Payload as DataPayload;
use flare_proto::common::{CustomData, DataKind, DataPacket};
use prost::Message;

use crate::application::commands::SendDataCommand;
use crate::domain::ports::IDataCommandPort;
use crate::domain::service::SyncService;

pub struct SendDataDomainService {
    data_port: Arc<dyn IDataCommandPort>,
    sync_service: Arc<SyncService>,
}

impl SendDataDomainService {
    pub fn new(data_port: Arc<dyn IDataCommandPort>, sync_service: Arc<SyncService>) -> Self {
        Self {
            data_port,
            sync_service,
        }
    }

    pub async fn execute(&self, tx: &Ctx, cmd: &SendDataCommand) -> Result<Option<Vec<u8>>> {
        match (cmd.packet.kind, cmd.packet.payload.as_ref()) {
            (k, Some(DataPayload::SyncRequest(sync))) => {
                if k != DataKind::SyncRequest as i32 {
                    return Err(FlareError::localized(
                        ErrorCode::MessageFormatError,
                        "DataPacket.kind must be DATA_KIND_SYNC_REQUEST when payload is sync_request",
                    ));
                }
                let sync = sync.clone();
                tracing::debug!(
                    connection_id = %cmd.connection_id,
                    sync_kind = sync.kind,
                    "DATA SYNC_REQUEST → forward"
                );
                let sync_res = self
                    .sync_service
                    .execute(tx, cmd.connection_id.as_str(), sync)
                    .await?;
                let out = DataPacket {
                    kind: DataKind::SyncResponse as i32,
                    payload: Some(DataPayload::SyncResponse(sync_res)),
                };
                Ok(Some(out.encode_to_vec()))
            }
            (k, Some(DataPayload::UserCustom(data))) => {
                if k != DataKind::UserCustom as i32 {
                    return Err(FlareError::localized(
                        ErrorCode::MessageFormatError,
                        "DataPacket.kind must be DATA_KIND_USER_CUSTOM when payload is user_custom",
                    ));
                }
                self.forward_user_custom(tx, data).await
            }
            (_, Some(DataPayload::SyncResponse(_))) => Err(FlareError::localized(
                ErrorCode::MessageFormatError,
                "uplink DataPacket must not use sync_response",
            )),
            (_, None) => Err(FlareError::localized(
                ErrorCode::MessageFormatError,
                "DataPacket.payload is required",
            )),
        }
    }

    async fn forward_user_custom(&self, tx: &Ctx, data: &CustomData) -> Result<Option<Vec<u8>>> {
        let opt = self
            .data_port
            .send_data(tx, data.clone())
            .await
            .map_err(|e| FlareError::system(format!("send_data failed: {e}")))?;
        let Some(raw) = opt else {
            return Ok(None);
        };
        let custom = CustomData::decode(raw.as_slice()).unwrap_or_else(|_| CustomData {
            r#type: "binary".to_string(),
            payload: raw,
            metadata: HashMap::new(),
        });
        let reply = DataPacket {
            kind: DataKind::UserCustom as i32,
            payload: Some(DataPayload::UserCustom(custom)),
        };
        Ok(Some(reply.encode_to_vec()))
    }
}
