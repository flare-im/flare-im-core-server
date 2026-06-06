//! 数据发送领域服务
//!
//! DATA 通道载荷为 [`flare_proto::common::DataPacket`]（`common/data.proto`）：`SYNC_REQUEST` / `SYNC_RESPONSE` / `USER_CUSTOM`。

use std::sync::Arc;

use flare_core::common::ErrorCode;
use flare_core::common::error::{FlareError, Result};
use flare_im_core::Ctx;
use flare_proto::common::data_packet::Payload as DataPayload;
use flare_proto::common::sync::Payload as SyncPayload;
use flare_proto::common::{CustomData, DataPacket};
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
        match cmd.packet.payload.as_ref() {
            Some(DataPayload::SyncRequest(sync)) => {
                let sync = sync.clone();
                tracing::trace!(
                    connection_id = %cmd.connection_id,
                    sync_payload = sync_payload_name(sync.payload.as_ref()),
                    "DATA SYNC_REQUEST → forward"
                );
                let sync_res = self
                    .sync_service
                    .execute(tx, cmd.connection_id.as_str(), sync)
                    .await?;
                let out = DataPacket {
                    payload: Some(DataPayload::SyncResponse(sync_res)),
                };
                Ok(Some(out.encode_to_vec()))
            }
            Some(DataPayload::UserCustom(data)) => self.forward_user_custom(tx, data).await,
            Some(DataPayload::SyncResponse(_)) => Err(FlareError::localized(
                ErrorCode::MessageFormatError,
                "uplink DataPacket must not use sync_response",
            )),
            Some(DataPayload::Capability(_)) => Ok(None),
            Some(DataPayload::RealtimeControl(_)) => Ok(None),
            None => Err(FlareError::localized(
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
            attributes: Default::default(),
        });
        let reply = DataPacket {
            payload: Some(DataPayload::UserCustom(custom)),
        };
        Ok(Some(reply.encode_to_vec()))
    }
}

fn sync_payload_name(payload: Option<&SyncPayload>) -> &'static str {
    match payload {
        Some(SyncPayload::SingleConversation(_)) => "single_conversation",
        Some(SyncPayload::MultiConversation(_)) => "multi_conversation",
        Some(SyncPayload::ConversationsIncremental(_)) => "conversations_incremental",
        Some(SyncPayload::ConversationsAll(_)) => "conversations_all",
        Some(SyncPayload::ConversationDetail(_)) => "conversation_detail",
        Some(SyncPayload::QueryEvents(_)) => "query_events",
        Some(SyncPayload::GetSyncCursor(_)) => "get_sync_cursor",
        Some(SyncPayload::UpdateSyncCursor(_)) => "update_sync_cursor",
        Some(SyncPayload::EventStreamAck(_)) => "event_stream_ack",
        Some(SyncPayload::SyncSnapshot(_)) => "sync_snapshot",
        Some(SyncPayload::ConversationMaxSeq(_)) => "conversation_max_seq",
        Some(SyncPayload::Conversations(_)) => "conversations",
        Some(SyncPayload::ConversationParticipants(_)) => "conversation_participants",
        None => "none",
    }
}
