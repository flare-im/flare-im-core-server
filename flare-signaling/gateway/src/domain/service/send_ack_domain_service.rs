//! 客户端 ACK 领域服务：上行 `Ack` 仅允许 `push` / `conversation` / `read` / `batch`（见 `common/ack.proto`）。
//! `send` / `event` 为下行回执语义，不得经本路径上行。

use std::sync::Arc;

use flare_core::common::error::{FlareError, Result};
use flare_im_contracts::Ctx;
use flare_proto::common::ack::Payload as AckPayload;
use tracing::instrument;

use crate::application::commands::SendAckCommand;
use crate::domain::ports::IAckReportPort;

pub struct SendAckDomainService {
    ack_port: Arc<dyn IAckReportPort>,
}

impl SendAckDomainService {
    pub fn new(ack_port: Arc<dyn IAckReportPort>) -> Self {
        Self { ack_port }
    }

    #[instrument(skip(self, tx, cmd), fields(connection_id = %cmd.connection_id))]
    pub async fn execute(&self, tx: &Ctx, cmd: &SendAckCommand) -> Result<()> {
        match cmd.ack.payload.as_ref() {
            Some(payload) => self.execute_with_payload(tx, cmd, payload).await,
            None => Ok(()),
        }
    }

    #[instrument(skip(self, tx, cmd), fields(connection_id = %cmd.connection_id))]
    pub async fn execute_with_payload(
        &self,
        tx: &Ctx,
        cmd: &SendAckCommand,
        payload: &AckPayload,
    ) -> Result<()> {
        match payload {
            AckPayload::Push(push_ack) => {
                self.ack_port
                    .report_push_ack(tx, push_ack.clone())
                    .await
                    .map_err(|e| FlareError::system(format!("report push ack failed: {e}")))?;
            }
            AckPayload::Conversation(conv_ack) => {
                self.ack_port
                    .report_conversation_ack(tx, conv_ack.clone())
                    .await
                    .map_err(|e| {
                        FlareError::system(format!("report conversation ack failed: {e}"))
                    })?;
            }
            AckPayload::Read(read_ack) => {
                self.ack_port
                    .report_read_ack(tx, read_ack.clone())
                    .await
                    .map_err(|e| FlareError::system(format!("report read ack failed: {e}")))?;
            }
            AckPayload::Batch(batch) => {
                self.ack_port
                    .report_ack_batch(tx, batch.clone())
                    .await
                    .map_err(|e| FlareError::system(format!("report ack batch failed: {e}")))?;
            }
            AckPayload::Send(_) => {
                return Err(FlareError::system(
                    "uplink Ack.payload.send is invalid: SendAck is for downlink send receipt only",
                ));
            }
            AckPayload::Event(_) => {
                return Err(FlareError::system(
                    "uplink Ack.payload.event is invalid: use EVENT channel for events",
                ));
            }
        }
        Ok(())
    }
}
