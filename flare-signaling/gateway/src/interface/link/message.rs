//! 消息/ACK/事件/DATA 处理：实现 ServerEventHandler，按 PayloadCommand 类型解码 flare-proto 并委托应用层。

use std::collections::HashMap;

use async_trait::async_trait;
use flare_core::common::error::{FlareError as CoreFlareError, Result as CoreResult};
use flare_core::common::protocol::{Frame, NotificationCommand, PayloadCommand};
use flare_core::server::events::handler::ServerEventHandler;
use tracing::{debug, error, instrument};

use crate::application::EventUplinkOutcome;
use crate::application::SendAckCommand;
use crate::application::commands::{SendDataCommand, SendEventCommand, SendMessageCommand};

use super::connection::LongConnectionHandler;
use crate::utils::{
    build_data_error_frame, build_data_frame_with_payload, build_event_ack_operation_frame,
    build_message_ack_frame, decode_ack_payload, decode_data_packet, decode_event_payload,
    decode_message_payload,
};

#[async_trait]
impl ServerEventHandler for LongConnectionHandler {
    /// MESSAGE：payload 为 Message，发送结果回 SendAck
    #[instrument(skip(self), fields(connection_id, message_id = %command.message_id))]
    async fn handle_message(
        &self,
        command: &PayloadCommand,
        connection_id: &str,
    ) -> CoreResult<Option<Frame>> {
        self.handle_message_send(command, connection_id).await
    }

    /// EVENT：payload 为 Event，回 EventAck
    #[instrument(skip(self), fields(connection_id, message_id = %command.message_id))]
    async fn handle_event(
        &self,
        command: &PayloadCommand,
        connection_id: &str,
    ) -> CoreResult<Option<Frame>> {
        self.handle_event_send(command, connection_id).await
    }

    /// ACK：payload 为 Ack（PushAck/ConversationAck/AckBatch），上报并更新游标
    #[instrument(skip(self), fields(connection_id, message_id = %command.message_id))]
    async fn handle_ack(
        &self,
        command: &PayloadCommand,
        connection_id: &str,
    ) -> CoreResult<Option<Frame>> {
        self.handle_ack_send(command, connection_id).await
    }

    /// DATA：payload 为 `DataPacket`（`sync_request` / `user_custom`）；响应同为 `DataPacket`（`sync_response` / `user_custom`）。
    #[instrument(skip(self), fields(connection_id, message_id = %command.message_id))]
    async fn handle_data(
        &self,
        command: &PayloadCommand,
        connection_id: &str,
    ) -> CoreResult<Option<Frame>> {
        self.handle_data_send(command, connection_id).await
    }
    /// 通知咱暂时不处理
    async fn handle_notification_command(
        &self,
        _command: &NotificationCommand,
        _connection_id: &str,
    ) -> CoreResult<Option<Frame>> {
        Ok(None)
    }

    async fn on_disconnect(&self, connection_id: &str, reason: Option<&str>) -> CoreResult<()> {
        debug!(connection_id = %connection_id, reason = ?reason, "Connection disconnected");
        self.disconnect_connection(connection_id).await;
        Ok(())
    }

    async fn on_error(&self, connection_id: &str, error: &str) -> CoreResult<()> {
        error!(connection_id = %connection_id, error = %error, "Connection error");
        self.disconnect_connection(connection_id).await;
        Ok(())
    }

    async fn handle_ping(&self, _frame: &Frame, connection_id: &str) -> CoreResult<Option<Frame>> {
        self.spawn_refresh_session(connection_id);
        Ok(None)
    }

    async fn handle_pong(&self, _frame: &Frame, connection_id: &str) -> CoreResult<Option<Frame>> {
        self.spawn_refresh_session(connection_id);
        Ok(None)
    }

    async fn handle_custom_command(
        &self,
        _command: &flare_core::common::protocol::CustomCommand,
        _connection_id: &str,
    ) -> CoreResult<Option<Frame>> {
        Ok(None)
    }

    /// 连接建立
    async fn on_connect(&self, connection_id: &str) -> CoreResult<()> {
        self.on_connect(connection_id).await
    }

    async fn handle_system_event(
        &self,
        _frame: &Frame,
        _connection_id: &str,
    ) -> CoreResult<Option<Frame>> {
        Ok(None)
    }
}

impl LongConnectionHandler {
    /// 从 metadata 或 Message payload 解析 conversation_id
    fn conversation_id_from_metadata_or_message(
        &self,
        metadata: &HashMap<String, Vec<u8>>,
        payload: &[u8],
    ) -> Option<String> {
        if let Some(v) = metadata.get("conversation_id")
            && let Ok(s) = String::from_utf8(v.clone())
            && !s.is_empty()
        {
            return Some(s);
        }
        decode_message_payload(payload)
            .ok()
            .filter(|m| !m.conversation_id.is_empty())
            .map(|m| m.conversation_id)
    }

    /// 消息发送处理
    async fn handle_message_send(
        &self,
        command: &PayloadCommand,
        connection_id: &str,
    ) -> CoreResult<Option<Frame>> {
        self.spawn_refresh_session(connection_id);
        let conversation_id =
            self.conversation_id_from_metadata_or_message(&command.metadata, &command.payload);
        let message = match decode_message_payload(&command.payload) {
            Ok(m) => m,
            Err(e) => {
                let frame = build_data_error_frame(command.message_id.clone(), "decode_error", &e);
                return Ok(Some(frame));
            }
        };
        let client_msg_id = message.client_msg_id.clone();
        let send_command = SendMessageCommand::new(connection_id.to_string(), message, command.seq);
        let result = self
            .send_handler
            .handle_send_message(&send_command)
            .await
            .map_err(|e| {
                (
                    flare_proto::common::ErrorCode::Internal as i32,
                    e.to_string(),
                )
            });
        build_message_ack_frame(
            &command.message_id,
            &client_msg_id,
            conversation_id.as_deref(),
            result,
        )
            .map_err(|e| {
                error!(
                    connection_id = %connection_id,
                    message_id = %command.message_id,
                    error = %e,
                    "build_message_ack_frame failed"
                );
                e
            })
            .map(Some)
    }

    /// 事件发送处理：解码 Event → SendEventCommand → SendHandler → **必须**返回下行 Frame。
    ///
    /// 若返回 `None`，flare-core 会发送通用自动 ACK，破坏 `EventAck` 协议。
    async fn handle_event_send(
        &self,
        command: &PayloadCommand,
        connection_id: &str,
    ) -> CoreResult<Option<Frame>> {
        self.spawn_refresh_session(connection_id);
        let event = decode_event_payload(&command.payload)
            .map_err(|e| CoreFlareError::system(format!("Decode event failed: {}", e)))?;
        let send_command = SendEventCommand::new(connection_id.to_string(), event, command.seq);
        let outcome = self
            .send_handler
            .handle_send_event(&send_command)
            .await
            .map_err(|e| CoreFlareError::system(format!("Send event failed: {}", e)))?;
        match outcome {
            EventUplinkOutcome::Operation { event_id } => {
                build_event_ack_operation_frame(&command.message_id, &event_id).map(Some)
            }
        }
    }

    /// 数据发送处理：解码 `DataPacket` → SendDataCommand → SendHandler；下行仍为 DATA（`PayloadCommand.type=DATA`）。
    async fn handle_data_send(
        &self,
        command: &PayloadCommand,
        connection_id: &str,
    ) -> CoreResult<Option<Frame>> {
        self.spawn_refresh_session(connection_id);
        let packet = match decode_data_packet(&command.payload) {
            Ok(p) => p,
            Err(e) => {
                let frame = build_data_error_frame(command.message_id.clone(), "decode_error", &e);
                return Ok(Some(frame));
            }
        };
        let send_command = SendDataCommand::new(connection_id.to_string(), packet, command.seq);
        match self.send_handler.handle_send_data(&send_command).await {
            Ok(Some(payload)) => Ok(Some(build_data_frame_with_payload(
                command.message_id.clone(),
                payload,
            ))),
            Ok(None) => Ok(None),
            Err(e) => Ok(Some(build_data_error_frame(
                command.message_id.clone(),
                "send_data_error",
                &e.to_string(),
            ))),
        }
    }

    /// ACK 发送处理：解码 [`flare_proto::common::Ack`] → 按 payload 分发上报（Push / Conversation / Batch）。
    async fn handle_ack_send(
        &self,
        command: &PayloadCommand,
        connection_id: &str,
    ) -> CoreResult<Option<Frame>> {
        self.spawn_refresh_session(connection_id);
        let ack = decode_ack_payload(&command.payload)
            .map_err(|e| CoreFlareError::system(format!("decode Ack: {}", e)))?;
        let send_command = SendAckCommand::new(
            connection_id.to_string(),
            ack,
            Some(command.message_id.to_string()),
        );
        self.send_handler
            .handle_send_ack(&send_command)
            .await
            .map_err(|e| CoreFlareError::system(format!("Send ack failed: {}", e)))?;
        Ok(None)
    }
}
