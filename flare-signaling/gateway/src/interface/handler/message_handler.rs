
//! 消息处理模块
//!
//! 实现 Flare 模式的 ServerEventHandler trait，处理客户端消息和连接事件

use async_trait::async_trait;
use flare_core::common::error::{FlareError as CoreFlareError, Result as CoreResult};
use flare_core::common::protocol::{Frame, MessageCommand, NotificationCommand, Reliability, frame_with_message_command};
use flare_core::common::protocol::builder::{FrameBuilder, current_timestamp};
use flare_core::common::protocol::flare::core::commands::command::Type as CommandType;
use flare_core::server::events::handler::ServerEventHandler;
use prost::Message;
use std::collections::HashMap;
use tracing::{debug, error, instrument, warn};

use super::connection::LongConnectionHandler;

/// 实现 ServerEventHandler trait（Flare 模式核心接口）
///
/// Flare 模式会自动路由消息到对应的方法，并自动处理 ACK 响应
#[async_trait]
impl ServerEventHandler for LongConnectionHandler {
    /// 处理 SEND 消息命令
    #[instrument(skip(self), fields(connection_id, message_id = %command.message_id))]
    async fn handle_message(
        &self,
        command: &MessageCommand,
        connection_id: &str,
    ) -> CoreResult<Option<Frame>> {
        let client_message_id = command.message_id.clone();

        // 刷新会话心跳（忽略错误，不影响主流程）
        if let Err(err) = self.refresh_session(connection_id).await {
            warn!(?err, %connection_id, "failed to refresh session heartbeat");
        }

        // 处理消息发送，获取服务端生成的消息ID
        let send_ack = match self.handle_message_send(command, connection_id).await {
            Ok((server_message_id, seq)) => {
                // 构建成功 ACK
                flare_proto::common::SendEnvelopeAck {
                    server_msg_id: server_message_id.to_string(),
                    status: flare_proto::common::AckStatus::Success as i32,
                    seq,
                    error_code: 0,
                    error_message: String::new(),
                }
            }
            Err(e) => {
                // 处理失败，记录错误
                error!(
                    connection_id = %connection_id,
                    message_id = %client_message_id,
                    error = %e,
                    "Failed to handle message send, sending error ACK"
                );
                
                // 构建错误 ACK
                flare_proto::common::SendEnvelopeAck {
                    server_msg_id: client_message_id.clone(), // 使用 client_msg_id 作为 fallback
                    status: flare_proto::common::AckStatus::Failed as i32,
                    seq: 0,
                    error_code: 1, // 通用错误码
                    error_message: e.to_string(),
                }
            }
        };
        
        // 统一构建 ACK Frame（成功和失败共用）
        let mut metadata = std::collections::HashMap::new();
        if let Some(conv_id_bytes) = command.metadata.get("conversation_id") {
            metadata.insert("conversation_id".to_string(), conv_id_bytes.clone());
        }
        
        // 序列化 SendEnvelopeAck 为 payload
        let mut payload = Vec::new();
        send_ack.encode(&mut payload).map_err(|e| {
            CoreFlareError::serialization_error(format!("Failed to encode SendEnvelopeAck: {}", e))
        })?;
        
        // 创建包含 payload 的 ACK 命令
        let ack_cmd = MessageCommand {
            r#type: flare_core::common::protocol::flare::core::commands::message_command::Type::Ack as i32,
            message_id: client_message_id,
            payload, // 包含 SendEnvelopeAck 的 payload
            metadata,
            seq: 0,
        };
        
        let frame = frame_with_message_command(ack_cmd, Reliability::AtLeastOnce);
        Ok(Some(frame))
    }

    /// 处理 ACK 消息命令
    #[instrument(skip(self), fields(connection_id, message_id = %command.message_id))]
    async fn handle_ack(
        &self,
        command: &MessageCommand,
        connection_id: &str,
    ) -> CoreResult<Option<Frame>> {
        self.handle_client_ack(command, connection_id).await?;
        Ok(None)
    }

    /// 处理 DATA 消息命令（Gateway 暂不支持）
    async fn handle_data(
        &self,
        command: &MessageCommand,
        connection_id: &str,
    ) -> CoreResult<Option<Frame>> {
        if let Err(err) = self.refresh_session(connection_id).await {
            warn!(?err, %connection_id, "failed to refresh session heartbeat");
        }

        let frame = self.handle_data_command(command, connection_id).await;
        Ok(Some(frame))
    }

    /// 处理通知命令（Gateway 暂不支持）
    async fn handle_notification_command(
        &self,
        _command: &NotificationCommand,
        _connection_id: &str,
    ) -> CoreResult<Option<Frame>> {
        Ok(None)
    }

    /// 处理连接断开事件
    async fn on_disconnect(&self, connection_id: &str, reason: Option<&str>) -> CoreResult<()> {
        debug!(connection_id = %connection_id, reason = ?reason, "Connection disconnected");
        self.on_disconnect_impl(connection_id).await
    }

    /// 处理连接错误事件
    async fn on_error(&self, connection_id: &str, error: &str) -> CoreResult<()> {
        error!(connection_id = %connection_id, error = %error, "Connection error");
        self.on_disconnect_impl(connection_id).await
    }

    /// 处理 PING 系统命令（框架已自动回复 PONG，这里只处理业务逻辑）
    async fn handle_ping(&self, _frame: &Frame, connection_id: &str) -> CoreResult<Option<Frame>> {
        let _ = self.refresh_session(connection_id).await;
        Ok(None)
    }

    /// 处理 PONG 系统命令（框架已更新连接活跃时间，这里只处理业务逻辑）
    async fn handle_pong(&self, _frame: &Frame, connection_id: &str) -> CoreResult<Option<Frame>> {
        let _ = self.refresh_session(connection_id).await;
        Ok(None)
    }

    /// 处理自定义命令
    async fn handle_custom_command(
        &self,
        _command: &flare_core::common::protocol::CustomCommand,
        _connection_id: &str,
    ) -> CoreResult<Option<Frame>> {
        Ok(None)
    }

    /// 处理连接建立完成事件
    async fn on_connect(&self, connection_id: &str) -> CoreResult<()> {
        self.on_connect_impl(connection_id).await
    }

    /// 处理系统事件（Gateway 暂不支持）
    async fn handle_system_event(
        &self,
        _frame: &Frame,
        _connection_id: &str,
    ) -> CoreResult<Option<Frame>> {
        Ok(None)
    }
}

// ============================================================================
// 消息处理业务逻辑（协议适配层）
// ============================================================================

impl LongConnectionHandler {
    async fn require_connection_user_id(
        &self,
        connection_id: &str,
    ) -> Option<String> {
        if let Some(user_id) = self.user_id_for_connection(connection_id).await {
            if !user_id.trim().is_empty() {
                return Some(user_id);
            }
        }

        if let Some(metadata) = self.get_connection_metadata(connection_id).await {
            if let Some(user_id) =
                crate::infrastructure::connection_context::extract_user_id_from_metadata(&metadata)
            {
                if !user_id.trim().is_empty() {
                    return Some(user_id);
                }
            }
        }

        None
    }

    async fn build_conversation_grpc_request<T>(
        &self,
        connection_id: &str,
        user_id: &str,
        msg: T,
    ) -> tonic::Request<T> {
        let connection_metadata = self.get_connection_metadata(connection_id).await;
        let ctx = crate::infrastructure::connection_context::build_context_from_connection(
            connection_metadata.as_ref(),
            Some(user_id),
            &self.default_tenant_id,
        );

        let mut req = tonic::Request::new(msg);
        flare_server_core::client::metadata_codec::encode_context_to_metadata(req.metadata_mut(), &ctx);
        req
    }

    async fn handle_data_command(
        &self,
        command: &MessageCommand,
        connection_id: &str,
    ) -> Frame {
        use flare_core::common::protocol::flare::core::commands::message_command::Type as MsgType;
        use flare_proto::common::{ClientPacket, ServerPacket, ErrorPacket};

        let message_id = command.message_id.clone();

        let client_packet = match ClientPacket::decode(command.payload.as_slice()) {
            Ok(p) => p,
            Err(e) => {
                let server_packet = ServerPacket {
                    payload: Some(flare_proto::common::server_packet::Payload::Error(ErrorPacket {
                        code: 400,
                        message: format!("decode ClientPacket failed: {}", e),
                        metadata: HashMap::new(),
                    })),
                };
                return Self::build_data_frame(message_id, server_packet);
            }
        };

        let server_packet = match self.handle_client_packet(client_packet, connection_id).await {
            Ok(p) => p,
            Err(e) => {
                let code = e
                    .code()
                    .map(|c| c.as_u32() as i32)
                    .unwrap_or(6000);
                ServerPacket {
                    payload: Some(flare_proto::common::server_packet::Payload::Error(
                        ErrorPacket {
                            code,
                            message: e.to_string(),
                            metadata: HashMap::new(),
                        },
                    )),
                }
            }
        };

        let mut payload = Vec::new();
        if let Err(e) = server_packet.encode(&mut payload) {
            let fallback_packet = ServerPacket {
                payload: Some(flare_proto::common::server_packet::Payload::Error(ErrorPacket {
                    code: 500,
                    message: format!("encode ServerPacket failed: {}", e),
                    metadata: HashMap::new(),
                })),
            };
            return Self::build_data_frame(message_id, fallback_packet);
        }

        let msg_cmd = MessageCommand {
            r#type: MsgType::Data as i32,
            message_id: message_id.clone(),
            payload,
            metadata: HashMap::new(),
            seq: 0,
        };

        FrameBuilder::new()
            .with_command(flare_core::common::protocol::flare::core::commands::Command {
                r#type: Some(CommandType::Message(msg_cmd)),
            })
            .with_message_id(message_id)
            .with_reliability(Reliability::AtLeastOnce)
            .with_timestamp(current_timestamp())
            .build()
    }

    async fn handle_client_packet(
        &self,
        packet: flare_proto::common::ClientPacket,
        connection_id: &str,
    ) -> CoreResult<flare_proto::common::ServerPacket> {
        use flare_proto::common::client_packet::Payload;
        use flare_proto::common::ServerPacket;

        let Some(payload) = packet.payload else {
            return Err(CoreFlareError::system("ClientPacket.payload is None".to_string()));
        };

            match payload {
            Payload::Send(envelope) => {
                let mut messages = envelope.messages;
                if messages.len() != 1 {
                    return Err(CoreFlareError::system(format!(
                        "SendMessageEnvelope.messages must contain exactly 1 message, got {}",
                        messages.len()
                    )));
                }
                let message = messages.remove(0);

                let conversation_id = message.conversation_id.clone();

                let mut buf = Vec::new();
                message.encode(&mut buf).map_err(|e| {
                    CoreFlareError::serialization_error(format!("encode Message: {}", e))
                })?;

                let msg_cmd = MessageCommand {
                    r#type: flare_core::common::protocol::flare::core::commands::message_command::Type::Send as i32,
                    message_id: message.client_msg_id.clone(),
                    payload: buf,
                    metadata: {
                        let mut md = HashMap::new();
                        md.insert("conversation_id".to_string(), conversation_id.as_bytes().to_vec());
                        md
                    },
                    seq: 0,
                };

                let (server_msg_id, seq) = self.handle_message_send(&msg_cmd, connection_id).await?;

                Ok(ServerPacket {
                    payload: Some(flare_proto::common::server_packet::Payload::SendAck(
                        flare_proto::common::SendEnvelopeAck {
                            server_msg_id,
                            status: flare_proto::common::AckStatus::Success as i32,
                            seq,
                            error_code: 0,
                            error_message: String::new(),
                        },
                    )),
                })
            }
            Payload::SyncConversations(req) => {
                let connection_user_id = self
                    .require_connection_user_id(connection_id)
                    .await
                    .ok_or_else(|| CoreFlareError::localized(
                        flare_core::common::error::code::ErrorCode::AuthenticationRequired,
                        "not authenticated",
                    ))?;

                let mut req = req;
                if !req.user_id.trim().is_empty() && req.user_id != connection_user_id {
                    return Err(CoreFlareError::localized(
                        flare_core::common::error::code::ErrorCode::PermissionDenied,
                        "user_id mismatch",
                    ));
                }
                req.user_id = connection_user_id;

                let user_id_for_ctx = req.user_id.clone();
                let mut client = self.ensure_conversation_client().await?;
                let resp = client
                    .sync_conversations(
                        self.build_conversation_grpc_request(connection_id, &user_id_for_ctx, req)
                            .await,
                    )
                    .await
                    .map_err(|e| CoreFlareError::system(e.to_string()))?
                    .into_inner();
                Ok(ServerPacket {
                    payload: Some(flare_proto::common::server_packet::Payload::SyncConversationsResp(resp)),
                })
            }
            Payload::SyncConversationsAll(req) => {
                let connection_user_id = self
                    .require_connection_user_id(connection_id)
                    .await
                    .ok_or_else(|| CoreFlareError::localized(
                        flare_core::common::error::code::ErrorCode::AuthenticationRequired,
                        "not authenticated",
                    ))?;

                let mut req = req;
                if !req.user_id.trim().is_empty() && req.user_id != connection_user_id {
                    return Err(CoreFlareError::localized(
                        flare_core::common::error::code::ErrorCode::PermissionDenied,
                        "user_id mismatch",
                    ));
                }
                req.user_id = connection_user_id;

                let user_id_for_ctx = req.user_id.clone();
                let mut client = self.ensure_conversation_client().await?;
                let resp = client
                    .get_all_conversations(
                        self.build_conversation_grpc_request(connection_id, &user_id_for_ctx, req)
                            .await,
                    )
                    .await
                    .map_err(|e| CoreFlareError::system(e.to_string()))?
                    .into_inner();
                Ok(ServerPacket {
                    payload: Some(flare_proto::common::server_packet::Payload::SyncConversationsAllResp(resp)),
                })
            }
            Payload::SyncMessages(req) => {
                let connection_user_id = self
                    .require_connection_user_id(connection_id)
                    .await
                    .ok_or_else(|| CoreFlareError::localized(
                        flare_core::common::error::code::ErrorCode::AuthenticationRequired,
                        "not authenticated",
                    ))?;

                let mut req = req;
                if !req.user_id.trim().is_empty() && req.user_id != connection_user_id {
                    return Err(CoreFlareError::localized(
                        flare_core::common::error::code::ErrorCode::PermissionDenied,
                        "user_id mismatch",
                    ));
                }
                req.user_id = connection_user_id;

                let mut client = self.ensure_conversation_client().await?;

                let conv_req = flare_proto::conversation::SyncMessagesRequest {
                    user_id: req.user_id,
                    conversation_id: req.conversation_id,
                    since_ts: req.since_ts_ms,
                    cursor: req.cursor,
                    limit: req.limit,
                    include_ack: req.include_ack,
                };

                let user_id_for_ctx = conv_req.user_id.clone();
                let resp = client
                    .sync_messages(
                        self.build_conversation_grpc_request(connection_id, &user_id_for_ctx, conv_req)
                            .await,
                    )
                    .await
                    .map_err(|e| CoreFlareError::system(e.to_string()))?
                    .into_inner();

                let server_cursor_ts = resp.server_cursor_ts;
                let envelope = flare_proto::common::MessageEnvelope {
                    kind: flare_proto::common::EnvelopeKind::KindSync as i32,
                    messages: resp.messages,
                    has_more: !resp.next_cursor.is_empty(),
                    max_seq: if server_cursor_ts > 0 {
                        server_cursor_ts as u64
                    } else {
                        0
                    },
                    next_cursor: resp.next_cursor,
                    window_id: String::new(),
                };

                let mut metadata = HashMap::new();
                if server_cursor_ts > 0 {
                    metadata.insert("server_cursor_ts_ms".to_string(), server_cursor_ts.to_string());
                }

                let common_resp = flare_proto::common::SyncMessagesResponse {
                    envelope: Some(envelope),
                    status: resp.status,
                    metadata,
                };

                Ok(ServerPacket {
                    payload: Some(flare_proto::common::server_packet::Payload::SyncMessagesResp(common_resp)),
                })
            }
            Payload::PushAck(_ack) => {
                Ok(ServerPacket {
                    payload: Some(flare_proto::common::server_packet::Payload::CustomPushData(
                        flare_proto::common::CustomPushData {
                            r#type: "ack".to_string(),
                            payload: Vec::new(),
                            metadata: HashMap::new(),
                        },
                    )),
                })
            }
            Payload::GetConversationDetail(_req) => Err(CoreFlareError::system(
                "GetConversationDetail is not supported by ConversationService".to_string(),
            )),
        }
    }

    fn build_data_frame(message_id: String, packet: flare_proto::common::ServerPacket) -> Frame {
        use flare_core::common::protocol::flare::core::commands::message_command::Type as MsgType;

        let mut payload = Vec::new();
        let _ = packet.encode(&mut payload);

        let msg_cmd = MessageCommand {
            r#type: MsgType::Data as i32,
            message_id: message_id.clone(),
            payload,
            metadata: HashMap::new(),
            seq: 0,
        };

        FrameBuilder::new()
            .with_command(flare_core::common::protocol::flare::core::commands::Command {
                r#type: Some(CommandType::Message(msg_cmd)),
            })
            .with_message_id(message_id)
            .with_reliability(Reliability::AtLeastOnce)
            .with_timestamp(current_timestamp())
            .build()
    }

    /// 处理消息发送（协议适配层）
    ///
    /// 从连接信息获取 user_id，委托给应用层服务处理
    ///
    /// # 返回值
    /// 返回服务端生成的消息 ID（server_id），用于在 ACK 中返回给 SDK
    #[instrument(skip(self), fields(connection_id, message_id = %msg_cmd.message_id))]
    pub(crate) async fn handle_message_send(
        &self,
        msg_cmd: &MessageCommand,
        connection_id: &str,
    ) -> CoreResult<(String,u64)> {
        let user_id = self
            .user_id_for_connection(connection_id)
            .await
            .ok_or_else(|| {
                CoreFlareError::system(format!(
                    "user_id is unknown for connection_id={}",
                    connection_id
                ))
            })?;

        let tenant_id = self.get_tenant_id_for_connection(connection_id).await;

        self.message_handler
            .handle_message_send(connection_id, &user_id, msg_cmd, Some(&tenant_id))
            .await
            .map_err(|e| CoreFlareError::system(format!("Failed to handle message send: {}", e)))
    }

    /// 处理客户端 ACK 消息（协议适配层）
    ///
    /// 处理客户端 ACK，更新会话游标，刷新心跳
    #[instrument(skip(self), fields(connection_id, message_id = %msg_cmd.message_id))]
    pub(crate) async fn handle_client_ack(
        &self,
        msg_cmd: &MessageCommand,
        connection_id: &str,
    ) -> CoreResult<()> {
        let user_id = self
            .user_id_for_connection(connection_id)
            .await
            .unwrap_or_else(|| "unknown".to_string());

        // 委托给应用层服务处理
        self.message_handler
            .handle_client_ack(connection_id, &user_id, msg_cmd)
            .await?;

        // 推送窗口 ACK 更新会话游标（如果提供）
        if let (Some(conversation_id_bytes), Some(ack_seq_bytes)) = (
            msg_cmd.metadata.get("conversation_id"),
            msg_cmd.metadata.get("ack_seq"),
        ) {
            if let (Ok(conversation_id), Some(ack_seq)) = (
                String::from_utf8(conversation_id_bytes.clone()),
                std::str::from_utf8(ack_seq_bytes.as_slice())
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok()),
            ) {
                if let Ok(mut client) = self.ensure_conversation_client().await {
                    let req = flare_proto::conversation::UpdateCursorRequest {
                        user_id: user_id.clone(),
                        conversation_id,
                        message_ts: ack_seq,
                        device_id: String::new(),
                    };
                    let _ = client.update_cursor(tonic::Request::new(req)).await;
                }
            }
        }

        // 刷新会话心跳（忽略错误，不影响主流程）
        let _ = self.refresh_session(connection_id).await;

        Ok(())
    }

    /// 确保 Conversation 服务客户端已初始化
    ///
    /// 用于更新会话游标等操作
    pub(crate) async fn ensure_conversation_client(
        &self,
    ) -> CoreResult<
        flare_proto::conversation::conversation_service_client::ConversationServiceClient<
            tonic::transport::Channel,
        >,
    > {
        use flare_im_core::service_names::{CONVERSATION, get_service_name};
        use tonic::transport::{Channel, Endpoint};
        let mut guard = self.conversation_service_client.lock().await;
        if let Some(client) = guard.as_ref() {
            return Ok(client.clone());
        }
        let mut discover_guard = self.conversation_service_discover.lock().await;
        if discover_guard.is_none() {
            let name = get_service_name(CONVERSATION);
            let discover = flare_im_core::discovery::create_discover(&name)
                .await
                .map_err(|e| CoreFlareError::system(format!("create discover: {}", e)))?;
            if let Some(d) = discover {
                *discover_guard = Some(flare_server_core::discovery::ServiceClient::new(d));
            }
        }
        let channel: Channel = if let Some(service_client) = discover_guard.as_mut() {
            match service_client.get_channel().await {
                Ok(ch) => ch,
                Err(_e) => {
                    let addr = std::env::var("CONVERSATION_GRPC_ADDR")
                        .ok()
                        .unwrap_or_else(|| "127.0.0.1:50090".to_string());
                    let endpoint = Endpoint::from_shared(format!("http://{}", addr))
                        .map_err(|err| CoreFlareError::system(err.to_string()))?;
                    endpoint
                        .connect()
                        .await
                        .map_err(|err| CoreFlareError::system(err.to_string()))?
                }
            }
        } else {
            let addr = std::env::var("CONVERSATION_GRPC_ADDR")
                .ok()
                .unwrap_or_else(|| "127.0.0.1:50090".to_string());
            let endpoint = Endpoint::from_shared(format!("http://{}", addr))
                .map_err(|err| CoreFlareError::system(err.to_string()))?;
            endpoint
                .connect()
                .await
                .map_err(|err| CoreFlareError::system(err.to_string()))?
        };
        let client =
            flare_proto::conversation::conversation_service_client::ConversationServiceClient::new(channel);
        *guard = Some(client.clone());
        Ok(client)
    }
}
