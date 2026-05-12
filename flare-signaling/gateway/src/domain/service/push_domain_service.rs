//! 推送领域服务
//!
//! 包含推送相关的核心业务逻辑，仅依赖 ConnectionQuery（读连接）与 IPushPort（写推送）。

use std::sync::Arc;

use flare_grpc_proto::access_gateway::PushOptions;
use flare_im_core::Ctx;
use flare_proto::common::{Event, EventEnvelope, Message, MessagePush};
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use prost::Message as ProstMessage;
use tracing::{info, instrument};

use crate::domain::model::{ConnectionInfo, DomainPushResult};
use crate::domain::ports::{ConnectionQuery, IPushPort};

/// 推送领域服务
pub struct PushDomainService {
    push_port: Arc<dyn IPushPort>,
    connection_query: Arc<dyn ConnectionQuery>,
}

impl PushDomainService {
    pub fn new(push_port: Arc<dyn IPushPort>, connection_query: Arc<dyn ConnectionQuery>) -> Self {
        Self {
            push_port,
            connection_query,
        }
    }

    /// 检查用户是否在线
    ///
    /// Gateway 直接查询本地连接状态，不维护缓存
    /// 在线状态由 Signaling Online 服务统一管理
    #[instrument(skip(self, tx), fields(user_id = %user_id))]
    pub async fn check_user_online(&self, tx: &Ctx, user_id: &str) -> Result<bool> {
        // 直接查询本地连接状态
        let connections = self
            .connection_query
            .query_user_connections(tx, user_id)
            .await?;

        Ok(!connections.is_empty())
    }

    /// 过滤连接（根据设备ID和平台）
    pub fn filter_connections(
        &self,
        _tx: &Ctx,
        connections: &[ConnectionInfo],
        options: &PushOptions,
    ) -> Vec<ConnectionInfo> {
        connections
            .iter()
            .filter(|conn| {
                if !options.device_ids.is_empty() && !options.device_ids.contains(&conn.device_id) {
                    return false;
                }
                let platform = conn.platform.as_deref().unwrap_or("");
                if !options.platforms.is_empty() && !options.platforms.iter().any(|p| p == platform)
                {
                    return false;
                }
                true
            })
            .cloned()
            .collect()
    }

    /// 推送消息到连接（委托仓储按 connection_id 批量推送）
    #[instrument(skip(self, tx, message_bytes), fields(user_id = %user_id, connection_count = connections.len()))]
    pub async fn push_to_connections(
        &self,
        tx: &Ctx,
        user_id: &str,
        connections: &[ConnectionInfo],
        message_bytes: &[u8],
    ) -> Result<(i32, i32)> {
        let connection_ids: Vec<String> = connections
            .iter()
            .map(|c| c.connection_id.clone())
            .collect();
        let payload_type = flare_core::common::protocol::payload_command::Type::Message as i32;
        self.push_port
            .push_payload_to_connections(tx, &connection_ids, payload_type, message_bytes.to_vec())
            .await
    }

    /// 向**单连接**下行已编码载荷（gRPC/领域编排共用，统一走 [`IPushPort`]）
    #[instrument(skip(self, tx, payload), fields(connection_id = %connection_id, payload_type = %payload_type))]
    pub async fn deliver_payload_to_connection(
        &self,
        tx: &Ctx,
        connection_id: &str,
        payload_type: i32,
        payload: Vec<u8>,
    ) -> Result<()> {
        self.push_port
            .push_payload_to_connection(tx, connection_id, payload_type, payload)
            .await
    }

    /// 按 Payload 类型推送字节到指定连接列表（2=Event 3=Ack 4=Data）
    #[instrument(skip(self, tx, payload_bytes), fields(user_id = %user_id, payload_type = %payload_type))]
    pub async fn push_payload_to_connections(
        &self,
        tx: &Ctx,
        user_id: &str,
        connections: &[ConnectionInfo],
        payload_type: i32,
        payload_bytes: &[u8],
    ) -> Result<(i32, i32)> {
        let _ = user_id;
        let connection_ids: Vec<String> = connections
            .iter()
            .map(|c| c.connection_id.clone())
            .collect();
        self.push_port
            .push_payload_to_connections(tx, &connection_ids, payload_type, payload_bytes.to_vec())
            .await
    }

    /// 获取用户连接并过滤
    #[instrument(skip(self, tx), fields(user_id = %user_id))]
    pub async fn get_filtered_connections(
        &self,
        tx: &Ctx,
        user_id: &str,
        options: &PushOptions,
    ) -> Result<Vec<ConnectionInfo>> {
        let connections = self
            .connection_query
            .query_user_connections(tx, user_id)
            .await?;

        Ok(self.filter_connections(tx, &connections, options))
    }

    /// 将业务消息下行给多个用户：载荷为 `MessagePush` 编码字节（与客户端 `chatroom_client` / SDK 解码一致）。
    ///
    /// 返回 `(user_id, pushed_device_count, failed_count, offline_pending_count)` 按用户一行。
    #[instrument(skip(self, tx, messages), fields(user_count = user_ids.len(), message_count = messages.len()))]
    pub async fn push_message_push_to_users(
        &self,
        tx: &Ctx,
        user_ids: &[String],
        messages: Vec<Message>,
        options: &PushOptions,
    ) -> Result<Vec<(String, i32, i32, i32)>> {
        let push = MessagePush {
            messages,
            notifications: vec![],
        };
        let mut payload = Vec::new();
        push.encode(&mut payload).map_err(|e| {
            ErrorBuilder::new(ErrorCode::InternalError, "encode MessagePush failed")
                .details(e.to_string())
                .build_error()
        })?;

        let mut out = Vec::with_capacity(user_ids.len());
        for user_id in user_ids {
            let connections = self.get_filtered_connections(tx, user_id, options).await?;
            if connections.is_empty() {
                info!(user_id = %user_id, "push_message: user has no matching online connection");
                out.push((user_id.clone(), 0, 0, 1));
                continue;
            }
            let (ok, fail) = self
                .push_to_connections(tx, user_id, &connections, &payload)
                .await?;
            info!(
                user_id = %user_id,
                pushed = ok,
                failed = fail,
                "push_message: MessagePush delivered"
            );
            out.push((user_id.clone(), ok, fail, 0));
        }
        Ok(out)
    }

    /// 将领域事件批下行给多个用户：载荷为 `EventEnvelope` 编码字节（与客户端 SDK `ProtobufCodec::decode_server` 一致，走 `PayloadCommand::Message` 内层解码）。
    ///
    /// 返回 `(user_id, pushed_device_count, failed_count, offline_pending_count)` 与 [`Self::push_message_push_to_users`] 对齐。
    #[instrument(skip(self, tx, events), fields(user_count = user_ids.len(), event_count = events.len()))]
    pub async fn push_event_envelope_to_users(
        &self,
        tx: &Ctx,
        user_ids: &[String],
        events: Vec<Event>,
        options: &PushOptions,
    ) -> Result<Vec<(String, i32, i32, i32)>> {
        let max_seq = events.iter().map(|e| e.seq).max().unwrap_or(0);
        let envelope = EventEnvelope {
            events,
            max_seq,
            has_more: false,
            next_cursor: String::new(),
            window_id: String::new(),
        };
        let mut payload = Vec::new();
        envelope.encode(&mut payload).map_err(|e| {
            ErrorBuilder::new(ErrorCode::InternalError, "encode EventEnvelope failed")
                .details(e.to_string())
                .build_error()
        })?;

        let mut out = Vec::with_capacity(user_ids.len());
        for user_id in user_ids {
            let connections = self.get_filtered_connections(tx, user_id, options).await?;
            if connections.is_empty() {
                info!(user_id = %user_id, "push_event: user has no matching online connection");
                out.push((user_id.clone(), 0, 0, 1));
                continue;
            }
            let (ok, fail) = self
                .push_to_connections(tx, user_id, &connections, &payload)
                .await?;
            info!(
                user_id = %user_id,
                pushed = ok,
                failed = fail,
                "push_event: EventEnvelope delivered"
            );
            out.push((user_id.clone(), ok, fail, 0));
        }
        Ok(out)
    }

    /// 推送 ACK 字节给用户（payload 为 common::Ack encode_to_vec）
    #[instrument(skip(self, tx, ack_payload), fields(user_id = %user_id))]
    pub async fn push_ack_to_user(
        &self,
        tx: &Ctx,
        user_id: &str,
        ack_payload: Vec<u8>,
    ) -> Result<()> {
        let payload_type = flare_core::common::protocol::payload_command::Type::Ack as i32;
        self.push_port
            .push_payload_to_user(tx, user_id, payload_type, ack_payload)
            .await
    }

    /// 构建推送结果
    pub fn build_push_result(
        _tx: &Ctx,
        user_id: String,
        success_count: i32,
        failure_count: i32,
    ) -> DomainPushResult {
        DomainPushResult {
            user_id,
            success_count,
            failure_count,
            error_message: if failure_count > 0 {
                format!("Failed to push to {} connections", failure_count)
            } else {
                String::new()
            },
        }
    }

    /// 查询用户连接列表（供 GetUserConnections 使用）：按平台过滤并限制条数
    #[instrument(skip(self, _tx), fields(user_id = %user_id))]
    pub async fn list_user_connections(
        &self,
        _tx: &Ctx,
        user_id: &str,
        platforms: &[String],
        limit: i32,
    ) -> Result<Vec<ConnectionInfo>> {
        let connections = self.connection_query.list_user_connections(user_id).await?;
        let limit = limit.max(0).min(500) as usize;
        let filtered: Vec<ConnectionInfo> = if platforms.is_empty() {
            connections
        } else {
            connections
                .into_iter()
                .filter(|c| {
                    c.platform
                        .as_ref()
                        .map(|p| platforms.iter().any(|f| f == p))
                        .unwrap_or(false)
                })
                .collect()
        };
        Ok(filtered.into_iter().take(limit).collect())
    }
}
