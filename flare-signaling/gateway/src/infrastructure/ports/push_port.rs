//! [`IPushPort`] 基础设施实现（与 `domain/ports/push_port.rs` 对应）
//!
//! [`PushRepository`]：基于 `ServerHandle` 的真实推送（需启动时装配句柄）。

use std::sync::Arc;

use async_trait::async_trait;
use flare_core::common::protocol::{
    payload_command::Type as PayloadType,
    PayloadCommand, Reliability, frame_with_payload_command, generate_message_id,
};
use flare_core::server::handle::ServerHandle;
use flare_im_core::Ctx;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::domain::ports::IPushPort;

/// 推送仓储：持有 `ServerHandle` 槽位，实际发送由 Flare 注入后执行
pub struct PushRepository {
    server_handle: Arc<Mutex<Option<Arc<dyn ServerHandle>>>>,
}

impl PushRepository {
    pub fn new(server_handle: Arc<Mutex<Option<Arc<dyn ServerHandle>>>>) -> Self {
        Self { server_handle }
    }
}

#[async_trait]
impl IPushPort for PushRepository {
    async fn push_message_to_user(&self, _tx: &Ctx, user_id: &str, message: Vec<u8>) -> Result<()> {
        let handle = {
            let guard = self.server_handle.lock().await;
            Arc::clone(guard.as_ref().ok_or_else(|| {
                ErrorBuilder::new(
                    ErrorCode::InternalError,
                    "ServerHandle not initialized",
                )
                .details("push_message_to_user")
                .build_error()
            })?)
        };

        let cmd = PayloadCommand {
            r#type: PayloadType::Message as i32,
            message_id: generate_message_id(),
            payload: message,
            metadata: Default::default(),
            seq: 0,
        };
        let frame = frame_with_payload_command(cmd, Reliability::AtLeastOnce);

        handle
            .send_to_user(user_id, &frame)
            .await
            .map_err(|e| {
                ErrorBuilder::new(ErrorCode::InternalError, "Failed to send message to user")
                    .details(format!("user_id={}, error={}", user_id, e))
                    .build_error()
            })?;

        info!(user_id = %user_id, "Message pushed to user");
        Ok(())
    }

    async fn push_message_to_connection(
        &self,
        _tx: &Ctx,
        connection_id: &str,
        message: Vec<u8>,
    ) -> Result<()> {
        let handle = {
            let guard = self.server_handle.lock().await;
            Arc::clone(guard.as_ref().ok_or_else(|| {
                ErrorBuilder::new(
                    ErrorCode::InternalError,
                    "ServerHandle not initialized",
                )
                .details("push_message_to_connection")
                .build_error()
            })?)
        };

        let cmd = PayloadCommand {
            r#type: PayloadType::Message as i32,
            message_id: generate_message_id(),
            payload: message,
            metadata: Default::default(),
            seq: 0,
        };
        let frame = frame_with_payload_command(cmd, Reliability::AtLeastOnce);

        handle
            .send_to(connection_id, &frame)
            .await
            .map_err(|e| {
                ErrorBuilder::new(ErrorCode::InternalError, "Failed to send message to connection")
                    .details(format!("connection_id={}, error={}", connection_id, e))
                    .build_error()
            })?;

        debug!(connection_id = %connection_id, "Message pushed to connection");
        Ok(())
    }

    async fn push_payload_to_connection(
        &self,
        _tx: &Ctx,
        connection_id: &str,
        payload_type: i32,
        payload: Vec<u8>,
    ) -> Result<()> {
        let handle = {
            let guard = self.server_handle.lock().await;
            Arc::clone(guard.as_ref().ok_or_else(|| {
                ErrorBuilder::new(
                    ErrorCode::InternalError,
                    "ServerHandle not initialized",
                )
                .details("push_payload_to_connection")
                .build_error()
            })?)
        };

        let message_id = generate_message_id();
        let cmd = PayloadCommand {
            r#type: payload_type,
            message_id: message_id.clone(),
            payload,
            metadata: Default::default(),
            seq: 0,
        };
        let frame = frame_with_payload_command(cmd, Reliability::AtLeastOnce);

        handle
            .send_to(connection_id, &frame)
            .await
            .map_err(|e| {
                ErrorBuilder::new(ErrorCode::InternalError, "Failed to send payload to connection")
                    .details(format!("connection_id={}, error={}", connection_id, e))
                    .build_error()
            })?;

        debug!(
            connection_id = %connection_id,
            message_id = %message_id,
            "Payload pushed to connection"
        );
        Ok(())
    }

    async fn push_payload_to_user(
        &self,
        _tx: &Ctx,
        user_id: &str,
        payload_type: i32,
        payload: Vec<u8>,
    ) -> Result<()> {
        let handle = {
            let guard = self.server_handle.lock().await;
            Arc::clone(guard.as_ref().ok_or_else(|| {
                ErrorBuilder::new(
                    ErrorCode::InternalError,
                    "ServerHandle not initialized",
                )
                .details("push_payload_to_user")
                .build_error()
            })?)
        };

        let message_id = generate_message_id();
        let cmd = PayloadCommand {
            r#type: payload_type,
            message_id: message_id.clone(),
            payload,
            metadata: Default::default(),
            seq: 0,
        };
        let frame = frame_with_payload_command(cmd, Reliability::AtLeastOnce);

        handle
            .send_to_user(user_id, &frame)
            .await
            .map_err(|e| {
                ErrorBuilder::new(ErrorCode::InternalError, "Failed to send payload to user")
                    .details(format!("user_id={}, error={}", user_id, e))
                    .build_error()
            })?;

        info!(
            user_id = %user_id,
            message_id = %message_id,
            "Payload pushed to user"
        );
        Ok(())
    }

    async fn push_payload_to_connections(
        &self,
        tx: &Ctx,
        connection_ids: &[String],
        payload_type: i32,
        payload: Vec<u8>,
    ) -> Result<(i32, i32)> {
        let mut seen = std::collections::HashSet::new();
        let mut success_count = 0i32;
        let mut failure_count = 0i32;
        for connection_id in connection_ids {
            if !seen.insert(connection_id.as_str()) {
                continue;
            }
            match self
                .push_payload_to_connection(tx, connection_id, payload_type, payload.clone())
                .await
            {
                Ok(()) => success_count += 1,
                Err(_) => failure_count += 1,
            }
        }
        Ok((success_count, failure_count))
    }
}
