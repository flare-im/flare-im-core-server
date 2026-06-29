//! [`IPushPort`] 基础设施实现（与 `domain/ports/push_port.rs` 对应）
//!
//! [`PushRepository`]：基于 `ServerHandle` 的真实推送（需启动时装配句柄）。

use std::sync::{
    Arc,
    atomic::{AtomicI32, Ordering},
};

use async_trait::async_trait;
use flare_core::common::protocol::{
    PayloadCommand, Reliability, frame_with_payload_command, generate_message_id,
    payload_command::Type as PayloadType,
};
use flare_core::server::handle::ServerHandle;
use flare_im_contracts::Ctx;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use futures::stream::{self, StreamExt};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::domain::ports::IPushPort;

/// 推送仓储：持有 `ServerHandle` 槽位，实际发送由 Flare 注入后执行
pub struct PushRepository {
    server_handle: Arc<Mutex<Option<Arc<dyn ServerHandle>>>>,
}

const PUSH_CONNECTION_FANOUT_CONCURRENCY: usize = 64;

impl PushRepository {
    pub fn new(server_handle: Arc<Mutex<Option<Arc<dyn ServerHandle>>>>) -> Self {
        Self { server_handle }
    }

    async fn server_handle(&self, details: &'static str) -> Result<Arc<dyn ServerHandle>> {
        let guard = self.server_handle.lock().await;
        Ok(Arc::clone(guard.as_ref().ok_or_else(|| {
            ErrorBuilder::new(ErrorCode::InternalError, "ServerHandle not initialized")
                .details(details)
                .build_error()
        })?))
    }
}

#[async_trait]
impl IPushPort for PushRepository {
    async fn push_message_to_user(&self, _tx: &Ctx, user_id: &str, message: Vec<u8>) -> Result<()> {
        let handle = self.server_handle("push_message_to_user").await?;

        let cmd = PayloadCommand {
            r#type: PayloadType::Message as i32,
            message_id: generate_message_id(),
            payload: message,
            metadata: Default::default(),
            seq: 0,
        };
        let frame = frame_with_payload_command(cmd, Reliability::AtLeastOnce);

        handle.send_to_user(user_id, &frame).await.map_err(|e| {
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
        let handle = self.server_handle("push_message_to_connection").await?;

        let cmd = PayloadCommand {
            r#type: PayloadType::Message as i32,
            message_id: generate_message_id(),
            payload: message,
            metadata: Default::default(),
            seq: 0,
        };
        let frame = frame_with_payload_command(cmd, Reliability::AtLeastOnce);

        handle.send_to(connection_id, &frame).await.map_err(|e| {
            ErrorBuilder::new(
                ErrorCode::InternalError,
                "Failed to send message to connection",
            )
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
        let handle = self.server_handle("push_payload_to_connection").await?;

        let message_id = generate_message_id();
        let cmd = PayloadCommand {
            r#type: payload_type,
            message_id: message_id.clone(),
            payload,
            metadata: Default::default(),
            seq: 0,
        };
        let frame = frame_with_payload_command(cmd, Reliability::AtLeastOnce);

        handle.send_to(connection_id, &frame).await.map_err(|e| {
            ErrorBuilder::new(
                ErrorCode::InternalError,
                "Failed to send payload to connection",
            )
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
        let handle = self.server_handle("push_payload_to_user").await?;

        let message_id = generate_message_id();
        let cmd = PayloadCommand {
            r#type: payload_type,
            message_id: message_id.clone(),
            payload,
            metadata: Default::default(),
            seq: 0,
        };
        let frame = frame_with_payload_command(cmd, Reliability::AtLeastOnce);

        handle.send_to_user(user_id, &frame).await.map_err(|e| {
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
        _tx: &Ctx,
        connection_ids: &[String],
        payload_type: i32,
        payload: Vec<u8>,
    ) -> Result<(i32, i32)> {
        let mut seen = std::collections::HashSet::new();
        let connection_ids = connection_ids
            .iter()
            .filter(|connection_id| seen.insert(connection_id.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if connection_ids.is_empty() {
            return Ok((0, 0));
        }

        let handle = self.server_handle("push_payload_to_connections").await?;
        let message_id = generate_message_id();
        let cmd = PayloadCommand {
            r#type: payload_type,
            message_id: message_id.clone(),
            payload,
            metadata: Default::default(),
            seq: 0,
        };
        let frame = Arc::new(frame_with_payload_command(cmd, Reliability::AtLeastOnce));
        let success_count = Arc::new(AtomicI32::new(0));
        let failure_count = Arc::new(AtomicI32::new(0));

        stream::iter(connection_ids)
            .for_each_concurrent(PUSH_CONNECTION_FANOUT_CONCURRENCY, |connection_id| {
                let handle = Arc::clone(&handle);
                let frame = Arc::clone(&frame);
                let success_count = Arc::clone(&success_count);
                let failure_count = Arc::clone(&failure_count);
                let message_id = message_id.clone();
                async move {
                    match handle.send_to(&connection_id, &frame).await {
                        Ok(()) => {
                            success_count.fetch_add(1, Ordering::Relaxed);
                            debug!(
                                connection_id = %connection_id,
                                message_id = %message_id,
                                "Payload pushed to connection"
                            );
                        }
                        Err(error) => {
                            failure_count.fetch_add(1, Ordering::Relaxed);
                            warn!(
                                connection_id = %connection_id,
                                message_id = %message_id,
                                ?error,
                                "Failed to send payload to connection"
                            );
                        }
                    }
                }
            })
            .await;

        Ok((
            success_count.load(Ordering::Relaxed),
            failure_count.load(Ordering::Relaxed),
        ))
    }
}
