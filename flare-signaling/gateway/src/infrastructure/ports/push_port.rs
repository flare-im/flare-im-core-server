//! [`IPushPort`] 基础设施实现（与 `domain/ports/push_port.rs` 对应）
//!
//! [`PushRepository`]：基于 `ServerHandle` 的真实推送（需启动时装配句柄）。

use std::sync::Arc;

use async_trait::async_trait;
use flare_core::common::protocol::{
    PayloadCommand, Reliability, frame_with_payload_command, generate_message_id,
    payload_command::Type as PayloadType,
};
use flare_core::server::DeviceOwner;
use flare_core::server::handle::ServerHandle;
use flare_im_contracts::Ctx;
use flare_im_contracts::utils::normalize_optional_tenant_id;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::domain::ports::IPushPort;

/// 推送仓储：持有 `ServerHandle` 槽位，实际发送由 Flare 注入后执行
pub struct PushRepository {
    server_handle: Arc<Mutex<Option<Arc<dyn ServerHandle>>>>,
}

impl PushRepository {
    pub fn new(server_handle: Arc<Mutex<Option<Arc<dyn ServerHandle>>>>) -> Self {
        Self { server_handle }
    }

    /// 按用户推送的目标：（租户，用户）。flare-core 只投递到该租户下的连接，
    /// 上下文没有租户时拒绝，不会退到默认租户去投。
    fn target_user(tx: &Ctx, user_id: &str) -> Result<DeviceOwner> {
        let tenant_id = normalize_optional_tenant_id(tx.tenant_id()).ok_or_else(|| {
            ErrorBuilder::new(
                ErrorCode::PermissionDenied,
                "tenant required to push to a user",
            )
            .build_error()
        })?;
        Ok(DeviceOwner::new(Some(tenant_id.as_str()), user_id))
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
    async fn push_message_to_user(&self, tx: &Ctx, user_id: &str, message: Vec<u8>) -> Result<()> {
        let target = Self::target_user(tx, user_id)?;
        let handle = self.server_handle("push_message_to_user").await?;

        let cmd = PayloadCommand {
            r#type: PayloadType::Message as i32,
            message_id: generate_message_id(),
            payload: message,
            metadata: Default::default(),
            seq: 0,
        };
        let frame = frame_with_payload_command(cmd, Reliability::AtLeastOnce);

        handle.send_to_user(&target, &frame).await.map_err(|e| {
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
        tx: &Ctx,
        user_id: &str,
        payload_type: i32,
        payload: Vec<u8>,
    ) -> Result<()> {
        let target = Self::target_user(tx, user_id)?;
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

        handle.send_to_user(&target, &frame).await.map_err(|e| {
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
        let frame = frame_with_payload_command(cmd, Reliability::AtLeastOnce);
        // 分组扇出（flare-core）：同（格式,压缩）无加密连接共享一次序列化，
        // N 订阅者下行从 N 次 serialize+compress → 组数次（通常 1）。
        let (success, failure) = handle.send_to_connections(&connection_ids, &frame).await;
        if failure > 0 {
            warn!(
                message_id = %message_id,
                success,
                failure,
                "payload fanout finished with failures"
            );
        } else {
            debug!(
                message_id = %message_id,
                success,
                "payload fanout finished"
            );
        }
        Ok((success, failure))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_core::common::error::Result as CoreResult;
    use flare_core::common::protocol::Frame;
    use std::sync::Mutex as StdMutex;

    /// 记下每次按用户推送的目标（租户，用户）。
    #[derive(Default)]
    struct RecordingHandle {
        targets: StdMutex<Vec<DeviceOwner>>,
    }

    #[async_trait]
    impl ServerHandle for RecordingHandle {
        async fn send_to(&self, _connection_id: &str, _frame: &Frame) -> CoreResult<()> {
            Ok(())
        }

        async fn send_to_user(&self, owner: &DeviceOwner, _frame: &Frame) -> CoreResult<()> {
            self.targets.lock().unwrap().push(owner.clone());
            Ok(())
        }

        async fn broadcast(&self, _frame: &Frame) -> CoreResult<()> {
            Ok(())
        }

        async fn broadcast_except(&self, _frame: &Frame, _exclude: &str) -> CoreResult<()> {
            Ok(())
        }

        async fn disconnect(&self, _connection_id: &str) -> CoreResult<()> {
            Ok(())
        }

        fn connection_count(&self) -> usize {
            0
        }

        fn user_count(&self) -> usize {
            0
        }
    }

    fn repository() -> (PushRepository, Arc<RecordingHandle>) {
        let handle = Arc::new(RecordingHandle::default());
        let slot: Arc<dyn ServerHandle> = handle.clone();
        (
            PushRepository::new(Arc::new(Mutex::new(Some(slot)))),
            handle,
        )
    }

    fn ctx(tenant_id: Option<&str>) -> Ctx {
        let root = flare_server_core::Context::root();
        Arc::new(match tenant_id {
            Some(tenant_id) => root.with_tenant_id(tenant_id),
            None => root,
        })
    }

    /// 按用户推送只投到上下文租户下的同名用户；`default` 与 `0` 是同一个默认租户。
    #[tokio::test]
    async fn push_to_user_targets_the_context_tenant() {
        let (repository, handle) = repository();
        repository
            .push_payload_to_user(&ctx(Some("acme")), "u1", PayloadType::Ack as i32, vec![1])
            .await
            .unwrap();
        repository
            .push_message_to_user(&ctx(Some("default")), "u1", vec![2])
            .await
            .unwrap();

        assert_eq!(
            *handle.targets.lock().unwrap(),
            vec![
                DeviceOwner::new(Some("acme"), "u1"),
                DeviceOwner::new(Some("0"), "u1"),
            ]
        );
    }

    /// 上下文没有租户时拒绝推送，不会退到默认租户去投。
    #[tokio::test]
    async fn push_to_user_without_a_tenant_is_rejected() {
        let (repository, handle) = repository();
        let error = repository
            .push_payload_to_user(&ctx(None), "u1", PayloadType::Ack as i32, vec![1])
            .await
            .expect_err("no tenant, no push");
        assert_eq!(error.code(), Some(ErrorCode::PermissionDenied));
        assert!(
            repository
                .push_message_to_user(&ctx(None), "u1", vec![2])
                .await
                .is_err()
        );
        assert!(handle.targets.lock().unwrap().is_empty());
    }
}
