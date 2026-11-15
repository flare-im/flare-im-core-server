//! [`ILinkProbePort`]：经 `ServerHandle` 探活与关闭连接（与 `domain/ports/link_probe_port.rs` 对应）

use std::sync::Arc;

use async_trait::async_trait;
use flare_core::common::protocol::{Reliability, frame_with_system_command, ping};
use flare_core::server::handle::ServerHandle;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use tokio::sync::Mutex;

use crate::domain::ports::ILinkProbePort;

/// 与 [`super::push_port::PushRepository`] 共用同一个 `ServerHandle` 槽位，长连接服务器启动时填入。
pub struct ServerHandleLinkProbe {
    server_handle: Arc<Mutex<Option<Arc<dyn ServerHandle>>>>,
}

impl ServerHandleLinkProbe {
    pub fn new(server_handle: Arc<Mutex<Option<Arc<dyn ServerHandle>>>>) -> Self {
        Self { server_handle }
    }

    async fn server_handle(&self) -> Result<Arc<dyn ServerHandle>> {
        let guard = self.server_handle.lock().await;
        Ok(Arc::clone(guard.as_ref().ok_or_else(|| {
            ErrorBuilder::new(ErrorCode::InternalError, "ServerHandle not initialized")
                .build_error()
        })?))
    }
}

#[async_trait]
impl ILinkProbePort for ServerHandleLinkProbe {
    async fn ping(&self, connection_id: &str) -> Result<()> {
        let handle = self.server_handle().await?;
        let frame = frame_with_system_command(ping(), Reliability::AtLeastOnce);
        handle.send_to(connection_id, &frame).await.map_err(|e| {
            ErrorBuilder::new(
                ErrorCode::InternalError,
                "Failed to send PING to connection",
            )
            .details(format!("connection_id={}, error={}", connection_id, e))
            .build_error()
        })
    }

    async fn close(&self, connection_id: &str) -> Result<()> {
        let handle = self.server_handle().await?;
        // 移出连接表并关掉底层传输；传输随后上报断开，业务侧的注销由那条断开事件完成。
        handle.disconnect(connection_id).await.map_err(|e| {
            ErrorBuilder::new(ErrorCode::InternalError, "Failed to close connection")
                .details(format!("connection_id={}, error={}", connection_id, e))
                .build_error()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_core::common::error::Result as CoreResult;
    use flare_core::common::protocol::{Frame, command, system_command};
    use flare_core::server::DeviceOwner;
    use std::sync::Mutex as StdMutex;

    /// 记下写给连接的系统命令类型与被断开的连接。
    #[derive(Default)]
    struct RecordingHandle {
        system_commands: StdMutex<Vec<(String, i32)>>,
        disconnected: StdMutex<Vec<String>>,
    }

    #[async_trait]
    impl ServerHandle for RecordingHandle {
        async fn send_to(&self, connection_id: &str, frame: &Frame) -> CoreResult<()> {
            if let Some(command::Type::System(system)) = frame
                .command
                .as_ref()
                .and_then(|command| command.r#type.as_ref())
            {
                self.system_commands
                    .lock()
                    .unwrap()
                    .push((connection_id.to_string(), system.r#type));
            }
            Ok(())
        }

        async fn send_to_user(&self, _owner: &DeviceOwner, _frame: &Frame) -> CoreResult<()> {
            Ok(())
        }

        async fn broadcast(&self, _frame: &Frame) -> CoreResult<()> {
            Ok(())
        }

        async fn broadcast_except(&self, _frame: &Frame, _exclude: &str) -> CoreResult<()> {
            Ok(())
        }

        async fn disconnect(&self, connection_id: &str) -> CoreResult<()> {
            self.disconnected
                .lock()
                .unwrap()
                .push(connection_id.to_string());
            Ok(())
        }

        fn connection_count(&self) -> usize {
            0
        }

        fn user_count(&self) -> usize {
            0
        }
    }

    #[tokio::test]
    async fn the_probe_writes_a_ping_and_closing_disconnects_the_connection() {
        let handle = Arc::new(RecordingHandle::default());
        let slot: Arc<dyn ServerHandle> = handle.clone();
        let probe = ServerHandleLinkProbe::new(Arc::new(Mutex::new(Some(slot))));

        probe.ping("c1").await.expect("ping written");
        assert_eq!(
            *handle.system_commands.lock().unwrap(),
            [("c1".to_string(), system_command::Type::Ping as i32)]
        );

        probe.close("c1").await.expect("closed");
        assert_eq!(*handle.disconnected.lock().unwrap(), ["c1"]);
    }

    /// 长连接服务器还没启动（槽位为空）时报错，不 panic。
    #[tokio::test]
    async fn an_empty_handle_slot_is_an_error() {
        let probe = ServerHandleLinkProbe::new(Arc::new(Mutex::new(None)));
        assert!(probe.ping("c1").await.is_err());
        assert!(probe.close("c1").await.is_err());
    }
}
