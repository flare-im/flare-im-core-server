//! 连接管理领域服务
//!
//! 封装连接管理的核心业务逻辑

use flare_proto::signaling::{HeartbeatRequest, LoginRequest, LogoutRequest};
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use std::sync::Arc;
use tracing::{info, instrument, warn};

use crate::domain::model::ConnectionDomainServiceConfig;
use crate::domain::ports::IConnectionPort;
use crate::domain::service::ConnectionQualityService;

/// 连接管理领域服务
///
/// 职责：
/// - 封装连接注册/注销逻辑
/// - 封装心跳管理逻辑
/// - 提供连接生命周期管理
pub struct ConnectionDomainService {
    connection_port: Arc<dyn IConnectionPort>,
    quality_service: Arc<ConnectionQualityService>,
    config: ConnectionDomainServiceConfig,
}

impl ConnectionDomainService {
    pub fn new(
        connection_port: Arc<dyn IConnectionPort>,
        quality_service: Arc<ConnectionQualityService>,
        config: ConnectionDomainServiceConfig,
    ) -> Self {
        Self {
            connection_port,
            quality_service,
            config,
        }
    }

    /// 注册连接（在线状态）
    ///
    /// 将用户的连接信息注册到 Signaling Online 服务
    #[instrument(skip(self), fields(user_id, device_id))]
    pub async fn register_connection(
        &self,
        user_id: &str,
        device_id: &str,
        connection_id: Option<&str>,
        connection_metadata: Option<&std::collections::HashMap<String, String>>,
    ) -> Result<String> {
        use uuid::Uuid;

        let _conversation_id = Uuid::new_v4().to_string();
        let server_id = self.config.gateway_id.clone();

        // 构建 metadata，包含 gateway_id
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("gateway_id".to_string(), self.config.gateway_id.clone());

        let login_request = LoginRequest {
            user_id: user_id.to_string(),
            token: String::new(),
            device_id: device_id.to_string(),
            server_id: server_id.clone(),
            metadata,
            device_platform: "unknown".to_string(),
            app_version: "unknown".to_string(),
            desired_conflict_strategy: 0,
            device_priority: 2, // Normal 优先级
            token_version: 0,
            initial_quality: None,
            resume_conversation_id: String::new(),
        };

        // 通过连接仓储调用登录，添加超时保护
        let login_result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.connection_port.login(login_request),
        )
        .await;

        match login_result {
            Ok(Ok(response)) => {
                if response.success {
                    info!(
                        user_id = %user_id,
                        conversation_id = %response.conversation_id,
                        "Connection registered successfully"
                    );
                    Ok(response.conversation_id)
                } else {
                    warn!(
                        user_id = %user_id,
                        error = %response.error_message,
                        "Failed to register connection"
                    );
                    Err(ErrorBuilder::new(
                        ErrorCode::OperationFailed,
                        format!("Failed to register connection: {}", response.error_message),
                    )
                    .build_error())
                }
            }
            Ok(Err(e)) => {
                warn!(
                    ?e,
                    user_id = %user_id,
                    "Failed to call signaling login"
                );
                Err(ErrorBuilder::new(
                    ErrorCode::InternalError,
                    format!("Signaling login failed: {}", e),
                )
                .build_error())
            }
            Err(_) => {
                warn!(
                    user_id = %user_id,
                    "Timeout while calling signaling login (5s)"
                );
                Err(
                    ErrorBuilder::new(ErrorCode::OperationTimeout, "Signaling login timeout")
                        .build_error(),
                )
            }
        }
    }

    /// 注销连接（离线状态）
    ///
    /// 通知 Signaling Online 服务注销用户连接
    #[instrument(skip(self), fields(user_id))]
    pub async fn unregister_connection(
        &self,
        user_id: &str,
        conversation_id: Option<&str>,
    ) -> Result<()> {
        let logout_request = LogoutRequest {
            user_id: user_id.to_string(),
            conversation_id: conversation_id.unwrap_or("").to_string(),
        };

        if let Err(e) = self.connection_port.logout(logout_request).await {
            warn!(
                ?e,
                user_id = %user_id,
                "Failed to call signaling logout"
            );
            return Err(ErrorBuilder::new(
                ErrorCode::InternalError,
                format!("Signaling logout failed: {}", e),
            )
            .build_error());
        }

        info!(
            user_id = %user_id,
            "Connection unregistered successfully"
        );
        Ok(())
    }

    /// 刷新连接心跳
    ///
    /// 向 Signaling Online 服务发送心跳，保持连接活跃。
    /// `connection_id` 可选，用于从质量服务获取该连接的 RTT/丢包率并上报。
    #[instrument(skip(self), fields(user_id))]
    pub async fn refresh_heartbeat(
        &self,
        user_id: &str,
        conversation_id: &str,
        connection_id: Option<&str>,
    ) -> Result<()> {
        // 从链接质量服务获取当前连接质量（按 connection_id 查）
        let current_quality = match connection_id {
            Some(cid) => self.quality_service.get_quality(cid).await,
            None => None,
        };
        let current_quality =
            current_quality.map(|metrics| flare_proto::common::ConnectionQuality {
                rtt_ms: metrics.rtt_ms,
                packet_loss_rate: metrics.packet_loss_rate,
                last_measure_ts: 0, // TODO: 填充正确的时间戳
                network_type: metrics.network_type,
                signal_strength: 0, // TODO: 填充正确的信号强度
            });

        let heartbeat_request = HeartbeatRequest {
            user_id: user_id.to_string(),
            conversation_id: conversation_id.to_string(),
            current_quality,
        };

        // 添加超时保护
        match tokio::time::timeout(
            std::time::Duration::from_secs(3),
            self.connection_port.heartbeat(heartbeat_request),
        )
        .await
        {
            Ok(Ok(_)) => {
                tracing::debug!(
                    user_id = %user_id,
                    conversation_id = %conversation_id,
                    "Heartbeat sent successfully"
                );
                Ok(())
            }
            Ok(Err(e)) => {
                warn!(
                    error = %e,
                    user_id = %user_id,
                    conversation_id = %conversation_id,
                    "Failed to send heartbeat"
                );
                Err(
                    ErrorBuilder::new(ErrorCode::InternalError, format!("Heartbeat failed: {}", e))
                        .build_error(),
                )
            }
            Err(_) => {
                warn!(
                    user_id = %user_id,
                    conversation_id = %conversation_id,
                    "Timeout sending heartbeat (3s)"
                );
                Err(
                    ErrorBuilder::new(ErrorCode::OperationTimeout, "Heartbeat timeout")
                        .build_error(),
                )
            }
        }
    }
}
