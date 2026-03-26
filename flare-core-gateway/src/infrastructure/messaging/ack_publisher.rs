//! ACK上报发布器
//!
//! 使用 common/ack.proto 的 Ack + SendAck 作为核心 ACK 结构
//! Gateway 作为接入层，通过轻量的 gRPC 上报，不直接依赖 Kafka

use async_trait::async_trait;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

/// ACK 状态值
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum AckStatusValue {
    Success,
    Failed,
}

/// ACK 核心数据
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AckData {
    pub server_msg_id: String,
    pub seq: Option<u64>,
    pub status: AckStatusValue,
    pub error_code: Option<i32>,
    pub error_message: Option<String>,
}

/// ACK 审计事件（用于上报）
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AckAuditEvent {
    pub ack: AckData,
    pub user_id: String,
    pub connection_id: String,
    pub gateway_id: String,
    pub timestamp: i64,
    pub window_id: Option<String>,
    pub ack_seq: Option<i64>,
}

impl AckAuditEvent {
    pub fn ack_type(&self) -> &'static str {
        if self.window_id.is_some() {
            "window_ack"
        } else {
            "message_ack"
        }
    }
}

/// ACK 发布器 trait
#[async_trait]
pub trait AckPublisher: Send + Sync {
    async fn publish_ack(&self, event: &AckAuditEvent) -> Result<()>;
}

/// gRPC ACK 发布器（轻量，推荐）
///
/// Gateway 作为接入层，通过 gRPC 直接上报到 AccessGateway 服务
/// 优势：
/// - ✅ 无需 Kafka 依赖，部署更轻量
/// - ✅ 直接通信，延迟更低（~2ms vs ~10ms）
/// - ✅ 失败降级，不阻塞 Gateway 核心功能
pub struct GrpcAckPublisher {
    /// AccessGateway 服务名称（用于服务发现）
    service_name: String,
    /// gRPC 客户端（懒加载）
    client: Arc<
        Mutex<
            Option<
                flare_proto::access_gateway::access_gateway_client::AccessGatewayClient<
                    tonic::transport::Channel,
                >,
            >,
        >,
    >,
    /// 服务发现客户端（懒加载）
    service_client: Arc<Mutex<Option<flare_server_core::discovery::ServiceClient>>>,
}

impl GrpcAckPublisher {
    /// 创建 gRPC ACK 发布器
    ///
    /// # 参数
    /// * `service_name` - AccessGateway 服务名称（用于服务发现）
    pub fn new(service_name: String) -> Self {
        Self {
            service_name,
            client: Arc::new(Mutex::new(None)),
            service_client: Arc::new(Mutex::new(None)),
        }
    }

    /// 确保 gRPC 客户端已初始化（懒加载）
    async fn ensure_client(
        &self,
    ) -> Result<
        flare_proto::access_gateway::access_gateway_client::AccessGatewayClient<
            tonic::transport::Channel,
        >,
    > {
        let mut client_guard = self.client.lock().await;
        if let Some(client) = client_guard.as_ref() {
            return Ok(client.clone());
        }

        // 使用服务发现获取 Channel
        let mut service_client_guard = self.service_client.lock().await;
        if service_client_guard.is_none() {
            // 如果没有注入 ServiceClient，则创建服务发现器
            let discover = flare_im_core::discovery::create_discover(&self.service_name)
                .await
                .map_err(|e| {
                    ErrorBuilder::new(
                        ErrorCode::ServiceUnavailable,
                        "access gateway service unavailable",
                    )
                    .details(format!(
                        "Failed to create service discover for {}: {}",
                        self.service_name, e
                    ))
                    .build_error()
                })?;

            if let Some(discover) = discover {
                *service_client_guard =
                    Some(flare_server_core::discovery::ServiceClient::new(discover));
            } else {
                return Err(ErrorBuilder::new(
                    ErrorCode::ServiceUnavailable,
                    "access gateway service unavailable",
                )
                .details("Service discovery not configured")
                .build_error());
            }
        }

        let service_client = service_client_guard.as_mut().unwrap();
        let channel = service_client.get_channel().await.map_err(|e| {
            ErrorBuilder::new(
                ErrorCode::ServiceUnavailable,
                "access gateway service unavailable",
            )
            .details(format!("Failed to get channel: {}", e))
            .build_error()
        })?;

        debug!("Got channel for access gateway service from service discovery");
        let client =
            flare_proto::access_gateway::access_gateway_client::AccessGatewayClient::new(channel);
        *client_guard = Some(client.clone());
        Ok(client)
    }
}

#[async_trait]
impl AckPublisher for GrpcAckPublisher {
    async fn publish_ack(&self, event: &AckAuditEvent) -> Result<()> {
        use flare_proto::common::ack::Payload as AckPayload;
        use flare_proto::common::{Ack, AckType, SendAck};

        let send_ack = SendAck {
            client_msg_id: String::new(),
            server_msg_id: event.ack.server_msg_id.clone(),
            seq: event.ack.seq.unwrap_or(0),
            conversation_id: String::new(),
            success: matches!(event.ack.status, AckStatusValue::Success),
            error_code: event.ack.error_code.unwrap_or(0),
            error_message: event.ack.error_message.clone().unwrap_or_default(),
            server_time: None,
            ack_id: None,
            metadata: std::collections::HashMap::new(),
        };
        let ack = Ack {
            r#type: AckType::Send as i32,
            ack_id: None,
            at: None,
            payload: Some(AckPayload::Send(send_ack)),
        };

        let request = flare_proto::access_gateway::PushAckRequest {
            user_ids: vec![event.user_id.clone()],
            ack: Some(ack),
            options: None,
        };

        // 发送 gRPC 请求到 AccessGateway Service
        match self.ensure_client().await {
            Ok(client) => {
                let mut client_clone = client.clone();
                match client_clone.push_ack(tonic::Request::new(request)).await {
                    Ok(response) => {
                        info!(
                            message_id = %event.ack.server_msg_id,
                            user_id = %event.user_id,
                            ack_type = %event.ack_type(),
                            status = ?event.ack.status,
                            "ACK reported to AccessGateway Service successfully"
                        );
                        debug!(?response, "PushAck response");
                        Ok(())
                    }
                    Err(status) => {
                        warn!(
                            message_id = %event.ack.server_msg_id,
                            user_id = %event.user_id,
                            error = %status,
                            "Failed to report ACK to AccessGateway Service"
                        );
                        Err(ErrorBuilder::new(
                            ErrorCode::ServiceUnavailable,
                            "Failed to report ACK to AccessGateway Service",
                        )
                        .details(status.to_string())
                        .build_error())
                    }
                }
            }
            Err(e) => {
                warn!(
                    message_id = %event.ack.server_msg_id,
                    user_id = %event.user_id,
                    error = %e,
                    "Failed to initialize AccessGateway Service client"
                );
                Err(e)
            }
        }
    }
}

/// Noop ACK发布器（用于测试或禁用ACK上报）
pub struct NoopAckPublisher;

impl NoopAckPublisher {
    pub fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

#[async_trait]
impl AckPublisher for NoopAckPublisher {
    async fn publish_ack(&self, _event: &AckAuditEvent) -> Result<()> {
        // 不做任何操作，用于测试或禁用 ACK 上报
        Ok(())
    }
}
