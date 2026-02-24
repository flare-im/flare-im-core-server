//! ACK上报发布器
//!
//! 使用 transport.proto 定义的 SendEnvelopeAck 作为核心 ACK 结构
//! Gateway 作为接入层，通过轻量的 gRPC 上报，不直接依赖 Kafka

use async_trait::async_trait;
use flare_server_core::discovery::{DiscoveryFactory, ServiceClient};
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tracing::{debug, warn};

/// ACK 状态值
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum AckStatusValue {
    Success,
    Failed,
}

/// ACK 核心数据
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AckData {
    pub message_id: String,
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

/// gRPC ACK发布器
pub struct GrpcAckPublisher {
    /// 服务发现客户端（用于获取 Push 服务地址）
    service_client: Arc<RwLock<Option<ServiceClient>>>,
    /// 服务发现工厂（用于创建服务发现器）
    discovery_factory: Arc<DiscoveryFactory>,
    /// 服务类型
    service_type: String,
}

impl GrpcAckPublisher {
    pub fn new(
        discovery_factory: Arc<flare_server_core::discovery::DiscoveryFactory>,
        service_type: String,
    ) -> Arc<Self> {
        Arc::new(Self {
            service_client: Arc::new(RwLock::new(None)),
            discovery_factory,
            service_type,
        })
    }

    /// 确保 Push Proxy gRPC 客户端已初始化（懒加载）
    ///
    /// 注意：跨区域部署时，Push Proxy 在本地区域，延迟更低
    async fn ensure_client(
        &self,
    ) -> Result<flare_proto::push::push_service_client::PushServiceClient<Channel>> {
        // 检查客户端是否已经初始化
        {
            let client_guard = self.service_client.read().await;
            if client_guard.is_some() {
                // 客户端已初始化，直接返回
                // 注意：这里我们需要实际获取 Channel 并创建 gRPC 客户端
                // 但由于 ServiceClient 不支持 Clone，我们需要重新创建
            }
        }

        // 初始化服务发现器
        let service_discover = flare_im_core::discovery::create_discover(&self.service_type)
            .await
            .map_err(|e| {
                ErrorBuilder::new(
                    ErrorCode::ServiceUnavailable,
                    format!("Failed to create service discover: {}", e),
                )
                .build_error()
            })?
            .ok_or_else(|| {
                ErrorBuilder::new(
                    ErrorCode::ServiceUnavailable,
                    "Service discovery not configured".to_string(),
                )
                .build_error()
            })?;

        // 创建服务客户端
        let mut service_client = flare_server_core::discovery::ServiceClient::new(service_discover);

        // 获取 Channel
        let channel = service_client.get_channel().await.map_err(|e| {
            ErrorBuilder::new(
                ErrorCode::ServiceUnavailable,
                format!("Failed to get channel from service client: {}", e),
            )
            .build_error()
        })?;

        // 创建 Push Proxy gRPC 客户端（Push Proxy 提供 PushService 接口）
        let grpc_client = flare_proto::push::push_service_client::PushServiceClient::new(channel);

        // 更新服务客户端缓存
        {
            let mut client_guard = self.service_client.write().await;
            *client_guard = Some(service_client);
        }

        Ok(grpc_client)
    }
}

#[async_trait]
impl AckPublisher for GrpcAckPublisher {
    async fn publish_ack(&self, event: &AckAuditEvent) -> Result<()> {
        // 使用服务发现获取 Push Proxy gRPC 客户端并上报 ACK
        // 跨区域部署时，Push Proxy 在本地区域，延迟更低
        // ACK 通过 Push Proxy → Kafka → Push Server，保持架构一致性
        match self.ensure_client().await {
            Ok(mut client) => {
                // 构造 PushAckRequest（使用 Push Proxy 的接口，与 Push Server 接口一致）
                let request = tonic::Request::new(flare_proto::flare::push::v1::PushAckRequest {
                    target_user_ids: vec![event.user_id.clone()],
                    ack: Some(flare_proto::common::SendEnvelopeAck {
                        server_msg_id: "".to_string(),
                        seq: 0,
                        status: match event.ack.status {
                            AckStatusValue::Success => {
                                flare_proto::common::AckStatus::Success as i32
                            }
                            AckStatusValue::Failed => flare_proto::common::AckStatus::Failed as i32,
                        },
                        error_code: event.ack.error_code.unwrap_or(0),
                        error_message: event.ack.error_message.clone().unwrap_or_default(),
                    }),
                    metadata: std::collections::HashMap::new(),
                    request_id: format!("ack-{}", uuid::Uuid::new_v4()),
                });

                // 调用 Push Server 的 PushAck 接口上报 ACK
                match client.push_ack(request).await {
                    Ok(response) => {
                        let ack_response = response.into_inner();
                        if ack_response.fail_count == 0 {
                            debug!(
                                message_id = %event.ack.message_id,
                                user_id = %event.user_id,
                                status = ?event.ack.status,
                                success_count = ack_response.success_count,
                                "ACK event successfully reported to Push Proxy"
                            );
                            Ok(())
                        } else {
                            warn!(
                                message_id = %event.ack.message_id,
                                user_id = %event.user_id,
                                status = ?event.ack.status,
                                success_count = ack_response.success_count,
                                fail_count = ack_response.fail_count,
                                failed_user_ids = ?ack_response.failed_user_ids,
                                "Failed to report ACK event to Push Proxy for some users"
                            );
                            Err(ErrorBuilder::new(
                                ErrorCode::ServiceUnavailable,
                                format!(
                                    "Push Proxy rejected ACK for {} users: {:?}",
                                    ack_response.fail_count, ack_response.failed_user_ids
                                ),
                            )
                            .build_error())
                        }
                    }
                    Err(e) => {
                        warn!(
                            message_id = %event.ack.message_id,
                            user_id = %event.user_id,
                            status = ?event.ack.status,
                            error = %e,
                            "Failed to call Push Proxy service for ACK reporting"
                        );
                        Err(ErrorBuilder::new(
                            ErrorCode::ServiceUnavailable,
                            format!("Failed to call Push Proxy service: {}", e),
                        )
                        .build_error())
                    }
                }
            }
            Err(e) => {
                warn!(
                    message_id = %event.ack.message_id,
                    user_id = %event.user_id,
                    status = ?event.ack.status,
                    error = %e,
                    "Failed to initialize gRPC client for ACK reporting"
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
