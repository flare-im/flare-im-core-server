//! 推送执行器实现
//!
//! 负责实际执行推送，通过 Flare-Core 长连接服务推送消息。
//!
//! ## 职责
//! 1. 序列化推送载荷
//! 2. 调用 Flare-Core gRPC 接口
//! 3. 处理推送结果

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use async_trait::async_trait;
use flare_im_core::Ctx;
use flare_proto::common::{PushEnvelope, PushResult};
use prost::Message as _;
use tonic::transport::Channel;
use tracing::{debug, error, instrument};

/// Flare-Core gRPC 客户端
///
/// ## 设计
/// - Infrastructure 层实现
/// - 封装 gRPC 调用细节
pub struct FlareCoreClient {
    #[allow(dead_code)]
    channel: Channel,
}

impl FlareCoreClient {
    /// 创建 Flare-Core 客户端
    pub fn new(channel: Channel) -> Self {
        Self { channel }
    }

    /// 推送消息到设备
    ///
    /// # 参数
    /// - `device_id`: 设备ID
    /// - `payload`: 推送载荷（序列化后的 PushEnvelope）
    ///
    /// # 返回
    /// - `Ok(())`: 推送成功
    /// - `Err`: 推送失败
    #[instrument(skip(self, payload), fields(device_id = %device_id))]
    pub async fn push_to_device(&self, device_id: &str, payload: &[u8]) -> Result<()> {
        // 这里需要调用 Flare-Core 的 gRPC 接口
        // 假设 Flare-Core 提供了 PushService gRPC 接口
        // 实际实现需要根据 Flare-Core 的 proto 定义调整

        debug!(
            device_id = %device_id,
            payload_size = payload.len(),
            "Pushing to device via Flare-Core"
        );

        // TODO: 实际的 gRPC 调用
        // 示例代码：
        // let mut client = flare_proto::grpc::push_service_client::PushServiceClient::new(self.channel.clone());
        // let request = tonic::Request::new(flare_proto::grpc::PushRequest {
        //     device_id: device_id.to_string(),
        //     payload: payload.to_vec(),
        // });
        // let response = client.push(request).await
        //     .map_err(|e| Error::internal(format!("gRPC push failed: {}", e)))?;

        // 模拟推送成功
        Ok(())
    }
}

/// 推送执行器实现
///
/// ## 设计
/// - Infrastructure 层实现
/// - 实现 application 层的 PushExecutor trait
/// - 调用 Flare-Core 长连接服务
pub struct PushExecutorImpl {
    flare_core_client: Arc<FlareCoreClient>,
}

impl PushExecutorImpl {
    /// 创建推送执行器
    pub fn new(flare_core_client: Arc<FlareCoreClient>) -> Self {
        Self { flare_core_client }
    }

    /// 序列化推送信封
    fn serialize_envelope(envelope: &PushEnvelope) -> Vec<u8> {
        envelope.encode_to_vec()
    }

    /// 获取当前时间戳（毫秒）
    fn current_time_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }
}

#[async_trait]
impl PushExecutor for PushExecutorImpl {
    /// 执行单次推送
    ///
    /// ## 流程
    /// 1. 序列化推送信封
    /// 2. 调用 Flare-Core 推送接口
    /// 3. 构造推送结果
    #[instrument(skip(self, envelope, device), fields(
        envelope_id = %envelope.envelope_id,
        device_id = %device.device_id
    ))]
    async fn execute(&self, ctx: &Ctx, envelope: &PushEnvelope, device: &DeviceInfo) -> PushResult {
        let envelope_id = envelope.envelope_id.clone();
        let device_id = device.device_id.clone();
        let user_id = device.user_id.clone();

        // 1. 序列化推送信封
        let payload = Self::serialize_envelope(envelope);

        // 2. 调用 Flare-Core 推送接口
        match self
            .flare_core_client
            .push_to_device(&device_id, &payload)
            .await
        {
            Ok(()) => {
                debug!(
                    envelope_id = %envelope_id,
                    device_id = %device_id,
                    user_id = %user_id,
                    payload_kind = ?envelope.payload_kind,
                    "Push succeeded"
                );

                PushResult {
                    envelope_id,
                    device_id,
                    user_id,
                    success: true,
                    error_code: String::new(),
                    error_message: String::new(),
                    pushed_at_ms: Self::current_time_ms(),
                }
            }
            Err(e) => {
                error!(
                    envelope_id = %envelope_id,
                    device_id = %device_id,
                    user_id = %user_id,
                    error = %e,
                    "Push failed"
                );

                PushResult {
                    envelope_id,
                    device_id,
                    user_id,
                    success: false,
                    error_code: "PUSH_FAILED".to_string(),
                    error_message: e.to_string(),
                    pushed_at_ms: Self::current_time_ms(),
                }
            }
        }
    }
}

/// 推送执行器 Trait（从 server 层导入）
///
/// 注意：这里需要与 server 层的 PushExecutor trait 保持一致
/// 实际项目中应该将 trait 定义在共享的 crate 中
#[async_trait]
pub trait PushExecutor: Send + Sync {
    /// 执行单次推送
    async fn execute(&self, ctx: &Ctx, envelope: &PushEnvelope, device: &DeviceInfo) -> PushResult;
}

/// 设备信息（从 server 层导入）
///
/// 注意：这里需要与 server 层的 DeviceInfo 保持一致
/// 实际项目中应该将 DeviceInfo 定义在共享的 crate 中
#[derive(Debug, Clone)]
pub struct DeviceInfo {
    pub device_id: String,
    pub user_id: String,
    pub platform: String,
    pub push_token: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_proto::common::{AckPayload, PushTargetType};

    #[test]
    fn test_serialize_envelope() {
        let envelope = PushEnvelope {
            envelope_id: "test-123".to_string(),
            tenant_id: "tenant-1".to_string(),
            trace_id: "trace-123".to_string(),
            created_at_ms: 1234567890,
            target_type: PushTargetType::Users as i32,
            target_user_ids: vec!["user-1".to_string()],
            target_device_ids: Vec::new(),
            payload_kind: PushPayloadKind::Ack as i32,
            options: None,
            payload: Some(flare_proto::common::push_envelope::Payload::Ack(
                AckPayload {
                    message_id: "msg-123".to_string(),
                    conversation_id: "conv-123".to_string(),
                    seq: 100,
                    ack_type: "received".to_string(),
                    ack_at_ms: 1234567890,
                },
            )),
            headers: std::collections::HashMap::new(),
        };

        let payload = PushExecutorImpl::serialize_envelope(&envelope);
        assert!(!payload.is_empty());
    }
}
