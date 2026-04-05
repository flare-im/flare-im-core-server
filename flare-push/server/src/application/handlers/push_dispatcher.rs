//! 推送调度器
//!
//! 负责调度推送任务，包括目标解析、设备过滤、并行推送、结果聚合。
//!
//! ## 职责
//! 1. 验证推送信封（过期检查）
//! 2. 解析推送目标
//! 3. 过滤无效设备
//! 4. 并行执行推送
//! 5. 聚合推送结果

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use flare_im_core::Ctx;
use flare_proto::common::{PushEnvelope, PushResult};
use tracing::{debug,info, instrument};

use crate::domain::{TargetResolver, DeviceInfo};

/// 推送调度器
///
/// ## 设计
/// - Application 层编排器
/// - 协调 TargetResolver 和 PushExecutor
/// - 支持并行推送和结果聚合
pub struct PushDispatcher {
    target_resolver: Arc<dyn TargetResolver>,
    push_executor: Arc<dyn PushExecutor>,
    max_concurrent_pushes: usize,
}

impl PushDispatcher {
    /// 创建推送调度器
    pub fn new(
        target_resolver: Arc<dyn TargetResolver>,
        push_executor: Arc<dyn PushExecutor>,
        max_concurrent_pushes: usize,
    ) -> Self {
        Self {
            target_resolver,
            push_executor,
            max_concurrent_pushes,
        }
    }

    /// 检查推送信封是否过期
    fn is_expired(&self, envelope: &PushEnvelope) -> bool {
        if let Some(ref options) = envelope.options {
            if options.expire_at_ms > 0 {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;
                return now > options.expire_at_ms;
            }
        }
        false
    }

    /// 过滤无效设备
    fn filter_devices(&self, devices: Vec<DeviceInfo>) -> Vec<DeviceInfo> {
        devices
            .into_iter()
            .filter(|device| {
                // 过滤没有 push_token 的设备
                device.push_token.is_some()
            })
            .collect()
    }

    /// 调度推送任务
    ///
    /// ## 流程
    /// 1. 检查过期
    /// 2. 解析目标
    /// 3. 过滤设备
    /// 4. 并行推送
    /// 5. 聚合结果
    #[instrument(skip(self, envelope), fields(envelope_id = %envelope.envelope_id))]
    pub async fn dispatch(
        &self,
        ctx: &Ctx,
        envelope: PushEnvelope,
    ) -> Result<Vec<PushResult>> {
        // 1. 检查过期
        if self.is_expired(&envelope) {
            debug!(
                envelope_id = %envelope.envelope_id,
                "Push envelope expired, skip"
            );
            return Ok(Vec::new());
        }

        // 2. 解析目标
        let devices = self.target_resolver.resolve(ctx, &envelope).await?;
        
        debug!(
            envelope_id = %envelope.envelope_id,
            device_count = devices.len(),
            "Resolved push targets"
        );

        // 3. 过滤设备
        let valid_devices = self.filter_devices(devices);

        if valid_devices.is_empty() {
            info!(
                envelope_id = %envelope.envelope_id,
                "No valid devices to push"
            );
            return Ok(Vec::new());
        }

        // 4. 并行推送
        let results = self.push_to_devices(ctx, &envelope, valid_devices).await;

        // 5. 返回结果
        Ok(results)
    }

    /// 并行推送到多个设备
    async fn push_to_devices(
        &self,
        ctx: &Ctx,
        envelope: &PushEnvelope,
        devices: Vec<DeviceInfo>,
    ) -> Vec<PushResult> {
        use futures::future::join_all;

        // 限制并发数
        let chunks: Vec<Vec<DeviceInfo>> = devices
            .chunks(self.max_concurrent_pushes)
            .map(|chunk| chunk.to_vec())
            .collect();

        let mut all_results = Vec::new();

        for chunk in chunks {
            let futures: Vec<_> = chunk
                .into_iter()
                .map(|device| {
                    let executor = self.push_executor.clone();
                    let envelope = envelope.clone();
                    async move {
                        executor.execute(ctx, &envelope, &device).await
                    }
                })
                .collect();

            let results = join_all(futures).await;
            all_results.extend(results);
        }

        all_results
    }
}

/// 推送执行器 Trait
///
/// ## 设计
/// - Application 层接口
/// - Infrastructure 层提供具体实现（调用 Push Proxy）
#[async_trait::async_trait]
pub trait PushExecutor: Send + Sync {
    /// 执行单次推送
    async fn execute(
        &self,
        ctx: &Ctx,
        envelope: &PushEnvelope,
        device: &DeviceInfo,
    ) -> PushResult;
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_proto::common::{PushOptions, PushPayloadKind};

    struct MockTargetResolver;

    #[async_trait::async_trait]
    impl TargetResolver for MockTargetResolver {
        async fn resolve(&self, _ctx: &Ctx, _envelope: &PushEnvelope) -> Result<Vec<DeviceInfo>> {
            Ok(vec![
                DeviceInfo {
                    device_id: "device-1".to_string(),
                    user_id: "user-1".to_string(),
                    platform: "ios".to_string(),
                    push_token: Some("token-1".to_string()),
                },
            ])
        }
    }

    struct MockPushExecutor;

    #[async_trait::async_trait]
    impl PushExecutor for MockPushExecutor {
        async fn execute(
            &self,
            _ctx: &Ctx,
            envelope: &PushEnvelope,
            device: &DeviceInfo,
        ) -> PushResult {
            PushResult {
                envelope_id: envelope.envelope_id.clone(),
                device_id: device.device_id.clone(),
                user_id: device.user_id.clone(),
                success: true,
                error_code: String::new(),
                error_message: String::new(),
                pushed_at_ms: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
            }
        }
    }

    #[tokio::test]
    async fn test_dispatch() {
        let resolver = Arc::new(MockTargetResolver);
        let executor = Arc::new(MockPushExecutor);
        let dispatcher = PushDispatcher::new(resolver, executor, 10);

        let envelope = PushEnvelope {
            envelope_id: "test-123".to_string(),
            tenant_id: "tenant-1".to_string(),
            trace_id: "trace-123".to_string(),
            created_at_ms: 1234567890,
            target_type: 0,
            target_user_ids: Vec::new(),
            target_device_ids: Vec::new(),
            payload_kind: PushPayloadKind::Ack as i32,
            options: None,
            payload: None,
            headers: std::collections::HashMap::new(),
        };

        let ctx = Ctx::new("trace-123", Some("tenant-1".to_string()));
        let results = dispatcher.dispatch(&ctx, envelope).await.unwrap();

        assert_eq!(results.len(), 1);
        assert!(results[0].success);
    }
}
