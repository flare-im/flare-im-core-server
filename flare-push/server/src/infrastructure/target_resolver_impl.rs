//! 推送目标解析器实现
//!
//! Infrastructure 层实现，依赖在线状态仓储。

use std::sync::Arc;
use async_trait::async_trait;
use anyhow::Result;
use flare_im_core::Ctx;
use flare_proto::common::{PushTargetType, PushEnvelope};

use crate::domain::model::DeviceInfo;
use crate::domain::service::TargetResolver;
use crate::domain::repository::OnlineStatusRepository;

/// 推送目标解析器实现
pub struct TargetResolverImpl {
    online_repo: Arc<dyn OnlineStatusRepository>,
}

impl TargetResolverImpl {
    pub fn new(online_repo: Arc<dyn OnlineStatusRepository>) -> Self {
        Self { online_repo }
    }
}

#[async_trait]
impl TargetResolver for TargetResolverImpl {
    async fn resolve(&self, ctx: &Ctx, envelope: &PushEnvelope) -> Result<Vec<DeviceInfo>> {
        let target_type = PushTargetType::try_from(envelope.target_type)
            .unwrap_or(PushTargetType::Unspecified);

        match target_type {
            PushTargetType::All => {
                // 全量推送：查询所有在线设备
                self.online_repo.get_all_online_devices(ctx).await
            }
            PushTargetType::Users => {
                // 用户列表推送：查询指定用户的在线设备
                self.online_repo
                    .get_devices_by_users(ctx, &envelope.target_user_ids)
                    .await
            }
            PushTargetType::Devices => {
                // 设备列表推送：直接返回设备列表
                // 需要从在线服务查询设备详情
                self.online_repo
                    .get_devices_by_ids(ctx, &envelope.target_device_ids)
                    .await
            }
            PushTargetType::Unspecified => {
                // 未指定目标类型，返回空列表
                Ok(Vec::new())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockOnlineRepo;

    #[async_trait]
    impl OnlineStatusRepository for MockOnlineRepo {
        async fn get_all_online_devices(&self, _ctx: &Ctx) -> Result<Vec<DeviceInfo>> {
            Ok(vec![
                DeviceInfo::new(
                    "device-1".to_string(),
                    "user-1".to_string(),
                    "ios".to_string(),
                    Some("token-1".to_string()),
                ),
            ])
        }

        async fn get_devices_by_users(&self, _ctx: &Ctx, user_ids: &[String]) -> Result<Vec<DeviceInfo>> {
            Ok(user_ids
                .iter()
                .map(|user_id| DeviceInfo::new(
                    format!("device-{}", user_id),
                    user_id.clone(),
                    "ios".to_string(),
                    Some(format!("token-{}", user_id)),
                ))
                .collect())
        }

        async fn get_devices_by_ids(&self, _ctx: &Ctx, device_ids: &[String]) -> Result<Vec<DeviceInfo>> {
            Ok(device_ids
                .iter()
                .map(|device_id| DeviceInfo::new(
                    device_id.clone(),
                    "user-unknown".to_string(),
                    "ios".to_string(),
                    Some(format!("token-{}", device_id)),
                ))
                .collect())
        }
    }

    #[tokio::test]
    async fn test_resolve_users() {
        let online_repo = Arc::new(MockOnlineRepo);
        let resolver = TargetResolverImpl::new(online_repo);
        
        let envelope = PushEnvelope {
            envelope_id: "test-123".to_string(),
            tenant_id: "tenant-1".to_string(),
            trace_id: "trace-123".to_string(),
            created_at_ms: 1234567890,
            target_type: PushTargetType::Users as i32,
            target_user_ids: vec!["user-1".to_string(), "user-2".to_string()],
            target_device_ids: Vec::new(),
            payload_kind: 0,
            options: None,
            payload: None,
            headers: std::collections::HashMap::new(),
        };

        let ctx = flare_server_core::context::Context::with_request_id("trace-123");
        
        assert_eq!(devices.len(), 2);
    }
}
