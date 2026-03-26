//! 查询处理器（查询侧）- 直接调用基础设施层，不经过领域服务
//!
//! 在 CQRS 架构中，查询侧通常直接调用基础设施层（仓储实现），
//! 因为查询是只读操作，不涉及业务逻辑，不需要经过领域层。

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use flare_proto::signaling::online::{GetOnlineStatusResponse, OnlineStatus};
use prost_types::Timestamp;
use tracing::instrument;

use crate::application::queries::GetOnlineStatusQuery;
use crate::domain::model::OnlineStatusRecord;
/// 在线状态查询处理器（查询侧）
///
/// 直接调用基础设施层的仓储实现，不经过领域服务
pub struct OnlineQueryHandler<R: crate::domain::repository::ConversationRepository + Send + Sync> {
    repository: Arc<R>,
}

impl<R: crate::domain::repository::ConversationRepository + Send + Sync> OnlineQueryHandler<R> {
    pub fn new(repository: Arc<R>) -> Self {
        Self { repository }
    }

    /// 查询在线状态（直接调用基础设施层）
    #[instrument(skip(self), fields(user_count = query.request.user_ids.len()))]
    pub async fn get_online_status(
        &self,
        query: GetOnlineStatusQuery,
    ) -> Result<GetOnlineStatusResponse> {
        let statuses = self
            .repository
            .fetch_statuses(&query.request.user_ids)
            .await?;

        let mut result = HashMap::new();
        for user_id in &query.request.user_ids {
            let status = statuses
                .get(user_id)
                .cloned()
                .unwrap_or_else(|| OnlineStatusRecord {
                    online: false,
                    server_id: String::new(),
                    gateway_id: None,
                    cluster_id: None,
                    last_seen: None,
                    device_id: None,
                    device_platform: None,
                });

            result.insert(
                user_id.clone(),
                OnlineStatus {
                    online: status.online,
                    server_id: status.server_id,
                    cluster_id: status.cluster_id.unwrap_or_default(),
                    last_seen: status.last_seen.as_ref().map(|dt| Timestamp {
                        seconds: dt.timestamp(),
                        nanos: dt.timestamp_subsec_nanos() as i32,
                    }),
                    device_id: status.device_id.unwrap_or_default(),
                    device_platform: status.device_platform.unwrap_or_default(),
                    gateway_id: status.gateway_id.unwrap_or_default(),
                },
            );
        }

        Ok(GetOnlineStatusResponse {
            statuses: result,
            status: crate::util::rpc_status_ok(),
        })
    }
}
