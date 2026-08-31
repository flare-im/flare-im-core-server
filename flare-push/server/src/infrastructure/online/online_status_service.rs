use std::collections::HashMap;
use std::sync::Arc;

use flare_grpc_proto::signaling::online::GetOnlineStatusRequest;
use flare_grpc_proto::signaling::online::online_service_client::OnlineServiceClient;
use flare_im_contracts::Ctx;
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError, Result};
use redis::aio::ConnectionManager;
use tonic::transport::Channel;

use crate::config::PushServerConfig;

enum OnlineStatusBackend {
    Grpc(OnlineServiceClient<Channel>),
    Redis(ConnectionManager),
}

pub struct OnlineStatusService {
    config: Arc<PushServerConfig>,
    backend: OnlineStatusBackend,
}

impl OnlineStatusService {
    pub async fn new(config: Arc<PushServerConfig>) -> Result<Self> {
        let backend = match config.online_status_backend.as_str() {
            "redis" => {
                let client = redis::Client::open(config.redis_url.clone()).map_err(|err| {
                    FlareError::system(format!(
                        "invalid push server redis uri {}: {err}",
                        config.redis_url
                    ))
                })?;
                let manager = ConnectionManager::new(client).await.map_err(|err| {
                    FlareError::system(format!("connect push server redis: {err}"))
                })?;
                OnlineStatusBackend::Redis(manager)
            }
            "grpc" => {
                let endpoint = config.online_service_endpoint.clone();
                let channel = Channel::from_shared(endpoint.clone())
                    .map_err(|err| {
                        FlareError::system(format!("invalid online grpc uri {endpoint}: {err}"))
                    })?
                    .connect()
                    .await
                    .map_err(|err| {
                        FlareError::system(format!("connect online grpc {endpoint}: {err}"))
                    })?;
                OnlineStatusBackend::Grpc(OnlineServiceClient::new(channel))
            }
            other => {
                return Err(ErrorBuilder::new(
                    ErrorCode::ConfigurationError,
                    "unsupported push server online status backend",
                )
                .param("backend", other.to_string())
                .build_error());
            }
        };
        Ok(Self { config, backend })
    }

    pub async fn is_online(&self, ctx: &Ctx, user_id: &str) -> Result<bool> {
        let statuses = self.online_statuses(ctx, &[user_id.to_string()]).await?;
        Ok(statuses.get(user_id).copied().unwrap_or(false))
    }

    pub async fn online_statuses(
        &self,
        ctx: &Ctx,
        user_ids: &[String],
    ) -> Result<HashMap<String, bool>> {
        if user_ids.is_empty() {
            return Ok(HashMap::new());
        }

        match &self.backend {
            OnlineStatusBackend::Grpc(client) => {
                self.grpc_online_statuses(ctx, client, user_ids).await
            }
            OnlineStatusBackend::Redis(manager) => {
                self.redis_online_statuses(manager, user_ids).await
            }
        }
    }

    pub async fn conversation_online_user_ids(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
    ) -> Result<Vec<String>> {
        match &self.backend {
            OnlineStatusBackend::Redis(manager) => {
                self.redis_conversation_online_user_ids(manager, ctx, conversation_id)
                    .await
            }
            OnlineStatusBackend::Grpc(_) => Err(ErrorBuilder::new(
                ErrorCode::ConfigurationError,
                "conversation online index requires redis online status backend",
            )
            .param("backend", "grpc")
            .build_error()),
        }
    }

    async fn grpc_online_statuses(
        &self,
        ctx: &Ctx,
        client: &OnlineServiceClient<Channel>,
        user_ids: &[String],
    ) -> Result<HashMap<String, bool>> {
        let mut client = client.clone();
        let mut req = tonic::Request::new(GetOnlineStatusRequest {
            user_ids: user_ids.to_vec(),
        });
        flare_server_core::grpc::client::encode_context_to_metadata(
            req.metadata_mut(),
            ctx.as_ref(),
        );
        let resp = client.get_online_status(req).await?.into_inner();
        Ok(resp
            .statuses
            .into_iter()
            .map(|(user_id, status)| (user_id, status.online))
            .collect())
    }

    async fn redis_online_statuses(
        &self,
        manager: &ConnectionManager,
        user_ids: &[String],
    ) -> Result<HashMap<String, bool>> {
        let mut conn = manager.clone();
        let mut pipe = redis::pipe();
        for user_id in user_ids {
            pipe.hlen(format!("session:{user_id}"));
        }
        let counts: Vec<usize> = pipe
            .query_async(&mut conn)
            .await
            .map_err(|err| FlareError::system(format!("query redis online status batch: {err}")))?;
        Ok(user_ids
            .iter()
            .cloned()
            .zip(counts.into_iter().map(|count| count > 0))
            .collect())
    }

    /// 单次 SSCAN 取回的成员数。给大了等于把 SMEMBERS 的问题原样搬过来，
    /// 给小了则往返次数过多；1024 是常见折中。
    const ONLINE_SCAN_BATCH: usize = 1024;

    /// 一个会话最多取多少在线成员。超出即截断并告警——宁可少推一部分，
    /// 也不能让单个超大群把推送进程撑爆。
    const ONLINE_SCAN_HARD_CAP: usize = 200_000;

    async fn redis_conversation_online_user_ids(
        &self,
        manager: &ConnectionManager,
        ctx: &Ctx,
        conversation_id: &str,
    ) -> Result<Vec<String>> {
        let tenant_id = ctx.tenant_id().unwrap_or_else(|| self.default_tenant_id());
        let key = format!("conv:online:{tenant_id}:{conversation_id}");
        let mut conn = manager.clone();

        // 用 SSCAN 游标分批，不用 SMEMBERS。
        //
        // SMEMBERS 会把整个集合一次性拉进 Vec：500 万人的群就是一次调用分配
        // 几百 MB，并发几条消息就是 GB 级。线上实测 push-server 因此涨到
        // 13.4GB，触发的是**全局** OOM（CONSTRAINT_NONE），把 NATS 和 Redis
        // 一起拖下水——NATS 因此重启了 34 次。
        //
        // SMEMBERS 还会阻塞 Redis 的单线程：大集合期间所有其他命令排队，
        // 表现为整个 IM 的 seq 分配、在线查询集体变慢。
        let mut user_ids: Vec<String> = Vec::new();
        let mut cursor: u64 = 0;
        let mut truncated = false;
        loop {
            let (next, batch): (u64, Vec<String>) = redis::cmd("SSCAN")
                .arg(&key)
                .arg(cursor)
                .arg("COUNT")
                .arg(Self::ONLINE_SCAN_BATCH)
                .query_async(&mut conn)
                .await
                .map_err(|err| {
                    FlareError::system(format!("scan redis conversation online index {key}: {err}"))
                })?;
            user_ids.extend(batch.into_iter().filter(|id| !id.trim().is_empty()));
            cursor = next;
            if user_ids.len() >= Self::ONLINE_SCAN_HARD_CAP {
                truncated = true;
                break;
            }
            // SSCAN 以游标回到 0 表示遍历完成
            if cursor == 0 {
                break;
            }
        }

        user_ids.sort();
        user_ids.dedup();
        if truncated {
            user_ids.truncate(Self::ONLINE_SCAN_HARD_CAP);
            tracing::warn!(
                conversation_id = %conversation_id,
                cap = Self::ONLINE_SCAN_HARD_CAP,
                "会话在线成员超过上限，本次推送已截断"
            );
        }
        Ok(user_ids)
    }

    pub fn default_tenant_id(&self) -> &str {
        &self.config.default_tenant_id
    }
}

#[cfg(test)]
mod online_index_scan_tests {
    use super::*;

    /// 会话在线索引必须用游标分批读，不能整集合一次性进内存。
    ///
    /// SMEMBERS 对 500 万人的群一次分配几百 MB，并发几条消息就是 GB 级：
    /// 线上实测 push-server 涨到 13.4GB 触发全局 OOM，把 NATS 一起拖死
    /// （NATS 因此重启 34 次）。它还会阻塞 Redis 单线程，拖慢全局 seq 分配。
    ///
    /// 断言源码而不是行为，是因为这条性质无法从返回值上观察到——
    /// 换回 SMEMBERS 结果完全一样，只有内存曲线会爆。
    #[test]
    fn conversation_online_index_uses_cursor_scan_not_smembers() {
        let src = include_str!("online_status_service.rs");
        let body = src
            .split("async fn redis_conversation_online_user_ids")
            .nth(1)
            .expect("函数应存在");
        let body = &body[..body.find("\n    pub fn ").unwrap_or(body.len())];

        assert!(
            body.contains("\"SSCAN\""),
            "会话在线索引必须用 SSCAN 游标分批读取"
        );
        assert!(
            !body.contains("\"SMEMBERS\""),
            "不得用 SMEMBERS：整集合一次性进内存，大群会直接把推送进程撑爆"
        );
    }

    /// 必须有硬上限：光有分批而没有上限，超大群照样能把内存堆满，
    /// 只是从"一次分配几百 MB"变成"分很多次堆到几百 MB"。
    #[test]
    fn scan_has_a_hard_cap_and_batches_are_bounded() {
        assert!(
            OnlineStatusService::ONLINE_SCAN_HARD_CAP > 0
                && OnlineStatusService::ONLINE_SCAN_HARD_CAP <= 1_000_000,
            "上限要存在且在合理量级"
        );
        assert!(
            OnlineStatusService::ONLINE_SCAN_BATCH > 0
                && OnlineStatusService::ONLINE_SCAN_BATCH <= 10_000,
            "单批过大等于把 SMEMBERS 的问题原样搬过来"
        );
        assert!(
            OnlineStatusService::ONLINE_SCAN_BATCH < OnlineStatusService::ONLINE_SCAN_HARD_CAP,
            "单批不该超过总上限"
        );
    }
}
