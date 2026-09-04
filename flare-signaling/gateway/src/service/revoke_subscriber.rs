//! 撤销即断：订阅 redis kick 频道，收到 user_id 就关闭本节点该用户的全部长连接。
//!
//! 频道 = `{namespace}:kick`，与 api-gateway 的 `RedisTokenStore::kick_channel` 一致。
//! api-gateway `/api/v1/auth/revoke` 撤销用户令牌后 publish user_id 到该频道；
//! 各 signaling-gateway 订阅后按 user_id 定位本地连接并强制关闭（配合建连撤销检查，
//! 达成「撤销即断 + 不能重连」的全链路撤销）。

use std::sync::Arc;
use std::time::Duration;

use flare_core::server::connection::ConnectionManagerTrait;
use futures::StreamExt;
use tracing::{info, warn};

/// 启动后台订阅任务（best-effort，断线自动重连）。
pub fn spawn_revoke_subscriber(
    redis_url: String,
    namespace: String,
    connection_manager: Arc<dyn ConnectionManagerTrait>,
) {
    let channel = format!("{namespace}:kick");
    tokio::spawn(async move {
        loop {
            if let Err(err) = run_subscriber(&redis_url, &channel, &connection_manager).await {
                warn!(%err, channel = %channel, "revoke subscriber error; reconnecting in 3s");
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    });
    info!("revoke(kick) subscriber spawned");
}

async fn run_subscriber(
    redis_url: &str,
    channel: &str,
    connection_manager: &Arc<dyn ConnectionManagerTrait>,
) -> anyhow::Result<()> {
    let client = redis::Client::open(redis_url)?;
    let mut pubsub = client.get_async_pubsub().await?;
    pubsub.subscribe(channel).await?;
    info!(channel = %channel, "subscribed to revoke/kick channel");
    let mut stream = pubsub.on_message();
    while let Some(msg) = stream.next().await {
        let user_id: String = match msg.get_payload() {
            Ok(v) => v,
            Err(err) => {
                warn!(%err, "bad kick payload; ignoring");
                continue;
            }
        };
        let user_id = user_id.trim().to_string();
        if user_id.is_empty() {
            continue;
        }
        let conn_ids = connection_manager.get_user_connections(&user_id).await;
        let mut closed = 0usize;
        for cid in &conn_ids {
            if connection_manager.remove_connection(cid).await.is_ok() {
                closed += 1;
            }
        }
        if closed > 0 {
            info!(user_id = %user_id, closed, "revoked user: force-closed live connections on this node");
        }
    }
    Ok(())
}
