//! 撤销即断：订阅 redis kick 频道，按已认证 `(tenant,user,device?)` 关闭本节点连接。
//!
//! 频道 = `{namespace}:kick`，与 api-gateway 的 `RedisTokenStore::kick_channel` 一致。
//! api-gateway `/api/v1/auth/revoke` 撤销用户令牌后 publish user_id 到该频道；
//! 各 signaling-gateway 订阅后按 user_id 定位本地连接并强制关闭（配合建连撤销检查，
//! 达成「撤销即断 + 不能重连」的全链路撤销）。

use std::sync::Arc;
use std::time::Duration;

use flare_core::server::connection::ConnectionManagerTrait;
use flare_im_contracts::domain::tenant::is_valid_tenant_id;
use flare_server_core::auth::KickSignal;
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
) -> flare_server_core::error::Result<()> {
    let client = redis::Client::open(redis_url)?;
    let mut pubsub = client.get_async_pubsub().await?;
    pubsub.subscribe(channel).await?;
    info!(channel = %channel, "subscribed to revoke/kick channel");
    let mut stream = pubsub.on_message();
    while let Some(msg) = stream.next().await {
        let payload: String = match msg.get_payload() {
            Ok(v) => v,
            Err(err) => {
                warn!(%err, "bad kick payload; ignoring");
                continue;
            }
        };
        let signal: KickSignal = match serde_json::from_str(&payload) {
            Ok(signal) => signal,
            Err(_) => {
                warn!("unscoped or malformed kick payload rejected");
                continue;
            }
        };
        if !is_valid_tenant_id(&signal.tenant_id) || signal.user_id.trim().is_empty() {
            warn!("invalid tenant-scoped kick payload rejected");
            continue;
        }
        let conn_ids = connection_manager
            .get_user_connections(&signal.user_id)
            .await;
        let mut closed = 0usize;
        for cid in &conn_ids {
            let Some((_, info)) = connection_manager.get_connection(cid).await else {
                continue;
            };
            if !kick_matches(
                &signal,
                cid,
                info.authenticated,
                info.user_id.as_deref(),
                info.metadata.get("tenant_id").map(String::as_str),
                info.device_info
                    .as_ref()
                    .map(|device| device.device_id.as_str()),
            ) {
                continue;
            }
            if connection_manager.remove_connection(cid).await.is_ok() {
                closed += 1;
            }
        }
        if closed > 0 {
            info!(tenant_id = %signal.tenant_id, user_id = %signal.user_id, closed, "revoked user: force-closed live connections on this node");
        }
    }
    Ok(())
}

fn kick_matches(
    signal: &KickSignal,
    connection_id: &str,
    authenticated: bool,
    user_id: Option<&str>,
    tenant_id: Option<&str>,
    device_id: Option<&str>,
) -> bool {
    authenticated
        && signal
            .connection_id
            .as_deref()
            .is_none_or(|target| target == connection_id)
        && user_id == Some(signal.user_id.as_str())
        && tenant_id == Some(signal.tenant_id.as_str())
        && signal
            .device_id
            .as_deref()
            .is_none_or(|target| device_id == Some(target))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kick_never_crosses_tenant_or_device() {
        let signal = KickSignal {
            tenant_id: "a".into(),
            user_id: "same".into(),
            device_id: Some("d1".into()),
            connection_id: None,
        };
        assert!(kick_matches(
            &signal,
            "old",
            true,
            Some("same"),
            Some("a"),
            Some("d1")
        ));
        assert!(!kick_matches(
            &signal,
            "old",
            true,
            Some("same"),
            Some("b"),
            Some("d1")
        ));
        assert!(!kick_matches(
            &signal,
            "old",
            true,
            Some("same"),
            Some("a"),
            Some("d2")
        ));
        assert!(!kick_matches(
            &signal,
            "old",
            false,
            Some("same"),
            Some("a"),
            Some("d1")
        ));
        assert!(serde_json::from_str::<KickSignal>("same").is_err());
        let targeted = KickSignal {
            connection_id: Some("old".into()),
            ..signal
        };
        assert!(kick_matches(
            &targeted,
            "old",
            true,
            Some("same"),
            Some("a"),
            Some("d1")
        ));
        assert!(!kick_matches(
            &targeted,
            "new",
            true,
            Some("same"),
            Some("a"),
            Some("d1")
        ));
    }
}
