//! 撤销即断：订阅 redis kick 频道，按已认证 `(tenant,user,device?)` 关闭本节点连接。
//!
//! 频道 = `{namespace}:kick`，与 api-gateway 的 `RedisTokenStore::kick_channel` 一致。
//! api-gateway `/api/v1/auth/revoke` 撤销用户令牌后 publish user_id 到该频道；
//! 各 signaling-gateway 订阅后按 user_id 定位本地连接并强制关闭（配合建连撤销检查，
//! 达成「撤销即断 + 不能重连」的全链路撤销）。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use flare_core::common::MessageParser;
use flare_core::common::protocol::{Reliability, builder, frame_with_system_command};
use flare_core::server::connection::{ConnectionManagerTrait, TraitConnectionInfo};
use flare_core::transport::connection::Connection;
use flare_im_contracts::domain::tenant::is_valid_tenant_id;
use flare_server_core::auth::KickSignal;
use futures::StreamExt;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

/// 信号没带原因时下发给连接端的原因（旧发布方）。
const DEFAULT_KICK_REASON: &str = "KICKED";
/// KICKED 帧最多等这么久；对端不读也不能拖住关连接。
const KICKED_SEND_TIMEOUT: Duration = Duration::from_secs(1);

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
        let closed = close_matching(connection_manager.as_ref(), &signal).await;
        if closed > 0 {
            info!(tenant_id = %signal.tenant_id, user_id = %signal.user_id, reason = kick_reason(&signal), closed, "revoked user: force-closed live connections on this node");
        }
    }
    Ok(())
}

fn kick_reason(signal: &KickSignal) -> &str {
    signal
        .reason
        .as_deref()
        .map(str::trim)
        .filter(|reason| !reason.is_empty())
        .unwrap_or(DEFAULT_KICK_REASON)
}

/// 关闭本节点上与信号匹配的连接，每条先下发带原因的 KICKED 帧；返回关闭数。
async fn close_matching(
    connection_manager: &dyn ConnectionManagerTrait,
    signal: &KickSignal,
) -> usize {
    let reason = kick_reason(signal);
    let conn_ids = connection_manager
        .get_user_connections(&signal.user_id)
        .await;
    let mut closed = 0usize;
    for cid in &conn_ids {
        let Some((connection, info)) = connection_manager.get_connection(cid).await else {
            continue;
        };
        if !kick_matches(
            signal,
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
        send_kicked(&connection, &info, reason).await;
        if connection_manager.remove_connection(cid).await.is_ok() {
            closed += 1;
        }
    }
    closed
}

/// 按连接协商好的编码写一个 KICKED 帧（message 与 metadata `reason` 都是原因）。直接写连接而不走
/// 写队列：随后的移除会立刻关闭写队列，排队中的帧会被丢掉。写不出去不影响关连接。
async fn send_kicked(
    connection: &Arc<Mutex<Box<dyn Connection>>>,
    info: &TraitConnectionInfo,
    reason: &str,
) {
    let metadata = HashMap::from([("reason".to_string(), reason.as_bytes().to_vec())]);
    let frame = frame_with_system_command(
        builder::kicked(reason, Some(metadata)),
        Reliability::BestEffort,
    );
    let encoded = match &info.cached_parser {
        Some(parser) => parser.serialize(&frame),
        None => MessageParser::new(
            info.serialization_format,
            info.compression.clone(),
            info.encryption.clone(),
        )
        .serialize(&frame),
    };
    let data = match encoded {
        Ok(data) => data,
        Err(err) => {
            warn!(connection_id = %info.connection_id, %err, "KICKED frame not encodable");
            return;
        }
    };
    let sent = tokio::time::timeout(KICKED_SEND_TIMEOUT, async {
        connection.lock().await.send(&data).await
    })
    .await;
    if !matches!(sent, Ok(Ok(()))) {
        debug!(connection_id = %info.connection_id, "KICKED frame not delivered before close");
    }
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
            reason: None,
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

    /// 记下写到连接上的每一帧。
    struct Recording {
        sent: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
        last_active: flare_core::common::platform::MonotonicInstant,
    }

    #[async_trait::async_trait]
    impl Connection for Recording {
        fn add_observer(&mut self, _: flare_core::transport::events::ArcObserver) {}

        fn remove_observer(&mut self, _: flare_core::transport::events::ArcObserver) {}

        async fn send(&mut self, data: &[u8]) -> flare_core::common::error::Result<()> {
            self.sent.lock().unwrap().push(data.to_vec());
            Ok(())
        }

        async fn close(&mut self) -> flare_core::common::error::Result<()> {
            Ok(())
        }

        fn last_active_time(&self) -> flare_core::common::platform::MonotonicInstant {
            self.last_active
        }

        fn update_active_time(&mut self) {
            self.last_active = flare_core::common::platform::monotonic_now();
        }
    }

    /// 一条已认证、JSON 编码的连接。
    async fn authenticated(
        manager: &flare_core::server::connection::ConnectionManager,
        connection_id: &str,
        tenant_id: &str,
    ) -> Arc<std::sync::Mutex<Vec<Vec<u8>>>> {
        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
        manager
            .add_connection(
                connection_id.into(),
                Box::new(Recording {
                    sent: sent.clone(),
                    last_active: flare_core::common::platform::monotonic_now(),
                }),
                Some("u1".into()),
                true,
            )
            .unwrap();
        manager
            .update_connection_negotiation(
                connection_id,
                None,
                flare_core::common::protocol::SerializationFormat::Json,
                flare_core::common::CompressionAlgorithm::None,
                flare_core::common::EncryptionAlgorithm::None,
                Some("u1".into()),
                Some(HashMap::from([("tenant_id".into(), tenant_id.into())])),
            )
            .unwrap();
        ConnectionManagerTrait::set_connection_authenticated(
            manager,
            connection_id,
            Some("u1".into()),
        )
        .await
        .unwrap();
        sent
    }

    fn kicked_message(data: &[u8]) -> String {
        use flare_core::common::protocol::flare::core::commands::{command, system_command};
        let frame = MessageParser::new(
            flare_core::common::protocol::SerializationFormat::Json,
            flare_core::common::CompressionAlgorithm::None,
            flare_core::common::EncryptionAlgorithm::None,
        )
        .parse(data)
        .expect("JSON frame");
        let Some(command::Type::System(system)) = frame.command.and_then(|c| c.r#type) else {
            panic!("system command expected");
        };
        assert_eq!(system.r#type, system_command::Type::Kicked as i32);
        assert_eq!(
            system.metadata.get("reason").map(Vec::as_slice),
            Some(system.message.as_bytes())
        );
        system.message
    }

    /// 被踢的连接在关闭前收到带原因的 KICKED；别的租户的同名用户不受影响。
    #[tokio::test]
    async fn kicked_connections_learn_the_reason_before_the_close() {
        let manager = flare_core::server::connection::ConnectionManager::new();
        let kicked = authenticated(&manager, "c-a", "tenant-a").await;
        let spared = authenticated(&manager, "c-b", "tenant-b").await;
        let signal = KickSignal {
            tenant_id: "tenant-a".into(),
            user_id: "u1".into(),
            device_id: None,
            connection_id: None,
            reason: Some("TENANT_UNAVAILABLE".into()),
        };

        assert_eq!(close_matching(&manager, &signal).await, 1);

        let frames = kicked.lock().unwrap().clone();
        assert_eq!(frames.len(), 1);
        assert_eq!(kicked_message(&frames[0]), "TENANT_UNAVAILABLE");
        assert!(
            manager.get_connection("c-a").is_none(),
            "kicked connection removed"
        );
        assert!(spared.lock().unwrap().is_empty());
        assert!(manager.get_connection("c-b").is_some());

        // 旧发布方的信号不带原因：照样关，原因是通用的 KICKED。
        let legacy = KickSignal {
            tenant_id: "tenant-b".into(),
            reason: None,
            ..signal
        };
        assert_eq!(close_matching(&manager, &legacy).await, 1);
        assert_eq!(
            kicked_message(&spared.lock().unwrap()[0]),
            DEFAULT_KICK_REASON
        );
    }
}
