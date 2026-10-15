//! 撤销即断：订阅 redis kick 频道，按已认证 `(tenant,user,device?)` 关闭本节点连接。
//!
//! 频道 = `{namespace}:kick`，与 api-gateway 的 `RedisTokenStore::kick_channel` 一致。
//! api-gateway `/api/v1/auth/revoke` 撤销用户令牌后 publish user_id 到该频道；
//! 各 signaling-gateway 订阅后按 user_id 定位本地连接并强制关闭（配合建连撤销检查，
//! 达成「撤销即断 + 不能重连」的全链路撤销）。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use flare_core::common::MessageParser;
use flare_core::common::protocol::{Reliability, builder, frame_with_system_command};
use flare_core::server::DeviceOwner;
use flare_core::server::connection::{ConnectionManagerTrait, TraitConnectionInfo};
use flare_core::transport::connection::Connection;
use flare_im_contracts::domain::tenant::is_valid_tenant_id;
use flare_server_core::auth::KickSignal;
use futures::StreamExt;
use tokio::sync::{Mutex, Semaphore};
use tracing::{debug, info, warn};

/// 信号没带原因时下发给连接端的原因（旧发布方）。
const DEFAULT_KICK_REASON: &str = "KICKED";
/// KICKED 帧最多等这么久；对端不读也不能拖住关连接。
const KICKED_SEND_TIMEOUT: Duration = Duration::from_secs(1);
/// 同时处理的踢线信号上限。每个信号在独立任务里处理，一条慢连接拖不住后面的信号；
/// 上限防止信号风暴时任务无界堆积，满了订阅循环就等一个名额。
const MAX_CONCURRENT_KICK_SIGNALS: usize = 64;

/// 启动后台订阅任务（best-effort，断线自动重连）。
pub fn spawn_revoke_subscriber(
    redis_url: String,
    namespace: String,
    connection_manager: Arc<dyn ConnectionManagerTrait>,
    gateway_id: String,
) {
    let channel = format!("{namespace}:kick");
    let dispatcher = KickDispatcher::new(
        connection_manager,
        MAX_CONCURRENT_KICK_SIGNALS,
        KICKED_SEND_TIMEOUT,
    );
    tokio::spawn(async move {
        loop {
            if let Err(err) = run_subscriber(&redis_url, &channel, &dispatcher, &gateway_id).await {
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
    dispatcher: &KickDispatcher,
    gateway_id: &str,
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
        if payload.len() > 4096 {
            continue;
        }
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
        // 不等这个信号踢完：它的连接再慢，也不耽误后面的信号
        let value: serde_json::Value = serde_json::from_str(&payload).unwrap_or_default();
        if value.get("command_id").is_some()
            && (value["target_gateway"].as_str() != Some(gateway_id)
                || value["command_id"]
                    .as_str()
                    .is_none_or(|id| uuid::Uuid::parse_str(id).is_err())
                || signal.connection_id.is_none())
        {
            continue;
        }
        let receipt = value
            .get("command_id")
            .and_then(|v| v.as_str())
            .filter(|id| uuid::Uuid::parse_str(id).is_ok())
            .filter(|_| value["target_gateway"].as_str() == Some(gateway_id))
            .zip(signal.connection_id.as_deref())
            .map(|(id, physical)| ReceiptAck {
                client: client.clone(),
                key: format!("{channel}:receipt:{id}"),
                physical: physical.into(),
            });
        drop(dispatcher.dispatch_internal(signal, receipt).await);
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

/// 把踢线信号交给独立任务执行，同时进行的信号数有上限。
#[derive(Clone)]
struct KickDispatcher {
    connection_manager: Arc<dyn ConnectionManagerTrait>,
    permits: Arc<Semaphore>,
    kicker: Arc<Kicker>,
}

struct ReceiptAck {
    client: redis::Client,
    key: String,
    physical: String,
}
async fn execute_and_confirm(
    kicker: &Kicker,
    manager: &dyn ConnectionManagerTrait,
    signal: &KickSignal,
    receipt: Option<ReceiptAck>,
) -> usize {
    let closed = kicker.kick(manager, signal).await;
    if let Some(receipt) = receipt
        && manager.get_connection(&receipt.physical).await.is_none()
    {
        use redis::AsyncCommands;
        // Acknowledges the addressed node's registry, not delivery of the KICKED frame.
        let _ = tokio::time::timeout(Duration::from_secs(1), async {
            let mut conn = receipt.client.get_multiplexed_async_connection().await?;
            conn.set_ex::<_, _, ()>(receipt.key, receipt.physical, 86400)
                .await
        })
        .await;
    }
    closed
}

impl KickDispatcher {
    fn new(
        connection_manager: Arc<dyn ConnectionManagerTrait>,
        max_signals: usize,
        send_timeout: Duration,
    ) -> Self {
        Self {
            connection_manager,
            permits: Arc::new(Semaphore::new(max_signals)),
            kicker: Arc::new(Kicker::new(send_timeout)),
        }
    }

    /// 等到有名额后在独立任务里执行信号，返回任务句柄（结果为关闭的连接数）。
    ///
    /// 名额池正常不会被关闭。万一被关闭，不 panic 也不丢信号：在调用方任务里串行执行，
    /// 由订阅循环本身限流，句柄在返回时已完成。
    #[cfg(test)]
    async fn dispatch(&self, signal: KickSignal) -> tokio::task::JoinHandle<usize> {
        self.dispatch_internal(signal, None).await
    }

    async fn dispatch_internal(
        &self,
        signal: KickSignal,
        receipt: Option<ReceiptAck>,
    ) -> tokio::task::JoinHandle<usize> {
        let connection_manager = self.connection_manager.clone();
        let kicker = self.kicker.clone();
        let Ok(permit) = self.permits.clone().acquire_owned().await else {
            warn!("kick limiter closed; executing kick signal inline");
            let closed =
                execute_and_confirm(&kicker, connection_manager.as_ref(), &signal, receipt).await;
            return tokio::spawn(std::future::ready(closed));
        };
        tokio::spawn(async move {
            let _permit = permit;
            execute_and_confirm(&kicker, connection_manager.as_ref(), &signal, receipt).await
        })
    }
}

/// 关闭匹配信号的连接。同一信号命中的连接并发下发 KICKED，每条最多等 `send_timeout`；
/// 同一条连接同一时刻只由一个信号处理，并发的重复信号不会让它收到两次 KICKED。
struct Kicker {
    send_timeout: Duration,
    in_flight: std::sync::Mutex<HashSet<String>>,
}

impl Kicker {
    fn new(send_timeout: Duration) -> Self {
        Self {
            send_timeout,
            in_flight: std::sync::Mutex::new(HashSet::new()),
        }
    }

    /// 执行一个踢线信号并记录结果；返回关闭数。
    async fn kick(
        &self,
        connection_manager: &dyn ConnectionManagerTrait,
        signal: &KickSignal,
    ) -> usize {
        let closed = self.close_matching(connection_manager, signal).await;
        if closed > 0 {
            info!(tenant_id = %signal.tenant_id, user_id = %signal.user_id, reason = kick_reason(signal), closed, "revoked user: force-closed live connections on this node");
        }
        closed
    }

    /// 关闭本节点上与信号匹配的连接，每条先下发带原因的 KICKED 帧；返回关闭数。
    async fn close_matching(
        &self,
        connection_manager: &dyn ConnectionManagerTrait,
        signal: &KickSignal,
    ) -> usize {
        let reason = kick_reason(signal);
        // 只取信号所属租户的连接；kick_matches 仍逐条核对租户、用户与设备
        let conn_ids = connection_manager
            .get_user_connections(&DeviceOwner::new(
                Some(signal.tenant_id.as_str()),
                signal.user_id.as_str(),
            ))
            .await;
        let mut targets = Vec::new();
        for cid in conn_ids {
            let Some((connection, info)) = connection_manager.get_connection(&cid).await else {
                continue;
            };
            if !kick_matches(
                signal,
                &cid,
                info.authenticated,
                info.user_id.as_deref(),
                info.metadata.get("tenant_id").map(String::as_str),
                info.device_info
                    .as_ref()
                    .map(|device| device.device_id.as_str()),
            ) {
                continue;
            }
            // 另一个信号正在踢它：交给那一个
            let Some(claim) = self.claim(&cid) else {
                continue;
            };
            targets.push((claim, connection, info));
        }
        let closed = futures::future::join_all(targets.into_iter().map(
            |(claim, connection, info)| async move {
                send_kicked(&connection, &info, reason, self.send_timeout).await;
                connection_manager
                    .remove_connection(&claim.connection_id)
                    .await
                    .is_ok()
            },
        ))
        .await;
        closed.into_iter().filter(|closed| *closed).count()
    }

    fn claim(&self, connection_id: &str) -> Option<InFlight<'_>> {
        let mut in_flight = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());
        in_flight
            .insert(connection_id.to_string())
            .then(|| InFlight {
                kicker: self,
                connection_id: connection_id.to_string(),
            })
    }
}

/// 正在被踢的连接；drop 时释放，任务被取消也不会一直占着。
struct InFlight<'a> {
    kicker: &'a Kicker,
    connection_id: String,
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.kicker
            .in_flight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.connection_id);
    }
}

/// 按连接协商好的编码写一个 KICKED 帧（message 与 metadata `reason` 都是原因）。直接写连接而不走
/// 写队列：随后的移除会立刻关闭写队列，排队中的帧会被丢掉。写不出去不影响关连接。
async fn send_kicked(
    connection: &Arc<Mutex<Box<dyn Connection>>>,
    info: &TraitConnectionInfo,
    reason: &str,
    send_timeout: Duration,
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
    let sent = tokio::time::timeout(send_timeout, async {
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

    #[tokio::test]
    #[ignore = "starts an owned redis-server; controlled transport, not real client socket"]
    async fn receiver_ack_names_only_its_gateway_and_removed_physical_connection() {
        use redis::AsyncCommands;
        use std::process::{Child, Command, Stdio};
        struct RedisOwner {
            child: Child,
            root: std::path::PathBuf,
        }
        impl Drop for RedisOwner {
            fn drop(&mut self) {
                let _ = self.child.kill();
                let _ = self.child.wait();
                let _ = std::fs::remove_dir_all(&self.root);
            }
        }
        struct Task(tokio::task::JoinHandle<()>);
        impl Drop for Task {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let root = std::path::PathBuf::from("/tmp")
            .join(format!("flare-kick-receiver-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let socket = root.join("redis.sock");
        let child = Command::new("redis-server")
            .args([
                "--port",
                "0",
                "--save",
                "",
                "--appendonly",
                "no",
                "--unixsocket",
            ])
            .arg(&socket)
            .args(["--unixsocketperm", "700"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let owned = RedisOwner { child, root };
        let url = format!("redis+unix://{}", socket.display());
        let client = redis::Client::open(url.clone()).unwrap();
        let mut conn = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Ok(c) = client.get_multiplexed_async_connection().await {
                    break c;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let manager = Arc::new(flare_core::server::connection::ConnectionManager::new());
        let sent = authenticated(&manager, "physical-old", "tenant-a").await;
        let new = authenticated(&manager, "physical-new", "tenant-a").await;
        let dispatcher = KickDispatcher::new(manager.clone(), 2, Duration::from_millis(50));
        let task = Task(tokio::spawn(async move {
            let _ = run_subscriber(&url, "owned:kick", &dispatcher, "gateway-a").await;
        }));
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let count: Vec<(String, i64)> = redis::cmd("PUBSUB")
                    .arg("NUMSUB")
                    .arg("owned:kick")
                    .query_async(&mut conn)
                    .await
                    .unwrap();
                if count[0].1 == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let id = uuid::Uuid::new_v4().to_string();
        let wrong = uuid::Uuid::new_v4().to_string();
        let mut signal = tenant_kick("u1");
        signal.connection_id = Some("physical-old".into());
        let mut payload = serde_json::to_value(&signal).unwrap();
        payload["command_id"] = wrong.clone().into();
        payload["target_gateway"] = "gateway-b".into();
        let _: i64 = conn
            .publish("owned:kick", payload.to_string())
            .await
            .unwrap();
        payload["command_id"] = id.clone().into();
        payload["target_gateway"] = "gateway-a".into();
        let _: i64 = conn
            .publish("owned:kick", payload.to_string())
            .await
            .unwrap();
        let key = format!("owned:kick:receipt:{id}");
        let ack: String = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let v: Option<String> = conn.get(&key).await.unwrap();
                if let Some(v) = v {
                    break v;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(ack, "physical-old");
        assert!(manager.get_connection("physical-old").is_none());
        assert!(manager.get_connection("physical-new").is_some());
        assert_eq!(sent.lock().unwrap().len(), 1);
        assert!(new.lock().unwrap().is_empty());
        let absent: Option<String> = conn
            .get(format!("owned:kick:receipt:{wrong}"))
            .await
            .unwrap();
        assert!(absent.is_none());
        drop(task);
        drop(owned);
    }

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

    /// 对端怎么收帧。
    #[derive(Clone, Copy)]
    enum Peer {
        /// 立刻收下
        Reads,
        /// 过这么久才收下
        Slow(Duration),
        /// 永远不读（写会一直挂着）
        Stuck,
    }

    /// 记下写到连接上的每一帧。
    struct Recording {
        sent: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
        last_active: flare_core::common::platform::MonotonicInstant,
        peer: Peer,
    }

    #[async_trait::async_trait]
    impl Connection for Recording {
        fn add_observer(&mut self, _: flare_core::transport::events::ArcObserver) {}

        fn remove_observer(&mut self, _: flare_core::transport::events::ArcObserver) {}

        async fn send(&mut self, data: &[u8]) -> flare_core::common::error::Result<()> {
            match self.peer {
                Peer::Reads => {}
                Peer::Slow(delay) => tokio::time::sleep(delay).await,
                Peer::Stuck => std::future::pending::<()>().await,
            }
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

    /// 一条已认证、JSON 编码、用户为 u1 的连接。
    async fn authenticated(
        manager: &flare_core::server::connection::ConnectionManager,
        connection_id: &str,
        tenant_id: &str,
    ) -> Arc<std::sync::Mutex<Vec<Vec<u8>>>> {
        connected(manager, connection_id, tenant_id, "u1", Peer::Reads).await
    }

    async fn connected(
        manager: &flare_core::server::connection::ConnectionManager,
        connection_id: &str,
        tenant_id: &str,
        user_id: &str,
        peer: Peer,
    ) -> Arc<std::sync::Mutex<Vec<Vec<u8>>>> {
        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
        manager
            .add_connection(
                connection_id.into(),
                Box::new(Recording {
                    sent: sent.clone(),
                    last_active: flare_core::common::platform::monotonic_now(),
                    peer,
                }),
                Some(user_id.into()),
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
                Some(user_id.into()),
                Some(HashMap::from([("tenant_id".into(), tenant_id.into())])),
            )
            .unwrap();
        ConnectionManagerTrait::set_connection_authenticated(
            manager,
            connection_id,
            Some(user_id.into()),
        )
        .await
        .unwrap();
        sent
    }

    fn tenant_kick(user_id: &str) -> KickSignal {
        KickSignal {
            tenant_id: "tenant-a".into(),
            user_id: user_id.into(),
            device_id: None,
            connection_id: None,
            reason: Some("TENANT_UNAVAILABLE".into()),
        }
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

        let kicker = Kicker::new(KICKED_SEND_TIMEOUT);
        assert_eq!(kicker.close_matching(&manager, &signal).await, 1);

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
        assert_eq!(kicker.close_matching(&manager, &legacy).await, 1);
        assert_eq!(
            kicked_message(&spared.lock().unwrap()[0]),
            DEFAULT_KICK_REASON
        );
    }

    /// Online 把冲突策略挤掉的同一用户其他设备报成 `DEVICE_CONFLICT`：网关原样下发给那条旧连接，
    /// flare-core 客户端认出它是结束会话的踢线（不再自动重连），新连接不受影响。多网关时新设备
    /// 所在网关的设备管理器不认识旧设备，旧设备只收到这一条。
    #[tokio::test]
    async fn device_conflict_kicks_reach_the_old_device_as_session_ending() {
        let manager = flare_core::server::connection::ConnectionManager::new();
        let old_device = authenticated(&manager, "old-pc", "tenant-a").await;
        let new_device = authenticated(&manager, "new-pc", "tenant-a").await;
        let signal = KickSignal {
            tenant_id: "tenant-a".into(),
            user_id: "u1".into(),
            device_id: None,
            connection_id: Some("old-pc".into()),
            reason: Some(flare_core::common::kick::DEVICE_CONFLICT_REASON.into()),
        };

        let kicker = Kicker::new(KICKED_SEND_TIMEOUT);
        assert_eq!(kicker.close_matching(&manager, &signal).await, 1);

        let frames = old_device.lock().unwrap().clone();
        assert_eq!(frames.len(), 1);
        let reason = kicked_message(&frames[0]);
        assert_eq!(reason, flare_core::common::kick::DEVICE_CONFLICT_REASON);
        assert!(flare_core::common::kick::ends_session(&reason));
        assert!(manager.get_connection("old-pc").is_none());
        assert!(new_device.lock().unwrap().is_empty());
        assert!(manager.get_connection("new-pc").is_some());
    }

    /// 同一信号命中的连接并发下发 KICKED：几条不读的连接一共只拖一个超时，
    /// 正常连接照样收到 KICKED，所有连接都被移除。
    #[tokio::test]
    async fn stuck_peers_do_not_serialize_one_signal() {
        const TIMEOUT: Duration = Duration::from_millis(300);
        let manager = flare_core::server::connection::ConnectionManager::new();
        for i in 0..4 {
            connected(
                &manager,
                &format!("stuck-{i}"),
                "tenant-a",
                "u1",
                Peer::Stuck,
            )
            .await;
        }
        let reader = connected(&manager, "reader", "tenant-a", "u1", Peer::Reads).await;

        let started = std::time::Instant::now();
        let closed = Kicker::new(TIMEOUT)
            .close_matching(&manager, &tenant_kick("u1"))
            .await;
        let elapsed = started.elapsed();

        assert_eq!(closed, 5);
        // 逐条下发要 4 × 300 ms
        assert!(elapsed < TIMEOUT * 3, "{elapsed:?}");
        assert_eq!(
            kicked_message(&reader.lock().unwrap()[0]),
            "TENANT_UNAVAILABLE"
        );
        for i in 0..4 {
            assert!(manager.get_connection(&format!("stuck-{i}")).is_none());
        }
        assert!(manager.get_connection("reader").is_none());
    }

    /// 信号在各自的任务里处理：前一个信号卡在不读的连接上，后一个信号照样立刻踢掉自己的连接。
    #[tokio::test]
    async fn a_stuck_signal_does_not_hold_back_the_next_one() {
        const TIMEOUT: Duration = Duration::from_secs(2);
        let manager = Arc::new(flare_core::server::connection::ConnectionManager::new());
        connected(&manager, "stuck", "tenant-a", "slow-user", Peer::Stuck).await;
        let reader = connected(&manager, "reader", "tenant-a", "u1", Peer::Reads).await;
        let dispatcher = KickDispatcher::new(manager.clone(), 8, TIMEOUT);

        let started = std::time::Instant::now();
        let stuck = dispatcher.dispatch(tenant_kick("slow-user")).await;
        let next = dispatcher.dispatch(tenant_kick("u1")).await;
        assert_eq!(next.await.unwrap(), 1);
        let elapsed = started.elapsed();

        assert!(elapsed < TIMEOUT / 2, "{elapsed:?}");
        assert!(!stuck.is_finished(), "the stuck kick is still waiting");
        assert_eq!(
            kicked_message(&reader.lock().unwrap()[0]),
            "TENANT_UNAVAILABLE"
        );
        assert!(manager.get_connection("reader").is_none());
        assert_eq!(stuck.await.unwrap(), 1, "the stuck peer is still closed");
        assert!(manager.get_connection("stuck").is_none());
    }

    /// 并发到达的重复信号（如设备踢与租户踢同时命中）不会让同一条连接收到两次 KICKED。
    #[tokio::test]
    async fn one_connection_is_kicked_once_by_concurrent_signals() {
        let manager = Arc::new(flare_core::server::connection::ConnectionManager::new());
        let slow = connected(
            &manager,
            "slow",
            "tenant-a",
            "u1",
            Peer::Slow(Duration::from_millis(200)),
        )
        .await;
        let dispatcher = KickDispatcher::new(manager.clone(), 8, KICKED_SEND_TIMEOUT);

        let first = dispatcher.dispatch(tenant_kick("u1")).await;
        let second = dispatcher.dispatch(tenant_kick("u1")).await;
        let closed = first.await.unwrap() + second.await.unwrap();

        assert_eq!(closed, 1);
        assert_eq!(slow.lock().unwrap().len(), 1, "exactly one KICKED frame");
        assert!(manager.get_connection("slow").is_none());
    }

    /// 名额用满时后来的信号等前面的做完，不会无界地起任务。
    #[tokio::test]
    async fn dispatch_waits_for_a_free_slot() {
        let manager = Arc::new(flare_core::server::connection::ConnectionManager::new());
        connected(&manager, "stuck", "tenant-a", "slow-user", Peer::Stuck).await;
        let dispatcher = KickDispatcher::new(manager.clone(), 1, Duration::from_millis(300));

        let stuck = dispatcher.dispatch(tenant_kick("slow-user")).await;
        let started = std::time::Instant::now();
        let next = dispatcher.dispatch(tenant_kick("nobody")).await;
        assert!(started.elapsed() >= Duration::from_millis(200));
        assert!(stuck.is_finished());
        assert_eq!(next.await.unwrap(), 0);
    }

    /// 名额池被关闭时不 panic：信号改在订阅循环里串行执行，连接照样收到 KICKED 并被移除，
    /// 后一个信号等前一个做完，不会无界地起任务。
    #[tokio::test]
    async fn a_closed_limiter_still_kicks_one_signal_at_a_time() {
        const TIMEOUT: Duration = Duration::from_millis(300);
        let manager = Arc::new(flare_core::server::connection::ConnectionManager::new());
        connected(&manager, "stuck", "tenant-a", "slow-user", Peer::Stuck).await;
        let reader = connected(&manager, "reader", "tenant-a", "u1", Peer::Reads).await;
        let dispatcher = KickDispatcher::new(manager.clone(), 8, TIMEOUT);
        dispatcher.permits.close();

        let started = std::time::Instant::now();
        let stuck = dispatcher.dispatch(tenant_kick("slow-user")).await;
        assert!(started.elapsed() >= TIMEOUT, "ran inline before returning");
        assert_eq!(stuck.await.unwrap(), 1);
        assert!(manager.get_connection("stuck").is_none());

        assert_eq!(
            dispatcher.dispatch(tenant_kick("u1")).await.await.unwrap(),
            1
        );
        assert_eq!(
            kicked_message(&reader.lock().unwrap()[0]),
            "TENANT_UNAVAILABLE"
        );
        assert!(manager.get_connection("reader").is_none());
    }
}
