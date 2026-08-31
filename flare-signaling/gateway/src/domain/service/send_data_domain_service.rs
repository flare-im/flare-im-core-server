//! 数据发送领域服务
//!
//! DATA 通道载荷为 [`flare_proto::common::DataPacket`]（`common/data.proto`）：`SYNC_REQUEST` / `SYNC_RESPONSE` / `USER_CUSTOM`。

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use flare_core::common::ErrorCode;
use flare_core::common::error::{FlareError, Result};
use flare_im_contracts::Ctx;
use flare_proto::common::data_packet::Payload as DataPayload;
use flare_proto::common::realtime_control_packet::Payload as RealtimePayload;
use flare_proto::common::sync::Payload as SyncPayload;
use flare_proto::common::{CustomData, DataPacket, RealtimeControlPacket, TypingAggregatePacket};
use prost::Message;

use crate::application::commands::SendDataCommand;
use crate::domain::ports::{IDataCommandPort, IPushPort};
use crate::domain::service::{ConversationSubscriptionRegistry, SyncService};

const TYPING_EMIT_MS_ENV: &str = "FLARE_GATEWAY_TYPING_COALESCE_MS";
const DEFAULT_TYPING_EMIT_MS: u64 = 1000;
const TYPING_TTL_MS_ENV: &str = "FLARE_GATEWAY_TYPING_TTL_MS";
const DEFAULT_TYPING_TTL_MS: u64 = 6000;
const TYPING_SAMPLE_CAP: usize = 6;
const TYPING_AGG_MAX_CONVERSATIONS: usize = 65536;

#[derive(Default)]
struct ConversationTypingState {
    users: HashMap<String, Instant>,
    last_emit: Option<Instant>,
}

/// 网关 typing 聚合器（超大群"N 人正在输入"+ 风暴防护）：按会话维护当前正在输入用户集合（带 TTL），
/// 按会话节流发射一条聚合包（采样 user_ids + 总数），把"逐键"与"人数"两个维度都压成 ≤1 条/窗口/会话。
/// `typing=false`(stop) 立即发射以及时反映停止。进程内 Mutex<HashMap>；有界 + 惰性清理。窗口=0 退化为每条都发。
struct TypingAggregator {
    emit_window: Duration,
    ttl: Duration,
    state: Mutex<HashMap<String, ConversationTypingState>>,
}

impl TypingAggregator {
    fn from_env() -> Self {
        let emit_ms = std::env::var(TYPING_EMIT_MS_ENV)
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_TYPING_EMIT_MS);
        let ttl_ms = std::env::var(TYPING_TTL_MS_ENV)
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_TYPING_TTL_MS);
        Self {
            emit_window: Duration::from_millis(emit_ms),
            ttl: Duration::from_millis(ttl_ms.max(emit_ms)),
            state: Mutex::new(HashMap::new()),
        }
    }

    /// 观察一条上行 typing，更新会话集合并按节流决定是否发射聚合包。
    /// 返回 `Some(aggregate)` 表示应向会话订阅者下发该聚合；`None` 表示窗口内折叠（仅更新集合）。
    fn observe(
        &self,
        conversation_id: &str,
        user_id: &str,
        typing: bool,
    ) -> Option<TypingAggregatePacket> {
        let now = Instant::now();
        let mut guard = self.state.lock().ok()?;

        if guard.len() >= TYPING_AGG_MAX_CONVERSATIONS && !guard.contains_key(conversation_id) {
            let ttl = self.ttl;
            guard.retain(|_, st| st.users.values().any(|ts| now.duration_since(*ts) < ttl));
        }

        let ttl = self.ttl;
        let entry = guard.entry(conversation_id.to_string()).or_default();
        if typing {
            if !user_id.is_empty() {
                entry.users.insert(user_id.to_string(), now);
            }
        } else {
            entry.users.remove(user_id);
        }
        entry.users.retain(|_, ts| now.duration_since(*ts) < ttl);

        let throttled = self.emit_window.is_zero()
            || entry
                .last_emit
                .is_none_or(|last| now.duration_since(last) >= self.emit_window);
        // stop 立即发射（及时反映停止）；typing 受窗口节流。
        if typing && !throttled {
            return None;
        }
        entry.last_emit = Some(now);

        let mut user_ids: Vec<String> = entry
            .users
            .keys()
            .take(TYPING_SAMPLE_CAP)
            .cloned()
            .collect();
        user_ids.sort();
        let typing_count = entry.users.len() as u32;
        let is_empty = entry.users.is_empty();
        if is_empty {
            guard.remove(conversation_id);
        }
        Some(TypingAggregatePacket {
            conversation_id: conversation_id.to_string(),
            typing_user_ids: user_ids,
            typing_count,
            occurred_at: Some(chrono::Utc::now().timestamp_millis()),
        })
    }
}

pub struct SendDataDomainService {
    data_port: Arc<dyn IDataCommandPort>,
    sync_service: Arc<SyncService>,
    /// 轻量信令直转地基：会话级在线订阅注册表（与读扩散投递共享同一实例）。
    conversation_subscriptions: Arc<ConversationSubscriptionRegistry>,
    /// 直接向连接写 DATA 帧（typing/presence 直转，绕开 NATS/持久化/ACK）。
    push_port: Arc<dyn IPushPort>,
    /// typing 聚合器（超大群"N 人正在输入" + 风暴防护）。
    typing_aggregator: TypingAggregator,
}

impl SendDataDomainService {
    pub fn new(
        data_port: Arc<dyn IDataCommandPort>,
        sync_service: Arc<SyncService>,
        conversation_subscriptions: Arc<ConversationSubscriptionRegistry>,
        push_port: Arc<dyn IPushPort>,
    ) -> Self {
        Self {
            data_port,
            sync_service,
            conversation_subscriptions,
            push_port,
            typing_aggregator: TypingAggregator::from_env(),
        }
    }

    pub async fn execute(&self, tx: &Ctx, cmd: &SendDataCommand) -> Result<Option<Vec<u8>>> {
        match cmd.packet.payload.as_ref() {
            Some(DataPayload::SyncRequest(sync)) => {
                let sync = sync.clone();
                let sync_payload = sync_payload_name(sync.payload.as_ref());
                tracing::trace!(
                    connection_id = %cmd.connection_id,
                    sync_payload,
                    "DATA SYNC_REQUEST → forward"
                );
                // 单次 sync_request 的服务端耗时。客户端引导阶段会**串行**发十几次
                // sync_request，每次的服务端耗时直接叠加成首屏时延（线上实测响应间隔
                // p50 407ms、max 2189ms，而 TCP RTT 只有 1ms）。此前这条链路上一个
                // 耗时数字都没有，只能看到客户端在等、看不到等在哪个 sync 变体上。
                let started = Instant::now();
                let sync_res = self
                    .sync_service
                    .execute(tx, cmd.connection_id.as_str(), sync)
                    .await;
                let elapsed_ms = started.elapsed().as_millis();
                if elapsed_ms >= sync_slow_log_threshold_ms() {
                    tracing::info!(
                        connection_id = %cmd.connection_id,
                        sync_payload,
                        elapsed_ms = elapsed_ms as u64,
                        ok = sync_res.is_ok(),
                        "slow sync_request"
                    );
                }
                let sync_res = sync_res?;
                let out = DataPacket {
                    payload: Some(DataPayload::SyncResponse(sync_res)),
                };
                Ok(Some(out.encode_to_vec()))
            }
            Some(DataPayload::UserCustom(data)) => self.forward_user_custom(tx, data).await,
            Some(DataPayload::SyncResponse(_)) => Err(FlareError::localized(
                ErrorCode::MessageFormatError,
                "uplink DataPacket must not use sync_response",
            )),
            Some(DataPayload::Capability(_)) => Ok(None),
            Some(DataPayload::RealtimeControl(rc)) => {
                // 轻量信令（typing/presence/已读光标）：网关在会话在线订阅集合内**直转**，
                // 绝不入 NATS/持久化/ACK；丢失可接受、最新覆盖旧。复杂度 O(本节点在线/会话)。
                self.relay_realtime_control(tx, &cmd.connection_id, rc, &cmd.packet)
                    .await;
                Ok(None)
            }
            None => Err(FlareError::localized(
                ErrorCode::MessageFormatError,
                "DataPacket.payload is required",
            )),
        }
    }

    /// 在会话在线订阅集合内直转轻量信令。
    /// - typing：经 [`TypingAggregator`] 聚合为"N 人正在输入"聚合包，按会话节流后下发给**全部**在线订阅者
    ///   （含正在输入者自身，客户端按自身 user_id 过滤显示）；窗口内折叠则不发。
    /// - presence/custom：原样直转给**其他**在线订阅者（排除发送方）。
    ///
    /// 不调 `ensure_conversation_members_subscribed`（高频，依赖消息活动已建立的订阅；
    /// 未订阅者收不到属有损语义）。
    async fn relay_realtime_control(
        &self,
        tx: &Ctx,
        sender_connection_id: &str,
        rc: &RealtimeControlPacket,
        packet: &DataPacket,
    ) {
        let conversation_id = Self::realtime_conversation_id(rc);
        if conversation_id.is_empty() {
            return;
        }

        let (relay_payload, include_sender) = match rc.payload.as_ref() {
            Some(RealtimePayload::Typing(typing)) => {
                // 超大群"N 人正在输入"聚合 + 风暴防护：逐键与人数两维都压成 ≤1 条/窗口/会话。
                let Some(aggregate) = self.typing_aggregator.observe(
                    &conversation_id,
                    &typing.user_id,
                    typing.typing,
                ) else {
                    return; // 窗口内折叠
                };
                let aggregate_packet = DataPacket {
                    payload: Some(DataPayload::RealtimeControl(RealtimeControlPacket {
                        control_type: "typing_aggregate".to_string(),
                        conversation_id: Some(conversation_id.clone()),
                        correlation_id: None,
                        attributes: Default::default(),
                        payload: Some(RealtimePayload::TypingAggregate(aggregate)),
                    })),
                };
                (aggregate_packet.encode_to_vec(), true)
            }
            _ => (packet.encode_to_vec(), false),
        };

        let targets: Vec<String> = self
            .conversation_subscriptions
            .local_subscribers(&conversation_id)
            .into_iter()
            .filter(|connection_id| include_sender || connection_id != sender_connection_id)
            .collect();
        if targets.is_empty() {
            return;
        }
        let payload_type = flare_core::common::protocol::payload_command::Type::Data as i32;
        if let Err(error) = self
            .push_port
            .push_payload_to_connections(tx, &targets, payload_type, relay_payload)
            .await
        {
            // 有损 ephemeral：失败仅 trace，不回报发送方。
            tracing::trace!(%conversation_id, ?error, "realtime control relay push failed (ignored)");
        }
    }

    /// 解析轻量信令的会话 ID：优先 RealtimeControlPacket.conversation_id，回退 typing 内层 conversation_id。
    fn realtime_conversation_id(rc: &RealtimeControlPacket) -> String {
        if let Some(conversation_id) = rc
            .conversation_id
            .as_ref()
            .map(|id| id.trim())
            .filter(|id| !id.is_empty())
        {
            return conversation_id.to_string();
        }
        match rc.payload.as_ref() {
            Some(RealtimePayload::Typing(typing)) => typing.conversation_id.trim().to_string(),
            _ => String::new(),
        }
    }

    async fn forward_user_custom(&self, tx: &Ctx, data: &CustomData) -> Result<Option<Vec<u8>>> {
        let opt = self
            .data_port
            .send_data(tx, data.clone())
            .await
            .map_err(|e| FlareError::system(format!("send_data failed: {e}")))?;
        let Some(raw) = opt else {
            return Ok(None);
        };
        let custom = CustomData::decode(raw.as_slice()).unwrap_or_else(|_| CustomData {
            r#type: "binary".to_string(),
            payload: raw,
            attributes: Default::default(),
        });
        let reply = DataPacket {
            payload: Some(DataPayload::UserCustom(custom)),
        };
        Ok(Some(reply.encode_to_vec()))
    }
}

/// 慢 `sync_request` 日志阈值（毫秒），env `FLARE_SYNC_SLOW_LOG_MS`，默认 100。
/// 设为 0 则每次都记（排障用，生产别开）。
fn sync_slow_log_threshold_ms() -> u128 {
    static VALUE: OnceLock<u128> = OnceLock::new();
    *VALUE.get_or_init(|| {
        parse_slow_log_ms(std::env::var("FLARE_SYNC_SLOW_LOG_MS").ok().as_deref())
    })
}

/// 拆成纯函数是为了能测：非法值必须回落到默认阈值，而不是变成 0
/// （0 等于给每条 sync_request 都写一行日志，生产上会把真正的错误淹掉）。
fn parse_slow_log_ms(raw: Option<&str>) -> u128 {
    const DEFAULT_MS: u128 = 100;
    match raw {
        Some(v) => v.trim().parse::<u128>().unwrap_or(DEFAULT_MS),
        None => DEFAULT_MS,
    }
}

fn sync_payload_name(payload: Option<&SyncPayload>) -> &'static str {
    match payload {
        Some(SyncPayload::SingleConversation(_)) => "single_conversation",
        Some(SyncPayload::MultiConversation(_)) => "multi_conversation",
        Some(SyncPayload::ConversationsIncremental(_)) => "conversations_incremental",
        Some(SyncPayload::ConversationsAll(_)) => "conversations_all",
        Some(SyncPayload::ConversationDetail(_)) => "conversation_detail",
        Some(SyncPayload::QueryEvents(_)) => "query_events",
        Some(SyncPayload::GetSyncCursor(_)) => "get_sync_cursor",
        Some(SyncPayload::UpdateSyncCursor(_)) => "update_sync_cursor",
        Some(SyncPayload::EventStreamAck(_)) => "event_stream_ack",
        Some(SyncPayload::SyncSnapshot(_)) => "sync_snapshot",
        Some(SyncPayload::ConversationMaxSeq(_)) => "conversation_max_seq",
        Some(SyncPayload::Conversations(_)) => "conversations",
        Some(SyncPayload::ConversationParticipants(_)) => "conversation_participants",
        Some(SyncPayload::ConversationUserSettings(_)) => "conversation_user_settings",
        Some(SyncPayload::EnsureConversation(_)) => "ensure_conversation",
        None => "none",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slow_log_threshold_defaults_and_rejects_garbage() {
        assert_eq!(parse_slow_log_ms(None), 100);
        assert_eq!(parse_slow_log_ms(Some("250")), 250);
        assert_eq!(parse_slow_log_ms(Some(" 250 ")), 250);
        // 0 是合法的显式取值（排障时全量记录）
        assert_eq!(parse_slow_log_ms(Some("0")), 0);
        // 非法值必须回落到默认，绝不能变成 0：那会给每条 sync_request 写一行日志
        assert_eq!(parse_slow_log_ms(Some("abc")), 100);
        assert_eq!(parse_slow_log_ms(Some("")), 100);
        assert_eq!(parse_slow_log_ms(Some("-5")), 100);
    }

    use crate::domain::ports::{IPushPort, ISyncPort};
    use async_trait::async_trait;
    use flare_proto::common::{Sync as ClientSync, SyncRes, TypingStatePacket};
    use std::sync::Mutex;

    /// 一次推送调用的捕获记录：(接收者 user_ids, 事件类型, 负载)。
    type PushCall = (Vec<String>, i32, Vec<u8>);

    #[derive(Default)]
    struct CapturingPushPort {
        calls: Mutex<Vec<PushCall>>,
    }

    type PushResult<T> = flare_server_core::error::Result<T>;

    #[async_trait]
    impl IPushPort for CapturingPushPort {
        async fn push_message_to_user(&self, _: &Ctx, _: &str, _: Vec<u8>) -> PushResult<()> {
            Ok(())
        }
        async fn push_message_to_connection(&self, _: &Ctx, _: &str, _: Vec<u8>) -> PushResult<()> {
            Ok(())
        }
        async fn push_payload_to_connection(
            &self,
            _: &Ctx,
            _: &str,
            _: i32,
            _: Vec<u8>,
        ) -> PushResult<()> {
            Ok(())
        }
        async fn push_payload_to_user(
            &self,
            _: &Ctx,
            _: &str,
            _: i32,
            _: Vec<u8>,
        ) -> PushResult<()> {
            Ok(())
        }
        async fn push_payload_to_connections(
            &self,
            _: &Ctx,
            connection_ids: &[String],
            payload_type: i32,
            payload: Vec<u8>,
        ) -> PushResult<(i32, i32)> {
            self.calls.lock().expect("calls mutex poisoned").push((
                connection_ids.to_vec(),
                payload_type,
                payload,
            ));
            Ok((connection_ids.len() as i32, 0))
        }
    }

    struct NoopDataPort;
    #[async_trait]
    impl IDataCommandPort for NoopDataPort {
        async fn send_data(
            &self,
            _: &Ctx,
            _: CustomData,
        ) -> flare_server_core::error::Result<Option<Vec<u8>>> {
            Ok(None)
        }
    }

    struct NoopSyncPort;
    #[async_trait]
    impl ISyncPort for NoopSyncPort {
        async fn forward_sync(
            &self,
            _: &Ctx,
            _: ClientSync,
        ) -> flare_server_core::error::Result<SyncRes> {
            unreachable!("sync not exercised in typing relay test")
        }
    }

    fn typing_packet(conversation_id: &str, user_id: &str, typing: bool) -> DataPacket {
        DataPacket {
            payload: Some(DataPayload::RealtimeControl(RealtimeControlPacket {
                control_type: "typing".to_string(),
                conversation_id: Some(conversation_id.to_string()),
                correlation_id: None,
                attributes: Default::default(),
                payload: Some(RealtimePayload::Typing(TypingStatePacket {
                    conversation_id: conversation_id.to_string(),
                    user_id: user_id.to_string(),
                    typing,
                    device_id: None,
                    occurred_at: None,
                })),
            })),
        }
    }

    #[tokio::test]
    async fn typing_emits_aggregate_to_all_subscribers() {
        let registry = Arc::new(ConversationSubscriptionRegistry::new());
        registry.join("conv-1", "conn-sender");
        registry.join("conv-1", "conn-b");
        registry.join("conv-1", "conn-c");

        let push = Arc::new(CapturingPushPort::default());
        let service = SendDataDomainService::new(
            Arc::new(NoopDataPort),
            Arc::new(SyncService::new(Arc::new(NoopSyncPort))),
            registry,
            push.clone(),
        );

        let cmd = SendDataCommand::new(
            "conn-sender".to_string(),
            typing_packet("conv-1", "u-sender", true),
            0,
        );
        let ctx: Ctx = Arc::new(flare_server_core::Context::root());

        let reply = service.execute(&ctx, &cmd).await.expect("relay ok");
        assert!(
            reply.is_none(),
            "ephemeral typing must not produce a DATA reply"
        );

        let calls = push.calls.lock().expect("calls mutex poisoned");
        assert_eq!(calls.len(), 1, "exactly one aggregate relay push");
        let (mut targets, payload_type, payload) = calls[0].clone();
        targets.sort();
        assert_eq!(
            targets,
            vec![
                "conn-b".to_string(),
                "conn-c".to_string(),
                "conn-sender".to_string()
            ],
            "aggregate relayed to all online subscribers (client filters self)"
        );
        assert_eq!(
            payload_type,
            flare_core::common::protocol::payload_command::Type::Data as i32,
            "typing aggregate relayed on DATA channel"
        );
        let decoded = DataPacket::decode(payload.as_slice()).expect("decode DataPacket");
        let Some(DataPayload::RealtimeControl(rc)) = decoded.payload else {
            panic!("expected realtime_control");
        };
        let Some(RealtimePayload::TypingAggregate(agg)) = rc.payload else {
            panic!("expected typing_aggregate payload");
        };
        assert_eq!(agg.conversation_id, "conv-1");
        assert_eq!(agg.typing_count, 1);
        assert_eq!(agg.typing_user_ids, vec!["u-sender".to_string()]);
    }

    #[test]
    fn typing_aggregator_folds_within_window_and_emits_stop_immediately() {
        let aggregator = TypingAggregator {
            emit_window: Duration::from_millis(1000),
            ttl: Duration::from_millis(6000),
            state: Mutex::new(HashMap::new()),
        };
        // 首条 typing → 发射，集合 {u1}
        let first = aggregator.observe("c", "u1", true).expect("first emits");
        assert_eq!(first.typing_count, 1);
        assert_eq!(first.typing_user_ids, vec!["u1".to_string()]);
        // 窗口内第二个用户 typing → 折叠（不发），但集合更新为 {u1,u2}
        assert!(
            aggregator.observe("c", "u2", true).is_none(),
            "second typing within window is folded"
        );
        // stop 立即发射，反映当前集合（u2 移除后 = {u1}）
        let stop = aggregator.observe("c", "u2", false).expect("stop emits");
        assert_eq!(stop.typing_count, 1);
        assert_eq!(stop.typing_user_ids, vec!["u1".to_string()]);
        // 不同会话独立、立即发射
        assert!(aggregator.observe("c2", "u1", true).is_some());
    }

    #[test]
    fn typing_aggregator_window_zero_emits_every_event() {
        let aggregator = TypingAggregator {
            emit_window: Duration::ZERO,
            ttl: Duration::from_millis(6000),
            state: Mutex::new(HashMap::new()),
        };
        assert!(aggregator.observe("c", "u1", true).is_some());
        assert!(
            aggregator.observe("c", "u2", true).is_some(),
            "window=0 emits on every event"
        );
    }

    #[tokio::test]
    async fn typing_with_no_other_subscribers_is_noop() {
        let registry = Arc::new(ConversationSubscriptionRegistry::new());
        registry.join("conv-1", "conn-sender");
        let push = Arc::new(CapturingPushPort::default());
        let service = SendDataDomainService::new(
            Arc::new(NoopDataPort),
            Arc::new(SyncService::new(Arc::new(NoopSyncPort))),
            registry,
            push.clone(),
        );
        let packet = DataPacket {
            payload: Some(DataPayload::RealtimeControl(RealtimeControlPacket {
                control_type: "typing".to_string(),
                conversation_id: Some("conv-1".to_string()),
                correlation_id: None,
                attributes: Default::default(),
                payload: None,
            })),
        };
        let cmd = SendDataCommand::new("conn-sender".to_string(), packet, 0);
        let ctx: Ctx = Arc::new(flare_server_core::Context::root());
        service.execute(&ctx, &cmd).await.expect("noop ok");
        assert!(
            push.calls.lock().expect("calls mutex poisoned").is_empty(),
            "no relay when sender is the only subscriber"
        );
    }
}
