//! 推送领域服务
//!
//! 包含推送相关的核心业务逻辑，仅依赖 ConnectionQuery（读连接）与 IPushPort（写推送）。

use std::sync::Arc;

use flare_grpc_proto::access_gateway::PushOptions;
use flare_im_contracts::Ctx;
use flare_proto::common::{Event, EventEnvelope, EventEnvelopeDeliveryMode, Message, MessagePush};
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use futures::{StreamExt, stream};
use prost::Message as ProstMessage;
use tracing::{info, instrument};

use crate::domain::model::{ConnectionInfo, DomainPushResult};
use crate::domain::ports::{ConnectionQuery, IPushPort};

const USER_PUSH_FANOUT_CONCURRENCY: usize = 64;

/// 默认"已 bootstrap 会话"缓存容量上限。长跑网关会服务大量不同会话，无界缓存会缓慢泄漏 → 终致 OOM。
const DEFAULT_RESOLVED_CONVERSATIONS_CAPACITY: usize = 100_000;

/// 有界的"已 bootstrap 会话"集合：FIFO 淘汰最旧条目，封顶内存。被淘汰的（冷）会话下次投递会**幂等重 bootstrap**
/// （`join` 幂等、`list_participants` 只读重取），故淘汰仅是极少的重做成本，不影响正确性。
/// 会话参与者缓存（LRU 有界）。缓存 `conversation_id → 参与者 user_id 列表`，
/// 使投递路径跳过 `list_participants` RPC，但**订阅仍每次执行**——
/// 后上线的成员连接（多端/重连/多实例）据此幂等补订阅，避免"首投递时已在线的
/// 连接被订阅、之后上线的连接永不订阅"导致实时下行漏送。
struct ResolvedConversations {
    order: std::collections::VecDeque<String>,
    participants: std::collections::HashMap<String, std::sync::Arc<Vec<String>>>,
    capacity: usize,
}

impl ResolvedConversations {
    fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            order: std::collections::VecDeque::with_capacity(capacity.min(1024)),
            participants: std::collections::HashMap::with_capacity(capacity.min(1024)),
            capacity,
        }
    }

    fn get(&self, conversation_id: &str) -> Option<std::sync::Arc<Vec<String>>> {
        self.participants.get(conversation_id).cloned()
    }

    fn insert(&mut self, conversation_id: String, participants: Vec<String>) {
        if !self.participants.contains_key(&conversation_id) {
            self.order.push_back(conversation_id.clone());
            while self.order.len() > self.capacity {
                if let Some(evicted) = self.order.pop_front() {
                    self.participants.remove(&evicted);
                }
            }
        }
        self.participants
            .insert(conversation_id, std::sync::Arc::new(participants));
    }
}

/// 推送领域服务
pub struct PushDomainService {
    push_port: Arc<dyn IPushPort>,
    connection_query: Arc<dyn ConnectionQuery>,
    /// 会话级在线订阅注册表（统一读扩散地基）：会话 publish 命中本节点时扇给本节点订阅连接。
    conversation_subscriptions: Arc<super::ConversationSubscriptionRegistry>,
    /// Conversation 读池：首次投递某会话时解析参与者，订阅本节点在线成员（确定性 bootstrap）。
    conversation_read: Arc<crate::infrastructure::ports::ConversationReadGrpcPool>,
    /// 已解析+订阅过成员的会话（每会话每网关一次成员解析，缓存避免每消息查成员）。**有界 FIFO**，防长跑泄漏。
    resolved_conversations: std::sync::RwLock<ResolvedConversations>,
    /// 大群上次全量补订阅的时刻。仅对超过 LARGE_GROUP_THRESHOLD 的会话记录，
    /// 用于把 O(成员数) 的遍历从「每条消息」降到「每 30 秒一次」。
    large_group_last_sweep:
        std::sync::RwLock<std::collections::HashMap<String, std::time::Instant>>,
    /// 推送投递指标。这一组以前只有声明没有写入路径，见 record_push_result 的说明。
    metrics: Arc<flare_im_service_kit::metrics::AccessGatewayMetrics>,
}

pub struct EventEnvelopePushRequest<'a> {
    pub user_ids: &'a [String],
    pub events: Vec<Event>,
    pub options: &'a PushOptions,
    pub window_id: &'a str,
    pub conversation_id: &'a str,
    pub max_conversation_seq: u64,
    pub delivery_mode: i32,
    pub inline_events_truncated: bool,
}

impl PushDomainService {
    pub fn new(
        push_port: Arc<dyn IPushPort>,
        connection_query: Arc<dyn ConnectionQuery>,
        conversation_subscriptions: Arc<super::ConversationSubscriptionRegistry>,
        conversation_read: Arc<crate::infrastructure::ports::ConversationReadGrpcPool>,
        metrics: Arc<flare_im_service_kit::metrics::AccessGatewayMetrics>,
    ) -> Self {
        Self {
            push_port,
            connection_query,
            conversation_subscriptions,
            conversation_read,
            metrics,
            large_group_last_sweep: std::sync::RwLock::new(std::collections::HashMap::new()),
            resolved_conversations: std::sync::RwLock::new(ResolvedConversations::new(
                std::env::var("GATEWAY_RESOLVED_CONVERSATIONS_CAPACITY")
                    .ok()
                    .and_then(|v| v.parse::<usize>().ok())
                    .unwrap_or(DEFAULT_RESOLVED_CONVERSATIONS_CAPACITY),
            )),
        }
    }

    /// 确定性 bootstrap：首次投递某会话时解析参与者，订阅其在**本节点**的在线连接。
    /// 每次投递前确保参与者的**当前在线连接**都已订阅（join 幂等）。
    /// 参与者列表每会话每网关仅解析一次（缓存跳过 `list_participants` RPC），
    /// 但订阅每次执行——覆盖"会话在连接后创建"以及"成员连接在首投递后才上线
    ///（多端登录/断线重连/客户端多实例）"的订阅时序漏洞。best-effort。
    async fn ensure_conversation_members_subscribed(&self, tx: &Ctx, conversation_id: &str) {
        let cached = self
            .resolved_conversations
            .read()
            .ok()
            .and_then(|cache| cache.get(conversation_id));
        let participants = match cached {
            Some(participants) => participants,
            None => {
                let participants = match self
                    .conversation_read
                    .list_participants(tx, conversation_id)
                    .await
                {
                    Ok(users) => users,
                    Err(error) => {
                        tracing::warn!(%conversation_id, ?error, "ensure members: list participants failed");
                        return;
                    }
                };
                let participants = std::sync::Arc::new(participants);
                if let Ok(mut cache) = self.resolved_conversations.write() {
                    cache.insert(conversation_id.to_string(), participants.as_ref().clone());
                }
                participants
            }
        };
        // 这个循环**每条消息都会对全部参与者跑一遍**（订阅每次执行是有意的：
        // 覆盖「成员在首次投递后才上线」的时序漏洞，见上面的注释）。
        // 所以它的代价直接乘在群规模上——万人群就是每条消息一万次查询。
        //
        // 两处优化，都不改变订阅结果、因此不影响送达可靠性：
        //
        // 1. 用 `list_user_connection_ids` 而不是 `list_user_connections`：
        //    后者会为每个连接额外 `get_connection().await` 组装设备/平台字段，
        //    而这里只用 connection_id，那些字段拿到就丢。
        // 2. 并发查询而不是串行 await：`join` 是幂等的，并发进入安全。
        //    并发度设上限而不是无界，避免大群瞬间打出上万个并发任务
        //    反而拖慢整体（无界并发在这里是「看起来更快」的陷阱）。
        // 大群节流：成员数超过这个阈值时，不再每条消息都全量遍历成员。
        //
        // 全量遍历每次执行是有意的——它覆盖「首投递后才上线的连接」（多端/重连/
        // 多实例），跳过会漏送。但代价是 O(成员数) 次在线查询，**每条消息一次**：
        // 500 人群无所谓，10 万人群就是每条消息 10 万次查询，按 64 并发要跑 1562 轮。
        //
        // 折中：小群保持原语义（成本可忽略，实时性最强）；大群按时间节流，
        // 窗口内复用上次的订阅结果。漏送风险由两条兜底覆盖——
        // 连接建立时的 eager subscribe 会订阅该用户全部会话，
        // 且离线成员本就靠版本号增量拉补齐。
        const LARGE_GROUP_THRESHOLD: usize = 2_000;
        const LARGE_GROUP_RESUBSCRIBE_INTERVAL: std::time::Duration =
            std::time::Duration::from_secs(30);

        if participants.len() >= LARGE_GROUP_THRESHOLD {
            let now = std::time::Instant::now();
            let skip = self
                .large_group_last_sweep
                .read()
                .ok()
                .and_then(|m| m.get(conversation_id).copied())
                .is_some_and(|last| now.duration_since(last) < LARGE_GROUP_RESUBSCRIBE_INTERVAL);
            if skip {
                tracing::debug!(
                    conversation_id = %conversation_id,
                    participants = participants.len(),
                    "push: 大群订阅节流，复用上次结果"
                );
                return;
            }
            if let Ok(mut m) = self.large_group_last_sweep.write() {
                m.insert(conversation_id.to_string(), now);
            }
        }

        const SUBSCRIBE_LOOKUP_CONCURRENCY: usize = 64;
        // 按固定大小分块、块内 join_all 并发，块间串行。
        //
        // 没用 `buffer_unordered`：这个函数被 gRPC handler 间接持有，
        // 闭包同时借用 `&self` 与 `conversation_id` 时编译器推不出足够通用的
        // 高阶生命周期（"implementation of FnOnce is not general enough"）。
        // 分块写法等价、且并发上限一样明确，就不为了写法优雅去跟生命周期缠。
        //
        // 上限而非无界：大群下无界并发会瞬间铺开上万个任务，
        // 调度开销反而吃掉收益——那是「看起来更快」的陷阱。
        let query = &self.connection_query;
        let mut joined_total = 0usize;
        for chunk in participants.chunks(SUBSCRIBE_LOOKUP_CONCURRENCY) {
            let looked_up = futures::future::join_all(
                chunk
                    .iter()
                    .map(|user_id| query.list_user_connection_ids(user_id)),
            )
            .await;
            for connection_ids in looked_up.into_iter().flatten() {
                for connection_id in connection_ids {
                    self.conversation_subscriptions
                        .join(conversation_id, &connection_id);
                    joined_total += 1;
                }
            }
        }
        // 这条日志是排查「消息发出去了但某人收不到」的第一落点：
        // participants 是该会话的成员数，joined 是本节点为他们补上的订阅数。
        // joined=0 而 participants>0 有两种解释——成员都在别的网关节点，
        // 或者成员确实全部离线；配合后面那条「跳过投递 / 投递完成」就能定位。
        tracing::debug!(
            conversation_id = %conversation_id,
            participants = participants.len(),
            joined = joined_total,
            "push: 会话成员订阅补齐"
        );
    }

    /// 统一读扩散投递：把已编码载荷扇给**本节点**订阅该会话的在线连接。
    /// 复杂度 O(本节点在线成员)，与群总人数无关；无本地订阅者直接跳过（返回 (0,0)）。
    /// 跨节点由"会话 publish 命中所有有订阅者的节点"达成（上层 MQ 主题广播，F2 接入）。
    pub async fn deliver_to_conversation(
        &self,
        tx: &Ctx,
        conversation_id: &str,
        payload_type: i32,
        payload: &[u8],
    ) -> Result<(i32, i32)> {
        // 确定性 bootstrap：首次投递该会话时解析参与者并订阅本节点在线成员（缓存，每会话一次）。
        self.ensure_conversation_members_subscribed(tx, conversation_id)
            .await;
        let connection_ids = self
            .conversation_subscriptions
            .local_subscribers(conversation_id);
        if connection_ids.is_empty() {
            // 本节点没有订阅该会话的在线连接。这不是错误（成员可能在别的网关节点，
            // 或全部离线），但**必须可见**：投递计数为 0 时要能一眼分清
            // "没有订阅者"和"发出去了但全失败"，否则排查只能靠猜。
            tracing::info!(
                conversation_id = %conversation_id,
                "push: 本节点无该会话的在线订阅者，跳过投递"
            );
            self.metrics
                .record_push_result(tx.tenant_id().unwrap_or("0"), 0, 0);
            return Ok((0, 0));
        }
        // 埋点必须在**这里**：统一读扩散上线后，实时投递走的是会话级订阅广播，
        // 而不是按用户查在线连接的 push_encoded_payload_to_user。
        // 先把埋点加在后者身上，指标一直是空的——代码在、路径不通。
        let started = std::time::Instant::now();
        let tenant_id = tx.tenant_id().unwrap_or("0").to_string();
        let result = self
            .push_port
            .push_payload_to_connections(tx, &connection_ids, payload_type, payload.to_vec())
            .await;
        if let Ok((ok, fail)) = &result {
            self.metrics.record_push_result(&tenant_id, *ok, *fail);
            self.metrics
                .observe_push_latency(&tenant_id, started.elapsed().as_secs_f64());
            tracing::info!(
                conversation_id = %conversation_id,
                subscribers = connection_ids.len(),
                delivered = *ok,
                failed = *fail,
                elapsed_ms = started.elapsed().as_millis(),
                "push: 会话级投递完成"
            );
        }
        result
    }

    /// 统一读扩散投递业务消息：编码 `MessagePush`（与 [`Self::push_message_push_to_users`] 一致）→
    /// 扇给本节点订阅该会话的在线连接。返回 (成功连接数, 失败连接数)；无本地订阅者返回 (0,0)。
    pub async fn push_message_to_conversation(
        &self,
        tx: &Ctx,
        conversation_id: &str,
        messages: Vec<Message>,
    ) -> Result<(i32, i32)> {
        let push = MessagePush {
            messages,
            notifications: vec![],
        };
        let mut payload = Vec::new();
        push.encode(&mut payload).map_err(|e| {
            ErrorBuilder::new(ErrorCode::InternalError, "encode MessagePush failed")
                .details(e.to_string())
                .build_error()
        })?;
        self.deliver_to_conversation(
            tx,
            conversation_id,
            flare_core::common::protocol::payload_command::Type::Message as i32,
            &payload,
        )
        .await
    }

    /// 统一读扩散投递领域事件批：编码 `EventEnvelope`（与 [`Self::push_event_envelope_to_users`] 一致）→
    /// 扇给本节点订阅该会话的在线连接。群消息主路径（消息经 EventEnvelope 下行）。
    /// 返回 (成功连接数, 失败连接数)；无本地订阅者返回 (0,0)。
    #[allow(clippy::too_many_arguments)]
    pub async fn deliver_event_envelope_to_conversation(
        &self,
        tx: &Ctx,
        conversation_id: &str,
        events: Vec<Event>,
        window_id: &str,
        max_conversation_seq: u64,
        delivery_mode: i32,
        inline_events_truncated: bool,
    ) -> Result<(i32, i32)> {
        let envelope = Self::build_event_envelope(
            events,
            window_id,
            conversation_id,
            max_conversation_seq,
            delivery_mode,
            inline_events_truncated,
        )?;
        let mut payload = Vec::new();
        envelope.encode(&mut payload).map_err(|e| {
            ErrorBuilder::new(ErrorCode::InternalError, "encode EventEnvelope failed")
                .details(e.to_string())
                .build_error()
        })?;
        self.deliver_to_conversation(
            tx,
            conversation_id,
            flare_core::common::protocol::payload_command::Type::Message as i32,
            &payload,
        )
        .await
    }

    fn build_event_envelope(
        events: Vec<Event>,
        window_id: &str,
        conversation_id_hint: &str,
        max_conversation_seq_hint: u64,
        delivery_mode_hint: i32,
        inline_events_truncated: bool,
    ) -> Result<EventEnvelope> {
        let conversation_id = events
            .first()
            .map(|event| event.conversation_id.trim().to_string())
            .filter(|id| !id.is_empty())
            .or_else(|| {
                let trimmed = conversation_id_hint.trim();
                (!trimmed.is_empty()).then(|| trimmed.to_string())
            })
            .ok_or_else(|| {
                ErrorBuilder::new(
                    ErrorCode::InvalidParameter,
                    "EventEnvelope: conversation_id is empty",
                )
                .build_error()
            })?;
        if conversation_id.is_empty() {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "EventEnvelope: conversation_id is empty",
            )
            .build_error());
        }
        if events
            .iter()
            .any(|event| event.conversation_id.trim() != conversation_id)
        {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "EventEnvelope: mixed conversation_id values are not supported",
            )
            .build_error());
        }

        let min_conversation_seq = events.iter().map(|e| e.conversation_seq).min().unwrap_or(0);
        let max_conversation_seq = events
            .iter()
            .map(|e| e.conversation_seq)
            .max()
            .unwrap_or(0)
            .max(max_conversation_seq_hint);
        if events.is_empty() && max_conversation_seq == 0 {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "EventEnvelope: pure ping requires max_conversation_seq",
            )
            .build_error());
        }
        let delivery_mode =
            Self::resolve_event_envelope_delivery_mode(delivery_mode_hint, events.is_empty())?;
        Ok(EventEnvelope {
            events,
            max_conversation_seq,
            has_more: false,
            next_cursor: if max_conversation_seq > 0 {
                format!("evt:{max_conversation_seq}")
            } else {
                String::new()
            },
            window_id: window_id.to_string(),
            delivery_mode,
            conversation_id,
            min_conversation_seq,
            inline_events_truncated,
            attributes: Default::default(),
        })
    }

    fn resolve_event_envelope_delivery_mode(delivery_mode: i32, events_empty: bool) -> Result<i32> {
        let mode = EventEnvelopeDeliveryMode::try_from(delivery_mode)
            .unwrap_or(EventEnvelopeDeliveryMode::Unspecified);
        let resolved = match mode {
            EventEnvelopeDeliveryMode::Unspecified if events_empty => {
                EventEnvelopeDeliveryMode::Ping
            }
            EventEnvelopeDeliveryMode::Unspecified => EventEnvelopeDeliveryMode::PingWithInline,
            other => other,
        };
        if events_empty && resolved != EventEnvelopeDeliveryMode::Ping {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "EventEnvelope: pure ping must use PING delivery mode",
            )
            .build_error());
        }
        Ok(resolved as i32)
    }

    /// 检查用户是否在线
    ///
    /// Gateway 直接查询本地连接状态，不维护缓存
    /// 在线状态由 Signaling Online 服务统一管理
    #[instrument(skip(self, tx), fields(user_id = %user_id))]
    pub async fn check_user_online(&self, tx: &Ctx, user_id: &str) -> Result<bool> {
        // 直接查询本地连接状态
        let connections = self
            .connection_query
            .query_user_connections(tx, user_id)
            .await?;

        Ok(!connections.is_empty())
    }

    /// 过滤连接（根据设备ID和平台）
    pub fn filter_connections(
        &self,
        _tx: &Ctx,
        connections: &[ConnectionInfo],
        options: &PushOptions,
    ) -> Vec<ConnectionInfo> {
        connections
            .iter()
            .filter(|conn| {
                if !options.device_ids.is_empty() && !options.device_ids.contains(&conn.device_id) {
                    return false;
                }
                let platform = conn.platform.as_deref().unwrap_or("");
                if !options.platforms.is_empty() && !options.platforms.iter().any(|p| p == platform)
                {
                    return false;
                }
                true
            })
            .cloned()
            .collect()
    }

    /// 推送消息到连接（委托仓储按 connection_id 批量推送）
    #[instrument(skip(self, tx, message_bytes), fields(user_id = %user_id, connection_count = connections.len()))]
    pub async fn push_to_connections(
        &self,
        tx: &Ctx,
        user_id: &str,
        connections: &[ConnectionInfo],
        message_bytes: &[u8],
    ) -> Result<(i32, i32)> {
        let connection_ids: Vec<String> = connections
            .iter()
            .map(|c| c.connection_id.clone())
            .collect();
        let payload_type = flare_core::common::protocol::payload_command::Type::Message as i32;
        self.push_port
            .push_payload_to_connections(tx, &connection_ids, payload_type, message_bytes.to_vec())
            .await
    }

    /// 向**单连接**下行已编码载荷（gRPC/领域编排共用，统一走 [`IPushPort`]）
    #[instrument(skip(self, tx, payload), fields(connection_id = %connection_id, payload_type = %payload_type))]
    pub async fn deliver_payload_to_connection(
        &self,
        tx: &Ctx,
        connection_id: &str,
        payload_type: i32,
        payload: Vec<u8>,
    ) -> Result<()> {
        self.push_port
            .push_payload_to_connection(tx, connection_id, payload_type, payload)
            .await
    }

    /// 按 Payload 类型推送字节到指定连接列表（2=Event 3=Ack 4=Data）
    #[instrument(skip(self, tx, payload_bytes), fields(user_id = %user_id, payload_type = %payload_type))]
    pub async fn push_payload_to_connections(
        &self,
        tx: &Ctx,
        user_id: &str,
        connections: &[ConnectionInfo],
        payload_type: i32,
        payload_bytes: &[u8],
    ) -> Result<(i32, i32)> {
        let _ = user_id;
        let connection_ids: Vec<String> = connections
            .iter()
            .map(|c| c.connection_id.clone())
            .collect();
        self.push_port
            .push_payload_to_connections(tx, &connection_ids, payload_type, payload_bytes.to_vec())
            .await
    }

    /// 获取用户连接并过滤
    #[instrument(skip(self, tx), fields(user_id = %user_id))]
    pub async fn get_filtered_connections(
        &self,
        tx: &Ctx,
        user_id: &str,
        options: &PushOptions,
    ) -> Result<Vec<ConnectionInfo>> {
        let connections = self
            .connection_query
            .query_user_connections(tx, user_id)
            .await?;

        Ok(self.filter_connections(tx, &connections, options))
    }

    async fn push_encoded_payload_to_user(
        &self,
        tx: &Ctx,
        user_id: String,
        options: &PushOptions,
        payload: Arc<Vec<u8>>,
        payload_type: i32,
        log_kind: &'static str,
    ) -> Result<(String, i32, i32, i32)> {
        let started = std::time::Instant::now();
        let tenant_id = tx.tenant_id().unwrap_or("0").to_string();
        let connections = self.get_filtered_connections(tx, &user_id, options).await?;
        if connections.is_empty() {
            info!(user_id = %user_id, "push: user has no matching online connection");
            return Ok((user_id, 0, 0, 1));
        }
        let (ok, fail) = self
            .push_payload_to_connections(
                tx,
                &user_id,
                &connections,
                payload_type,
                payload.as_slice(),
            )
            .await?;
        self.metrics.record_push_result(&tenant_id, ok, fail);
        self.metrics
            .observe_push_latency(&tenant_id, started.elapsed().as_secs_f64());
        info!(
            user_id = %user_id,
            pushed = ok,
            failed = fail,
            kind = log_kind,
            "push: encoded payload delivered"
        );
        Ok((user_id, ok, fail, 0))
    }

    async fn push_encoded_payload_to_users(
        &self,
        tx: &Ctx,
        user_ids: &[String],
        options: &PushOptions,
        payload: Vec<u8>,
        payload_type: i32,
        log_kind: &'static str,
    ) -> Result<Vec<(String, i32, i32, i32)>> {
        let payload = Arc::new(payload);
        let results = stream::iter(user_ids.iter().cloned().enumerate())
            .map(|(index, user_id)| {
                let payload = Arc::clone(&payload);
                async move {
                    self.push_encoded_payload_to_user(
                        tx,
                        user_id,
                        options,
                        payload,
                        payload_type,
                        log_kind,
                    )
                    .await
                    .map(|row| (index, row))
                }
            })
            .buffer_unordered(USER_PUSH_FANOUT_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;

        let mut ordered = Vec::with_capacity(results.len());
        for result in results {
            ordered.push(result?);
        }
        ordered.sort_by_key(|(index, _)| *index);
        Ok(ordered.into_iter().map(|(_, row)| row).collect())
    }

    /// 将业务消息下行给多个用户：载荷为 `MessagePush` 编码字节（与客户端 `chatroom_client` / SDK 解码一致）。
    ///
    /// 返回 `(user_id, pushed_device_count, failed_count, offline_pending_count)` 按用户一行。
    #[instrument(skip(self, tx, messages), fields(user_count = user_ids.len(), message_count = messages.len()))]
    pub async fn push_message_push_to_users(
        &self,
        tx: &Ctx,
        user_ids: &[String],
        messages: Vec<Message>,
        options: &PushOptions,
    ) -> Result<Vec<(String, i32, i32, i32)>> {
        let push = MessagePush {
            messages,
            notifications: vec![],
        };
        let mut payload = Vec::new();
        push.encode(&mut payload).map_err(|e| {
            ErrorBuilder::new(ErrorCode::InternalError, "encode MessagePush failed")
                .details(e.to_string())
                .build_error()
        })?;

        self.push_encoded_payload_to_users(
            tx,
            user_ids,
            options,
            payload,
            flare_core::common::protocol::payload_command::Type::Message as i32,
            "message",
        )
        .await
    }

    /// 将领域事件批下行给多个用户：载荷为 `EventEnvelope` 编码字节（与客户端 SDK `ProtobufCodec::decode_server` 一致，走 `PayloadCommand::Message` 内层解码）。
    ///
    /// 返回 `(user_id, pushed_device_count, failed_count, offline_pending_count)` 与 [`Self::push_message_push_to_users`] 对齐。
    #[instrument(skip(self, tx, request), fields(user_count = request.user_ids.len(), event_count = request.events.len()))]
    pub async fn push_event_envelope_to_users(
        &self,
        tx: &Ctx,
        request: EventEnvelopePushRequest<'_>,
    ) -> Result<Vec<(String, i32, i32, i32)>> {
        let envelope = Self::build_event_envelope(
            request.events,
            request.window_id,
            request.conversation_id,
            request.max_conversation_seq,
            request.delivery_mode,
            request.inline_events_truncated,
        )?;
        let mut payload = Vec::new();
        envelope.encode(&mut payload).map_err(|e| {
            ErrorBuilder::new(ErrorCode::InternalError, "encode EventEnvelope failed")
                .details(e.to_string())
                .build_error()
        })?;

        self.push_encoded_payload_to_users(
            tx,
            request.user_ids,
            request.options,
            payload,
            flare_core::common::protocol::payload_command::Type::Message as i32,
            "event_envelope",
        )
        .await
    }

    /// 推送 ACK 字节给用户（payload 为 common::Ack encode_to_vec）
    #[instrument(skip(self, tx, ack_payload), fields(user_id = %user_id))]
    pub async fn push_ack_to_user(
        &self,
        tx: &Ctx,
        user_id: &str,
        ack_payload: Vec<u8>,
    ) -> Result<()> {
        let payload_type = flare_core::common::protocol::payload_command::Type::Ack as i32;
        self.push_port
            .push_payload_to_user(tx, user_id, payload_type, ack_payload)
            .await
    }

    /// 推送 ACK 字节给多个用户，按设备过滤并返回每个用户的下发结果。
    #[instrument(skip(self, tx, ack_payload), fields(user_count = user_ids.len()))]
    pub async fn push_ack_to_users(
        &self,
        tx: &Ctx,
        user_ids: &[String],
        ack_payload: Vec<u8>,
        options: &PushOptions,
    ) -> Result<Vec<(String, i32, i32, i32)>> {
        self.push_encoded_payload_to_users(
            tx,
            user_ids,
            options,
            ack_payload,
            flare_core::common::protocol::payload_command::Type::Ack as i32,
            "ack",
        )
        .await
    }

    /// 构建推送结果
    pub fn build_push_result(
        _tx: &Ctx,
        user_id: String,
        success_count: i32,
        failure_count: i32,
    ) -> DomainPushResult {
        DomainPushResult {
            user_id,
            success_count,
            failure_count,
            error_message: if failure_count > 0 {
                format!("Failed to push to {} connections", failure_count)
            } else {
                String::new()
            },
        }
    }

    /// 查询用户连接列表（供 GetUserConnections 使用）：按平台过滤并限制条数
    #[instrument(skip(self, _tx), fields(user_id = %user_id))]
    pub async fn list_user_connections(
        &self,
        _tx: &Ctx,
        user_id: &str,
        platforms: &[String],
        limit: i32,
    ) -> Result<Vec<ConnectionInfo>> {
        let connections = self.connection_query.list_user_connections(user_id).await?;
        let limit = limit.clamp(0, 500) as usize;
        let filtered: Vec<ConnectionInfo> = if platforms.is_empty() {
            connections
        } else {
            connections
                .into_iter()
                .filter(|c| {
                    c.platform
                        .as_ref()
                        .map(|p| platforms.iter().any(|f| f == p))
                        .unwrap_or(false)
                })
                .collect()
        };
        Ok(filtered.into_iter().take(limit).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use async_trait::async_trait;
    use std::sync::Mutex;

    #[test]
    fn resolved_conversations_evicts_oldest_when_capacity_exceeded() {
        let mut set = ResolvedConversations::new(2);
        set.insert("a".to_string(), vec!["u1".to_string()]);
        set.insert("b".to_string(), vec!["u2".to_string()]);
        assert!(set.get("a").is_some() && set.get("b").is_some());
        assert_eq!(set.get("a").unwrap().as_slice(), ["u1"]);
        set.insert("c".to_string(), vec!["u3".to_string()]); // 越界 → 淘汰最旧的 "a"
        assert!(set.get("a").is_none(), "oldest evicted");
        assert!(set.get("b").is_some() && set.get("c").is_some());
        assert_eq!(set.order.len(), 2, "memory bounded at capacity");
        // 重复插入刷新参与者但不增长、不重复入队（LRU 顺序稳定）。
        set.insert("b".to_string(), vec!["u2b".to_string()]);
        assert_eq!(set.order.len(), 2);
        assert_eq!(set.get("b").unwrap().as_slice(), ["u2b"]);
    }

    #[derive(Default)]
    struct CapturingPushPort {
        payloads: Mutex<Vec<(i32, Vec<u8>)>>,
    }

    #[async_trait]
    impl IPushPort for CapturingPushPort {
        async fn push_message_to_user(
            &self,
            _tx: &Ctx,
            _user_id: &str,
            _message: Vec<u8>,
        ) -> Result<()> {
            Ok(())
        }

        async fn push_message_to_connection(
            &self,
            _tx: &Ctx,
            _connection_id: &str,
            _message: Vec<u8>,
        ) -> Result<()> {
            Ok(())
        }

        async fn push_payload_to_connection(
            &self,
            _tx: &Ctx,
            _connection_id: &str,
            payload_type: i32,
            payload: Vec<u8>,
        ) -> Result<()> {
            self.payloads
                .lock()
                .expect("payload mutex poisoned")
                .push((payload_type, payload));
            Ok(())
        }

        async fn push_payload_to_user(
            &self,
            _tx: &Ctx,
            _user_id: &str,
            payload_type: i32,
            payload: Vec<u8>,
        ) -> Result<()> {
            self.payloads
                .lock()
                .expect("payload mutex poisoned")
                .push((payload_type, payload));
            Ok(())
        }

        async fn push_payload_to_connections(
            &self,
            _tx: &Ctx,
            connection_ids: &[String],
            payload_type: i32,
            payload: Vec<u8>,
        ) -> Result<(i32, i32)> {
            self.payloads
                .lock()
                .expect("payload mutex poisoned")
                .push((payload_type, payload));
            Ok((connection_ids.len() as i32, 0))
        }
    }

    struct StaticConnectionQuery {
        connections: Vec<ConnectionInfo>,
    }

    #[async_trait]
    impl ConnectionQuery for StaticConnectionQuery {
        async fn query_user_connections(
            &self,
            _tx: &Ctx,
            _user_id: &str,
        ) -> Result<Vec<ConnectionInfo>> {
            Ok(self.connections.clone())
        }

        async fn list_user_connections(&self, _user_id: &str) -> Result<Vec<ConnectionInfo>> {
            Ok(self.connections.clone())
        }
    }

    #[tokio::test]
    async fn push_event_envelope_sets_window_and_delivery_contract() {
        let push_port = Arc::new(CapturingPushPort::default());
        let connection_query = Arc::new(StaticConnectionQuery {
            connections: vec![
                ConnectionInfo::new(
                    "conn-1".to_string(),
                    "u1".to_string(),
                    "t1".to_string(),
                    "dev-1".to_string(),
                )
                .with_platform("ios".to_string()),
            ],
        });
        let service = PushDomainService::new(
            push_port.clone(),
            connection_query,
            Arc::new(crate::domain::service::ConversationSubscriptionRegistry::new()),
            Arc::new(crate::infrastructure::ports::ConversationReadGrpcPool::new()),
            Arc::new(flare_im_service_kit::metrics::AccessGatewayMetrics::new()),
        );
        let ctx: Ctx = Arc::new(flare_server_core::Context::root());
        let user_ids = vec!["u1".to_string()];
        let event = Event {
            conversation_id: "c1".to_string(),
            conversation_seq: 42,
            ..Default::default()
        };

        let result = service
            .push_event_envelope_to_users(
                &ctx,
                EventEnvelopePushRequest {
                    user_ids: &user_ids,
                    events: vec![event],
                    options: &PushOptions::default(),
                    window_id: "window-1",
                    conversation_id: "",
                    max_conversation_seq: 0,
                    delivery_mode: 0,
                    inline_events_truncated: false,
                },
            )
            .await
            .expect("push should succeed");

        assert_eq!(result, vec![("u1".to_string(), 1, 0, 0)]);
        let payloads = push_port.payloads.lock().expect("payload mutex poisoned");
        assert_eq!(payloads.len(), 1);
        assert_eq!(
            payloads[0].0,
            flare_core::common::protocol::payload_command::Type::Message as i32
        );
        let envelope =
            EventEnvelope::decode(payloads[0].1.as_slice()).expect("decode EventEnvelope");
        assert_eq!(envelope.window_id, "window-1");
        assert_eq!(envelope.conversation_id, "c1");
        assert_eq!(envelope.min_conversation_seq, 42);
        assert_eq!(envelope.max_conversation_seq, 42);
        assert_eq!(
            envelope.delivery_mode,
            EventEnvelopeDeliveryMode::PingWithInline as i32
        );
        assert!(!envelope.inline_events_truncated);
    }

    #[test]
    fn build_event_envelope_rejects_mixed_conversation_ids() {
        let events = vec![
            Event {
                conversation_id: "c1".to_string(),
                conversation_seq: 1,
                ..Default::default()
            },
            Event {
                conversation_id: "c2".to_string(),
                conversation_seq: 2,
                ..Default::default()
            },
        ];

        let error = PushDomainService::build_event_envelope(events, "window-1", "", 0, 0, false)
            .expect_err("mixed conversations must be rejected");

        assert!(error.to_string().contains("mixed conversation_id"));
    }

    #[test]
    fn build_event_envelope_supports_pure_ping() {
        let envelope = PushDomainService::build_event_envelope(
            vec![],
            "window-1",
            "c1",
            99,
            EventEnvelopeDeliveryMode::Ping as i32,
            true,
        )
        .expect("pure ping should be valid");

        assert!(envelope.events.is_empty());
        assert_eq!(envelope.conversation_id, "c1");
        assert_eq!(envelope.max_conversation_seq, 99);
        assert_eq!(
            envelope.delivery_mode,
            EventEnvelopeDeliveryMode::Ping as i32
        );
        assert!(envelope.inline_events_truncated);
    }
}
