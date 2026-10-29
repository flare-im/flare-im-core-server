//! 推送领域服务
//!
//! 包含推送相关的核心业务逻辑，仅依赖 ConnectionQuery（读连接）与 IPushPort（写推送）。

use std::sync::Arc;

use flare_grpc_proto::access_gateway::PushOptions;
use flare_im_contracts::Ctx;
use flare_proto::common::{
    CapabilityPacket, DataPacket, Event, EventEnvelope, EventEnvelopeDeliveryMode, Message,
    MessagePush, data_packet,
};
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use futures::{StreamExt, stream};
use prost::Message as ProstMessage;
use tracing::{info, instrument};

use crate::domain::model::{ConnectionInfo, DomainPushResult};
use crate::domain::ports::{ConnectionQuery, ConversationParticipantSource, IPushPort};

const USER_PUSH_FANOUT_CONCURRENCY: usize = 64;

/// 默认"已 bootstrap 会话"缓存容量上限。长跑网关会服务大量不同会话，无界缓存会缓慢泄漏 → 终致 OOM。
const DEFAULT_RESOLVED_CONVERSATIONS_CAPACITY: usize = 100_000;

/// 已确认成员快照的内存复用有效期，不是授权有效期。每次广播仍读取权威当前成员；
/// 只有内容与本次权威结果完全相同才复用 Arc，不能用 TTL 延迟撤权。
const DEFAULT_PARTICIPANTS_TTL: std::time::Duration = std::time::Duration::from_secs(10);
/// 大群已确认快照的内存复用有效期；仍须每次分页读取全部权威成员。
const DEFAULT_LARGE_GROUP_PARTICIPANTS_TTL: std::time::Duration =
    std::time::Duration::from_secs(60);

/// 大群快照内存复用的阈值，不允许跳过本次授权或按旧订阅广播。
const LARGE_GROUP_THRESHOLD: usize = 2_000;

fn env_secs(name: &str, default: std::time::Duration) -> std::time::Duration {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(std::time::Duration::from_secs)
        .unwrap_or(default)
}

/// 有界的"已 bootstrap 会话"集合：FIFO 淘汰最旧条目，封顶内存。被淘汰的（冷）会话下次投递会**幂等重 bootstrap**
/// （`join` 幂等、`list_participants` 只读重取），故淘汰仅是极少的重做成本，不影响正确性。
/// 会话参与者快照（FIFO 有界），仅供与新权威结果完全相同时复用分配。
/// 不跳过成员 RPC，也不把曾经订阅过解释为现在有读取权限。
type ConversationCacheKey = (String, String);

struct ResolvedConversations {
    order: std::collections::VecDeque<ConversationCacheKey>,
    participants: std::collections::HashMap<
        ConversationCacheKey,
        (std::sync::Arc<Vec<String>>, std::time::Instant),
    >,
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

    #[cfg(test)]
    fn get(&self, tenant_id: &str, conversation_id: &str) -> Option<std::sync::Arc<Vec<String>>> {
        self.participants
            .get(&(tenant_id.to_string(), conversation_id.to_string()))
            .map(|(participants, _)| participants.clone())
    }

    /// 仍在有效期内的名单；过期或没有则 `None`（调用方重新解析）。有效期按名单大小取。
    fn get_fresh(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        now: std::time::Instant,
        ttl: std::time::Duration,
        large_group_ttl: std::time::Duration,
    ) -> Option<std::sync::Arc<Vec<String>>> {
        let (participants, resolved_at) = self
            .participants
            .get(&(tenant_id.to_string(), conversation_id.to_string()))?;
        let ttl = if participants.len() >= LARGE_GROUP_THRESHOLD {
            large_group_ttl
        } else {
            ttl
        };
        (now.saturating_duration_since(*resolved_at) < ttl).then(|| participants.clone())
    }

    fn insert(&mut self, tenant_id: String, conversation_id: String, participants: Vec<String>) {
        let key = (tenant_id, conversation_id);
        if !self.participants.contains_key(&key) {
            self.order.push_back(key.clone());
            while self.order.len() > self.capacity {
                if let Some(evicted) = self.order.pop_front() {
                    self.participants.remove(&evicted);
                }
            }
        }
        self.participants.insert(
            key,
            (std::sync::Arc::new(participants), std::time::Instant::now()),
        );
    }
}

/// 推送领域服务
pub struct PushDomainService {
    push_port: Arc<dyn IPushPort>,
    connection_query: Arc<dyn ConnectionQuery>,
    /// 会话级在线订阅注册表（统一读扩散地基）：会话 publish 命中本节点时扇给本节点订阅连接。
    conversation_subscriptions: Arc<super::ConversationSubscriptionRegistry>,
    /// 会话成员来源（生产是 Conversation 读池）：每次投递权威读取、订阅与权限收口。
    conversation_read: Arc<dyn ConversationParticipantSource>,
    /// 权威确认后的快照分配复用；不能代替每次成员读取。有界 FIFO，防长跑泄漏。
    resolved_conversations: std::sync::RwLock<ResolvedConversations>,
    /// 权威确认后快照的内存复用有效期（普通会话 / 大群），见 [`DEFAULT_PARTICIPANTS_TTL`]。
    participants_ttl: std::time::Duration,
    large_group_participants_ttl: std::time::Duration,
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
        conversation_read: Arc<dyn ConversationParticipantSource>,
        metrics: Arc<flare_im_service_kit::metrics::AccessGatewayMetrics>,
    ) -> Self {
        Self {
            push_port,
            connection_query,
            conversation_subscriptions,
            conversation_read,
            metrics,
            participants_ttl: env_secs("GATEWAY_PARTICIPANTS_TTL_SECS", DEFAULT_PARTICIPANTS_TTL),
            large_group_participants_ttl: env_secs(
                "GATEWAY_LARGE_GROUP_PARTICIPANTS_TTL_SECS",
                DEFAULT_LARGE_GROUP_PARTICIPANTS_TTL,
            ),
            resolved_conversations: std::sync::RwLock::new(ResolvedConversations::new(
                std::env::var("GATEWAY_RESOLVED_CONVERSATIONS_CAPACITY")
                    .ok()
                    .and_then(|v| v.parse::<usize>().ok())
                    .unwrap_or(DEFAULT_RESOLVED_CONVERSATIONS_CAPACITY),
            )),
        }
    }

    /// 覆盖快照内存复用有效期；无论该值多大都不延长成员授权。
    pub fn with_participants_ttl(
        mut self,
        ttl: std::time::Duration,
        large_group_ttl: std::time::Duration,
    ) -> Self {
        self.participants_ttl = ttl;
        self.large_group_participants_ttl = large_group_ttl;
        self
    }

    /// 每次成员制广播读取权威当前成员，返回本次允许的本节点连接集合。
    /// 成员和连接查询失败必须阻止投递；订阅和缓存均不能充当授权源。
    ///
    /// 当前没有可靠的跨节点成员撤权 barrier，因此必须付出每次成员读取及有界连接查询的成本。
    /// 这封闭了撤权已提交但 TTL 未到的窗口，不声称查询期间的并发撤权已全局线性化。
    async fn ensure_conversation_members_subscribed(
        &self,
        tx: &Ctx,
        tenant_id: &str,
        conversation_id: &str,
    ) -> Result<Option<std::collections::HashSet<String>>> {
        // sync 收件箱不是真实会话、没有成员行：订阅在连接建立时就做了，查成员必然 NOT_FOUND。
        if flare_im_contracts::constants::sync_inbox::is_sync_inbox_conversation_id(conversation_id)
        {
            return Ok(None);
        }
        let current = self
            .conversation_read
            .list_participants(tx, conversation_id)
            .await?;
        let cached = self.resolved_conversations.read().ok().and_then(|cache| {
            cache.get_fresh(
                tenant_id,
                conversation_id,
                std::time::Instant::now(),
                self.participants_ttl,
                self.large_group_participants_ttl,
            )
        });
        let participants = match cached {
            Some(participants) if participants.as_ref() == &current => participants,
            _ => {
                let participants = std::sync::Arc::new(current);
                if let Ok(mut cache) = self.resolved_conversations.write() {
                    cache.insert(
                        tenant_id.to_string(),
                        conversation_id.to_string(),
                        participants.as_ref().clone(),
                    );
                }
                participants
            }
        };
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
        // 对账基线取在查询之前：查询期间才建立的连接（eager 订阅刚 join）不在基线里，不会被误摘。
        let subscribed_before: std::collections::HashSet<String> = self
            .conversation_subscriptions
            .local_subscribers(tenant_id, conversation_id)
            .into_iter()
            .collect();
        let mut member_connections = std::collections::HashSet::new();
        for chunk in participants.chunks(SUBSCRIBE_LOOKUP_CONCURRENCY) {
            let looked_up = futures::future::join_all(
                chunk
                    .iter()
                    .map(|user_id| query.list_user_connection_ids(tx, user_id)),
            )
            .await;
            for connection_ids in looked_up {
                member_connections.extend(connection_ids?);
            }
        }
        for connection_id in &member_connections {
            self.conversation_subscriptions
                .join(tenant_id, conversation_id, connection_id);
        }
        // 订阅里不属于任何在册成员的连接：这个人已经被移出 / 退出了，不能再实时收到这个会话。
        // 权威空名单同样摘除。任何查询失败早已返回，不允许带旧名单继续推送。
        let mut left_total = 0usize;
        for connection_id in subscribed_before.difference(&member_connections) {
            self.conversation_subscriptions
                .leave(tenant_id, conversation_id, connection_id);
            left_total += 1;
        }
        // 这条日志是排查「消息发出去了但某人收不到」的第一落点：
        // participants 是该会话的成员数，joined 是本节点为他们补上的订阅数。
        // joined=0 而 participants>0 有两种解释——成员都在别的网关节点，
        // 或者成员确实全部离线；配合后面那条「跳过投递 / 投递完成」就能定位。
        tracing::debug!(
            conversation_id = %conversation_id,
            participants = participants.len(),
            joined = member_connections.len(),
            left = left_total,
            "push: 会话成员订阅补齐"
        );
        if left_total > 0 {
            // 摘掉的是「已不在会话里」的成员连接，属于权限收口，要能在 info 级别看到。
            tracing::info!(
                conversation_id = %conversation_id,
                left = left_total,
                "push: 摘除已不在会话里的成员连接"
            );
        }
        Ok(Some(member_connections))
    }

    /// 取该会话在本节点仍然存在的订阅连接，顺带把已消失的从订阅表摘掉，返回 `(存活, 摘除数)`。
    ///
    /// 订阅表只增不减是个陷阱：连接断开时 `handle_disconnect` 会清一次，但连接表要等心跳
    /// 回收（分钟级）才摘条目，这期间任何一次投递都会经
    /// `ensure_conversation_members_subscribed` 把这条**已死**的连接重新 join 回来——
    /// 而它的断开事件已经过去了，再没有任何东西会移除它。于是每跑一个客户端就永久留一个
    /// 幽灵：每条消息都要为它白扇一次，而 push 层对解析不到的连接既不计成功也不计失败，
    /// `subscribers - delivered - failed` 的差额就是这些幽灵，日志上看不出来。
    async fn live_subscribers(
        &self,
        tenant_id: &str,
        conversation_id: &str,
    ) -> (Vec<String>, usize) {
        let subscribed = self
            .conversation_subscriptions
            .local_subscribers(tenant_id, conversation_id);
        let mut live = Vec::with_capacity(subscribed.len());
        let mut pruned = 0usize;
        for connection_id in subscribed {
            if self
                .connection_query
                .connection_exists(&connection_id)
                .await
            {
                live.push(connection_id);
            } else {
                self.conversation_subscriptions
                    .leave(tenant_id, conversation_id, &connection_id);
                pruned += 1;
            }
        }
        if pruned > 0 {
            tracing::debug!(
                conversation_id = %conversation_id,
                pruned,
                remaining = live.len(),
                "push: 摘除已消失连接的会话订阅"
            );
        }
        (live, pruned)
    }

    /// 统一读扩散投递：把已编码载荷扇给**本节点**订阅该会话的在线连接。
    /// 每次成员制广播需 O(群成员数) 的权威分页读取与有界连接查询，随后按本节点在线连接扇出。
    /// 没有获授权的本地在线连接时返回 (0,0)；不能因旧订阅为空或 TTL 未到而跳过授权。
    /// 跨节点由"会话 publish 命中所有有订阅者的节点"达成（上层 MQ 主题广播，F2 接入）。
    pub async fn deliver_to_conversation(
        &self,
        tx: &Ctx,
        conversation_id: &str,
        payload_type: i32,
        payload: &[u8],
    ) -> Result<(i32, i32)> {
        let tenant_id = tx
            .tenant_id()
            .filter(|tenant| {
                flare_im_contracts::domain::tenant::is_valid_tenant_id(tenant)
                    && flare_im_contracts::utils::normalize_tenant_id(tenant) == *tenant
            })
            .ok_or_else(|| {
                ErrorBuilder::new(
                    ErrorCode::PermissionDenied,
                    "canonical tenant required for conversation delivery",
                )
                .build_error()
            })?;
        let authorized = self
            .ensure_conversation_members_subscribed(tx, tenant_id, conversation_id)
            .await?;
        let (mut connection_ids, pruned) = self.live_subscribers(tenant_id, conversation_id).await;
        if let Some(authorized) = authorized {
            // A late eager-subscribe may join after reconciliation. It cannot widen this send's
            // authority: only this lookup's authorized connections may receive the payload.
            connection_ids.retain(|connection_id| authorized.contains(connection_id));
        }
        if connection_ids.is_empty() {
            // 本节点没有订阅该会话的在线连接。这不是错误（成员可能在别的网关节点，
            // 或全部离线），但**必须可见**：投递计数为 0 时要能一眼分清
            // "没有订阅者"和"发出去了但全失败"，否则排查只能靠猜。
            tracing::info!(
                conversation_id = %conversation_id,
                "push: 本节点无该会话的在线订阅者，跳过投递"
            );
            self.metrics.record_push_result(tenant_id, 0, 0);
            return Ok((0, 0));
        }
        // 埋点必须在**这里**：统一读扩散上线后，实时投递走的是会话级订阅广播，
        // 而不是按用户查在线连接的 push_encoded_payload_to_user。
        // 先把埋点加在后者身上，指标一直是空的——代码在、路径不通。
        let started = std::time::Instant::now();
        let result = self
            .push_port
            .push_payload_to_connections(tx, &connection_ids, payload_type, payload.to_vec())
            .await;
        if let Ok((ok, fail)) = &result {
            self.metrics.record_push_result(tenant_id, *ok, *fail);
            self.metrics
                .observe_push_latency(tenant_id, started.elapsed().as_secs_f64());
            tracing::info!(
                conversation_id = %conversation_id,
                subscribers = connection_ids.len(),
                pruned,
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
        // 连接上下文的租户在认证时已绑定；缺租户即拒绝，不按租户 "0" 查连接、记指标（W12-T）。
        // 与推送端口的 `target_user` 同一判定，在查连接之前就拒绝。
        let tenant_id = tx
            .tenant_id()
            .filter(|tenant_id| !tenant_id.trim().is_empty())
            .map(str::to_string)
            .ok_or_else(|| {
                ErrorBuilder::new(
                    ErrorCode::PermissionDenied,
                    "tenant required to push to a user",
                )
                .build_error()
            })?;
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

    /// A plugin realtime packet as `DataPacket.capability` to every connection of each user on
    /// this node, in the context's tenant. A user without a connection here is skipped quietly:
    /// the packet is broadcast to every node and most hold none of the recipients.
    ///
    /// Returns `(user_id, delivered_connection_count, failed_connection_count)` per user.
    #[instrument(skip(self, tx, packet), fields(user_count = user_ids.len(), capability_id = %packet.capability_id))]
    pub async fn push_capability_to_users(
        &self,
        tx: &Ctx,
        user_ids: &[String],
        packet: CapabilityPacket,
        options: &PushOptions,
    ) -> Result<Vec<(String, i32, i32)>> {
        let tenant_id = tx
            .tenant_id()
            .filter(|tenant_id| !tenant_id.trim().is_empty())
            .map(str::to_string)
            .ok_or_else(|| {
                ErrorBuilder::new(
                    ErrorCode::PermissionDenied,
                    "tenant required to push to a user",
                )
                .build_error()
            })?;
        let payload = Arc::new(
            DataPacket {
                payload: Some(data_packet::Payload::Capability(packet)),
            }
            .encode_to_vec(),
        );
        let payload_type = flare_core::common::protocol::payload_command::Type::Data as i32;
        let results = stream::iter(user_ids.iter().cloned())
            .map(|user_id| {
                let payload = Arc::clone(&payload);
                async move {
                    let connections = self.get_filtered_connections(tx, &user_id, options).await?;
                    if connections.is_empty() {
                        return Ok((user_id, 0, 0));
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
                    Ok::<_, flare_server_core::error::FlareError>((user_id, ok, fail))
                }
            })
            .buffer_unordered(USER_PUSH_FANOUT_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
        let mut rows = Vec::with_capacity(results.len());
        for result in results {
            rows.push(result?);
        }
        let (ok, fail) = rows
            .iter()
            .fold((0, 0), |(ok, fail), (_, o, f)| (ok + o, fail + f));
        if ok + fail > 0 {
            self.metrics.record_push_result(&tenant_id, ok, fail);
        }
        Ok(rows)
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
        let connections = self
            .connection_query
            .list_user_connections(_tx, user_id)
            .await?;
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
        set.insert("t1".to_string(), "a".to_string(), vec!["u1".to_string()]);
        set.insert("t1".to_string(), "b".to_string(), vec!["u2".to_string()]);
        assert!(set.get("t1", "a").is_some() && set.get("t1", "b").is_some());
        assert_eq!(set.get("t1", "a").unwrap().as_slice(), ["u1"]);
        set.insert("t1".to_string(), "c".to_string(), vec!["u3".to_string()]); // 越界 → 淘汰最旧的 "a"
        assert!(set.get("t1", "a").is_none(), "oldest evicted");
        assert!(set.get("t1", "b").is_some() && set.get("t1", "c").is_some());
        assert_eq!(set.order.len(), 2, "memory bounded at capacity");
        // 重复插入刷新参与者但不增长、不重复入队（LRU 顺序稳定）。
        set.insert("t1".to_string(), "b".to_string(), vec!["u2b".to_string()]);
        assert_eq!(set.order.len(), 2);
        assert_eq!(set.get("t1", "b").unwrap().as_slice(), ["u2b"]);
    }

    #[test]
    fn resolved_member_snapshots_are_tenant_scoped() {
        let mut set = ResolvedConversations::new(2);
        set.insert("t1".into(), "same".into(), vec!["alice".into()]);
        set.insert("t2".into(), "same".into(), vec!["bob".into()]);
        assert_eq!(set.get("t1", "same").unwrap().as_slice(), ["alice"]);
        assert_eq!(set.get("t2", "same").unwrap().as_slice(), ["bob"]);
        assert!(set.get("t3", "same").is_none());
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
        /// 仍然存在的连接 id；空集合表示「全部存在」（兼容既有用例）。
        live: Option<std::collections::HashSet<String>>,
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

        async fn list_user_connections(
            &self,
            _tx: &Ctx,
            _user_id: &str,
        ) -> Result<Vec<ConnectionInfo>> {
            Ok(self.connections.clone())
        }

        async fn connection_exists(&self, connection_id: &str) -> bool {
            match &self.live {
                Some(live) => live.contains(connection_id),
                None => true,
            }
        }
    }

    #[tokio::test]
    async fn push_event_envelope_sets_window_and_delivery_contract() {
        let push_port = Arc::new(CapturingPushPort::default());
        let connection_query = Arc::new(StaticConnectionQuery {
            live: None,
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
        let ctx: Ctx = Arc::new(flare_server_core::Context::root().with_tenant_id("t1"));
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

    /// 按用户推送时上下文缺租户（含空白）即 `PERMISSION_DENIED`，不查连接、不投递，也不再按
    /// 租户 "0" 记推送指标（W12-T）；以前这里照常投递，指标记在租户 "0" 名下。
    #[tokio::test]
    async fn push_to_users_without_a_context_tenant_is_rejected_not_counted_as_tenant_zero() {
        let push_port = Arc::new(CapturingPushPort::default());
        let connection_query = Arc::new(StaticConnectionQuery {
            live: None,
            connections: vec![ConnectionInfo::new(
                "conn-1".to_string(),
                "u1".to_string(),
                "t1".to_string(),
                "dev-1".to_string(),
            )],
        });
        let service = PushDomainService::new(
            push_port.clone(),
            connection_query,
            Arc::new(crate::domain::service::ConversationSubscriptionRegistry::new()),
            Arc::new(crate::infrastructure::ports::ConversationReadGrpcPool::new()),
            Arc::new(flare_im_service_kit::metrics::AccessGatewayMetrics::new()),
        );
        let user_ids = vec!["u1".to_string()];
        for ctx in [
            flare_server_core::Context::root(),
            flare_server_core::Context::root().with_tenant_id("  "),
        ] {
            let ctx: Ctx = Arc::new(ctx);
            let error = service
                .push_event_envelope_to_users(
                    &ctx,
                    EventEnvelopePushRequest {
                        user_ids: &user_ids,
                        events: vec![Event {
                            conversation_id: "c1".to_string(),
                            conversation_seq: 1,
                            ..Default::default()
                        }],
                        options: &PushOptions::default(),
                        window_id: "window-1",
                        conversation_id: "",
                        max_conversation_seq: 0,
                        delivery_mode: 0,
                        inline_events_truncated: false,
                    },
                )
                .await
                .expect_err("no tenant, no push");
            assert_eq!(error.code(), Some(ErrorCode::PermissionDenied), "{error}");
            assert!(error.reason().contains("tenant required"), "{error}");
        }
        assert!(
            push_port
                .payloads
                .lock()
                .expect("payload mutex poisoned")
                .is_empty(),
            "nothing may be delivered without a tenant"
        );
    }

    #[tokio::test]
    async fn conversation_delivery_prunes_subscriptions_of_gone_connections() {
        // 订阅表只增不减：连接断开后下一次投递会把它重新 join 回来，而断开事件已经过去，
        // 于是每条消息都要为幽灵做一次无效扇出，且 push 层对解析不到的连接既不计成功
        // 也不计失败——计数对不上还看不出来。投递必须就地把它们摘掉。
        let push_port = Arc::new(CapturingPushPort::default());
        let subscriptions =
            Arc::new(crate::domain::service::ConversationSubscriptionRegistry::new());
        subscriptions.join("t1", "c1", "conn-live");
        subscriptions.join("t1", "c1", "conn-ghost-1");
        subscriptions.join("t1", "c1", "conn-ghost-2");
        let connection_query = Arc::new(StaticConnectionQuery {
            connections: Vec::new(),
            live: Some(std::collections::HashSet::from(["conn-live".to_string()])),
        });
        let service = PushDomainService::new(
            push_port.clone(),
            connection_query,
            subscriptions.clone(),
            Arc::new(crate::infrastructure::ports::ConversationReadGrpcPool::new()),
            Arc::new(flare_im_service_kit::metrics::AccessGatewayMetrics::new()),
        );
        let ctx: Ctx = Arc::new(flare_server_core::Context::root());

        let _ = &ctx;
        let (live, pruned) = service.live_subscribers("t1", "c1").await;

        assert_eq!(
            live,
            vec!["conn-live".to_string()],
            "只应投给仍然存在的那条连接"
        );
        assert_eq!(pruned, 2, "两条幽灵连接必须被计入摘除数，让日志计数对得上");
        assert_eq!(
            subscriptions.local_subscribers("t1", "c1"),
            vec!["conn-live".to_string()],
            "已经不存在的连接必须从订阅表摘掉，否则每条消息都要为它白跑一次"
        );
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

    // ── 权威成员读取与订阅收口 ────────────────────────────────────────────────
    // 不等待 TTL、不依赖通知消息，也不把旧连接订阅当作成员授权。

    /// 可改写的成员名单，记录被查询的次数。
    #[derive(Default)]
    struct FakeParticipants {
        members: Mutex<std::collections::HashMap<ConversationCacheKey, Vec<String>>>,
        calls: std::sync::atomic::AtomicUsize,
        failing: std::sync::atomic::AtomicBool,
    }

    impl FakeParticipants {
        fn set(&self, conversation_id: &str, members: &[&str]) {
            self.set_for_tenant("t1", conversation_id, members);
        }

        fn set_for_tenant(&self, tenant_id: &str, conversation_id: &str, members: &[&str]) {
            self.members.lock().expect("members mutex poisoned").insert(
                (tenant_id.to_string(), conversation_id.to_string()),
                members.iter().map(|m| m.to_string()).collect(),
            );
        }

        fn calls(&self) -> usize {
            self.calls.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl ConversationParticipantSource for FakeParticipants {
        async fn list_participants(&self, tx: &Ctx, conversation_id: &str) -> Result<Vec<String>> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.failing.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(ErrorBuilder::new(
                    ErrorCode::ServiceUnavailable,
                    "authoritative member source unavailable",
                )
                .build_error());
            }
            Ok(self
                .members
                .lock()
                .expect("members mutex poisoned")
                .get(&(
                    tx.tenant_id().unwrap_or_default().to_string(),
                    conversation_id.to_string(),
                ))
                .cloned()
                .unwrap_or_default())
        }
    }

    /// 每个用户在本节点的连接；`failing` 里的用户查询报错。
    struct LateSubscriptionJoin {
        subscriptions: Arc<crate::domain::service::ConversationSubscriptionRegistry>,
        tenant: String,
        conversation: String,
        connection: String,
    }

    #[derive(Default)]
    struct UserConnections {
        by_user: std::collections::HashMap<(String, String), Vec<String>>,
        failing: Mutex<std::collections::HashSet<String>>,
        late_join: Mutex<Option<LateSubscriptionJoin>>,
    }

    impl UserConnections {
        fn with(pairs: &[(&str, &str)]) -> Self {
            let pairs: Vec<(&str, &str, &str)> = pairs
                .iter()
                .map(|(user, connection)| ("t1", *user, *connection))
                .collect();
            Self::with_tenants(&pairs)
        }

        fn with_tenants(pairs: &[(&str, &str, &str)]) -> Self {
            let mut by_user = std::collections::HashMap::<(String, String), Vec<String>>::new();
            for (tenant, user, connection) in pairs {
                by_user
                    .entry((tenant.to_string(), user.to_string()))
                    .or_default()
                    .push(connection.to_string());
            }
            Self {
                by_user,
                failing: Mutex::default(),
                late_join: Mutex::default(),
            }
        }
    }

    #[async_trait]
    impl ConnectionQuery for UserConnections {
        async fn query_user_connections(
            &self,
            _tx: &Ctx,
            _user_id: &str,
        ) -> Result<Vec<ConnectionInfo>> {
            Ok(Vec::new())
        }

        async fn list_user_connections(
            &self,
            _tx: &Ctx,
            _user_id: &str,
        ) -> Result<Vec<ConnectionInfo>> {
            Ok(Vec::new())
        }

        async fn list_user_connection_ids(&self, tx: &Ctx, user_id: &str) -> Result<Vec<String>> {
            if let Some(join) = self.late_join.lock().expect("late join mutex").take() {
                // A concurrent eager-subscribe joins after the reconciliation baseline.
                join.subscriptions
                    .join(&join.tenant, &join.conversation, &join.connection);
            }
            if self
                .failing
                .lock()
                .expect("failing mutex poisoned")
                .contains(user_id)
            {
                return Err(ErrorBuilder::new(
                    ErrorCode::ServiceUnavailable,
                    "connection table down",
                )
                .build_error());
            }
            Ok(self
                .by_user
                .get(&(
                    tx.tenant_id().unwrap_or_default().to_string(),
                    user_id.to_string(),
                ))
                .cloned()
                .unwrap_or_default())
        }

        async fn connection_exists(&self, _connection_id: &str) -> bool {
            true
        }
    }

    /// 记录每次会话级投递实际扇到的连接。
    #[derive(Default)]
    struct RecordingPushPort {
        deliveries: Mutex<Vec<Vec<String>>>,
    }

    impl RecordingPushPort {
        fn last(&self) -> Vec<String> {
            self.deliveries
                .lock()
                .expect("deliveries mutex poisoned")
                .last()
                .cloned()
                .unwrap_or_default()
        }
    }

    #[async_trait]
    impl IPushPort for RecordingPushPort {
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
            _payload_type: i32,
            _payload: Vec<u8>,
        ) -> Result<()> {
            Ok(())
        }

        async fn push_payload_to_user(
            &self,
            _tx: &Ctx,
            _user_id: &str,
            _payload_type: i32,
            _payload: Vec<u8>,
        ) -> Result<()> {
            Ok(())
        }

        async fn push_payload_to_connections(
            &self,
            _tx: &Ctx,
            connection_ids: &[String],
            _payload_type: i32,
            _payload: Vec<u8>,
        ) -> Result<(i32, i32)> {
            let mut delivered = connection_ids.to_vec();
            delivered.sort();
            self.deliveries
                .lock()
                .expect("deliveries mutex poisoned")
                .push(delivered);
            Ok((connection_ids.len() as i32, 0))
        }
    }

    /// Connections keyed by `(tenant, user)`, answered for the context's tenant only.
    struct TenantConnections(Vec<ConnectionInfo>);

    #[async_trait]
    impl ConnectionQuery for TenantConnections {
        async fn query_user_connections(
            &self,
            tx: &Ctx,
            user_id: &str,
        ) -> Result<Vec<ConnectionInfo>> {
            Ok(self
                .0
                .iter()
                .filter(|c| Some(c.tenant_id.as_str()) == tx.tenant_id() && c.user_id == user_id)
                .cloned()
                .collect())
        }

        async fn list_user_connections(
            &self,
            tx: &Ctx,
            user_id: &str,
        ) -> Result<Vec<ConnectionInfo>> {
            self.query_user_connections(tx, user_id).await
        }

        async fn connection_exists(&self, _connection_id: &str) -> bool {
            true
        }
    }

    /// The packet reaches every connection of each listed user in the producer's tenant, as one
    /// `DataPacket.capability`; the same user id in another tenant receives nothing, and a user
    /// with no connection on this node is skipped.
    #[tokio::test]
    async fn capability_push_reaches_the_users_connections_in_its_tenant_only() {
        let connection = |id: &str, user: &str, tenant: &str| {
            ConnectionInfo::new(id.into(), user.into(), tenant.into(), format!("dev-{id}"))
        };
        let push = Arc::new(RecordingPushPort::default());
        let service = PushDomainService::new(
            push.clone(),
            Arc::new(TenantConnections(vec![
                connection("c1", "u1", "t1"),
                connection("c2", "u1", "t1"),
                connection("x1", "u1", "t2"),
                connection("x2", "u2", "t2"),
            ])),
            Arc::new(crate::domain::service::ConversationSubscriptionRegistry::new()),
            Arc::new(crate::infrastructure::ports::ConversationReadGrpcPool::new()),
            Arc::new(flare_im_service_kit::metrics::AccessGatewayMetrics::new()),
        );
        let packet = CapabilityPacket {
            capability_id: "flare.call".into(),
            packet_type: "call.snapshot".into(),
            version: "1".into(),
            payload: br#"{"callId":"c-1"}"#.to_vec(),
            ..Default::default()
        };
        let ctx: Ctx = Arc::new(flare_server_core::Context::root().with_tenant_id("t1"));
        let rows = service
            .push_capability_to_users(
                &ctx,
                &["u1".to_string(), "u2".to_string()],
                packet.clone(),
                &PushOptions::default(),
            )
            .await
            .unwrap();
        let mut rows = rows;
        rows.sort();
        assert_eq!(
            rows,
            vec![("u1".to_string(), 2, 0), ("u2".to_string(), 0, 0)]
        );
        let deliveries = push.deliveries.lock().unwrap().clone();
        assert_eq!(deliveries, vec![vec!["c1".to_string(), "c2".to_string()]]);

        // No tenant: refused before any connection is looked up.
        let bare: Ctx = Arc::new(flare_server_core::Context::root());
        assert!(
            service
                .push_capability_to_users(
                    &bare,
                    &["u1".to_string()],
                    packet,
                    &PushOptions::default()
                )
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn capability_push_writes_one_data_packet() {
        let push = Arc::new(CapturingPushPort::default());
        let service = PushDomainService::new(
            push.clone(),
            Arc::new(TenantConnections(vec![ConnectionInfo::new(
                "c1".into(),
                "u1".into(),
                "t1".into(),
                "dev-1".into(),
            )])),
            Arc::new(crate::domain::service::ConversationSubscriptionRegistry::new()),
            Arc::new(crate::infrastructure::ports::ConversationReadGrpcPool::new()),
            Arc::new(flare_im_service_kit::metrics::AccessGatewayMetrics::new()),
        );
        let packet = CapabilityPacket {
            capability_id: "flare.call".into(),
            packet_type: "call.snapshot".into(),
            ..Default::default()
        };
        let ctx: Ctx = Arc::new(flare_server_core::Context::root().with_tenant_id("t1"));
        service
            .push_capability_to_users(
                &ctx,
                &["u1".to_string()],
                packet.clone(),
                &PushOptions::default(),
            )
            .await
            .unwrap();
        let payloads = push.payloads.lock().unwrap().clone();
        assert_eq!(payloads.len(), 1);
        assert_eq!(
            payloads[0].0,
            flare_core::common::protocol::payload_command::Type::Data as i32
        );
        assert_eq!(
            DataPacket::decode(payloads[0].1.as_slice())
                .unwrap()
                .payload,
            Some(data_packet::Payload::Capability(packet))
        );
    }

    struct MemberHarness {
        service: PushDomainService,
        participants: Arc<FakeParticipants>,
        connections: Arc<UserConnections>,
        subscriptions: Arc<crate::domain::service::ConversationSubscriptionRegistry>,
        push: Arc<RecordingPushPort>,
    }

    impl MemberHarness {
        fn new(connections: UserConnections, ttl: std::time::Duration) -> Self {
            let participants = Arc::new(FakeParticipants::default());
            let connections = Arc::new(connections);
            let subscriptions =
                Arc::new(crate::domain::service::ConversationSubscriptionRegistry::new());
            let push = Arc::new(RecordingPushPort::default());
            let service = PushDomainService::new(
                push.clone(),
                connections.clone(),
                subscriptions.clone(),
                participants.clone(),
                Arc::new(flare_im_service_kit::metrics::AccessGatewayMetrics::new()),
            )
            .with_participants_ttl(ttl, ttl);
            Self {
                service,
                participants,
                connections,
                subscriptions,
                push,
            }
        }

        async fn deliver(&self, conversation_id: &str) -> Vec<String> {
            self.deliver_for_tenant("t1", conversation_id).await
        }

        async fn deliver_group_message(&self, conversation_id: &str, system: bool) -> Vec<String> {
            use flare_proto::common::{
                ConversationType, MessageContent, MessageSource, SystemContent, TextContent,
                message_content::Content,
            };
            let message = Message {
                conversation_id: conversation_id.into(),
                conversation_type: ConversationType::Group as i32,
                source: if system {
                    MessageSource::System
                } else {
                    MessageSource::User
                } as i32,
                content: Some(MessageContent {
                    content: Some(if system {
                        Content::System(SystemContent {
                            event_kind: "group.member_kicked".into(),
                            body: "member removed".into(),
                            ..Default::default()
                        })
                    } else {
                        Content::Text(TextContent {
                            text: "ordinary group message".into(),
                            ..Default::default()
                        })
                    }),
                }),
                ..Default::default()
            };
            let ctx = Arc::new(flare_server_core::Context::root().with_tenant_id("t1"));
            let before = self.push.deliveries.lock().expect("deliveries mutex").len();
            self.service
                .push_message_to_conversation(&ctx, conversation_id, vec![message])
                .await
                .expect("group message delivery");
            if self.push.deliveries.lock().expect("deliveries mutex").len() == before {
                Vec::new()
            } else {
                self.push.last()
            }
        }

        async fn deliver_for_tenant(&self, tenant_id: &str, conversation_id: &str) -> Vec<String> {
            let ctx: Ctx = Arc::new(flare_server_core::Context::root().with_tenant_id(tenant_id));
            let before = self.push.deliveries.lock().expect("mutex").len();
            self.service
                .deliver_to_conversation(&ctx, conversation_id, 1, b"payload")
                .await
                .expect("deliver");
            if self.push.deliveries.lock().expect("mutex").len() == before {
                return Vec::new();
            }
            self.push.last()
        }
    }

    #[tokio::test]
    async fn same_conversation_id_in_two_tenants_never_reuses_members_or_subscribers() {
        let h = MemberHarness::new(
            UserConnections::with_tenants(&[("t1", "alice", "c-alice"), ("t2", "bob", "c-bob")]),
            std::time::Duration::from_secs(3600),
        );
        h.participants.set_for_tenant("t1", "same", &["alice"]);
        h.participants.set_for_tenant("t2", "same", &["bob"]);
        assert_eq!(h.deliver_for_tenant("t1", "same").await, ["c-alice"]);
        assert_eq!(h.deliver_for_tenant("t2", "same").await, ["c-bob"]);
        assert_eq!(
            h.participants.calls(),
            2,
            "each tenant resolves its own member list"
        );
        assert_eq!(h.subscriptions.local_subscribers("t1", "same"), ["c-alice"]);
        assert_eq!(h.subscriptions.local_subscribers("t2", "same"), ["c-bob"]);
    }

    #[tokio::test]
    async fn large_group_authorization_is_never_throttled_or_shared_between_tenants() {
        let h = MemberHarness::new(
            UserConnections::default(),
            std::time::Duration::from_secs(3600),
        );
        let members = vec!["member"; LARGE_GROUP_THRESHOLD];
        h.participants.set_for_tenant("t1", "same", &members);
        h.participants.set_for_tenant("t2", "same", &members);
        h.deliver_for_tenant("t1", "same").await;
        h.deliver_for_tenant("t2", "same").await;
        h.deliver_for_tenant("t1", "same").await;
        h.deliver_for_tenant("t2", "same").await;
        assert_eq!(h.participants.calls(), 4);
    }

    // 建群时推送第一条系统消息，名单里只有群主；之后在线的 bob 被拉进群。
    #[tokio::test]
    async fn a_member_added_while_online_is_reached_without_waiting_for_ttl() {
        let h = MemberHarness::new(
            UserConnections::with(&[("owner", "c-owner"), ("bob", "c-bob")]),
            std::time::Duration::from_secs(3600),
        );
        h.participants.set("g1", &["owner"]);
        assert_eq!(h.deliver("g1").await, ["c-owner"]);

        h.participants.set("g1", &["owner", "bob"]);
        assert_eq!(h.deliver("g1").await, ["c-bob", "c-owner"]);
    }

    #[tokio::test]
    async fn a_fresh_member_read_is_required_even_with_a_long_snapshot_ttl() {
        let h = MemberHarness::new(
            UserConnections::with(&[("owner", "c-owner"), ("bob", "c-bob")]),
            std::time::Duration::from_secs(3600),
        );
        h.participants.set("g1", &["owner"]);
        h.deliver("g1").await;
        h.participants.set("g1", &["owner", "bob"]);
        assert_eq!(h.deliver("g1").await, ["c-bob", "c-owner"]);
        assert_eq!(h.participants.calls(), 2);
    }

    #[tokio::test]
    async fn a_member_removed_while_online_stops_receiving() {
        let h = MemberHarness::new(
            UserConnections::with(&[("owner", "c-owner"), ("bob", "c-bob")]),
            std::time::Duration::from_secs(3600),
        );
        h.participants.set("g1", &["owner", "bob"]);
        assert_eq!(h.deliver("g1").await, ["c-bob", "c-owner"]);

        h.participants.set("g1", &["owner"]);
        assert_eq!(h.deliver("g1").await, ["c-owner"]);
        assert!(
            !h.subscriptions
                .local_subscribers("t1", "g1")
                .contains(&"c-bob".to_string())
        );
    }

    // 查询失败不误摘有效成员，但也绝不能沿旧订阅投递。
    #[tokio::test]
    async fn a_failed_connection_lookup_never_unsubscribes_anyone() {
        let h = MemberHarness::new(
            UserConnections::with(&[("owner", "c-owner"), ("bob", "c-bob")]),
            std::time::Duration::from_secs(3600),
        );
        h.participants.set("g1", &["owner", "bob"]);
        h.deliver("g1").await;

        h.connections
            .failing
            .lock()
            .expect("failing mutex poisoned")
            .insert("owner".to_string());
        let before = h.push.deliveries.lock().expect("deliveries mutex").len();
        let ctx = Arc::new(flare_server_core::Context::root().with_tenant_id("t1"));
        let error = h
            .service
            .deliver_to_conversation(&ctx, "g1", 1, b"must not send")
            .await
            .unwrap_err();
        assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable));
        assert_eq!(
            h.push.deliveries.lock().expect("deliveries mutex").len(),
            before
        );
        let mut subscribed = h.subscriptions.local_subscribers("t1", "g1");
        subscribed.sort();
        assert_eq!(subscribed, ["c-bob", "c-owner"]);
    }

    #[tokio::test]
    async fn batch_removed_members_cannot_receive_system_or_regular_messages_with_long_ttl() {
        let h = MemberHarness::new(
            UserConnections::with(&[("a", "c-a"), ("b", "c-b"), ("c", "c-c")]),
            std::time::Duration::from_secs(3600),
        );
        h.participants.set("g1", &["a", "b", "c"]);
        assert_eq!(
            h.deliver_group_message("g1", false).await,
            ["c-a", "c-b", "c-c"]
        );
        h.participants.set("g1", &["a"]);
        assert_eq!(h.deliver_group_message("g1", true).await, ["c-a"]);
        assert_eq!(h.deliver_group_message("g1", false).await, ["c-a"]);
        assert_eq!(h.participants.calls(), 3);
    }

    #[tokio::test]
    async fn removal_authorization_does_not_require_a_visible_system_notification() {
        let h = MemberHarness::new(
            UserConnections::with(&[("a", "c-a"), ("b", "c-b"), ("c", "c-c")]),
            std::time::Duration::from_secs(3600),
        );
        h.participants.set("g1", &["a", "b", "c"]);
        h.deliver_group_message("g1", false).await;
        h.participants.set("g1", &["a"]);
        // No membership System is emitted when the product's message switch is off.
        assert_eq!(h.deliver_group_message("g1", false).await, ["c-a"]);
    }

    #[tokio::test]
    async fn authoritative_empty_members_remove_every_previous_subscription_and_send_nothing() {
        let h = MemberHarness::new(
            UserConnections::with(&[("a", "c-a"), ("b", "c-b")]),
            std::time::Duration::from_secs(3600),
        );
        h.participants.set("g1", &["a", "b"]);
        h.deliver("g1").await;
        let before = h.push.deliveries.lock().expect("deliveries mutex").len();
        h.participants.set("g1", &[]);
        assert!(h.deliver("g1").await.is_empty());
        assert_eq!(
            h.push.deliveries.lock().expect("deliveries mutex").len(),
            before
        );
        assert!(h.subscriptions.local_subscribers("t1", "g1").is_empty());
    }

    #[tokio::test]
    async fn unavailable_authority_never_falls_back_to_cached_members_or_subscribers() {
        let h = MemberHarness::new(
            UserConnections::with(&[("a", "c-a"), ("b", "c-b")]),
            std::time::Duration::from_secs(3600),
        );
        h.participants.set("g1", &["a", "b"]);
        h.deliver("g1").await;
        h.participants
            .failing
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let before = h.push.deliveries.lock().expect("deliveries mutex").len();
        let ctx = Arc::new(flare_server_core::Context::root().with_tenant_id("t1"));
        let error = h
            .service
            .deliver_to_conversation(&ctx, "g1", 1, b"no send")
            .await
            .unwrap_err();
        assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable));
        assert_eq!(
            h.push.deliveries.lock().expect("deliveries mutex").len(),
            before
        );
        h.participants
            .failing
            .store(false, std::sync::atomic::Ordering::SeqCst);
        h.participants.set("g1", &["a"]);
        assert_eq!(h.deliver("g1").await, ["c-a"]);
    }

    #[tokio::test]
    async fn a_late_eager_subscription_cannot_widen_the_current_delivery() {
        let h = MemberHarness::new(
            UserConnections::with(&[("a", "c-a"), ("b", "c-b")]),
            std::time::Duration::from_secs(3600),
        );
        h.participants.set("g1", &["a"]);
        *h.connections.late_join.lock().expect("late join mutex") = Some(LateSubscriptionJoin {
            subscriptions: h.subscriptions.clone(),
            tenant: "t1".into(),
            conversation: "g1".into(),
            connection: "c-b".into(),
        });
        assert_eq!(h.deliver("g1").await, ["c-a"]);
        assert!(
            h.subscriptions
                .local_subscribers("t1", "g1")
                .contains(&"c-b".into()),
            "the race fixture joined after the reconciliation baseline"
        );
        assert_eq!(h.deliver("g1").await, ["c-a"]);
        assert!(
            !h.subscriptions
                .local_subscribers("t1", "g1")
                .contains(&"c-b".into())
        );
    }

    #[tokio::test]
    async fn batch_removal_revokes_every_device_without_removing_other_members_devices() {
        let h = MemberHarness::new(
            UserConnections::with(&[
                ("a", "a-mobile"),
                ("a", "a-web"),
                ("b", "b-mobile"),
                ("b", "b-web"),
                ("c", "c-web"),
            ]),
            std::time::Duration::from_secs(3600),
        );
        h.participants.set("g1", &["a", "b", "c"]);
        assert_eq!(
            h.deliver("g1").await,
            ["a-mobile", "a-web", "b-mobile", "b-web", "c-web"]
        );
        h.participants.set("g1", &["a"]);
        assert_eq!(h.deliver("g1").await, ["a-mobile", "a-web"]);
    }

    #[tokio::test]
    async fn the_sync_inbox_is_delivered_without_a_member_lookup() {
        let h = MemberHarness::new(UserConnections::default(), std::time::Duration::ZERO);
        let inbox = flare_im_contracts::constants::sync_inbox::sync_inbox_conversation_id("u1");
        h.subscriptions.join("t1", &inbox, "c-u1");

        assert_eq!(h.deliver(&inbox).await, ["c-u1"]);
        assert_eq!(h.participants.calls(), 0);
        assert_eq!(h.subscriptions.local_subscribers("t1", &inbox), ["c-u1"]);
    }
}
