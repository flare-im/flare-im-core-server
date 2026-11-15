//! 送达确认跟踪（DeliveryWatch）
//!
//! 帧写进套接字不等于送达：半开连接、被系统冻结的进程都会照单全收却什么也不提示。网关按连接
//! 记下还没被客户端确认的可提示下行帧，客户端按会话回 `ConversationAck.last_delivered_seq`
//! 清掉它们；到期没清掉的，连同之后写给这条连接的可提示帧，一律交接给推送通道。
//!
//! 每条连接只有两样东西：一个有界环（[`RING_CAPACITY`] 项，存会话、seq、消息 ID 与写出时间），
//! 以及环里最早一条的写出时间（就是环头，条目按写出顺序入环）。全部在内存里，不随群成员数
//! 增长，不改下行帧——同一帧仍然只编码一次、扇给所有连接。
//!
//! 超时由一个后台任务驱动。期限队列里每条连接至多占一项，只有环里有过条目的连接才会进队列，
//! 任务每次只处理到期的那几项，开销与「有待确认条目的连接数」成正比，与连接总数无关。
//!
//! 一条连接超时后，环里的条目作为一次交接上报，同时进入两个彼此独立的状态：
//!
//! - **待关闭**：立刻发一帧 PING，探活期限内对端毫无动静就关闭连接。PONG、一次 DEVICE_STATE
//!   上报、任何一条送达确认都证明进程还活着，都能取消它。
//! - **立即交接**：之后写给它的可提示帧照常写出，同时立即交接、不再入环。只有它回了送达确认，
//!   或设备经 DEVICE_STATE 报告回到前台，才回到正常跟踪。PONG 和后台状态的上报不结束这个
//!   状态：它们证明进程活着，证明不了进程在提示用户。
//!
//! 所以一台进程还活着、但不确认送达的后台手机不会被断开，它的提示也不用每条都先等满期限：
//! 第一条超时之后的都直接走推送通道。
//!
//! 设备也可以事先声明自己在后台不能提示（`Deferred`）：推送路由在路由消息时就直接给它推送，
//! 这条链路上的帧照常写出，但完全不跟踪——不入环、没有期限、不交接、不探活。

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use flare_im_contracts::domain::push_policy::{
    alert_class_needs_delivery_watch, event_alert_class, message_alert_class,
};
use flare_im_service_kit::metrics::AccessGatewayMetrics;
use flare_proto::common::event::Payload as EventPayload;
use flare_proto::common::{
    ConversationAck, DeviceAppState, DeviceHandoff, DeviceHandoffCause, DeviceHandoffItem,
    EventEnvelope, Message,
};
use tracing::{debug, info};

use crate::domain::model::DeviceLinkState;
use crate::domain::ports::{IDeviceHandoffPort, ILinkProbePort};
use crate::domain::service::ConnectionQualityService;

/// 每条连接的环容量。环满时最旧的条目按一次交接上报后让位，不丢也不增长。
const RING_CAPACITY: usize = 64;

/// 已知往返时延时，期限的下限：再快的链路也给客户端留出批处理与合并确认的时间。
const DEADLINE_FLOOR: Duration = Duration::from_secs(1);

/// 已知往返时延时，期限 = 4 × RTT + 这段余量（客户端 200ms 内的确认合并成一批）。
const RTT_MARGIN: Duration = Duration::from_millis(500);

/// 连接表的分片数。大群一次扇出要逐连接登记，分片后与确认、状态上报互不排队。
const SHARD_COUNT: usize = 64;

/// 后台任务检查期限队列的间隔，也是超时判定的最大滞后。
const SWEEP_TICK: Duration = Duration::from_millis(100);

/// 期限队列的一项：到期时间、序号、连接 ID。序号对不上连接当前登记的那个就是作废的旧项。
type Timer = (Instant, u64, Arc<str>);

type Shard = HashMap<Arc<str>, WatchedConnection>;

/// 送达确认期限与探活时限。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryWatchConfig {
    /// 前台连接等送达确认的期限
    pub active_deadline: Duration,
    /// 后台连接等送达确认的期限：后台进程随时可能被冻结，等得更短
    pub standby_deadline: Duration,
    /// 确认超时后发出 PING，这么久没有回应就关闭连接
    pub probe_timeout: Duration,
}

impl Default for DeliveryWatchConfig {
    fn default() -> Self {
        Self {
            active_deadline: Duration::from_secs(3),
            standby_deadline: Duration::from_millis(1_500),
            probe_timeout: Duration::from_secs(5),
        }
    }
}

/// 一条需要送达确认的下行内容。同一帧扇给很多连接时各连接的环共享这两段字符串。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryItem {
    pub conversation_id: Arc<str>,
    /// 会话 seq，客户端的确认按它清条目
    pub seq: u64,
    /// 服务端消息 ID
    pub message_id: Arc<str>,
}

impl DeliveryItem {
    pub fn new(conversation_id: &str, seq: u64, message_id: &str) -> Self {
        Self {
            conversation_id: Arc::from(conversation_id),
            seq,
            message_id: Arc::from(message_id),
        }
    }

    /// 一批下行消息里需要送达确认的那些：提示等级要求跟踪，并且带会话 seq。
    /// 没有 seq 的内容客户端无从确认，跟踪它只会必然超时，所以跳过。
    pub fn from_messages(messages: &[Message]) -> Vec<Self> {
        messages
            .iter()
            .filter(|message| {
                message.conversation_seq > 0
                    && alert_class_needs_delivery_watch(message_alert_class(message))
            })
            .map(|message| {
                Self::new(
                    &message.conversation_id,
                    message.conversation_seq,
                    &message.server_id,
                )
            })
            .collect()
    }

    /// 一个下行事件批里需要送达确认的那些：批里内联的、提示等级要求跟踪的事件。
    pub fn from_envelope(envelope: &EventEnvelope) -> Vec<Self> {
        // 纯水位通知（没有内联事件）不跟踪：客户端靠同步拉取这些消息，拉来的消息不回送达确认。
        envelope
            .events
            .iter()
            .filter(|event| alert_class_needs_delivery_watch(event_alert_class(event)))
            .filter_map(|event| {
                let (message_seq, message_id) = match &event.payload {
                    Some(EventPayload::Message(message)) => {
                        (message.conversation_seq, message.server_id.as_str())
                    }
                    _ => (0, ""),
                };
                let seq = if event.conversation_seq > 0 {
                    event.conversation_seq
                } else {
                    message_seq
                };
                (seq > 0).then(|| Self::new(&event.conversation_id, seq, message_id))
            })
            .collect()
    }

    /// 这条确认是否覆盖本条目：同一会话，seq 不大于确认值，或消息 ID 被逐条点到。
    fn confirmed_by(&self, ack: &ConversationAck) -> bool {
        &*self.conversation_id == ack.conversation_id.as_str()
            && (self.seq <= ack.last_delivered_seq
                || (!self.message_id.is_empty()
                    && ack
                        .server_msg_ids
                        .iter()
                        .any(|message_id| message_id.as_str() == &*self.message_id)))
    }
}

/// 交接上报里标识设备的字段，登记连接时取自已认证的连接信息。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchedDevice {
    pub tenant_id: String,
    pub user_id: String,
    pub device_id: String,
}

/// 超时处理要对链路做的动作，由后台任务执行。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LinkAction {
    /// 确认超时：立刻探活
    Ping(Arc<str>),
    /// 探活期限内没有回应：关闭连接
    Close(Arc<str>),
}

/// 环里的一项：内容与写出时间。
struct PendingAlert {
    item: DeliveryItem,
    written_at: Instant,
}

/// 确认超时后发出的探活。
#[derive(Debug, Clone, Copy)]
struct Probe {
    sent_at: Instant,
    deadline: Instant,
}

struct WatchedConnection {
    id: Arc<str>,
    device: WatchedDevice,
    state: DeviceLinkState,
    /// 未确认的可提示帧，按写出顺序；环头的写出时间就是「最早一条未确认帧的写出时间」。
    ring: VecDeque<PendingAlert>,
    /// 期限队列里代表这条连接的那一项的序号；`None` 表示队列里没有它的有效项。
    timer: Option<u64>,
    /// 待关闭：确认超时后已发 PING，探活期限内没有 PONG、设备状态上报或送达确认就关闭连接。
    probe: Option<Probe>,
    /// 立即交接：这条连接没在确认送达。可提示的帧照常写出，同时立即交接、不入环，直到它回了
    /// 送达确认或设备报告回到前台。与待关闭彼此独立，PONG 只取消待关闭。
    handing_off: bool,
}

impl WatchedConnection {
    /// 取消待关闭（没有就什么都不做），探活期限那一项随之作废。
    ///
    /// 待关闭期间连接一定处于立即交接（两者同时开始，能结束立即交接的事件也都取消待关闭），
    /// 环是空的，所以这里没有要重新排期的条目；之后写入的可提示帧会自己排期。
    fn cancel_probe(&mut self) -> Option<Probe> {
        let probe = self.probe.take()?;
        self.timer = None;
        Some(probe)
    }
}

/// 送达确认跟踪：每条已登记 Online 会话的连接一份状态。
pub struct DeliveryWatch {
    gateway_id: String,
    config: DeliveryWatchConfig,
    shards: Vec<Mutex<Shard>>,
    /// 期限队列（最小堆）。锁顺序：先连接分片、后队列，两把锁不同时持有。
    timers: Mutex<BinaryHeap<Reverse<Timer>>>,
    next_timer: AtomicU64,
    handoffs: Arc<dyn IDeviceHandoffPort>,
    probe: Arc<dyn ILinkProbePort>,
    quality: Arc<ConnectionQualityService>,
    metrics: Arc<AccessGatewayMetrics>,
}

impl DeliveryWatch {
    pub fn new(
        gateway_id: String,
        config: DeliveryWatchConfig,
        handoffs: Arc<dyn IDeviceHandoffPort>,
        probe: Arc<dyn ILinkProbePort>,
        quality: Arc<ConnectionQualityService>,
        metrics: Arc<AccessGatewayMetrics>,
    ) -> Self {
        Self {
            gateway_id,
            config,
            shards: (0..SHARD_COUNT)
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
            timers: Mutex::new(BinaryHeap::new()),
            next_timer: AtomicU64::new(0),
            handoffs,
            probe,
            quality,
            metrics,
        }
    }

    /// 起后台任务驱动超时（须在运行时内调用）。
    pub fn spawn(self: &Arc<Self>) {
        let watch = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(SWEEP_TICK);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                let actions = watch.sweep(Instant::now()).await;
                watch.dispatch(actions);
            }
        });
        info!(
            active_deadline_ms = self.config.active_deadline.as_millis() as u64,
            standby_deadline_ms = self.config.standby_deadline.as_millis() as u64,
            probe_timeout_ms = self.config.probe_timeout.as_millis() as u64,
            "delivery watch started"
        );
    }

    /// 开始跟踪一条连接。与写入 Online 会话成对调用；同一连接重复登记时只更新设备标识，
    /// 已跟踪的条目与客户端报过的状态保留。
    pub fn register(&self, connection_id: &str, device: WatchedDevice, state: DeviceLinkState) {
        let mut shard = self.shard(connection_id);
        if let Some(existing) = shard.get_mut(connection_id) {
            existing.device = device;
            return;
        }
        let id: Arc<str> = Arc::from(connection_id);
        shard.insert(
            Arc::clone(&id),
            WatchedConnection {
                id,
                device,
                state,
                ring: VecDeque::new(),
                timer: None,
                probe: None,
                handing_off: false,
            },
        );
    }

    /// 连接结束：环里还没确认的条目交接给推送通道（托管中的连接按 `Parked`，其余按
    /// `LinkLost`），然后清掉这条连接的全部状态。
    pub async fn unregister(&self, connection_id: &str) {
        if let Some(handoff) = self.remove(connection_id) {
            debug!(
                connection_id = %connection_id,
                user_id = %handoff.user_id,
                device_id = %handoff.device_id,
                cause = handoff.cause,
                pending = handoff.pending.len(),
                "connection closed with unconfirmed alerts: handed to push"
            );
            self.hand_off(handoff);
        }
        // 探活量到的往返时延记在质量服务里，随连接一起清掉。
        self.quality.remove_connection(connection_id).await;
    }

    /// 客户端经 DEVICE_STATE 报来了设备状态。连接没在跟踪（还没登记或已断开）时返回 `false`，
    /// 状态没有生效。
    ///
    /// 能发来上报就说明进程还活着：待关闭取消，与 PONG 相同。此外：
    ///
    /// - 报告回到前台：立即交接结束，前台的核心重新负责提示，之后写出的可提示帧恢复等确认。
    /// - 报告 `Deferred`（后台、自己不提示）：此后它的提示由推送路由在路由时直接推送，这条
    ///   连接不再跟踪，立即交接随之结束。环里还没确认的条目都是它声明之前写出的，路由没有为
    ///   它们推送过，现在也等不到确认了：立刻交接。
    pub fn set_device_state(&self, connection_id: &str, state: DeviceLinkState) -> bool {
        let handoff = {
            let mut shard = self.shard(connection_id);
            let Some(connection) = shard.get_mut(connection_id) else {
                return false;
            };
            connection.state = state;
            connection.cancel_probe();
            match state.app_state {
                DeviceAppState::Active => {
                    self.leave_immediate_handoff(connection);
                    None
                }
                DeviceAppState::Deferred => {
                    self.leave_immediate_handoff(connection);
                    if connection.ring.is_empty() {
                        None
                    } else {
                        let handoff = self.handoff(
                            connection,
                            DeviceHandoffCause::AckTimeout,
                            connection.ring.iter().map(|pending| &pending.item),
                        );
                        connection.ring.clear();
                        Some(handoff)
                    }
                }
                _ => None,
            }
        };
        if let Some(handoff) = handoff {
            debug!(
                connection_id = %connection_id,
                user_id = %handoff.user_id,
                device_id = %handoff.device_id,
                pending = handoff.pending.len(),
                "device deferred its alerts to push: unconfirmed entries handed off"
            );
            self.hand_off(handoff);
        }
        true
    }

    /// 连接当前的设备状态。
    pub fn device_state(&self, connection_id: &str) -> Option<DeviceLinkState> {
        self.shard(connection_id)
            .get(connection_id)
            .map(|connection| connection.state)
    }

    /// 一帧消息 / 事件即将写给 `connection_ids`：`items` 是其中需要送达确认的内容（没有就是空）。
    /// 返回时 `connection_ids` 只剩真正要写的连接。
    ///
    /// - 托管中的连接不再收消息帧：客户端已被告知此后的提示归推送通道，可提示的内容直接交接。
    /// - 声明了 `Deferred` 的连接照常写，不跟踪也不交接：推送路由已经直接给这台设备推送。
    /// - 处于立即交接状态的连接照常写，可提示的内容同时交接，不入环。
    /// - 其余连接把可提示的内容记进环里等确认。
    /// - 没在跟踪的连接（Online 会话还没登记完）照常写，不跟踪。
    ///
    /// 必须在写出之前调用：确认只会晚于写出到达，先登记不会漏清；写失败的连接等不到确认，
    /// 到期后同样转给推送。
    pub fn admit(&self, connection_ids: &mut Vec<String>, items: &[DeliveryItem]) {
        self.admit_at(connection_ids, items, Instant::now());
    }

    fn admit_at(&self, connection_ids: &mut Vec<String>, items: &[DeliveryItem], now: Instant) {
        let mut handoffs = Vec::new();
        let mut timers = Vec::new();
        connection_ids.retain(|connection_id| {
            let mut shard = self.shard(connection_id);
            let Some(connection) = shard.get_mut(connection_id.as_str()) else {
                return true;
            };
            if connection.state.is_parking() {
                if !items.is_empty() {
                    handoffs.push(self.handoff(
                        connection,
                        DeviceHandoffCause::Parked,
                        items.iter(),
                    ));
                }
                return false;
            }
            if items.is_empty() || connection.state.is_deferred() {
                return true;
            }
            if connection.handing_off {
                handoffs.push(self.handoff(
                    connection,
                    DeviceHandoffCause::AckTimeout,
                    items.iter(),
                ));
                return true;
            }
            let mut evicted = Vec::new();
            for item in items {
                if connection.ring.len() >= RING_CAPACITY
                    && let Some(oldest) = connection.ring.pop_front()
                {
                    evicted.push(oldest.item);
                }
                connection.ring.push_back(PendingAlert {
                    item: item.clone(),
                    written_at: now,
                });
            }
            if !evicted.is_empty() {
                handoffs.push(self.handoff(
                    connection,
                    DeviceHandoffCause::AckTimeout,
                    evicted.iter(),
                ));
            }
            if connection.timer.is_none() {
                // 先按可能的最短期限排一项；到点时再按当时的状态与往返时延算真正的期限。
                timers.push(self.schedule(connection, now + self.earliest_deadline()));
            }
            true
        });
        if !timers.is_empty() {
            self.lock_timers().extend(timers);
        }
        for handoff in handoffs {
            self.hand_off(handoff);
        }
    }

    /// 客户端确认了某个会话的送达：清掉这条连接的环里被它覆盖的条目。
    ///
    /// 回了送达确认，说明对端在处理下行、也在负责提示（不管确认的是哪个会话）：待关闭取消，
    /// 立即交接结束，之后写出的可提示帧恢复入环等确认。
    pub fn confirm(&self, connection_id: &str, ack: &ConversationAck) {
        let mut shard = self.shard(connection_id);
        let Some(connection) = shard.get_mut(connection_id) else {
            return;
        };
        connection
            .ring
            .retain(|pending| !pending.item.confirmed_by(ack));
        connection.cancel_probe();
        self.leave_immediate_handoff(connection);
    }

    /// 收到 PONG：取消待关闭，并把这次探活的往返时延记进质量服务。立即交接不受影响；
    /// 连接不在探活中时什么都不做。
    pub async fn on_pong(&self, connection_id: &str) {
        self.on_pong_at(connection_id, Instant::now()).await;
    }

    async fn on_pong_at(&self, connection_id: &str, now: Instant) {
        let Some((rtt, user_id, device_id)) = self.end_probe(connection_id, now) else {
            return;
        };
        self.quality
            .record_heartbeat_rtt(
                connection_id,
                &user_id,
                &device_id,
                i64::try_from(rtt.as_millis()).unwrap_or(i64::MAX),
            )
            .await;
    }

    /// 取消待关闭，返回这次探活的往返时延与设备标识；连接不在探活中时返回 `None`。
    fn end_probe(&self, connection_id: &str, now: Instant) -> Option<(Duration, String, String)> {
        let mut shard = self.shard(connection_id);
        let connection = shard.get_mut(connection_id)?;
        let probe = connection.cancel_probe()?;
        Some((
            now.saturating_duration_since(probe.sent_at),
            connection.device.user_id.clone(),
            connection.device.device_id.clone(),
        ))
    }

    /// 结束立即交接（不在这个状态时什么都不做）。
    fn leave_immediate_handoff(&self, connection: &mut WatchedConnection) {
        if std::mem::take(&mut connection.handing_off) {
            self.metrics.immediate_handoff_left();
        }
    }

    /// 给连接登记一个新的期限项。调用方放开分片锁后把返回值推进期限队列。
    fn schedule(&self, connection: &mut WatchedConnection, at: Instant) -> Reverse<Timer> {
        let timer = self.next_timer.fetch_add(1, Ordering::Relaxed);
        connection.timer = Some(timer);
        Reverse((at, timer, Arc::clone(&connection.id)))
    }

    /// 上报一次交接并计数。
    fn hand_off(&self, handoff: DeviceHandoff) {
        self.metrics
            .record_device_handoff(handoff_cause_label(handoff.cause));
        self.handoffs.report(handoff);
    }

    /// 处理到期的期限项，返回要对链路做的动作；交接在这里直接上报。
    pub(crate) async fn sweep(&self, now: Instant) -> Vec<LinkAction> {
        let mut actions = Vec::new();
        for (_, timer, connection_id) in self.take_due(now) {
            // 往返时延只在这里查：写出路径不碰质量服务的锁。
            let rtt = self.quality.rtt(&connection_id).await;
            if let Some(action) = self.on_timer(&connection_id, timer, now, rtt) {
                actions.push(action);
            }
        }
        actions
    }

    /// 执行超时动作。逐个放进独立任务：半开连接上的写可能迟迟不返回，不能拖住期限检查。
    fn dispatch(&self, actions: Vec<LinkAction>) {
        for action in actions {
            let probe = Arc::clone(&self.probe);
            tokio::spawn(async move {
                match action {
                    LinkAction::Ping(connection_id) => {
                        if let Err(error) = probe.ping(&connection_id).await {
                            debug!(
                                connection_id = %connection_id,
                                %error,
                                "delivery probe PING not written"
                            );
                        }
                    }
                    LinkAction::Close(connection_id) => {
                        info!(
                            connection_id = %connection_id,
                            "no PONG or delivery ack within the probe timeout: closing the connection"
                        );
                        if let Err(error) = probe.close(&connection_id).await {
                            debug!(
                                connection_id = %connection_id,
                                %error,
                                "probed connection already gone"
                            );
                        }
                    }
                }
            });
        }
    }

    fn on_timer(
        &self,
        connection_id: &Arc<str>,
        timer: u64,
        now: Instant,
        rtt: Option<Duration>,
    ) -> Option<LinkAction> {
        let mut rearm = None;
        let mut handoff = None;
        let action = {
            let mut shard = self.shard(connection_id);
            let connection = shard.get_mut(&**connection_id)?;
            if connection.timer != Some(timer) {
                // 作废的旧项：待关闭已取消，或者连接断开后同一 ID 又登记过。
                return None;
            }
            let oldest_written_at = connection.ring.front().map(|pending| pending.written_at);
            if let Some(probe) = connection.probe {
                if now >= probe.deadline {
                    // 探活期限内对端毫无动静（没有 PONG、设备状态上报或送达确认）：关闭连接。
                    connection.timer = None;
                    self.metrics.record_connection_probe_closed();
                    Some(LinkAction::Close(Arc::clone(connection_id)))
                } else {
                    rearm = Some(probe.deadline);
                    None
                }
            } else if let Some(written_at) = oldest_written_at {
                let due = written_at + self.ack_deadline(connection.state.app_state, rtt);
                if now >= due {
                    handoff = Some(self.handoff(
                        connection,
                        DeviceHandoffCause::AckTimeout,
                        connection.ring.iter().map(|pending| &pending.item),
                    ));
                    // 交接出去的条目归推送通道负责，不再留在环里等确认。
                    connection.ring.clear();
                    // 待关闭与立即交接同时开始，之后各自按自己的条件结束。
                    let deadline = now + self.config.probe_timeout;
                    connection.probe = Some(Probe {
                        sent_at: now,
                        deadline,
                    });
                    if !std::mem::replace(&mut connection.handing_off, true) {
                        self.metrics.immediate_handoff_entered();
                    }
                    self.metrics.record_delivery_ack_timeout();
                    rearm = Some(deadline);
                    Some(LinkAction::Ping(Arc::clone(connection_id)))
                } else {
                    rearm = Some(due);
                    None
                }
            } else {
                // 环已清空：这条连接退出期限队列，下次有可提示帧写入时再排。
                connection.timer = None;
                None
            }
        };
        if let Some(at) = rearm {
            self.lock_timers()
                .push(Reverse((at, timer, Arc::clone(connection_id))));
        }
        if let Some(handoff) = handoff {
            info!(
                connection_id = %connection_id,
                user_id = %handoff.user_id,
                device_id = %handoff.device_id,
                pending = handoff.pending.len(),
                "delivery ack overdue: alerts handed to push, probing the link"
            );
            self.hand_off(handoff);
        }
        action
    }

    fn take_due(&self, now: Instant) -> Vec<Timer> {
        let mut timers = self.lock_timers();
        let mut due = Vec::new();
        while timers.peek().is_some_and(|Reverse((at, _, _))| *at <= now) {
            if let Some(Reverse(timer)) = timers.pop() {
                due.push(timer);
            }
        }
        due
    }

    fn remove(&self, connection_id: &str) -> Option<DeviceHandoff> {
        let connection = self.shard(connection_id).remove(connection_id)?;
        if connection.handing_off {
            self.metrics.immediate_handoff_left();
        }
        // `Deferred` 的连接环一定是空的（声明时清掉，之后不入环），断开时没有可交接的。
        if connection.ring.is_empty() {
            return None;
        }
        let cause = if connection.state.is_parking() {
            DeviceHandoffCause::Parked
        } else {
            DeviceHandoffCause::LinkLost
        };
        Some(self.handoff(
            &connection,
            cause,
            connection.ring.iter().map(|pending| &pending.item),
        ))
    }

    /// 等送达确认的期限：按设备状态取上限；往返时延已知时取
    /// `clamp(4 × RTT + 500ms, 1s, 上限)`，好链路上更早发现没确认的帧。
    fn ack_deadline(&self, app_state: DeviceAppState, rtt: Option<Duration>) -> Duration {
        let ceiling = match app_state {
            DeviceAppState::Active => self.config.active_deadline,
            // 后台与托管中的进程随时可能被挂起，按较短的期限等。`Deferred` 的连接不跟踪，
            // 环是空的，走不到这里。
            _ => self.config.standby_deadline,
        };
        match rtt {
            // 先抬到下限再压到上限：上限配得比下限还小时以上限为准。
            Some(rtt) => rtt
                .saturating_mul(4)
                .saturating_add(RTT_MARGIN)
                .max(DEADLINE_FLOOR)
                .min(ceiling),
            None => ceiling,
        }
    }

    /// 任何状态、任何往返时延下都不会更早到期的时间：新条目先按它排期。
    fn earliest_deadline(&self) -> Duration {
        DEADLINE_FLOOR
            .min(self.config.active_deadline)
            .min(self.config.standby_deadline)
    }

    /// 组一次交接。条目按写出顺序，最多带一个环的量：一帧里塞了更多可提示内容时只留最新的那些
    /// ——推送侧按会话取最新一条补提示，更早的条目不改变结果，而过大的交接会被 Push Proxy 拒收。
    fn handoff<'a>(
        &self,
        connection: &WatchedConnection,
        cause: DeviceHandoffCause,
        items: impl Iterator<Item = &'a DeliveryItem>,
    ) -> DeviceHandoff {
        let mut pending: Vec<DeviceHandoffItem> = items
            .map(|item| DeviceHandoffItem {
                conversation_id: item.conversation_id.to_string(),
                seq: item.seq,
                message_id: item.message_id.to_string(),
            })
            .collect();
        if pending.len() > RING_CAPACITY {
            pending.drain(..pending.len() - RING_CAPACITY);
        }
        DeviceHandoff {
            tenant_id: connection.device.tenant_id.clone(),
            user_id: connection.device.user_id.clone(),
            device_id: connection.device.device_id.clone(),
            connection_id: connection.id.to_string(),
            gateway_id: self.gateway_id.clone(),
            cause: cause as i32,
            since: 0,
            occurred_at: unix_millis(),
            pending,
        }
    }

    fn shard(&self, connection_id: &str) -> MutexGuard<'_, Shard> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        connection_id.hash(&mut hasher);
        let index = (hasher.finish() % self.shards.len() as u64) as usize;
        self.shards[index]
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn lock_timers(&self) -> MutexGuard<'_, BinaryHeap<Reverse<Timer>>> {
        self.timers.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// 这条连接还在等确认的条目（测试观察用）。
    #[cfg(test)]
    pub(crate) fn pending(&self, connection_id: &str) -> Vec<DeliveryItem> {
        self.shard(connection_id)
            .get(connection_id)
            .map(|connection| {
                connection
                    .ring
                    .iter()
                    .map(|pending| pending.item.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    #[cfg(test)]
    fn timer_count(&self) -> usize {
        self.lock_timers().len()
    }
}

/// 交接原因在指标里的标签。
fn handoff_cause_label(cause: i32) -> &'static str {
    match DeviceHandoffCause::try_from(cause) {
        Ok(DeviceHandoffCause::Parked) => "parked",
        Ok(DeviceHandoffCause::AckTimeout) => "ack_timeout",
        Ok(DeviceHandoffCause::LinkLost) => "link_lost",
        Ok(DeviceHandoffCause::GatewayLost) => "gateway_lost",
        Ok(DeviceHandoffCause::Unspecified) | Err(_) => "unspecified",
    }
}

fn unix_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// 测试替身：记录交接与探活动作的端口，以及接好它们的 [`DeliveryWatch`]。
#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use async_trait::async_trait;
    use flare_server_core::error::Result;

    #[derive(Default)]
    pub(crate) struct RecordingHandoffs {
        reported: Mutex<Vec<DeviceHandoff>>,
    }

    impl RecordingHandoffs {
        /// 取走到目前为止上报的交接。
        pub(crate) fn take(&self) -> Vec<DeviceHandoff> {
            std::mem::take(
                &mut *self
                    .reported
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            )
        }
    }

    impl IDeviceHandoffPort for RecordingHandoffs {
        fn report(&self, handoff: DeviceHandoff) {
            self.reported
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(handoff);
        }
    }

    #[derive(Default)]
    pub(crate) struct RecordingProbe {
        pub(crate) pings: Mutex<Vec<String>>,
        pub(crate) closes: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl ILinkProbePort for RecordingProbe {
        async fn ping(&self, connection_id: &str) -> Result<()> {
            self.pings
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(connection_id.to_string());
            Ok(())
        }

        async fn close(&self, connection_id: &str) -> Result<()> {
            self.closes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(connection_id.to_string());
            Ok(())
        }
    }

    pub(crate) struct WatchFixture {
        pub(crate) watch: Arc<DeliveryWatch>,
        pub(crate) handoffs: Arc<RecordingHandoffs>,
        pub(crate) probe: Arc<RecordingProbe>,
        pub(crate) quality: Arc<ConnectionQualityService>,
        pub(crate) metrics: Arc<AccessGatewayMetrics>,
    }

    impl WatchFixture {
        /// 指标里按原因记下的交接数。
        pub(crate) fn handoffs_counted(&self, cause: &str) -> u64 {
            self.metrics
                .device_handoffs_total
                .with_label_values(&[cause])
                .get()
        }
    }

    pub(crate) fn fixture(config: DeliveryWatchConfig) -> WatchFixture {
        let handoffs = Arc::new(RecordingHandoffs::default());
        let probe = Arc::new(RecordingProbe::default());
        let quality = Arc::new(ConnectionQualityService::new());
        let metrics = Arc::new(AccessGatewayMetrics::new());
        let watch = Arc::new(DeliveryWatch::new(
            "gateway-test".to_string(),
            config,
            handoffs.clone(),
            probe.clone(),
            quality.clone(),
            metrics.clone(),
        ));
        WatchFixture {
            watch,
            handoffs,
            probe,
            quality,
            metrics,
        }
    }

    /// 不关心交接与探活的用例用它：默认期限，后台任务没有启动。
    pub(crate) fn detached() -> Arc<DeliveryWatch> {
        fixture(DeliveryWatchConfig::default()).watch
    }

    pub(crate) fn device(user_id: &str) -> WatchedDevice {
        WatchedDevice {
            tenant_id: "t1".to_string(),
            user_id: user_id.to_string(),
            device_id: format!("dev-{user_id}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::{WatchFixture, detached, device, fixture};
    use super::*;
    use flare_proto::common::message_content::Content;
    use flare_proto::common::{Event, EventType, MessageContent, NotificationContent};

    const PARKING: DeviceLinkState = DeviceLinkState {
        app_state: DeviceAppState::Parking,
        attention: flare_proto::common::DeviceAttention::Unspecified,
    };
    const STANDBY: DeviceLinkState = DeviceLinkState {
        app_state: DeviceAppState::Standby,
        attention: flare_proto::common::DeviceAttention::Unspecified,
    };
    const DEFERRED: DeviceLinkState = DeviceLinkState {
        app_state: DeviceAppState::Deferred,
        attention: flare_proto::common::DeviceAttention::Unspecified,
    };

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    fn item(conversation_id: &str, seq: u64) -> DeliveryItem {
        DeliveryItem::new(conversation_id, seq, &format!("m-{seq}"))
    }

    fn ack(conversation_id: &str, last_delivered_seq: u64) -> ConversationAck {
        ConversationAck {
            conversation_id: conversation_id.to_string(),
            last_delivered_seq,
            ..Default::default()
        }
    }

    fn pending_of(handoff: &DeviceHandoff) -> Vec<(String, u64)> {
        handoff
            .pending
            .iter()
            .map(|item| (item.conversation_id.clone(), item.seq))
            .collect()
    }

    /// 登记一条前台连接并写一帧可提示的消息，返回写出时刻。
    fn written(f: &WatchFixture, connection_id: &str, items: &[DeliveryItem]) -> Instant {
        f.watch
            .register(connection_id, device("u1"), DeviceLinkState::FOREGROUND);
        let at = Instant::now();
        let mut targets = vec![connection_id.to_string()];
        f.watch.admit_at(&mut targets, items, at);
        assert_eq!(targets, [connection_id], "a live connection is written to");
        at
    }

    #[test]
    fn an_ack_clears_that_conversation_up_to_the_acked_seq() {
        let f = fixture(DeliveryWatchConfig::default());
        written(&f, "c1", &[item("g1", 5), item("g1", 6), item("g2", 9)]);
        f.watch
            .register("c2", device("u2"), DeviceLinkState::FOREGROUND);
        let mut targets = vec!["c2".to_string()];
        f.watch
            .admit_at(&mut targets, &[item("g1", 5)], Instant::now());

        f.watch.confirm("c1", &ack("g1", 5));
        assert_eq!(f.watch.pending("c1"), [item("g1", 6), item("g2", 9)]);
        // 只清发确认的那条连接。
        assert_eq!(f.watch.pending("c2"), [item("g1", 5)]);

        // 逐条点到消息 ID 的确认也算数。
        f.watch.confirm(
            "c1",
            &ConversationAck {
                conversation_id: "g2".to_string(),
                server_msg_ids: vec!["m-9".to_string()],
                ..Default::default()
            },
        );
        assert_eq!(f.watch.pending("c1"), [item("g1", 6)]);

        f.watch.confirm("c1", &ack("g1", 100));
        assert!(f.watch.pending("c1").is_empty());
        // 没在跟踪的连接发来确认：忽略。
        f.watch.confirm("ghost", &ack("g1", 100));
        assert!(f.handoffs.take().is_empty());
    }

    #[tokio::test]
    async fn an_unconfirmed_alert_is_handed_off_probed_and_the_dead_link_closed() {
        let f = fixture(DeliveryWatchConfig::default());
        let t0 = written(&f, "c1", &[item("g1", 5), item("g2", 7)]);

        // 前台期限 3s 之内不动作。
        assert!(f.watch.sweep(t0 + ms(2_900)).await.is_empty());
        assert!(f.handoffs.take().is_empty());

        let actions = f.watch.sweep(t0 + ms(3_000)).await;
        assert_eq!(actions, [LinkAction::Ping(Arc::from("c1"))]);
        let handoffs = f.handoffs.take();
        assert_eq!(handoffs.len(), 1);
        let handoff = &handoffs[0];
        assert_eq!(handoff.cause, DeviceHandoffCause::AckTimeout as i32);
        assert_eq!(handoff.tenant_id, "t1");
        assert_eq!(handoff.user_id, "u1");
        assert_eq!(handoff.device_id, "dev-u1");
        assert_eq!(handoff.connection_id, "c1");
        assert_eq!(handoff.gateway_id, "gateway-test");
        assert!(handoff.occurred_at > 0);
        assert_eq!(
            pending_of(handoff),
            [("g1".to_string(), 5), ("g2".to_string(), 7)]
        );
        assert_eq!(handoff.pending[0].message_id, "m-5");
        assert!(
            f.watch.pending("c1").is_empty(),
            "handed-off entries leave the ring"
        );
        assert_eq!(f.metrics.delivery_ack_timeouts_total.get(), 1);
        assert_eq!(f.handoffs_counted("ack_timeout"), 1);
        assert_eq!(f.metrics.connections_immediate_handoff.get(), 1);

        // 探活期限 5s 内没有 PONG 也没有确认：关闭，且只关一次。
        assert!(f.watch.sweep(t0 + ms(7_900)).await.is_empty());
        assert_eq!(
            f.watch.sweep(t0 + ms(8_000)).await,
            [LinkAction::Close(Arc::from("c1"))]
        );
        assert!(f.watch.sweep(t0 + ms(60_000)).await.is_empty());
        assert_eq!(f.watch.timer_count(), 0);
        assert!(f.handoffs.take().is_empty());
        assert_eq!(f.metrics.connections_probe_closed_total.get(), 1);

        // 关闭带来的断开把这条连接的状态清掉，立即交接的计数随之归零。
        f.watch.unregister("c1").await;
        assert_eq!(f.metrics.connections_immediate_handoff.get(), 0);
    }

    /// PONG 只取消待关闭：连接保留，但它仍然没在确认送达，可提示的帧继续当场交接；
    /// 回了送达确认才回到正常跟踪。
    #[tokio::test]
    async fn a_pong_keeps_the_connection_but_its_alerts_keep_going_to_push_until_an_ack() {
        let f = fixture(DeliveryWatchConfig::default());
        let t0 = written(&f, "c1", &[item("g1", 5)]);
        assert_eq!(f.watch.sweep(t0 + ms(3_000)).await.len(), 1);
        f.handoffs.take();

        // PONG 在探活发出 1s 后到达：不再关连接，往返时延记进质量服务。
        f.watch.on_pong_at("c1", t0 + ms(4_000)).await;
        assert_eq!(f.quality.rtt("c1").await, Some(ms(1_000)));
        assert!(
            f.watch.sweep(t0 + ms(8_000)).await.is_empty(),
            "a PONG cancels the pending close"
        );
        assert_eq!(f.metrics.connections_probe_closed_total.get(), 0);
        assert_eq!(f.metrics.connections_immediate_handoff.get(), 1);

        // 仍在立即交接：帧照写，可提示的内容不等期限、当场交接、不入环；不提示的帧不受影响。
        let mut targets = vec!["c1".to_string()];
        f.watch
            .admit_at(&mut targets, &[item("g1", 6)], t0 + ms(9_000));
        assert_eq!(targets, ["c1"]);
        f.watch.admit_at(&mut targets, &[], t0 + ms(9_100));
        assert_eq!(targets, ["c1"]);
        let handoffs = f.handoffs.take();
        assert_eq!(handoffs.len(), 1);
        assert_eq!(handoffs[0].cause, DeviceHandoffCause::AckTimeout as i32);
        assert_eq!(pending_of(&handoffs[0]), [("g1".to_string(), 6)]);
        assert!(f.watch.pending("c1").is_empty());
        // 没有在等确认的条目，也就不会再超时、再探活。
        assert!(f.watch.sweep(t0 + ms(60_000)).await.is_empty());
        assert_eq!(f.metrics.delivery_ack_timeouts_total.get(), 1);
        assert_eq!(f.handoffs_counted("ack_timeout"), 2);

        // 回了送达确认：恢复入环等确认。
        f.watch.confirm("c1", &ack("g1", 6));
        assert_eq!(f.metrics.connections_immediate_handoff.get(), 0);
        f.watch
            .admit_at(&mut targets, &[item("g1", 7)], t0 + ms(61_000));
        assert_eq!(f.watch.pending("c1"), [item("g1", 7)]);
        assert!(f.handoffs.take().is_empty());

        // 不在探活中的 PONG 不产生样本，也不出错。
        f.watch.on_pong_at("ghost", t0 + ms(61_000)).await;
        assert_eq!(f.quality.rtt("ghost").await, None);
    }

    #[tokio::test]
    async fn any_delivery_ack_cancels_the_close_and_ends_immediate_handoff() {
        let f = fixture(DeliveryWatchConfig::default());
        let t0 = written(&f, "c1", &[item("g1", 5)]);
        assert_eq!(f.watch.sweep(t0 + ms(3_000)).await.len(), 1);
        f.handoffs.take();

        // 确认的是别的会话也算：对端在处理下行，也在负责提示。
        f.watch.confirm("c1", &ack("another", 1));
        assert_eq!(f.metrics.connections_immediate_handoff.get(), 0);
        assert!(f.watch.sweep(t0 + ms(8_000)).await.is_empty());
        let mut targets = vec!["c1".to_string()];
        f.watch
            .admit_at(&mut targets, &[item("g1", 6)], t0 + ms(8_100));
        assert_eq!(f.watch.pending("c1"), [item("g1", 6)]);
        assert!(f.handoffs.take().is_empty());
    }

    /// 任何一次设备状态上报都证明进程还活着，取消待关闭；只有报告回到前台才结束立即交接。
    #[tokio::test]
    async fn a_device_state_report_cancels_the_close_and_only_active_ends_immediate_handoff() {
        let f = fixture(DeliveryWatchConfig::default());
        let t0 = written(&f, "c1", &[item("g1", 5)]);
        assert!(f.watch.set_device_state("c1", STANDBY));
        assert_eq!(
            f.watch.sweep(t0 + ms(1_500)).await,
            [LinkAction::Ping(Arc::from("c1"))]
        );
        f.handoffs.take();

        // 仍报后台：进程活着，不再关连接；但它没在提示用户，可提示的帧继续当场交接。
        assert!(f.watch.set_device_state("c1", STANDBY));
        assert!(
            f.watch.sweep(t0 + ms(6_500)).await.is_empty(),
            "a device state report cancels the pending close"
        );
        assert_eq!(f.metrics.connections_probe_closed_total.get(), 0);
        assert_eq!(f.metrics.connections_immediate_handoff.get(), 1);
        let mut targets = vec!["c1".to_string()];
        f.watch
            .admit_at(&mut targets, &[item("g1", 6)], t0 + ms(7_000));
        assert_eq!(targets, ["c1"]);
        assert_eq!(f.handoffs.take().len(), 1);
        assert!(f.watch.pending("c1").is_empty());

        // 报告回到前台：恢复入环等确认。
        assert!(f.watch.set_device_state("c1", DeviceLinkState::FOREGROUND));
        assert_eq!(f.metrics.connections_immediate_handoff.get(), 0);
        f.watch
            .admit_at(&mut targets, &[item("g1", 7)], t0 + ms(7_100));
        assert_eq!(f.watch.pending("c1"), [item("g1", 7)]);
        assert!(f.handoffs.take().is_empty());
    }

    /// 报告回到前台时待关闭还在：立即交接结束、待关闭取消，一步到位回到正常跟踪。
    #[tokio::test]
    async fn reporting_the_foreground_during_a_probe_returns_to_normal_tracking() {
        let f = fixture(DeliveryWatchConfig::default());
        let t0 = written(&f, "c1", &[item("g1", 5)]);
        assert_eq!(f.watch.sweep(t0 + ms(3_000)).await.len(), 1);
        f.handoffs.take();

        assert!(f.watch.set_device_state("c1", DeviceLinkState::FOREGROUND));
        let mut targets = vec!["c1".to_string()];
        f.watch
            .admit_at(&mut targets, &[item("g1", 6)], t0 + ms(4_000));
        assert_eq!(f.watch.pending("c1"), [item("g1", 6)]);

        // 原先的探活期限（t0 + 8s）作废；新条目按前台期限 3s 在 t0 + 7s 超时。
        assert!(f.watch.sweep(t0 + ms(6_900)).await.is_empty());
        assert_eq!(
            f.watch.sweep(t0 + ms(7_000)).await,
            [LinkAction::Ping(Arc::from("c1"))]
        );
        assert_eq!(f.metrics.connections_probe_closed_total.get(), 0);
        assert_eq!(f.metrics.delivery_ack_timeouts_total.get(), 2);
    }

    /// 声明了 `Deferred` 的连接每一帧都照常收，但完全不跟踪：不入环、没有期限、不交接、
    /// 不探活；断开时也没有可交接的。
    #[tokio::test]
    async fn a_deferred_connection_receives_every_frame_and_is_never_watched() {
        let f = fixture(DeliveryWatchConfig::default());
        f.watch.register("c1", device("u1"), DEFERRED);
        let t0 = Instant::now();
        let mut targets = vec!["c1".to_string()];
        f.watch.admit_at(&mut targets, &[item("g1", 5)], t0);
        assert_eq!(targets, ["c1"]);
        f.watch.admit_at(&mut targets, &[], t0);
        assert_eq!(targets, ["c1"]);
        assert!(f.watch.pending("c1").is_empty());
        assert_eq!(f.watch.timer_count(), 0);

        assert!(f.watch.sweep(t0 + ms(60_000)).await.is_empty());
        f.watch.unregister("c1").await;
        assert!(f.handoffs.take().is_empty());
        assert_eq!(f.metrics.delivery_ack_timeouts_total.get(), 0);
        assert_eq!(f.metrics.connections_immediate_handoff.get(), 0);
        for cause in ["parked", "ack_timeout", "link_lost"] {
            assert_eq!(f.handoffs_counted(cause), 0, "{cause}");
        }
    }

    /// 前台 → `Deferred`：声明之前写出、还没确认的条目，路由没有为它们推送过，声明的那一刻
    /// 立刻交接；此后写出的帧不再跟踪。重复声明、断开都不会再交接。
    #[tokio::test]
    async fn declaring_deferred_hands_off_what_was_written_before_and_stops_watching() {
        let f = fixture(DeliveryWatchConfig::default());
        let t0 = written(&f, "c1", &[item("g1", 5), item("g2", 7)]);

        assert!(f.watch.set_device_state("c1", DEFERRED));
        let handoffs = f.handoffs.take();
        assert_eq!(handoffs.len(), 1);
        assert_eq!(handoffs[0].cause, DeviceHandoffCause::AckTimeout as i32);
        assert_eq!(handoffs[0].connection_id, "c1");
        assert_eq!(
            pending_of(&handoffs[0]),
            [("g1".to_string(), 5), ("g2".to_string(), 7)]
        );
        assert!(f.watch.pending("c1").is_empty());
        assert_eq!(f.handoffs_counted("ack_timeout"), 1);

        // 声明之后写出的帧：照写，不入环，不交接。
        let mut targets = vec!["c1".to_string()];
        f.watch
            .admit_at(&mut targets, &[item("g1", 8)], t0 + ms(500));
        assert_eq!(targets, ["c1"]);
        assert!(f.watch.pending("c1").is_empty());
        // 声明之前排下的期限到点时环已清空：不算超时，不探活。
        assert!(f.watch.sweep(t0 + ms(60_000)).await.is_empty());
        assert_eq!(f.metrics.delivery_ack_timeouts_total.get(), 0);

        assert!(f.watch.set_device_state("c1", DEFERRED));
        f.watch.unregister("c1").await;
        assert!(f.handoffs.take().is_empty());
    }

    /// 声明 `Deferred` 时正在待关闭、立即交接：两者都结束，之后的可提示帧不再当场交接。
    #[tokio::test]
    async fn declaring_deferred_cancels_the_close_and_ends_immediate_handoff() {
        let f = fixture(DeliveryWatchConfig::default());
        let t0 = written(&f, "c1", &[item("g1", 5)]);
        assert!(f.watch.set_device_state("c1", STANDBY));
        assert_eq!(f.watch.sweep(t0 + ms(1_500)).await.len(), 1);
        f.handoffs.take();
        assert_eq!(f.metrics.connections_immediate_handoff.get(), 1);

        assert!(f.watch.set_device_state("c1", DEFERRED));
        assert_eq!(f.metrics.connections_immediate_handoff.get(), 0);
        let mut targets = vec!["c1".to_string()];
        f.watch
            .admit_at(&mut targets, &[item("g1", 6)], t0 + ms(2_000));
        assert_eq!(targets, ["c1"]);
        assert!(f.watch.sweep(t0 + ms(6_500)).await.is_empty());
        assert!(f.handoffs.take().is_empty());
        assert_eq!(f.metrics.connections_probe_closed_total.get(), 0);
    }

    /// 离开 `Deferred`（回到后台自己提示，或回到前台）：从那一刻起写出的帧恢复跟踪，
    /// 之前在 `Deferred` 期间写出的不补记。
    #[tokio::test]
    async fn leaving_deferred_resumes_watching_frames_written_from_then_on() {
        let f = fixture(DeliveryWatchConfig::default());
        f.watch.register("c1", device("u1"), DEFERRED);
        let t0 = Instant::now();
        let mut targets = vec!["c1".to_string()];
        f.watch.admit_at(&mut targets, &[item("g1", 5)], t0);

        assert!(f.watch.set_device_state("c1", STANDBY));
        assert!(f.handoffs.take().is_empty());
        f.watch
            .admit_at(&mut targets, &[item("g1", 6)], t0 + ms(1_000));
        assert_eq!(f.watch.pending("c1"), [item("g1", 6)]);

        // 后台期限 1.5s：只有离开之后写出的那一条超时、交接。
        assert!(f.watch.sweep(t0 + ms(2_400)).await.is_empty());
        assert_eq!(
            f.watch.sweep(t0 + ms(2_500)).await,
            [LinkAction::Ping(Arc::from("c1"))]
        );
        let handoffs = f.handoffs.take();
        assert_eq!(handoffs.len(), 1);
        assert_eq!(pending_of(&handoffs[0]), [("g1".to_string(), 6)]);

        // 回到前台同理。
        assert!(f.watch.set_device_state("c1", DEFERRED));
        assert!(f.watch.set_device_state("c1", DeviceLinkState::FOREGROUND));
        f.watch
            .admit_at(&mut targets, &[item("g1", 7)], t0 + ms(3_000));
        assert_eq!(f.watch.pending("c1"), [item("g1", 7)]);
    }

    #[tokio::test]
    async fn a_standby_connection_times_out_sooner_and_a_known_rtt_tightens_it() {
        let f = fixture(DeliveryWatchConfig::default());
        let t0 = written(&f, "slow", &[item("g1", 5)]);
        assert!(f.watch.set_device_state("slow", STANDBY));
        assert!(f.watch.sweep(t0 + ms(1_400)).await.is_empty());
        assert_eq!(
            f.watch.sweep(t0 + ms(1_500)).await,
            [LinkAction::Ping(Arc::from("slow"))]
        );

        // 往返 50ms 的链路：4 × 50 + 500 = 700ms，抬到下限 1s。
        f.quality
            .record_heartbeat_rtt("fast", "u1", "dev-u1", 50)
            .await;
        let t1 = written(&f, "fast", &[item("g1", 5)]);
        assert!(f.watch.sweep(t1 + ms(999)).await.is_empty());
        assert_eq!(
            f.watch.sweep(t1 + ms(1_000)).await,
            [LinkAction::Ping(Arc::from("fast"))]
        );
    }

    #[test]
    fn the_deadline_follows_state_and_rtt_within_bounds() {
        let watch = detached();
        assert_eq!(watch.ack_deadline(DeviceAppState::Active, None), ms(3_000));
        assert_eq!(watch.ack_deadline(DeviceAppState::Standby, None), ms(1_500));
        assert_eq!(watch.ack_deadline(DeviceAppState::Parking, None), ms(1_500));
        assert_eq!(
            watch.ack_deadline(DeviceAppState::Active, Some(ms(50))),
            ms(1_000)
        );
        // 4 × 300 + 500 = 1.7s：前台直接用，后台压到 1.5s。
        assert_eq!(
            watch.ack_deadline(DeviceAppState::Active, Some(ms(300))),
            ms(1_700)
        );
        assert_eq!(
            watch.ack_deadline(DeviceAppState::Standby, Some(ms(300))),
            ms(1_500)
        );
        assert_eq!(
            watch.ack_deadline(DeviceAppState::Active, Some(ms(5_000))),
            ms(3_000)
        );
        assert_eq!(watch.earliest_deadline(), ms(1_000));

        // 上限配得比 1s 下限还小：以上限为准，排期也跟着提前。
        let tight = fixture(DeliveryWatchConfig {
            active_deadline: ms(400),
            standby_deadline: ms(200),
            probe_timeout: ms(100),
        })
        .watch;
        assert_eq!(
            tight.ack_deadline(DeviceAppState::Active, Some(ms(10))),
            ms(400)
        );
        assert_eq!(tight.earliest_deadline(), ms(200));
    }

    #[test]
    fn a_parking_connection_gets_no_message_frames_and_its_alerts_go_to_push() {
        let f = fixture(DeliveryWatchConfig::default());
        f.watch
            .register("c1", device("u1"), DeviceLinkState::FOREGROUND);
        f.watch
            .register("c2", device("u2"), DeviceLinkState::FOREGROUND);
        assert!(f.watch.set_device_state("c1", PARKING));
        assert_eq!(f.watch.device_state("c1"), Some(PARKING));
        assert!(!f.watch.set_device_state("ghost", PARKING));

        let mut targets = vec!["c1".to_string(), "c2".to_string(), "unknown".to_string()];
        f.watch.admit(&mut targets, &[item("g1", 5)]);
        assert_eq!(targets, ["c2", "unknown"]);
        let handoffs = f.handoffs.take();
        assert_eq!(handoffs.len(), 1);
        assert_eq!(handoffs[0].cause, DeviceHandoffCause::Parked as i32);
        assert_eq!(handoffs[0].connection_id, "c1");
        assert_eq!(pending_of(&handoffs[0]), [("g1".to_string(), 5)]);
        assert_eq!(f.handoffs_counted("parked"), 1);
        assert!(f.watch.pending("c1").is_empty());
        assert_eq!(f.watch.pending("c2"), [item("g1", 5)]);
        // 没在跟踪的连接照常写，但不留任何状态。
        assert_eq!(f.watch.device_state("unknown"), None);

        // 不提示的消息帧同样不再写给托管中的连接，也没有东西要交接。
        let mut targets = vec!["c1".to_string(), "c2".to_string()];
        f.watch.admit(&mut targets, &[]);
        assert_eq!(targets, ["c2"]);
        assert!(f.handoffs.take().is_empty());
    }

    #[test]
    fn a_full_ring_hands_its_oldest_entry_to_push_instead_of_growing() {
        let f = fixture(DeliveryWatchConfig::default());
        f.watch
            .register("c1", device("u1"), DeviceLinkState::FOREGROUND);
        let mut targets = vec!["c1".to_string()];
        for seq in 1..=RING_CAPACITY as u64 {
            f.watch.admit(&mut targets, &[item("g1", seq)]);
        }
        assert!(f.handoffs.take().is_empty());
        assert_eq!(f.watch.pending("c1").len(), RING_CAPACITY);

        f.watch
            .admit(&mut targets, &[item("g1", 65), item("g1", 66)]);
        let handoffs = f.handoffs.take();
        assert_eq!(handoffs.len(), 1, "one handoff per frame, not per entry");
        assert_eq!(handoffs[0].cause, DeviceHandoffCause::AckTimeout as i32);
        assert_eq!(
            pending_of(&handoffs[0]),
            [("g1".to_string(), 1), ("g1".to_string(), 2)]
        );
        let pending = f.watch.pending("c1");
        assert_eq!(pending.len(), RING_CAPACITY);
        assert_eq!(pending.first(), Some(&item("g1", 3)));
        assert_eq!(pending.last(), Some(&item("g1", 66)));
        // 写了 66 帧，这条连接在期限队列里仍然只占一项。
        assert_eq!(f.watch.timer_count(), 1);
    }

    /// 一次交接最多带一个环的量，留下的是最新的条目。
    #[test]
    fn a_handoff_never_carries_more_than_one_ring_of_entries() {
        let f = fixture(DeliveryWatchConfig::default());
        f.watch.register("c1", device("u1"), PARKING);
        let frame: Vec<DeliveryItem> = (1..=100).map(|seq| item("g1", seq)).collect();
        let mut targets = vec!["c1".to_string()];
        f.watch.admit(&mut targets, &frame);

        assert!(targets.is_empty());
        let handoffs = f.handoffs.take();
        assert_eq!(handoffs.len(), 1);
        assert_eq!(handoffs[0].pending.len(), RING_CAPACITY);
        assert_eq!(handoffs[0].pending.first().map(|item| item.seq), Some(37));
        assert_eq!(handoffs[0].pending.last().map(|item| item.seq), Some(100));
    }

    #[tokio::test]
    async fn a_disconnect_hands_the_unconfirmed_ring_to_push_and_forgets_the_connection() {
        let f = fixture(DeliveryWatchConfig::default());
        let t0 = written(&f, "lost", &[item("g1", 5)]);
        written(&f, "parked", &[item("g1", 5)]);
        // 托管应答之前写出的帧留在环里；客户端没确认就关了连接。
        assert!(f.watch.set_device_state("parked", PARKING));
        written(&f, "clean", &[item("g1", 5)]);
        f.watch.confirm("clean", &ack("g1", 5));

        f.watch.unregister("lost").await;
        f.watch.unregister("parked").await;
        f.watch.unregister("clean").await;
        f.watch.unregister("never-registered").await;

        let handoffs = f.handoffs.take();
        let causes: Vec<(String, i32)> = handoffs
            .iter()
            .map(|handoff| (handoff.connection_id.clone(), handoff.cause))
            .collect();
        assert_eq!(
            causes,
            [
                ("lost".to_string(), DeviceHandoffCause::LinkLost as i32),
                ("parked".to_string(), DeviceHandoffCause::Parked as i32),
            ]
        );
        assert_eq!(pending_of(&handoffs[0]), [("g1".to_string(), 5)]);
        assert_eq!(f.handoffs_counted("link_lost"), 1);
        assert_eq!(f.handoffs_counted("parked"), 1);

        for connection_id in ["lost", "parked", "clean"] {
            assert_eq!(f.watch.device_state(connection_id), None);
        }
        // 留在期限队列里的旧项到点后自行消失，不再产生动作。
        assert!(f.watch.sweep(t0 + ms(60_000)).await.is_empty());
        assert_eq!(f.watch.timer_count(), 0);
        assert!(f.handoffs.take().is_empty());
        assert!(f.probe.pings.lock().unwrap().is_empty());
        assert!(f.probe.closes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn timeout_actions_reach_the_link() {
        let f = fixture(DeliveryWatchConfig::default());
        f.watch.dispatch(vec![
            LinkAction::Ping(Arc::from("c1")),
            LinkAction::Close(Arc::from("c2")),
        ]);
        for _ in 0..100 {
            if !f.probe.pings.lock().unwrap().is_empty()
                && !f.probe.closes.lock().unwrap().is_empty()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(*f.probe.pings.lock().unwrap(), ["c1"]);
        assert_eq!(*f.probe.closes.lock().unwrap(), ["c2"]);
    }

    #[test]
    fn only_alerting_content_with_a_seq_is_watched() {
        let text = Message {
            server_id: "m1".to_string(),
            conversation_id: "g1".to_string(),
            conversation_seq: 7,
            ..Default::default()
        };
        let silent_fact = Message {
            conversation_seq: 8,
            content: Some(MessageContent {
                content: Some(Content::Notification(NotificationContent::default())),
            }),
            ..text.clone()
        };
        let unsequenced = Message {
            conversation_seq: 0,
            ..text.clone()
        };
        assert_eq!(
            DeliveryItem::from_messages(&[text.clone(), silent_fact, unsequenced]),
            [DeliveryItem::new("g1", 7, "m1")]
        );

        let envelope = |events: Vec<Event>, max_conversation_seq: u64| EventEnvelope {
            events,
            conversation_id: "g1".to_string(),
            max_conversation_seq,
            ..Default::default()
        };
        let message_event = Event {
            conversation_id: "g1".to_string(),
            conversation_seq: 7,
            r#type: EventType::EventMessage as i32,
            payload: Some(EventPayload::Message(text)),
            ..Default::default()
        };
        let read_receipt = Event {
            conversation_id: "g1".to_string(),
            conversation_seq: 8,
            r#type: EventType::EventReadReceipt as i32,
            ..Default::default()
        };
        assert_eq!(
            DeliveryItem::from_envelope(&envelope(vec![message_event, read_receipt], 8)),
            [DeliveryItem::new("g1", 7, "m1")]
        );
        // 纯水位通知不跟踪，带不带会话 seq 都一样。
        assert!(DeliveryItem::from_envelope(&envelope(Vec::new(), 9)).is_empty());
        assert!(DeliveryItem::from_envelope(&envelope(Vec::new(), 0)).is_empty());
    }
}
