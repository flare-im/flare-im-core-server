//! 通话信令事件视图：与 `EVENT_CALL_SIGNAL` / `CallSignalEvent` 对齐的薄封装（网关侧无业务 FSM）。

use flare_proto::common::call_signal_event::Signal;
use flare_proto::common::event::Payload;
use flare_proto::common::{CallSignalEvent, Event, EventType};

/// 与 proto `EventType::EventCallSignal` 对应。
pub const EVENT_CALL_SIGNAL: i32 = EventType::EventCallSignal as i32;

/// 上行/下行统一的信令种类视图（oneof `signal` 的镜像，便于路由表匹配）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallSignalType {
    Invite,
    Accept,
    Reject,
    Hangup,
    /// ICE / SDP / SFU 状态等子类型后续按需细分
    Other,
}

impl CallSignalType {
    pub fn from_proto(cs: &CallSignalEvent) -> Self {
        match cs.signal.as_ref() {
            Some(Signal::Invite(_)) => Self::Invite,
            Some(Signal::Accept(_)) => Self::Accept,
            Some(Signal::Reject(_)) => Self::Reject,
            Some(Signal::Hangup(_)) => Self::Hangup,
            _ => Self::Other,
        }
    }
}

/// 从领域 `Event` 拆出通话负载（若非通话事件返回 `None`）。
pub fn try_unwrap_call_signal(event: &Event) -> Option<&CallSignalEvent> {
    if event.r#type != EVENT_CALL_SIGNAL {
        return None;
    }
    match event.payload.as_ref()? {
        Payload::CallSignal(cs) => Some(cs),
        _ => None,
    }
}
