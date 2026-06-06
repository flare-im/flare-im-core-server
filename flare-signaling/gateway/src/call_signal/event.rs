//! 通话信令路由视图。
//!
//! Gateway 不再依赖 `common.Event` 中的通话 payload。RTC 插件或实时控制层在进入
//! Gateway 路由前，把自己的协议对象归一化为这个本地视图即可。

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

/// 通话信令路由所需的最小信息。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallSignalRouteView {
    pub signal_type: CallSignalType,
    pub call_id: Option<String>,
    pub sfu_room_id: Option<String>,
}

impl CallSignalRouteView {
    pub fn new(
        signal_type: CallSignalType,
        call_id: impl Into<Option<String>>,
        sfu_room_id: impl Into<Option<String>>,
    ) -> Self {
        Self {
            signal_type,
            call_id: call_id.into().filter(|id| !id.trim().is_empty()),
            sfu_room_id: sfu_room_id.into().filter(|id| !id.trim().is_empty()),
        }
    }
}
