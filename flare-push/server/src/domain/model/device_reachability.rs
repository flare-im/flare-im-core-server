//! 设备可达视图
//!
//! 在线与否按设备判定，不按用户判定：电脑在线不代表手机上有人在读。

use std::collections::{HashMap, HashSet};

/// 一台设备在会话记录里报的应用状态，只用于决策链：路由时这台设备处在哪个状态。
///
/// 按「链路负责得多」排序：同一台设备有几条会话时取最靠后的那个。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LinkState {
    /// 已托管：提示归推送通道。
    Parked,
    /// 后台让渡：链路还在，提示归推送通道。
    Deferred,
    /// 后台在线，进程自己能发系统通知。
    Standby,
    /// 前台在线（不上报状态的旧客户端也算）。
    Active,
}

impl LinkState {
    /// 决策链里的名字。
    pub fn label(self) -> &'static str {
        match self {
            Self::Parked => "parked",
            Self::Deferred => "deferred",
            Self::Standby => "standby",
            Self::Active => "active",
        }
    }
}

/// 一个用户此刻的设备可达视图，来自 Online 的会话记录。
///
/// 两个字段回答两个不同的问题，不能互相替代：
/// - `has_session` 只决定要不要给这个用户发在线任务。它比「有活跃设备」宽：连接还在、
///   提示却归推送通道的设备也算，消息仍要经链路送到它那里。
/// - `live_devices` 只决定哪些设备**不需要**离线推送。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UserReachability {
    /// 用户有至少一条在线会话，不论设备处于什么应用状态。
    pub has_session: bool,
    /// 活跃设备：有一条应用状态为前台（`active`，缺省也算）或后台但能自己提示（`standby`）
    /// 的在线会话。这些设备由链路负责提示。只有已托管（`parking`）或已转交推送（`deferred`）
    /// 会话的设备不在这里，它们的提示归推送通道。
    ///
    /// 会话记录里没有设备标识的会话只计入 `has_session`，不出现在这里：分不清是哪台设备，
    /// 就不能据此压掉任何一台设备的推送。
    pub live_devices: HashSet<String>,
    /// 前台且用户正在看的设备（应用状态 `active`、注意力 `attentive`）。用户在这样一台设备上时，
    /// 他别的设备的推送按「多端」设置推迟、立即或只更新角标。
    pub attentive_devices: HashSet<String>,
    /// 各设备报的应用状态，只用于决策链；认不出的状态与没有设备标识的会话不在这里。
    pub link_states: HashMap<String, LinkState>,
}

impl UserReachability {
    /// 没有任何会话的用户。
    pub fn offline() -> Self {
        Self::default()
    }

    /// 这台设备是否由链路负责提示。
    pub fn is_live(&self, device_id: &str) -> bool {
        self.live_devices.contains(device_id)
    }

    /// 除了这台设备，用户是否还有一台前台且正在看的设备。
    pub fn other_device_attentive(&self, device_id: &str) -> bool {
        self.attentive_devices
            .iter()
            .any(|attentive| attentive != device_id)
    }

    /// 记下一台前台且用户正在看的设备。没有设备标识的会话不记。
    pub fn record_attentive(&mut self, device_id: &str) {
        let device_id = device_id.trim();
        if !device_id.is_empty() {
            self.attentive_devices.insert(device_id.to_string());
        }
    }

    /// 记下一台设备报的应用状态（决策链用）。同一台设备有几条会话时留链路负责得最多的那个；
    /// 没有设备标识的会话不记。
    pub fn record_link_state(&mut self, device_id: &str, state: LinkState) {
        let device_id = device_id.trim();
        if device_id.is_empty() {
            return;
        }
        self.link_states
            .entry(device_id.to_string())
            .and_modify(|known| *known = (*known).max(state))
            .or_insert(state);
    }

    /// 一台设备报的应用状态；没有会话或状态认不出时为 `None`。
    pub fn link_state(&self, device_id: &str) -> Option<LinkState> {
        self.link_states.get(device_id).copied()
    }

    /// 记入一条会话。`push_owned` 表示这条会话上设备的提示归推送通道（已托管或已转交推送），
    /// 而不是由链路负责。
    pub fn record_session(&mut self, device_id: &str, push_owned: bool) {
        self.has_session = true;
        let device_id = device_id.trim();
        if !push_owned && !device_id.is_empty() {
            self.live_devices.insert(device_id.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_push_owned_or_anonymous_session_counts_as_a_session_but_not_as_a_live_device() {
        let mut reach = UserReachability::offline();
        assert!(!reach.has_session);

        reach.record_session("phone", true);
        assert!(reach.has_session, "提示归推送通道的会话仍然是一条连接");
        assert!(!reach.is_live("phone"));

        reach.record_session("", false);
        reach.record_session("  ", false);
        assert!(reach.live_devices.is_empty());

        reach.record_session("desktop", false);
        assert!(reach.is_live("desktop"));
        assert!(!reach.is_live("phone"));
    }

    #[test]
    fn only_another_attentive_device_counts() {
        let mut reach = UserReachability::offline();
        assert!(!reach.other_device_attentive("phone"));
        reach.record_attentive("phone");
        reach.record_attentive(" ");
        assert!(!reach.other_device_attentive("phone"), "自己不算别的设备");
        reach.record_attentive("desktop");
        assert!(reach.other_device_attentive("phone"));
        assert!(reach.other_device_attentive("pad"));
    }

    /// 决策链里的设备状态：几条会话取链路负责得最多的那个，没有设备标识的不记。
    #[test]
    fn the_link_state_keeps_the_most_link_owned_session() {
        let mut reach = UserReachability::offline();
        reach.record_link_state("phone", LinkState::Parked);
        reach.record_link_state("phone", LinkState::Deferred);
        reach.record_link_state("phone", LinkState::Parked);
        reach.record_link_state(" ", LinkState::Active);
        assert_eq!(reach.link_state("phone"), Some(LinkState::Deferred));
        assert_eq!(reach.link_state("desktop"), None);
        assert_eq!(reach.link_states.len(), 1);
        reach.record_link_state("phone", LinkState::Active);
        assert_eq!(
            reach.link_state("phone").map(LinkState::label),
            Some("active")
        );
    }

    /// 同一台设备有两条会话（重连交叠）时，只要有一条由链路负责提示它就是活跃的。
    #[test]
    fn one_live_session_is_enough_for_a_device() {
        let mut reach = UserReachability::offline();
        reach.record_session("phone", true);
        reach.record_session("phone", false);
        assert!(reach.is_live("phone"));
    }
}
