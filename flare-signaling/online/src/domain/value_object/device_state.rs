//! 设备可达状态值对象
//!
//! 客户端经网关上报的应用状态、用户注意力与角标基线，随会话记录保存。推送路由据此判断新消息的
//! 提示由这条链路上的进程自己负责，还是归推送通道（[`DeviceAppState::alerts_owned_by_push`]）：
//! 已托管的设备连链路都不再算可达，后台不能自行提示的设备消息仍走链路、提示走推送。

use flare_proto::common::{
    DeviceAppState as ProtoDeviceAppState, DeviceAttention as ProtoDeviceAttention,
};
use serde::{Deserialize, Serialize};

/// 设备应用状态。
///
/// 序列化后的小写名就是会话记录里 `app_state` 的取值（[`Self::as_str`]），推送路由按同样的名字读，
/// 改名等于改存储格式。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceAppState {
    /// 前台：核心在应用内提示。会话记录里没有这个字段时也是前台。
    #[default]
    Active,
    /// 后台、链路保留，进程能自己发系统通知。
    Standby,
    /// 已托管：提示交给推送通道，客户端随后关闭链路。
    Parking,
    /// 后台、链路保留，但进程不能自己提示：消息照常经链路下发，提示由推送通道负责。
    Deferred,
}

impl DeviceAppState {
    /// 线上枚举转领域值。未指定按前台处理：建连时客户端可以不带状态，本版本不认识的取值经
    /// prost 的访问器也落在未指定上。
    pub fn from_proto(state: ProtoDeviceAppState) -> Self {
        match state {
            ProtoDeviceAppState::Unspecified | ProtoDeviceAppState::Active => Self::Active,
            ProtoDeviceAppState::Standby => Self::Standby,
            ProtoDeviceAppState::Parking => Self::Parking,
            ProtoDeviceAppState::Deferred => Self::Deferred,
        }
    }

    pub fn to_proto(self) -> ProtoDeviceAppState {
        match self {
            Self::Active => ProtoDeviceAppState::Active,
            Self::Standby => ProtoDeviceAppState::Standby,
            Self::Parking => ProtoDeviceAppState::Parking,
            Self::Deferred => ProtoDeviceAppState::Deferred,
        }
    }

    /// 会话记录里的取值。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Standby => "standby",
            Self::Parking => "parking",
            Self::Deferred => "deferred",
        }
    }

    /// 这台设备的提示是否归推送通道：已托管（链路即将关闭），或在后台保着链路却不能自己提示。
    /// 前台与能自行提示的后台设备，提示由链路上的进程自己负责。
    pub fn alerts_owned_by_push(self) -> bool {
        matches!(self, Self::Parking | Self::Deferred)
    }
}

/// 用户注意力。别的设备处于前台且 [`Self::Attentive`] 时，手机推送可以延迟并在已读后取消。
///
/// 序列化后的小写名就是会话记录里 `attention` 的取值（[`Self::as_str`]）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceAttention {
    /// 客户端没有上报注意力。会话记录里没有这个字段时也是未指定。
    #[default]
    Unspecified,
    /// 有输入焦点且近期有操作。
    Attentive,
    /// 窗口在前台但一段时间没有操作。
    Idle,
}

impl DeviceAttention {
    pub fn from_proto(attention: ProtoDeviceAttention) -> Self {
        match attention {
            ProtoDeviceAttention::Unspecified => Self::Unspecified,
            ProtoDeviceAttention::Attentive => Self::Attentive,
            ProtoDeviceAttention::Idle => Self::Idle,
        }
    }

    pub fn to_proto(self) -> ProtoDeviceAttention {
        match self {
            Self::Unspecified => ProtoDeviceAttention::Unspecified,
            Self::Attentive => ProtoDeviceAttention::Attentive,
            Self::Idle => ProtoDeviceAttention::Idle,
        }
    }

    /// 会话记录里的取值。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Attentive => "attentive",
            Self::Idle => "idle",
        }
    }
}

/// 一次设备状态上报的内容。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeviceStateUpdate {
    pub app_state: DeviceAppState,
    pub attention: DeviceAttention,
    /// 设备当前的未读总数，作为服务端角标的基线。
    pub badge: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 会话记录里的取值是固定的小写名，serde 写出的与 `as_str` 是同一个值：Lua 脚本按 `as_str`
    /// 写，Rust 按 serde 读。
    #[test]
    fn stored_names_match_the_serialized_form() {
        for (state, name) in [
            (DeviceAppState::Active, "active"),
            (DeviceAppState::Standby, "standby"),
            (DeviceAppState::Parking, "parking"),
            (DeviceAppState::Deferred, "deferred"),
        ] {
            assert_eq!(state.as_str(), name);
            assert_eq!(
                serde_json::to_string(&state).unwrap(),
                format!("\"{name}\"")
            );
            assert_eq!(
                serde_json::from_str::<DeviceAppState>(&format!("\"{name}\"")).unwrap(),
                state
            );
        }
        for (attention, name) in [
            (DeviceAttention::Unspecified, "unspecified"),
            (DeviceAttention::Attentive, "attentive"),
            (DeviceAttention::Idle, "idle"),
        ] {
            assert_eq!(attention.as_str(), name);
            assert_eq!(
                serde_json::to_string(&attention).unwrap(),
                format!("\"{name}\"")
            );
            assert_eq!(
                serde_json::from_str::<DeviceAttention>(&format!("\"{name}\"")).unwrap(),
                attention
            );
        }
    }

    /// 未指定的应用状态按前台处理；注意力的未指定原样保留。
    #[test]
    fn an_unspecified_app_state_is_active() {
        assert_eq!(
            DeviceAppState::from_proto(ProtoDeviceAppState::Unspecified),
            DeviceAppState::Active
        );
        assert_eq!(DeviceAppState::default(), DeviceAppState::Active);
        assert_eq!(
            DeviceAttention::from_proto(ProtoDeviceAttention::Unspecified),
            DeviceAttention::Unspecified
        );
        for state in [
            DeviceAppState::Active,
            DeviceAppState::Standby,
            DeviceAppState::Parking,
            DeviceAppState::Deferred,
        ] {
            assert_eq!(DeviceAppState::from_proto(state.to_proto()), state);
        }
        assert_eq!(
            DeviceAppState::Deferred.to_proto(),
            ProtoDeviceAppState::Deferred
        );
        for attention in [
            DeviceAttention::Unspecified,
            DeviceAttention::Attentive,
            DeviceAttention::Idle,
        ] {
            assert_eq!(DeviceAttention::from_proto(attention.to_proto()), attention);
        }
    }

    /// 提示归推送通道的是已托管与「后台、不能自己提示」两种；前台与能自行提示的后台设备，提示
    /// 由链路上的进程自己负责。
    #[test]
    fn push_owns_the_alerts_of_parked_and_deferred_devices() {
        assert!(DeviceAppState::Parking.alerts_owned_by_push());
        assert!(DeviceAppState::Deferred.alerts_owned_by_push());
        assert!(!DeviceAppState::Active.alerts_owned_by_push());
        assert!(!DeviceAppState::Standby.alerts_owned_by_push());
    }
}
