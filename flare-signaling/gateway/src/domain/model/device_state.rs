//! 设备链路状态值对象
//!
//! 一条连接背后的应用处于前台、后台（自己提示或把提示交给推送通道）还是即将被挂起（托管中），
//! 以及用户是否正在操作。网关据此决定：可提示的下行帧还写不写给这条连接、要不要等送达确认、
//! 等多久。

use std::collections::HashMap;

use flare_proto::common::{DeviceAppState, DeviceAttention, DeviceStateReport};

use crate::constants::METADATA_KEY_DEVICE_STATE;

/// 设备链路状态：应用状态 + 用户注意力。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeviceLinkState {
    pub app_state: DeviceAppState,
    pub attention: DeviceAttention,
}

impl DeviceLinkState {
    /// 客户端没报状态时的取值：前台，注意力未知。
    pub const FOREGROUND: Self = Self {
        app_state: DeviceAppState::Active,
        attention: DeviceAttention::Unspecified,
    };

    /// 由客户端的 DEVICE_STATE 上报构造。状态缺省或是本版本不认识的值时返回 `None`：这样的
    /// 上报无法生效，调用方不能当它已经应用。
    pub fn from_report(report: &DeviceStateReport) -> Option<Self> {
        let app_state = match DeviceAppState::try_from(report.state) {
            Ok(DeviceAppState::Unspecified) | Err(_) => return None,
            Ok(app_state) => app_state,
        };
        Some(Self {
            app_state,
            attention: DeviceAttention::try_from(report.attention)
                .unwrap_or(DeviceAttention::Unspecified),
        })
    }

    /// CONNECT 元数据里的建连状态：小写文本 `active`（前台）、`standby`（后台，本进程自己提示）
    /// 或 `deferred`（后台，提示交给推送通道）。CONNECT 只带状态本身，注意力与角标由连上之后的
    /// DEVICE_STATE 补齐：明说在前台的按用户正在操作算，后台的注意力未知；缺省或其他值按前台、
    /// 注意力未知处理。
    pub fn from_connect_value(value: Option<&str>) -> Self {
        match value {
            Some("active") => Self {
                app_state: DeviceAppState::Active,
                attention: DeviceAttention::Attentive,
            },
            Some("standby") => Self {
                app_state: DeviceAppState::Standby,
                attention: DeviceAttention::Unspecified,
            },
            Some("deferred") => Self {
                app_state: DeviceAppState::Deferred,
                attention: DeviceAttention::Unspecified,
            },
            _ => Self::FOREGROUND,
        }
    }

    /// 从连接元数据读建连时的状态（CONNECT 设备元数据里的 `device_state`）。
    pub fn from_connection_metadata(metadata: Option<&HashMap<String, String>>) -> Self {
        Self::from_connect_value(
            metadata
                .and_then(|metadata| metadata.get(METADATA_KEY_DEVICE_STATE))
                .map(String::as_str),
        )
    }

    /// 托管中：客户端即将关闭链路，提示已经交给推送通道。
    pub fn is_parking(&self) -> bool {
        self.app_state == DeviceAppState::Parking
    }

    /// 后台、链路保留，但进程不能自己提示：帧照常下发，提示由推送路由在路由时直接推送，
    /// 这条链路上的帧不需要等送达确认。
    pub fn is_deferred(&self) -> bool {
        self.app_state == DeviceAppState::Deferred
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(state: DeviceAppState, attention: DeviceAttention) -> DeviceStateReport {
        DeviceStateReport {
            state: state as i32,
            attention: attention as i32,
            badge: 3,
            reported_at: 1_794_000_000_000,
        }
    }

    #[test]
    fn a_report_without_a_known_state_cannot_be_applied() {
        assert_eq!(
            DeviceLinkState::from_report(&report(DeviceAppState::Standby, DeviceAttention::Idle)),
            Some(DeviceLinkState {
                app_state: DeviceAppState::Standby,
                attention: DeviceAttention::Idle,
            })
        );
        assert_eq!(
            DeviceLinkState::from_report(&DeviceStateReport::default()),
            None
        );
        assert_eq!(
            DeviceLinkState::from_report(&DeviceStateReport {
                state: 99,
                ..Default::default()
            }),
            None
        );
        assert_eq!(
            DeviceLinkState::from_report(&report(
                DeviceAppState::Deferred,
                DeviceAttention::Unspecified
            )),
            Some(DeviceLinkState {
                app_state: DeviceAppState::Deferred,
                attention: DeviceAttention::Unspecified,
            })
        );
        // 注意力是附带信息：不认识的值不影响状态生效。
        assert_eq!(
            DeviceLinkState::from_report(&DeviceStateReport {
                state: DeviceAppState::Active as i32,
                attention: 99,
                ..Default::default()
            }),
            Some(DeviceLinkState::FOREGROUND)
        );
    }

    #[test]
    fn the_connect_value_is_plain_lower_case_text() {
        assert_eq!(
            DeviceLinkState::from_connect_value(Some("active")),
            DeviceLinkState {
                app_state: DeviceAppState::Active,
                attention: DeviceAttention::Attentive,
            }
        );
        let standby = DeviceLinkState::from_connect_value(Some("standby"));
        assert_eq!(
            standby,
            DeviceLinkState {
                app_state: DeviceAppState::Standby,
                attention: DeviceAttention::Unspecified,
            }
        );
        assert!(!standby.is_parking());
        assert!(!standby.is_deferred());
        let deferred = DeviceLinkState::from_connect_value(Some("deferred"));
        assert_eq!(
            deferred,
            DeviceLinkState {
                app_state: DeviceAppState::Deferred,
                attention: DeviceAttention::Unspecified,
            }
        );
        assert!(deferred.is_deferred());

        // 缺省或其他任何值（含大小写不对的、托管）按前台处理。
        for value in [
            None,
            Some(""),
            Some("Active"),
            Some("STANDBY"),
            Some("Deferred"),
            Some("parking"),
        ] {
            assert_eq!(
                DeviceLinkState::from_connect_value(value),
                DeviceLinkState::FOREGROUND,
                "{value:?}"
            );
        }
    }

    #[test]
    fn the_connect_state_is_read_from_connection_metadata() {
        let metadata =
            HashMap::from([(METADATA_KEY_DEVICE_STATE.to_string(), "standby".to_string())]);
        assert_eq!(
            DeviceLinkState::from_connection_metadata(Some(&metadata)).app_state,
            DeviceAppState::Standby
        );
        assert_eq!(
            DeviceLinkState::from_connection_metadata(Some(&HashMap::new())),
            DeviceLinkState::FOREGROUND
        );
        assert_eq!(
            DeviceLinkState::from_connection_metadata(None),
            DeviceLinkState::FOREGROUND
        );
    }
}
