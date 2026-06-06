//! ConnectionQuality 转换适配器
//!
//! 职责：
//! - 实现 domain::ConnectionQuality 到 proto::ConnectionQuality 的转换
//! - 实现 proto::ConnectionQuality 到 domain::ConnectionQuality 的转换

use crate::domain::value_object::{ConnectionQuality, NetworkType};
use flare_proto::common::ConnectionQuality as ProtoConnectionQuality;

/// 实现 domain::ConnectionQuality 到 proto::ConnectionQuality 的转换
impl From<ConnectionQuality> for ProtoConnectionQuality {
    fn from(domain: ConnectionQuality) -> Self {
        Self {
            rtt_ms: domain.rtt_ms(),
            packet_loss_rate: domain.packet_loss_rate(),
            last_measured_at: domain.last_measure_ts().timestamp_millis(),
            network_type: domain.network_type().as_str().to_string(),
            signal_strength: domain.signal_strength().unwrap_or(0),
        }
    }
}

/// 实现 proto::ConnectionQuality 到 domain::ConnectionQuality 的转换
impl TryFrom<ProtoConnectionQuality> for ConnectionQuality {
    type Error = String;

    fn try_from(proto: ProtoConnectionQuality) -> Result<Self, Self::Error> {
        ConnectionQuality::new(
            proto.rtt_ms,
            proto.packet_loss_rate,
            NetworkType::from_string(&proto.network_type),
            if proto.signal_strength != 0 {
                Some(proto.signal_strength)
            } else {
                None
            },
        )
    }
}
