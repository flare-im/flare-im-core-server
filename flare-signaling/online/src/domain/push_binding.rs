//! 推送绑定吊销
//!
//! 服务端主动结束一台设备的会话（踢线、身份会话被吊销、被别的设备顶下线）之后，这台设备不该再
//! 收到带消息预览的推送。正常登出时客户端自己吊销绑定；被踢的设备可能根本不在线、收不到踢线
//! 通知，所以由 Online 在结束会话的同时请推送侧吊销。
//!
//! 只在「服务端有意结束会话」时吊销。普通断开、客户端登出、心跳过期、托管、网关失联清扫都不是：
//! 那些设备的登录仍然有效，推送正是它们的通道。

use flare_server_core::error::Result;

/// 吊销原因（诊断用）。[`Self::as_str`] 是推送侧约定的小写蛇形取值。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BindingRevocationReason {
    /// 运维或用户自己把这台设备踢下线。
    Kicked,
    /// 这台设备的身份会话被吊销（改密、重置密码）。
    IdentityRevoked,
    /// 整个租户被踢下线。
    TenantKicked,
    /// 同一用户的另一台设备登录，冲突策略把这台设备顶下线。
    DeviceReplaced,
}

impl BindingRevocationReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Kicked => "kicked",
            Self::IdentityRevoked => "identity_revoked",
            Self::TenantKicked => "tenant_kicked",
            Self::DeviceReplaced => "device_replaced",
        }
    }
}

/// 一次吊销请求：某用户在某租户下的一台设备，或全部设备。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingRevocation {
    pub tenant_id: String,
    pub user_id: String,
    /// 目标设备；`None` 表示该用户在本租户下的全部设备。
    pub device_id: Option<String>,
    pub reason: BindingRevocationReason,
    /// 只吊销登记时间不晚于它的绑定（Unix 毫秒）：触发吊销的那件事发生的时刻。
    ///
    /// 吊销是异步送达的，中途还可能重试；设备可能在这期间已经重新登录、登记了新绑定，新绑定不
    /// 能被迟到的吊销清掉。所以这个时刻在提交时定下，之后原样带到每一次重试，绝不是发送时的
    /// 「现在」。
    pub registered_before_ms: i64,
}

impl BindingRevocation {
    /// 吊销一台设备在 `registered_before_ms` 及之前登记的绑定。
    pub fn device(
        tenant_id: &str,
        user_id: &str,
        device_id: &str,
        reason: BindingRevocationReason,
        registered_before_ms: i64,
    ) -> Self {
        Self {
            tenant_id: tenant_id.to_string(),
            user_id: user_id.to_string(),
            device_id: Some(device_id.to_string()),
            reason,
            registered_before_ms,
        }
    }

    /// 吊销该用户在本租户下全部设备在 `registered_before_ms` 及之前登记的绑定。
    pub fn all_devices(
        tenant_id: &str,
        user_id: &str,
        reason: BindingRevocationReason,
        registered_before_ms: i64,
    ) -> Self {
        Self {
            tenant_id: tenant_id.to_string(),
            user_id: user_id.to_string(),
            device_id: None,
            reason,
            registered_before_ms,
        }
    }
}

/// 领域服务提交吊销请求的出口。提交不等待、也不会失败：结束会话的操作不能因为推送侧慢或不可达
/// 而卡住或报错，送达与重试是实现的事。
pub trait BindingRevocationSink: Send + Sync {
    fn submit(&self, revocation: BindingRevocation);
}

/// 把吊销请求送到推送侧的出站端口。
pub trait PushBindingRevoker: Send + Sync {
    /// 吊销绑定，返回实际吊销的数量；没有绑定时为 0，不算失败。重复执行是安全的。
    async fn revoke(&self, revocation: &BindingRevocation) -> Result<u32>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 原因的取值是与推送侧共用的四个名字。
    #[test]
    fn reasons_use_the_agreed_names() {
        assert_eq!(BindingRevocationReason::Kicked.as_str(), "kicked");
        assert_eq!(
            BindingRevocationReason::IdentityRevoked.as_str(),
            "identity_revoked"
        );
        assert_eq!(
            BindingRevocationReason::TenantKicked.as_str(),
            "tenant_kicked"
        );
        assert_eq!(
            BindingRevocationReason::DeviceReplaced.as_str(),
            "device_replaced"
        );
    }

    #[test]
    fn a_revocation_names_one_device_or_all_of_them_up_to_a_cutoff() {
        let one = BindingRevocation::device(
            "tenant-a",
            "alice",
            "phone",
            BindingRevocationReason::Kicked,
            1_700_000_000_000,
        );
        assert_eq!(one.device_id.as_deref(), Some("phone"));
        assert_eq!(one.registered_before_ms, 1_700_000_000_000);
        let all = BindingRevocation::all_devices(
            "tenant-a",
            "alice",
            BindingRevocationReason::IdentityRevoked,
            1_700_000_000_001,
        );
        assert_eq!(all.device_id, None);
        assert_eq!(all.registered_before_ms, 1_700_000_000_001);
        assert_eq!(
            (all.tenant_id.as_str(), all.user_id.as_str()),
            ("tenant-a", "alice")
        );
    }
}
