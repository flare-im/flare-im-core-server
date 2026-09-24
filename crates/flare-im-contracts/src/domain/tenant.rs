//! 租户运行时契约（控制面投影在核里的只读视图）。
//!
//! 真源在控制面（flare-im-console），核经 `flare.control.v1.TenantProjection` 接收投影并写入
//! `tenants` 表；网关 / ingest / sync 只读本模块的快照。这里不放任何产品语义：状态只有
//! 「能否接入」，配额只有核自己执行的三项运行时配额。

use serde::{Deserialize, Serialize};

/// 租户状态（与 `tenants.status` 的 CHECK 约束一致）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TenantStatus {
    Active,
    /// 拒新连、拒重连；数据不删。
    Suspended,
    /// 等价 `Suspended` + 后台清理中。
    Deleting,
}

impl TenantStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            TenantStatus::Active => "active",
            TenantStatus::Suspended => "suspended",
            TenantStatus::Deleting => "deleting",
        }
    }

    /// 解析数据库列值；未知值返回 `None`（调用方按 fail-closed 处理）。
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim() {
            "active" => Some(TenantStatus::Active),
            "suspended" => Some(TenantStatus::Suspended),
            "deleting" => Some(TenantStatus::Deleting),
            _ => None,
        }
    }

    /// 是否允许建立 / 保持长连接。
    pub const fn admits_connections(self) -> bool {
        matches!(self, TenantStatus::Active)
    }
}

/// 核侧执行的运行时配额；`0` = 不限（回落全局配置）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TenantCoreQuota {
    pub send_qps: u32,
    pub sync_pull_qps: u32,
    pub max_online_devices_per_user: u32,
}

/// 完整配额（Social 侧的三项核只透传存储，不执行）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TenantQuota {
    pub max_users: u32,
    pub max_groups: u32,
    pub max_group_members: u32,
    pub core: TenantCoreQuota,
}

/// 进程内缓存持有的租户快照。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantRuntimeSnapshot {
    pub tenant_id: String,
    pub status: TenantStatus,
    pub quota: TenantQuota,
    /// 控制面版本；`0` = 从未投影（如 init.sql 预置的默认租户）。
    pub version: u64,
}

impl TenantRuntimeSnapshot {
    pub fn core_quota(&self) -> TenantCoreQuota {
        self.quota.core
    }
}

/// 数据面租户 ID 允许的字符集：`^[0-9A-Za-z_-]{1,32}$`；`*` 只属于功能开关的平台默认行，租户投影不允许。
pub fn is_valid_tenant_id(tenant_id: &str) -> bool {
    !tenant_id.is_empty()
        && tenant_id.len() <= 32
        && tenant_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_round_trips_through_column_value() {
        for status in [
            TenantStatus::Active,
            TenantStatus::Suspended,
            TenantStatus::Deleting,
        ] {
            assert_eq!(TenantStatus::parse(status.as_str()), Some(status));
        }
        assert_eq!(TenantStatus::parse("deleted"), None);
        assert!(TenantStatus::Active.admits_connections());
        assert!(!TenantStatus::Suspended.admits_connections());
        assert!(!TenantStatus::Deleting.admits_connections());
    }

    #[test]
    fn quota_json_defaults_missing_fields_to_unlimited() {
        let quota: TenantQuota =
            serde_json::from_str(r#"{"max_users": 10, "core": {"send_qps": 5}}"#).unwrap();
        assert_eq!(quota.max_users, 10);
        assert_eq!(quota.core.send_qps, 5);
        assert_eq!(quota.core.sync_pull_qps, 0);
        let empty: TenantQuota = serde_json::from_str("{}").unwrap();
        assert_eq!(empty, TenantQuota::default());
    }

    #[test]
    fn tenant_id_charset() {
        assert!(is_valid_tenant_id("0"));
        assert!(is_valid_tenant_id("acme_corp-01"));
        assert!(!is_valid_tenant_id(""));
        assert!(!is_valid_tenant_id("*"));
        assert!(!is_valid_tenant_id("a b"));
        assert!(!is_valid_tenant_id(&"x".repeat(33)));
    }
}
