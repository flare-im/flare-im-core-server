//! User capability grant value object.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserCapabilityGrant {
    pub tenant_id: String,
    pub user_id: String,
    pub capability_id: String,
    pub granted_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub plan_code: Option<String>,
    pub source: Option<String>,
}

impl UserCapabilityGrant {
    pub fn is_active(&self, now: DateTime<Utc>) -> bool {
        match self.expires_at {
            Some(expires_at) => expires_at > now,
            None => true,
        }
    }
}

/// 租户级能力开关的一行（`capability_tenant_switches`）。
///
/// 行存在即「装过」：`enabled=false` 是停用，没有这一行才是从未安装。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantCapabilitySwitch {
    pub tenant_id: String,
    pub capability_id: String,
    pub enabled: bool,
    /// 最近一次写入时间；内存后端不记录时间，恒为 `None`。
    pub updated_at: Option<DateTime<Utc>>,
}
