//! User capability grant value object.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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
