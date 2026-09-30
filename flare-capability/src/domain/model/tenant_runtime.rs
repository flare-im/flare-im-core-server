//! 管理员只读的租户运行时视图：本进程 `tenants` 表里的一行（控制面投影或 init 预置行）。

use chrono::{DateTime, Utc};
use flare_im_contracts::domain::tenant::{TenantQuota, TenantStatus};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantRuntimeView {
    pub tenant_id: String,
    pub name: String,
    pub status: TenantStatus,
    pub quota: TenantQuota,
    /// `tenants.config`（控制面透传的 settings_json）的 JSON 对象文本。
    pub settings_json: String,
    /// 本地持有的投影版本；`0` = 预置行，从未收到过投影。
    pub version: u64,
    pub projected_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}
