//! 请求/租户上下文（与 proto metadata 解耦）

/// 请求上下文
#[derive(Debug, Clone, Default)]
pub struct RequestContext {
    pub request_id: String,
    pub device_id: Option<String>,
    pub platform: Option<String>,
}

/// 租户上下文
#[derive(Debug, Clone, Default)]
pub struct TenantContext {
    pub tenant_id: String,
    pub user_id: Option<String>,
}
