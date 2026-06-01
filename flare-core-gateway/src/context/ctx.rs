use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// 请求上下文
///
/// 承载 TraceID、UserID、TenantID 等信息
/// 必须在所有业务逻辑链路中显式传递
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ctx {
    /// 追踪 ID
    pub trace_id: String,
    /// 用户 ID
    pub user_id: Option<String>,
    /// 租户 ID
    pub tenant_id: Option<String>,
    /// 请求 ID
    pub request_id: String,
}

impl Ctx {
    /// 创建新的上下文
    pub fn new(trace_id: impl Into<String>, user_id: Option<String>) -> Self {
        Self {
            trace_id: trace_id.into(),
            user_id,
            tenant_id: None,
            request_id: Uuid::new_v4().to_string(),
        }
    }

    /// 创建带有租户 ID 的上下文
    pub fn with_tenant(mut self, tenant_id: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant_id.into());
        self
    }

    /// 从 HTTP 请求头构建上下文
    pub fn from_headers(headers: &axum::http::HeaderMap) -> Self {
        let trace_id = headers
            .get("x-trace-id")
            .and_then(|v| v.to_str().ok())
            .map(String::from)
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        let user_id = headers
            .get("x-user-id")
            .and_then(|v| v.to_str().ok())
            .map(String::from);

        let tenant_id = headers
            .get("x-tenant-id")
            .and_then(|v| v.to_str().ok())
            .map(String::from);

        let request_id = headers
            .get("x-request-id")
            .and_then(|v| v.to_str().ok())
            .map(String::from)
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        Self {
            trace_id,
            user_id,
            tenant_id,
            request_id,
        }
    }

    /// 注入到 gRPC 元数据
    pub fn inject_to_grpc_metadata(&self, metadata: &mut tonic::metadata::MetadataMap) {
        if let Ok(trace_id) = self.trace_id.parse() {
            metadata.insert("x-trace-id", trace_id);
        }

        if let Ok(request_id) = self.request_id.parse() {
            metadata.insert("x-request-id", request_id);
        }

        if let Some(ref user_id) = self.user_id {
            if let Ok(user_id_value) = user_id.parse() {
                metadata.insert("x-user-id", user_id_value);
            }
        }

        if let Some(ref tenant_id) = self.tenant_id {
            if let Ok(tenant_id_value) = tenant_id.parse() {
                metadata.insert("x-tenant-id", tenant_id_value);
            }
        }
    }

    /// 获取用户 ID,如果不存在则返回错误
    pub fn require_user_id(&self) -> Result<&str, &'static str> {
        self.user_id
            .as_deref()
            .ok_or("User ID is required but not present in context")
    }
}

impl Default for Ctx {
    fn default() -> Self {
        Self::new(Uuid::new_v4().to_string(), None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;

    #[test]
    fn test_ctx_new() {
        let ctx = Ctx::new("trace-123", Some("user-456".to_string()));
        assert_eq!(ctx.trace_id, "trace-123");
        assert_eq!(ctx.user_id, Some("user-456".to_string()));
    }

    #[test]
    fn test_ctx_from_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("x-trace-id", "trace-789".parse().unwrap());
        headers.insert("x-request-id", "request-789".parse().unwrap());
        headers.insert("x-user-id", "user-012".parse().unwrap());

        let ctx = Ctx::from_headers(&headers);
        assert_eq!(ctx.trace_id, "trace-789");
        assert_eq!(ctx.request_id, "request-789");
        assert_eq!(ctx.user_id, Some("user-012".to_string()));
    }

    #[test]
    fn test_ctx_inject_to_grpc() {
        let ctx = Ctx::new("trace-abc", Some("user-def".to_string())).with_tenant("tenant-123");

        let mut metadata = tonic::metadata::MetadataMap::new();
        ctx.inject_to_grpc_metadata(&mut metadata);

        assert_eq!(
            metadata.get("x-trace-id").unwrap().to_str().unwrap(),
            "trace-abc"
        );
        assert_eq!(
            metadata.get("x-request-id").unwrap().to_str().unwrap(),
            ctx.request_id
        );
        assert_eq!(
            metadata.get("x-user-id").unwrap().to_str().unwrap(),
            "user-def"
        );
        assert_eq!(
            metadata.get("x-tenant-id").unwrap().to_str().unwrap(),
            "tenant-123"
        );
    }
}
