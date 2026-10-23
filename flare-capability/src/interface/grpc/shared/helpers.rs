//! gRPC 侧通用辅助：上下文提取、领域错误映射。
//!
//! 管理面鉴权不在这里：员工经 admin-gateway 转发的 Staff `x-flare-ctx` 由各 RPC 用
//! `flare_im_service_kit::staff_context::require_staff` 按权限码复核（见 `capability/service.rs`
//! 与 `capability_admin.rs`）；平台断言只剩租户投影的 worker（`tenant_projection.rs`）。

use std::sync::Arc;

use tonic::metadata::MetadataMap;
use tonic::{Request, Status};

use crate::domain::capability::CapabilityError;

/// 从 metadata 取 `Ctx`；缺失时生成带新 `request_id` 的上下文（与 `ContextLayer::allow_missing` 配合）。
pub fn ctx_allow_missing<T>(req: &Request<T>) -> flare_im_contracts::Ctx {
    flare_server_core::middleware::get_context(req)
        .cloned()
        .unwrap_or_else(|| {
            Arc::new(flare_server_core::context::Context::with_request_id(
                uuid::Uuid::new_v4().to_string(),
            ))
        })
}

pub fn capability_error_to_status(e: CapabilityError) -> Status {
    // A plugin platform refusal keeps its code as the error reason (`ErrorDetail`): the API
    // gateway answers `PLUGIN_DISABLED`, `PLUGIN_CARD_UNBOUND` and so on with their own status.
    if let CapabilityError::Plugin { .. } = e {
        return Status::from(e.into_flare());
    }
    let msg = e.to_string();
    match &e {
        CapabilityError::PolicyDenied(_) | CapabilityError::Rejected(_) => {
            Status::permission_denied(msg)
        }
        CapabilityError::NotRegistered(_) => Status::not_found(msg),
        CapabilityError::NotSupported(_) => Status::unimplemented(msg),
        CapabilityError::Timeout(_) => Status::deadline_exceeded(msg),
        _ => Status::internal(msg),
    }
}

pub fn metadata_ascii<'a>(m: &'a MetadataMap, key: &str) -> Option<&'a str> {
    m.get(key).and_then(|v| v.to_str().ok())
}

pub fn trace_id_from_request<T>(req: &Request<T>) -> Option<String> {
    metadata_ascii(req.metadata(), "x-trace-id").map(str::to_string)
}
