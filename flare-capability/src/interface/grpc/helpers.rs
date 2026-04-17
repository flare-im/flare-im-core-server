//! gRPC 侧通用辅助：上下文提取、领域错误映射、管理面鉴权。

use std::sync::Arc;

use subtle::ConstantTimeEq;
use tonic::metadata::MetadataMap;
use tonic::{Request, Status};

use crate::domain::capability::CapabilityError;
use crate::infrastructure::config::CapabilityRuntimeConfig;

/// 从 metadata 取 `Ctx`；缺失时生成带新 `request_id` 的上下文（与 `ContextLayer::allow_missing` 配合）。
pub fn ctx_allow_missing<T>(req: &Request<T>) -> flare_im_core::Ctx {
    flare_server_core::middleware::get_context(req)
        .cloned()
        .unwrap_or_else(|| {
            Arc::new(flare_server_core::context::Context::with_request_id(
                uuid::Uuid::new_v4().to_string(),
            ))
        })
}

pub fn capability_error_to_status(e: CapabilityError) -> Status {
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

/// 运营操作者：优先 `x-actor-id`，否则 `x-user-id`。
pub fn actor_id_from_request<T>(req: &Request<T>) -> Option<String> {
    metadata_ascii(req.metadata(), "x-actor-id")
        .or_else(|| metadata_ascii(req.metadata(), "x-user-id"))
        .map(str::to_string)
}

fn secret_eq_ct(got: &str, expected: &str) -> bool {
    if got.len() != expected.len() {
        return false;
    }
    got.as_bytes().ct_eq(expected.as_bytes()).into()
}

/// `Grant` / `Revoke` / `SetTenantCapabilitySwitch` 前的管理面校验。
pub fn require_capability_policy_admin<T>(
    req: &Request<T>,
    runtime: &CapabilityRuntimeConfig,
) -> Result<(), Status> {
    if !runtime.policy_mutations_allowed() {
        return Err(Status::failed_precondition(
            "capability policy mutations disabled: configure FLARE_CAPABILITY_ADMIN_SECRET or unset FLARE_CAPABILITY_DENY_POLICY_MUTATIONS_WITHOUT_SECRET",
        ));
    }
    let Some(ref secret) = runtime.admin_secret else {
        return Ok(());
    };
    let got = metadata_ascii(req.metadata(), "x-capability-admin-secret").ok_or_else(|| {
        Status::permission_denied("missing metadata x-capability-admin-secret")
    })?;
    if !secret_eq_ct(got, secret) {
        return Err(Status::permission_denied("invalid x-capability-admin-secret"));
    }
    Ok(())
}
