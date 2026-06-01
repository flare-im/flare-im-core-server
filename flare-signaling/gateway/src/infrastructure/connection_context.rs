//! 连接上下文提取模块
//!
//! 从连接信息中提取上下文（租户ID、用户ID等）并设置到 Context，与 flare_server_core::context 对齐。

use flare_im_core::utils::normalize_tenant_id;
use flare_server_core::context::{ActorContext, ActorType, Context};
use std::collections::HashMap;
use uuid::Uuid;

/// 连接上下文键（用于存储在 ConnectionInfo.metadata 中）
pub const METADATA_KEY_TENANT_ID: &str = "tenant_id";
pub const METADATA_KEY_USER_ID: &str = "user_id";
pub const METADATA_KEY_DEVICE_ID: &str = "device_id";

/// 从连接信息的 metadata 中提取租户ID
pub fn extract_tenant_id_from_metadata(metadata: &HashMap<String, String>) -> Option<String> {
    metadata
        .get(METADATA_KEY_TENANT_ID)
        .map(normalize_tenant_id)
}

/// 从连接信息的 metadata 中提取用户ID
pub fn extract_user_id_from_metadata(metadata: &HashMap<String, String>) -> Option<String> {
    metadata.get(METADATA_KEY_USER_ID).cloned()
}

/// 从连接信息的 metadata 中提取设备ID
pub fn extract_device_id_from_metadata(metadata: &HashMap<String, String>) -> Option<String> {
    metadata.get(METADATA_KEY_DEVICE_ID).cloned()
}

/// 从连接 metadata / 用户 ID 构建上行链路 [`Context`]（request_id、trace_id 自动生成）
pub fn build_context_from_connection(
    connection_metadata: Option<&HashMap<String, String>>,
    user_id: Option<&str>,
    default_tenant_id: &str,
) -> Context {
    let request_id = Uuid::new_v4().to_string();
    let trace_id = Uuid::new_v4().to_string();
    let mut ctx = Context::with_request_id(request_id).with_trace_id(trace_id);
    let tenant_id = connection_metadata
        .and_then(|m| m.get(METADATA_KEY_TENANT_ID).cloned())
        .map(normalize_tenant_id)
        .unwrap_or_else(|| normalize_tenant_id(default_tenant_id));
    ctx = ctx.with_tenant_id(tenant_id);
    let uid = user_id
        .map(str::to_string)
        .or_else(|| connection_metadata.and_then(|m| m.get(METADATA_KEY_USER_ID).cloned()));
    let uid = uid.unwrap_or_default();
    ctx = ctx.with_user_id(uid.clone());
    if !uid.is_empty() {
        ctx = ctx.with_actor(ActorContext::new(uid).with_type(ActorType::User));
    }
    ctx = ctx.with_device_id(
        connection_metadata
            .and_then(|m| m.get(METADATA_KEY_DEVICE_ID).cloned())
            .unwrap_or_default(),
    );
    ctx
}
