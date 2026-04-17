//! 能力分发命令与结果（应用层 / gRPC 共用，与传输无关）。

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// 一次 `Dispatch` 的输入（由 gRPC 等入口组装）。
#[derive(Debug, Deserialize)]
pub struct CapabilityDispatchCommand {
    pub capability_id: String,
    pub tenant_id: Option<String>,
    pub user_id: Option<String>,
    pub conversation_id: Option<String>,
    pub payload: Option<Value>,
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityDispatchResult {
    pub request_id: String,
    pub success: bool,
    pub plugin_id: String,
    pub capability_id: String,
    pub data: Value,
    pub error: Option<String>,
}

impl CapabilityDispatchResult {
    pub fn ok(
        request_id: impl Into<String>,
        plugin_id: impl Into<String>,
        capability_id: impl Into<String>,
        data: Value,
    ) -> Self {
        Self {
            request_id: request_id.into(),
            success: true,
            plugin_id: plugin_id.into(),
            capability_id: capability_id.into(),
            data,
            error: None,
        }
    }

    pub fn fail(
        request_id: impl Into<String>,
        plugin_id: impl Into<String>,
        capability_id: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            request_id: request_id.into(),
            success: false,
            plugin_id: plugin_id.into(),
            capability_id: capability_id.into(),
            data: Value::Null,
            error: Some(message.into()),
        }
    }
}
