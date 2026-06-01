//! 能力调用上下文：会话类型、解析触发点、PreSend 入参（补充传输层 `Ctx`）。

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ConversationKind {
    Direct,
    Group,
    Custom(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ResolveTrigger {
    MessageDelivery,
    RtcInvite,
    Broadcast,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityInvokeMeta {
    pub tenant_id: String,
    pub request_id: String,
    pub ext: Value,
}

impl CapabilityInvokeMeta {
    pub fn new(tenant_id: impl Into<String>, request_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            request_id: request_id.into(),
            ext: Value::Object(Default::default()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreSendEvaluateInput {
    pub meta: CapabilityInvokeMeta,
    pub sender_user_id: String,
    pub conversation_id: String,
    pub conversation_kind: ConversationKind,
    pub rtc_intent: bool,
    pub payload_type: Option<String>,
    /// 消息正文（与 Hook draft.payload 对齐），供进程内 Guard 复用 Social PreSend 逻辑。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Vec<u8>>,
    pub ext: Value,
}
