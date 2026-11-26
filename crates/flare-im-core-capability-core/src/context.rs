//! Capability invocation context shared by capability contracts.

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
