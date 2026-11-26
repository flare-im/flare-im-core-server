//! Recipient resolution contract DTOs.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::context::{CapabilityInvokeMeta, ConversationKind, ResolveTrigger};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecipientResolveRequest {
    pub meta: CapabilityInvokeMeta,
    pub conversation_id: String,
    pub conversation_kind: ConversationKind,
    pub trigger: ResolveTrigger,
    pub sender_user_id: String,
    pub direct_peer_user_id: Option<String>,
    pub ext: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecipientResolveResult {
    pub recipient_user_ids: Vec<String>,
    pub ext: Value,
}
