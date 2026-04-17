//! RTC 领域 DTO。

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RtcParticipant {
    pub user_id: String,
    pub role: Option<String>,
    pub ext: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateCallRequest {
    pub tenant_id: String,
    pub request_id: String,
    pub conversation_id: String,
    pub initiator_user_id: String,
    pub media: Option<String>,
    pub ext: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateCallResponse {
    pub call_id: String,
    pub room_id: String,
    pub ext: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcceptCallRequest {
    pub tenant_id: String,
    pub request_id: String,
    pub call_id: String,
    pub user_id: String,
    pub ext: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcceptCallResponse {
    pub call_id: String,
    pub ext: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RejectCallRequest {
    pub tenant_id: String,
    pub request_id: String,
    pub call_id: String,
    pub user_id: String,
    pub reason: Option<String>,
    pub ext: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RejectCallResponse {
    pub call_id: String,
    pub ext: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HangupCallRequest {
    pub tenant_id: String,
    pub request_id: String,
    pub call_id: String,
    pub user_id: String,
    pub ext: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HangupCallResponse {
    pub call_id: String,
    pub ext: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetJoinTokenRequest {
    pub tenant_id: String,
    pub request_id: String,
    pub call_id: String,
    pub user_id: String,
    pub ext: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetJoinTokenResponse {
    pub token: String,
    pub ttl_seconds: u32,
    pub ext: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListParticipantsRequest {
    pub tenant_id: String,
    pub request_id: String,
    pub call_id: String,
    pub ext: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListParticipantsResponse {
    pub participants: Vec<RtcParticipant>,
    pub ext: Value,
}
