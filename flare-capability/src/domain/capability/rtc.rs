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

// --- flare-strom-sfu `SfuControl` 信令（经 CapabilityService.Dispatch 透传）---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SfuJoinRoomRequest {
    pub tenant_id: String,
    pub request_id: String,
    pub room_id: String,
    pub call_id: String,
    pub user_id: String,
    pub role: String,
    pub peer_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SfuJoinRoomResponse {
    pub room_id: String,
    pub peer_id: String,
    pub session_id: String,
    pub call_id: String,
    pub ext: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SfuLeaveRoomRequest {
    pub tenant_id: String,
    pub request_id: String,
    pub room_id: String,
    pub peer_id: String,
    pub session_id: String,
    pub user_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SfuLeaveRoomResponse {
    pub left: bool,
    pub ext: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandleSdpOfferRequest {
    pub tenant_id: String,
    pub request_id: String,
    pub room_id: String,
    pub peer_id: String,
    pub sdp_offer: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandleSdpOfferResponse {
    pub sdp_answer: String,
    pub ext: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandleSdpAnswerRequest {
    pub tenant_id: String,
    pub request_id: String,
    pub room_id: String,
    pub peer_id: String,
    pub sdp_answer: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandleSdpAnswerResponse {
    pub accepted: bool,
    pub ext: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddIceCandidateRequest {
    pub tenant_id: String,
    pub request_id: String,
    pub room_id: String,
    pub peer_id: String,
    pub candidate_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddIceCandidateResponse {
    pub accepted: bool,
    pub ext: Value,
}
