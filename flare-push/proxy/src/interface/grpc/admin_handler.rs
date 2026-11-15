//! PushAdminService gRPC：管理员按用户查推送决策链（Console → admin-gateway → Push Proxy）。
//!
//! 与 Online 的管理员只读入口同一种校验：只认 admin-gateway 原样转发的 Staff `x-flare-ctx`
//! （网关 admin 分区签，`kid=gateway`），签名里的权限码必须是 `core:users:read`；租户只取自签名，
//! 请求体不带租户。

use std::sync::Arc;

use flare_grpc_proto::push::push_admin_service_server::PushAdminService;
use flare_grpc_proto::push::{
    AdminQueryPushTraceRequest, AdminQueryPushTraceResponse, PushTraceEntryView, PushTraceView,
};
use flare_im_contracts::push_trace::trace_key;
use flare_im_service_kit::staff_context::require_staff;
use flare_server_core::SignedContextVerifier;
use flare_server_core::auth::core_admin::permission::USERS_READ;
use tonic::{Request, Response, Status};

use crate::infrastructure::{PushTraceRead, PushTraceStore};

/// 不给 `limit` 时列最近多少条消息。
const DEFAULT_TRACE_LIMIT: u32 = 20;
/// 一次至多列多少条消息。
const MAX_TRACE_LIMIT: u32 = 50;
/// 按消息查时消息 ID 的长度上限（也接受「会话#seq」形式的键）。
const MAX_TRACE_KEY_LEN: usize = 256;

#[derive(Clone)]
pub struct PushAdminHandler {
    store: Option<Arc<PushTraceStore>>,
    staff_verifier: Option<SignedContextVerifier>,
}

impl PushAdminHandler {
    /// `store` 为空时查询回不可用；`staff_verifier` 为空时一律回未认证（与 Online 相同）。
    pub fn new(
        store: Option<Arc<PushTraceStore>>,
        staff_verifier: Option<SignedContextVerifier>,
    ) -> Self {
        Self {
            store,
            staff_verifier,
        }
    }
}

fn require_admin_user_id(user_id: &str) -> Result<(), Status> {
    if user_id.trim().is_empty() || user_id.trim() != user_id {
        return Err(Status::invalid_argument("user_id required"));
    }
    Ok(())
}

/// 列最近的消息时的条数：0 取缺省，超过上限的收到上限。
fn trace_limit(limit: u32) -> u32 {
    if limit == 0 {
        DEFAULT_TRACE_LIMIT
    } else {
        limit.min(MAX_TRACE_LIMIT)
    }
}

/// 按消息查时的键。空白算没给；过长或带控制字符的拒绝。
fn requested_trace_key(message_id: Option<&str>) -> Result<Option<String>, Status> {
    let Some(message_id) = message_id.map(str::trim).filter(|id| !id.is_empty()) else {
        return Ok(None);
    };
    if message_id.len() > MAX_TRACE_KEY_LEN || message_id.chars().any(char::is_control) {
        return Err(Status::invalid_argument("message_id is invalid"));
    }
    Ok(Some(trace_key(message_id, "", 0)))
}

fn trace_view(read: PushTraceRead) -> PushTraceView {
    PushTraceView {
        conversation_id: read.conversation_id().to_string(),
        trace_key: read.trace_key,
        first_at_ms: read.first_at_ms,
        entries: read
            .entries
            .into_iter()
            .map(|entry| PushTraceEntryView {
                at_ms: entry.at_ms,
                stage: entry.stage.as_str().to_string(),
                device_id: entry.device_id,
                fields: entry.fields.into_iter().collect(),
            })
            .collect(),
    }
}

#[tonic::async_trait]
impl PushAdminService for PushAdminHandler {
    async fn admin_query_push_trace(
        &self,
        request: Request<AdminQueryPushTraceRequest>,
    ) -> Result<Response<AdminQueryPushTraceResponse>, Status> {
        let verifier = self
            .staff_verifier
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("staff context verifier not configured"))?;
        let staff = require_staff(&request, verifier, USERS_READ)?;
        let req = request.into_inner();
        require_admin_user_id(&req.user_id)?;
        let trace_key = requested_trace_key(req.message_id.as_deref())?;
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::unavailable("push trace store unavailable"))?;
        tracing::info!(actor_id = %staff.actor_id, tenant_id = %staff.tenant_id, request_id = %staff.request_id, "admin query_push_trace");
        let reads = match trace_key {
            Some(trace_key) => store
                .trace(&staff.tenant_id, &req.user_id, &trace_key)
                .await
                .map_err(|error| Status::unavailable(error.to_string()))?
                .into_iter()
                .collect(),
            None => store
                .recent(
                    &staff.tenant_id,
                    &req.user_id,
                    trace_limit(req.limit) as usize,
                )
                .await
                .map_err(|error| Status::unavailable(error.to_string()))?,
        };
        Ok(Response::new(AdminQueryPushTraceResponse {
            traces: reads.into_iter().map(trace_view).collect(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_im_contracts::push_trace::{PushTraceEntry, TraceStage};

    #[test]
    fn the_limit_defaults_to_twenty_and_stops_at_fifty() {
        assert_eq!(trace_limit(0), 20);
        assert_eq!(trace_limit(7), 7);
        assert_eq!(trace_limit(50), 50);
        assert_eq!(trace_limit(500), 50);
    }

    #[test]
    fn a_message_id_names_one_trace_and_blank_means_recent() {
        assert_eq!(requested_trace_key(None).unwrap(), None);
        assert_eq!(requested_trace_key(Some("  ")).unwrap(), None);
        assert_eq!(
            requested_trace_key(Some(" m-9 ")).unwrap().as_deref(),
            Some("m-9")
        );
        assert_eq!(
            requested_trace_key(Some("c-1#123")).unwrap().as_deref(),
            Some("c-1#123"),
            "没有消息 ID 的链按「会话#seq」查"
        );
        assert!(requested_trace_key(Some("m\n9")).is_err());
        assert!(requested_trace_key(Some("m".repeat(257).as_str())).is_err());
    }

    #[test]
    fn user_ids_must_be_given_without_surrounding_blanks() {
        assert!(require_admin_user_id("u-1").is_ok());
        for bad in ["", "  ", " u-1"] {
            assert_eq!(
                require_admin_user_id(bad).unwrap_err().code(),
                tonic::Code::InvalidArgument
            );
        }
    }

    #[test]
    fn a_read_becomes_a_view_with_the_first_conversation() {
        let read = PushTraceRead {
            trace_key: "m-9".into(),
            first_at_ms: 10,
            entries: vec![
                PushTraceEntry::new(10, TraceStage::Route, "d-1")
                    .with_conversation("c-1")
                    .with("state", "parked"),
                PushTraceEntry::new(20, TraceStage::Receipt, "d-1").with_conversation("c-1"),
            ],
        };
        let view = trace_view(read);
        assert_eq!(view.trace_key, "m-9");
        assert_eq!(view.conversation_id, "c-1");
        assert_eq!(view.first_at_ms, 10);
        assert_eq!(view.entries.len(), 2);
        assert_eq!(view.entries[0].stage, "route");
        assert_eq!(
            view.entries[0].fields.get("state").map(String::as_str),
            Some("parked")
        );
        assert_eq!(view.entries[1].stage, "receipt");
    }

    /// 没配验签器时与 Online 相同：一律未认证，不碰存储。
    #[tokio::test]
    async fn without_a_staff_verifier_every_query_is_unauthenticated() {
        let handler = PushAdminHandler::new(None, None);
        let status = handler
            .admin_query_push_trace(Request::new(AdminQueryPushTraceRequest {
                user_id: "u-1".into(),
                message_id: None,
                limit: 0,
            }))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }
}
