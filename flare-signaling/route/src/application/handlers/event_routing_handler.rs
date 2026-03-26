//! 事件路由处理器（CQRS 写侧 - 命令）
//!
//! 负责操作事件（撤回/编辑/标记等）的路由编排：流控 → 转发至 Orchestrator ExecuteEvent。
//! 与 message_event_flow 一致：经 Route 顺序/流控/权限后再到 Orchestrator。

use std::sync::Arc;
use std::time::Instant;

use flare_proto::common::event::Payload as EventPayload;
use flare_proto::common::Event;
use flare_proto::signaling::router::RouteOptions;
use flare_server_core::context::{ActorType, Context, ContextExt};
use flare_server_core::error::ErrorCode;
use tracing::instrument;

use crate::application::dto::{build_route_metadata, EventRouteResult};
use crate::domain::service::RouteContext;
use crate::domain::value_objects::{FlowController, DefaultFlowController};
use crate::infrastructure::forwarder::MessageForwarder;

/// 从 Event 构建流控用 RouteContext（operator_id 由 metadata/ctx 注入，proto Event 无此字段）
fn build_route_ctx_from_event(ctx: &Context, svid: &str, event: &Event) -> RouteContext {
    let user_id = ctx.actor().map(|a| a.actor_id().to_string());
    RouteContext {
        svid: svid.to_string(),
        conversation_id: if event.conversation_id.is_empty() {
            None
        } else {
            Some(event.conversation_id.clone())
        },
        user_id,
        tenant_id: ctx.tenant_id().map(|s| s.to_string()),
        client_geo: None,
        login_gateway: None,
    }
}

fn is_admin_actor(ctx: &Context) -> bool {
    let Some(actor) = ctx.actor() else {
        return false;
    };
    if matches!(actor.actor_type, ActorType::TenantAdmin | ActorType::System) {
        return true;
    }
    actor
        .roles
        .iter()
        .any(|r| r.eq_ignore_ascii_case("admin") || r.eq_ignore_ascii_case("owner"))
}

fn is_hard_delete_event(event: &Event) -> bool {
    let Some(EventPayload::Delete(delete)) = event.payload.as_ref() else {
        return false;
    };
    matches!(delete.delete_type, Some(2)) || matches!(delete.scope, Some(2))
}

/// 事件路由处理器（Command 侧）
///
/// 职责：
/// - 编排事件路由流程（流控 → 转发 ExecuteEvent）
/// - 与 MessageRoutingHandler 分离，符合 CQRS 下命令分线
pub struct EventRoutingHandler {
    message_forwarder: Arc<MessageForwarder>,
    flow_controller: Option<Arc<DefaultFlowController>>,
}

impl EventRoutingHandler {
    pub fn new(
        message_forwarder: Arc<MessageForwarder>,
        flow_controller: Option<Arc<DefaultFlowController>>,
    ) -> Self {
        Self {
            message_forwarder,
            flow_controller,
        }
    }

    /// 路由操作事件到业务系统（与 message_event_flow 一致）
    #[instrument(skip(self, ctx, event), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        svid = %svid,
        event_type = ?event.r#type,
    ))]
    pub async fn route_event(
        &self,
        ctx: &Context,
        svid: &str,
        event: Event,
        route_options: RouteOptions,
    ) -> EventRouteResult {
        ctx.ensure_not_cancelled().ok();
        let start_time = Instant::now();
        let decision_start = Instant::now();

        let decision_duration = decision_start.elapsed();

        if is_hard_delete_event(&event) && !is_admin_actor(ctx) {
            let total_duration = start_time.elapsed();
            let op_id = ctx.actor().map(|a| a.actor_id().to_string()).unwrap_or_default();
            tracing::warn!(
                operator_id = %op_id,
                conversation_id = %event.conversation_id,
                "Route rejected hard delete event: operator is not admin/owner"
            );
            return EventRouteResult {
                response_data: vec![],
                routed_endpoint: String::new(),
                metadata: build_route_metadata(
                    total_duration.as_millis() as i64,
                    0,
                    decision_duration.as_millis() as i64,
                    svid,
                    route_options.load_balance_strategy,
                ),
                error_code: Some(ErrorCode::PermissionDenied as u32),
                error_message: Some("Hard delete requires admin or owner role".to_string()),
            };
        }

        if let Some(ref fc) = self.flow_controller {
            let route_ctx = build_route_ctx_from_event(ctx, svid, &event);
            if let Err(e) = fc.check(&route_ctx).await {
                let total_duration = start_time.elapsed();
                tracing::warn!(
                    error = %e,
                    svid = %svid,
                    conversation_id = ?route_ctx.conversation_id,
                    "Flow control rejected event"
                );
                return EventRouteResult {
                    response_data: vec![],
                    routed_endpoint: String::new(),
                    metadata: build_route_metadata(
                        total_duration.as_millis() as i64,
                        0,
                        decision_duration.as_millis() as i64,
                        svid,
                    route_options.load_balance_strategy,
                ),
                    error_code: Some(ErrorCode::ResourceExhausted as u32),
                    error_message: Some(e.to_string()),
                };
            }
        }

        let business_start = Instant::now();
        match self
            .message_forwarder
            .forward_event(ctx, svid, &event, Arc::new(crate::domain::repository::NoopRouteRepository))
            .await
        {
            Ok((endpoint, response_data)) => {
                let business_duration = business_start.elapsed();
                let total_duration = start_time.elapsed();
                tracing::info!(
                    svid = %svid,
                    routed_endpoint = %endpoint,
                    "Event routed successfully"
                );
                EventRouteResult {
                    response_data,
                    routed_endpoint: endpoint,
                    metadata: build_route_metadata(
                        total_duration.as_millis() as i64,
                        business_duration.as_millis() as i64,
                        decision_duration.as_millis() as i64,
                        svid,
                    route_options.load_balance_strategy,
                ),
                    error_code: None,
                    error_message: None,
                }
            }
            Err(e) => {
                let total_duration = start_time.elapsed();
                tracing::error!(error = %e, svid = %svid, "Failed to forward event");
                EventRouteResult {
                    response_data: vec![],
                    routed_endpoint: String::new(),
                    metadata: build_route_metadata(
                        total_duration.as_millis() as i64,
                        0,
                        decision_duration.as_millis() as i64,
                        svid,
                    route_options.load_balance_strategy,
                ),
                    error_code: Some(ErrorCode::InternalError as u32),
                    error_message: Some(format!("Failed to forward event: {}", e)),
                }
            }
        }
    }
}
