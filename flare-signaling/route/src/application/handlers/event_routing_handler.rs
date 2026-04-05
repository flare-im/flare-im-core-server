//! 事件路由处理器（CQRS 写侧 - 命令）
//!
//! 负责操作事件（撤回/编辑/标记等）的路由编排：流控 → 转发至 Orchestrator ExecuteEvent。
//! 与 message_event_flow 一致：经 Route 顺序/流控/权限后再到 Orchestrator。

use std::sync::Arc;
use std::time::Instant;

use flare_proto::common::Event;
use flare_proto::common::event::Payload as EventPayload;
use flare_grpc_proto::signaling::router::RouteOptions;
use flare_server_core::context::{ActorType, Context, ContextExt};
use flare_im_core::error::{ErrorCode, Result, map_infra_error};
use flare_server_core::flare_err;
use tracing::instrument;

use crate::application::dto::{EventRouteResult, build_route_metadata};
use crate::domain::service::RouteContext;
use crate::domain::value_objects::DefaultFlowController;
use crate::infrastructure::forwarder::MessageForwarder;

/// 从 Event 构建流控用 RouteContext（operator_id 由 metadata/ctx 注入，proto Event 无此字段）
fn build_route_ctx_from_event(ctx: &Context, svid: &str, event: &Event) -> RouteContext {
    let user_id = ctx
        .actor()
        .map(|a| a.actor_id().to_string())
        .or_else(|| ctx.user_id().map(|u| u.to_string()));
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
    matches!(delete.delete_type, Some(2))
}

fn operator_id_from_ctx(ctx: &Context) -> String {
    ctx.actor()
        .map(|a| a.actor_id().to_string())
        .or_else(|| ctx.user_id().map(|u| u.to_string()))
        .unwrap_or_default()
}

fn validate_delete_scope(ctx: &Context, event: &Event) -> Result<()> {
    let Some(EventPayload::Delete(delete)) = event.payload.as_ref() else {
        return Ok(());
    };
    // 私有删除仅允许指定为当前操作者本人，防止伪造 target_user_id 删除他人私有视图。
    if matches!(delete.scope, Some(1)) {
        let target = delete.target_user_id.as_deref().unwrap_or_default();
        if !target.is_empty() {
            let operator_id = operator_id_from_ctx(ctx);
            if operator_id.is_empty() || operator_id != target {
                return Err(flare_err!(
                    ErrorCode::PermissionDenied,
                    "delete.user_private.target_must_be_operator"
                ));
            }
        }
    }
    Ok(())
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
    ) -> Result<EventRouteResult> {
        ctx.ensure_not_cancelled().ok();
        let start_time = Instant::now();
        let decision_start = Instant::now();
        validate_delete_scope(ctx, &event)?;

        let decision_duration = decision_start.elapsed();

        if is_hard_delete_event(&event) && !is_admin_actor(ctx) {
            let total_duration = start_time.elapsed();
            let op_id = ctx
                .actor()
                .map(|a| a.actor_id().to_string())
                .unwrap_or_default();
            tracing::warn!(
                operator_id = %op_id,
                conversation_id = %event.conversation_id,
                decision_duration_ms = decision_duration.as_millis(),
                total_duration_ms = total_duration.as_millis(),
                "Route rejected hard delete event: operator is not admin/owner"
            );
            return Err(flare_err!(ErrorCode::PermissionDenied, "Hard delete requires admin or owner role"));
        }

        if let Some(ref fc) = self.flow_controller {
            let route_ctx = build_route_ctx_from_event(ctx, svid, &event);
            fc.check(&route_ctx).await.map_err(|e| {
                let total_duration = start_time.elapsed();
                tracing::warn!(
                    error = %e,
                    svid = %svid,
                    conversation_id = ?route_ctx.conversation_id,
                    decision_duration_ms = decision_duration.as_millis(),
                    total_duration_ms = total_duration.as_millis(),
                    "Flow control rejected event"
                );
                map_infra_error(e, ErrorCode::ResourceExhausted, "Flow control rejected")
            })?;
        }

        let business_start = Instant::now();
        let (endpoint, response_data) = self
            .message_forwarder
            .forward_event(
                ctx,
                svid,
                &event,
                Arc::new(crate::domain::repository::NoopRouteRepository),
            )
            .await
            .map_err(|e| {
                let total_duration = start_time.elapsed();
                tracing::error!(
                    error = %e,
                    svid = %svid,
                    decision_duration_ms = decision_duration.as_millis(),
                    total_duration_ms = total_duration.as_millis(),
                    "Failed to forward event"
                );
                map_infra_error(e, ErrorCode::InternalError, "Failed to forward event")
            })?;

        let business_duration = business_start.elapsed();
        let total_duration = start_time.elapsed();
        tracing::info!(
            svid = %svid,
            routed_endpoint = %endpoint,
            business_duration_ms = business_duration.as_millis(),
            total_duration_ms = total_duration.as_millis(),
            "Event routed successfully"
        );
        
        Ok(EventRouteResult {
            response_data,
            routed_endpoint: endpoint,
            metadata: build_route_metadata(
                total_duration.as_millis() as i64,
                business_duration.as_millis() as i64,
                decision_duration.as_millis() as i64,
                svid,
                route_options.load_balance_strategy,
            ),
        })
    }
}
