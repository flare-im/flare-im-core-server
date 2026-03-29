//! 消息路由处理器（CQRS 写侧 - 命令）
//!
//! 负责「发送消息」的路由编排：流控 → 转发至 Orchestrator SendMessage。
//! 与 `EventRoutingHandler` / `AckRoutingHandler` / `DataRoutingHandler` 分离。

use std::sync::Arc;
use std::time::Instant;

use flare_proto::common::Message;
use flare_proto::signaling::router::RouteOptions;
use flare_server_core::context::{Context, ContextExt};
use flare_server_core::error::ErrorCode;
use tracing::instrument;

use crate::application::dto::{MessageRouteResult, build_route_metadata};
use crate::domain::service::RouteContext;
use crate::domain::value_objects::DefaultFlowController;
use crate::infrastructure::forwarder::MessageForwarder;

fn build_route_ctx_for_flow(ctx: &Context, svid: &str, message: &Message) -> Option<RouteContext> {
    let conversation_id = if message.conversation_id.is_empty() {
        None
    } else {
        Some(message.conversation_id.clone())
    };
    Some(RouteContext {
        svid: svid.to_string(),
        conversation_id,
        user_id: if message.sender_id.is_empty() {
            None
        } else {
            Some(message.sender_id.clone())
        },
        tenant_id: ctx.tenant_id().map(|s| s.to_string()),
        client_geo: None,
        login_gateway: None,
    })
}

pub struct MessageRoutingHandler {
    message_forwarder: Arc<MessageForwarder>,
    flow_controller: Option<Arc<DefaultFlowController>>,
}

impl MessageRoutingHandler {
    pub fn new(
        message_forwarder: Arc<MessageForwarder>,
        flow_controller: Option<Arc<DefaultFlowController>>,
    ) -> Self {
        Self {
            message_forwarder,
            flow_controller,
        }
    }

    #[instrument(skip(self, ctx, message), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        svid = %svid,
    ))]
    pub async fn route_message(
        &self,
        ctx: &Context,
        svid: &str,
        message: Message,
        route_options: RouteOptions,
    ) -> MessageRouteResult {
        ctx.ensure_not_cancelled()
            .map_err(|e| {
                flare_server_core::error::ErrorBuilder::new(
                    ErrorCode::InternalError,
                    "Request cancelled",
                )
                .details(e.to_string())
                .build_error()
            })
            .ok();
        let start_time = Instant::now();
        let decision_start = Instant::now();
        let decision_duration = decision_start.elapsed();

        if let Some(ref fc) = self.flow_controller {
            if let Some(route_ctx) = build_route_ctx_for_flow(ctx, svid, &message) {
                if let Err(e) = fc.check(&route_ctx).await {
                    let total_duration = start_time.elapsed();
                    tracing::warn!(
                        error = %e,
                        svid = %svid,
                        conversation_id = ?route_ctx.conversation_id,
                        "Flow control rejected"
                    );
                    return MessageRouteResult {
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
        }

        let business_start = Instant::now();
        match self
            .message_forwarder
            .forward_message(
                ctx,
                svid,
                message,
                Arc::new(crate::domain::repository::NoopRouteRepository),
            )
            .await
        {
            Ok((endpoint, response_data)) => {
                let business_duration = business_start.elapsed();
                let total_duration = start_time.elapsed();
                tracing::info!(
                    svid = %svid,
                    routed_endpoint = %endpoint,
                    response_len = %response_data.len(),
                    decision_duration_ms = decision_duration.as_millis(),
                    business_duration_ms = business_duration.as_millis(),
                    total_duration_ms = total_duration.as_millis(),
                    "Message routed successfully"
                );
                MessageRouteResult {
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
                tracing::error!(
                    error = %e,
                    svid = %svid,
                    decision_duration_ms = decision_duration.as_millis(),
                    total_duration_ms = total_duration.as_millis(),
                    "Failed to forward message to business system"
                );
                MessageRouteResult {
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
                    error_message: Some(format!("Failed to forward message: {}", e)),
                }
            }
        }
    }
}
