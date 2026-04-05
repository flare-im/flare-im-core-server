//! DATA 通道 UserCustom：`common.CustomData` 上行路由（下游编排 RPC 未全时可为空响应）。

use std::sync::Arc;
use std::time::Instant;

use flare_proto::common::CustomData;
use flare_grpc_proto::signaling::router::RouteOptions;
use flare_server_core::context::Context;
use flare_im_core::error::{ErrorCode, Result, map_infra_error};
use tracing::instrument;

use crate::application::dto::{MessageRouteResult, build_route_metadata};
use crate::infrastructure::forwarder::MessageForwarder;

pub struct DataRoutingHandler {
    forwarder: Arc<MessageForwarder>,
}

impl DataRoutingHandler {
    pub fn new(forwarder: Arc<MessageForwarder>) -> Self {
        Self { forwarder }
    }

    #[instrument(skip(self, ctx, data), fields(request_id = %ctx.request_id(), trace_id = %ctx.trace_id(), svid = %svid))]
    pub async fn route_data(
        &self,
        ctx: &Context,
        svid: &str,
        data: CustomData,
        route_options: RouteOptions,
    ) -> Result<MessageRouteResult> {
        let start_time = Instant::now();
        let decision_duration = start_time.elapsed();
        let business_start = Instant::now();

        let (endpoint, response_data) = self
            .forwarder
            .forward_custom_data(
                ctx,
                svid,
                data,
                Arc::new(crate::domain::repository::NoopRouteRepository),
            )
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::InternalError, "forward custom data"))?;

        let business_duration = business_start.elapsed();
        let total_duration = start_time.elapsed();
        
        Ok(MessageRouteResult {
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
