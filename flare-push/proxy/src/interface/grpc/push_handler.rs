//! PushService gRPC：映射协议并调用 `application::handlers` / `queries`。

use std::sync::Arc;

use flare_proto::RpcStatusExt;
use flare_proto::common::RpcStatus;
use flare_proto::push::push_service_server::PushService;
use flare_proto::push::{
    PushCustomRequest, PushCustomResponse, PushMessageRequest, PushMessageResponse,
    PushNotificationRequest, PushNotificationResponse, QueryPushStatusRequest,
    QueryPushStatusResponse,
};
use flare_server_core::utils::require_ctx_from_request;
use tonic::{Request, Response, Status};
use tracing::instrument;

use crate::application::{PushProxyCommandHandler, PushTaskStatusQuery};
use crate::infrastructure::RedisStateStore;

#[derive(Clone)]
pub struct PushServiceHandler {
    command_handler: Arc<PushProxyCommandHandler>,
    status_query: Arc<PushTaskStatusQuery>,
    store: Arc<RedisStateStore>,
}

impl PushServiceHandler {
    pub fn new(
        command_handler: Arc<PushProxyCommandHandler>,
        status_query: Arc<PushTaskStatusQuery>,
        store: Arc<RedisStateStore>,
    ) -> Self {
        Self {
            command_handler,
            status_query,
            store,
        }
    }
}

#[tonic::async_trait]
impl PushService for PushServiceHandler {
    #[instrument(skip(self, request))]
    async fn push_message(
        &self,
        request: Request<PushMessageRequest>,
    ) -> Result<Response<PushMessageResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        if req.user_ids.is_empty() {
            return Ok(Response::new(PushMessageResponse {
                success_count: 0,
                fail_count: 0,
                failed_user_ids: vec![],
                failures: vec![],
                task_id: String::new(),
                status: Some(RpcStatus::default()),
            }));
        }
        self.command_handler
            .enqueue_push_message(&ctx, &req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let task_id = uuid::Uuid::new_v4().to_string();
        self.store
            .save_task_status(&task_id, "pending")
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PushMessageResponse {
            success_count: req.user_ids.len() as i32,
            fail_count: 0,
            failed_user_ids: vec![],
            failures: vec![],
            task_id,
            status: Some(RpcStatus::ok()),
        }))
    }

    #[instrument(skip(self, request))]
    async fn push_notification(
        &self,
        request: Request<PushNotificationRequest>,
    ) -> Result<Response<PushNotificationResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        if req.user_ids.is_empty() {
            return Ok(Response::new(PushNotificationResponse {
                success_count: 0,
                fail_count: 0,
                failures: vec![],
                task_id: String::new(),
                status: Some(RpcStatus::default()),
            }));
        }
        self.command_handler
            .enqueue_push_notification(&ctx, &req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let task_id = uuid::Uuid::new_v4().to_string();
        self.store
            .save_task_status(&task_id, "pending")
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PushNotificationResponse {
            success_count: req.user_ids.len() as i32,
            fail_count: 0,
            failures: vec![],
            task_id,
            status: Some(RpcStatus::ok()),
        }))
    }

    #[instrument(skip(self, request))]
    async fn push_custom(
        &self,
        request: Request<PushCustomRequest>,
    ) -> Result<Response<PushCustomResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        if req.user_ids.is_empty() {
            return Ok(Response::new(PushCustomResponse {
                success_count: 0,
                fail_count: 0,
                failed_user_ids: vec![],
                failures: vec![],
                task_id: String::new(),
                status: Some(RpcStatus::default()),
            }));
        }
        self.command_handler
            .enqueue_push_custom(&ctx, &req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let task_id = uuid::Uuid::new_v4().to_string();
        self.store
            .save_task_status(&task_id, "pending")
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PushCustomResponse {
            success_count: req.user_ids.len() as i32,
            fail_count: 0,
            failed_user_ids: vec![],
            failures: vec![],
            task_id,
            status: Some(RpcStatus::ok()),
        }))
    }

    async fn query_push_status(
        &self,
        request: Request<QueryPushStatusRequest>,
    ) -> Result<Response<QueryPushStatusResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        let status = self
            .status_query
            .get_task_status(&ctx, &req.task_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .unwrap_or_else(|| "unknown".to_string());

        Ok(Response::new(QueryPushStatusResponse {
            task_id: req.task_id,
            status,
            rpc_status: Some(RpcStatus::ok()),
        }))
    }
}
