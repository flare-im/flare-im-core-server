//! PushService gRPC：映射协议并调用 `application::handlers` / `queries`。

use std::sync::Arc;

use flare_grpc_proto::push::push_service_server::PushService;
use flare_grpc_proto::push::{
    DevicePushProvider, PushCustomRequest, PushCustomResponse, PushMessageRequest,
    PushMessageResponse, PushNotificationRequest, PushNotificationResponse, QueryPushStatusRequest,
    QueryPushStatusResponse, RegisterDevicePushTokenRequest, RegisterDevicePushTokenResponse,
    UnregisterDevicePushTokenRequest, UnregisterDevicePushTokenResponse,
};
use flare_im_contracts::DevicePushToken;
use flare_server_core::utils::require_ctx_from_request;
use tonic::{Request, Response, Status};
use tracing::instrument;

use crate::application::{PushProxyCommandHandler, PushTaskStatusQuery};
use crate::infrastructure::{RedisDeviceTokenRegistry, RedisStateStore};

#[derive(Clone)]
pub struct PushServiceHandler {
    command_handler: Arc<PushProxyCommandHandler>,
    status_query: Arc<PushTaskStatusQuery>,
    store: Arc<RedisStateStore>,
    device_tokens: Arc<RedisDeviceTokenRegistry>,
}

impl PushServiceHandler {
    pub fn new(
        command_handler: Arc<PushProxyCommandHandler>,
        status_query: Arc<PushTaskStatusQuery>,
        store: Arc<RedisStateStore>,
        device_tokens: Arc<RedisDeviceTokenRegistry>,
    ) -> Self {
        Self {
            command_handler,
            status_query,
            store,
            device_tokens,
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
        }))
    }

    async fn register_device_push_token(
        &self,
        request: Request<RegisterDevicePushTokenRequest>,
    ) -> Result<Response<RegisterDevicePushTokenResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        validate_registration_owner(&ctx, &req.user_id)?;
        let provider = provider_string(req.provider)?;
        require_non_empty(&req.device_id, "device_id")?;
        require_non_empty(&req.platform, "platform")?;
        require_non_empty(&req.token, "token")?;
        let tenant_id = require_tenant_id(&ctx)?;
        let token = DevicePushToken {
            tenant_id,
            user_id: req.user_id,
            device_id: req.device_id.trim().to_string(),
            platform: req.platform.trim().to_string(),
            provider,
            token: req.token.trim().to_string(),
        };
        self.device_tokens
            .register(&token)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(RegisterDevicePushTokenResponse {
            success: true,
        }))
    }

    async fn unregister_device_push_token(
        &self,
        request: Request<UnregisterDevicePushTokenRequest>,
    ) -> Result<Response<UnregisterDevicePushTokenResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        validate_registration_owner(&ctx, &req.user_id)?;
        let provider = provider_string(req.provider)?;
        require_non_empty(&req.device_id, "device_id")?;
        let tenant_id = require_tenant_id(&ctx)?;
        self.device_tokens
            .unregister(&tenant_id, &req.user_id, &provider, req.device_id.trim())
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(UnregisterDevicePushTokenResponse {
            success: true,
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
        }))
    }
}

fn require_tenant_id(ctx: &flare_server_core::context::Ctx) -> Result<String, Status> {
    ctx.tenant_id()
        .map(|tenant_id| tenant_id.trim().to_string())
        .filter(|tenant_id| !tenant_id.is_empty())
        .ok_or_else(|| Status::invalid_argument("tenant_id is required"))
}

fn validate_registration_owner(
    ctx: &flare_server_core::context::Ctx,
    request_user_id: &str,
) -> Result<(), Status> {
    require_non_empty(request_user_id, "user_id")?;
    let authenticated_user_id = ctx
        .user_id()
        .ok_or_else(|| Status::unauthenticated("authenticated user_id is required"))?;
    if authenticated_user_id != request_user_id.trim() {
        return Err(Status::permission_denied(
            "device push token user_id must match authenticated user",
        ));
    }
    Ok(())
}

fn require_non_empty(value: &str, name: &'static str) -> Result<(), Status> {
    if value.trim().is_empty() {
        return Err(Status::invalid_argument(format!("{name} is required")));
    }
    Ok(())
}

fn provider_string(provider: i32) -> Result<String, Status> {
    match DevicePushProvider::try_from(provider) {
        Ok(DevicePushProvider::Getui) => Ok("getui".to_string()),
        Ok(DevicePushProvider::Unspecified) | Err(_) => Err(Status::invalid_argument(
            "valid device push provider is required",
        )),
    }
}
