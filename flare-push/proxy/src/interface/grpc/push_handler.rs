//! PushService gRPC：映射协议并调用 `application::handlers` / `queries`。

use std::collections::HashMap;
use std::sync::Arc;

use flare_grpc_proto::push::push_service_server::PushService;
use flare_grpc_proto::push::{
    DevicePushProvider, InspectDevicePushBindingRequest, InspectDevicePushBindingResponse,
    PushCapabilityRequest, PushCapabilityResponse, PushCustomRequest, PushCustomResponse,
    PushMessageRequest, PushMessageResponse, PushNotificationRequest, PushNotificationResponse,
    PushReceipt, QueryPushStatusRequest, QueryPushStatusResponse, RegisterDevicePushTokenRequest,
    RegisterDevicePushTokenResponse, ReportDeviceHandoffRequest, ReportDeviceHandoffResponse,
    ReportPushReceiptsRequest, ReportPushReceiptsResponse, RevokeDeviceBindingsRequest,
    RevokeDeviceBindingsResponse, RevokeDevicePushBindingRequest, RevokeDevicePushBindingResponse,
};
use flare_im_contracts::Ctx;
use flare_im_contracts::DevicePushToken;
use flare_im_contracts::domain::push_binding::{
    APNS_ENVIRONMENT_PRODUCTION, APNS_ENVIRONMENT_SANDBOX, PUSH_BINDING_CAPABILITIES,
};
use flare_im_contracts::push_trace::{PushTraceEntry, TraceStage, trace_key};
use flare_im_contracts::service_names::{ACCESS_GATEWAY, SIGNALING_ONLINE};
use flare_im_service_kit::metrics::{PushProxyMetrics, sanitized_app_version};
use flare_im_service_kit::push_trace::PushTraceRecorder;
use flare_proto::common::{DeviceHandoff, DeviceHandoffCause};
use flare_server_core::auth::{SignedActorType, SignedContextClaims};
use flare_server_core::grpc::middleware::signed_context::VerifiedContext;
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
    /// Why enqueue is refused (`None` = broker provenance is enforced); decided once at startup.
    delivery_refusal: Option<Arc<str>>,
    /// 到达回执写进推送决策链；未接线时不记。
    trace: PushTraceRecorder,
    /// 到达回执的计数；未接线时不计。
    metrics: Option<Arc<PushProxyMetrics>>,
}

impl PushServiceHandler {
    pub fn new(
        command_handler: Arc<PushProxyCommandHandler>,
        status_query: Arc<PushTaskStatusQuery>,
        store: Arc<RedisStateStore>,
        device_tokens: Arc<RedisDeviceTokenRegistry>,
        delivery_refusal: Option<String>,
    ) -> Self {
        Self {
            command_handler,
            status_query,
            store,
            device_tokens,
            delivery_refusal: delivery_refusal.map(Arc::from),
            trace: PushTraceRecorder::disabled(),
            metrics: None,
        }
    }

    /// 接入到达回执的去处：推送决策链与 `flare_push_receipts_total`。
    pub fn with_receipts(
        mut self,
        trace: PushTraceRecorder,
        metrics: Arc<PushProxyMetrics>,
    ) -> Self {
        self.trace = trace;
        self.metrics = Some(metrics);
        self
    }

    /// Enqueue only when the Push consumers can run: acknowledging a task that no
    /// consumer may process would silently drop it.
    fn require_delivery_ready(&self) -> Result<(), Status> {
        match &self.delivery_refusal {
            None => Ok(()),
            Some(reason) => Err(Status::unavailable(reason.to_string())),
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
        let ctx = require_verified_actor(&request, SignedActorType::Service)?;
        let tenant_id = require_tenant_id(&ctx)?;
        require_metadata_tenant(&request.get_ref().metadata, &tenant_id)?;
        if let Some(options) = request.get_ref().options.as_ref() {
            require_metadata_tenant(&options.metadata, &tenant_id)?;
        }
        self.require_delivery_ready()?;
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
            .save_task_status(&tenant_id, &task_id, "pending")
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
        let ctx = require_verified_actor(&request, SignedActorType::Service)?;
        let tenant_id = require_tenant_id(&ctx)?;
        require_metadata_tenant(&request.get_ref().metadata, &tenant_id)?;
        if let Some(options) = request.get_ref().options.as_ref() {
            require_metadata_tenant(&options.metadata, &tenant_id)?;
        }
        if let Some(notification) = request.get_ref().notification.as_ref() {
            require_metadata_tenant(&notification.metadata, &tenant_id)?;
        }
        self.require_delivery_ready()?;
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
            .save_task_status(&tenant_id, &task_id, "pending")
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
        let ctx = require_verified_actor(&request, SignedActorType::Service)?;
        let tenant_id = require_tenant_id(&ctx)?;
        require_metadata_tenant(&request.get_ref().metadata, &tenant_id)?;
        if let Some(options) = request.get_ref().options.as_ref() {
            require_metadata_tenant(&options.metadata, &tenant_id)?;
        }
        if let Some(custom_data) = request.get_ref().custom_data.as_ref() {
            require_metadata_tenant(&custom_data.attributes, &tenant_id)?;
        }
        self.require_delivery_ready()?;
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
            .save_task_status(&tenant_id, &task_id, "pending")
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

    /// Online-only plugin realtime packet. No task status is kept: there is nothing to query
    /// for a packet that is dropped once delivered or expired.
    #[instrument(skip(self, request))]
    async fn push_capability(
        &self,
        request: Request<PushCapabilityRequest>,
    ) -> Result<Response<PushCapabilityResponse>, Status> {
        let ctx = require_verified_actor(&request, SignedActorType::Service)?;
        require_tenant_id(&ctx)?;
        validate_capability_request(request.get_ref())?;
        self.require_delivery_ready()?;
        let req = request.into_inner();
        self.command_handler
            .enqueue_push_capability(&ctx, &req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PushCapabilityResponse {
            accepted_user_count: req.user_ids.len() as i32,
        }))
    }

    async fn register_device_push_token(
        &self,
        request: Request<RegisterDevicePushTokenRequest>,
    ) -> Result<Response<RegisterDevicePushTokenResponse>, Status> {
        let ctx = require_verified_actor(&request, SignedActorType::User)?;
        let req = request.into_inner();
        validate_registration_owner(&ctx, &req.user_id)?;
        let provider = provider_string(req.provider)?;
        require_non_empty(&req.device_id, "device_id")?;
        require_non_empty(&req.platform, "platform")?;
        require_non_empty(&req.token, "token")?;
        validate_device_registration(&provider, &req.device_id, &req.platform, &req.token)?;
        let attributes = binding_attributes(&provider, &req)?;
        let tenant_id = require_tenant_id(&ctx)?;
        let token = DevicePushToken {
            tenant_id,
            // 归属校验按去掉首尾空白后的用户比较，登记也必须存同一个值：
            // 带空白存进去的绑定，路由、投递和吊销按规范化的用户都查不到。
            user_id: req.user_id.trim().to_string(),
            device_id: req.device_id.trim().to_string(),
            platform: req.platform.trim().to_string(),
            provider,
            token: req.token.trim().to_string(),
            registration_id: req.binding_id.clone(),
            registered_at_ms: 0, // Redis records its own time; replay digest remains immutable.
            app_bundle: attributes.app_bundle,
            environment: attributes.environment,
            locale: attributes.locale,
            time_zone: attributes.time_zone,
            app_version: attributes.app_version,
            capabilities: attributes.capabilities,
            // 活跃时间由 Redis 在核对时写；登记请求里固定为 0，重放摘要才不变。
            last_seen_ms: 0,
        };
        self.device_tokens
            .register(&token, &req.expected_version, req.expires_at_ms)
            .await
            .map_err(binding_error)?;
        Ok(Response::new(RegisterDevicePushTokenResponse {
            success: true,
            binding_id: req.binding_id,
        }))
    }

    async fn inspect_device_push_binding(
        &self,
        request: Request<InspectDevicePushBindingRequest>,
    ) -> Result<Response<InspectDevicePushBindingResponse>, Status> {
        let ctx = require_verified_actor(&request, SignedActorType::User)?;
        let req = request.into_inner();
        validate_registration_owner(&ctx, &req.user_id)?;
        let provider = provider_string(req.provider)?;
        require_non_empty(&req.device_id, "device_id")?;
        let tenant_id = require_tenant_id(&ctx)?;
        let row = self
            .device_tokens
            .inspect(
                &tenant_id,
                &req.user_id,
                &provider,
                req.device_id.trim(),
                &req.binding_id,
            )
            .await
            .map_err(binding_error)?;
        Ok(Response::new(InspectDevicePushBindingResponse {
            version: row.version,
            current_binding_id: row.current_binding_id,
            state: row.state,
        }))
    }

    async fn revoke_device_push_binding(
        &self,
        request: Request<RevokeDevicePushBindingRequest>,
    ) -> Result<Response<RevokeDevicePushBindingResponse>, Status> {
        let ctx = require_verified_actor(&request, SignedActorType::Service)?;
        let claims = ctx
            .get_data::<VerifiedContext>()
            .and_then(|marker| marker.claims())
            .ok_or_else(|| Status::permission_denied("gateway revoke capability required"))?;
        if claims.credential_id.as_deref() != Some("api-gateway") {
            return Err(Status::permission_denied(
                "gateway revoke capability required",
            ));
        }
        self.device_tokens
            .revoke(&request.into_inner().revocation_token)
            .await
            .map_err(binding_error)?;
        Ok(Response::new(RevokeDevicePushBindingResponse {
            success: true,
        }))
    }

    /// 设备交接入队。不在任何用户路径上：只有接入网关与 Online 的已验证服务身份可以上报，
    /// 否则任何能连到这个端口的进程都能替别人的设备触发推送。
    #[instrument(skip(self, request))]
    async fn report_device_handoff(
        &self,
        request: Request<ReportDeviceHandoffRequest>,
    ) -> Result<Response<ReportDeviceHandoffResponse>, Status> {
        let ctx = require_verified_actor(&request, SignedActorType::Service)?;
        let signed_tenant = require_handoff_reporter(&ctx)?;
        let handoff = request
            .into_inner()
            .handoff
            .ok_or_else(|| Status::invalid_argument("device handoff required"))?;
        validate_device_handoff(&handoff, Some(signed_tenant.as_str()))?;
        self.require_delivery_ready()?;
        self.command_handler
            .enqueue_device_handoff(&ctx, &handoff)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(ReportDeviceHandoffResponse {
            accepted: true,
        }))
    }

    /// 服务端吊销推送绑定：设备被踢下线、被另一台设备顶替、被租户踢出，或它的身份会话被吊销之后，
    /// 这台设备不该再收到任何推送。不在任何用户路径上，只有 Online 的已验证服务身份可以调用：
    /// 请求体点名了用户与设备，谁都能调就等于谁都能让别人的设备收不到推送。
    ///
    /// 可以重复调用：已经吊销的绑定不会再被数到，返回 0 也是成功。
    #[instrument(skip(self, request))]
    async fn revoke_device_bindings(
        &self,
        request: Request<RevokeDeviceBindingsRequest>,
    ) -> Result<Response<RevokeDeviceBindingsResponse>, Status> {
        let ctx = require_verified_actor(&request, SignedActorType::Service)?;
        let signed_tenant = require_binding_revoker(&ctx)?;
        let req = request.into_inner();
        validate_binding_revocation(&req, &signed_tenant)?;
        // 登记时设备 ID 去掉了首尾空白，这里用同样的写法去比。
        let device_id = req.device_id.trim();
        let revoked = self
            .device_tokens
            .revoke_bindings(
                &req.tenant_id,
                &req.user_id,
                device_id,
                req.registered_before,
            )
            .await
            .map_err(binding_error)?;
        tracing::info!(
            tenant_id = %req.tenant_id,
            user_id = %req.user_id,
            device_id = %revoked_device_label(device_id),
            reason = %req.reason,
            revoked,
            "Revoked device push bindings"
        );
        Ok(Response::new(RevokeDeviceBindingsResponse { revoked }))
    }

    /// 到达回执：只接受网关代签的用户身份，租户与用户必须等于签名。一条一条校验，认不出的跳过，
    /// 不让整批失败；记下的每条进决策链（`receipt`）与 `flare_push_receipts_total`。
    /// 回执是尽力而为的观测数据，不入队、不影响投递，所以不看 broker 的来源校验是否就绪。
    #[instrument(skip(self, request))]
    async fn report_push_receipts(
        &self,
        request: Request<ReportPushReceiptsRequest>,
    ) -> Result<Response<ReportPushReceiptsResponse>, Status> {
        let ctx = require_verified_actor(&request, SignedActorType::User)?;
        let tenant_id = require_tenant_id(&ctx)?;
        let req = request.into_inner();
        validate_registration_owner(&ctx, &req.user_id)?;
        if !req.tenant_id.trim().is_empty() && req.tenant_id.trim() != tenant_id {
            return Err(Status::permission_denied(
                "receipt tenant must match the signed tenant",
            ));
        }
        let device_id = receipt_device_id(&req.device_id)?;
        if req.receipts.len() > MAX_RECEIPTS_PER_REPORT {
            return Err(Status::invalid_argument(format!(
                "at most {MAX_RECEIPTS_PER_REPORT} receipts per report"
            )));
        }
        let user_id = req.user_id.trim();
        let app_version = receipt_app_version(&req.app_version);
        let received_ms = chrono::Utc::now().timestamp_millis();
        let mut accepted: u32 = 0;
        for receipt in &req.receipts {
            let Some(receipt) = ValidReceipt::of(receipt) else {
                continue;
            };
            let key = trace_key(receipt.message_id, receipt.conversation_id, receipt.seq);
            let mut entry = PushTraceEntry::new(received_ms, TraceStage::Receipt, device_id)
                .with_conversation(receipt.conversation_id)
                .with("provider", receipt.provider)
                .with("path", receipt.path)
                .with("app_version", app_version);
            if receipt.received_at_ms > 0 {
                entry = entry.with("received_at_ms", receipt.received_at_ms.to_string());
            }
            if !receipt.disposition.is_empty() {
                entry = entry.with("disposition", receipt.disposition);
            }
            self.trace.record(&tenant_id, user_id, &key, entry);
            if let Some(metrics) = &self.metrics {
                metrics.record_receipt(receipt.provider, app_version, receipt.path);
            }
            accepted += 1;
        }
        Ok(Response::new(ReportPushReceiptsResponse { accepted }))
    }

    async fn query_push_status(
        &self,
        request: Request<QueryPushStatusRequest>,
    ) -> Result<Response<QueryPushStatusResponse>, Status> {
        let ctx = require_verified_actor(&request, SignedActorType::Service)?;
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

fn binding_error(error: crate::infrastructure::BindingError) -> Status {
    use crate::infrastructure::BindingError;
    match error {
        BindingError::Conflict | BindingError::Revoked => {
            Status::failed_precondition("push binding version conflict or revoked")
        }
        BindingError::Invalid => Status::invalid_argument("push binding command rejected"),
        BindingError::Limit => Status::resource_exhausted("device registration limit"),
        BindingError::Unavailable => Status::unavailable("device registry unavailable"),
    }
}

/// The downstream Push Server/Worker consumers are intentionally paused until
/// signed MQ ingress and both delivery stages have been verified. Do not
/// acknowledge new tasks as "pending" when no consumer can deliver them.
fn require_tenant_id(ctx: &flare_server_core::context::Ctx) -> Result<String, Status> {
    ctx.tenant_id()
        .filter(|tenant_id| {
            flare_im_contracts::domain::tenant::is_valid_tenant_id(tenant_id)
                && flare_im_contracts::utils::normalize_tenant_id(tenant_id) == *tenant_id
        })
        .map(ToString::to_string)
        .ok_or_else(|| Status::permission_denied("canonical signed tenant_id is required"))
}

fn require_verified_actor<T>(
    request: &Request<T>,
    expected: SignedActorType,
) -> Result<Ctx, Status> {
    let ctx = require_ctx_from_request(request)?;
    let claims = ctx
        .get_data::<VerifiedContext>()
        .and_then(|marker| marker.claims())
        .ok_or_else(|| Status::unauthenticated("signed Push Proxy identity required"))?;
    validate_actor_claims(claims, expected)?;
    Ok(ctx)
}

fn validate_actor_claims(
    claims: &SignedContextClaims,
    expected: SignedActorType,
) -> Result<(), Status> {
    if claims.actor_type != expected
        || !flare_im_contracts::domain::tenant::is_valid_tenant_id(&claims.tenant_id)
        || flare_im_contracts::utils::normalize_tenant_id(&claims.tenant_id) != claims.tenant_id
    {
        return Err(Status::permission_denied(
            "Push Proxy actor or tenant not permitted",
        ));
    }
    Ok(())
}

/// 可以上报设备交接的内部工作负载：接入网关（客户端托管、送达确认超时、链路断开）与
/// Online（清扫失联网关）。
const HANDOFF_REPORTERS: [&str; 2] = [ACCESS_GATEWAY, SIGNALING_ONLINE];
/// 一次交接最多带多少条待补提示的条目。网关每条连接的未确认环是 64 项，这里留一倍余量；
/// 再多就不是一条连接攒得出来的量了。
const MAX_HANDOFF_PENDING: usize = 128;

/// 调用方必须是 [`HANDOFF_REPORTERS`] 之一的已验证服务身份；返回它签名里的租户。
fn require_handoff_reporter(ctx: &Ctx) -> Result<String, Status> {
    let claims = ctx
        .get_data::<VerifiedContext>()
        .and_then(|marker| marker.claims())
        .ok_or_else(|| Status::permission_denied("device handoff reporter identity required"))?;
    validate_handoff_reporter(claims)?;
    Ok(claims.tenant_id.clone())
}

fn validate_handoff_reporter(claims: &SignedContextClaims) -> Result<(), Status> {
    if claims.actor_type != SignedActorType::Service
        || !HANDOFF_REPORTERS.contains(&claims.actor_id.as_str())
    {
        return Err(Status::permission_denied(
            "device handoff reporter not permitted",
        ));
    }
    Ok(())
}

/// 交接体校验。`signed_tenant` 是调用方已验证上下文里的租户：上下文带租户时交接必须属于它，
/// 一个租户的工作负载身份不能替另一个租户的设备上报。
fn validate_device_handoff(
    handoff: &DeviceHandoff,
    signed_tenant: Option<&str>,
) -> Result<(), Status> {
    if handoff.tenant_id.trim().is_empty()
        || handoff.user_id.trim().is_empty()
        || handoff.device_id.trim().is_empty()
    {
        return Err(Status::invalid_argument(
            "device handoff requires tenant, user and device",
        ));
    }
    if signed_tenant.is_some_and(|tenant| tenant != handoff.tenant_id) {
        return Err(Status::permission_denied("device handoff tenant mismatch"));
    }
    if handoff.pending.len() > MAX_HANDOFF_PENDING {
        return Err(Status::invalid_argument(
            "device handoff carries too many pending items",
        ));
    }
    match DeviceHandoffCause::try_from(handoff.cause) {
        Ok(DeviceHandoffCause::Unspecified) | Err(_) => {
            Err(Status::invalid_argument("device handoff cause is required"))
        }
        Ok(_) => Ok(()),
    }
}

/// 服务端吊销绑定的原因，只用于诊断。四种情形各一个：被踢下线、身份会话被吊销、
/// 被租户踢出、被另一台设备顶替。
const BINDING_REVOCATION_REASONS: [&str; 4] = [
    "kicked",
    "identity_revoked",
    "tenant_kicked",
    "device_replaced",
];

/// 调用方必须是 Online 的已验证服务身份；返回它签名里的租户。接入网关不在其中：
/// 决定一台设备该不该再收推送的是会话与身份的状态，那是 Online 的事。
fn require_binding_revoker(ctx: &Ctx) -> Result<String, Status> {
    let claims = ctx
        .get_data::<VerifiedContext>()
        .and_then(|marker| marker.claims())
        .ok_or_else(|| Status::permission_denied("binding revoker identity required"))?;
    validate_binding_revoker(claims)?;
    Ok(claims.tenant_id.clone())
}

fn validate_binding_revoker(claims: &SignedContextClaims) -> Result<(), Status> {
    if claims.actor_type != SignedActorType::Service || claims.actor_id != SIGNALING_ONLINE {
        return Err(Status::permission_denied("binding revoker not permitted"));
    }
    Ok(())
}

/// 吊销请求体校验。`signed_tenant` 是调用方已验证上下文里的租户：请求必须属于它，
/// 一个租户的工作负载身份不能吊销另一个租户的绑定。
fn validate_binding_revocation(
    req: &RevokeDeviceBindingsRequest,
    signed_tenant: &str,
) -> Result<(), Status> {
    if req.tenant_id != signed_tenant {
        return Err(Status::permission_denied(
            "binding revocation tenant mismatch",
        ));
    }
    require_non_empty(&req.user_id, "user_id")?;
    // 空的设备 ID 表示这个用户的全部设备；只有空白的设备 ID 说不清是哪一种，拒绝。
    if !req.device_id.is_empty() && req.device_id.trim().is_empty() {
        return Err(Status::invalid_argument("device_id is blank"));
    }
    // 没有时间上界的吊销会把设备重新登录后登记的新绑定一起清掉。
    if req.registered_before <= 0 {
        return Err(Status::invalid_argument("registered_before is required"));
    }
    if !BINDING_REVOCATION_REASONS.contains(&req.reason.as_str()) {
        return Err(Status::invalid_argument(
            "unknown binding revocation reason",
        ));
    }
    Ok(())
}

/// 日志里的设备：空的设备 ID 记成 `all`。
fn revoked_device_label(device_id: &str) -> &str {
    if device_id.is_empty() {
        "all"
    } else {
        device_id
    }
}

/// 一次上报至多多少条到达回执。
const MAX_RECEIPTS_PER_REPORT: usize = 100;
/// 回执里会话、消息与合并键的长度上限：真实取值都远短于它。
const MAX_RECEIPT_ID_LEN: usize = 128;
/// 认不出的处置的统一取值。
const RECEIPT_OTHER: &str = "other";
/// 回执里认得的处置（推送载荷的 `disposition`）。
const RECEIPT_DISPOSITIONS: [&str; 6] = [
    "loud",
    "quiet",
    "clear",
    "badge_only",
    "recall_replace",
    "digest",
];

fn receipt_device_id(raw: &str) -> Result<&str, Status> {
    let device_id = raw.trim();
    if device_id.is_empty() || device_id.len() > 128 || device_id.chars().any(char::is_control) {
        return Err(Status::invalid_argument("device_id is invalid"));
    }
    Ok(device_id)
}

/// 应用版本：至多 32 个 `[0-9A-Za-z._+-]` 字符，其余（含空）记为 `other`。决策链记这个值；
/// 指标在此之上再限个数（`flare_im_service_kit::metrics::app_version_label`，每进程 64 个），
/// 与 Push Worker 的发送计数同一份规则。
fn receipt_app_version(raw: &str) -> &str {
    sanitized_app_version(raw)
}

/// 回执里的标识：去掉首尾空白，过长或带控制字符的不收。
fn receipt_id(raw: &str) -> Option<&str> {
    let value = raw.trim();
    (value.len() <= MAX_RECEIPT_ID_LEN && !value.chars().any(char::is_control)).then_some(value)
}

/// 校验过的一条回执。通道与来路是固定的小集合，处置认不出时记为 `other`。
#[derive(Debug, PartialEq, Eq)]
struct ValidReceipt<'a> {
    conversation_id: &'a str,
    message_id: &'a str,
    seq: u64,
    provider: &'static str,
    path: &'static str,
    disposition: &'a str,
    received_at_ms: i64,
}

impl<'a> ValidReceipt<'a> {
    /// 认不出通道或来路、标识不合格、或会话与消息都没有的回执为 `None`。
    fn of(receipt: &'a PushReceipt) -> Option<Self> {
        let provider = match receipt.provider.trim() {
            "apns" => "apns",
            "fcm" => "fcm",
            "getui" => "getui",
            _ => return None,
        };
        let path = match receipt.path.trim() {
            "extension" => "extension",
            "app" => "app",
            _ => return None,
        };
        let conversation_id = receipt_id(&receipt.conversation_id)?;
        let message_id = receipt_id(&receipt.message_id)?;
        receipt_id(&receipt.collapse_id)?;
        if conversation_id.is_empty() && message_id.is_empty() {
            return None;
        }
        let disposition = match receipt.disposition.trim() {
            "" => "",
            known if RECEIPT_DISPOSITIONS.contains(&known) => known,
            _ => RECEIPT_OTHER,
        };
        Some(Self {
            conversation_id,
            message_id,
            seq: receipt.seq,
            provider,
            path,
            disposition,
            received_at_ms: receipt.received_at_ms,
        })
    }
}

#[cfg(test)]
mod receipt_tests {
    use super::*;

    fn receipt() -> PushReceipt {
        PushReceipt {
            conversation_id: "c-1".into(),
            message_id: "m-9".into(),
            seq: 123,
            collapse_id: "c:0123456789abcdef0123456789abcdef".into(),
            disposition: "loud".into(),
            provider: "apns".into(),
            path: "extension".into(),
            received_at_ms: 1_731_400_000_000,
        }
    }

    #[test]
    fn a_well_formed_receipt_is_accepted_as_is() {
        let raw = receipt();
        let valid = ValidReceipt::of(&raw).expect("valid");
        assert_eq!(valid.provider, "apns");
        assert_eq!(valid.path, "extension");
        assert_eq!(valid.disposition, "loud");
        assert_eq!(valid.message_id, "m-9");
        assert_eq!(
            trace_key(valid.message_id, valid.conversation_id, valid.seq),
            "m-9"
        );
        let without_message = PushReceipt {
            message_id: String::new(),
            ..receipt()
        };
        let valid = ValidReceipt::of(&without_message).expect("conversation and seq suffice");
        assert_eq!(
            trace_key(valid.message_id, valid.conversation_id, valid.seq),
            "c-1#123"
        );
    }

    #[test]
    fn unknown_providers_paths_and_ids_are_skipped() {
        for bad in [
            PushReceipt {
                provider: "huawei".into(),
                ..receipt()
            },
            PushReceipt {
                provider: "APNS".into(),
                ..receipt()
            },
            PushReceipt {
                path: "widget".into(),
                ..receipt()
            },
            PushReceipt {
                message_id: "m\n9".into(),
                ..receipt()
            },
            PushReceipt {
                conversation_id: "c".repeat(MAX_RECEIPT_ID_LEN + 1),
                ..receipt()
            },
            PushReceipt {
                conversation_id: String::new(),
                message_id: "  ".into(),
                ..receipt()
            },
        ] {
            assert!(ValidReceipt::of(&bad).is_none(), "{bad:?}");
        }
        let odd_disposition = PushReceipt {
            disposition: "shout".into(),
            ..receipt()
        };
        assert_eq!(
            ValidReceipt::of(&odd_disposition).unwrap().disposition,
            "other"
        );
    }

    #[test]
    fn app_versions_outside_the_label_alphabet_are_other() {
        assert_eq!(receipt_app_version(" 1.4.0 "), "1.4.0");
        assert_eq!(
            receipt_app_version("2.0.0-rc.1+build_7"),
            "2.0.0-rc.1+build_7"
        );
        let too_long = "9".repeat(33);
        for bad in ["", "1.4 beta", "版本", too_long.as_str(), "1.4/0"] {
            assert_eq!(receipt_app_version(bad), "other", "{bad}");
        }
    }

    #[test]
    fn the_receipt_device_must_be_named() {
        assert_eq!(receipt_device_id(" d-1 ").unwrap(), "d-1");
        for bad in ["", "  ", "d\u{7}1"] {
            assert!(receipt_device_id(bad).is_err(), "{bad:?}");
        }
        assert!(receipt_device_id(&"d".repeat(129)).is_err());
    }
}

/// Recipients of one realtime packet; Capability sends at most 32.
const MAX_CAPABILITY_RECIPIENTS: usize = 32;
/// Encoded packet bound: a 6 KiB JSON payload plus its attributes.
const MAX_CAPABILITY_PACKET_BYTES: usize = 8 * 1024;

fn validate_capability_request(req: &PushCapabilityRequest) -> Result<(), Status> {
    use prost::Message as _;
    let packet = req
        .packet
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("capability packet required"))?;
    let unique = req
        .user_ids
        .iter()
        .collect::<std::collections::BTreeSet<_>>()
        .len()
        == req.user_ids.len();
    if req.user_ids.is_empty()
        || req.user_ids.len() > MAX_CAPABILITY_RECIPIENTS
        || !unique
        || req.user_ids.iter().any(|id| id.trim().is_empty())
        || packet.capability_id.trim().is_empty()
        || packet.packet_type.trim().is_empty()
        || packet.encoded_len() > MAX_CAPABILITY_PACKET_BYTES
    {
        return Err(Status::invalid_argument("invalid capability push"));
    }
    Ok(())
}

fn require_metadata_tenant(
    metadata: &HashMap<String, String>,
    tenant_id: &str,
) -> Result<(), Status> {
    for key in ["tenant_id", "x-tenant-id"] {
        if metadata.get(key).is_some_and(|value| value != tenant_id) {
            return Err(Status::permission_denied("push body tenant mismatch"));
        }
    }
    Ok(())
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

#[cfg(test)]
mod identity_boundary_tests {
    use super::*;

    #[test]
    fn capability_pushes_need_a_packet_and_one_to_thirty_two_unique_users() {
        let packet = flare_proto::common::CapabilityPacket {
            capability_id: "flare.call".into(),
            packet_type: "call.snapshot".into(),
            version: "1".into(),
            payload: b"{}".to_vec(),
            ..Default::default()
        };
        let request = |users: Vec<&str>, packet: Option<flare_proto::common::CapabilityPacket>| {
            PushCapabilityRequest {
                user_ids: users.into_iter().map(String::from).collect(),
                packet,
                expire_at_ms: 0,
            }
        };
        assert!(
            validate_capability_request(&request(vec!["u1", "u2"], Some(packet.clone()))).is_ok()
        );
        let many: Vec<String> = (0..33).map(|i| format!("u{i}")).collect();
        for bad in [
            request(vec![], Some(packet.clone())),
            request(vec!["u1", "u1"], Some(packet.clone())),
            request(vec!["u1"], None),
            PushCapabilityRequest {
                user_ids: many,
                packet: Some(packet.clone()),
                expire_at_ms: 0,
            },
            request(
                vec!["u1"],
                Some(flare_proto::common::CapabilityPacket {
                    payload: vec![b'x'; 9 * 1024],
                    ..packet.clone()
                }),
            ),
        ] {
            assert_eq!(
                validate_capability_request(&bad).unwrap_err().code(),
                tonic::Code::InvalidArgument
            );
        }
    }

    fn claims(actor_type: SignedActorType, tenant_id: &str) -> SignedContextClaims {
        SignedContextClaims {
            sub: "producer".into(),
            iss: "flare-cell".into(),
            aud: "flare-cell-internal".into(),
            token_use: "internal_context".into(),
            iat: 1,
            exp: 2,
            jti: "test".into(),
            actor_type,
            actor_id: "producer".into(),
            tenant_id: tenant_id.into(),
            roles: Vec::new(),
            credential_id: None,
            request_id: "req-1".into(),
            org_id: None,
            organization_scope: None,
            permission: None,
            reason: None,
            perm_version: None,
            trace_id: None,
            session_id: None,
            kid: flare_server_core::auth::LEGACY_KID.to_owned(),
        }
    }

    #[test]
    fn service_push_and_user_device_roles_are_separate() {
        let service = claims(SignedActorType::Service, "t1");
        let user = claims(SignedActorType::User, "t1");
        assert!(validate_actor_claims(&service, SignedActorType::Service).is_ok());
        assert!(validate_actor_claims(&user, SignedActorType::User).is_ok());
        assert_eq!(
            validate_actor_claims(&user, SignedActorType::Service)
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            validate_actor_claims(&service, SignedActorType::User)
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }

    #[test]
    fn canonical_tenant_and_body_metadata_must_match_signed_claims() {
        for tenant in ["", "default", " t1", "t1:other", "*"] {
            assert_eq!(
                validate_actor_claims(
                    &claims(SignedActorType::Service, tenant),
                    SignedActorType::Service
                )
                .unwrap_err()
                .code(),
                tonic::Code::PermissionDenied
            );
        }
        let forged = HashMap::from([("tenant_id".into(), "t2".into())]);
        assert_eq!(
            require_metadata_tenant(&forged, "t1").unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
        let matching = HashMap::from([("x-tenant-id".into(), "t1".into())]);
        assert!(require_metadata_tenant(&matching, "t1").is_ok());
    }

    #[test]
    fn bare_legacy_metadata_cannot_authenticate_push_proxy() {
        let mut request = Request::new(PushMessageRequest::default());
        request
            .metadata_mut()
            .insert("x-tenant-id", "t1".parse().unwrap());
        request
            .metadata_mut()
            .insert("x-actor-type", "2".parse().unwrap());
        assert_eq!(
            require_verified_actor(&request, SignedActorType::Service)
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
    }

    fn service_claims(actor_id: &str) -> SignedContextClaims {
        SignedContextClaims {
            actor_id: actor_id.into(),
            ..claims(SignedActorType::Service, "t1")
        }
    }

    /// 交接只收接入网关与 Online 的服务身份：别的服务、以及任何用户身份（哪怕 actor_id 撞名）
    /// 都不行。
    #[test]
    fn only_the_gateway_and_online_services_may_report_a_device_handoff() {
        for reporter in [ACCESS_GATEWAY, SIGNALING_ONLINE] {
            assert!(validate_handoff_reporter(&service_claims(reporter)).is_ok());
        }
        for other in [
            "flare-push-worker",
            "flare-push-server",
            "flare-api-gateway",
            "flare-orchestrator",
            "producer",
            "",
        ] {
            assert_eq!(
                validate_handoff_reporter(&service_claims(other))
                    .unwrap_err()
                    .code(),
                tonic::Code::PermissionDenied,
                "{other}"
            );
        }
        let user_named_like_the_gateway = SignedContextClaims {
            actor_id: ACCESS_GATEWAY.into(),
            ..claims(SignedActorType::User, "t1")
        };
        assert_eq!(
            validate_handoff_reporter(&user_named_like_the_gateway)
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }

    fn handoff() -> DeviceHandoff {
        DeviceHandoff {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            device_id: "d1".into(),
            connection_id: "c1".into(),
            gateway_id: "g1".into(),
            cause: DeviceHandoffCause::LinkLost as i32,
            since: 0,
            occurred_at: 1,
            pending: vec![flare_proto::common::DeviceHandoffItem {
                conversation_id: "conv".into(),
                seq: 3,
                message_id: "m3".into(),
            }],
        }
    }

    #[test]
    fn a_device_handoff_needs_its_targets_a_cause_and_a_bounded_pending_list() {
        assert!(validate_device_handoff(&handoff(), Some("t1")).is_ok());
        // 网关失联没有 pending，补提示范围由 since 决定。
        let gateway_lost = DeviceHandoff {
            cause: DeviceHandoffCause::GatewayLost as i32,
            since: 1_760_000_000_000,
            pending: Vec::new(),
            ..handoff()
        };
        assert!(validate_device_handoff(&gateway_lost, Some("t1")).is_ok());

        let full = DeviceHandoff {
            pending: vec![Default::default(); MAX_HANDOFF_PENDING],
            ..handoff()
        };
        assert!(validate_device_handoff(&full, Some("t1")).is_ok());

        for invalid in [
            DeviceHandoff {
                tenant_id: String::new(),
                ..handoff()
            },
            DeviceHandoff {
                user_id: "  ".into(),
                ..handoff()
            },
            DeviceHandoff {
                device_id: String::new(),
                ..handoff()
            },
            DeviceHandoff {
                cause: DeviceHandoffCause::Unspecified as i32,
                ..handoff()
            },
            DeviceHandoff {
                cause: 99,
                ..handoff()
            },
            DeviceHandoff {
                pending: vec![Default::default(); MAX_HANDOFF_PENDING + 1],
                ..handoff()
            },
        ] {
            assert_eq!(
                validate_device_handoff(&invalid, Some("t1"))
                    .unwrap_err()
                    .code(),
                tonic::Code::InvalidArgument
            );
        }
    }

    /// 上下文带租户时交接必须属于它；不带时只做交接体自身的校验。
    #[test]
    fn a_device_handoff_must_belong_to_the_signed_tenant() {
        assert_eq!(
            validate_device_handoff(&handoff(), Some("t2"))
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        assert!(validate_device_handoff(&handoff(), None).is_ok());
    }

    /// 吊销绑定只收 Online 的服务身份：接入网关能上报交接，但不能吊销绑定；别的服务、
    /// 以及任何用户身份（哪怕 actor_id 撞名）都不行。
    #[test]
    fn only_the_online_service_may_revoke_device_bindings() {
        assert!(validate_binding_revoker(&service_claims(SIGNALING_ONLINE)).is_ok());
        for other in [
            ACCESS_GATEWAY,
            "flare-push-worker",
            "flare-push-server",
            "flare-api-gateway",
            "producer",
            "",
        ] {
            assert_eq!(
                validate_binding_revoker(&service_claims(other))
                    .unwrap_err()
                    .code(),
                tonic::Code::PermissionDenied,
                "{other}"
            );
        }
        let user_named_like_online = SignedContextClaims {
            actor_id: SIGNALING_ONLINE.into(),
            ..claims(SignedActorType::User, "t1")
        };
        assert_eq!(
            validate_binding_revoker(&user_named_like_online)
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }

    fn revocation() -> RevokeDeviceBindingsRequest {
        RevokeDeviceBindingsRequest {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            device_id: "d1".into(),
            reason: "kicked".into(),
            registered_before: 1_760_000_000_000,
        }
    }

    /// 四种原因都收；设备 ID 为空表示这个用户的全部设备。
    #[test]
    fn a_binding_revocation_names_a_user_a_known_reason_and_a_time_bound() {
        for reason in BINDING_REVOCATION_REASONS {
            let request = RevokeDeviceBindingsRequest {
                reason: reason.into(),
                ..revocation()
            };
            assert!(
                validate_binding_revocation(&request, "t1").is_ok(),
                "{reason}"
            );
        }
        let every_device = RevokeDeviceBindingsRequest {
            device_id: String::new(),
            ..revocation()
        };
        assert!(validate_binding_revocation(&every_device, "t1").is_ok());

        for invalid in [
            RevokeDeviceBindingsRequest {
                user_id: String::new(),
                ..revocation()
            },
            RevokeDeviceBindingsRequest {
                user_id: "  ".into(),
                ..revocation()
            },
            RevokeDeviceBindingsRequest {
                device_id: "  ".into(),
                ..revocation()
            },
            RevokeDeviceBindingsRequest {
                registered_before: 0,
                ..revocation()
            },
            RevokeDeviceBindingsRequest {
                registered_before: -1,
                ..revocation()
            },
            RevokeDeviceBindingsRequest {
                reason: String::new(),
                ..revocation()
            },
            RevokeDeviceBindingsRequest {
                reason: "KICKED".into(),
                ..revocation()
            },
            RevokeDeviceBindingsRequest {
                reason: "logout".into(),
                ..revocation()
            },
        ] {
            assert_eq!(
                validate_binding_revocation(&invalid, "t1")
                    .unwrap_err()
                    .code(),
                tonic::Code::InvalidArgument,
                "{invalid:?}"
            );
        }
    }

    /// 请求体里的租户必须就是签名里的租户：别的租户、没写租户都拒绝，而且先于其余校验。
    #[test]
    fn a_binding_revocation_must_belong_to_the_signed_tenant() {
        assert_eq!(
            validate_binding_revocation(&revocation(), "t2")
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        let no_tenant = RevokeDeviceBindingsRequest {
            tenant_id: String::new(),
            ..revocation()
        };
        assert_eq!(
            validate_binding_revocation(&no_tenant, "t1")
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        let foreign_and_malformed = RevokeDeviceBindingsRequest {
            tenant_id: "t2".into(),
            reason: "logout".into(),
            ..revocation()
        };
        assert_eq!(
            validate_binding_revocation(&foreign_and_malformed, "t1")
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }

    #[test]
    fn the_log_names_every_device_as_all() {
        assert_eq!(revoked_device_label(""), "all");
        assert_eq!(revoked_device_label("d1"), "d1");
    }
}

#[cfg(test)]
mod delivery_gate_tests {
    #[test]
    fn refusal_reason_becomes_unavailable_and_absence_allows_enqueue() {
        fn gate(refusal: Option<&str>) -> Result<(), tonic::Status> {
            match refusal {
                None => Ok(()),
                Some(reason) => Err(tonic::Status::unavailable(reason)),
            }
        }
        let refused = gate(Some("Push Proxy is paused")).unwrap_err();
        assert_eq!(refused.code(), tonic::Code::Unavailable);
        assert!(gate(None).is_ok());
    }
}

fn validate_device_registration(
    provider: &str,
    device: &str,
    platform: &str,
    token: &str,
) -> Result<(), Status> {
    if device.len() > 128
        || platform.len() > 32
        || token.len() > 4096
        || [device, platform, token].iter().any(|s| {
            s.trim().is_empty()
                || s.bytes()
                    .any(|b| b.is_ascii_control() || b.is_ascii_whitespace())
        })
        || (provider == "apns"
            && (token.len() > 512 || !token.bytes().all(|b| b.is_ascii_hexdigit())))
    {
        return Err(Status::invalid_argument("device push registration invalid"));
    }
    Ok(())
}

/// 应用标识的上限：iOS bundle id 与 Android applicationId 都远短于它。
const MAX_APP_BUNDLE_LEN: usize = 255;
/// BCP 47 标签实际用到的长度（语言-文字-地区）远小于它。
const MAX_LOCALE_LEN: usize = 35;
/// IANA 时区名最长的也不到 40 字节。
const MAX_TIME_ZONE_LEN: usize = 64;
const MAX_APP_VERSION_LEN: usize = 64;

/// 校验过、规范化过的绑定属性。
#[derive(Debug, PartialEq, Eq)]
struct BindingAttributes {
    app_bundle: String,
    environment: String,
    locale: String,
    time_zone: String,
    app_version: String,
    /// 已排序：同一组能力换个顺序还是同一次登记，重放摘要不能因此变化。
    capabilities: Vec<String>,
}

/// 绑定属性的校验。三个通道都离不开应用标识：APNs 拿它当 topic，FCM 拿它区分同一项目下的
/// 多个应用，个推的厂商通知点开时要打开这个包里的页面。缺了它就投不出去，所以登记时就拒收。
/// 环境只对 APNs 有意义，别的通道带了说明客户端把字段填错了位置。
fn binding_attributes(
    provider: &str,
    req: &RegisterDevicePushTokenRequest,
) -> Result<BindingAttributes, Status> {
    let app_bundle = req.app_bundle.trim();
    if app_bundle.is_empty() {
        return Err(Status::invalid_argument("app_bundle is required"));
    }
    if app_bundle.len() > MAX_APP_BUNDLE_LEN
        || !app_bundle
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))
    {
        return Err(Status::invalid_argument("app_bundle invalid"));
    }
    let environment = req.environment.trim();
    match (provider, environment) {
        (_, "") | ("apns", APNS_ENVIRONMENT_SANDBOX | APNS_ENVIRONMENT_PRODUCTION) => {}
        _ => {
            return Err(Status::invalid_argument(
                "environment must be sandbox or production and is only for apns",
            ));
        }
    }
    let text = |value: &str, max_len: usize, name: &str| {
        let value = value.trim();
        if value.len() <= max_len && value.bytes().all(|b| b.is_ascii_graphic()) {
            Ok(value.to_string())
        } else {
            Err(Status::invalid_argument(format!("{name} invalid")))
        }
    };
    let locale = text(&req.locale, MAX_LOCALE_LEN, "locale")?;
    let time_zone = text(&req.time_zone, MAX_TIME_ZONE_LEN, "time_zone")?;
    let app_version = text(&req.app_version, MAX_APP_VERSION_LEN, "app_version")?;
    let mut capabilities = Vec::with_capacity(req.capabilities.len());
    for capability in &req.capabilities {
        let capability = capability.trim();
        if !PUSH_BINDING_CAPABILITIES.contains(&capability)
            || capabilities
                .iter()
                .any(|known: &String| known == capability)
        {
            return Err(Status::invalid_argument(
                "capabilities must be unique dataMessages or serviceExtension",
            ));
        }
        capabilities.push(capability.to_string());
    }
    capabilities.sort();
    Ok(BindingAttributes {
        app_bundle: app_bundle.to_string(),
        environment: environment.to_string(),
        locale,
        time_zone,
        app_version,
        capabilities,
    })
}

fn provider_string(provider: i32) -> Result<String, Status> {
    match DevicePushProvider::try_from(provider) {
        Ok(DevicePushProvider::Getui) => Ok("getui".to_string()),
        Ok(DevicePushProvider::Fcm) => Ok("fcm".to_string()),
        Ok(DevicePushProvider::Apns) => Ok("apns".to_string()),
        Ok(DevicePushProvider::Unspecified) | Err(_) => Err(Status::invalid_argument(
            "valid device push provider is required",
        )),
    }
}

#[cfg(test)]
mod provider_contract_tests {
    use super::*;
    #[test]
    fn all_vendor_registrations_are_typed_and_bounded() {
        for (provider, name) in [
            (DevicePushProvider::Getui, "getui"),
            (DevicePushProvider::Fcm, "fcm"),
            (DevicePushProvider::Apns, "apns"),
        ] {
            assert_eq!(provider_string(provider as i32).unwrap(), name);
        }
        assert!(provider_string(0).is_err());
        assert!(provider_string(99).is_err());
        assert!(validate_device_registration("apns", "ios", "ios", "a1b2").is_ok());
        assert!(validate_device_registration("apns", "ios", "ios", "a1/path").is_err());
        assert!(
            validate_device_registration("fcm", "android", "android", &"a".repeat(4097)).is_err()
        );
        assert!(validate_device_registration("fcm", "android", "android", "a\nb").is_err());
    }

    /// 只带必填的应用标识。
    fn registration() -> RegisterDevicePushTokenRequest {
        RegisterDevicePushTokenRequest {
            app_bundle: "com.example.app".into(),
            ..Default::default()
        }
    }

    #[test]
    fn binding_attributes_are_normalized() {
        let request = RegisterDevicePushTokenRequest {
            app_bundle: " com.example.app ".into(),
            environment: "sandbox".into(),
            locale: " zh-CN ".into(),
            time_zone: "Asia/Shanghai".into(),
            app_version: "3.2.1+45".into(),
            capabilities: vec!["serviceExtension".into(), " dataMessages ".into()],
            ..Default::default()
        };
        assert_eq!(
            binding_attributes("apns", &request).unwrap(),
            BindingAttributes {
                app_bundle: "com.example.app".into(),
                environment: "sandbox".into(),
                locale: "zh-CN".into(),
                time_zone: "Asia/Shanghai".into(),
                app_version: "3.2.1+45".into(),
                // 顺序无关：同一组能力只有一种写法。
                capabilities: vec!["dataMessages".into(), "serviceExtension".into()],
            }
        );
        // 应用标识之外的属性三个通道都可以不带。
        for provider in ["apns", "fcm", "getui"] {
            assert!(binding_attributes(provider, &registration()).is_ok());
        }
        let production = RegisterDevicePushTokenRequest {
            environment: "production".into(),
            ..registration()
        };
        assert!(binding_attributes("apns", &production).is_ok());
    }

    #[test]
    fn binding_attributes_reject_missing_bundles_misplaced_environments_and_unknown_capabilities() {
        let invalid = [
            // 三个通道都要应用标识：个推的厂商通知点开时打开的是这个包里的页面。
            ("apns", RegisterDevicePushTokenRequest::default()),
            ("fcm", RegisterDevicePushTokenRequest::default()),
            ("getui", RegisterDevicePushTokenRequest::default()),
            (
                "getui",
                RegisterDevicePushTokenRequest {
                    app_bundle: "   ".into(),
                    ..Default::default()
                },
            ),
            (
                "apns",
                RegisterDevicePushTokenRequest {
                    app_bundle: "com.example/app".into(),
                    ..Default::default()
                },
            ),
            (
                "apns",
                RegisterDevicePushTokenRequest {
                    app_bundle: "a".repeat(MAX_APP_BUNDLE_LEN + 1),
                    ..Default::default()
                },
            ),
            (
                "apns",
                RegisterDevicePushTokenRequest {
                    environment: "development".into(),
                    ..registration()
                },
            ),
            (
                "apns",
                RegisterDevicePushTokenRequest {
                    environment: "Sandbox".into(),
                    ..registration()
                },
            ),
            (
                "fcm",
                RegisterDevicePushTokenRequest {
                    environment: "sandbox".into(),
                    ..registration()
                },
            ),
            (
                "getui",
                RegisterDevicePushTokenRequest {
                    environment: "production".into(),
                    ..registration()
                },
            ),
            (
                "fcm",
                RegisterDevicePushTokenRequest {
                    locale: "zh CN".into(),
                    ..registration()
                },
            ),
            (
                "fcm",
                RegisterDevicePushTokenRequest {
                    locale: "a".repeat(MAX_LOCALE_LEN + 1),
                    ..registration()
                },
            ),
            (
                "fcm",
                RegisterDevicePushTokenRequest {
                    time_zone: "a".repeat(MAX_TIME_ZONE_LEN + 1),
                    ..registration()
                },
            ),
            (
                "fcm",
                RegisterDevicePushTokenRequest {
                    app_version: "1.0\n".repeat(20),
                    ..registration()
                },
            ),
            (
                "fcm",
                RegisterDevicePushTokenRequest {
                    capabilities: vec!["voip".into()],
                    ..registration()
                },
            ),
            (
                "fcm",
                RegisterDevicePushTokenRequest {
                    capabilities: vec!["dataMessages".into(), "dataMessages".into()],
                    ..registration()
                },
            ),
        ];
        for (provider, request) in invalid {
            assert_eq!(
                binding_attributes(provider, &request).unwrap_err().code(),
                tonic::Code::InvalidArgument,
                "{provider} {request:?}"
            );
        }
    }
}
