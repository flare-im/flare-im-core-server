//! gRPC：`StorageReaderService` 协议适配。

mod storage_reader_handler;

pub use storage_reader_handler::StorageReaderGrpcHandler;

use flare_grpc_proto::storage::storage_message_lookup_service_server::StorageMessageLookupServiceServer;
use flare_grpc_proto::storage::storage_reader_service_server::StorageReaderServiceServer;
use flare_server_core::{SignedFirstContextLayer, SignedFirstContextService};
use tonic::server::NamedService;

use crate::domain::repository::MessageStorage;

/// 只认 admin-gateway 原样转发的 Staff `x-flare-ctx` 的管理检索（处理器里 `admin_ctx` 用钉死
/// `kid=gateway` 的验签器验，按 RPC 族复核权限码，租户与操作人只取自签名）。它们不经读接口
/// 身份边界：边界的验签器不钉 kid、也只放行 User/TenantAdmin/Service，Staff 的复核在处理器里。
pub const ADMIN_TOKEN_METHODS: [&str; 5] = [
    "SearchMessages",
    "GetMessage",
    "ExportMessages",
    "QueryMessageEvents",
    "QueryMessageWriteLedger",
];

/// 套上读接口身份边界的 `StorageReaderService`（见 `flare_im_service_kit::signed_read`）：
/// 消息、窗口、标签、编辑历史、已读、标记、反应、会话事件与会话头这些读，带签名的
/// `x-flare-ctx` 决定租户与身份，与签名不一致的裸 `x-tenant-id`、`x-user-id` 一律拒绝；只有
/// 边界不要求签名时，未签名调用才按元数据取身份。管理检索（[`ADMIN_TOKEN_METHODS`]）只认转发的
/// Staff 上下文。
pub fn signed_read_service<M>(
    handler: StorageReaderGrpcHandler<M>,
    layer: &SignedFirstContextLayer,
) -> SignedFirstContextService<StorageReaderServiceServer<StorageReaderGrpcHandler<M>>>
where
    M: MessageStorage + Send + Sync + Clone + 'static,
{
    let service = <StorageReaderServiceServer<StorageReaderGrpcHandler<M>> as NamedService>::NAME;
    ADMIN_TOKEN_METHODS
        .iter()
        .fold(layer.clone(), |layer, method| {
            layer.exempt_method(format!("/{service}/{method}"))
        })
        .layer(StorageReaderServiceServer::new(handler))
}

/// `StorageMessageLookupService`：按 ID 批量定位消息，只认 Orchestrator 自己签的 Service 上下文
/// （与 `GetMessage` 的消息操作读同一个身份，处理器里验），不经读接口身份边界，其他调用一律拒绝。
pub fn message_lookup_service<M>(
    handler: StorageReaderGrpcHandler<M>,
) -> StorageMessageLookupServiceServer<StorageReaderGrpcHandler<M>>
where
    M: MessageStorage + Send + Sync + Clone + 'static,
{
    StorageMessageLookupServiceServer::new(handler)
}
