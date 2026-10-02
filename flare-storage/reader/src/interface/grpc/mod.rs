//! gRPC：`StorageReaderService` 协议适配。

mod storage_reader_handler;

pub use storage_reader_handler::StorageReaderGrpcHandler;

use flare_grpc_proto::storage::storage_reader_service_server::StorageReaderServiceServer;
use flare_server_core::{SignedFirstContextLayer, SignedFirstContextService};
use tonic::server::NamedService;

use crate::domain::repository::MessageStorage;

/// 只认 admin-gateway 原样转发的 admin 令牌的管理检索（处理器里 `admin_ctx` 验令牌，租户与
/// 操作人只取自令牌）。它们不经读接口身份边界，也不要求 `x-flare-ctx`。
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
/// 边界不要求签名时，未签名调用才按元数据取身份。管理检索（[`ADMIN_TOKEN_METHODS`]）照旧只认
/// admin 令牌。
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
