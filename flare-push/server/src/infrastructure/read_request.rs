//! 出站读请求的身份。

use flare_im_contracts::Ctx;

/// 调用方元数据，另把记录的签名身份（broker-isolated 下每条记录都以本服务身份重新签过）原样
/// 转发：会话服务与 Storage Reader 的读接口以签名为准，服务身份按租户读取。记录没有签名时只带
/// 元数据，由对端的 `FLARE_REQUIRE_SIGNED_READ_CTX` 决定收不收。
pub(crate) fn request_with_context<T>(payload: T, ctx: &Ctx) -> tonic::Request<T> {
    let mut request = flare_server_core::client::request_with_context(payload, ctx);
    if let Err(status) = flare_server_core::client::forward_signed_context(&mut request, ctx) {
        tracing::warn!(code = ?status.code(), "signed context could not be forwarded");
    }
    request
}
