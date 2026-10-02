//! 连接上下文提取模块
//!
//! 从连接信息中提取上下文（租户ID、用户ID等）并设置到 Context，与 flare_server_core::context 对齐。

use flare_im_contracts::utils::normalize_tenant_id;
use flare_server_core::context::{ActorContext, ActorType, Context};
use std::collections::HashMap;
use uuid::Uuid;

/// Provenance recorded only after the connection manager reports a completed
/// token-authenticated connection. It is never decoded from client metadata.
pub(crate) struct AuthenticatedConnectionOrigin {
    connection_id: String,
    user_id: String,
    tenant_id: String,
}

impl AuthenticatedConnectionOrigin {
    pub(crate) fn new(connection_id: String, user_id: String, tenant_id: String) -> Self {
        Self {
            connection_id,
            user_id,
            tenant_id,
        }
    }

    pub(crate) fn connection_id(&self) -> &str {
        &self.connection_id
    }

    pub(crate) fn user_id(&self) -> &str {
        &self.user_id
    }

    pub(crate) fn tenant_id(&self) -> &str {
        &self.tenant_id
    }
}

/// 连接上下文键（用于存储在 ConnectionInfo.metadata 中）
pub const METADATA_KEY_TENANT_ID: &str = crate::constants::METADATA_KEY_TENANT_ID;
pub const METADATA_KEY_USER_ID: &str = "user_id";
pub const METADATA_KEY_DEVICE_ID: &str = "device_id";

/// 从连接信息的 metadata 中提取租户ID
pub fn extract_tenant_id_from_metadata(metadata: &HashMap<String, String>) -> Option<String> {
    metadata
        .get(METADATA_KEY_TENANT_ID)
        .map(normalize_tenant_id)
}

/// 从连接信息的 metadata 中提取用户ID
pub fn extract_user_id_from_metadata(metadata: &HashMap<String, String>) -> Option<String> {
    metadata.get(METADATA_KEY_USER_ID).cloned()
}

/// 从连接信息的 metadata 中提取设备ID
pub fn extract_device_id_from_metadata(metadata: &HashMap<String, String>) -> Option<String> {
    metadata.get(METADATA_KEY_DEVICE_ID).cloned()
}

/// 从连接 metadata / 用户 ID 构建上行链路 [`Context`]（request_id、trace_id 自动生成）
pub fn build_context_from_connection(
    connection_metadata: Option<&HashMap<String, String>>,
    user_id: Option<&str>,
    default_tenant_id: &str,
) -> Context {
    let request_id = Uuid::new_v4().to_string();
    let trace_id = Uuid::new_v4().to_string();
    let mut ctx = Context::with_request_id(request_id).with_trace_id(trace_id);
    let tenant_id = connection_metadata
        .and_then(|m| m.get(METADATA_KEY_TENANT_ID).cloned())
        .map(normalize_tenant_id)
        .unwrap_or_else(|| normalize_tenant_id(default_tenant_id));
    ctx = ctx.with_tenant_id(tenant_id);
    let uid = user_id
        .map(str::to_string)
        .or_else(|| connection_metadata.and_then(|m| m.get(METADATA_KEY_USER_ID).cloned()));
    let uid = uid.unwrap_or_default();
    ctx = ctx.with_user_id(uid.clone());
    if !uid.is_empty() {
        ctx = ctx.with_actor(ActorContext::new(uid).with_type(ActorType::User));
    }
    ctx = ctx.with_device_id(
        connection_metadata
            .and_then(|m| m.get(METADATA_KEY_DEVICE_ID).cloned())
            .unwrap_or_default(),
    );
    ctx
}

/// 网关代已认证连接读会话、同步时的请求身份（与发送、在线的写路径一致）。
///
/// 只认连接认证时记下的来源（[`AuthenticatedConnectionOrigin`]），上下文里的用户或租户与它
/// 对不上就拒绝。元数据只写这个用户与租户（另带请求号、追踪号与设备），另附网关为它签的
/// `x-flare-ctx`：会话、Storage Reader 与同步的读接口以签名为准，元数据与签名同一身份。网关
/// 没配签名私钥时只带元数据，由下游的 `FLARE_REQUIRE_SIGNED_READ_CTX` 决定收不收。
pub(crate) fn signed_connection_read<T>(
    payload: T,
    tx: &Context,
    signer: Option<&flare_server_core::SignedContextSigner>,
) -> flare_server_core::error::Result<tonic::Request<T>> {
    use flare_server_core::error::{ErrorBuilder, ErrorCode};

    let denied =
        |reason: &str| ErrorBuilder::new(ErrorCode::PermissionDenied, reason).build_error();
    let origin = tx
        .get_data::<AuthenticatedConnectionOrigin>()
        .ok_or_else(|| denied("authenticated connection required"))?;
    if tx.user_id() != Some(origin.user_id()) || tx.tenant_id() != Some(origin.tenant_id()) {
        return Err(denied("connection identity mismatch"));
    }
    let mut ctx = Context::with_request_id(tx.request_id())
        .with_tenant_id(origin.tenant_id())
        .with_user_id(origin.user_id())
        .with_actor(ActorContext::new(origin.user_id()).with_type(ActorType::User));
    if !tx.trace_id().is_empty() {
        ctx = ctx.with_trace_id(tx.trace_id());
    }
    if let Some(device_id) = tx.device_id().filter(|device| !device.is_empty()) {
        ctx = ctx.with_device_id(device_id);
    }
    let mut request = flare_server_core::client::request_with_context(payload, &ctx);
    let Some(signer) = signer else {
        return Ok(request);
    };
    let token = signer
        .sign(
            &flare_server_core::SignedContextIdentity::new(
                flare_server_core::SignedActorType::User,
                origin.user_id(),
                origin.tenant_id(),
                tx.request_id(),
            )
            .with_credential_id("signaling-gateway"),
        )
        .map_err(|_| {
            ErrorBuilder::new(
                ErrorCode::ServiceUnavailable,
                "unable to sign read identity",
            )
            .build_error()
        })?;
    flare_server_core::set_signed_context_token(&mut request, &token).map_err(|_| {
        ErrorBuilder::new(
            ErrorCode::ServiceUnavailable,
            "unable to encode read identity",
        )
        .build_error()
    })?;
    Ok(request)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_server_core::error::ErrorCode;
    use flare_server_core::grpc::middleware::signed_context::{
        VerifiedContext, verify_signed_context_metadata,
    };
    use flare_server_core::{SignedActorType, SignedContextSigner, SignedContextVerifier};

    use super::*;

    fn keys() -> (SignedContextSigner, SignedContextVerifier) {
        let key = SigningKey::from_bytes(&[33; 32]);
        (
            SignedContextSigner::from_private_key(&key.to_pkcs8_pem(Default::default()).unwrap())
                .unwrap(),
            SignedContextVerifier::from_public_key(
                &key.verifying_key()
                    .to_public_key_pem(Default::default())
                    .unwrap(),
            )
            .unwrap(),
        )
    }

    fn connection_ctx(user: &str, tenant: &str) -> Context {
        Context::with_request_id("req-1")
            .with_tenant_id(tenant)
            .with_user_id(user)
            .with_device_id("phone")
            .with_actor(ActorContext::new(user).with_type(ActorType::User))
            .insert_data(AuthenticatedConnectionOrigin::new(
                "conn-1".into(),
                "alice".into(),
                "tenant-a".into(),
            ))
    }

    fn metadata(request: &tonic::Request<()>, key: &str) -> Option<String> {
        request
            .metadata()
            .get(key)
            .map(|value| value.to_str().unwrap().to_owned())
    }

    /// 签的是连接认证时记下的用户与租户；元数据只写同一身份，下游读接口不会判为冲突。
    #[test]
    fn reads_are_signed_for_the_authenticated_connection() {
        let (signer, verifier) = keys();
        let request =
            signed_connection_read((), &connection_ctx("alice", "tenant-a"), Some(&signer))
                .unwrap();
        let ctx = verify_signed_context_metadata(request.metadata(), &verifier).unwrap();
        let claims = ctx
            .get_data::<VerifiedContext>()
            .and_then(VerifiedContext::claims)
            .unwrap();
        assert_eq!(claims.actor_type, SignedActorType::User);
        assert_eq!(claims.actor_id, "alice");
        assert_eq!(claims.tenant_id, "tenant-a");
        assert_eq!(claims.credential_id.as_deref(), Some("signaling-gateway"));
        assert_eq!(metadata(&request, "x-user-id").as_deref(), Some("alice"));
        assert_eq!(
            metadata(&request, "x-tenant-id").as_deref(),
            Some("tenant-a")
        );
        assert_eq!(metadata(&request, "x-actor-id").as_deref(), Some("alice"));
        assert_eq!(metadata(&request, "x-actor-type").as_deref(), Some("1"));

        // 没配私钥：只带元数据，由下游的开关决定收不收。
        let unsigned =
            signed_connection_read((), &connection_ctx("alice", "tenant-a"), None).unwrap();
        assert!(metadata(&unsigned, "x-flare-ctx").is_none());
        assert_eq!(metadata(&unsigned, "x-user-id").as_deref(), Some("alice"));
    }

    /// 没有认证来源、或上下文与来源对不上，不签也不发。
    #[test]
    fn a_context_without_matching_connection_origin_is_refused() {
        let (signer, _) = keys();
        let no_origin = Context::with_request_id("req-1")
            .with_tenant_id("tenant-a")
            .with_user_id("alice");
        for ctx in [
            no_origin,
            connection_ctx("bob", "tenant-a"),
            connection_ctx("alice", "tenant-b"),
        ] {
            let error = signed_connection_read((), &Arc::new(ctx), Some(&signer)).unwrap_err();
            assert_eq!(error.code(), Some(ErrorCode::PermissionDenied));
        }
    }
}
