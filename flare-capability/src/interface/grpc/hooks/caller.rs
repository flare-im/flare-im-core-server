//! `HookPlugin` 调用方身份：只认 Cell 签名的上下文（`x-flare-ctx`，用
//! `FLARE_INTERNAL_CTX_PUBLIC_KEY` 验签）。
//!
//! 实际调用方是 `flare_im_hooks` 的 gRPC Hook 适配器（Message Ingest 等），它原样转发自己收到的
//! 签名令牌，并保证请求体里的租户与令牌一致、PreSend 由签名用户发起。这里对称地校验：没有或
//! 验不过签名的调用一律 `UNAUTHENTICATED`；请求体里的租户、操作者与签名不一致时
//! `PERMISSION_DENIED`。执行 Hook 用的上下文的身份（租户、用户、请求 ID、actor）只取签名，
//! 选哪些 Hook 也按签名租户。

use flare_grpc_proto::capability::HookInvocationContext;
use flare_server_core::context::{Context, Ctx};
use flare_server_core::grpc::middleware::signed_context::{
    VerifiedContext, verify_signed_context_metadata,
};
use flare_server_core::{SignedActorType, SignedContextClaims, SignedContextVerifier};
use tonic::Status;
use tonic::metadata::MetadataMap;

use crate::infrastructure::adapters::conversion::proto_to_hook_context_data;
use crate::infrastructure::adapters::hook_context_data::set_hook_context_data;

/// PreSend 的 operation（只收签名用户发起的调用）。
pub(crate) const PRE_SEND_OPERATION: &str = "flare.hook.v1.pre_send";

/// 已验签的 `HookPlugin` 调用方。只能由 [`HookCaller::verify`] 构造。
pub(crate) struct HookCaller {
    ctx: Ctx,
    claims: SignedContextClaims,
}

impl HookCaller {
    /// 验证 `x-flare-ctx`；没有配置验签公钥时一律拒绝（fail closed）。
    pub(crate) fn verify(
        metadata: &MetadataMap,
        verifier: Option<&SignedContextVerifier>,
    ) -> Result<Self, Status> {
        let verifier = verifier.ok_or_else(|| {
            Status::unauthenticated("signed hook context verifier not configured")
        })?;
        let ctx = verify_signed_context_metadata(metadata, verifier)?;
        let claims = ctx
            .get_data::<VerifiedContext>()
            .and_then(VerifiedContext::claims)
            .cloned()
            .ok_or_else(|| Status::unauthenticated("signed hook context required"))?;
        Ok(Self { ctx, claims })
    }

    /// 签名里的租户：按它选 Hook 配置。
    pub(crate) fn tenant_id(&self) -> &str {
        &self.claims.tenant_id
    }

    /// 校验请求体的调用上下文并构造执行 Hook 用的上下文。
    ///
    /// - 请求体的 `tenant_id` 为空或等于签名租户；
    /// - PreSend 只收签名用户（[`SignedActorType::User`]）；
    /// - 用户身份（用户、租户管理员）的调用方，请求体的 `operator_user_id` 为空或等于签名用户；
    ///   服务身份代用户调用时不限。
    pub(crate) fn hook_context(
        &self,
        proto: &HookInvocationContext,
        operation: &str,
    ) -> Result<Context, Status> {
        if !proto.tenant_id.is_empty() && proto.tenant_id != self.claims.tenant_id {
            return Err(Status::permission_denied(
                "hook context tenant does not match signed context",
            ));
        }
        if operation == PRE_SEND_OPERATION && self.claims.actor_type != SignedActorType::User {
            return Err(Status::permission_denied(
                "pre-send hook requires signed user",
            ));
        }
        let acts_as_user = matches!(
            self.claims.actor_type,
            SignedActorType::User | SignedActorType::TenantAdmin
        );
        if acts_as_user
            && !proto.operator_user_id.is_empty()
            && proto.operator_user_id != self.claims.actor_id
        {
            return Err(Status::permission_denied(
                "hook operator does not match signed context",
            ));
        }

        let mut ctx = (*self.ctx).clone();
        if !proto.conversation_id.is_empty() {
            ctx = ctx.with_session_id(&proto.conversation_id);
        }
        Ok(set_hook_context_data(
            ctx,
            proto_to_hook_context_data(proto),
        ))
    }

    /// 请求体的调用上下文，身份字段换成签名里的（租户、请求 ID；用户身份时还有操作者），供直接
    /// 读 `HookInvocationContext` 的下游（PreSend Guard）使用。先经 [`Self::hook_context`] 校验。
    pub(crate) fn signed_invocation_context(
        &self,
        proto: &HookInvocationContext,
    ) -> HookInvocationContext {
        let mut bound = proto.clone();
        bound.tenant_id = self.claims.tenant_id.clone();
        bound.request_id = self.claims.request_id.clone();
        if matches!(
            self.claims.actor_type,
            SignedActorType::User | SignedActorType::TenantAdmin
        ) {
            bound.operator_user_id = self.claims.actor_id.clone();
        }
        bound
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_server_core::{SignedContextIdentity, SignedContextSigner};

    fn keys(seed: u8) -> (SignedContextSigner, SignedContextVerifier) {
        let key = SigningKey::from_bytes(&[seed; 32]);
        let private = key.to_pkcs8_pem(Default::default()).unwrap();
        let public = key
            .verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap();
        (
            SignedContextSigner::from_private_key(&private).unwrap(),
            SignedContextVerifier::from_public_key(&public).unwrap(),
        )
    }

    fn metadata(signer: &SignedContextSigner, actor: SignedActorType, id: &str) -> MetadataMap {
        let token = signer
            .sign(&SignedContextIdentity::new(actor, id, "tenant-a", "req-1"))
            .unwrap();
        let mut metadata = MetadataMap::new();
        metadata.insert("x-flare-ctx", token.parse().unwrap());
        metadata
    }

    fn body(tenant: &str, operator: &str) -> HookInvocationContext {
        HookInvocationContext {
            conversation_id: "conv-1".into(),
            conversation_type: "single".into(),
            tenant_id: tenant.into(),
            operator_user_id: operator.into(),
            request_id: "body-request".into(),
            ..Default::default()
        }
    }

    #[test]
    fn unsigned_or_wrongly_signed_callers_are_unauthenticated() {
        let (signer, verifier) = keys(71);
        let (_, other) = keys(72);
        let signed = metadata(&signer, SignedActorType::User, "u1");

        let missing = HookCaller::verify(&MetadataMap::new(), Some(&verifier))
            .err()
            .unwrap();
        assert_eq!(missing.code(), tonic::Code::Unauthenticated);
        let wrong_key = HookCaller::verify(&signed, Some(&other)).err().unwrap();
        assert_eq!(wrong_key.code(), tonic::Code::Unauthenticated);
        let no_verifier = HookCaller::verify(&signed, None).err().unwrap();
        assert_eq!(no_verifier.code(), tonic::Code::Unauthenticated);

        let caller = HookCaller::verify(&signed, Some(&verifier)).unwrap();
        assert_eq!(caller.tenant_id(), "tenant-a");
    }

    #[test]
    fn identity_comes_from_the_signature_and_the_body_must_agree() {
        let (signer, verifier) = keys(73);
        let caller = HookCaller::verify(
            &metadata(&signer, SignedActorType::User, "u1"),
            Some(&verifier),
        )
        .unwrap();

        let ctx = caller
            .hook_context(&body("", ""), PRE_SEND_OPERATION)
            .unwrap();
        assert_eq!(ctx.tenant_id(), Some("tenant-a"));
        assert_eq!(ctx.user_id(), Some("u1"));
        assert_eq!(ctx.request_id(), "req-1");
        assert_eq!(ctx.session_id(), Some("conv-1"));
        assert!(
            caller
                .hook_context(&body("tenant-a", "u1"), PRE_SEND_OPERATION)
                .is_ok()
        );
        let bound = caller.signed_invocation_context(&body("", ""));
        assert_eq!(bound.tenant_id, "tenant-a");
        assert_eq!(bound.operator_user_id, "u1");
        assert_eq!(bound.request_id, "req-1");
        assert_eq!(bound.conversation_id, "conv-1");

        for (tenant, operator) in [("tenant-b", "u1"), ("tenant-a", "u2")] {
            let status = caller
                .hook_context(&body(tenant, operator), PRE_SEND_OPERATION)
                .unwrap_err();
            assert_eq!(
                status.code(),
                tonic::Code::PermissionDenied,
                "{tenant}/{operator}"
            );
        }
    }

    #[test]
    fn pre_send_needs_a_signed_user_and_services_may_act_for_users_elsewhere() {
        let (signer, verifier) = keys(74);
        let service = HookCaller::verify(
            &metadata(&signer, SignedActorType::Service, "flare-orchestrator"),
            Some(&verifier),
        )
        .unwrap();
        let status = service
            .hook_context(&body("tenant-a", "u1"), PRE_SEND_OPERATION)
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::PermissionDenied);

        let ctx = service
            .hook_context(&body("tenant-a", "u1"), "flare.hook.v1.post_send")
            .unwrap();
        assert_eq!(ctx.tenant_id(), Some("tenant-a"));
        assert_eq!(ctx.user_id(), None);
        let status = service
            .hook_context(&body("tenant-b", "u1"), "flare.hook.v1.post_send")
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }
}
