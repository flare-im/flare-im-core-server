//! 基于 storage-reader 的 [`SeqFloorProvider`] 实现。
//!
//! 通过 `StorageReaderService.GetConversationMessageHead` 读取会话在**消息存储中的权威
//! `max_seq`**，作为 seq 分配的 floor 下限（自愈 Redis 高水位丢失导致的 seq 回退）。
//! 选用消息存储的权威 MAX(seq) 而非会话服务的异步投影，避免投影滞后导致 floor 偏低撞号。

use std::future::Future;
use std::pin::Pin;

use flare_grpc_proto::storage::GetConversationMessageHeadRequest;
use flare_grpc_proto::storage::storage_reader_service_client::StorageReaderServiceClient;
use flare_im_contracts::Ctx;
use flare_server_core::SignedContextSigner;
use flare_server_core::context::{ActorContext, ActorType, Context};
use flare_server_core::error::{FlareError, Result};
use flare_server_core::grpc::middleware::signed_context::VerifiedContext;
use tonic::transport::Channel;

use crate::domain::repository::SeqFloorProvider;

/// storage-reader 支撑的会话持久化 max_seq 提供者。
#[derive(Clone)]
pub struct StorageSeqFloorProvider {
    client: StorageReaderServiceClient<Channel>,
    /// 摄入自己的 Service 身份（Cell 私钥与 actor id）：floor 是本服务按租户与会话做的读，不是
    /// 哪个用户的读。
    service_signer: Option<(SignedContextSigner, &'static str)>,
}

impl std::fmt::Debug for StorageSeqFloorProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageSeqFloorProvider")
            .field(
                "service_signer",
                &self.service_signer.as_ref().map(|(_, actor)| *actor),
            )
            .finish_non_exhaustive()
    }
}

impl StorageSeqFloorProvider {
    /// 用已连接（可懒连接）的 storage-reader gRPC channel 构造。
    pub fn from_channel(channel: Channel) -> Self {
        Self {
            client: StorageReaderServiceClient::new(channel),
            service_signer: None,
        }
    }

    /// floor 查询以 `actor_id` 的 Service 身份为会话租户签名（Cell 私钥）。没有私钥时沿用入站签名
    /// （见 [`Self::floor_request`]）。
    pub fn with_service_signer(
        mut self,
        signer: Option<SignedContextSigner>,
        actor_id: &'static str,
    ) -> Self {
        self.service_signer = signer.map(|signer| (signer, actor_id));
        self
    }

    /// floor 查询的请求身份，按顺序：
    /// 1. 配了服务签名：以摄入自己的 Service 身份为 `tenant_id`（摄入派生的消息租户）签名，元数据也只
    ///    写这个服务身份。以前原样转发入站签名，入站签名的租户与派生的租户不同时 Storage Reader 判为
    ///    元数据与签名冲突（`PERMISSION_DENIED`），自愈失效（`message_ingest_seq_floor_tenant_conflict_total`
    ///    的来源）；入站没有签名（WAL 重放）时开关打开的 Reader 也会拒绝；
    /// 2. 没有私钥、入站验过签且租户相同：元数据 + 原样转发那份签名（与以前相同）；
    /// 3. 没有私钥、入站签名的租户不同或入站没有签名：只带派生租户的调用方元数据——转发别的租户的
    ///    签名必被拒绝；只带元数据时，关着签名开关的 Reader 照常应答，开着的照样拒绝（未迁移的部署）。
    fn floor_request(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        conversation_id: String,
    ) -> Result<tonic::Request<GetConversationMessageHeadRequest>> {
        let payload = GetConversationMessageHeadRequest { conversation_id };
        if let Some((signer, actor_id)) = self.service_signer.as_ref() {
            let token = flare_im_service_kit::signed_read::sign_service_read(
                signer,
                actor_id,
                tenant_id,
                ctx.request_id(),
            )?;
            let mut service_ctx = Context::with_request_id(ctx.request_id())
                .with_tenant_id(tenant_id)
                .with_actor(ActorContext::new(*actor_id).with_type(ActorType::Service));
            if !ctx.trace_id().is_empty() {
                service_ctx = service_ctx.with_trace_id(ctx.trace_id());
            }
            let mut request = flare_server_core::request_with_context(payload, &service_ctx);
            flare_server_core::set_signed_context_token(&mut request, &token)
                .map_err(|status| FlareError::system(status.message().to_string()))?;
            return Ok(request);
        }
        // reader 侧从 ctx metadata 还原租户：显式参数与 ctx 不一致时以参数为准派生子 ctx，
        // 避免双通道分歧导致 floor 查错租户静默失效。
        let ctx_for_rpc: Ctx = if ctx.tenant_id() == Some(tenant_id) {
            ctx.clone()
        } else {
            std::sync::Arc::new(ctx.with_tenant_id(tenant_id))
        };
        let mut request = flare_server_core::request_with_context(payload, &ctx_for_rpc);
        let signed_tenant = ctx
            .get_data::<VerifiedContext>()
            .and_then(VerifiedContext::claims)
            .map(|claims| claims.tenant_id.as_str());
        if signed_tenant.is_none_or(|signed| signed == tenant_id) {
            flare_server_core::forward_signed_context(&mut request, ctx)
                .map_err(|status| FlareError::system(status.message().to_string()))?;
        } else {
            // 别的租户的签名不转发：转发了必被拒绝。
            tracing::debug!(
                tenant_id,
                "seq floor read without a service signer: the inbound signature belongs to another tenant and is not forwarded"
            );
        }
        Ok(request)
    }
}

impl SeqFloorProvider for StorageSeqFloorProvider {
    fn persisted_max_seq<'a>(
        &'a self,
        ctx: &'a Ctx,
        tenant_id: &'a str,
        conversation_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<u64>> + Send + 'a>> {
        let mut client = self.client.clone();
        let request = self.floor_request(ctx, tenant_id, conversation_id.to_string());
        Box::pin(async move {
            let response = client.get_conversation_message_head(request?).await?;
            // max_seq 为 i64（无历史时为 0 或负值兜底），转成非负 u64 高水位。
            Ok(response.into_inner().max_seq.max(0) as u64)
        })
    }
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_server_core::grpc::middleware::signed_context::verify_signed_context_metadata;
    use flare_server_core::{
        SignedActorType, SignedContextIdentity, SignedContextVerifier, set_signed_context_token,
    };
    use std::sync::Arc;

    use super::*;

    fn keys() -> (SignedContextSigner, SignedContextVerifier) {
        let key = SigningKey::from_bytes(&[41; 32]);
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

    fn provider(signer: Option<SignedContextSigner>) -> StorageSeqFloorProvider {
        StorageSeqFloorProvider::from_channel(
            Channel::from_static("http://127.0.0.1:9").connect_lazy(),
        )
        .with_service_signer(signer, "flare-message-ingest")
    }

    /// 入站验过签的用户上下文（租户 `tenant`）。
    fn signed_user(tenant: &str, verifier: &SignedContextVerifier) -> (Ctx, String) {
        let (signer, _) = keys();
        let token = signer
            .sign(&SignedContextIdentity::new(
                SignedActorType::User,
                "alice",
                tenant,
                "req-floor",
            ))
            .unwrap();
        let mut inbound = tonic::Request::new(());
        set_signed_context_token(&mut inbound, &token).unwrap();
        let ctx = verify_signed_context_metadata(inbound.metadata(), verifier).unwrap();
        (ctx, token)
    }

    fn claims<T>(
        request: &tonic::Request<T>,
        verifier: &SignedContextVerifier,
    ) -> flare_server_core::SignedContextClaims {
        verify_signed_context_metadata(request.metadata(), verifier)
            .unwrap()
            .get_data::<VerifiedContext>()
            .and_then(VerifiedContext::claims)
            .cloned()
            .unwrap()
    }

    /// 摄入派生的消息租户与入站签名的租户不同时，floor 查询以摄入自己的 Service 身份为派生租户签名，
    /// 元数据与签名一致，Storage Reader 不再判为冲突（W14-R (e)）。修复前原样转发入站签名
    /// （租户 tenant-a）而元数据是 tenant-b，Reader 回 PERMISSION_DENIED，自愈失效。
    #[tokio::test]
    async fn the_floor_read_is_signed_as_the_ingest_service_for_the_derived_tenant() {
        let (signer, verifier) = keys();
        let (ctx, inbound_token) = signed_user("tenant-a", &verifier);
        let request = provider(Some(signer))
            .floor_request(&ctx, "tenant-b", "conv-1".into())
            .unwrap();

        let signed = claims(&request, &verifier);
        assert_eq!(signed.actor_type, SignedActorType::Service);
        assert_eq!(signed.actor_id, "flare-message-ingest");
        assert_eq!(signed.tenant_id, "tenant-b");
        assert_eq!(signed.request_id, "req-floor");
        let metadata = request.metadata();
        assert_ne!(
            metadata.get("x-flare-ctx").unwrap(),
            inbound_token.as_str(),
            "the other tenant's signature is not forwarded"
        );
        assert_eq!(metadata.get("x-tenant-id").unwrap(), "tenant-b");
        assert_eq!(metadata.get("x-actor-id").unwrap(), "flare-message-ingest");
        assert!(metadata.get("x-user-id").is_none());
        // 元数据与签名一致：签名优先的服务端入口（SignedFirstContextLayer）照收。
        let verified = verify_signed_context_metadata(metadata, &verifier).unwrap();
        assert_eq!(verified.tenant_id(), Some("tenant-b"));

        // 同租户与没有入站签名（WAL 重放）同样以服务身份签名。
        let (same, _) = signed_user("tenant-b", &verifier);
        let (signer, _) = keys();
        let replay: Ctx = Arc::new(Context::with_request_id("req-wal").with_tenant_id("tenant-b"));
        for ctx in [same, replay] {
            let request = provider(Some(signer.clone()))
                .floor_request(&ctx, "tenant-b", "conv-1".into())
                .unwrap();
            let signed = claims(&request, &verifier);
            assert_eq!(signed.actor_type, SignedActorType::Service);
            assert_eq!(signed.tenant_id, "tenant-b");
        }
    }

    /// 没有私钥（未迁移的部署）：同租户的入站签名照旧转发；别的租户的签名不转发，只带派生租户的
    /// 元数据。
    #[tokio::test]
    async fn without_a_signer_only_a_same_tenant_signature_is_forwarded() {
        let (_, verifier) = keys();
        let (ctx, token) = signed_user("tenant-a", &verifier);

        let same = provider(None)
            .floor_request(&ctx, "tenant-a", "conv-1".into())
            .unwrap();
        assert_eq!(same.metadata().get("x-flare-ctx").unwrap(), token.as_str());
        assert_eq!(same.metadata().get("x-tenant-id").unwrap(), "tenant-a");

        let other = provider(None)
            .floor_request(&ctx, "tenant-b", "conv-1".into())
            .unwrap();
        assert!(other.metadata().get("x-flare-ctx").is_none());
        assert_eq!(other.metadata().get("x-tenant-id").unwrap(), "tenant-b");
    }
}
