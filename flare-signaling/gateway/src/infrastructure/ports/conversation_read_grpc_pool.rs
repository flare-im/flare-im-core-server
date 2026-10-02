//! Conversation Read gRPC 客户端池（统一读扩散：登录时拉取用户会话列表用于 eager 订阅）。

use flare_grpc_proto::conversation::conversation_read_service_client::ConversationReadServiceClient;
use flare_im_contracts::service_names::{ACCESS_GATEWAY, CONVERSATION, get_service_name};
use flare_im_service_kit::discovery::connect_grpc_channel_resilient;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use tokio::sync::Mutex;
use tonic::transport::Channel;

const DEFAULT_CONVERSATION_URI: &str = "http://127.0.0.1:50090";

pub struct ConversationReadGrpcPool {
    service_name: String,
    client: Mutex<Option<ConversationReadServiceClient<Channel>>>,
    /// 代已认证连接读会话列表、以网关服务身份读成员时签 `x-flare-ctx`（Cell 私钥）。
    signer: Option<flare_server_core::SignedContextSigner>,
}

impl ConversationReadGrpcPool {
    pub fn new() -> Self {
        Self {
            service_name: get_service_name(CONVERSATION),
            client: Mutex::new(None),
            signer: None,
        }
    }

    pub fn with_signer(mut self, signer: Option<flare_server_core::SignedContextSigner>) -> Self {
        self.signer = signer;
        self
    }

    /// 代已认证连接读会话（见 [`crate::infrastructure::connection_context::signed_connection_read`]）。
    pub(crate) fn user_read_request<T>(
        &self,
        payload: T,
        tx: &flare_im_contracts::Ctx,
    ) -> Result<tonic::Request<T>> {
        crate::infrastructure::connection_context::signed_connection_read(
            payload,
            tx,
            self.signer.as_ref(),
        )
    }

    /// 以网关服务身份读某租户的会话成员：签 Service 身份，元数据也只写这个服务身份（服务不是
    /// 哪个用户）。没配私钥时沿用旧的未签名元数据（受信内部 Service actor）。
    fn fanout_read_request<T>(
        &self,
        payload: T,
        tenant_id: Option<&str>,
    ) -> Result<tonic::Request<T>> {
        use flare_server_core::context::{ActorContext, ActorType, Context};

        const READ_FANOUT_ACTOR: &str = "gateway-read-fanout";
        let Some(signer) = self.signer.as_ref() else {
            let mut base = Context::root();
            if let Some(tenant) = tenant_id {
                base = base.with_tenant_id(tenant);
            }
            let internal_ctx = base
                .with_user_id(READ_FANOUT_ACTOR)
                .with_actor(ActorContext::new(READ_FANOUT_ACTOR).with_type(ActorType::Service));
            return Ok(flare_server_core::client::request_with_context(
                payload,
                &internal_ctx,
            ));
        };
        let tenant_id = tenant_id
            .filter(|tenant| !tenant.is_empty())
            .ok_or_else(|| {
                ErrorBuilder::new(ErrorCode::PermissionDenied, "read fan-out needs a tenant")
                    .build_error()
            })?;
        let actor_id = get_service_name(ACCESS_GATEWAY);
        let ctx = Context::root()
            .with_tenant_id(tenant_id)
            .with_actor(ActorContext::new(&actor_id).with_type(ActorType::Service));
        let token = flare_im_service_kit::signed_read::sign_service_read(
            signer,
            &actor_id,
            tenant_id,
            ctx.request_id(),
        )?;
        let mut request = flare_server_core::client::request_with_context(payload, &ctx);
        flare_server_core::set_signed_context_token(&mut request, &token)?;
        Ok(request)
    }

    pub async fn ensure_client(&self) -> Result<ConversationReadServiceClient<Channel>> {
        let mut guard = self.client.lock().await;
        if let Some(c) = guard.as_ref() {
            return Ok(c.clone());
        }
        let channel = connect_grpc_channel_resilient(&self.service_name, DEFAULT_CONVERSATION_URI)
            .await
            .map_err(|e| {
                ErrorBuilder::new(
                    ErrorCode::ServiceUnavailable,
                    "conversation read unavailable",
                )
                .details(e)
                .build_error()
            })?;
        let client = ConversationReadServiceClient::new(channel);
        *guard = Some(client.clone());
        Ok(client)
    }

    /// 拉取会话全部参与者 user_id（分页聚合）。用于网关首次投递时解析成员订阅（每会话每网关一次，缓存）。
    ///
    /// 读扩散广播投递无具体用户身份（DeliverToConversation 的 ctx 无 user_id），而会话服务的
    /// 成员列表默认要求"调用者须为成员"。这里以**网关的 Service 身份**签名调用，会话服务对
    /// Service/System actor 跳过成员鉴权（仅按 tenant + conversation 范围读取），保留 tenant 作用域。
    pub async fn list_participants(
        &self,
        tx: &flare_im_contracts::Ctx,
        conversation_id: &str,
    ) -> Result<Vec<String>> {
        use flare_grpc_proto::conversation::ListConversationParticipantsRequest;

        let mut client = self.ensure_client().await?;
        let mut user_ids = Vec::new();
        let mut cursor = String::new();
        loop {
            let req = self.fanout_read_request(
                ListConversationParticipantsRequest {
                    conversation_id: conversation_id.to_string(),
                    cursor: cursor.clone(),
                    limit: 1000,
                    include_removed: false,
                    ext: Default::default(),
                },
                tx.tenant_id(),
            )?;
            let resp = client
                .list_conversation_participants(req)
                .await
                .map_err(|e| {
                    ErrorBuilder::new(ErrorCode::ServiceUnavailable, "list participants failed")
                        .details(e.to_string())
                        .build_error()
                })?;
            let resp = resp.into_inner();
            for p in resp.participants {
                if !p.user_id.is_empty() {
                    user_ids.push(p.user_id);
                }
            }
            if !resp.has_more || resp.next_cursor.is_empty() {
                break;
            }
            cursor = resp.next_cursor;
        }
        Ok(user_ids)
    }
}

#[async_trait::async_trait]
impl crate::domain::ports::ConversationParticipantSource for ConversationReadGrpcPool {
    async fn list_participants(
        &self,
        tx: &flare_im_contracts::Ctx,
        conversation_id: &str,
    ) -> Result<Vec<String>> {
        ConversationReadGrpcPool::list_participants(self, tx, conversation_id).await
    }
}

impl Default for ConversationReadGrpcPool {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_server_core::grpc::middleware::signed_context::{
        VerifiedContext, verify_signed_context_metadata,
    };
    use flare_server_core::{SignedActorType, SignedContextSigner, SignedContextVerifier};

    use super::*;

    fn keys() -> (SignedContextSigner, SignedContextVerifier) {
        let key = SigningKey::from_bytes(&[34; 32]);
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

    /// 读扩散成员读以网关的服务身份签名：服务不是用户，元数据里没有 `x-user-id`。
    #[test]
    fn fanout_member_reads_are_signed_as_the_gateway_service() {
        let (signer, verifier) = keys();
        let pool = ConversationReadGrpcPool::new().with_signer(Some(signer));
        let request = pool.fanout_read_request((), Some("tenant-a")).unwrap();
        let ctx = verify_signed_context_metadata(request.metadata(), &verifier).unwrap();
        let claims = ctx
            .get_data::<VerifiedContext>()
            .and_then(VerifiedContext::claims)
            .unwrap();
        assert_eq!(claims.actor_type, SignedActorType::Service);
        assert_eq!(claims.actor_id, get_service_name(ACCESS_GATEWAY));
        assert_eq!(claims.tenant_id, "tenant-a");
        let metadata = request.metadata();
        assert!(metadata.get("x-user-id").is_none());
        assert_eq!(metadata.get("x-tenant-id").unwrap(), "tenant-a");
        assert_eq!(metadata.get("x-actor-type").unwrap(), "2");
        assert_eq!(
            metadata.get("x-actor-id").unwrap().to_str().unwrap(),
            claims.actor_id
        );
        assert!(pool.fanout_read_request((), None).is_err());
    }
}
