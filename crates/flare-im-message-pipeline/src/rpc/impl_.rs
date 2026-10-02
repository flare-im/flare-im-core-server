//! Tonic 框架的 RPC 客户端实现
//!
//! 本模块提供基于 tonic 的 RPC 客户端具体实现。
//! 未来切换到其他框架时，仅需修改本文件，trait 定义保持不变。

use super::ConversationRpcClient;
use crate::model::ConversationType;
use flare_grpc_proto::conversation::conversation_manage_service_client::ConversationManageServiceClient;
use flare_grpc_proto::conversation::conversation_read_service_client::ConversationReadServiceClient;
use flare_grpc_proto::conversation::{
    GetConversationDetailRequest, ListConversationParticipantsRequest, ManageParticipantsRequest,
    MarkConversationAsReadRequest,
};
use flare_server_core::SignedContextSigner;
use flare_server_core::client::{forward_signed_context, set_context_metadata};
use flare_server_core::context::{ActorContext, ActorType, Context, ContextExt};
use flare_server_core::error::{FlareError, Result};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tonic::transport::Channel;
use tracing::{debug, info, instrument, warn};

/// 会话服务客户端（基于 tonic）
///
/// 使用 tonic 框架实现的会话服务客户端，
/// 通过 gRPC 调用下游的 Conversation 服务。
pub struct ConversationClient {
    manage_client: Arc<tokio::sync::Mutex<ConversationManageServiceClient<Channel>>>,
    read_client: Arc<tokio::sync::Mutex<ConversationReadServiceClient<Channel>>>,
    /// 读成员时没有入站签名可转发（orchestrator 处理 MQ 与未签名的动作）就以本服务身份签名。
    read_signer: Option<(SignedContextSigner, &'static str)>,
}

impl std::fmt::Debug for ConversationClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConversationClient")
            .field(
                "read_signer",
                &self.read_signer.as_ref().map(|(_, actor)| *actor),
            )
            .finish_non_exhaustive()
    }
}

impl ConversationClient {
    /// 创建新的会话服务客户端
    ///
    /// # 参数
    /// - `channel`: tonic Channel，已连接到 Conversation 服务
    pub fn new(channel: Channel) -> Self {
        Self {
            manage_client: Arc::new(tokio::sync::Mutex::new(
                ConversationManageServiceClient::new(channel.clone()),
            )),
            read_client: Arc::new(tokio::sync::Mutex::new(ConversationReadServiceClient::new(
                channel,
            ))),
            read_signer: None,
        }
    }

    /// 读成员时，入站上下文不是验过签的就以 `actor_id` 的服务身份签名（Cell 私钥）。没有私钥时
    /// 只带调用方元数据，会话服务打开 `FLARE_REQUIRE_SIGNED_READ_CTX` 后会拒绝。
    pub fn with_service_read_signer(
        mut self,
        signer: Option<SignedContextSigner>,
        actor_id: &'static str,
    ) -> Self {
        self.read_signer = signer.map(|signer| (signer, actor_id));
        self
    }

    /// 读会话成员的请求身份，按顺序：
    /// 1. 入站验过签（ingest 收到的发送者或 Social 服务）：元数据 + 原样转发那份签名；
    /// 2. 配了服务签名：以本服务的 Service 身份签该租户，元数据也只写这个服务身份——服务不是
    ///    哪个用户，元数据里不能再带发送者，否则会话服务按冲突拒绝；
    /// 3. 都没有：只带调用方元数据（未迁移的部署）。
    fn member_read_request<T>(&self, payload: T, ctx: &Context) -> Result<tonic::Request<T>> {
        let mut request = tonic::Request::new(payload);
        if ctx
            .get_data::<flare_server_core::VerifiedContext>()
            .and_then(flare_server_core::VerifiedContext::token)
            .is_some()
        {
            set_context_metadata(&mut request, ctx);
            forward_signed_context(&mut request, ctx)
                .map_err(|status| FlareError::system(status.message().to_string()))?;
            return Ok(request);
        }
        let Some((signer, actor_id)) = self.read_signer.as_ref() else {
            set_context_metadata(&mut request, ctx);
            return Ok(request);
        };
        let Some(tenant_id) = ctx.tenant_id().filter(|tenant| !tenant.trim().is_empty()) else {
            warn!("conversation member read without a tenant cannot be signed");
            set_context_metadata(&mut request, ctx);
            return Ok(request);
        };
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
        set_context_metadata(&mut request, &service_ctx);
        flare_server_core::set_signed_context_token(&mut request, &token)
            .map_err(|status| FlareError::system(status.message().to_string()))?;
        Ok(request)
    }
}

impl ConversationRpcClient for ConversationClient {
    #[instrument(skip(self, ctx, participants), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        conversation_id = %conversation_id,
    ))]
    fn ensure_conversation<'a>(
        &'a self,
        ctx: &'a Context,
        conversation_id: &'a str,
        conversation_type: ConversationType,
        business_type: &'a str,
        participants: Vec<String>,
        stored_channel_id: String,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        ctx.ensure_not_cancelled().ok(); // 忽略取消错误，继续处理

        debug!(
            tenant_id = ctx.tenant_id().unwrap_or("none"),
            "Context for ensure_conversation"
        );

        let conversation_id = conversation_id.to_string();
        let conversation_type_proto = conversation_type.as_proto();
        let business_type = business_type.to_string();

        // 建群约定（attributes 携带 conversation_id 等）唯一实现在 flare-grpc-proto。
        let request = flare_grpc_proto::ensure_conversation_request(
            &conversation_id,
            conversation_type_proto,
            business_type.clone(),
            participants,
            stored_channel_id.clone(),
        );
        // AlreadyExists 修补参与者时复用同一份参与者列表。
        let participant_protos = request.participants.clone();

        let client = Arc::clone(&self.manage_client);
        Box::pin(async move {
            let mut grpc_request = tonic::Request::new(request);
            set_context_metadata(&mut grpc_request, ctx);

            let tenant_id_in_ctx = ctx.tenant_id();
            let tenant_id_in_metadata = grpc_request
                .metadata()
                .get("x-tenant-id")
                .and_then(|v| v.to_str().ok());
            debug!(
                tenant_id_in_ctx = tenant_id_in_ctx.unwrap_or("none"),
                tenant_id_in_metadata = tenant_id_in_metadata.unwrap_or("none"),
                "Context metadata before gRPC call"
            );

            // 锁内只做 clone，调用在锁外。
            //
            // tonic 生成的客户端调用需要 &mut self，所以 Mutex<Client> 的写法会让
            // **每一次 RPC 都串行**——这条路径在消息入库时被高频调用，串行等于把
            // 整个会话服务的吞吐压到单个在途请求。
            // 这类客户端 clone 很廉价：它们共享底层 Channel 的连接池，clone 出来的
            // 是同一个连接上的新句柄，不会多开连接。
            let mut client = { client.lock().await.clone() };
            match client.create_conversation(grpc_request).await {
                Ok(response) => {
                    let inner = response.into_inner();
                    if let Some(conv) = inner.conversation {
                        debug!(
                            conversation_id = %conv.conversation_id,
                            "Conversation ensured (created or already exists)"
                        );
                        // 验证返回的 conversation_id 是否与请求的一致
                        if conv.conversation_id != conversation_id {
                            warn!(
                                requested_conversation_id = %conversation_id,
                                returned_conversation_id = %conv.conversation_id,
                                "Conversation ID mismatch: requested vs returned"
                            );
                        }
                    }
                    Ok(())
                }
                Err(e) => {
                    // 如果会话已存在，仍然要补齐参与者。历史脏数据或并发创建可能留下
                    // 只有发送方的 conversation_participants，直接忽略会导致收件人无法同步会话。
                    if e.code() == tonic::Code::AlreadyExists {
                        debug!(conversation_id = %conversation_id, "Conversation already exists, repairing participants");
                        let repair_request = ManageParticipantsRequest {
                            conversation_id: conversation_id.clone(),
                            to_add: participant_protos,
                            to_remove: vec![],
                            role_updates: vec![],
                        };
                        let mut grpc_request = tonic::Request::new(repair_request);
                        set_context_metadata(&mut grpc_request, ctx);
                        client
                            .manage_participants(grpc_request)
                            .await
                            .map_err(|e| {
                                warn!(
                                    error = %e,
                                    conversation_id = %conversation_id,
                                    "Failed to repair conversation participants"
                                );
                                FlareError::system(format!(
                                    "Failed to repair conversation participants: {}",
                                    e
                                ))
                            })?;
                        Ok(())
                    } else {
                        warn!(
                            error = %e,
                            conversation_id = %conversation_id,
                            "Failed to ensure conversation"
                        );
                        Err(FlareError::system(format!(
                            "Failed to ensure conversation: {}",
                            e
                        )))
                    }
                }
            }
        })
    }

    fn get_conversation_members<'a>(
        &'a self,
        ctx: &'a Context,
        conversation_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + 'a>> {
        let client = Arc::clone(&self.read_client);
        let conversation_id = conversation_id.to_string();
        Box::pin(async move {
            let mut member_ids = Vec::new();
            let mut cursor = String::new();
            loop {
                let grpc_request = self.member_read_request(
                    ListConversationParticipantsRequest {
                        conversation_id: conversation_id.clone(),
                        cursor: cursor.clone(),
                        // 这里是**串行**游标循环：页大小直接决定往返次数。
                        // 500 时十万人群要 200 次往返，实测一条已读回执因此耗时 47 秒。
                        // 服务端每页代价由固定开销主导（5000 行 61ms vs 500 行 68ms），
                        // 所以放大页大小几乎是纯收益。服务端 clamp 同步放到 5000。
                        limit: 5000,
                        include_removed: false,
                        // 扇出只取 user_id,不看 total/version → 让服务端跳过首页两次 O(成员) 聚合
                        // (COUNT(*) + MAX(updated_at)),十万群每条消息省两次全表聚合。
                        ext: std::collections::HashMap::from([(
                            "skip_meta".to_string(),
                            "1".to_string(),
                        )]),
                    },
                    ctx,
                )?;

                // 锁内只做 clone，调用在锁外。
                //
                // tonic 生成的客户端调用需要 &mut self，所以 Mutex<Client> 的写法会让
                // **每一次 RPC 都串行**——这条路径在消息入库时被高频调用，串行等于把
                // 整个会话服务的吞吐压到单个在途请求。
                // 这类客户端 clone 很廉价：它们共享底层 Channel 的连接池，clone 出来的
                // 是同一个连接上的新句柄，不会多开连接。
                let mut client = { client.lock().await.clone() };
                let response = client
                    .list_conversation_participants(grpc_request)
                    .await
                    .map_err(|e| {
                        FlareError::system(format!("ListConversationParticipants failed: {}", e))
                    })?;
                let page = response.into_inner();
                member_ids.extend(
                    page.participants
                        .into_iter()
                        .map(|p| p.user_id)
                        .filter(|id| !id.trim().is_empty()),
                );
                if !page.has_more {
                    break;
                }
                cursor = page.next_cursor;
            }
            debug!(
                conversation_id = %conversation_id,
                member_count = member_ids.len(),
                "Retrieved conversation members"
            );
            Ok(member_ids)
        })
    }

    fn get_conversation_member_count<'a>(
        &'a self,
        ctx: &'a Context,
        conversation_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<usize>> + Send + 'a>> {
        let client = Arc::clone(&self.read_client);
        let conversation_id = conversation_id.to_string();
        Box::pin(async move {
            let mut grpc_request = tonic::Request::new(GetConversationDetailRequest {
                conversation_id: conversation_id.clone(),
            });
            set_context_metadata(&mut grpc_request, ctx);
            // 会话详情只给成员看，服务签名读不到；只转发入站验过签的用户身份。
            forward_signed_context(&mut grpc_request, ctx)
                .map_err(|status| FlareError::system(status.message().to_string()))?;

            // 锁内只做 clone，调用在锁外。
            //
            // tonic 生成的客户端调用需要 &mut self，所以 Mutex<Client> 的写法会让
            // **每一次 RPC 都串行**——这条路径在消息入库时被高频调用，串行等于把
            // 整个会话服务的吞吐压到单个在途请求。
            // 这类客户端 clone 很廉价：它们共享底层 Channel 的连接池，clone 出来的
            // 是同一个连接上的新句柄，不会多开连接。
            let mut client = { client.lock().await.clone() };
            let response = client
                .get_conversation_detail(grpc_request)
                .await
                .map_err(|e| FlareError::system(format!("GetConversationDetail failed: {}", e)))?;
            let detail = response
                .into_inner()
                .detail
                .ok_or_else(|| FlareError::system("GetConversationDetail missing detail"))?;
            let member_count = usize::try_from(detail.member_count).unwrap_or(usize::MAX);
            debug!(
                conversation_id = %conversation_id,
                member_count,
                "Retrieved conversation member count"
            );
            Ok(member_count)
        })
    }

    fn mark_conversation_as_read<'a>(
        &'a self,
        ctx: &'a Context,
        conversation_id: &'a str,
        read_seq: i64,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        let request = MarkConversationAsReadRequest {
            conversation_id: conversation_id.to_string(),
            read_seq,
        };

        let client = Arc::clone(&self.manage_client);
        let conversation_id = conversation_id.to_string();
        Box::pin(async move {
            let mut grpc_request = tonic::Request::new(request);
            set_context_metadata(&mut grpc_request, ctx);

            // 锁内只做 clone，调用在锁外。
            //
            // tonic 生成的客户端调用需要 &mut self，所以 Mutex<Client> 的写法会让
            // **每一次 RPC 都串行**——这条路径在消息入库时被高频调用，串行等于把
            // 整个会话服务的吞吐压到单个在途请求。
            // 这类客户端 clone 很廉价：它们共享底层 Channel 的连接池，clone 出来的
            // 是同一个连接上的新句柄，不会多开连接。
            let mut client = { client.lock().await.clone() };
            client
                .mark_conversation_as_read(grpc_request)
                .await
                .map_err(|e| {
                    FlareError::system(format!("Conversation MarkConversationAsRead failed: {}", e))
                })?;
            info!(
                conversation_id = %conversation_id,
                read_seq,
                "Mark conversation as read via Conversation service"
            );
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_server_core::grpc::middleware::signed_context::{
        VerifiedContext, verify_signed_context_metadata,
    };
    use flare_server_core::{
        SignedActorType, SignedContextIdentity, SignedContextVerifier, set_signed_context_token,
    };

    use super::*;

    fn keys() -> (SignedContextSigner, SignedContextVerifier) {
        let key = SigningKey::from_bytes(&[35; 32]);
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

    fn client(signer: Option<SignedContextSigner>) -> ConversationClient {
        ConversationClient::new(Channel::from_static("http://127.0.0.1:9").connect_lazy())
            .with_service_read_signer(signer, "flare-orchestrator")
    }

    fn claims(
        request: &tonic::Request<()>,
        verifier: &SignedContextVerifier,
    ) -> flare_server_core::SignedContextClaims {
        verify_signed_context_metadata(request.metadata(), verifier)
            .unwrap()
            .get_data::<VerifiedContext>()
            .and_then(VerifiedContext::claims)
            .cloned()
            .unwrap()
    }

    /// 入站验过签（ingest 收到的发送者）：原样转发那份签名，元数据是同一身份。
    #[tokio::test]
    async fn a_verified_inbound_identity_is_forwarded() {
        let (signer, verifier) = keys();
        let token = signer
            .sign(&SignedContextIdentity::new(
                SignedActorType::User,
                "alice",
                "tenant-a",
                "req-1",
            ))
            .unwrap();
        let mut inbound = tonic::Request::new(());
        set_signed_context_token(&mut inbound, &token).unwrap();
        let verified = verify_signed_context_metadata(inbound.metadata(), &verifier).unwrap();
        let (service_signer, _) = keys();
        let request = client(Some(service_signer))
            .member_read_request((), &verified)
            .unwrap();
        assert_eq!(
            request.metadata().get("x-flare-ctx").unwrap(),
            token.as_str()
        );
        assert_eq!(request.metadata().get("x-user-id").unwrap(), "alice");
        assert_eq!(request.metadata().get("x-tenant-id").unwrap(), "tenant-a");
    }

    /// 没有可转发的签名（orchestrator 的 MQ 与动作入口）：以本服务身份签该租户，元数据不带
    /// 发送者——服务不是用户，带了会被会话服务判为与签名冲突。
    #[tokio::test]
    async fn without_an_inbound_signature_the_service_signs_for_itself() {
        let (signer, verifier) = keys();
        let legacy = Context::with_request_id("req-2")
            .with_tenant_id("tenant-a")
            .with_user_id("alice")
            .with_actor(ActorContext::new("alice").with_type(ActorType::User));
        let request = client(Some(signer))
            .member_read_request((), &legacy)
            .unwrap();
        let claims = claims(&request, &verifier);
        assert_eq!(claims.actor_type, SignedActorType::Service);
        assert_eq!(claims.actor_id, "flare-orchestrator");
        assert_eq!(claims.tenant_id, "tenant-a");
        assert_eq!(claims.request_id, "req-2");
        let metadata = request.metadata();
        assert!(metadata.get("x-user-id").is_none());
        assert_eq!(metadata.get("x-actor-id").unwrap(), "flare-orchestrator");
        assert_eq!(metadata.get("x-actor-type").unwrap(), "2");
        assert_eq!(metadata.get("x-tenant-id").unwrap(), "tenant-a");

        // 没有私钥（未迁移的部署）：只带调用方元数据。
        let request = client(None).member_read_request((), &legacy).unwrap();
        assert!(request.metadata().get("x-flare-ctx").is_none());
        assert_eq!(request.metadata().get("x-user-id").unwrap(), "alice");
    }
}
