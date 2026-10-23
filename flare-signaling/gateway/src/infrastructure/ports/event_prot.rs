//! [`IEventCommandPort`]：经 Signaling **Router** `RouteEvent`（与 `domain/ports/event_prot.rs` 对应）
//!
//! 成功时 `RouteEventResponse.response_data` 可为空（业务仅通过 gRPC OK 表达）；不再解码 `OperationResponse`。
//!
//! 已认证连接的事件带网关为该连接用户签的 `x-flare-ctx`（与发消息同一把 Cell 键、同一个用户与租户）：
//! Route 验签后原样转发给 Orchestrator，操作者由签名决定，而不是由元数据决定。网关没配签名私钥时只带
//! 元数据（与此前相同）。

use std::sync::Arc;

use async_trait::async_trait;
use flare_grpc_proto::signaling::router::{RouteEventRequest, RouteOptions};
use flare_im_contracts::Ctx;
use flare_proto::common::Event;
use flare_server_core::client::request_with_context;
use flare_server_core::error::{ErrorBuilder, ErrorCode as ServerErrorCode, Result};
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, set_signed_context_token,
};

use crate::constants::DEFAULT_ROUTE_SVID;
use crate::domain::ports::IEventCommandPort;

use super::route_grpc_pool::SignalingRouteGrpcPool;

pub struct RouterEventCommandPort {
    pool: Arc<SignalingRouteGrpcPool>,
    signer: Option<SignedContextSigner>,
}

impl RouterEventCommandPort {
    pub fn new(pool: Arc<SignalingRouteGrpcPool>, signer: Option<SignedContextSigner>) -> Self {
        Self { pool, signer }
    }
}

/// The signed identity of an authenticated connection's event, when the gateway can sign one.
fn event_token(tx: &Ctx, signer: Option<&SignedContextSigner>) -> Result<Option<String>> {
    let (Some(signer), Some(origin)) = (
        signer,
        tx.get_data::<crate::infrastructure::connection_context::AuthenticatedConnectionOrigin>(),
    ) else {
        return Ok(None);
    };
    if tx.user_id() != Some(origin.user_id()) || tx.tenant_id() != Some(origin.tenant_id()) {
        return Err(ErrorBuilder::new(
            ServerErrorCode::PermissionDenied,
            "connection identity mismatch",
        )
        .build_error());
    }
    signer
        .sign(&SignedContextIdentity::new(
            SignedActorType::User,
            origin.user_id(),
            origin.tenant_id(),
            tx.request_id(),
        ))
        .map(Some)
        .map_err(|_| {
            ErrorBuilder::new(
                ServerErrorCode::ServiceUnavailable,
                "signed event context unavailable",
            )
            .build_error()
        })
}

#[async_trait]
impl IEventCommandPort for RouterEventCommandPort {
    async fn send_event(&self, tx: &Ctx, event: Event) -> Result<()> {
        let mut client = self.pool.ensure_client().await?;
        let req = RouteEventRequest {
            svid: DEFAULT_ROUTE_SVID.to_string(),
            event: Some(event),
            options: Some(RouteOptions {
                timeout_seconds: 15,
                enable_tracing: true,
                retry_strategy: 1,
                load_balance_strategy: 1,
                priority: 0,
            }),
        };

        let token = event_token(tx, self.signer.as_ref())?;
        let mut request = request_with_context(req, tx);
        if let Some(token) = token {
            set_signed_context_token(&mut request, &token).map_err(|_| {
                ErrorBuilder::new(
                    ServerErrorCode::ServiceUnavailable,
                    "signed event context unavailable",
                )
                .build_error()
            })?;
        }
        client.route_event(request).await.map_err(|status| {
            ErrorBuilder::new(ServerErrorCode::ServiceUnavailable, "RouteEvent RPC failed")
                .details(status.to_string())
                .build_error()
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_server_core::context::{ActorContext, ActorType, Context};
    use flare_server_core::grpc::middleware::signed_context::{
        VerifiedContext, verify_signed_context_metadata,
    };
    use flare_server_core::{SignedActorType, SignedContextSigner, SignedContextVerifier};

    use super::*;
    use crate::infrastructure::connection_context::AuthenticatedConnectionOrigin;

    fn connection(user: &str) -> Ctx {
        Arc::new(
            Context::with_request_id("req-1")
                .with_tenant_id("tenant-a")
                .with_user_id(user)
                .with_actor(ActorContext::new(user).with_type(ActorType::User))
                .insert_data(AuthenticatedConnectionOrigin::new(
                    "conn-1".into(),
                    "alice".into(),
                    "tenant-a".into(),
                )),
        )
    }

    /// An authenticated connection's event carries the connection's signed user, so the
    /// Orchestrator decides the operator from a signature (forwarded by Route), not from metadata.
    #[test]
    fn events_are_signed_as_the_connections_user() {
        let key = SigningKey::from_bytes(&[34; 32]);
        let signer =
            SignedContextSigner::from_private_key(&key.to_pkcs8_pem(Default::default()).unwrap())
                .unwrap();
        let verifier = SignedContextVerifier::from_public_key(
            &key.verifying_key()
                .to_public_key_pem(Default::default())
                .unwrap(),
        )
        .unwrap();
        let token = event_token(&connection("alice"), Some(&signer))
            .unwrap()
            .expect("signed");
        let mut carrier = tonic::Request::new(());
        carrier
            .metadata_mut()
            .insert("x-flare-ctx", token.parse().unwrap());
        let ctx = verify_signed_context_metadata(carrier.metadata(), &verifier).unwrap();
        let claims = ctx
            .get_data::<VerifiedContext>()
            .and_then(VerifiedContext::claims)
            .unwrap();
        assert_eq!(claims.actor_type, SignedActorType::User);
        assert_eq!(claims.actor_id, "alice");
        assert_eq!(claims.tenant_id, "tenant-a");
        // A context that disagrees with the connection is refused, not signed.
        assert!(event_token(&connection("bob"), Some(&signer)).is_err());
        // Without a key, or without an authenticated connection, nothing is signed.
        assert!(event_token(&connection("alice"), None).unwrap().is_none());
        let bare: Ctx = Arc::new(Context::with_request_id("req-1").with_tenant_id("tenant-a"));
        assert!(event_token(&bare, Some(&signer)).unwrap().is_none());
    }
}
