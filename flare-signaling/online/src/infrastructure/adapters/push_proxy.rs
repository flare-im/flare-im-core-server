//! Push Proxy `PushService` 的 gRPC 适配器，Online 的两个出站端口都落在它上面：
//! [`DeviceHandoffReporter`] → `ReportDeviceHandoff`（网关失联交接），
//! [`PushBindingRevoker`] → `RevokeDeviceBindings`（服务端有意结束会话时吊销推送绑定）。
//!
//! 每次调用带一枚本 Cell 现签的 Service 上下文：主体是 Online 自己，租户是这次调用所属的租户。
//! Push Proxy 对这两个 RPC 只收内部工作负载的已验证身份，并把调用限定在签名租户内。
//! 静态地址的通道取自 service-kit 的通道缓存（建连有上限、HTTP/2 探活），两个端口共用；调用遇到
//! 传输层失败时丢弃，下一次调用重新解析、新建连接。

use flare_grpc_proto::push::push_service_client::PushServiceClient;
use flare_grpc_proto::push::{ReportDeviceHandoffRequest, RevokeDeviceBindingsRequest};
use flare_im_contracts::service_names::SIGNALING_ONLINE;
use flare_im_service_kit::discovery::{
    DISCOVERY_ROUTE_PREFIX, invalidate_static_channel_on_transport_failure, resolve_grpc_channel,
};
use flare_proto::common::{DeviceHandoff, DeviceHandoffCause};
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError, Result};
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, set_signed_context_token,
};
use tonic::transport::Channel;

use crate::domain::device_handoff::{DeviceHandoffReporter, GatewayLostHandoff};
use crate::domain::push_binding::{BindingRevocation, PushBindingRevoker};

/// 签名上下文里的凭据标识：与接入网关的 `signaling-gateway` 同一套命名，Push Proxy 可据此区分
/// 调用来自哪类工作负载。
pub const PUSH_PROXY_CREDENTIAL_ID: &str = "signaling-online";

pub struct GrpcPushProxyClient {
    /// Push Proxy `PushService` 的地址：`http(s)://` 静态地址或 `discovery://<服务名>`。
    authority: String,
    /// 本 Cell 的签名器（`FLARE_INTERNAL_CTX_PRIVATE_KEY`）。
    signer: SignedContextSigner,
}

impl GrpcPushProxyClient {
    /// 通道惰性建立：Push Proxy 暂不可达时 Online 照常启动，调用按次失败。地址写错则在启动时
    /// 报出来，而不是等到第一次要用的时候。
    pub fn new(authority: &str, signer: SignedContextSigner) -> Result<Self> {
        let authority = authority.trim();
        let addressed = ["http://", "https://", DISCOVERY_ROUTE_PREFIX]
            .into_iter()
            .any(|scheme| {
                authority
                    .strip_prefix(scheme)
                    .is_some_and(|target| !target.is_empty())
            });
        if !addressed || authority.chars().any(char::is_whitespace) {
            return Err(ErrorBuilder::new(
                ErrorCode::ConfigurationError,
                "push proxy address must be an http://, https:// or discovery:// gRPC address",
            )
            .build_error());
        }
        Ok(Self {
            authority: authority.to_string(),
            signer,
        })
    }

    /// Online 自己的 Service 上下文，限定在这次调用所属的租户。
    fn context_token(&self, tenant_id: &str) -> Result<String> {
        self.signer
            .sign(
                &SignedContextIdentity::new(
                    SignedActorType::Service,
                    SIGNALING_ONLINE,
                    tenant_id,
                    uuid::Uuid::new_v4().to_string(),
                )
                .with_credential_id(PUSH_PROXY_CREDENTIAL_ID),
            )
            .map_err(|error| unavailable("push proxy context could not be signed", error))
    }

    /// 带签名上下文的请求。
    fn signed_request<T>(&self, tenant_id: &str, payload: T) -> Result<tonic::Request<T>> {
        let token = self.context_token(tenant_id)?;
        let mut request = tonic::Request::new(payload);
        set_signed_context_token(&mut request, &token)
            .map_err(|status| unavailable("push proxy context could not be attached", status))?;
        Ok(request)
    }

    async fn channel(&self) -> Result<Channel> {
        resolve_grpc_channel(&self.authority)
            .await
            .map_err(|error| unavailable("push proxy channel unavailable", error))
    }

    /// 调用失败：传输层出的错就丢弃缓存的通道，下一次调用重建。
    fn call_failed(&self, reason: &str, status: tonic::Status) -> FlareError {
        invalidate_static_channel_on_transport_failure(&self.authority, &status);
        unavailable(reason, status)
    }
}

/// 线上的交接记录。Online 只报网关失联：`pending` 为空，补提示范围由 `since` 起的变更决定；
/// `connection_id` 是被清扫的会话 ID。
fn device_handoff(handoff: &GatewayLostHandoff) -> DeviceHandoff {
    DeviceHandoff {
        tenant_id: handoff.tenant_id.clone(),
        user_id: handoff.user_id.clone(),
        device_id: handoff.device_id.clone(),
        connection_id: handoff.session_id.clone(),
        gateway_id: handoff.gateway_id.clone(),
        cause: DeviceHandoffCause::GatewayLost as i32,
        since: handoff.since_ms,
        occurred_at: handoff.occurred_at_ms,
        pending: Vec::new(),
    }
}

/// 线上的吊销请求。空的 `device_id` 在线上表示「该用户的全部设备」，所以点名一台设备的请求
/// 设备 ID 不能为空：宁可报错，也不能把一台设备的吊销放大成全部设备。`registered_before` 原样取
/// 请求提交时定下的时刻，重试不会改它；没有这个时刻的请求不发：不带上限的吊销会清掉设备重新
/// 登录后登记的绑定。
fn revocation_request(revocation: &BindingRevocation) -> Result<RevokeDeviceBindingsRequest> {
    if revocation.registered_before_ms <= 0 {
        return Err(ErrorBuilder::new(
            ErrorCode::InvalidParameter,
            "a binding revocation needs the time of the event that triggered it",
        )
        .build_error());
    }
    let device_id = match revocation.device_id.as_deref() {
        Some(device_id) if device_id.trim().is_empty() => {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "a device binding revocation needs a device id",
            )
            .build_error());
        }
        Some(device_id) => device_id.to_string(),
        None => String::new(),
    };
    Ok(RevokeDeviceBindingsRequest {
        tenant_id: revocation.tenant_id.clone(),
        user_id: revocation.user_id.clone(),
        device_id,
        reason: revocation.reason.as_str().to_string(),
        registered_before: revocation.registered_before_ms,
    })
}

fn unavailable(reason: &str, error: impl std::fmt::Display) -> FlareError {
    ErrorBuilder::new(ErrorCode::ServiceUnavailable, reason)
        .details(error.to_string())
        .build_error()
}

impl DeviceHandoffReporter for GrpcPushProxyClient {
    async fn report(&self, handoff: &GatewayLostHandoff) -> Result<()> {
        let request = self.signed_request(
            &handoff.tenant_id,
            ReportDeviceHandoffRequest {
                handoff: Some(device_handoff(handoff)),
            },
        )?;
        let channel = self.channel().await?;
        match PushServiceClient::new(channel)
            .report_device_handoff(request)
            .await
        {
            Ok(response) if response.get_ref().accepted => Ok(()),
            Ok(_) => Err(ErrorBuilder::new(
                ErrorCode::ServiceUnavailable,
                "push proxy did not accept the device handoff",
            )
            .build_error()),
            Err(status) => Err(self.call_failed("device handoff report failed", status)),
        }
    }
}

impl PushBindingRevoker for GrpcPushProxyClient {
    async fn revoke(&self, revocation: &BindingRevocation) -> Result<u32> {
        let request =
            self.signed_request(&revocation.tenant_id, revocation_request(revocation)?)?;
        let channel = self.channel().await?;
        match PushServiceClient::new(channel)
            .revoke_device_bindings(request)
            .await
        {
            Ok(response) => Ok(response.get_ref().revoked),
            Err(status) => Err(self.call_failed("push binding revocation failed", status)),
        }
    }
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_server_core::SignedContextVerifier;

    use super::*;
    use crate::domain::push_binding::BindingRevocationReason;

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

    fn handoff() -> GatewayLostHandoff {
        GatewayLostHandoff {
            tenant_id: "tenant-a".to_string(),
            user_id: "alice".to_string(),
            device_id: "phone".to_string(),
            session_id: "7c0a3f0e-5b1d-4a55-9c57-2f1f6f3f8a10".to_string(),
            gateway_id: "gw-1".to_string(),
            since_ms: 1_700_000_000_000,
            occurred_at_ms: 1_700_000_030_000,
        }
    }

    /// 没有进程在听的地址。
    fn unreachable_authority() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        format!("http://{}", listener.local_addr().unwrap())
    }

    /// 线上记录：原因是网关失联，`connection_id` 是会话 ID，`pending` 为空。
    #[test]
    fn a_gateway_lost_handoff_maps_onto_the_wire_record() {
        let wire = device_handoff(&handoff());
        assert_eq!(wire.tenant_id, "tenant-a");
        assert_eq!(wire.user_id, "alice");
        assert_eq!(wire.device_id, "phone");
        assert_eq!(wire.connection_id, "7c0a3f0e-5b1d-4a55-9c57-2f1f6f3f8a10");
        assert_eq!(wire.gateway_id, "gw-1");
        assert_eq!(wire.cause(), DeviceHandoffCause::GatewayLost);
        assert_eq!(wire.since, 1_700_000_000_000);
        assert_eq!(wire.occurred_at, 1_700_000_030_000);
        assert!(wire.pending.is_empty());
    }

    /// 触发吊销的事件发生的时刻（用例里的固定值）。
    const DECIDED_AT_MS: i64 = 1_700_000_040_000;

    /// 吊销请求：点名设备时带设备 ID，全部设备时 `device_id` 为空；原因是约定的小写名；
    /// `registered_before` 是请求里定下的时刻。
    #[test]
    fn a_binding_revocation_maps_onto_the_wire_request() {
        let one = revocation_request(&BindingRevocation::device(
            "tenant-a",
            "alice",
            "phone",
            BindingRevocationReason::DeviceReplaced,
            DECIDED_AT_MS,
        ))
        .unwrap();
        assert_eq!(one.tenant_id, "tenant-a");
        assert_eq!(one.user_id, "alice");
        assert_eq!(one.device_id, "phone");
        assert_eq!(one.reason, "device_replaced");
        assert_eq!(one.registered_before, DECIDED_AT_MS);

        let all = revocation_request(&BindingRevocation::all_devices(
            "tenant-a",
            "alice",
            BindingRevocationReason::IdentityRevoked,
            DECIDED_AT_MS + 1,
        ))
        .unwrap();
        assert_eq!(all.device_id, "");
        assert_eq!(all.reason, "identity_revoked");
        assert_eq!(all.registered_before, DECIDED_AT_MS + 1);
    }

    /// 点名一台设备却没有设备 ID：报错，不会变成吊销全部设备。
    #[test]
    fn a_device_revocation_without_a_device_id_is_never_widened_to_all_devices() {
        for device_id in ["", "  "] {
            let error = revocation_request(&BindingRevocation::device(
                "tenant-a",
                "alice",
                device_id,
                BindingRevocationReason::Kicked,
                DECIDED_AT_MS,
            ))
            .err()
            .unwrap_or_else(|| panic!("{device_id:?} must be rejected"));
            assert_eq!(error.code(), Some(ErrorCode::InvalidParameter));
        }
    }

    /// 没有事件时刻的吊销不发：不带上限会清掉设备重新登录后登记的绑定。
    #[test]
    fn a_revocation_without_the_event_time_is_not_sent() {
        for registered_before_ms in [0, -1] {
            let error = revocation_request(&BindingRevocation::device(
                "tenant-a",
                "alice",
                "phone",
                BindingRevocationReason::Kicked,
                registered_before_ms,
            ))
            .err()
            .unwrap_or_else(|| panic!("{registered_before_ms} must be rejected"));
            assert_eq!(error.code(), Some(ErrorCode::InvalidParameter));
        }
    }

    /// 调用带的是 Online 自己的 Service 身份，租户取自这次调用；每次现签，请求 ID 各不相同。
    #[test]
    fn calls_are_signed_as_the_online_service_within_the_tenant() {
        let (signer, verifier) = keys();
        let client = GrpcPushProxyClient::new("http://127.0.0.1:50091", signer).unwrap();

        let first = verifier
            .verify(&client.context_token("tenant-a").unwrap())
            .unwrap();
        assert_eq!(first.actor_type, SignedActorType::Service);
        assert_eq!(first.actor_id, SIGNALING_ONLINE);
        assert_eq!(first.actor_id, "flare-signaling-online");
        assert_eq!(first.tenant_id, "tenant-a");
        assert_eq!(
            first.credential_id.as_deref(),
            Some(PUSH_PROXY_CREDENTIAL_ID)
        );

        let second = verifier
            .verify(&client.context_token("tenant-b").unwrap())
            .unwrap();
        assert_eq!(second.tenant_id, "tenant-b");
        assert_ne!(first.request_id, second.request_id);
        // 租户为空签不出上下文：不会带着无租户的身份去调用。
        assert!(client.context_token("").is_err());
    }

    /// 地址只收 `http(s)://` 与 `discovery://`，写错在构造时报出来。
    #[test]
    fn the_push_proxy_address_is_checked_up_front() {
        for accepted in [
            "http://127.0.0.1:50091",
            "https://push-proxy.internal:443",
            " http://push-proxy:50091 ",
            "discovery://flare-push-proxy",
        ] {
            assert!(
                GrpcPushProxyClient::new(accepted, keys().0).is_ok(),
                "{accepted}"
            );
        }
        for rejected in [
            "",
            "push-proxy:50091",
            "http://",
            "discovery://",
            "http://push proxy:50091",
            "nats://push-proxy:4222",
        ] {
            let error = GrpcPushProxyClient::new(rejected, keys().0)
                .err()
                .unwrap_or_else(|| panic!("{rejected:?} must be rejected"));
            assert_eq!(error.code(), Some(ErrorCode::ConfigurationError));
        }
    }

    /// Push Proxy 不可达：两个端口的调用都按次失败，不 panic、不挂住。
    #[tokio::test]
    async fn an_unreachable_push_proxy_fails_the_call() {
        let client = GrpcPushProxyClient::new(&unreachable_authority(), keys().0).unwrap();
        let limit = std::time::Duration::from_secs(10);

        let error = tokio::time::timeout(limit, client.report(&handoff()))
            .await
            .expect("a refused connection fails without waiting")
            .expect_err("nothing answers on that port");
        assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable));

        let revocation = BindingRevocation::device(
            "tenant-a",
            "alice",
            "phone",
            BindingRevocationReason::Kicked,
            DECIDED_AT_MS,
        );
        let error = tokio::time::timeout(limit, client.revoke(&revocation))
            .await
            .expect("a refused connection fails without waiting")
            .expect_err("nothing answers on that port");
        assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable));
    }
}
