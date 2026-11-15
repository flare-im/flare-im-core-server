//! [`IDeviceHandoffPort`]：经 Push Proxy `ReportDeviceHandoff` 上报设备交接（与 `domain/ports/device_handoff_port.rs` 对应）
//!
//! 上报方在下行写出路径和超时检查里，不能等 RPC：交接先进一个有界队列，后台任务取出后并发
//! 发送，暂时性失败退避重试几次。排不进队列或重试后仍失败的交接记进指标
//! （`device_handoffs_dropped_total`）并留日志——消息本身不会丢（客户端回来后按 seq 同步），
//! 丢的是这一次提示。

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use flare_grpc_proto::push::ReportDeviceHandoffRequest;
use flare_im_contracts::service_names::ACCESS_GATEWAY;
use flare_im_service_kit::metrics::AccessGatewayMetrics;
use flare_proto::common::DeviceHandoff;
use flare_server_core::client::request_with_context;
use flare_server_core::context::{ActorContext, ActorType, Context};
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, set_signed_context_token,
};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Semaphore, mpsc};
use tracing::{debug, warn};

use crate::domain::ports::IDeviceHandoffPort;

use super::push_proxy_grpc_pool::PushProxyGrpcPool;

/// 队列容量。大面积断网时一轮超时检查会一次交出上万条交接，队列要装得下这一波；
/// 每条只有几百字节。
const QUEUE_CAPACITY: usize = 65_536;

/// 同时在途的上报数。
const SEND_CONCURRENCY: usize = 32;

/// 每条交接最多尝试的次数（含第一次）。
const MAX_ATTEMPTS: u32 = 3;

/// 第一次重试前的等待，之后每次翻倍。
const RETRY_BACKOFF: Duration = Duration::from_millis(200);

/// 单次 RPC 的时限：交接晚到几秒就不如让下一条消息的正常推送来提示了。
const CALL_TIMEOUT: Duration = Duration::from_secs(2);

/// 网关签内部上下文时用的凭据标识，与网关的其它内部调用一致。
const GATEWAY_CREDENTIAL: &str = "signaling-gateway";

/// 交接没能交到推送侧的原因，即 `device_handoffs_dropped_total` 的 `reason` 标签。
const DROPPED_QUEUE_FULL: &str = "queue_full";
const DROPPED_RETRIES_EXHAUSTED: &str = "retries_exhausted";
const DROPPED_REFUSED: &str = "refused";

/// 一次上报尝试的失败：值得重试的，与重试结果也一样的。
#[derive(Debug)]
enum SendFailure {
    Retryable(String),
    Fatal(String),
}

/// 把一条交接送到推送侧的一次尝试。重试共用同一个请求号。
#[async_trait]
trait HandoffSink: Send + Sync {
    async fn send(
        &self,
        handoff: &DeviceHandoff,
        request_id: &str,
    ) -> std::result::Result<(), SendFailure>;
}

/// 设备交接上报：有界队列 + 后台发送任务。
pub struct PushProxyHandoffReporter {
    queue: mpsc::Sender<DeviceHandoff>,
    metrics: Arc<AccessGatewayMetrics>,
}

impl PushProxyHandoffReporter {
    /// 建队列并起后台发送任务（须在运行时内调用）。没有内部上下文签名私钥时 Push Proxy 不会
    /// 接受上报：照常运行，每条交接按丢弃计数。
    pub fn spawn(
        pool: Arc<PushProxyGrpcPool>,
        signer: Option<SignedContextSigner>,
        metrics: Arc<AccessGatewayMetrics>,
    ) -> Self {
        if signer.is_none() {
            warn!(
                "device handoff reports disabled: internal context signer missing; unconfirmed alerts are not handed to push"
            );
        }
        Self::spawn_with(
            Arc::new(PushProxyHandoffSink { pool, signer }),
            metrics,
            QUEUE_CAPACITY,
            RETRY_BACKOFF,
        )
    }

    fn spawn_with(
        sink: Arc<dyn HandoffSink>,
        metrics: Arc<AccessGatewayMetrics>,
        capacity: usize,
        backoff: Duration,
    ) -> Self {
        let (queue, inbox) = mpsc::channel(capacity);
        tokio::spawn(deliver(inbox, sink, Arc::clone(&metrics), backoff));
        Self { queue, metrics }
    }
}

impl IDeviceHandoffPort for PushProxyHandoffReporter {
    fn report(&self, handoff: DeviceHandoff) {
        if let Err(TrySendError::Full(handoff) | TrySendError::Closed(handoff)) =
            self.queue.try_send(handoff)
        {
            record_drop(
                &self.metrics,
                &handoff,
                DROPPED_QUEUE_FULL,
                "report queue full",
            );
        }
    }
}

/// 后台发送：逐条取出，限并发地各自发送，一条慢的不挡后面的。
async fn deliver(
    mut inbox: mpsc::Receiver<DeviceHandoff>,
    sink: Arc<dyn HandoffSink>,
    metrics: Arc<AccessGatewayMetrics>,
    backoff: Duration,
) {
    let in_flight = Arc::new(Semaphore::new(SEND_CONCURRENCY));
    while let Some(handoff) = inbox.recv().await {
        let Ok(permit) = Arc::clone(&in_flight).acquire_owned().await else {
            break;
        };
        let sink = Arc::clone(&sink);
        let metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
            let _permit = permit;
            if let Err((reason, detail)) = send_with_retry(&*sink, &handoff, backoff).await {
                record_drop(&metrics, &handoff, reason, &detail);
            }
        });
    }
}

/// 发送一条交接，暂时性失败退避重试。失败时返回丢弃原因（指标标签）与最后一次的错误说明。
async fn send_with_retry(
    sink: &dyn HandoffSink,
    handoff: &DeviceHandoff,
    backoff: Duration,
) -> std::result::Result<(), (&'static str, String)> {
    let request_id = uuid::Uuid::new_v4().to_string();
    let mut wait = backoff;
    let mut attempt = 1;
    loop {
        match sink.send(handoff, &request_id).await {
            Ok(()) => return Ok(()),
            Err(SendFailure::Retryable(reason)) if attempt < MAX_ATTEMPTS => {
                debug!(
                    connection_id = %handoff.connection_id,
                    attempt,
                    reason = %reason,
                    "device handoff report failed, retrying"
                );
                tokio::time::sleep(wait).await;
                wait = wait.saturating_mul(2);
                attempt += 1;
            }
            Err(SendFailure::Retryable(reason)) => {
                return Err((DROPPED_RETRIES_EXHAUSTED, reason));
            }
            Err(SendFailure::Fatal(reason)) => return Err((DROPPED_REFUSED, reason)),
        }
    }
}

fn record_drop(
    metrics: &AccessGatewayMetrics,
    handoff: &DeviceHandoff,
    reason: &str,
    detail: &str,
) {
    metrics.record_device_handoff_dropped(reason);
    warn!(
        connection_id = %handoff.connection_id,
        user_id = %handoff.user_id,
        device_id = %handoff.device_id,
        cause = handoff.cause,
        pending = handoff.pending.len(),
        reason = %reason,
        detail = %detail,
        "device handoff not delivered to push"
    );
}

/// 经 Push Proxy `PushService.ReportDeviceHandoff` 发送。
struct PushProxyHandoffSink {
    pool: Arc<PushProxyGrpcPool>,
    signer: Option<SignedContextSigner>,
}

impl PushProxyHandoffSink {
    /// 以网关的服务身份签名：交接是网关自己得出的结论，不代表哪个用户；租户取交接里的设备租户。
    /// 身份标识用服务名常量本身：Push Proxy 按常量核对上报方，不随部署改名。
    fn signed_request(
        &self,
        handoff: &DeviceHandoff,
        request_id: &str,
    ) -> std::result::Result<tonic::Request<ReportDeviceHandoffRequest>, SendFailure> {
        let signer = self.signer.as_ref().ok_or_else(|| {
            SendFailure::Fatal("internal context signer not configured".to_string())
        })?;
        let ctx = Context::with_request_id(request_id)
            .with_tenant_id(handoff.tenant_id.as_str())
            .with_actor(ActorContext::new(ACCESS_GATEWAY).with_type(ActorType::Service));
        let identity = SignedContextIdentity::new(
            SignedActorType::Service,
            ACCESS_GATEWAY,
            handoff.tenant_id.as_str(),
            ctx.request_id(),
        )
        .with_credential_id(GATEWAY_CREDENTIAL);
        let token = signer
            .sign(&identity)
            .map_err(|_| SendFailure::Fatal("handoff identity could not be signed".to_string()))?;
        let mut request = request_with_context(
            ReportDeviceHandoffRequest {
                handoff: Some(handoff.clone()),
            },
            &ctx,
        );
        request.set_timeout(CALL_TIMEOUT);
        set_signed_context_token(&mut request, &token).map_err(|_| {
            SendFailure::Fatal("handoff identity is not a valid header".to_string())
        })?;
        Ok(request)
    }
}

#[async_trait]
impl HandoffSink for PushProxyHandoffSink {
    async fn send(
        &self,
        handoff: &DeviceHandoff,
        request_id: &str,
    ) -> std::result::Result<(), SendFailure> {
        let request = self.signed_request(handoff, request_id)?;
        let mut client = self
            .pool
            .ensure_client()
            .await
            .map_err(|error| SendFailure::Retryable(error.to_string()))?;
        match client.report_device_handoff(request).await {
            Ok(response) if response.get_ref().accepted => Ok(()),
            Ok(_) => Err(SendFailure::Fatal(
                "push proxy did not accept the handoff".to_string(),
            )),
            Err(status) => Err(classify(&status)),
        }
    }
}

/// 对端明确拒绝（身份或载荷不被接受）时重试结果一样；其余按暂时性故障重试。
fn classify(status: &tonic::Status) -> SendFailure {
    match status.code() {
        tonic::Code::PermissionDenied
        | tonic::Code::Unauthenticated
        | tonic::Code::InvalidArgument
        | tonic::Code::FailedPrecondition
        | tonic::Code::Unimplemented => SendFailure::Fatal(status.to_string()),
        _ => SendFailure::Retryable(status.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_proto::common::{DeviceHandoffCause, DeviceHandoffItem};
    use flare_server_core::SignedContextVerifier;
    use flare_server_core::grpc::middleware::signed_context::{
        VerifiedContext, verify_signed_context_metadata,
    };

    use super::*;

    fn handoff(connection_id: &str) -> DeviceHandoff {
        DeviceHandoff {
            tenant_id: "tenant-a".to_string(),
            user_id: "u1".to_string(),
            device_id: "d1".to_string(),
            connection_id: connection_id.to_string(),
            gateway_id: "gateway-test".to_string(),
            cause: DeviceHandoffCause::AckTimeout as i32,
            since: 0,
            occurred_at: 1_794_000_000_000,
            pending: vec![DeviceHandoffItem {
                conversation_id: "g1".to_string(),
                seq: 5,
                message_id: "m-5".to_string(),
            }],
        }
    }

    enum Outcome {
        Accepted,
        Transient,
        Refused,
        /// 一直不返回，模拟推送侧卡住。
        Stuck,
    }

    /// 按脚本应答的接收端，记下每次尝试的（连接，请求号）。脚本用完后一律接受。
    #[derive(Default)]
    struct ScriptedSink {
        script: Mutex<VecDeque<Outcome>>,
        attempts: Mutex<Vec<(String, String)>>,
    }

    impl ScriptedSink {
        fn with(script: impl IntoIterator<Item = Outcome>) -> Arc<Self> {
            Arc::new(Self {
                script: Mutex::new(script.into_iter().collect()),
                attempts: Mutex::default(),
            })
        }

        fn attempts(&self) -> Vec<(String, String)> {
            self.attempts.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl HandoffSink for ScriptedSink {
        async fn send(
            &self,
            handoff: &DeviceHandoff,
            request_id: &str,
        ) -> std::result::Result<(), SendFailure> {
            self.attempts
                .lock()
                .unwrap()
                .push((handoff.connection_id.clone(), request_id.to_string()));
            let outcome = self.script.lock().unwrap().pop_front();
            match outcome {
                None | Some(Outcome::Accepted) => Ok(()),
                Some(Outcome::Transient) => Err(SendFailure::Retryable("unavailable".to_string())),
                Some(Outcome::Refused) => Err(SendFailure::Fatal("permission denied".to_string())),
                Some(Outcome::Stuck) => std::future::pending().await,
            }
        }
    }

    /// 起一个接到 `sink` 的上报器，连同它记丢弃数的指标。
    fn reporter(
        sink: Arc<ScriptedSink>,
        capacity: usize,
    ) -> (PushProxyHandoffReporter, Arc<AccessGatewayMetrics>) {
        let metrics = Arc::new(AccessGatewayMetrics::new());
        let reporter = PushProxyHandoffReporter::spawn_with(
            sink,
            metrics.clone(),
            capacity,
            Duration::from_millis(1),
        );
        (reporter, metrics)
    }

    fn dropped(metrics: &AccessGatewayMetrics, reason: &str) -> u64 {
        metrics
            .device_handoffs_dropped_total
            .with_label_values(&[reason])
            .get()
    }

    /// 等到条件成立；后台任务在别的任务里跑，这里让出执行权并给重试的退避留时间。
    async fn eventually(condition: impl Fn() -> bool) {
        for _ in 0..2_000 {
            if condition() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("condition not reached");
    }

    #[tokio::test]
    async fn a_transient_failure_is_retried_under_the_same_request_id() {
        let sink = ScriptedSink::with([Outcome::Transient, Outcome::Transient, Outcome::Accepted]);
        let (reporter, metrics) = reporter(sink.clone(), 16);

        reporter.report(handoff("c1"));
        eventually(|| sink.attempts().len() == 3).await;

        let attempts = sink.attempts();
        assert!(attempts.iter().all(|(connection, _)| connection == "c1"));
        assert!(
            attempts
                .iter()
                .all(|(_, request)| *request == attempts[0].1),
            "retries of one handoff share a request id: {attempts:?}"
        );
        for reason in [
            DROPPED_QUEUE_FULL,
            DROPPED_RETRIES_EXHAUSTED,
            DROPPED_REFUSED,
        ] {
            assert_eq!(dropped(&metrics, reason), 0, "{reason}");
        }
    }

    #[tokio::test]
    async fn a_handoff_that_keeps_failing_or_is_refused_counts_as_dropped() {
        let sink = ScriptedSink::with([
            Outcome::Transient,
            Outcome::Transient,
            Outcome::Transient,
            Outcome::Refused,
        ]);
        let (reporter, metrics) = reporter(sink.clone(), 16);

        reporter.report(handoff("c1"));
        eventually(|| dropped(&metrics, DROPPED_RETRIES_EXHAUSTED) == 1).await;
        assert_eq!(sink.attempts().len(), MAX_ATTEMPTS as usize);

        // 被明确拒绝的不重试。
        reporter.report(handoff("c2"));
        eventually(|| dropped(&metrics, DROPPED_REFUSED) == 1).await;
        assert_eq!(sink.attempts().len(), MAX_ATTEMPTS as usize + 1);

        // 之后的交接不受影响。
        reporter.report(handoff("c3"));
        eventually(|| sink.attempts().len() == MAX_ATTEMPTS as usize + 2).await;
        assert_eq!(dropped(&metrics, DROPPED_RETRIES_EXHAUSTED), 1);
        assert_eq!(dropped(&metrics, DROPPED_REFUSED), 1);
        assert_eq!(dropped(&metrics, DROPPED_QUEUE_FULL), 0);
    }

    #[tokio::test]
    async fn a_full_queue_drops_and_counts_without_blocking_the_caller() {
        let sink = ScriptedSink::with(std::iter::repeat_with(|| Outcome::Stuck).take(64));
        let (reporter, metrics) = reporter(sink, 1);

        // 发送任务还没有机会运行：队列只装得下一条，其余当场丢弃并计数。
        for index in 0..10 {
            reporter.report(handoff(&format!("c{index}")));
        }
        assert_eq!(dropped(&metrics, DROPPED_QUEUE_FULL), 9);
    }

    fn keys() -> (SignedContextSigner, SignedContextVerifier) {
        let key = SigningKey::from_bytes(&[57; 32]);
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

    /// 上报以网关的服务身份签名，租户是设备所在的租户；请求体原样带上交接。
    #[test]
    fn reports_are_signed_as_the_gateway_service_in_the_device_tenant() {
        let (signer, verifier) = keys();
        let sink = PushProxyHandoffSink {
            pool: Arc::new(PushProxyGrpcPool::new()),
            signer: Some(signer),
        };
        let request = sink
            .signed_request(&handoff("c1"), "req-1")
            .expect("signed request");

        let ctx = verify_signed_context_metadata(request.metadata(), &verifier).unwrap();
        let claims = ctx
            .get_data::<VerifiedContext>()
            .and_then(VerifiedContext::claims)
            .unwrap();
        assert_eq!(claims.actor_type, SignedActorType::Service);
        assert_eq!(claims.actor_id, ACCESS_GATEWAY);
        assert_eq!(claims.tenant_id, "tenant-a");
        assert_eq!(claims.credential_id.as_deref(), Some(GATEWAY_CREDENTIAL));
        assert_eq!(claims.request_id, "req-1");
        assert!(request.metadata().get("x-user-id").is_none());
        assert_eq!(request.get_ref().handoff, Some(handoff("c1")));
    }

    /// 没配签名私钥时不发未签名的上报：Push Proxy 只认已验证的内部身份。
    #[test]
    fn an_unsigned_report_is_never_sent() {
        let sink = PushProxyHandoffSink {
            pool: Arc::new(PushProxyGrpcPool::new()),
            signer: None,
        };
        assert!(matches!(
            sink.signed_request(&handoff("c1"), "req-1"),
            Err(SendFailure::Fatal(_))
        ));
    }

    #[test]
    fn only_explicit_refusals_are_final() {
        for status in [
            tonic::Status::permission_denied("caller not permitted"),
            tonic::Status::unauthenticated("signed identity required"),
            tonic::Status::invalid_argument("handoff required"),
        ] {
            assert!(matches!(classify(&status), SendFailure::Fatal(_)));
        }
        for status in [
            tonic::Status::unavailable("connection refused"),
            tonic::Status::deadline_exceeded("timeout"),
            tonic::Status::internal("broker publish failed"),
        ] {
            assert!(matches!(classify(&status), SendFailure::Retryable(_)));
        }
    }
}
