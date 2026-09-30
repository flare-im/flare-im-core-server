//! 登录 / 登出的 span 不把令牌和调用上下文写进日志。
//!
//! 这条用例单独成一个测试二进制，二进制里只有它一个用例。它断言的是日志输出，而 tracing 的
//! callsite 兴趣缓存是进程级的：只有一个线程级订阅者（`set_default`）时，tracing-core 认为
//! “只有一个分发器”，别的线程第一次命中某个 callsite 时按自己线程的默认分发器（无订阅者）
//! 算出 `never` 并缓存下来，这个线程的订阅者此后就收不到这个 callsite 的事件。放在 lib 单测里，
//! 并行的其他用例（例如也走 `handle_login` 的租户闸门用例）常常先命中 `handle_login` 的 span，
//! 日志里就没有它，用例间歇失败。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use flare_grpc_proto::signaling::online::{
    DeviceConflictStrategy, LoginRequest, LogoutRequest, PresenceEvent, UserPresenceEvent,
};
use flare_server_core::context::Context;
use flare_server_core::error::Result;
use flare_signaling_online::application::{LoginCommand, LogoutCommand, OnlineCommandHandler};
use flare_signaling_online::domain::repository::{PresencePublisher, SubscriptionRepository};
use flare_signaling_online::domain::service::online_status_service::NoopConversationRepository;
use flare_signaling_online::domain::service::{OnlineStatusService, SubscriptionService};
use tracing_subscriber::fmt::format::FmtSpan;

struct NoopSubscriptions;

impl SubscriptionRepository for NoopSubscriptions {
    async fn add_subscription(&self, _: &str, _: String, _: String) -> Result<()> {
        Ok(())
    }
    async fn remove_subscription(&self, _: &Context, _: &[String]) -> Result<()> {
        Ok(())
    }
    async fn get_user_subscriptions(
        &self,
        _: &Context,
    ) -> Result<Vec<(String, HashMap<String, String>)>> {
        Ok(Vec::new())
    }
    async fn get_topic_subscribers(&self, _: &str, _: &str) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
}

struct NoopPresence;

impl PresencePublisher for NoopPresence {
    async fn publish_presence_event(&self, _: &str, _: PresenceEvent) -> Result<()> {
        Ok(())
    }
    async fn publish_user_presence_event(&self, _: &str, _: UserPresenceEvent) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn login_and_logout_spans_never_log_the_token_or_context() {
    const SECRET_PART: &str = "SIGNATURE-THAT-MUST-NOT-BE-LOGGED";
    let logs = CapturedLogs::default();
    let writer = logs.clone();
    // FmtSpan::NEW renders every span's recorded fields even without events.
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_span_events(FmtSpan::NEW)
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let handler = OnlineCommandHandler::new(
        Arc::new(OnlineStatusService::<_>::new(
            Arc::new(NoopConversationRepository),
            "gateway-test".to_string(),
        )),
        Arc::new(SubscriptionService::new(
            Arc::new(NoopSubscriptions),
            Arc::new(NoopPresence),
        )),
    );
    let ctx = Context::root()
        .with_tenant_id("tenant-a")
        .with_user_id("alice");
    let request = LoginRequest {
        user_id: "alice".to_string(),
        token: format!("eyJhbGciOi.payload.{SECRET_PART}"),
        device_id: "device-a".to_string(),
        device_platform: "web".to_string(),
        desired_conflict_strategy: DeviceConflictStrategy::Coexist as i32,
        metadata: HashMap::from([("connection_id".to_string(), "conn-a".to_string())]),
        ..Default::default()
    };
    let login = handler
        .handle_login(
            &ctx,
            LoginCommand {
                request,
                ctx: ctx.clone(),
            },
        )
        .await
        .expect("login");
    handler
        .handle_logout(
            &ctx,
            LogoutCommand {
                request: LogoutRequest {
                    user_id: "alice".to_string(),
                    conversation_id: login.conversation_id.clone(),
                },
                ctx: ctx.clone(),
            },
        )
        .await
        .expect("logout");

    let output = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    assert!(output.contains("handle_login{"), "{output}");
    assert!(output.contains("device_id=device-a"), "{output}");
    assert!(output.contains("handle_logout{"), "{output}");
    assert!(
        output.contains(&format!("conversation_id={}", login.conversation_id)),
        "{output}"
    );
    assert!(!output.contains(SECRET_PART), "{output}");
    assert!(!output.contains("LoginRequest"), "{output}");
    assert!(!output.contains("ContextInner"), "{output}");
}
