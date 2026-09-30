//! 长连接鉴权不把令牌写进日志。
//!
//! 这条用例单独成一个测试二进制，二进制里只有它一个用例。它断言的是日志输出，而 tracing 的
//! callsite 兴趣缓存是进程级的：只有一个线程级订阅者（`set_default`）时，tracing-core 认为
//! “只有一个分发器”，别的线程第一次命中某个 callsite 时按自己线程的默认分发器（无订阅者）
//! 算出 `never` 并缓存下来，这个线程的订阅者此后就收不到这个 callsite 的事件。放在 lib 单测里，
//! 并行的其他用例常常先命中 `authenticate` 里的 callsite，日志就是空的，用例间歇失败。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use flare_core::common::{DeviceInfo, DevicePlatform};
use flare_core::server::Authenticator;
use flare_im_service_kit::tenant_runtime::{TenantAccessPolicy, TenantRuntimeCache};
use flare_server_core::auth::{
    AuthError, AuthenticatedPrincipal, TokenValidationRequest, TokenValidator,
};
use flare_signaling_gateway::application::handlers::AuthHandler;

struct StaticTokenValidator(Option<AuthenticatedPrincipal>);

#[async_trait]
impl TokenValidator for StaticTokenValidator {
    async fn validate(
        &self,
        _request: TokenValidationRequest,
    ) -> Result<AuthenticatedPrincipal, AuthError> {
        self.0
            .clone()
            .ok_or_else(|| AuthError::InvalidToken("test token rejected".to_string()))
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
async fn authenticate_never_logs_the_token_or_connect_metadata() {
    const SECRET_PART: &str = "SIGNATURE-THAT-MUST-NOT-BE-LOGGED";
    let token = format!("eyJhbGciOi.payload.{SECRET_PART}");
    let logs = CapturedLogs::default();
    let writer = logs.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_writer(move || writer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let mut device = DeviceInfo::new("device-a".to_string(), DevicePlatform::Web);
    device.metadata.insert("token".to_string(), token.clone());
    let metadata = HashMap::from([("token".to_string(), token.clone().into_bytes())]);
    let principal = AuthenticatedPrincipal {
        user_id: "user-a".to_string(),
        tenant_id: Some("tenant-a".to_string()),
        device_id: Some("device-a".to_string()),
        app_id: None,
        expires_at: None,
        scopes: Vec::new(),
        metadata: Default::default(),
    };
    // 令牌被拒、令牌通过两条路径都走一遍
    for principal in [None, Some(principal)] {
        let handler = AuthHandler::with_default_tenant_id(
            Arc::new(StaticTokenValidator(principal)),
            "tenant-default",
            TenantRuntimeCache::disabled(),
            TenantAccessPolicy::Lenient,
        );
        let _ = handler
            .authenticate(&token, "conn-a", Some(&device), Some(&metadata))
            .await;
    }

    let output = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    // 先确认日志确实采到了（否则下一条断言是空转）
    assert!(output.contains("conn-a"), "{output}");
    assert!(!output.contains(SECRET_PART), "{output}");
}
