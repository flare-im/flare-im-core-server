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

/// 日志里不能出现令牌的任何 8 个字符以上的片段（以前记前 12 个字符，不透明令牌的开头可能就是
/// 秘密的一部分）。
fn assert_no_token_fragment(output: &str, token: &str) {
    let chars: Vec<char> = token.chars().collect();
    for window in chars.windows(8) {
        let fragment: String = window.iter().collect();
        assert!(
            !output.contains(&fragment),
            "log leaks a token fragment {fragment:?}:\n{output}"
        );
    }
}

#[tokio::test]
async fn authenticate_never_logs_the_token_or_connect_metadata() {
    const SECRET_PART: &str = "SIGNATURE-THAT-MUST-NOT-BE-LOGGED";
    let jwt = format!("eyJhbGciOi.payload.{SECRET_PART}");
    // 不透明令牌：整枚都是秘密，开头的字符同样不能进日志
    let opaque = "opq_HEADSECRET_Zx81Qm_TAILSECRET_Kp20".to_string();
    let logs = CapturedLogs::default();
    let writer = logs.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let principal = AuthenticatedPrincipal {
        user_id: "user-a".to_string(),
        tenant_id: Some("tenant-a".to_string()),
        device_id: Some("device-a".to_string()),
        app_id: None,
        expires_at: None,
        scopes: Vec::new(),
        metadata: Default::default(),
    };
    // 两种令牌 × 令牌被拒、令牌通过两条路径都走一遍
    for token in [&jwt, &opaque] {
        let mut device = DeviceInfo::new("device-a".to_string(), DevicePlatform::Web);
        device.metadata.insert("token".to_string(), token.clone());
        let metadata = HashMap::from([("token".to_string(), token.clone().into_bytes())]);
        for principal in [None, Some(principal.clone())] {
            let handler = AuthHandler::with_default_tenant_id(
                Arc::new(StaticTokenValidator(principal)),
                "tenant-default",
                TenantRuntimeCache::disabled(),
                TenantAccessPolicy::Lenient,
            );
            let _ = handler
                .authenticate(token, "conn-a", Some(&device), Some(&metadata))
                .await;
        }
    }

    let output = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    // 先确认日志确实采到了（否则下面的断言是空转）：两条路径都记了长度
    assert!(output.contains("conn-a"), "{output}");
    assert!(output.contains("Token 验证失败"), "{output}");
    assert!(
        output.contains(&format!("token_len={}", opaque.len())),
        "{output}"
    );
    assert!(!output.contains(SECRET_PART), "{output}");
    assert_no_token_fragment(&output, &jwt);
    assert_no_token_fragment(&output, &opaque);
}
