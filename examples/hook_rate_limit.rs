//! 开源插件样例：发送频率限制。
//!
//! # 这是什么
//!
//! 一个**完整可运行**的 HookPlugin：它是一个独立进程，用 gRPC 与 flare-im-core
//! 通信，在消息落库之前决定放行还是拒绝。核心不需要为它重新编译，也不需要 fork。
//!
//! 选「频率限制」作为第一个样例，是因为它用到了 PreSend 最关键的语义——**拒绝**，
//! 而且几乎每个上线的 IM 都要做。代码短到可以一口气读完。
//!
//! # 跑起来
//!
//! ```bash
//! # 1. 起插件（默认 127.0.0.1:7801）
//! cargo run --example hook_rate_limit
//!
//! # 2. 让核心用它：把下面这段写进 flare-im-core 的 config/hooks.toml
//! #    [[pre_send]]
//! #    name = "rate-limit"
//! #    priority = 10
//! #    timeout_ms = 200
//! #    require_success = true          # 插件不可用时拒发；改 false 则放行
//! #    [pre_send.transport]
//! #    type = "grpc"
//! #    endpoint = "http://127.0.0.1:7801"
//!
//! # 3. 起服务端，然后连发几条消息，第 N+1 条会被拒
//! ```
//!
//! 可调参数走环境变量，便于在不改代码的情况下试：
//!
//! - `HOOK_RATE_LIMIT_ADDR`（默认 `127.0.0.1:7801`）
//! - `HOOK_RATE_LIMIT_MAX`：窗口内允许的条数（默认 5）
//! - `HOOK_RATE_LIMIT_WINDOW_SECS`：窗口长度（默认 10）
//!
//! # 写自己的插件时照抄哪几处
//!
//! 1. `impl HookPlugin for ...` —— 只有一个 `call`，靠 `operation` 分派。
//! 2. `operation` 的取值是 `flare.hook.v1.<hook 名>`，例如 `flare.hook.v1.pre_send`。
//! 3. 请求/响应都装在 `Any` 里：按 `type_url` 对应的类型解包，回包同理。
//! 4. **不认识的 operation 要放行**（见 `call` 的兜底分支），否则将来核心新增 hook
//!    点时，你的插件会把所有消息挡下来。

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use flare_grpc_proto::capability::hook_plugin_server::{HookPlugin, HookPluginServer};
use flare_grpc_proto::capability::{
    GenericRequest, GenericResponse, PreSendHookRequest, PreSendHookResponse,
};
use prost::Message as _;
use tonic::{Request, Response, Status, transport::Server};

/// 每个 hook 点的 operation 名。核心按这个字符串分派。
const OP_PRE_SEND: &str = "flare.hook.v1.pre_send";

/// 回包的 type_url 要与核心期望的类型一致，否则对面解不开。
const PRE_SEND_RESPONSE_TYPE: &str = "type.googleapis.com/flare.capability.v1.PreSendHookResponse";

/// 拒绝原因码。**机器可读的那个要稳定**——客户端会拿它做分支，
/// 改动等于破坏契约；人类可读的那句可以随便改。
const DENY_CODE_RATE_LIMITED: &str = "RATE_LIMITED";

fn env_usize(key: &str, fallback: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(fallback)
}

/// 滑动窗口计数器。
///
/// 有意保持进程内存实现：样例的重点是**插件契约**，不是限流算法。
/// 真上生产该换成 Redis 之类的共享存储——否则多副本部署时每个副本各限各的。
struct SlidingWindow {
    max_per_window: usize,
    window: Duration,
    /// key = 租户 + 用户；value = 该窗口内的发送时刻
    hits: Mutex<HashMap<String, Vec<Instant>>>,
}

impl SlidingWindow {
    fn new(max_per_window: usize, window: Duration) -> Self {
        Self {
            max_per_window,
            window,
            hits: Mutex::new(HashMap::new()),
        }
    }

    /// 返回 true 表示放行。
    fn allow(&self, key: &str) -> bool {
        let now = Instant::now();
        let mut hits = self.hits.lock().expect("rate limit state poisoned");
        let entry = hits.entry(key.to_string()).or_default();
        entry.retain(|t| now.duration_since(*t) < self.window);
        if entry.len() >= self.max_per_window {
            return false;
        }
        entry.push(now);
        true
    }
}

struct RateLimitPlugin {
    limiter: SlidingWindow,
}

impl RateLimitPlugin {
    fn handle_pre_send(&self, payload: &[u8]) -> Result<PreSendHookResponse, Status> {
        let request = PreSendHookRequest::decode(payload)
            .map_err(|e| Status::invalid_argument(format!("decode PreSendHookRequest: {e}")))?;

        let ctx = request.context.unwrap_or_default();
        // operator_user_id 在系统动作时为空——那类消息不该被用户级限流挡住。
        let Some(user) = Some(ctx.operator_user_id).filter(|s| !s.is_empty()) else {
            return Ok(allow(request.draft));
        };
        let key = format!("{}:{}", ctx.tenant_id, user);

        if self.limiter.allow(&key) {
            return Ok(allow(request.draft));
        }

        tracing::info!(tenant = %ctx.tenant_id, %user, "rate limited");
        Ok(PreSendHookResponse {
            allow: false,
            deny_reason_code: DENY_CODE_RATE_LIMITED.to_string(),
            // 这句会一路回到客户端，写成用户能看懂的话，别写内部术语
            deny_reason_message: "发送太频繁了，请稍后再试".to_string(),
            ..Default::default()
        })
    }
}

/// 放行：原样带回草稿。**必须回传 draft** —— 核心用回包里的草稿继续往下走，
/// 不回传等于把消息内容清空。改写内容（脱敏、加标签）也在这里做。
fn allow(draft: Option<flare_grpc_proto::capability::HookMessageDraft>) -> PreSendHookResponse {
    PreSendHookResponse {
        allow: true,
        draft,
        ..Default::default()
    }
}

#[tonic::async_trait]
impl HookPlugin for RateLimitPlugin {
    async fn call(
        &self,
        request: Request<GenericRequest>,
    ) -> Result<Response<GenericResponse>, Status> {
        let req = request.into_inner();
        let request_id = req.request_id.clone();

        let inner = match req.operation.as_str() {
            OP_PRE_SEND => {
                let payload = req.payload.map(|a| a.value).unwrap_or_default();
                self.handle_pre_send(&payload)?
            }
            // 不认识的 operation 一律放行。
            //
            // 这条兜底很重要：核心将来新增 hook 点时，老插件会收到没见过的
            // operation。默认拒绝的话，升级核心当天所有消息都会被这个插件挡下来。
            other => {
                tracing::debug!(operation = %other, "未处理的 operation，放行");
                PreSendHookResponse {
                    allow: true,
                    ..Default::default()
                }
            }
        };

        Ok(Response::new(GenericResponse {
            ok: true,
            request_id,
            payload: Some(prost_types::Any {
                type_url: PRE_SEND_RESPONSE_TYPE.to_string(),
                value: inner.encode_to_vec(),
            }),
            ..Default::default()
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let addr = std::env::var("HOOK_RATE_LIMIT_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:7801".to_string())
        .parse()?;
    let max = env_usize("HOOK_RATE_LIMIT_MAX", 5);
    let window_secs = env_usize("HOOK_RATE_LIMIT_WINDOW_SECS", 10) as u64;

    tracing::info!(
        %addr, max_per_window = max, window_secs,
        "rate limit hook plugin 已启动；把 endpoint 写进 flare-im-core 的 config/hooks.toml"
    );

    let plugin = RateLimitPlugin {
        limiter: SlidingWindow::new(max, Duration::from_secs(window_secs)),
    };

    Server::builder()
        .add_service(HookPluginServer::new(plugin))
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod rate_limit_tests {
    use super::*;

    #[test]
    fn allows_up_to_the_limit_then_rejects() {
        let w = SlidingWindow::new(3, Duration::from_secs(60));
        assert!(w.allow("t0:alice"));
        assert!(w.allow("t0:alice"));
        assert!(w.allow("t0:alice"));
        assert!(!w.allow("t0:alice"), "第 4 条应被拒");
    }

    #[test]
    fn counts_per_user_not_globally() {
        // 一个人刷屏不该把别人也挡住——这是最容易写错的一处
        let w = SlidingWindow::new(1, Duration::from_secs(60));
        assert!(w.allow("t0:alice"));
        assert!(!w.allow("t0:alice"));
        assert!(w.allow("t0:bob"), "bob 不该受 alice 影响");
    }

    #[test]
    fn separates_tenants() {
        let w = SlidingWindow::new(1, Duration::from_secs(60));
        assert!(w.allow("t0:alice"));
        assert!(w.allow("t1:alice"), "不同租户的同名用户是两个人");
    }

    #[test]
    fn forgets_hits_after_the_window() {
        let w = SlidingWindow::new(1, Duration::from_millis(40));
        assert!(w.allow("t0:alice"));
        assert!(!w.allow("t0:alice"));
        std::thread::sleep(Duration::from_millis(60));
        assert!(w.allow("t0:alice"), "窗口过后应重新放行");
    }
}
