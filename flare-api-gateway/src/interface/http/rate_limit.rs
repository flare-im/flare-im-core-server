//! API Gateway 的请求限流（`FLARE_API_GATEWAY_RATE_LIMIT_*`）。
//!
//! 网关进程一个令牌桶：每秒补 `RATE_LIMIT_RPS` 个令牌，最多存 `RATE_LIMIT_BURST` 个（至少 1
//! 个）；拿不到令牌的请求直接回 429（`RATE_LIMITED`，带 `Retry-After: 1`），不进鉴权，也不
//! 碰下游。只按进程计数，不分租户与来源：网关在 Edge 之后，来源地址都是 Edge。`/health`
//! 不计入，限流时健康检查照常。`RATE_LIMIT_ENABLED=false` 时不挂这一层。

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{HeaderValue, header::RETRY_AFTER};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use flare_im_service_kit::gateway::RateLimitConfig;
use flare_server_core::error::{ErrorBuilder, ErrorCode};
use flare_server_core::http::HttpApiError;

/// 不计入限流的路径：健康检查。
const UNLIMITED_PATHS: &[&str] = &["/health"];

#[derive(Clone)]
pub struct RequestRateLimiter(Arc<Mutex<TokenBucket>>);

struct TokenBucket {
    per_second: f64,
    capacity: f64,
    tokens: f64,
    refilled_at: Instant,
}

impl RequestRateLimiter {
    /// 关闭时为 `None`，网关不挂限流层。配置校验已保证开启时速率大于 0。
    pub fn from_config(config: &RateLimitConfig) -> Option<Self> {
        if !config.enabled {
            return None;
        }
        Some(Self::new(
            config.requests_per_second,
            config.burst_capacity,
            Instant::now(),
        ))
    }

    fn new(per_second: u32, burst: u32, now: Instant) -> Self {
        let capacity = f64::from(burst.max(1));
        Self(Arc::new(Mutex::new(TokenBucket {
            per_second: f64::from(per_second),
            capacity,
            tokens: capacity,
            refilled_at: now,
        })))
    }

    /// 按 `now` 补令牌后取一个；取不到为 `false`。
    fn try_acquire(&self, now: Instant) -> bool {
        let mut bucket = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let elapsed = now.saturating_duration_since(bucket.refilled_at);
        bucket.tokens =
            (bucket.tokens + elapsed.as_secs_f64() * bucket.per_second).min(bucket.capacity);
        bucket.refilled_at = bucket.refilled_at.max(now);
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// 按配置给路由挂上限流层；关闭时原样返回。
pub fn with_request_rate_limit(router: Router, config: &RateLimitConfig) -> Router {
    match RequestRateLimiter::from_config(config) {
        Some(limiter) => {
            tracing::info!(
                requests_per_second = config.requests_per_second,
                burst_capacity = config.burst_capacity,
                "API Gateway request rate limit enabled"
            );
            router.layer(axum::middleware::from_fn_with_state(
                limiter,
                limit_requests,
            ))
        }
        None => {
            tracing::warn!("API Gateway request rate limit is disabled");
            router
        }
    }
}

async fn limit_requests(
    State(limiter): State<RequestRateLimiter>,
    request: Request,
    next: Next,
) -> Response {
    if UNLIMITED_PATHS.contains(&request.uri().path()) || limiter.try_acquire(Instant::now()) {
        return next.run(request).await;
    }
    let mut response = HttpApiError(
        ErrorBuilder::new(ErrorCode::HttpTooManyRequests, "RATE_LIMITED")
            .details("gateway request rate limit exceeded; retry later")
            .build_error(),
    )
    .into_response();
    response
        .headers_mut()
        .insert(RETRY_AFTER, HeaderValue::from_static("1"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::StatusCode;
    use axum::routing::get;
    use std::time::Duration;
    use tower::ServiceExt;

    #[test]
    fn the_bucket_allows_a_burst_then_refills_at_the_configured_rate() {
        let start = Instant::now();
        let limiter = RequestRateLimiter::new(2, 3, start);
        assert!((0..3).all(|_| limiter.try_acquire(start)));
        assert!(!limiter.try_acquire(start));
        // 半秒补 1 个，再取就空了。
        let later = start + Duration::from_millis(500);
        assert!(limiter.try_acquire(later));
        assert!(!limiter.try_acquire(later));
        // 空闲很久也只存到突发容量。
        let idle = later + Duration::from_secs(60);
        assert!((0..3).all(|_| limiter.try_acquire(idle)));
        assert!(!limiter.try_acquire(idle));
    }

    #[test]
    fn a_disabled_limit_installs_nothing_and_a_zero_burst_still_admits_one() {
        let mut config = RateLimitConfig {
            enabled: false,
            requests_per_second: 1,
            burst_capacity: 0,
        };
        assert!(RequestRateLimiter::from_config(&config).is_none());
        config.enabled = true;
        let limiter = RequestRateLimiter::from_config(&config).unwrap();
        let now = Instant::now();
        assert!(limiter.try_acquire(now));
        assert!(!limiter.try_acquire(now));
    }

    async fn status(router: &Router, path: &str) -> (StatusCode, Option<String>, String) {
        let response = router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let retry_after = response
            .headers()
            .get(RETRY_AFTER)
            .map(|value| value.to_str().unwrap().to_string());
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (
            status,
            retry_after,
            String::from_utf8_lossy(&body).into_owned(),
        )
    }

    /// 超过突发容量的请求回 429，不进入路由；健康检查不计入、也不被拒。修复前这些变量只被
    /// 解析与校验，没有任何一层使用，请求从不被限流。
    #[tokio::test]
    async fn requests_beyond_the_burst_get_429_and_health_is_not_limited() {
        let config = RateLimitConfig {
            enabled: true,
            requests_per_second: 1,
            burst_capacity: 2,
        };
        let reached = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = reached.clone();
        let router = Router::new()
            .route(
                "/api/v1/x",
                get(move || {
                    counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    async { "ok" }
                }),
            )
            .route("/health", get(|| async { "OK" }));
        let router = with_request_rate_limit(router, &config);

        for _ in 0..2 {
            assert_eq!(status(&router, "/api/v1/x").await.0, StatusCode::OK);
        }
        let (code, retry_after, body) = status(&router, "/api/v1/x").await;
        assert_eq!(code, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(retry_after.as_deref(), Some("1"));
        assert!(body.contains("RATE_LIMITED"), "{body}");
        assert_eq!(reached.load(std::sync::atomic::Ordering::SeqCst), 2);
        for _ in 0..5 {
            assert_eq!(status(&router, "/health").await.0, StatusCode::OK);
        }
    }
}
