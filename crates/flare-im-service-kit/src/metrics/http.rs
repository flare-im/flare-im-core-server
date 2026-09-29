//! HTTP 网关请求级黄金信号：请求量/状态码、耗时、在途数。
//!
//! 通过 `Router::layer` 挂载，此时路由已匹配，标签取 axum 的路由模板（`MatchedPath`，如
//! `/api/v1/media/files/{file_id}`），不会把路径参数展开成新序列；未匹配的请求（404 fallback）
//! 统一记为 [`UNMATCHED_ROUTE`]。方法只保留标准动词，其余归为 `OTHER`。

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use axum::extract::MatchedPath;
use once_cell::sync::Lazy;
use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts};
use tonic::codegen::http;
use tower::{Layer, Service};

use super::REGISTRY;

/// 未匹配任何路由的请求的统一标签。
pub const UNMATCHED_ROUTE: &str = "_unmatched";

struct HttpServerMetrics {
    requests_total: IntCounterVec,
    request_seconds: HistogramVec,
    in_flight: IntGaugeVec,
}

static HTTP_SERVER_METRICS: Lazy<HttpServerMetrics> = Lazy::new(|| {
    let requests_total = IntCounterVec::new(
        Opts::new(
            "http_server_requests_total",
            "HTTP requests completed by the gateway, by route template, method and status",
        ),
        &["service", "route", "method", "status"],
    )
    .expect("http_server_requests_total metric definition is static");
    let request_seconds = HistogramVec::new(
        HistogramOpts::new(
            "http_server_request_duration_seconds",
            "Time from receiving an HTTP request to sending response headers",
        )
        .buckets(vec![
            0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ]),
        &["service", "route", "method"],
    )
    .expect("http_server_request_duration_seconds metric definition is static");
    let in_flight = IntGaugeVec::new(
        Opts::new(
            "http_server_in_flight_requests",
            "HTTP requests currently being handled by the gateway",
        ),
        &["service"],
    )
    .expect("http_server_in_flight_requests metric definition is static");
    let _ = REGISTRY.register(Box::new(requests_total.clone()));
    let _ = REGISTRY.register(Box::new(request_seconds.clone()));
    let _ = REGISTRY.register(Box::new(in_flight.clone()));
    HttpServerMetrics {
        requests_total,
        request_seconds,
        in_flight,
    }
});

/// axum 路由指标层；须经 `Router::layer` 挂载才能拿到路由模板。
#[derive(Debug, Clone)]
pub struct HttpMetricsLayer {
    service: Arc<str>,
}

impl HttpMetricsLayer {
    pub fn new(service: impl Into<Arc<str>>) -> Self {
        Self {
            service: service.into(),
        }
    }
}

impl<S> Layer<S> for HttpMetricsLayer {
    type Service = HttpMetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        HttpMetricsService {
            inner,
            service: self.service.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HttpMetricsService<S> {
    inner: S,
    service: Arc<str>,
}

struct InFlight(prometheus::IntGauge);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.dec();
    }
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for HttpMetricsService<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<ReqBody>) -> Self::Future {
        let metrics = &*HTTP_SERVER_METRICS;
        let service = self.service.clone();
        let route: Arc<str> = request
            .extensions()
            .get::<MatchedPath>()
            .map(|matched| Arc::from(matched.as_str()))
            .unwrap_or_else(|| Arc::from(UNMATCHED_ROUTE));
        let method = method_label(request.method());
        let gauge = metrics.in_flight.with_label_values(&[service.as_ref()]);
        gauge.inc();
        let in_flight = InFlight(gauge);
        let started = Instant::now();
        let future = self.inner.call(request);
        Box::pin(async move {
            let result = future.await;
            let status = match &result {
                Ok(response) => response.status().as_u16().to_string(),
                Err(_) => "error".to_string(),
            };
            metrics
                .request_seconds
                .with_label_values(&[service.as_ref(), route.as_ref(), method])
                .observe(started.elapsed().as_secs_f64());
            metrics
                .requests_total
                .with_label_values(&[service.as_ref(), route.as_ref(), method, status.as_str()])
                .inc();
            drop(in_flight);
            result
        })
    }
}

fn method_label(method: &http::Method) -> &'static str {
    match *method {
        http::Method::GET => "GET",
        http::Method::POST => "POST",
        http::Method::PUT => "PUT",
        http::Method::PATCH => "PATCH",
        http::Method::DELETE => "DELETE",
        http::Method::HEAD => "HEAD",
        http::Method::OPTIONS => "OPTIONS",
        _ => "OTHER",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, body::Body, routing::get};
    use tower::ServiceExt;

    fn requests(service: &str, route: &str, method: &str, status: &str) -> u64 {
        HTTP_SERVER_METRICS
            .requests_total
            .with_label_values(&[service, route, method, status])
            .get()
    }

    #[tokio::test]
    async fn labels_use_route_templates_and_merge_unmatched_paths() {
        let service = "http-layer-test";
        let app = Router::new()
            .route("/files/{file_id}", get(|| async { "ok" }))
            .layer(HttpMetricsLayer::new(service));

        for path in ["/files/a1", "/files/b2", "/nope/1", "/nope/2"] {
            let request = http::Request::builder()
                .uri(path)
                .body(Body::empty())
                .expect("request");
            app.clone().oneshot(request).await.expect("infallible");
        }
        let request = http::Request::builder()
            .method("PURGE")
            .uri("/files/c3")
            .body(Body::empty())
            .expect("request");
        app.clone().oneshot(request).await.expect("infallible");

        assert_eq!(requests(service, "/files/{file_id}", "GET", "200"), 2);
        assert_eq!(requests(service, UNMATCHED_ROUTE, "GET", "404"), 2);
        assert_eq!(requests(service, "/files/a1", "GET", "200"), 0);
        let in_flight = HTTP_SERVER_METRICS.in_flight.with_label_values(&[service]);
        assert_eq!(in_flight.get(), 0);
        let latency = HTTP_SERVER_METRICS.request_seconds.with_label_values(&[
            service,
            "/files/{file_id}",
            "GET",
        ]);
        assert_eq!(latency.get_sample_count(), 2);
    }
}
