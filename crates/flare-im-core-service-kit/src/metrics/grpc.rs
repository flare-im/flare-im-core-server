//! gRPC 服务端黄金信号：请求量/状态码、耗时、在途数（设计 §13.2）。
//!
//! 挂在 tonic `Server::builder().layer(..)` 上，覆盖该进程注册的全部 gRPC 服务。
//! 标签只有 `service`、`method`、`code`：`method` 取自路由路径，但返回 `Unimplemented`
//! 的未知路径统一记为 [`UNKNOWN_METHOD`]，客户端乱打路径不会放大时序基数。
//!
//! 状态码取响应头里的 `grpc-status`：tonic 的一元调用错误以 trailers-only 响应返回，
//! 状态就在响应头里；成功调用的状态在 trailer 中，记为 `Ok`。流式调用在响应头之后才出错时
//! 同样记为 `Ok`，耗时为到响应头的时间——这是不包裹响应体换来的简单性，流式细节看 trace。

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use once_cell::sync::Lazy;
use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts};
use tonic::codegen::http;
use tower::{Layer, Service};

use super::REGISTRY;

/// 未知方法（`Unimplemented`）的统一标签。
pub const UNKNOWN_METHOD: &str = "_unknown";
/// 内层服务直接返回错误（连接级失败），没有 gRPC 状态码。
pub const TRANSPORT_ERROR_CODE: &str = "TransportError";

struct GrpcServerMetrics {
    handled_total: IntCounterVec,
    handling_seconds: HistogramVec,
    in_flight: IntGaugeVec,
}

static GRPC_SERVER_METRICS: Lazy<GrpcServerMetrics> = Lazy::new(|| {
    let handled_total = IntCounterVec::new(
        Opts::new(
            "grpc_server_handled_total",
            "gRPC requests completed by the server, by service, method and status code",
        ),
        &["service", "method", "code"],
    )
    .expect("grpc_server_handled_total metric definition is static");
    let handling_seconds = HistogramVec::new(
        HistogramOpts::new(
            "grpc_server_handling_seconds",
            "Time from receiving a gRPC request to sending response headers",
        )
        .buckets(vec![
            0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
        ]),
        &["service", "method"],
    )
    .expect("grpc_server_handling_seconds metric definition is static");
    let in_flight = IntGaugeVec::new(
        Opts::new(
            "grpc_server_in_flight_requests",
            "gRPC requests currently being handled by the server",
        ),
        &["service"],
    )
    .expect("grpc_server_in_flight_requests metric definition is static");
    let _ = REGISTRY.register(Box::new(handled_total.clone()));
    let _ = REGISTRY.register(Box::new(handling_seconds.clone()));
    let _ = REGISTRY.register(Box::new(in_flight.clone()));
    GrpcServerMetrics {
        handled_total,
        handling_seconds,
        in_flight,
    }
});

/// tonic 服务端指标层。`service` 是进程级服务名（与 `/metrics` 的 `flare_service_info` 一致）。
#[derive(Debug, Clone)]
pub struct GrpcMetricsLayer {
    service: Arc<str>,
}

impl GrpcMetricsLayer {
    pub fn new(service: impl Into<Arc<str>>) -> Self {
        Self {
            service: service.into(),
        }
    }
}

impl<S> Layer<S> for GrpcMetricsLayer {
    type Service = GrpcMetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcMetricsService {
            inner,
            service: self.service.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct GrpcMetricsService<S> {
    inner: S,
    service: Arc<str>,
}

/// 在途计数随请求 future 的生命周期增减：请求被取消（客户端断开）时同样递减。
struct InFlight(prometheus::IntGauge);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.dec();
    }
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for GrpcMetricsService<S>
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
        let metrics = &*GRPC_SERVER_METRICS;
        let service = self.service.clone();
        let method = request.uri().path().to_string();
        let gauge = metrics.in_flight.with_label_values(&[service.as_ref()]);
        gauge.inc();
        let in_flight = InFlight(gauge);
        let started = Instant::now();
        let future = self.inner.call(request);
        Box::pin(async move {
            let result = future.await;
            let code = match &result {
                Ok(response) => grpc_status_name(response.headers()),
                Err(_) => TRANSPORT_ERROR_CODE,
            };
            let method = method_label(&method, code);
            metrics
                .handling_seconds
                .with_label_values(&[service.as_ref(), method])
                .observe(started.elapsed().as_secs_f64());
            metrics
                .handled_total
                .with_label_values(&[service.as_ref(), method, code])
                .inc();
            drop(in_flight);
            result
        })
    }
}

fn grpc_status_name(headers: &http::HeaderMap) -> &'static str {
    let Some(value) = headers.get("grpc-status") else {
        return "Ok";
    };
    let code = value
        .to_str()
        .ok()
        .and_then(|raw| raw.trim().parse::<i32>().ok())
        .map(tonic::Code::from_i32)
        .unwrap_or(tonic::Code::Unknown);
    code_name(code)
}

fn code_name(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::Ok => "Ok",
        tonic::Code::Cancelled => "Cancelled",
        tonic::Code::Unknown => "Unknown",
        tonic::Code::InvalidArgument => "InvalidArgument",
        tonic::Code::DeadlineExceeded => "DeadlineExceeded",
        tonic::Code::NotFound => "NotFound",
        tonic::Code::AlreadyExists => "AlreadyExists",
        tonic::Code::PermissionDenied => "PermissionDenied",
        tonic::Code::ResourceExhausted => "ResourceExhausted",
        tonic::Code::FailedPrecondition => "FailedPrecondition",
        tonic::Code::Aborted => "Aborted",
        tonic::Code::OutOfRange => "OutOfRange",
        tonic::Code::Unimplemented => "Unimplemented",
        tonic::Code::Internal => "Internal",
        tonic::Code::Unavailable => "Unavailable",
        tonic::Code::DataLoss => "DataLoss",
        tonic::Code::Unauthenticated => "Unauthenticated",
    }
}

/// 只有路由存在的方法才以路径为标签；未知路径与非 gRPC 形状的路径归并。
fn method_label<'a>(path: &'a str, code: &str) -> &'a str {
    let well_formed = path
        .strip_prefix('/')
        .and_then(|rest| rest.split_once('/'))
        .is_some_and(|(service, method)| {
            !service.is_empty() && !method.is_empty() && !method.contains('/')
        });
    if code == "Unimplemented" || !well_formed {
        UNKNOWN_METHOD
    } else {
        path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;

    #[derive(Clone)]
    struct Fixed(Option<&'static str>);

    impl Service<http::Request<()>> for Fixed {
        type Response = http::Response<()>;
        type Error = Infallible;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _: http::Request<()>) -> Self::Future {
            let mut response = http::Response::new(());
            if let Some(status) = self.0 {
                response
                    .headers_mut()
                    .insert("grpc-status", http::HeaderValue::from_static(status));
            }
            std::future::ready(Ok(response))
        }
    }

    async fn call(service: &str, status: Option<&'static str>, path: &str) {
        let mut svc = GrpcMetricsLayer::new(service).layer(Fixed(status));
        let request = http::Request::builder()
            .uri(path)
            .body(())
            .expect("request");
        svc.call(request).await.expect("infallible");
    }

    fn handled(service: &str, method: &str, code: &str) -> u64 {
        GRPC_SERVER_METRICS
            .handled_total
            .with_label_values(&[service, method, code])
            .get()
    }

    #[tokio::test]
    async fn records_code_latency_and_in_flight() {
        let service = "grpc-layer-test";
        let method = "/flare.test.v1.Probe/Ping";
        call(service, None, method).await;
        call(service, Some("3"), method).await;
        assert_eq!(handled(service, method, "Ok"), 1);
        assert_eq!(handled(service, method, "InvalidArgument"), 1);
        let latency = GRPC_SERVER_METRICS
            .handling_seconds
            .with_label_values(&[service, method]);
        assert_eq!(latency.get_sample_count(), 2);
        let in_flight = GRPC_SERVER_METRICS.in_flight.with_label_values(&[service]);
        assert_eq!(in_flight.get(), 0, "gauge returns to zero after completion");
    }

    #[tokio::test]
    async fn unknown_or_malformed_paths_do_not_create_new_series() {
        let service = "grpc-layer-cardinality";
        call(service, Some("12"), "/attacker.Random/abc123").await;
        call(service, Some("12"), "/attacker.Random/def456").await;
        call(service, None, "/not-grpc").await;
        assert_eq!(handled(service, UNKNOWN_METHOD, "Unimplemented"), 2);
        assert_eq!(handled(service, UNKNOWN_METHOD, "Ok"), 1);
        assert_eq!(
            handled(service, "/attacker.Random/abc123", "Unimplemented"),
            0
        );
    }

    #[test]
    fn unparseable_status_is_unknown() {
        let mut headers = http::HeaderMap::new();
        headers.insert("grpc-status", http::HeaderValue::from_static("x"));
        assert_eq!(grpc_status_name(&headers), "Unknown");
        headers.insert("grpc-status", http::HeaderValue::from_static("16"));
        assert_eq!(grpc_status_name(&headers), "Unauthenticated");
    }
}
