//! Reporter metrics, labelled by producer only: per-tenant usage lives in the events, not in
//! time series.

use flare_im_service_kit::metrics::REGISTRY;
use prometheus::{IntCounterVec, IntGaugeVec, Opts};
use std::sync::LazyLock;

/// Why a counted record never reached a window event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordDrop {
    /// Empty or malformed tenant id.
    InvalidTenant,
    /// The window already holds the maximum number of counter series.
    CounterLimit,
    /// The pending active-user hashes are at their limit.
    ActiveUserLimit,
}

impl RecordDrop {
    const ALL: [Self; 3] = [
        Self::InvalidTenant,
        Self::CounterLimit,
        Self::ActiveUserLimit,
    ];

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidTenant => "invalid_tenant",
            Self::CounterLimit => "counter_limit",
            Self::ActiveUserLimit => "active_user_limit",
        }
    }
}

/// Why a built event was never published.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EventDrop {
    /// The unpublished backlog was full; the oldest event went.
    BacklogFull,
    /// Still unpublished when the shutdown flush ran out of time.
    Shutdown,
    /// Could not be serialized.
    Encode,
}

impl EventDrop {
    const ALL: [Self; 3] = [Self::BacklogFull, Self::Shutdown, Self::Encode];

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::BacklogFull => "backlog_full",
            Self::Shutdown => "shutdown",
            Self::Encode => "encode",
        }
    }
}

pub(crate) struct ReporterMetrics {
    pub(crate) published: IntCounterVec,
    pub(crate) publish_failures: IntCounterVec,
    pub(crate) records_dropped: IntCounterVec,
    pub(crate) events_dropped: IntCounterVec,
    pub(crate) backlog: IntGaugeVec,
}

static METRICS: LazyLock<ReporterMetrics> = LazyLock::new(|| {
    let metrics = ReporterMetrics::build();
    for collector in [
        Box::new(metrics.published.clone()) as Box<dyn prometheus::core::Collector>,
        Box::new(metrics.publish_failures.clone()),
        Box::new(metrics.records_dropped.clone()),
        Box::new(metrics.events_dropped.clone()),
        Box::new(metrics.backlog.clone()),
    ] {
        let _ = REGISTRY.register(collector);
    }
    metrics
});

pub(crate) fn metrics() -> &'static ReporterMetrics {
    &METRICS
}

/// Creates every series of an enabled reporter at zero. A labelled series only shows up in
/// `/metrics` once touched; without this, a service would expose none of them until its first
/// window closed or something was dropped.
pub(crate) fn register_producer(producer: &str) {
    let metrics = metrics();
    metrics.published.with_label_values(&[producer]);
    metrics.publish_failures.with_label_values(&[producer]);
    metrics.backlog.with_label_values(&[producer]);
    for reason in RecordDrop::ALL {
        metrics
            .records_dropped
            .with_label_values(&[producer, reason.as_str()]);
    }
    for reason in EventDrop::ALL {
        metrics
            .events_dropped
            .with_label_values(&[producer, reason.as_str()]);
    }
}

impl ReporterMetrics {
    fn build() -> Self {
        let counter = |name: &str, help: &str, labels: &[&str]| {
            IntCounterVec::new(Opts::new(name, help), labels).expect("valid usage metric")
        };
        Self {
            published: counter(
                "usage_events_published_total",
                "Usage window events acknowledged by the usage stream",
                &["producer"],
            ),
            publish_failures: counter(
                "usage_publish_failures_total",
                "Usage window event publishes that failed after the producer's retries",
                &["producer"],
            ),
            records_dropped: counter(
                "usage_records_dropped_total",
                "Usage records not counted into any window event",
                &["producer", "reason"],
            ),
            events_dropped: counter(
                "usage_events_dropped_total",
                "Usage window events dropped before they were published",
                &["producer", "reason"],
            ),
            backlog: IntGaugeVec::new(
                Opts::new(
                    "usage_backlog_events",
                    "Usage window events waiting to be published",
                ),
                &["producer"],
            )
            .expect("valid usage metric"),
        }
    }
}
