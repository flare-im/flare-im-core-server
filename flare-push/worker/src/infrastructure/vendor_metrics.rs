//! Vendor governance metrics, registered in the service-kit registry the worker exports.
//!
//! Labels are small fixed sets (vendor, failure reason, breaker state) plus the app version on
//! the sent counter, bounded per process (the service kit's `app_version_label`: 64 values, the
//! rest `other`, the same rule as the proxy's receipts counter); never tenant, user, device or
//! conversation.

use std::sync::LazyLock;
use std::time::Duration;

use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts};

use crate::domain::vendor_delivery::{FailureReason, Vendor};
use crate::domain::vendor_policy::BreakerState;

fn register<C: prometheus::core::Collector + Clone + 'static>(collector: C) -> C {
    let _ = flare_im_service_kit::metrics::REGISTRY.register(Box::new(collector.clone()));
    collector
}

static SENT: LazyLock<Option<IntCounterVec>> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "flare_push_vendor_sent_total",
            "Pushes a vendor accepted, by vendor, app version and whether the device reports arrival",
        ),
        &["vendor", "app_version", "receipt_capable"],
    )
    .ok()
    .map(register)
});

static LATENCY: LazyLock<Option<HistogramVec>> = LazyLock::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "flare_push_vendor_latency_seconds",
            "Time from handing a push to the vendor adapter to its answer, by vendor",
        )
        .buckets(vec![0.025, 0.05, 0.1, 0.2, 0.4, 0.8, 1.5, 3.0, 6.0, 12.0]),
        &["vendor"],
    )
    .ok()
    .map(register)
});

static FAILURES: LazyLock<Option<IntCounterVec>> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "flare_push_vendor_failures_total",
            "Push attempts through a vendor that did not end in acceptance, by vendor and reason",
        ),
        &["vendor", "reason"],
    )
    .ok()
    .map(register)
});

static THROTTLED: LazyLock<Option<IntCounterVec>> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "flare_push_vendor_throttled_total",
            "Push attempts refused for throttling (vendor 429 or quota, or the local rate limit), by vendor",
        ),
        &["vendor"],
    )
    .ok()
    .map(register)
});

static BREAKER_STATE: LazyLock<Option<IntGaugeVec>> = LazyLock::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "flare_push_vendor_breaker_state",
            "Vendor circuit breaker state: 0 closed, 1 half-open, 2 open",
        ),
        &["vendor"],
    )
    .ok()
    .map(register)
});

static SWITCHES: LazyLock<Option<IntCounterVec>> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "flare_push_vendor_switch_total",
            "Tasks moved from one binding of the device to the next, by the vendor left and the vendor tried",
        ),
        &["from", "to"],
    )
    .ok()
    .map(register)
});

static COALESCED: LazyLock<Option<IntCounterVec>> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "flare_push_digest_coalesced_total",
            "Tasks held for a throttled vendor and merged into one push per device, by vendor",
        ),
        &["vendor"],
    )
    .ok()
    .map(register)
});

static DIGEST_FLUSHES: LazyLock<Option<IntCounterVec>> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "flare_push_digest_flush_total",
            "Merged pushes the digest coalescer handed back to delivery, by result (sent, waiting, retry, dropped, expired)",
        ),
        &["result"],
    )
    .ok()
    .map(register)
});

/// `app_version` is the binding's raw value; the label is bounded here.
pub fn record_sent(vendor: Vendor, app_version: &str, receipt_capable: bool) {
    if let Some(counter) = SENT.as_ref() {
        counter
            .with_label_values(&[
                vendor.as_str(),
                flare_im_service_kit::metrics::app_version_label(app_version),
                if receipt_capable { "true" } else { "false" },
            ])
            .inc();
    }
}

pub fn record_latency(vendor: Vendor, elapsed: Duration) {
    if let Some(histogram) = LATENCY.as_ref() {
        histogram
            .with_label_values(&[vendor.as_str()])
            .observe(elapsed.as_secs_f64());
    }
}

pub fn record_failure(vendor: Vendor, reason: FailureReason) {
    record_failure_label(vendor, reason.as_str());
}

/// Dead tokens are counted with the failures under their own reason.
pub fn record_invalid_token(vendor: Vendor) {
    record_failure_label(vendor, "invalid_token");
}

fn record_failure_label(vendor: Vendor, reason: &str) {
    if let Some(counter) = FAILURES.as_ref() {
        counter.with_label_values(&[vendor.as_str(), reason]).inc();
    }
}

pub fn record_throttled(vendor: Vendor) {
    if let Some(counter) = THROTTLED.as_ref() {
        counter.with_label_values(&[vendor.as_str()]).inc();
    }
}

pub fn set_breaker_state(vendor: Vendor, state: BreakerState) {
    if let Some(gauge) = BREAKER_STATE.as_ref() {
        gauge
            .with_label_values(&[vendor.as_str()])
            .set(state.gauge());
    }
}

pub fn record_switch(from: Vendor, to: Vendor) {
    if let Some(counter) = SWITCHES.as_ref() {
        counter
            .with_label_values(&[from.as_str(), to.as_str()])
            .inc();
    }
}

pub fn record_coalesced(vendor: Vendor) {
    if let Some(counter) = COALESCED.as_ref() {
        counter.with_label_values(&[vendor.as_str()]).inc();
    }
}

pub fn record_digest_flush(result: &'static str) {
    if let Some(counter) = DIGEST_FLUSHES.as_ref() {
        counter.with_label_values(&[result]).inc();
    }
}

#[cfg(test)]
pub(crate) fn sent_count(vendor: Vendor, app_version: &str, receipt_capable: bool) -> u64 {
    SENT.as_ref().map_or(0, |counter| {
        counter
            .with_label_values(&[
                vendor.as_str(),
                app_version,
                if receipt_capable { "true" } else { "false" },
            ])
            .get()
    })
}

#[cfg(test)]
pub(crate) fn switch_count(from: Vendor, to: Vendor) -> u64 {
    SWITCHES.as_ref().map_or(0, |counter| {
        counter
            .with_label_values(&[from.as_str(), to.as_str()])
            .get()
    })
}

#[cfg(test)]
pub(crate) fn coalesced_count(vendor: Vendor) -> u64 {
    COALESCED.as_ref().map_or(0, |counter| {
        counter.with_label_values(&[vendor.as_str()]).get()
    })
}
