//! The worker's entries in the push decision chain: the `vendor` stage.
//!
//! Each entry carries `conv` and, as far as known, `provider`, `outcome`, `latency_ms`,
//! `app_version` and `code`. Writing goes through the shared recorder: it never blocks and
//! never fails. No token, credential or message content ever goes into an entry.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use flare_im_contracts::push_trace::{PushTraceEntry, TraceStage, trace_key};
use flare_im_service_kit::push_trace::PushTraceRecorder;
use flare_proto::PushTaskEnvelope;

use crate::domain::vendor_delivery::{Vendor, app_version_label, trace_code};

/// The outcome values of the `vendor` stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VendorOutcome {
    Accepted,
    Throttled,
    BreakerOpen,
    InvalidToken,
    Retryable,
    Terminal,
    /// The task moved to the next binding of the device; `provider` is the vendor it moved to,
    /// `code` the vendor it left.
    Switched,
    /// Held for a throttled vendor, to go out merged with the device's other held tasks.
    Coalesced,
    /// Dropped because its expiry passed before it could be sent.
    Expired,
}

impl VendorOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Throttled => "throttled",
            Self::BreakerOpen => "breaker_open",
            Self::InvalidToken => "invalid_token",
            Self::Retryable => "retryable",
            Self::Terminal => "terminal",
            Self::Switched => "switched",
            Self::Coalesced => "coalesced",
            Self::Expired => "expired",
        }
    }
}

/// One `vendor` entry about one task on its device.
#[derive(Debug, Clone)]
pub struct VendorEvent<'a> {
    pub vendor: Option<Vendor>,
    pub outcome: VendorOutcome,
    pub latency: Option<Duration>,
    /// The binding's app version, raw; written sanitised.
    pub app_version: Option<&'a str>,
    /// A vendor or local code; written only when it is a short printable code.
    pub code: Option<String>,
}

impl<'a> VendorEvent<'a> {
    pub fn new(vendor: Option<Vendor>, outcome: VendorOutcome) -> Self {
        Self {
            vendor,
            outcome,
            latency: None,
            app_version: None,
            code: None,
        }
    }

    pub fn latency(mut self, latency: Duration) -> Self {
        self.latency = Some(latency);
        self
    }

    pub fn app_version(mut self, app_version: &'a str) -> Self {
        self.app_version = Some(app_version);
        self
    }

    pub fn code(mut self, code: impl Into<String>) -> Self {
        self.code = Some(code.into());
        self
    }
}

/// The worker's writer of `vendor` entries. Cloning shares the recorder.
#[derive(Clone, Debug, Default)]
pub struct VendorTrace {
    recorder: PushTraceRecorder,
}

impl VendorTrace {
    pub fn new(recorder: PushTraceRecorder) -> Self {
        Self { recorder }
    }

    pub fn disabled() -> Self {
        Self::new(PushTraceRecorder::disabled())
    }

    /// Record `event` for `task`, keyed like every other stage writes the same message.
    pub fn record(&self, task: &PushTaskEnvelope, event: VendorEvent<'_>) {
        if !self.recorder.is_enabled() {
            return;
        }
        let mut entry = PushTraceEntry::new(now_ms(), TraceStage::Vendor, task.device_id.as_str())
            .with_conversation(task.conversation_id.as_str())
            .with("outcome", event.outcome.as_str());
        if let Some(vendor) = event.vendor {
            entry = entry.with("provider", vendor.as_str());
        }
        if let Some(latency) = event.latency {
            entry = entry.with("latency_ms", latency.as_millis().to_string());
        }
        if let Some(app_version) = event.app_version {
            entry = entry.with("app_version", app_version_label(app_version));
        }
        if let Some(code) = event.code.as_deref().and_then(trace_code) {
            entry = entry.with("code", code);
        }
        let key = trace_key(
            &task.message_id,
            &task.conversation_id,
            task.conversation_seq,
        );
        self.recorder
            .record(&task.tenant_id, &task.user_id, &key, entry);
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}
