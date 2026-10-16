//! Window flusher: closes each wall-clock window, encodes the tenant events into a bounded
//! backlog and publishes the backlog to the usage stream with at-least-once semantics.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use flare_server_core::context::{Context, Ctx};
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use flare_server_core::mq::nats::NatsProducer;
use flare_server_core::mq::nats::config::{NatsProducerConfig, NatsStreamSpec};
use flare_server_core::mq::producer::{Producer, ProducerError};
use futures::StreamExt;
use parking_lot::Mutex;

use crate::event::{Origin, OutgoingEvent, now_ms, tenant_events};
use crate::metrics::{EventDrop, metrics};
use crate::recorder::{Shared, UsageRecorder, WindowState};
use crate::{NATS_URL_ENV, STREAM, SUBJECT_PREFIX, WINDOW_SECS_ENV};

const DEFAULT_WINDOW_SECS: u64 = 60;
const MIN_WINDOW_SECS: u64 = 10;
const MAX_WINDOW_SECS: u64 = 3_600;
/// Concurrent publishes while draining the backlog.
const PUBLISH_CONCURRENCY: usize = 16;
/// Time the shutdown flush may take; the service runtime stops waiting after 5 seconds.
const SHUTDOWN_FLUSH_TIMEOUT: Duration = Duration::from_secs(3);

/// Memory bounds of one reporter. Whatever does not fit is dropped and counted, never queued
/// without limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UsageLimits {
    /// Distinct `(tenant, meter, dims)` counter series per window.
    pub max_counter_series: usize,
    /// Active-user hashes remembered for the day, across tenants (8 bytes each plus set
    /// overhead).
    pub max_tracked_active_users: usize,
    /// Active-user hashes waiting for the next window event, across tenants.
    pub max_pending_active_users: usize,
    /// Encoded events waiting to be published.
    pub max_backlog_events: usize,
    /// Encoded bytes waiting to be published.
    pub max_backlog_bytes: usize,
}

impl Default for UsageLimits {
    fn default() -> Self {
        Self {
            max_counter_series: 50_000,
            max_tracked_active_users: 500_000,
            max_pending_active_users: 200_000,
            max_backlog_events: 20_000,
            max_backlog_bytes: 32 * 1024 * 1024,
        }
    }
}

/// The usage reporter of one service process: hands out [`UsageRecorder`]s and runs the
/// window flusher until shutdown.
pub struct UsageReporter {
    recorder: UsageRecorder,
    publisher: Option<Publisher>,
}

impl std::fmt::Debug for UsageReporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UsageReporter")
            .field("enabled", &self.is_enabled())
            .finish()
    }
}

struct Publisher {
    shared: Arc<Shared>,
    sink: Arc<dyn Producer>,
    instance: String,
    window: Duration,
    /// Shared so a publish round works on a snapshot (pointer copies) while the backlog stays
    /// untouched until the round is over.
    backlog: VecDeque<Arc<OutgoingEvent>>,
    backlog_bytes: usize,
    last_window_start_ms: Option<i64>,
}

/// Producer settings of the usage stream. The stream spec must stay identical to the Social
/// snapshot publisher's and the platform consumer's (`flare_usage`, `flare.usage.v1.>`,
/// 90 days, 10 minute duplicate window): every producer declares it on connect, and a
/// different spec would flip the stream configuration on each restart.
struct UsageNats {
    url: String,
}

impl NatsProducerConfig for UsageNats {
    fn nats_url(&self) -> &str {
        &self.url
    }
    fn timeout_ms(&self) -> u64 {
        2_000
    }
    fn retries(&self) -> u32 {
        1
    }
    fn retry_backoff_ms(&self) -> u64 {
        100
    }
    fn stream_specs(&self) -> Vec<NatsStreamSpec> {
        vec![usage_stream_spec()]
    }
}

fn usage_stream_spec() -> NatsStreamSpec {
    NatsStreamSpec::new(STREAM, vec![format!("{SUBJECT_PREFIX}.>")])
        .with_max_age(Duration::from_secs(90 * 24 * 3600))
        .with_duplicate_window(Duration::from_secs(10 * 60))
}

impl UsageReporter {
    /// Connects when [`NATS_URL_ENV`] is set; otherwise a disabled reporter with one warning.
    /// [`WINDOW_SECS_ENV`] sets the window (10..=3600 seconds, default 60); it is only read
    /// when reporting is enabled.
    pub async fn from_env(producer: &'static str) -> Result<Self> {
        match std::env::var(NATS_URL_ENV) {
            Ok(url) if !url.trim().is_empty() => {
                let window = window_from_env(std::env::var(WINDOW_SECS_ENV).ok().as_deref())?;
                Self::connect(producer, url.trim(), window).await
            }
            Ok(_) | Err(std::env::VarError::NotPresent) => {
                tracing::warn!(producer, "{NATS_URL_ENV} not set: usage is not reported");
                Ok(Self::disabled())
            }
            Err(std::env::VarError::NotUnicode(_)) => {
                Err(config_error(format!("{NATS_URL_ENV} is not valid UTF-8")))
            }
        }
    }

    /// Connects to the usage stream (declaring it when missing); unreachable is an error.
    pub async fn connect(producer: &'static str, url: &str, window: Duration) -> Result<Self> {
        let sink = NatsProducer::new(&UsageNats {
            url: url.to_owned(),
        })
        .await
        .map_err(|_| config_error("usage stream unavailable".to_string()))?;
        tracing::info!(
            producer,
            window_secs = window.as_secs(),
            "usage reporting enabled"
        );
        Ok(Self::with_producer(
            producer,
            Arc::new(sink),
            window,
            UsageLimits::default(),
        ))
    }

    /// Reporter over any [`Producer`] (tests, embedding).
    pub fn with_producer(
        producer: &'static str,
        sink: Arc<dyn Producer>,
        window: Duration,
        limits: UsageLimits,
    ) -> Self {
        let shared = Arc::new(Shared {
            producer,
            limits,
            state: Mutex::new(WindowState::default()),
        });
        // Every series at zero from the start, so alerts and dashboards see them before the
        // first window closes.
        crate::metrics::register_producer(producer);
        Self {
            recorder: UsageRecorder::enabled(shared.clone()),
            publisher: Some(Publisher {
                shared,
                sink,
                instance: format!("{producer}-{}", ulid::Ulid::new()),
                window: window.max(Duration::from_secs(1)),
                backlog: VecDeque::new(),
                backlog_bytes: 0,
                last_window_start_ms: None,
            }),
        }
    }

    pub fn disabled() -> Self {
        Self {
            recorder: UsageRecorder::disabled(),
            publisher: None,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.publisher.is_some()
    }

    pub fn recorder(&self) -> UsageRecorder {
        self.recorder.clone()
    }

    /// Flushes every window at its wall-clock boundary until `shutdown` resolves, then flushes
    /// the current partial window (bounded by [`SHUTDOWN_FLUSH_TIMEOUT`]). A publish round in
    /// flight when shutdown arrives is abandoned, not awaited: its events are still queued and
    /// the shutdown flush sends them again under the same ids. A disabled reporter just waits.
    pub async fn run<F>(
        self,
        shutdown: F,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        F: Future,
    {
        let Some(mut publisher) = self.publisher else {
            shutdown.await;
            return Ok(());
        };
        tokio::pin!(shutdown);
        loop {
            let now = now_ms();
            let boundary = publisher.next_boundary(now);
            let wait = Duration::from_millis(u64::try_from(boundary - now).unwrap_or(0));
            tokio::select! {
                _ = &mut shutdown => break,
                _ = tokio::time::sleep(wait) => {}
            }
            // The sleep runs on the monotonic clock, the boundary is wall-clock time. Woken
            // before the wall clock reached it (drift, a step back), the window is not over:
            // keep waiting rather than close it early under a label that is not its own.
            if now_ms() < boundary {
                continue;
            }
            publisher.close_window(boundary - publisher.window_ms());
            let published = AtomicUsize::new(0);
            tokio::select! {
                _ = &mut shutdown => break,
                _ = publisher.publish_backlog(&published) => {}
            }
        }
        publisher.shutdown_flush().await;
        Ok(())
    }
}

impl Publisher {
    fn window_ms(&self) -> i64 {
        i64::try_from(self.window.as_millis()).unwrap_or(i64::MAX)
    }

    /// End of the next window to close: the wall-clock window in progress, but never the last
    /// closed one again. After the wall clock stepped back, the next window simply lasts until
    /// the clock is past its end, so labels stay aligned to the wall clock.
    fn next_boundary(&self, now_ms: i64) -> i64 {
        let window_ms = self.window_ms();
        let current_end = (now_ms.div_euclid(window_ms) + 1) * window_ms;
        match self.last_window_start_ms {
            Some(last) => current_end.max(last.saturating_add(2 * window_ms)),
            None => current_end,
        }
    }

    fn producer(&self) -> &'static str {
        self.shared.producer
    }

    /// Takes the window out of the recorder and queues its events. Labels never repeat inside
    /// a process, even when the wall clock steps back: a repeated label would be dropped by
    /// the stream as a duplicate.
    fn close_window(&mut self, window_start_ms: i64) {
        let window_start_ms = match self.last_window_start_ms {
            Some(last) if window_start_ms <= last => last + self.window_ms(),
            _ => window_start_ms,
        };
        self.last_window_start_ms = Some(window_start_ms);
        let tenants = self.shared.state.lock().take_window(Instant::now());
        let origin = Origin {
            producer: self.producer(),
            instance: &self.instance,
            window_start_ms,
            window_secs: self.window.as_secs(),
        };
        let mut events = Vec::new();
        for (app, window) in tenants {
            events.extend(tenant_events(&origin, &app, window));
        }
        for event in events {
            match event.encode() {
                Ok(encoded) => self.enqueue(encoded),
                Err(error) => {
                    tracing::error!(error = %error, app = %event.app, "usage event not encodable");
                    self.drop_events(EventDrop::Encode, 1);
                }
            }
        }
        self.update_backlog_gauge();
    }

    fn enqueue(&mut self, event: OutgoingEvent) {
        let limits = self.shared.limits;
        self.backlog_bytes += event.payload.len();
        self.backlog.push_back(Arc::new(event));
        let mut dropped = 0;
        while self.backlog.len() > limits.max_backlog_events
            || self.backlog_bytes > limits.max_backlog_bytes
        {
            let Some(oldest) = self.backlog.pop_front() else {
                break;
            };
            self.backlog_bytes -= oldest.payload.len();
            dropped += 1;
        }
        if dropped > 0 {
            tracing::warn!(
                producer = self.producer(),
                dropped,
                "usage backlog full: oldest unpublished windows dropped"
            );
            self.drop_events(EventDrop::BacklogFull, dropped);
        }
    }

    /// Publishes the backlog; acknowledged events leave it, the others stay where they were,
    /// oldest first, for the next window. After the first failure no new publish starts in this
    /// round (the stream is likely down; each attempt would wait out its timeout).
    ///
    /// Cancel-safe: the backlog only changes once the round is over, so a cancelled round
    /// leaves every event queued. Some of them may already be in the stream; the retry carries
    /// the same id and the stream drops it.
    async fn publish_backlog(&mut self, published: &AtomicUsize) {
        if self.backlog.is_empty() {
            return;
        }
        let producer = self.producer();
        let sink = self.sink.clone();
        let broken = AtomicBool::new(false);
        let round: Vec<Arc<OutgoingEvent>> = self.backlog.iter().cloned().collect();
        let acknowledged: Vec<bool> = futures::stream::iter(round)
            .map(|event| {
                let sink = sink.clone();
                let broken = &broken;
                async move {
                    if broken.load(Ordering::Relaxed) {
                        return false;
                    }
                    match send(sink.as_ref(), &event).await {
                        Ok(()) => {
                            published.fetch_add(1, Ordering::Relaxed);
                            metrics().published.with_label_values(&[producer]).inc();
                            true
                        }
                        Err(_) => {
                            metrics()
                                .publish_failures
                                .with_label_values(&[producer])
                                .inc();
                            if !broken.swap(true, Ordering::Relaxed) {
                                tracing::warn!(
                                    producer,
                                    subject = %event.subject,
                                    "usage event publish failed; unpublished windows stay queued"
                                );
                            }
                            false
                        }
                    }
                }
            })
            // In backlog order, so each answer lines up with its event below.
            .buffered(PUBLISH_CONCURRENCY)
            .collect()
            .await;
        let mut acknowledged = acknowledged.into_iter();
        let mut freed = 0;
        self.backlog.retain(|event| {
            let done = acknowledged.next().unwrap_or(false);
            if done {
                freed += event.payload.len();
            }
            !done
        });
        self.backlog_bytes -= freed;
        self.update_backlog_gauge();
    }

    /// Final flush on shutdown: the current partial window plus anything still queued, within
    /// [`SHUTDOWN_FLUSH_TIMEOUT`]; what is left then is counted as lost.
    async fn shutdown_flush(&mut self) {
        let window_ms = self.window_ms();
        self.close_window(now_ms().div_euclid(window_ms) * window_ms);
        let queued = self.backlog.len();
        let published = AtomicUsize::new(0);
        let finished =
            tokio::time::timeout(SHUTDOWN_FLUSH_TIMEOUT, self.publish_backlog(&published))
                .await
                .is_ok();
        let published = published.load(Ordering::Relaxed);
        let lost = queued.saturating_sub(published);
        if lost > 0 {
            tracing::warn!(
                producer = self.producer(),
                lost,
                finished,
                "usage windows not published before shutdown"
            );
            self.drop_events(EventDrop::Shutdown, lost);
        } else {
            tracing::info!(
                producer = self.producer(),
                published,
                "usage flushed on shutdown"
            );
        }
        self.backlog.clear();
        self.backlog_bytes = 0;
        self.update_backlog_gauge();
    }

    fn drop_events(&self, reason: EventDrop, count: usize) {
        metrics()
            .events_dropped
            .with_label_values(&[self.producer(), reason.as_str()])
            .inc_by(u64::try_from(count).unwrap_or(u64::MAX));
    }

    fn update_backlog_gauge(&self) {
        metrics()
            .backlog
            .with_label_values(&[self.producer()])
            .set(i64::try_from(self.backlog.len()).unwrap_or(i64::MAX));
    }
}

/// JetStream's deduplication header.
const NATS_MESSAGE_ID: &str = "Nats-Msg-Id";

/// One publish. The message id deduplicates a retry inside the stream's duplicate window;
/// the consumer upserts by the same key after it.
///
/// The id goes out as `Nats-Msg-Id` explicitly: `NatsProducer::send` sets it from
/// `x-message-id` and then replaces the whole header map with the caller's headers, which
/// drops it, so without this header the stream never deduplicates.
async fn send(
    sink: &dyn Producer,
    event: &OutgoingEvent,
) -> std::result::Result<(), ProducerError> {
    let ctx: Ctx = Arc::new(Context::with_request_id(&event.message_id).with_tenant_id(&event.app));
    let headers = HashMap::from([
        ("x-message-id".to_owned(), event.message_id.clone()),
        (NATS_MESSAGE_ID.to_owned(), event.message_id.clone()),
    ]);
    sink.send(
        &ctx,
        &event.subject,
        Some(&event.app),
        event.payload.clone(),
        Some(headers),
    )
    .await
}

fn window_from_env(raw: Option<&str>) -> Result<Duration> {
    let secs = match raw.map(str::trim).filter(|raw| !raw.is_empty()) {
        None => DEFAULT_WINDOW_SECS,
        Some(raw) => raw
            .parse::<u64>()
            .ok()
            .filter(|secs| (MIN_WINDOW_SECS..=MAX_WINDOW_SECS).contains(secs))
            .ok_or_else(|| {
                config_error(format!(
                    "{WINDOW_SECS_ENV} must be {MIN_WINDOW_SECS}..={MAX_WINDOW_SECS} seconds"
                ))
            })?,
    };
    Ok(Duration::from_secs(secs))
}

fn config_error(message: String) -> flare_server_core::error::FlareError {
    ErrorBuilder::new(ErrorCode::ConfigurationError, message).build_error()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU32;

    use flare_server_core::mq::producer::ProducerMessage;

    use super::*;
    use crate::event::UsageEvent;
    use crate::meter::PushOutcome;

    /// Records every publish; fails the first `fail_first` of them.
    #[derive(Default)]
    struct RecordingProducer {
        fail_first: AtomicU32,
        attempts: AtomicU32,
        sent: std::sync::Mutex<Vec<(String, String, UsageEvent)>>,
    }

    impl RecordingProducer {
        fn failing(times: u32) -> Self {
            Self {
                fail_first: AtomicU32::new(times),
                ..Self::default()
            }
        }

        fn sent(&self) -> Vec<(String, String, UsageEvent)> {
            self.sent.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl Producer for RecordingProducer {
        async fn send(
            &self,
            _ctx: &Ctx,
            topic: &str,
            key: Option<&str>,
            payload: Vec<u8>,
            headers: Option<HashMap<String, String>>,
        ) -> std::result::Result<(), ProducerError> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            if self
                .fail_first
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    left.checked_sub(1)
                })
                .is_ok()
            {
                return Err(ProducerError::Send("stream down".into()));
            }
            let event: UsageEvent = serde_json::from_slice(&payload).unwrap();
            assert_eq!(key, Some(event.app.as_str()));
            let headers = headers.unwrap();
            let id = headers["x-message-id"].clone();
            assert_eq!(headers["Nats-Msg-Id"], id);
            self.sent
                .lock()
                .unwrap()
                .push((topic.to_owned(), id, event));
            Ok(())
        }

        async fn send_batch(
            &self,
            _ctx: &Ctx,
            _messages: Vec<ProducerMessage>,
        ) -> std::result::Result<(), ProducerError> {
            unreachable!("the reporter publishes one event at a time")
        }

        fn name(&self) -> &str {
            "recording"
        }
    }

    fn reporter(
        producer: &'static str,
        sink: Arc<RecordingProducer>,
        limits: UsageLimits,
    ) -> UsageReporter {
        UsageReporter::with_producer(producer, sink, Duration::from_secs(60), limits)
    }

    fn counter(name: &str, producer: &str, reason: Option<&str>) -> u64 {
        let m = metrics();
        match (name, reason) {
            ("published", _) => m.published.with_label_values(&[producer]).get(),
            ("failures", _) => m.publish_failures.with_label_values(&[producer]).get(),
            ("events_dropped", Some(reason)) => m
                .events_dropped
                .with_label_values(&[producer, reason])
                .get(),
            ("records_dropped", Some(reason)) => m
                .records_dropped
                .with_label_values(&[producer, reason])
                .get(),
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn a_window_publishes_one_event_per_tenant_with_stable_ids() {
        let sink = Arc::new(RecordingProducer::default());
        let mut reporter = reporter("usage-test-window", sink.clone(), UsageLimits::default());
        let recorder = reporter.recorder();
        recorder.message_accepted("tenant-a", "normal");
        recorder.message_accepted("tenant-a", "normal");
        recorder.push_attempt("tenant-a", "apns", PushOutcome::Completed);
        recorder.media_stored("tenant-b", 1024);
        recorder.connection_opened("tenant-b");
        recorder.user_active("tenant-b", "user-1");
        recorder.user_active("tenant-b", "user-1");
        // Invalid tenants never reach a window.
        recorder.message_accepted("", "normal");
        recorder.message_accepted("bad.tenant", "normal");

        let publisher = reporter.publisher.as_mut().unwrap();
        publisher.close_window(1_760_140_800_000);
        publisher.publish_backlog(&AtomicUsize::new(0)).await;

        let mut sent = sink.sent();
        sent.sort_by(|a, b| a.2.app.cmp(&b.2.app));
        assert_eq!(sent.len(), 2);
        let (subject, id, event) = &sent[0];
        assert_eq!(subject, "flare.usage.v1.tenant-a");
        assert_eq!(id, &event.message_id());
        assert_eq!(
            id,
            &format!("{}:tenant-a:1760140800000:0", publisher.instance)
        );
        assert!(publisher.instance.starts_with("usage-test-window-"));
        let meters: Vec<_> = event
            .meters
            .iter()
            .map(|m| {
                (
                    m.meter.as_str(),
                    m.dims.values().cloned().collect::<Vec<_>>(),
                    m.value,
                )
            })
            .collect();
        assert_eq!(
            meters,
            [
                ("messages.accepted", vec!["normal".to_owned()], 2),
                (
                    "push.attempts",
                    vec!["apns".to_owned(), "completed".to_owned()],
                    1
                ),
            ]
        );
        let (_, _, event) = &sent[1];
        assert_eq!(event.app, "tenant-b");
        let names: Vec<_> = event.meters.iter().map(|m| m.meter.as_str()).collect();
        assert_eq!(names, ["media.bytes_stored", "connections.peak"]);
        let users = event.active_users.as_ref().unwrap();
        assert_eq!(
            users.user_hashes,
            [format!("{:016x}", crate::user_hash("tenant-b", "user-1"))]
        );
        assert_eq!(
            counter(
                "records_dropped",
                "usage-test-window",
                Some("invalid_tenant")
            ),
            2
        );
        assert_eq!(counter("published", "usage-test-window", None), 2);
        assert!(publisher.backlog.is_empty());

        // The next window only carries the still-open connection.
        publisher.close_window(1_760_140_860_000);
        publisher.publish_backlog(&AtomicUsize::new(0)).await;
        let sent = sink.sent();
        assert_eq!(sent.len(), 3);
        assert_eq!(sent[2].2.app, "tenant-b");
        assert_eq!(sent[2].2.meters.len(), 1);
        assert_eq!(sent[2].2.meters[0].meter, "connections.peak");
    }

    #[tokio::test]
    async fn unpublished_events_are_retried_with_the_same_ids() {
        let sink = Arc::new(RecordingProducer::failing(1));
        let mut reporter = reporter("usage-test-retry", sink.clone(), UsageLimits::default());
        let recorder = reporter.recorder();
        for tenant in ["t1", "t2", "t3"] {
            recorder.api_call(tenant, "messages");
        }
        let publisher = reporter.publisher.as_mut().unwrap();
        publisher.close_window(1_760_140_800_000);
        let queued_ids: Vec<_> = publisher
            .backlog
            .iter()
            .map(|event| event.message_id.clone())
            .collect();
        publisher.publish_backlog(&AtomicUsize::new(0)).await;
        // The first failure stops the round: nothing else was attempted.
        assert_eq!(sink.attempts.load(Ordering::SeqCst), 1);
        assert!(sink.sent().is_empty());
        assert_eq!(publisher.backlog.len(), 3);
        assert_eq!(counter("failures", "usage-test-retry", None), 1);

        // The next window publishes the backlog unchanged, plus its own (empty) window.
        publisher.close_window(1_760_140_860_000);
        publisher.publish_backlog(&AtomicUsize::new(0)).await;
        let mut sent_ids: Vec<_> = sink.sent().into_iter().map(|(_, id, _)| id).collect();
        sent_ids.sort();
        let mut queued_ids = queued_ids;
        queued_ids.sort();
        assert_eq!(sent_ids, queued_ids);
        assert!(publisher.backlog.is_empty());
    }

    #[tokio::test]
    async fn the_backlog_is_bounded_and_drops_the_oldest_events() {
        let sink = Arc::new(RecordingProducer::failing(u32::MAX));
        let limits = UsageLimits {
            max_backlog_events: 2,
            ..UsageLimits::default()
        };
        let mut reporter = reporter("usage-test-backlog", sink.clone(), limits);
        let recorder = reporter.recorder();
        let publisher = reporter.publisher.as_mut().unwrap();
        for (window, tenant) in [(0, "t1"), (1, "t2"), (2, "t3")] {
            recorder.messages_persisted(tenant, 1);
            publisher.close_window(1_760_140_800_000 + window * 60_000);
        }
        let apps: Vec<_> = publisher.backlog.iter().map(|e| e.app.as_str()).collect();
        assert_eq!(apps, ["t2", "t3"]);
        assert_eq!(
            counter("events_dropped", "usage-test-backlog", Some("backlog_full")),
            1
        );
        publisher.publish_backlog(&AtomicUsize::new(0)).await;
        // A failed round keeps the order, so the next overflow still drops the oldest.
        let apps: Vec<_> = publisher.backlog.iter().map(|e| e.app.as_str()).collect();
        assert_eq!(apps, ["t2", "t3"]);
        recorder.messages_persisted("t4", 1);
        publisher.close_window(1_760_140_980_000);
        let apps: Vec<_> = publisher.backlog.iter().map(|e| e.app.as_str()).collect();
        assert_eq!(apps, ["t3", "t4"]);
        assert_eq!(
            counter("events_dropped", "usage-test-backlog", Some("backlog_full")),
            2
        );
        let bytes: usize = publisher.backlog.iter().map(|e| e.payload.len()).sum();
        assert_eq!(publisher.backlog_bytes, bytes);
    }

    /// Accepts the first `succeed_first` publishes, then never answers.
    #[derive(Default)]
    struct StalledProducer {
        succeed_first: u32,
        attempts: AtomicU32,
    }

    #[async_trait::async_trait]
    impl Producer for StalledProducer {
        async fn send(
            &self,
            _ctx: &Ctx,
            _topic: &str,
            _key: Option<&str>,
            _payload: Vec<u8>,
            _headers: Option<HashMap<String, String>>,
        ) -> std::result::Result<(), ProducerError> {
            if self.attempts.fetch_add(1, Ordering::SeqCst) < self.succeed_first {
                return Ok(());
            }
            std::future::pending().await
        }

        async fn send_batch(
            &self,
            _ctx: &Ctx,
            _messages: Vec<ProducerMessage>,
        ) -> std::result::Result<(), ProducerError> {
            unreachable!("the reporter publishes one event at a time")
        }

        fn name(&self) -> &str {
            "stalled"
        }
    }

    #[tokio::test]
    async fn a_cancelled_round_leaves_every_event_queued() {
        let sink = Arc::new(StalledProducer {
            succeed_first: 1,
            ..StalledProducer::default()
        });
        let mut reporter = UsageReporter::with_producer(
            "usage-test-cancel",
            sink.clone(),
            Duration::from_secs(60),
            UsageLimits::default(),
        );
        let recorder = reporter.recorder();
        for tenant in ["t1", "t2", "t3"] {
            recorder.api_call(tenant, "messages");
        }
        let publisher = reporter.publisher.as_mut().unwrap();
        publisher.close_window(1_760_140_800_000);
        let ids: Vec<_> = publisher
            .backlog
            .iter()
            .map(|e| e.message_id.clone())
            .collect();
        let bytes = publisher.backlog_bytes;

        let published = AtomicUsize::new(0);
        let round = publisher.publish_backlog(&published);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), round)
                .await
                .is_err()
        );
        assert_eq!(sink.attempts.load(Ordering::SeqCst), 3);
        assert_eq!(published.load(Ordering::Relaxed), 1);
        // Including the one already published: its retry carries the same id.
        let queued: Vec<_> = publisher
            .backlog
            .iter()
            .map(|e| e.message_id.clone())
            .collect();
        assert_eq!(queued, ids);
        assert_eq!(publisher.backlog_bytes, bytes);
    }

    /// Shutdown does not wait for a round stuck on the stream: the flush runs at once and is
    /// bounded, and what it could not publish is counted as lost.
    #[tokio::test]
    async fn shutdown_does_not_wait_for_a_round_in_flight() {
        let sink = Arc::new(StalledProducer::default());
        let reporter = UsageReporter::with_producer(
            "usage-test-stalled",
            sink.clone(),
            Duration::from_secs(1),
            UsageLimits::default(),
        );
        reporter.recorder().messages_persisted("t1", 1);
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(reporter.run(stopped));
        let started = Instant::now();
        while sink.attempts.load(Ordering::SeqCst) == 0 {
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "no round started"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let stopping = Instant::now();
        stop.send(()).unwrap();
        tokio::time::timeout(SHUTDOWN_FLUSH_TIMEOUT + Duration::from_secs(1), task)
            .await
            .expect("shutdown waited for the stalled round")
            .unwrap()
            .unwrap();
        assert!(stopping.elapsed() < SHUTDOWN_FLUSH_TIMEOUT + Duration::from_secs(1));
        assert_eq!(
            counter("events_dropped", "usage-test-stalled", Some("shutdown")),
            1
        );
    }

    #[test]
    fn a_window_closes_at_its_wall_clock_end_and_never_twice() {
        let sink = Arc::new(RecordingProducer::default());
        let mut reporter = reporter("usage-test-boundary", sink, UsageLimits::default());
        let publisher = reporter.publisher.as_mut().unwrap();
        assert_eq!(
            publisher.next_boundary(1_760_140_830_000),
            1_760_140_860_000
        );
        assert_eq!(
            publisher.next_boundary(1_760_140_860_000),
            1_760_140_920_000
        );

        publisher.close_window(1_760_140_800_000);
        assert_eq!(
            publisher.next_boundary(1_760_140_860_001),
            1_760_140_920_000
        );
        // The wall clock stepped back into (or before) the closed window: the next window is
        // still the one after it, and it closes once the clock is past its end.
        assert_eq!(
            publisher.next_boundary(1_760_140_850_000),
            1_760_140_920_000
        );
        assert_eq!(
            publisher.next_boundary(1_760_140_700_000),
            1_760_140_920_000
        );
    }

    #[test]
    fn an_enabled_reporter_exposes_its_metrics_before_the_first_window() {
        let _reporter = reporter(
            "usage-test-registered",
            Arc::new(RecordingProducer::default()),
            UsageLimits::default(),
        );
        let mut exposition = Vec::new();
        prometheus::Encoder::encode(
            &prometheus::TextEncoder::new(),
            &flare_im_service_kit::metrics::REGISTRY.gather(),
            &mut exposition,
        )
        .unwrap();
        let exposition = String::from_utf8(exposition).unwrap();
        let series = |name: &str| {
            exposition
                .lines()
                .filter(|line| {
                    line.starts_with(&format!("{name}{{"))
                        && line.contains("producer=\"usage-test-registered\"")
                })
                .count()
        };
        assert_eq!(series("usage_events_published_total"), 1);
        assert_eq!(series("usage_publish_failures_total"), 1);
        assert_eq!(series("usage_backlog_events"), 1);
        assert_eq!(series("usage_records_dropped_total"), 3);
        assert_eq!(series("usage_events_dropped_total"), 3);
    }

    #[tokio::test]
    async fn counter_series_beyond_the_limit_are_dropped_and_counted() {
        let sink = Arc::new(RecordingProducer::default());
        let limits = UsageLimits {
            max_counter_series: 1,
            ..UsageLimits::default()
        };
        let reporter = reporter("usage-test-series", sink, limits);
        let recorder = reporter.recorder();
        recorder.api_call("t1", "messages");
        recorder.api_call("t1", "media");
        recorder.api_call("t2", "messages");
        assert_eq!(
            counter(
                "records_dropped",
                "usage-test-series",
                Some("counter_limit")
            ),
            2
        );
    }

    #[tokio::test]
    async fn window_labels_never_repeat_within_a_process() {
        let sink = Arc::new(RecordingProducer::default());
        let mut reporter = reporter("usage-test-labels", sink.clone(), UsageLimits::default());
        let recorder = reporter.recorder();
        let publisher = reporter.publisher.as_mut().unwrap();
        recorder.messages_persisted("t1", 1);
        publisher.close_window(1_760_140_800_000);
        recorder.messages_persisted("t1", 1);
        // The wall clock stepped back a window.
        publisher.close_window(1_760_140_740_000);
        publisher.publish_backlog(&AtomicUsize::new(0)).await;
        let mut starts: Vec<_> = sink
            .sent()
            .into_iter()
            .map(|(_, _, event)| event.window_start_ms)
            .collect();
        starts.sort_unstable();
        assert_eq!(starts, [1_760_140_800_000, 1_760_140_860_000]);
    }

    #[tokio::test]
    async fn shutdown_flushes_the_partial_window() {
        let sink = Arc::new(RecordingProducer::default());
        let reporter = reporter("usage-test-shutdown", sink.clone(), UsageLimits::default());
        let recorder = reporter.recorder();
        recorder.message_accepted("tenant-a", "normal");
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(reporter.run(stopped));
        recorder.message_accepted("tenant-a", "normal");
        stop.send(()).unwrap();
        task.await.unwrap().unwrap();
        // Usually one event; two when the test straddles a minute boundary.
        let sent = sink.sent();
        assert!(!sent.is_empty());
        let total: u64 = sent.iter().map(|(_, _, event)| event.meters[0].value).sum();
        assert_eq!(total, 2);
        for (_, _, event) in &sent {
            assert_eq!(event.window_start_ms % 60_000, 0);
            assert!(event.window_start_ms <= now_ms());
        }
        // Recording after shutdown is harmless.
        recorder.message_accepted("tenant-a", "normal");
    }

    #[tokio::test]
    async fn shutdown_with_the_stream_down_counts_the_lost_windows() {
        let sink = Arc::new(RecordingProducer::failing(u32::MAX));
        let reporter = reporter("usage-test-lost", sink, UsageLimits::default());
        reporter.recorder().messages_persisted("t1", 3);
        reporter.recorder().messages_persisted("t2", 3);
        reporter.run(async {}).await.unwrap();
        assert_eq!(
            counter("events_dropped", "usage-test-lost", Some("shutdown")),
            2
        );
    }

    #[tokio::test]
    async fn a_disabled_reporter_waits_for_shutdown_and_reports_nothing() {
        let reporter = UsageReporter::disabled();
        assert!(!reporter.is_enabled());
        reporter.recorder().message_accepted("tenant-a", "normal");
        reporter.run(async {}).await.unwrap();
    }

    #[test]
    fn the_stream_spec_matches_the_other_usage_producers() {
        let spec = usage_stream_spec();
        assert_eq!(spec.name, "flare_usage");
        assert_eq!(spec.subjects, ["flare.usage.v1.>"]);
        assert_eq!(spec.max_age, Duration::from_secs(90 * 24 * 3600));
        assert_eq!(spec.duplicate_window, Duration::from_secs(600));
        assert_eq!(
            spec,
            NatsStreamSpec::new("flare_usage", vec!["flare.usage.v1.>".into()])
                .with_max_age(Duration::from_secs(90 * 24 * 3600))
                .with_duplicate_window(Duration::from_secs(600))
        );
    }

    #[test]
    fn the_window_comes_from_the_environment_within_bounds() {
        assert_eq!(window_from_env(None).unwrap(), Duration::from_secs(60));
        assert_eq!(window_from_env(Some(" ")).unwrap(), Duration::from_secs(60));
        assert_eq!(
            window_from_env(Some("10")).unwrap(),
            Duration::from_secs(10)
        );
        assert_eq!(
            window_from_env(Some("3600")).unwrap(),
            Duration::from_secs(3600)
        );
        for invalid in ["9", "3601", "abc", "-1"] {
            assert!(window_from_env(Some(invalid)).is_err(), "{invalid}");
        }
    }
}
