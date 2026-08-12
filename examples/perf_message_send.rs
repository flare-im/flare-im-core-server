//! Message send performance runner.
//!
//! This example sends text messages directly to `MessageSendService` and prints
//! ACK latency statistics. The ACK boundary is broker acceptance; storage
//! durability should be verified through `message_write_ledger` after the run.
//!
//! Environment:
//! - `PERF_ENDPOINT`, default `http://127.0.0.1:50182`
//! - `PERF_TOTAL`, default `1000`
//! - `PERF_CONCURRENCY`, default `32`
//! - `PERF_PAIRS`, default `64`
//! - `PERF_TENANT_ID`, default `0`
//! - `PERF_PAYLOAD_BYTES`, default `64`
//! - `PERF_METRICS_ENABLED`, default `true`
//! - `PERF_METRICS_SETTLE_MS`, default `500`
//! - `PERF_STORAGE_WAIT_TIMEOUT_MS`, default `10000`
//! - `PERF_ORCHESTRATOR_METRICS_ENDPOINT`, default `http://127.0.0.1:19181/metrics`
//! - `PERF_STORAGE_WRITER_METRICS_ENDPOINT`, default `http://127.0.0.1:19182/metrics`

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};

use flare_core::common::conversation::generate_single_chat_conversation_id;
use flare_core::common::protocol::generate_message_id;
use flare_core_base::context::Context;
use flare_grpc_proto::message::SendMessageRequest;
use flare_grpc_proto::message::message_send_service_client::MessageSendServiceClient;
use flare_proto::common::message_content::Content;
use flare_proto::common::{
    ConversationType, Message, MessageContent, MessageSource, MessageStatus, MessageType,
    TextContent,
};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tonic::transport::Channel;

#[derive(Debug, Clone)]
struct Config {
    endpoint: String,
    total: usize,
    concurrency: usize,
    pairs: usize,
    tenant_id: String,
    payload_bytes: usize,
    metrics_enabled: bool,
    metrics_settle_ms: u64,
    storage_wait_timeout_ms: u64,
    orchestrator_metrics_endpoint: String,
    storage_writer_metrics_endpoint: String,
}

#[derive(Debug)]
struct Sample {
    latency: Duration,
}

#[derive(Debug)]
struct Failure {
    message_id: String,
    error: String,
}

#[derive(Debug, Clone, Default)]
struct MetricsSnapshot {
    histograms: HashMap<HistogramKey, HistogramData>,
    counters: HashMap<SeriesKey, f64>,
}

#[derive(Debug, Clone, Default)]
struct HistogramData {
    sum: f64,
    count: f64,
    buckets: BTreeMap<BucketBound, f64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct HistogramKey {
    name: String,
    labels: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SeriesKey {
    name: String,
    labels: Vec<(String, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct BucketBound(f64);

impl Eq for BucketBound {}

impl PartialOrd for BucketBound {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BucketBound {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::from_env();
    if config.total == 0 {
        anyhow::bail!("PERF_TOTAL must be greater than 0");
    }
    if config.concurrency == 0 {
        anyhow::bail!("PERF_CONCURRENCY must be greater than 0");
    }
    if config.pairs == 0 {
        anyhow::bail!("PERF_PAIRS must be greater than 0");
    }

    let channel = Channel::from_shared(config.endpoint.clone())?
        .connect()
        .await?;
    let client = MessageSendServiceClient::new(channel);
    let semaphore = Arc::new(Semaphore::new(config.concurrency));

    let before_orchestrator_metrics = fetch_metrics_if_enabled(
        config.metrics_enabled,
        &config.orchestrator_metrics_endpoint,
        "orchestrator",
    )
    .await;
    let before_storage_metrics = fetch_metrics_if_enabled(
        config.metrics_enabled,
        &config.storage_writer_metrics_endpoint,
        "storage_writer",
    )
    .await;

    let started = Instant::now();
    let mut tasks = JoinSet::new();

    for index in 0..config.total {
        let permit = semaphore.clone().acquire_owned().await?;
        let mut client = client.clone();
        let config = config.clone();
        tasks.spawn(async move {
            let _permit = permit;
            send_one(&mut client, &config, index).await
        });
    }

    let mut samples = Vec::with_capacity(config.total);
    let mut failures = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Ok(sample)) => samples.push(sample),
            Ok(Err(failure)) => failures.push(failure),
            Err(err) => failures.push(Failure {
                message_id: String::new(),
                error: format!("task join failed: {err}"),
            }),
        }
    }
    let elapsed = started.elapsed();

    if config.metrics_enabled && config.metrics_settle_ms > 0 {
        tokio::time::sleep(Duration::from_millis(config.metrics_settle_ms)).await;
    }

    let after_orchestrator_metrics = fetch_metrics_if_enabled(
        config.metrics_enabled,
        &config.orchestrator_metrics_endpoint,
        "orchestrator",
    )
    .await;
    let after_storage_metrics =
        wait_for_storage_metrics(&config, &before_storage_metrics, samples.len()).await;

    print_report(&config, elapsed, &mut samples, &failures);
    print_metrics_delta(
        &before_orchestrator_metrics,
        &after_orchestrator_metrics,
        &before_storage_metrics,
        &after_storage_metrics,
    );
    Ok(())
}

impl Config {
    fn from_env() -> Self {
        Self {
            endpoint: env_string("PERF_ENDPOINT", "http://127.0.0.1:50182"),
            total: env_usize("PERF_TOTAL", 1000),
            concurrency: env_usize("PERF_CONCURRENCY", 32),
            pairs: env_usize("PERF_PAIRS", 64),
            tenant_id: env_string("PERF_TENANT_ID", "0"),
            payload_bytes: env_usize("PERF_PAYLOAD_BYTES", 64),
            metrics_enabled: env_bool("PERF_METRICS_ENABLED", true),
            metrics_settle_ms: env_u64("PERF_METRICS_SETTLE_MS", 500),
            storage_wait_timeout_ms: env_u64("PERF_STORAGE_WAIT_TIMEOUT_MS", 10_000),
            orchestrator_metrics_endpoint: env_string(
                "PERF_ORCHESTRATOR_METRICS_ENDPOINT",
                "http://127.0.0.1:19180/metrics",
            ),
            storage_writer_metrics_endpoint: env_string(
                "PERF_STORAGE_WRITER_METRICS_ENDPOINT",
                "http://127.0.0.1:19182/metrics",
            ),
        }
    }
}

async fn send_one(
    client: &mut MessageSendServiceClient<Channel>,
    config: &Config,
    index: usize,
) -> Result<Sample, Failure> {
    let pair = index % config.pairs;
    let sender_id = format!("perf-user-{pair}-a");
    let recipient_id = format!("perf-user-{pair}-b");
    let conversation_id = generate_single_chat_conversation_id(&sender_id, &recipient_id);
    let message_id = generate_message_id();
    let request_id = format!("perf-req-{message_id}");

    let mut attributes = HashMap::new();
    attributes.insert("recipient_id".to_string(), recipient_id.clone());
    attributes.insert("source".to_string(), "perf".to_string());
    attributes.insert("conversation_type".to_string(), "single".to_string());
    attributes.insert("perf_index".to_string(), index.to_string());

    let message = Message {
        server_id: message_id.clone(),
        conversation_id: conversation_id.clone(),
        client_msg_id: format!("perf-client-{message_id}"),
        sender_id: sender_id.clone(),
        source: MessageSource::User as i32,
        conversation_seq: 0,
        created_at: chrono::Utc::now().timestamp_millis(),
        conversation_type: ConversationType::Single as i32,
        message_type: MessageType::Text as i32,
        channel_id: recipient_id,
        content: Some(MessageContent {
            content: Some(Content::Text(TextContent {
                text: payload_text(config.payload_bytes, index),
                mentions: Vec::new(),
            })),
        }),
        status: MessageStatus::Created as i32,
        attributes,
        ..Default::default()
    };

    let ctx = Context::with_request_id(request_id)
        .with_tenant_id(&config.tenant_id)
        .with_user_id(&sender_id);
    let request = flare_server_core::request_with_context(
        SendMessageRequest {
            conversation_id,
            message: Some(message),
            sync: false,
            svid: "perf".to_string(),
        },
        &ctx,
    );

    let started = Instant::now();
    let response = client
        .send_message(request)
        .await
        .map_err(|status| Failure {
            message_id: message_id.clone(),
            error: status.to_string(),
        })?;
    let latency = started.elapsed();
    let response = response.into_inner();
    if !response.success {
        return Err(Failure {
            message_id,
            error: "success=false".to_string(),
        });
    }
    Ok(Sample { latency })
}

fn payload_text(payload_bytes: usize, index: usize) -> String {
    let prefix = format!("perf-message-{index}-");
    if payload_bytes <= prefix.len() {
        return prefix;
    }
    let fill = "x".repeat(payload_bytes - prefix.len());
    format!("{prefix}{fill}")
}

fn print_report(config: &Config, elapsed: Duration, samples: &mut [Sample], failures: &[Failure]) {
    samples.sort_by_key(|sample| sample.latency);
    let success = samples.len();
    let throughput = if elapsed.as_secs_f64() > 0.0 {
        success as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    println!("flare_perf_message_send_report");
    println!("endpoint={}", config.endpoint);
    println!("total={}", config.total);
    println!("concurrency={}", config.concurrency);
    println!("pairs={}", config.pairs);
    println!("payload_bytes={}", config.payload_bytes);
    println!("metrics_enabled={}", config.metrics_enabled);
    if config.metrics_enabled {
        println!("metrics_settle_ms={}", config.metrics_settle_ms);
        println!("storage_wait_timeout_ms={}", config.storage_wait_timeout_ms);
        println!(
            "orchestrator_metrics_endpoint={}",
            config.orchestrator_metrics_endpoint
        );
        println!(
            "storage_writer_metrics_endpoint={}",
            config.storage_writer_metrics_endpoint
        );
    }
    println!("success={success}");
    println!("failure={}", failures.len());
    println!("elapsed_ms={:.3}", elapsed.as_secs_f64() * 1000.0);
    println!("throughput_ack_per_sec={throughput:.2}");

    if !samples.is_empty() {
        println!("latency_min_ms={:.3}", latency_ms(samples[0].latency));
        println!("latency_avg_ms={:.3}", avg_latency_ms(samples));
        println!("latency_p50_ms={:.3}", percentile_ms(samples, 50.0));
        println!("latency_p90_ms={:.3}", percentile_ms(samples, 90.0));
        println!("latency_p95_ms={:.3}", percentile_ms(samples, 95.0));
        println!("latency_p99_ms={:.3}", percentile_ms(samples, 99.0));
        println!(
            "latency_max_ms={:.3}",
            latency_ms(samples[samples.len() - 1].latency)
        );
    }

    if let Some(first) = failures.first() {
        println!("first_failure_message_id={}", first.message_id);
        println!("first_failure_error={}", first.error);
        diagnose_failures(success, failures);
    }
}

/// 全军覆没时给出**能直接照做的**下一步，而不是让人对着一行截断的错误猜。
///
/// 最常见的一种：带业务 Hook 档（`start_server.sh` 的 social 档）跑压测——
/// 本工具用的是合成用户对，它们彼此不是好友、也不在同一个群，于是每条消息都被
/// PreSend Hook 挡下。此时打印出来的只有 `success=0`，看不出是「压测姿势不对」
/// 还是「服务端真的坏了」，而这两者的处理方式完全相反。
fn diagnose_failures(success: usize, failures: &[Failure]) {
    if success > 0 {
        return;
    }
    let hook_rejected = failures
        .iter()
        .filter(|f| f.error.contains("PreSend hook") || f.error.contains("pre-send hook"))
        .count();
    if hook_rejected * 2 < failures.len() {
        return;
    }
    println!();
    println!("diagnosis=all_sends_rejected_by_pre_send_hook");
    println!("  本工具用的是合成用户对：它们不是好友、也不同群，业务 Hook 档下会被逐条拒绝。");
    println!("  这不是服务端故障，是压测跑在了带业务 Hook 的实例上。改用其一：");
    println!("    1) ./scripts/start_server_core.sh   # 业务中立档，不注册业务 Hook");
    println!("    2) 先给压测用户建立好友/群关系，再用同样的 PERF_PAIRS 跑");
    println!("  注意：此时的 pre_send_hook 阶段耗时只反映「拒绝路径」，不能当作发信基线。");
}

async fn wait_for_storage_metrics(
    config: &Config,
    before: &Option<MetricsSnapshot>,
    expected_messages: usize,
) -> Option<MetricsSnapshot> {
    if !config.metrics_enabled {
        return None;
    }

    let Some(before) = before else {
        return fetch_metrics_if_enabled(
            true,
            &config.storage_writer_metrics_endpoint,
            "storage_writer",
        )
        .await;
    };

    let deadline = Instant::now() + Duration::from_millis(config.storage_wait_timeout_ms);
    let mut last = None;
    loop {
        let current = fetch_metrics_if_enabled(
            true,
            &config.storage_writer_metrics_endpoint,
            "storage_writer",
        )
        .await;
        if let Some(snapshot) = current {
            let persisted = counter_delta_total(before, &snapshot, "messages_persisted_total");
            if persisted >= expected_messages as f64 {
                return Some(snapshot);
            }
            last = Some(snapshot);
        }

        if Instant::now() >= deadline {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn fetch_metrics_if_enabled(
    enabled: bool,
    endpoint: &str,
    label: &str,
) -> Option<MetricsSnapshot> {
    if !enabled {
        return None;
    }

    match reqwest::get(endpoint).await {
        Ok(response) => match response.text().await {
            Ok(body) => Some(parse_prometheus_metrics(&body)),
            Err(error) => {
                eprintln!("metrics_fetch_error{{source=\"{label}\"}}={error}");
                None
            }
        },
        Err(error) => {
            eprintln!("metrics_fetch_error{{source=\"{label}\"}}={error}");
            None
        }
    }
}

fn print_metrics_delta(
    before_orchestrator: &Option<MetricsSnapshot>,
    after_orchestrator: &Option<MetricsSnapshot>,
    before_storage: &Option<MetricsSnapshot>,
    after_storage: &Option<MetricsSnapshot>,
) {
    if let (Some(before), Some(after)) = (before_orchestrator, after_orchestrator) {
        println!("orchestrator_stage_metrics_delta=prometheus_histogram_delta");
        print_histogram_delta(
            before,
            after,
            "message_orchestrator_send_stage_duration_seconds",
            |key| {
                let stage = label_value(&key.labels, "stage").unwrap_or("unknown");
                let outcome = label_value(&key.labels, "outcome").unwrap_or("unknown");
                format!("orchestrator_stage{{stage=\"{stage}\",outcome=\"{outcome}\"}}")
            },
        );
    }

    if let (Some(before), Some(after)) = (before_storage, after_storage) {
        println!("storage_writer_metrics_delta=prometheus_delta");
        print_histogram_delta(
            before,
            after,
            "messages_persisted_duration_seconds",
            |key| format!("storage_writer_histogram{{name=\"{}\"}}", key.name),
        );
        print_histogram_delta(before, after, "db_write_duration_seconds", |key| {
            format!("storage_writer_histogram{{name=\"{}\"}}", key.name)
        });
        print_counter_delta(before, after, "messages_persisted_total", |key| {
            let tenant = label_value(&key.labels, "tenant_id").unwrap_or("unknown");
            format!(
                "storage_writer_counter{{name=\"{}\",tenant_id=\"{}\"}}",
                key.name, tenant
            )
        });
    }
}

fn print_histogram_delta<F>(
    before: &MetricsSnapshot,
    after: &MetricsSnapshot,
    metric_name: &str,
    label: F,
) where
    F: Fn(&HistogramKey) -> String,
{
    let mut rows = after
        .histograms
        .iter()
        .filter(|(key, _)| key.name == metric_name)
        .filter_map(|(key, after_data)| {
            let before_data = before.histograms.get(key);
            let delta = after_data.delta(before_data);
            if delta.count <= 0.0 {
                return None;
            }
            Some((key, delta))
        })
        .collect::<Vec<_>>();

    rows.sort_by(|(left, _), (right, _)| left.labels.cmp(&right.labels));

    for (key, delta) in rows {
        println!(
            "{}_count={:.0} avg_ms={:.3} p95_bucket_ms={} p99_bucket_ms={}",
            label(key),
            delta.count,
            delta.avg_ms(),
            format_bound_ms(delta.quantile_bucket(0.95)),
            format_bound_ms(delta.quantile_bucket(0.99))
        );
    }
}

fn print_counter_delta<F>(
    before: &MetricsSnapshot,
    after: &MetricsSnapshot,
    metric_name: &str,
    label: F,
) where
    F: Fn(&SeriesKey) -> String,
{
    let mut rows = after
        .counters
        .iter()
        .filter(|(key, _)| key.name == metric_name)
        .filter_map(|(key, after_value)| {
            let before_value = before.counters.get(key).copied().unwrap_or_default();
            let delta = *after_value - before_value;
            if delta <= 0.0 {
                return None;
            }
            Some((key, delta))
        })
        .collect::<Vec<_>>();

    rows.sort_by(|(left, _), (right, _)| left.labels.cmp(&right.labels));

    for (key, delta) in rows {
        println!("{}_delta={delta:.0}", label(key));
    }
}

fn counter_delta_total(
    before: &MetricsSnapshot,
    after: &MetricsSnapshot,
    metric_name: &str,
) -> f64 {
    after
        .counters
        .iter()
        .filter(|(key, _)| key.name == metric_name)
        .map(|(key, after_value)| {
            let before_value = before.counters.get(key).copied().unwrap_or_default();
            (*after_value - before_value).max(0.0)
        })
        .sum()
}

fn parse_prometheus_metrics(input: &str) -> MetricsSnapshot {
    let mut snapshot = MetricsSnapshot::default();
    for raw_line in input.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((series, value_raw)) = line.rsplit_once(' ') else {
            continue;
        };
        let Ok(value) = value_raw.parse::<f64>() else {
            continue;
        };
        let (name, labels) = parse_series(series);
        if let Some(base) = name.strip_suffix("_bucket") {
            let Some(le_raw) = label_value(&labels, "le") else {
                continue;
            };
            let Some(bound) = parse_bucket_bound(le_raw) else {
                continue;
            };
            let key = HistogramKey {
                name: base.to_string(),
                labels: labels_without(&labels, "le"),
            };
            snapshot
                .histograms
                .entry(key)
                .or_default()
                .buckets
                .insert(BucketBound(bound), value);
        } else if let Some(base) = name.strip_suffix("_sum") {
            let key = HistogramKey {
                name: base.to_string(),
                labels,
            };
            snapshot.histograms.entry(key).or_default().sum = value;
        } else if let Some(base) = name.strip_suffix("_count") {
            let key = HistogramKey {
                name: base.to_string(),
                labels,
            };
            snapshot.histograms.entry(key).or_default().count = value;
        } else {
            snapshot.counters.insert(SeriesKey { name, labels }, value);
        }
    }
    snapshot
}

fn parse_series(series: &str) -> (String, Vec<(String, String)>) {
    let Some(open) = series.find('{') else {
        return (series.to_string(), Vec::new());
    };
    let name = series[..open].to_string();
    let labels_raw = series
        .get(open + 1..series.len().saturating_sub(1))
        .unwrap_or_default();
    (name, parse_labels(labels_raw))
}

fn parse_labels(input: &str) -> Vec<(String, String)> {
    let mut labels = Vec::new();
    let mut rest = input.trim();
    while !rest.is_empty() {
        let Some(eq) = rest.find('=') else {
            break;
        };
        let key = rest[..eq].trim().to_string();
        rest = rest[eq + 1..].trim_start();
        if !rest.starts_with('"') {
            break;
        }
        rest = &rest[1..];
        let mut value = String::new();
        let mut chars = rest.char_indices();
        let mut end = None;
        while let Some((idx, ch)) = chars.next() {
            match ch {
                '\\' => {
                    if let Some((_, escaped)) = chars.next() {
                        value.push(escaped);
                    }
                }
                '"' => {
                    end = Some(idx);
                    break;
                }
                _ => value.push(ch),
            }
        }
        labels.push((key, value));
        let Some(end) = end else {
            break;
        };
        rest = rest[end + 1..].trim_start();
        if let Some(next) = rest.strip_prefix(',') {
            rest = next.trim_start();
        } else {
            break;
        }
    }
    labels.sort_by(|left, right| left.0.cmp(&right.0));
    labels
}

fn label_value<'a>(labels: &'a [(String, String)], name: &str) -> Option<&'a str> {
    labels
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_str())
}

fn labels_without(labels: &[(String, String)], name: &str) -> Vec<(String, String)> {
    labels
        .iter()
        .filter(|(key, _)| key != name)
        .cloned()
        .collect()
}

fn parse_bucket_bound(value: &str) -> Option<f64> {
    if value == "+Inf" {
        Some(f64::INFINITY)
    } else {
        value.parse::<f64>().ok()
    }
}

fn format_bound_ms(bound: Option<f64>) -> String {
    match bound {
        Some(value) if value.is_infinite() => "+Inf".to_string(),
        Some(value) => format!("{:.3}", value * 1000.0),
        None => "n/a".to_string(),
    }
}

impl HistogramData {
    fn delta(&self, before: Option<&HistogramData>) -> Self {
        let before = before.cloned().unwrap_or_default();
        let buckets = self
            .buckets
            .iter()
            .map(|(bound, value)| {
                let before_value = before.buckets.get(bound).copied().unwrap_or_default();
                (*bound, value - before_value)
            })
            .collect::<BTreeMap<_, _>>();

        Self {
            sum: self.sum - before.sum,
            count: self.count - before.count,
            buckets,
        }
    }

    fn avg_ms(&self) -> f64 {
        if self.count <= 0.0 {
            0.0
        } else {
            (self.sum / self.count) * 1000.0
        }
    }

    fn quantile_bucket(&self, quantile: f64) -> Option<f64> {
        if self.count <= 0.0 {
            return None;
        }
        let target = (self.count * quantile).ceil().max(1.0);
        self.buckets
            .iter()
            .find(|(_, cumulative)| **cumulative >= target)
            .map(|(bound, _)| bound.0)
    }
}

fn latency_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn avg_latency_ms(samples: &[Sample]) -> f64 {
    let total: f64 = samples
        .iter()
        .map(|sample| sample.latency.as_secs_f64() * 1000.0)
        .sum();
    total / samples.len() as f64
}

fn percentile_ms(samples: &[Sample], percentile: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let rank = ((percentile / 100.0) * (samples.len().saturating_sub(1)) as f64).round() as usize;
    latency_ms(samples[rank.min(samples.len() - 1)].latency)
}

fn env_string(key: &str, default: &str) -> String {
    env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    env::var(key)
        .ok()
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}
