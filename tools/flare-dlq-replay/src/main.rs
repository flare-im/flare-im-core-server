use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::Utc;
use flare_server_core::Context;
use flare_server_core::context::Ctx;
use flare_server_core::error::{ErrorCode, FlareError, Result};
use flare_server_core::mq::kafka::{KafkaProducer, KafkaProducerConfig};
use flare_server_core::mq::nats::{NatsProducer, NatsProducerConfig, NatsStreamSpec};
use flare_server_core::mq::producer::{Producer, ProducerError};
use serde::Deserialize;

const DEFAULT_MAX_PAYLOAD_BYTES: usize = 10 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Backend {
    DryRun,
    Nats,
    Kafka,
}

impl Backend {
    fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "dry-run" | "dryrun" => Ok(Self::DryRun),
            "nats" => Ok(Self::Nats),
            "kafka" => Ok(Self::Kafka),
            other => Err(invalid(format!("unsupported backend: {other}"))),
        }
    }
}

#[derive(Debug)]
struct Args {
    backend: Backend,
    input: PathBuf,
    target_topic: Option<String>,
    limit: Option<usize>,
    dry_run: bool,
    max_payload_bytes: usize,
    tenant_id: String,
    nats_url: String,
    nats_timeout_ms: u64,
    nats_retries: u32,
    nats_retry_backoff_ms: u64,
    kafka_brokers: Vec<String>,
    kafka_client_id: String,
}

impl Args {
    fn parse() -> Result<Self> {
        let mut backend = Backend::DryRun;
        let mut input = None;
        let mut target_topic = None;
        let mut limit = None;
        let mut dry_run = false;
        let mut max_payload_bytes = DEFAULT_MAX_PAYLOAD_BYTES;
        let mut tenant_id =
            env::var("FLARE_DLQ_REPLAY_TENANT_ID").unwrap_or_else(|_| "0".to_string());
        let mut nats_url = env::var("FLARE_DLQ_REPLAY_NATS_URL")
            .unwrap_or_else(|_| "nats://127.0.0.1:24222".to_string());
        let mut nats_timeout_ms = env_u64("FLARE_DLQ_REPLAY_NATS_TIMEOUT_MS").unwrap_or(5000);
        let mut nats_retries = env_u32("FLARE_DLQ_REPLAY_NATS_RETRIES").unwrap_or(3);
        let mut nats_retry_backoff_ms =
            env_u64("FLARE_DLQ_REPLAY_NATS_RETRY_BACKOFF_MS").unwrap_or(100);
        let mut kafka_brokers = csv_env("FLARE_DLQ_REPLAY_KAFKA_BROKERS")
            .filter(|brokers| !brokers.is_empty())
            .unwrap_or_else(|| vec!["127.0.0.1:29092".to_string()]);
        let mut kafka_client_id = env::var("FLARE_DLQ_REPLAY_KAFKA_CLIENT_ID")
            .unwrap_or_else(|_| "flare-dlq-replay".to_string());

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--backend" => {
                    backend = Backend::parse(&next_value(&mut args, "--backend")?)?;
                }
                "--input" => {
                    input = Some(PathBuf::from(next_value(&mut args, "--input")?));
                }
                "--target-topic" => {
                    target_topic = Some(next_value(&mut args, "--target-topic")?);
                }
                "--limit" => {
                    limit = Some(parse_value::<usize>(
                        &next_value(&mut args, "--limit")?,
                        "--limit",
                    )?);
                }
                "--dry-run" => {
                    dry_run = true;
                }
                "--max-payload-bytes" => {
                    max_payload_bytes = parse_value::<usize>(
                        &next_value(&mut args, "--max-payload-bytes")?,
                        "--max-payload-bytes",
                    )?;
                }
                "--tenant-id" => {
                    tenant_id = next_value(&mut args, "--tenant-id")?;
                }
                "--nats-url" => {
                    nats_url = next_value(&mut args, "--nats-url")?;
                }
                "--nats-timeout-ms" => {
                    nats_timeout_ms = parse_value::<u64>(
                        &next_value(&mut args, "--nats-timeout-ms")?,
                        "--nats-timeout-ms",
                    )?;
                }
                "--nats-retries" => {
                    nats_retries = parse_value::<u32>(
                        &next_value(&mut args, "--nats-retries")?,
                        "--nats-retries",
                    )?;
                }
                "--nats-retry-backoff-ms" => {
                    nats_retry_backoff_ms = parse_value::<u64>(
                        &next_value(&mut args, "--nats-retry-backoff-ms")?,
                        "--nats-retry-backoff-ms",
                    )?;
                }
                "--kafka-brokers" => {
                    kafka_brokers = parse_csv(&next_value(&mut args, "--kafka-brokers")?);
                }
                "--kafka-client-id" => {
                    kafka_client_id = next_value(&mut args, "--kafka-client-id")?;
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => return Err(invalid(format!("unknown argument: {other}"))),
            }
        }

        let input = input.ok_or_else(|| invalid("--input is required"))?;
        if max_payload_bytes == 0 {
            return Err(invalid("--max-payload-bytes must be greater than zero"));
        }
        if !dry_run && backend == Backend::DryRun {
            dry_run = true;
        }

        Ok(Self {
            backend,
            input,
            target_topic,
            limit,
            dry_run,
            max_payload_bytes,
            tenant_id,
            nats_url,
            nats_timeout_ms,
            nats_retries,
            nats_retry_backoff_ms,
            kafka_brokers,
            kafka_client_id,
        })
    }
}

#[derive(Debug, Deserialize)]
struct DlqReplayRecord {
    #[serde(default)]
    source_topic: Option<String>,
    #[serde(default)]
    target_topic: Option<String>,
    #[serde(default)]
    key: Option<String>,
    payload_base64: String,
    #[serde(default)]
    headers: HashMap<String, String>,
}

impl DlqReplayRecord {
    fn replay_topic<'a>(&'a self, fallback: Option<&'a str>) -> Result<&'a str> {
        self.target_topic
            .as_deref()
            .or(fallback)
            .filter(|topic| !topic.trim().is_empty())
            .ok_or_else(|| invalid("target topic is required"))
    }

    fn decode_payload(&self, max_payload_bytes: usize) -> Result<Vec<u8>> {
        let payload = BASE64.decode(self.payload_base64.trim()).map_err(|err| {
            FlareError::deserialization_error(format!("payload_base64 is not valid base64: {err}"))
        })?;
        if payload.len() > max_payload_bytes {
            return Err(invalid(format!(
                "payload too large: {} bytes, max {} bytes",
                payload.len(),
                max_payload_bytes
            )));
        }
        Ok(payload)
    }

    fn replay_headers(&self) -> HashMap<String, String> {
        let mut headers = self.headers.clone();
        headers.insert("x-flare-dlq-replayed".to_string(), "true".to_string());
        headers.insert(
            "x-flare-dlq-replayed-at-ms".to_string(),
            Utc::now().timestamp_millis().to_string(),
        );
        if let Some(source_topic) = self
            .source_topic
            .as_ref()
            .filter(|source_topic| !source_topic.trim().is_empty())
        {
            headers.insert(
                "x-flare-dlq-source-topic".to_string(),
                source_topic.to_string(),
            );
        }
        headers
    }
}

#[derive(Debug, Default)]
struct ReplayStats {
    scanned: usize,
    replayed: usize,
    dry_run: usize,
}

struct ReplayConfig {
    nats_url: String,
    nats_timeout_ms: u64,
    nats_retries: u32,
    nats_retry_backoff_ms: u64,
    kafka_brokers: Vec<String>,
    kafka_client_id: String,
}

impl From<&Args> for ReplayConfig {
    fn from(args: &Args) -> Self {
        Self {
            nats_url: args.nats_url.clone(),
            nats_timeout_ms: args.nats_timeout_ms,
            nats_retries: args.nats_retries,
            nats_retry_backoff_ms: args.nats_retry_backoff_ms,
            kafka_brokers: args.kafka_brokers.clone(),
            kafka_client_id: args.kafka_client_id.clone(),
        }
    }
}

impl NatsProducerConfig for ReplayConfig {
    fn nats_url(&self) -> &str {
        &self.nats_url
    }

    fn timeout_ms(&self) -> u64 {
        self.nats_timeout_ms
    }

    fn retries(&self) -> u32 {
        self.nats_retries
    }

    fn retry_backoff_ms(&self) -> u64 {
        self.nats_retry_backoff_ms
    }

    fn stream_specs(&self) -> Vec<NatsStreamSpec> {
        flare_server_core::mq::nats::default_stream_specs()
    }
}

impl KafkaProducerConfig for ReplayConfig {
    fn kafka_brokers(&self) -> Vec<String> {
        self.kafka_brokers.clone()
    }

    fn kafka_client_id(&self) -> &str {
        &self.kafka_client_id
    }
}

enum ReplayProducer {
    Nats(Box<NatsProducer>),
    Kafka(KafkaProducer),
}

impl ReplayProducer {
    async fn new(backend: Backend, config: &ReplayConfig) -> Result<Option<Self>> {
        match backend {
            Backend::DryRun => Ok(None),
            Backend::Nats => Ok(Some(Self::Nats(Box::new(
                NatsProducer::new(config)
                    .await
                    .map_err(ProducerError::into_flare_error)?,
            )))),
            Backend::Kafka => Ok(Some(Self::Kafka(
                KafkaProducer::new(config).map_err(ProducerError::into_flare_error)?,
            ))),
        }
    }

    async fn send(
        &self,
        ctx: &Ctx,
        topic: &str,
        key: Option<&str>,
        payload: Vec<u8>,
        headers: HashMap<String, String>,
    ) -> std::result::Result<(), ProducerError> {
        match self {
            Self::Nats(producer) => producer.send(ctx, topic, key, payload, Some(headers)).await,
            Self::Kafka(producer) => producer.send(ctx, topic, key, payload, Some(headers)).await,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            env::var("FLARE_DLQ_REPLAY_LOG")
                .unwrap_or_else(|_| "flare_dlq_replay=info,info".to_string()),
        )
        .init();

    let args = Args::parse()?;
    let ctx: Ctx = Arc::new(
        Context::with_request_id("dlq-replay")
            .with_trace_id(format!("dlq-replay-{}", Utc::now().timestamp_millis()))
            .with_tenant_id(args.tenant_id.clone()),
    );
    let config = ReplayConfig::from(&args);
    let producer = ReplayProducer::new(args.backend, &config).await?;
    let stats = replay_file(&args, producer.as_ref(), &ctx).await?;

    println!(
        "dlq replay complete: scanned={}, replayed={}, dry_run={}",
        stats.scanned, stats.replayed, stats.dry_run
    );
    Ok(())
}

async fn replay_file(
    args: &Args,
    producer: Option<&ReplayProducer>,
    ctx: &Ctx,
) -> Result<ReplayStats> {
    let file = File::open(&args.input).map_err(|err| {
        FlareError::io(format!("open input file {}: {err}", args.input.display()))
    })?;
    let reader = BufReader::new(file);
    let mut stats = ReplayStats::default();

    for line in reader.lines() {
        if args.limit.is_some_and(|limit| stats.scanned >= limit) {
            break;
        }

        let line = line.map_err(|err| FlareError::io(format!("read input line: {err}")))?;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        stats.scanned += 1;
        let record: DlqReplayRecord = serde_json::from_str(trimmed).map_err(|err| {
            FlareError::deserialization_error(format!("parse DLQ replay JSONL record: {err}"))
        })?;
        let topic = record.replay_topic(args.target_topic.as_deref())?;
        let payload = record.decode_payload(args.max_payload_bytes)?;
        let headers = record.replay_headers();

        if args.dry_run {
            stats.dry_run += 1;
            tracing::info!(
                target_topic = %topic,
                key = record.key.as_deref().unwrap_or(""),
                payload_bytes = payload.len(),
                "dry-run DLQ replay record"
            );
            continue;
        }

        let Some(producer) = producer else {
            return Err(invalid(
                "producer is not configured; pass --backend nats or --backend kafka",
            ));
        };
        producer
            .send(ctx, topic, record.key.as_deref(), payload, headers)
            .await
            .map_err(ProducerError::into_flare_error)?;
        stats.replayed += 1;
    }

    Ok(stats)
}

fn next_value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    args.next()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| invalid(format!("{flag} requires a value")))
}

fn parse_value<T>(raw: &str, flag: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    raw.parse::<T>()
        .map_err(|err| invalid(format!("invalid {flag}: {err}")))
}

fn invalid(reason: impl Into<String>) -> FlareError {
    FlareError::localized(ErrorCode::InvalidParameter, reason)
}

fn env_u64(name: &str) -> Option<u64> {
    env::var(name).ok().and_then(|raw| raw.parse::<u64>().ok())
}

fn env_u32(name: &str) -> Option<u32> {
    env::var(name).ok().and_then(|raw| raw.parse::<u32>().ok())
}

fn csv_env(name: &str) -> Option<Vec<String>> {
    env::var(name).ok().map(|raw| parse_csv(&raw))
}

fn parse_csv(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn print_usage() {
    println!(
        "\
Usage:
  flare-dlq-replay --input dlq.jsonl --target-topic flare.im.message.main --dry-run
  flare-dlq-replay --backend nats --input dlq.jsonl --target-topic flare.im.message.main
  flare-dlq-replay --backend kafka --input dlq.jsonl --target-topic flare.im.push.messages

JSONL record:
  {{\"source_topic\":\"flare.im.message.main.dlq\",\"key\":\"conversation-a\",\"payload_base64\":\"...\",\"headers\":{{}}}}

Options:
  --backend dry-run|nats|kafka
  --input <path>
  --target-topic <topic>
  --limit <n>
  --dry-run
  --max-payload-bytes <bytes>
  --tenant-id <tenant>
  --nats-url <url>
  --kafka-brokers <host:port,...>
"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_decodes_payload_and_adds_replay_headers() {
        let record: DlqReplayRecord = serde_json::from_str(
            r#"{"source_topic":"flare.im.message.main.dlq","payload_base64":"aGVsbG8=","headers":{"x-trace-id":"trace-a"}}"#,
        )
        .expect("record should parse");

        let payload = record
            .decode_payload(DEFAULT_MAX_PAYLOAD_BYTES)
            .expect("payload should decode");
        assert_eq!(payload, b"hello");

        let headers = record.replay_headers();
        assert_eq!(
            headers.get("x-flare-dlq-replayed").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            headers.get("x-flare-dlq-source-topic").map(String::as_str),
            Some("flare.im.message.main.dlq")
        );
        assert_eq!(
            headers.get("x-trace-id").map(String::as_str),
            Some("trace-a")
        );
    }

    #[test]
    fn target_topic_can_be_record_specific_or_global() {
        let record: DlqReplayRecord = serde_json::from_str(
            r#"{"target_topic":"flare.im.push.messages","payload_base64":"AA=="}"#,
        )
        .expect("record should parse");
        assert_eq!(
            record
                .replay_topic(Some("flare.im.message.main"))
                .expect("topic should exist"),
            "flare.im.push.messages"
        );

        let record: DlqReplayRecord =
            serde_json::from_str(r#"{"payload_base64":"AA=="}"#).expect("record should parse");
        assert_eq!(
            record
                .replay_topic(Some("flare.im.message.main"))
                .expect("topic should exist"),
            "flare.im.message.main"
        );
    }

    #[test]
    fn payload_size_guard_rejects_large_records() {
        let record: DlqReplayRecord =
            serde_json::from_str(r#"{"payload_base64":"aGVsbG8="}"#).expect("record should parse");
        assert!(record.decode_payload(4).is_err());
    }
}
