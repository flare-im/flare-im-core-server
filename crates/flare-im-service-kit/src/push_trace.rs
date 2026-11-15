//! 推送决策链的写入：push-server、push-worker、push-proxy 共用这一份实现。
//!
//! 记录写在推送 Redis（与设备绑定、闸门状态同一个实例），格式与键见
//! [`flare_im_contracts::push_trace`]。写入绝不挡推送路径：
//!
//! - [`PushTraceRecorder::record`] 只把编码好的记录放进一个有界队列，不等 Redis、不会失败；
//!   队列满了这一条就丢掉，计入 `flare_push_trace_dropped_total{service}`。
//! - 后台一个任务按批（满 `batch` 条，或第一条进来后过了 `flush_interval_ms`）用一条流水线写下：
//!   `RPUSH`、`LTRIM -128 -1`、`EXPIRE`，索引 `ZADD NX`、`ZREMRANGEBYRANK 0 -201`、`EXPIRE`。
//!   Redis 不可用时整批丢掉并计数，不重试：决策链是排障用的软数据。
//! - 关掉（`enabled = false`）或进程里没有 tokio 运行时时是 [`PushTraceRecorder::disabled`]，
//!   什么都不做。
//!
//! 各服务读自己的 `[services.<service>.trace]`（见 [`PushTraceConfig::load`]）。

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use flare_im_contracts::push_trace::{
    PUSH_TRACE_INDEX_LIMIT, PUSH_TRACE_MAX_ENTRIES, PUSH_TRACE_TTL_SECS, PushTraceEntry,
    trace_index_key, trace_list_key,
};
use once_cell::sync::Lazy;
use prometheus::{IntCounter, IntCounterVec, Opts};
use redis::aio::ConnectionManager;
use serde::Deserialize;
use tokio::sync::mpsc;

use crate::redis_connection::{RedisConnectionLimits, SharedRedisConnection};

/// 缺省：队列容量。
pub const DEFAULT_TRACE_QUEUE: usize = 8_192;
/// 缺省：一批至多多少条。
pub const DEFAULT_TRACE_BATCH: usize = 256;
/// 缺省：第一条进队后至多等多久就写（毫秒）。
pub const DEFAULT_TRACE_FLUSH_INTERVAL_MS: u64 = 100;

/// 队列容量的上限：再大就不算有界了。
const MAX_TRACE_QUEUE: usize = 1_000_000;
/// 一批的上限。
const MAX_TRACE_BATCH: usize = 10_000;
/// 攒批等待的上限（毫秒）。
const MAX_TRACE_FLUSH_INTERVAL_MS: u64 = 10_000;
/// 一次批量写（取连接加流水线）的时限。
const WRITE_BUDGET: Duration = Duration::from_secs(3);
/// 写失败的告警至多多久一次：Redis 宕机时每 100 毫秒一条告警只是噪声。
const WARN_INTERVAL: Duration = Duration::from_secs(60);

static TRACE_DROPPED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    let counter = IntCounterVec::new(
        Opts::new(
            "flare_push_trace_dropped_total",
            "Push decision trace entries dropped instead of written (queue full or push Redis unavailable), by service",
        ),
        &["service"],
    )
    .expect("flare_push_trace_dropped_total metric definition is static");
    let _ = crate::metrics::REGISTRY.register(Box::new(counter.clone()));
    counter
});

/// `[services.<service>.trace]`。没写的字段用缺省值；0 也按缺省值处理。
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PushTraceConfig {
    /// 关掉时不写任何记录。
    pub enabled: bool,
    /// 记录留多久（秒），缺省 86400。
    pub ttl_secs: u64,
    /// 队列容量，缺省 8192。满了新记录丢掉并计数。
    pub queue: usize,
    /// 一批至多多少条，缺省 256。
    pub batch: usize,
    /// 第一条进队后至多等多久就写（毫秒），缺省 100。
    pub flush_interval_ms: u64,
}

impl Default for PushTraceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            ttl_secs: PUSH_TRACE_TTL_SECS,
            queue: DEFAULT_TRACE_QUEUE,
            batch: DEFAULT_TRACE_BATCH,
            flush_interval_ms: DEFAULT_TRACE_FLUSH_INTERVAL_MS,
        }
    }
}

impl PushTraceConfig {
    /// 从配置目录读出 `[services.<service>.trace]`（`service` 是配置里的服务名，例如 `push_server`），
    /// 叠到缺省值上。目录下的 `base.toml`、`shared/*.toml`、`services/*.toml`、`overrides/*.toml`
    /// 按这个顺序合并，后读的覆盖先读的；`config_path` 是单个文件时只读那个文件。读不懂的段告警后
    /// 用缺省值，不阻止启动。
    pub fn load(config_path: &str, service: &str) -> Self {
        read_trace_section(Path::new(config_path), service)
            .unwrap_or_default()
            .sanitized()
    }

    /// [`Self::load`]，配置目录取 `FLARE_CONFIG_PATH`（缺省 `config`）。
    pub fn for_service(service: &str) -> Self {
        Self::load(&crate::runtime::resolve_config_path(), service)
    }

    /// 0 换成缺省值，过大的收到上限；一批不超过队列容量。
    pub fn sanitized(self) -> Self {
        let defaults = Self::default();
        let queue = if self.queue == 0 {
            defaults.queue
        } else {
            self.queue.min(MAX_TRACE_QUEUE)
        };
        let batch = if self.batch == 0 {
            defaults.batch
        } else {
            self.batch.min(MAX_TRACE_BATCH)
        }
        .min(queue);
        Self {
            enabled: self.enabled,
            ttl_secs: if self.ttl_secs == 0 {
                defaults.ttl_secs
            } else {
                self.ttl_secs
            },
            queue,
            batch,
            flush_interval_ms: if self.flush_interval_ms == 0 {
                defaults.flush_interval_ms
            } else {
                self.flush_interval_ms.min(MAX_TRACE_FLUSH_INTERVAL_MS)
            },
        }
    }
}

/// 记录写到哪个 Redis。进程里已有的共享连接、`ConnectionManager` 或一个客户端都可以：
/// 客户端会按缺省上限建一个懒连接（[`SharedRedisConnection`]）。
#[derive(Clone)]
pub enum PushTraceRedis {
    Shared(Arc<SharedRedisConnection>),
    Manager(ConnectionManager),
}

impl PushTraceRedis {
    async fn connection(&self) -> redis::RedisResult<ConnectionManager> {
        match self {
            Self::Shared(shared) => shared.get().await,
            Self::Manager(manager) => Ok(manager.clone()),
        }
    }
}

impl From<SharedRedisConnection> for PushTraceRedis {
    fn from(connection: SharedRedisConnection) -> Self {
        Self::Shared(Arc::new(connection))
    }
}

impl From<Arc<SharedRedisConnection>> for PushTraceRedis {
    fn from(connection: Arc<SharedRedisConnection>) -> Self {
        Self::Shared(connection)
    }
}

impl From<ConnectionManager> for PushTraceRedis {
    fn from(manager: ConnectionManager) -> Self {
        Self::Manager(manager)
    }
}

impl From<redis::Client> for PushTraceRedis {
    fn from(client: redis::Client) -> Self {
        Self::Shared(Arc::new(SharedRedisConnection::new(
            client,
            RedisConnectionLimits::default(),
        )))
    }
}

/// 队列里的一条：键已经算好、记录已经编码。
#[derive(Debug, Clone, PartialEq, Eq)]
struct TraceItem {
    list_key: String,
    index_key: String,
    trace_key: String,
    at_ms: i64,
    json: String,
}

struct RecorderInner {
    tx: mpsc::Sender<TraceItem>,
    dropped: IntCounter,
}

/// 决策链的写入端。克隆即共享同一个队列与后台任务。
#[derive(Clone, Default)]
pub struct PushTraceRecorder {
    inner: Option<Arc<RecorderInner>>,
}

impl std::fmt::Debug for PushTraceRecorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PushTraceRecorder")
            .field("enabled", &self.is_enabled())
            .finish()
    }
}

impl PushTraceRecorder {
    /// 什么都不写的写入端。
    pub fn disabled() -> Self {
        Self { inner: None }
    }

    /// 起后台写入任务。`config.enabled` 为假、或调用时不在 tokio 运行时里时返回
    /// [`Self::disabled`]。指标标签 `service` 取本进程可执行文件的名字（`flare-push-server` 等）。
    pub fn spawn(redis: impl Into<PushTraceRedis>, config: PushTraceConfig) -> Self {
        let config = config.sanitized();
        if !config.enabled {
            tracing::info!("push decision trace disabled");
            return Self::disabled();
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            tracing::warn!("push decision trace disabled: no async runtime to run its writer");
            return Self::disabled();
        };
        let service = process_service_label();
        let dropped = TRACE_DROPPED_TOTAL.with_label_values(&[service.as_str()]);
        let (tx, rx) = mpsc::channel(config.queue);
        let writer = TraceWriter {
            redis: redis.into(),
            ttl_secs: config.ttl_secs,
            batch: config.batch,
            flush_interval: Duration::from_millis(config.flush_interval_ms),
            dropped: dropped.clone(),
            last_warning: None,
        };
        runtime.spawn(writer.run(rx));
        Self {
            inner: Some(Arc::new(RecorderInner { tx, dropped })),
        }
    }

    /// 是否真的会写。调用方可以据此省掉拼记录的开销。
    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// 记下一条。不阻塞、不失败：关掉时什么都不做，队列满了这一条丢掉并计数。租户、用户或键为空
    /// 的记录无处可放，直接丢掉（不计数：那是调用方的缺陷，不是容量问题）。
    pub fn record(&self, tenant_id: &str, user_id: &str, trace_key: &str, entry: PushTraceEntry) {
        let Some(inner) = self.inner.as_ref() else {
            return;
        };
        let (tenant_id, user_id, trace_key) = (tenant_id.trim(), user_id.trim(), trace_key.trim());
        if tenant_id.is_empty() || user_id.is_empty() || trace_key.is_empty() {
            return;
        }
        let json = entry.encode();
        if json.is_empty() {
            return;
        }
        let item = TraceItem {
            list_key: trace_list_key(tenant_id, user_id, trace_key),
            index_key: trace_index_key(tenant_id, user_id),
            trace_key: trace_key.to_string(),
            at_ms: entry.at_ms,
            json,
        };
        if inner.tx.try_send(item).is_err() {
            inner.dropped.inc();
        }
    }
}

/// 本进程可执行文件的名字（不带扩展名），作为指标的 `service` 标签；取不到时是 `unknown`。
fn process_service_label() -> String {
    std::env::current_exe()
        .ok()
        .and_then(|path| {
            path.file_stem()
                .map(|stem| stem.to_string_lossy().into_owned())
        })
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

struct TraceWriter {
    redis: PushTraceRedis,
    ttl_secs: u64,
    batch: usize,
    flush_interval: Duration,
    dropped: IntCounter,
    last_warning: Option<Instant>,
}

impl TraceWriter {
    /// 一直跑到所有写入端都被丢掉：每批写完再取下一批。
    async fn run(mut self, mut rx: mpsc::Receiver<TraceItem>) {
        let mut batch = Vec::with_capacity(self.batch);
        while let Some(first) = rx.recv().await {
            batch.push(first);
            let deadline = tokio::time::Instant::now() + self.flush_interval;
            while batch.len() < self.batch {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(item)) => batch.push(item),
                    Ok(None) | Err(_) => break,
                }
            }
            self.write(&batch).await;
            batch.clear();
        }
    }

    async fn write(&mut self, batch: &[TraceItem]) {
        if batch.is_empty() {
            return;
        }
        let pipeline = batch_pipeline(batch, self.ttl_secs);
        let redis = self.redis.clone();
        let outcome = tokio::time::timeout(WRITE_BUDGET, async move {
            let mut connection = redis.connection().await?;
            pipeline.query_async::<()>(&mut connection).await
        })
        .await;
        let failure = match outcome {
            Ok(Ok(())) => return,
            Ok(Err(error)) => error.to_string(),
            Err(_) => "timed out".to_string(),
        };
        self.dropped.inc_by(batch.len() as u64);
        let due = self
            .last_warning
            .is_none_or(|at| at.elapsed() >= WARN_INTERVAL);
        if due {
            self.last_warning = Some(Instant::now());
            tracing::warn!(
                error = %failure,
                entries = batch.len(),
                "push decision trace batch dropped: push Redis unavailable"
            );
        }
    }
}

/// 一批记录的流水线。同一个键在一批里只裁剪、续期一次；索引里同一条消息取这一批里最早的时刻，
/// `ZADD NX` 保证分数始终是这条消息第一条记录的时刻。
fn batch_pipeline(batch: &[TraceItem], ttl_secs: u64) -> redis::Pipeline {
    let max_entries = i64::try_from(PUSH_TRACE_MAX_ENTRIES).unwrap_or(i64::MAX);
    let index_limit = i64::try_from(PUSH_TRACE_INDEX_LIMIT).unwrap_or(i64::MAX);
    let mut lists: BTreeSet<&str> = BTreeSet::new();
    let mut indexes: BTreeSet<&str> = BTreeSet::new();
    let mut members: BTreeMap<(&str, &str), i64> = BTreeMap::new();
    let mut pipeline = redis::pipe();
    for item in batch {
        pipeline
            .cmd("RPUSH")
            .arg(&item.list_key)
            .arg(&item.json)
            .ignore();
        lists.insert(&item.list_key);
        indexes.insert(&item.index_key);
        members
            .entry((item.index_key.as_str(), item.trace_key.as_str()))
            .and_modify(|first| *first = (*first).min(item.at_ms))
            .or_insert(item.at_ms);
    }
    for list in lists {
        pipeline
            .cmd("LTRIM")
            .arg(list)
            .arg(-max_entries)
            .arg(-1)
            .ignore();
        pipeline.cmd("EXPIRE").arg(list).arg(ttl_secs).ignore();
    }
    for ((index, member), first_at_ms) in members {
        pipeline
            .cmd("ZADD")
            .arg(index)
            .arg("NX")
            .arg(first_at_ms)
            .arg(member)
            .ignore();
    }
    for index in indexes {
        pipeline
            .cmd("ZREMRANGEBYRANK")
            .arg(index)
            .arg(0)
            .arg(-(index_limit + 1))
            .ignore();
        pipeline.cmd("EXPIRE").arg(index).arg(ttl_secs).ignore();
    }
    pipeline
}

/// 配置目录里按合并顺序排好的文件。
fn fragment_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = vec![dir.join("base.toml")];
    for sub in ["shared", "services", "overrides"] {
        let Ok(entries) = fs::read_dir(dir.join(sub)) else {
            continue;
        };
        let mut found: Vec<PathBuf> = entries
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| {
                path.extension()
                    .and_then(OsStr::to_str)
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("toml"))
            })
            .collect();
        found.sort();
        files.extend(found);
    }
    files
}

fn merge(base: &mut toml::Value, overlay: toml::Value) {
    match (base, overlay) {
        (toml::Value::Table(base), toml::Value::Table(overlay)) => {
            for (key, value) in overlay {
                match base.get_mut(&key) {
                    Some(existing) => merge(existing, value),
                    None => {
                        base.insert(key, value);
                    }
                }
            }
        }
        (base, overlay) => *base = overlay,
    }
}

fn read_trace_section(path: &Path, service: &str) -> Option<PushTraceConfig> {
    let files = if path.is_file() {
        vec![path.to_path_buf()]
    } else {
        fragment_files(path)
    };
    let mut merged: Option<toml::Value> = None;
    for file in files {
        let Ok(text) = fs::read_to_string(&file) else {
            continue;
        };
        let value: toml::Value = match toml::from_str(&text) {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(file = %file.display(), error = %error, "config fragment not readable for the push decision trace");
                continue;
            }
        };
        let Some(section) = value
            .get("services")
            .and_then(|services| services.get(service))
            .and_then(|service| service.get("trace"))
        else {
            continue;
        };
        match merged.as_mut() {
            Some(merged) => merge(merged, section.clone()),
            None => merged = Some(section.clone()),
        }
    }
    let merged = merged?;
    match merged.try_into::<PushTraceConfig>() {
        Ok(config) => Some(config),
        Err(error) => {
            tracing::error!(service, error = %error, "invalid [services.<service>.trace]; using defaults");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_im_contracts::push_trace::TraceStage;

    #[test]
    fn defaults_follow_the_interface() {
        let config = PushTraceConfig::default();
        assert!(config.enabled);
        assert_eq!(config.ttl_secs, 86_400);
        assert_eq!(config.queue, 8_192);
        assert_eq!(config.batch, 256);
        assert_eq!(config.flush_interval_ms, 100);
    }

    #[test]
    fn zero_means_default_and_a_batch_never_exceeds_the_queue() {
        let config = PushTraceConfig {
            enabled: false,
            ttl_secs: 0,
            queue: 16,
            batch: 0,
            flush_interval_ms: 0,
        }
        .sanitized();
        assert!(!config.enabled);
        assert_eq!(config.ttl_secs, 86_400);
        assert_eq!(config.queue, 16);
        assert_eq!(config.batch, 16, "缺省的 256 收到队列容量");
        assert_eq!(config.flush_interval_ms, 100);
    }

    #[test]
    fn the_section_is_read_per_service_and_later_fragments_win() {
        let dir = std::env::temp_dir().join(format!(
            "flare-push-trace-config-{}-{}",
            std::process::id(),
            line!()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("services")).unwrap();
        fs::create_dir_all(dir.join("overrides")).unwrap();
        fs::write(
            dir.join("services/push-server.toml"),
            "[services.push_server.trace]\nqueue = 1024\nbatch = 64\n",
        )
        .unwrap();
        fs::write(
            dir.join("overrides/local.toml"),
            "[services.push_server.trace]\nbatch = 32\n\n[services.push_proxy.trace]\nenabled = false\n",
        )
        .unwrap();
        let server = PushTraceConfig::load(dir.to_str().unwrap(), "push_server");
        assert_eq!(server.queue, 1_024);
        assert_eq!(server.batch, 32);
        assert!(server.enabled);
        let proxy = PushTraceConfig::load(dir.to_str().unwrap(), "push_proxy");
        assert!(!proxy.enabled);
        assert_eq!(proxy.queue, 8_192);
        let worker = PushTraceConfig::load(dir.to_str().unwrap(), "push_worker");
        assert_eq!(worker, PushTraceConfig::default());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_missing_directory_or_unknown_field_means_defaults() {
        assert_eq!(
            PushTraceConfig::load("/definitely/not/a/config/dir", "push_server"),
            PushTraceConfig::default()
        );
        let dir = std::env::temp_dir().join(format!(
            "flare-push-trace-config-{}-{}",
            std::process::id(),
            line!()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("services")).unwrap();
        fs::write(
            dir.join("services/push-server.toml"),
            "[services.push_server.trace]\nqueue_size = 3\n",
        )
        .unwrap();
        assert_eq!(
            PushTraceConfig::load(dir.to_str().unwrap(), "push_server"),
            PushTraceConfig::default()
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_disabled_recorder_accepts_everything_and_writes_nothing() {
        let recorder = PushTraceRecorder::disabled();
        assert!(!recorder.is_enabled());
        recorder.record(
            "t-1",
            "u-1",
            "m-1",
            PushTraceEntry::new(1, TraceStage::Route, "d-1"),
        );
    }

    #[test]
    fn spawning_outside_a_runtime_or_while_disabled_gives_a_disabled_recorder() {
        let client = redis::Client::open("redis://127.0.0.1:1/").unwrap();
        assert!(!PushTraceRecorder::spawn(client.clone(), PushTraceConfig::default()).is_enabled());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let disabled = PushTraceConfig {
                enabled: false,
                ..PushTraceConfig::default()
            };
            assert!(!PushTraceRecorder::spawn(client, disabled).is_enabled());
        });
    }

    /// 队列满了不等：多出来的记录丢掉并计数。
    #[test]
    fn a_full_queue_drops_and_counts_instead_of_blocking() {
        let (tx, mut rx) = mpsc::channel(1);
        let dropped = TRACE_DROPPED_TOTAL.with_label_values(&["test-overflow"]);
        let before = dropped.get();
        let recorder = PushTraceRecorder {
            inner: Some(Arc::new(RecorderInner {
                tx,
                dropped: dropped.clone(),
            })),
        };
        for at in 0..3 {
            recorder.record(
                "t-1",
                "u-1",
                "m-1",
                PushTraceEntry::new(at, TraceStage::Gate, "d-1").with_conversation("c-1"),
            );
        }
        // 键不全的记录不进队、也不计数。
        recorder.record(
            "",
            "u-1",
            "m-1",
            PushTraceEntry::new(9, TraceStage::Gate, ""),
        );
        assert_eq!(dropped.get() - before, 2);
        let item = rx.try_recv().expect("the first entry is queued");
        assert_eq!(item.list_key, "push:trace:t-1:u-1:m-1");
        assert_eq!(item.index_key, "push:trace:idx:t-1:u-1");
        assert_eq!(item.at_ms, 0);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn a_batch_trims_and_expires_each_key_once_and_indexes_the_first_time() {
        let item = |trace_key: &str, at_ms: i64| TraceItem {
            list_key: trace_list_key("t-1", "u-1", trace_key),
            index_key: trace_index_key("t-1", "u-1"),
            trace_key: trace_key.to_string(),
            at_ms,
            json: format!(r#"{{"at":{at_ms},"st":"route","dev":"","f":{{}}}}"#),
        };
        let pipeline = batch_pipeline(&[item("m-1", 20), item("m-1", 10), item("m-2", 30)], 86_400);
        let commands: Vec<Vec<String>> = pipeline
            .cmd_iter()
            .map(|cmd| {
                cmd.args_iter()
                    .map(|arg| match arg {
                        redis::Arg::Simple(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                        redis::Arg::Cursor => "<cursor>".to_string(),
                    })
                    .collect()
            })
            .collect();
        let count = |name: &str| commands.iter().filter(|cmd| cmd[0] == name).count();
        assert_eq!(count("RPUSH"), 3);
        assert_eq!(count("LTRIM"), 2);
        assert_eq!(count("ZADD"), 2);
        assert_eq!(count("ZREMRANGEBYRANK"), 1);
        assert_eq!(count("EXPIRE"), 3);
        assert!(commands.contains(&vec![
            "LTRIM".to_string(),
            "push:trace:t-1:u-1:m-1".to_string(),
            "-128".to_string(),
            "-1".to_string(),
        ]));
        assert!(commands.contains(&vec![
            "ZADD".to_string(),
            "push:trace:idx:t-1:u-1".to_string(),
            "NX".to_string(),
            "10".to_string(),
            "m-1".to_string(),
        ]));
        assert!(commands.contains(&vec![
            "ZREMRANGEBYRANK".to_string(),
            "push:trace:idx:t-1:u-1".to_string(),
            "0".to_string(),
            "-201".to_string(),
        ]));
    }
}
