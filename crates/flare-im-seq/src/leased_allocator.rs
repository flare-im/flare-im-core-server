//! A1：会话亲和单写者 seq 分配 —— 本地号段 + Redis 租约 + fencing token。
//!
//! # 动机
//! [`crate::SequenceAllocator::allocate_seq`] 每条消息一次 Redis `INCR`，会话写入吞吐被 Redis RTT 限制。
//! 本模块让**会话 owner 节点**从本地号段（[`SequenceRange`]）发号（快路径无 RTT），由 Redis 租约保证
//! 同一时刻同一会话只有一个 owner，fencing token + 本地安全余量防止脑裂下的旧 owner 继续发号。
//!
//! # 正确性前提（必须满足，否则退化）
//! 1. **会话亲和路由**：同一会话的消息需稳定落到同一节点（NATS 按 `conversation_id` 分区消费）。
//!    否则租约频繁易主、本地号段大量空洞，退化得比单次 `INCR` 还差。**这是 A1 的部署前提。**
//! 2. **租约安全余量**：本地在 `lease_ttl - local_margin` 即停止本地发号并强制续租，确保旧 owner 在
//!    新 owner 可接管（Redis 租约 TTL 到期）之前就停手。`local_margin` 必须 ≥ 最大时钟偏移 + 单批处理时延。
//!    fencing token 作为时钟异常时的兜底：失去租约的旧 owner 续租时 token 不匹配 → 被拒。
//!
//! # 故障切换（⚠️ 仅多节点集群可验证；本地单测覆盖状态机，不覆盖真实 Redis/多节点）
//! owner 宕机 → Redis 租约 TTL 到期 → 其他节点 `acquire` 成功、fence `INCR` 自增 → 旧 owner 即便复活，
//! 其本地 `local_valid_until` 早已过期（停发），且续租时 token 与新 owner 不符 → Lua 拒绝 → 不发冲突 seq。
//!
//! seq key 与 [`crate::SequenceAllocator`] 一致（`seq:{conv_key}`），故同一会话在「单次 INCR」与
//! 「租约号段」两种模式间切换时高水位连续、不回退。

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use flare_server_core::error::{AnyhowContext, Result};
use redis::aio::ConnectionManager;
use tokio::sync::Mutex;
use tracing::debug;

use crate::sequence_allocator::SequenceRange;

/// 单调毫秒时钟（抽象以便测试租约过期）。
pub trait MonotonicClock: Send + Sync {
    fn now_ms(&self) -> u64;
}

/// 基于 [`std::time::Instant`] 的单调时钟。
pub struct SystemClock {
    base: std::time::Instant,
}

impl SystemClock {
    pub fn new() -> Self {
        Self {
            base: std::time::Instant::now(),
        }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl MonotonicClock for SystemClock {
    fn now_ms(&self) -> u64 {
        self.base.elapsed().as_millis() as u64
    }
}

/// 租约后端「获取/续租租约 + 领一个号段」的原子结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseOutcome {
    /// 成为/续租 owner，领到号段 `[start, end]` 与当前 fencing token。
    Granted {
        fencing_token: u64,
        start: u64,
        end: u64,
    },
    /// 会话当前被他人持有（未过期），或本地 token 已失配。调用方应让消息重投/重试。
    NotOwner,
}

/// 号段租约后端：一次原子操作完成「获取或续租租约 + 领取一个号段」。
///
/// 静态分发（泛型）以便生产用 [`RedisSeqLeaseBackend`]、测试用 mock，无需 `async_trait` / `dyn`。
pub trait SeqLeaseBackend: Send + Sync {
    fn acquire_and_refill(
        &self,
        conv_key: &str,
        node_id: &str,
        expected_token: Option<u64>,
        lease_ttl: Duration,
        segment_size: u64,
    ) -> impl Future<Output = Result<LeaseOutcome>> + Send;
}

#[derive(Debug)]
struct ConvState {
    segment: SequenceRange,
    fencing_token: u64,
    /// 本地租约有效截止（ms）。过此点即使 Redis 上仍持有也停止本地发号、强制续租。
    local_valid_until_ms: u64,
}

/// 单次 [`LeasedSegmentAllocator::allocate`] 的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeqAllocation {
    /// 本节点持有该会话租约，本地号段发号成功。
    Allocated(u64),
    /// 本节点非该会话 owner；调用方应让该消息重投（亲和路由保证最终落到 owner）。
    NotOwner,
}

/// 本地号段 + 租约 + fencing 的单写者 seq 分配器（A1）。
pub struct LeasedSegmentAllocator<B: SeqLeaseBackend, C: MonotonicClock = SystemClock> {
    node_id: String,
    backend: B,
    segment_size: u64,
    lease_ttl_ms: u64,
    local_margin_ms: u64,
    clock: Arc<C>,
    state: Mutex<HashMap<String, ConvState>>,
}

impl<B: SeqLeaseBackend, C: MonotonicClock> LeasedSegmentAllocator<B, C> {
    /// `conv_key` 形如 `{tenant}:{conversation}`，与 [`crate::SequenceAllocator`] 的 seq key 同源。
    pub fn new(
        node_id: impl Into<String>,
        backend: B,
        segment_size: u64,
        lease_ttl: Duration,
        local_margin: Duration,
        clock: Arc<C>,
    ) -> Self {
        let lease_ttl_ms = (lease_ttl.as_millis() as u64).max(1);
        let local_margin_ms = (local_margin.as_millis() as u64).min(lease_ttl_ms.saturating_sub(1));
        Self {
            node_id: node_id.into(),
            backend,
            segment_size: segment_size.max(1),
            lease_ttl_ms,
            local_margin_ms,
            clock,
            state: Mutex::new(HashMap::new()),
        }
    }

    /// 为会话分配下一个 seq。
    ///
    /// 快路径：持有效租约且本地号段有余 → 直接 `take_next`（无 Redis）。
    /// 慢路径：号段耗尽或本地租约到期 → 经后端续租/重新获取并领新号段。
    ///
    /// 注：当前实现整把状态锁会跨慢路径的后端 `.await`（保证同会话发号串行、号段无竞态）；
    /// 慢路径仅在号段耗尽（约每 `segment_size` 次）或续租时发生，故全局锁争用有界。
    /// 后续可改为 per-conversation 锁进一步降低跨会话干扰。
    pub async fn allocate(&self, conv_key: &str) -> Result<SeqAllocation> {
        let now = self.clock.now_ms();
        let mut state = self.state.lock().await;

        // 快路径：租约本地有效 + 号段有余。
        if let Some(cs) = state.get_mut(conv_key)
            && now < cs.local_valid_until_ms
            && let Some(seq) = cs.segment.take_next()
        {
            return Ok(SeqAllocation::Allocated(seq));
        }

        // 慢路径：续租（携带已知 token 供 fencing 校验）或首次获取。
        let expected_token = state.get(conv_key).map(|cs| cs.fencing_token);
        let outcome = self
            .backend
            .acquire_and_refill(
                conv_key,
                &self.node_id,
                expected_token,
                Duration::from_millis(self.lease_ttl_ms),
                self.segment_size,
            )
            .await
            .context("seq lease acquire/refill failed")?;

        match outcome {
            LeaseOutcome::Granted {
                fencing_token,
                start,
                end,
            } => {
                let mut segment = SequenceRange::new(start, end);
                let Some(seq) = segment.take_next() else {
                    // 号段为空（异常返回）→ 不本地发号，按非 owner 处理。
                    state.remove(conv_key);
                    return Ok(SeqAllocation::NotOwner);
                };
                let local_valid_until_ms =
                    now.saturating_add(self.lease_ttl_ms.saturating_sub(self.local_margin_ms));
                debug!(
                    conv_key = %conv_key,
                    node_id = %self.node_id,
                    fencing_token,
                    start,
                    end,
                    "leased new seq segment"
                );
                state.insert(
                    conv_key.to_string(),
                    ConvState {
                        segment,
                        fencing_token,
                        local_valid_until_ms,
                    },
                );
                Ok(SeqAllocation::Allocated(seq))
            }
            LeaseOutcome::NotOwner => {
                // 失去/未取得租约 → 清除本地号段，杜绝旧 owner 用陈旧号段发号（straggler）。
                state.remove(conv_key);
                Ok(SeqAllocation::NotOwner)
            }
        }
    }
}

/// 原子「获取/续租租约 + 领号段」的 Lua（KEYS: lease/fence/seq；ARGV: node_id/ttl_ms/seg/expected_token(-1=无)）。
///
/// 返回 4 元整数数组：`{granted(1/0), token, start, end}`；非 owner 为 `{0,0,0,0}`。
const LEASE_REFILL_LUA: &str = r#"
local lease = redis.call('GET', KEYS[1])
local node_id = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
local seg = tonumber(ARGV[3])
local expected = tonumber(ARGV[4])
if lease then
  local sep = string.find(lease, ':')
  local owner = string.sub(lease, 1, sep - 1)
  local token = tonumber(string.sub(lease, sep + 1))
  if owner ~= node_id then
    return {0, 0, 0, 0}
  end
  if expected >= 0 and token ~= expected then
    return {0, 0, 0, 0}
  end
  redis.call('PEXPIRE', KEYS[1], ttl_ms)
  local endseq = redis.call('INCRBY', KEYS[3], seg)
  return {1, token, endseq - seg + 1, endseq}
else
  local token = redis.call('INCR', KEYS[2])
  redis.call('SET', KEYS[1], node_id .. ':' .. token, 'PX', ttl_ms)
  local endseq = redis.call('INCRBY', KEYS[3], seg)
  return {1, token, endseq - seg + 1, endseq}
end
"#;

/// 生产用 Redis 租约后端（Lua 原子）。⚠️ 故障切换/多节点正确性需集群验证。
#[derive(Clone)]
pub struct RedisSeqLeaseBackend {
    connection_manager: ConnectionManager,
    script: Arc<redis::Script>,
}

impl RedisSeqLeaseBackend {
    pub fn new(connection_manager: ConnectionManager) -> Self {
        Self {
            connection_manager,
            script: Arc::new(redis::Script::new(LEASE_REFILL_LUA)),
        }
    }

    /// lease/fence/seq 三键；seq key 与 [`crate::SequenceAllocator`] 一致以保证高水位连续。
    fn keys(conv_key: &str) -> (String, String, String) {
        (
            format!("seqlease:{conv_key}"),
            format!("seqfence:{conv_key}"),
            format!("seq:{conv_key}"),
        )
    }
}

impl SeqLeaseBackend for RedisSeqLeaseBackend {
    async fn acquire_and_refill(
        &self,
        conv_key: &str,
        node_id: &str,
        expected_token: Option<u64>,
        lease_ttl: Duration,
        segment_size: u64,
    ) -> Result<LeaseOutcome> {
        let (lease_key, fence_key, seq_key) = Self::keys(conv_key);
        let mut conn = self.connection_manager.clone();
        let expected_arg: i64 = expected_token.map(|t| t as i64).unwrap_or(-1);

        let res: Vec<i64> = self
            .script
            .key(&lease_key)
            .key(&fence_key)
            .key(&seq_key)
            .arg(node_id)
            .arg(lease_ttl.as_millis() as u64)
            .arg(segment_size)
            .arg(expected_arg)
            .invoke_async(&mut conn)
            .await
            .context("seq lease lua invoke failed")?;

        if res.first().copied().unwrap_or(0) == 1 && res.len() >= 4 {
            Ok(LeaseOutcome::Granted {
                fencing_token: res[1].max(0) as u64,
                start: res[2].max(0) as u64,
                end: res[3].max(0) as u64,
            })
        } else {
            Ok(LeaseOutcome::NotOwner)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// 可控时钟。
    struct TestClock {
        now: AtomicU64,
    }
    impl TestClock {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                now: AtomicU64::new(0),
            })
        }
        fn advance(&self, ms: u64) {
            self.now.fetch_add(ms, Ordering::SeqCst);
        }
    }
    impl MonotonicClock for TestClock {
        fn now_ms(&self) -> u64 {
            self.now.load(Ordering::SeqCst)
        }
    }

    /// Mock 后端：按递增号段连续发放，记录调用次数；可被设置为「下一次返回 NotOwner」。
    struct MockBackend {
        next_start: AtomicU64,
        seg: u64,
        token: AtomicU64,
        calls: AtomicU64,
        deny: std::sync::atomic::AtomicBool,
    }
    impl MockBackend {
        fn new(seg: u64) -> Self {
            Self {
                next_start: AtomicU64::new(1),
                seg,
                token: AtomicU64::new(1),
                calls: AtomicU64::new(0),
                deny: std::sync::atomic::AtomicBool::new(false),
            }
        }
    }
    impl SeqLeaseBackend for MockBackend {
        async fn acquire_and_refill(
            &self,
            _conv_key: &str,
            _node_id: &str,
            _expected_token: Option<u64>,
            _lease_ttl: Duration,
            _segment_size: u64,
        ) -> Result<LeaseOutcome> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            // 模拟 Redis 往返的让出点，迫使并发任务在「持状态锁续租」处交错，检验串行化。
            tokio::task::yield_now().await;
            if self.deny.load(Ordering::SeqCst) {
                return Ok(LeaseOutcome::NotOwner);
            }
            let start = self.next_start.fetch_add(self.seg, Ordering::SeqCst);
            Ok(LeaseOutcome::Granted {
                fencing_token: self.token.load(Ordering::SeqCst),
                start,
                end: start + self.seg - 1,
            })
        }
    }

    fn allocator(
        seg: u64,
        clock: Arc<TestClock>,
    ) -> LeasedSegmentAllocator<MockBackend, TestClock> {
        LeasedSegmentAllocator::new(
            "node-A",
            MockBackend::new(seg),
            seg,
            Duration::from_millis(1000),
            Duration::from_millis(200),
            clock,
        )
    }

    #[tokio::test]
    async fn fast_path_uses_local_segment_without_backend_after_first_acquire() {
        let clock = TestClock::new();
        let alloc = allocator(3, clock.clone());

        // 前 3 次同号段：仅 1 次后端调用（首次获取）。
        assert_eq!(
            alloc.allocate("t:c1").await.unwrap(),
            SeqAllocation::Allocated(1)
        );
        assert_eq!(
            alloc.allocate("t:c1").await.unwrap(),
            SeqAllocation::Allocated(2)
        );
        assert_eq!(
            alloc.allocate("t:c1").await.unwrap(),
            SeqAllocation::Allocated(3)
        );
        assert_eq!(alloc.backend.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn refills_when_segment_exhausted_and_stays_monotonic() {
        let clock = TestClock::new();
        let alloc = allocator(3, clock.clone());

        let mut seqs = Vec::new();
        for _ in 0..7 {
            match alloc.allocate("t:c1").await.unwrap() {
                SeqAllocation::Allocated(s) => seqs.push(s),
                SeqAllocation::NotOwner => panic!("unexpected NotOwner"),
            }
        }
        // 严格递增。
        assert!(
            seqs.windows(2).all(|w| w[1] > w[0]),
            "seqs must be strictly increasing: {seqs:?}"
        );
        // seg=3 → 7 次需要 3 次后端调用（号段 [1-3],[4-6],[7-9]）。
        assert_eq!(alloc.backend.calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn reacquires_after_local_lease_margin_even_with_capacity_left() {
        let clock = TestClock::new();
        let alloc = allocator(100, clock.clone()); // 大号段，确保不是因耗尽而续租

        assert_eq!(
            alloc.allocate("t:c1").await.unwrap(),
            SeqAllocation::Allocated(1)
        );
        assert_eq!(alloc.backend.calls.load(Ordering::SeqCst), 1);

        // 本地有效期 = ttl(1000) - margin(200) = 800ms。推进到 801ms → 即便号段仍有余也必须续租。
        clock.advance(801);
        let _ = alloc.allocate("t:c1").await.unwrap();
        assert_eq!(
            alloc.backend.calls.load(Ordering::SeqCst),
            2,
            "expired local lease must force re-acquire"
        );
    }

    #[tokio::test]
    async fn not_owner_clears_local_segment() {
        let clock = TestClock::new();
        let alloc = allocator(100, clock.clone());

        assert_eq!(
            alloc.allocate("t:c1").await.unwrap(),
            SeqAllocation::Allocated(1)
        );
        // 模拟租约被他人接管。
        alloc.backend.deny.store(true, Ordering::SeqCst);
        // 本地号段仍有效期内 → 仍走快路径发 2（未触发后端）。
        assert_eq!(
            alloc.allocate("t:c1").await.unwrap(),
            SeqAllocation::Allocated(2)
        );
        // 推进过本地有效期 → 续租触发 → 后端拒绝 → NotOwner + 清本地。
        clock.advance(1001);
        assert_eq!(
            alloc.allocate("t:c1").await.unwrap(),
            SeqAllocation::NotOwner
        );
        // 再次分配仍触发后端（本地已清空），仍被拒。
        assert_eq!(
            alloc.allocate("t:c1").await.unwrap(),
            SeqAllocation::NotOwner
        );
    }

    #[tokio::test]
    async fn distinct_conversations_keep_independent_segments() {
        let clock = TestClock::new();
        let alloc = allocator(2, clock.clone());

        assert_eq!(
            alloc.allocate("t:c1").await.unwrap(),
            SeqAllocation::Allocated(1)
        );
        assert_eq!(
            alloc.allocate("t:c2").await.unwrap(),
            SeqAllocation::Allocated(3)
        ); // c2 领到下一号段 [3-4]
        assert_eq!(
            alloc.allocate("t:c1").await.unwrap(),
            SeqAllocation::Allocated(2)
        );
        assert_eq!(
            alloc.allocate("t:c2").await.unwrap(),
            SeqAllocation::Allocated(4)
        );
    }

    /// 集群验证清单 #5（本地可验证部分）：高并发下，同一会话**绝不**重复发号。
    /// 小号段 → 频繁续租 → 在「持状态锁跨续租 await」处制造最大争用。
    #[tokio::test]
    async fn concurrent_allocate_never_duplicates_within_conversation() {
        const TASKS: usize = 16;
        const PER_TASK: usize = 50;
        let clock = TestClock::new(); // 不推进 → 租约恒有效 → 仅因号段耗尽续租
        let alloc = Arc::new(allocator(5, clock));

        let mut handles = Vec::with_capacity(TASKS);
        for _ in 0..TASKS {
            let a = alloc.clone();
            handles.push(tokio::spawn(async move {
                let mut seqs = Vec::with_capacity(PER_TASK);
                for _ in 0..PER_TASK {
                    match a.allocate("t:c1").await.unwrap() {
                        SeqAllocation::Allocated(s) => seqs.push(s),
                        SeqAllocation::NotOwner => unreachable!("owner never denied in this test"),
                    }
                }
                seqs
            }));
        }

        let mut all = Vec::with_capacity(TASKS * PER_TASK);
        for h in handles {
            all.extend(h.await.unwrap());
        }

        assert_eq!(
            all.len(),
            TASKS * PER_TASK,
            "every allocate must yield a seq"
        );
        let mut sorted = all.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            all.len(),
            "no duplicate seq may be handed out for one conversation under concurrency"
        );
    }

    // ---- Redis integration (cluster-validation checklist #1, single-node) ----

    async fn redis_backend() -> RedisSeqLeaseBackend {
        // 端口随环境(本机 podman 映射到 26379);默认回退 6379。
        let url = std::env::var("FLARE_TEST_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
        let client = redis::Client::open(url).unwrap();
        let cm = client.get_connection_manager().await.unwrap();
        RedisSeqLeaseBackend::new(cm)
    }

    /// 每次调用唯一会话键：seq key 无 TTL（持久高水位）。并行测试同一时钟刻度调用也必须互不相同，
    /// 故在纳秒戳后再缀一个进程内原子计数器（否则同 tick 并行 → 同 key → 互相污染）。
    fn unique_conv() -> String {
        static CTR: AtomicU64 = AtomicU64::new(0);
        let n = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let c = CTR.fetch_add(1, Ordering::Relaxed);
        format!("itest:{n}:{c}")
    }

    #[tokio::test]
    #[ignore = "requires a local Redis instance"]
    async fn redis_acquire_then_renew_is_monotonic_with_stable_token() {
        let backend = redis_backend().await;
        let conv = unique_conv();
        let ttl = Duration::from_secs(5);

        let g1 = backend
            .acquire_and_refill(&conv, "node-A", None, ttl, 100)
            .await
            .unwrap();
        let LeaseOutcome::Granted {
            fencing_token: t1,
            start: s1,
            end: e1,
        } = g1
        else {
            panic!("fresh acquire must be granted");
        };
        assert_eq!((s1, e1), (1, 100), "fresh seq key starts at 1");

        // 续租（同节点、带 token）→ 下一号段、token 不变。
        let g2 = backend
            .acquire_and_refill(&conv, "node-A", Some(t1), ttl, 100)
            .await
            .unwrap();
        let LeaseOutcome::Granted {
            fencing_token: t2,
            start: s2,
            end: e2,
        } = g2
        else {
            panic!("renewal by same owner must be granted");
        };
        assert_eq!(t2, t1, "renewal keeps the same fencing token");
        assert_eq!((s2, e2), (101, 200), "renewal continues the high-water");
    }

    #[tokio::test]
    #[ignore = "requires a local Redis instance"]
    async fn redis_lease_enforces_single_writer() {
        let backend = redis_backend().await;
        let conv = unique_conv();
        let ttl = Duration::from_secs(5);

        let a = backend
            .acquire_and_refill(&conv, "node-A", None, ttl, 100)
            .await
            .unwrap();
        assert!(matches!(a, LeaseOutcome::Granted { .. }), "A acquires");

        // A 持有未过期 → B 必须被拒（单写者）。
        let b = backend
            .acquire_and_refill(&conv, "node-B", None, ttl, 100)
            .await
            .unwrap();
        assert_eq!(
            b,
            LeaseOutcome::NotOwner,
            "B must be denied while A holds the lease"
        );
    }

    #[tokio::test]
    #[ignore = "requires a local Redis instance"]
    async fn redis_end_to_end_allocator_strictly_increasing_across_refills() {
        let backend = redis_backend().await;
        let conv = unique_conv();
        let alloc = LeasedSegmentAllocator::new(
            "node-A",
            backend,
            50,
            Duration::from_secs(5),
            Duration::from_millis(500),
            Arc::new(SystemClock::new()),
        );

        let mut prev = 0u64;
        for _ in 0..130 {
            // >2 个号段 → 真实 Redis 上经历多次续租/refill
            match alloc.allocate(&conv).await.unwrap() {
                SeqAllocation::Allocated(s) => {
                    assert!(s > prev, "strictly increasing: {s} after {prev}");
                    prev = s;
                }
                SeqAllocation::NotOwner => panic!("single owner must not be denied"),
            }
        }
        assert!(prev >= 130, "allocated at least 130 seqs, got {prev}");
    }
}
