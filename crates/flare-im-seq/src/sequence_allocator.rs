/// 会话序列号分配器（Session Sequence Allocator）
///
/// # 核心职责
///
/// 为每个会话（session）分配单调递增的序列号（seq），保证同会话消息的全局顺序。
/// 这是 IM 系统的核心能力，直接决定了消息顺序的可靠性。
///
/// # 设计原理（对标微信 MsgService / Telegram）
///
/// 1. **每会话独立序列**：每个 conversation_id 维护独立的递增序列号
/// 2. **Redis INCR 原子性**：使用 Redis INCR 命令保证原子递增（单机 10w+ QPS）
/// 3. **分区保证顺序**：JetStream 按 conversation_id key 分区，确保同会话消息进入同一 partition
/// 4. **消费端顺序写入**：StorageWriter 按 partition 顺序消费，保证数据库写入顺序
///
/// # 对比传统方案
///
/// | 方案 | 优点 | 缺点 | 适用场景 |
/// |------|------|------|----------|
/// | **Redis INCR** | 性能高（10w+ QPS）、强一致 | 依赖 Redis | ✅ 推荐（微信方案） |
/// | 数据库自增 ID | 强一致 | 性能差（<1w QPS）、单点 | ❌ 不推荐 |
/// | Snowflake | 无依赖 | 仅保证"趋势递增"，非严格递增 | 部分场景 |
///
/// # 使用示例
///
/// ```rust,ignore
/// let allocator = SequenceAllocator::new(redis_client, 100);
///
/// // 单次分配
/// let seq = allocator.allocate_seq("session-123", "tenant-a").await?;
///
/// // 显式批量分配（仅用于调用方能立即按顺序提交整批操作的场景）
/// let seqs = allocator.allocate_batch("session-456", "tenant-b").await?;
/// ```
///
/// # 参考资料
///
/// - 微信 MsgService 序列号设计：https://cloud.tencent.com/developer/article/1006035
/// - Telegram Sequence Number：https://core.telegram.org/mtproto/description#message-identifier-msg-id
use flare_server_core::error::AnyhowContext;
use flare_server_core::error::Result;
use redis::aio::ConnectionManager;
use std::sync::Arc;
use tracing::debug;

/// "带下限"分配的原子 Lua 脚本（与 [`SequenceAllocator::seq_after_floor`] 语义一一对应）。
///
/// 读取当前高水位（缺失视为 0），与 `floor`(ARGV[1]) 取大，`+1` 后写回并返回。
/// 整段脚本在 Redis 单线程内原子执行，天然跨实例串行；不设 TTL —— 持久会话高水位必须长期保留。
const FLOOR_SEQ_LUA: &str = r"local cur = tonumber(redis.call('GET', KEYS[1]) or '0')
local floor = tonumber(ARGV[1])
if cur < floor then cur = floor end
cur = cur + 1
redis.call('SET', KEYS[1], cur)
return cur";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SequenceRange {
    next: u64,
    end: u64,
}

impl SequenceRange {
    pub(crate) fn new(start: u64, end: u64) -> Self {
        Self { next: start, end }
    }

    pub(crate) fn take_next(&mut self) -> Option<u64> {
        if self.next > self.end {
            return None;
        }

        let seq = self.next;
        self.next = self.next.saturating_add(1);
        Some(seq)
    }

    #[cfg(test)]
    fn is_exhausted(&self) -> bool {
        self.next > self.end
    }

    #[cfg(test)]
    fn remaining(&self) -> u64 {
        if self.is_exhausted() {
            0
        } else {
            self.end - self.next + 1
        }
    }
}

/// 会话序列号分配器
///
/// # 配置参数
///
/// - `redis_client`: Redis 客户端（用于 INCR 原子操作）
/// - `batch_size`: 显式 `allocate_batch` 的批次大小
/// - Redis key 不设置过期时间：持久会话的 seq 高水位必须长期保留，避免空闲后从 1 重启。
#[derive(Clone)]
pub struct SequenceAllocator {
    /// Redis 客户端（保留用于健康检查等场景）
    _redis_client: Arc<redis::Client>,
    /// Redis 连接管理器（用于异步操作）
    connection_manager: ConnectionManager,
    /// 显式批量分配的批次大小。
    ///
    /// 普通持久消息/事件必须走 `allocate_seq` 单次 INCR，避免跨服务本地号段缓存破坏会话内严格顺序。
    batch_size: u64,
}

impl SequenceAllocator {
    /// 创建序列号分配器
    ///
    /// # 参数
    ///
    /// - `redis_client`: Redis 客户端
    /// - `batch_size`: 显式批量分配大小（默认 100）
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let redis_client = redis::Client::open("redis://127.0.0.1/")?;
    /// let allocator = SequenceAllocator::new(Arc::new(redis_client), 100);
    /// ```
    pub async fn new(redis_client: Arc<redis::Client>, batch_size: u64) -> Result<Self> {
        let connection_manager = redis_client
            .get_connection_manager()
            .await
            .context("Failed to create Redis connection manager")?;

        Ok(Self {
            _redis_client: redis_client, // 添加下划线前缀表示保留但暂时未使用
            connection_manager,
            batch_size: batch_size.max(1),
        })
    }

    fn build_allocate_seq_pipeline(key: &str) -> redis::Pipeline {
        let mut pipe = redis::pipe();
        pipe.atomic().cmd("INCR").arg(key);
        pipe
    }

    /// 计算"带下限(floor)"的下一个序列号：`max(当前高水位, floor) + 1`。
    ///
    /// 这是 [`Self::allocate_seq_with_floor`] 的纯逻辑核心，Lua 原子脚本与之一一对应。
    /// - `current_high_water`：Redis 中当前 seq 高水位（key 缺失时为 0）。
    /// - `floor`：持久化存储的会话 max_seq（权威高水位下限）。
    ///
    /// 语义保证：
    /// 1. 返回值恒 `> max(current_high_water, floor)`，即绝不回退到已用/已持久化区间；
    /// 2. `floor` 陈旧偏低无害——`max` 会优先采用更高的 Redis live 高水位；
    /// 3. `floor` 仅在 Redis 被 flush（高水位 < 持久化 max_seq）时起"救回"作用。
    ///
    /// 这是 [`FLOOR_SEQ_LUA`] 原子脚本的参考规格，仅供单测校验语义。
    #[cfg(test)]
    pub(crate) fn seq_after_floor(current_high_water: u64, floor: u64) -> u64 {
        current_high_water.max(floor).saturating_add(1)
    }

    /// 构建"带下限"分配的原子 Lua 脚本。
    ///
    /// 等价于 `redis.call` 版的 [`Self::seq_after_floor`]：读取当前值（缺失视为 0），
    /// 与 `floor` 取大，`+1` 后写回并返回。整段脚本在 Redis 单线程内原子执行，
    /// 天然跨实例串行，不设 TTL（持久会话高水位必须长期保留）。
    fn floor_seq_script() -> redis::Script {
        redis::Script::new(FLOOR_SEQ_LUA)
    }

    fn build_allocate_batch_pipeline(key: &str, batch_size: u64) -> redis::Pipeline {
        let mut pipe = redis::pipe();
        pipe.atomic().cmd("INCRBY").arg(key).arg(batch_size);
        pipe
    }

    /// 为持久会话操作分配 session_seq（严格同步模式）
    ///
    /// # 核心逻辑
    ///
    /// 1. 构建 Redis key：`seq:{tenant_id}:{conversation_id}`
    /// 2. 执行 `INCR key` 原子递增
    /// 3. 返回递增后的序列号
    ///
    /// # 性能
    ///
    /// - Redis INCR 单机性能：10w+ QPS
    /// - 网络延迟：局域网 <1ms，跨机房 5-10ms
    ///
    /// # 参数
    ///
    /// - `conversation_id`: 会话 ID（如 "1-{hash}" 或 "2-group123"）
    /// - `tenant_id`: 租户 ID（用于多租户隔离）
    ///
    /// # 返回
    ///
    /// - `Ok(seq)`: 分配成功，返回序列号（从 1 开始递增）
    /// - `Err`: Redis 不可用时返回错误。调用方必须失败或重试，**不得降级发号**——
    ///   会话内 seq 是严格排序高水位，降级会破坏顺序/造成回退碰撞。
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let seq = allocator.allocate_seq("session-123", "tenant-a").await?;
    /// println!("Allocated seq: {}", seq); // 输出：Allocated seq: 42
    /// ```
    pub async fn allocate_seq(&self, conversation_id: &str, tenant_id: &str) -> Result<u64> {
        let key = self.build_redis_key(tenant_id, conversation_id);
        self.allocate_single_seq(&key, conversation_id, tenant_id)
            .await
    }

    /// 分配 seq，并保证结果不低于持久化高水位 `floor`（自愈防回退）。
    ///
    /// # 背景
    ///
    /// `allocate_seq` 依赖 Redis 计数器作为唯一高水位。一旦 Redis 被 flush / 实例重启 /
    /// key 丢失，缺失 key 的 `INCR` 会从 1 重新计数，而持久化历史（Postgres）仍保留旧的
    /// 更高 seq —— 新消息因此拿到 **低于历史** 的 seq，客户端按 seq 升序渲染时会把新消息
    /// 插到历史中间。本方法以持久化 max_seq 作下限，用原子 Lua `max(GET,floor)+1` 消除回退。
    ///
    /// # 参数
    ///
    /// - `floor`：该会话在持久化存储中的权威 `max_seq`；无历史（新会话）时传 0，
    ///   行为退化为普通递增。传入偏低的陈旧值也安全——脚本会与 Redis live 高水位取大。
    ///
    /// # 返回
    ///
    /// - `Ok(seq)`：恒 `> max(redis_high_water, floor)`，严格递增。
    /// - `Err`：Redis 不可用时返回错误（同 `allocate_seq`，调用方须失败/重试，不得降级发号）。
    pub async fn allocate_seq_with_floor(
        &self,
        conversation_id: &str,
        tenant_id: &str,
        floor: u64,
    ) -> Result<u64> {
        // floor 为 0 时没有回退风险，走更轻量的单次 INCR 快路径。
        if floor == 0 {
            return self.allocate_seq(conversation_id, tenant_id).await;
        }

        let key = self.build_redis_key(tenant_id, conversation_id);
        let mut conn = self.connection_manager.clone();
        let seq: u64 = Self::floor_seq_script()
            .key(&key)
            .arg(floor)
            .invoke_async(&mut conn)
            .await
            .context("Failed to allocate floor-guarded sequence in Redis")?;

        debug!(
            conversation_id = %conversation_id,
            tenant_id = %tenant_id,
            floor = floor,
            seq = seq,
            "Allocated floor-guarded session sequence"
        );

        Ok(seq)
    }

    async fn allocate_single_seq(
        &self,
        key: &str,
        conversation_id: &str,
        tenant_id: &str,
    ) -> Result<u64> {
        // 获取 Redis 连接
        let mut conn = self.connection_manager.clone();

        // 执行原子递增。持久会话的 seq 是同步和排序高水位，不能设置 TTL。
        let (seq,): (u64,) = Self::build_allocate_seq_pipeline(key)
            .query_async(&mut conn)
            .await
            .context("Failed to allocate sequence in Redis")?;

        debug!(
            conversation_id = %conversation_id,
            tenant_id = %tenant_id,
            seq = seq,
            "Allocated session sequence"
        );

        Ok(seq)
    }

    /// 显式批量模式（批量获取 seq，减少 Redis 调用）
    ///
    /// # 适用场景
    ///
    /// 调用方能立即按顺序提交整批操作的场景，可以批量获取序列号，
    /// 减少 Redis 调用次数，提升性能。普通持久消息/事件不得通过本地缓存发号。
    ///
    /// # 原理
    ///
    /// 1. 一次性执行 `INCR key batch_size`（如 100）
    /// 2. 返回区间 `[start_seq, end_seq]`（如 [100, 199]）
    /// 3. 调用方在内存中逐个分配
    ///
    /// # 性能提升
    ///
    /// - 单次分配：10w QPS，每次 Redis 调用
    /// - 批量分配：100w QPS（100 倍提升），100 次分配 1 次 Redis 调用
    ///
    /// # 注意事项
    ///
    /// ⚠️ 如果调用方获取整批后未全部提交，seq 可能会有空洞（如 [150-199] 未使用）。
    /// 这在 IM 系统中是可接受的，因为 seq 只需保证"严格递增"，不需要"连续"。
    ///
    /// # 参数
    ///
    /// - `conversation_id`: 会话 ID
    /// - `tenant_id`: 租户 ID
    ///
    /// # 返回
    ///
    /// - `Ok(vec)`: 成功，返回序列号区间 `[start, start+1, ..., end]`
    /// - `Err`: Redis 不可用时返回错误
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let seqs = allocator.allocate_batch("session-456", "tenant-b").await?;
    /// println!("Allocated batch: {:?}", seqs); // [100, 101, ..., 199]
    ///
    /// for seq in seqs {
    ///     message.seq = seq;
    ///     send_to_jetstream(message);
    /// }
    /// ```
    pub async fn allocate_batch(&self, conversation_id: &str, tenant_id: &str) -> Result<Vec<u64>> {
        let key = self.build_redis_key(tenant_id, conversation_id);
        let range = self
            .allocate_batch_range(&key, conversation_id, tenant_id)
            .await?;

        // 返回区间 [start_seq, end_seq]
        Ok((range.next..=range.end).collect())
    }

    async fn allocate_batch_range(
        &self,
        key: &str,
        conversation_id: &str,
        tenant_id: &str,
    ) -> Result<SequenceRange> {
        let mut conn = self.connection_manager.clone();

        // 一次性递增 batch_size（如 100）。持久会话的 seq 高水位不能设置 TTL。
        let (end_seq,): (u64,) = Self::build_allocate_batch_pipeline(key, self.batch_size)
            .query_async(&mut conn)
            .await
            .with_context(|| "Failed to allocate batch sequence in Redis")?;

        // 计算起始序列号
        let start_seq = end_seq.saturating_sub(self.batch_size) + 1;

        debug!(
            conversation_id = %conversation_id,
            tenant_id = %tenant_id,
            start_seq = start_seq,
            end_seq = end_seq,
            batch_size = self.batch_size,
            "Allocated batch sequence"
        );

        Ok(SequenceRange::new(start_seq, end_seq))
    }

    /// 构建 Redis key（格式：seq:{tenant_id}:{conversation_id}）
    ///
    /// # 设计考虑
    ///
    /// 1. **租户隔离**：不同租户的 seq 互不影响
    /// 2. **会话隔离**：不同会话的 seq 互不影响
    /// 3. **可读性**：key 格式清晰，便于调试
    ///
    /// # 示例
    ///
    /// ```text
    /// seq:tenant-a:1-{hash}  → 单聊
    /// seq:tenant-b:group:group123      → 群聊
    /// ```
    fn build_redis_key(&self, tenant_id: &str, conversation_id: &str) -> String {
        format!("seq:{}:{}", tenant_id, conversation_id)
    }

    /// 健康检查：测试 Redis 连接是否可用
    ///
    /// # 用途
    ///
    /// - 启动时检查 Redis 可用性
    /// - 运行时定期健康检查
    /// - 故障切换/熔断决策依据（Redis 不可用时调用方应失败重试，不降级发号）
    ///
    /// # 返回
    ///
    /// - `Ok(true)`: Redis 可用
    /// - `Ok(false)` 或 `Err`: Redis 不可用
    pub async fn health_check(&self) -> Result<bool> {
        let mut conn = self.connection_manager.clone();

        // 执行 PING 命令
        let pong: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .context("Redis PING failed")?;

        Ok(pong == "PONG")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packed_contains(packed: &[u8], needle: &[u8]) -> bool {
        packed.windows(needle.len()).any(|window| window == needle)
    }

    #[test]
    fn sequence_range_allocates_until_exhausted() {
        let mut range = SequenceRange::new(10, 12);

        assert_eq!(range.remaining(), 3);
        assert_eq!(range.take_next(), Some(10));
        assert_eq!(range.take_next(), Some(11));
        assert_eq!(range.remaining(), 1);
        assert_eq!(range.take_next(), Some(12));
        assert_eq!(range.take_next(), None);
        assert!(range.is_exhausted());
    }

    #[test]
    fn empty_sequence_range_is_exhausted() {
        let mut range = SequenceRange::new(20, 19);

        assert_eq!(range.remaining(), 0);
        assert_eq!(range.take_next(), None);
        assert!(range.is_exhausted());
    }

    #[test]
    fn allocate_seq_pipeline_keeps_persistent_high_water() {
        let packed = SequenceAllocator::build_allocate_seq_pipeline("seq:tenant:conversation")
            .get_packed_pipeline();

        assert!(packed_contains(&packed, b"INCR"));
        assert!(
            !packed_contains(&packed, b"EXPIRE"),
            "persistent conversation sequence keys must not expire"
        );
    }

    #[test]
    fn allocate_batch_pipeline_keeps_persistent_high_water() {
        let packed =
            SequenceAllocator::build_allocate_batch_pipeline("seq:tenant:conversation", 100)
                .get_packed_pipeline();

        assert!(packed_contains(&packed, b"INCRBY"));
        assert!(
            !packed_contains(&packed, b"EXPIRE"),
            "persistent conversation sequence keys must not expire"
        );
    }

    #[test]
    fn seq_after_floor_never_regresses_below_floor() {
        // 冷 key（Redis 被 flush）后高水位=5，但持久化 max_seq=76 → 必须救回到 77。
        assert_eq!(SequenceAllocator::seq_after_floor(5, 76), 77);
        // key 缺失（高水位=0）+ 有历史 → 从 floor 之上继续。
        assert_eq!(SequenceAllocator::seq_after_floor(0, 76), 77);
        // floor 恰等于高水位 → 正常 +1。
        assert_eq!(SequenceAllocator::seq_after_floor(76, 76), 77);
    }

    #[test]
    fn seq_after_floor_prefers_live_high_water_when_floor_is_stale() {
        // Redis live 高水位(100)高于陈旧 floor(76) → 采用 live，floor 不拖低。
        assert_eq!(SequenceAllocator::seq_after_floor(100, 76), 101);
        // 全新会话（无历史，floor=0，高水位=0）→ 从 1 开始。
        assert_eq!(SequenceAllocator::seq_after_floor(0, 0), 1);
    }

    #[test]
    fn seq_after_floor_is_strictly_monotonic_over_both_inputs() {
        // 返回值恒 > max(current, floor)，绝不落进已用/已持久化区间。
        for &(cur, floor) in &[(0u64, 0u64), (5, 76), (76, 5), (999, 1000), (1000, 999)] {
            let next = SequenceAllocator::seq_after_floor(cur, floor);
            assert!(next > cur, "next({next}) must exceed current({cur})");
            assert!(next > floor, "next({next}) must exceed floor({floor})");
        }
    }

    #[test]
    fn floor_seq_lua_reads_and_writes_high_water_without_expiry() {
        // Lua 脚本必须：GET 当前值、与 floor(ARGV[1]) 取大、SET 回写，且绝不 EXPIRE。
        assert!(FLOOR_SEQ_LUA.contains("redis.call('GET', KEYS[1])"));
        assert!(FLOOR_SEQ_LUA.contains("tonumber(ARGV[1])"));
        assert!(FLOOR_SEQ_LUA.contains("if cur < floor then cur = floor end"));
        assert!(FLOOR_SEQ_LUA.contains("cur = cur + 1"));
        assert!(FLOOR_SEQ_LUA.contains("redis.call('SET', KEYS[1], cur)"));
        assert!(
            !FLOOR_SEQ_LUA.to_uppercase().contains("EXPIRE"),
            "persistent conversation sequence keys must not expire"
        );
    }

    /// 测试：单次分配序列号
    #[tokio::test]
    #[ignore = "requires a local Redis instance"]
    async fn test_allocate_seq() {
        // 注意：需要本地运行 Redis（127.0.0.1:6379）
        let redis_client = redis::Client::open("redis://127.0.0.1/").unwrap();
        let allocator = SequenceAllocator::new(Arc::new(redis_client), 100)
            .await
            .unwrap();

        let conversation_id = "test-session-1";
        let tenant_id = "test-tenant";

        // 分配 3 次，验证递增
        let seq1 = allocator
            .allocate_seq(conversation_id, tenant_id)
            .await
            .unwrap();
        let seq2 = allocator
            .allocate_seq(conversation_id, tenant_id)
            .await
            .unwrap();
        let seq3 = allocator
            .allocate_seq(conversation_id, tenant_id)
            .await
            .unwrap();

        assert!(seq2 > seq1);
        assert!(seq3 > seq2);
        assert_eq!(seq2, seq1 + 1);
        assert_eq!(seq3, seq2 + 1);
    }

    /// 测试：显式批量分配
    #[tokio::test]
    #[ignore = "requires a local Redis instance"]
    async fn test_allocate_batch() {
        let redis_client = redis::Client::open("redis://127.0.0.1/").unwrap();
        let allocator = SequenceAllocator::new(Arc::new(redis_client), 10)
            .await
            .unwrap();

        let conversation_id = "test-session-2";
        let tenant_id = "test-tenant";

        let seqs = allocator
            .allocate_batch(conversation_id, tenant_id)
            .await
            .unwrap();

        // 验证批次大小
        assert_eq!(seqs.len(), 10);

        // 验证连续性
        for i in 1..seqs.len() {
            assert_eq!(seqs[i], seqs[i - 1] + 1);
        }
    }

    /// 测试：健康检查
    #[tokio::test]
    #[ignore = "requires a local Redis instance"]
    async fn test_health_check() {
        let redis_client = redis::Client::open("redis://127.0.0.1/").unwrap();
        let allocator = SequenceAllocator::new(Arc::new(redis_client), 100)
            .await
            .unwrap();

        let healthy = allocator.health_check().await.unwrap();
        assert!(healthy);
    }
}
