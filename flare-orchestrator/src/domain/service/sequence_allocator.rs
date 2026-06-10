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
/// | 时间戳 + 随机数 | 无依赖 | 无法保证严格顺序 | ⚠️ 降级方案 |
///
/// # 使用示例
///
/// ```rust
/// let allocator = SequenceAllocator::new(redis_client, 100);
///
/// // 单次分配
/// let seq = allocator.allocate_seq("session-123", "tenant-a").await?;
///
/// // 批量预分配（减少 Redis 调用）
/// let seqs = allocator.allocate_batch("session-456", "tenant-b").await?;
///
/// // 降级模式（Redis 不可用时）
/// let seq = allocator.allocate_seq_degraded();
/// ```
///
/// # 参考资料
///
/// - 微信 MsgService 序列号设计：https://cloud.tencent.com/developer/article/1006035
/// - Telegram Sequence Number：https://core.telegram.org/mtproto/description#message-identifier-msg-id
use flare_server_core::error::{ErrorCode, Result};
use flare_server_core::{error::AnyhowContext, flare_err};
use redis::aio::ConnectionManager;
use std::sync::Arc;
use tracing::{debug, warn};

/// 会话序列号分配器
///
/// # 配置参数
///
/// - `redis_client`: Redis 客户端（用于 INCR 原子操作）
/// - `batch_size`: 预分配批次大小（默认 100，高频场景可调整到 500-1000）
/// - Redis key 不设置过期时间：持久会话的 seq 高水位必须长期保留，避免空闲后从 1 重启。
#[derive(Clone)]
pub struct SequenceAllocator {
    /// Redis 客户端（保留用于健康检查等场景）
    _redis_client: Arc<redis::Client>,
    /// Redis 连接管理器（用于异步操作）
    connection_manager: ConnectionManager,
    /// 预分配批次大小（减少 Redis 调用频率）
    batch_size: u64,
}

impl SequenceAllocator {
    /// 创建序列号分配器
    ///
    /// # 参数
    ///
    /// - `redis_client`: Redis 客户端
    /// - `batch_size`: 预分配批次大小（默认 100）
    ///
    /// # 示例
    ///
    /// ```rust
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
            batch_size,
        })
    }

    fn build_allocate_seq_pipeline(key: &str) -> redis::Pipeline {
        let mut pipe = redis::pipe();
        pipe.atomic().cmd("INCR").arg(key);
        pipe
    }

    fn build_allocate_batch_pipeline(key: &str, batch_size: u64) -> redis::Pipeline {
        let mut pipe = redis::pipe();
        pipe.atomic().cmd("INCRBY").arg(key).arg(batch_size);
        pipe
    }

    /// 为消息分配 session_seq（同步模式）
    ///
    /// # 核心逻辑
    ///
    /// 1. 构建 Redis key：`seq:{tenant_id}:{conversation_id}`
    /// 2. 执行 `INCR key` 原子递增（保证线程安全）
    /// 3. 设置 TTL 为 7 天（避免 key 堆积）
    /// 4. 返回递增后的序列号
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
    /// - `Err`: Redis 不可用时返回错误（调用方应使用降级策略）
    ///
    /// # 示例
    ///
    /// ```rust
    /// let seq = allocator.allocate_seq("session-123", "tenant-a").await?;
    /// println!("Allocated seq: {}", seq); // 输出：Allocated seq: 42
    /// ```
    pub async fn allocate_seq(&self, conversation_id: &str, tenant_id: &str) -> Result<u64> {
        // 构建 Redis key（格式：seq:{tenant_id}:{conversation_id}）
        let key = self.build_redis_key(tenant_id, conversation_id);

        // 获取 Redis 连接
        let mut conn = self.connection_manager.clone();

        // 执行原子递增。持久会话的 seq 是同步和排序高水位，不能设置 TTL。
        let (seq,): (u64,) = Self::build_allocate_seq_pipeline(&key)
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

    /// 预分配批次模式（批量获取 seq，减少 Redis 调用）
    ///
    /// # 适用场景
    ///
    /// 高频消息场景（如群聊、直播间、弹幕），可以批量预分配序列号到内存，
    /// 减少 Redis 调用次数，提升性能。
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
    /// ⚠️ 如果服务重启，预分配的 seq 可能会有空洞（如 [150-199] 未使用）。
    /// 这在 IM 系统中是可接受的，因为 seq 只需保证"单调递增"，不需要"连续"。
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
    /// ```rust
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

        let mut conn = self.connection_manager.clone();

        // 一次性递增 batch_size（如 100）。持久会话的 seq 高水位不能设置 TTL。
        let (end_seq,): (u64,) = Self::build_allocate_batch_pipeline(&key, self.batch_size)
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

        // 返回区间 [start_seq, end_seq]
        Ok((start_seq..=end_seq).collect())
    }

    /// 降级策略：Redis 不可用时使用时间戳 + 随机数
    ///
    /// # ⚠️ 重要说明
    ///
    /// 降级模式下**无法保证严格顺序**，只能保证"大致有序"（趋势递增）。
    ///
    /// 适用场景：
    /// - Redis 短时故障（集群切换、网络抖动）
    /// - 临时过渡方案（等待 Redis 恢复）
    ///
    /// 不适用场景：
    /// - 需要严格顺序的场景（如金融交易、订单系统）
    ///
    /// # 设计原理
    ///
    /// 生成 64 位序列号：
    /// - 高 48 位：时间戳（毫秒）→ 保证"趋势递增"
    /// - 低 16 位：随机数 → 避免同毫秒冲突
    ///
    /// # 示例
    ///
    /// ```rust
    /// // Redis 不可用时降级
    /// let seq = match allocator.allocate_seq(conversation_id, tenant_id).await {
    ///     Ok(seq) => seq,
    ///     Err(e) => {
    ///         warn!("Redis unavailable: {}, using degraded mode", e);
    ///         allocator.allocate_seq_degraded()?
    ///     }
    /// };
    /// ```
    pub fn allocate_seq_degraded(&self) -> Result<u64> {
        Self::generate_degraded_seq()
    }

    fn generate_degraded_seq() -> Result<u64> {
        use std::time::{SystemTime, UNIX_EPOCH};

        // 获取当前时间戳（毫秒）
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| {
                flare_err!(
                    ErrorCode::InternalError,
                    &format!("System time went backwards: {}", e)
                )
            })?
            .as_millis() as u64;

        // 生成 16 位随机数
        let random = rand::random::<u16>() as u64;

        // 组合：时间戳（48 位） + 随机数（16 位）
        let seq = (now << 16) | random;

        warn!(
            seq = seq,
            timestamp_ms = now,
            random = random,
            "Using degraded sequence allocation (timestamp-based)"
        );

        Ok(seq)
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
    /// ```
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
    /// - 降级策略决策依据
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

    /// 测试：批量预分配
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

    /// 测试：降级模式
    #[test]
    fn test_allocate_seq_degraded() {
        let seq1 = SequenceAllocator::generate_degraded_seq().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let seq2 = SequenceAllocator::generate_degraded_seq().unwrap();

        // 验证趋势递增（但不保证严格递增）
        assert!(seq2 > seq1);
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
