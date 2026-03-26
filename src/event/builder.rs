//! EventEnvelope 构建器和辅助函数
//!
//! 提供快速构建 EventEnvelope 的便捷方法

use anyhow::Result;
use flare_server_core::event_bus::EventEnvelope;
use prost::Message;

/// EventEnvelope 构建器
///
/// 提供链式调用的构建方法
pub struct EventEnvelopeBuilder {
    event_type: String,
    partition_key: String,
    seq: u64,
    payload: Vec<u8>,
    timestamp_ms: Option<u64>,
    source: Option<String>,
}

impl EventEnvelopeBuilder {
    /// 创建新的构建器
    ///
    /// # 参数
    /// - `event_type`: 事件类型
    /// - `partition_key`: 分区键
    /// - `seq`: 序号
    /// - `payload`: 载荷字节
    ///
    /// # 返回
    /// - `Self`: 构建器实例
    pub fn new(
        event_type: impl Into<String>,
        partition_key: impl Into<String>,
        seq: u64,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            event_type: event_type.into(),
            partition_key: partition_key.into(),
            seq,
            payload,
            timestamp_ms: None,
            source: None,
        }
    }

    /// 设置时间戳（毫秒）
    ///
    /// # 参数
    /// - `ms`: 时间戳（毫秒）
    ///
    /// # 返回
    /// - `Self`: 构建器实例
    pub fn with_timestamp_ms(mut self, ms: u64) -> Self {
        self.timestamp_ms = Some(ms);
        self
    }

    /// 设置当前时间戳
    ///
    /// # 返回
    /// - `Self`: 构建器实例
    pub fn with_current_timestamp(mut self) -> Self {
        self.timestamp_ms = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        );
        self
    }

    /// 设置来源服务
    ///
    /// # 参数
    /// - `source`: 来源服务标识
    ///
    /// # 返回
    /// - `Self`: 构建器实例
    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    /// 构建 EventEnvelope
    ///
    /// # 返回
    /// - `EventEnvelope`: 事件信封实例
    pub fn build(self) -> EventEnvelope {
        let mut e = EventEnvelope::new(
            self.event_type,
            self.partition_key,
            self.seq,
            self.payload,
        );
        if let Some(ts) = self.timestamp_ms {
            e = e.with_timestamp_ms(ts);
        }
        if let Some(s) = self.source {
            e = e.with_source(s);
        }
        e
    }
}

// =============================================================================
// 便捷构建函数
// =============================================================================

/// 从 Protobuf 消息构建 EventEnvelope
///
/// # 参数
/// - `event_type`: 事件类型
/// - `partition_key`: 分区键
/// - `seq`: 序号
/// - `message`: Protobuf 消息
///
/// # 返回
/// - `Result<EventEnvelope>`: 事件信封实例
pub fn from_protobuf<M>(
    event_type: impl Into<String>,
    partition_key: impl Into<String>,
    seq: u64,
    message: &M,
) -> Result<EventEnvelope>
where
    M: Message,
{
    let payload = message.encode_to_vec();

    Ok(EventEnvelope::new(event_type, partition_key, seq, payload))
}

/// 从 Protobuf 消息构建 EventEnvelope（带时间戳）
///
/// # 参数
/// - `event_type`: 事件类型
/// - `partition_key`: 分区键
/// - `seq`: 序号
/// - `message`: Protobuf 消息
/// - `timestamp_ms`: 时间戳（毫秒）
///
/// # 返回
/// - `Result<EventEnvelope>`: 事件信封实例
pub fn from_protobuf_with_timestamp<M>(
    event_type: impl Into<String>,
    partition_key: impl Into<String>,
    seq: u64,
    message: &M,
    timestamp_ms: u64,
) -> Result<EventEnvelope>
where
    M: Message,
{
    let payload = message.encode_to_vec();

    Ok(EventEnvelope::new(event_type, partition_key, seq, payload)
        .with_timestamp_ms(timestamp_ms))
}

/// 从 Protobuf 消息构建 EventEnvelope（带来源）
///
/// # 参数
/// - `event_type`: 事件类型
/// - `partition_key`: 分区键
/// - `seq`: 序号
/// - `message`: Protobuf 消息
/// - `source`: 来源服务标识
///
/// # 返回
/// - `Result<EventEnvelope>`: 事件信封实例
pub fn from_protobuf_with_source<M>(
    event_type: impl Into<String>,
    partition_key: impl Into<String>,
    seq: u64,
    message: &M,
    source: impl Into<String>,
) -> Result<EventEnvelope>
where
    M: Message,
{
    let payload = message.encode_to_vec();

    Ok(EventEnvelope::new(event_type, partition_key, seq, payload)
        .with_source(source))
}

/// 从 Protobuf 消息构建 EventEnvelope（完整版）
///
/// # 参数
/// - `event_type`: 事件类型
/// - `partition_key`: 分区键
/// - `seq`: 序号
/// - `message`: Protobuf 消息
/// - `timestamp_ms`: 时间戳（毫秒）
/// - `source`: 来源服务标识
///
/// # 返回
/// - `Result<EventEnvelope>`: 事件信封实例
pub fn from_protobuf_full<M>(
    event_type: impl Into<String>,
    partition_key: impl Into<String>,
    seq: u64,
    message: &M,
    timestamp_ms: u64,
    source: impl Into<String>,
) -> Result<EventEnvelope>
where
    M: Message,
{
    let payload = message.encode_to_vec();

    Ok(EventEnvelope::new(event_type, partition_key, seq, payload)
        .with_timestamp_ms(timestamp_ms)
        .with_source(source))
}

/// 从 Protobuf 消息构建 EventEnvelope（使用当前时间戳）
///
/// # 参数
/// - `event_type`: 事件类型
/// - `partition_key`: 分区键
/// - `seq`: 序号
/// - `message`: Protobuf 消息
///
/// # 返回
/// - `Result<EventEnvelope>`: 事件信封实例
pub fn from_protobuf_with_current_timestamp<M>(
    event_type: impl Into<String>,
    partition_key: impl Into<String>,
    seq: u64,
    message: &M,
) -> Result<EventEnvelope>
where
    M: Message,
{
    let payload = message.encode_to_vec();

    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| anyhow::anyhow!("system clock error: {}", e))?
        .as_millis() as u64;
    
    Ok(EventEnvelope::new(event_type, partition_key, seq, payload)
        .with_timestamp_ms(timestamp_ms))
}
