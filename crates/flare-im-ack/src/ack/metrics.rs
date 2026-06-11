//! ACK模块监控指标
//! 提供ACK处理的性能监控和指标收集

use prometheus::{
    Encoder, Gauge, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    Opts, Registry, TextEncoder,
};
use std::sync::Arc;
use zstd;

/// ACK监控指标
#[derive(Clone)]
pub struct AckMetrics {
    /// 总ACK处理数
    pub total_acks_processed: IntCounter,
    /// ACK持久化数
    pub acks_persisted: IntCounter,
    /// ACK缓存数
    pub acks_cached: IntCounter,
    /// 批处理延迟直方图
    pub batch_processing_latency: Histogram,
    /// 缓存命中率
    pub cache_hit_rate: Gauge,
    /// Redis连接数
    pub redis_connections: IntGauge,
    /// 批处理队列大小
    pub batch_queue_size: IntGauge,
    /// 高优先级队列大小
    pub high_priority_queue_size: IntGauge,
    /// 按重要性分类的ACK处理数
    pub acks_by_importance: IntCounterVec,
    /// 按类型分类的ACK处理数
    pub acks_by_type: IntCounterVec,
    /// ACK处理错误数
    pub ack_processing_errors: IntCounter,
    /// Redis操作延迟
    pub redis_operation_latency: HistogramVec,
    /// ACK超时次数
    pub ack_timeouts: IntCounter,
    /// ACK重试次数
    pub ack_retries: IntCounter,
    /// ACK处理延迟分布
    pub ack_processing_latency: HistogramVec,
    /// 内存使用情况
    pub memory_usage_bytes: IntGauge,
    /// 不同重要性级别的处理延迟
    pub ack_processing_latency_by_importance: HistogramVec,
}

impl AckMetrics {
    /// 创建新的ACK监控指标
    pub fn new(registry: &Registry) -> Result<Self, Box<dyn std::error::Error>> {
        let total_acks_processed =
            IntCounter::new("ack_total_processed", "Total number of ACKs processed")?;

        let acks_persisted =
            IntCounter::new("ack_persisted", "Number of ACKs persisted to storage")?;

        let acks_cached = IntCounter::new("ack_cached", "Number of ACKs cached in memory")?;

        let batch_processing_latency = Histogram::with_opts(HistogramOpts::new(
            "ack_batch_processing_duration_seconds",
            "Batch processing latency in seconds",
        ))?;

        let cache_hit_rate = Gauge::new("ack_cache_hit_rate", "Cache hit rate percentage")?;

        let redis_connections = IntGauge::new(
            "ack_redis_connections",
            "Number of active Redis connections",
        )?;

        let batch_queue_size = IntGauge::new("ack_batch_queue_size", "Current batch queue size")?;

        let high_priority_queue_size = IntGauge::new(
            "ack_high_priority_queue_size",
            "Current high priority queue size",
        )?;

        let acks_by_importance = IntCounterVec::new(
            Opts::new(
                "ack_processed_by_importance",
                "Number of ACKs processed by importance level",
            ),
            &["importance"],
        )?;

        let acks_by_type = IntCounterVec::new(
            Opts::new("ack_processed_by_type", "Number of ACKs processed by type"),
            &["type"],
        )?;

        let ack_processing_errors = IntCounter::new(
            "ack_processing_errors_total",
            "Total number of ACK processing errors",
        )?;

        let redis_operation_latency = HistogramVec::new(
            HistogramOpts::new(
                "ack_redis_operation_duration_seconds",
                "Redis operation latency in seconds",
            ),
            &["operation"],
        )?;

        let ack_timeouts = IntCounter::new("ack_timeouts_total", "Total number of ACK timeouts")?;

        let ack_retries = IntCounter::new("ack_retries_total", "Total number of ACK retries")?;

        let ack_processing_latency = HistogramVec::new(
            HistogramOpts::new(
                "ack_processing_duration_seconds",
                "ACK processing latency distribution in seconds",
            ),
            &["type"],
        )?;

        let memory_usage_bytes =
            IntGauge::new("ack_memory_usage_bytes", "Current memory usage in bytes")?;

        let ack_processing_latency_by_importance = HistogramVec::new(
            HistogramOpts::new(
                "ack_processing_latency_by_importance",
                "ACK processing latency by importance level",
            ),
            &["importance"],
        )?;

        registry.register(Box::new(total_acks_processed.clone()))?;
        registry.register(Box::new(acks_persisted.clone()))?;
        registry.register(Box::new(acks_cached.clone()))?;
        registry.register(Box::new(batch_processing_latency.clone()))?;
        registry.register(Box::new(cache_hit_rate.clone()))?;
        registry.register(Box::new(redis_connections.clone()))?;
        registry.register(Box::new(batch_queue_size.clone()))?;
        registry.register(Box::new(high_priority_queue_size.clone()))?;
        registry.register(Box::new(acks_by_importance.clone()))?;
        registry.register(Box::new(acks_by_type.clone()))?;
        registry.register(Box::new(ack_processing_errors.clone()))?;
        registry.register(Box::new(redis_operation_latency.clone()))?;
        registry.register(Box::new(ack_timeouts.clone()))?;
        registry.register(Box::new(ack_retries.clone()))?;
        registry.register(Box::new(ack_processing_latency.clone()))?;
        registry.register(Box::new(memory_usage_bytes.clone()))?;
        registry.register(Box::new(ack_processing_latency_by_importance.clone()))?;

        Ok(Self {
            total_acks_processed,
            acks_persisted,
            acks_cached,
            batch_processing_latency,
            cache_hit_rate,
            redis_connections,
            batch_queue_size,
            high_priority_queue_size,
            acks_by_importance,
            acks_by_type,
            ack_processing_errors,
            redis_operation_latency,
            ack_timeouts,
            ack_retries,
            ack_processing_latency,
            memory_usage_bytes,
            ack_processing_latency_by_importance,
        })
    }

    /// 记录ACK处理
    pub fn record_ack_processed(
        &self,
        persisted: bool,
        cached: bool,
        importance: &str,
        ack_type: &str,
    ) {
        self.total_acks_processed.inc();

        if persisted {
            self.acks_persisted.inc();
        }

        if cached {
            self.acks_cached.inc();
        }

        self.acks_by_importance
            .with_label_values(&[importance])
            .inc();
        self.acks_by_type.with_label_values(&[ack_type]).inc();
    }

    /// 记录ACK处理延迟
    pub fn record_ack_processing_latency(&self, ack_type: &str, duration: f64) {
        self.ack_processing_latency
            .with_label_values(&[ack_type])
            .observe(duration);
    }

    /// 记录不同重要性级别的处理延迟
    pub fn record_ack_processing_latency_by_importance(&self, importance: &str, duration: f64) {
        self.ack_processing_latency_by_importance
            .with_label_values(&[importance])
            .observe(duration);
    }

    /// 记录批处理延迟
    pub fn record_batch_processing_latency(&self, duration: f64) {
        self.batch_processing_latency.observe(duration);
    }

    /// 更新缓存命中率
    pub fn update_cache_hit_rate(&self, hit_rate: f64) {
        self.cache_hit_rate.set(hit_rate);
    }

    /// 更新Redis连接数
    pub fn update_redis_connections(&self, count: i64) {
        self.redis_connections.set(count);
    }

    /// 更新批处理队列大小
    pub fn update_batch_queue_size(&self, size: i64) {
        self.batch_queue_size.set(size);
    }

    /// 更新高优先级队列大小
    pub fn update_high_priority_queue_size(&self, size: i64) {
        self.high_priority_queue_size.set(size);
    }

    /// 更新内存使用情况
    pub fn update_memory_usage(&self, bytes: i64) {
        self.memory_usage_bytes.set(bytes);
    }

    /// 记录ACK处理错误
    pub fn record_ack_processing_error(&self, error_type: &str) {
        self.ack_processing_errors.inc();
        self.acks_by_type.with_label_values(&[error_type]).inc();
    }

    /// 记录Redis操作延迟
    pub fn record_redis_operation_latency(&self, operation: &str, duration: f64) {
        self.redis_operation_latency
            .with_label_values(&[operation])
            .observe(duration);
    }

    /// 计算缓存命中率
    pub fn calculate_cache_hit_rate(&self) -> f64 {
        let cached = self.acks_cached.get();
        let total = self.total_acks_processed.get();

        if total == 0 {
            0.0
        } else {
            (cached as f64) / (total as f64) * 100.0
        }
    }

    /// 获取指标数据
    pub fn get_metrics_data(
        &self,
        registry: &Registry,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        let metric_families = registry.gather();
        encoder.encode(&metric_families, &mut buffer)?;
        Ok(String::from_utf8(buffer)?)
    }

    /// 记录ACK超时
    pub fn record_ack_timeout(&self) {
        self.ack_timeouts.inc();
    }

    /// 记录ACK重试
    pub fn record_ack_retry(&self) {
        self.ack_retries.inc();
    }
}

/// 性能优化配置
#[derive(Debug, Clone)]
pub struct PerformanceConfig {
    /// 是否启用压缩
    pub enable_compression: bool,
    /// 压缩阈值（字节）
    pub compression_threshold: usize,
    /// 最大并发处理数
    pub max_concurrent: usize,
    /// 批处理大小
    pub batch_size: usize,
    /// 内存缓存容量
    pub cache_capacity: usize,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            enable_compression: true,
            compression_threshold: 1024,
            max_concurrent: 100,
            batch_size: 100,
            cache_capacity: 10000,
        }
    }
}

/// ACK性能优化器
pub struct AckPerformanceOptimizer {
    /// 性能配置
    config: PerformanceConfig,
    /// 监控指标
    metrics: Arc<AckMetrics>,
}

impl AckPerformanceOptimizer {
    /// 创建新的性能优化器
    pub fn new(config: PerformanceConfig, metrics: Arc<AckMetrics>) -> Self {
        Self { config, metrics }
    }

    /// 优化ACK数据大小
    pub fn optimize_ack_data_size(
        &self,
        data: &[u8],
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        if self.config.enable_compression && data.len() > self.config.compression_threshold {
            // 使用压缩算法优化数据大小
            let compressed = zstd::encode_all(data, 3)?;
            self.metrics
                .record_ack_processed(false, true, "high", "compressed");
            Ok(compressed)
        } else {
            Ok(data.to_vec())
        }
    }

    /// 限制并发处理
    pub fn limit_concurrent_processing<F, R>(&self, func: F) -> R
    where
        F: FnOnce() -> R,
    {
        // 这里可以实现更复杂的并发控制逻辑
        // 例如使用信号量限制并发数
        func()
    }

    /// 获取性能配置
    pub fn get_config(&self) -> &PerformanceConfig {
        &self.config
    }

    /// 更新性能配置
    pub fn update_config(&mut self, new_config: PerformanceConfig) {
        self.config = new_config;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::Registry;
    // use std::thread;
    // use std::time::Duration;

    #[test]
    fn test_ack_metrics() -> Result<(), Box<dyn std::error::Error>> {
        let registry = Registry::new();
        let metrics = AckMetrics::new(&registry)?;

        // 记录一些ACK处理
        metrics.record_ack_processed(true, true, "high", "client");
        metrics.record_ack_processed(false, true, "medium", "push");
        metrics.record_ack_processed(false, true, "low", "storage");

        // 记录处理延迟
        metrics.record_ack_processing_latency("client", 0.05);
        metrics.record_ack_processing_latency_by_importance("high", 0.05);

        // 更新队列大小
        metrics.update_batch_queue_size(10);
        metrics.update_high_priority_queue_size(5);
        metrics.update_memory_usage(1024 * 1024);

        // 检查计数器
        assert_eq!(metrics.total_acks_processed.get(), 3);
        assert_eq!(metrics.acks_persisted.get(), 1);
        assert_eq!(metrics.acks_cached.get(), 3);

        // 计算缓存命中率
        let hit_rate = metrics.calculate_cache_hit_rate();
        assert_eq!(hit_rate, 100.0);

        // 获取指标数据
        let metrics_data = metrics.get_metrics_data(&registry)?;
        assert!(!metrics_data.is_empty());

        Ok(())
    }

    #[test]
    fn test_performance_optimizer() -> Result<(), Box<dyn std::error::Error>> {
        let registry = Registry::new();
        let metrics = Arc::new(AckMetrics::new(&registry)?);
        let config = PerformanceConfig::default();
        let optimizer = AckPerformanceOptimizer::new(config, metrics);

        // 测试数据优化
        let small_data = vec![1u8; 100];
        let optimized_small = optimizer.optimize_ack_data_size(&small_data)?;
        assert_eq!(small_data, optimized_small);

        let large_data = vec![1u8; 2048];
        let optimized_large = optimizer.optimize_ack_data_size(&large_data)?;
        assert!(optimized_large.len() < large_data.len());

        // 测试并发限制
        let result = optimizer.limit_concurrent_processing(|| 42);
        assert_eq!(result, 42);

        Ok(())
    }
}
