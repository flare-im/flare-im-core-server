//! 性能监控指标模块
//! 
//! 提供关键性能指标的收集、统计和报告功能
//! 包括查询性能、缓存命中率、批量处理效率等指标

use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

use prometheus::{
    Histogram, HistogramOpts, IntCounter, IntGauge, Opts,
};
use tracing::info;

/// 性能指标收集器
pub struct PerformanceMetrics {
    // 查询性能指标
    pub query_duration: Histogram,
    pub query_count: IntCounter,
    pub query_errors: IntCounter,

    // 缓存性能指标
    pub cache_hits: IntCounter,
    pub cache_misses: IntCounter,
    pub cache_evictions: IntCounter,
    
    // 批量处理指标
    pub batch_query_size: Histogram,
    pub batch_query_duration: Histogram,
    
    // 连接池指标
    pub active_connections: IntGauge,
    pub idle_connections: IntGauge,
    
    // 系统资源指标
    pub memory_usage_bytes: IntGauge,
    pub cpu_usage_percent: IntGauge,
}

impl PerformanceMetrics {
    pub fn new() -> Result<Self, prometheus::Error> {
        let query_duration = Histogram::with_opts(
            HistogramOpts::new("message_query_duration_seconds", "Duration of message queries")
                .buckets(prometheus::exponential_buckets(0.0005, 2.0, 20)?),
        )?;

        let query_count = IntCounter::with_opts(
            Opts::new("message_queries_total", "Total number of message queries"),
        )?;

        let query_errors = IntCounter::with_opts(
            Opts::new("message_query_errors_total", "Total number of query errors"),
        )?;

        let cache_hits = IntCounter::with_opts(
            Opts::new("cache_hits_total", "Total number of cache hits"),
        )?;

        let cache_misses = IntCounter::with_opts(
            Opts::new("cache_misses_total", "Total number of cache misses"),
        )?;

        let cache_evictions = IntCounter::with_opts(
            Opts::new("cache_evictions_total", "Total number of cache evictions"),
        )?;

        let batch_query_size = Histogram::with_opts(
            HistogramOpts::new("batch_query_size", "Size of batch queries")
                .buckets(prometheus::exponential_buckets(1.0, 2.0, 10)?),
        )?;

        let batch_query_duration = Histogram::with_opts(
            HistogramOpts::new("batch_query_duration_seconds", "Duration of batch queries")
                .buckets(prometheus::exponential_buckets(0.001, 2.0, 20)?),
        )?;

        let active_connections = IntGauge::with_opts(
            Opts::new("active_connections", "Number of active database connections"),
        )?;

        let idle_connections = IntGauge::with_opts(
            Opts::new("idle_connections", "Number of idle database connections"),
        )?;

        let memory_usage_bytes = IntGauge::with_opts(
            Opts::new("memory_usage_bytes", "Current memory usage in bytes"),
        )?;

        let cpu_usage_percent = IntGauge::with_opts(
            Opts::new("cpu_usage_percent", "Current CPU usage percentage"),
        )?;

        // 注册指标
        prometheus::register(Box::new(query_duration.clone()))?;
        prometheus::register(Box::new(query_count.clone()))?;
        prometheus::register(Box::new(query_errors.clone()))?;
        prometheus::register(Box::new(cache_hits.clone()))?;
        prometheus::register(Box::new(cache_misses.clone()))?;
        prometheus::register(Box::new(cache_evictions.clone()))?;
        prometheus::register(Box::new(batch_query_size.clone()))?;
        prometheus::register(Box::new(batch_query_duration.clone()))?;
        prometheus::register(Box::new(active_connections.clone()))?;
        prometheus::register(Box::new(idle_connections.clone()))?;
        prometheus::register(Box::new(memory_usage_bytes.clone()))?;
        prometheus::register(Box::new(cpu_usage_percent.clone()))?;

        Ok(Self {
            query_duration,
            query_count,
            query_errors,
            cache_hits,
            cache_misses,
            cache_evictions,
            batch_query_size,
            batch_query_duration,
            active_connections,
            idle_connections,
            memory_usage_bytes,
            cpu_usage_percent,
        })
    }

    /// 记录单次查询的性能指标
    pub fn record_query(&self, duration: Duration, success: bool) {
        self.query_duration.observe(duration.as_secs_f64());
        self.query_count.inc();
        
        if !success {
            self.query_errors.inc();
        }
    }

    /// 记录缓存命中
    pub fn record_cache_hit(&self) {
        self.cache_hits.inc();
    }

    /// 记录缓存未命中
    pub fn record_cache_miss(&self) {
        self.cache_misses.inc();
    }

    /// 记录缓存驱逐
    pub fn record_cache_eviction(&self) {
        self.cache_evictions.inc();
    }

    /// 记录批量查询的性能指标
    pub fn record_batch_query(&self, size: usize, duration: Duration) {
        self.batch_query_size.observe(size as f64);
        self.batch_query_duration.observe(duration.as_secs_f64());
    }

    /// 更新连接池指标
    pub fn update_connection_pool_metrics(&self, active: u32, idle: u32) {
        self.active_connections.set(active as i64);
        self.idle_connections.set(idle as i64);
    }

    /// 更新内存使用情况
    pub fn update_memory_usage(&self, bytes: u64) {
        self.memory_usage_bytes.set(bytes as i64);
    }

    /// 更新CPU使用情况
    pub fn update_cpu_usage(&self, percent: f64) {
        self.cpu_usage_percent.set(percent as i64);
    }

    /// 获取缓存命中率
    pub fn get_cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.get();
        let misses = self.cache_misses.get();
        
        if hits + misses == 0 {
            0.0
        } else {
            hits as f64 / (hits + misses) as f64
        }
    }

    /// 获取查询错误率
    pub fn get_error_rate(&self) -> f64 {
        let total = self.query_count.get();
        let errors = self.query_errors.get();
        
        if total == 0 {
            0.0
        } else {
            errors as f64 / total as f64
        }
    }

    /// 打印性能统计摘要
    pub fn print_performance_summary(&self) {
        let hit_rate = self.get_cache_hit_rate();
        let error_rate = self.get_error_rate();
        
        info!(
            "Performance Summary: Cache Hit Rate: {:.2}%, Error Rate: {:.2}%, Total Queries: {}",
            hit_rate * 100.0,
            error_rate * 100.0,
            self.query_count.get()
        );
    }
}

/// 查询性能监控器
pub struct QueryPerformanceMonitor {
    metrics: Arc<PerformanceMetrics>,
    start_time: Instant,
    #[allow(dead_code)]
    query_type: String,
}

impl QueryPerformanceMonitor {
    pub fn new(metrics: Arc<PerformanceMetrics>, query_type: String) -> Self {
        Self {
            metrics,
            start_time: Instant::now(),
            query_type, // 用于日志或扩展
        }
    }

    pub fn finish(&self, success: bool) {
        let duration = self.start_time.elapsed();
        self.metrics.record_query(duration, success);
    }
}

/// 批量查询性能监控器
pub struct BatchQueryPerformanceMonitor {
    metrics: Arc<PerformanceMetrics>,
    start_time: Instant,
    size: usize,
}

impl BatchQueryPerformanceMonitor {
    pub fn new(metrics: Arc<PerformanceMetrics>, size: usize) -> Self {
        Self {
            metrics,
            start_time: Instant::now(),
            size,
        }
    }

    pub fn finish(&self) {
        let duration = self.start_time.elapsed();
        self.metrics.record_batch_query(self.size, duration);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let metrics = PerformanceMetrics::new().expect("Failed to create metrics");
        assert_eq!(metrics.query_count.get(), 0);
        assert_eq!(metrics.cache_hits.get(), 0);
    }

    #[test]
    fn test_cache_hit_rate() {
        let metrics = PerformanceMetrics::new().expect("Failed to create metrics");
        
        metrics.record_cache_hit();
        metrics.record_cache_hit();
        metrics.record_cache_miss();
        
        assert_eq!(metrics.get_cache_hit_rate(), 0.6666666666666666);
    }
}