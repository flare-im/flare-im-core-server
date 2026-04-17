//! # Hook监控统计层
//!
//! 提供Hook执行指标收集、执行记录和告警触发能力。

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::warn;

use crate::domain::model::{HookExecutionResult, HookStatistics};

/// 指标收集器
pub struct MetricsCollector {
    statistics: Arc<RwLock<HashMap<String, HookStatistics>>>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            statistics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 记录Hook执行结果
    pub async fn record(&self, result: &HookExecutionResult) {
        let mut stats = self.statistics.write().await;
        let hook_stats = stats
            .entry(result.hook_name.clone())
            .or_insert_with(Default::default);
        hook_stats.update(result);
    }

    /// 获取Hook统计信息
    pub async fn get_statistics(&self, hook_name: &str) -> Option<HookStatistics> {
        let stats = self.statistics.read().await;
        stats.get(hook_name).cloned()
    }

    /// 获取所有Hook统计信息
    pub async fn get_all_statistics(&self) -> HashMap<String, HookStatistics> {
        let stats = self.statistics.read().await;
        stats.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// 执行记录器
pub struct ExecutionRecorder {
    records: Arc<RwLock<Vec<HookExecutionResult>>>,
    max_records: usize,
}

impl ExecutionRecorder {
    pub fn new() -> Self {
        Self {
            records: Arc::new(RwLock::new(Vec::new())),
            max_records: 10000, // 最多保留10000条记录
        }
    }

    /// 记录Hook执行结果
    pub async fn record(&self, result: &HookExecutionResult) {
        let mut records = self.records.write().await;
        records.push(result.clone());

        // 限制记录数量
        if records.len() > self.max_records {
            records.remove(0);
        }
    }

    /// 查询执行记录
    pub async fn query(&self, hook_name: Option<&str>, limit: usize) -> Vec<HookExecutionResult> {
        let records = self.records.read().await;

        let mut filtered: Vec<_> = records
            .iter()
            .filter(|r| {
                if let Some(name) = hook_name {
                    r.hook_name == name
                } else {
                    true
                }
            })
            .cloned()
            .collect();

        // 按时间倒序排序
        filtered.sort_by(|a, b| b.executed_at.cmp(&a.executed_at));

        filtered.into_iter().take(limit).collect()
    }
}

impl Default for ExecutionRecorder {
    fn default() -> Self {
        Self::new()
    }
}

/// 告警触发器
pub struct AlertTrigger {
    failure_rate_threshold: f64,
    latency_threshold_ms: u64,
}

impl AlertTrigger {
    pub fn new(failure_rate_threshold: f64, latency_threshold_ms: u64) -> Self {
        Self {
            failure_rate_threshold,
            latency_threshold_ms,
        }
    }

    /// 检查是否需要告警
    pub async fn check(&self, collector: &MetricsCollector) {
        let stats = collector.get_all_statistics().await;

        for (hook_name, hook_stats) in stats {
            // 检查失败率
            let failure_rate = 1.0 - hook_stats.success_rate();
            if failure_rate > self.failure_rate_threshold {
                warn!(
                    hook = %hook_name,
                    failure_rate = %failure_rate,
                    threshold = %self.failure_rate_threshold,
                    "Hook failure rate exceeds threshold"
                );
            }

            // 检查平均延迟
            if hook_stats.avg_latency_ms > self.latency_threshold_ms as f64 {
                warn!(
                    hook = %hook_name,
                    avg_latency_ms = hook_stats.avg_latency_ms,
                    threshold_ms = self.latency_threshold_ms,
                    "Hook average latency exceeds threshold"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn create_test_result(name: &str, success: bool, latency_ms: u64) -> HookExecutionResult {
        HookExecutionResult {
            hook_name: name.to_string(),
            executed_at: SystemTime::now(),
            success,
            latency_ms,
            error_message: if success {
                None
            } else {
                Some("test error".to_string())
            },
        }
    }

    #[tokio::test]
    async fn test_metrics_collector() {
        let collector = MetricsCollector::new();

        // 记录成功结果
        collector
            .record(&create_test_result("test-hook", true, 100))
            .await;
        let stats = collector.get_statistics("test-hook").await.unwrap();
        assert_eq!(stats.total_count, 1);
        assert_eq!(stats.success_count, 1);
        assert_eq!(stats.failure_count, 0);
        assert_eq!(stats.avg_latency_ms, 100.0);
        assert_eq!(stats.success_rate(), 1.0);

        // 记录失败结果
        collector
            .record(&create_test_result("test-hook", false, 200))
            .await;
        let stats = collector.get_statistics("test-hook").await.unwrap();
        assert_eq!(stats.total_count, 2);
        assert_eq!(stats.success_count, 1);
        assert_eq!(stats.failure_count, 1);
        assert_eq!(stats.avg_latency_ms, 150.0);
        assert_eq!(stats.success_rate(), 0.5);
        assert_eq!(stats.max_latency_ms, 200);
        assert_eq!(stats.min_latency_ms, 100);
    }

    #[tokio::test]
    async fn test_metrics_collector_multiple_hooks() {
        let collector = MetricsCollector::new();

        collector
            .record(&create_test_result("hook-1", true, 100))
            .await;
        collector
            .record(&create_test_result("hook-2", true, 200))
            .await;
        collector
            .record(&create_test_result("hook-1", false, 150))
            .await;

        let all_stats = collector.get_all_statistics().await;
        assert_eq!(all_stats.len(), 2);

        let hook1_stats = all_stats.get("hook-1").unwrap();
        assert_eq!(hook1_stats.total_count, 2);
        assert_eq!(hook1_stats.success_count, 1);

        let hook2_stats = all_stats.get("hook-2").unwrap();
        assert_eq!(hook2_stats.total_count, 1);
        assert_eq!(hook2_stats.success_count, 1);
    }

    #[tokio::test]
    async fn test_execution_recorder() {
        let recorder = ExecutionRecorder::new();

        // 记录执行结果
        recorder
            .record(&create_test_result("test-hook", true, 100))
            .await;
        recorder
            .record(&create_test_result("test-hook", false, 200))
            .await;
        recorder
            .record(&create_test_result("other-hook", true, 150))
            .await;

        // 查询所有记录
        let all_records = recorder.query(None, 10).await;
        assert_eq!(all_records.len(), 3);

        // 查询特定Hook的记录
        let hook_records = recorder.query(Some("test-hook"), 10).await;
        assert_eq!(hook_records.len(), 2);
        assert!(hook_records.iter().all(|r| r.hook_name == "test-hook"));

        // 测试限制数量
        let limited = recorder.query(None, 2).await;
        assert_eq!(limited.len(), 2);
    }

    #[tokio::test]
    async fn test_execution_recorder_max_records() {
        let recorder = ExecutionRecorder::new();

        // 记录超过最大数量的记录
        for i in 0..15000 {
            recorder
                .record(&create_test_result("test-hook", true, i as u64))
                .await;
        }

        let records = recorder.query(None, 20000).await;
        assert_eq!(records.len(), 10000); // 应该被限制在 max_records
    }

    #[tokio::test]
    async fn test_alert_trigger() {
        let collector = MetricsCollector::new();

        // 创建高失败率的情况
        for _ in 0..10 {
            collector
                .record(&create_test_result("test-hook", true, 100))
                .await;
        }
        for _ in 0..10 {
            collector
                .record(&create_test_result("test-hook", false, 200))
                .await;
        }

        // 创建低失败率的情况
        for _ in 0..10 {
            collector
                .record(&create_test_result("good-hook", true, 50))
                .await;
        }

        // 检查告警（失败率阈值 0.5）
        let trigger = AlertTrigger::new(0.5, 1000);
        trigger.check(&collector).await; // 应该警告 test-hook
    }

    #[tokio::test]
    async fn test_metrics_collector_not_found() {
        let collector = MetricsCollector::new();
        let stats = collector.get_statistics("non-existent").await;
        assert!(stats.is_none());
    }
}
