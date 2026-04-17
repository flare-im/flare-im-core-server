//! # Hook查询处理器（编排层）
//!
//! 负责处理查询，调用应用服务

use std::sync::Arc;

use crate::domain::model::HookStatistics;
use crate::infrastructure::monitoring::MetricsCollector;

/// Hook查询处理器（编排层）
pub struct HookQueryHandler {
    metrics_collector: Arc<MetricsCollector>,
}

impl HookQueryHandler {
    pub fn new(metrics_collector: Arc<MetricsCollector>) -> Self {
        Self { metrics_collector }
    }

    /// 处理获取Hook统计信息查询
    pub async fn handle_get_statistics(&self, hook_name: &str) -> Option<HookStatistics> {
        self.metrics_collector.get_statistics(hook_name).await
    }

    /// 处理获取所有Hook统计信息查询
    pub async fn handle_get_all_statistics(
        &self,
    ) -> std::collections::HashMap<String, HookStatistics> {
        self.metrics_collector.get_all_statistics().await
    }
}
