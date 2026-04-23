//! Hook **Query** 编排：读模型经基础设施 [`crate::infrastructure::monitoring::MetricsCollector`] 提供（后续可下沉为领域读端口）。

use std::sync::Arc;

use crate::domain::model::HookStatistics;
use crate::infrastructure::monitoring::MetricsCollector;

/// Hook 查询处理器（应用层编排读路径）。
pub struct HookQueryHandler {
    metrics_collector: Arc<MetricsCollector>,
}

impl HookQueryHandler {
    pub fn new(metrics_collector: Arc<MetricsCollector>) -> Self {
        Self { metrics_collector }
    }

    /// 处理获取 Hook 统计信息查询
    pub async fn handle_get_statistics(&self, hook_name: &str) -> Option<HookStatistics> {
        self.metrics_collector.get_statistics(hook_name).await
    }

    /// 处理获取所有 Hook 统计信息查询
    pub async fn handle_get_all_statistics(
        &self,
    ) -> std::collections::HashMap<String, HookStatistics> {
        self.metrics_collector.get_all_statistics().await
    }
}
