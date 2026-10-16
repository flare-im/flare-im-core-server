//! Hook **Query** 编排：执行统计与执行记录经领域读端口 [`HookExecutionRepository`] 读
//! `hook_executions`（按租户与 Hook 限定，计数按采样权重加权）。

use std::sync::Arc;

use flare_server_core::error::Result;

use crate::domain::model::{
    HookExecutionFilter, HookExecutionScope, HookExecutionStatistics, HookExecutionTimeRange,
    StoredHookExecution,
};
use crate::domain::repository::HookExecutionRepository;

/// Hook 查询处理器（应用层编排读路径）。
pub struct HookQueryHandler {
    repository: Arc<dyn HookExecutionRepository>,
}

impl HookQueryHandler {
    pub fn new(repository: Arc<dyn HookExecutionRepository>) -> Self {
        Self { repository }
    }

    /// 一个租户的一条 Hook 在一段时间里的执行统计。
    pub async fn statistics(
        &self,
        scope: &HookExecutionScope,
        range: &HookExecutionTimeRange,
    ) -> Result<HookExecutionStatistics> {
        self.repository.statistics(scope, range).await
    }

    /// 一个租户的一条 Hook 的执行记录，从新到旧。
    pub async fn executions(
        &self,
        filter: &HookExecutionFilter,
    ) -> Result<Vec<StoredHookExecution>> {
        self.repository.query(filter).await
    }
}
