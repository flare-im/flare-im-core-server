//! # Hook仓储接口
//!
//! 定义Hook配置的仓储接口

use crate::domain::model::HookConfig;

/// Hook配置仓储接口
pub trait HookConfigRepository: Send + Sync {
    /// 加载Hook配置
    async fn load(&self) -> flare_server_core::error::Result<HookConfig>;

    /// 保存Hook配置
    async fn save(&self, config: &HookConfig) -> flare_server_core::error::Result<()>;

    /// 监听配置变更
    async fn watch<F>(&self, callback: F) -> flare_server_core::error::Result<()>
    where
        F: Fn(HookConfig) + Send + Sync + 'static;
}

/// 租户投影仓储：按 version 幂等写入 `tenants` 表，并回答本地持有的版本。
///
/// 用 `async_trait`：实现被 tonic 服务持有并跨线程执行，future 必须是 `Send`。
#[async_trait::async_trait]
pub trait TenantProjectionRepository: Send + Sync {
    /// 仅当来件 `version` 大于本地版本时写入；返回是否接受与写入后的本地版本。
    async fn upsert_if_newer(
        &self,
        record: &crate::domain::model::TenantProjectionRecord,
    ) -> flare_server_core::error::Result<crate::domain::model::TenantProjectionOutcome>;

    /// 本地持有的租户版本；`tenant_ids` 为空表示全部。未投影的租户不出现在结果里。
    async fn versions(
        &self,
        tenant_ids: &[String],
    ) -> flare_server_core::error::Result<Vec<(String, u64)>>;
}
