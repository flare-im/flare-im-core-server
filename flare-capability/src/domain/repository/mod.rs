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
    async fn replace_plugins(
        &self,
        _snapshot: &flare_im_capability_core::plugin::runtime::PluginRuntimeSnapshot,
    ) -> flare_server_core::error::Result<crate::domain::model::TenantProjectionOutcome> {
        Err(flare_server_core::error::FlareError::system(
            "plugin projection store unavailable",
        ))
    }
    async fn plugin_versions(
        &self,
        _app_ids: &[String],
    ) -> flare_server_core::error::Result<Vec<(String, u64)>> {
        Ok(vec![])
    }
    /// Validated full app snapshot; replace keys and version atomically. Empty
    /// snapshots keep their watermarks, and old/equal versions do nothing.
    async fn replace_credentials(
        &self,
        snapshot: &flare_server_core::auth::credential_snapshot::CredentialSnapshot,
    ) -> flare_server_core::error::Result<crate::domain::model::TenantProjectionOutcome>;

    async fn credential_versions(
        &self,
        app_ids: &[String],
    ) -> flare_server_core::error::Result<Vec<(String, u64)>>;

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

/// Local, durable authorization. No Marketplace call is permitted on this port.
#[async_trait::async_trait]
pub trait PluginAuthorizationReader: Send + Sync {
    async fn plugin_snapshot(
        &self,
        tenant: &str,
    ) -> flare_server_core::error::Result<
        Option<flare_im_capability_core::plugin::runtime::PluginRuntimeSnapshot>,
    >;
}

/// 租户运行时只读（管理员查看本进程持有的投影）。与写路径分开，读侧不需要写权限。
#[async_trait::async_trait]
pub trait TenantRuntimeReader: Send + Sync {
    /// 本地 `tenants` 表里的该租户；没有这一行返回 `None`。
    async fn tenant_runtime(
        &self,
        tenant_id: &str,
    ) -> flare_server_core::error::Result<Option<crate::domain::model::TenantRuntimeView>>;
}

/// 经 `RegisterPluginEndpoint` 登记的插件端点的持久化端口（`capability_plugin_endpoints`）。
/// 读模型仍是进程内的 `PluginRouteBook`：启动时整表载入、登记/注销先写这里、按周期重读。
#[async_trait::async_trait]
pub trait PluginEndpointRepository: Send + Sync {
    /// 写入或覆盖一条登记；首次登记时间保留，`updated_at` 取当前时间。
    async fn upsert(
        &self,
        endpoint: &crate::domain::model::PluginEndpoint,
    ) -> flare_server_core::error::Result<()>;

    /// 删除一条登记；返回是否删到了行。
    async fn delete(
        &self,
        tenant_id: &str,
        plugin_id: &str,
    ) -> flare_server_core::error::Result<bool>;

    /// 全部登记，按 `(tenant_id, plugin_id)` 排序。
    async fn list_all(
        &self,
    ) -> flare_server_core::error::Result<Vec<crate::domain::model::PluginEndpoint>>;

    /// 把这些登记的 `last_healthy_at` 记为当前时间（一条语句）；返回更新的行数。
    async fn record_healthy(
        &self,
        keys: &[crate::domain::model::PluginEndpointKey],
    ) -> flare_server_core::error::Result<u64>;

    /// 删除 `cutoff` 之后既没重新登记、也没有健康检查通过的登记；返回删掉的键。
    async fn delete_stale(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
    ) -> flare_server_core::error::Result<Vec<crate::domain::model::PluginEndpointKey>>;
}

/// Hook 执行证据的持久化端口（`hook_executions`）。写入方是后台的批量写入任务，读取方是
/// `hook_config.get_statistics` / `query_executions`；读写都按租户与 Hook 限定。
#[async_trait::async_trait]
pub trait HookExecutionRepository: Send + Sync {
    /// 一次写入一批记录，返回写入的行数。
    async fn insert_batch(
        &self,
        records: &[crate::domain::model::HookExecutionRecord],
    ) -> flare_server_core::error::Result<u64>;

    /// 删除执行时间早于 `cutoff` 的至多 `limit` 行（最旧的先删），返回删除的行数。
    async fn delete_executed_before(
        &self,
        cutoff: std::time::SystemTime,
        limit: u32,
    ) -> flare_server_core::error::Result<u64>;

    /// 一个租户的一条 Hook 在一段时间里的统计（按 `sample_weight` 加权）。
    async fn statistics(
        &self,
        scope: &crate::domain::model::HookExecutionScope,
        range: &crate::domain::model::HookExecutionTimeRange,
    ) -> flare_server_core::error::Result<crate::domain::model::HookExecutionStatistics>;

    /// 按条件查执行记录，从新到旧，至多 `filter.limit` 条。
    async fn query(
        &self,
        filter: &crate::domain::model::HookExecutionFilter,
    ) -> flare_server_core::error::Result<Vec<crate::domain::model::StoredHookExecution>>;
}
pub mod entity_lifecycle;
pub mod plugin_card;
pub mod plugin_state;
pub mod realtime_delivery;
pub mod resource_access;
