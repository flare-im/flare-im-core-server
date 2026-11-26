//! 控制面仓储端口。

/// 租户投影仓储：按 version 幂等写入 `tenants` 表，并回答本地持有的版本。
///
/// 用 `async_trait`：实现被 tonic 服务持有并跨线程执行，future 必须是 `Send`。
#[async_trait::async_trait]
pub trait TenantProjectionRepository: Send + Sync {
    async fn replace_plugins(
        &self,
        _projection: &flare_im_core_capability_core::plugin::runtime::PluginProjection,
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

    /// 仅当来件 `version` 大于本地版本时写入 `tenant_client_policy`，并在同一事务里排队水位前进
    /// 欠下的租户通知（[`tenant_notice_outbox`]）；水位不回退。
    async fn upsert_client_policy(
        &self,
        record: &crate::domain::model::ClientPolicyRecord,
    ) -> flare_server_core::error::Result<crate::domain::model::TenantProjectionOutcome>;

    /// 本地持有的客户端策略版本；`app_ids` 为空表示全部。
    async fn client_policy_versions(
        &self,
        app_ids: &[String],
    ) -> flare_server_core::error::Result<Vec<(String, u64)>>;
}

/// Local, durable authorization. No Marketplace call is permitted on this port.
#[derive(Default)]
pub struct PluginEventDirectory {
    /// Desired, approved subscriptions, retained when their deployment is temporarily absent.
    pub subscriptions: std::collections::BTreeSet<(String, String)>,
    /// Enabled bindings whose release is not yet available. They retain retries without
    /// preventing other bindings from receiving the event.
    pub unavailable_grants: std::collections::BTreeSet<String>,
    /// Current routing facts. `None` cannot erase a desired subscription.
    pub routes: Option<
        std::sync::Arc<flare_im_core_capability_core::plugin::runtime::PluginRuntimeSnapshot>,
    >,
}

#[async_trait::async_trait]
pub trait PluginAuthorizationReader: Send + Sync {
    async fn event_directory(
        &self,
        _tenant: &str,
    ) -> flare_server_core::error::Result<PluginEventDirectory> {
        Err(flare_server_core::error::FlareError::system(
            "plugin event directory unavailable",
        ))
    }
    async fn plugin_snapshot(
        &self,
        tenant: &str,
    ) -> flare_server_core::error::Result<
        Option<
            std::sync::Arc<flare_im_core_capability_core::plugin::runtime::PluginRuntimeSnapshot>,
        >,
    >;
    /// Whether the application has a plugin projection its control plane stopped renewing:
    /// `plugin_snapshot` then answers `None`, yet nothing was uninstalled. Clients are told
    /// the plugins cannot be read for now, never that none is installed. A reader without
    /// durable projections has nothing to lapse.
    async fn plugin_projection_lapsed(
        &self,
        _tenant: &str,
    ) -> flare_server_core::error::Result<bool> {
        Ok(false)
    }
    /// Durable activation of the effective grant/configuration. A reader that cannot prove
    /// this boundary cannot disclose historical events. Lease renewal never advances it.
    async fn event_active_since_ms(
        &self,
        _tenant: &str,
        _grant: &str,
        _revision: u64,
        _configuration_digest: &str,
    ) -> flare_server_core::error::Result<Option<i64>> {
        Ok(None)
    }
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

/// Hook 执行证据的读与保留期端口（`hook_executions`）。写入方是 Ingest；这里是
/// `hook_config.get_statistics` / `query_executions` 与保留期清理，读都按租户与 Hook 限定。
#[async_trait::async_trait]
pub trait HookExecutionRepository: Send + Sync {
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
pub mod connection_builds;
pub mod entity_lifecycle;
pub mod plugin_card;
pub mod plugin_registry;
pub mod plugin_runtime;
pub mod plugin_state;
pub mod realtime_delivery;
pub mod resource_access;
pub mod service_dispatch;
pub mod tenant_notice;
pub mod tenant_notice_outbox;
