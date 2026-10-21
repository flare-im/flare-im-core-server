//! Hook 配置与能力策略的 PostgreSQL 持久化（与 `deploy/init.sql` 对齐）。

pub mod postgres_capability_audit;
pub mod postgres_capability_policy;
pub mod postgres_config;
pub mod postgres_hook_executions;
pub mod postgres_plugin_endpoints;
pub mod postgres_tenant_projection;

pub use postgres_capability_audit::PostgresCapabilityAuditLog;
pub use postgres_capability_policy::PostgresCapabilityPolicy;
pub use postgres_config::PostgresHookConfigRepository;
pub use postgres_hook_executions::PostgresHookExecutionRepository;
pub use postgres_plugin_endpoints::PostgresPluginEndpointRepository;
pub use postgres_tenant_projection::PostgresTenantProjectionRepository;

#[cfg(test)]
mod plugin_endpoints_pg_tests;
pub mod postgres_plugin_authorization;
pub mod postgres_resource_budget;
