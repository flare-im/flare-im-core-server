//! Hook 配置与能力策略的 PostgreSQL 持久化（与 `deploy/init.sql` 对齐）。

pub mod postgres_capability_audit;
pub mod postgres_capability_policy;
pub mod postgres_config;

pub use postgres_capability_audit::PostgresCapabilityAuditLog;
pub use postgres_capability_policy::PostgresCapabilityPolicy;
pub use postgres_config::PostgresHookConfigRepository;
