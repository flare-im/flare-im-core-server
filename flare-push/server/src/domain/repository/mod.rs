//! 领域仓储接口（Port）

mod notify_policy_repository;
mod online_status_repository;

pub use notify_policy_repository::NotifyPolicyRepository;
pub use online_status_repository::OnlineStatusRepository;
