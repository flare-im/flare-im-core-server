//! 领域层：推送任务信封元数据等纯函数与值对象。

pub mod model;
pub mod repository;
pub mod service;
pub mod push_metadata;

pub use push_metadata::merge_envelope_metadata;
pub use model::DeviceInfo;
pub use repository::OnlineStatusRepository;
pub use service::TargetResolver;
