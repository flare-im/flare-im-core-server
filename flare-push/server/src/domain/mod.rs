//! 领域层：推送任务信封元数据、提示规则的输入换算等纯函数与值对象。

pub mod model;
pub mod notify_inputs;
pub mod push_metadata;
pub mod repository;

pub use push_metadata::merge_envelope_metadata;
