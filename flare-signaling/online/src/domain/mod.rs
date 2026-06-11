//! 领域层模块
//!
//! 与 flare-storage 一致：model、repository、service；online 扩展 aggregate、event、value_object。
//! connection_event_publisher 与 flare_im_contracts Connection BC 对齐，可选发布 ConnectionEvent。

pub mod aggregate;
pub mod connection_event_publisher;
pub mod event;
pub mod model;
pub mod repository;
pub mod service;
pub mod value_object;

pub use connection_event_publisher::{ConnectionEventPublisher, NoopConnectionEventPublisher};
