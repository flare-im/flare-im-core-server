//! 领域层模块
//!
//! 与 flare-storage 一致：model、repository、service；online 扩展 aggregate、event、value_object。
//! connection_event_publisher 与 flare_im_contracts Connection BC 对齐，可选发布 ConnectionEvent。
//! device_handoff 是网关失联时把设备交接给推送通道的出站端口；push_binding 是服务端有意结束
//! 会话时吊销推送绑定的出口与出站端口。

pub mod aggregate;
pub mod connection_event_publisher;
pub mod device_handoff;
pub mod event;
pub mod model;
pub mod push_binding;
pub mod repository;
pub mod service;
pub mod value_object;

pub use connection_event_publisher::{ConnectionEventPublisher, NoopConnectionEventPublisher};
pub use device_handoff::{DeviceHandoffReporter, GatewayLostHandoff};
pub use push_binding::{
    BindingRevocation, BindingRevocationReason, BindingRevocationSink, PushBindingRevoker,
};
