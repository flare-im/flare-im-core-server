//! Flare Push Worker
//!
//! 以设计文档为准：
//! - 消费 `push-online` 与离线任务的三条优先级通道（`offline`、`offline_group`、`offline_bulk`），
//!   每个主题一个消费者、各自的消费组与并发
//! - 在线：查 Online 设备、按网关定位后由 `GatewayRouter` 直推 Access Gateway（与 access_gateway 对齐）
//! - 离线：一条任务对应一台设备，按绑定偏好顺序只经其中一条绑定投递（厂商熔断、限流或可重试失败时
//!   换下一条；都被限流时按设备攒成一条摘要），并按路由给的处置决定响不响、覆盖哪条通知。显式选择 FCM/APNs/Getui 或多通道 mobile；默认 disabled，生产环境必须显式配置
//!   后端且不接受 outbox。Redis outbox 只属于开发暂存，不冒充手机投递。
//! - 失败：可重试错误 Nack 由 JetStream 重投；终态错误转入 `push-dlq`

pub mod application;
pub mod config;
pub mod domain;
pub mod infrastructure;
pub mod interface;
pub mod service;

pub use service::ApplicationBootstrap;
