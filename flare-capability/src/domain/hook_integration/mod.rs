//! # Hook 集成限界上下文（目标：gRPC / Webhook）
//!
//! Hook 的运行时在 `flare-im-core-hooks`，嵌在 Ingest 里；这里是控制面的规则：
//! - **本模块**：目标分类与写入校验（纯领域规则）。
//! - **接口** [`crate::interface::grpc`]：`CapabilityService.Administer`（配置 CRUD、统计、执行记录）。

mod policy;
mod transport;

pub use policy::validate_hook_item;
pub use transport::{HookTransportSurface, classify_transport};
