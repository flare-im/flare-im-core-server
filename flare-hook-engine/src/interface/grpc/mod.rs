//! gRPC 接口层：HookExtension / HookService 的 tonic 实现，入参映射为应用层调用。

mod hook_extension;
mod hook_service;

pub use hook_extension::HookExtensionServer;
pub use hook_service::HookServiceServer;
