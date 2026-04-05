//! RPC 客户端抽象层
//!
//! 本模块提供框架无关的 RPC 客户端实现，
//! 支持未来切换不同的 RPC 框架（如从 tonic 切换到 volo）。

mod impl_;

pub use impl_::StorageReaderClient;
