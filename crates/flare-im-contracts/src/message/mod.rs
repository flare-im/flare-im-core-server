//! 统一消息领域模型与 Proto 转换
//!
//! 供 storage writer/reader、orchestrator、hook 等在与 proto 边界处统一使用。

mod convert;
mod model;

pub use convert::{message_from_proto, message_to_proto};
pub use model::{Attachment, Message, RetentionTransitionError};
