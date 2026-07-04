//! 仓储接口定义（Port）
//!
//! 各 Repository 独立文件，与 writer 一致：
//! - [MessageStorage]：消息与操作/事件/同步/标签查询与更新。

pub mod message_storage;

pub use message_storage::{MessageStorage, is_backfill_tail_page};
