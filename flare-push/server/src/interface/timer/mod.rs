//! 接口层：定时驱动的入口。
//!
//! - `NotifyDueLoop`：领取防打扰闸门到期队列里的条目（尾部刷新、推迟的提示、群会话的波次），
//!   交给路由处理。

pub mod notify_due;

pub use notify_due::{DueContextFactory, NotifyDueLoop};
