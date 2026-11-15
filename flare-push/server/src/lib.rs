//! # Flare Push Server（以设计文档为准）
//!
//! 职责：
//! - 分别消费 `push-message` / `push-event` / `push-notification` / `push-ack` / `push-custom` topic
//! - 查询每个收件人的设备可达状态（flare-signaling/online 的会话记录）与设备推送绑定
//! - 路由到 `push-online` 与三条离线优先级通道（`offline` 单聊、`offline_group` 群、
//!   `offline_bulk` 批量）：在线任务按连接发，离线任务给每台有推送绑定且不活跃的设备各一条
//! - 消费设备交接（`flare.im.push.handoff`），对转入推送通道的设备补一次提示
//! - 防打扰闸门：每台候选设备按提示规则（`flare-notify-rules`）决定响铃、静默更新、合并、推迟或
//!   只更新角标；已读、撤回修正已发出的通知；到期循环补发尾部刷新与推迟到期的提示
//! - 群会话的波次：按会话限频枚举接收者，超过成员上限的群只枚举订阅者，点名的成员直接推，
//!   @所有人 按会话限频
//! - 失败转入 `push-dlq`

pub mod application;
pub mod config;
pub mod domain;
pub mod infrastructure;
pub mod interface;
pub mod service;

pub use service::ApplicationBootstrap;
