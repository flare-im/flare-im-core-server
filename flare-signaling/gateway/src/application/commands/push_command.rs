//! 推送相关命令（与 access_gateway.proto 对齐）
//!
//! PushMessage / PushEvent / PushNotification / PushAck / PushCustom 均使用 user_ids + options。

use flare_grpc_proto::access_gateway::PushOptions;
use flare_proto::common::{Ack, CustomData, Event, Message, NotificationMessage};

/// 推送消息命令（对应 PushMessageRequest）
#[derive(Debug, Clone)]
pub struct PushMessageCommand {
    /// 目标用户 ID 列表（至少一个）
    pub user_ids: Vec<String>,
    /// 消息列表（至少一条）
    pub messages: Vec<Message>,
    /// 推送选项，空则用默认（至少一次、全部设备）
    pub options: Option<PushOptions>,
}

impl PushMessageCommand {
    pub fn new(
        user_ids: Vec<String>,
        messages: Vec<Message>,
        options: Option<PushOptions>,
    ) -> Self {
        Self {
            user_ids,
            messages,
            options,
        }
    }
}

/// 推送事件命令（对应 PushEventRequest）
#[derive(Debug, Clone)]
pub struct PushEventCommand {
    pub user_ids: Vec<String>,
    pub events: Vec<Event>,
    pub options: Option<PushOptions>,
    pub conversation_id: String,
    pub max_conversation_seq: u64,
    pub delivery_mode: i32,
    pub inline_events_truncated: bool,
}

impl PushEventCommand {
    pub fn new(
        user_ids: Vec<String>,
        events: Vec<Event>,
        options: Option<PushOptions>,
        conversation_id: String,
        max_conversation_seq: u64,
        delivery_mode: i32,
        inline_events_truncated: bool,
    ) -> Self {
        Self {
            user_ids,
            events,
            options,
            conversation_id,
            max_conversation_seq,
            delivery_mode,
            inline_events_truncated,
        }
    }
}

/// 推送通知命令（对应 PushNotificationRequest）
#[derive(Debug, Clone)]
pub struct PushNotificationCommand {
    pub user_ids: Vec<String>,
    pub notification: NotificationMessage,
    pub options: Option<PushOptions>,
}

impl PushNotificationCommand {
    pub fn new(
        user_ids: Vec<String>,
        notification: NotificationMessage,
        options: Option<PushOptions>,
    ) -> Self {
        Self {
            user_ids,
            notification,
            options,
        }
    }
}

/// 推送 ACK 命令（对应 PushAckRequest）
#[derive(Debug, Clone)]
pub struct PushAckCommand {
    pub user_ids: Vec<String>,
    pub ack: Ack,
    pub options: Option<PushOptions>,
}

impl PushAckCommand {
    pub fn new(user_ids: Vec<String>, ack: Ack, options: Option<PushOptions>) -> Self {
        Self {
            user_ids,
            ack,
            options,
        }
    }
}

/// 推送自定义数据命令（对应 PushCustomRequest）
#[derive(Debug, Clone)]
pub struct PushCustomDataCommand {
    pub user_ids: Vec<String>,
    pub custom_data: CustomData,
    pub options: Option<PushOptions>,
}

impl PushCustomDataCommand {
    pub fn new(
        user_ids: Vec<String>,
        custom_data: CustomData,
        options: Option<PushOptions>,
    ) -> Self {
        Self {
            user_ids,
            custom_data,
            options,
        }
    }
}
