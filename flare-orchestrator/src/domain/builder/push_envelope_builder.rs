//! 推送信封构建器
//!
//! 提供统一的 PushEnvelope 构建能力，支持 ACK、通知、CustomData、系统消息。
//!
//! ## 设计原则
//! - Builder 模式：流式 API，易于使用
//! - 类型安全：编译时检查必填字段
//! - 默认值：合理的默认值，减少样板代码
//!
//! ## 使用示例
//! ```rust
//! use crate::domain::builder::PushEnvelopeBuilder;
//! use flare_proto::common::{AckPayload, PushTargetType};
//!
//! let envelope = PushEnvelopeBuilder::ack()
//!     .envelope_id("env-123")
//!     .tenant_id("tenant-1")
//!     .trace_id("trace-123")
//!     .target_users(vec!["user-1".to_string(), "user-2".to_string()])
//!     .ack_payload(AckPayload {
//!         message_id: "msg-123".to_string(),
//!         conversation_id: "conv-123".to_string(),
//!         seq: 100,
//!         ack_type: "received".to_string(),
//!         ack_at: 1234567890,
//!     })
//!     .priority(5)
//!     .build();
//! ```

use flare_proto::{
    AckPayload, CustomPayload, NotificationPayload, PushEnvelope, PushOptions, PushPayloadKind,
    PushTargetType, SystemPayload,
};
use std::time::{SystemTime, UNIX_EPOCH};

/// 推送信封构建器
pub struct PushEnvelopeBuilder {
    envelope_id: Option<String>,
    tenant_id: Option<String>,
    trace_id: Option<String>,
    created_at_ms: Option<i64>,
    target_type: PushTargetType,
    target_user_ids: Vec<String>,
    target_device_ids: Vec<String>,
    payload_kind: PushPayloadKind,
    options: Option<PushOptions>,
    ack: Option<AckPayload>,
    notification: Option<NotificationPayload>,
    custom: Option<CustomPayload>,
    system: Option<SystemPayload>,
    headers: std::collections::HashMap<String, String>,
}

impl PushEnvelopeBuilder {
    /// 创建 ACK 推送构建器
    pub fn ack() -> Self {
        Self {
            envelope_id: None,
            tenant_id: None,
            trace_id: None,
            created_at_ms: None,
            target_type: PushTargetType::Unspecified,
            target_user_ids: Vec::new(),
            target_device_ids: Vec::new(),
            payload_kind: PushPayloadKind::Ack,
            options: None,
            ack: None,
            notification: None,
            custom: None,
            system: None,
            headers: std::collections::HashMap::new(),
        }
    }

    /// 创建通知推送构建器
    pub fn notification() -> Self {
        Self {
            envelope_id: None,
            tenant_id: None,
            trace_id: None,
            created_at_ms: None,
            target_type: PushTargetType::Unspecified,
            target_user_ids: Vec::new(),
            target_device_ids: Vec::new(),
            payload_kind: PushPayloadKind::Notification,
            options: None,
            ack: None,
            notification: None,
            custom: None,
            system: None,
            headers: std::collections::HashMap::new(),
        }
    }

    /// 创建自定义数据推送构建器
    pub fn custom() -> Self {
        Self {
            envelope_id: None,
            tenant_id: None,
            trace_id: None,
            created_at_ms: None,
            target_type: PushTargetType::Unspecified,
            target_user_ids: Vec::new(),
            target_device_ids: Vec::new(),
            payload_kind: PushPayloadKind::Custom,
            options: None,
            ack: None,
            notification: None,
            custom: None,
            system: None,
            headers: std::collections::HashMap::new(),
        }
    }

    /// 创建系统消息推送构建器
    pub fn system() -> Self {
        Self {
            envelope_id: None,
            tenant_id: None,
            trace_id: None,
            created_at_ms: None,
            target_type: PushTargetType::Unspecified,
            target_user_ids: Vec::new(),
            target_device_ids: Vec::new(),
            payload_kind: PushPayloadKind::System,
            options: None,
            ack: None,
            notification: None,
            custom: None,
            system: None,
            headers: std::collections::HashMap::new(),
        }
    }

    /// 设置信封 ID
    pub fn envelope_id(mut self, id: impl Into<String>) -> Self {
        self.envelope_id = Some(id.into());
        self
    }

    /// 设置租户 ID
    pub fn tenant_id(mut self, id: impl Into<String>) -> Self {
        self.tenant_id = Some(id.into());
        self
    }

    /// 设置追踪 ID
    pub fn trace_id(mut self, id: impl Into<String>) -> Self {
        self.trace_id = Some(id.into());
        self
    }

    /// 设置创建时间（毫秒时间戳）
    pub fn created_at_ms(mut self, ts: i64) -> Self {
        self.created_at_ms = Some(ts);
        self
    }

    /// 设置全量推送目标（所有在线设备）
    pub fn target_all(mut self) -> Self {
        self.target_type = PushTargetType::All;
        self.target_user_ids.clear();
        self.target_device_ids.clear();
        self
    }

    /// 设置用户列表推送目标
    pub fn target_users(mut self, user_ids: Vec<String>) -> Self {
        self.target_type = PushTargetType::Users;
        self.target_user_ids = user_ids;
        self.target_device_ids.clear();
        self
    }

    /// 设置设备列表推送目标
    pub fn target_devices(mut self, device_ids: Vec<String>) -> Self {
        self.target_type = PushTargetType::Devices;
        self.target_device_ids = device_ids;
        self.target_user_ids.clear();
        self
    }

    /// 设置 ACK 载荷
    pub fn ack_payload(mut self, payload: AckPayload) -> Self {
        self.ack = Some(payload);
        self
    }

    /// 设置通知载荷
    pub fn notification_payload(mut self, payload: NotificationPayload) -> Self {
        self.notification = Some(payload);
        self
    }

    /// 设置自定义数据载荷
    pub fn custom_payload(mut self, payload: CustomPayload) -> Self {
        self.custom = Some(payload);
        self
    }

    /// 设置系统消息载荷
    pub fn system_payload(mut self, payload: SystemPayload) -> Self {
        self.system = Some(payload);
        self
    }

    /// 设置优先级（0-9，默认5）
    pub fn priority(mut self, priority: i32) -> Self {
        let mut options = self.options.unwrap_or_default();
        options.priority = priority;
        self.options = Some(options);
        self
    }

    /// 设置过期时间（毫秒时间戳）
    pub fn expire_at_ms(mut self, ts: i64) -> Self {
        let mut options = self.options.unwrap_or_default();
        options.expire_at = (ts > 0).then_some(ts);
        self.options = Some(options);
        self
    }

    /// 设置是否需要客户端确认
    pub fn require_ack(mut self, require: bool) -> Self {
        let mut options = self.options.unwrap_or_default();
        options.require_ack = require;
        self.options = Some(options);
        self
    }

    /// 设置重试次数
    pub fn retry_count(mut self, count: i32) -> Self {
        let mut options = self.options.unwrap_or_default();
        options.retry_count = count;
        self.options = Some(options);
        self
    }

    /// 设置重试延迟（毫秒）
    pub fn retry_delay_ms(mut self, delay: i32) -> Self {
        let mut options = self.options.unwrap_or_default();
        options.retry_delay_ms = delay;
        self.options = Some(options);
        self
    }

    /// 添加扩展选项
    pub fn extra_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let mut options = self.options.unwrap_or_default();
        options.attributes.insert(key.into(), value.into());
        self.options = Some(options);
        self
    }

    /// 添加头信息
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// 构建推送信封
    ///
    /// # Panics
    /// 如果未设置 envelope_id、tenant_id 或对应的 payload，将 panic
    pub fn build(self) -> PushEnvelope {
        let envelope_id = self.envelope_id.expect("envelope_id is required");
        let tenant_id = self.tenant_id.expect("tenant_id is required");
        let created_at_ms = self.created_at_ms.unwrap_or_else(current_time_ms);

        // 构建 payload oneof
        let payload = match self.payload_kind {
            PushPayloadKind::Ack => Some(flare_proto::push_envelope::Payload::Ack(
                self.ack.expect("ack payload is required for ACK kind"),
            )),
            PushPayloadKind::Notification => {
                Some(flare_proto::push_envelope::Payload::Notification(
                    self.notification
                        .expect("notification payload is required for Notification kind"),
                ))
            }
            PushPayloadKind::Custom => Some(flare_proto::push_envelope::Payload::Custom(
                self.custom
                    .expect("custom payload is required for Custom kind"),
            )),
            PushPayloadKind::System => Some(flare_proto::push_envelope::Payload::System(
                self.system
                    .expect("system payload is required for System kind"),
            )),
            PushPayloadKind::Unspecified => {
                panic!("payload kind must be specified");
            }
        };

        PushEnvelope {
            envelope_id,
            tenant_id,
            trace_id: self.trace_id.unwrap_or_default(),
            created_at: created_at_ms,
            target_type: self.target_type as i32,
            target_user_ids: self.target_user_ids,
            target_device_ids: self.target_device_ids,
            payload_kind: self.payload_kind as i32,
            options: self.options,
            payload,
            headers: self.headers,
        }
    }
}

/// 获取当前时间戳（毫秒）
fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// 快速构建 ACK 推送信封
pub fn build_ack_push(
    envelope_id: impl Into<String>,
    tenant_id: impl Into<String>,
    trace_id: impl Into<String>,
    target_user_ids: Vec<String>,
    ack: AckPayload,
) -> PushEnvelope {
    PushEnvelopeBuilder::ack()
        .envelope_id(envelope_id)
        .tenant_id(tenant_id)
        .trace_id(trace_id)
        .target_users(target_user_ids)
        .ack_payload(ack)
        .build()
}

/// 快速构建通知推送信封
pub fn build_notification_push(
    envelope_id: impl Into<String>,
    tenant_id: impl Into<String>,
    trace_id: impl Into<String>,
    target_user_ids: Vec<String>,
    notification: NotificationPayload,
) -> PushEnvelope {
    PushEnvelopeBuilder::notification()
        .envelope_id(envelope_id)
        .tenant_id(tenant_id)
        .trace_id(trace_id)
        .target_users(target_user_ids)
        .notification_payload(notification)
        .build()
}

/// 快速构建自定义数据推送信封
pub fn build_custom_push(
    envelope_id: impl Into<String>,
    tenant_id: impl Into<String>,
    trace_id: impl Into<String>,
    target_user_ids: Vec<String>,
    custom: CustomPayload,
) -> PushEnvelope {
    PushEnvelopeBuilder::custom()
        .envelope_id(envelope_id)
        .tenant_id(tenant_id)
        .trace_id(trace_id)
        .target_users(target_user_ids)
        .custom_payload(custom)
        .build()
}

/// 快速构建系统消息推送信封
pub fn build_system_push(
    envelope_id: impl Into<String>,
    tenant_id: impl Into<String>,
    trace_id: impl Into<String>,
    target_type: PushTargetType,
    target_user_ids: Vec<String>,
    system: SystemPayload,
) -> PushEnvelope {
    let mut builder = PushEnvelopeBuilder::system()
        .envelope_id(envelope_id)
        .tenant_id(tenant_id)
        .trace_id(trace_id)
        .system_payload(system);

    builder = match target_type {
        PushTargetType::All => builder.target_all(),
        PushTargetType::Users => builder.target_users(target_user_ids),
        PushTargetType::Devices => builder.target_devices(target_user_ids),
        PushTargetType::Unspecified => builder.target_users(target_user_ids),
    };

    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_ack_push() {
        let envelope = PushEnvelopeBuilder::ack()
            .envelope_id("env-123")
            .tenant_id("tenant-1")
            .trace_id("trace-123")
            .target_users(vec!["user-1".to_string()])
            .ack_payload(AckPayload {
                message_id: "msg-123".to_string(),
                conversation_id: "conv-123".to_string(),
                seq: 100,
                ack_type: "received".to_string(),
                ack_at: 1234567890,
            })
            .priority(5)
            .build();

        assert_eq!(envelope.envelope_id, "env-123");
        assert_eq!(envelope.tenant_id, "tenant-1");
        assert_eq!(envelope.trace_id, "trace-123");
        assert_eq!(envelope.target_user_ids, vec!["user-1"]);
        assert_eq!(envelope.payload_kind, PushPayloadKind::Ack as i32);
    }

    #[test]
    fn test_build_notification_push() {
        let envelope = PushEnvelopeBuilder::notification()
            .envelope_id("env-456")
            .tenant_id("tenant-1")
            .target_all()
            .notification_payload(NotificationPayload {
                notification_id: "notif-123".to_string(),
                title: "System Update".to_string(),
                body: "New version available".to_string(),
                icon: String::new(),
                sound: String::new(),
                click_action: String::new(),
                attributes: std::collections::HashMap::new(),
                created_at: 1234567890,
            })
            .build();

        assert_eq!(envelope.envelope_id, "env-456");
        assert_eq!(envelope.target_type, PushTargetType::All as i32);
        assert_eq!(envelope.payload_kind, PushPayloadKind::Notification as i32);
    }
}
