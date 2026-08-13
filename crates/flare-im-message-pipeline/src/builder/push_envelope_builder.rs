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
//! use flare_im_message_pipeline::PushEnvelopeBuilder;
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

    /// 构建推送信封；缺字段时返回错误而不是 panic。
    ///
    /// 本 builder 是 `pub` 的（经 crate 根导出），业务侧与插件都能构造它。
    /// 一个 `pub` 的构造器把「调用方少设了一个字段」变成进程 panic，等于把
    /// 编程错误升级成一次服务中断——推送链路尤其不该这样。
    /// 仓内的便捷函数（`build_ack_push` 等）成对设置 kind 与 payload，
    /// 走不到这些错误分支；它们继续用 [`Self::build`]。
    pub fn try_build(self) -> Result<PushEnvelope, PushEnvelopeBuildError> {
        use PushEnvelopeBuildError as E;

        let envelope_id = self.envelope_id.ok_or(E::MissingField("envelope_id"))?;
        let tenant_id = self.tenant_id.ok_or(E::MissingField("tenant_id"))?;
        let created_at_ms = self.created_at_ms.unwrap_or_else(current_time_ms);

        // 构建 payload oneof
        let payload = match self.payload_kind {
            PushPayloadKind::Ack => Some(flare_proto::push_envelope::Payload::Ack(
                self.ack.ok_or(E::MissingPayload("Ack"))?,
            )),
            PushPayloadKind::Notification => {
                Some(flare_proto::push_envelope::Payload::Notification(
                    self.notification.ok_or(E::MissingPayload("Notification"))?,
                ))
            }
            PushPayloadKind::Custom => Some(flare_proto::push_envelope::Payload::Custom(
                self.custom.ok_or(E::MissingPayload("Custom"))?,
            )),
            PushPayloadKind::System => Some(flare_proto::push_envelope::Payload::System(
                self.system.ok_or(E::MissingPayload("System"))?,
            )),
            PushPayloadKind::Unspecified => return Err(E::UnspecifiedKind),
        };

        Ok(PushEnvelope {
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
        })
    }

    /// 构建推送信封。
    ///
    /// # Panics
    /// 缺少 envelope_id / tenant_id / 对应 payload，或未指定 kind 时 panic。
    /// 不确定字段是否齐全时用 [`Self::try_build`]。
    pub fn build(self) -> PushEnvelope {
        match self.try_build() {
            Ok(envelope) => envelope,
            Err(err) => panic!("build push envelope: {err}"),
        }
    }
}

/// 构建推送信封时的字段缺失。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushEnvelopeBuildError {
    /// 必填字段未设置
    MissingField(&'static str),
    /// 已声明 kind 但没给对应的 payload
    MissingPayload(&'static str),
    /// 没有声明 payload kind
    UnspecifiedKind,
}

impl std::fmt::Display for PushEnvelopeBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingField(name) => write!(f, "缺少必填字段 {name}"),
            Self::MissingPayload(kind) => {
                write!(f, "payload_kind 是 {kind}，但没有设置对应的 payload")
            }
            Self::UnspecifiedKind => f.write_str("未指定 payload_kind"),
        }
    }
}

impl std::error::Error for PushEnvelopeBuildError {}

/// 获取当前时间戳（毫秒）
fn current_time_ms() -> i64 {
    // 构造推送信封是消息热路径，时钟异常不该在这里 panic
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
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

#[cfg(test)]
mod try_build_tests {
    use super::*;

    #[test]
    fn missing_required_field_is_an_error_not_a_panic() {
        // 少一个必填字段就 panic，等于把调用方的编程错误升级成服务中断。
        let err = PushEnvelopeBuilder::ack()
            .tenant_id("t1")
            .ack_payload(AckPayload::default())
            .try_build()
            .expect_err("缺 envelope_id 应当返回错误");
        assert_eq!(err, PushEnvelopeBuildError::MissingField("envelope_id"));
    }

    #[test]
    fn kind_without_matching_payload_is_reported() {
        let err = PushEnvelopeBuilder::notification()
            .envelope_id("e1")
            .tenant_id("t1")
            .try_build()
            .expect_err("声明了 kind 却没给 payload，应当返回错误");
        assert_eq!(err, PushEnvelopeBuildError::MissingPayload("Notification"));
        // 错误要能直接读懂：它会出现在日志里，读的人未必看过这段代码
        assert!(err.to_string().contains("Notification"));
    }

    #[test]
    fn complete_builder_still_produces_the_same_envelope() {
        let envelope = PushEnvelopeBuilder::ack()
            .envelope_id("e1")
            .tenant_id("t1")
            .ack_payload(AckPayload::default())
            .try_build()
            .expect("字段齐全时不该失败");
        assert_eq!(envelope.envelope_id, "e1");
        assert_eq!(envelope.tenant_id, "t1");
    }
}
