//! 业务接入埋点模型。
//!
//! 该模块只定义 Core 可观测业务事件的稳定数据结构和 Noop 默认实现。
//! 业务方通过 HookPlugin、CapabilityService、MQ/HTTP 网关消费这些事件，Core 不依赖任何业务 crate。

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::Ctx;
use crate::error::Result;

/// Core 对外承诺的业务埋点命名空间。
pub mod probe_name {
    pub const MESSAGE_PRE_SEND_INVOKED: &str = "core.hook.message.pre_send.invoked";
    pub const MESSAGE_PRE_SEND_ALLOWED: &str = "core.hook.message.pre_send.allowed";
    pub const MESSAGE_PRE_SEND_REJECTED: &str = "core.hook.message.pre_send.rejected";
    pub const MESSAGE_POST_SEND_INVOKED: &str = "core.hook.message.post_send.invoked";
    pub const MESSAGE_DELIVERY_OBSERVED: &str = "core.hook.message.delivery.observed";
    pub const MESSAGE_RECALL_INVOKED: &str = "core.hook.message.recall.invoked";
    pub const MESSAGE_READ_OBSERVED: &str = "core.hook.message.read.observed";
    pub const MESSAGE_REACTION_INVOKED: &str = "core.hook.message.reaction.invoked";
    pub const CONVERSATION_LIFECYCLE_OBSERVED: &str = "core.hook.conversation.lifecycle.observed";
    pub const CONVERSATION_MEMBER_INVOKED: &str = "core.hook.conversation.member.invoked";
    pub const MESSAGE_SEND_ACCEPTED: &str = "core.message.send.accepted";
    pub const MESSAGE_SEND_REJECTED: &str = "core.message.send.rejected";
    pub const MESSAGE_PERSISTED: &str = "core.message.persisted";
    pub const MESSAGE_RECALLED: &str = "core.message.recalled";
    pub const CONVERSATION_CREATED: &str = "core.conversation.created";
    pub const CONVERSATION_UPDATED: &str = "core.conversation.updated";
    pub const CONVERSATION_PARTICIPANTS_CHANGED: &str = "core.conversation.participants.changed";
    pub const SYNC_EXECUTED: &str = "core.sync.executed";
    pub const PUSH_ENQUEUED: &str = "core.push.enqueued";
    pub const CAPABILITY_DISPATCHED: &str = "core.capability.dispatched";
}

/// 业务接入埋点类别。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BusinessProbeKind {
    Message,
    Conversation,
    Participant,
    Hook,
    Sync,
    Push,
    Capability,
    Security,
    Custom,
}

/// 埋点建议投递语义。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BusinessProbeDelivery {
    /// 内存/日志观测即可，不能影响主链。
    BestEffort,
    /// 需要进入审计/MQ，但失败仍不阻断主链。
    ReliableAsync,
    /// 仅用于业务明确要求的强审计点；默认不建议。
    BlockingAudit,
}

/// Core 业务接入埋点事件。
///
/// 字段保持轻量稳定；复杂业务数据放入 `payload`，并通过 `schema` 标识版本。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BusinessProbeEvent {
    pub name: String,
    pub kind: BusinessProbeKind,
    pub delivery: BusinessProbeDelivery,
    pub schema: String,
    pub tenant_id: Option<String>,
    pub user_id: Option<String>,
    pub request_id: String,
    pub trace_id: String,
    pub conversation_id: Option<String>,
    pub message_id: Option<String>,
    pub subject_id: Option<String>,
    pub attributes: HashMap<String, String>,
    pub payload: Value,
}

impl BusinessProbeEvent {
    pub fn new(
        ctx: &Ctx,
        name: impl Into<String>,
        kind: BusinessProbeKind,
        delivery: BusinessProbeDelivery,
    ) -> Self {
        Self {
            name: name.into(),
            kind,
            delivery,
            schema: "flare.im.business_probe.v1".to_string(),
            tenant_id: ctx.tenant_id().map(str::to_string),
            user_id: ctx.user_id().map(str::to_string),
            request_id: ctx.request_id().to_string(),
            trace_id: ctx.trace_id().to_string(),
            conversation_id: None,
            message_id: None,
            subject_id: None,
            attributes: HashMap::new(),
            payload: Value::Null,
        }
    }

    pub fn conversation_id(mut self, conversation_id: impl Into<String>) -> Self {
        self.conversation_id = Some(conversation_id.into());
        self
    }

    pub fn message_id(mut self, message_id: impl Into<String>) -> Self {
        self.message_id = Some(message_id.into());
        self
    }

    pub fn subject_id(mut self, subject_id: impl Into<String>) -> Self {
        self.subject_id = Some(subject_id.into());
        self
    }

    pub fn attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }

    pub fn payload(mut self, payload: Value) -> Self {
        self.payload = payload;
        self
    }

    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = schema.into();
        self
    }
}

/// 业务埋点投递端口。
///
/// Core 默认使用 [`NoopBusinessProbeSink`]；业务接入时可在网关、编排器或独立 sidecar
/// 中替换为 MQ / Webhook / Capability Dispatch 实现。
#[async_trait]
pub trait BusinessProbeSink: Send + Sync {
    async fn emit(&self, ctx: &Ctx, event: BusinessProbeEvent) -> Result<()>;
}

/// 默认空实现，保证没有业务接入时 Core 可独立运行。
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopBusinessProbeSink;

#[async_trait]
impl BusinessProbeSink for NoopBusinessProbeSink {
    async fn emit(&self, _ctx: &Ctx, _event: BusinessProbeEvent) -> Result<()> {
        Ok(())
    }
}

pub type SharedBusinessProbeSink = Arc<dyn BusinessProbeSink>;
