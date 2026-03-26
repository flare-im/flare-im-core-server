//! Protobuf 消息构建器（Builder 模式）
//!
//! 统一 PushMessageRequest 等复杂结构的组装，便于维护与扩展。
//!
//! **架构落地**: 见 `doc/ARCHITECTURE_REFACTOR.md` §3（Builder）。
//! - 访问: `flare_im_core::abstractions::builders::PushMessageRequestBuilder`
//! - 使用: `MessageDomainService::build_push_request`（flare-orchestrator）使用本 Builder 组装推送请求。

use flare_proto::common::Message as ProtoMessage;
use flare_proto::push::{PushMessageRequest, PushOptions};
use std::collections::HashMap;

/// PushMessageRequest 构建器
///
/// 用于推送任务、领域事件转推送等场景，避免各处 `Default::default()` + 逐字段赋值。
#[derive(Default)]
pub struct PushMessageRequestBuilder {
    user_ids: Vec<String>,
    message: Option<ProtoMessage>,
    options: Option<PushOptions>,
    metadata: HashMap<String, String>,
}

impl PushMessageRequestBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn user_ids(mut self, user_ids: Vec<String>) -> Self {
        self.user_ids = user_ids;
        self
    }

    pub fn message(mut self, message: Option<ProtoMessage>) -> Self {
        self.message = message;
        self
    }

    pub fn options(mut self, options: Option<PushOptions>) -> Self {
        self.options = options;
        self
    }

    pub fn persist_if_offline(mut self, persist: bool) -> Self {
        self.options
            .get_or_insert_with(Default::default)
            .persist_if_offline = persist;
        self
    }

    pub fn priority(mut self, priority: i32) -> Self {
        self.options.get_or_insert_with(Default::default).priority = priority;
        self
    }

    pub fn metadata(mut self, data: HashMap<String, String>) -> Self {
        self.metadata = data;
        self
    }

    pub fn build(self) -> PushMessageRequest {
        PushMessageRequest {
            user_ids: self.user_ids,
            message: self.message,
            options: self.options,
            metadata: self.metadata,
        }
    }
}
